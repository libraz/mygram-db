"""Replication of MariaDB COMPRESSED columns through both ingest paths."""

from __future__ import annotations

import os
import socket
import subprocess
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path

import pytest

from lib.mygramdb_client import MygramdbClient
from lib.mysql_client import MysqlClient
from lib.wait import wait_until, wait_until_value

pytestmark = [pytest.mark.mysql, pytest.mark.mariadb_only]

E2E_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = E2E_ROOT.parent
MYGRAMDB_BINARY = Path(os.environ.get("MYGRAMDB_BINARY", PROJECT_ROOT / "build/bin/mygramdb"))
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "13306"))
TABLE = "compressed_docs"


def _free_port() -> int:
    """Reserve an ephemeral loopback port long enough to learn its number."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _write_config(path: Path, tcp_port: int, http_port: int, dump_dir: Path) -> None:
    """Write a config that indexes only the compressed table."""
    path.write_text(
        f"""mysql:
  host: "127.0.0.1"
  port: {MYSQL_PORT}
  user: "repl_user"
  password: "test_password"
  database: "testdb"
  use_gtid: true
  connect_timeout_ms: 5000
  datetime_timezone: "+00:00"

tables:
  - name: "{TABLE}"
    primary_key: "id"
    text_source:
      concat: ["title", "content"]
    ngram_size: 2
    kanji_ngram_size: 1

replication:
  enable: true
  auto_initial_snapshot: true
  server_id: 199997
  start_from: "snapshot"

dump:
  dir: "{dump_dir}"
  interval_sec: 0

api:
  tcp:
    bind: "127.0.0.1"
    port: {tcp_port}
  http:
    enable: true
    bind: "127.0.0.1"
    port: {http_port}

network:
  allow_cidrs:
    - "127.0.0.0/8"

logging:
  level: "info"
  format: "json"
  file: ""
""",
        encoding="utf-8",
    )


@contextmanager
def _mygramdb_instance(
    config: Path, tcp_port: int, http_port: int, log_path: Path
) -> Generator[MygramdbClient, None, None]:
    """Run a dedicated MygramDB process against the session database."""
    log_file = log_path.open("w", encoding="utf-8")
    process_env = os.environ.copy()
    for name in (
        "MYGRAM_MYSQL_HOST",
        "MYGRAM_MYSQL_PORT",
        "MYGRAM_MYSQL_USER",
        "MYGRAM_MYSQL_PASSWORD",
        "MYGRAM_MYSQL_DATABASE",
    ):
        process_env.pop(name, None)
    process = subprocess.Popen(
        [str(MYGRAMDB_BINARY), "-c", str(config)],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        cwd=PROJECT_ROOT,
        env=process_env,
    )
    client = MygramdbClient("127.0.0.1", tcp_port=tcp_port, http_port=http_port)
    try:
        wait_until(
            client.ping, timeout=90, interval=0.5, description="MygramDB to accept connections"
        )
        wait_until(client.health_ready, timeout=90, interval=0.5, description="MygramDB readiness")
        yield client
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        log_file.close()


@pytest.fixture
def compressed_table(mysql: MysqlClient) -> Generator[None, None, None]:
    mysql.execute(f"DROP TABLE IF EXISTS {TABLE}")
    mysql.execute(
        f"""CREATE TABLE {TABLE} (
            id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
            title VARCHAR(200) COMPRESSED NOT NULL,
            content TEXT COMPRESSED NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"""
    )
    try:
        yield
    finally:
        mysql.execute(f"DROP TABLE IF EXISTS {TABLE}")


@pytest.mark.timeout(240)
def test_compressed_columns_replicate_like_plain_text(
    mysql: MysqlClient, compressed_table: None, tmp_path: Path
) -> None:
    """Snapshot and binlog rows of COMPRESSED columns index the stored text."""
    marker = f"cc{uuid.uuid4().hex[:8]}"
    # Long, repetitive values so the server stores them deflated rather than
    # behind its "not compressed" header; the short title exercises the latter.
    long_ascii = " ".join([f"{marker}snap"] * 80)
    mysql.insert_rows(TABLE, [{"title": "t", "content": long_ascii}])

    tcp_port = _free_port()
    http_port = _free_port()
    dump_dir = tmp_path / "dumps"
    dump_dir.mkdir()
    config = tmp_path / "mygramdb-column-compression.yaml"
    _write_config(config, tcp_port, http_port, dump_dir)
    table = f"testdb.{TABLE}"

    with _mygramdb_instance(config, tcp_port, http_port, tmp_path / "mygramdb.log") as client:
        assert client.count(table, f"{marker}snap") == 1, "the snapshot row was not indexed"

        japanese = "全文検索エンジンの圧縮列" * 60
        mysql.insert_rows(
            TABLE,
            [
                {"title": f"{marker}head", "content": " ".join([f"{marker}live"] * 80)},
                {"title": "j", "content": japanese},
                {"title": "e", "content": ""},
            ],
        )
        wait_until_value(
            lambda: client.count(table, f"{marker}live"),
            expected=1,
            timeout=60,
            interval=0.5,
            description="the deflated binlog row to be indexed",
        )
        assert client.count(table, f"{marker}head") == 1, "the stored-as-is title was not indexed"
        assert client.count(table, "圧縮列") == 1, "the deflated Japanese row was not indexed"

        mysql.update(TABLE, f"content = '{' '.join([f'{marker}upd'] * 80)}'", "title = 'j'")
        wait_until_value(
            lambda: client.count(table, f"{marker}upd"),
            expected=1,
            timeout=60,
            interval=0.5,
            description="the updated compressed row to be reindexed",
        )
        assert client.count(table, "圧縮列") == 0, "the pre-update text is still indexed"

        mysql.delete(TABLE, f"title = '{marker}head'")
        wait_until_value(
            lambda: client.count(table, f"{marker}live"),
            expected=0,
            timeout=60,
            interval=0.5,
            description="the deleted compressed row to leave the index",
        )

        status = client.tcp_command_multiline(
            "REPLICATION STATUS", timeout=5.0, terminator=b"END\r\n"
        )
        assert status is not None and "status: running" in status, status
