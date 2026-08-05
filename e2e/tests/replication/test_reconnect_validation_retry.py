"""Exercise retryable schema validation failure through a real MySQL connection."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from lib.mygramdb_client import MygramdbClient
from lib.mysql_fault_proxy import MySQLFaultProxy
from lib.wait import wait_until, wait_until_gte

pytestmark = [pytest.mark.replication, pytest.mark.mysql_only]

PROJECT_ROOT = Path(__file__).resolve().parents[3]
MYGRAMDB_BINARY = Path(os.environ.get("MYGRAMDB_BINARY", PROJECT_ROOT / "build/bin/mygramdb"))


def _free_port() -> int:
    """Return an ephemeral loopback port for an isolated MygramDB process."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _write_config(path: Path, mysql_port: int, tcp_port: int, http_port: int) -> None:
    """Write a one-table config whose MySQL endpoint is the fault proxy."""
    path.write_text(
        f"""mysql:
  host: "127.0.0.1"
  port: {mysql_port}
  user: "repl_user"
  password: "test_password"
  database: "testdb"
  use_gtid: true
  connect_timeout_ms: 5000
  datetime_timezone: "+00:00"

tables:
  - name: "articles"
    primary_key: "id"
    text_source:
      column: "content"
    ngram_size: 2
    kanji_ngram_size: 1

replication:
  enable: true
  auto_initial_snapshot: true
  server_id: 299999
  start_from: "snapshot"

dump:
  dir: "{path.parent / "dumps"}"
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
  level: "debug"
  format: "json"
  file: ""
""",
        encoding="utf-8",
    )


@contextmanager
def _mygramdb_instance(
    config: Path, tcp_port: int, http_port: int, log_path: Path
) -> Generator[MygramdbClient, None, None]:
    """Run a dedicated process so the session MygramDB remains undisturbed."""
    log_file = log_path.open("wb")
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
            client.ping,
            timeout=90,
            interval=0.5,
            description="dedicated MygramDB to accept connections",
        )
        wait_until(
            client.health_ready,
            timeout=90,
            interval=0.5,
            description="dedicated MygramDB replication readiness",
        )
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


def _binlog_connection_ids(mysql: Any) -> set[int]:
    """Return active replication-stream connection IDs."""
    rows = mysql.execute(
        "SELECT ID FROM information_schema.PROCESSLIST "
        "WHERE COMMAND IN ('Binlog Dump', 'Binlog Dump GTID')"
    )
    return {int(row["ID"]) for row in rows}


def _log_records_after(path: Path, offset: int) -> list[dict[str, Any]]:
    """Parse structured JSON records appended after a known log offset."""
    try:
        with path.open("rb") as log_file:
            log_file.seek(offset)
            lines = log_file.read().decode("utf-8", errors="replace").splitlines()
    except (FileNotFoundError, OSError):
        return []

    records: list[dict[str, Any]] = []
    for line in lines:
        json_start = line.find("{")
        if json_start < 0:
            continue
        try:
            record = json.loads(line[json_start:])
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            records.append(record)
    return records


def _has_recovery_sequence(records: list[dict[str, Any]]) -> bool:
    """Require transient validation failure, successful retry, then stream reopen."""
    expected = (
        ("binlog_error", "type", "connection_validation_failed"),
        ("binlog_debug", "action", "connection_validated_after_reconnect"),
        ("binlog_stream_opened", None, None),
    )
    position = 0
    for record in records:
        event, field, value = expected[position]
        if record.get("event") != event:
            continue
        if field is not None and record.get(field) != value:
            continue
        if event == "binlog_error" and record.get("context") != "after_reconnect":
            continue
        position += 1
        if position == len(expected):
            return True
    return False


@pytest.mark.timeout(180)
@pytest.mark.parametrize(
    "fault_query_fragment",
    [
        pytest.param(b"SELECT @@GLOBAL.gtid_mode", id="early-gtid-mode"),
        pytest.param(
            b"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA",
            id="middle-required-table",
        ),
        pytest.param(
            b"SHOW VARIABLES LIKE 'binlog_checksum'",
            id="late-binlog-checksum",
        ),
    ],
)
def test_reconnect_retries_transport_failure_during_schema_validation(
    mysql: Any,
    mygramdb: MygramdbClient,
    seed_data: None,
    tmp_path: Path,
    fault_query_fragment: bytes,
) -> None:
    """A reset during post-reconnect table validation is retried without data loss."""
    assert "running" in mygramdb.replication_status().lower()
    before_ids = _binlog_connection_ids(mysql)
    tcp_port = _free_port()
    http_port = _free_port()
    config = tmp_path / "mygramdb-reconnect-validation.yaml"
    log_path = tmp_path / "mygramdb-reconnect-validation.log"
    marker = f"validation_retry_{uuid.uuid4().hex}"
    inserted = False

    with MySQLFaultProxy("127.0.0.1", int(os.environ.get("MYSQL_PORT", "23306"))) as proxy:
        _write_config(config, proxy.port, tcp_port, http_port)
        with _mygramdb_instance(config, tcp_port, http_port, log_path) as dedicated:
            wait_until(
                lambda: bool(_binlog_connection_ids(mysql) - before_ids),
                timeout=15,
                interval=0.25,
                description="dedicated binlog connection to appear",
            )
            dedicated_ids = _binlog_connection_ids(mysql) - before_ids
            assert len(dedicated_ids) == 1, f"unexpected dedicated binlog IDs: {dedicated_ids}"

            log_offset = log_path.stat().st_size
            proxy.arm_query_reset_once(fault_query_fragment)
            mysql.execute(f"KILL CONNECTION {dedicated_ids.pop()}")

            assert proxy.wait_for_fault(15), "schema validation query did not reach fault proxy"
            wait_until(
                lambda: _has_recovery_sequence(_log_records_after(log_path, log_offset)),
                timeout=30,
                interval=0.25,
                description="validation retry and binlog stream reopen",
            )
            assert proxy.fault_count == 1
            assert "running" in dedicated.replication_status().lower()

            try:
                mysql.insert_rows(
                    "articles",
                    [
                        {
                            "title": "Reconnect validation retry",
                            "content": marker,
                            "status": 1,
                            "category": "reconnect",
                            "enabled": 1,
                        }
                    ],
                )
                inserted = True
                wait_until_gte(
                    lambda: dedicated.count("testdb.articles", marker),
                    minimum=1,
                    timeout=20,
                    interval=0.25,
                    description="INSERT propagation after validation retry",
                )
            finally:
                if inserted:
                    mysql.execute("DELETE FROM articles WHERE content = %s", (marker,))


@pytest.mark.timeout(180)
def test_transient_ddl_metadata_failures_do_not_consume_deterministic_replay_budget(
    mysql: Any,
    mygramdb: MygramdbClient,
    seed_data: None,
    tmp_path: Path,
) -> None:
    """Four same-GTID metadata resets recover instead of hitting the three-replay cap."""
    assert "running" in mygramdb.replication_status().lower()
    before_ids = _binlog_connection_ids(mysql)
    tcp_port = _free_port()
    http_port = _free_port()
    config = tmp_path / "mygramdb-ddl-metadata-retry.yaml"
    log_path = tmp_path / "mygramdb-ddl-metadata-retry.log"
    column_name = f"c4_retry_{uuid.uuid4().hex[:12]}"
    marker = f"ddl_metadata_retry_{uuid.uuid4().hex}"
    column_added = False
    inserted = False

    with MySQLFaultProxy("127.0.0.1", int(os.environ.get("MYSQL_PORT", "23306"))) as proxy:
        _write_config(config, proxy.port, tcp_port, http_port)
        with _mygramdb_instance(config, tcp_port, http_port, log_path) as dedicated:
            wait_until(
                lambda: bool(_binlog_connection_ids(mysql) - before_ids),
                timeout=15,
                interval=0.25,
                description="dedicated binlog connection to appear",
            )

            proxy.arm_query_reset(b"SHOW FULL COLUMNS FROM", count=4)
            try:
                mysql.execute(f"ALTER TABLE articles ADD COLUMN `{column_name}` INT NULL")
                column_added = True
                wait_until(
                    lambda: proxy.fault_count >= 4,
                    timeout=45,
                    interval=0.25,
                    description="four post-DDL metadata query resets",
                )
                wait_until(
                    lambda: "running" in dedicated.replication_status().lower(),
                    timeout=45,
                    interval=0.25,
                    description="replication recovery after repeated metadata resets",
                )

                mysql.insert_rows(
                    "articles",
                    [
                        {
                            "title": "DDL metadata retry",
                            "content": marker,
                            "status": 1,
                            "category": "reconnect",
                            "enabled": 1,
                        }
                    ],
                )
                inserted = True
                wait_until_gte(
                    lambda: dedicated.count("testdb.articles", marker),
                    minimum=1,
                    timeout=30,
                    interval=0.25,
                    description="INSERT propagation after repeated DDL metadata resets",
                )
                assert proxy.fault_count == 4
            finally:
                if inserted:
                    mysql.execute("DELETE FROM articles WHERE content = %s", (marker,))
                if column_added:
                    mysql.execute(f"ALTER TABLE articles DROP COLUMN `{column_name}`")
