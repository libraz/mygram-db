"""Reject dumps created against a different physical database instance."""

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
from lib.wait import wait_until

pytestmark = pytest.mark.persistence

E2E_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = E2E_ROOT.parent
MYGRAMDB_BINARY = Path(os.environ.get("MYGRAMDB_BINARY", PROJECT_ROOT / "build/bin/mygramdb"))


def _free_port() -> int:
    """Reserve an ephemeral loopback port long enough to learn its number."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _database_container_command(name: str, port: int, server_id: int) -> list[str]:
    """Build an isolated MySQL/MariaDB container command for the active matrix flavor."""
    flavor = os.environ.get("DB_FLAVOR", "mysql")
    version = os.environ.get("MYSQL_VERSION", "8.4")
    password = os.environ.get("MYSQL_TEST_PASSWORD", "test_root_password")
    if flavor == "mariadb":
        image = f"mariadb:{version}"
        init_dir = E2E_ROOT / "docker/mariadb-init"
        environment = [
            "-e",
            f"MARIADB_ROOT_PASSWORD={password}",
            "-e",
            "MARIADB_DATABASE=testdb",
            "-e",
            "MARIADB_USER=repl_user",
            "-e",
            "MARIADB_PASSWORD=test_password",
        ]
        server_args = [
            f"--server-id={server_id}",
            "--log-bin=mariadb-bin",
            "--binlog-format=ROW",
            "--binlog-row-image=FULL",
            "--character-set-server=utf8mb4",
            "--collation-server=utf8mb4_unicode_ci",
        ]
    else:
        image = f"mysql:{version}"
        init_dir = E2E_ROOT / "docker/mysql-init"
        environment = [
            "-e",
            f"MYSQL_ROOT_PASSWORD={password}",
            "-e",
            "MYSQL_DATABASE=testdb",
            "-e",
            "MYSQL_USER=repl_user",
            "-e",
            "MYSQL_PASSWORD=test_password",
        ]
        server_args = [
            f"--server-id={server_id}",
            "--log-bin=mysql-bin",
            "--gtid-mode=ON",
            "--enforce-gtid-consistency=ON",
            "--binlog-row-image=FULL",
            "--character-set-server=utf8mb4",
            "--collation-server=utf8mb4_unicode_ci",
        ]

    return [
        "docker",
        "run",
        "--detach",
        "--name",
        name,
        "--publish",
        f"127.0.0.1:{port}:3306",
        *environment,
        "--volume",
        f"{init_dir}:/docker-entrypoint-initdb.d:ro",
        image,
        *server_args,
    ]


@contextmanager
def _database_instance(port: int, server_id: int) -> Generator[MysqlClient, None, None]:
    """Run one fresh database instance and remove its anonymous data volume afterward."""
    name = f"mygramdb-dump-source-{uuid.uuid4().hex[:12]}"
    subprocess.run(
        _database_container_command(name, port, server_id),
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    client = MysqlClient(host="127.0.0.1", port=port)
    try:
        wait_until(
            client.ping,
            timeout=90,
            interval=1,
            description=f"isolated database {server_id} to accept connections",
        )
        yield client
    finally:
        subprocess.run(
            ["docker", "rm", "--force", "--volumes", name],
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )


def _write_config(
    path: Path, mysql_port: int, tcp_port: int, http_port: int, dump_dir: Path
) -> None:
    """Write the minimal identical endpoint config used against both physical instances."""
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
  server_id: 199999
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
    """Run a dedicated MygramDB process for one physical source instance."""
    log_file = log_path.open("w", encoding="utf-8")
    process_env = os.environ.copy()
    # Matrix/run-all variables target the session-scoped primary database.
    # The dedicated process must use the endpoint in its generated config.
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
            description="isolated MygramDB to accept connections",
        )
        wait_until(
            client.health_ready,
            timeout=90,
            interval=0.5,
            description="isolated MygramDB replication readiness",
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


def _wait_for_dump_save(client: MygramdbClient) -> None:
    """Wait for asynchronous DUMP SAVE completion and worker-slot release."""

    def completed() -> bool:
        status = client.tcp_command_multiline(
            "DUMP STATUS",
            timeout=5.0,
            terminator=b"END\r\n",
        )
        if status is not None and "status: FAILED" in status:
            raise AssertionError(f"DUMP SAVE failed:\n{status}")
        return bool(
            status and "status: COMPLETED" in status and "save_in_progress: false" in status
        )

    wait_until(completed, timeout=60, interval=0.25, description="isolated DUMP SAVE completion")


@pytest.mark.timeout(240)
def test_dump_load_rejects_different_physical_source(tmp_path: Path) -> None:
    """A dump cannot cross two fresh servers hidden behind the same endpoint."""
    mysql_port = _free_port()
    tcp_port = _free_port()
    http_port = _free_port()
    dump_dir = tmp_path / "dumps"
    dump_dir.mkdir()
    config = tmp_path / "mygramdb-cross-instance.yaml"
    _write_config(config, mysql_port, tcp_port, http_port, dump_dir)
    marker = f"cross_instance_{uuid.uuid4().hex}"

    with _database_instance(mysql_port, server_id=101) as source_a:
        source_a.insert_rows(
            "articles",
            [
                {
                    "title": "Source A",
                    "content": marker,
                    "status": 1,
                    "category": "source-a",
                    "enabled": 1,
                }
            ],
        )
        with _mygramdb_instance(config, tcp_port, http_port, tmp_path / "source-a.log") as mygram_a:
            assert mygram_a.count("testdb.articles", marker) == 1
            assert mygram_a.dump_save(), "DUMP SAVE should start against source A"
            _wait_for_dump_save(mygram_a)
            assert (dump_dir / "mygramdb.dmp").is_file()

    # Reuse the exact endpoint/config with a fresh data directory and identity.
    with (
        _database_instance(mysql_port, server_id=202),
        _mygramdb_instance(config, tcp_port, http_port, tmp_path / "source-b.log") as mygram_b,
    ):
        assert mygram_b.count("testdb.articles", marker) == 0
        response = mygram_b.tcp_command("DUMP LOAD mygramdb.dmp", timeout=60.0)
        assert response is not None and response.startswith("ERROR "), response
        assert "source server UUID does not match" in response, response
        assert mygram_b.count("testdb.articles", marker) == 0
        assert "running" in mygram_b.replication_status().lower()
