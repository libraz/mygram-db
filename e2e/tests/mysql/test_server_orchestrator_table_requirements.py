"""Startup refuses an unsupported table definition before the initial snapshot."""

from __future__ import annotations

import os
import socket
import subprocess
from pathlib import Path

import pytest

from lib.mysql_client import MysqlClient

pytestmark = [pytest.mark.mysql, pytest.mark.dedicated_process]

E2E_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = E2E_ROOT.parent
MYGRAMDB_BINARY = Path(os.environ.get("MYGRAMDB_BINARY", PROJECT_ROOT / "build/bin/mygramdb"))
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "23306"))


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _config(table: str, primary_key: str, dump_dir: Path) -> str:
    return f"""mysql:
  host: "127.0.0.1"
  port: {MYSQL_PORT}
  user: "repl_user"
  password: "test_password"
  database: "testdb"
  use_gtid: true
  connect_timeout_ms: 5000

tables:
  - name: "{table}"
    primary_key: "{primary_key}"
    text_source:
      column: "content"

replication:
  enable: true
  auto_initial_snapshot: true
  server_id: 199995
  start_from: "snapshot"

dump:
  dir: "{dump_dir}"
  interval_sec: 0

api:
  tcp:
    bind: "127.0.0.1"
    port: {_free_port()}
  http:
    enable: false

network:
  allow_cidrs:
    - "127.0.0.1/32"

logging:
  level: "info"
  format: "json"
"""


def _run_startup(tmp_path: Path, table: str, primary_key: str) -> str:
    config_path = tmp_path / f"{table}.yaml"
    config_path.write_text(_config(table, primary_key, tmp_path / "dumps"), encoding="utf-8")
    env = os.environ.copy()
    for name in (
        "MYGRAM_MYSQL_HOST",
        "MYGRAM_MYSQL_PORT",
        "MYGRAM_MYSQL_USER",
        "MYGRAM_MYSQL_PASSWORD",
        "MYGRAM_MYSQL_DATABASE",
    ):
        env.pop(name, None)
    result = subprocess.run(
        [str(MYGRAMDB_BINARY), "-c", str(config_path)],
        cwd=PROJECT_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    return output


@pytest.mark.timeout(120)
def test_cascading_foreign_key_child_is_refused_before_snapshot(
    tmp_path: Path, mysql: MysqlClient
) -> None:
    """A child of an ON DELETE CASCADE foreign key fails with 2012 before any table is scanned."""
    native = mysql.execute("SHOW VARIABLES LIKE 'innodb_native_foreign_keys'")
    if native and native[0]["Value"].upper() == "OFF":
        pytest.skip("the server layer writes row events for cascaded child rows")

    mysql.execute("DROP TABLE IF EXISTS trq_child")
    mysql.execute("DROP TABLE IF EXISTS trq_parent")
    mysql.execute("CREATE TABLE trq_parent (id INT PRIMARY KEY) ENGINE=InnoDB")
    mysql.execute(
        "CREATE TABLE trq_child (id INT PRIMARY KEY, parent_id INT, content TEXT,"
        " FOREIGN KEY (parent_id) REFERENCES trq_parent(id) ON DELETE CASCADE) ENGINE=InnoDB"
    )
    try:
        output = _run_startup(tmp_path, "trq_child", "id")
        assert "(2012)" in output, output
        assert "snapshot_building" not in output, output
    finally:
        mysql.execute("DROP TABLE IF EXISTS trq_child")
        mysql.execute("DROP TABLE IF EXISTS trq_parent")


@pytest.mark.timeout(120)
def test_composite_primary_key_is_refused_before_snapshot(
    tmp_path: Path, mysql: MysqlClient
) -> None:
    """A configured key column that is only part of a composite key fails with 2011."""
    mysql.execute("DROP TABLE IF EXISTS trq_composite")
    mysql.execute(
        "CREATE TABLE trq_composite (a INT NOT NULL, b INT NOT NULL, content TEXT,"
        " PRIMARY KEY (a, b)) ENGINE=InnoDB"
    )
    try:
        output = _run_startup(tmp_path, "trq_composite", "a")
        assert "(2011)" in output, output
        assert "snapshot_building" not in output, output
    finally:
        mysql.execute("DROP TABLE IF EXISTS trq_composite")
