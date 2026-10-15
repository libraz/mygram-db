"""Initial load and replication must admit the same rows for a required_filters
temporal comparison, including zero and partly-zero dates and fractional TIME values.

The initial load judges a row with bare-column bounds in SQL, where the server orders a
zero or partly zero date by its components. Replication decodes the same row from the
binlog; for a date that has no instant it has to reach the verdict the SQL reached.
"""

from __future__ import annotations

import os
import socket
import subprocess
from pathlib import Path

import pytest

from lib.mygramdb_client import MygramdbClient
from lib.mysql_client import MysqlClient
from lib.wait import wait_until

pytestmark = [pytest.mark.mysql, pytest.mark.dedicated_process]

E2E_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = E2E_ROOT.parent
MYGRAMDB_BINARY = Path(os.environ.get("MYGRAMDB_BINARY", PROJECT_ROOT / "build/bin/mygramdb"))
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "23306"))

TABLE = "_e2e_zero_date"
THRESHOLD = "2024-01-01 00:00:00"

# Zero, partly zero, before, after and equal to the threshold.
ROWS = [
    (1, "0000-00-00 00:00:00"),
    (2, "2024-00-00 00:00:00"),
    (3, "2023-05-05 10:00:00"),
    (4, "2025-01-01 00:00:00"),
    (5, THRESHOLD),
]

# Fractional TIME(3) values at x.500 and -x.500 around whole-second 10:00:00.
TIME_THRESHOLD = "10:00:00"
TIME_ROWS = [
    (1, "10:00:00.500"),
    (2, "-10:00:00.500"),
    (3, "10:00:00"),
    (4, "10:00:01"),
    (5, "09:59:59.999"),
]
REPLICATED_OFFSET = 100


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _config(
    tcp_port: int, http_port: int, filter_type: str, op: str, value: str | None, dump_dir: Path
) -> str:
    value_field = "" if value is None else f'        value: "{value}"'
    return f"""mysql:
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
      column: "content"
    required_filters:
      - name: "happened_at"
        type: "{filter_type}"
        op: "{op}"
{value_field}
    ngram_size: 2
    kanji_ngram_size: 1

build:
  batch_size: 5000
  parallelism: 1

replication:
  enable: true
  server_id: 199996
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
    - "0.0.0.0/0"

logging:
  level: "debug"
  format: "json"
  file: ""
"""


def _insert_permissive(mysql: MysqlClient, rows: list[tuple[int, str]]) -> None:
    """Insert with a sql_mode that stores zero and partly-zero dates."""
    conn = mysql._connect()
    try:
        cursor = conn.cursor()
        cursor.execute("SET SESSION sql_mode = ''")
        for row_id, value in rows:
            cursor.execute(
                f"INSERT INTO {TABLE} (id, content, happened_at) VALUES (%s, 'zdrow', %s)",
                (row_id, value),
            )
        cursor.close()
    finally:
        conn.close()


def _assert_load_and_replication_agree(
    tmp_path: Path,
    mysql: MysqlClient,
    *,
    column_type: str,
    filter_type: str,
    op: str,
    value: str | None,
    rows: list[tuple[int, str]],
    expected_load_ids: set[int] | None = None,
) -> None:
    mysql.execute(f"DROP TABLE IF EXISTS {TABLE}")
    mysql.execute(
        f"CREATE TABLE {TABLE} (id INT PRIMARY KEY, content VARCHAR(50), happened_at "
        f"{column_type} NULL)"
    )
    process = None
    try:
        _insert_permissive(mysql, rows)

        tcp_port = _free_port()
        http_port = _free_port()
        config_path = tmp_path / "zero-date.yaml"
        config_path.write_text(
            _config(tcp_port, http_port, filter_type, op, value, tmp_path), encoding="utf-8"
        )
        env = os.environ.copy()
        for name in (
            "MYGRAM_MYSQL_HOST",
            "MYGRAM_MYSQL_PORT",
            "MYGRAM_MYSQL_USER",
            "MYGRAM_MYSQL_PASSWORD",
            "MYGRAM_MYSQL_DATABASE",
        ):
            env.pop(name, None)

        with (tmp_path / "zero-date.log").open("w", encoding="utf-8") as log_file:
            process = subprocess.Popen(
                [str(MYGRAMDB_BINARY), "-c", str(config_path)],
                stdout=log_file,
                stderr=subprocess.STDOUT,
                cwd=PROJECT_ROOT,
                env=env,
            )
            client = MygramdbClient("127.0.0.1", tcp_port=tcp_port, http_port=http_port)
            wait_until(client.ping, timeout=90, interval=0.5, description="zero-date server")
            assert client.sync(f"testdb.{TABLE}", timeout=90), "initial SYNC did not complete"
            wait_until(client.health_ready, timeout=90, interval=0.5, description="initial load")

            def admitted() -> set[int]:
                return set(client.search(TABLE, "zdrow", limit=100)["ids"])

            loaded = admitted()
            assert loaded, f"initial load admitted nothing for '{op}'"
            if expected_load_ids is not None:
                assert loaded == expected_load_ids, (
                    f"initial load admitted {sorted(loaded)}, "
                    f"expected {sorted(expected_load_ids)} for '{op}'"
                )

            _insert_permissive(
                mysql, [(row_id + REPLICATED_OFFSET, row_value) for row_id, row_value in rows]
            )
            expected = loaded | {row_id + REPLICATED_OFFSET for row_id in loaded}

            wait_until(
                lambda: admitted() == expected,
                timeout=30,
                interval=0.5,
                description=f"replication to admit the rows the load admitted for '{op}'",
            )
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        mysql.execute(f"DROP TABLE IF EXISTS {TABLE}")


@pytest.mark.timeout(240)
@pytest.mark.parametrize("op", ["<", "=", "!="])
def test_zero_and_partial_dates_are_admitted_alike_by_load_and_replication(
    tmp_path: Path, mysql: MysqlClient, op: str
) -> None:
    _assert_load_and_replication_agree(
        tmp_path,
        mysql,
        column_type="DATETIME",
        filter_type="datetime",
        op=op,
        value=THRESHOLD,
        rows=ROWS,
    )


@pytest.mark.timeout(240)
def test_partial_date_between_whole_seconds_is_admitted_alike_by_load_and_replication(
    tmp_path: Path, mysql: MysqlClient
) -> None:
    # '2024-00-00' sorts between the half-open bounds the SQL puts at x and x + 1.
    _assert_load_and_replication_agree(
        tmp_path,
        mysql,
        column_type="DATETIME",
        filter_type="datetime",
        op="<=",
        value="2023-12-31 23:59:59",
        rows=ROWS,
        expected_load_ids={1, 2, 3},
    )


@pytest.mark.timeout(240)
def test_zero_and_partial_dates_are_admitted_by_is_not_null_load_and_replication(
    tmp_path: Path, mysql: MysqlClient
) -> None:
    _assert_load_and_replication_agree(
        tmp_path,
        mysql,
        column_type="DATETIME",
        filter_type="datetime",
        op="IS NOT NULL",
        value=None,
        rows=ROWS,
        expected_load_ids={row_id for row_id, _ in ROWS},
    )


@pytest.mark.timeout(240)
@pytest.mark.parametrize("op", [">", "=", "!="])
def test_fractional_time_is_admitted_alike_by_load_and_replication(
    tmp_path: Path, mysql: MysqlClient, op: str
) -> None:
    _assert_load_and_replication_agree(
        tmp_path,
        mysql,
        column_type="TIME(3)",
        filter_type="time",
        op=op,
        value=TIME_THRESHOLD,
        rows=TIME_ROWS,
    )
