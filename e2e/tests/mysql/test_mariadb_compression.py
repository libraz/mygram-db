"""Fail-closed coverage for unsupported MariaDB compressed binlog events."""

from __future__ import annotations

import os
import socket
import subprocess
import uuid
from pathlib import Path
from typing import Any

import pytest

from lib.mysql_client import MysqlClient
from lib.wait import wait_until

pytestmark = [pytest.mark.mysql, pytest.mark.mariadb_only]

PROJECT_ROOT = Path(__file__).parents[3]


def _replication_status(mygramdb: Any) -> dict[str, str]:
    with socket.create_connection((mygramdb.host, mygramdb.tcp_port), timeout=5) as sock:
        sock.settimeout(5)
        sock.sendall(b"REPLICATION STATUS\r\n")
        data = b""
        while b"END\r\n" not in data:
            chunk = sock.recv(65536)
            if not chunk:
                break
            data += chunk

    values: dict[str, str] = {}
    for line in data.decode("utf-8", errors="replace").splitlines():
        line = line.removeprefix("OK ")
        if ":" in line:
            key, value = line.split(":", 1)
            values[key.strip()] = value.strip()
    return values


def test_log_bin_compress_is_rejected_before_startup(mysql: MysqlClient, mygramdb: Any) -> None:
    """Startup and runtime must fail closed without advancing the compressed transaction."""
    config_path = Path(os.environ["MYGRAMDB_CONFIG"])
    binary = PROJECT_ROOT / "build" / "bin" / "mygramdb"
    marker = f"mariadb_compressed_{uuid.uuid4().hex[:8]}"

    try:
        mysql.execute("SET GLOBAL log_bin_compress = ON")
        value = mysql.execute("SHOW GLOBAL VARIABLES LIKE 'log_bin_compress'")
        assert value and str(value[0]["Value"]).upper() == "ON"

        result = subprocess.run(
            [str(binary), "-c", str(config_path)],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        output = result.stdout + result.stderr
        assert result.returncode != 0, output
        assert "log_bin_compress=ON is not supported" in output, output

        before = _replication_status(mygramdb)
        assert before.get("status") == "running", before
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "MariaDB compressed runtime event",
                    # Stay within MySQL/MariaDB TEXT's 65,535-byte limit while
                    # remaining far above log_bin_compress_min_len.
                    "content": marker + (" compressible payload" * 2048),
                    "status": 1,
                    "category": "compression",
                    "enabled": 1,
                }
            ],
        )
        # A fatal event is a distinct terminal state from an operator stop: the
        # status carries "failed" and the reason stays queryable, so asserting
        # on "stopped" here would pass only while the two were conflated.
        wait_until(
            lambda: _replication_status(mygramdb).get("status") == "failed",
            timeout=20,
            interval=0.2,
            description="reader to fail closed on a MariaDB compressed row event",
        )
        after = _replication_status(mygramdb)
        assert after.get("current_gtid") == before.get("current_gtid"), (before, after)
        assert "compressed binlog event" in after.get("last_error", ""), after
    finally:
        mysql.execute("SET GLOBAL log_bin_compress = OFF")

    # Turning the setting off does not remove the event already written, so the
    # reader is still stalled just before it and the remediation has to name a
    # recovery that gets past it rather than one that replays it.
    stalled = _replication_status(mygramdb)
    assert stalled.get("status") == "failed", stalled
    assert "run SYNC for every replicated table" in stalled.get("last_error", ""), stalled

    # Recovery has to restart the stream after the snapshot marker. Restarting
    # from the drained position replays the undecodable event, which stops
    # replication again at the same place no matter how often SYNC is run.
    assert mygramdb.sync("testdb.articles", timeout=60)
    wait_until(
        lambda: _replication_status(mygramdb).get("status") == "running",
        timeout=30,
        interval=0.2,
        description="replication to resume past the undecodable event",
    )
    wait_until(
        lambda: mygramdb.count("testdb.articles", marker) == 1,
        timeout=20,
        interval=0.5,
        description="compressed-event row to become visible after explicit recovery SYNC",
    )
    mysql.execute("DELETE FROM articles WHERE content LIKE %s", (f"{marker}%",))
