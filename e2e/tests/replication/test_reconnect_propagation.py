"""Test that binlog events are not lost across an observed reconnect."""

import json
import os
import uuid
from pathlib import Path

import pytest

from lib.wait import wait_until, wait_until_gte

pytestmark = pytest.mark.replication


def _stream_open_count() -> int:
    """Count completed binlog stream opens in the structured server log."""
    log_path = Path(os.environ.get("MYGRAMDB_LOG", "/tmp/mygramdb-e2e.log"))
    try:
        # Structured logs may contain a truncated or non-UTF-8 payload emitted
        # by an upstream test value. It must not make reconnect observation
        # itself fail before JSON records can be scanned.
        lines = log_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except (FileNotFoundError, OSError):
        return 0

    count = 0
    for line in lines:
        json_start = line.find("{")
        if json_start < 0:
            continue
        try:
            record = json.loads(line[json_start:])
        except json.JSONDecodeError:
            continue
        if record.get("event") == "binlog_stream_opened":
            count += 1
    return count


def _force_and_wait_for_reconnect(mysql) -> None:
    """Drop the binlog connection and observe the replacement stream opening."""
    connections = mysql.execute(
        "SELECT ID FROM information_schema.PROCESSLIST "
        "WHERE COMMAND IN ('Binlog Dump', 'Binlog Dump GTID')"
    )
    if not connections:
        pytest.fail("MygramDB binlog connection not found in MySQL PROCESSLIST")

    before = _stream_open_count()
    connection_id = int(connections[0]["ID"])
    mysql.execute(f"KILL CONNECTION {connection_id}")
    wait_until(
        lambda: _stream_open_count() > before,
        timeout=15,
        interval=0.25,
        description="binlog stream to reopen after idle read timeout",
    )


class TestReconnectPropagation:
    """Test that events are not lost after the binlog connection is replaced."""

    def test_event_after_observed_reconnect(self, mysql, mygramdb, seed_data):
        """INSERT after a forced, observed reconnect should propagate correctly.

        MySQL heartbeats keep a healthy idle stream alive, so sleeping does not
        imply a reconnect. Drop the actual binlog connection and require a new
        ``binlog_stream_opened`` event before testing GTID resume behavior.
        """
        _force_and_wait_for_reconnect(mysql)

        # Insert a row after the reconnection
        marker = f"reconnect_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Reconnect Test",
                    "content": f"Content with {marker} after idle reconnect",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        # The row should be found within 20 seconds
        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="INSERT propagation after idle reconnect",
        )

    def test_multiple_events_after_observed_reconnect(self, mysql, mygramdb, seed_data):
        """Multiple INSERTs after an observed reconnect should all propagate.

        Verifies that not just one, but multiple consecutive events are
        correctly received after reconnection.
        """
        _force_and_wait_for_reconnect(mysql)

        # Insert multiple rows
        marker = f"reconnmulti_{uuid.uuid4().hex[:8]}"
        rows = [
            {
                "title": f"Reconnect Multi {i}",
                "content": f"Multi content {marker} number {i}",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
            for i in range(5)
        ]
        mysql.insert_rows("articles", rows)

        # All 5 rows should be found
        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=5,
            timeout=20,
            interval=0.5,
            description="multiple INSERTs after idle reconnect",
        )
