"""Test that binlog events are not lost after read_timeout reconnection."""

import time
import uuid

import pytest

from lib.wait import wait_until_gte

pytestmark = pytest.mark.replication


class TestReconnectPropagation:
    """Test that events are not lost after binlog reader read_timeout reconnection."""

    def test_event_after_idle_reconnect(self, mysql, mygramdb, seed_data):
        """INSERT after idle timeout should propagate correctly.

        The binlog reader has a 5-second read_timeout. When no events arrive
        within this period, the connection times out and the reader reconnects.
        This test verifies that events inserted after the reconnection are
        not missed due to incorrect GTID handling during reconnection.
        """
        # Wait for idle timeout to trigger (read_timeout is 5 seconds)
        # Wait 8 seconds to ensure at least one timeout + reconnection cycle
        time.sleep(8)

        # Insert a row after the reconnection
        marker = f"reconnect_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows("articles", [{
            "title": "Reconnect Test",
            "content": f"Content with {marker} after idle reconnect",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])

        # The row should be found within 20 seconds
        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="INSERT propagation after idle reconnect",
        )

    def test_multiple_events_after_idle_reconnect(self, mysql, mygramdb, seed_data):
        """Multiple INSERTs after idle timeout should all propagate.

        Verifies that not just one, but multiple consecutive events are
        correctly received after reconnection.
        """
        # Wait for idle timeout
        time.sleep(8)

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
            lambda: mygramdb.count("articles", marker),
            minimum=5,
            timeout=20,
            interval=0.5,
            description="multiple INSERTs after idle reconnect",
        )
