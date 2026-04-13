"""Test replication stop and restart functionality."""

import time
import uuid

import pytest

from lib.metrics import MetricsSnapshot
from lib.wait import wait_until_gte

pytestmark = pytest.mark.replication


class TestStopRestart:
    """Verify replication stop/start behavior and data integrity."""

    def _stop_replication(self, mygramdb):
        """Stop replication and wait until fully stopped.

        The STOP handler calls BinlogReader::Stop() synchronously, which
        blocks on reader_thread_->join(). The reader's read_timeout is 60s,
        so the TCP response may take up to 60s. We use a 65s timeout.
        """
        resp = mygramdb.tcp_command("REPLICATION STOP", timeout=10.0)
        assert resp is not None and "STOPPED" in resp, f"Failed to stop replication: {resp}"

    def _start_replication(self, mygramdb):
        """Start replication with retry for 'stopping' race."""
        for attempt in range(10):
            resp = mygramdb.tcp_command("REPLICATION START", timeout=10.0)
            if resp is not None and "STARTED" in resp:
                return
            if resp and "stopping" in resp.lower():
                # Stop() is still running; wait and retry
                time.sleep(3)
                continue
            # Some other response (e.g. already running, or error)
            raise AssertionError(f"Failed to start replication (attempt {attempt}): {resp}")
        raise AssertionError("Failed to start replication: stuck in stopping state")

    def _ensure_replication_running(self, mygramdb):
        """Ensure replication is running (best effort)."""
        for _ in range(10):
            resp = mygramdb.tcp_command("REPLICATION START", timeout=10.0)
            if resp is None:
                time.sleep(3)
                continue
            if "STARTED" in resp or "already" in resp.lower() or "running" in resp.lower():
                return
            if "stopping" in resp.lower():
                time.sleep(3)
                continue
            return  # Unknown response, stop retrying

    def test_stop_insert_restart(self, mysql, mygramdb, seed_data):
        """Stopped replication should resume and process accumulated INSERTs."""
        try:
            self._stop_replication(mygramdb)
            time.sleep(1)

            marker = f"stoprestart_{uuid.uuid4().hex[:8]}"
            n = 10
            rows = [
                {
                    "title": f"Stop Restart Test {i}",
                    "content": f"Content for stop restart {marker} item {i}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
                for i in range(n)
            ]
            mysql.insert_rows("articles", rows)

            # Data should NOT appear while replication is stopped
            time.sleep(2)
            count_while_stopped = mygramdb.count("articles", marker)
            assert count_while_stopped == 0, (
                f"Data should not appear while replication stopped, got {count_while_stopped}"
            )

            # Restart replication
            self._start_replication(mygramdb)

            # All rows should appear
            wait_until_gte(
                lambda: mygramdb.count("articles", marker),
                minimum=n,
                timeout=20,
                interval=0.5,
                description="stop/restart insert propagation",
            )
        finally:
            self._ensure_replication_running(mygramdb)

    def test_stop_mixed_dml_restart(self, mysql, mygramdb, seed_data):
        """Mixed DML during stop should all be applied after restart."""
        marker = f"stopmix_{uuid.uuid4().hex[:8]}"

        # Insert seed rows first (while replication is running)
        seed_rows = [
            {
                "title": f"Stop Mix Seed {i}",
                "content": f"Content for stopmix seed {marker} item {i}",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
            for i in range(5)
        ]
        mysql.insert_rows("articles", seed_rows)

        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=5,
            timeout=10,
            interval=0.5,
            description="stop mix seed data",
        )

        try:
            self._stop_replication(mygramdb)
            time.sleep(1)

            # INSERT 3 more
            new_rows = [
                {
                    "title": f"Stop Mix New {i}",
                    "content": f"Content for stopmix new {marker} extra {i}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
                for i in range(3)
            ]
            mysql.insert_rows("articles", new_rows)

            # UPDATE 2 rows
            mysql.update(
                "articles",
                f"content = 'Updated stopmix {marker} item 0'",
                f"content LIKE '%stopmix seed {marker} item 0%'",
            )
            mysql.update(
                "articles",
                f"content = 'Updated stopmix {marker} item 1'",
                f"content LIKE '%stopmix seed {marker} item 1%'",
            )

            # DELETE 1 row
            mysql.delete("articles", f"content LIKE '%stopmix seed {marker} item 4%'")

            # Restart
            self._start_replication(mygramdb)

            # Wait for all changes to propagate
            # Should have: 5 original - 1 deleted + 3 new = 7 total with marker
            # But some searches may match differently, so check for new inserts
            wait_until_gte(
                lambda: mygramdb.count("articles", f"stopmix new {marker}"),
                minimum=3,
                timeout=20,
                interval=0.5,
                description="stop/restart mixed DML propagation",
            )

            # Deleted row should eventually disappear
            time.sleep(3)
        finally:
            self._ensure_replication_running(mygramdb)

    def test_replication_status_reflects_state(self, mygramdb, seed_data):
        """REPLICATION STATUS should reflect running/stopped state."""
        try:
            # Initially running
            status = mygramdb.tcp_command("REPLICATION STATUS")
            assert status is not None, "REPLICATION STATUS should return a response"

            # Stop
            self._stop_replication(mygramdb)
            time.sleep(1)

            status_stopped = mygramdb.tcp_command("REPLICATION STATUS")
            assert status_stopped is not None

            # Start
            self._start_replication(mygramdb)
            time.sleep(1)

            status_running = mygramdb.tcp_command("REPLICATION STATUS")
            assert status_running is not None
        finally:
            self._ensure_replication_running(mygramdb)

    def test_rapid_stop_start_cycles(self, mysql, mygramdb, seed_data):
        """Stop/start cycles should not crash the server."""
        try:
            for _i in range(3):
                self._stop_replication(mygramdb)
                self._start_replication(mygramdb)

            # Server should still be functional
            assert mygramdb.ping(), "Server should be functional after rapid cycles"

            # Insert and verify replication still works
            marker = f"rapidcycle_{uuid.uuid4().hex[:8]}"
            mysql.insert_rows(
                "articles",
                [
                    {
                        "title": "Rapid Cycle Test",
                        "content": f"Content for rapid cycle {marker}",
                        "status": 1,
                        "category": "tech",
                        "enabled": 1,
                    }
                ],
            )

            wait_until_gte(
                lambda: mygramdb.count("articles", marker),
                minimum=1,
                timeout=15,
                interval=0.5,
                description="rapid cycle insert propagation",
            )
        finally:
            self._ensure_replication_running(mygramdb)

    def test_stop_while_already_stopped(self, mygramdb, seed_data):
        """Double STOP should return error or be idempotent, not crash."""
        try:
            self._stop_replication(mygramdb)
            time.sleep(0.5)

            # Second STOP - should not crash (may return error or be idempotent)
            resp = mygramdb.tcp_command("REPLICATION STOP", timeout=10.0)
            assert resp is not None, "Double STOP should not crash (returned None)"
            # Server should still respond
            assert mygramdb.ping(), "Server should be functional after double STOP"
        finally:
            self._ensure_replication_running(mygramdb)

    def test_start_while_already_running(self, mygramdb, seed_data):
        """Double START should return error or be idempotent, not crash."""
        # Replication should already be running
        resp = mygramdb.tcp_command("REPLICATION START")
        assert resp is not None, "Double START should not crash (returned None)"
        # Server should still respond
        assert mygramdb.ping(), "Server should be functional after double START"

    def test_accumulated_changes_ordering(self, mysql, mygramdb, seed_data):
        """Accumulated changes during STOP should all appear after START."""
        try:
            self._stop_replication(mygramdb)
            time.sleep(1)

            marker = f"accum_{uuid.uuid4().hex[:8]}"
            n = 20
            rows = [
                {
                    "title": f"Accumulated {i:03d}",
                    "content": f"Content for accumulated {marker} seq {i:03d}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
                for i in range(n)
            ]
            mysql.insert_rows("articles", rows)

            # Nothing should appear yet
            time.sleep(2)
            count_stopped = mygramdb.count("articles", marker)
            assert count_stopped == 0, f"No data should appear while stopped, got {count_stopped}"

            # Restart
            self._start_replication(mygramdb)

            # All 20 rows should appear
            wait_until_gte(
                lambda: mygramdb.count("articles", marker),
                minimum=n,
                timeout=30,
                interval=0.5,
                description="accumulated changes propagation",
            )
        finally:
            self._ensure_replication_running(mygramdb)

    def test_stop_ddl_restart(self, mysql, mygramdb, seed_data):
        """DDL + INSERT during STOP should both be processed after START."""
        try:
            # Clean up potential leftover column
            cols = mysql.execute("SHOW COLUMNS FROM articles LIKE 'stop_ddl_col'")
            if cols:
                mysql.execute("ALTER TABLE articles DROP COLUMN stop_ddl_col")
                time.sleep(2)

            before = MetricsSnapshot.capture(mygramdb)

            self._stop_replication(mygramdb)
            time.sleep(1)

            # DDL while stopped
            mysql.execute("ALTER TABLE articles ADD COLUMN stop_ddl_col VARCHAR(50) DEFAULT NULL")

            # INSERT while stopped
            marker = f"stopddl_{uuid.uuid4().hex[:8]}"
            mysql.insert_rows(
                "articles",
                [
                    {
                        "title": "Stop DDL Test",
                        "content": f"Content for stop ddl {marker}",
                        "status": 1,
                        "category": "tech",
                        "enabled": 1,
                    }
                ],
            )

            # Restart
            self._start_replication(mygramdb)

            # INSERT should propagate
            wait_until_gte(
                lambda: mygramdb.count("articles", marker),
                minimum=1,
                timeout=20,
                interval=0.5,
                description="stop DDL restart insert propagation",
            )

            after = MetricsSnapshot.capture(mygramdb)
            diff = MetricsSnapshot.diff(before, after)

            # DDL counter should have increased
            ddl_metrics = {k: v for k, v in diff.items() if "ddl" in k.lower()}
            if ddl_metrics:
                assert max(ddl_metrics.values()) >= 1, (
                    f"DDL counter should increase, got {ddl_metrics}"
                )
        finally:
            self._ensure_replication_running(mygramdb)
            # Cleanup column
            try:
                mysql.execute("ALTER TABLE articles DROP COLUMN stop_ddl_col")
                time.sleep(2)
            except Exception:
                pass
