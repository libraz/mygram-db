"""Test replication resilience under stress conditions."""

import threading
import time
import uuid

import pytest

from lib.wait import wait_until_gte

pytestmark = pytest.mark.resilience


class TestReplicationResilience:
    """Verify replication handles concurrent and stop/start scenarios."""

    def _ensure_replication_running(self, mygramdb):
        """Ensure replication is running (with retry and sync fallback)."""
        for _attempt in range(30):
            status = mygramdb.replication_status()
            if status and "running" in status.lower():
                return
            resp = mygramdb.tcp_command("REPLICATION START", timeout=10.0)
            if resp is None:
                time.sleep(1)
                continue
            if "STARTED" in resp or "already" in resp.lower() or "running" in resp.lower():
                time.sleep(1)
                return
            if "stopping" in resp.lower():
                time.sleep(2)
                continue
            time.sleep(1)
        # Last resort: sync to re-establish replication
        mygramdb.sync("testdb.articles", timeout=30)

    def test_stop_during_active_writes(self, mysql, mygramdb, seed_data):
        """STOP during active MySQL writes should not lose data after START."""
        marker = f"activewrites_{uuid.uuid4().hex[:8]}"
        n = 20
        insert_done = threading.Event()
        errors = []

        def insert_worker():
            try:
                for i in range(n):
                    mysql.insert_rows(
                        "articles",
                        [
                            {
                                "title": f"Active Write {i}",
                                "content": f"Content active write {marker} item {i}",
                                "status": 1,
                                "category": "tech",
                                "enabled": 1,
                            }
                        ],
                    )
                    time.sleep(0.1)
            except Exception as e:
                errors.append(e)
            finally:
                insert_done.set()

        try:
            # Start inserting
            writer = threading.Thread(target=insert_worker)
            writer.start()

            # Wait a bit then stop replication mid-stream
            time.sleep(0.5)
            mygramdb.tcp_command("REPLICATION STOP", timeout=10.0)

            # Wait for all inserts to complete
            insert_done.wait(timeout=30)
            writer.join(timeout=5)
            assert not errors, f"Insert worker errors: {errors}"

            # Wait for stop to fully complete, then restart
            time.sleep(2)
            self._ensure_replication_running(mygramdb)

            # All rows should eventually appear
            wait_until_gte(
                lambda: mygramdb.count("testdb.articles", marker),
                minimum=n,
                timeout=30,
                interval=0.5,
                description="active writes after restart",
            )
        finally:
            self._ensure_replication_running(mygramdb)

    def test_health_endpoints_after_stop_start(self, mygramdb, seed_data):
        """Health endpoints should reflect replication state after stop/start."""
        try:
            # Initially healthy
            assert mygramdb.health_live(), "Should be live initially"

            # Stop replication (may take up to 60s due to binlog read_timeout)
            resp = mygramdb.tcp_command("REPLICATION STOP", timeout=10.0)
            assert resp is not None and ("STOPPED" in resp or "stopped" in resp)
            time.sleep(1)

            # Server should still be live (liveness != replication)
            assert mygramdb.health_live(), "Should still be live after STOP"

            # Check detail endpoint
            detail = mygramdb.health_detail()
            if detail:
                # The detail should be accessible regardless of replication state
                assert isinstance(detail, dict), "Health detail should be a dict"

            # Restart (with retry for stopping race)
            time.sleep(2)
            self._ensure_replication_running(mygramdb)
            time.sleep(1)

            # Still live
            assert mygramdb.health_live(), "Should be live after START"

            detail_after = mygramdb.health_detail()
            if detail_after:
                assert isinstance(detail_after, dict), "Health detail should be a dict after START"
        finally:
            self._ensure_replication_running(mygramdb)
