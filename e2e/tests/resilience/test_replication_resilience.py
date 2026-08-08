"""Test replication resilience under stress conditions."""

import threading
import time
import uuid

import pytest

from lib.wait import wait_until, wait_until_gte

pytestmark = pytest.mark.resilience


class TestReplicationResilience:
    """Verify replication handles concurrent and stop/start scenarios."""

    def _ensure_replication_running(self, mygramdb):
        """Bring replication back to running and confirm the reported state.

        START is rejected while a previous STOP is still winding down, so it is
        retried until accepted; the reported state then has to agree, because
        returning on the acknowledgement alone would let the next assertion run
        against a reader that is not consuming yet.
        """

        def _started() -> bool:
            if mygramdb.replication_is_running():
                return True
            resp = mygramdb.tcp_command("REPLICATION START", timeout=10.0)
            if resp is None or "stopping" in resp.lower():
                return False
            return mygramdb.replication_is_running()

        wait_until(_started, timeout=40, interval=0.5, description="replication to be running")

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

            # Stop only once the stream is demonstrably flowing, so the STOP
            # really does land mid-stream rather than before the first row.
            wait_until_gte(
                lambda: mygramdb.count("testdb.articles", marker),
                minimum=1,
                timeout=20,
                interval=0.1,
                description="the first row of the write burst to be indexed",
            )
            mygramdb.tcp_command("REPLICATION STOP", timeout=10.0)

            # Wait for all inserts to complete
            insert_done.wait(timeout=30)
            writer.join(timeout=5)
            assert not errors, f"Insert worker errors: {errors}"

            # Wait for stop to fully complete, then restart
            wait_until(
                lambda: not mygramdb.replication_is_running(),
                timeout=70,
                interval=0.5,
                description="the reader to finish stopping",
            )
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
        """A manual STOP makes readiness fail with actionable replication diagnostics."""
        try:
            # Initially healthy
            assert mygramdb.health_live(), "Should be live initially"
            assert mygramdb.health_ready(), "Should be ready while replication is running"

            # Stop replication (may take up to 60s due to binlog read_timeout)
            resp = mygramdb.tcp_command("REPLICATION STOP", timeout=10.0)
            assert resp is not None and ("STOPPED" in resp or "stopped" in resp)

            # Server should still be live (liveness != replication)
            assert mygramdb.health_live(), "Should still be live after STOP"

            def _readiness_is_unavailable() -> bool:
                status, _body = mygramdb.http_get_with_status("/health/ready")
                return status == 503

            wait_until(
                _readiness_is_unavailable,
                timeout=15,
                interval=0.25,
                description="readiness to become unavailable after replication STOP",
            )
            ready_status, ready = mygramdb.http_get_with_status("/health/ready")
            assert ready_status == 503
            assert isinstance(ready, dict)
            assert ready["status"] == "not_ready"
            assert ready["replication_running"] is False
            assert ready["reason"] == "Replication is not running"
            assert "replication_last_error_code" in ready
            assert "replication_seconds_since_last_applied" in ready

            # Detail and replication endpoints remain observable and identify
            # the stopped lifecycle state rather than reporting a false-ready.
            detail = mygramdb.health_detail()
            assert isinstance(detail, dict)
            assert detail["status"] == "degraded"
            binlog = detail["components"]["binlog"]
            assert binlog["replication_state"] == "stopped"
            assert "crc_errors" in binlog
            assert "last_error_code" in binlog

            replication_status, replication = mygramdb.http_get_with_status("/replication/status")
            assert replication_status == 200
            assert isinstance(replication, dict)
            assert replication["status"] == "stopped"
            assert "seconds_since_last_applied" in replication

            metrics = mygramdb.metrics()
            assert 'mygramdb_replication_state{state="stopped"} 1' in metrics
            assert "mygramdb_replication_crc_errors_total" in metrics

            # Restart (with retry for stopping race)
            self._ensure_replication_running(mygramdb)

            wait_until(
                mygramdb.health_ready,
                timeout=15,
                interval=0.25,
                description="readiness to recover after replication START",
            )
            assert mygramdb.health_live(), "Should be live after START"
            assert mygramdb.health_ready(), "Should be ready after START"
        finally:
            self._ensure_replication_running(mygramdb)
