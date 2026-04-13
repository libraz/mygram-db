"""Test replication lag metrics."""

import time

import pytest

pytestmark = pytest.mark.statistics


class TestReplicationLag:
    """Verify replication lag converges to zero after sync."""

    def test_lag_converges(self, mysql, mygramdb, seed_data):
        """After all data is synced, lag should be minimal."""
        # Insert a small batch and wait
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Lag Test",
                    "content": "lag convergence test marker",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        time.sleep(5)  # Wait for sync

        from lib.metrics import MetricsSnapshot

        snapshot = MetricsSnapshot.capture(mygramdb)

        # Look for lag/delay metrics
        lag_metrics = snapshot.get_matching("lag|delay|behind")
        # If lag metrics exist, they should be small
        for key, value in lag_metrics.items():
            # Allow some tolerance but should be near zero
            assert value < 60, f"Replication lag {key}={value} is too high"
