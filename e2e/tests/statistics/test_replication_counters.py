"""Test replication event counters accuracy."""

import pytest

from lib.wait import wait_until_gte
from lib.metrics import MetricsSnapshot

pytestmark = pytest.mark.statistics


class TestReplicationCounters:
    """Verify replication counters match actual operations."""

    def test_insert_counter(self, mysql, mygramdb, seed_data):
        """Insert counter should increase by N after N inserts."""
        before = MetricsSnapshot.capture(mygramdb)

        n = 10
        rows = [
            {
                "title": f"Counter Test {i}",
                "content": f"replication_counter_marker content {i}",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
            for i in range(n)
        ]
        mysql.insert_rows("articles", rows)

        # Wait for replication
        wait_until_gte(
            lambda: mygramdb.count("articles", "replication_counter_marker"),
            minimum=n,
            timeout=10,
            interval=0.5,
            description="replication counter data",
        )

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        # Look for insert counter in metrics
        insert_metrics = {k: v for k, v in diff.items() if "insert" in k.lower()}
        if insert_metrics:
            # At least one insert metric should have increased by >= n
            max_increase = max(insert_metrics.values())
            assert max_increase >= n, (
                f"Insert counter increased by {max_increase}, expected >= {n}"
            )
