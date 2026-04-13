"""Test command execution counters."""

import pytest

from lib.metrics import MetricsSnapshot

pytestmark = pytest.mark.statistics


class TestCommandCounters:
    """Verify command counters match actual executions."""

    def test_search_counter(self, mygramdb, seed_data):
        """Search counter should increase after searches."""
        before = MetricsSnapshot.capture(mygramdb)

        m = 5
        for _ in range(m):
            mygramdb.search("articles", "test", limit=10)

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        # Look for search/command counter
        search_metrics = {
            k: v for k, v in diff.items() if "search" in k.lower() or "command" in k.lower()
        }
        if search_metrics:
            max_increase = max(search_metrics.values())
            assert max_increase >= m, f"Search counter increased by {max_increase}, expected >= {m}"

    def test_count_counter(self, mygramdb, seed_data):
        """Count counter should increase after count commands."""
        before = MetricsSnapshot.capture(mygramdb)

        m = 3
        for _ in range(m):
            mygramdb.count("articles", "test")

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        count_metrics = {
            k: v for k, v in diff.items() if "count" in k.lower() or "command" in k.lower()
        }
        if count_metrics:
            max_increase = max(count_metrics.values())
            assert max_increase >= m
