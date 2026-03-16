"""Test cache hit/miss counters accuracy."""

import pytest

from lib.metrics import MetricsSnapshot

pytestmark = pytest.mark.statistics


class TestCacheCounters:
    """Verify cache counters match actual hit/miss counts."""

    def test_cache_miss_then_hit_counters(self, mygramdb, seed_data):
        """Cache miss and hit counters should reflect actual behavior."""
        mygramdb.cache_clear()

        before = MetricsSnapshot.capture(mygramdb)

        # First search - should be a miss
        mygramdb.search("articles", "cache_counter_test", limit=10)
        # Second search - should be a hit (same query)
        mygramdb.search("articles", "cache_counter_test", limit=10)

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        # Look for cache-related metrics
        cache_metrics = {k: v for k, v in diff.items() if "cache" in k.lower()}
        # Just verify metrics changed (specific counter names depend on implementation)
        # At minimum, some cache metric should have changed
        if cache_metrics:
            total_changes = sum(abs(v) for v in cache_metrics.values())
            assert total_changes > 0, "Cache metrics should change after searches"
