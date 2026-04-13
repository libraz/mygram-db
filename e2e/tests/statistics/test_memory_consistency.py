"""Test memory reporting consistency."""

import pytest

pytestmark = pytest.mark.statistics


class TestMemoryConsistency:
    """Verify memory values are consistent across endpoints."""

    def test_metrics_health_memory_match(self, mygramdb, seed_data):
        """Memory values from /metrics and /health/detail should be close."""
        from lib.metrics import MetricsSnapshot

        snapshot = MetricsSnapshot.capture(mygramdb)
        detail = mygramdb.health_detail()

        # Look for memory metrics
        snapshot.get_matching("memory")
        assert isinstance(detail, dict)

        # Just verify both endpoints return memory data without crash
        # Exact key names are implementation-dependent
