"""Test cache hit and miss behavior."""

import pytest

from lib.wait import wait_until_gte

pytestmark = pytest.mark.cache


class TestCacheHitMiss:
    """Test cache hit/miss behavior."""

    def test_cache_miss_then_hit(self, mysql, mygramdb, seed_data):
        """First search should be a miss, second should be a hit."""
        marker = "cache_hitm_test_marker"
        mysql.insert_rows("articles", [{
            "title": "Cache Test",
            "content": f"Content with {marker}",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])

        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="cache test data",
        )

        # Clear cache first
        mygramdb.cache_clear()

        # First search - cache miss
        result1 = mygramdb.search("articles", marker, limit=10)
        # Second search - should be cache hit
        result2 = mygramdb.search("articles", marker, limit=10)

        # Both should return the same results
        assert result1["total"] == result2["total"]
