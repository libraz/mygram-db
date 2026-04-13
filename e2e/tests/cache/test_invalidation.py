"""Test cache invalidation on data changes."""

import pytest

from lib.wait import wait_until_gte

pytestmark = pytest.mark.cache


class TestCacheInvalidation:
    """Test that cache is invalidated when data changes."""

    def test_insert_invalidates_cache(self, mysql, mygramdb, seed_data):
        """INSERT should invalidate relevant cache entries."""
        marker = "cache_inval_marker"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Cache Inval",
                    "content": f"Content with {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="cache invalidation data",
        )

        # Populate cache
        mygramdb.cache_clear()
        result1 = mygramdb.search("articles", marker, limit=100)

        # Insert another row
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Cache Inval 2",
                    "content": f"Another content with {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=result1["total"] + 1,
            timeout=10,
            interval=0.5,
            description="cache invalidation after insert",
        )

    def test_cache_clear_command(self, mygramdb, seed_data):
        """CACHE CLEAR should clear all cached entries."""
        # Do a search to populate cache
        mygramdb.search("articles", "test", limit=10)

        # Clear cache
        result = mygramdb.cache_clear()
        assert result, "CACHE CLEAR should succeed"

        # System should still work after cache clear
        assert mygramdb.ping()
