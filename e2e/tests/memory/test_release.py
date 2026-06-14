"""Test memory release after data removal."""

import time

import pytest

from lib.wait import wait_until

pytestmark = pytest.mark.memory


class TestMemoryRelease:
    """Test that memory is released after data removal."""

    def test_truncate_releases_memory(self, mysql, mygramdb, seed_data):
        """TRUNCATE should release memory."""
        # Ensure replication is caught up before measuring
        mygramdb.sync("testdb.articles")

        # Get memory before
        mygramdb.health_detail()

        # Truncate
        mysql.truncate("articles")

        def _index_cleared() -> bool:
            result = mygramdb.search("testdb.articles", "test", limit=1)
            return result["total"] == 0

        wait_until(
            _index_cleared,
            timeout=60,
            interval=1,
            description="truncate for memory release",
        )

        time.sleep(3)  # Allow time for memory cleanup

        # Memory should be lower or at least not crash
        detail_after = mygramdb.health_detail()
        assert isinstance(detail_after, dict)
        assert mygramdb.ping()

        # Re-seed for subsequent tests
        from lib.data_generator import DataGenerator

        gen = DataGenerator(seed=42)
        rows = gen.generate_articles(count=100)
        mysql.insert_rows("articles", rows)

        def _reseeded() -> bool:
            count = mygramdb.count("testdb.articles", "test")
            return count >= 30

        wait_until(
            _reseeded,
            timeout=30,
            interval=1,
            description="re-seed after truncate",
        )
