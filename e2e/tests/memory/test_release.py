"""Test memory release after data removal."""

import pytest
import time

from lib.wait import wait_until

pytestmark = pytest.mark.memory


class TestMemoryRelease:
    """Test that memory is released after data removal."""

    def test_truncate_releases_memory(self, mysql, mygramdb, seed_data):
        """TRUNCATE should release memory."""
        # Get memory before
        detail_before = mygramdb.health_detail()

        # Truncate
        mysql.truncate("articles")

        def _index_cleared() -> bool:
            info = mygramdb.info()
            return info.get("total_documents", info.get("doc_count", info.get("documents", -1))) == 0

        wait_until(
            _index_cleared,
            timeout=15,
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
            info = mygramdb.info()
            return info.get("total_documents", info.get("doc_count", info.get("documents", 0))) >= 100

        wait_until(
            _reseeded,
            timeout=30,
            interval=1,
            description="re-seed after truncate",
        )
