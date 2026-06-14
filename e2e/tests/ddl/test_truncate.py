"""Test TRUNCATE TABLE handling."""

import pytest

from lib.data_generator import DataGenerator
from lib.wait import wait_until, wait_until_gte

pytestmark = pytest.mark.ddl


class TestTruncate:
    """Test TRUNCATE TABLE event handling."""

    def test_truncate_clears_index(self, mysql, mygramdb, seed_data):
        """TRUNCATE TABLE should clear the MygramDB index."""
        # Verify we have data
        marker = "truncate_test_marker"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Truncate Test",
                    "content": f"Content with {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="truncate test data",
        )

        # Truncate
        mysql.truncate("articles")

        # Wait for articles search to return no results
        def _articles_cleared() -> bool:
            mygramdb.count("testdb.articles", "truncate_test_marker")
            # Also verify a broad search returns nothing for articles
            result = mygramdb.search("testdb.articles", "test", limit=1)
            return result["total"] == 0

        wait_until(
            _articles_cleared,
            timeout=15,
            interval=1,
            description="TRUNCATE to clear articles index",
        )

    def test_truncate_then_reinsert(self, mysql, mygramdb):
        """After TRUNCATE, new data should be indexed correctly."""
        # Insert new data after truncate
        gen = DataGenerator(seed=777)
        rows = gen.generate_articles(count=50)
        mysql.insert_rows("articles", rows)

        def _get_doc_count() -> int:
            return mygramdb.count("testdb.articles", "test")

        wait_until_gte(
            _get_doc_count,
            minimum=30,
            timeout=30,
            interval=1,
            description="re-insert after TRUNCATE",
        )
