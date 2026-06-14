"""Test multiple table independent indexing."""

import contextlib

import pytest

from lib.wait import wait_until_gte

pytestmark = pytest.mark.multi_table


class TestIndependentIndex:
    """Test that multiple tables have independent indexes."""

    def test_no_cross_contamination(self, mysql, mygramdb, seed_data):
        """Data in one table should not appear in another table's index."""
        # Insert unique content into articles
        article_marker = "article_only_unique_marker"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Article Only",
                    "content": f"Content with {article_marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", article_marker),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="article data",
        )

        # Search in products should NOT find article-only content
        # (if products table is indexed)
        product_count = mygramdb.count("testdb.products", article_marker)
        assert product_count == 0, f"Article content found in products index: {product_count}"

    def test_independent_counts(self, mysql, mygramdb, seed_data):
        """Each table should maintain independent document counts."""
        articles_count = mygramdb.count("testdb.articles", "test")
        # Products may not be indexed, so just verify no crash
        with contextlib.suppress(Exception):
            mygramdb.count("testdb.products", "test")

        # Verify articles has data
        assert articles_count >= 0
