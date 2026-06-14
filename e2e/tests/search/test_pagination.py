"""Test search pagination (LIMIT/OFFSET)."""

import pytest

from lib.wait import wait_until_gte

pytestmark = pytest.mark.search


class TestPagination:
    """Test LIMIT and OFFSET functionality."""

    def test_limit(self, mysql, mygramdb, seed_data):
        """LIMIT should restrict result count."""
        marker = "pagination_test_limit_marker"
        rows = [
            {
                "title": f"Page {i}",
                "content": f"Content with {marker} number {i}",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
            for i in range(20)
        ]
        mysql.insert_rows("articles", rows)

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=20,
            timeout=10,
            interval=0.5,
            description="pagination test data",
        )

        result = mygramdb.search("testdb.articles", marker, limit=5)
        assert len(result["ids"]) <= 5

    def test_offset(self, mysql, mygramdb, seed_data):
        """OFFSET should skip results."""
        marker = "pagination_offset_test_marker"
        rows = [
            {
                "title": f"Offset {i}",
                "content": f"Content with {marker} number {i}",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
            for i in range(20)
        ]
        mysql.insert_rows("articles", rows)

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=20,
            timeout=10,
            interval=0.5,
            description="offset test data",
        )

        page1 = mygramdb.search("testdb.articles", marker, limit=5, offset=0, sort="id ASC")
        page2 = mygramdb.search("testdb.articles", marker, limit=5, offset=5, sort="id ASC")

        # Pages should not overlap (if sort is deterministic)
        if page1["ids"] and page2["ids"]:
            assert set(page1["ids"]).isdisjoint(set(page2["ids"])), (
                "Page 1 and page 2 should not overlap"
            )
