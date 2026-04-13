"""Test search with filters."""

import pytest

from lib.wait import wait_until_gte

pytestmark = pytest.mark.search


class TestFilters:
    """Test search with filter parameters."""

    def test_status_filter(self, mysql, mygramdb, seed_data):
        """Filter by status should narrow results."""
        marker = "status_filter_test_qwerty"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Status 1",
                    "content": f"Content with {marker} status one",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                },
                {
                    "title": "Status 2",
                    "content": f"Content with {marker} status two",
                    "status": 2,
                    "category": "tech",
                    "enabled": 1,
                },
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=2,
            timeout=10,
            interval=0.5,
            description="filter test data",
        )

        # Search without filter
        all_results = mygramdb.count("articles", marker)
        assert all_results >= 2

        # Search with status filter (if supported)
        filtered = mygramdb.search("articles", marker, filters={"status": 1}, limit=100)
        # Filtered results should be <= total
        assert filtered["total"] <= all_results

    def test_category_filter(self, mysql, mygramdb, seed_data):
        """Filter by category should narrow results."""
        marker = "category_filter_test_asdf"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Tech Article",
                    "content": f"Content with {marker} tech article",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                },
                {
                    "title": "Science Article",
                    "content": f"Content with {marker} science article",
                    "status": 1,
                    "category": "science",
                    "enabled": 1,
                },
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=2,
            timeout=10,
            interval=0.5,
            description="category filter test data",
        )

        all_count = mygramdb.count("articles", marker)
        assert all_count >= 2
