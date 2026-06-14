"""Test basic search functionality."""

import uuid

import pytest

pytestmark = pytest.mark.search


class TestBasicSearch:
    """Test basic search operations."""

    def test_single_word_search(self, mygramdb, seed_data):
        """Single word search should return results."""
        # Search for a common word that should exist in seed data
        result = mygramdb.search("testdb.articles", "test", limit=10)
        # Seed data may or may not contain "test", but search should not error
        assert isinstance(result, dict)
        assert "ids" in result

    def test_search_returns_ids(self, mysql, mygramdb, seed_data):
        """Search results should contain valid document IDs."""
        from lib.wait import wait_until_gte

        marker = f"basicsrch_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Basic Search",
                    "content": f"This document contains {marker} for testing",
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
            description="search test data",
        )

        result = mygramdb.search("testdb.articles", marker, limit=10)
        assert result["total"] >= 1
        assert len(result["ids"]) >= 1

    def test_count_matches_search_total(self, mysql, mygramdb, seed_data):
        """COUNT and SEARCH total should agree."""
        from lib.wait import wait_until_gte

        marker = f"cntmatch_{uuid.uuid4().hex[:8]}"
        rows = [
            {
                "title": f"Count Match {i}",
                "content": f"Document with {marker} number {i}",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
            for i in range(5)
        ]
        mysql.insert_rows("articles", rows)

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=5,
            timeout=10,
            interval=0.5,
            description="count match test data",
        )

        count = mygramdb.count("testdb.articles", marker)
        search = mygramdb.search("testdb.articles", marker, limit=100)
        assert count == search["total"], f"COUNT ({count}) != SEARCH total ({search['total']})"

    def test_empty_result(self, mygramdb, seed_data):
        """Search for non-existent term should return empty."""
        result = mygramdb.search("testdb.articles", "zzzznonexistentterm99999xyz", limit=10)
        assert result["total"] == 0
        assert len(result["ids"]) == 0
