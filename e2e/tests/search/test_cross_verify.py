"""Cross-verify MygramDB results with MySQL FULLTEXT."""

import uuid

import pytest

from lib.wait import wait_until_gte

pytestmark = [
    pytest.mark.search,
    pytest.mark.mysql_only,
]


class TestCrossVerify:
    """Compare MygramDB search results with MySQL FULLTEXT results."""

    @pytest.fixture(autouse=True)
    def setup_cross_verify_data(self, mysql, mygramdb, seed_data):
        """Ensure data is loaded for cross-verification."""
        pass

    def test_cross_verify_basic_queries(self, mysql, mygramdb):
        """Search results should broadly agree with MySQL FULLTEXT."""
        # Insert known data for cross-verification
        marker = f"crossvfy_{uuid.uuid4().hex[:8]}"
        rows = [
            {
                "title": f"Cross {i}",
                "content": f"Content with {marker} and some searchable text number {i}",
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
            description="cross-verify data",
        )

        # Both should find the same count for exact marker
        mygramdb_count = mygramdb.count("testdb.articles", marker)
        mysql_results = mysql.fulltext_search("articles", "content", marker, where="enabled = 1")
        mysql_count = len(mysql_results)

        # Allow some tolerance due to different tokenization
        assert abs(mygramdb_count - mysql_count) <= mysql_count * 0.2 + 1, (
            f"MygramDB ({mygramdb_count}) vs MySQL ({mysql_count}) diverge too much"
        )
