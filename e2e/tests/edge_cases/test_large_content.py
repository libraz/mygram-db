"""Test large content edge cases."""

import pytest

from lib.data_generator import DataGenerator
from lib.wait import wait_until_gte

pytestmark = pytest.mark.edge_cases


class TestLargeContent:
    """Test handling of large documents and result sets."""

    def test_large_document(self, mysql, mygramdb, seed_data):
        """1MB document should be indexed without crash."""
        gen = DataGenerator(seed=12345)
        large_content = gen.generate_large_content(50_000)  # 50KB (within TEXT column 65KB limit)
        marker = "large_doc_unique_marker"
        large_content = f"{marker} {large_content}"

        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Large Document",
                    "content": large_content,
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=1,
            timeout=30,
            interval=1,
            description="large document indexing",
        )

    def test_large_result_set(self, mysql, mygramdb, seed_data):
        """Search returning many results should work."""
        # Use seed_data which has 100 rows, search for common term
        result = mygramdb.search("testdb.articles", "the", limit=1000)
        assert isinstance(result, dict)
        assert "ids" in result
