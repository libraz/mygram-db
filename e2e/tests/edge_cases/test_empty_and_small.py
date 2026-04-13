"""Test empty and small document edge cases."""

import pytest

from lib.wait import wait_until_gte

pytestmark = pytest.mark.edge_cases


class TestEmptyAndSmall:
    """Test edge cases with empty and very small documents."""

    def test_empty_content(self, mysql, mygramdb, seed_data):
        """Empty content should not crash."""
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Empty Content",
                    "content": "",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        import time

        time.sleep(2)
        # Should not crash
        assert mygramdb.ping()

    def test_single_character(self, mysql, mygramdb, seed_data):
        """Single character content should be handled."""
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Single Char",
                    "content": "a",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        import time

        time.sleep(2)
        assert mygramdb.ping()

    def test_minimum_bigram(self, mysql, mygramdb, seed_data):
        """Two-character content (minimum bigram) should be indexable."""
        marker = "zq"  # unlikely to exist elsewhere
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Min Bigram",
                    "content": f"{marker}",
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
            description="minimum bigram search",
        )
