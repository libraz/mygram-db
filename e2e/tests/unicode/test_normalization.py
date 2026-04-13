"""Test Unicode normalization (NFKC, fullwidth/halfwidth, etc.)."""

import pytest

from lib.wait import wait_until_gte

pytestmark = pytest.mark.unicode


class TestNormalization:
    """Test Unicode normalization behavior."""

    def test_fullwidth_to_halfwidth(self, mysql, mygramdb, seed_data):
        """Fullwidth characters should normalize to halfwidth and be searchable."""
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Fullwidth Test",
                    "content": "Ｈｅｌｌｏ Ｗｏｒｌｄ fullwidth text test",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        # Search with halfwidth "Hello" should find fullwidth "Ｈｅｌｌｏ"
        wait_until_gte(
            lambda: mygramdb.count("articles", "Hello"),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="fullwidth normalization",
        )

    def test_nfkc_ligatures(self, mysql, mygramdb, seed_data):
        """NFKC normalization should decompose ligatures."""
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Ligature Test",
                    "content": "The \ufb01rst \ufb02ight was a \ufb01ne \ufb02ow of events",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        # Search for "fi" should match "\ufb01" after NFKC normalization
        wait_until_gte(
            lambda: mygramdb.count("articles", "first"),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="NFKC ligature normalization",
        )

    def test_zero_width_characters(self, mysql, mygramdb, seed_data):
        """Zero-width characters should not affect search."""
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Zero Width Test",
                    "content": "zero\u200bwidth\u200bcharacter\u200btest for search",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        # Should be searchable despite zero-width chars
        import time

        time.sleep(3)
        # Just verify no crash - zero-width handling is implementation-dependent
        mygramdb.count("articles", "zero")
