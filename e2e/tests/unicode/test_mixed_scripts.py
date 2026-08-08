"""Test mixed script and emoji handling."""

import uuid

import pytest

from lib.wait import wait_until_gte, wait_until_value

pytestmark = pytest.mark.unicode


class TestMixedScripts:
    """Test mixed script and special character handling."""

    def test_mixed_english_japanese(self, mysql, mygramdb, seed_data):
        """Mixed English and Japanese should both be searchable."""
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Mixed Script",
                    "content": (
                        "Hello \u4e16\u754c \u3053\u3093\u306b\u3061\u306f World mixed script test"
                    ),
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", "Hello"),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="mixed script English search",
        )

        # Japanese part should also be searchable (same document, already indexed)
        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", "\u4e16\u754c"),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="mixed script Japanese search",
        )

    def test_emoji_content(self, mysql, mygramdb, seed_data):
        """Text next to an emoji stays findable.

        Emoji are outside the Basic Multilingual Plane, so a byte- or UTF-16
        oriented slip in normalization corrupts the n-grams around them rather
        than crashing. That shows up as the neighbouring words becoming
        unsearchable, which is what this requires -- a call that merely returned
        a dictionary would pass with the text mangled.
        """
        marker = f"emoji{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Emoji Test",
                    "content": (
                        f"\U0001f389\U0001f38a\U0001f388 Party time"
                        f" celebration {marker} \U0001f973 aftermath"
                    ),
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=1,
            timeout=20,
            interval=0.5,
            description="the emoji-bearing row to be indexed",
        )

        # Words on both sides of an emoji run must survive it.
        assert mygramdb.count("testdb.articles", "Party") >= 1, (
            "text before an emoji became unsearchable"
        )
        assert mygramdb.count("testdb.articles", "aftermath") >= 1, (
            "text after an emoji became unsearchable"
        )

    def test_accented_characters(self, mysql, mygramdb, seed_data):
        """Accented characters should be handled properly."""
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Accented Test",
                    "content": "caf\u00e9 r\u00e9sum\u00e9 na\u00efve accented character test",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", "caf\u00e9"),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="accented character search",
        )
