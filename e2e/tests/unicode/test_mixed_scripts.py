"""Test mixed script and emoji handling."""

import pytest

from lib.wait import wait_until_gte

pytestmark = pytest.mark.unicode


class TestMixedScripts:
    """Test mixed script and special character handling."""

    def test_mixed_english_japanese(self, mysql, mygramdb, seed_data):
        """Mixed English and Japanese should both be searchable."""
        mysql.insert_rows("articles", [{
            "title": "Mixed Script",
            "content": "Hello \u4e16\u754c \u3053\u3093\u306b\u3061\u306f World mixed script test",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])

        wait_until_gte(
            lambda: mygramdb.count("articles", "Hello"),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="mixed script English search",
        )

        # Japanese part should also be searchable
        count = mygramdb.count("articles", "\u4e16\u754c")
        assert count >= 1

    def test_emoji_content(self, mysql, mygramdb, seed_data):
        """Content with emoji should not crash search."""
        mysql.insert_rows("articles", [{
            "title": "Emoji Test",
            "content": "\U0001f389\U0001f38a\U0001f388 Party time celebration emoji test \U0001f973",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])

        import time
        time.sleep(3)

        # Search for text adjacent to emoji
        result = mygramdb.search("articles", "Party", limit=10)
        assert isinstance(result, dict)

    def test_accented_characters(self, mysql, mygramdb, seed_data):
        """Accented characters should be handled properly."""
        mysql.insert_rows("articles", [{
            "title": "Accented Test",
            "content": "caf\u00e9 r\u00e9sum\u00e9 na\u00efve accented character test",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])

        wait_until_gte(
            lambda: mygramdb.count("articles", "caf\u00e9"),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="accented character search",
        )
