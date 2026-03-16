"""Test CJK (Chinese, Japanese, Korean) search functionality."""

import pytest

from lib.wait import wait_until_gte

pytestmark = pytest.mark.unicode


class TestCJKSearch:
    """Test CJK text search capabilities."""

    def test_japanese_unigram_search(self, mysql, mygramdb, seed_data):
        """Japanese text should be searchable via unigram."""
        mysql.insert_rows("articles", [{
            "title": "Japanese Test",
            "content": "東京タワーは東京都港区にある電波塔です",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])

        wait_until_gte(
            lambda: mygramdb.count("articles", "東京"),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="Japanese unigram search",
        )

    def test_chinese_search(self, mysql, mygramdb, seed_data):
        """Chinese text should be searchable."""
        mysql.insert_rows("articles", [{
            "title": "Chinese Test",
            "content": "北京市是中华人民共和国的首都",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])

        wait_until_gte(
            lambda: mygramdb.count("articles", "北京"),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="Chinese search",
        )

    def test_long_japanese_text(self, mysql, mygramdb, seed_data):
        """Longer Japanese text should be searchable."""
        mysql.insert_rows("articles", [{
            "title": "Long Japanese",
            "content": "日本語のテスト文章です。全文検索エンジンは日本語のテキストを正しくインデックス化する必要があります。",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])

        wait_until_gte(
            lambda: mygramdb.count("articles", "全文検索"),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="long Japanese text search",
        )
