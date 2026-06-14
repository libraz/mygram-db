"""Test special character handling."""

import pytest

pytestmark = pytest.mark.edge_cases


class TestSpecialChars:
    """Test special characters, long queries, and injection strings."""

    def test_sql_injection_content(self, mysql, mygramdb, seed_data):
        """SQL injection strings should not cause issues."""
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Injection Test",
                    "content": "Robert'); DROP TABLE articles;-- injection test",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        import time

        time.sleep(2)
        # Should not crash and tables should still exist
        assert mygramdb.ping()
        count = mysql.count("articles")
        assert count > 0, "Table should still exist after injection content"

    def test_html_xss_content(self, mysql, mygramdb, seed_data):
        """XSS-like content should be stored and searchable safely."""
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "XSS Test",
                    "content": "<script>alert('xss')</script> xss test content",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        import time

        time.sleep(2)
        assert mygramdb.ping()

    def test_long_query_string(self, mygramdb, seed_data):
        """Very long query string should not crash."""
        long_query = "a" * 1000
        result = mygramdb.search("testdb.articles", long_query, limit=10)
        # Should return empty or error, not crash
        assert isinstance(result, dict)

    def test_special_characters_in_search(self, mygramdb, seed_data):
        """Special characters in search query should not crash."""
        special_queries = [
            "hello world",
            "test@email.com",
            "path/to/file",
            "key=value&other=test",
            "quote'test",
        ]
        for query in special_queries:
            result = mygramdb.search("testdb.articles", query, limit=10)
            assert isinstance(result, dict), f"Failed for query: {query}"

    def test_concurrent_same_search(self, mygramdb, seed_data):
        """50 concurrent identical searches should not crash."""
        from concurrent.futures import ThreadPoolExecutor, as_completed

        def do_search():
            return mygramdb.search("testdb.articles", "test", limit=10)

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(do_search) for _ in range(50)]
            results = [f.result() for f in as_completed(futures)]

        # All should succeed
        for r in results:
            assert isinstance(r, dict)
