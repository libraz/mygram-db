"""Test special character handling."""

import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from lib.wait import wait_until_value

pytestmark = pytest.mark.edge_cases


def _insert(mysql, title: str, content: str) -> None:
    mysql.insert_rows(
        "articles",
        [
            {
                "title": title,
                "content": content,
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
        ],
    )


class TestSpecialChars:
    """Test special characters, long queries, and injection strings.

    Content that looks like SQL or markup must be treated as opaque text on
    both the write and read paths. The interesting failure is not a crash but
    the text being interpreted somewhere — which shows up as the row becoming
    unsearchable by its own literal content.
    """

    def test_sql_injection_content(self, mysql, mygramdb, seed_data):
        """A SQL fragment stored as content stays literal and searchable.

        Requiring the row back by the exact injected substring is what shows
        the fragment travelled through replication and indexing as data. A
        liveness check passes even if the text were mangled or dropped.
        """
        marker = f"inj{uuid.uuid4().hex[:8]}"
        content = f"Robert'); DROP TABLE articles;-- {marker}"
        rows_before = mysql.count("articles")

        _insert(mysql, f"Injection Test {marker}", content)
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=1,
            timeout=15,
            interval=0.5,
            description="injection-shaped content to be indexed",
        )

        # The distinctive punctuation must have survived verbatim.
        assert mygramdb.count("testdb.articles", "DROP TABLE articles") >= 1, (
            "the stored SQL fragment is not searchable as literal text"
        )
        assert mysql.count("articles") == rows_before + 1, (
            "the injected fragment altered the source table"
        )

    def test_html_xss_content(self, mysql, mygramdb, seed_data):
        """Markup stored as content is returned literally, never stripped.

        If tags were stripped anywhere on the path the row would stop matching
        its own markup, which is both a correctness bug and a sign that the
        text is being interpreted rather than stored.
        """
        marker = f"xss{uuid.uuid4().hex[:8]}"
        _insert(mysql, f"XSS Test {marker}", f"<script>alert('xss')</script> {marker}")
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=1,
            timeout=15,
            interval=0.5,
            description="markup content to be indexed",
        )
        assert mygramdb.count("testdb.articles", "<script>") >= 1, (
            "markup was stripped instead of stored as literal text"
        )

    def test_long_query_string(self, mygramdb, seed_data):
        """A query past the length cap is refused rather than truncated.

        Truncating would silently answer a different question than the one
        asked, which is worse than an explicit rejection.
        """
        response = mygramdb.tcp_command("SEARCH testdb.articles " + "a" * 1000)
        assert response is not None
        assert response.startswith("ERROR "), (
            f"an over-length query must be refused, got: {response[:120]!r}"
        )
        assert "length" in response.lower(), f"the refusal must say why, got: {response[:120]!r}"

    @pytest.mark.parametrize(
        "query",
        [
            "hello world",
            "test@email.com",
            "path/to/file",
            "key=value&other=test",
        ],
    )
    def test_special_characters_in_search(self, mysql, mygramdb, seed_data, query):
        """Punctuation in a query matches the document containing it verbatim.

        Asserting only that a dict came back would pass even if every one of
        these queries silently returned nothing. Quote characters are excluded
        deliberately — they are query grammar rather than text, and are covered
        by their own case below.
        """
        marker = f"sc{uuid.uuid4().hex[:8]}"
        _insert(mysql, f"Special {marker}", f"{query} {marker}")
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=1,
            timeout=15,
            interval=0.5,
            description=f"document containing {query!r} to be indexed",
        )

        result = mygramdb.search("testdb.articles", query, limit=50)
        assert result["total"] >= 1, f"a document containing {query!r} was not found by that query"

    def test_unbalanced_quote_in_search_is_grammar_not_text(self, mysql, mygramdb, seed_data):
        """A lone quote in a query is refused, even when a document contains it.

        Quotes delimit phrases, so an unbalanced one is a malformed query
        rather than a literal to match. Treating it as text instead would make
        every phrase query ambiguous.
        """
        marker = f"qt{uuid.uuid4().hex[:8]}"
        _insert(mysql, f"Quote {marker}", f"quote'test {marker}")
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=1,
            timeout=15,
            interval=0.5,
            description="document containing a quote to be indexed",
        )

        response = mygramdb.tcp_command("SEARCH testdb.articles quote'test")
        assert response is not None
        assert response.startswith("ERROR "), (
            f"an unbalanced quote must be refused, got: {response[:120]!r}"
        )
        assert "quote" in response.lower(), (
            f"the refusal must name the quote, got: {response[:120]!r}"
        )

    def test_concurrent_same_search(self, mygramdb, seed_data):
        """Concurrent identical searches all return the same result set.

        Divergence between concurrent readers is the failure that matters here
        — a torn read or a cache race shows up as differing results, not as a
        crash, so comparing the responses to each other is what detects it.
        """
        expected = mygramdb.search("testdb.articles", "test", limit=10)

        with ThreadPoolExecutor(max_workers=50) as executor:
            results = list(
                executor.map(
                    lambda _: mygramdb.search("testdb.articles", "test", limit=10), range(50)
                )
            )

        assert len(results) == 50
        for index, result in enumerate(results):
            assert result["total"] == expected["total"], (
                f"concurrent search {index} saw total {result['total']}, "
                f"expected {expected['total']}"
            )
            assert result["ids"] == expected["ids"], (
                f"concurrent search {index} returned a different result set"
            )
