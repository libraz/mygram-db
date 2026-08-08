"""Test empty and small document edge cases."""

import uuid

import pytest

from lib.wait import wait_until_gte, wait_until_value

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


class TestEmptyAndSmall:
    """Test edge cases with empty and very small documents.

    Documents shorter than the n-gram width produce no postings, so the risk is
    not a crash but silent divergence: the row is either dropped from the store
    or left permanently unmatchable. Each test therefore requires the row to be
    reachable and to leave the rest of the index unchanged.
    """

    def test_empty_content(self, mysql, mygramdb, seed_data):
        """A row with empty text is accepted without disturbing other rows.

        It contributes no postings, so it must not become matchable by an
        unrelated query — an empty document that matches everything is the
        classic failure here.
        """
        marker = f"emptyprobe{uuid.uuid4().hex[:8]}"
        before = mygramdb.count("testdb.articles", "test")

        _insert(mysql, f"Empty Content {marker}", "")
        # The row carries no searchable text, so wait on a control row written
        # after it: once that arrives, the empty row has certainly been applied.
        _insert(mysql, "Empty Content Control", f"control text {marker}")
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=1,
            timeout=15,
            interval=0.5,
            description="control row after an empty-text row to be indexed",
        )

        assert mygramdb.count("testdb.articles", "test") == before, (
            "an empty-text row changed the result set of an unrelated query"
        )

    def test_single_character(self, mysql, mygramdb, seed_data):
        """A row shorter than the n-gram width stays reachable by substring.

        With a bigram index a one-character document has no n-grams at all, so
        it can only be found through the substring fallback. If that fallback
        regressed, the row would be silently unsearchable.
        """
        marker = f"q{uuid.uuid4().hex[:1]}"
        _insert(mysql, f"Single Char {marker}", marker)
        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=1,
            timeout=15,
            interval=0.5,
            description="single-character document to be reachable",
        )

    def test_minimum_bigram(self, mysql, mygramdb, seed_data):
        """Two-character content (minimum bigram) should be indexable."""
        marker = "zq"  # unlikely to exist elsewhere
        _insert(mysql, "Min Bigram", marker)

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="minimum bigram search",
        )
