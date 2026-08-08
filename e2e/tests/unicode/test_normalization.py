"""Test Unicode normalization (NFKC, fullwidth/halfwidth, etc.)."""

import uuid

import pytest

from lib.wait import wait_until_gte, wait_until_value

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
            lambda: mygramdb.count("testdb.articles", "Hello"),
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
            lambda: mygramdb.count("testdb.articles", "first"),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="NFKC ligature normalization",
        )

    def test_zero_width_characters(self, mysql, mygramdb, seed_data):
        """A zero-width separator keeps both sides independently searchable.

        U+200B is not a compatibility character, so NFKC preserves it: it is
        neither dropped (which would join the two tokens into one) nor widened
        to a space (which would split them). Both the indexing path and the
        query path must agree on that, otherwise text containing the separator
        becomes unreachable by any query.

        The three assertions pin exactly that: each side is findable on its
        own, the two sides are findable together via an explicit AND, and the
        forms that would only match under the drop or widen behaviours do not.
        """
        marker = f"zwprobe{uuid.uuid4().hex[:8]}"
        left, right = f"{marker}L", f"{marker}R"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Zero Width Test",
                    "content": f"{left}\u200b{right}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until_value(
            lambda: mygramdb.count("testdb.articles", left),
            expected=1,
            timeout=10,
            interval=0.5,
            description="text before a zero-width separator to be searchable",
        )
        assert mygramdb.count("testdb.articles", right) == 1, (
            "text after a zero-width separator must be searchable on its own"
        )
        assert mygramdb.count("testdb.articles", f"{left} AND {right}") == 1, (
            "both sides of a zero-width separator must belong to the same document"
        )

        # The separator is preserved, so neither the dropped nor the widened
        # form of the stored text matches.
        assert mygramdb.count("testdb.articles", f"{left}{right}") == 0, (
            "a zero-width separator must not be dropped during normalization"
        )
        assert mygramdb.count("testdb.articles", f"{left} {right}") == 0, (
            "a zero-width separator must not be widened to a space"
        )
