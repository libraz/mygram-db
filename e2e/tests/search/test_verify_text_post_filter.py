"""Real-MySQL regression coverage for the verify_text post-filter."""

from __future__ import annotations

from uuid import uuid4

import pytest

pytestmark = pytest.mark.search


def test_verify_text_removes_ngram_false_positive(mysql, mygramdb):
    """A row containing every bigram separately must not match the whole token."""
    token = f"verifytext{uuid4().hex}"
    bigrams = [token[index : index + 2] for index in range(len(token) - 1)]
    false_positive_text = " separator ".join(bigrams)
    exact_match_text = f"prefix {token} suffix"

    mysql.insert_rows(
        "post_filter_docs",
        [
            {"content": false_positive_text},
            {"content": exact_match_text},
        ],
    )
    rows = mysql.execute(
        "SELECT id, content FROM post_filter_docs WHERE content IN (%s, %s)",
        (false_positive_text, exact_match_text),
    )
    expected_id = next(row["id"] for row in rows if row["content"] == exact_match_text)

    assert mygramdb.sync("testdb.post_filter_docs")
    result = mygramdb.search("testdb.post_filter_docs", token, limit=10)

    assert result["total"] == 1
    assert result["ids"] == [expected_id]
