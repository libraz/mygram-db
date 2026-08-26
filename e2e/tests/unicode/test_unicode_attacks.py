"""Unicode attack and edge case tests.

Each case asserts the response the server actually promises — the error code,
the absence of reflected control bytes, and the result set — rather than only
that the process survived. A liveness-only check still passes when the server
starts echoing attacker bytes, when an error code silently moves to a different
module, or when input that used to be rejected is quietly accepted.
"""

from __future__ import annotations

import pytest

from lib.protocol import (
    QUERY_ERRORS,
    assert_error,
    assert_no_control_bytes,
    parse_search_results,
)
from lib.raw_socket import raw_tcp_exchange

# Longest query the test configuration accepts. Inputs above this are rejected
# on length before any normalization runs.
MAX_QUERY_LENGTH = 128


@pytest.mark.unicode
class TestUnicodeAttacks:
    """Unicode robustness tests using raw bytes and protocol commands."""

    def _exchange(self, mygramdb, data: bytes, timeout: float = 5.0) -> bytes:
        return raw_tcp_exchange(mygramdb.host, mygramdb.tcp_port, data, timeout=timeout)

    def test_invalid_utf8_in_search(self, mygramdb, seed_data):
        """Invalid UTF-8 bytes must be rejected as a query error, not indexed."""
        raw = self._exchange(mygramdb, b"SEARCH testdb.articles \xff\xfe\r\n")
        assert_error(
            raw,
            code_in=QUERY_ERRORS,
            message_contains="utf-8",
            # The rejected bytes must not come back out in the message.
            must_not_contain="�",
        )

    def test_overlong_utf8_encoding(self, mygramdb, seed_data):
        """Overlong encoding of '/' must be rejected, never decoded to '/'.

        Accepting an overlong form would let a caller smuggle a character past
        any filter that inspects the literal bytes.
        """
        raw = self._exchange(mygramdb, b"SEARCH testdb.articles \xc0\xaf\r\n")
        assert_error(raw, code_in=QUERY_ERRORS, message_contains="utf-8")

    def test_surrogate_pair_in_search(self, mygramdb, seed_data):
        """A lone surrogate half is invalid UTF-8 and must be rejected."""
        raw = self._exchange(mygramdb, b"SEARCH testdb.articles \xed\xa0\x80\r\n")
        assert_error(raw, code_in=QUERY_ERRORS, message_contains="utf-8")

    def test_utf8_bom_in_command(self, mygramdb, seed_data):
        """A BOM before the verb makes the command unknown, and is not executed.

        The BOM must not be silently stripped: that would make the same bytes
        mean different things depending on an invisible prefix.
        """
        raw = self._exchange(mygramdb, b"\xef\xbb\xbfSEARCH testdb.articles test\r\n")
        resp = assert_error(raw, code_in=QUERY_ERRORS, message_contains="unknown command")
        assert "SEARCH" in resp.message, (
            f"the rejected verb should be named in the message, got: {resp.message!r}"
        )

    def test_rtl_override_in_query(self, mygramdb, seed_data):
        """An RTL override is treated as ordinary query text, matching nothing.

        The seeded corpus contains no U+202E, so a non-empty result set would
        mean the override was stripped and the query matched on 'testevil'.
        """
        raw = self._exchange(mygramdb, "SEARCH testdb.articles test‮evil\r\n".encode())
        assert parse_search_results(raw).ids == []

    def test_combining_character_bomb(self, mygramdb, seed_data):
        """A combining-mark bomb is refused on length before normalization.

        Rejecting on length is what bounds the ICU normalization work; if the
        limit stopped being enforced, this input would reach the normalizer.
        """
        bomb = "a" + "́" * 200
        # The limit counts characters, so the bomb has to carry more combining
        # marks than the cap itself. Each mark is two bytes, so the byte length
        # the cap admits still bounds what reaches the normalizer.
        assert len(bomb) > MAX_QUERY_LENGTH
        raw = self._exchange(mygramdb, f"SEARCH testdb.articles {bomb}\r\n".encode())
        resp = assert_error(raw, code_in=QUERY_ERRORS, message_contains="length")
        assert str(len(bomb)) in resp.message, (
            f"the rejected character length should be reported, got: {resp.message!r}"
        )
        assert str(MAX_QUERY_LENGTH) in resp.message, (
            f"the configured limit should be reported, got: {resp.message!r}"
        )

    def test_emoji_zwj_sequence(self, mygramdb, seed_data):
        """A ZWJ emoji sequence is accepted as query text and matches nothing."""
        family = "\U0001f468‍\U0001f469‍\U0001f467‍\U0001f466"
        raw = self._exchange(mygramdb, f"SEARCH testdb.articles {family}\r\n".encode())
        assert parse_search_results(raw).ids == []

    def test_control_characters_in_query(self, mygramdb, seed_data):
        """Control bytes in a query must not be echoed back to the client.

        Whether the query matches is secondary; a control byte surviving into
        the response can desynchronise a line-oriented client.
        """
        raw = self._exchange(mygramdb, b"SEARCH testdb.articles \x01\x02\x03test\r\n")
        assert_no_control_bytes(raw, context="search with control bytes")
        assert parse_search_results(raw).ids == []
