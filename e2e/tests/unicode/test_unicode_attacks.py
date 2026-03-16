"""Unicode attack and edge case tests.

Verify server handles invalid UTF-8, overlong encodings, surrogate pairs,
BOM markers, RTL overrides, combining character bombs, and control characters
without crashing.
"""

from __future__ import annotations

import pytest

from lib.raw_socket import raw_tcp_exchange

MYGRAMDB_HOST = "127.0.0.1"
MYGRAMDB_TCP_PORT = 11016


@pytest.mark.unicode
class TestUnicodeAttacks:
    """Unicode robustness tests using raw bytes and protocol commands."""

    def test_invalid_utf8_in_search(self, mygramdb, seed_data):
        """Invalid UTF-8 bytes (\\xff\\xfe) in search query — should not crash."""
        data = b"SEARCH articles \xff\xfe\r\n"
        resp = raw_tcp_exchange(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT, data, timeout=5.0)
        # Error or empty results — not a crash
        assert mygramdb.ping(), "Server unresponsive after invalid UTF-8"

    def test_overlong_utf8_encoding(self, mygramdb, seed_data):
        """Overlong UTF-8 encoding of '/' (\\xc0\\xaf) — ICU should reject."""
        data = b"SEARCH articles \xc0\xaf\r\n"
        resp = raw_tcp_exchange(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT, data, timeout=5.0)
        assert mygramdb.ping(), "Server unresponsive after overlong UTF-8"

    def test_surrogate_pair_in_search(self, mygramdb, seed_data):
        """Lone surrogate half (\\xed\\xa0\\x80) — invalid in UTF-8."""
        data = b"SEARCH articles \xed\xa0\x80\r\n"
        resp = raw_tcp_exchange(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT, data, timeout=5.0)
        assert mygramdb.ping(), "Server unresponsive after surrogate pair"

    def test_utf8_bom_in_command(self, mygramdb, seed_data):
        """UTF-8 BOM (\\xef\\xbb\\xbf) prefix before command."""
        data = b"\xef\xbb\xbfSEARCH articles test\r\n"
        resp = raw_tcp_exchange(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT, data, timeout=5.0)
        # May parse correctly (ignoring BOM) or return error
        assert mygramdb.ping(), "Server unresponsive after BOM prefix"

    def test_rtl_override_in_query(self, mygramdb, seed_data):
        """RTL Override character (U+202E) in search query."""
        query = "test\u202eevil"
        resp = mygramdb.tcp_command(f"SEARCH articles {query}")
        assert resp is not None
        assert mygramdb.ping(), "Server unresponsive after RTL override"

    def test_combining_character_bomb(self, mygramdb, seed_data):
        """Base character + 100 combining diacritical marks — ICU normalization stress."""
        # 'a' followed by 100 combining acute accents (U+0301)
        bomb = "a" + "\u0301" * 100
        resp = mygramdb.tcp_command(f"SEARCH articles {bomb}")
        assert resp is not None
        assert mygramdb.ping(), "Server unresponsive after combining char bomb"

    def test_emoji_zwj_sequence(self, mygramdb, seed_data):
        """Complex emoji ZWJ sequence (family emoji) — should not crash."""
        # Family: Man, Woman, Girl, Boy joined with ZWJ
        family = "\U0001F468\u200D\U0001F469\u200D\U0001F467\u200D\U0001F466"
        resp = mygramdb.tcp_command(f"SEARCH articles {family}")
        assert resp is not None
        assert mygramdb.ping(), "Server unresponsive after emoji ZWJ"

    def test_control_characters_in_query(self, mygramdb, seed_data):
        """ASCII control characters (\\x01\\x02\\x03) in query."""
        data = b"SEARCH articles \x01\x02\x03test\r\n"
        resp = raw_tcp_exchange(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT, data, timeout=5.0)
        assert mygramdb.ping(), "Server unresponsive after control characters"
