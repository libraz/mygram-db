"""Protocol-level attack and edge case tests.

Each case asserts the framing and error contract the server actually promises:
which byte sequences terminate a command, which are rejected and with what
code, and that nothing an attacker supplies is reflected back verbatim. A
liveness-only check passes even when the framing rule silently changes, which
is exactly the kind of regression that desynchronises real clients.
"""

from __future__ import annotations

import os

import pytest

from lib.protocol import (
    QUERY_ERRORS,
    assert_error,
    assert_no_control_bytes,
    parse_response,
    parse_search_results,
    split_responses,
)
from lib.raw_socket import (
    raw_tcp_exchange,
    raw_tcp_send_only,
    raw_tcp_slow_send,
)
from lib.wait import wait_until

# Longest query the test configuration accepts.
MAX_QUERY_LENGTH = 128


@pytest.mark.edge_cases
class TestProtocolAttacks:
    """Protocol-level robustness tests using raw TCP sockets."""

    def _exchange(self, mygramdb, data: bytes, timeout: float = 5.0) -> bytes:
        return raw_tcp_exchange(mygramdb.host, mygramdb.tcp_port, data, timeout=timeout)

    def test_binary_garbage_data(self, mygramdb, seed_data):
        """Unterminated binary garbage yields no response and no leaked bytes.

        Without a CRLF the server has no complete command, so it must buffer
        rather than guess. Emitting anything here would mean it acted on a
        fragment.
        """
        garbage = os.urandom(4096).replace(b"\r\n", b"\x00\x00")
        raw = self._exchange(mygramdb, garbage, timeout=5.0)
        assert raw == b"", f"unterminated garbage produced a response: {raw[:200]!r}"
        # A following command on a fresh connection must still be framed correctly.
        assert parse_response(self._exchange(mygramdb, b"INFO\r\n")).is_ok

    def test_null_bytes_in_command(self, mygramdb, seed_data):
        """NUL bytes are sanitized out of the echoed verb, not passed through.

        The command is unknown, and the NULs must be replaced before the
        offending text is quoted back to the client.
        """
        raw = self._exchange(mygramdb, b"SEARCH\x00articles\x00test\r\n")
        assert_error(raw, code_in=QUERY_ERRORS, message_contains="unknown command")
        assert b"\x00" not in raw, f"NUL byte reflected to the client: {raw[:200]!r}"

    def test_bare_lf_delimiter(self, mygramdb, seed_data):
        """A bare LF does not terminate a command; the server keeps waiting."""
        raw = self._exchange(mygramdb, b"INFO\n", timeout=3.0)
        assert raw == b"", f"bare LF was accepted as a terminator: {raw[:200]!r}"

    def test_bare_cr_delimiter(self, mygramdb, seed_data):
        """A bare CR does not terminate a command; the server keeps waiting."""
        raw = self._exchange(mygramdb, b"INFO\r", timeout=3.0)
        assert raw == b"", f"bare CR was accepted as a terminator: {raw[:200]!r}"

    def test_mixed_delimiters(self, mygramdb, seed_data):
        """A bare LF between CRLF commands joins lines instead of splitting them.

        ``INFO\\r\\nINFO\\nINFO\\r\\n`` spells INFO three times but contains only
        two CRLF frames. The middle bare LF ends up as whitespace inside the
        second frame, so exactly two responses come back; a third would mean
        the framing had started accepting a bare LF as a terminator.
        """
        raw = self._exchange(mygramdb, b"INFO\r\nINFO\nINFO\r\n", timeout=5.0)
        assert raw.startswith(b"OK INFO"), f"first INFO was not answered: {raw[:120]!r}"
        assert raw.count(b"OK INFO") == 2, (
            f"expected 2 CRLF-framed responses, got {raw.count(b'OK INFO')}: "
            "a bare LF was treated as a command terminator"
        )

    def test_oversized_request_no_newline(self, mygramdb, seed_data):
        """An 11 MB unterminated line is dropped without a response.

        The connection buffer must not grow without bound waiting for a CRLF
        that never comes.
        """
        raw = self._exchange(mygramdb, b"a" * (11 * 1024 * 1024), timeout=10.0)
        assert raw == b"", f"oversized unterminated input produced a response: {raw[:200]!r}"
        assert parse_response(self._exchange(mygramdb, b"INFO\r\n")).is_ok

    def test_oversized_single_line_command(self, mygramdb, seed_data):
        """An over-length query is refused by length, naming both numbers."""
        long_query = "a" * 200_000
        raw = self._exchange(
            mygramdb, f"SEARCH testdb.articles {long_query}\r\n".encode(), timeout=10.0
        )
        resp = assert_error(raw, code_in=QUERY_ERRORS, message_contains="length")
        assert str(len(long_query)) in resp.message, (
            f"the rejected length should be reported, got: {resp.message!r}"
        )
        assert str(MAX_QUERY_LENGTH) in resp.message, (
            f"the configured limit should be reported, got: {resp.message!r}"
        )
        # The rejected payload itself must not be echoed back.
        assert len(resp.message) < 1000, "the error message echoed the oversized query"

    def test_partial_send_then_close(self, mygramdb, seed_data):
        """Partial-send-then-close cycles must not leak connection slots.

        If each abandoned connection leaked, the accepted-connection counter
        would keep climbing while the server stopped serving; asserting a real
        INFO still parses is what catches that.
        """
        for _ in range(10):
            raw_tcp_send_only(mygramdb.host, mygramdb.tcp_port, b"SEAR")

        wait_until(
            lambda: parse_response(self._exchange(mygramdb, b"INFO\r\n")).is_ok,
            timeout=10,
            interval=0.5,
            description="server to serve INFO after partial-send cycles",
        )
        info = mygramdb.info()
        assert info.get("version"), f"INFO body incomplete after partial sends: {info}"

    def test_partial_send_slow_completion(self, mygramdb, seed_data):
        """A command split across the CRLF is answered once the LF arrives."""
        raw = raw_tcp_slow_send(
            mygramdb.host,
            mygramdb.tcp_port,
            [b"INFO\r", b"\n"],
            delay=2.0,
            timeout=10.0,
        )
        assert raw.startswith(b"OK INFO"), f"slowly-completed INFO was not answered: {raw[:120]!r}"

    def test_pipelined_commands(self, mygramdb, seed_data):
        """Three pipelined commands get three responses, in order."""
        raw = self._exchange(mygramdb, b"INFO\r\nINFO\r\nINFO\r\n", timeout=5.0)
        assert raw.count(b"OK INFO") == 3, (
            f"expected 3 pipelined INFO responses, got {raw.count(b'OK INFO')}: {raw[:200]!r}"
        )

    def test_empty_lines_between_commands(self, mygramdb, seed_data):
        """Empty lines are answered as errors, not silently skipped.

        Silently skipping them would leave the client's request/response
        accounting one short for every blank line sent.
        """
        raw = self._exchange(mygramdb, b"\r\n\r\nINFO\r\n\r\n", timeout=5.0)
        assert_no_control_bytes(raw, context="empty-line responses")
        errors = [line for line in split_responses(raw) if line.startswith(b"ERROR ")]
        assert len(errors) >= 2, (
            f"each empty line should be answered, got {len(errors)} errors: {raw[:200]!r}"
        )
        for line in errors[:2]:
            assert_error(line, code_in=QUERY_ERRORS, message_contains="empty")
        assert b"OK INFO" in raw, "the real command after the empty lines was not processed"

    def test_command_with_trailing_spaces(self, mygramdb, seed_data):
        """Trailing spaces are trimmed, not treated as arguments."""
        raw = self._exchange(mygramdb, b"INFO   \r\n")
        resp = parse_response(raw)
        assert resp.is_ok and raw.startswith(b"OK INFO"), (
            f"INFO with trailing spaces was not accepted: {resp.text[:200]!r}"
        )

    def test_crlf_in_query_term(self, mygramdb, seed_data):
        """An embedded CRLF splits one request into two independent commands.

        This is the request-smuggling shape: the client believes it sent one
        SEARCH, so the server must not treat the tail as part of that query.
        """
        raw = self._exchange(mygramdb, b"SEARCH testdb.articles te\r\nst\r\n", timeout=5.0)
        lines = split_responses(raw)
        assert len(lines) == 2, (
            f"embedded CRLF should yield exactly 2 responses, got {len(lines)}: {raw[:200]!r}"
        )
        # First line is the truncated SEARCH; it must be a well-formed result set.
        parse_search_results(lines[0])
        # Second line is the orphaned tail parsed as its own command verb.
        assert_error(lines[1], code_in=QUERY_ERRORS, message_contains="unknown command")
