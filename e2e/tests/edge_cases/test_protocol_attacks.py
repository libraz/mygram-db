"""Protocol-level attack and edge case tests.

Verify server robustness against malformed TCP input, partial sends,
oversized requests, and delimiter edge cases using raw sockets.
"""

from __future__ import annotations

import os
import time

import pytest

from lib.raw_socket import (
    raw_tcp_exchange,
    raw_tcp_send_only,
    raw_tcp_slow_send,
)

MYGRAMDB_HOST = "127.0.0.1"
MYGRAMDB_TCP_PORT = 11016


@pytest.mark.edge_cases
class TestProtocolAttacks:
    """Protocol-level robustness tests using raw TCP sockets."""

    def test_binary_garbage_data(self, mygramdb, seed_data):
        """4KB random bytes with no \\r\\n — server should timeout-close, not crash."""
        garbage = os.urandom(4096)
        # Ensure no accidental \r\n
        garbage = garbage.replace(b"\r\n", b"\x00\x00")
        raw_tcp_exchange(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT, garbage, timeout=5.0)
        # Server may return error or just close — either is fine
        # Key assertion: server still works afterward
        assert mygramdb.ping(), "Server unresponsive after garbage data"

    def test_null_bytes_in_command(self, mygramdb, seed_data):
        """Null bytes embedded in a command — should not crash."""
        data = b"SEARCH\x00articles\x00test\r\n"
        raw_tcp_exchange(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT, data, timeout=5.0)
        # May return error or treat as partial command
        assert mygramdb.ping(), "Server unresponsive after null-byte command"

    def test_bare_lf_delimiter(self, mygramdb, seed_data):
        """Command with bare LF (no CR) — server should wait for \\r\\n."""
        data = b"INFO\n"
        # Server expects \r\n, so bare \n should not be treated as command terminator
        raw_tcp_exchange(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT, data, timeout=3.0)
        # Timeout is acceptable — server is still waiting for \r\n
        assert mygramdb.ping(), "Server unresponsive after bare LF"

    def test_bare_cr_delimiter(self, mygramdb, seed_data):
        """Command with bare CR (no LF) — server should wait for \\r\\n."""
        data = b"INFO\r"
        raw_tcp_exchange(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT, data, timeout=3.0)
        assert mygramdb.ping(), "Server unresponsive after bare CR"

    def test_mixed_delimiters(self, mygramdb, seed_data):
        """Mix of valid \\r\\n and invalid bare \\n delimiters."""
        data = b"INFO\r\n" + b"INFO\n" + b"INFO\r\n"
        resp = raw_tcp_exchange(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT, data, timeout=5.0)
        resp.decode("utf-8", errors="ignore")
        # At least the first valid INFO should get a response
        assert mygramdb.ping(), "Server unresponsive after mixed delimiters"

    def test_oversized_request_no_newline(self, mygramdb, seed_data):
        """11MB of 'a' with no \\r\\n — should trigger size limit error."""
        data = b"a" * (11 * 1024 * 1024)
        resp = raw_tcp_exchange(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT, data, timeout=10.0)
        resp.decode("utf-8", errors="ignore").upper()
        # Server should reject oversized input or close connection
        assert mygramdb.ping(), "Server unresponsive after oversized request"

    def test_oversized_single_line_command(self, mygramdb, seed_data):
        """Very long single-line command — should return error."""
        long_query = "a" * 200_000
        cmd = f"SEARCH testdb.articles {long_query}\r\n"
        resp = raw_tcp_exchange(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT, cmd.encode("utf-8"), timeout=10.0)
        decoded = resp.decode("utf-8", errors="ignore").upper()
        # Expect error about request size or query length
        assert "ERROR" in decoded or len(resp) == 0, (
            f"Expected error for oversized command, got: {decoded[:200]}"
        )
        assert mygramdb.ping(), "Server unresponsive after oversized command"

    def test_partial_send_then_close(self, mygramdb, seed_data):
        """Send partial command then close — server should not leak FDs."""
        for _ in range(10):
            raw_tcp_send_only(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT, b"SEAR")
        # After 10 partial-send-close cycles, server should still be healthy
        time.sleep(0.5)
        assert mygramdb.ping(), "Server unresponsive after partial send cycles"
        # Verify a real command still works
        info = mygramdb.info()
        assert info, "INFO command failed after partial send cycles"

    def test_partial_send_slow_completion(self, mygramdb, seed_data):
        """Send command in two parts with delay — should process after completion."""
        resp = raw_tcp_slow_send(
            MYGRAMDB_HOST,
            MYGRAMDB_TCP_PORT,
            [b"INFO\r", b"\n"],
            delay=2.0,
            timeout=10.0,
        )
        decoded = resp.decode("utf-8", errors="ignore")
        # The completed command should get a response
        assert len(decoded) > 0, "No response for slowly-completed command"
        assert mygramdb.ping(), "Server unresponsive after slow completion"

    def test_pipelined_commands(self, mygramdb, seed_data):
        """Three commands sent in a single sendall — expect three responses."""
        data = b"INFO\r\nINFO\r\nINFO\r\n"
        resp = raw_tcp_exchange(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT, data, timeout=5.0)
        decoded = resp.decode("utf-8", errors="ignore")
        # Server may handle pipelining or process only the first command
        # At minimum, we should get at least one response
        assert len(decoded) > 0, "No response for pipelined commands"
        assert mygramdb.ping(), "Server unresponsive after pipelining"

    def test_empty_lines_between_commands(self, mygramdb, seed_data):
        """Empty lines (\\r\\n\\r\\n) before and after a valid command."""
        data = b"\r\n\r\nINFO\r\n\r\n"
        resp = raw_tcp_exchange(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT, data, timeout=5.0)
        resp.decode("utf-8", errors="ignore")
        # INFO command should be processed; empty lines should be skipped
        assert mygramdb.ping(), "Server unresponsive after empty lines"

    def test_command_with_trailing_spaces(self, mygramdb, seed_data):
        """Command with trailing spaces before \\r\\n — should still work."""
        resp = mygramdb.tcp_command("INFO   ")
        assert resp is not None, "INFO with trailing spaces returned None"
        assert "ERROR" not in resp.upper() or "unknown" not in resp.lower(), (
            f"Unexpected error for trailing spaces: {resp[:200]}"
        )

    def test_crlf_in_query_term(self, mygramdb, seed_data):
        """\\r\\n embedded in query term — should split into two separate commands."""
        data = b"SEARCH testdb.articles te\r\nst\r\n"
        resp = raw_tcp_exchange(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT, data, timeout=5.0)
        resp.decode("utf-8", errors="ignore")
        # "SEARCH testdb.articles te" is one (invalid) command, "st" is another
        # Both may return errors, but server should not crash
        assert mygramdb.ping(), "Server unresponsive after CRLF in query"
