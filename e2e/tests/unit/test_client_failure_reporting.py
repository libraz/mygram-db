"""Unit tests for how the e2e client reports a failed exchange.

A count of zero is the expected value in the convergence tests (cache
invalidation, DELETE propagation), so a client that answers zero when it could
not reach the server would make those tests pass hardest exactly when the
server is most broken. These tests pin the opposite behaviour: only a success
frame the client recognised produces a number.
"""

from __future__ import annotations

import socket
import threading
from collections.abc import Callable, Iterator

import pytest

from lib.mygramdb_client import (
    MygramdbClient,
    MygramdbCommandError,
    MygramdbProtocolError,
    MygramdbSynchronizingError,
    MygramdbTransportError,
)
from lib.wait import WaitTimeoutError, wait_until_value


def _free_port() -> int:
    """Reserve and immediately release a loopback port, leaving it unused."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


class _CannedServer:
    """One-shot TCP server replying with a fixed frame to every connection."""

    def __init__(self, reply: bytes | None) -> None:
        self._reply = reply
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self.port = int(self._sock.getsockname()[1])
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            with conn:
                conn.settimeout(5.0)
                try:
                    conn.recv(65536)
                    if self._reply is not None:
                        conn.sendall(self._reply)
                except OSError:
                    pass

    def close(self) -> None:
        self._stop.set()
        self._sock.close()
        self._thread.join(timeout=5)


@pytest.fixture
def canned_server() -> Iterator[Callable[[bytes | None], MygramdbClient]]:
    """Yield a factory for canned-reply servers, closing them afterwards."""
    servers: list[_CannedServer] = []

    def factory(reply: bytes | None) -> MygramdbClient:
        server = _CannedServer(reply)
        servers.append(server)
        return MygramdbClient("127.0.0.1", tcp_port=server.port)

    yield factory

    for server in servers:
        server.close()


def test_count_raises_when_the_server_is_unreachable() -> None:
    """A refused connection must not read as a count of zero."""
    client = MygramdbClient("127.0.0.1", tcp_port=_free_port())

    with pytest.raises(MygramdbTransportError):
        client.count("testdb.articles", "marker")


def test_wait_for_zero_times_out_when_the_server_is_unreachable() -> None:
    """The convergence oracle must not be satisfied by an unreachable server."""
    client = MygramdbClient("127.0.0.1", tcp_port=_free_port())

    with pytest.raises(WaitTimeoutError) as excinfo:
        wait_until_value(
            lambda: client.count("testdb.articles", "marker"),
            expected=0,
            timeout=1.0,
            interval=0.2,
            description="marker rows to disappear",
        )
    assert "last error" in str(excinfo.value)


def test_count_raises_when_the_connection_closes_without_a_reply(canned_server) -> None:
    """A server that accepts and hangs up has not reported a count."""
    client = canned_server(None)

    with pytest.raises(MygramdbTransportError):
        client.count("testdb.articles", "marker")


def test_count_raises_on_an_error_frame(canned_server) -> None:
    """Any refusal is a failure, not an empty result."""
    client = canned_server(b"ERROR 4001 unknown table 'testdb.articles'\r\n")

    with pytest.raises(MygramdbCommandError) as excinfo:
        client.count("testdb.articles", "marker")
    assert "unknown table" in str(excinfo.value)


def test_count_distinguishes_a_mid_sync_rejection(canned_server) -> None:
    """The mid-sync window keeps its own type so pollers can retry it."""
    client = canned_server(b"ERROR 4010 table testdb.articles is synchronizing\r\n")

    with pytest.raises(MygramdbSynchronizingError):
        client.count("testdb.articles", "marker")


def test_count_raises_on_an_unrecognised_frame(canned_server) -> None:
    """A frame the client cannot read is a failure, not a zero."""
    client = canned_server(b"OK\r\n")

    with pytest.raises(MygramdbProtocolError):
        client.count("testdb.articles", "marker")


def test_count_returns_the_reported_total(canned_server) -> None:
    """A recognised success frame yields exactly the number the server sent."""
    client = canned_server(b"OK COUNT 7\r\n")

    assert client.count("testdb.articles", "marker") == 7


def test_count_returns_a_true_zero(canned_server) -> None:
    """A genuine zero still satisfies a wait for zero."""
    client = canned_server(b"OK COUNT 0\r\n")

    assert client.count("testdb.articles", "marker") == 0
    assert (
        wait_until_value(
            lambda: client.count("testdb.articles", "marker"),
            expected=0,
            timeout=2.0,
            interval=0.1,
            description="marker rows to disappear",
        )
        == 0
    )


def test_search_raises_on_an_error_frame(canned_server) -> None:
    """A refused SEARCH must not read as an empty result set."""
    client = canned_server(b"ERROR 3001 malformed query\r\n")

    with pytest.raises(MygramdbCommandError):
        client.search("testdb.articles", "marker")


def test_search_returns_the_reported_results(canned_server) -> None:
    """A success frame is parsed into its total and primary keys."""
    client = canned_server(b"OK RESULTS 2\r\n1\t\r\n2\t\r\n")

    result = client.search("testdb.articles", "marker")

    assert result["total"] == 2
    assert result["ids"] == [1, 2]


def test_ping_reports_unreachability_without_raising() -> None:
    """ping()'s return value is the reachability report, so it stays a bool."""
    assert MygramdbClient("127.0.0.1", tcp_port=_free_port()).ping() is False
