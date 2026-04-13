"""Connection stress and edge case tests.

Verify ConnectionAcceptor and ThreadPool stability under connection storms,
rapid connect/disconnect cycles, slow clients, and half-close scenarios.
"""

from __future__ import annotations

import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from lib.raw_socket import (
    raw_tcp_connect_disconnect,
    raw_tcp_slow_send,
)

MYGRAMDB_HOST = "127.0.0.1"
MYGRAMDB_TCP_PORT = 11016


@pytest.mark.load
class TestConnectionStress:
    """Connection-level stress tests."""

    def test_connection_storm(self, mygramdb, seed_data):
        """200 concurrent connections each sending INFO — server should stay stable."""
        results = []

        def _connect_and_query(idx: int) -> tuple[int, bool, str]:
            """Open a fresh TCP connection, send INFO, read until \\r\\n, close."""
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(10.0)
                sock.connect((MYGRAMDB_HOST, MYGRAMDB_TCP_PORT))
                sock.sendall(b"INFO\r\n")
                data = b""
                while True:
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    data += chunk
                    if data.endswith(b"\r\n"):
                        break
                sock.close()
                decoded = data.decode("utf-8", errors="ignore")
                return idx, len(decoded) > 0, decoded[:100]
            except Exception as e:
                return idx, False, str(e)

        with ThreadPoolExecutor(max_workers=200) as executor:
            futures = [executor.submit(_connect_and_query, i) for i in range(200)]
            for f in as_completed(futures):
                results.append(f.result())

        successes = sum(1 for _, ok, _ in results if ok)
        # All connections should succeed — server supports 10000 max_connections
        assert successes >= 180, f"Only {successes}/200 connections succeeded"
        # Server must remain healthy
        time.sleep(1)
        assert mygramdb.ping(), "Server unresponsive after connection storm"

    def test_connect_disconnect_rapid(self, mygramdb, seed_data):
        """500 rapid connect-disconnect cycles with no data sent."""
        errors = 0
        for _ in range(500):
            try:
                raw_tcp_connect_disconnect(MYGRAMDB_HOST, MYGRAMDB_TCP_PORT)
            except Exception:
                errors += 1

        # Some connection refusals under rapid cycling are acceptable
        assert errors < 250, f"Too many connection errors: {errors}/500"
        time.sleep(1)
        assert mygramdb.ping(), "Server unresponsive after rapid connect/disconnect"
        info = mygramdb.info()
        assert info, "INFO failed after rapid connect/disconnect"

    def test_slow_client_send(self, mygramdb, seed_data):
        """Send 1 byte at a time with delays — server should eventually process."""
        command = b"INFO\r\n"
        chunks = [bytes([b]) for b in command]
        resp = raw_tcp_slow_send(
            MYGRAMDB_HOST,
            MYGRAMDB_TCP_PORT,
            chunks,
            delay=0.3,
            timeout=15.0,
        )
        decoded = resp.decode("utf-8", errors="ignore")
        assert len(decoded) > 0, "No response for slowly-sent command"
        assert mygramdb.ping(), "Server unresponsive after slow client"

    def test_slow_client_read(self, mygramdb, seed_data):
        """Send command normally, then read response 1 byte at a time."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)
        try:
            sock.connect((MYGRAMDB_HOST, MYGRAMDB_TCP_PORT))
            sock.sendall(b"INFO\r\n")

            # Read 1 byte at a time
            data = b""
            while True:
                try:
                    byte = sock.recv(1)
                    if not byte:
                        break
                    data += byte
                    if data.endswith(b"\r\n"):
                        break
                except TimeoutError:
                    break
        finally:
            sock.close()

        decoded = data.decode("utf-8", errors="ignore")
        assert len(decoded) > 0, "No response for slow-read client"
        assert mygramdb.ping()

    def test_concurrent_persistent_pipelines(self, mygramdb, seed_data):
        """50 persistent connections each sending 20 pipelined commands."""
        results = []

        def _pipeline_worker(idx: int) -> tuple[int, int, int]:
            """Returns (worker_idx, commands_sent, responses_received)."""
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(15.0)
                sock.connect((MYGRAMDB_HOST, MYGRAMDB_TCP_PORT))

                commands_sent = 0
                for _ in range(20):
                    sock.sendall(b"INFO\r\n")
                    commands_sent += 1

                # Collect all responses
                data = b""
                while True:
                    try:
                        chunk = sock.recv(65536)
                        if not chunk:
                            break
                        data += chunk
                    except TimeoutError:
                        break

                sock.close()
                # Count response blocks (rough estimate)
                decoded = data.decode("utf-8", errors="ignore")
                # Each INFO response contains multiple lines ending with \r\n
                response_count = (
                    decoded.count("total_documents")
                    or decoded.count("doc_count")
                    or decoded.count("uptime")
                )
                return idx, commands_sent, max(response_count, 1 if decoded else 0)
            except Exception:
                return idx, 0, 0

        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = [executor.submit(_pipeline_worker, i) for i in range(50)]
            for f in as_completed(futures):
                results.append(f.result())

        total_sent = sum(sent for _, sent, _ in results)
        total_responded = sum(resp for _, _, resp in results)
        assert total_sent > 0, "No commands were sent"
        assert total_responded > 0, "No responses received"
        time.sleep(1)
        assert mygramdb.ping(), "Server unresponsive after concurrent pipelines"

    def test_half_close_write(self, mygramdb, seed_data):
        """Send command then shutdown(SHUT_WR) — should still receive response."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(10.0)
        try:
            sock.connect((MYGRAMDB_HOST, MYGRAMDB_TCP_PORT))
            sock.sendall(b"INFO\r\n")
            sock.shutdown(socket.SHUT_WR)

            # Should still be able to read the response
            data = b""
            while True:
                try:
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    data += chunk
                except TimeoutError:
                    break
        finally:
            sock.close()

        decoded = data.decode("utf-8", errors="ignore")
        assert len(decoded) > 0, "No response after half-close"
        assert mygramdb.ping(), "Server unresponsive after half-close"
