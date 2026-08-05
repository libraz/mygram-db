"""A tiny query-aware TCP fault proxy for MySQL protocol E2E tests."""

from __future__ import annotations

import socket
import struct
import threading
from contextlib import suppress


class MySQLFaultProxy:
    """Relay MySQL traffic and reset one connection on an armed COM_QUERY."""

    def __init__(self, upstream_host: str, upstream_port: int) -> None:
        self._upstream = (upstream_host, upstream_port)
        self._listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen()
        self._listener.settimeout(0.2)
        self.port = int(self._listener.getsockname()[1])

        self._stop = threading.Event()
        self._fault_event = threading.Event()
        self._fault_lock = threading.Lock()
        self._armed_query: bytes | None = None
        self._remaining_faults = 0
        self._fault_count = 0
        self._sockets: set[socket.socket] = set()
        self._sockets_lock = threading.Lock()
        self._workers: list[threading.Thread] = []
        self._accept_thread = threading.Thread(target=self._accept_loop, daemon=True)

    @property
    def fault_count(self) -> int:
        with self._fault_lock:
            return self._fault_count

    def start(self) -> None:
        """Start accepting proxied connections."""
        self._accept_thread.start()

    def arm_query_reset_once(self, query_fragment: bytes) -> None:
        """Reset the first connection whose COM_QUERY contains the fragment."""
        self.arm_query_reset(query_fragment, count=1)

    def arm_query_reset(self, query_fragment: bytes, count: int) -> None:
        """Reset the next ``count`` connections whose COM_QUERY matches."""
        if not query_fragment:
            raise ValueError("query_fragment must not be empty")
        if count <= 0:
            raise ValueError("count must be positive")
        with self._fault_lock:
            if self._armed_query is not None:
                raise RuntimeError("a query fault is already armed")
            self._armed_query = query_fragment
            self._remaining_faults = count
            self._fault_event.clear()

    def wait_for_fault(self, timeout: float) -> bool:
        """Wait until the armed query has been matched and reset."""
        return self._fault_event.wait(timeout)

    def stop(self) -> None:
        """Stop the listener and close every active relay socket."""
        self._stop.set()
        self._close_socket(self._listener)
        with self._sockets_lock:
            sockets = list(self._sockets)
        for sock in sockets:
            self._close_socket(sock)
        if self._accept_thread.is_alive():
            self._accept_thread.join(timeout=2)
        for worker in self._workers:
            if worker.is_alive():
                worker.join(timeout=2)

    def __enter__(self) -> MySQLFaultProxy:
        self.start()
        return self

    def __exit__(self, *_args: object) -> None:
        self.stop()

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                client, _address = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            try:
                upstream = socket.create_connection(self._upstream, timeout=5)
                upstream.settimeout(None)
            except OSError:
                self._close_socket(client)
                continue
            self._track(client, upstream)
            worker = threading.Thread(
                target=self._handle_connection,
                args=(client, upstream),
                daemon=True,
            )
            self._workers.append(worker)
            worker.start()

    def _handle_connection(self, client: socket.socket, upstream: socket.socket) -> None:
        response_relay = threading.Thread(
            target=self._relay_responses,
            args=(upstream, client),
            daemon=True,
        )
        response_relay.start()
        try:
            while not self._stop.is_set():
                header = self._recv_exact(client, 4)
                if header is None:
                    return
                payload_length = int.from_bytes(header[:3], "little")
                payload = self._recv_exact(client, payload_length)
                if payload is None:
                    return
                if self._consume_fault(payload):
                    self._reset_socket(client)
                    self._reset_socket(upstream)
                    return
                upstream.sendall(header + payload)
        except OSError:
            return
        finally:
            self._close_socket(client)
            self._close_socket(upstream)
            self._untrack(client, upstream)
            response_relay.join(timeout=1)

    def _relay_responses(self, upstream: socket.socket, client: socket.socket) -> None:
        try:
            while not self._stop.is_set():
                data = upstream.recv(65536)
                if not data:
                    return
                client.sendall(data)
        except OSError:
            return
        finally:
            # Propagate an upstream close (including KILL CONNECTION) to the
            # client immediately instead of leaving its read blocked until a
            # socket timeout. The request relay owns final untracking.
            self._close_socket(client)
            self._close_socket(upstream)

    def _consume_fault(self, payload: bytes) -> bool:
        # COM_QUERY is command byte 0x03 followed by the SQL text.
        if not payload or payload[0] != 0x03:
            return False
        with self._fault_lock:
            if self._armed_query is None or self._armed_query not in payload[1:]:
                return False
            self._remaining_faults -= 1
            if self._remaining_faults == 0:
                self._armed_query = None
            self._fault_count += 1
            self._fault_event.set()
            return True

    @staticmethod
    def _recv_exact(sock: socket.socket, size: int) -> bytes | None:
        data = bytearray()
        while len(data) < size:
            chunk = sock.recv(size - len(data))
            if not chunk:
                return None
            data.extend(chunk)
        return bytes(data)

    def _track(self, *sockets: socket.socket) -> None:
        with self._sockets_lock:
            self._sockets.update(sockets)

    def _untrack(self, *sockets: socket.socket) -> None:
        with self._sockets_lock:
            for sock in sockets:
                self._sockets.discard(sock)

    @staticmethod
    def _close_socket(sock: socket.socket) -> None:
        with suppress(OSError):
            sock.shutdown(socket.SHUT_RDWR)
        with suppress(OSError):
            sock.close()

    @staticmethod
    def _reset_socket(sock: socket.socket) -> None:
        with suppress(OSError):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        MySQLFaultProxy._close_socket(sock)
