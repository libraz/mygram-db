"""Raw TCP socket utilities for protocol-level e2e tests."""

from __future__ import annotations

import socket
import time


def raw_tcp_exchange(
    host: str, port: int, data: bytes, timeout: float = 5.0
) -> bytes:
    """Send raw bytes over TCP and collect response until close or timeout.

    Args:
        host: Target host.
        port: Target port.
        data: Raw bytes to send.
        timeout: Socket timeout in seconds.

    Returns:
        All bytes received before the connection closed or timed out.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        try:
            sock.sendall(data)
        except (BrokenPipeError, ConnectionResetError):
            return b""
        response = b""
        while True:
            try:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                response += chunk
            except (socket.timeout, ConnectionResetError):
                break
        return response
    finally:
        sock.close()


def raw_tcp_send_only(host: str, port: int, data: bytes) -> None:
    """Send raw bytes and immediately close the connection.

    Args:
        host: Target host.
        port: Target port.
        data: Raw bytes to send.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5.0)
    try:
        sock.connect((host, port))
        sock.sendall(data)
    finally:
        sock.close()


def raw_tcp_slow_send(
    host: str, port: int, chunks: list[bytes], delay: float, timeout: float = 10.0
) -> bytes:
    """Send bytes in chunks with delay between each, then collect response.

    Args:
        host: Target host.
        port: Target port.
        chunks: List of byte chunks to send sequentially.
        delay: Delay in seconds between each chunk.
        timeout: Socket timeout for receiving the response.

    Returns:
        All bytes received after all chunks have been sent.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        for i, chunk in enumerate(chunks):
            sock.sendall(chunk)
            if i < len(chunks) - 1:
                time.sleep(delay)
        response = b""
        while True:
            try:
                chunk_recv = sock.recv(65536)
                if not chunk_recv:
                    break
                response += chunk_recv
            except socket.timeout:
                break
        return response
    finally:
        sock.close()


def raw_tcp_connect_disconnect(host: str, port: int) -> None:
    """Connect to the server and immediately disconnect without sending data."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5.0)
    try:
        sock.connect((host, port))
    finally:
        sock.close()
