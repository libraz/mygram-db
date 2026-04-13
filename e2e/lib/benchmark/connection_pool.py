"""Connection pool module for MygramDB and MySQL benchmark comparisons.

Provides thread-safe connection pools with pre-established connections,
TCP/UDS switching, and timed query execution for both MygramDB and MySQL.
"""

from __future__ import annotations

import contextlib
import socket
import time
from abc import ABC, abstractmethod
from queue import Empty, Queue
from types import TracebackType
from typing import Any

import mysql.connector


class ConnectionPool(ABC):
    """Base class for thread-safe connection pools."""

    def __init__(self, pool_size: int) -> None:
        self._pool_size = pool_size
        self._pool: Queue[Any] = Queue(maxsize=pool_size)
        self._established = False

    @property
    def pool_size(self) -> int:
        return self._pool_size

    @abstractmethod
    def _create_connection(self) -> Any:
        """Create and return a single connection."""

    @abstractmethod
    def _close_connection(self, conn: Any) -> None:
        """Close a single connection."""

    def establish_all(self) -> None:
        """Pre-establish all connections before benchmark measurement."""
        if self._established:
            return
        for _ in range(self._pool_size):
            conn = self._create_connection()
            self._pool.put(conn)
        self._established = True

    def get_connection(self, timeout: float = 30.0) -> Any:
        """Check out a connection from the pool."""
        return self._pool.get(timeout=timeout)

    def return_connection(self, conn: Any) -> None:
        """Return a connection to the pool."""
        self._pool.put_nowait(conn)

    def close_all(self) -> None:
        """Drain the pool and close all connections."""
        while True:
            try:
                conn = self._pool.get_nowait()
                with contextlib.suppress(Exception):
                    self._close_connection(conn)
            except Empty:
                break

    def __enter__(self) -> ConnectionPool:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.close_all()


class MygramdbConnection:
    """Wrapper around a raw socket providing a command_timed interface."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._closed = False

    @property
    def is_closed(self) -> bool:
        return self._closed

    def command_timed(self, cmd: str) -> tuple[bool, float, str]:
        """Send a command and return (success, elapsed_ms, response)."""
        try:
            start = time.perf_counter()
            self._sock.sendall((cmd + "\r\n").encode("utf-8"))

            data = b""
            while True:
                chunk = self._sock.recv(65536)
                if not chunk:
                    self._closed = True
                    return False, 0.0, "connection closed"
                data += chunk
                if data.endswith(b"\r\n"):
                    break

            elapsed = (time.perf_counter() - start) * 1000
            response = data.decode("utf-8", errors="ignore").strip()
            success = response.startswith(("OK", "(integer)"))
            return success, elapsed, response
        except Exception as e:
            self._closed = True
            return False, 0.0, str(e)

    def close(self) -> None:
        self._closed = True
        with contextlib.suppress(Exception):
            self._sock.close()


class MygramdbConnectionPool(ConnectionPool):
    """Connection pool for MygramDB using persistent TCP or UDS sockets."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 11016,
        unix_socket_path: str = "",
        pool_size: int = 1,
        socket_timeout: float = 30.0,
    ) -> None:
        super().__init__(pool_size)
        self._host = host
        self._port = port
        self._unix_socket_path = unix_socket_path
        self._socket_timeout = socket_timeout

    def _create_connection(self) -> MygramdbConnection:
        """Create a persistent TCP or UDS socket connection."""
        if self._unix_socket_path:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(self._socket_timeout)
            sock.connect(self._unix_socket_path)
        else:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(self._socket_timeout)
            sock.connect((self._host, self._port))
        return MygramdbConnection(sock)

    def _close_connection(self, conn: MygramdbConnection) -> None:
        conn.close()

    def command_timed(self, cmd: str) -> tuple[bool, float, str]:
        """Execute a command using a pooled connection.

        Gets a connection, sends the command, returns the connection.
        On broken connection, creates a replacement.
        """
        conn = self.get_connection()
        success, elapsed, response = conn.command_timed(cmd)
        if conn.is_closed:
            conn.close()
            with contextlib.suppress(Exception):
                conn = self._create_connection()
        self.return_connection(conn)
        return success, elapsed, response


class MysqlConnection:
    """Wrapper around a MySQL connection providing a command_timed interface."""

    def __init__(self, conn: mysql.connector.MySQLConnection) -> None:
        self._conn = conn
        self._closed = False

    @property
    def is_closed(self) -> bool:
        return self._closed

    def command_timed(self, cmd: str) -> tuple[bool, float, str]:
        """Execute a SQL query and return (success, elapsed_ms, response).

        The cmd format is: sql|param1|param2|...
        """
        parts = cmd.split("|")
        sql = parts[0]
        params = tuple(parts[1:]) if len(parts) > 1 else ()

        try:
            cursor = self._conn.cursor()
            try:
                start = time.perf_counter()
                cursor.execute(sql, params)
                rows = cursor.fetchall()
                elapsed = (time.perf_counter() - start) * 1000
                return True, elapsed, f"OK {len(rows)} rows"
            except Exception as exc:
                self._closed = True
                return False, 0.0, str(exc)
            finally:
                cursor.close()
        except Exception as exc:
            self._closed = True
            return False, 0.0, str(exc)

    def close(self) -> None:
        self._closed = True
        with contextlib.suppress(Exception):
            self._conn.close()


class MysqlConnectionPool(ConnectionPool):
    """Connection pool for MySQL using mysql.connector."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        database: str = "testdb",
        unix_socket: str = "",
        pool_size: int = 1,
    ) -> None:
        super().__init__(pool_size)
        self._config: dict[str, Any] = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database,
            "charset": "utf8mb4",
            "use_unicode": True,
            "autocommit": True,
        }
        if unix_socket:
            self._config["unix_socket"] = unix_socket

    def _create_connection(self) -> MysqlConnection:
        conn = mysql.connector.connect(**self._config)
        return MysqlConnection(conn)

    def _close_connection(self, conn: MysqlConnection) -> None:
        conn.close()

    def search_timed(
        self,
        table: str,
        column: str,
        query: str,
        where: str = "",
        limit: int = 100,
    ) -> tuple[bool, float, str]:
        """Execute a FULLTEXT search on a pooled connection."""
        conn = self.get_connection()
        try:
            match_clause = f"MATCH({column}) AGAINST(%s IN BOOLEAN MODE)"
            sql = f"SELECT id FROM {table} WHERE {match_clause}"
            if where:
                sql += f" AND {where}"
            sql += f" LIMIT {limit}"
            cmd = sql + "|" + query
            result = conn.command_timed(cmd)
            self.return_connection(conn)
            return result
        except Exception as exc:
            with contextlib.suppress(Exception):
                conn.close()
            try:
                replacement = self._create_connection()
                self.return_connection(replacement)
            except Exception:
                pass
            return False, 0.0, str(exc)
