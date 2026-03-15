"""MygramDB TCP and HTTP client for e2e tests."""

from __future__ import annotations

import json
import re
import socket
import time
from typing import Any
from urllib.request import urlopen, Request
from urllib.error import URLError


class MygramdbClient:
    """MygramDB client supporting both TCP and HTTP protocols."""

    def __init__(self, host: str, tcp_port: int = 11016, http_port: int = 18080, unix_socket_path: str = "") -> None:
        self.host = host
        self.tcp_port = tcp_port
        self.http_port = http_port
        self.unix_socket_path = unix_socket_path

    def ping(self) -> bool:
        """Check if MygramDB accepts TCP connections."""
        try:
            resp = self.tcp_command("INFO")
            return resp is not None
        except Exception:
            return False

    def tcp_command(self, cmd: str, timeout: float = 30.0) -> str | None:
        """Send a TCP command and return the response string."""
        try:
            if self.unix_socket_path:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            if self.unix_socket_path:
                sock.connect(self.unix_socket_path)
            else:
                sock.connect((self.host, self.tcp_port))
            sock.sendall((cmd + "\r\n").encode("utf-8"))

            data = b""
            while True:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                data += chunk
                if data.endswith(b"\r\n"):
                    break

            sock.close()
            return data.decode("utf-8", errors="ignore").strip()
        except Exception:
            return None

    def tcp_command_timed(self, cmd: str, timeout: float = 30.0) -> tuple[bool, float, str]:
        """Send a TCP command and return (success, elapsed_ms, response)."""
        try:
            if self.unix_socket_path:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            if self.unix_socket_path:
                sock.connect(self.unix_socket_path)
            else:
                sock.connect((self.host, self.tcp_port))

            start = time.perf_counter()
            sock.sendall((cmd + "\r\n").encode("utf-8"))

            data = b""
            while True:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                data += chunk
                if data.endswith(b"\r\n"):
                    break

            elapsed = (time.perf_counter() - start) * 1000
            sock.close()

            response = data.decode("utf-8", errors="ignore").strip()
            success = response.startswith("OK ") or response.startswith("(integer)") or response.startswith("OK")
            return success, elapsed, response
        except Exception as e:
            return False, 0.0, str(e)

    def search(
        self,
        table: str,
        query: str,
        *,
        sort: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute a SEARCH command and parse results.

        Returns dict with keys: total, ids, raw_response
        """
        cmd = f"SEARCH {table} {query}"
        if filters:
            for key, value in filters.items():
                cmd += f" FILTER {key} {value}"
        if sort:
            cmd += f" SORT {sort}"
        if limit is not None:
            if offset is not None and offset > 0:
                cmd += f" LIMIT {offset},{limit}"
            else:
                cmd += f" LIMIT {limit}"

        resp = self.tcp_command(cmd)
        return self._parse_search_response(resp)

    def count(self, table: str, query: str, *, filters: dict[str, Any] | None = None) -> int:
        """Execute a COUNT command."""
        cmd = f"COUNT {table} {query}"
        if filters:
            for key, value in filters.items():
                cmd += f" FILTER {key} {value}"
        resp = self.tcp_command(cmd)
        if not resp:
            return 0
        # Handle "OK COUNT N" format
        if resp.startswith("OK COUNT "):
            parts = resp.split()
            if len(parts) >= 3:
                try:
                    return int(parts[2])
                except ValueError:
                    pass
        # Handle "(integer) N" format
        if resp.startswith("(integer)"):
            return int(resp.split(")", 1)[1].strip())
        return 0

    def info(self) -> dict[str, Any]:
        """Execute INFO command and parse result."""
        resp = self.tcp_command("INFO")
        if not resp:
            return {}
        result: dict[str, Any] = {}
        for line in resp.split("\n"):
            line = line.strip()
            if ":" in line:
                key, _, value = line.partition(":")
                key = key.strip()
                value = value.strip()
                # Try to parse numeric values
                try:
                    result[key] = int(value)
                except ValueError:
                    try:
                        result[key] = float(value)
                    except ValueError:
                        result[key] = value
        return result

    def sync(self, table: str | None = None, timeout: float = 30.0) -> bool:
        """Execute SYNC and wait for completion.

        Sends the SYNC command, then polls SYNC STATUS until the operation
        reaches COMPLETED or FAILED state, or the timeout expires.
        """
        cmd = f"SYNC {table}" if table else "SYNC"
        resp = self.tcp_command(cmd, timeout=timeout)
        if resp is None or "OK" not in resp:
            return False

        # Poll SYNC STATUS until completion
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            status_resp = self.tcp_command("SYNC STATUS", timeout=5.0)
            if status_resp is None:
                time.sleep(0.5)
                continue
            if "COMPLETED" in status_resp:
                return True
            if "FAILED" in status_resp or "CANCELLED" in status_resp:
                return False
            # Still IN_PROGRESS or STARTING
            time.sleep(0.5)

        return False

    def cache_clear(self) -> bool:
        """Clear the query cache."""
        resp = self.tcp_command("CACHE CLEAR")
        return resp is not None and "OK" in resp

    def dump_save(self) -> bool:
        """Trigger a dump save."""
        resp = self.tcp_command("DUMP SAVE", timeout=60.0)
        return resp is not None and "OK" in resp

    def dump_load(self, filepath: str = "mygramdb.dmp") -> bool:
        """Trigger a dump load."""
        resp = self.tcp_command(f"DUMP LOAD {filepath}", timeout=60.0)
        return resp is not None and "OK" in resp

    def http_get(self, path: str, timeout: float = 10.0) -> dict[str, Any] | str:
        """HTTP GET request to MygramDB."""
        url = f"http://{self.host}:{self.http_port}{path}"
        try:
            req = Request(url)
            with urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8")
                try:
                    return json.loads(body)
                except json.JSONDecodeError:
                    return body
        except URLError:
            return {}

    def health_live(self) -> bool:
        """Check /health/live endpoint."""
        resp = self.http_get("/health/live")
        if isinstance(resp, dict):
            return resp.get("status") in ("ok", "UP", "alive")
        return False

    def health_ready(self) -> bool:
        """Check /health/ready endpoint."""
        resp = self.http_get("/health/ready")
        if isinstance(resp, dict):
            return resp.get("status") in ("ok", "UP", "ready")
        return False

    def health_detail(self) -> dict[str, Any]:
        """Get /health/detail endpoint."""
        resp = self.http_get("/health/detail")
        return resp if isinstance(resp, dict) else {}

    def metrics(self) -> str:
        """Get raw /metrics endpoint (Prometheus format)."""
        resp = self.http_get("/metrics")
        return resp if isinstance(resp, str) else ""

    def _parse_search_response(self, resp: str | None) -> dict[str, Any]:
        """Parse a SEARCH response into structured data."""
        if not resp:
            return {"total": 0, "ids": [], "raw_response": ""}

        result: dict[str, Any] = {"raw_response": resp, "ids": [], "total": 0}

        lines = resp.strip().split("\n")
        ids = []
        for line in lines:
            line = line.strip()
            # "OK RESULTS <total> <id1> <id2> ..." format
            if line.startswith("OK RESULTS "):
                parts = line.split()
                if len(parts) >= 3:
                    try:
                        result["total"] = int(parts[2])
                    except ValueError:
                        pass
                    for p in parts[3:]:
                        try:
                            ids.append(int(p))
                        except ValueError:
                            pass
                continue
            # "OK <total>" format
            if line.startswith("OK "):
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        result["total"] = int(parts[1])
                    except ValueError:
                        pass
                continue
            if line.isdigit():
                ids.append(int(line))
            elif re.match(r"^\d+\)?\s+\d+$", line):
                # "1) 42" numbered list format
                parts = line.split()
                if len(parts) >= 2:
                    try:
                        ids.append(int(parts[-1]))
                    except ValueError:
                        pass

        result["ids"] = ids
        if not result["total"] and ids:
            result["total"] = len(ids)

        return result
