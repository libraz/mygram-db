"""MygramDB TCP and HTTP client for e2e tests."""

from __future__ import annotations

import contextlib
import json
import re
import socket
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class MygramdbError(RuntimeError):
    """Base class for failures raised while talking to MygramDB.

    Every failure to obtain a well-formed answer is raised rather than
    reported as an empty or zero-valued result, so a broken or unreachable
    server can never satisfy an assertion that expects a count of zero.
    """


class MygramdbTransportError(MygramdbError):
    """The command could not be exchanged with the server."""


class MygramdbCommandError(MygramdbError):
    """The server answered with an ERROR frame."""


class MygramdbSynchronizingError(MygramdbCommandError):
    """The server refused the command because the table is mid-sync."""


class MygramdbProtocolError(MygramdbError):
    """The response matched no frame the client knows how to read."""


class MygramdbClient:
    """MygramDB client supporting both TCP and HTTP protocols."""

    def __init__(
        self, host: str, tcp_port: int = 11016, http_port: int = 18080, unix_socket_path: str = ""
    ) -> None:
        self.host = host
        self.tcp_port = tcp_port
        self.http_port = http_port
        self.unix_socket_path = unix_socket_path

    def ping(self) -> bool:
        """Check whether MygramDB accepts TCP connections.

        Reachability is this method's return value, so an unreachable server
        is reported as False rather than raised. Callers that need a command
        result must use the command methods, which raise instead.
        """
        try:
            self.tcp_command("INFO")
        except MygramdbError:
            return False
        return True

    def _open_socket(self, timeout: float) -> socket.socket:
        """Connect to the configured TCP or Unix endpoint."""
        family = socket.AF_UNIX if self.unix_socket_path else socket.AF_INET
        sock = socket.socket(family, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        try:
            if self.unix_socket_path:
                sock.connect(self.unix_socket_path)
            else:
                sock.connect((self.host, self.tcp_port))
        except OSError:
            sock.close()
            raise
        return sock

    def _exchange(self, cmd: str, timeout: float, terminator: bytes) -> str:
        """Send ``cmd`` and read the response, or raise.

        A connection failure, a timeout, or a connection closed before any
        byte arrived is raised as MygramdbTransportError. Returning a sentinel
        instead would let an unreachable server be parsed as an empty result.
        """
        try:
            sock = self._open_socket(timeout)
            try:
                sock.sendall((cmd + "\r\n").encode("utf-8"))

                data = b""
                while True:
                    chunk = sock.recv(65536)
                    if not chunk:
                        break
                    data += chunk
                    if data.endswith(terminator):
                        break
            finally:
                sock.close()
        except OSError as exc:
            raise MygramdbTransportError(f"{cmd!r} failed: {exc!r}") from exc

        if not data:
            raise MygramdbTransportError(f"{cmd!r}: connection closed before any response")
        return data.decode("utf-8", errors="ignore").strip()

    def tcp_command(self, cmd: str, timeout: float = 30.0) -> str:
        """Send a TCP command and return the single-line response string."""
        return self._exchange(cmd, timeout, b"\r\n")

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
            success = (
                response.startswith("OK ")
                or response.startswith("(integer)")
                or response.startswith("OK")
            )
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
                cmd += f" FILTER {key}={value}"
        if sort:
            cmd += f" SORT {sort}"
        if limit is not None:
            if offset is not None and offset > 0:
                cmd += f" LIMIT {offset},{limit}"
            else:
                cmd += f" LIMIT {limit}"

        resp = self.tcp_command(cmd)
        # A refusal must not be parsed as an empty result set.
        self._raise_for_error_frame("SEARCH", resp)
        return self._parse_search_response(resp)

    def count(self, table: str, query: str, *, filters: dict[str, Any] | None = None) -> int:
        """Execute a COUNT command and return the count the server reported.

        An integer is returned only when the server answered with a success
        frame this client recognises. Every other outcome raises: an
        unreachable server, a refusal, and an unreadable frame must all be
        distinguishable from a genuine count of zero, because a poll waiting
        for zero would otherwise be satisfied by a server that never answered.
        """
        cmd = f"COUNT {table} {query}"
        if filters:
            for key, value in filters.items():
                cmd += f" FILTER {key}={value}"
        resp = self.tcp_command(cmd)
        self._raise_for_error_frame("COUNT", resp)

        # "OK COUNT N" format
        if resp.startswith("OK COUNT "):
            parts = resp.split()
            if len(parts) >= 3:
                try:
                    return int(parts[2])
                except ValueError as exc:
                    raise MygramdbProtocolError(
                        f"COUNT reported a non-numeric total: {resp!r}"
                    ) from exc
        # "(integer) N" format
        if resp.startswith("(integer)"):
            try:
                return int(resp.split(")", 1)[1].strip())
            except ValueError as exc:
                raise MygramdbProtocolError(
                    f"COUNT reported a non-numeric total: {resp!r}"
                ) from exc
        raise MygramdbProtocolError(f"unrecognised COUNT response: {resp!r}")

    def facet(
        self,
        table: str,
        column: str,
        query: str | None = None,
        *,
        limit: int | None = None,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, int]:
        """Execute a TCP FACET command and parse value counts."""
        cmd = f"FACET {table} {column}"
        if query:
            cmd += f" {query}"
        if filters:
            for key, value in filters.items():
                cmd += f" FILTER {key}={value}"
        if limit is not None:
            cmd += f" LIMIT {limit}"

        resp = self.tcp_command_multiline(cmd)
        self._raise_for_error_frame("FACET", resp)
        if not resp.startswith("OK FACET"):
            raise MygramdbProtocolError(f"unrecognised FACET response: {resp!r}")

        counts: dict[str, int] = {}
        for line in resp.splitlines()[1:]:
            if not line.strip() or "\t" not in line:
                continue
            value, count = line.rsplit("\t", 1)
            with contextlib.suppress(ValueError):
                counts[value] = int(count)
        return counts

    def tcp_command_multiline(
        self, cmd: str, timeout: float = 30.0, terminator: bytes = b"\r\n\r\n"
    ) -> str:
        """Send a TCP command and read until a multi-line response terminator."""
        return self._exchange(cmd, timeout, terminator)

    def info(self) -> dict[str, Any]:
        """Execute INFO command and parse result."""
        resp = self.tcp_command("INFO")
        self._raise_for_error_frame("INFO", resp)
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

    def _sync_status_line(self, table: str | None) -> str | None:
        """Return the SYNC STATUS line for ``table`` (or the first line).

        ``SYNC STATUS`` is a multi-line response terminated by ``END``; each
        monitored table gets its own ``table=<name> status=<...>`` line. The
        whole-response read used elsewhere can stop at the first line, so this
        uses a multi-line read and isolates the line for the requested table.
        Returns None if the response could not be read.
        """
        # The response body ends with ``END\r\n`` and the reactor appends the
        # protocol frame terminator, yielding ``END\r\n\r\n`` on the wire.
        # Waiting for only the body suffix makes ``endswith`` miss the frame
        # and turns every status read into a five-second socket timeout.
        try:
            resp = self.tcp_command_multiline("SYNC STATUS", timeout=5.0, terminator=b"END\r\n\r\n")
        except MygramdbTransportError:
            # This probe only ever runs inside sync()'s bounded polling loop,
            # which keeps retrying and ends in a False return. A single
            # unreadable poll is therefore reported, not hidden: a server that
            # stays unreachable exhausts the deadline and fails the caller.
            return None
        if table is None:
            return resp
        for line in resp.splitlines():
            if f"table={table} " in line or line.strip().endswith(f"table={table}"):
                return line
        # Requested table not present in the status report yet.
        return ""

    def _is_synchronizing_response(self, resp: str | None) -> bool:
        """True if ``resp`` is a server "table is synchronizing" rejection."""
        return resp is not None and "synchronizing" in resp.lower()

    def _raise_for_error_frame(self, command: str, resp: str) -> None:
        """Raise if ``resp`` is an ERROR frame.

        The mid-sync rejection gets its own type so a polling caller can retry
        that transient window while every other refusal still fails the test.
        """
        if not resp.startswith("ERROR"):
            return
        if self._is_synchronizing_response(resp):
            raise MygramdbSynchronizingError(
                f"{command} rejected while table synchronizing: {resp}"
            )
        raise MygramdbCommandError(f"{command} refused by the server: {resp}")

    @staticmethod
    def _normalize_completed_marker(line: str) -> str:
        """Drop volatile fields from a COMPLETED status line for comparison.

        A COMPLETED line carries ``time=<elapsed>s`` (and ``rate=``) which are
        recomputed on every poll, so they change even when no new sync has run.
        Stripping them leaves stable fields (``rows=``, ``gtid=``) that only
        advance when a genuinely new sync completes.
        """
        kept = [tok for tok in line.split() if not tok.startswith(("time=", "rate="))]
        return " ".join(kept)

    def _table_is_queryable(self, table: str | None) -> bool:
        """Probe whether ``table`` is queryable (not mid-sync rebuild).

        After SYNC publishes COMPLETED the table can briefly remain in the
        server's "synchronizing" set (the index is swapped but the sync guard
        has not yet released it), during which COUNT/SEARCH return an
        ``ERROR ... is synchronizing`` response. A trivial COUNT probe detects
        that window without depending on any seeded data.
        """
        if table is None:
            return True
        try:
            resp = self.tcp_command(f"COUNT {table} _readiness_probe_", timeout=5.0)
        except MygramdbTransportError:
            # Same bounded-loop contract as _sync_status_line: sync() retries
            # until its deadline and then returns False, so an unreadable probe
            # can only delay a failure, never turn one into a success.
            return False
        return not self._is_synchronizing_response(resp)

    def sync(self, table: str | None = None, timeout: float = 30.0) -> bool:
        """Execute SYNC and wait for the issued sync to fully complete.

        Sends the SYNC command, then polls SYNC STATUS until the operation it
        issued reaches COMPLETED, and finally confirms the table is queryable
        again before returning.

        SYNC STATUS does not expose a sync generation/sequence number, so a bare
        "COMPLETED" can belong to a *previous* sync of the same table. To avoid
        accepting a stale completion, this requires observing a fresh active
        state (STARTING/IN_PROGRESS) for the target table -- or an advance of the
        COMPLETED marker (rows/time/gtid) -- before honouring COMPLETED. After
        completion it probes the table until the "synchronizing" window clears,
        because COMPLETED is published slightly before the table leaves the
        server's syncing set.
        """
        # Capture the target table's status line *before* issuing SYNC so a
        # pre-existing COMPLETED can be distinguished from a fresh one. Compare
        # on the volatile-field-stripped marker so the ever-advancing time=
        # field does not make a stale completion look fresh.
        pre_line = self._sync_status_line(table)
        pre_completed_marker = (
            self._normalize_completed_marker(pre_line)
            if (pre_line and "COMPLETED" in pre_line)
            else None
        )

        cmd = f"SYNC {table}" if table else "SYNC"
        resp = self.tcp_command(cmd, timeout=timeout)
        if "OK" not in resp:
            return False

        deadline = time.monotonic() + timeout
        observed_active = False
        while time.monotonic() < deadline:
            line = self._sync_status_line(table)
            if line is None:
                time.sleep(0.25)
                continue

            if "FAILED" in line or "CANCELLED" in line:
                return False

            if "STARTING" in line or "IN_PROGRESS" in line:
                # Our sync is running; once we have seen this, a subsequent
                # COMPLETED is guaranteed to be the one we issued.
                observed_active = True
                time.sleep(0.25)
                continue

            if "COMPLETED" in line:
                is_fresh = (
                    observed_active
                    or pre_completed_marker is None
                    or self._normalize_completed_marker(line) != pre_completed_marker
                )
                if not is_fresh:
                    # Stale COMPLETED from a prior sync; keep polling until this
                    # sync transitions (or its marker advances).
                    time.sleep(0.25)
                    continue
                # Completed for this request: wait out the synchronizing window
                # so the index is actually queryable before returning.
                while time.monotonic() < deadline:
                    if self._table_is_queryable(table):
                        return True
                    time.sleep(0.25)
                return False

            time.sleep(0.25)

        return False

    def cache_clear(self) -> bool:
        """Clear the query cache."""
        return "OK" in self.tcp_command("CACHE CLEAR")

    def dump_save(self) -> bool:
        """Trigger a dump save."""
        return "OK" in self.tcp_command("DUMP SAVE", timeout=60.0)

    def dump_load(self, filepath: str = "mygramdb.dmp") -> bool:
        """Trigger a dump load."""
        return "OK" in self.tcp_command(f"DUMP LOAD {filepath}", timeout=60.0)

    def http_get_with_status(
        self, path: str, timeout: float = 10.0
    ) -> tuple[int, dict[str, Any] | str]:
        """HTTP GET request returning its status code and decoded response body."""
        url = f"http://{self.host}:{self.http_port}{path}"
        try:
            req = Request(url)
            with urlopen(req, timeout=timeout) as resp:
                body = resp.read().decode("utf-8")
                return resp.status, self._parse_json_or_text(body)
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            return exc.code, self._parse_json_or_text(body)
        except URLError as exc:
            return 0, str(exc)

    def http_get(self, path: str, timeout: float = 10.0) -> dict[str, Any] | str:
        """HTTP GET request to MygramDB, returning only its decoded response body."""
        _status, body = self.http_get_with_status(path, timeout)
        return body

    def http_post(
        self, path: str, payload: dict[str, Any], timeout: float = 10.0
    ) -> tuple[int, dict[str, Any] | str]:
        """HTTP POST request to MygramDB returning (status, parsed body)."""
        url = f"http://{self.host}:{self.http_port}{path}"
        body = json.dumps(payload).encode("utf-8")
        req = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urlopen(req, timeout=timeout) as resp:
                raw = resp.read().decode("utf-8")
                return resp.status, self._parse_json_or_text(raw)
        except HTTPError as exc:
            raw = exc.read().decode("utf-8", errors="replace")
            return exc.code, self._parse_json_or_text(raw)
        except URLError as exc:
            return 0, str(exc)

    def http_search(
        self,
        table: str,
        query: str,
        *,
        sort: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
        filters: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute HTTP search and normalize the result shape to match search()."""
        payload: dict[str, Any] = {"q": query}
        if filters:
            payload["filters"] = filters
        if sort:
            payload["sort"] = self._http_sort_payload(sort)
        if limit is not None:
            payload["limit"] = limit
        if offset is not None:
            payload["offset"] = offset

        status, body = self.http_post(f"{self._http_table_path(table)}/search", payload)
        if status != 200 or not isinstance(body, dict):
            return {"total": 0, "ids": [], "raw_response": body, "status": status}

        ids = []
        for item in body.get("results", []):
            if isinstance(item, dict) and "primary_key" in item:
                with contextlib.suppress(ValueError, TypeError):
                    ids.append(int(item["primary_key"]))
        return {
            "total": int(body.get("count", len(ids))),
            "ids": ids,
            "raw_response": body,
            "status": status,
        }

    def http_count(
        self, table: str, query: str, *, filters: dict[str, Any] | None = None
    ) -> tuple[int, int]:
        """Execute HTTP count and return (status, count)."""
        payload: dict[str, Any] = {"q": query}
        if filters:
            payload["filters"] = filters
        status, body = self.http_post(f"{self._http_table_path(table)}/count", payload)
        if status == 200 and isinstance(body, dict):
            return status, int(body.get("count", 0))
        return status, 0

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

    def replication_stop(self) -> bool:
        """Send REPLICATION STOP command."""
        return "STOPPED" in self.tcp_command("REPLICATION STOP")

    def replication_start(self) -> bool:
        """Send REPLICATION START command."""
        return "STARTED" in self.tcp_command("REPLICATION START")

    def replication_status(self) -> str:
        """Send REPLICATION STATUS command and return response."""
        return self.tcp_command_multiline("REPLICATION STATUS", timeout=10.0, terminator=b"END\r\n")

    def replication_field(self, key: str) -> str:
        """Read one field from the REPLICATION STATUS response.

        Raises when the field is absent so a renamed field fails the caller's
        assertion instead of silently reading as an empty string.
        """
        response = self.replication_status()
        for line in response.splitlines():
            line = line.strip().removeprefix("OK ")
            if line.startswith(f"{key}:"):
                return line.split(":", 1)[1].strip()
        raise AssertionError(f"{key} not found in REPLICATION STATUS:\n{response}")

    def replication_gtid(self) -> str:
        """Position replication has consumed up to."""
        return self.replication_field("current_gtid")

    def replication_is_running(self) -> bool:
        """Whether the replication reader is currently consuming events."""
        return self.replication_field("status") == "running"

    @staticmethod
    def _http_table_path(table: str) -> str:
        """Build the single-segment HTTP route prefix for a table.

        The server exposes table routes under ``/tables/{identity}/...`` where
        ``{identity}`` is the qualified ``<database>.<table>`` (or a bare
        ``table`` in single-database configurations). Callers pass the identity
        verbatim, so it is emitted as a single path segment without splitting.
        """
        return f"/tables/{table}"

    def _parse_json_or_text(self, raw: str) -> dict[str, Any] | str:
        with contextlib.suppress(json.JSONDecodeError):
            return json.loads(raw)
        return raw

    def _http_sort_payload(self, sort: str) -> dict[str, str]:
        parts = sort.split()
        column = parts[0]
        order = parts[1].upper() if len(parts) > 1 else "DESC"
        return {"column": column, "order": order}

    def _parse_search_response(self, resp: str) -> dict[str, Any]:
        """Parse a SEARCH response into structured data.

        Only a frame opening with a success status is parsed; anything else
        raises rather than reading as a zero-result set.
        """
        if not resp.startswith("OK"):
            raise MygramdbProtocolError(f"unrecognised SEARCH response: {resp!r}")

        result: dict[str, Any] = {"raw_response": resp, "ids": [], "total": 0}

        lines = resp.strip().split("\n")
        ids = []
        for line in lines:
            line = line.strip()
            # "OK RESULTS <total> <id1> <id2> ..." format
            if line.startswith("OK RESULTS "):
                parts = line.split()
                if len(parts) >= 3:
                    with contextlib.suppress(ValueError):
                        result["total"] = int(parts[2])
                    for p in parts[3:]:
                        with contextlib.suppress(ValueError):
                            ids.append(int(p))
                continue
            # "OK <total>" format
            if line.startswith("OK "):
                parts = line.split()
                if len(parts) >= 2:
                    with contextlib.suppress(ValueError):
                        result["total"] = int(parts[1])
                continue
            if line.isdigit():
                ids.append(int(line))
            elif re.match(r"^\d+\)?\s+\d+$", line):
                # "1) 42" numbered list format
                parts = line.split()
                if len(parts) >= 2:
                    with contextlib.suppress(ValueError):
                        ids.append(int(parts[-1]))

        result["ids"] = ids
        if not result["total"] and ids:
            result["total"] = len(ids)

        return result
