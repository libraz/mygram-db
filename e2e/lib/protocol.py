"""Assertions on the MygramDB TCP protocol surface.

These helpers exist so that robustness tests can assert what the server
actually promises instead of only that it stayed alive. A liveness check
passes even when the server starts reflecting raw attack bytes, changes an
error code, or silently accepts input it used to reject; the checks here fail
in all three cases.

The wire contract they encode:

* An error response is ``ERROR <code> <message>`` terminated by CRLF, where
  ``<code>`` is the numeric :class:`ErrorCode` and every control character in
  ``<message>`` has been replaced by a space. A control byte surviving into a
  response means an attacker-supplied byte reached the client verbatim, which
  can desynchronise a line-oriented client.
* A success response starts with ``OK`` and, for SEARCH/COUNT, carries a
  count that later fields must agree with.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

# Error code ranges, mirroring the module split in the project docs. Tests
# assert the range rather than the exact code where several codes in the same
# module are individually defensible, so that a legitimate refinement of the
# code does not fail the test but a jump to a different module does.
CONFIG_ERRORS = range(1000, 2000)
DATABASE_ERRORS = range(2000, 3000)
QUERY_ERRORS = range(3000, 4000)
INDEX_ERRORS = range(4000, 5000)
STORAGE_ERRORS = range(5000, 6000)
NETWORK_ERRORS = range(6000, 7000)

_ERROR_LINE = re.compile(r"^ERROR (\d+) (.*)$", re.DOTALL)

# Control characters that must never survive into a response line. The server
# replaces these with spaces when formatting an error message.
_CONTROL_BYTES = bytes(range(0x00, 0x20)) + b"\x7f"


@dataclass(frozen=True)
class Response:
    """A single parsed protocol response."""

    raw: bytes
    text: str
    is_error: bool
    code: int | None
    message: str

    @property
    def is_ok(self) -> bool:
        return self.text.startswith("OK") or self.text.startswith("+OK")


def parse_response(raw: bytes) -> Response:
    """Parse one response, tolerating the trailing CRLF.

    Args:
        raw: Bytes read from the socket.

    Returns:
        The parsed response. Unparseable input still yields a Response so that
        callers can assert on it rather than raising during collection.
    """
    text = raw.decode("utf-8", errors="replace").rstrip("\r\n")
    match = _ERROR_LINE.match(text)
    if match:
        return Response(
            raw=raw, text=text, is_error=True, code=int(match.group(1)), message=match.group(2)
        )
    return Response(raw=raw, text=text, is_error=False, code=None, message=text)


def assert_no_control_bytes(raw: bytes, *, context: str = "response") -> None:
    """Fail if any control byte survived into the response body.

    The trailing CRLF terminator is stripped before the check, so only bytes
    the server placed inside a response line are considered.

    Args:
        raw: Bytes read from the socket.
        context: Label used in the failure message.
    """
    body = raw
    while body.endswith(b"\r\n"):
        body = body[:-2]
    # Multi-line responses (INFO, REPLICATION) legitimately use CRLF between
    # their own lines; every other control byte is a leak.
    body = body.replace(b"\r\n", b"")
    leaked = sorted({byte for byte in body if byte in _CONTROL_BYTES})
    assert not leaked, (
        f"{context} leaked control bytes {[hex(b) for b in leaked]} to the client: {raw[:200]!r}"
    )


def assert_error(
    raw: bytes,
    *,
    code: int | None = None,
    code_in: range | None = None,
    message_contains: str | None = None,
    must_not_contain: str | None = None,
) -> Response:
    """Assert the response is a well-formed error and check its code/message.

    Args:
        raw: Bytes read from the socket.
        code: Exact error code required, when only one code is defensible.
        code_in: Error-code range required, when several codes are defensible.
        message_contains: Substring the message must contain (case-insensitive).
        must_not_contain: Substring the message must NOT contain — used to
            assert that attacker-supplied input is not reflected verbatim.

    Returns:
        The parsed response, so callers can make further assertions.
    """
    resp = parse_response(raw)
    assert resp.is_error, f"expected an ERROR response, got: {resp.text[:200]!r}"
    assert_no_control_bytes(raw, context="error response")

    if code is not None:
        assert resp.code == code, f"expected error code {code}, got {resp.code}: {resp.text[:200]}"
    if code_in is not None:
        assert resp.code in code_in, (
            f"expected an error code in [{code_in.start}, {code_in.stop}), "
            f"got {resp.code}: {resp.text[:200]}"
        )
    if message_contains is not None:
        assert message_contains.lower() in resp.message.lower(), (
            f"expected {message_contains!r} in the error message, got: {resp.message[:200]!r}"
        )
    if must_not_contain is not None:
        assert must_not_contain not in resp.message, (
            f"error message reflected {must_not_contain!r} back to the client: "
            f"{resp.message[:200]!r}"
        )
    return resp


@dataclass(frozen=True)
class SearchResult:
    """An ``OK RESULTS`` response split into its total and its returned ids.

    The leading number is the total number of matches, which the returned ids
    are a LIMIT-bounded page of — the two are not required to be equal.
    """

    total: int
    ids: list[int]


def parse_search_results(raw: bytes) -> SearchResult:
    """Assert an ``OK RESULTS`` response is internally consistent.

    The header and the body must agree in the directions that are always true
    regardless of LIMIT: a page can never be longer than the total, and a total
    of zero can never come with ids attached.

    Args:
        raw: Bytes read from the socket.

    Returns:
        The parsed total and the document ids, in server order.
    """
    resp = parse_response(raw)
    assert_no_control_bytes(raw, context="search response")
    assert resp.text.startswith("OK RESULTS"), (
        f"expected an OK RESULTS response, got: {resp.text[:200]!r}"
    )
    fields = resp.text.split()
    # "OK", "RESULTS", <total>, <id>...
    assert len(fields) >= 3, f"malformed OK RESULTS response: {resp.text[:200]!r}"
    total = int(fields[2])
    ids = [int(field) for field in fields[3:]]
    assert len(ids) <= total, (
        f"OK RESULTS returned {len(ids)} ids for a total of {total}: {resp.text[:200]!r}"
    )
    assert total != 0 or not ids, (
        f"OK RESULTS declared no matches but returned ids: {resp.text[:200]!r}"
    )
    assert len(ids) == len(set(ids)), f"OK RESULTS returned duplicate ids: {resp.text[:200]!r}"
    return SearchResult(total=total, ids=ids)


def parse_count(raw: bytes) -> int:
    """Assert an ``OK COUNT`` response and return the count.

    Args:
        raw: Bytes read from the socket.

    Returns:
        The document count reported by the server.
    """
    resp = parse_response(raw)
    assert_no_control_bytes(raw, context="count response")
    assert resp.text.startswith("OK COUNT"), (
        f"expected an OK COUNT response, got: {resp.text[:200]!r}"
    )
    return int(resp.text.split()[2])


def assert_ok_or_error(raw: bytes, *, code_in: range | None = None) -> Response:
    """Assert the response is a well-formed protocol response of either kind.

    Use for inputs where both accepting and rejecting are defensible, but a
    malformed response, a leaked control byte, or a silent disconnect is not.

    Args:
        raw: Bytes read from the socket.
        code_in: If the response is an error, the range its code must fall in.

    Returns:
        The parsed response.
    """
    resp = parse_response(raw)
    assert raw, "server returned no response at all"
    assert_no_control_bytes(raw)
    assert resp.is_error or resp.is_ok, f"response is neither OK nor ERROR: {resp.text[:200]!r}"
    if resp.is_error and code_in is not None:
        assert resp.code in code_in, (
            f"expected an error code in [{code_in.start}, {code_in.stop}), "
            f"got {resp.code}: {resp.text[:200]}"
        )
    return resp


def split_responses(raw: bytes) -> list[bytes]:
    """Split a pipelined byte stream into single-line responses.

    Only single-line responses (``OK RESULTS``, ``ERROR``, ``OK COUNT``) split
    correctly; multi-line responses such as INFO are returned as their own
    fragments and should be counted by prefix instead.

    Args:
        raw: Bytes read from the socket.

    Returns:
        Each CRLF-terminated line, with empty lines dropped.
    """
    return [line for line in raw.split(b"\r\n") if line]
