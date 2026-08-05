"""Unit tests for HTTP response handling in the E2E client."""

from __future__ import annotations

import io
from urllib.error import HTTPError

from lib.mygramdb_client import MygramdbClient


def test_http_get_with_status_preserves_error_status_and_json_body(monkeypatch) -> None:
    """Readiness tests must be able to distinguish 503 from a transport failure."""

    def unavailable(*_args, **_kwargs):
        raise HTTPError(
            "http://127.0.0.1:20080/health/ready",
            503,
            "Service Unavailable",
            None,
            io.BytesIO(b'{"status":"not_ready","reason":"Replication is not running"}'),
        )

    monkeypatch.setattr("lib.mygramdb_client.urlopen", unavailable)
    client = MygramdbClient("127.0.0.1", http_port=20080)

    status, body = client.http_get_with_status("/health/ready")

    assert status == 503
    assert body == {"status": "not_ready", "reason": "Replication is not running"}
    assert client.http_get("/health/ready") == body
