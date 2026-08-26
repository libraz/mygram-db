"""Drive the admin-token configuration the shipped compose stack requires.

``docker-compose.yml`` refuses to start without a non-empty ``API_ADMIN_TOKEN``,
so the default deployment always runs with administrative commands gated behind
``AUTH``. That gate is per-connection, which no single-command client helper can
express, so this test speaks the TCP protocol directly on one socket.
"""

from __future__ import annotations

import os
import socket
import subprocess
import uuid
from collections.abc import Generator
from pathlib import Path

import pytest

from lib.mygramdb_client import MygramdbClient
from lib.wait import wait_until

pytestmark = pytest.mark.security

E2E_ROOT = Path(__file__).resolve().parents[2]
PROJECT_ROOT = E2E_ROOT.parent
MYGRAMDB_BINARY = Path(os.environ.get("MYGRAMDB_BINARY", PROJECT_ROOT / "build/bin/mygramdb"))
MYSQL_PORT = int(os.environ.get("MYSQL_PORT", "23306"))

ADMIN_TOKEN = f"e2e-admin-{uuid.uuid4().hex}"


def _free_port() -> int:
    """Reserve an ephemeral loopback port long enough to learn its number."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _write_config(path: Path, tcp_port: int, http_port: int, dump_dir: Path) -> None:
    """Write a config that mirrors the shipped deployment's admin-token gate."""
    path.write_text(
        f"""mysql:
  host: "127.0.0.1"
  port: {MYSQL_PORT}
  user: "repl_user"
  password: "test_password"
  database: "testdb"
  use_gtid: true
  connect_timeout_ms: 5000
  datetime_timezone: "+00:00"

tables:
  - name: "articles"
    primary_key: "id"
    text_source:
      column: "content"
    ngram_size: 2
    kanji_ngram_size: 1

replication:
  enable: true
  server_id: 99123
  start_from: "snapshot"

dump:
  dir: "{dump_dir}"
  interval_sec: 0

api:
  tcp:
    bind: "127.0.0.1"
    port: {tcp_port}
  http:
    enable: true
    bind: "127.0.0.1"
    port: {http_port}
  admin_token: "{ADMIN_TOKEN}"

network:
  allow_cidrs:
    - "127.0.0.0/8"

logging:
  level: "info"
  format: "json"
  file: ""
""",
        encoding="utf-8",
    )


class _AdminSession:
    """One TCP connection, so the per-connection AUTH state is observable."""

    def __init__(self, host: str, port: int, timeout: float = 15.0) -> None:
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._sock.settimeout(timeout)

    def command(self, text: str) -> str:
        self._sock.sendall((text + "\r\n").encode("utf-8"))
        data = b""
        while not data.endswith(b"\r\n"):
            chunk = self._sock.recv(65536)
            if not chunk:
                break
            data += chunk
        return data.decode("utf-8", errors="ignore").strip()

    def close(self) -> None:
        self._sock.close()


@pytest.fixture(scope="module")
def gated_server(tmp_path_factory: pytest.TempPathFactory) -> Generator[tuple, None, None]:
    """Run a MygramDB instance configured the way the shipped compose stack runs it."""
    if not MYGRAMDB_BINARY.exists():
        pytest.fail(f"MygramDB binary not found at {MYGRAMDB_BINARY}")

    tmp_path = tmp_path_factory.mktemp("admin-token")
    tcp_port = _free_port()
    http_port = _free_port()
    dump_dir = tmp_path / "dumps"
    dump_dir.mkdir()
    config = tmp_path / "mygramdb-admin-token.yaml"
    _write_config(config, tcp_port, http_port, dump_dir)

    process_env = os.environ.copy()
    # Matrix/run-all variables target the session-scoped server's endpoint and
    # would override what this config asks for.
    for name in (
        "MYGRAM_MYSQL_HOST",
        "MYGRAM_MYSQL_PORT",
        "MYGRAM_MYSQL_USER",
        "MYGRAM_MYSQL_PASSWORD",
        "MYGRAM_MYSQL_DATABASE",
        "MYGRAM_API_ADMIN_TOKEN",
    ):
        process_env.pop(name, None)

    log_file = (tmp_path / "admin-token.log").open("w", encoding="utf-8")
    process = subprocess.Popen(
        [str(MYGRAMDB_BINARY), "-c", str(config)],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        cwd=PROJECT_ROOT,
        env=process_env,
    )
    client = MygramdbClient("127.0.0.1", tcp_port=tcp_port, http_port=http_port)
    try:
        wait_until(
            client.ping,
            timeout=90,
            interval=0.5,
            description="admin-token MygramDB to accept connections",
        )
        yield tcp_port, client
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        log_file.close()


@pytest.mark.timeout(180)
def test_administrative_command_requires_auth_on_the_same_connection(gated_server) -> None:
    """The gate opens only after AUTH, and only for the connection that sent it."""
    tcp_port, _ = gated_server

    session = _AdminSession("127.0.0.1", tcp_port)
    try:
        before = session.command("REPLICATION STATUS")
        assert before.startswith("ERROR "), before
        assert "requires AUTH" in before, before

        assert session.command(f"AUTH {ADMIN_TOKEN}") == "OK AUTHENTICATED"

        after = session.command("REPLICATION STATUS")
        assert not after.startswith("ERROR "), after
    finally:
        session.close()

    # A second connection starts unauthenticated, so the gate is not global.
    fresh = _AdminSession("127.0.0.1", tcp_port)
    try:
        assert "requires AUTH" in fresh.command("REPLICATION STATUS")
    finally:
        fresh.close()


@pytest.mark.timeout(180)
def test_wrong_token_is_rejected_and_leaves_the_gate_closed(gated_server) -> None:
    """A failed AUTH must not leave the connection able to run admin commands."""
    tcp_port, _ = gated_server

    session = _AdminSession("127.0.0.1", tcp_port)
    try:
        rejected = session.command("AUTH not-the-configured-token")
        assert rejected.startswith("ERROR "), rejected
        assert ADMIN_TOKEN not in rejected, "the response echoed the configured token"
        assert "requires AUTH" in session.command("REPLICATION STATUS")
    finally:
        session.close()


@pytest.mark.timeout(180)
def test_thread_pool_is_running_under_the_gated_configuration(gated_server) -> None:
    """Search stays open and the worker pool is staffed with the gate in place."""
    _, client = gated_server

    metrics = client.metrics()
    workers = None
    for line in metrics.splitlines():
        if line.startswith("mygramdb_thread_pool_workers "):
            workers = float(line.split()[1])
            break
    assert workers is not None, "metrics did not expose mygramdb_thread_pool_workers"
    assert workers > 0, metrics
