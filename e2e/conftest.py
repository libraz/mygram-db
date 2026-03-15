"""Global pytest fixtures for MygramDB e2e tests."""

from __future__ import annotations

import signal
import subprocess
from pathlib import Path
from typing import Generator

import pytest

from lib.mysql_client import MysqlClient
from lib.mygramdb_client import MygramdbClient
from lib.metrics import MetricsSnapshot
from lib.wait import wait_until

# Constants
MYSQL_CONTAINER = "inttest_mysql"
MYSQL_HOST = "127.0.0.1"
MYSQL_PORT = 13306
MYGRAMDB_HOST = "127.0.0.1"
MYGRAMDB_TCP_PORT = 11016
MYGRAMDB_HTTP_PORT = 18080

PROJECT_ROOT = Path(__file__).parent.parent
MYGRAMDB_BINARY = PROJECT_ROOT / "build" / "bin" / "mygramdb"
MYGRAMDB_CONFIG = Path(__file__).parent / "docker" / "mygramdb-test.yaml"
MYGRAMDB_LOG = Path("/tmp/mygramdb-e2e.log")
MYGRAMDB_DUMP_DIR = PROJECT_ROOT / "e2e" / "results" / "dumps"


@pytest.fixture(scope="session")
def mysql() -> MysqlClient:
    """Session-scoped MySQL client connected to the test database."""
    client = MysqlClient(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user="root",
        password="test_root_password",
        database="testdb",
    )
    wait_until(
        lambda: client.ping(),
        timeout=60,
        interval=2,
        description="MySQL to be ready",
    )
    return client


@pytest.fixture(scope="session")
def mygramdb_process() -> Generator[subprocess.Popen, None, None]:
    """Start MygramDB binary and yield the process. Teardown sends SIGTERM."""
    if not MYGRAMDB_BINARY.exists():
        pytest.fail(
            f"MygramDB binary not found at {MYGRAMDB_BINARY}. "
            f"Run 'cmake --build build --parallel' first."
        )
    if not MYGRAMDB_CONFIG.exists():
        pytest.fail(f"MygramDB config not found at {MYGRAMDB_CONFIG}")

    MYGRAMDB_DUMP_DIR.mkdir(parents=True, exist_ok=True)

    log_file = open(MYGRAMDB_LOG, "w")
    proc = subprocess.Popen(
        [str(MYGRAMDB_BINARY), "-c", str(MYGRAMDB_CONFIG)],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        cwd=str(PROJECT_ROOT),
    )

    yield proc

    # Teardown: graceful shutdown
    if proc.poll() is None:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    log_file.close()


@pytest.fixture(scope="session")
def mygramdb(mygramdb_process: subprocess.Popen) -> MygramdbClient:
    """Session-scoped MygramDB client."""
    client = MygramdbClient(
        host=MYGRAMDB_HOST,
        tcp_port=MYGRAMDB_TCP_PORT,
        http_port=MYGRAMDB_HTTP_PORT,
    )
    wait_until(
        lambda: client.ping(),
        timeout=90,
        interval=2,
        description="MygramDB to accept connections",
    )
    return client


@pytest.fixture(scope="session")
def seed_data(mysql: MysqlClient, mygramdb: MygramdbClient) -> None:
    """Seed test data and wait for sync to complete."""
    from lib.data_generator import DataGenerator

    gen = DataGenerator(seed=42)
    rows = gen.generate_articles(count=100)
    mysql.insert_rows("articles", rows)

    # Trigger initial sync (auto_initial_snapshot is false in test config)
    mygramdb.sync("articles")

    def _doc_count() -> bool:
        info = mygramdb.info()
        count = info.get("total_documents", info.get("doc_count", info.get("documents", 0)))
        return count >= 90  # Some rows filtered by required_filters (enabled=1, deleted_at IS NULL)

    wait_until(
        _doc_count,
        timeout=30,
        interval=1,
        description="MygramDB to sync seed data",
    )


@pytest.fixture
def clear_cache(mygramdb: MygramdbClient) -> None:
    """Clear MygramDB query cache before the test."""
    mygramdb.cache_clear()


@pytest.fixture
def metrics_snapshot(mygramdb: MygramdbClient) -> Generator[dict, None, None]:
    """Capture metrics before and after a test. Yields the 'before' snapshot."""
    before = MetricsSnapshot.capture(mygramdb)
    yield {"before": before, "after_fn": lambda: MetricsSnapshot.capture(mygramdb)}
