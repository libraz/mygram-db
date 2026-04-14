"""Global pytest fixtures for MygramDB e2e tests."""

from __future__ import annotations

import os
import signal
import subprocess
import time
from collections.abc import Generator
from pathlib import Path

import pytest

from lib.metrics import MetricsSnapshot
from lib.mygramdb_client import MygramdbClient
from lib.mysql_client import MysqlClient
from lib.wait import wait_until

# Constants
DB_FLAVOR = os.environ.get("DB_FLAVOR", "mysql")  # "mysql" or "mariadb"
MYSQL_CONTAINER = "inttest_mariadb" if DB_FLAVOR == "mariadb" else "inttest_mysql"
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


def _mysql_major_minor(version_str: str) -> tuple[int, int]:
    """Parse MySQL version string (e.g. '9.6.0') into (major, minor)."""
    parts = version_str.split(".")
    return int(parts[0]), int(parts[1]) if len(parts) > 1 else 0


@pytest.fixture(scope="session")
def db_flavor() -> str:
    """Database flavor: 'mysql' or 'mariadb'."""
    return DB_FLAVOR


def pytest_collection_modifyitems(config, items):
    """Auto-skip tests marked mysql_only when running against MariaDB."""
    if DB_FLAVOR != "mariadb":
        return
    skip_marker = pytest.mark.skip(reason="MySQL-only test (requires ngram FULLTEXT)")
    for item in items:
        if "mysql_only" in item.keywords:
            item.add_marker(skip_marker)


@pytest.fixture(scope="session")
def mysql_version() -> tuple[int, int]:
    """MySQL/MariaDB version as (major, minor) tuple, from MYSQL_VERSION env var."""
    return _mysql_major_minor(os.environ.get("MYSQL_VERSION", "8.4"))


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

    log_file = open(MYGRAMDB_LOG, "w")  # noqa: SIM115
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
def setup_vector_table(mysql: MysqlClient, mysql_version: tuple[int, int], db_flavor: str) -> bool:
    """Create VECTOR table if MySQL >= 9.0. Returns True if created."""
    if db_flavor == "mariadb" or mysql_version[0] < 9:
        return False
    mysql.execute(
        "CREATE TABLE IF NOT EXISTS vec_articles ("
        "  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,"
        "  title VARCHAR(255) NOT NULL,"
        "  content TEXT NOT NULL,"
        "  embedding VECTOR(3) NOT NULL,"
        "  status INT NOT NULL DEFAULT 1,"
        "  enabled TINYINT NOT NULL DEFAULT 1,"
        "  PRIMARY KEY (id)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
    )
    return True


@pytest.fixture(scope="session")
def seed_data(mysql: MysqlClient, mygramdb: MygramdbClient) -> None:
    """Seed test data and wait for sync to complete."""
    from lib.data_generator import DataGenerator

    gen = DataGenerator(seed=42)

    # Seed articles
    rows = gen.generate_articles(count=100)
    mysql.insert_rows("articles", rows)
    mygramdb.sync("articles")

    # Seed products
    product_rows = gen.generate_products(count=50)
    mysql.insert_rows("products", product_rows)
    mygramdb.sync("products")

    def _doc_count() -> bool:
        info = mygramdb.info()
        count = info.get("total_documents", info.get("doc_count", info.get("documents", 0)))
        return count >= 130  # ~100 articles + ~50 products (some filtered by enabled=1)

    wait_until(
        _doc_count,
        timeout=30,
        interval=1,
        description="MygramDB to sync seed data (articles + products)",
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


@pytest.fixture(autouse=True)
def ensure_replication(mygramdb: MygramdbClient) -> None:
    """Ensure replication is running before each test.

    Some tests (e.g., resilience) stop/start replication and may leave it
    in a stopped state. This fixture automatically restarts it.
    After MySQL restart, REPLICATION START may fail with stale connections,
    so we fall back to SYNC which creates fresh connections.
    """
    status = mygramdb.replication_status()
    if "running" in status.lower():
        return

    # Try REPLICATION START first (fast path)
    for _ in range(5):
        resp = mygramdb.tcp_command("REPLICATION START", timeout=10.0)
        if resp and ("STARTED" in resp or "already" in resp.lower() or "running" in resp.lower()):
            time.sleep(1)
            return
        if resp and "stopping" in resp.lower():
            time.sleep(3)
            continue
        time.sleep(1)

    # REPLICATION START failed (likely stale MySQL connection after restart).
    # SYNC creates a fresh connection and restarts replication.
    mygramdb.sync("articles", timeout=30)
