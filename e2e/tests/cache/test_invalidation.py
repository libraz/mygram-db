"""Test cache invalidation on data changes."""

from __future__ import annotations

import json
import os
import socket
import subprocess
import uuid
from collections.abc import Generator
from pathlib import Path

import pytest

from lib.metrics import CACHE_ENTRIES, CACHE_HITS, CACHE_INVALIDATIONS_IMMEDIATE, read_counter
from lib.mygramdb_client import MygramdbClient
from lib.mysql_client import MysqlClient
from lib.wait import wait_until, wait_until_gte

pytestmark = pytest.mark.cache

PROJECT_ROOT = Path(__file__).resolve().parents[3]
MYGRAMDB_BINARY = Path(os.environ.get("MYGRAMDB_BINARY", PROJECT_ROOT / "build/bin/mygramdb"))


def _free_tcp_http_ports() -> tuple[int, int]:
    """Find distinct ephemeral loopback ports for a dedicated daemon."""
    sockets: list[socket.socket] = []
    try:
        for _ in range(2):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
        return int(sockets[0].getsockname()[1]), int(sockets[1].getsockname()[1])
    finally:
        for sock in sockets:
            sock.close()


def _write_sync_cache_config(
    path: Path,
    mysql: MysqlClient,
    table: str,
    tcp_port: int,
    http_port: int,
    dump_dir: Path,
    *,
    replication_enabled: bool = False,
    auto_initial_snapshot: bool = False,
) -> None:
    """Write an isolated one-table config for SYNC or replication tests."""
    mysql_config = mysql.config
    mysql_host = json.dumps(str(mysql_config["host"]))
    mysql_port = int(mysql_config["port"])
    mysql_user = json.dumps(str(mysql_config["user"]))
    mysql_password = json.dumps(str(mysql_config["password"]))
    mysql_database = json.dumps(str(mysql_config["database"]))
    dump_path = json.dumps(str(dump_dir))
    server_id = 100_000 + (int(uuid.uuid4().hex[:6], 16) % 900_000)
    path.write_text(
        f"""mysql:
  host: {mysql_host}
  port: {mysql_port}
  user: {mysql_user}
  password: {mysql_password}
  database: {mysql_database}
  use_gtid: true
  connect_timeout_ms: 5000
  datetime_timezone: "+00:00"

tables:
  - name: "{table}"
    primary_key: "id"
    text_source:
      column: "content"
    ngram_size: 2
    kanji_ngram_size: 1

replication:
  enable: {str(replication_enabled).lower()}
  auto_initial_snapshot: {str(auto_initial_snapshot).lower()}
  server_id: {server_id}
  start_from: "snapshot"

dump:
  dir: {dump_path}
  interval_sec: 0

api:
  tcp:
    bind: "127.0.0.1"
    port: {tcp_port}
  http:
    enable: true
    bind: "127.0.0.1"
    port: {http_port}

network:
  allow_cidrs:
    - "127.0.0.0/8"

cache:
  enabled: true
  min_query_cost_ms: 0.0

logging:
  level: "info"
  format: "json"
  file: ""
""",
        encoding="utf-8",
    )


@pytest.fixture
def sync_cache_server(
    request: pytest.FixtureRequest, tmp_path: Path, mysql: MysqlClient
) -> Generator[tuple[MygramdbClient, str, str, str], None, None]:
    """Run an isolated daemon and MySQL table for cache invalidation regressions."""
    if not MYGRAMDB_BINARY.exists():
        pytest.fail(f"MygramDB binary not found at {MYGRAMDB_BINARY}")

    table = f"e2e_cache_sync_{uuid.uuid4().hex}"
    old_marker = f"oldmarker{uuid.uuid4().hex}"
    new_marker = f"newmarker{uuid.uuid4().hex}"
    qualified_table = f"{mysql.config['database']}.{table}"
    table_created = False
    process: subprocess.Popen | None = None
    log_file = None
    try:
        mysql.execute(
            f"CREATE TABLE `{table}` (id BIGINT UNSIGNED NOT NULL "
            "PRIMARY KEY, content TEXT NOT NULL) "
            "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci"
        )
        table_created = True
        mysql.execute(
            f"INSERT INTO `{table}` (id, content) VALUES (%s, %s)",
            (1, f"document {old_marker}"),
        )

        tcp_port, http_port = _free_tcp_http_ports()
        dump_dir = tmp_path / "dumps"
        dump_dir.mkdir()
        config_path = tmp_path / "mygramdb-cache-sync.yaml"
        replication_enabled = bool(getattr(request, "param", False))
        _write_sync_cache_config(
            config_path,
            mysql,
            table,
            tcp_port,
            http_port,
            dump_dir,
            replication_enabled=replication_enabled,
            auto_initial_snapshot=replication_enabled,
        )

        process_env = os.environ.copy()
        for name in (
            "MYGRAM_MYSQL_HOST",
            "MYGRAM_MYSQL_PORT",
            "MYGRAM_MYSQL_USER",
            "MYGRAM_MYSQL_PASSWORD",
            "MYGRAM_MYSQL_DATABASE",
        ):
            process_env.pop(name, None)

        log_file = (tmp_path / "mygramdb-cache-sync.log").open("w", encoding="utf-8")
        process = subprocess.Popen(
            [str(MYGRAMDB_BINARY), "-c", str(config_path)],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            cwd=PROJECT_ROOT,
            env=process_env,
        )
        client = MygramdbClient("127.0.0.1", tcp_port=tcp_port, http_port=http_port)
        wait_until(
            client.ping,
            timeout=90,
            interval=0.5,
            description="dedicated cache-test MygramDB to accept connections",
        )
        yield client, qualified_table, old_marker, new_marker
    finally:
        try:
            if process is not None and process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
        finally:
            if log_file is not None:
                log_file.close()
            if table_created:
                mysql.execute(f"DROP TABLE IF EXISTS `{table}`")


class TestCacheInvalidation:
    """Test that cache is invalidated when data changes."""

    @pytest.mark.timeout(300)
    @pytest.mark.dedicated_process
    @pytest.mark.parametrize("sync_cache_server", [True], indirect=True)
    def test_cold_start_binlog_event_invalidates_warmed_search(self, mysql, sync_cache_server):
        """Cold-start replication must invalidate a result warmed before a row event."""
        client, table, old_marker, new_marker = sync_cache_server
        wait_until(
            client.health_ready,
            timeout=90,
            interval=0.5,
            description="cold-start snapshot readiness",
        )

        hits_before = read_counter(client, CACHE_HITS)
        original = client.search(table, old_marker, limit=10)
        hits_after_first_search = read_counter(client, CACHE_HITS)
        repeated = client.search(table, old_marker, limit=10)
        hits_after_repeat = read_counter(client, CACHE_HITS)
        assert original["ids"] == [1]
        assert repeated["ids"] == [1]
        assert hits_after_first_search == hits_before, (
            "the first SEARCH should miss the empty cache"
        )
        assert hits_after_repeat > hits_after_first_search, (
            "the repeated SEARCH must register an actual cache hit"
        )

        mysql.execute(
            f"UPDATE `{table.split('.', 1)[1]}` SET content = %s WHERE id = %s",
            (f"document {new_marker}", 1),
        )
        first_cleared_search: dict[str, int] = {}

        def old_result_was_invalidated() -> bool:
            hits_before_search = read_counter(client, CACHE_HITS)
            old_result = client.search(table, old_marker, limit=10)
            hits_after_search = read_counter(client, CACHE_HITS)
            if old_result["ids"]:
                return False
            first_cleared_search["hit_delta"] = hits_after_search - hits_before_search
            return True

        wait_until(
            old_result_was_invalidated,
            timeout=30,
            interval=0.5,
            description="binlog event to invalidate the warmed old result",
        )
        assert first_cleared_search["hit_delta"] == 0, (
            "the first SEARCH that observes the row event must miss the invalidated cache entry"
        )
        updated = client.search(table, new_marker, limit=10)
        assert updated["ids"] == [1], "replication must publish the updated MySQL row"

    @pytest.mark.timeout(300)
    @pytest.mark.dedicated_process
    def test_sync_rebuild_invalidates_cached_search(self, mysql, sync_cache_server):
        """A successful manual SYNC must evict results cached before its swap."""
        client, table, old_marker, new_marker = sync_cache_server
        assert client.sync(table, timeout=90), "initial SYNC did not complete"

        # The dedicated process has not served queries yet, so its cache starts empty.
        hits_before = read_counter(client, CACHE_HITS)
        original = client.search(table, old_marker, limit=10)
        hits_after_first_search = read_counter(client, CACHE_HITS)
        repeated = client.search(table, old_marker, limit=10)
        hits_after_repeat = read_counter(client, CACHE_HITS)
        assert original["ids"] == [1]
        assert repeated["ids"] == [1]
        assert hits_after_first_search == hits_before, (
            "the first SEARCH should miss the empty cache"
        )
        assert hits_after_repeat > hits_after_first_search >= hits_before, (
            "the repeated SEARCH must register an actual cache hit"
        )

        # Replication is disabled for this daemon, so only SYNC can publish the
        # changed row or invalidate the cached result.
        mysql.execute(
            f"UPDATE `{table.split('.', 1)[1]}` SET content = %s WHERE id = %s",
            (f"document {new_marker}", 1),
        )
        stale = client.search(table, old_marker, limit=10)
        assert stale["ids"] == [1], "pre-SYNC SEARCH should still be served from its cached result"
        assert read_counter(client, CACHE_HITS) > hits_after_repeat, (
            "the pre-SYNC SEARCH must hit the existing cached result"
        )

        assert client.sync(table, timeout=90), "rebuild SYNC did not complete"
        hits_before_post_sync_search = read_counter(client, CACHE_HITS)
        old_result = client.search(table, old_marker, limit=10)
        assert old_result["ids"] == [], "SYNC must evict the old cached result"
        assert read_counter(client, CACHE_HITS) == hits_before_post_sync_search, (
            "the first post-SYNC SEARCH must miss the pre-SYNC cache entry"
        )
        new_result = client.search(table, new_marker, limit=10)
        assert new_result["ids"] == [1], "SYNC must publish the updated MySQL row"

    def test_insert_invalidates_cache(self, mysql, mygramdb, seed_data):
        """INSERT should invalidate the cached SEARCH result for a matching query."""
        table = f"{mysql.config['database']}.articles"
        marker = "cache_inval_marker"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Cache Inval",
                    "content": f"Content with {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count(table, marker),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="cache invalidation data",
        )

        # Warm the cache and confirm the repeated SEARCH is served from it.
        mygramdb.cache_clear()
        hits_before = read_counter(mygramdb, CACHE_HITS)
        warmed = mygramdb.search(table, marker, limit=100)
        repeated = mygramdb.search(table, marker, limit=100)
        assert repeated["total"] == warmed["total"]
        assert read_counter(mygramdb, CACHE_HITS) > hits_before, (
            "the repeated SEARCH must register an actual cache hit"
        )
        invalidations_before = read_counter(mygramdb, CACHE_INVALIDATIONS_IMMEDIATE)

        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Cache Inval 2",
                    "content": f"Another content with {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        # Re-running the warmed SEARCH must eventually observe the new row, which
        # a stale cached result can never return.
        wait_until(
            lambda: mygramdb.search(table, marker, limit=100)["total"] == warmed["total"] + 1,
            timeout=10,
            interval=0.5,
            description="warmed SEARCH to reflect the inserted row",
        )
        assert read_counter(mygramdb, CACHE_INVALIDATIONS_IMMEDIATE) > invalidations_before, (
            "the insert must have invalidated the cached entry"
        )

    def test_cache_clear_command(self, mysql, mygramdb, seed_data):
        """CACHE CLEAR should empty the cache so the next SEARCH misses."""
        table = f"{mysql.config['database']}.articles"
        mygramdb.cache_clear()
        mygramdb.search(table, "test", limit=10)
        hits_before = read_counter(mygramdb, CACHE_HITS)
        mygramdb.search(table, "test", limit=10)
        assert read_counter(mygramdb, CACHE_HITS) > hits_before, (
            "the repeated SEARCH must be served from the cache before CACHE CLEAR"
        )
        assert read_counter(mygramdb, CACHE_ENTRIES) > 0

        result = mygramdb.cache_clear()
        assert result, "CACHE CLEAR should succeed"
        assert read_counter(mygramdb, CACHE_ENTRIES) == 0, "CACHE CLEAR must drop every entry"

        hits_after_clear = read_counter(mygramdb, CACHE_HITS)
        mygramdb.search(table, "test", limit=10)
        assert read_counter(mygramdb, CACHE_HITS) == hits_after_clear, (
            "the first SEARCH after CACHE CLEAR must miss"
        )
        assert mygramdb.ping()
