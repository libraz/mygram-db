"""Test query and command statistics completeness."""

import time
import uuid

import pytest

from lib.metrics import MetricsSnapshot
from lib.wait import wait_until_gte

pytestmark = pytest.mark.statistics


class TestQueryStatsCompleteness:
    """Verify query and command counters not covered by existing tests."""

    def test_info_counter(self, mygramdb, seed_data):
        """INFO command counter should increase."""
        before = MetricsSnapshot.capture(mygramdb)

        n = 10
        for _ in range(n):
            mygramdb.info()

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        info_metrics = {
            k: v for k, v in diff.items() if "info" in k.lower() and "command" in k.lower()
        }
        if info_metrics:
            max_increase = max(info_metrics.values())
            assert max_increase >= n, f"INFO counter increased by {max_increase}, expected >= {n}"

    def test_replication_status_counter(self, mygramdb, seed_data):
        """REPLICATION STATUS command counter should increase."""
        before = MetricsSnapshot.capture(mygramdb)

        n = 5
        for _ in range(n):
            mygramdb.tcp_command("REPLICATION STATUS")

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        repl_metrics = {k: v for k, v in diff.items() if "replication_status" in k.lower()}
        if repl_metrics:
            max_increase = max(repl_metrics.values())
            assert max_increase >= n, (
                f"REPLICATION STATUS counter increased by {max_increase}, expected >= {n}"
            )

    def test_total_commands_processed(self, mygramdb, seed_data):
        """total_commands_processed should increase with mixed commands."""
        info_before = mygramdb.info()
        before_total = info_before.get("total_commands_processed", 0)

        n = 10
        for i in range(n):
            if i % 3 == 0:
                mygramdb.info()
            elif i % 3 == 1:
                mygramdb.search("articles", "test", limit=1)
            else:
                mygramdb.count("articles", "test")

        info_after = mygramdb.info()
        after_total = info_after.get("total_commands_processed", 0)

        # The INFO calls themselves also count, so total should increase by at least n
        increase = after_total - before_total
        assert increase >= n, f"total_commands_processed increased by {increase}, expected >= {n}"

    def test_total_connections_received(self, mygramdb, seed_data):
        """total_connections_received should increase with new TCP connections."""
        info_before = mygramdb.info()
        before_conns = info_before.get("total_connections_received", 0)

        n = 5
        for _ in range(n):
            mygramdb.tcp_command("INFO")

        info_after = mygramdb.info()
        after_conns = info_after.get("total_connections_received", 0)

        increase = after_conns - before_conns
        # Each tcp_command opens a new connection
        assert increase >= n, f"total_connections_received increased by {increase}, expected >= {n}"

    def test_total_requests(self, mygramdb, seed_data):
        """total_requests should increase with commands."""
        info_before = mygramdb.info()
        # Try both possible field names
        before_reqs = info_before.get(
            "total_requests", info_before.get("total_commands_processed", 0)
        )

        n = 8
        for _ in range(n):
            mygramdb.search("articles", "test", limit=1)

        info_after = mygramdb.info()
        after_reqs = info_after.get("total_requests", info_after.get("total_commands_processed", 0))

        increase = after_reqs - before_reqs
        assert increase >= n, f"total_requests increased by {increase}, expected >= {n}"

    def test_cache_invalidation_on_insert(self, mysql, mygramdb, seed_data, clear_cache):
        """Cache invalidation counter should increase on INSERT."""
        # Prime the cache
        mygramdb.search("articles", "test", limit=10)
        time.sleep(0.5)

        before = MetricsSnapshot.capture(mygramdb)

        marker = f"cacheinv_ins_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Cache Invalidation Insert",
                    "content": f"Content with {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="cache invalidation insert data",
        )

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        inv_metrics = {k: v for k, v in diff.items() if "invalidat" in k.lower()}
        if inv_metrics:
            total_inv = sum(v for v in inv_metrics.values() if v > 0)
            assert total_inv >= 1, (
                f"Cache invalidation counter should increase on INSERT, got {inv_metrics}"
            )

    def test_cache_invalidation_on_update(self, mysql, mygramdb, seed_data, clear_cache):
        """Cache invalidation counter should increase on UPDATE."""
        marker = f"cacheinv_upd_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Cache Invalidation Update",
                    "content": f"Content with {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="cache invalidation update seed",
        )

        # Prime cache
        mygramdb.search("articles", marker, limit=10)
        time.sleep(0.5)

        before = MetricsSnapshot.capture(mygramdb)

        mysql.update(
            "articles", f"content = 'Updated {marker} content'", f"content LIKE '%{marker}%'"
        )

        # Wait for replication
        time.sleep(3)

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        inv_metrics = {k: v for k, v in diff.items() if "invalidat" in k.lower()}
        if inv_metrics:
            total_inv = sum(v for v in inv_metrics.values() if v > 0)
            assert total_inv >= 1, (
                f"Cache invalidation counter should increase on UPDATE, got {inv_metrics}"
            )

    def test_cache_invalidation_on_delete(self, mysql, mygramdb, seed_data, clear_cache):
        """Cache invalidation counter should increase on DELETE."""
        marker = f"cacheinv_del_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Cache Invalidation Delete",
                    "content": f"Content with {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="cache invalidation delete seed",
        )

        # Prime cache
        mygramdb.search("articles", marker, limit=10)
        time.sleep(0.5)

        before = MetricsSnapshot.capture(mygramdb)

        mysql.delete("articles", f"content LIKE '%{marker}%'")

        # Wait for replication
        time.sleep(3)

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        inv_metrics = {k: v for k, v in diff.items() if "invalidat" in k.lower()}
        if inv_metrics:
            total_inv = sum(v for v in inv_metrics.values() if v > 0)
            assert total_inv >= 1, (
                f"Cache invalidation counter should increase on DELETE, got {inv_metrics}"
            )

    def test_cache_hit_rate_sanity(self, mygramdb, seed_data, clear_cache):
        """Cache hit rate should be between 0.0 and 1.0."""
        # Execute some searches to populate cache
        for _ in range(5):
            mygramdb.search("articles", "test", limit=10)

        snapshot = MetricsSnapshot.capture(mygramdb)

        hit_rate_metrics = snapshot.get_matching(r"cache_hit_rate")
        for name, value in hit_rate_metrics.items():
            assert 0.0 <= value <= 1.0, f"Cache hit rate {name} = {value}, expected 0.0-1.0"

        entries_metrics = snapshot.get_matching(r"cache_entries")
        for name, value in entries_metrics.items():
            assert value >= 0, f"Cache entries {name} = {value}, expected >= 0"

    def test_cache_memory_sanity(self, mygramdb, seed_data, clear_cache):
        """Cache memory bytes should be non-negative after searches."""
        # Execute searches to populate cache
        for _ in range(3):
            mygramdb.search("articles", "test", limit=10)

        snapshot = MetricsSnapshot.capture(mygramdb)

        mem_metrics = snapshot.get_matching(r"cache_memory")
        for name, value in mem_metrics.items():
            assert value >= 0, f"Cache memory {name} = {value}, expected >= 0"
