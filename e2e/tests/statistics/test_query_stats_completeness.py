"""Test query and command statistics completeness."""

from __future__ import annotations

import uuid

import pytest

from lib.metrics import (
    CACHE_ENTRIES,
    CACHE_HIT_RATE,
    CACHE_HITS,
    CACHE_INVALIDATIONS_DEFERRED,
    CACHE_INVALIDATIONS_IMMEDIATE,
    CACHE_MEMORY_CACHE,
    CACHE_MISSES_INVALIDATED,
    CACHE_MISSES_NOT_FOUND,
    CACHE_MISSES_TTL_EXPIRED,
    CLIENTS_TOTAL,
    SERVER_COMMANDS_TOTAL,
    MetricsSnapshot,
    command_total,
    read_counter,
)
from lib.wait import wait_until_gte, wait_until_value

pytestmark = pytest.mark.statistics

# The per-command counters that the server sums into its command total. The
# aggregate is maintained by hand, so this list is what detects a new counter
# being exported without being added to the sum.
COMMANDS = (
    "search",
    "count",
    "get",
    "info",
    "save",
    "load",
    "replication_status",
    "replication_stop",
    "replication_start",
    "config",
    "other",
    "unknown",
)

CACHE_MISS_REASONS = (
    CACHE_MISSES_NOT_FOUND,
    CACHE_MISSES_TTL_EXPIRED,
    CACHE_MISSES_INVALIDATED,
)


def _require(snapshot: MetricsSnapshot, names) -> dict[str, float]:
    """Read the named metrics, failing when the endpoint omits any of them."""
    missing = [name for name in names if name not in snapshot.data]
    if missing:
        raise AssertionError(f"/metrics does not export: {missing}")
    return {name: snapshot.data[name] for name in names}


def _command_counters(mygramdb) -> dict[str, float]:
    """Read the command buckets alongside the aggregate they must sum to.

    A bucket at zero is deliberately not exported, to keep the label
    cardinality of the endpoint down, so an absent bucket reads as zero here.
    The aggregate is always exported and stays required.
    """
    snapshot = MetricsSnapshot.capture(mygramdb)
    counters = _require(snapshot, (SERVER_COMMANDS_TOTAL,))
    for command in COMMANDS:
        key = command_total(command)
        counters[key] = snapshot.data.get(key, 0.0)
    return counters


def _cache_invalidations(mygramdb) -> float:
    """Total invalidations across the phases an event can be retired in."""
    snapshot = MetricsSnapshot.capture(mygramdb)
    values = _require(snapshot, (CACHE_INVALIDATIONS_IMMEDIATE, CACHE_INVALIDATIONS_DEFERRED))
    return sum(values.values())


def _insert_marked_row(mysql, marker: str) -> None:
    mysql.insert_rows(
        "articles",
        [
            {
                "title": "Cache Invalidation Probe",
                "content": f"probe content {marker}",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
        ],
    )


class TestQueryStatsCompleteness:
    """Verify query, connection, and cache counters.

    Each command opens its own connection and is classified into exactly one
    bucket, so these counters can be checked for an exact delta rather than for
    mere movement. An off-by-one or a command landing in the wrong bucket is the
    realistic defect, and only exact arithmetic catches it.
    """

    def test_info_command_is_counted_once_per_call(self, mygramdb, seed_data):
        """N INFO commands advance the info bucket by exactly N, and nothing else."""
        before = _command_counters(mygramdb)

        calls = 10
        for _ in range(calls):
            mygramdb.info()

        after = _command_counters(mygramdb)
        assert after[command_total("info")] - before[command_total("info")] == calls, (
            "the info command counter did not advance once per INFO"
        )
        for command in COMMANDS:
            if command == "info":
                continue
            key = command_total(command)
            assert after[key] == before[key], f"INFO was also counted as {command}"

    def test_replication_status_is_counted_in_its_own_bucket(self, mygramdb, seed_data):
        """REPLICATION STATUS has a dedicated counter and does not fall through."""
        before = _command_counters(mygramdb)

        calls = 5
        for _ in range(calls):
            assert mygramdb.tcp_command("REPLICATION STATUS") is not None

        after = _command_counters(mygramdb)
        key = command_total("replication_status")
        assert after[key] - before[key] == calls, (
            "REPLICATION STATUS did not advance its own counter once per call"
        )
        assert after[command_total("other")] == before[command_total("other")], (
            "REPLICATION STATUS fell through to the catch-all bucket"
        )
        assert after[command_total("unknown")] == before[command_total("unknown")], (
            "REPLICATION STATUS was rejected as an unknown command"
        )

    def test_command_total_is_the_sum_of_its_buckets(self, mygramdb, seed_data):
        """The aggregate command counter equals the sum of the per-command ones.

        The aggregate is maintained separately from the buckets, so a command
        added to one and not the other would leave the total permanently
        understated. Comparing the two deltas is what detects that drift.
        """
        before = _command_counters(mygramdb)

        for index in range(9):
            if index % 3 == 0:
                mygramdb.info()
            elif index % 3 == 1:
                mygramdb.search("testdb.articles", "test", limit=1)
            else:
                mygramdb.count("testdb.articles", "test")

        after = _command_counters(mygramdb)
        bucket_delta = sum(
            after[command_total(command)] - before[command_total(command)] for command in COMMANDS
        )
        total_delta = after[SERVER_COMMANDS_TOTAL] - before[SERVER_COMMANDS_TOTAL]
        assert bucket_delta == 9, f"expected 9 counted commands, buckets moved by {bucket_delta}"
        assert total_delta == bucket_delta, (
            f"the command total moved by {total_delta} while its buckets moved by {bucket_delta}"
        )

    def test_every_command_connection_is_counted(self, mygramdb, seed_data):
        """The connection counter advances once per TCP command.

        The client opens and closes a socket per command, so an exact delta
        holds. A connection counted twice, or not at all, would misreport load
        in exactly the way an operator would act on.
        """
        before = read_counter(mygramdb, CLIENTS_TOTAL)
        before_info = read_counter(mygramdb, command_total("info"))

        calls = 5
        for _ in range(calls):
            assert mygramdb.tcp_command("INFO") is not None

        assert read_counter(mygramdb, command_total("info")) - before_info == calls, (
            "the commands under test were not all processed"
        )
        assert read_counter(mygramdb, CLIENTS_TOTAL) - before == calls, (
            "the connection counter did not advance once per connection"
        )

    def test_insert_invalidates_the_cached_result(self, mysql, mygramdb, seed_data, clear_cache):
        """An INSERT retires the cached result set that it would change.

        The cache only stores answers that matched something, so the query is
        primed against an existing row and the second row is what must make the
        cached answer wrong.
        """
        marker = f"cacheins{uuid.uuid4().hex[:8]}"
        _insert_marked_row(mysql, marker)
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=1,
            timeout=20,
            interval=0.5,
            description="the first row to be indexed",
        )

        # Prime: this answer of 1 is what the second insert must invalidate.
        assert mygramdb.count("testdb.articles", marker) == 1
        before = _cache_invalidations(mygramdb)

        _insert_marked_row(mysql, marker)
        wait_until_gte(
            lambda: _cache_invalidations(mygramdb) - before,
            minimum=1,
            timeout=20,
            interval=0.25,
            description="the INSERT to invalidate cached results",
        )

        # The counter is only meaningful if the stale answer is actually gone.
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=2,
            timeout=20,
            interval=0.5,
            description="the cached answer to be replaced by the up-to-date count",
        )

    def test_update_invalidates_the_cached_result(self, mysql, mygramdb, seed_data, clear_cache):
        """An UPDATE retires the cached result set built from the old text."""
        marker = f"cacheupd{uuid.uuid4().hex[:8]}"
        _insert_marked_row(mysql, marker)
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=1,
            timeout=20,
            interval=0.5,
            description="the probe row to be indexed",
        )

        rewritten = f"{marker}rewritten"
        assert mygramdb.count("testdb.articles", rewritten) == 0
        before = _cache_invalidations(mygramdb)

        mysql.update("articles", f"content = 'rewritten {rewritten}'", f"content LIKE '%{marker}%'")
        wait_until_gte(
            lambda: _cache_invalidations(mygramdb) - before,
            minimum=1,
            timeout=20,
            interval=0.25,
            description="the UPDATE to invalidate cached results",
        )
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", rewritten),
            expected=1,
            timeout=20,
            interval=0.5,
            description="the rewritten text to replace the cached answer",
        )

    def test_delete_invalidates_the_cached_result(self, mysql, mygramdb, seed_data, clear_cache):
        """A DELETE retires the cached result set that still contains the row."""
        marker = f"cachedel{uuid.uuid4().hex[:8]}"
        _insert_marked_row(mysql, marker)
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=1,
            timeout=20,
            interval=0.5,
            description="the probe row to be indexed",
        )

        # Prime the cache with the result that the delete must retire.
        assert mygramdb.count("testdb.articles", marker) == 1
        before = _cache_invalidations(mygramdb)

        mysql.delete("articles", f"content LIKE '%{marker}%'")
        wait_until_gte(
            lambda: _cache_invalidations(mygramdb) - before,
            minimum=1,
            timeout=20,
            interval=0.25,
            description="the DELETE to invalidate cached results",
        )
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=0,
            timeout=20,
            interval=0.5,
            description="the deleted row to disappear from the cached answer",
        )

    def test_cache_hit_rate_matches_its_hit_and_miss_counters(
        self, mysql, mygramdb, seed_data, clear_cache
    ):
        """The exported hit rate is derivable from the exported counters.

        A hit rate computed from a different pair of counters than the ones it
        is published beside is a reporting bug an operator cannot see, so the
        rate is recomputed here rather than merely range-checked.
        """
        # Only answers that matched something are stored, so the query needs a
        # row behind it before repeating it can produce hits.
        query = f"hitrate{uuid.uuid4().hex[:8]}"
        _insert_marked_row(mysql, query)
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", query),
            expected=1,
            timeout=20,
            interval=0.5,
            description="the probe row to be indexed",
        )
        for _ in range(5):
            mygramdb.count("testdb.articles", query)

        snapshot = MetricsSnapshot.capture(mygramdb)
        values = _require(
            snapshot,
            (CACHE_HITS, CACHE_ENTRIES, CACHE_HIT_RATE, CACHE_MEMORY_CACHE, *CACHE_MISS_REASONS),
        )

        hits = values[CACHE_HITS]
        misses = sum(values[reason] for reason in CACHE_MISS_REASONS)
        assert hits + misses > 0, "the searches above were not seen by the cache at all"

        expected_rate = hits / (hits + misses)
        # The gauge is published rounded to four decimals.
        assert abs(values[CACHE_HIT_RATE] - expected_rate) <= 5e-4, (
            f"hit rate {values[CACHE_HIT_RATE]} does not match "
            f"{hits} hits / {hits + misses} lookups"
        )
        assert values[CACHE_ENTRIES] >= 1, "no entry was retained after five cacheable searches"
        assert values[CACHE_MEMORY_CACHE] > 0, "cache entries are reported as using no memory"
