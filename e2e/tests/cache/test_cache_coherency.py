"""Cache coherency and consistency tests.

Verify cache behavior under concurrent reads/writes, rapid invalidation,
and stale-data scenarios.
"""

from __future__ import annotations

import threading
import time
import uuid

import pytest

from lib.data_generator import DataGenerator
from lib.wait import wait_until_value


@pytest.mark.cache
class TestCacheCoherency:
    """Cache consistency tests under concurrent access."""

    def test_search_during_rapid_inserts(self, mysql, mygramdb, seed_data, clear_cache):
        """Concurrent inserts and searches — results should be eventually consistent."""
        marker = f"cachecoherent_{uuid.uuid4().hex[:8]}"
        insert_count = 20
        errors: list[str] = []
        stop_event = threading.Event()

        def _inserter():
            """Insert rows one by one with unique content."""
            DataGenerator(seed=99)
            for i in range(insert_count):
                if stop_event.is_set():
                    break
                mysql.insert_rows(
                    "articles",
                    [
                        {
                            "title": f"{marker} insert {i}",
                            "content": f"{marker} coherency content batch {i}",
                            "status": 1,
                            "category": "tech",
                            "enabled": 1,
                        }
                    ],
                )
                time.sleep(0.1)

        def _searcher(thread_id: int):
            """Search repeatedly, track result counts."""
            counts = []
            while not stop_event.is_set():
                try:
                    c = mygramdb.count("testdb.articles", marker)
                    counts.append(c)
                except Exception as e:
                    errors.append(f"searcher-{thread_id}: {e}")
                time.sleep(0.05)
            return counts

        # Start inserter
        insert_thread = threading.Thread(target=_inserter)
        insert_thread.start()

        # Start 5 search threads
        search_results: list[list[int]] = [[] for _ in range(5)]

        def _run_searcher(idx: int):
            search_results[idx] = _searcher(idx)

        search_threads = []
        for i in range(5):
            t = threading.Thread(target=_run_searcher, args=(i,))
            t.start()
            search_threads.append(t)

        # Wait for inserter to finish
        insert_thread.join(timeout=30)
        assert not insert_thread.is_alive(), "inserter did not finish"
        stop_event.set()
        for t in search_threads:
            t.join(timeout=10)

        assert not errors, f"Search errors: {errors}"

        # The concurrent reads are the point of running the searchers, so they
        # are checked rather than merely collected. The workload only inserts,
        # so every observation has to sit between an empty index and the full
        # set; anything outside that means a reader saw an index state that
        # was never written.
        observed = [count for counts in search_results for count in counts]
        assert observed, "no searcher thread recorded a result"
        assert all(0 <= count <= insert_count for count in observed), (
            f"a concurrent read reported a count outside [0, {insert_count}]: "
            f"{sorted(set(observed))}"
        )

        # Sync and verify final state
        mygramdb.sync("testdb.articles", timeout=15)
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=insert_count,
            timeout=15,
            interval=0.5,
            description=f"all {marker} inserts",
        )

    def test_cache_invalidation_under_updates(self, mysql, mygramdb, seed_data, clear_cache):
        """Cache a search result, UPDATE all matching rows, verify cache invalidated."""
        marker = f"cacheinval_{uuid.uuid4().hex[:8]}"

        # Insert specific rows
        rows = [
            {
                "title": f"{marker} original title {i}",
                "content": f"{marker} original content for cache invalidation test",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
            for i in range(5)
        ]
        mysql.insert_rows("articles", rows)
        mygramdb.sync("testdb.articles", timeout=15)

        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=5,
            timeout=15,
            interval=0.5,
            description=f"initial {marker} sync",
        )

        # Cache the search result
        cache_key = f"{marker} original"
        assert mygramdb.count("testdb.articles", cache_key) == 5

        # Update content via MySQL
        new_marker = f"updated_{uuid.uuid4().hex[:8]}"
        mysql.execute(
            f"UPDATE articles SET title = REPLACE(title, '{marker}', '{new_marker}'), "
            f"content = REPLACE(content, '{marker}', '{new_marker}') "
            f"WHERE title LIKE '%{marker}%'"
        )
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", cache_key),
            expected=0,
            timeout=15,
            interval=0.5,
            description=f"cached {cache_key} invalidation after UPDATE",
        )
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", new_marker),
            expected=5,
            timeout=15,
            interval=0.5,
            description=f"updated {new_marker} visibility",
        )

    def test_delete_then_search_cache_stale(self, mysql, mygramdb, seed_data, clear_cache):
        """Cache search result, DELETE rows, verify results decrease."""
        marker = f"cachestale_{uuid.uuid4().hex[:8]}"

        rows = [
            {
                "title": f"{marker} to delete {i}",
                "content": f"{marker} content that will be deleted soon",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
            for i in range(10)
        ]
        mysql.insert_rows("articles", rows)
        mygramdb.sync("testdb.articles", timeout=15)

        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=10,
            timeout=15,
            interval=0.5,
            description=f"{marker} sync",
        )

        # Cache the result
        assert mygramdb.count("testdb.articles", marker) == 10

        # Delete via MySQL
        mysql.delete("articles", f"content LIKE '%{marker}%'")
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=0,
            timeout=15,
            interval=0.5,
            description=f"cached {marker} invalidation after DELETE",
        )

    def test_concurrent_cache_clear_and_search(self, mygramdb, seed_data):
        """5 CACHE CLEAR threads + 10 SEARCH threads for 5 seconds — no crash."""
        errors: list[str] = []
        stop_event = threading.Event()

        def _cache_clearer(thread_id: int):
            while not stop_event.is_set():
                try:
                    mygramdb.cache_clear()
                except Exception as e:
                    errors.append(f"clearer-{thread_id}: {e}")
                time.sleep(0.2)

        def _searcher(thread_id: int):
            while not stop_event.is_set():
                try:
                    # search() rejects anything that is not a result frame, so a
                    # refusal caused by a concurrent clear is recorded here
                    # rather than passing for a well-formed empty answer.
                    mygramdb.search("testdb.articles", "test", limit=10)
                except Exception as e:
                    errors.append(f"searcher-{thread_id}: {e}")
                time.sleep(0.1)

        threads = []
        for i in range(5):
            t = threading.Thread(target=_cache_clearer, args=(i,))
            t.start()
            threads.append(t)
        for i in range(10):
            t = threading.Thread(target=_searcher, args=(i,))
            t.start()
            threads.append(t)

        # A fixed soak window, not a readiness wait: the point is to keep the
        # clear and read paths colliding for long enough to shake out a race.
        time.sleep(5)
        stop_event.set()
        for t in threads:
            t.join(timeout=10)

        assert mygramdb.ping(), "Server unresponsive after concurrent cache clear + search"
        # Clearing the cache is transparent to a reader: it may cost a lookup a
        # cache hit, never an answer. Any error here is a read path breaking
        # under a concurrent clear.
        assert not errors, f"{len(errors)} search or clear failures: {errors[:5]}"
