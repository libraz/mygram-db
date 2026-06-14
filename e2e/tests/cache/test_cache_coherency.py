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
from lib.wait import wait_until_gte


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
        time.sleep(2)  # Allow replication lag
        stop_event.set()
        for t in search_threads:
            t.join(timeout=10)

        # Sync and verify final state
        mygramdb.sync("testdb.articles", timeout=15)
        final_count = mygramdb.count("testdb.articles", marker)

        # Final count should reflect all inserts (eventually consistent)
        assert final_count >= insert_count * 0.8, (
            f"Expected ~{insert_count} results, got {final_count}"
        )
        assert not errors, f"Search errors: {errors}"

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

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=5,
            timeout=15,
            interval=0.5,
            description=f"initial {marker} sync",
        )

        # Cache the search result
        result1 = mygramdb.count("testdb.articles", f"{marker} original")
        assert result1 >= 5

        # Update content via MySQL
        new_marker = f"updated_{uuid.uuid4().hex[:8]}"
        mysql.execute(
            f"UPDATE articles SET content = REPLACE(content, '{marker}', '{new_marker}') "
            f"WHERE content LIKE '%{marker}%'"
        )
        mygramdb.sync("testdb.articles", timeout=15)
        time.sleep(2)  # Allow replication and cache invalidation

        # Search with new content
        result2 = mygramdb.count("testdb.articles", new_marker)
        assert result2 >= 3, f"Expected updated content in results, got count={result2}"

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

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=10,
            timeout=15,
            interval=0.5,
            description=f"{marker} sync",
        )

        # Cache the result
        initial_count = mygramdb.count("testdb.articles", marker)
        assert initial_count >= 10

        # Delete via MySQL
        mysql.delete("articles", f"content LIKE '%{marker}%'")
        mygramdb.sync("testdb.articles", timeout=15)
        time.sleep(2)

        # Eventually the count should drop
        final_count = mygramdb.count("testdb.articles", marker)
        assert final_count < initial_count, (
            f"Expected count to decrease after DELETE: initial={initial_count}, final={final_count}"
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
                    mygramdb.tcp_command("SEARCH testdb.articles test")
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

        time.sleep(5)
        stop_event.set()
        for t in threads:
            t.join(timeout=10)

        assert mygramdb.ping(), "Server unresponsive after concurrent cache clear + search"
        # Some errors from race conditions are tolerable, but should be minimal
        assert len(errors) < 10, f"Too many errors ({len(errors)}): {errors[:5]}"
