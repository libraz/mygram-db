"""DDL + DML concurrency tests.

Verify server stability when schema changes (TRUNCATE, ALTER TABLE,
bulk DELETE) occur while searches are actively running.
"""

from __future__ import annotations

import contextlib
import threading
import time
import uuid

import pytest

from lib.data_generator import DataGenerator
from lib.wait import wait_until, wait_until_gte, wait_until_value


def _await_searcher_running(observations: list, *, minimum: int = 3) -> None:
    """Block until the background reader has actually issued queries.

    The mutation under test has to land while reads are in flight; starting it
    on a timer would let it run before the thread was scheduled, and the test
    would then be exercising nothing.
    """
    wait_until(
        lambda: len(observations) >= minimum,
        timeout=20,
        interval=0.05,
        description="the background searcher to start issuing queries",
    )


@pytest.mark.concurrency
class TestDDLDuringQueries:
    """Tests for DDL/DML operations concurrent with searches."""

    def test_search_during_truncate(self, mysql, mygramdb, seed_data):
        """TRUNCATE TABLE while searches are running — should converge to 0 results."""
        marker = f"trunctest_{uuid.uuid4().hex[:8]}"

        # Insert identifiable rows
        DataGenerator(seed=77)
        rows = [
            {
                "title": f"{marker} row {i}",
                "content": f"{marker} content for truncate test item {i}",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
            for i in range(20)
        ]
        mysql.insert_rows("articles", rows)
        mygramdb.sync("testdb.articles", timeout=30)

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=15,
            timeout=30,
            interval=0.5,
            description=f"{marker} sync",
        )

        errors: list[str] = []
        stop_event = threading.Event()
        search_counts: list[int] = []

        def _searcher():
            while not stop_event.is_set():
                try:
                    c = mygramdb.count("testdb.articles", marker)
                    search_counts.append(c)
                except Exception as e:
                    errors.append(str(e))
                time.sleep(0.1)

        # Start search thread
        search_thread = threading.Thread(target=_searcher)
        search_thread.start()
        _await_searcher_running(search_counts)

        # Truncate and re-seed (to keep other tests working)
        mysql.truncate("articles")
        mygramdb.sync("testdb.articles", timeout=15)
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=0,
            timeout=30,
            interval=0.5,
            description="the truncate to empty the marker result set",
        )

        stop_event.set()
        search_thread.join(timeout=10)

        # Re-seed for subsequent tests
        gen2 = DataGenerator(seed=42)
        new_rows = gen2.generate_articles(count=100)
        mysql.insert_rows("articles", new_rows)
        mygramdb.sync("testdb.articles", timeout=15)

        # Verify search count converged toward 0 for our marker
        assert mygramdb.ping(), "Server unresponsive after truncate"
        final_count = mygramdb.count("testdb.articles", marker)
        assert final_count == 0, f"Expected 0 after truncate, got {final_count}"

    def test_add_column_during_search(self, mysql, mygramdb, seed_data):
        """ALTER TABLE ADD COLUMN while searches are running."""
        col_name = f"extra_{uuid.uuid4().hex[:6]}"
        errors: list[str] = []
        stop_event = threading.Event()
        observations: list[str | None] = []

        def _searcher():
            while not stop_event.is_set():
                try:
                    observations.append(mygramdb.tcp_command("SEARCH testdb.articles test"))
                except Exception as e:
                    errors.append(str(e))
                time.sleep(0.1)

        search_thread = threading.Thread(target=_searcher)
        search_thread.start()
        _await_searcher_running(observations)

        # Add a column (may already exist or DDL may fail — that's OK)
        with contextlib.suppress(Exception):
            mysql.execute(f"ALTER TABLE articles ADD COLUMN {col_name} VARCHAR(50) DEFAULT NULL")

        mygramdb.sync("testdb.articles", timeout=15)
        # The searcher has to keep reading after the schema change, or a break
        # introduced by the new column would go unobserved.
        settled = len(observations)
        _await_searcher_running(observations, minimum=settled + 3)

        stop_event.set()
        search_thread.join(timeout=10)

        assert mygramdb.ping(), "Server unresponsive after ALTER TABLE"
        assert not errors, f"Search errors during ALTER: {errors[:5]}"

        # Cleanup: drop the added column
        with contextlib.suppress(Exception):
            mysql.execute(f"ALTER TABLE articles DROP COLUMN {col_name}")

    def test_bulk_delete_during_search(self, mysql, mygramdb, seed_data):
        """DELETE 80% of rows while searches run — results should decrease."""
        marker = f"bulkdel_{uuid.uuid4().hex[:8]}"

        # Insert rows for this test
        rows = [
            {
                "title": f"{marker} item {i}",
                "content": f"{marker} bulk delete test content number {i}",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
            for i in range(50)
        ]
        mysql.insert_rows("articles", rows)
        mygramdb.sync("testdb.articles", timeout=15)

        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=50,
            timeout=15,
            interval=0.5,
            description=f"{marker} sync",
        )

        errors: list[str] = []
        stop_event = threading.Event()
        search_counts: list[int] = []

        def _searcher():
            while not stop_event.is_set():
                try:
                    c = mygramdb.count("testdb.articles", marker)
                    search_counts.append(c)
                except Exception as e:
                    errors.append(str(e))
                time.sleep(0.1)

        search_thread = threading.Thread(target=_searcher)
        search_thread.start()
        _await_searcher_running(search_counts)

        # Delete 80% of our test rows
        mysql.delete("articles", f"content LIKE '%{marker}%' ORDER BY id LIMIT 40")
        mygramdb.sync("testdb.articles", timeout=15)
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=10,
            timeout=15,
            interval=0.5,
            description=f"exact {marker} count after deleting 40/50 rows",
        )

        stop_event.set()
        search_thread.join(timeout=10)

        # The concurrent reader's observations are the point of running it.
        # This test issues an explicit SYNC, and a table being rebuilt refuses
        # reads with a dedicated message rather than serving a half-built
        # index — that rejection is the contract, so it is the one failure mode
        # allowed here. Any other error means a read path broke under
        # concurrent mutation.
        assert search_counts or errors, "the concurrent searcher recorded nothing"
        unexpected = [error for error in errors if "synchronizing" not in error]
        assert not unexpected, (
            f"searches failed for reasons other than an in-progress sync: {unexpected[:5]}"
        )
