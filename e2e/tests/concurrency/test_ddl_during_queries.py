"""DDL + DML concurrency tests.

Verify server stability when schema changes (TRUNCATE, ALTER TABLE,
bulk DELETE) occur while searches are actively running.
"""

from __future__ import annotations

import threading
import time
import uuid

import pytest

from lib.data_generator import DataGenerator
from lib.wait import wait_until, wait_until_gte


@pytest.mark.concurrency
class TestDDLDuringQueries:
    """Tests for DDL/DML operations concurrent with searches."""

    def test_search_during_truncate(self, mysql, mygramdb, seed_data):
        """TRUNCATE TABLE while searches are running — should converge to 0 results."""
        marker = f"trunctest_{uuid.uuid4().hex[:8]}"

        # Insert identifiable rows
        gen = DataGenerator(seed=77)
        rows = [{
            "title": f"{marker} row {i}",
            "content": f"{marker} content for truncate test item {i}",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        } for i in range(20)]
        mysql.insert_rows("articles", rows)
        mygramdb.sync("articles", timeout=30)
        time.sleep(2)

        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
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
                    c = mygramdb.count("articles", marker)
                    search_counts.append(c)
                except Exception as e:
                    errors.append(str(e))
                time.sleep(0.1)

        # Start search thread
        search_thread = threading.Thread(target=_searcher)
        search_thread.start()

        time.sleep(0.5)

        # Truncate and re-seed (to keep other tests working)
        mysql.truncate("articles")
        mygramdb.sync("articles", timeout=15)
        time.sleep(3)

        stop_event.set()
        search_thread.join(timeout=10)

        # Re-seed for subsequent tests
        gen2 = DataGenerator(seed=42)
        new_rows = gen2.generate_articles(count=100)
        mysql.insert_rows("articles", new_rows)
        mygramdb.sync("articles", timeout=15)

        # Verify search count converged toward 0 for our marker
        assert mygramdb.ping(), "Server unresponsive after truncate"
        final_count = mygramdb.count("articles", marker)
        assert final_count == 0, f"Expected 0 after truncate, got {final_count}"

    def test_add_column_during_search(self, mysql, mygramdb, seed_data):
        """ALTER TABLE ADD COLUMN while searches are running."""
        col_name = f"extra_{uuid.uuid4().hex[:6]}"
        errors: list[str] = []
        stop_event = threading.Event()

        def _searcher():
            while not stop_event.is_set():
                try:
                    mygramdb.tcp_command("SEARCH articles test")
                except Exception as e:
                    errors.append(str(e))
                time.sleep(0.1)

        search_thread = threading.Thread(target=_searcher)
        search_thread.start()

        time.sleep(0.5)

        # Add a column
        try:
            mysql.execute(f"ALTER TABLE articles ADD COLUMN {col_name} VARCHAR(50) DEFAULT NULL")
        except Exception:
            pass  # Column may already exist or DDL may fail — that's OK

        mygramdb.sync("articles", timeout=15)
        time.sleep(2)

        stop_event.set()
        search_thread.join(timeout=10)

        assert mygramdb.ping(), "Server unresponsive after ALTER TABLE"
        assert not errors, f"Search errors during ALTER: {errors[:5]}"

        # Cleanup: drop the added column
        try:
            mysql.execute(f"ALTER TABLE articles DROP COLUMN {col_name}")
        except Exception:
            pass

    def test_bulk_delete_during_search(self, mysql, mygramdb, seed_data):
        """DELETE 80% of rows while searches run — results should decrease."""
        marker = f"bulkdel_{uuid.uuid4().hex[:8]}"

        # Insert rows for this test
        rows = [{
            "title": f"{marker} item {i}",
            "content": f"{marker} bulk delete test content number {i}",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        } for i in range(50)]
        mysql.insert_rows("articles", rows)
        mygramdb.sync("articles", timeout=15)

        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=40,
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
                    c = mygramdb.count("articles", marker)
                    search_counts.append(c)
                except Exception as e:
                    errors.append(str(e))
                time.sleep(0.1)

        search_thread = threading.Thread(target=_searcher)
        search_thread.start()

        time.sleep(0.5)

        # Delete 80% of our test rows
        mysql.delete("articles", f"content LIKE '%{marker}%' ORDER BY id LIMIT 40")
        mygramdb.sync("articles", timeout=15)
        time.sleep(3)

        stop_event.set()
        search_thread.join(timeout=10)

        assert mygramdb.ping(), "Server unresponsive after bulk delete"

        # Final count should reflect deletions
        final_count = mygramdb.count("articles", marker)
        assert final_count <= 15, (
            f"Expected <=15 results after deleting 40/50, got {final_count}"
        )
