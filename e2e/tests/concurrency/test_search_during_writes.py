"""Test search operations during concurrent writes."""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from lib.data_generator import DataGenerator

pytestmark = pytest.mark.concurrency


class TestSearchDuringWrites:
    """Test that search works correctly during write operations."""

    def test_search_during_bulk_insert(self, mysql, mygramdb, seed_data):
        """Search should work while bulk INSERT is happening."""
        gen = DataGenerator(seed=555)
        errors = []

        def insert_batch():
            """Insert a batch of rows."""
            try:
                rows = gen.generate_articles(count=100)
                mysql.insert_rows("articles", rows)
            except Exception as e:
                errors.append(f"insert: {e}")

        def search_repeatedly():
            """Search repeatedly during inserts."""
            try:
                for _ in range(20):
                    result = mygramdb.search("articles", "test", limit=10)
                    assert isinstance(result, dict)
                    time.sleep(0.1)
            except Exception as e:
                errors.append(f"search: {e}")

        with ThreadPoolExecutor(max_workers=12) as executor:
            # 2 insert threads + 10 search threads
            futures = []
            for _ in range(2):
                futures.append(executor.submit(insert_batch))
            for _ in range(10):
                futures.append(executor.submit(search_repeatedly))

            for f in as_completed(futures):
                f.result()

        assert not errors, f"Errors during concurrent operations: {errors}"
