"""Test batch and rapid operations for replication correctness."""

import uuid

import pytest

from lib.wait import wait_until_gte
from lib.data_generator import DataGenerator

pytestmark = pytest.mark.replication


class TestBatchOperations:
    """Test batch and edge-case replication scenarios."""

    def test_batch_insert_1000(self, mysql, mygramdb, seed_data):
        """Batch INSERT of 1000 rows should all propagate."""
        gen = DataGenerator(seed=999)
        marker = f"batch1k_{uuid.uuid4().hex[:8]}"
        rows = []
        for i in range(1000):
            rows.append({
                "title": f"Batch1000 #{i}",
                "content": f"Content {marker} item number {i} {gen._random_sentence(5, 15)}",
                "status": gen.rng.choice([1, 2, 3]),
                "category": gen.rng.choice(DataGenerator.CATEGORIES),
                "enabled": 1,
            })
        mysql.insert_rows("articles", rows)

        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=1000,
            timeout=30,
            interval=1,
            description="batch 1000 INSERT propagation",
        )

    def test_insert_then_immediate_delete(self, mysql, mygramdb, seed_data):
        """INSERT followed immediately by DELETE should result in no document."""
        marker = f"churn_{uuid.uuid4().hex[:8]}"

        # Insert and immediately delete
        mysql.insert_rows("articles", [{
            "title": "Churn Test",
            "content": f"Content with {marker}",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])
        mysql.delete("articles", f"content LIKE '%{marker}%'")

        # Wait for events to process
        import time
        time.sleep(5)

        # Should have 0 documents with this marker
        count = mygramdb.count("articles", marker)
        assert count == 0, f"Expected 0 documents after insert+delete, got {count}"
