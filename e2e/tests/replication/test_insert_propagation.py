"""Test INSERT event propagation from MySQL to MygramDB."""

import uuid

import pytest

from lib.wait import wait_until_gte

pytestmark = pytest.mark.replication


class TestInsertPropagation:
    """Test that INSERTs in MySQL propagate to MygramDB."""

    def test_single_insert(self, mysql, mygramdb, seed_data):
        """Single INSERT should be reflected in MygramDB."""
        marker = f"prop_{uuid.uuid4().hex[:8]}"
        before_count = mygramdb.count("testdb.articles", marker)

        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Propagation Test",
                    "content": f"This contains {marker} for testing",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=before_count + 1,
            timeout=20,
            interval=0.5,
            description="INSERT propagation",
        )

    def test_multiple_inserts(self, mysql, mygramdb, seed_data):
        """Multiple INSERTs should all propagate."""
        marker = f"batchins_{uuid.uuid4().hex[:8]}"
        rows = [
            {
                "title": f"Batch {i}",
                "content": f"Content with {marker} number {i}",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
            for i in range(10)
        ]
        mysql.insert_rows("articles", rows)

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=10,
            timeout=20,
            interval=0.5,
            description="batch INSERT propagation",
        )
