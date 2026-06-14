"""Test rapid mutation scenarios."""

import time

import pytest

pytestmark = pytest.mark.concurrency


class TestRapidMutations:
    """Test rapid update and churn scenarios."""

    def test_rapid_updates_same_row(self, mysql, mygramdb, seed_data):
        """Rapidly updating the same row should not cause corruption."""
        marker = "rapid_update_target"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Rapid Update Target",
                    "content": f"Original content with {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        time.sleep(2)

        # Rapidly update the same row
        for i in range(20):
            mysql.update(
                "articles",
                f"content = 'Updated content {marker} version {i}'",
                "title = 'Rapid Update Target'",
            )

        time.sleep(5)
        assert mygramdb.ping(), "MygramDB should still be responsive"

    def test_insert_delete_churn(self, mysql, mygramdb, seed_data):
        """Rapid insert+delete cycles should not corrupt the index."""
        for i in range(10):
            mysql.insert_rows(
                "articles",
                [
                    {
                        "title": f"Churn {i}",
                        "content": f"churn_cycle_marker content {i}",
                        "status": 1,
                        "category": "tech",
                        "enabled": 1,
                    }
                ],
            )
            mysql.delete("articles", f"title = 'Churn {i}'")

        time.sleep(5)
        # Index should be clean
        count = mygramdb.count("testdb.articles", "churn_cycle_marker")
        assert count == 0, f"Expected 0 churned docs, got {count}"
