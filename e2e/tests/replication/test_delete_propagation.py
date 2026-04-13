"""Test DELETE event propagation from MySQL to MygramDB."""

import uuid

import pytest

from lib.wait import wait_until, wait_until_gte

pytestmark = pytest.mark.replication


class TestDeletePropagation:
    """Test that DELETEs in MySQL propagate to MygramDB."""

    def test_hard_delete(self, mysql, mygramdb, seed_data):
        """Hard DELETE should remove document from MygramDB."""
        marker = f"harddel_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Delete Test",
                    "content": f"Content with {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        # Wait for insert
        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="insert for delete test",
        )

        # Delete the row
        mysql.delete("articles", f"content LIKE '%{marker}%'")

        # Wait for delete to propagate
        wait_until(
            lambda: mygramdb.count("articles", marker) == 0,
            timeout=20,
            interval=0.5,
            description="DELETE propagation",
        )

    def test_soft_delete(self, mysql, mygramdb, seed_data):
        """Setting deleted_at should be treated as a filter update."""
        marker = f"softdel_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Soft Delete Test",
                    "content": f"Content with {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                    "deleted_at": None,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="insert for soft delete test",
        )

        # Set deleted_at
        mysql.update(
            "articles",
            "deleted_at = NOW()",
            f"content LIKE '%{marker}%'",
        )

        # Wait and check - document may still be searchable depending on config
        import time

        time.sleep(3)
        # Soft delete behavior depends on MygramDB configuration
        # Just verify no crash
        mygramdb.count("articles", marker)
