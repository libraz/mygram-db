"""Test DELETE event propagation from MySQL to MygramDB."""

import uuid

import pytest

from lib.wait import wait_until, wait_until_gte, wait_until_value

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
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="insert for delete test",
        )

        # Delete the row
        mysql.delete("articles", f"content LIKE '%{marker}%'")

        # Wait for delete to propagate
        wait_until(
            lambda: mygramdb.count("testdb.articles", marker) == 0,
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
            lambda: mygramdb.count("testdb.articles", marker),
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

        # deleted_at is not a configured filter, so a soft delete is an ordinary
        # column change: the row stays indexed and searchable. Treating it as a
        # removal would silently drop rows for every deployment that soft-deletes.
        # A control row written afterwards is the barrier that proves the update
        # was applied before the assertion below runs.
        control = f"ctl{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Soft Delete Control",
                    "content": f"control after soft delete {control}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", control),
            expected=1,
            timeout=20,
            interval=0.5,
            description="the control row written after the soft delete",
        )
        assert mygramdb.count("testdb.articles", marker) == 1, (
            "setting an unconfigured column removed the row from the index"
        )
