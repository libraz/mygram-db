"""Test UPDATE event propagation from MySQL to MygramDB."""

import uuid

import pytest

from lib.wait import wait_until_gte

pytestmark = pytest.mark.replication


class TestUpdatePropagation:
    """Test that UPDATEs in MySQL propagate to MygramDB."""

    def test_text_update(self, mysql, mygramdb, seed_data):
        """UPDATE to text column should be reflected in search results."""
        # Insert a row with known content
        old_marker = f"updold_{uuid.uuid4().hex[:8]}"
        new_marker = f"updnew_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows("articles", [{
            "title": "Update Test",
            "content": f"This has {old_marker} in it",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])

        # Wait for initial insert
        wait_until_gte(
            lambda: mygramdb.count("articles", old_marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="initial insert for update test",
        )

        # Update the content
        mysql.update(
            "articles",
            f"content = 'Now has {new_marker} instead'",
            f"content LIKE '%{old_marker}%'",
        )

        # Wait for update to propagate
        wait_until_gte(
            lambda: mygramdb.count("articles", new_marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="text UPDATE propagation",
        )

    def test_filter_update(self, mysql, mygramdb, seed_data):
        """UPDATE to filter column (status) should be reflected."""
        marker = f"filtupd_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows("articles", [{
            "title": "Filter Update Test",
            "content": f"Content with {marker}",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])

        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="insert for filter update test",
        )

        # Update the status
        mysql.update("articles", "status = 2", f"content LIKE '%{marker}%'")

        # The document should still be searchable (status change doesn't remove it)
        import time
        time.sleep(3)  # Wait for propagation
        count = mygramdb.count("articles", marker)
        assert count >= 1, "Document should still be searchable after status update"
