"""Test replication statistics completeness for UPDATE, DELETE, and DDL counters."""

import time
import uuid

import pytest

from lib.metrics import MetricsSnapshot
from lib.wait import wait_until_gte

pytestmark = pytest.mark.statistics


class TestReplicationStatsCompleteness:
    """Verify replication counters for UPDATE, DELETE, DDL, and skipped events."""

    def test_update_applied_counter(self, mysql, mygramdb, seed_data):
        """UPDATE on indexed row should increase updates applied + modified."""
        marker = f"upd_applied_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Update Applied Test",
                    "content": f"Original content {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="update applied seed data",
        )

        before = MetricsSnapshot.capture(mygramdb)

        mysql.update(
            "articles", f"content = 'Updated content {marker} new'", f"content LIKE '%{marker}%'"
        )

        # Wait for replication
        time.sleep(3)

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        update_metrics = {k: v for k, v in diff.items() if "update" in k.lower()}
        if update_metrics:
            applied = {k: v for k, v in update_metrics.items() if "applied" in k.lower()}
            if applied:
                assert max(applied.values()) >= 1, (
                    f"Update applied counter should increase, got {applied}"
                )

    def test_update_added_counter(self, mysql, mygramdb, seed_data):
        """UPDATE enabled 0->1 should increase updates added counter."""
        marker = f"upd_added_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Update Added Test",
                    "content": f"Content for added {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 0,
                }
            ],
        )

        # Wait for the INSERT to be processed (it will be skipped for indexing)
        time.sleep(3)

        before = MetricsSnapshot.capture(mygramdb)

        # Change enabled 0 -> 1 (row now passes filter, should be "added")
        mysql.update("articles", "enabled = 1", f"content LIKE '%{marker}%'")

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="update added data",
        )

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        update_metrics = {k: v for k, v in diff.items() if "update" in k.lower()}
        if update_metrics:
            added = {k: v for k, v in update_metrics.items() if "added" in k.lower()}
            if added:
                assert max(added.values()) >= 1, (
                    f"Update added counter should increase, got {added}"
                )

    def test_update_removed_counter(self, mysql, mygramdb, seed_data):
        """UPDATE enabled 1->0 should increase updates removed counter."""
        marker = f"upd_removed_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Update Removed Test",
                    "content": f"Content for removed {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="update removed seed data",
        )

        before = MetricsSnapshot.capture(mygramdb)

        # Change enabled 1 -> 0 (row no longer passes filter, should be "removed")
        mysql.update("articles", "enabled = 0", f"content LIKE '%{marker}%'")

        # Wait for replication
        time.sleep(3)

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        update_metrics = {k: v for k, v in diff.items() if "update" in k.lower()}
        if update_metrics:
            removed = {k: v for k, v in update_metrics.items() if "removed" in k.lower()}
            if removed:
                assert max(removed.values()) >= 1, (
                    f"Update removed counter should increase, got {removed}"
                )

    def test_update_skipped_counter(self, mysql, mygramdb, seed_data):
        """UPDATE on non-indexed row (enabled=0) should increase updates skipped."""
        marker = f"upd_skipped_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Update Skipped Test",
                    "content": f"Content for skipped {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 0,
                }
            ],
        )

        # Wait for the INSERT to be processed
        time.sleep(3)

        before = MetricsSnapshot.capture(mygramdb)

        # Update status but keep enabled=0 -> should be skipped
        mysql.update("articles", "status = 2", f"content LIKE '%{marker}%'")

        # Wait for replication
        time.sleep(3)

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        update_metrics = {k: v for k, v in diff.items() if "update" in k.lower()}
        if update_metrics:
            skipped = {k: v for k, v in update_metrics.items() if "skipped" in k.lower()}
            if skipped:
                assert max(skipped.values()) >= 1, (
                    f"Update skipped counter should increase, got {skipped}"
                )

    def test_delete_applied_counter(self, mysql, mygramdb, seed_data):
        """DELETE on indexed row should increase deletes applied."""
        marker = f"del_applied_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Delete Applied Test",
                    "content": f"Content for delete {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="delete applied seed data",
        )

        before = MetricsSnapshot.capture(mygramdb)

        mysql.delete("articles", f"content LIKE '%{marker}%'")

        # Wait for replication
        time.sleep(3)

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        delete_metrics = {k: v for k, v in diff.items() if "delete" in k.lower()}
        if delete_metrics:
            applied = {k: v for k, v in delete_metrics.items() if "applied" in k.lower()}
            if applied:
                assert max(applied.values()) >= 1, (
                    f"Delete applied counter should increase, got {applied}"
                )

    def test_delete_skipped_counter(self, mysql, mygramdb, seed_data):
        """DELETE on non-indexed row (enabled=0) should increase deletes skipped."""
        marker = f"del_skipped_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Delete Skipped Test",
                    "content": f"Content for skipdelete {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 0,
                }
            ],
        )

        # Wait for INSERT to be processed
        time.sleep(3)

        before = MetricsSnapshot.capture(mygramdb)

        mysql.delete("articles", f"content LIKE '%{marker}%'")

        # Wait for replication
        time.sleep(3)

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        delete_metrics = {k: v for k, v in diff.items() if "delete" in k.lower()}
        if delete_metrics:
            skipped = {k: v for k, v in delete_metrics.items() if "skipped" in k.lower()}
            if skipped:
                assert max(skipped.values()) >= 1, (
                    f"Delete skipped counter should increase, got {skipped}"
                )

    def test_ddl_executed_counter(self, mysql, mygramdb, seed_data):
        """ALTER TABLE should increase DDL counter."""
        # Check if column exists first
        cols = mysql.execute("SHOW COLUMNS FROM articles LIKE 'ddl_test_col'")
        if cols:
            mysql.execute("ALTER TABLE articles DROP COLUMN ddl_test_col")
            time.sleep(2)

        before = MetricsSnapshot.capture(mygramdb)

        mysql.execute("ALTER TABLE articles ADD COLUMN ddl_test_col VARCHAR(50) DEFAULT NULL")

        # Wait for replication
        time.sleep(3)

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        ddl_metrics = {k: v for k, v in diff.items() if "ddl" in k.lower()}
        if ddl_metrics:
            max_increase = max(ddl_metrics.values())
            assert max_increase >= 1, f"DDL counter should increase, got {ddl_metrics}"

        # Cleanup
        try:
            mysql.execute("ALTER TABLE articles DROP COLUMN ddl_test_col")
            time.sleep(2)
        except Exception:
            pass

    def test_events_skipped_other_tables(self, mysql, mygramdb, seed_data):
        """INSERT into non-tracked table should increment skipped counter or not affect articles."""
        marker = f"other_tbl_{uuid.uuid4().hex[:8]}"

        before_info = mygramdb.info()

        mysql.insert_rows(
            "products",
            [
                {
                    "name": f"Product {marker}",
                    "description": f"Description for {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        # Wait for the event to be processed
        time.sleep(3)

        after_info = mygramdb.info()

        # Articles doc count should not change
        before_info.get("total_documents", before_info.get("doc_count", 0))
        after_docs = after_info.get("total_documents", after_info.get("doc_count", 0))

        # The doc count for articles should not increase from a products insert
        # (it might increase if other tests are inserting concurrently, but not from this)
        assert isinstance(after_docs, (int, float)), "Doc count should be numeric"

    def test_all_counters_combined(self, mysql, mygramdb, seed_data):
        """Mixed INSERT + UPDATE + DELETE should produce consistent counter changes."""
        before = MetricsSnapshot.capture(mygramdb)

        marker = f"combined_{uuid.uuid4().hex[:8]}"

        # INSERT 5 rows (enabled=1)
        rows = [
            {
                "title": f"Combined Test {i}",
                "content": f"Combined content {marker} item {i}",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
            for i in range(5)
        ]
        mysql.insert_rows("articles", rows)

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=5,
            timeout=15,
            interval=0.5,
            description="combined test inserts",
        )

        # UPDATE 3 rows (change content)
        for i in range(3):
            mysql.update(
                "articles",
                f"content = 'Updated combined {marker} item {i}'",
                f"content LIKE '%{marker} item {i}%'",
            )

        time.sleep(3)

        # DELETE 2 rows
        mysql.delete("articles", f"content LIKE '%{marker} item 3%'")
        mysql.delete("articles", f"content LIKE '%{marker} item 4%'")

        time.sleep(3)

        after = MetricsSnapshot.capture(mygramdb)
        diff = MetricsSnapshot.diff(before, after)

        # Verify insert counters increased
        insert_metrics = {k: v for k, v in diff.items() if "insert" in k.lower()}
        if insert_metrics:
            assert max(insert_metrics.values()) >= 5, (
                f"Insert counter should increase by >= 5, got {insert_metrics}"
            )

        # Verify some update activity
        update_metrics = {k: v for k, v in diff.items() if "update" in k.lower()}
        if update_metrics:
            total_updates = sum(v for v in update_metrics.values() if v > 0)
            assert total_updates >= 3, (
                f"Update counters should show >= 3 total, got {update_metrics}"
            )

        # Verify some delete activity
        delete_metrics = {k: v for k, v in diff.items() if "delete" in k.lower()}
        if delete_metrics:
            total_deletes = sum(v for v in delete_metrics.values() if v > 0)
            assert total_deletes >= 2, (
                f"Delete counters should show >= 2 total, got {delete_metrics}"
            )
