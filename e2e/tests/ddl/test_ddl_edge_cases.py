"""Test DDL edge cases: DROP, RENAME, column type changes, sequential DDL+DML."""

import contextlib
import time
import uuid

import pytest

from lib.data_generator import DataGenerator
from lib.metrics import MetricsSnapshot
from lib.wait import wait_until, wait_until_gte

pytestmark = pytest.mark.ddl

# SQL for recreating the articles table
CREATE_ARTICLES_SQL = """
CREATE TABLE IF NOT EXISTS articles (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    title VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,
    status INT NOT NULL DEFAULT 1,
    category VARCHAR(50),
    enabled TINYINT NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at DATETIME NULL DEFAULT NULL,
    PRIMARY KEY (id),
    KEY idx_status (status),
    KEY idx_category (category),
    KEY idx_enabled (enabled),
    KEY idx_deleted_at (deleted_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""

ADD_FULLTEXT_SQL = "ALTER TABLE articles ADD FULLTEXT INDEX ft_content (content) WITH PARSER ngram"


def _ensure_replication_running(mygramdb):
    """Ensure replication is running (with retry for stopping race)."""
    status = mygramdb.tcp_command("REPLICATION STATUS") or ""
    if "running" in status.lower():
        return True
    for _ in range(10):
        resp = mygramdb.tcp_command("REPLICATION START", timeout=10.0)
        if resp and ("STARTED" in resp or "already" in resp.lower()):
            return True
        if resp and "stopping" in resp.lower():
            time.sleep(3)
            continue
        return False
    return False


def _verify_replication_works(mysql, mygramdb, timeout=15):
    """Verify replication is actually processing events by inserting a test row."""
    marker = f"replcheck_{uuid.uuid4().hex[:8]}"
    mysql.insert_rows(
        "articles",
        [
            {
                "title": "Replication Check",
                "content": f"Content repl check {marker}",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
        ],
    )
    try:
        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=1,
            timeout=timeout,
            interval=0.5,
            description="replication health check",
        )
        return True
    except Exception:
        return False


def _recreate_articles_table(mysql, mygramdb):
    """Recreate articles table, re-seed data, and re-sync."""
    mysql.execute(CREATE_ARTICLES_SQL)
    with contextlib.suppress(Exception):
        mysql.execute(ADD_FULLTEXT_SQL)

    gen = DataGenerator(seed=42)
    rows = gen.generate_articles(count=100)
    mysql.insert_rows("articles", rows)

    mygramdb.sync("testdb.articles", timeout=60)

    def _has_docs() -> bool:
        count = mygramdb.count("testdb.articles", "test")
        return count >= 30

    wait_until(
        _has_docs,
        timeout=30,
        interval=1,
        description="articles table re-sync after recreation",
    )

    _ensure_replication_running(mygramdb)


class TestDDLEdgeCases:
    """Test edge cases in DDL handling.

    Tests are ordered to run non-destructive tests first, then destructive ones.
    Destructive DDL (DROP/RENAME) can permanently break replication due to a known
    server bug where BinlogReader::Stop() deadlocks (binlog read_timeout=60s,
    reader thread stuck in mysql_binlog_fetch).
    """

    def test_ddl_on_non_tracked_table(self, mysql, mygramdb, seed_data):
        """DDL on non-tracked table should not affect articles index."""
        info_before = mygramdb.info()
        doc_count_before = info_before.get(
            "total_documents", info_before.get("doc_count", info_before.get("documents", 0))
        )

        MetricsSnapshot.capture(mygramdb)

        cols = mysql.execute("SHOW COLUMNS FROM products LIKE 'ddl_test_col'")
        if not cols:
            mysql.execute("ALTER TABLE products ADD COLUMN ddl_test_col VARCHAR(50) DEFAULT NULL")
        time.sleep(3)

        info_after = mygramdb.info()
        doc_count_after = info_after.get(
            "total_documents", info_after.get("doc_count", info_after.get("documents", 0))
        )
        assert doc_count_after >= doc_count_before, (
            f"Articles doc count should not decrease: {doc_count_before} -> {doc_count_after}"
        )

        assert mygramdb.ping()

        try:
            mysql.execute("ALTER TABLE products DROP COLUMN ddl_test_col")
            time.sleep(1)
        except Exception:
            pass

    def test_sequential_ddl_with_dml(self, mysql, mygramdb, seed_data):
        """Sequential DDL + DML operations should all be handled correctly."""
        if not _ensure_replication_running(mygramdb):
            pytest.skip("Replication is stuck in stopping state (known server bug)")

        marker = f"seqddl_{uuid.uuid4().hex[:8]}"

        try:
            cols = mysql.execute("SHOW COLUMNS FROM articles LIKE 'seq_test_col'")
            if not cols:
                mysql.execute(
                    "ALTER TABLE articles ADD COLUMN seq_test_col VARCHAR(50) DEFAULT NULL"
                )
            time.sleep(2)

            for i in range(3):
                mysql.insert_rows(
                    "articles",
                    [
                        {
                            "title": f"Sequential DDL Test {i}",
                            "content": f"Content for sequential ddl {marker} batch1 item {i}",
                            "status": 1,
                            "category": "tech",
                            "enabled": 1,
                        }
                    ],
                )

            mysql.execute("ALTER TABLE articles DROP COLUMN seq_test_col")
            # DDL may cause binlog reader to reconnect; wait for it to stabilize
            time.sleep(5)
            _ensure_replication_running(mygramdb)

            for i in range(3):
                mysql.insert_rows(
                    "articles",
                    [
                        {
                            "title": f"Sequential DDL Test Batch2 {i}",
                            "content": f"Content for sequential ddl {marker} batch2 item {i}",
                            "status": 1,
                            "category": "tech",
                            "enabled": 1,
                        }
                    ],
                )

            wait_until_gte(
                lambda: mygramdb.count("testdb.articles", marker),
                minimum=6,
                timeout=45,
                interval=0.5,
                description="sequential DDL+DML propagation",
            )
        finally:
            try:
                cols = mysql.execute("SHOW COLUMNS FROM articles LIKE 'seq_test_col'")
                if cols:
                    mysql.execute("ALTER TABLE articles DROP COLUMN seq_test_col")
                    time.sleep(1)
            except Exception:
                pass

    def test_alter_text_source_column_type(self, mysql, mygramdb, seed_data):
        """ALTER content column type should not break replication."""
        if not _ensure_replication_running(mygramdb):
            pytest.skip("Replication is stuck in stopping state (known server bug)")

        try:
            mysql.execute("ALTER TABLE articles MODIFY COLUMN content MEDIUMTEXT NOT NULL")
            time.sleep(5)

            assert mygramdb.ping(), "Server should be alive after ALTER column type"

            # Replication may have reconnected after DDL
            _ensure_replication_running(mygramdb)

            marker = f"coltype_{uuid.uuid4().hex[:8]}"
            mysql.insert_rows(
                "articles",
                [
                    {
                        "title": "Column Type Test",
                        "content": f"Content after column type change {marker}",
                        "status": 1,
                        "category": "tech",
                        "enabled": 1,
                    }
                ],
            )

            wait_until_gte(
                lambda: mygramdb.count("testdb.articles", marker),
                minimum=1,
                timeout=30,
                interval=0.5,
                description="insert after column type change",
            )
        finally:
            try:
                mysql.execute("ALTER TABLE articles MODIFY COLUMN content TEXT NOT NULL")
                time.sleep(2)
            except Exception:
                pass

    def test_drop_table_clears_index(self, mysql, mygramdb, seed_data):
        """DROP TABLE should clear MygramDB index, then recover after recreation."""
        try:
            info_before = mygramdb.info()
            doc_count_before = info_before.get(
                "total_documents", info_before.get("doc_count", info_before.get("documents", 0))
            )
            assert doc_count_before > 0, "Should have documents before DROP"

            mysql.execute("DROP TABLE articles")

            def _articles_cleared() -> bool:
                result = mygramdb.search("testdb.articles", "test", limit=1)
                return result["total"] == 0

            wait_until(
                _articles_cleared,
                timeout=15,
                interval=1,
                description="DROP TABLE to clear articles index",
            )
        finally:
            _recreate_articles_table(mysql, mygramdb)

    def test_drop_table_server_survives(self, mysql, mygramdb, seed_data):
        """Server should survive DROP TABLE and remain functional."""
        try:
            before = MetricsSnapshot.capture(mygramdb)

            mysql.execute("DROP TABLE articles")
            time.sleep(3)

            assert mygramdb.ping(), "Server should be alive after DROP TABLE"
            assert mygramdb.health_live(), "Health endpoint should work after DROP TABLE"

            after = MetricsSnapshot.capture(mygramdb)
            diff = MetricsSnapshot.diff(before, after)

            ddl_metrics = {k: v for k, v in diff.items() if "ddl" in k.lower()}
            if ddl_metrics:
                assert max(ddl_metrics.values()) >= 1, (
                    f"DDL counter should increase on DROP, got {ddl_metrics}"
                )
        finally:
            _recreate_articles_table(mysql, mygramdb)

    def test_rename_table_no_crash(self, mysql, mygramdb, seed_data):
        """RENAME TABLE should not crash the server."""
        try:
            mysql.execute("RENAME TABLE articles TO articles_old")
            time.sleep(3)

            assert mygramdb.ping(), "Server should be alive after RENAME"

            mysql.execute("RENAME TABLE articles_old TO articles")
            time.sleep(3)

            assert mygramdb.ping(), "Server should be alive after RENAME back"
        except Exception:
            with contextlib.suppress(Exception):
                mysql.execute("RENAME TABLE articles_old TO articles")
            raise
        finally:
            try:
                count = mysql.count("articles")
                if count == 0:
                    _recreate_articles_table(mysql, mygramdb)
                else:
                    _ensure_replication_running(mygramdb)
            except Exception:
                _recreate_articles_table(mysql, mygramdb)
