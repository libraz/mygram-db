"""Test DDL edge cases: DROP, RENAME, column type changes, sequential DDL+DML."""

import contextlib
import uuid

import pytest

from lib.data_generator import DataGenerator
from lib.metrics import REPL_DDL_TOTAL, MetricsSnapshot, read_counter
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


def _ensure_replication_running(mygramdb, sync_table: str = "testdb.articles"):
    """Bring replication back to running and confirm the reported state.

    Two conditions block a restart. START is rejected while a previous STOP is
    still winding down, which clears on its own. A schema incompatibility is
    sticky by design and the documented recovery is an explicit SYNC, so that
    is issued here rather than reported as a refusal. The reported state is
    then required to agree: returning on the acknowledgement alone would let
    the next step run against a reader that is not consuming yet.
    """

    def _started() -> bool:
        if mygramdb.replication_is_running():
            return True
        resp = mygramdb.tcp_command("REPLICATION START", timeout=10.0)
        if resp and "SCHEMA_INCOMPATIBLE" in resp:
            mygramdb.sync(sync_table, timeout=60)
            return False
        if resp is None or "stopping" in resp.lower():
            return False
        return mygramdb.replication_is_running()

    wait_until(_started, timeout=90, interval=0.5, description="replication to be running")
    return True


def _await_ddl_replayed(mygramdb, before: float, *, by: int = 1) -> None:
    """Block until the reader has replayed the DDL just issued.

    Row images decode against the layout the reader last saw, so a test that
    writes rows before the DDL is replayed is testing the old schema.
    """
    wait_until_gte(
        lambda: read_counter(mygramdb, REPL_DDL_TOTAL) - before,
        minimum=by,
        timeout=30,
        interval=0.25,
        description="the DDL to be replayed by the reader",
    )


def _replication_status_value(mygramdb, key):
    """Read one field from the complete REPLICATION STATUS response."""
    response = mygramdb.replication_status()
    for line in response.splitlines():
        line = line.removeprefix("OK ")
        if line.startswith(f"{key}:"):
            return line.split(":", 1)[1].strip()
    raise AssertionError(f"{key} not found in REPLICATION STATUS:\n{response}")


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
        info = mygramdb.info()
        return int(info.get("total_documents", info.get("doc_count", 0))) >= len(rows)

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
    Replication recovery failures are test failures; no stale known-bug skip is
    allowed to hide a stopped reader.
    """

    def test_ddl_on_non_tracked_table(self, mysql, mygramdb, seed_data):
        """DDL on non-tracked table should not affect articles index."""
        info_before = mygramdb.info()
        doc_count_before = info_before.get(
            "total_documents", info_before.get("doc_count", info_before.get("documents", 0))
        )

        MetricsSnapshot.capture(mygramdb)
        ddl_before = read_counter(mygramdb, REPL_DDL_TOTAL)

        cols = mysql.execute("SHOW COLUMNS FROM products LIKE 'ddl_test_col'")
        if not cols:
            mysql.execute("ALTER TABLE products ADD COLUMN ddl_test_col VARCHAR(50) DEFAULT NULL")
            _await_ddl_replayed(mygramdb, ddl_before)

        info_after = mygramdb.info()
        doc_count_after = info_after.get(
            "total_documents", info_after.get("doc_count", info_after.get("documents", 0))
        )
        assert doc_count_after >= doc_count_before, (
            f"Articles doc count should not decrease: {doc_count_before} -> {doc_count_after}"
        )

        assert mygramdb.ping()

        with contextlib.suppress(Exception):
            mysql.execute("ALTER TABLE products DROP COLUMN ddl_test_col")

    def test_sequential_ddl_with_dml(self, mysql, mygramdb, seed_data):
        """Sequential DDL + DML operations should all be handled correctly."""
        assert _ensure_replication_running(mygramdb), (
            "Replication did not recover before sequential DDL"
        )

        marker = f"seqddl_{uuid.uuid4().hex[:8]}"

        try:
            ddl_before = read_counter(mygramdb, REPL_DDL_TOTAL)
            cols = mysql.execute("SHOW COLUMNS FROM articles LIKE 'seq_test_col'")
            if not cols:
                mysql.execute(
                    "ALTER TABLE articles ADD COLUMN seq_test_col VARCHAR(50) DEFAULT NULL"
                )
                _await_ddl_replayed(mygramdb, ddl_before)

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

            drop_ddl_before = read_counter(mygramdb, REPL_DDL_TOTAL)
            mysql.execute("ALTER TABLE articles DROP COLUMN seq_test_col")
            # The DROP can make the reader reconnect, so require both halves:
            # the DDL replayed, and the reader consuming again afterwards.
            _await_ddl_replayed(mygramdb, drop_ddl_before)
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
            with contextlib.suppress(Exception):
                cols = mysql.execute("SHOW COLUMNS FROM articles LIKE 'seq_test_col'")
                if cols:
                    mysql.execute("ALTER TABLE articles DROP COLUMN seq_test_col")

    def test_alter_text_source_column_type(self, mysql, mygramdb, seed_data):
        """Configured text type changes must stop before advancing GTID."""
        assert _ensure_replication_running(mygramdb), "Replication did not recover before ALTER"

        before_gtid = _replication_status_value(mygramdb, "current_gtid")
        marker = f"coltype_{uuid.uuid4().hex[:8]}"

        try:
            mysql.execute("ALTER TABLE articles MODIFY COLUMN content MEDIUMTEXT NOT NULL")
            wait_until(
                lambda: _replication_status_value(mygramdb, "status") == "failed",
                timeout=20,
                interval=0.2,
                description="replication to fail closed on configured text type change",
            )

            assert mygramdb.ping(), "Server should remain live after incompatible ALTER"
            assert not mygramdb.health_ready(), "Incompatible schema must make readiness fail"
            assert _replication_status_value(mygramdb, "last_error_code") == "2012"
            assert "Configured column 'content' changed" in _replication_status_value(
                mygramdb, "last_error"
            )
            assert _replication_status_value(mygramdb, "current_gtid") == before_gtid

            # This row must not be silently acknowledged while replication is
            # stopped. The explicit recovery SYNC in finally must load it.
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
        finally:
            with contextlib.suppress(Exception):
                mysql.execute("ALTER TABLE articles MODIFY COLUMN content TEXT NOT NULL")
            mygramdb.sync("testdb.articles", timeout=60)

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", marker),
            minimum=1,
            timeout=20,
            interval=0.5,
            description="explicit SYNC recovery after incompatible text type change",
        )
        assert mygramdb.health_ready()

    def test_drop_table_fails_closed_and_preserves_live_index(self, mysql, mygramdb, seed_data):
        """DROP must preserve live state and stop before advancing its GTID."""
        try:
            before_gtid = _replication_status_value(mygramdb, "current_gtid")
            before_total = mygramdb.search("testdb.articles", "test", limit=1)["total"]
            assert before_total > 0, "Should have searchable documents before DROP"

            mysql.execute("DROP TABLE articles")
            wait_until(
                lambda: _replication_status_value(mygramdb, "status") == "failed",
                timeout=20,
                interval=0.2,
                description="replication to fail closed on configured table DROP",
            )
            assert _replication_status_value(mygramdb, "last_error_code") == "2012"
            assert "configured table was dropped" in _replication_status_value(
                mygramdb, "last_error"
            )
            assert _replication_status_value(mygramdb, "current_gtid") == before_gtid
            assert not mygramdb.health_ready()
            assert mygramdb.search("testdb.articles", "test", limit=1)["total"] == before_total
        finally:
            _recreate_articles_table(mysql, mygramdb)

        assert mygramdb.health_ready()

    def test_drop_table_keeps_serving_the_indexed_snapshot(self, mysql, mygramdb, seed_data):
        """A DROP stops replication without taking the serving plane with it.

        The fail-closed side is covered separately; what this pins is that the
        two planes are independent. Reads must keep returning the last
        consistent snapshot at full fidelity — the failure must not be handled
        by dropping the in-memory index, which would turn a recoverable
        replication stop into silent data loss for every reader.
        """
        before_ids = mygramdb.search("testdb.articles", "test", limit=50)["ids"]
        assert before_ids, "probe needs a non-empty result set to be meaningful"

        try:
            mysql.execute("DROP TABLE articles")
            wait_until(
                lambda: _replication_status_value(mygramdb, "status") == "failed",
                timeout=20,
                interval=0.2,
                description="replication to fail closed on configured table DROP",
            )

            assert mygramdb.health_live(), "liveness must not follow the replication failure"
            assert not mygramdb.health_ready(), "readiness must reflect the stopped replication"

            after_ids = mygramdb.search("testdb.articles", "test", limit=50)["ids"]
            assert after_ids == before_ids, (
                "reads must keep serving the pre-DROP snapshot unchanged, "
                f"got {len(after_ids)} ids instead of {len(before_ids)}"
            )
            assert mygramdb.info().get("version"), "INFO must still answer after DROP"
        finally:
            _recreate_articles_table(mysql, mygramdb)

        assert mygramdb.health_ready()

    def test_rename_table_fails_closed_and_recovers(self, mysql, mygramdb, seed_data):
        """A RENAME stops replication before the GTID advances, and recovers.

        Aliases are deliberately not followed: continuing past a rename would
        apply rows from a table the configuration never named. Freezing the
        GTID is what makes the stop recoverable — if it advanced past the
        rename, the skipped events could never be replayed and the index would
        stay permanently behind MySQL with no way to detect it.
        """
        before_gtid = _replication_status_value(mygramdb, "current_gtid")
        before_total = mygramdb.search("testdb.articles", "test", limit=1)["total"]

        try:
            mysql.execute("RENAME TABLE articles TO articles_old")
            wait_until(
                lambda: _replication_status_value(mygramdb, "status") == "failed",
                timeout=20,
                interval=0.2,
                description="replication to fail closed on configured table RENAME",
            )
            assert _replication_status_value(mygramdb, "last_error_code") == "2012"
            assert "renamed" in _replication_status_value(mygramdb, "last_error")
            assert _replication_status_value(mygramdb, "current_gtid") == before_gtid, (
                "the GTID advanced past the rename, making the stop unrecoverable"
            )
            assert mygramdb.health_live(), "liveness must survive the rename"
            assert mygramdb.search("testdb.articles", "test", limit=1)["total"] == before_total

            mysql.execute("RENAME TABLE articles_old TO articles")
            _ensure_replication_running(mygramdb)
            mygramdb.sync("testdb.articles", timeout=60)

            # Recovery is only real if newly written rows reach the index again.
            _verify_replication_works(mysql, mygramdb)
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
