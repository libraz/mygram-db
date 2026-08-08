"""Multi-table isolation and cross-impact tests.

Verifies that operations on one table (INSERT/UPDATE/DELETE/DROP/TRUNCATE)
do not affect search results on another table. Uses SEARCH (not COUNT)
to validate actual document retrieval.
"""

from __future__ import annotations

import contextlib
import threading
import uuid

import pytest

from lib.data_generator import DataGenerator
from lib.wait import wait_until, wait_until_gte

pytestmark = pytest.mark.multi_table

# SQL for recreating dropped tables
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

CREATE_PRODUCTS_SQL = """
CREATE TABLE IF NOT EXISTS products (
    id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    status INT NOT NULL DEFAULT 1,
    category VARCHAR(50),
    enabled TINYINT NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    deleted_at DATETIME NULL DEFAULT NULL,
    PRIMARY KEY (id),
    KEY idx_status (status),
    KEY idx_category (category),
    KEY idx_enabled (enabled)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
"""


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


ADD_FULLTEXT_SQL = "ALTER TABLE articles ADD FULLTEXT INDEX ft_content (content) WITH PARSER ngram"


def _reseed_table(mysql, mygramdb, table, count=50):
    """Re-seed a table after destructive operations."""
    gen = DataGenerator(seed=42)
    if table == "articles":
        mysql.execute(CREATE_ARTICLES_SQL)
        # Restore FULLTEXT index for cross-verify tests
        with contextlib.suppress(Exception):
            mysql.execute(ADD_FULLTEXT_SQL)
        rows = gen.generate_articles(count=count)
    else:
        mysql.execute(CREATE_PRODUCTS_SQL)
        rows = gen.generate_products(count=count)
    mysql.insert_rows(table, rows)
    mygramdb.sync(f"testdb.{table}", timeout=60)
    _ensure_replication_running(mygramdb)


class TestMultiTableSearch:
    """Verify both tables are independently searchable via SEARCH command."""

    def test_both_tables_searchable(self, mysql, mygramdb, seed_data):
        """Both articles and products should return SEARCH results."""
        # Insert identifiable data into both tables
        art_marker = f"bothsrch_{uuid.uuid4().hex[:8]}"
        prod_marker = f"bothsrch_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Both Tables Test",
                    "content": f"Article content {art_marker} searchable",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        mysql.insert_rows(
            "products",
            [
                {
                    "name": "Both Tables Product",
                    "description": f"Product content {prod_marker} searchable",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        # Sequential rather than concurrent: SYNC STATUS reports per table with
        # no generation number, so overlapping syncs cannot be told apart. The
        # client already blocks until each table is complete and queryable.
        mygramdb.sync("testdb.articles", timeout=30)
        mygramdb.sync("testdb.products", timeout=30)

        wait_until_gte(
            lambda: mygramdb.count("testdb.articles", art_marker),
            minimum=1,
            timeout=15,
            interval=0.5,
            description="articles searchable",
        )
        wait_until_gte(
            lambda: mygramdb.count("testdb.products", prod_marker),
            minimum=1,
            timeout=15,
            interval=0.5,
            description="products searchable",
        )

    def test_no_cross_contamination_search(self, mysql, mygramdb, seed_data):
        """Unique content in articles must NOT appear in products SEARCH."""
        article_marker = f"artonly_{uuid.uuid4().hex[:8]}"
        product_marker = f"prodonly_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Article Only",
                    "content": f"Unique article content {article_marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        mysql.insert_rows(
            "products",
            [
                {
                    "name": "Product Only",
                    "description": f"Unique product content {product_marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until(
            lambda: mygramdb.search("testdb.articles", article_marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"articles SEARCH to find {article_marker}",
        )
        wait_until(
            lambda: mygramdb.search("testdb.products", product_marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"products SEARCH to find {product_marker}",
        )

        # Cross-check: article marker NOT in products, product marker NOT in articles
        cross_a = mygramdb.search("testdb.products", article_marker, limit=10)
        assert cross_a["total"] == 0, (
            f"Article-only marker found in products SEARCH: total={cross_a['total']}"
        )
        cross_p = mygramdb.search("testdb.articles", product_marker, limit=10)
        assert cross_p["total"] == 0, (
            f"Product-only marker found in articles SEARCH: total={cross_p['total']}"
        )

    def test_filter_isolation(self, mysql, mygramdb, seed_data):
        """Filter operations on one table must not affect the other."""
        marker = f"filtiso_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Filter Iso Article",
                    "content": f"Content with {marker} in articles",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        mysql.insert_rows(
            "products",
            [
                {
                    "name": "Filter Iso Product",
                    "description": f"Description with {marker} in products",
                    "status": 2,
                    "category": "news",
                    "enabled": 1,
                }
            ],
        )

        wait_until(
            lambda: mygramdb.search("testdb.articles", marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"articles to find {marker}",
        )
        wait_until(
            lambda: mygramdb.search("testdb.products", marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"products to find {marker}",
        )

        # Filter status=1 should match in articles, not in products
        art_s1 = mygramdb.search("testdb.articles", marker, filters={"status": 1}, limit=10)
        prod_s1 = mygramdb.search("testdb.products", marker, filters={"status": 1}, limit=10)
        assert art_s1["total"] >= 1
        assert prod_s1["total"] == 0, (
            f"Product with status=2 should not match status=1 filter, got {prod_s1['total']}"
        )


class TestMultiTableReplication:
    """Verify replication events are correctly routed per table."""

    def test_insert_both_tables_independently(self, mysql, mygramdb, seed_data):
        """Simultaneous INSERTs to both tables are both reflected in SEARCH."""
        art_marker = f"dualins_a_{uuid.uuid4().hex[:8]}"
        prod_marker = f"dualins_p_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Dual Insert Art",
                    "content": f"Article content {art_marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        mysql.insert_rows(
            "products",
            [
                {
                    "name": "Dual Insert Prod",
                    "description": f"Product desc {prod_marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until(
            lambda: (
                mygramdb.search("testdb.articles", art_marker, limit=10)["total"] >= 1
                and mygramdb.search("testdb.products", prod_marker, limit=10)["total"] >= 1
            ),
            timeout=20,
            interval=0.5,
            description="both tables to reflect INSERTs",
        )

    def test_update_one_table_no_impact_on_other(self, mysql, mygramdb, seed_data):
        """UPDATE on articles should not change products SEARCH results."""
        shared_marker = f"updiso_{uuid.uuid4().hex[:8]}"
        new_marker = f"updiso_new_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Update Iso Art",
                    "content": f"Content {shared_marker} in articles",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        mysql.insert_rows(
            "products",
            [
                {
                    "name": "Update Iso Prod",
                    "description": f"Description {shared_marker} in products",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until(
            lambda: (
                mygramdb.search("testdb.articles", shared_marker, limit=10)["total"] >= 1
                and mygramdb.search("testdb.products", shared_marker, limit=10)["total"] >= 1
            ),
            timeout=20,
            interval=0.5,
            description="both tables synced",
        )

        # UPDATE only articles
        mysql.update(
            "articles",
            f"content = 'Changed to {new_marker}'",
            f"content LIKE '%{shared_marker}%'",
        )

        wait_until(
            lambda: mygramdb.search("testdb.articles", new_marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description="articles UPDATE reflected",
        )

        # Products must still have the original marker
        prod_result = mygramdb.search("testdb.products", shared_marker, limit=10)
        assert prod_result["total"] >= 1, (
            f"Products should still have '{shared_marker}' after articles UPDATE, "
            f"got total={prod_result['total']}"
        )

    def test_delete_one_table_no_impact_on_other(self, mysql, mygramdb, seed_data):
        """DELETE from articles should not remove products SEARCH results."""
        shared_marker = f"deliso_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Delete Iso Art",
                    "content": f"Content {shared_marker} in articles",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        mysql.insert_rows(
            "products",
            [
                {
                    "name": "Delete Iso Prod",
                    "description": f"Description {shared_marker} in products",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until(
            lambda: (
                mygramdb.search("testdb.articles", shared_marker, limit=10)["total"] >= 1
                and mygramdb.search("testdb.products", shared_marker, limit=10)["total"] >= 1
            ),
            timeout=20,
            interval=0.5,
            description="both tables synced",
        )

        # DELETE only from articles
        mysql.delete("articles", f"content LIKE '%{shared_marker}%'")

        wait_until(
            lambda: mygramdb.search("testdb.articles", shared_marker, limit=10)["total"] == 0,
            timeout=20,
            interval=0.5,
            description="articles DELETE reflected",
        )

        # Products must still be intact
        prod_result = mygramdb.search("testdb.products", shared_marker, limit=10)
        assert prod_result["total"] >= 1, (
            f"Products should still have '{shared_marker}' after articles DELETE, "
            f"got total={prod_result['total']}"
        )


class TestMultiTableDDL:
    """Verify DDL on one table does not break the other table's index."""

    def test_drop_one_table_other_still_works(self, mysql, mygramdb, seed_data):
        """DROP articles — products SEARCH must still work. Then restore."""
        prod_marker = f"dropiso_{uuid.uuid4().hex[:8]}"
        art_marker = f"dropisoart_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows(
            "products",
            [
                {
                    "name": "Drop Iso Product",
                    "description": f"Product {prod_marker} survives article drop",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Drop Iso Article",
                    "content": f"Article {art_marker} is dropped with its table",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until(
            lambda: (
                mygramdb.search("testdb.products", prod_marker, limit=10)["total"] >= 1
                and mygramdb.search("testdb.articles", art_marker, limit=10)["total"] >= 1
            ),
            timeout=20,
            interval=0.5,
            description="both markers to be searchable before the drop",
        )

        try:
            mysql.execute("DROP TABLE articles")
            # Dropping a configured table is refused rather than applied: the
            # reader fails closed without advancing the GTID so the operator
            # can restore the table. Waiting for that state is what proves the
            # DDL reached the reader -- otherwise the isolation check below
            # could run before it and pass for the wrong reason.
            wait_until(
                lambda: mygramdb.replication_field("status") == "failed",
                timeout=30,
                interval=0.25,
                description="replication to fail closed on the configured table DROP",
            )
            assert "dropped" in mygramdb.replication_field("last_error"), (
                "replication stopped for a reason other than the DROP"
            )
            assert mygramdb.search("testdb.articles", art_marker, limit=10)["total"] >= 1, (
                "the index was cleared by a DROP that was supposed to be refused"
            )

            # Products SEARCH must still work
            result = mygramdb.search("testdb.products", prod_marker, limit=10)
            assert result["total"] >= 1, (
                f"Products SEARCH should still work after DROP articles, "
                f"got total={result['total']}"
            )

            # Server must be alive
            assert mygramdb.ping(), "Server should survive DROP articles"

        finally:
            _reseed_table(mysql, mygramdb, "articles", count=100)

    def test_truncate_one_table_other_intact(self, mysql, mygramdb, seed_data):
        """TRUNCATE articles — products SEARCH must be unaffected."""
        art_marker = f"truncart_{uuid.uuid4().hex[:8]}"
        prod_marker = f"truncprod_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Truncate Test Art",
                    "content": f"Article content {art_marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        mysql.insert_rows(
            "products",
            [
                {
                    "name": "Truncate Test Prod",
                    "description": f"Product content {prod_marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        wait_until(
            lambda: (
                mygramdb.search("testdb.articles", art_marker, limit=10)["total"] >= 1
                and mygramdb.search("testdb.products", prod_marker, limit=10)["total"] >= 1
            ),
            timeout=20,
            interval=0.5,
            description="both markers synced",
        )

        try:
            mysql.execute("TRUNCATE TABLE articles")

            # Articles should be empty
            wait_until(
                lambda: mygramdb.search("testdb.articles", art_marker, limit=10)["total"] == 0,
                timeout=30,
                interval=0.5,
                description="the truncated table to stop matching its own marker",
            )

            # Products must be intact
            prod_result = mygramdb.search("testdb.products", prod_marker, limit=10)
            assert prod_result["total"] >= 1, (
                f"Products should be intact after articles TRUNCATE, "
                f"got total={prod_result['total']}"
            )
        finally:
            _reseed_table(mysql, mygramdb, "articles", count=100)

    def test_drop_and_recreate_table_search_recovers(self, mysql, mygramdb, seed_data):
        """DROP + recreate articles — SEARCH should work again after re-sync.

        Note: DDL causes binlog reader reconnect. We use SYNC to rebuild
        the index from scratch rather than relying on replication recovery.
        """
        pre_marker = f"predrop_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Pre Drop",
                    "content": f"Content before the drop {pre_marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        wait_until(
            lambda: mygramdb.search("testdb.articles", pre_marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"articles SEARCH to find {pre_marker} before the drop",
        )

        try:
            mysql.execute("DROP TABLE articles")
            # The reader refuses the DROP and holds its position. Rebuilding
            # before that state is reached would race the refusal against the
            # SYNC, so the recovery below would not be the one under test.
            wait_until(
                lambda: mygramdb.replication_field("status") == "failed",
                timeout=30,
                interval=0.25,
                description="replication to fail closed on the articles DROP",
            )

            # Recreate, re-seed, and SYNC (full rebuild from MySQL)
            _reseed_table(mysql, mygramdb, "articles", count=50)

            # Wait for SYNC to complete and verify data is searchable
            wait_until(
                lambda: mygramdb.search("testdb.articles", "test", limit=10)["total"] >= 1,
                timeout=60,
                interval=1,
                description="articles SEARCH to work after recreate+sync",
            )

            # Ensure replication is running for the new marker test
            _ensure_replication_running(mygramdb)

            # Insert a new unique row via replication
            marker = f"recreate_{uuid.uuid4().hex[:8]}"
            mysql.insert_rows(
                "articles",
                [
                    {
                        "title": "Post-Recreate",
                        "content": f"Content after recreation {marker}",
                        "status": 1,
                        "category": "tech",
                        "enabled": 1,
                    }
                ],
            )

            wait_until(
                lambda: mygramdb.search("testdb.articles", marker, limit=10)["total"] >= 1,
                timeout=30,
                interval=1,
                description=f"articles SEARCH to find {marker} via replication",
            )
        except Exception:
            _reseed_table(mysql, mygramdb, "articles", count=100)
            raise

    def test_sync_one_table_no_impact_on_other(self, mysql, mygramdb, seed_data):
        """Non-target INSERT/UPDATE/DELETE during SYNC must be replayed."""
        prod_marker = f"synciso_{uuid.uuid4().hex[:8]}"
        delete_marker = f"syncdelete_{uuid.uuid4().hex[:8]}"
        insert_marker = f"syncinsert_{uuid.uuid4().hex[:8]}"
        updated_marker = f"syncupdated_{uuid.uuid4().hex[:8]}"
        load_marker = f"syncload_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows(
            "products",
            [
                {
                    "name": "Sync Iso Product",
                    "description": f"Product {prod_marker} should survive articles sync",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                },
                {
                    "name": "Sync Delete Product",
                    "description": f"Product {delete_marker} will be deleted during articles sync",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                },
            ],
        )

        wait_until(
            lambda: mygramdb.search("testdb.products", prod_marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"products to find {prod_marker}",
        )
        wait_until(
            lambda: mygramdb.search("testdb.products", delete_marker, limit=10)["total"] >= 1,
            timeout=20,
            interval=0.5,
            description=f"products to find {delete_marker}",
        )

        # Make the target snapshot long enough to observe its active state.
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": f"SYNC load row {i}",
                    "content": f"{load_marker} content {i}",
                    "status": 1,
                    "category": "sync-load",
                    "enabled": 1,
                }
                for i in range(3000)
            ],
        )

        sync_result: list[bool] = []
        sync_error: list[BaseException] = []

        def _run_sync() -> None:
            try:
                sync_result.append(mygramdb.sync("testdb.articles", timeout=120))
            except BaseException as exc:  # surfaced in the main test thread
                sync_error.append(exc)

        sync_thread = threading.Thread(target=_run_sync, daemon=True)
        sync_thread.start()
        wait_until(
            lambda: any(
                state in (mygramdb._sync_status_line("testdb.articles") or "")
                for state in ("STARTING", "IN_PROGRESS")
            ),
            timeout=20,
            interval=0.02,
            description="articles SYNC to enter an active state",
        )

        product_rows = mysql.execute(
            "SELECT id, description FROM products WHERE description LIKE %s OR description LIKE %s",
            (f"%{prod_marker}%", f"%{delete_marker}%"),
        )
        product_id = next(row["id"] for row in product_rows if prod_marker in row["description"])
        delete_id = next(row["id"] for row in product_rows if delete_marker in row["description"])
        mysql.update(
            "products",
            f"description = 'Product {updated_marker} updated during articles sync'",
            f"id = {int(product_id)}",
        )
        mysql.delete("products", f"id = {int(delete_id)}")
        mysql.insert_rows(
            "products",
            [
                {
                    "name": "Sync Insert Product",
                    "description": f"Product {insert_marker} inserted during articles sync",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        sync_thread.join(timeout=130)
        assert not sync_thread.is_alive(), "SYNC did not complete"
        assert not sync_error, f"SYNC raised: {sync_error}"
        assert sync_result == [True]

        wait_until(
            lambda: mygramdb.search("testdb.products", insert_marker, limit=10)["total"] == 1,
            timeout=30,
            interval=0.25,
            description="non-target INSERT during SYNC to be replayed",
        )
        wait_until(
            lambda: mygramdb.search("testdb.products", updated_marker, limit=10)["total"] == 1,
            timeout=30,
            interval=0.25,
            description="non-target UPDATE during SYNC to be replayed",
        )
        wait_until(
            lambda: mygramdb.search("testdb.products", delete_marker, limit=10)["total"] == 0,
            timeout=30,
            interval=0.25,
            description="non-target DELETE during SYNC to be replayed",
        )
        assert mygramdb.search("testdb.products", prod_marker, limit=10)["total"] == 0

        mysql.execute("DELETE FROM articles WHERE content LIKE %s", (f"{load_marker}%",))
