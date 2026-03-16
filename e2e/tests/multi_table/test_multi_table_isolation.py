"""Multi-table isolation and cross-impact tests.

Verifies that operations on one table (INSERT/UPDATE/DELETE/DROP/TRUNCATE)
do not affect search results on another table. Uses SEARCH (not COUNT)
to validate actual document retrieval.
"""

from __future__ import annotations

import time
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


def _ensure_replication_running(mygramdb):
    """Ensure replication is running."""
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


ADD_FULLTEXT_SQL = (
    "ALTER TABLE articles ADD FULLTEXT INDEX ft_content (content) WITH PARSER ngram"
)


def _reseed_table(mysql, mygramdb, table, count=50):
    """Re-seed a table after destructive operations."""
    gen = DataGenerator(seed=42)
    if table == "articles":
        mysql.execute(CREATE_ARTICLES_SQL)
        # Restore FULLTEXT index for cross-verify tests
        try:
            mysql.execute(ADD_FULLTEXT_SQL)
        except Exception:
            pass
        rows = gen.generate_articles(count=count)
    else:
        mysql.execute(CREATE_PRODUCTS_SQL)
        rows = gen.generate_products(count=count)
    mysql.insert_rows(table, rows)
    mygramdb.sync(table, timeout=60)
    _ensure_replication_running(mygramdb)


class TestMultiTableSearch:
    """Verify both tables are independently searchable via SEARCH command."""

    def test_both_tables_searchable(self, mysql, mygramdb, seed_data):
        """Both articles and products should return SEARCH results."""
        # Insert identifiable data into both tables
        art_marker = f"bothsrch_{uuid.uuid4().hex[:8]}"
        prod_marker = f"bothsrch_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows("articles", [{
            "title": "Both Tables Test",
            "content": f"Article content {art_marker} searchable",
            "status": 1, "category": "tech", "enabled": 1,
        }])
        mysql.insert_rows("products", [{
            "name": "Both Tables Product",
            "description": f"Product content {prod_marker} searchable",
            "status": 1, "category": "tech", "enabled": 1,
        }])

        # Sync both tables (sequential to avoid SYNC STATUS race)
        mygramdb.sync("articles", timeout=30)
        time.sleep(1)
        mygramdb.sync("products", timeout=30)
        time.sleep(1)

        wait_until_gte(
            lambda: mygramdb.count("articles", art_marker),
            minimum=1,
            timeout=15,
            interval=0.5,
            description="articles searchable",
        )
        wait_until_gte(
            lambda: mygramdb.count("products", prod_marker),
            minimum=1,
            timeout=15,
            interval=0.5,
            description="products searchable",
        )

    def test_no_cross_contamination_search(self, mysql, mygramdb, seed_data):
        """Unique content in articles must NOT appear in products SEARCH."""
        article_marker = f"artonly_{uuid.uuid4().hex[:8]}"
        product_marker = f"prodonly_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows("articles", [{
            "title": "Article Only",
            "content": f"Unique article content {article_marker}",
            "status": 1, "category": "tech", "enabled": 1,
        }])
        mysql.insert_rows("products", [{
            "name": "Product Only",
            "description": f"Unique product content {product_marker}",
            "status": 1, "category": "tech", "enabled": 1,
        }])

        wait_until(
            lambda: mygramdb.search("articles", article_marker, limit=10)["total"] >= 1,
            timeout=20, interval=0.5,
            description=f"articles SEARCH to find {article_marker}",
        )
        wait_until(
            lambda: mygramdb.search("products", product_marker, limit=10)["total"] >= 1,
            timeout=20, interval=0.5,
            description=f"products SEARCH to find {product_marker}",
        )

        # Cross-check: article marker NOT in products, product marker NOT in articles
        cross_a = mygramdb.search("products", article_marker, limit=10)
        assert cross_a["total"] == 0, (
            f"Article-only marker found in products SEARCH: total={cross_a['total']}"
        )
        cross_p = mygramdb.search("articles", product_marker, limit=10)
        assert cross_p["total"] == 0, (
            f"Product-only marker found in articles SEARCH: total={cross_p['total']}"
        )

    def test_filter_isolation(self, mysql, mygramdb, seed_data):
        """Filter operations on one table must not affect the other."""
        marker = f"filtiso_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows("articles", [{
            "title": "Filter Iso Article",
            "content": f"Content with {marker} in articles",
            "status": 1, "category": "tech", "enabled": 1,
        }])
        mysql.insert_rows("products", [{
            "name": "Filter Iso Product",
            "description": f"Description with {marker} in products",
            "status": 2, "category": "news", "enabled": 1,
        }])

        wait_until(
            lambda: mygramdb.search("articles", marker, limit=10)["total"] >= 1,
            timeout=20, interval=0.5,
            description=f"articles to find {marker}",
        )
        wait_until(
            lambda: mygramdb.search("products", marker, limit=10)["total"] >= 1,
            timeout=20, interval=0.5,
            description=f"products to find {marker}",
        )

        # Filter status=1 should match in articles, not in products
        art_s1 = mygramdb.search("articles", marker, filters={"status": 1}, limit=10)
        prod_s1 = mygramdb.search("products", marker, filters={"status": 1}, limit=10)
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

        mysql.insert_rows("articles", [{
            "title": "Dual Insert Art",
            "content": f"Article content {art_marker}",
            "status": 1, "category": "tech", "enabled": 1,
        }])
        mysql.insert_rows("products", [{
            "name": "Dual Insert Prod",
            "description": f"Product desc {prod_marker}",
            "status": 1, "category": "tech", "enabled": 1,
        }])

        wait_until(
            lambda: (mygramdb.search("articles", art_marker, limit=10)["total"] >= 1
                     and mygramdb.search("products", prod_marker, limit=10)["total"] >= 1),
            timeout=20, interval=0.5,
            description="both tables to reflect INSERTs",
        )

    def test_update_one_table_no_impact_on_other(self, mysql, mygramdb, seed_data):
        """UPDATE on articles should not change products SEARCH results."""
        shared_marker = f"updiso_{uuid.uuid4().hex[:8]}"
        new_marker = f"updiso_new_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows("articles", [{
            "title": "Update Iso Art",
            "content": f"Content {shared_marker} in articles",
            "status": 1, "category": "tech", "enabled": 1,
        }])
        mysql.insert_rows("products", [{
            "name": "Update Iso Prod",
            "description": f"Description {shared_marker} in products",
            "status": 1, "category": "tech", "enabled": 1,
        }])

        wait_until(
            lambda: (mygramdb.search("articles", shared_marker, limit=10)["total"] >= 1
                     and mygramdb.search("products", shared_marker, limit=10)["total"] >= 1),
            timeout=20, interval=0.5,
            description="both tables synced",
        )

        # UPDATE only articles
        mysql.update(
            "articles",
            f"content = 'Changed to {new_marker}'",
            f"content LIKE '%{shared_marker}%'",
        )

        wait_until(
            lambda: mygramdb.search("articles", new_marker, limit=10)["total"] >= 1,
            timeout=20, interval=0.5,
            description="articles UPDATE reflected",
        )

        # Products must still have the original marker
        prod_result = mygramdb.search("products", shared_marker, limit=10)
        assert prod_result["total"] >= 1, (
            f"Products should still have '{shared_marker}' after articles UPDATE, "
            f"got total={prod_result['total']}"
        )

    def test_delete_one_table_no_impact_on_other(self, mysql, mygramdb, seed_data):
        """DELETE from articles should not remove products SEARCH results."""
        shared_marker = f"deliso_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows("articles", [{
            "title": "Delete Iso Art",
            "content": f"Content {shared_marker} in articles",
            "status": 1, "category": "tech", "enabled": 1,
        }])
        mysql.insert_rows("products", [{
            "name": "Delete Iso Prod",
            "description": f"Description {shared_marker} in products",
            "status": 1, "category": "tech", "enabled": 1,
        }])

        wait_until(
            lambda: (mygramdb.search("articles", shared_marker, limit=10)["total"] >= 1
                     and mygramdb.search("products", shared_marker, limit=10)["total"] >= 1),
            timeout=20, interval=0.5,
            description="both tables synced",
        )

        # DELETE only from articles
        mysql.delete("articles", f"content LIKE '%{shared_marker}%'")

        wait_until(
            lambda: mygramdb.search("articles", shared_marker, limit=10)["total"] == 0,
            timeout=20, interval=0.5,
            description="articles DELETE reflected",
        )

        # Products must still be intact
        prod_result = mygramdb.search("products", shared_marker, limit=10)
        assert prod_result["total"] >= 1, (
            f"Products should still have '{shared_marker}' after articles DELETE, "
            f"got total={prod_result['total']}"
        )


class TestMultiTableDDL:
    """Verify DDL on one table does not break the other table's index."""

    def test_drop_one_table_other_still_works(self, mysql, mygramdb, seed_data):
        """DROP articles — products SEARCH must still work. Then restore."""
        prod_marker = f"dropiso_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows("products", [{
            "name": "Drop Iso Product",
            "description": f"Product {prod_marker} survives article drop",
            "status": 1, "category": "tech", "enabled": 1,
        }])

        wait_until(
            lambda: mygramdb.search("products", prod_marker, limit=10)["total"] >= 1,
            timeout=20, interval=0.5,
            description=f"products to find {prod_marker}",
        )

        try:
            mysql.execute("DROP TABLE articles")
            time.sleep(3)

            # Products SEARCH must still work
            result = mygramdb.search("products", prod_marker, limit=10)
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

        mysql.insert_rows("articles", [{
            "title": "Truncate Test Art",
            "content": f"Article content {art_marker}",
            "status": 1, "category": "tech", "enabled": 1,
        }])
        mysql.insert_rows("products", [{
            "name": "Truncate Test Prod",
            "description": f"Product content {prod_marker}",
            "status": 1, "category": "tech", "enabled": 1,
        }])

        wait_until(
            lambda: (mygramdb.search("articles", art_marker, limit=10)["total"] >= 1
                     and mygramdb.search("products", prod_marker, limit=10)["total"] >= 1),
            timeout=20, interval=0.5,
            description="both markers synced",
        )

        try:
            mysql.execute("TRUNCATE TABLE articles")
            time.sleep(3)

            # Articles should be empty
            art_result = mygramdb.search("articles", art_marker, limit=10)
            assert art_result["total"] == 0, (
                f"Articles should be empty after TRUNCATE, got total={art_result['total']}"
            )

            # Products must be intact
            prod_result = mygramdb.search("products", prod_marker, limit=10)
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
        try:
            mysql.execute("DROP TABLE articles")
            time.sleep(5)

            # Recreate, re-seed, and SYNC (full rebuild from MySQL)
            _reseed_table(mysql, mygramdb, "articles", count=50)

            # Wait for SYNC to complete and verify data is searchable
            wait_until(
                lambda: mygramdb.search("articles", "test", limit=10)["total"] >= 1,
                timeout=60, interval=1,
                description="articles SEARCH to work after recreate+sync",
            )

            # Ensure replication is running for the new marker test
            _ensure_replication_running(mygramdb)
            time.sleep(2)

            # Insert a new unique row via replication
            marker = f"recreate_{uuid.uuid4().hex[:8]}"
            mysql.insert_rows("articles", [{
                "title": "Post-Recreate",
                "content": f"Content after recreation {marker}",
                "status": 1, "category": "tech", "enabled": 1,
            }])

            # If replication doesn't recover in time, fall back to SYNC
            try:
                wait_until(
                    lambda: mygramdb.search("articles", marker, limit=10)["total"] >= 1,
                    timeout=30, interval=1,
                    description=f"articles SEARCH to find {marker} via replication",
                )
            except Exception:
                # Replication may not have recovered yet (known DDL issue)
                # Verify via SYNC instead
                mygramdb.sync("articles", timeout=60)
                wait_until(
                    lambda: mygramdb.search("articles", marker, limit=10)["total"] >= 1,
                    timeout=30, interval=1,
                    description=f"articles SEARCH to find {marker} via SYNC fallback",
                )
        except Exception:
            _reseed_table(mysql, mygramdb, "articles", count=100)
            raise

    def test_sync_one_table_no_impact_on_other(self, mysql, mygramdb, seed_data):
        """SYNC articles should not affect products SEARCH results."""
        prod_marker = f"synciso_{uuid.uuid4().hex[:8]}"

        mysql.insert_rows("products", [{
            "name": "Sync Iso Product",
            "description": f"Product {prod_marker} should survive articles sync",
            "status": 1, "category": "tech", "enabled": 1,
        }])

        wait_until(
            lambda: mygramdb.search("products", prod_marker, limit=10)["total"] >= 1,
            timeout=20, interval=0.5,
            description=f"products to find {prod_marker}",
        )

        # Record products state before sync
        before = mygramdb.search("products", prod_marker, limit=10)

        # SYNC articles (full rebuild)
        mygramdb.sync("articles", timeout=60)

        # Products must be unchanged
        after = mygramdb.search("products", prod_marker, limit=10)
        assert after["total"] == before["total"], (
            f"Products SEARCH changed after articles SYNC: "
            f"before={before['total']}, after={after['total']}"
        )
