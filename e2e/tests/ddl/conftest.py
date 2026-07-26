"""Isolation fixtures for destructive DDL tests."""

from collections.abc import Generator

import pytest

from lib.data_generator import DataGenerator

CREATE_ARTICLES_SQL = """
CREATE TABLE articles (
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


@pytest.fixture(autouse=True)
def restore_tracked_table_after_ddl(mysql, mygramdb, db_flavor) -> Generator[None, None, None]:
    """Recreate and re-sync the tracked table after every DDL test.

    The teardown is deliberately unconditional so a failed assertion or an
    interrupted cleanup cannot make the next test depend on execution order.
    """
    yield

    mysql.execute("DROP TABLE IF EXISTS articles_old")
    mysql.execute("DROP TABLE IF EXISTS articles")
    mysql.execute(CREATE_ARTICLES_SQL)
    if db_flavor == "mariadb":
        mysql.execute("ALTER TABLE articles ADD FULLTEXT INDEX ft_content (content)")
    else:
        mysql.execute(
            "ALTER TABLE articles ADD FULLTEXT INDEX ft_content (content) WITH PARSER ngram"
        )

    product_columns = mysql.execute("SHOW COLUMNS FROM products LIKE 'ddl_test_col'")
    if product_columns:
        mysql.execute("ALTER TABLE products DROP COLUMN ddl_test_col")

    rows = DataGenerator(seed=42).generate_articles(count=100)
    mysql.insert_rows("articles", rows)
    mygramdb.sync("testdb.articles", timeout=60)
