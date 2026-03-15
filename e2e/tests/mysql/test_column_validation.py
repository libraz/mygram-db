"""Column validation tests for PK/UK detection.

Replaces C++ ConnectionValidateIntegrationTest with Python e2e tests.
Verifies that INFORMATION_SCHEMA correctly reports key columns.
"""

from __future__ import annotations

import pytest

from lib.mysql_client import MysqlClient

pytestmark = pytest.mark.mysql

# Test database name from e2e config
TEST_DB = "testdb"


class TestColumnValidation:
    """Verify PK/UK column detection via INFORMATION_SCHEMA."""

    def test_primary_key_detected(self, mysql: MysqlClient) -> None:
        """articles.id must be detected as PRIMARY KEY."""
        result = mysql.execute(
            "SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.STATISTICS "
            "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = 'articles' "
            "AND COLUMN_NAME = 'id' AND INDEX_NAME = 'PRIMARY' "
            "AND SEQ_IN_INDEX = 1",
            (TEST_DB,),
        )
        assert result[0]["cnt"] == 1

    def test_unique_key_detected(self, mysql: MysqlClient) -> None:
        """UNIQUE KEY column must be detected."""
        mysql.execute("DROP TABLE IF EXISTS _e2e_validate_uk")
        mysql.execute(
            "CREATE TABLE _e2e_validate_uk "
            "(id INT, code VARCHAR(50) UNIQUE, name VARCHAR(100))"
        )
        try:
            result = mysql.execute(
                "SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.STATISTICS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = '_e2e_validate_uk' "
                "AND COLUMN_NAME = 'code' AND NON_UNIQUE = 0 "
                "AND SEQ_IN_INDEX = 1",
                (TEST_DB,),
            )
            assert result[0]["cnt"] >= 1
        finally:
            mysql.execute("DROP TABLE IF EXISTS _e2e_validate_uk")

    def test_composite_key_not_single(self, mysql: MysqlClient) -> None:
        """Composite PK columns must not be treated as single-column keys."""
        mysql.execute("DROP TABLE IF EXISTS _e2e_validate_comp")
        mysql.execute(
            "CREATE TABLE _e2e_validate_comp (a INT, b INT, PRIMARY KEY(a, b))"
        )
        try:
            total = mysql.execute(
                "SELECT COUNT(*) as cnt FROM INFORMATION_SCHEMA.STATISTICS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = '_e2e_validate_comp' "
                "AND INDEX_NAME = 'PRIMARY'",
                (TEST_DB,),
            )
            assert total[0]["cnt"] > 1  # Composite key = multiple records
        finally:
            mysql.execute("DROP TABLE IF EXISTS _e2e_validate_comp")
