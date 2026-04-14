"""MySQL server configuration validation tests.

Replaces C++ ConnectionValidatorIntegrationTest with Python e2e tests.
Verifies that the MySQL server is configured correctly for binlog replication.
"""

from __future__ import annotations

import re

import pytest

from lib.mysql_client import MysqlClient

pytestmark = pytest.mark.mysql


class TestServerValidation:
    """Verify MySQL server settings required for binlog replication."""

    def test_gtid_mode_enabled(self, mysql: MysqlClient, db_flavor: str) -> None:
        """GTID mode must be ON for replication (MySQL) or always-on (MariaDB)."""
        if db_flavor == "mariadb":
            # MariaDB doesn't have gtid_mode variable; GTID is always enabled
            result = mysql.execute("SELECT @@gtid_current_pos AS pos")
            assert len(result) == 1
        else:
            result = mysql.execute("SHOW VARIABLES LIKE 'gtid_mode'")
            assert len(result) == 1
            assert result[0]["Value"] == "ON"

    def test_binlog_format_row(self, mysql: MysqlClient) -> None:
        """binlog_format must be ROW."""
        result = mysql.execute("SHOW VARIABLES LIKE 'binlog_format'")
        assert len(result) == 1
        assert result[0]["Value"].upper() == "ROW"

    def test_binlog_row_image_full(self, mysql: MysqlClient) -> None:
        """binlog_row_image must be FULL."""
        result = mysql.execute("SHOW VARIABLES LIKE 'binlog_row_image'")
        assert len(result) == 1
        assert result[0]["Value"].upper() == "FULL"

    def test_server_uuid_valid(self, mysql: MysqlClient, db_flavor: str) -> None:
        """Server UUID (MySQL) or server_id (MariaDB) must be valid."""
        if db_flavor == "mariadb":
            result = mysql.execute("SELECT @@server_id AS id")
            assert len(result) == 1
            assert int(result[0]["id"]) > 0
        else:
            result = mysql.execute("SHOW VARIABLES LIKE 'server_uuid'")
            assert len(result) == 1
            uuid_pattern = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
            assert re.match(uuid_pattern, result[0]["Value"])

    def test_required_tables_exist(self, mysql: MysqlClient) -> None:
        """Test database must have the articles table."""
        result = mysql.execute("SHOW TABLES LIKE 'articles'")
        assert len(result) > 0

    def test_missing_table_detected(self, mysql: MysqlClient) -> None:
        """Non-existent table should return empty result."""
        result = mysql.execute("SHOW TABLES LIKE 'nonexistent_xyz'")
        assert len(result) == 0
