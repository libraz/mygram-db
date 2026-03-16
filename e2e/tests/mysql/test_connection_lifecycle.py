"""MySQL connection lifecycle tests.

Replaces C++ integration/mysql/connection_test.cpp with Python e2e tests.
Verifies basic MySQL connectivity, UUID consistency, and GTID availability.
"""

from __future__ import annotations

import pytest

from lib.mysql_client import MysqlClient

pytestmark = pytest.mark.mysql


class TestConnectionLifecycle:
    """Verify MySQL connection basics."""

    def test_mysql_ping(self, mysql: MysqlClient) -> None:
        """MySQL must be reachable."""
        assert mysql.ping()

    def test_server_uuid_consistent(self, mysql: MysqlClient) -> None:
        """Server UUID must be stable across queries."""
        r1 = mysql.execute("SELECT @@server_uuid as uuid")
        r2 = mysql.execute("SELECT @@server_uuid as uuid")
        assert r1[0]["uuid"] == r2[0]["uuid"]

    def test_gtid_executed_available(self, mysql: MysqlClient) -> None:
        """GTID executed set must be available."""
        result = mysql.execute("SELECT @@gtid_executed as gtid")
        assert result[0]["gtid"] is not None

    def test_reconnect_after_close(self, mysql: MysqlClient) -> None:
        """Fresh connection must work (MysqlClient reconnects per query)."""
        assert mysql.ping()
        result = mysql.execute("SELECT 1 as v")
        assert result[0]["v"] == 1
