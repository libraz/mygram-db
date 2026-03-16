"""Test MySQL restart recovery."""

import subprocess
import uuid

import pytest

from lib.wait import wait_until

pytestmark = pytest.mark.resilience

MYSQL_CONTAINER = "inttest_mysql"


class TestMySQLRestart:
    """Test MygramDB recovery after MySQL restart."""

    def test_reconnect_after_restart(self, mysql, mygramdb, seed_data):
        """MygramDB should reconnect after MySQL restart."""
        # Verify initial state
        assert mygramdb.ping(), "MygramDB should be running"

        # Restart MySQL
        subprocess.run(
            ["docker", "restart", MYSQL_CONTAINER],
            check=True,
            capture_output=True,
        )

        # Wait for MySQL to come back
        wait_until(
            lambda: mysql.ping(),
            timeout=60,
            interval=2,
            description="MySQL to restart",
        )

        # Wait for MygramDB to reconnect
        wait_until(
            lambda: mygramdb.ping(),
            timeout=60,
            interval=2,
            description="MygramDB to reconnect",
        )

        # Verify MygramDB is functional after reconnect
        info = mygramdb.info()
        assert isinstance(info, dict)
        assert len(info) > 0

    def test_data_preserved_after_restart(self, mysql, mygramdb, seed_data):
        """Data should be preserved after MySQL restart."""
        from lib.wait import wait_until_gte

        marker = f"rstprsv_{uuid.uuid4().hex[:8]}"
        mysql.insert_rows("articles", [{
            "title": "Restart Test",
            "content": f"Content with {marker}",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])

        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=1,
            timeout=30,
            interval=1,
            description="restart test data",
        )

        # Restart MySQL
        subprocess.run(
            ["docker", "restart", MYSQL_CONTAINER],
            check=True,
            capture_output=True,
        )

        # Wait for recovery
        wait_until(
            lambda: mysql.ping(),
            timeout=60,
            interval=2,
            description="MySQL recovery",
        )
        wait_until(
            lambda: mygramdb.ping(),
            timeout=60,
            interval=2,
            description="MygramDB recovery",
        )

        # Data should still be searchable
        count = mygramdb.count("articles", marker)
        assert count >= 1, "Data should be preserved after MySQL restart"
