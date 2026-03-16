"""Test ALTER TABLE handling."""

import pytest

pytestmark = pytest.mark.ddl


class TestAlterTable:
    """Test ALTER TABLE event handling."""

    def test_add_column(self, mysql, mygramdb, seed_data):
        """ALTER TABLE ADD COLUMN should not crash MygramDB."""
        # Add a new column
        # Check if column already exists before adding
        cols = mysql.execute("SHOW COLUMNS FROM articles LIKE 'test_col'")
        if not cols:
            mysql.execute(
                "ALTER TABLE articles ADD COLUMN test_col VARCHAR(50) DEFAULT NULL"
            )
        import time
        time.sleep(3)

        # MygramDB should still be functional
        assert mygramdb.ping()
        info = mygramdb.info()
        assert isinstance(info, dict)

        # Clean up
        try:
            mysql.execute("ALTER TABLE articles DROP COLUMN test_col")
        except Exception:
            pass
        time.sleep(2)

    def test_modify_column(self, mysql, mygramdb, seed_data):
        """ALTER TABLE MODIFY COLUMN should not crash MygramDB."""
        mysql.execute(
            "ALTER TABLE articles MODIFY COLUMN category VARCHAR(100)"
        )
        import time
        time.sleep(3)
        assert mygramdb.ping()

        # Revert
        mysql.execute(
            "ALTER TABLE articles MODIFY COLUMN category VARCHAR(50)"
        )
        time.sleep(2)
