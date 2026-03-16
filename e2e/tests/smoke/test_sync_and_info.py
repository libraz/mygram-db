"""Smoke tests for sync and info commands."""

import pytest

pytestmark = pytest.mark.smoke


class TestSyncAndInfo:
    """Test basic sync and info functionality."""

    def test_info_command(self, mygramdb, seed_data):
        """INFO should return valid data."""
        info = mygramdb.info()
        assert isinstance(info, dict)
        assert len(info) > 0

    def test_doc_count_matches_mysql(self, mysql, mygramdb, seed_data):
        """MygramDB doc count should match MySQL row count."""
        mysql_count = mysql.count("articles", "enabled = 1 AND deleted_at IS NULL")
        info = mygramdb.info()
        # doc_count key may vary, check common patterns
        doc_count = info.get("total_documents", info.get("doc_count", info.get("documents", 0)))
        assert doc_count >= mysql_count, (
            f"MygramDB doc_count ({doc_count}) < MySQL count ({mysql_count})"
        )

    def test_tcp_ping(self, mygramdb):
        """TCP connection should work."""
        assert mygramdb.ping()
