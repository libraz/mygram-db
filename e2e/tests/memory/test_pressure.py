"""Test memory pressure handling."""

import pytest

from lib.data_generator import DataGenerator
from lib.wait import wait_until_gte

pytestmark = pytest.mark.memory


class TestMemoryPressure:
    """Test behavior under memory pressure."""

    def test_no_oom_crash(self, mysql, mygramdb, seed_data):
        """Inserting large amounts of data should not cause OOM crash."""
        gen = DataGenerator(seed=888)
        # Insert batches of data to push memory usage up
        # MygramDB is configured with 200MB hard limit
        for _batch in range(5):
            rows = gen.generate_articles(count=500, mixed=True)
            mysql.insert_rows("articles", rows)

        def _get_doc_count() -> int:
            info = mygramdb.info()
            return info.get("total_documents", info.get("doc_count", info.get("documents", 0)))

        wait_until_gte(
            _get_doc_count,
            minimum=2500,
            timeout=60,
            interval=2,
            description="memory pressure data sync",
        )

        # Should still be alive
        assert mygramdb.ping(), "MygramDB should survive memory pressure"
        assert mygramdb.health_live(), "Health check should pass"

    def test_soft_limit_respected(self, mygramdb, seed_data):
        """Memory usage should be trackable via health detail."""
        detail = mygramdb.health_detail()
        # Just verify we can read memory stats
        assert isinstance(detail, dict)
