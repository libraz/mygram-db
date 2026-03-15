"""Test document count consistency across endpoints."""

import pytest

pytestmark = pytest.mark.statistics


class TestDocumentCount:
    """Verify document counts are consistent across INFO, health, and metrics."""

    def test_info_health_consistency(self, mygramdb, seed_data):
        """INFO doc_count and health/detail should agree."""
        info = mygramdb.info()
        detail = mygramdb.health_detail()

        info_count = info.get("total_documents", info.get("doc_count", info.get("documents", -1)))
        # health_detail may have different key names
        detail_count = None
        for key in ["doc_count", "documents", "documents_total", "total_documents"]:
            if key in detail:
                detail_count = detail[key]
                break

        if detail_count is not None and info_count >= 0:
            assert info_count == detail_count, (
                f"INFO ({info_count}) != health/detail ({detail_count})"
            )
