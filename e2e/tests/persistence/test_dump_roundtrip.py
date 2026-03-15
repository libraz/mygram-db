"""Test dump save and load roundtrip."""

import pytest
import time

from lib.wait import wait_until

pytestmark = pytest.mark.persistence


class TestDumpRoundtrip:
    """Test DUMP SAVE and DUMP LOAD data preservation."""

    def test_dump_save_load(self, mygramdb, seed_data):
        """DUMP SAVE followed by DUMP LOAD should preserve data."""
        # Get current state
        info_before = mygramdb.info()
        doc_count_before = info_before.get("total_documents", info_before.get("doc_count", info_before.get("documents", 0)))
        assert doc_count_before > 0, "Need data to test dump"

        # Save dump and wait for completion (async: stops replication, writes, restarts)
        assert mygramdb.dump_save(), "DUMP SAVE should succeed"

        # Poll until DUMP LOAD succeeds (save must complete first)
        def _try_load():
            resp = mygramdb.tcp_command("DUMP LOAD mygramdb.dmp", timeout=60.0)
            return resp is not None and "OK" in resp

        wait_until(
            _try_load,
            timeout=60,
            interval=2,
            description="DUMP LOAD after DUMP SAVE",
        )
        time.sleep(3)

        # Verify data preserved
        info_after = mygramdb.info()
        doc_count_after = info_after.get("total_documents", info_after.get("doc_count", info_after.get("documents", 0)))
        assert doc_count_after == doc_count_before, (
            f"Doc count changed: before={doc_count_before}, after={doc_count_after}"
        )

    def test_search_after_dump_load(self, mysql, mygramdb, seed_data):
        """Search should work correctly after DUMP LOAD."""
        from lib.wait import wait_until_gte

        marker = "dump_search_marker"
        mysql.insert_rows("articles", [{
            "title": "Dump Search Test",
            "content": f"Content with {marker}",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }])

        wait_until_gte(
            lambda: mygramdb.count("articles", marker),
            minimum=1,
            timeout=10,
            interval=0.5,
            description="dump search data",
        )

        # Save and load
        mygramdb.dump_save()
        time.sleep(10)
        mygramdb.dump_load()
        time.sleep(3)

        # Search should still work
        count = mygramdb.count("articles", marker)
        assert count >= 1, "Search should work after DUMP LOAD"
