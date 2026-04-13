"""Test dump save and load roundtrip."""

import time

import pytest

from lib.wait import wait_until

pytestmark = pytest.mark.persistence


class TestDumpRoundtrip:
    """Test DUMP SAVE and DUMP LOAD data preservation."""

    def test_dump_save_load(self, mygramdb, seed_data):
        """DUMP SAVE followed by DUMP LOAD should preserve data."""

        def _get_doc_count():
            info = mygramdb.info()
            return info.get("total_documents", info.get("doc_count", info.get("documents", 0)))

        # Wait for replication to catch up so baseline is stable.
        # Seed data inserts happen before this test; replication may still be
        # processing events. Two consecutive identical counts = stable.
        prev_count = -1
        for _ in range(15):
            cur = _get_doc_count()
            if cur == prev_count:
                break
            prev_count = cur
            time.sleep(1)

        doc_count_before = _get_doc_count()
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

        # Wait for replication to catch up after DUMP LOAD restart.
        # DUMP LOAD restores the dump snapshot and restarts replication from
        # the dump's GTID, so events after that point are re-consumed.
        prev_count = -1
        for _ in range(15):
            cur = _get_doc_count()
            if cur == prev_count:
                break
            prev_count = cur
            time.sleep(1)

        # Verify data preserved: after catch-up the count should match the
        # stable baseline (same MySQL state, same GTID convergence point).
        doc_count_after = _get_doc_count()
        assert doc_count_after >= doc_count_before, (
            f"Data lost after DUMP LOAD: before={doc_count_before}, after={doc_count_after}"
        )

    def test_search_after_dump_load(self, mysql, mygramdb, seed_data):
        """Search should work correctly after DUMP LOAD."""
        marker = "dump_search_marker"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Dump Search Test",
                    "content": f"Content with {marker}",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )

        # Sync to ensure marker data is in the index
        mygramdb.sync("articles", timeout=30)
        time.sleep(1)

        count_before = mygramdb.count("articles", marker)
        assert count_before >= 1, f"Marker should exist after sync, got {count_before}"

        # Save dump (async, writes to file then resumes replication)
        mygramdb.dump_save()
        time.sleep(15)

        # Load the dump
        def _try_load():
            resp = mygramdb.tcp_command("DUMP LOAD mygramdb.dmp", timeout=60.0)
            return resp is not None and "OK" in resp

        wait_until(
            _try_load,
            timeout=30,
            interval=2,
            description="dump load after save",
        )

        # After dump load, sync to ensure index is rebuilt
        mygramdb.sync("articles", timeout=30)
        time.sleep(2)

        # Search should still work
        count = mygramdb.count("articles", marker)
        assert count >= 1, f"Search should work after DUMP LOAD, got {count}"
