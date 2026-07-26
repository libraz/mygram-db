"""Test dump save and load roundtrip."""

import uuid

import pytest

from lib.wait import wait_until

pytestmark = pytest.mark.persistence


class TestDumpRoundtrip:
    """Test DUMP SAVE and DUMP LOAD data preservation."""

    @staticmethod
    def _stop_replication(mygramdb) -> None:
        """Stop replication and wait for the synchronous STOP response."""
        response = mygramdb.tcp_command("REPLICATION STOP", timeout=65.0)
        assert response is not None and "STOPPED" in response, (
            f"Failed to stop replication: {response}"
        )

    @staticmethod
    def _snapshot(mygramdb, marker: str) -> dict:
        """Capture independent observables for the UUID-scoped documents."""
        search = mygramdb.search("testdb.articles", marker, sort="id ASC", limit=100)
        filtered = mygramdb.search(
            "testdb.articles",
            marker,
            sort="id ASC",
            limit=100,
            filters={"status": 1},
        )
        return {
            "primary_keys": frozenset(search["ids"]),
            "search_total": search["total"],
            "count": mygramdb.count("testdb.articles", marker),
            "filtered_primary_keys": frozenset(filtered["ids"]),
            "filtered_total": filtered["total"],
            "facet_counts": mygramdb.facet("testdb.articles", "category", marker),
        }

    @staticmethod
    def _wait_for_dump_save(mygramdb) -> None:
        """Wait until the asynchronous save is complete and its slot is released."""

        def _save_completed() -> bool:
            status = mygramdb.tcp_command_multiline(
                "DUMP STATUS",
                timeout=5.0,
                terminator=b"END\r\n",
            )
            if status is not None and "status: FAILED" in status:
                raise AssertionError(f"DUMP SAVE failed:\n{status}")
            return (
                status is not None
                and "status: COMPLETED" in status
                and "save_in_progress: false" in status
            )

        wait_until(
            _save_completed,
            timeout=60,
            interval=0.25,
            description="DUMP SAVE completion",
        )

    def test_dump_save_load(self, mysql, mygramdb, seed_data):
        """DUMP LOAD restores the exact saved state without a masking SYNC."""
        marker = f"dump_roundtrip_{uuid.uuid4().hex}"
        cleanup_errors = []
        core_failed = False

        rows = [
            {
                "title": "Dump Roundtrip Tech One",
                "content": f"{marker} saved document one",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            },
            {
                "title": "Dump Roundtrip Tech Two",
                "content": f"{marker} saved document two",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            },
            {
                "title": "Dump Roundtrip Science",
                "content": f"{marker} saved document three",
                "status": 2,
                "category": "science",
                "enabled": 1,
            },
        ]

        try:
            mysql.insert_rows("articles", rows)
            marker_rows = mysql.execute(
                "SELECT id, status FROM articles WHERE content LIKE %s ORDER BY id",
                (f"%{marker}%",),
            )
            marker_ids = [int(row["id"]) for row in marker_rows]
            status_one_ids = {int(row["id"]) for row in marker_rows if int(row["status"]) == 1}
            assert len(marker_ids) == len(rows)

            # Establish the dump's GTID and make the marker state queryable once.
            assert mygramdb.sync("testdb.articles", timeout=30)
            self._stop_replication(mygramdb)

            state_a = self._snapshot(mygramdb, marker)
            assert state_a == {
                "primary_keys": frozenset(marker_ids),
                "search_total": 3,
                "count": 3,
                "filtered_primary_keys": frozenset(status_one_ids),
                "filtered_total": 2,
                "facet_counts": {"tech": 2, "science": 1},
            }

            assert mygramdb.dump_save(), "DUMP SAVE should start"
            self._wait_for_dump_save(mygramdb)

            # Build a distinctly different live state after the saved snapshot.
            mysql.execute(
                "DELETE FROM articles WHERE id IN (%s, %s)",
                (marker_ids[0], marker_ids[1]),
            )
            mysql.execute(
                "UPDATE articles SET status = %s, category = %s WHERE id = %s",
                (9, "archive", marker_ids[2]),
            )
            assert mygramdb.sync("testdb.articles", timeout=30)

            state_b = self._snapshot(mygramdb, marker)
            assert state_b == {
                "primary_keys": frozenset({marker_ids[2]}),
                "search_total": 1,
                "count": 1,
                "filtered_primary_keys": frozenset(),
                "filtered_total": 0,
                "facet_counts": {"archive": 1},
            }

            self._stop_replication(mygramdb)
            assert mygramdb.dump_load("mygramdb.dmp"), "DUMP LOAD should succeed"

            # Deliberately no SYNC here: these results must come from the dump.
            assert self._snapshot(mygramdb, marker) == state_a
        except BaseException:
            core_failed = True
            raise
        finally:
            try:
                mysql.execute(
                    "DELETE FROM articles WHERE content LIKE %s",
                    (f"%{marker}%",),
                )
            except Exception as exc:
                cleanup_errors.append(f"MySQL marker cleanup failed: {exc}")

            try:
                status = mygramdb.replication_status()
                if "running" not in status.lower():
                    response = mygramdb.tcp_command("REPLICATION START", timeout=30.0)
                    if response is None or not any(
                        token in response.lower() for token in ("started", "already", "running")
                    ):
                        assert mygramdb.sync("testdb.articles", timeout=30), (
                            f"Could not restore replication after response: {response}"
                        )

                wait_until(
                    lambda: mygramdb.count("testdb.articles", marker) == 0,
                    timeout=30,
                    interval=0.25,
                    description="dump marker cleanup convergence",
                )
            except Exception as exc:
                cleanup_errors.append(f"MygramDB cleanup convergence failed: {exc}")

            if cleanup_errors and not core_failed:
                pytest.fail("; ".join(cleanup_errors))
