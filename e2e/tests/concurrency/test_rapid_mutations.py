"""Test rapid mutation scenarios."""

import uuid

import pytest

from lib.wait import wait_until_value

pytestmark = pytest.mark.concurrency


class TestRapidMutations:
    """Test rapid update and churn scenarios."""

    def test_rapid_updates_same_row(self, mysql, mygramdb, seed_data):
        """After a burst of updates the index reflects only the final text.

        Each UPDATE must retract the previous text's n-grams as it applies the
        new ones. If a retraction is dropped under load the document stays
        matchable by superseded text, which a liveness check cannot see: the
        server is perfectly responsive while returning stale hits.
        """
        marker = f"rapid{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": f"Rapid Update Target {marker}",
                    "content": f"original content {marker} version init",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", f"{marker} version init"),
            expected=1,
            timeout=15,
            interval=0.5,
            description="initial row to be indexed",
        )

        updates = 20
        for i in range(updates):
            mysql.update(
                "articles",
                f"content = 'updated content {marker} version v{i}x'",
                f"title = 'Rapid Update Target {marker}'",
            )

        final = f"{marker} version v{updates - 1}x"
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", final),
            expected=1,
            timeout=20,
            interval=0.5,
            description="final update to be applied",
        )

        # The marker itself survives every revision, so the document must still
        # be a single document rather than one per revision.
        assert mygramdb.count("testdb.articles", marker) == 1, (
            "repeated updates produced duplicate documents for one row"
        )

        # Every superseded revision must have been retracted from the index.
        assert mygramdb.count("testdb.articles", f"{marker} version init") == 0, (
            "the pre-update text is still matchable"
        )
        for i in range(updates - 1):
            stale = f"{marker} version v{i}x"
            assert mygramdb.count("testdb.articles", stale) == 0, (
                f"superseded revision {i} is still matchable as {stale!r}"
            )

    def test_insert_delete_churn(self, mysql, mygramdb, seed_data):
        """Insert+delete cycles leave neither documents nor stale postings.

        The count going to zero only shows the documents are gone. Re-inserting
        one of the churned rows afterwards and requiring it to be found exactly
        once is what shows the postings were retracted rather than merely
        hidden behind a deleted-document check.
        """
        marker = f"churn{uuid.uuid4().hex[:8]}"
        cycles = 10
        for i in range(cycles):
            mysql.insert_rows(
                "articles",
                [
                    {
                        "title": f"Churn {marker} {i}",
                        "content": f"{marker} content {i}",
                        "status": 1,
                        "category": "tech",
                        "enabled": 1,
                    }
                ],
            )
            mysql.delete("articles", f"title = 'Churn {marker} {i}'")

        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=0,
            timeout=20,
            interval=0.5,
            description="churned documents to be removed",
        )

        # Re-insert one of the churned rows: it must be found exactly once.
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": f"Churn {marker} revived",
                    "content": f"{marker} content 0",
                    "status": 1,
                    "category": "tech",
                    "enabled": 1,
                }
            ],
        )
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=1,
            timeout=20,
            interval=0.5,
            description="re-inserted row to be indexed exactly once",
        )
