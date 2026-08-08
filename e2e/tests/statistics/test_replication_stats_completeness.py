"""Test replication statistics completeness for UPDATE, DELETE, and DDL counters."""

from __future__ import annotations

import contextlib
import uuid

import pytest

from lib.metrics import (
    REPL_DDL_TOTAL,
    REPL_DELETES_APPLIED,
    REPL_DELETES_SKIPPED,
    REPL_INSERTS_APPLIED,
    REPL_INSERTS_SKIPPED,
    REPL_UPDATES_ADDED,
    REPL_UPDATES_APPLIED,
    REPL_UPDATES_MODIFIED,
    REPL_UPDATES_REMOVED,
    REPL_UPDATES_SKIPPED,
    MetricsSnapshot,
    read_counter,
)
from lib.wait import wait_until, wait_until_gte, wait_until_value

pytestmark = pytest.mark.statistics

# Every counter this module reasons about, so a test can assert what did *not*
# move as precisely as what did.
ALL_REPL_COUNTERS = (
    REPL_INSERTS_APPLIED,
    REPL_INSERTS_SKIPPED,
    REPL_UPDATES_APPLIED,
    REPL_UPDATES_ADDED,
    REPL_UPDATES_REMOVED,
    REPL_UPDATES_MODIFIED,
    REPL_UPDATES_SKIPPED,
    REPL_DELETES_APPLIED,
    REPL_DELETES_SKIPPED,
    REPL_DDL_TOTAL,
)


def _capture(mygramdb) -> dict[str, float]:
    """Read every replication counter at once, failing if any is missing."""
    snapshot = MetricsSnapshot.capture(mygramdb)
    missing = [name for name in ALL_REPL_COUNTERS if name not in snapshot.data]
    if missing:
        raise AssertionError(f"/metrics does not export: {missing}")
    return {name: snapshot.data[name] for name in ALL_REPL_COUNTERS}


def _await_increase(mygramdb, metric: str, baseline: float, *, by: int, description: str) -> None:
    """Wait for one counter to advance by at least ``by`` from ``baseline``.

    Waiting on the counter under test is what makes the assertion real: a
    counter that never moves fails here instead of being skipped for lack of a
    matching metric name.
    """
    wait_until_gte(
        lambda: read_counter(mygramdb, metric) - baseline,
        minimum=by,
        timeout=20,
        interval=0.25,
        description=description,
    )


def _replication_gtid(mygramdb) -> str:
    """Read the position replication has consumed up to."""
    response = mygramdb.replication_status()
    for line in response.splitlines():
        line = line.removeprefix("OK ")
        if line.startswith("current_gtid:"):
            return line.split(":", 1)[1].strip()
    raise AssertionError(f"current_gtid not found in REPLICATION STATUS:\n{response}")


def _insert_article(mysql, marker: str, *, enabled: int) -> None:
    mysql.insert_rows(
        "articles",
        [
            {
                "title": "Replication Counter Probe",
                "content": f"probe content {marker}",
                "status": 1,
                "category": "tech",
                "enabled": enabled,
            }
        ],
    )


class TestReplicationStatsCompleteness:
    """Verify replication counters for UPDATE, DELETE, DDL, and skipped events.

    An operator uses these counters to tell "replication is idle" from
    "replication is dropping my rows", so each test pins the exact counter the
    event is supposed to move and requires the neighbouring counters to stay
    put. Counting only "some update metric went up" would pass even if every
    update were classified as skipped.
    """

    def test_update_of_indexed_text_counts_as_modified(self, mysql, mygramdb, seed_data):
        """Rewriting the text of an indexed row is an applied+modified update."""
        marker = f"updmod{uuid.uuid4().hex[:8]}"
        _insert_article(mysql, marker, enabled=1)
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=1,
            timeout=15,
            interval=0.5,
            description="the probe row to be indexed",
        )

        before = _capture(mygramdb)
        new_marker = f"{marker}rewritten"
        mysql.update(
            "articles", f"content = 'rewritten {new_marker}'", f"content LIKE '%{marker}%'"
        )
        _await_increase(
            mygramdb,
            REPL_UPDATES_MODIFIED,
            before[REPL_UPDATES_MODIFIED],
            by=1,
            description="the modified-update counter to advance",
        )

        after = _capture(mygramdb)
        assert after[REPL_UPDATES_APPLIED] - before[REPL_UPDATES_APPLIED] >= 1, (
            "a modified update did not count towards applied updates"
        )
        assert after[REPL_UPDATES_ADDED] == before[REPL_UPDATES_ADDED], (
            "an in-place text change was counted as a newly added row"
        )
        assert after[REPL_UPDATES_REMOVED] == before[REPL_UPDATES_REMOVED], (
            "an in-place text change was counted as a removal"
        )
        assert after[REPL_UPDATES_SKIPPED] == before[REPL_UPDATES_SKIPPED], (
            "an update to an indexed row was also counted as skipped"
        )
        # The counter must describe work the index actually did.
        assert mygramdb.count("testdb.articles", new_marker) == 1, (
            "the update was counted but the new text is not searchable"
        )

    def test_update_into_the_filter_counts_as_added(self, mysql, mygramdb, seed_data):
        """enabled 0 -> 1 is an applied+added update, not a modification."""
        marker = f"updadd{uuid.uuid4().hex[:8]}"
        skipped_before = read_counter(mygramdb, REPL_INSERTS_SKIPPED)
        _insert_article(mysql, marker, enabled=0)
        # The row fails the required filter, so it is never searchable and the
        # skipped-insert counter is the only signal that it has been applied.
        _await_increase(
            mygramdb,
            REPL_INSERTS_SKIPPED,
            skipped_before,
            by=1,
            description="the filtered-out insert to be processed",
        )

        before = _capture(mygramdb)
        mysql.update("articles", "enabled = 1", f"content LIKE '%{marker}%'")
        _await_increase(
            mygramdb,
            REPL_UPDATES_ADDED,
            before[REPL_UPDATES_ADDED],
            by=1,
            description="the added-update counter to advance",
        )

        after = _capture(mygramdb)
        assert after[REPL_UPDATES_APPLIED] - before[REPL_UPDATES_APPLIED] >= 1, (
            "an added update did not count towards applied updates"
        )
        assert after[REPL_UPDATES_MODIFIED] == before[REPL_UPDATES_MODIFIED], (
            "a row entering the filter was counted as an in-place modification"
        )
        assert after[REPL_UPDATES_REMOVED] == before[REPL_UPDATES_REMOVED], (
            "a row entering the filter was counted as a removal"
        )
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=1,
            timeout=15,
            interval=0.5,
            description="the row to become searchable once it passes the filter",
        )

    def test_update_out_of_the_filter_counts_as_removed(self, mysql, mygramdb, seed_data):
        """enabled 1 -> 0 is an applied+removed update and drops the row."""
        marker = f"updrem{uuid.uuid4().hex[:8]}"
        _insert_article(mysql, marker, enabled=1)
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=1,
            timeout=15,
            interval=0.5,
            description="the probe row to be indexed",
        )

        before = _capture(mygramdb)
        mysql.update("articles", "enabled = 0", f"content LIKE '%{marker}%'")
        _await_increase(
            mygramdb,
            REPL_UPDATES_REMOVED,
            before[REPL_UPDATES_REMOVED],
            by=1,
            description="the removed-update counter to advance",
        )

        after = _capture(mygramdb)
        assert after[REPL_UPDATES_APPLIED] - before[REPL_UPDATES_APPLIED] >= 1, (
            "a removed update did not count towards applied updates"
        )
        assert after[REPL_UPDATES_ADDED] == before[REPL_UPDATES_ADDED], (
            "a row leaving the filter was counted as an addition"
        )
        # A counted removal that leaves the row searchable is the failure that
        # matters — the statistic would then be actively misleading.
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=0,
            timeout=15,
            interval=0.5,
            description="the row to leave the index once it fails the filter",
        )

    def test_update_of_a_filtered_out_row_counts_as_skipped(self, mysql, mygramdb, seed_data):
        """Touching a row that stays outside the filter is skipped, not applied."""
        marker = f"updskip{uuid.uuid4().hex[:8]}"
        skipped_before = read_counter(mygramdb, REPL_INSERTS_SKIPPED)
        _insert_article(mysql, marker, enabled=0)
        _await_increase(
            mygramdb,
            REPL_INSERTS_SKIPPED,
            skipped_before,
            by=1,
            description="the filtered-out insert to be processed",
        )

        before = _capture(mygramdb)
        mysql.update("articles", "status = 2", f"content LIKE '%{marker}%'")
        _await_increase(
            mygramdb,
            REPL_UPDATES_SKIPPED,
            before[REPL_UPDATES_SKIPPED],
            by=1,
            description="the skipped-update counter to advance",
        )

        after = _capture(mygramdb)
        assert after[REPL_UPDATES_APPLIED] == before[REPL_UPDATES_APPLIED], (
            "an update to a filtered-out row was counted as applied index work"
        )
        assert mygramdb.count("testdb.articles", marker) == 0, (
            "a row that never passed the filter became searchable"
        )

    def test_delete_of_an_indexed_row_counts_as_applied(self, mysql, mygramdb, seed_data):
        """DELETE of an indexed row is an applied delete and frees the row."""
        marker = f"delapp{uuid.uuid4().hex[:8]}"
        _insert_article(mysql, marker, enabled=1)
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=1,
            timeout=15,
            interval=0.5,
            description="the probe row to be indexed",
        )

        before = _capture(mygramdb)
        mysql.delete("articles", f"content LIKE '%{marker}%'")
        _await_increase(
            mygramdb,
            REPL_DELETES_APPLIED,
            before[REPL_DELETES_APPLIED],
            by=1,
            description="the applied-delete counter to advance",
        )

        after = _capture(mygramdb)
        assert after[REPL_DELETES_SKIPPED] == before[REPL_DELETES_SKIPPED], (
            "a delete of an indexed row was also counted as skipped"
        )
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=0,
            timeout=15,
            interval=0.5,
            description="the deleted row to leave the index",
        )

    def test_delete_of_a_filtered_out_row_counts_as_skipped(self, mysql, mygramdb, seed_data):
        """DELETE of a row that was never indexed is skipped, not applied."""
        marker = f"delskip{uuid.uuid4().hex[:8]}"
        skipped_before = read_counter(mygramdb, REPL_INSERTS_SKIPPED)
        _insert_article(mysql, marker, enabled=0)
        _await_increase(
            mygramdb,
            REPL_INSERTS_SKIPPED,
            skipped_before,
            by=1,
            description="the filtered-out insert to be processed",
        )

        before = _capture(mygramdb)
        mysql.delete("articles", f"content LIKE '%{marker}%'")
        _await_increase(
            mygramdb,
            REPL_DELETES_SKIPPED,
            before[REPL_DELETES_SKIPPED],
            by=1,
            description="the skipped-delete counter to advance",
        )

        after = _capture(mygramdb)
        assert after[REPL_DELETES_APPLIED] == before[REPL_DELETES_APPLIED], (
            "deleting a row that was never indexed was counted as applied work"
        )

    def test_ddl_executed_counter(self, mysql, mygramdb, seed_data):
        """Each replicated ALTER TABLE advances the DDL counter exactly once."""
        column = f"ddlcol_{uuid.uuid4().hex[:6]}"

        before = _capture(mygramdb)
        mysql.execute(f"ALTER TABLE articles ADD COLUMN {column} VARCHAR(50) DEFAULT NULL")
        _await_increase(
            mygramdb,
            REPL_DDL_TOTAL,
            before[REPL_DDL_TOTAL],
            by=1,
            description="the DDL counter to advance for ADD COLUMN",
        )

        after_add = _capture(mygramdb)
        assert after_add[REPL_DDL_TOTAL] - before[REPL_DDL_TOTAL] == 1, (
            "one ALTER TABLE moved the DDL counter by more than one"
        )

        with contextlib.suppress(Exception):
            mysql.execute(f"ALTER TABLE articles DROP COLUMN {column}")
            _await_increase(
                mygramdb,
                REPL_DDL_TOTAL,
                after_add[REPL_DDL_TOTAL],
                by=1,
                description="the DDL counter to advance for DROP COLUMN",
            )

    def test_writes_to_untracked_tables_advance_gtid_without_indexing(
        self, mysql, mygramdb, seed_data
    ):
        """A write to a table outside the config is consumed but never indexed.

        ``edge_cases`` exists in MySQL but is not configured. Its transaction
        must still be consumed -- stalling on it would freeze replication for
        every table -- while leaving the index untouched. Requiring the GTID to
        advance is what separates "correctly ignored" from "never delivered",
        which an index-only assertion cannot tell apart.
        """
        marker = f"untracked{uuid.uuid4().hex[:8]}"
        gtid_before = _replication_gtid(mygramdb)
        articles_before = mygramdb.count("testdb.articles", "test")

        mysql.insert_rows("edge_cases", [{"content": f"untracked content {marker}"}])
        wait_until(
            lambda: _replication_gtid(mygramdb) != gtid_before,
            timeout=20,
            interval=0.25,
            description="replication to consume the untracked-table transaction",
        )

        assert mygramdb.count("testdb.articles", marker) == 0, (
            "a row from an untracked table was indexed into articles"
        )
        assert mygramdb.count("testdb.articles", "test") == articles_before, (
            "a write to an untracked table changed the articles index"
        )

    def test_applied_updates_decompose_into_added_removed_modified(
        self, mysql, mygramdb, seed_data
    ):
        """A mixed workload keeps applied == added + removed + modified.

        The three outcome counters partition the applied ones, so an event
        classified twice or lost between classifications shows up here as an
        arithmetic mismatch rather than as a plausible-looking total.
        """
        marker = f"mixed{uuid.uuid4().hex[:8]}"
        before = _capture(mygramdb)

        rows = [
            {
                "title": f"Mixed {index}",
                "content": f"mixed content {marker} item {index}",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
            for index in range(5)
        ]
        mysql.insert_rows("articles", rows)
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=5,
            timeout=20,
            interval=0.5,
            description="all five rows to be indexed",
        )

        for index in range(3):
            mysql.update(
                "articles",
                f"content = 'mixed rewritten {marker} item {index}'",
                f"content LIKE '%{marker} item {index}%'",
            )
        _await_increase(
            mygramdb,
            REPL_UPDATES_MODIFIED,
            before[REPL_UPDATES_MODIFIED],
            by=3,
            description="all three text updates to be applied",
        )

        mysql.delete("articles", f"content LIKE '%{marker} item 3%'")
        mysql.delete("articles", f"content LIKE '%{marker} item 4%'")
        _await_increase(
            mygramdb,
            REPL_DELETES_APPLIED,
            before[REPL_DELETES_APPLIED],
            by=2,
            description="both deletes to be applied",
        )

        after = _capture(mygramdb)
        assert after[REPL_INSERTS_APPLIED] - before[REPL_INSERTS_APPLIED] >= 5
        applied = after[REPL_UPDATES_APPLIED] - before[REPL_UPDATES_APPLIED]
        parts = (
            after[REPL_UPDATES_ADDED]
            - before[REPL_UPDATES_ADDED]
            + after[REPL_UPDATES_REMOVED]
            - before[REPL_UPDATES_REMOVED]
            + after[REPL_UPDATES_MODIFIED]
            - before[REPL_UPDATES_MODIFIED]
        )
        assert applied == parts, (
            f"applied updates ({applied}) do not decompose into added+removed+modified ({parts})"
        )
        assert mygramdb.count("testdb.articles", marker) == 3, (
            "the surviving rows do not match the applied insert and delete counts"
        )
