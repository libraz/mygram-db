"""Test ALTER TABLE handling."""

import contextlib
import uuid

import pytest

from lib.wait import wait_until, wait_until_value

pytestmark = pytest.mark.ddl


def _replication_status_value(mygramdb, key):
    """Read one field from the complete REPLICATION STATUS response."""
    response = mygramdb.replication_status()
    for line in response.splitlines():
        line = line.removeprefix("OK ")
        if line.startswith(f"{key}:"):
            return line.split(":", 1)[1].strip()
    raise AssertionError(f"{key} not found in REPLICATION STATUS:\n{response}")


def _insert_probe(mysql, marker: str, category: str = "tech") -> None:
    """Insert one row whose text carries a unique marker."""
    mysql.insert_rows(
        "articles",
        [
            {
                "title": "Alter Probe",
                "content": f"probe text {marker}",
                "status": 1,
                "category": category,
                "enabled": 1,
            }
        ],
    )


class TestAlterTable:
    """Test ALTER TABLE event handling.

    A DDL change rewrites the column layout that every subsequent ROWS event is
    decoded against. The risk is not that the server dies but that it keeps
    running while silently decoding later rows against the old layout, so each
    test writes a row *after* the DDL and requires it to become searchable with
    its filter values intact.
    """

    def test_add_column(self, mysql, mygramdb, seed_data):
        """Rows written after ADD COLUMN still replicate and index correctly."""
        marker = f"addcol{uuid.uuid4().hex[:8]}"
        before_probe = f"addcolpre{uuid.uuid4().hex[:8]}"
        _insert_probe(mysql, before_probe)
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", before_probe),
            expected=1,
            timeout=15,
            interval=0.5,
            description="pre-ALTER row to be indexed",
        )

        cols = mysql.execute("SHOW COLUMNS FROM articles LIKE 'test_col'")
        if not cols:
            mysql.execute("ALTER TABLE articles ADD COLUMN test_col VARCHAR(50) DEFAULT NULL")

        try:
            _insert_probe(mysql, marker)
            wait_until_value(
                lambda: mygramdb.count("testdb.articles", marker),
                expected=1,
                timeout=15,
                interval=0.5,
                description="row inserted after ADD COLUMN to be indexed",
            )
            # A trailing column must not shift the filter values of that row.
            assert mygramdb.count("testdb.articles", marker, filters={"category": "tech"}) == 1, (
                "filter values were misread after ADD COLUMN"
            )
            # The pre-ALTER row must remain reachable.
            assert mygramdb.count("testdb.articles", before_probe) == 1, (
                "a row indexed before ADD COLUMN disappeared"
            )
        finally:
            with contextlib.suppress(Exception):
                mysql.execute("ALTER TABLE articles DROP COLUMN test_col")

    def test_modify_configured_filter_column_fails_closed(self, mysql, mygramdb, seed_data):
        """Retyping a configured filter column stops replication, then recovers.

        A filter column carries the same risk as the text column: its width is
        part of the layout every later row image is decoded against, so
        continuing past the change would misread every column after it. Filter
        columns must therefore get the same fail-closed treatment, with the
        GTID frozen so the skipped rows can still be replayed by a SYNC.
        """
        marker = f"modcol{uuid.uuid4().hex[:8]}"
        before_gtid = _replication_status_value(mygramdb, "current_gtid")

        try:
            mysql.execute("ALTER TABLE articles MODIFY COLUMN category VARCHAR(100)")
            wait_until(
                lambda: _replication_status_value(mygramdb, "status") == "failed",
                timeout=20,
                interval=0.2,
                description="replication to fail closed on configured filter type change",
            )
            assert _replication_status_value(mygramdb, "last_error_code") == "2012"
            assert "category" in _replication_status_value(mygramdb, "last_error"), (
                "the error must name the column that changed"
            )
            assert _replication_status_value(mygramdb, "current_gtid") == before_gtid, (
                "the GTID advanced past the incompatible ALTER"
            )

            # Written while replication is stopped: it must not be lost, only
            # deferred until the recovery SYNC below.
            _insert_probe(mysql, marker, category="science")
        finally:
            with contextlib.suppress(Exception):
                mysql.execute("ALTER TABLE articles MODIFY COLUMN category VARCHAR(50)")
            mygramdb.sync("testdb.articles", timeout=60)

        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=1,
            timeout=30,
            interval=0.5,
            description="the deferred row to be loaded by the recovery sync",
        )
        science = mygramdb.count("testdb.articles", marker, filters={"category": "science"})
        assert science == 1, "the filter value was misread after the recovery sync"
        assert mygramdb.count("testdb.articles", marker, filters={"category": "tech"}) == 0, (
            "the row matched a category it does not have"
        )
