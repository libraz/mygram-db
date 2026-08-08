"""Test replication stop and restart functionality."""

from __future__ import annotations

import contextlib
import uuid

import pytest

from lib.metrics import REPL_DDL_TOTAL, read_counter
from lib.wait import wait_until, wait_until_gte, wait_until_value

pytestmark = pytest.mark.replication


def _articles(marker: str, prefix: str, count: int) -> list[dict]:
    return [
        {
            "title": f"{prefix} {index}",
            "content": f"{prefix} {marker} item {index}",
            "status": 1,
            "category": "tech",
            "enabled": 1,
        }
        for index in range(count)
    ]


class TestStopRestart:
    """Verify replication stop/start behavior and data integrity.

    Stopping replication must be a clean pause rather than a data loss window:
    writes made while stopped have to be waiting in the binlog and land in full
    once the reader restarts. Each test therefore pins both halves -- nothing
    arrives while stopped, and everything arrives afterwards -- because a reader
    that silently skipped the gap would satisfy either half alone.
    """

    def _stop_replication(self, mygramdb):
        """Stop replication and confirm the reader is no longer consuming."""
        resp = mygramdb.tcp_command("REPLICATION STOP", timeout=65.0)
        assert resp is not None and "STOPPED" in resp, f"Failed to stop replication: {resp}"
        # STOP joins the reader thread before replying, so the state is settled
        # by the time the response arrives; assert it rather than sleeping.
        assert not mygramdb.replication_is_running(), (
            "REPLICATION STOP acknowledged while the reader is still running"
        )

    def _start_replication(self, mygramdb):
        """Start replication, tolerating an in-flight stop."""

        def _attempt() -> bool:
            resp = mygramdb.tcp_command("REPLICATION START", timeout=10.0)
            if resp is not None and "STARTED" in resp:
                return True
            if resp and "stopping" in resp.lower():
                return False
            raise AssertionError(f"Failed to start replication: {resp}")

        wait_until(_attempt, timeout=40, interval=1, description="replication to start")

    def _ensure_replication_running(self, mygramdb):
        """Best-effort restore of the running state for the next test."""
        with contextlib.suppress(Exception):
            if mygramdb.replication_is_running():
                return
            self._start_replication(mygramdb)

    def _assert_frozen_while_stopped(self, mygramdb, marker: str, gtid_before: str) -> None:
        """Nothing written after STOP may reach the index or move the position.

        Checking the count alone would also pass if replication were merely
        slow, so the consumed position has to be unchanged as well.
        """
        assert mygramdb.count("testdb.articles", marker) == 0, (
            "rows written while replication was stopped reached the index"
        )
        assert mygramdb.replication_gtid() == gtid_before, (
            "the consumed position advanced while replication was stopped"
        )

    def test_stop_insert_restart(self, mysql, mygramdb, seed_data):
        """INSERTs made while stopped are all applied after restart."""
        try:
            self._stop_replication(mygramdb)
            gtid_before = mygramdb.replication_gtid()

            marker = f"stoprestart{uuid.uuid4().hex[:8]}"
            rows = _articles(marker, "stop restart", 10)
            mysql.insert_rows("articles", rows)
            self._assert_frozen_while_stopped(mygramdb, marker, gtid_before)

            self._start_replication(mygramdb)
            wait_until_value(
                lambda: mygramdb.count("testdb.articles", marker),
                expected=len(rows),
                timeout=30,
                interval=0.5,
                description="every row written during the stop to be applied",
            )
        finally:
            self._ensure_replication_running(mygramdb)

    def test_stop_mixed_dml_restart(self, mysql, mygramdb, seed_data):
        """INSERT, UPDATE and DELETE made while stopped all land after restart."""
        marker = f"stopmix{uuid.uuid4().hex[:8]}"
        mysql.insert_rows("articles", _articles(marker, "stopmix seed", 5))
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", f"stopmix seed {marker}"),
            expected=5,
            timeout=20,
            interval=0.5,
            description="the seed rows to be indexed",
        )

        try:
            self._stop_replication(mygramdb)
            gtid_before = mygramdb.replication_gtid()

            mysql.insert_rows("articles", _articles(marker, "stopmix new", 3))
            for index in range(2):
                mysql.update(
                    "articles",
                    f"content = 'stopmix rewritten {marker} item {index}'",
                    f"content LIKE '%stopmix seed {marker} item {index}%'",
                )
            mysql.delete("articles", f"content LIKE '%stopmix seed {marker} item 4%'")
            self._assert_frozen_while_stopped(mygramdb, f"stopmix new {marker}", gtid_before)

            self._start_replication(mygramdb)

            # 5 seeded - 2 rewritten - 1 deleted leaves 2 of the original rows.
            wait_until_value(
                lambda: mygramdb.count("testdb.articles", f"stopmix seed {marker}"),
                expected=2,
                timeout=30,
                interval=0.5,
                description="the deleted and rewritten rows to leave the seed result set",
            )
            assert mygramdb.count("testdb.articles", f"stopmix new {marker}") == 3, (
                "inserts made during the stop did not all arrive"
            )
            assert mygramdb.count("testdb.articles", f"stopmix rewritten {marker}") == 2, (
                "updates made during the stop did not all arrive"
            )
            assert mygramdb.count("testdb.articles", f"stopmix seed {marker} item 4") == 0, (
                "the row deleted during the stop is still searchable"
            )
        finally:
            self._ensure_replication_running(mygramdb)

    def test_replication_status_reflects_state(self, mygramdb, seed_data):
        """The reported state tracks the stop and start commands.

        An operator gates deploys on this field, so it reporting "running"
        while the reader is stopped is worse than the reader being stopped.
        """
        try:
            assert mygramdb.replication_is_running(), "replication is not running to begin with"

            self._stop_replication(mygramdb)
            assert mygramdb.replication_field("status") == "stopped", (
                "the status field still claims replication is running after STOP"
            )

            self._start_replication(mygramdb)
            wait_until(
                mygramdb.replication_is_running,
                timeout=20,
                interval=0.25,
                description="the status field to report running again",
            )
        finally:
            self._ensure_replication_running(mygramdb)

    def test_a_restart_clears_the_previous_runs_error(self, mygramdb, seed_data):
        """A stop after a successful restart reports "stopped", not "failed".

        Tearing the stream down to honour STOP records an error on the reader.
        If a later successful START leaves it in place, every subsequent
        deliberate stop is reported as a failure and the status surface keeps
        naming a condition that has already been recovered from -- which is
        exactly what an operator would be paged on.
        """
        try:
            self._stop_replication(mygramdb)
            self._start_replication(mygramdb)

            assert mygramdb.replication_field("last_error") == "", (
                "the restart kept the error the previous run ended with"
            )
            assert mygramdb.replication_field("last_error_code") == "0", (
                "the restart kept the error code the previous run ended with"
            )

            self._stop_replication(mygramdb)
            assert mygramdb.replication_field("status") == "stopped", (
                "a deliberate stop is reported as a failure after a restart"
            )
        finally:
            self._ensure_replication_running(mygramdb)

    def test_rapid_stop_start_cycles(self, mysql, mygramdb, seed_data):
        """Replication still works after repeated stop/start cycles."""
        try:
            for _ in range(3):
                self._stop_replication(mygramdb)
                self._start_replication(mygramdb)

            marker = f"rapidcycle{uuid.uuid4().hex[:8]}"
            mysql.insert_rows("articles", _articles(marker, "rapid cycle", 1))
            wait_until_value(
                lambda: mygramdb.count("testdb.articles", marker),
                expected=1,
                timeout=20,
                interval=0.5,
                description="replication to still deliver rows after the cycles",
            )
        finally:
            self._ensure_replication_running(mygramdb)

    def test_stop_while_already_stopped_is_idempotent(self, mysql, mygramdb, seed_data):
        """A second STOP leaves replication stopped and restartable."""
        try:
            self._stop_replication(mygramdb)
            resp = mygramdb.tcp_command("REPLICATION STOP", timeout=65.0)
            assert resp is not None, "the second STOP produced no response"
            assert mygramdb.replication_field("status") == "stopped", (
                "the second STOP left the reported state inconsistent"
            )

            # The real risk is a repeated STOP wedging the reader, so require it
            # to still resume and deliver a row.
            self._start_replication(mygramdb)
            marker = f"doublestop{uuid.uuid4().hex[:8]}"
            mysql.insert_rows("articles", _articles(marker, "double stop", 1))
            wait_until_value(
                lambda: mygramdb.count("testdb.articles", marker),
                expected=1,
                timeout=20,
                interval=0.5,
                description="replication to resume after a repeated STOP",
            )
        finally:
            self._ensure_replication_running(mygramdb)

    def test_start_while_already_running_is_idempotent(self, mysql, mygramdb, seed_data):
        """A redundant START does not disturb a healthy reader."""
        assert mygramdb.replication_is_running()
        resp = mygramdb.tcp_command("REPLICATION START")
        assert resp is not None, "the redundant START produced no response"
        assert mygramdb.replication_is_running(), "a redundant START stopped the reader"

        marker = f"doublestart{uuid.uuid4().hex[:8]}"
        mysql.insert_rows("articles", _articles(marker, "double start", 1))
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=1,
            timeout=20,
            interval=0.5,
            description="replication to keep delivering rows after a redundant START",
        )

    def test_accumulated_changes_ordering(self, mysql, mygramdb, seed_data):
        """A long backlog accumulated during a stop is applied in full."""
        try:
            self._stop_replication(mygramdb)
            gtid_before = mygramdb.replication_gtid()

            marker = f"accum{uuid.uuid4().hex[:8]}"
            rows = _articles(marker, "accumulated", 20)
            mysql.insert_rows("articles", rows)
            self._assert_frozen_while_stopped(mygramdb, marker, gtid_before)

            self._start_replication(mygramdb)
            wait_until_value(
                lambda: mygramdb.count("testdb.articles", marker),
                expected=len(rows),
                timeout=40,
                interval=0.5,
                description="the whole backlog to be applied",
            )
        finally:
            self._ensure_replication_running(mygramdb)

    def test_stop_ddl_restart(self, mysql, mygramdb, seed_data):
        """A DDL and an INSERT queued during a stop are both replayed."""
        column = f"stopddl_{uuid.uuid4().hex[:6]}"
        try:
            ddl_before = read_counter(mygramdb, REPL_DDL_TOTAL)
            self._stop_replication(mygramdb)

            mysql.execute(f"ALTER TABLE articles ADD COLUMN {column} VARCHAR(50) DEFAULT NULL")
            marker = f"stopddl{uuid.uuid4().hex[:8]}"
            mysql.insert_rows("articles", _articles(marker, "stop ddl", 1))

            self._start_replication(mygramdb)
            wait_until_value(
                lambda: mygramdb.count("testdb.articles", marker),
                expected=1,
                timeout=30,
                interval=0.5,
                description="the row queued behind the DDL to be indexed",
            )
            # The row arriving is not enough: the DDL ahead of it must have been
            # replayed too, or later row images decode against a stale layout.
            wait_until_gte(
                lambda: read_counter(mygramdb, REPL_DDL_TOTAL) - ddl_before,
                minimum=1,
                timeout=20,
                interval=0.25,
                description="the queued DDL to be replayed",
            )
            # A trailing column must not shift the filter values of that row.
            assert mygramdb.count("testdb.articles", marker, filters={"category": "tech"}) == 1, (
                "filter values were misread after replaying the queued DDL"
            )
        finally:
            self._ensure_replication_running(mygramdb)
            with contextlib.suppress(Exception):
                mysql.execute(f"ALTER TABLE articles DROP COLUMN {column}")
