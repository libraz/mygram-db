"""Test replication of MySQL tagged GTIDs across a stop and restart.

A transaction committed with ``SET gtid_next='AUTOMATIC:<tag>'`` is logged as
GTID_TAGGED_LOG_EVENT and enters the executed set as ``uuid:tag:N``. Resuming
after a stop sends that set back in COM_BINLOG_DUMP_GTID, which takes the tagged
encoding, so a wrong encoding shows up as a tagged transaction delivered twice
or never.

Requires MySQL >= 8.4 (skipped on MariaDB and older MySQL).
"""

from __future__ import annotations

import uuid

import pytest

from lib.wait import wait_until, wait_until_value

pytestmark = [
    pytest.mark.replication,
    pytest.mark.mysql_only,
]

GTID_TAG = "mygram_e2e"


def _skip_unless_tagged_gtids(mysql_version: tuple[int, int], db_flavor: str) -> None:
    if db_flavor == "mariadb":
        pytest.skip("tagged GTIDs are MySQL-only")
    if mysql_version < (8, 4):
        pytest.skip("tagged GTIDs require MySQL >= 8.4")


def _insert_tagged(mysql, marker: str, prefix: str) -> None:
    """Insert one article in its own transaction under the tagged automatic GTID.

    gtid_next is a session variable, so the SET and the INSERT share one connection.
    """
    conn = mysql._connect()
    try:
        cursor = conn.cursor()
        cursor.execute(f"SET gtid_next = 'AUTOMATIC:{GTID_TAG}'")
        cursor.execute(
            "INSERT INTO articles (title, content, status, category, enabled) "
            "VALUES (%s, %s, 1, 'tech', 1)",
            (f"{prefix} tagged", f"{prefix} {marker} tagged row"),
        )
        cursor.execute("SET gtid_next = 'AUTOMATIC'")
        cursor.close()
    finally:
        conn.close()


class TestTaggedGtid:
    """A tagged transaction is applied exactly once across a stop and restart."""

    def _stop_replication(self, mygramdb) -> None:
        resp = mygramdb.tcp_command("REPLICATION STOP", timeout=65.0)
        assert resp is not None and "STOPPED" in resp, f"Failed to stop replication: {resp}"
        assert not mygramdb.replication_is_running(), (
            "REPLICATION STOP acknowledged while the reader is still running"
        )

    def _start_replication(self, mygramdb) -> None:
        def _attempt() -> bool:
            resp = mygramdb.tcp_command("REPLICATION START", timeout=10.0)
            if resp is not None and "STARTED" in resp:
                return True
            if resp and "stopping" in resp.lower():
                return False
            raise AssertionError(f"Failed to start replication: {resp}")

        wait_until(_attempt, timeout=40, interval=1, description="replication to start")

    def test_tagged_transaction_resumes_exactly_once(
        self, mysql, mygramdb, mysql_version, db_flavor, seed_data
    ):
        """Tagged transactions before and during a stop each land once after restart.

        The first tagged row is deleted again before the stop, so a resume
        position that failed to exclude its transaction would bring it back.
        """
        _skip_unless_tagged_gtids(mysql_version, db_flavor)
        marker = f"taggedgtid{uuid.uuid4().hex[:8]}"

        _insert_tagged(mysql, marker, "before")
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", f"before {marker}"),
            expected=1,
            timeout=30,
            interval=0.5,
            description="the tagged row written before the stop to be indexed",
        )
        assert f":{GTID_TAG}:" in mygramdb.replication_gtid(), (
            "the applied position does not carry the tagged GTID"
        )

        mysql.delete("articles", f"content LIKE '%before {marker}%'")
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", f"before {marker}"),
            expected=0,
            timeout=30,
            interval=0.5,
            description="the tagged row to be deleted from the index",
        )

        try:
            self._stop_replication(mygramdb)
            gtid_before = mygramdb.replication_gtid()

            _insert_tagged(mysql, marker, "during")
            assert mygramdb.count("testdb.articles", f"during {marker}") == 0, (
                "a row written while replication was stopped reached the index"
            )
            assert mygramdb.replication_gtid() == gtid_before, (
                "the consumed position advanced while replication was stopped"
            )

            self._start_replication(mygramdb)
            wait_until_value(
                lambda: mygramdb.count("testdb.articles", f"during {marker}"),
                expected=1,
                timeout=30,
                interval=0.5,
                description="the tagged row written during the stop to be applied",
            )
            assert mygramdb.count("testdb.articles", f"before {marker}") == 0, (
                "the tagged transaction applied before the stop was delivered again"
            )
            assert mygramdb.replication_is_running(), (
                "replication stopped after the tagged transaction"
            )
            assert mygramdb.replication_field("last_error_code") == "0", (
                f"replication reported an error: {mygramdb.replication_field('last_error')}"
            )
        finally:
            if not mygramdb.replication_is_running():
                self._start_replication(mygramdb)
