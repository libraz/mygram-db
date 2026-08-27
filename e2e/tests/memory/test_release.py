"""Test memory release after data removal."""

from __future__ import annotations

import uuid

import pytest

from lib.data_generator import DataGenerator
from lib.wait import wait_until, wait_until_value

pytestmark = pytest.mark.memory


def _used_memory(mygramdb) -> int:
    """Live index memory, read from INFO rather than the metrics endpoint.

    The Prometheus surface serves an aggregated snapshot that is refreshed on a
    timer, so it reports a stale figure for a while after a bulk removal. INFO
    reports the current accounting, which is what a test about prompt release
    needs to look at.

    The figure covers every configured table, not just the one under test, so
    assertions here compare it against itself over time instead of expecting it
    to reach zero.
    """
    info = mygramdb.info()
    assert "used_memory_bytes" in info, "INFO does not report used_memory_bytes"
    return int(info["used_memory_bytes"])


class TestMemoryRelease:
    """Test that memory is released after data removal.

    Emptying a table has to give the memory back, not just stop matching. An
    index that keeps its postings after every document is gone leaks a table's
    worth of memory per truncate, which a search-only assertion cannot see.
    """

    def test_truncate_releases_memory(self, mysql, mygramdb, seed_data):
        """TRUNCATE frees the memory the indexed rows occupied."""
        mygramdb.sync("testdb.articles")
        baseline = _used_memory(mygramdb)
        assert baseline > 0, "the seeded tables report no memory, so a drop would prove nothing"

        # Grow the table first, so the accounting is shown to respond to
        # documents arriving as well as to them leaving. Without this half, a
        # figure that only ever counts down would still pass.
        marker = f"memgrow{uuid.uuid4().hex[:8]}"
        added = [
            {
                "title": f"Memory Growth {index}",
                "content": f"memory growth probe {marker} row {index}",
                "status": 1,
                "category": "tech",
                "enabled": 1,
            }
            for index in range(200)
        ]
        mysql.insert_rows("articles", added)
        wait_until_value(
            lambda: mygramdb.count("testdb.articles", marker),
            expected=len(added),
            timeout=60,
            interval=0.5,
            description="the growth rows to be indexed",
        )
        # INFO serves its memory figures from a snapshot the server rebuilds at
        # most once a second, so a read taken immediately after the rows are
        # indexed can still be answered from the snapshot that predates them.
        # Poll for the increase rather than reading once.
        wait_until(
            lambda: _used_memory(mygramdb) > baseline,
            timeout=30,
            interval=0.5,
            description=f"indexing {len(added)} more rows to increase the reported memory above {baseline}",
        )

        mysql.truncate("articles")
        wait_until(
            lambda: mygramdb.search("testdb.articles", "test", limit=1)["total"] == 0,
            timeout=60,
            interval=1,
            description="the truncate to empty the index",
        )

        # Every document this table ever held is gone, including the ones that
        # were there before this test ran, so the reported figure has to fall
        # below where it started rather than merely give back the growth.
        # Accounting is updated as documents are retired and can trail the last
        # removal slightly, so poll rather than assume it is instantaneous.
        wait_until(
            lambda: _used_memory(mygramdb) < baseline,
            timeout=30,
            interval=0.5,
            description="index memory to fall below its pre-growth level after the truncate",
        )

        rows = DataGenerator(seed=42).generate_articles(count=100)
        mysql.insert_rows("articles", rows)
        wait_until(
            lambda: int(mygramdb.info().get("total_documents", 0)) >= len(rows),
            timeout=30,
            interval=1,
            description="the table to be re-seeded for the following tests",
        )
        assert _used_memory(mygramdb) > 0, "re-seeded rows are accounted as using no memory"
