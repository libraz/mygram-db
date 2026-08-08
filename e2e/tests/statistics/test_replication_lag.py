"""Test replication lag metrics."""

from __future__ import annotations

import uuid

import pytest

from lib.metrics import MetricsSnapshot
from lib.wait import wait_until, wait_until_value

pytestmark = pytest.mark.statistics

LAST_APPLIED = "mygramdb_replication_last_applied_unixtime"
SECONDS_SINCE_APPLIED = "mygramdb_replication_seconds_since_last_applied"

# Everything this suite writes lands within a few seconds, so a lag above this
# means the reported figure is not tracking the work actually being applied.
MAX_CONVERGED_LAG_SECONDS = 30

# The applied timestamp is a whole-second gauge, so two events in the same
# second are indistinguishable. Let the reported lag reach this before writing
# the probe row; a stamp taken afterwards is then necessarily larger.
MIN_AGE_BEFORE_PROBE_SECONDS = 2


def _lag_metrics(mygramdb) -> dict[str, float]:
    snapshot = MetricsSnapshot.capture(mygramdb)
    missing = [name for name in (LAST_APPLIED, SECONDS_SINCE_APPLIED) if name not in snapshot.data]
    if missing:
        raise AssertionError(f"/metrics does not export: {missing}")
    return {name: snapshot.data[name] for name in (LAST_APPLIED, SECONDS_SINCE_APPLIED)}


class TestReplicationLag:
    """Verify the reported replication lag tracks applied work.

    Operators page on this figure, so the failure that matters is a lag that
    stays flat -- reading as healthy while nothing is being applied, or as
    permanently behind after the backlog cleared. Both are caught by tying the
    metric to a write whose arrival is independently confirmed.
    """

    def test_lag_converges_after_a_write_is_applied(self, mysql, mygramdb, seed_data):
        """Applying a row advances the applied timestamp and leaves lag small."""
        # Seeding stamps the timestamp, so wait for that stamp to age out of the
        # gauge's resolution before probing. This doubles as evidence that the
        # gauge is computed live rather than frozen at a captured value.
        wait_until(
            lambda: _lag_metrics(mygramdb)[SECONDS_SINCE_APPLIED] >= MIN_AGE_BEFORE_PROBE_SECONDS,
            timeout=20,
            interval=0.5,
            description="the reported lag to grow while nothing is being applied",
        )
        before = _lag_metrics(mygramdb)[LAST_APPLIED]

        marker = f"lag{uuid.uuid4().hex[:8]}"
        mysql.insert_rows(
            "articles",
            [
                {
                    "title": "Lag Probe",
                    "content": f"lag convergence probe {marker}",
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
            description="the probe row to be indexed",
        )

        # The row is in the index, so the applied timestamp must have moved with
        # it -- a frozen timestamp would make the lag gauge grow without bound.
        wait_until(
            lambda: _lag_metrics(mygramdb)[LAST_APPLIED] > before,
            timeout=20,
            interval=0.25,
            description="the last-applied timestamp to advance with the applied row",
        )

        lag = _lag_metrics(mygramdb)[SECONDS_SINCE_APPLIED]
        assert 0 <= lag < MAX_CONVERGED_LAG_SECONDS, (
            f"replication reports {lag}s since the last applied event, just after applying one"
        )
