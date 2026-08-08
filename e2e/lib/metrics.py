"""Prometheus metrics parsing and snapshot comparison."""

from __future__ import annotations

import contextlib
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.mygramdb_client import MygramdbClient

# Exact exported names of the replication counters. Tests name the metric they
# depend on rather than substring-matching, so a renamed or dropped metric fails
# the test instead of silently matching nothing.
REPL_INSERTS_APPLIED = 'mygramdb_replication_inserts_total{status="applied"}'
REPL_INSERTS_SKIPPED = 'mygramdb_replication_inserts_total{status="skipped"}'
REPL_UPDATES_APPLIED = 'mygramdb_replication_updates_total{status="applied"}'
REPL_UPDATES_ADDED = 'mygramdb_replication_updates_total{status="added"}'
REPL_UPDATES_REMOVED = 'mygramdb_replication_updates_total{status="removed"}'
REPL_UPDATES_MODIFIED = 'mygramdb_replication_updates_total{status="modified"}'
REPL_UPDATES_SKIPPED = 'mygramdb_replication_updates_total{status="skipped"}'
REPL_DELETES_APPLIED = 'mygramdb_replication_deletes_total{status="applied"}'
REPL_DELETES_SKIPPED = 'mygramdb_replication_deletes_total{status="skipped"}'
REPL_DDL_TOTAL = "mygramdb_replication_ddl_total"
REPL_EVENTS_PROCESSED = "mygramdb_replication_events_processed"

SERVER_COMMANDS_TOTAL = "mygramdb_server_commands_total"
CLIENTS_TOTAL = "mygramdb_clients_total"

CACHE_HITS = "mygramdb_cache_hits_total"
CACHE_MISSES_NOT_FOUND = 'mygramdb_cache_misses_total{reason="not_found"}'
CACHE_MISSES_TTL_EXPIRED = 'mygramdb_cache_misses_total{reason="ttl_expired"}'
CACHE_MISSES_INVALIDATED = 'mygramdb_cache_misses_total{reason="invalidated"}'
CACHE_ENTRIES = "mygramdb_cache_entries"
CACHE_HIT_RATE = "mygramdb_cache_hit_rate"
CACHE_INVALIDATIONS_IMMEDIATE = 'mygramdb_cache_invalidations_total{phase="immediate"}'
CACHE_INVALIDATIONS_DEFERRED = 'mygramdb_cache_invalidations_total{phase="deferred"}'
CACHE_MEMORY_CACHE = 'mygramdb_cache_memory_bytes{type="cache"}'


def command_total(command: str) -> str:
    """Name of the per-command counter for ``command`` (e.g. ``search``)."""
    return f'mygramdb_command_total{{command="{command}"}}'


def read_counter(client: MygramdbClient, metric: str) -> float:
    """Read one exported counter, failing if the endpoint does not export it.

    Returning a default for a missing metric would turn every assertion built on
    it into a no-op, so an absent name is an error rather than zero.
    """
    snapshot = MetricsSnapshot.capture(client)
    if metric not in snapshot.data:
        raise AssertionError(f"metric {metric} is not exported by /metrics")
    return snapshot.data[metric]


class MetricsSnapshot:
    """A snapshot of Prometheus metrics."""

    def __init__(self, data: dict[str, float]) -> None:
        self.data = data

    @classmethod
    def capture(cls, client: MygramdbClient) -> MetricsSnapshot:
        """Capture current metrics from MygramDB."""
        raw = client.metrics()
        return cls(cls._parse_prometheus(raw))

    @staticmethod
    def _parse_prometheus(text: str) -> dict[str, float]:
        """Parse Prometheus text format into a dict."""
        result: dict[str, float] = {}
        for line in text.split("\n"):
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Match: metric_name{labels} value or metric_name value
            match = re.match(
                r"^([a-zA-Z_:][a-zA-Z0-9_:]*(?:\{[^}]*\})?)\s+([\d.eE+-]+|NaN|Inf|-Inf)$", line
            )
            if match:
                key = match.group(1)
                val_str = match.group(2)
                with contextlib.suppress(ValueError):
                    result[key] = float(val_str)
        return result

    def get(self, metric: str, default: float = 0.0) -> float:
        """Get a metric value."""
        return self.data.get(metric, default)

    def get_matching(self, pattern: str) -> dict[str, float]:
        """Get all metrics matching a regex pattern."""
        regex = re.compile(pattern)
        return {k: v for k, v in self.data.items() if regex.search(k)}

    @staticmethod
    def diff(before: MetricsSnapshot, after: MetricsSnapshot) -> dict[str, float]:
        """Compute the difference between two snapshots."""
        all_keys = set(before.data.keys()) | set(after.data.keys())
        result: dict[str, float] = {}
        for key in all_keys:
            before_val = before.data.get(key, 0.0)
            after_val = after.data.get(key, 0.0)
            delta = after_val - before_val
            if delta != 0:
                result[key] = delta
        return result
