"""Prometheus metrics parsing and snapshot comparison."""

from __future__ import annotations

import contextlib
import re
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from lib.mygramdb_client import MygramdbClient


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
