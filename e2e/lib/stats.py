"""Statistics calculation utilities for benchmarks and load tests."""

from __future__ import annotations

import statistics
from dataclasses import dataclass, field
from typing import Any


@dataclass
class BenchmarkResult:
    """Aggregated benchmark results."""

    total_queries: int = 0
    successful: int = 0
    failed: int = 0
    total_time_ms: float = 0.0
    times: list[float] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def avg_ms(self) -> float:
        return statistics.mean(self.times) if self.times else 0.0

    @property
    def min_ms(self) -> float:
        return min(self.times) if self.times else 0.0

    @property
    def max_ms(self) -> float:
        return max(self.times) if self.times else 0.0

    @property
    def p50_ms(self) -> float:
        return statistics.median(self.times) if self.times else 0.0

    @property
    def p95_ms(self) -> float:
        return self._percentile(0.95)

    @property
    def p99_ms(self) -> float:
        return self._percentile(0.99)

    @property
    def qps(self) -> float:
        if self.total_time_ms <= 0:
            return 0.0
        return self.successful / (self.total_time_ms / 1000)

    @property
    def error_rate(self) -> float:
        if self.total_queries <= 0:
            return 0.0
        return self.failed / self.total_queries

    def _percentile(self, p: float) -> float:
        if len(self.times) < 2:
            return self.times[0] if self.times else 0.0
        sorted_times = sorted(self.times)
        idx = int(len(sorted_times) * p)
        idx = min(idx, len(sorted_times) - 1)
        return sorted_times[idx]

    def summary(self) -> dict[str, Any]:
        """Return a summary dict."""
        result: dict[str, Any] = {
            "total_queries": self.total_queries,
            "successful": self.successful,
            "failed": self.failed,
            "total_time_ms": round(self.total_time_ms, 1),
            "error_rate": round(self.error_rate, 4),
        }
        if self.times:
            result.update({
                "avg_ms": round(self.avg_ms, 2),
                "min_ms": round(self.min_ms, 2),
                "max_ms": round(self.max_ms, 2),
                "p50_ms": round(self.p50_ms, 2),
                "p95_ms": round(self.p95_ms, 2),
                "p99_ms": round(self.p99_ms, 2),
                "qps": round(self.qps, 1),
            })
        return result


@dataclass
class ComparisonResult:
    """Result of a MygramDB vs MySQL comparison for a single scenario/concurrency."""

    scenario_name: str = ""
    concurrency: int = 1
    mygramdb: BenchmarkResult = field(default_factory=BenchmarkResult)
    mysql: BenchmarkResult = field(default_factory=BenchmarkResult)

    @property
    def mg_p50_ms(self) -> float:
        return self.mygramdb.p50_ms

    @property
    def my_p50_ms(self) -> float:
        return self.mysql.p50_ms

    @property
    def mg_p99_ms(self) -> float:
        return self.mygramdb.p99_ms

    @property
    def my_p99_ms(self) -> float:
        return self.mysql.p99_ms

    @property
    def qps_ratio(self) -> float:
        """MygramDB QPS / MySQL QPS. Higher means MygramDB is faster."""
        if self.mysql.qps <= 0:
            return 0.0
        return self.mygramdb.qps / self.mysql.qps

    @property
    def p50_ratio(self) -> float:
        """MygramDB p50 / MySQL p50. Lower means MygramDB is faster."""
        if self.my_p50_ms <= 0:
            return 0.0
        return self.mg_p50_ms / self.my_p50_ms

    def summary(self) -> dict[str, Any]:
        """Return a summary dict."""
        return {
            "scenario": self.scenario_name,
            "concurrency": self.concurrency,
            "mygramdb": self.mygramdb.summary(),
            "mysql": self.mysql.summary(),
            "mg_p50_ms": round(self.mg_p50_ms, 2),
            "my_p50_ms": round(self.my_p50_ms, 2),
            "mg_p99_ms": round(self.mg_p99_ms, 2),
            "my_p99_ms": round(self.my_p99_ms, 2),
            "qps_ratio": round(self.qps_ratio, 2),
            "p50_ratio": round(self.p50_ratio, 2),
        }
