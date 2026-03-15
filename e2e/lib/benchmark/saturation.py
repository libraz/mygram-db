"""Concurrent connection ramp-up and performance ceiling detection."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Callable

from lib.benchmark.connection_pool import ConnectionPool
from lib.benchmark.runner import BenchmarkRunner
from lib.stats import BenchmarkResult

logger = logging.getLogger(__name__)


@dataclass
class SaturationLevel:
    """Metrics captured at a single concurrency level."""

    concurrency: int
    result: BenchmarkResult
    qps: float
    p50_ms: float
    p99_ms: float
    error_rate: float


@dataclass
class SaturationResult:
    """Aggregate result of a saturation analysis across concurrency levels."""

    levels: list[SaturationLevel] = field(default_factory=list)
    peak_qps: float = 0.0
    peak_concurrency: int = 0
    breaking_point: int | None = None


class SaturationAnalyzer:
    """Ramps concurrency to find peak throughput and the breaking point.

    Args:
        pool_factory: Callable(concurrency) -> ConnectionPool.
        duration_per_level: Seconds per concurrency level.
    """

    def __init__(
        self,
        pool_factory: Callable[[int], ConnectionPool],
        duration_per_level: float = 10.0,
    ) -> None:
        self._pool_factory = pool_factory
        self._duration_per_level = duration_per_level

    def run(
        self,
        queries: list[str],
        concurrency_levels: list[int],
    ) -> SaturationResult:
        """Execute benchmarks across increasing concurrency levels."""
        sat_result = SaturationResult()

        for concurrency in concurrency_levels:
            logger.info(
                "Saturation level: concurrency=%d, duration=%.1fs",
                concurrency,
                self._duration_per_level,
            )

            pool = self._pool_factory(concurrency)
            try:
                pool.establish_all()

                runner = BenchmarkRunner(
                    pool=pool,
                    warmup_queries=0,
                    measurement_duration=self._duration_per_level,
                )
                result = runner.run(
                    queries=queries,
                    concurrency=concurrency,
                    duration=self._duration_per_level,
                )
            finally:
                pool.close_all()

            level = SaturationLevel(
                concurrency=concurrency,
                result=result,
                qps=result.qps,
                p50_ms=result.p50_ms,
                p99_ms=result.p99_ms,
                error_rate=result.error_rate,
            )
            sat_result.levels.append(level)

            logger.info(
                "Level c=%d: qps=%.1f p50=%.2fms p99=%.2fms err=%.4f",
                concurrency,
                level.qps,
                level.p50_ms,
                level.p99_ms,
                level.error_rate,
            )

            if level.qps > sat_result.peak_qps:
                sat_result.peak_qps = level.qps
                sat_result.peak_concurrency = concurrency

        sat_result.breaking_point = self._detect_breaking_point(sat_result)
        return sat_result

    @staticmethod
    def _detect_breaking_point(sat_result: SaturationResult) -> int | None:
        """Find first concurrency level with significant degradation.

        Breaking point: error_rate > 1% OR QPS dropped > 20% from peak.
        """
        if not sat_result.levels or sat_result.peak_qps <= 0:
            return None

        for level in sat_result.levels:
            if level.error_rate > 0.01:
                return level.concurrency

            qps_drop = (sat_result.peak_qps - level.qps) / sat_result.peak_qps
            if qps_drop > 0.20:
                return level.concurrency

        return None
