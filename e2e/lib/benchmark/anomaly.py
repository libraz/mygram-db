"""Threshold-based anomaly detection for benchmark results."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any

from lib.stats import BenchmarkResult


@dataclass
class Anomaly:
    """A detected anomaly in benchmark results."""

    severity: str  # "critical", "warning", "info"
    scenario: str
    metric: str
    actual: float
    threshold: float
    message: str


def _concurrency_factor(concurrency: int) -> float:
    """Scaling factor for latency thresholds based on concurrency."""
    if concurrency <= 1:
        return 1.0
    return 1.0 + math.log2(concurrency)


class AnomalyDetector:
    """Detects anomalies in benchmark results using configurable thresholds."""

    def __init__(self, thresholds_path: str) -> None:
        with open(thresholds_path, encoding="utf-8") as f:
            self._thresholds: dict[str, Any] = json.load(f)

    def check_absolute(
        self,
        scenario_name: str,
        result: BenchmarkResult,
        concurrency: int,
    ) -> list[Anomaly]:
        """Check results against absolute thresholds (scaled by concurrency)."""
        absolute = self._thresholds.get("absolute", {})
        scenario_thresholds = absolute.get(scenario_name)
        if scenario_thresholds is None:
            return []

        factor = _concurrency_factor(concurrency)
        anomalies: list[Anomaly] = []

        for metric in ("p50_ms", "p99_ms"):
            base_threshold = scenario_thresholds.get(metric)
            if base_threshold is None:
                continue
            threshold = base_threshold * factor
            actual = getattr(result, metric, 0.0)
            anomaly = self._classify_higher_is_worse(
                scenario_name, metric, actual, threshold
            )
            if anomaly is not None:
                anomalies.append(anomaly)

        min_qps = scenario_thresholds.get("min_qps")
        if min_qps is not None:
            anomaly = self._classify_lower_is_worse(
                scenario_name, "qps", result.qps, min_qps
            )
            if anomaly is not None:
                anomalies.append(anomaly)

        return anomalies

    def check_relative(
        self,
        scenario_name: str,
        p50_ratio: float,
        p99_ratio: float,
    ) -> list[Anomaly]:
        """Check MygramDB/MySQL latency ratios. Ratio > 1.0 means MygramDB slower."""
        relative = self._thresholds.get("relative", {})
        anomalies: list[Anomaly] = []

        checks = [
            ("p50_ratio", p50_ratio, relative.get("max_p50_ratio", 1.0)),
            ("p99_ratio", p99_ratio, relative.get("max_p99_ratio", 1.5)),
        ]

        for metric, ratio, max_ratio in checks:
            if ratio <= 0:
                continue
            if ratio > 2.0:
                anomalies.append(Anomaly(
                    severity="critical",
                    scenario=scenario_name,
                    metric=metric,
                    actual=ratio,
                    threshold=max_ratio,
                    message=f"{metric}={ratio:.2f} critically exceeds threshold {max_ratio:.2f}",
                ))
            elif ratio > max_ratio:
                anomalies.append(Anomaly(
                    severity="warning",
                    scenario=scenario_name,
                    metric=metric,
                    actual=ratio,
                    threshold=max_ratio,
                    message=f"{metric}={ratio:.2f} exceeds threshold {max_ratio:.2f}",
                ))

        return anomalies

    def check_regression(
        self,
        scenario_name: str,
        current: BenchmarkResult,
        baseline: BenchmarkResult,
    ) -> list[Anomaly]:
        """Check for performance regressions against a baseline."""
        regression = self._thresholds.get("regression", {})
        anomalies: list[Anomaly] = []

        # p99 degradation
        if baseline.p99_ms > 0:
            p99_change = ((current.p99_ms - baseline.p99_ms) / baseline.p99_ms) * 100
            p99_critical = regression.get("p99_critical_pct", 50)
            p99_warning = regression.get("p99_warning_pct", 20)

            if p99_change > p99_critical:
                anomalies.append(Anomaly(
                    severity="critical",
                    scenario=scenario_name,
                    metric="p99_ms",
                    actual=current.p99_ms,
                    threshold=baseline.p99_ms,
                    message=f"p99 degraded {p99_change:.1f}% ({baseline.p99_ms:.2f} -> {current.p99_ms:.2f}ms)",
                ))
            elif p99_change > p99_warning:
                anomalies.append(Anomaly(
                    severity="warning",
                    scenario=scenario_name,
                    metric="p99_ms",
                    actual=current.p99_ms,
                    threshold=baseline.p99_ms,
                    message=f"p99 degraded {p99_change:.1f}% ({baseline.p99_ms:.2f} -> {current.p99_ms:.2f}ms)",
                ))

        # QPS drop
        if baseline.qps > 0:
            qps_change = ((baseline.qps - current.qps) / baseline.qps) * 100
            qps_critical = regression.get("qps_critical_pct", 50)
            qps_warning = regression.get("qps_warning_pct", 20)

            if qps_change > qps_critical:
                anomalies.append(Anomaly(
                    severity="critical",
                    scenario=scenario_name,
                    metric="qps",
                    actual=current.qps,
                    threshold=baseline.qps,
                    message=f"QPS dropped {qps_change:.1f}% ({baseline.qps:.1f} -> {current.qps:.1f})",
                ))
            elif qps_change > qps_warning:
                anomalies.append(Anomaly(
                    severity="warning",
                    scenario=scenario_name,
                    metric="qps",
                    actual=current.qps,
                    threshold=baseline.qps,
                    message=f"QPS dropped {qps_change:.1f}% ({baseline.qps:.1f} -> {current.qps:.1f})",
                ))

        return anomalies

    def check_from_report(self, report_data: dict[str, Any]) -> list[Anomaly]:
        """Check anomalies from a benchmark report dict.

        Handles both comparison and single-target report formats.
        """
        anomalies: list[Anomaly] = []

        # Check comparisons
        for comp in report_data.get("comparisons", []):
            scenario = comp.get("scenario", "unknown")
            concurrency = comp.get("concurrency", 1)

            mg_data = comp.get("mygramdb", {})
            if mg_data and mg_data.get("p50_ms"):
                result = _benchmark_result_from_dict(mg_data)
                anomalies.extend(self.check_absolute(scenario, result, concurrency))

            # Relative checks
            mg_p50 = comp.get("mg_p50_ms", 0)
            my_p50 = comp.get("my_p50_ms", 0)
            mg_p99 = comp.get("mg_p99_ms", 0)
            my_p99 = comp.get("my_p99_ms", 0)
            if my_p50 > 0 and my_p99 > 0:
                p50_ratio = mg_p50 / my_p50
                p99_ratio = mg_p99 / my_p99
                anomalies.extend(self.check_relative(scenario, p50_ratio, p99_ratio))

        # Check single-target benchmarks
        for bench in report_data.get("benchmarks", []):
            scenario = bench.get("scenario", "unknown")
            concurrency = bench.get("concurrency", 1)
            if bench.get("p50_ms"):
                result = _benchmark_result_from_dict(bench)
                anomalies.extend(self.check_absolute(scenario, result, concurrency))

        return anomalies

    def check_regression_from_files(
        self, current_data: dict[str, Any], baseline_path: str
    ) -> list[Anomaly]:
        """Check regressions between current data and a baseline file."""
        try:
            with open(baseline_path, encoding="utf-8") as f:
                baseline_data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return []

        anomalies: list[Anomaly] = []

        # Build lookup of baseline benchmarks by scenario
        baseline_map: dict[str, dict] = {}
        for bench in baseline_data.get("benchmarks", []):
            scenario = bench.get("scenario", bench.get("name", ""))
            if scenario:
                baseline_map[scenario] = bench

        # Check current benchmarks against baseline
        for bench in current_data.get("benchmarks", []):
            scenario = bench.get("scenario", "unknown")
            if scenario in baseline_map and bench.get("p99_ms"):
                current = _benchmark_result_from_dict(bench)
                baseline = _benchmark_result_from_dict(baseline_map[scenario])
                anomalies.extend(self.check_regression(scenario, current, baseline))

        return anomalies

    @staticmethod
    def _classify_higher_is_worse(
        scenario: str, metric: str, actual: float, threshold: float
    ) -> Anomaly | None:
        """Classify latency metric (higher = worse)."""
        if actual > threshold * 2:
            return Anomaly(
                severity="critical",
                scenario=scenario,
                metric=metric,
                actual=actual,
                threshold=threshold,
                message=f"{metric}={actual:.2f}ms exceeds 2x threshold ({threshold:.2f}ms)",
            )
        if actual > threshold:
            return Anomaly(
                severity="warning",
                scenario=scenario,
                metric=metric,
                actual=actual,
                threshold=threshold,
                message=f"{metric}={actual:.2f}ms exceeds threshold ({threshold:.2f}ms)",
            )
        if actual > threshold * 0.8:
            return Anomaly(
                severity="info",
                scenario=scenario,
                metric=metric,
                actual=actual,
                threshold=threshold,
                message=f"{metric}={actual:.2f}ms approaching threshold ({threshold:.2f}ms)",
            )
        return None

    @staticmethod
    def _classify_lower_is_worse(
        scenario: str, metric: str, actual: float, threshold: float
    ) -> Anomaly | None:
        """Classify QPS metric (lower = worse)."""
        if actual < threshold * 0.5:
            return Anomaly(
                severity="critical",
                scenario=scenario,
                metric=metric,
                actual=actual,
                threshold=threshold,
                message=f"{metric}={actual:.1f} critically below threshold ({threshold:.1f})",
            )
        if actual < threshold:
            return Anomaly(
                severity="warning",
                scenario=scenario,
                metric=metric,
                actual=actual,
                threshold=threshold,
                message=f"{metric}={actual:.1f} below threshold ({threshold:.1f})",
            )
        if actual < threshold * 1.25:
            return Anomaly(
                severity="info",
                scenario=scenario,
                metric=metric,
                actual=actual,
                threshold=threshold,
                message=f"{metric}={actual:.1f} approaching threshold ({threshold:.1f})",
            )
        return None


def _benchmark_result_from_dict(data: dict[str, Any]) -> BenchmarkResult:
    """Reconstruct a minimal BenchmarkResult from a summary dict."""
    result = BenchmarkResult(
        total_queries=data.get("total_queries", 0),
        successful=data.get("successful", 0),
        failed=data.get("failed", 0),
        total_time_ms=data.get("total_time_ms", 0.0),
    )
    # Reconstruct enough timing data for percentile calculations
    p50 = data.get("p50_ms", 0.0)
    p99 = data.get("p99_ms", 0.0)
    qps = data.get("qps", 0.0)
    if p50 > 0:
        # Create synthetic times that reproduce the key percentiles
        count = max(result.successful, 100)
        p50_count = int(count * 0.5)
        p99_count = int(count * 0.49)
        tail_count = count - p50_count - p99_count
        result.times = [p50] * p50_count + [p99] * p99_count + [p99 * 1.1] * tail_count
    if qps > 0 and result.total_time_ms <= 0:
        result.total_time_ms = (result.successful / qps) * 1000 if qps > 0 else 0
    return result
