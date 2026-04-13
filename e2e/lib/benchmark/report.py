"""Output formatting for benchmark results: JSON reports and console tables."""

from __future__ import annotations

import contextlib
import json
import os
import platform
import subprocess
from datetime import datetime, timezone
from typing import Any

from lib.benchmark.anomaly import Anomaly
from lib.benchmark.saturation import SaturationResult
from lib.stats import ComparisonResult

# ---------------------------------------------------------------------------
# Console output
# ---------------------------------------------------------------------------


def print_comparison_table(results: list[ComparisonResult]) -> None:
    """Print a formatted comparison table of MygramDB vs MySQL results."""
    if not results:
        print("No comparison results to display.")
        return

    columns = [
        ("Scenario", 19),
        ("C", 5),
        ("MG p50ms", 10),
        ("MY p50ms", 10),
        ("MG p99ms", 10),
        ("MY p99ms", 10),
        ("QPS Ratio", 10),
    ]

    print("=== MygramDB vs MySQL Comparison ===")
    _print_border(columns, "top")
    _print_header(columns)
    _print_border(columns, "middle")

    for r in results:
        cells = [
            f" {r.scenario_name:<17s} ",
            f" {r.concurrency:>3d} ",
            f" {r.mg_p50_ms:>8.2f} ",
            f" {r.my_p50_ms:>8.2f} ",
            f" {r.mg_p99_ms:>8.2f} ",
            f" {r.my_p99_ms:>8.2f} ",
            f" {r.qps_ratio:>7.1f}x ",
        ]
        print("\u2502" + "\u2502".join(cells) + "\u2502")

    _print_border(columns, "bottom")


def print_saturation_summary(result: SaturationResult) -> None:
    """Print a formatted saturation analysis summary."""
    print("=== Saturation Analysis ===")
    print(f"  Max QPS: {result.peak_qps:.0f} @ concurrency={result.peak_concurrency}")

    if result.breaking_point is not None:
        print(f"  Breaking point: concurrency={result.breaking_point}")
    else:
        print("  Breaking point: none detected")
    print()

    if not result.levels:
        return

    columns = [("C", 6), ("QPS", 9), ("p50 (ms)", 10), ("p99 (ms)", 10), ("Error (%)", 11)]

    _print_border(columns, "top", indent="  ")
    _print_header(columns, indent="  ")
    _print_border(columns, "middle", indent="  ")

    for level in result.levels:
        cells = [
            f" {level.concurrency:>4d} ",
            f" {level.qps:>7.1f} ",
            f" {level.p50_ms:>8.2f} ",
            f" {level.p99_ms:>8.2f} ",
            f" {level.error_rate * 100:>9.2f} ",
        ]
        print("  \u2502" + "\u2502".join(cells) + "\u2502")

    _print_border(columns, "bottom", indent="  ")


def print_anomalies(anomalies: list[Anomaly]) -> None:
    """Print detected anomalies with severity markers."""
    if not anomalies:
        return

    print("=== Anomalies ===")
    for a in anomalies:
        tag = a.severity.upper()
        print(f"  [{tag}] {a.scenario}/{a.metric}: {a.message}")


# ---------------------------------------------------------------------------
# Table drawing helpers
# ---------------------------------------------------------------------------


def _print_border(columns: list[tuple[str, int]], position: str, indent: str = "") -> None:
    chars = {
        "top": ("\u250c", "\u252c", "\u2510"),
        "middle": ("\u251c", "\u253c", "\u2524"),
        "bottom": ("\u2514", "\u2534", "\u2518"),
    }
    left, mid, right = chars[position]
    segments = ["\u2500" * w for _, w in columns]
    print(f"{indent}{left}{mid.join(segments)}{right}")


def _print_header(columns: list[tuple[str, int]], indent: str = "") -> None:
    cells = [f" {name:^{w - 2}s} " for name, w in columns]
    sep = "\u2502"
    print(f"{indent}{sep}{sep.join(cells)}{sep}")


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------


def generate_json_report(
    results: dict[str, Any],
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Create a pytest-benchmark compatible JSON report."""
    report: dict[str, Any] = {
        "machine_info": _get_machine_info(),
        "commit_info": _get_commit_info(),
        "datetime": datetime.now(timezone.utc).isoformat(),
        "results": results,
    }

    if metadata:
        report["metadata"] = metadata

    return report


def save_json_report(report: dict[str, Any], path: str) -> None:
    """Save a JSON report to file."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)


def _get_machine_info() -> dict[str, Any]:
    """Gather basic machine information."""
    return {
        "platform": platform.platform(),
        "python_version": platform.python_version(),
        "cpu_count": os.cpu_count(),
        "machine": platform.machine(),
    }


def _get_commit_info() -> dict[str, str]:
    """Gather current git commit hash and branch name."""
    info: dict[str, str] = {"hash": "", "branch": ""}
    with contextlib.suppress(subprocess.CalledProcessError, FileNotFoundError):
        info["hash"] = (
            subprocess.check_output(["git", "rev-parse", "HEAD"], stderr=subprocess.DEVNULL)
            .decode()
            .strip()
        )
    with contextlib.suppress(subprocess.CalledProcessError, FileNotFoundError):
        info["branch"] = (
            subprocess.check_output(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"], stderr=subprocess.DEVNULL
            )
            .decode()
            .strip()
        )
    return info
