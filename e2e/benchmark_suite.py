#!/usr/bin/env python3
"""
MygramDB Performance Benchmark Suite

Fair comparison between MygramDB and MySQL, saturation analysis,
and anomaly detection.

Usage:
    # Quick comparison (1, 4, 16 concurrency x 5s)
    python benchmark_suite.py --mode quick --compare

    # Standard benchmark (7 levels x 15s)
    python benchmark_suite.py --mode standard --compare --json-output results/report.json

    # Saturation test (ramp up to find ceiling)
    python benchmark_suite.py --mode saturation --target mygramdb

    # Specific scenarios only
    python benchmark_suite.py --scenarios single_term_small,with_filter --compare

    # Save baseline
    python benchmark_suite.py --mode standard --save-baseline

    # UDS transport
    python benchmark_suite.py --mode quick --compare --transport uds \
        --mygramdb-socket /tmp/mygramdb.sock --mysql-socket /var/run/mysqld/mysqld.sock

    # Check anomalies on existing results
    python benchmark_suite.py --check-anomalies results/report.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

# Add e2e root to path
sys.path.insert(0, os.path.dirname(__file__))

from lib.benchmark.anomaly import AnomalyDetector
from lib.benchmark.comparison import ComparisonOrchestrator
from lib.benchmark.connection_pool import MygramdbConnectionPool, MysqlConnectionPool
from lib.benchmark.report import (
    generate_json_report,
    print_anomalies,
    print_comparison_table,
    print_saturation_summary,
    save_json_report,
)
from lib.benchmark.runner import BenchmarkRunner
from lib.benchmark.saturation import SaturationAnalyzer
from lib.benchmark.scenarios import Scenario, get_concurrency_levels, load_scenarios
from lib.data_generator import DataGenerator
from lib.mygramdb_client import MygramdbClient
from lib.mysql_client import MysqlClient
from lib.stats import BenchmarkResult, ComparisonResult
from lib.wait import wait_until_gte

SCENARIOS_PATH = os.path.join(os.path.dirname(__file__), "tests", "benchmark", "scenarios.json")
THRESHOLDS_PATH = os.path.join(os.path.dirname(__file__), "tests", "benchmark", "thresholds.json")
BASELINE_PATH = os.path.join(os.path.dirname(__file__), "results", "baselines", "benchmark.json")


def _ensure_data(
    mysql: MysqlClient,
    mygramdb: MygramdbClient,
    count: int,
    mixed: bool = False,
) -> None:
    """Ensure sufficient data exists in both MySQL and MygramDB."""
    current = mysql.count("articles")
    if current >= count:
        return

    print(f"  Seeding data: {current} -> {count} rows...")
    gen = DataGenerator(seed=42)
    rows = gen.generate_articles(count=count - current, mixed=mixed)
    mysql.insert_rows("articles", rows)

    # Wait for MygramDB replication
    mygramdb.sync("articles")

    def _get_doc_count() -> int:
        info = mygramdb.info()
        return info.get("total_documents", info.get("doc_count", info.get("documents", 0)))

    # Wait for at least 90% of data (some filtered by required_filters)
    target = int(count * 0.9)
    wait_until_gte(
        _get_doc_count,
        minimum=target,
        timeout=120,
        interval=2,
        description=f"MygramDB to sync {target}+ docs",
    )
    print(f"  Data ready: MySQL={mysql.count('articles')}, MygramDB={_get_doc_count()}")


def _run_comparison_mode(
    args: argparse.Namespace,
    scenarios: list[Scenario],
    mysql: MysqlClient,
    mygramdb: MygramdbClient,
) -> tuple[list[ComparisonResult], dict]:
    """Run comparison benchmarks between MygramDB and MySQL."""
    concurrency_levels = get_concurrency_levels(args.mode)
    duration = {"quick": 5.0, "standard": 15.0, "saturation": 10.0}.get(args.mode, 15.0)
    warmup_queries = {"quick": 20, "standard": 50, "saturation": 30}.get(args.mode, 50)

    all_results: list[ComparisonResult] = []
    raw_data: dict = {"comparisons": [], "mode": args.mode, "transport": args.transport}

    for scenario in scenarios:
        if scenario.query_type == "connection_cost":
            continue  # Handled separately

        print(f"\n--- Scenario: {scenario.name} (data: {scenario.data_count}) ---")
        _ensure_data(mysql, mygramdb, scenario.data_count, scenario.mixed_content)

        mg_pool = MygramdbConnectionPool(
            host=args.mygramdb_host,
            port=args.mygramdb_port,
            unix_socket_path=args.mygramdb_socket or "",
            pool_size=max(concurrency_levels),
        )
        my_pool = MysqlConnectionPool(
            host=args.mysql_host,
            port=args.mysql_port,
            user=args.mysql_user,
            password=args.mysql_password,
            database=args.mysql_database,
            unix_socket=args.mysql_socket or "",
            pool_size=max(concurrency_levels),
        )

        try:
            orchestrator = ComparisonOrchestrator(
                mygramdb_pool=mg_pool,
                mysql_pool=my_pool,
                mygramdb_client=mygramdb,
            )
            orchestrator.prepare()

            for c in concurrency_levels:
                print(f"  concurrency={c}, duration={duration}s ...", end=" ", flush=True)
                start = time.perf_counter()
                result = orchestrator.run_comparison(
                    scenario=scenario,
                    concurrency=c,
                    duration=duration,
                    warmup_queries=warmup_queries,
                )
                elapsed = time.perf_counter() - start
                print(
                    f"MG:{result.mg_p50_ms:.1f}ms MySQL:{result.my_p50_ms:.1f}ms "
                    f"ratio:{result.qps_ratio:.1f}x ({elapsed:.0f}s)"
                )
                all_results.append(result)
                raw_data["comparisons"].append(result.summary())

            orchestrator.restore()
        finally:
            mg_pool.close_all()
            my_pool.close_all()

    return all_results, raw_data


def _run_saturation_mode(
    args: argparse.Namespace,
    scenarios: list[Scenario],
    mygramdb: MygramdbClient,
    mysql: MysqlClient | None = None,
) -> dict:
    """Run saturation analysis to find performance ceiling."""
    concurrency_levels = get_concurrency_levels(args.mode)
    duration = {"quick": 5.0, "standard": 15.0, "saturation": 10.0}.get(args.mode, 10.0)

    raw_data: dict = {"saturation": [], "mode": args.mode, "target": args.target}

    for scenario in scenarios:
        if scenario.query_type == "connection_cost":
            continue

        print(f"\n--- Saturation: {scenario.name} (data: {scenario.data_count}) ---")
        if mysql:
            _ensure_data(mysql, mygramdb, scenario.data_count, scenario.mixed_content)

        # Stop replication during saturation test
        mygramdb.replication_stop()
        mygramdb.cache_clear()

        try:

            def _pool_factory(concurrency: int) -> MygramdbConnectionPool:
                return MygramdbConnectionPool(
                    host=args.mygramdb_host,
                    port=args.mygramdb_port,
                    unix_socket_path=args.mygramdb_socket or "",
                    pool_size=concurrency,
                )

            analyzer = SaturationAnalyzer(
                pool_factory=_pool_factory,
                duration_per_level=duration,
            )

            from lib.benchmark.scenarios import build_mygramdb_commands

            queries = build_mygramdb_commands(scenario)
            sat_result = analyzer.run(queries=queries, concurrency_levels=concurrency_levels)

            print_saturation_summary(sat_result)
            raw_data["saturation"].append(
                {
                    "scenario": scenario.name,
                    "peak_qps": sat_result.peak_qps,
                    "peak_concurrency": sat_result.peak_concurrency,
                    "breaking_point": sat_result.breaking_point,
                    "levels": [
                        {
                            "concurrency": lvl.concurrency,
                            "qps": round(lvl.qps, 1),
                            "p50_ms": round(lvl.p50_ms, 2),
                            "p99_ms": round(lvl.p99_ms, 2),
                            "error_rate": round(lvl.error_rate, 4),
                        }
                        for lvl in sat_result.levels
                    ],
                }
            )
        finally:
            mygramdb.replication_start()

    return raw_data


def _run_connection_cost(args: argparse.Namespace, mygramdb: MygramdbClient) -> dict:
    """Measure connection establishment cost."""
    print("\n--- Connection Cost ---")
    iterations = 100
    times: list[float] = []

    for _ in range(iterations):
        success, elapsed, _ = mygramdb.tcp_command_timed("INFO")
        if success:
            times.append(elapsed)

    result = BenchmarkResult(
        total_queries=iterations,
        successful=len(times),
        failed=iterations - len(times),
        total_time_ms=sum(times),
        times=times,
    )
    summary = result.summary()
    transport = args.transport
    print(f"  Transport: {transport}")
    print(f"  Iterations: {iterations}")
    if times:
        print(
            f"  p50: {result.p50_ms:.2f}ms, p99: {result.p99_ms:.2f}ms, avg: {result.avg_ms:.2f}ms"
        )
    return {"connection_cost": {"transport": transport, **summary}}


def _check_anomalies_from_file(path: str) -> None:
    """Load results from JSON and check for anomalies."""
    with open(path) as f:
        data = json.load(f)

    detector = AnomalyDetector(THRESHOLDS_PATH)
    anomalies = detector.check_from_report(data)
    print_anomalies(anomalies)

    if any(a.severity == "critical" for a in anomalies):
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="MygramDB Performance Benchmark Suite",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Mode
    parser.add_argument(
        "--mode",
        choices=["quick", "standard", "saturation"],
        default="quick",
        help="Benchmark mode (default: quick)",
    )

    # Target
    parser.add_argument(
        "--target",
        choices=["mygramdb", "mysql", "both"],
        default="both",
        help="Target system (default: both)",
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Run MygramDB vs MySQL comparison",
    )

    # Scenarios
    parser.add_argument(
        "--scenarios",
        help="Comma-separated scenario names (default: all)",
    )

    # Transport
    parser.add_argument(
        "--transport",
        choices=["tcp", "uds"],
        default="tcp",
        help="Connection transport (default: tcp)",
    )

    # Output
    parser.add_argument("--json-output", help="Save results to JSON file")
    parser.add_argument("--save-baseline", action="store_true", help="Save as baseline")
    parser.add_argument("--check-anomalies", help="Check anomalies on existing JSON report")

    # MygramDB connection
    parser.add_argument(
        "--mygramdb-host",
        default=os.environ.get("MYGRAMDB_HOST", "127.0.0.1"),
    )
    parser.add_argument(
        "--mygramdb-port",
        type=int,
        default=int(os.environ.get("MYGRAMDB_PORT", "11016")),
    )
    parser.add_argument("--mygramdb-socket", default="", help="MygramDB Unix socket path")

    # MySQL connection
    parser.add_argument(
        "--mysql-host",
        default=os.environ.get("MYSQL_HOST", "127.0.0.1"),
    )
    parser.add_argument(
        "--mysql-port",
        type=int,
        default=int(os.environ.get("MYSQL_PORT", "13306")),
    )
    parser.add_argument(
        "--mysql-user",
        default=os.environ.get("MYSQL_USER", "root"),
    )
    parser.add_argument(
        "--mysql-password",
        default=os.environ.get("MYSQL_PASSWORD", "test_root_password"),
    )
    parser.add_argument(
        "--mysql-database",
        default=os.environ.get("MYSQL_DATABASE", "testdb"),
    )
    parser.add_argument("--mysql-socket", default="", help="MySQL Unix socket path")

    args = parser.parse_args()

    # Handle --check-anomalies standalone mode
    if args.check_anomalies:
        _check_anomalies_from_file(args.check_anomalies)
        return

    # Load scenarios
    all_scenarios = load_scenarios(SCENARIOS_PATH)
    if args.scenarios:
        selected = {s.strip() for s in args.scenarios.split(",")}
        scenarios = [s for s in all_scenarios if s.name in selected]
        if not scenarios:
            print(f"No matching scenarios found. Available: {[s.name for s in all_scenarios]}")
            sys.exit(1)
    else:
        scenarios = all_scenarios

    print("=" * 60)
    print("MygramDB Performance Benchmark Suite")
    print("=" * 60)
    print(f"Mode: {args.mode}")
    print(f"Transport: {args.transport}")
    print(f"Scenarios: {[s.name for s in scenarios]}")
    print(f"Concurrency levels: {get_concurrency_levels(args.mode)}")

    # Create clients
    mygramdb = MygramdbClient(
        args.mygramdb_host,
        args.mygramdb_port,
        unix_socket_path=args.mygramdb_socket,
    )
    mysql = MysqlClient(
        host=args.mysql_host,
        port=args.mysql_port,
        user=args.mysql_user,
        password=args.mysql_password,
        database=args.mysql_database,
    )

    # Verify connectivity
    print("\nChecking connectivity...")
    if not mygramdb.ping():
        print("ERROR: Cannot connect to MygramDB")
        sys.exit(1)
    print("  MygramDB: OK")

    if args.compare or args.target in ("mysql", "both"):
        if not mysql.ping():
            print("ERROR: Cannot connect to MySQL")
            sys.exit(1)
        print("  MySQL: OK")

    all_report_data: dict = {}

    # Run connection cost scenario if selected
    conn_cost_scenarios = [s for s in scenarios if s.query_type == "connection_cost"]
    if conn_cost_scenarios:
        result = _run_connection_cost(args, mygramdb)
        all_report_data.update(result)

    # Run comparison mode
    if args.compare:
        results, raw_data = _run_comparison_mode(args, scenarios, mysql, mygramdb)
        all_report_data.update(raw_data)

        print()
        print_comparison_table(results)

    # Run saturation mode
    if args.mode == "saturation":
        sat_data = _run_saturation_mode(args, scenarios, mygramdb, mysql)
        all_report_data.update(sat_data)

    # Run single-target benchmark (non-comparison)
    if not args.compare and args.mode != "saturation":
        concurrency_levels = get_concurrency_levels(args.mode)
        duration = {"quick": 5.0, "standard": 15.0}.get(args.mode, 15.0)
        target_results: dict = {"benchmarks": [], "target": args.target}

        for scenario in scenarios:
            if scenario.query_type == "connection_cost":
                continue

            print(f"\n--- Scenario: {scenario.name} ---")
            _ensure_data(mysql, mygramdb, scenario.data_count, scenario.mixed_content)

            pool = MygramdbConnectionPool(
                host=args.mygramdb_host,
                port=args.mygramdb_port,
                unix_socket_path=args.mygramdb_socket or "",
                pool_size=max(concurrency_levels),
            )

            try:
                mygramdb.replication_stop()
                mygramdb.cache_clear()

                runner = BenchmarkRunner(pool, warmup_queries=50)
                runner.ensure_connections()

                from lib.benchmark.scenarios import build_mygramdb_commands

                queries = build_mygramdb_commands(scenario)
                runner.warmup(queries)

                for c in concurrency_levels:
                    print(f"  concurrency={c} ...", end=" ", flush=True)
                    benchmark_result = runner.run(queries, concurrency=c, duration=duration)
                    summary = benchmark_result.summary()
                    print(
                        f"p50={benchmark_result.p50_ms:.1f}ms "
                        f"p99={benchmark_result.p99_ms:.1f}ms "
                        f"qps={benchmark_result.qps:.0f}"
                    )
                    target_results["benchmarks"].append(
                        {
                            "scenario": scenario.name,
                            "concurrency": c,
                            **summary,
                        }
                    )
            finally:
                mygramdb.replication_start()
                pool.close_all()

        all_report_data.update(target_results)

    # Anomaly detection
    detector = AnomalyDetector(THRESHOLDS_PATH)
    anomalies = detector.check_from_report(all_report_data)

    # Check against baseline
    if os.path.exists(BASELINE_PATH):
        baseline_anomalies = detector.check_regression_from_files(all_report_data, BASELINE_PATH)
        anomalies.extend(baseline_anomalies)

    if anomalies:
        print()
        print_anomalies(anomalies)

    # Save outputs
    if args.json_output:
        report = generate_json_report(all_report_data)
        save_json_report(report, args.json_output)
        print(f"\nResults saved to {args.json_output}")

    if args.save_baseline:
        os.makedirs(os.path.dirname(BASELINE_PATH), exist_ok=True)
        report = generate_json_report(all_report_data)
        save_json_report(report, BASELINE_PATH)
        print(f"\nBaseline saved to {BASELINE_PATH}")

    # Exit with error if critical anomalies
    if any(a.severity == "critical" for a in anomalies):
        sys.exit(1)


if __name__ == "__main__":
    main()
