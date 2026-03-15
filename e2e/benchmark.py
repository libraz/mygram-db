#!/usr/bin/env python3
"""
MygramDB vs MySQL FULLTEXT Benchmark Tool

Migrated from support/benchmark/benchmark.py to use shared e2e libraries.

Usage:
    python benchmark.py --target mygramdb --table articles --words "hello" --concurrency 1
    python benchmark.py --target mysql --table articles --words "hello,world" --concurrency 100
"""

import argparse
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add e2e root to path
sys.path.insert(0, os.path.dirname(__file__))

from lib.mygramdb_client import MygramdbClient
from lib.stats import BenchmarkResult

# Optional MySQL
try:
    from lib.mysql_client import MysqlClient
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False


def run_benchmark(
    query_fn,
    queries: list[str],
    concurrency: int,
    iterations: int,
) -> BenchmarkResult:
    """Run benchmark with given queries and concurrency."""
    result = BenchmarkResult()
    all_queries = queries * iterations
    result.total_queries = len(all_queries)

    start_total = time.perf_counter()

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(query_fn, q) for q in all_queries]
        for future in as_completed(futures):
            success, elapsed, response = future.result()
            if success:
                result.successful += 1
                result.times.append(elapsed)
            else:
                result.failed += 1
                result.errors.append(response)

    result.total_time_ms = (time.perf_counter() - start_total) * 1000
    return result


def main():
    parser = argparse.ArgumentParser(description="MygramDB vs MySQL Benchmark")
    parser.add_argument("--target", choices=["mygramdb", "mysql", "both"], default="both")
    parser.add_argument("--table", required=True, help="Table name")
    parser.add_argument("--column", default="content", help="Text column (MySQL)")
    parser.add_argument("--words", required=True, help="Comma-separated search words")
    parser.add_argument("--query-type", choices=["search", "count"], default="search")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--offset", type=int, default=0)
    parser.add_argument("--concurrency", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=5)
    parser.add_argument("--json-output", help="Save results to JSON file")

    parser.add_argument("--mysql-host", default=os.environ.get("MYSQL_HOST", "127.0.0.1"))
    parser.add_argument("--mysql-port", type=int, default=int(os.environ.get("MYSQL_PORT", "3306")))
    parser.add_argument("--mysql-user", default=os.environ.get("MYSQL_USER", "root"))
    parser.add_argument("--mysql-password", default=os.environ.get("MYSQL_PASSWORD", ""))
    parser.add_argument("--mysql-database", default=os.environ.get("MYSQL_DATABASE", "testdb"))
    parser.add_argument("--mygramdb-host", default=os.environ.get("MYGRAMDB_HOST", "127.0.0.1"))
    parser.add_argument("--mygramdb-port", type=int, default=int(os.environ.get("MYGRAMDB_PORT", "11016")))

    args = parser.parse_args()
    words = [w.strip() for w in args.words.split(",")]
    all_results = {}

    print("=== Benchmark Configuration ===")
    print(f"Table: {args.table}, Words: {words}")
    print(f"Type: {args.query_type}, Limit: {args.limit}, Offset: {args.offset}")
    print(f"Concurrency: {args.concurrency}, Iterations: {args.iterations}")
    print()

    # MygramDB
    if args.target in ("mygramdb", "both"):
        print("=== MygramDB Benchmark ===")
        client = MygramdbClient(args.mygramdb_host, args.mygramdb_port)

        queries = []
        for word in words:
            if args.query_type == "search":
                cmd = f"SEARCH {args.table} {word} LIMIT {args.limit}"
                if args.offset > 0:
                    cmd = f"SEARCH {args.table} {word} SORT id ASC LIMIT {args.offset},{args.limit}"
                queries.append(cmd)
            else:
                queries.append(f"COUNT {args.table} {word}")

        result = run_benchmark(
            client.tcp_command_timed, queries, args.concurrency, args.iterations
        )
        summary = result.summary()
        all_results["mygramdb"] = summary

        for key, value in summary.items():
            print(f"  {key}: {value}")
        print()

    # MySQL
    if args.target in ("mysql", "both"):
        if not HAS_MYSQL:
            print("MySQL: mysql-connector-python not installed")
        else:
            print("=== MySQL Benchmark ===")
            # MySQL benchmark using subprocess for simplicity
            print("  (MySQL benchmark not yet implemented in migrated version)")
        print()

    if args.json_output:
        with open(args.json_output, "w") as f:
            json.dump(all_results, f, indent=2)
        print(f"Results saved to {args.json_output}")


if __name__ == "__main__":
    main()
