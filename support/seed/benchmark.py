#!/usr/bin/env python3
"""Benchmark MygramDB vs MySQL FULLTEXT on 1.1M Wikipedia dataset.

Measures:
1. Query performance (SORT id LIMIT 100) - various match rates
2. COUNT performance
3. Concurrent performance (10, 100 connections)
4. CJK search performance

Usage:
    python benchmark.py [--mysql-host 127.0.0.1] [--mysql-port 3306] [--mygramdb-host 127.0.0.1] [--mygramdb-port 11016]
"""

from __future__ import annotations

import argparse
import json
import socket
import statistics
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import mysql.connector


# ============================================================================
# MygramDB Client
# ============================================================================

class MygramDBClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def _query(self, cmd: str) -> tuple[str, float]:
        """Send command and return (response, elapsed_ms)."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(30)
        sock.connect((self.host, self.port))
        try:
            start = time.perf_counter()
            sock.sendall((cmd + "\r\n").encode())
            chunks = []
            while True:
                data = sock.recv(65536)
                if not data:
                    break
                chunks.append(data)
                joined = b"".join(chunks)
                # Response ends with \r\n (single-line responses like OK RESULTS... or OK COUNT...)
                if joined.endswith(b"\r\n"):
                    break
            elapsed = (time.perf_counter() - start) * 1000
            return joined.decode("utf-8", errors="replace"), elapsed
        finally:
            sock.close()

    def search(self, table: str, query: str, sort: str = "id", limit: int = 100) -> tuple[int, float]:
        """SEARCH and return (result_count, elapsed_ms)."""
        cmd = f"SEARCH {table} {query} SORT {sort} LIMIT {limit}"
        resp, elapsed = self._query(cmd)
        # Count result lines (each result is "VALUE <id>\r\n")
        count = resp.count("VALUE ")
        return count, elapsed

    def count(self, table: str, query: str) -> tuple[int, float]:
        """COUNT and return (count, elapsed_ms)."""
        cmd = f"COUNT {table} {query}"
        resp, elapsed = self._query(cmd)
        # Parse "COUNT <n>"
        for line in resp.split("\r\n"):
            if "COUNT " in line:
                # Format: "OK COUNT 12345"
                parts = line.strip().split()
                for i, p in enumerate(parts):
                    if p == "COUNT" and i + 1 < len(parts):
                        return int(parts[i + 1]), elapsed
        return 0, elapsed


# ============================================================================
# Benchmark Helpers
# ============================================================================

def benchmark_mysql_search(
    host: str, port: int, user: str, password: str, db: str,
    query: str, limit: int = 100, iterations: int = 5,
) -> dict[str, Any] | None:
    """Benchmark MySQL FULLTEXT search. Returns None on MySQL error."""
    conn = mysql.connector.connect(
        host=host, port=port, user=user, password=password, database=db,
    )
    cursor = conn.cursor()

    sql = f"SELECT id FROM articles WHERE MATCH(content) AGAINST(%s IN BOOLEAN MODE) ORDER BY id LIMIT {limit}"
    try:
        # Warmup
        cursor.execute(sql, (query,))
        cursor.fetchall()
    except Exception as e:
        cursor.close()
        conn.close()
        return {"error": str(e), "p50": float("inf"), "mean": float("inf"), "min": float("inf"), "max": float("inf"), "rows": 0}

    times = []
    row_count = 0
    for _ in range(iterations):
        start = time.perf_counter()
        try:
            cursor.execute(sql, (query,))
            rows = cursor.fetchall()
            elapsed = (time.perf_counter() - start) * 1000
            times.append(elapsed)
            row_count = len(rows)
        except Exception:
            elapsed = (time.perf_counter() - start) * 1000
            times.append(elapsed)

    cursor.close()
    conn.close()

    return {
        "p50": statistics.median(times),
        "mean": statistics.mean(times),
        "min": min(times),
        "max": max(times),
        "rows": row_count,
    }


def benchmark_mysql_count(
    host: str, port: int, user: str, password: str, db: str,
    query: str, iterations: int = 5,
) -> dict[str, Any]:
    """Benchmark MySQL FULLTEXT count."""
    try:
        conn = mysql.connector.connect(
            host=host, port=port, user=user, password=password, database=db,
        )
    except Exception as e:
        return {"error": str(e), "p50": float("inf"), "mean": float("inf"), "count": 0}
    cursor = conn.cursor()

    sql = "SELECT COUNT(*) FROM articles WHERE MATCH(content) AGAINST(%s IN BOOLEAN MODE)"
    try:
        cursor.execute(sql, (query,))
        cursor.fetchall()
    except Exception as e:
        cursor.close()
        conn.close()
        return {"error": str(e), "p50": float("inf"), "mean": float("inf"), "count": 0}

    times = []
    count = 0
    for _ in range(iterations):
        start = time.perf_counter()
        try:
            cursor.execute(sql, (query,))
            rows = cursor.fetchall()
            elapsed = (time.perf_counter() - start) * 1000
            times.append(elapsed)
            count = rows[0][0]
        except Exception:
            elapsed = (time.perf_counter() - start) * 1000
            times.append(elapsed)

    cursor.close()
    conn.close()

    return {
        "p50": statistics.median(times),
        "mean": statistics.mean(times),
        "count": count,
    }


def benchmark_mygramdb_search(
    host: str, port: int, query: str, limit: int = 100, iterations: int = 5,
) -> dict[str, Any]:
    """Benchmark MygramDB search."""
    client = MygramDBClient(host, port)

    # Warmup
    client.search("articles", query, limit=limit)

    times = []
    row_count = 0
    for _ in range(iterations):
        rows, elapsed = client.search("articles", query, limit=limit)
        times.append(elapsed)
        row_count = rows

    return {
        "p50": statistics.median(times),
        "mean": statistics.mean(times),
        "min": min(times),
        "max": max(times),
        "rows": row_count,
    }


def benchmark_mygramdb_count(
    host: str, port: int, query: str, iterations: int = 5,
) -> dict[str, Any]:
    """Benchmark MygramDB count."""
    client = MygramDBClient(host, port)

    # Warmup
    client.count("articles", query)

    times = []
    count = 0
    for _ in range(iterations):
        c, elapsed = client.count("articles", query)
        times.append(elapsed)
        count = c

    return {
        "p50": statistics.median(times),
        "mean": statistics.mean(times),
        "count": count,
    }


def benchmark_concurrent(
    func, concurrency: int, duration_sec: int = 10,
) -> dict[str, Any]:
    """Run concurrent benchmark and measure QPS and success rate."""
    total_queries = 0
    total_errors = 0
    latencies: list[float] = []
    stop_event = threading.Event()
    lock = threading.Lock()

    def worker():
        nonlocal total_queries, total_errors
        local_queries = 0
        local_errors = 0
        local_latencies: list[float] = []
        while not stop_event.is_set():
            try:
                _, elapsed = func()
                local_queries += 1
                local_latencies.append(elapsed)
            except Exception:
                local_errors += 1
        with lock:
            total_queries += local_queries
            total_errors += local_errors
            latencies.extend(local_latencies)

    threads = [threading.Thread(target=worker) for _ in range(concurrency)]
    start = time.perf_counter()
    for t in threads:
        t.start()

    time.sleep(duration_sec)
    stop_event.set()
    for t in threads:
        t.join()

    wall_time = time.perf_counter() - start
    total = total_queries + total_errors
    success_rate = (total_queries / total * 100) if total > 0 else 0
    qps = total_queries / wall_time if wall_time > 0 else 0

    result: dict[str, Any] = {
        "concurrency": concurrency,
        "duration": wall_time,
        "total_queries": total_queries,
        "total_errors": total_errors,
        "success_rate": success_rate,
        "qps": qps,
    }
    if latencies:
        latencies.sort()
        result["p50"] = latencies[len(latencies) // 2]
        result["p99"] = latencies[int(len(latencies) * 0.99)]
    return result


# ============================================================================
# Main
# ============================================================================

def fmt_ms(ms: float) -> str:
    if ms < 1:
        return f"{ms:.1f}ms"
    elif ms < 100:
        return f"{ms:.0f}ms"
    else:
        return f"{ms:,.0f}ms"


def main():
    parser = argparse.ArgumentParser(description="MygramDB vs MySQL FULLTEXT Benchmark")
    parser.add_argument("--mysql-host", default="127.0.0.1")
    parser.add_argument("--mysql-port", type=int, default=3306)
    parser.add_argument("--mysql-user", default="root")
    parser.add_argument("--mysql-password", default="root_secure_password_here")
    parser.add_argument("--mysql-db", default="mydb")
    parser.add_argument("--mygramdb-host", default="127.0.0.1")
    parser.add_argument("--mygramdb-port", type=int, default=11016)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--concurrent-duration", type=int, default=10)
    parser.add_argument("--json-output", type=str, default=None)
    args = parser.parse_args()

    mysql_args = dict(
        host=args.mysql_host, port=args.mysql_port,
        user=args.mysql_user, password=args.mysql_password, db=args.mysql_db,
    )
    mg_args = dict(host=args.mygramdb_host, port=args.mygramdb_port)

    # Get total row count
    conn = mysql.connector.connect(
        host=args.mysql_host, port=args.mysql_port,
        user=args.mysql_user, password=args.mysql_password, database=args.mysql_db,
    )
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM articles")
    total_rows = cursor.fetchone()[0]
    cursor.close()
    conn.close()

    print(f"=" * 70)
    print(f"MygramDB vs MySQL FULLTEXT Benchmark")
    print(f"Dataset: {total_rows:,} rows (Wikipedia articles, EN+JA)")
    print(f"Iterations per query: {args.iterations}")
    print(f"=" * 70)

    # Define test queries with expected match rates
    # These are chosen to represent different selectivity levels
    search_queries = [
        ("the", "Ultra high-freq"),      # very common English word
        ("university", "High-freq"),      # common but more specific
        ("quantum", "Medium-freq"),       # specialized term
        ("algorithm", "Low-freq"),        # technical term
    ]

    count_queries = [
        ("the", "Ultra high-freq"),
        ("algorithm", "Low-freq"),
    ]

    # Japanese queries
    ja_queries = [
        ("日本", "JA high-freq"),
        ("東京", "JA medium-freq"),
        ("科学", "JA low-freq"),
    ]

    results: dict[str, Any] = {"dataset_rows": total_rows, "tests": {}}

    # ---- Query Performance (SORT id LIMIT 100) ----
    print(f"\n## Query Performance (SORT id LIMIT 100)\n")
    print(f"| {'Query Type':<20} | {'Match Count':>12} | {'MySQL':>10} | {'MygramDB':>10} | {'Speedup':>8} |")
    print(f"|{'-'*22}|{'-'*14}|{'-'*12}|{'-'*12}|{'-'*10}|")

    for query, label in search_queries:
        mysql_r = benchmark_mysql_search(**mysql_args, query=query, iterations=args.iterations)
        mg_r = benchmark_mygramdb_search(**mg_args, query=query, iterations=args.iterations)

        # Get match count from MygramDB COUNT
        mg_count_r = benchmark_mygramdb_count(**mg_args, query=query, iterations=1)
        match_count = mg_count_r["count"]
        match_pct = match_count / total_rows * 100

        if "error" in mysql_r:
            mysql_str = "Error"
            speedup_str = "N/A"
            speedup = float("inf")
        else:
            mysql_str = fmt_ms(mysql_r["p50"])
            speedup = mysql_r["p50"] / mg_r["p50"] if mg_r["p50"] > 0 else float("inf")
            speedup_str = f"{speedup:.0f}x"
        print(f"| {label:<20} | {match_count:>8,} ({match_pct:>2.0f}%) | {mysql_str:>10} | {fmt_ms(mg_r['p50']):>10} | {speedup_str:>8} |")

        results["tests"][f"search_{label}"] = {
            "query": query, "match_count": match_count,
            "mysql_p50": mysql_r["p50"], "mygramdb_p50": mg_r["p50"],
            "speedup": speedup, "mysql_error": mysql_r.get("error"),
        }

    # ---- Japanese Query Performance ----
    print(f"\n## CJK Query Performance (SORT id LIMIT 100)\n")
    print(f"| {'Query Type':<20} | {'Match Count':>12} | {'MySQL':>10} | {'MygramDB':>10} | {'Speedup':>8} |")
    print(f"|{'-'*22}|{'-'*14}|{'-'*12}|{'-'*12}|{'-'*10}|")

    for query, label in ja_queries:
        mysql_r = benchmark_mysql_search(**mysql_args, query=query, iterations=args.iterations)
        mg_r = benchmark_mygramdb_search(**mg_args, query=query, iterations=args.iterations)

        mg_count_r = benchmark_mygramdb_count(**mg_args, query=query, iterations=1)
        match_count = mg_count_r["count"]
        match_pct = match_count / total_rows * 100

        if "error" in mysql_r:
            mysql_str = "Error"
            speedup_str = "N/A"
            speedup = float("inf")
        else:
            mysql_str = fmt_ms(mysql_r["p50"])
            speedup = mysql_r["p50"] / mg_r["p50"] if mg_r["p50"] > 0 else float("inf")
            speedup_str = f"{speedup:.0f}x"
        print(f"| {label:<20} | {match_count:>8,} ({match_pct:>2.0f}%) | {mysql_str:>10} | {fmt_ms(mg_r['p50']):>10} | {speedup_str:>8} |")

        results["tests"][f"cjk_{label}"] = {
            "query": query, "match_count": match_count,
            "mysql_p50": mysql_r["p50"], "mygramdb_p50": mg_r["p50"],
            "speedup": speedup, "mysql_error": mysql_r.get("error"),
        }

    # ---- COUNT Performance ----
    print(f"\n## COUNT Performance\n")
    print(f"| {'Query Type':<20} | {'Count':>12} | {'MySQL':>10} | {'MygramDB':>10} | {'Speedup':>8} |")
    print(f"|{'-'*22}|{'-'*14}|{'-'*12}|{'-'*12}|{'-'*10}|")

    for query, label in count_queries:
        mysql_r = benchmark_mysql_count(**mysql_args, query=query, iterations=args.iterations)
        mg_r = benchmark_mygramdb_count(**mg_args, query=query, iterations=args.iterations)

        count_val = mysql_r["count"] if mysql_r["count"] > 0 else mg_r["count"]
        if "error" in mysql_r:
            mysql_str = "Error"
            speedup_str = "N/A"
            speedup = float("inf")
        else:
            mysql_str = fmt_ms(mysql_r["p50"])
            speedup = mysql_r["p50"] / mg_r["p50"] if mg_r["p50"] > 0 else float("inf")
            speedup_str = f"{speedup:.0f}x"
        print(f"| {label:<20} | {count_val:>12,} | {mysql_str:>10} | {fmt_ms(mg_r['p50']):>10} | {speedup_str:>8} |")

        results["tests"][f"count_{label}"] = {
            "query": query, "count": count_val,
            "mysql_p50": mysql_r["p50"], "mygramdb_p50": mg_r["p50"],
            "speedup": speedup, "mysql_error": mysql_r.get("error"),
        }

    # ---- Concurrent Performance ----
    print(f"\n## Concurrent Performance (query: '{query_for_concurrent}', duration: {args.concurrent_duration}s)\n")
    print(f"| {'Load':<20} | {'MySQL':>30} | {'MygramDB':>30} |")
    print(f"|{'-'*22}|{'-'*32}|{'-'*32}|")

    query_for_concurrent = "quantum"

    for conc in [10, 100]:
        # Each thread creates its own connection via thread-local storage
        mysql_local = threading.local()
        mg_local = threading.local()

        def mysql_func():
            if not hasattr(mysql_local, "conn") or mysql_local.conn is None:
                mysql_local.conn = mysql.connector.connect(
                    host=args.mysql_host, port=args.mysql_port,
                    user=args.mysql_user, password=args.mysql_password,
                    database=args.mysql_db, connection_timeout=30,
                )
            c = mysql_local.conn
            cur = c.cursor()
            sql = "SELECT id FROM articles WHERE MATCH(content) AGAINST(%s IN BOOLEAN MODE) ORDER BY id LIMIT 100"
            start = time.perf_counter()
            cur.execute(sql, (query_for_concurrent,))
            cur.fetchall()
            elapsed = (time.perf_counter() - start) * 1000
            cur.close()
            return None, elapsed

        def mg_func():
            if not hasattr(mg_local, "sock") or mg_local.sock is None:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(10)
                s.connect((args.mygramdb_host, args.mygramdb_port))
                mg_local.sock = s
            s = mg_local.sock
            cmd = f"SEARCH articles {query_for_concurrent} SORT id LIMIT 100\r\n"
            start = time.perf_counter()
            s.sendall(cmd.encode())
            chunks = []
            while True:
                data = s.recv(65536)
                if not data:
                    break
                chunks.append(data)
                joined = b"".join(chunks)
                if joined.endswith(b"\r\n"):
                    break
            elapsed = (time.perf_counter() - start) * 1000
            return None, elapsed

        print(f"  Running {conc} concurrent (MySQL)...", file=sys.stderr, flush=True)
        mysql_conc = benchmark_concurrent(mysql_func, conc, args.concurrent_duration)
        print(f"  Running {conc} concurrent (MygramDB)...", file=sys.stderr, flush=True)
        mg_conc = benchmark_concurrent(mg_func, conc, args.concurrent_duration)

        mysql_str = f"{mysql_conc['success_rate']:.0f}% ok, QPS {mysql_conc['qps']:.1f}"
        mg_str = f"{mg_conc['success_rate']:.0f}% ok, QPS {mg_conc['qps']:.1f}"

        print(f"| {conc} concurrent{'':<9} | {mysql_str:>30} | {mg_str:>30} |")

        results["tests"][f"concurrent_{conc}"] = {
            "mysql": mysql_conc, "mygramdb": mg_conc,
        }

    print(f"\n{'=' * 70}")

    if args.json_output:
        with open(args.json_output, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nJSON results saved to {args.json_output}")


if __name__ == "__main__":
    main()
