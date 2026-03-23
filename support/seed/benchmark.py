#!/usr/bin/env python3
"""Benchmark MygramDB vs MySQL FULLTEXT on 1.1M Wikipedia dataset.

Compares search latency, count performance, and concurrent throughput
between MygramDB (in-memory n-gram index) and MySQL (InnoDB FULLTEXT with ngram parser).

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
from typing import Any


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
        count = resp.count("VALUE ")
        return count, elapsed

    def count(self, table: str, query: str) -> tuple[int, float]:
        """COUNT and return (count, elapsed_ms)."""
        cmd = f"COUNT {table} {query}"
        resp, elapsed = self._query(cmd)
        for line in resp.split("\r\n"):
            if "COUNT " in line:
                parts = line.strip().split()
                for i, p in enumerate(parts):
                    if p == "COUNT" and i + 1 < len(parts):
                        return int(parts[i + 1]), elapsed
        return 0, elapsed


# ============================================================================
# MySQL Client
# ============================================================================

def _mysql_connect(host: str, port: int, user: str, password: str, db: str):
    import mysql.connector
    return mysql.connector.connect(
        host=host, port=port, user=user, password=password, database=db,
    )


# ============================================================================
# Benchmark Helpers
# ============================================================================

def run_iterations(func, iterations: int) -> list[float]:
    """Run a function N times, return list of elapsed_ms."""
    # Warmup
    func()
    return [func() for _ in range(iterations)]


def benchmark_mysql_search(
    host: str, port: int, user: str, password: str, db: str,
    query: str, limit: int = 100, iterations: int = 5,
) -> dict[str, Any] | None:
    conn = _mysql_connect(host, port, user, password, db)
    cursor = conn.cursor()
    sql = f"SELECT id FROM articles WHERE MATCH(content) AGAINST(%s IN BOOLEAN MODE) ORDER BY id LIMIT {limit}"
    try:
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
    return {"p50": statistics.median(times), "mean": statistics.mean(times), "min": min(times), "max": max(times), "rows": row_count}


def benchmark_mysql_count(
    host: str, port: int, user: str, password: str, db: str,
    query: str, iterations: int = 5,
) -> dict[str, Any]:
    try:
        conn = _mysql_connect(host, port, user, password, db)
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
    return {"p50": statistics.median(times), "mean": statistics.mean(times), "count": count}


def benchmark_mygramdb_search(
    host: str, port: int, query: str, limit: int = 100, iterations: int = 5,
) -> dict[str, Any]:
    client = MygramDBClient(host, port)
    client.search("articles", query, limit=limit)
    times = []
    row_count = 0
    for _ in range(iterations):
        rows, elapsed = client.search("articles", query, limit=limit)
        times.append(elapsed)
        row_count = rows
    return {"p50": statistics.median(times), "mean": statistics.mean(times), "min": min(times), "max": max(times), "rows": row_count}


def benchmark_mygramdb_count(
    host: str, port: int, query: str, iterations: int = 5,
) -> dict[str, Any]:
    client = MygramDBClient(host, port)
    client.count("articles", query)
    times = []
    count = 0
    for _ in range(iterations):
        c, elapsed = client.count("articles", query)
        times.append(elapsed)
        count = c
    return {"p50": statistics.median(times), "mean": statistics.mean(times), "count": count}


def benchmark_concurrent(
    func, concurrency: int, duration_sec: int = 10,
) -> dict[str, Any]:
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
        # Warmup: establish connection before measurement starts
        try:
            func()
        except Exception:
            pass
        ready_event.wait()
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

    ready_event = threading.Event()
    threads = [threading.Thread(target=worker) for _ in range(concurrency)]
    for t in threads:
        t.start()
    time.sleep(2)  # wait for all threads to warmup
    ready_event.set()
    start = time.perf_counter()
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
# Formatting
# ============================================================================

def fmt_ms(ms: float) -> str:
    if ms == float("inf"):
        return "N/A"
    if ms < 1:
        return f"{ms:.2f}ms"
    elif ms < 100:
        return f"{ms:.1f}ms"
    else:
        return f"{ms:,.0f}ms"


def fmt_speedup(speedup: float) -> str:
    if speedup == float("inf"):
        return "N/A"
    if speedup >= 10:
        return f"\033[32m{speedup:,.0f}x faster\033[0m"
    elif speedup >= 2:
        return f"\033[32m{speedup:.1f}x faster\033[0m"
    elif speedup >= 1:
        return f"{speedup:.1f}x"
    else:
        return f"\033[31m{1/speedup:.1f}x slower\033[0m"


def bar(value: float, max_value: float, width: int = 30, char: str = "█") -> str:
    """Generate a horizontal bar."""
    if max_value <= 0 or value == float("inf"):
        return ""
    filled = int(min(value / max_value, 1.0) * width)
    return char * filled + "░" * (width - filled)


def section_header(title: str) -> str:
    return f"\n{'─' * 70}\n  {title}\n{'─' * 70}"


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(description="MygramDB vs MySQL FULLTEXT Benchmark")
    parser.add_argument("--mysql-host", default="127.0.0.1")
    parser.add_argument("--mysql-port", type=int, default=3306)
    parser.add_argument("--mysql-user", default="root")
    parser.add_argument("--mysql-password", default="mygramdb")
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

    # ── Connectivity check ────────────────────────────────────
    import mysql.connector
    print("\nChecking connectivity...")
    try:
        conn = mysql.connector.connect(
            host=args.mysql_host, port=args.mysql_port,
            user=args.mysql_user, password=args.mysql_password, database=args.mysql_db,
        )
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM articles")
        total_rows = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        print(f"  MySQL     : OK ({args.mysql_host}:{args.mysql_port})")
    except Exception as e:
        print(f"  MySQL     : FAILED - {e}")
        sys.exit(1)

    try:
        mg_client = MygramDBClient(args.mygramdb_host, args.mygramdb_port)
        mg_client._query("INFO")
        print(f"  MygramDB  : OK ({args.mygramdb_host}:{args.mygramdb_port})")
    except Exception as e:
        print(f"  MygramDB  : FAILED - {e}")
        sys.exit(1)

    # ── Header ────────────────────────────────────────────────
    print()
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║           MygramDB vs MySQL FULLTEXT — Benchmark Report            ║")
    print("╠══════════════════════════════════════════════════════════════════════╣")
    print("║                                                                    ║")
    print(f"║  Dataset      : {total_rows:>10,} rows (Wikipedia EN + JA articles)     ║")
    print(f"║  Iterations   : {args.iterations:>10} per query (median reported)         ║")
    print("║                                                                    ║")
    print("║  What this tests:                                                  ║")
    print("║    • Search latency — FULLTEXT vs in-memory n-gram index           ║")
    print("║    • Count performance — aggregate match counting                  ║")
    print("║    • CJK handling — Japanese bi-gram tokenization                  ║")
    print("║    • Concurrent load — throughput under parallel connections        ║")
    print("║                                                                    ║")
    print("║  Lower latency = better. Higher QPS = better.                      ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")

    # ── Test definitions ──────────────────────────────────────
    search_queries = [
        ("quantum physics", "Multi-word", "Phrase query — tests intersection of posting lists"),
        ("quantum", "Medium frequency", "Domain-specific term — good selectivity"),
        ("algorithm", "Low frequency", "Technical term — high selectivity"),
        ("fibonacci", "Rare term", "Very selective — minimal posting list scan"),
    ]

    ja_queries = [
        ("日本", "JA high-freq", "Common bigram — broad match across JA articles"),
        ("東京", "JA medium-freq", "City name — moderate selectivity"),
        ("科学", "JA low-freq", "Academic term — narrow match"),
    ]

    count_queries = [
        ("quantum", "Medium frequency"),
        ("algorithm", "Low frequency"),
    ]

    results: dict[str, Any] = {"dataset_rows": total_rows, "tests": {}}

    # ── 1. Search Performance ─────────────────────────────────
    print(section_header("1. SEARCH LATENCY  (SORT id LIMIT 100, p50)"))
    print()
    print(f"  {'Query':<18} {'Matches':>10}  {'MySQL':>10}  {'MygramDB':>10}  {'Result':>14}")
    print(f"  {'─' * 18} {'─' * 10}  {'─' * 10}  {'─' * 10}  {'─' * 14}")

    for query, label, _desc in search_queries:
        print(f"  Running '{query}'...{' ' * 60}", end="\r", flush=True)
        mysql_r = benchmark_mysql_search(**mysql_args, query=query, iterations=args.iterations)
        mg_r = benchmark_mygramdb_search(**mg_args, query=query, iterations=args.iterations)
        mg_count_r = benchmark_mygramdb_count(**mg_args, query=query, iterations=1)
        match_count = mg_count_r["count"]

        if "error" in mysql_r:
            speedup = float("inf")
        else:
            speedup = mysql_r["p50"] / mg_r["p50"] if mg_r["p50"] > 0 else float("inf")

        mysql_str = "Error" if "error" in mysql_r else fmt_ms(mysql_r["p50"])
        print(f"  {label:<18} {match_count:>9,}  {mysql_str:>10}  {fmt_ms(mg_r['p50']):>10}  {fmt_speedup(speedup):>14}")

        results["tests"][f"search_{label}"] = {
            "query": query, "match_count": match_count,
            "mysql_p50": mysql_r["p50"], "mygramdb_p50": mg_r["p50"],
            "speedup": speedup, "mysql_error": mysql_r.get("error"),
        }

    # ── 2. CJK Search Performance ────────────────────────────
    print(section_header("2. CJK SEARCH LATENCY  (Japanese bi-gram, SORT id LIMIT 100, p50)"))
    print()
    print(f"  {'Query':<18} {'Matches':>10}  {'MySQL':>10}  {'MygramDB':>10}  {'Result':>14}")
    print(f"  {'─' * 18} {'─' * 10}  {'─' * 10}  {'─' * 10}  {'─' * 14}")

    for query, label, _desc in ja_queries:
        print(f"  Running '{query}'...{' ' * 60}", end="\r", flush=True)
        mysql_r = benchmark_mysql_search(**mysql_args, query=query, iterations=args.iterations)
        mg_r = benchmark_mygramdb_search(**mg_args, query=query, iterations=args.iterations)
        mg_count_r = benchmark_mygramdb_count(**mg_args, query=query, iterations=1)
        match_count = mg_count_r["count"]

        if "error" in mysql_r:
            speedup = float("inf")
        else:
            speedup = mysql_r["p50"] / mg_r["p50"] if mg_r["p50"] > 0 else float("inf")

        mysql_str = "Error" if "error" in mysql_r else fmt_ms(mysql_r["p50"])
        print(f"  {label:<18} {match_count:>9,}  {mysql_str:>10}  {fmt_ms(mg_r['p50']):>10}  {fmt_speedup(speedup):>14}")

        results["tests"][f"cjk_{label}"] = {
            "query": query, "match_count": match_count,
            "mysql_p50": mysql_r["p50"], "mygramdb_p50": mg_r["p50"],
            "speedup": speedup, "mysql_error": mysql_r.get("error"),
        }

    # ── 3. COUNT Performance ──────────────────────────────────
    print(section_header("3. COUNT PERFORMANCE  (total matching rows, p50)"))
    print()
    print(f"  {'Query':<18} {'Count':>10}  {'MySQL':>10}  {'MygramDB':>10}  {'Result':>14}")
    print(f"  {'─' * 18} {'─' * 10}  {'─' * 10}  {'─' * 10}  {'─' * 14}")

    for query, label in count_queries:
        print(f"  Running '{query}'...{' ' * 60}", end="\r", flush=True)
        mysql_r = benchmark_mysql_count(**mysql_args, query=query, iterations=args.iterations)
        mg_r = benchmark_mygramdb_count(**mg_args, query=query, iterations=args.iterations)
        count_val = mysql_r["count"] if mysql_r["count"] > 0 else mg_r["count"]

        if "error" in mysql_r:
            speedup = float("inf")
        else:
            speedup = mysql_r["p50"] / mg_r["p50"] if mg_r["p50"] > 0 else float("inf")

        mysql_str = "Error" if "error" in mysql_r else fmt_ms(mysql_r["p50"])
        print(f"  {label:<18} {count_val:>10,}  {mysql_str:>10}  {fmt_ms(mg_r['p50']):>10}  {fmt_speedup(speedup):>14}")

        results["tests"][f"count_{label}"] = {
            "query": query, "count": count_val,
            "mysql_p50": mysql_r["p50"], "mygramdb_p50": mg_r["p50"],
            "speedup": speedup, "mysql_error": mysql_r.get("error"),
        }

    # ── 4. Concurrent Throughput ──────────────────────────────
    query_for_concurrent = "algorithm"
    print(section_header(f"4. CONCURRENT THROUGHPUT  (query: '{query_for_concurrent}', {args.concurrent_duration}s per level)"))
    print()
    print(f"  {'Connections':<14} {'MySQL QPS':>12} {'MygramDB QPS':>14} {'MySQL p50':>10} {'MG p50':>10} {'QPS ratio':>12}")
    print(f"  {'─' * 14} {'─' * 12} {'─' * 14} {'─' * 10} {'─' * 10} {'─' * 12}")

    for conc in [1, 4]:
        # Each worker thread creates its own connection
        import mysql.connector as mc

        def make_mysql_func(conc_level):
            conns: dict[int, mc.MySQLConnection] = {}
            def mysql_func():
                tid = threading.get_ident()
                if tid not in conns:
                    conns[tid] = mc.connect(
                        host=args.mysql_host, port=args.mysql_port,
                        user=args.mysql_user, password=args.mysql_password,
                        database=args.mysql_db, connection_timeout=60,
                    )
                c = conns[tid]
                cur = c.cursor()
                sql = "SELECT id FROM articles WHERE MATCH(content) AGAINST(%s IN BOOLEAN MODE) ORDER BY id LIMIT 100"
                start = time.perf_counter()
                cur.execute(sql, (query_for_concurrent,))
                cur.fetchall()
                elapsed = (time.perf_counter() - start) * 1000
                cur.close()
                return None, elapsed
            return mysql_func, conns

        def make_mg_func(conc_level):
            socks: dict[int, socket.socket] = {}
            def mg_func():
                tid = threading.get_ident()
                if tid not in socks:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(30)
                    s.connect((args.mygramdb_host, args.mygramdb_port))
                    socks[tid] = s
                s = socks[tid]
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
            return mg_func, socks

        print(f"  {conc} conn ...    ", end="", flush=True)
        mysql_func, mysql_conns = make_mysql_func(conc)
        mysql_conc = benchmark_concurrent(mysql_func, conc, args.concurrent_duration)
        for c in mysql_conns.values():
            try: c.close()
            except: pass

        mg_func, mg_socks = make_mg_func(conc)
        mg_conc = benchmark_concurrent(mg_func, conc, args.concurrent_duration)
        for s in mg_socks.values():
            try: s.close()
            except: pass

        qps_ratio = mg_conc["qps"] / mysql_conc["qps"] if mysql_conc["qps"] > 0 else float("inf")
        mysql_p50 = fmt_ms(mysql_conc.get("p50", 0))
        mg_p50 = fmt_ms(mg_conc.get("p50", 0))
        print(f"\r  {conc:<14} {mysql_conc['qps']:>11,.0f} {mg_conc['qps']:>13,.0f} {mysql_p50:>10} {mg_p50:>10} {fmt_speedup(qps_ratio):>12}")

        results["tests"][f"concurrent_{conc}"] = {"mysql": mysql_conc, "mygramdb": mg_conc}

    # ── Summary ───────────────────────────────────────────────
    print()
    print("╔══════════════════════════════════════════════════════════════════════╗")
    print("║                             Summary                                ║")
    print("╠══════════════════════════════════════════════════════════════════════╣")

    speedups = []
    for key, val in results["tests"].items():
        if key.startswith("concurrent_"):
            continue
        s = val.get("speedup", 0)
        if s != float("inf") and s > 0:
            speedups.append((key, s))

    if speedups:
        avg_speedup = statistics.mean([s for _, s in speedups])
        min_label, min_s = min(speedups, key=lambda x: x[1])
        max_label, max_s = max(speedups, key=lambda x: x[1])
        print(f"║  Average speedup  : {avg_speedup:>6.0f}x  (MygramDB vs MySQL FULLTEXT)       ║")
        print(f"║  Best case        : {max_s:>6.0f}x  ({max_label:<38s})║")
        print(f"║  Worst case       : {min_s:>6.0f}x  ({min_label:<38s})║")

    for conc in [10, 100]:
        key = f"concurrent_{conc}"
        if key in results["tests"]:
            my_qps = results["tests"][key]["mysql"]["qps"]
            mg_qps = results["tests"][key]["mygramdb"]["qps"]
            ratio = mg_qps / my_qps if my_qps > 0 else float("inf")
            print(f"║  Throughput @{conc:>3}c  : {mg_qps:>6,.0f} QPS vs {my_qps:>6,.0f} QPS ({ratio:.1f}x){' ' * (13 - len(f'{ratio:.1f}'))}║")

    print("║                                                                    ║")
    print("║  MygramDB keeps a full n-gram index in memory, eliminating disk     ║")
    print("║  I/O entirely. MySQL FULLTEXT uses B-tree on disk (with buffer      ║")
    print("║  pool caching). The gap widens with dataset size and concurrency.   ║")
    print("╚══════════════════════════════════════════════════════════════════════╝")
    print()

    if args.json_output:
        with open(args.json_output, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"JSON results saved to {args.json_output}")


if __name__ == "__main__":
    main()
