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
import math
import platform
import socket
import statistics
import subprocess
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

    def search_ids(self, table: str, query: str, sort: str = "id", limit: int = 100) -> list[int]:
        """SEARCH and return list of matching IDs."""
        cmd = f"SEARCH {table} {query} SORT {sort} LIMIT {limit}"
        resp, _ = self._query(cmd)
        # Response format: "OK RESULTS <total> <id1> <id2> ...\r\n"
        ids = []
        for line in resp.split("\r\n"):
            if line.startswith("OK RESULTS "):
                parts = line.split()
                # parts[0]="OK", parts[1]="RESULTS", parts[2]=total_count, parts[3:]=ids
                for p in parts[3:]:
                    try:
                        ids.append(int(p))
                    except ValueError:
                        pass
        return ids

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

def progress(message: str) -> None:
    """Write transient progress to stderr.

    Keeping it off stdout means a piped run produces a report that can be
    published verbatim, instead of one littered with carriage returns.
    """
    print(f"  {message}{' ' * 40}", end="\r", file=sys.stderr, flush=True)


def percentile(sorted_times: list[float], q: float) -> float:
    """Nearest-rank percentile over an already-sorted list."""
    if not sorted_times:
        return float("inf")
    rank = max(1, math.ceil(q * len(sorted_times)))
    return sorted_times[min(rank, len(sorted_times)) - 1]


def summarize(times: list[float]) -> dict[str, Any]:
    """Reduce raw timings to the distribution actually worth publishing.

    A median on its own hides the tail that decides whether an engine is
    usable, so p95/p99 and the spread are reported alongside it, with the
    sample count so a reader can tell how much the tail is worth.
    """
    if not times:
        return {"n": 0, "p50": float("inf"), "p95": float("inf"), "p99": float("inf"),
                "mean": float("inf"), "stdev": 0.0, "min": float("inf"), "max": float("inf")}
    ordered = sorted(times)
    return {
        "n": len(ordered),
        "p50": percentile(ordered, 0.50),
        "p95": percentile(ordered, 0.95),
        "p99": percentile(ordered, 0.99),
        "mean": statistics.mean(ordered),
        "stdev": statistics.stdev(ordered) if len(ordered) > 1 else 0.0,
        "min": ordered[0],
        "max": ordered[-1],
    }


def error_stats(message: str) -> dict[str, Any]:
    stats = summarize([])
    stats["error"] = message
    return stats


def benchmark_mysql_search(
    host: str, port: int, user: str, password: str, db: str,
    query: str, limit: int = 100, iterations: int = 5, warmup: int = 3,
) -> dict[str, Any] | None:
    conn = _mysql_connect(host, port, user, password, db)
    cursor = conn.cursor()
    sql = f"SELECT id FROM articles WHERE MATCH(content) AGAINST(%s IN BOOLEAN MODE) ORDER BY id LIMIT {limit}"
    try:
        for _ in range(warmup):
            cursor.execute(sql, (query,))
            cursor.fetchall()
    except Exception as e:
        cursor.close()
        conn.close()
        stats = error_stats(str(e))
        stats["rows"] = 0
        return stats

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
    stats = summarize(times)
    stats["rows"] = row_count
    return stats


def benchmark_mysql_count(
    host: str, port: int, user: str, password: str, db: str,
    query: str, iterations: int = 5, warmup: int = 3,
) -> dict[str, Any]:
    try:
        conn = _mysql_connect(host, port, user, password, db)
    except Exception as e:
        stats = error_stats(str(e))
        stats["count"] = 0
        return stats
    cursor = conn.cursor()
    sql = "SELECT COUNT(*) FROM articles WHERE MATCH(content) AGAINST(%s IN BOOLEAN MODE)"
    try:
        for _ in range(warmup):
            cursor.execute(sql, (query,))
            cursor.fetchall()
    except Exception as e:
        cursor.close()
        conn.close()
        stats = error_stats(str(e))
        stats["count"] = 0
        return stats
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
    stats = summarize(times)
    stats["count"] = count
    return stats


def benchmark_mygramdb_search(
    host: str, port: int, query: str, limit: int = 100, iterations: int = 5, warmup: int = 3,
) -> dict[str, Any]:
    client = MygramDBClient(host, port)
    for _ in range(warmup):
        client.search("articles", query, limit=limit)
    times = []
    row_count = 0
    for _ in range(iterations):
        rows, elapsed = client.search("articles", query, limit=limit)
        times.append(elapsed)
        row_count = rows
    stats = summarize(times)
    stats["rows"] = row_count
    return stats


def benchmark_mygramdb_count(
    host: str, port: int, query: str, iterations: int = 5, warmup: int = 3,
) -> dict[str, Any]:
    client = MygramDBClient(host, port)
    for _ in range(warmup):
        client.count("articles", query)
    times = []
    count = 0
    for _ in range(iterations):
        c, elapsed = client.count("articles", query)
        times.append(elapsed)
        count = c
    stats = summarize(times)
    stats["count"] = count
    return stats


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
        result.update(summarize(latencies))
    return result


# ============================================================================
# Environment capture
# ============================================================================

def _mysql_settings(host: str, port: int, user: str, password: str, db: str) -> dict[str, Any]:
    """Record the baseline's own configuration alongside its numbers.

    A latency figure means nothing without the settings that produced it, and
    the buffer pool in particular decides whether MySQL is being measured on
    memory or on disk.
    """
    wanted = [
        "version",
        "version_comment",
        "innodb_buffer_pool_size",
        "innodb_buffer_pool_instances",
        "innodb_ft_cache_size",
        "innodb_ft_total_cache_size",
        "ngram_token_size",
    ]
    settings: dict[str, Any] = {}
    try:
        conn = _mysql_connect(host, port, user, password, db)
        cursor = conn.cursor()
        for name in wanted:
            cursor.execute(f"SELECT @@GLOBAL.{name}")
            settings[name] = cursor.fetchone()[0]
        cursor.execute(
            "SELECT ROUND(SUM(data_length)/1048576), ROUND(SUM(index_length)/1048576) "
            "FROM information_schema.tables WHERE table_schema=%s AND table_name='articles'",
            (db,),
        )
        row = cursor.fetchone()
        if row:
            settings["articles_data_mb"] = row[0]
            settings["articles_index_mb"] = row[1]
        cursor.close()
        conn.close()
    except Exception as e:
        settings["error"] = str(e)
    return settings


def _mysql_buffer_pool_state(host: str, port: int, user: str, password: str, db: str) -> dict[str, Any]:
    """Measure how much of the baseline's work was served from RAM.

    Sizing the pool is only a claim of fairness; the hit ratio is the evidence
    for it. A run where MySQL still fell back to disk is a run where the
    comparison was against I/O rather than against the index structure.
    """
    state: dict[str, Any] = {}
    try:
        conn = _mysql_connect(host, port, user, password, db)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT VARIABLE_NAME, VARIABLE_VALUE FROM performance_schema.global_status "
            "WHERE VARIABLE_NAME IN ('Innodb_buffer_pool_read_requests','Innodb_buffer_pool_reads',"
            "'Innodb_buffer_pool_pages_data','Innodb_buffer_pool_pages_total')"
        )
        raw = {name: int(value) for name, value in cursor.fetchall()}
        cursor.close()
        conn.close()
        requests = raw.get("Innodb_buffer_pool_read_requests", 0)
        disk = raw.get("Innodb_buffer_pool_reads", 0)
        state = dict(raw)
        if requests:
            state["hit_ratio_pct"] = round(100.0 * (requests - disk) / requests, 4)
    except Exception as e:
        state["error"] = str(e)
    return state


def _host_info() -> dict[str, Any]:
    info: dict[str, Any] = {
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
    }
    try:
        if sys.platform == "darwin":
            info["cpu"] = subprocess.check_output(
                ["sysctl", "-n", "machdep.cpu.brand_string"], text=True,
            ).strip()
            info["cpu_count"] = int(subprocess.check_output(["sysctl", "-n", "hw.ncpu"], text=True).strip())
            info["memory_gb"] = round(
                int(subprocess.check_output(["sysctl", "-n", "hw.memsize"], text=True).strip()) / 1024**3
            )
    except Exception:
        pass
    try:
        info["docker"] = subprocess.check_output(
            ["docker", "version", "--format", "{{.Server.Version}}"], text=True, stderr=subprocess.DEVNULL,
        ).strip()
    except Exception:
        pass
    return info


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
    return f"\n{'─' * 94}\n  {title}\n{'─' * 94}"


def latency_header(first: str = "Query", second: str = "Matches") -> str:
    return (
        f"  {first:<18} {second:>10}  {'MySQL p50':>10} {'p99':>9}  "
        f"{'MygramDB p50':>13} {'p99':>9}  {'Result':>14}"
    )


def latency_rule() -> str:
    return (
        f"  {'─' * 18} {'─' * 10}  {'─' * 10} {'─' * 9}  "
        f"{'─' * 13} {'─' * 9}  {'─' * 14}"
    )


def latency_row(label: str, count: int, mysql_r: dict, mg_r: dict, speedup: float) -> str:
    failed = "error" in mysql_r
    mysql_p50 = "Error" if failed else fmt_ms(mysql_r["p50"])
    mysql_p99 = "" if failed else fmt_ms(mysql_r["p99"])
    return (
        f"  {label:<18} {count:>10,}  {mysql_p50:>10} {mysql_p99:>9}  "
        f"{fmt_ms(mg_r['p50']):>13} {fmt_ms(mg_r['p99']):>9}  {fmt_speedup(speedup):>14}"
    )


def compute_speedup(mysql_r: dict, mg_r: dict) -> float:
    if "error" in mysql_r:
        return float("inf")
    return mysql_r["p50"] / mg_r["p50"] if mg_r["p50"] > 0 else float("inf")


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
    # A p99 needs enough samples to mean anything; 10 iterations cannot
    # produce one, so the default is high enough for the tail to be real.
    parser.add_argument("--iterations", type=int, default=200)
    parser.add_argument("--warmup", type=int, default=5)
    parser.add_argument("--concurrent-duration", type=int, default=10)
    parser.add_argument(
        "--concurrency-levels", type=str, default="1,2,4,8,16",
        help="Comma-separated connection counts for the throughput sweep",
    )
    parser.add_argument("--json-output", type=str, default=None)
    args = parser.parse_args()

    concurrency_levels = [int(c) for c in args.concurrency_levels.split(",") if c.strip()]

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
    print(f"║  Iterations   : {args.iterations:>10} per query (p50/p95/p99 reported)    ║")
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

    mysql_settings = _mysql_settings(
        args.mysql_host, args.mysql_port, args.mysql_user, args.mysql_password, args.mysql_db,
    )
    results: dict[str, Any] = {
        "dataset_rows": total_rows,
        "iterations": args.iterations,
        "warmup": args.warmup,
        "environment": {
            "host": _host_info(),
            "mysql": mysql_settings,
        },
        "tests": {},
    }

    pool_bytes = mysql_settings.get("innodb_buffer_pool_size")
    if isinstance(pool_bytes, int):
        data_mb = mysql_settings.get("articles_data_mb") or 0
        index_mb = mysql_settings.get("articles_index_mb") or 0
        print(
            f"  Baseline    : MySQL buffer pool {pool_bytes / 1024**3:.1f}GB "
            f"vs {float(data_mb) + float(index_mb):,.0f}MB of table + index"
        )

    # ── 1. Search Performance ─────────────────────────────────
    print(section_header("1. SEARCH LATENCY  (SORT id LIMIT 100)"))
    print()
    print(latency_header())
    print(latency_rule())

    for query, label, _desc in search_queries:
        progress(f"Running '{query}'...")
        mysql_r = benchmark_mysql_search(**mysql_args, query=query, iterations=args.iterations, warmup=args.warmup)
        mg_r = benchmark_mygramdb_search(**mg_args, query=query, iterations=args.iterations, warmup=args.warmup)
        mg_count_r = benchmark_mygramdb_count(**mg_args, query=query, iterations=1, warmup=1)
        match_count = mg_count_r["count"]
        speedup = compute_speedup(mysql_r, mg_r)
        print(latency_row(label, match_count, mysql_r, mg_r, speedup))

        results["tests"][f"search_{label}"] = {
            "query": query, "match_count": match_count,
            "mysql": mysql_r, "mygramdb": mg_r,
            "mysql_p50": mysql_r["p50"], "mygramdb_p50": mg_r["p50"],
            "speedup": speedup, "mysql_error": mysql_r.get("error"),
        }

    # ── 2. CJK Search Performance ────────────────────────────
    print(section_header("2. CJK SEARCH LATENCY  (Japanese bi-gram, SORT id LIMIT 100)"))
    print()
    print(latency_header())
    print(latency_rule())

    for query, label, _desc in ja_queries:
        progress(f"Running '{query}'...")
        mysql_r = benchmark_mysql_search(**mysql_args, query=query, iterations=args.iterations, warmup=args.warmup)
        mg_r = benchmark_mygramdb_search(**mg_args, query=query, iterations=args.iterations, warmup=args.warmup)
        mg_count_r = benchmark_mygramdb_count(**mg_args, query=query, iterations=1, warmup=1)
        match_count = mg_count_r["count"]
        speedup = compute_speedup(mysql_r, mg_r)
        print(latency_row(label, match_count, mysql_r, mg_r, speedup))

        results["tests"][f"cjk_{label}"] = {
            "query": query, "match_count": match_count,
            "mysql": mysql_r, "mygramdb": mg_r,
            "mysql_p50": mysql_r["p50"], "mygramdb_p50": mg_r["p50"],
            "speedup": speedup, "mysql_error": mysql_r.get("error"),
        }

    # ── 3. COUNT Performance ──────────────────────────────────
    print(section_header("3. COUNT PERFORMANCE  (total matching rows)"))
    print()
    print(latency_header(first="Query", second="Count"))
    print(latency_rule())

    for query, label in count_queries:
        progress(f"Running '{query}'...")
        mysql_r = benchmark_mysql_count(**mysql_args, query=query, iterations=args.iterations, warmup=args.warmup)
        mg_r = benchmark_mygramdb_count(**mg_args, query=query, iterations=args.iterations, warmup=args.warmup)
        count_val = mysql_r["count"] if mysql_r["count"] > 0 else mg_r["count"]
        speedup = compute_speedup(mysql_r, mg_r)
        print(latency_row(label, count_val, mysql_r, mg_r, speedup))

        results["tests"][f"count_{label}"] = {
            "query": query, "count": count_val,
            "mysql": mysql_r, "mygramdb": mg_r,
            "mysql_p50": mysql_r["p50"], "mygramdb_p50": mg_r["p50"],
            "speedup": speedup, "mysql_error": mysql_r.get("error"),
        }

    # ── 4. Result Consistency ─────────────────────────────────
    consistency_queries = [
        ("quantum", "EN medium"),
        ("algorithm", "EN rare"),
        ("日本", "JA common"),
        ("科学", "JA rare"),
    ]
    print(section_header("4. RESULT CONSISTENCY  (MygramDB vs MySQL, total match count)"))
    print()
    print("  Compares total match counts between both engines.")
    print("  N-gram indexes may produce false positives (matches on partial n-gram overlap).")
    print()
    print(f"  {'Query':<14} {'MySQL':>10} {'MygramDB':>10} {'Diff':>8} {'Note':>30}")
    print(f"  {'─' * 14} {'─' * 10} {'─' * 10} {'─' * 8} {'─' * 30}")

    mg_client = MygramDBClient(args.mygramdb_host, args.mygramdb_port)

    for query, label in consistency_queries:
        progress(f"Running '{query}'...")
        # MySQL COUNT
        try:
            conn = _mysql_connect(args.mysql_host, args.mysql_port, args.mysql_user, args.mysql_password, args.mysql_db)
            cursor = conn.cursor()
            cursor.execute(
                "SELECT COUNT(*) FROM articles WHERE MATCH(content) AGAINST(%s IN BOOLEAN MODE)",
                (query,),
            )
            mysql_count = cursor.fetchone()[0]
            cursor.close()
            conn.close()
        except Exception:
            mysql_count = -1

        # MygramDB COUNT
        mg_count, _ = mg_client.count("articles", query)

        if mysql_count < 0:
            diff_str = "N/A"
            note = "MySQL error"
        else:
            diff = mg_count - mysql_count
            diff_pct = (diff / mysql_count * 100) if mysql_count > 0 else 0
            diff_str = f"{diff:+,}"
            if diff == 0:
                note = "\033[32mexact match\033[0m"
            elif diff > 0:
                note = f"\033[33m+{diff_pct:.1f}% false positives (n-gram)\033[0m"
            else:
                note = f"\033[31m{diff_pct:.1f}% missing\033[0m"

        mysql_str = f"{mysql_count:,}" if mysql_count >= 0 else "Error"
        print(f"  {label:<14} {mysql_str:>10} {mg_count:>10,} {diff_str:>8} {note:>30}")

        results["tests"][f"consistency_{label}"] = {
            "query": query,
            "mysql_count": mysql_count,
            "mygramdb_count": mg_count,
        }

    print()
    print("  N-gram false positives are expected. Enable verify_text (v1.5+) to eliminate them.")

    # ── 5. Concurrent Throughput ──────────────────────────────
    query_for_concurrent = "algorithm"
    print(section_header(f"5. CONCURRENT THROUGHPUT  (query: '{query_for_concurrent}', {args.concurrent_duration}s per level)"))
    print()
    print(
        f"  {'Connections':<12} {'MySQL QPS':>10} {'MygramDB QPS':>13} "
        f"{'MySQL p50':>10} {'p99':>9} {'MG p50':>9} {'p99':>9} {'QPS ratio':>12}"
    )
    print(
        f"  {'─' * 12} {'─' * 10} {'─' * 13} "
        f"{'─' * 10} {'─' * 9} {'─' * 9} {'─' * 9} {'─' * 12}"
    )

    for conc in concurrency_levels:
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

        progress(f"Running {conc}-connection level...")
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
        print(
            f"  {conc:<12} {mysql_conc['qps']:>10,.0f} {mg_conc['qps']:>13,.0f} "
            f"{fmt_ms(mysql_conc.get('p50', 0)):>10} {fmt_ms(mysql_conc.get('p99', 0)):>9} "
            f"{fmt_ms(mg_conc.get('p50', 0)):>9} {fmt_ms(mg_conc.get('p99', 0)):>9} "
            f"{fmt_speedup(qps_ratio):>12}"
        )

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
        median_speedup = statistics.median([s for _, s in speedups])
        for line in (
            f"  Median speedup : {median_speedup:>6.1f}x  (MygramDB vs MySQL FULLTEXT)",
            f"  Mean speedup   : {avg_speedup:>6.1f}x  (skewed by the best case below)",
            f"  Best case      : {max_s:>6.1f}x  ({max_label})",
            f"  Worst case     : {min_s:>6.1f}x  ({min_label})",
        ):
            print(f"║{line:<68}║")

    for conc in concurrency_levels:
        key = f"concurrent_{conc}"
        if key in results["tests"]:
            my_qps = results["tests"][key]["mysql"]["qps"]
            mg_qps = results["tests"][key]["mygramdb"]["qps"]
            ratio = mg_qps / my_qps if my_qps > 0 else float("inf")
            line = f"  Throughput @{conc:>3}c  : {mg_qps:>6,.0f} QPS vs {my_qps:>6,.0f} QPS ({ratio:.1f}x)"
            print(f"║{line:<68}║")

    print("║                                                                    ║")
    print("║                                                                    ║")
    if isinstance(pool_bytes, int):
        baseline = f"  Baseline     : MySQL {mysql_settings.get('version', '?')} with a {pool_bytes / 1024**3:.0f}GB buffer pool"
        print(f"║{baseline:<68}║")
    print("║                                                                    ║")
    print("║  MygramDB keeps a full n-gram index in memory. MySQL FULLTEXT uses  ║")
    print("║  a B-tree the buffer pool above is sized to hold, so what is left   ║")
    print("║  is the difference between the two index structures rather than a   ║")
    print("║  difference in how much disk each one had to touch.                 ║")
    print("║                                                                    ║")
    print("║  The ratios carry across machines; the millisecond figures do not.  ║")
    print("║  Re-run with 'make bench-up && make bench-run' on your own hardware.║")
    print("╚══════════════════════════════════════════════════════════════════════╝")
    print()

    # Sampled after the workload so the ratio describes this run, not the idle
    # server that preceded it.
    pool_state = _mysql_buffer_pool_state(
        args.mysql_host, args.mysql_port, args.mysql_user, args.mysql_password, args.mysql_db,
    )
    results["environment"]["mysql_buffer_pool_after_run"] = pool_state
    if "hit_ratio_pct" in pool_state:
        print(
            f"  MySQL served {pool_state['hit_ratio_pct']:.3f}% of page requests from its buffer pool "
            f"during this run ({pool_state.get('Innodb_buffer_pool_reads', 0):,} disk reads)."
        )
        print()

    if args.json_output:
        with open(args.json_output, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"JSON results saved to {args.json_output}")


if __name__ == "__main__":
    main()
