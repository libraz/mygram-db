#!/usr/bin/env python3
"""
MygramDB vs MySQL FULLTEXT Benchmark Tool

Usage:
    # Single query benchmark
    python benchmark.py --target mygramdb --table articles --words "hello" --concurrency 1

    # Concurrent benchmark
    python benchmark.py --target mysql --table articles --words "hello,world,test" --concurrency 100

Environment variables:
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
    MYGRAMDB_HOST, MYGRAMDB_PORT
"""

import argparse
import os
import socket
import time
import statistics
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Tuple, Protocol

# Optional MySQL connector
try:
    import mysql.connector
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False


class BenchmarkClient(Protocol):
    """Protocol for benchmark clients"""
    def query(self, cmd: str, timeout: float = 60.0) -> Tuple[bool, float, str]: ...


class MygramDBClient:
    """MygramDB TCP client"""

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    def query(self, cmd: str, timeout: float = 60.0) -> Tuple[bool, float, str]:
        """Execute query and return (success, elapsed_ms, response)"""
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect((self.host, self.port))

            start = time.perf_counter()
            sock.sendall((cmd + "\r\n").encode('utf-8'))

            data = b""
            while True:
                chunk = sock.recv(65536)
                if not chunk:
                    break
                data += chunk
                # Response ends with \r\n
                if data.endswith(b"\r\n"):
                    break

            elapsed = (time.perf_counter() - start) * 1000
            sock.close()

            response = data.decode('utf-8', errors='ignore')
            success = response.startswith("OK ") or response.startswith("(integer)")
            return success, elapsed, response
        except Exception as e:
            return False, 0.0, str(e)


class MySQLClient:
    """MySQL client using mysql-connector-python"""

    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        self.config = {
            'host': host,
            'port': port,
            'user': user,
            'password': password,
            'database': database,
            'charset': 'utf8mb4',
            'use_unicode': True,
        }

    def query(self, sql: str, timeout: float = 60.0) -> Tuple[bool, float, str]:
        """Execute query and return (success, elapsed_ms, response)"""
        if not HAS_MYSQL:
            return False, 0.0, "mysql-connector-python not installed"

        try:
            conn = mysql.connector.connect(**self.config)
            cursor = conn.cursor()

            start = time.perf_counter()
            cursor.execute(sql)
            results = cursor.fetchall()
            elapsed = (time.perf_counter() - start) * 1000

            cursor.close()
            conn.close()

            return True, elapsed, f"{len(results)} rows"
        except Exception as e:
            return False, 0.0, str(e)


def run_benchmark(
    client: BenchmarkClient,
    queries: List[str],
    concurrency: int,
    iterations: int,
) -> dict:
    """Run benchmark with given queries and concurrency"""

    times: List[float] = []
    errors: List[str] = []
    successful = 0
    failed = 0

    def execute_query(query: str) -> Tuple[bool, float, str]:
        return client.query(query)

    all_queries = queries * iterations

    start_total = time.perf_counter()

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(execute_query, q) for q in all_queries]

        for future in as_completed(futures):
            success, elapsed, response = future.result()

            if success:
                successful += 1
                times.append(elapsed)
            else:
                failed += 1
                errors.append(response)

    total_time_ms = (time.perf_counter() - start_total) * 1000

    results: dict = {
        'total_queries': len(all_queries),
        'successful': successful,
        'failed': failed,
        'total_time_ms': total_time_ms,
        'times': times,
        'errors': errors,
    }

    if times:
        results['avg_ms'] = statistics.mean(times)
        results['min_ms'] = min(times)
        results['max_ms'] = max(times)
        results['p50_ms'] = statistics.median(times)
        if len(times) >= 20:
            sorted_times = sorted(times)
            p95_idx = int(len(sorted_times) * 0.95)
            p99_idx = int(len(sorted_times) * 0.99)
            results['p95_ms'] = sorted_times[p95_idx]
            results['p99_ms'] = sorted_times[p99_idx]

    return results


def build_mygramdb_queries(table: str, words: List[str], query_type: str, limit: int, offset: int = 0) -> List[str]:
    """Build MygramDB queries"""
    queries = []
    for word in words:
        if query_type == 'search':
            if offset > 0:
                queries.append(f"SEARCH {table} {word} SORT id ASC LIMIT {offset},{limit}")
            else:
                queries.append(f"SEARCH {table} {word} SORT id ASC LIMIT {limit}")
        elif query_type == 'count':
            queries.append(f"COUNT {table} {word}")
    return queries


def build_mysql_queries(table: str, column: str, words: List[str], query_type: str, limit: int, offset: int = 0):
    """Build MySQL FULLTEXT queries"""
    queries = []
    for word in words:
        match_clause = f"MATCH({column}) AGAINST('{word}' IN BOOLEAN MODE)"
        if query_type == 'search':
            sql = f"SELECT id FROM {table} WHERE enabled=1 AND {match_clause} ORDER BY id LIMIT {offset},{limit}"
            queries.append(sql)
        elif query_type == 'count':
            queries.append(f"SELECT COUNT(*) FROM {table} WHERE enabled=1 AND {match_clause}")
    return queries


def main():
    parser = argparse.ArgumentParser(description='MygramDB vs MySQL Benchmark')
    parser.add_argument('--target', choices=['mygramdb', 'mysql', 'both'], default='both', help='Target to benchmark')
    parser.add_argument('--table', required=True, help='Table name')
    parser.add_argument('--column', default='name', help='FULLTEXT column (MySQL only)')
    parser.add_argument('--words', required=True, help='Comma-separated search words')
    parser.add_argument('--query-type', choices=['search', 'count'], default='search', help='Query type')
    parser.add_argument('--limit', type=int, default=100, help='LIMIT for search queries')
    parser.add_argument('--offset', type=int, default=0, help='OFFSET for search queries (pagination)')
    parser.add_argument('--concurrency', type=int, default=1, help='Number of concurrent queries')
    parser.add_argument('--iterations', type=int, default=5, help='Iterations per query')

    # Connection options (override env vars)
    parser.add_argument('--mysql-host', help='MySQL host')
    parser.add_argument('--mysql-port', type=int, help='MySQL port')
    parser.add_argument('--mysql-user', help='MySQL user')
    parser.add_argument('--mysql-password', help='MySQL password')
    parser.add_argument('--mysql-database', help='MySQL database')
    parser.add_argument('--mygramdb-host', help='MygramDB host')
    parser.add_argument('--mygramdb-port', type=int, help='MygramDB port')

    args = parser.parse_args()

    words = [w.strip() for w in args.words.split(',')]

    # MySQL configuration
    mysql_config = {
        'host': args.mysql_host or os.environ.get('MYSQL_HOST', '127.0.0.1'),
        'port': args.mysql_port or int(os.environ.get('MYSQL_PORT', '3306')),
        'user': args.mysql_user or os.environ.get('MYSQL_USER', 'root'),
        'password': args.mysql_password or os.environ.get('MYSQL_PASSWORD', ''),
        'database': args.mysql_database or os.environ.get('MYSQL_DATABASE', 'test'),
    }

    # MygramDB configuration
    mygramdb_config = {
        'host': args.mygramdb_host or os.environ.get('MYGRAMDB_HOST', '127.0.0.1'),
        'port': args.mygramdb_port or int(os.environ.get('MYGRAMDB_PORT', '11016')),
    }

    print("=== Benchmark Configuration ===")
    print(f"Table: {args.table}")
    print(f"Words: {words}")
    print(f"Query Type: {args.query_type}")
    print(f"Limit: {args.limit}, Offset: {args.offset}")
    print(f"Concurrency: {args.concurrency}")
    print(f"Iterations: {args.iterations}")
    print()

    # MygramDB benchmark
    if args.target in ('mygramdb', 'both'):
        print("=== MygramDB Benchmark ===")
        print(f"Host: {mygramdb_config['host']}:{mygramdb_config['port']}")

        client = MygramDBClient(mygramdb_config['host'], mygramdb_config['port'])
        queries = build_mygramdb_queries(args.table, words, args.query_type, args.limit, args.offset)

        results = run_benchmark(client, queries, args.concurrency, args.iterations)

        print(f"Total queries: {results['total_queries']}")
        print(f"Successful: {results['successful']}")
        print(f"Failed: {results['failed']}")
        print(f"Total time: {results['total_time_ms']:.1f}ms")
        if results['times']:
            print(f"Avg: {results['avg_ms']:.2f}ms")
            print(f"Min: {results['min_ms']:.2f}ms")
            print(f"Max: {results['max_ms']:.2f}ms")
            print(f"P50: {results['p50_ms']:.2f}ms")
            if 'p95_ms' in results:
                print(f"P95: {results['p95_ms']:.2f}ms")
                print(f"P99: {results['p99_ms']:.2f}ms")
            print(f"QPS: {results['successful'] / (results['total_time_ms'] / 1000):.1f}")
        if results['errors']:
            print(f"Errors: {results['errors'][:3]}")
        print()

    # MySQL benchmark
    if args.target in ('mysql', 'both'):
        if not HAS_MYSQL:
            print("=== MySQL Benchmark ===")
            print("ERROR: mysql-connector-python not installed")
            print("Install with: pip install mysql-connector-python")
            print()
        else:
            print("=== MySQL Benchmark ===")
            print(f"Host: {mysql_config['host']}:{mysql_config['port']}")

            client = MySQLClient(
                mysql_config['host'],
                mysql_config['port'],
                mysql_config['user'],
                mysql_config['password'],
                mysql_config['database'],
            )
            queries = build_mysql_queries(args.table, args.column, words, args.query_type, args.limit, args.offset)

            results = run_benchmark(client, queries, args.concurrency, args.iterations)

            print(f"Total queries: {results['total_queries']}")
            print(f"Successful: {results['successful']}")
            print(f"Failed: {results['failed']}")
            print(f"Total time: {results['total_time_ms']:.1f}ms")
            if results['times']:
                print(f"Avg: {results['avg_ms']:.2f}ms")
                print(f"Min: {results['min_ms']:.2f}ms")
                print(f"Max: {results['max_ms']:.2f}ms")
                print(f"P50: {results['p50_ms']:.2f}ms")
                if 'p95_ms' in results:
                    print(f"P95: {results['p95_ms']:.2f}ms")
                    print(f"P99: {results['p99_ms']:.2f}ms")
                print(f"QPS: {results['successful'] / (results['total_time_ms'] / 1000):.1f}")
            if results['errors']:
                print(f"Errors: {results['errors'][:3]}")
            print()


if __name__ == '__main__':
    main()
