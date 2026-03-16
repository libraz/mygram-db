"""Benchmark execution engine with warmup/measurement phases."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor

from lib.benchmark.connection_pool import ConnectionPool
from lib.stats import BenchmarkResult


class BenchmarkRunner:
    """Orchestrates benchmark execution with warmup and measurement phases.

    Args:
        pool: Connection pool providing persistent connections.
        warmup_queries: Number of warmup iterations per query.
        measurement_duration: Default measurement duration in seconds.
    """

    def __init__(
        self,
        pool: ConnectionPool,
        warmup_queries: int = 50,
        measurement_duration: float = 15.0,
    ) -> None:
        self._pool = pool
        self._warmup_queries = warmup_queries
        self._measurement_duration = measurement_duration

    def ensure_connections(self) -> None:
        """Ensure all pool connections are established."""
        self._pool.establish_all()

    def warmup(self, queries: list[str], iterations: int | None = None) -> None:
        """Run each query sequentially for warmup. Results are discarded."""
        if iterations is None:
            iterations = self._warmup_queries
        conn = self._pool.get_connection()
        try:
            for query in queries:
                for _ in range(iterations):
                    success, _, resp = conn.command_timed(query)
                    if hasattr(conn, "is_closed") and conn.is_closed:
                        # Connection broke, get a fresh one
                        conn.close()
                        try:
                            conn = self._pool._create_connection()
                        except Exception:
                            self._pool.return_connection(conn)
                            return
                        break
        finally:
            self._pool.return_connection(conn)

    def run(
        self,
        queries: list[str],
        concurrency: int,
        duration: float | None = None,
    ) -> BenchmarkResult:
        """Execute the benchmark measurement phase.

        Runs queries concurrently for the specified duration. Each worker gets
        a persistent connection and executes queries in round-robin until time
        expires.
        """
        if not queries:
            return BenchmarkResult()

        measurement_secs = duration if duration is not None else self._measurement_duration
        stop_time = time.monotonic() + measurement_secs
        start_wall = time.perf_counter()

        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(self._worker, queries, stop_time)
                for _ in range(concurrency)
            ]
            all_results: list[list[tuple[bool, float, str]]] = [
                f.result() for f in futures
            ]

        total_time_ms = (time.perf_counter() - start_wall) * 1000

        result = BenchmarkResult()
        result.total_time_ms = total_time_ms

        for worker_results in all_results:
            for success, elapsed_ms, response in worker_results:
                result.total_queries += 1
                if success:
                    result.successful += 1
                    result.times.append(elapsed_ms)
                else:
                    result.failed += 1
                    result.errors.append(response)

        return result

    def _worker(
        self,
        queries: list[str],
        stop_time: float,
    ) -> list[tuple[bool, float, str]]:
        """Worker: get a persistent connection, run queries until stop time."""
        conn = self._pool.get_connection()
        try:
            results: list[tuple[bool, float, str]] = []
            idx = 0
            num_queries = len(queries)
            while time.monotonic() < stop_time:
                success, elapsed, resp = conn.command_timed(queries[idx % num_queries])
                results.append((success, elapsed, resp))
                idx += 1
                if hasattr(conn, "is_closed") and conn.is_closed:
                    # Reconnect with retry
                    conn.close()
                    try:
                        conn = self._pool._create_connection()
                    except Exception:
                        # Cannot reconnect, stop this worker
                        break
            return results
        finally:
            self._pool.return_connection(conn)
