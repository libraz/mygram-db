"""MygramDB vs MySQL fair comparison orchestrator.

Runs A/B comparisons with order-bias elimination by executing benchmarks
in both orders (MygramDB-first and MySQL-first) and averaging results.
"""

from __future__ import annotations

import logging

from lib.benchmark.connection_pool import MygramdbConnectionPool, MysqlConnectionPool
from lib.benchmark.runner import BenchmarkRunner
from lib.benchmark.scenarios import Scenario, build_mygramdb_commands, build_mysql_commands
from lib.mygramdb_client import MygramdbClient
from lib.stats import BenchmarkResult, ComparisonResult

logger = logging.getLogger(__name__)


class ComparisonOrchestrator:
    """Orchestrates fair A/B comparisons between MygramDB and MySQL.

    Eliminates order bias by running each system first in alternating rounds
    and averaging the results.

    Args:
        mygramdb_pool: Connection pool for MygramDB.
        mysql_pool: Connection pool for MySQL.
        mygramdb_client: MygramDB client for admin commands (replication, cache).
    """

    def __init__(
        self,
        mygramdb_pool: MygramdbConnectionPool,
        mysql_pool: MysqlConnectionPool,
        mygramdb_client: MygramdbClient,
    ) -> None:
        self._mg_pool = mygramdb_pool
        self._my_pool = mysql_pool
        self._mg_client = mygramdb_client

    def prepare(self) -> None:
        """Stop replication and clear cache before benchmarking."""
        self._mg_client.replication_stop()
        self._mg_client.cache_clear()

    def restore(self) -> None:
        """Restart replication after benchmarking."""
        self._mg_client.replication_start()

    def run_comparison(
        self,
        scenario: Scenario,
        concurrency: int,
        duration: float,
        warmup_queries: int = 50,
    ) -> ComparisonResult:
        """Run a fair A/B comparison for the given scenario.

        Caller must call prepare() before and restore() after the comparison
        series to stop/start replication.
        """
        mg_queries = build_mygramdb_commands(scenario)
        my_queries = build_mysql_commands(scenario)

        mg_runner = BenchmarkRunner(
            self._mg_pool, warmup_queries=warmup_queries
        )
        my_runner = BenchmarkRunner(
            self._my_pool, warmup_queries=warmup_queries
        )

        # Pre-establish all connections
        mg_runner.ensure_connections()
        my_runner.ensure_connections()

        # Warmup both systems
        mg_runner.warmup(mg_queries)
        my_runner.warmup(my_queries)

        # Round 1: MygramDB first, then MySQL
        mg_result_1 = mg_runner.run(mg_queries, concurrency, duration)
        my_result_1 = my_runner.run(my_queries, concurrency, duration)

        # Round 2: MySQL first, then MygramDB
        my_result_2 = my_runner.run(my_queries, concurrency, duration)
        mg_result_2 = mg_runner.run(mg_queries, concurrency, duration)

        # Average results from both rounds
        mg_avg = _average_results(mg_result_1, mg_result_2)
        my_avg = _average_results(my_result_1, my_result_2)

        return ComparisonResult(
            scenario_name=scenario.name,
            concurrency=concurrency,
            mygramdb=mg_avg,
            mysql=my_avg,
        )


def _average_results(r1: BenchmarkResult, r2: BenchmarkResult) -> BenchmarkResult:
    """Average two benchmark results to eliminate order bias.

    Combines all timing samples, sums query counts, averages total time.
    """
    return BenchmarkResult(
        total_queries=r1.total_queries + r2.total_queries,
        successful=r1.successful + r2.successful,
        failed=r1.failed + r2.failed,
        total_time_ms=(r1.total_time_ms + r2.total_time_ms) / 2.0,
        times=r1.times + r2.times,
        errors=r1.errors + r2.errors,
    )
