"""Load and performance tests.

Uses persistent TCP connections per worker to avoid ephemeral port exhaustion.
The server supports pipelined commands over a single connection: ReactorConnection
frames each ``\\r\\n``-delimited request inside a single drain task, so each
worker keeps one socket open for the entire duration.
"""

import json
import os
import socket
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest

from lib.data_generator import DataGenerator
from lib.stats import BenchmarkResult
from lib.wait import wait_until_gte

pytestmark = pytest.mark.load

SCENARIOS_PATH = os.path.join(os.path.dirname(__file__), "scenarios.json")
BASELINE_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "results", "baselines", "baseline.json"
)


def _load_scenarios() -> dict:
    if os.path.exists(SCENARIOS_PATH):
        with open(SCENARIOS_PATH) as f:
            return json.load(f)
    return {
        "default": {
            "concurrency_levels": [1, 10, 50],
            "duration_seconds": 10,
            "search_ratio": 0.8,
            "count_ratio": 0.2,
        }
    }


def _persistent_command(sock: socket.socket, cmd: str) -> tuple[bool, float, str]:
    """Send a command on a persistent connection and return (success, elapsed_ms, response)."""
    start = time.perf_counter()
    sock.sendall((cmd + "\r\n").encode("utf-8"))

    data = b""
    while True:
        chunk = sock.recv(65536)
        if not chunk:
            return False, 0.0, "connection closed"
        data += chunk
        if data.endswith(b"\r\n"):
            break

    elapsed = (time.perf_counter() - start) * 1000
    response = data.decode("utf-8", errors="ignore").strip()
    success = response.startswith(("OK", "(integer)"))
    return success, elapsed, response


class TestLoad:
    """Load and performance tests."""

    @pytest.fixture(autouse=True)
    def setup_load_data(self, mysql, mygramdb, seed_data):
        """Ensure sufficient data for load testing."""
        gen = DataGenerator(seed=1234)
        info = mygramdb.info()
        current = info.get("total_documents", info.get("doc_count", info.get("documents", 0)))
        if current < 1000:
            rows = gen.generate_articles(count=1000, mixed=True)
            mysql.insert_rows("articles", rows)

            def _get_doc_count() -> int:
                i = mygramdb.info()
                return i.get("total_documents", i.get("doc_count", i.get("documents", 0)))

            wait_until_gte(
                _get_doc_count,
                minimum=1000,
                timeout=60,
                interval=2,
                description="load test data",
            )

    def test_concurrent_search_performance(self, mygramdb):
        """Concurrent search should maintain acceptable latency."""
        scenarios = _load_scenarios()
        scenario = scenarios.get("default", {})
        duration = scenario.get("duration_seconds", 10)

        search_words = ["test", "search", "data", "content", "article"]
        result = BenchmarkResult()

        error_samples: list[str] = []

        def worker(word: str, stop_time: float) -> list[tuple[bool, float]]:
            """Worker that reuses a single persistent TCP connection."""
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30.0)
            sock.connect((mygramdb.host, mygramdb.tcp_port))

            results = []
            try:
                while time.monotonic() < stop_time:
                    success, elapsed, resp = _persistent_command(
                        sock, f"SEARCH testdb.articles {word} LIMIT 10"
                    )
                    results.append((success, elapsed))
                    if not success:
                        if len(error_samples) < 20:
                            error_samples.append(resp[:200])
                        if resp == "connection closed":
                            break
            finally:
                sock.close()
            return results

        for concurrency in scenario.get("concurrency_levels", [1, 10]):
            stop_time = time.monotonic() + duration
            start_time = time.monotonic()

            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = [
                    executor.submit(worker, search_words[i % len(search_words)], stop_time)
                    for i in range(concurrency)
                ]

                for future in as_completed(futures):
                    for success, elapsed in future.result():
                        result.total_queries += 1
                        if success:
                            result.successful += 1
                            result.times.append(elapsed)
                        else:
                            result.failed += 1

            result.total_time_ms = (time.monotonic() - start_time) * 1000

        # Verify performance
        if error_samples:
            unique_errors = list(set(error_samples))
            print(f"\nError samples ({len(unique_errors)} unique):")
            for err in unique_errors[:10]:
                print(f"  - {err}")

        assert result.error_rate < 0.01, (
            f"Error rate {result.error_rate:.2%} exceeds 1% "
            f"(total={result.total_queries}, failed={result.failed})"
        )

        summary = result.summary()
        print(f"\nLoad test results: {json.dumps(summary, indent=2)}")

        # Check against baseline if exists
        if os.path.exists(BASELINE_PATH):
            with open(BASELINE_PATH) as f:
                baseline = json.load(f)
            if "p99_ms" in baseline and "p99_ms" in summary:
                degradation = summary["p99_ms"] / baseline["p99_ms"]
                assert degradation < 1.2, (
                    f"P99 degradation: {degradation:.1%} "
                    f"(current: {summary['p99_ms']:.1f}ms, "
                    f"baseline: {baseline['p99_ms']:.1f}ms)"
                )
