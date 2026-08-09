# MygramDB E2E Test Suite

A system that automatically runs end-to-end integration tests, load tests, and edge case tests including MySQL integration.

## Quick Start

```bash
# Run all tests (native build → compose up → pytest → compose down)
make e2e-test

# Smoke tests only
make e2e-test-smoke

# Load tests only
make e2e-test-load

# Full database-version matrix
make e2e-matrix

# Focused MariaDB replication check
bash e2e/test_mariadb_replication.sh

# Cleanup (when containers remain after a test failure)
make e2e-test-cleanup
```

## Prerequisites

- Docker / Docker Compose
- Python 3.10+
- A native MygramDB build at `build/bin/mygramdb` (`make build`)

## Architecture

```
e2e/
├── docker/                     # Test-dedicated Docker environment
│   ├── docker-compose.yml      #   inttest_mysql; MygramDB runs natively
│   └── mysql-init/             #   Table definitions + FULLTEXT indexes
├── lib/                        # Common helpers
│   ├── mysql_client.py         #   Direct MySQL connection client
│   ├── mygramdb_client.py      #   TCP + HTTP client
│   ├── metrics.py              #   Prometheus metrics parser
│   ├── stats.py                #   Statistics calculation (p50/p95/p99/QPS)
│   ├── data_generator.py       #   Synthetic data generation (fixed seed)
│   ├── wait.py                 #   Polling wait utility
│   └── wordlists/              #   English/Japanese/Unicode word lists
├── tests/                      # Test suites grouped by capability
│   ├── unit/                   #   Service-independent helper tests
│   ├── smoke/                  #   Basic connectivity (health, sync, info)
│   ├── replication/            #   INSERT/UPDATE/DELETE propagation
│   ├── search/                 #   Search accuracy, filters, pagination
│   ├── unicode/                #   CJK, NFKC normalization, mixed scripts
│   ├── edge_cases/             #   Empty documents, large documents, special characters
│   ├── ddl/                    #   TRUNCATE, ALTER TABLE
│   ├── concurrency/            #   Search during writes, rapid UPDATEs
│   ├── cache/                  #   Hit/Miss, invalidation
│   ├── memory/                 #   Memory pressure, release
│   ├── statistics/             #   Prometheus counter accuracy
│   ├── load/                   #   Concurrent load, performance regression detection
│   ├── persistence/            #   DUMP SAVE/LOAD round-trip
│   ├── resilience/             #   MySQL restart recovery
│   └── multi_table/            #   Multi-table independence
├── benchmark.py                # CLI benchmark tool
├── conftest.py                 # pytest fixtures
├── pyproject.toml              # Python dependencies, pytest/ruff/mypy config
├── run-all.sh                  # Default MySQL entry point (skips mariadb_only)
├── run-matrix.sh               # MySQL/MariaDB version matrix entry point
├── test_mariadb_replication.sh # Focused MariaDB replication entry point
├── python-env.sh               # Shared interpreter provisioning (rye, or venv fallback)
└── results/                    # Generated at runtime
    ├── reports/                #   JUnit XML
    ├── metrics/                #   Prometheus snapshots
    └── baselines/              #   Performance baselines (machine-specific, not tracked)
```

## Test Categories and Markers

The table intentionally omits hand-maintained test counts. Use
`rye run pytest --collect-only -q` when an exact inventory is needed.

| Marker | Category | Description |
|--------|----------|-------------|
| `smoke` | Basic Connectivity | health endpoints, sync, info, TCP ping |
| `replication` | Replication | INSERT/UPDATE/DELETE propagation, batch replication |
| `search` | Search Accuracy | Word search, filters, pagination, MySQL FULLTEXT comparison |
| `unicode` | Unicode | Japanese/Chinese, NFKC, fullwidth/halfwidth, emoji |
| `edge_cases` | Boundary Conditions | Empty documents, large documents, SQL injection strings |
| `ddl` | DDL Events | TRUNCATE, ALTER, DROP, and RENAME handling |
| `concurrency` | Concurrent Access | Search during writes and rapid UPDATEs |
| `cache` | Cache | Hit/miss, invalidation after writes, CACHE CLEAR |
| `memory` | Memory Management | Soft/hard limits and release after TRUNCATE |
| `statistics` | Metrics | Replication/command/cache counter accuracy |
| `load` | Load Testing | Concurrent load and performance regression detection |
| `persistence` | Persistence | DUMP SAVE/LOAD round-trip |
| `resilience` | Failure Recovery | Reconnection after MySQL restart |
| `multi_table` | Multi-table | Table independence |

### Running by Category

```bash
# Select by pytest marker
bash e2e/run-all.sh -m smoke
bash e2e/run-all.sh -m "replication or search"
bash e2e/run-all.sh -m "not load"

# Specific file only
bash e2e/run-all.sh tests/unicode/test_cjk_search.py
```

## Docker Environment

The E2E runner starts a dedicated MySQL container and a native MygramDB process.

- **Container**: `inttest_mysql`; the MygramDB process is started by pytest
- **Ports**: MySQL is exposed on host port `23306` by default; set `MYSQL_PORT` to change it. MygramDB uses TCP `11016` and HTTP `20080` by default.
- **Memory limits**: the E2E config sets MygramDB to 256MB hard limit / 200MB soft target
- **MySQL**: 8.4, GTID enabled, binlog ROW format, utf8mb4

## Data Generation

No external downloads required. Synthetically generated from checked-in word lists with a fixed seed.

| Dataset | Rows | Purpose |
|---------|------|---------|
| seed_data | 100 | Smoke and basic verification (session fixture) |
| load test | 1,000+ | Load testing (auto-scaling) |
| edge_cases | ~15 | Empty strings, 1MB, emoji, SQL injection, etc. |

## Benchmark CLI

Integrated benchmark tool migrated from `support/benchmark/benchmark.py`.

```bash
# MygramDB benchmark
make e2e-benchmark

# Custom execution
cd e2e && python3 benchmark.py \
  --target mygramdb \
  --table articles \
  --words "hello,world,test" \
  --concurrency 50 \
  --iterations 10 \
  --json-output results/benchmark.json
```

## Python Development

```bash
# Lint
make e2e-lint

# Format
make e2e-format

# Lint fix + format
make e2e-fix
```

## Pass/Fail Criteria

| Category | Pass Condition | Fail Condition |
|----------|---------------|----------------|
| Smoke | All checks pass | Any single failure |
| Replication | Reflected within 10 seconds | Timeout or mismatch |
| Search | Result set matches expected | Mismatch |
| Unicode | All normalization tests pass | Search misses |
| Edge cases | No crashes | Crash or hang |
| DDL | Index state is correct | Incorrect count |
| Concurrency | Final state is consistent | Data corruption |
| Cache | Hit/Miss/invalidation are correct | Stale cache |
| Memory | No OOM crashes | OOM kill |
| Statistics | Counters match actual operations | Discrepancy |
| Load | p99 < baseline x 1.2, error rate < 1% | Performance degradation |
| Persistence | Data integrity after round-trip | Data loss |
| Resilience | Reconnect within 60 seconds | Stuck |
| Multi-table | Tables are independent | Cross-contamination |

Exit code 0 when all categories pass. Non-zero if any category fails.
