# MygramDB E2E Test Suite

A system that automatically runs end-to-end integration tests, load tests, and edge case tests including MySQL integration.

## Quick Start

```bash
# Run all tests (Docker image build → compose up → pytest → compose down)
make e2e-test

# Smoke tests only
make e2e-test-smoke

# Load tests only
make e2e-test-load

# Cleanup (when containers remain after a test failure)
make e2e-test-cleanup
```

## Prerequisites

- Docker / Docker Compose
- Python 3.10+
- `mygramdb:latest` Docker image (must be pre-built via `make docker-build`)

## Architecture

```
e2e/
├── docker/                     # Test-dedicated Docker environment
│   ├── docker-compose.yml      #   inttest_mysql + inttest_mygramdb (no host port exposure)
│   └── mysql-init/             #   Table definitions + FULLTEXT indexes
├── lib/                        # Common helpers
│   ├── mysql_client.py         #   Direct MySQL connection client
│   ├── mygramdb_client.py      #   TCP + HTTP client
│   ├── metrics.py              #   Prometheus metrics parser
│   ├── stats.py                #   Statistics calculation (p50/p95/p99/QPS)
│   ├── data_generator.py       #   Synthetic data generation (fixed seed)
│   ├── wait.py                 #   Polling wait utility
│   └── wordlists/              #   English/Japanese/Unicode word lists
├── tests/                      # Test suites (14 categories, 34 files, 70 tests)
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
├── run-all.sh                  # Entry point
└── results/                    # Generated at runtime
    ├── reports/                #   JUnit XML
    ├── metrics/                #   Prometheus snapshots
    └── baselines/              #   Performance baselines (tracked in git)
```

## Test Categories and Markers

| Marker | Category | Test Count | Description |
|--------|----------|------------|-------------|
| `smoke` | Basic Connectivity | 7 | health endpoints, sync, info, TCP ping |
| `replication` | Replication | 8 | INSERT/UPDATE/DELETE propagation, batch 1000 rows |
| `search` | Search Accuracy | 10 | Word search, filters, pagination, MySQL FULLTEXT comparison |
| `unicode` | Unicode | 9 | Japanese/Chinese, NFKC, fullwidth/halfwidth, emoji |
| `edge_cases` | Boundary Conditions | 8 | Empty documents, 1MB documents, SQL injection strings |
| `ddl` | DDL Events | 4 | TRUNCATE, ALTER TABLE |
| `concurrency` | Concurrent Access | 4 | Search during writes (10 parallel), rapid UPDATEs |
| `cache` | Cache | 4 | Miss→Hit, invalidation after INSERT, CACHE CLEAR |
| `memory` | Memory Management | 3 | Soft/hard limits, release after TRUNCATE |
| `statistics` | Metrics | 8 | Replication/command/cache counter accuracy |
| `load` | Load Testing | 1 | Concurrent load, p99 regression detection |
| `persistence` | Persistence | 2 | DUMP SAVE→LOAD round-trip |
| `resilience` | Failure Recovery | 2 | Reconnection after MySQL restart |
| `multi_table` | Multi-table | 2 | Index independence |

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

Uses a test-dedicated Docker environment, fully isolated from the existing development environment.

- **Container names**: `inttest_mysql`, `inttest_mygramdb` (no conflict with existing `mygramdb_*`)
- **Network**: `inttest_network` (Docker internal only, no host port exposure)
- **Memory limits**: MygramDB 200MB hard limit / 150MB soft target (for memory pressure testing)
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
