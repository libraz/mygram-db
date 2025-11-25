# MygramDB Benchmark Tool

Benchmark tool for comparing MygramDB and MySQL FULLTEXT performance.

## Requirements

```bash
# Using rye (recommended)
cd support/benchmark
rye sync

# Or manually
pip install mysql-connector-python
```

## Usage

### Environment Variables

Set connection information via environment variables:

```bash
# MySQL
export MYSQL_HOST=127.0.0.1
export MYSQL_PORT=3306
export MYSQL_USER=root
export MYSQL_PASSWORD=yourpassword
export MYSQL_DATABASE=test

# MygramDB
export MYGRAMDB_HOST=127.0.0.1
export MYGRAMDB_PORT=11016
```

### Basic Usage

```bash
# Single query benchmark (MygramDB only)
rye run python benchmark.py --target mygramdb --table articles --words "hello"

# Single query benchmark (MySQL only)
rye run python benchmark.py --target mysql --table articles --words "hello"

# Compare both
rye run python benchmark.py --target both --table articles --words "hello"
```

### Concurrent Benchmark

```bash
# 100 concurrent queries
rye run python benchmark.py --target mygramdb --table articles --words "hello" --concurrency 100

# Multiple search terms
rye run python benchmark.py --target both --table articles --words "hello,world,test" --concurrency 20
```

### Query Types

```bash
# SEARCH query (default)
rye run python benchmark.py --target mygramdb --table articles --words "hello" --query-type search

# COUNT query
rye run python benchmark.py --target mygramdb --table articles --words "hello" --query-type count
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--target` | `both` | Target: `mygramdb`, `mysql`, or `both` |
| `--table` | (required) | Table name |
| `--column` | `name` | FULLTEXT column (MySQL only) |
| `--words` | (required) | Comma-separated search words |
| `--query-type` | `search` | Query type: `search` or `count` |
| `--limit` | `100` | LIMIT for search queries |
| `--offset` | `0` | OFFSET for pagination |
| `--concurrency` | `1` | Number of concurrent queries |
| `--iterations` | `5` | Iterations per query |

### Connection Override

Override environment variables with command-line options:

```bash
rye run python benchmark.py \
  --mysql-host 192.168.1.100 \
  --mysql-port 3306 \
  --mysql-user myuser \
  --mysql-password mypass \
  --mysql-database mydb \
  --mygramdb-host 127.0.0.1 \
  --mygramdb-port 11016 \
  --target both \
  --table articles \
  --words "hello"
```

## Example Output

```
=== Benchmark Configuration ===
Table: articles
Words: ['hello', 'world', 'test']
Query Type: search
Limit: 100, Offset: 0
Concurrency: 100
Iterations: 1

=== MygramDB Benchmark ===
Host: 127.0.0.1:11016
Total queries: 300
Successful: 300
Failed: 0
Total time: 5707.1ms
Avg: 57.07ms
Min: 12.34ms
Max: 186.23ms
P50: 45.67ms
P95: 156.78ms
P99: 178.90ms
QPS: 52.6

=== MySQL Benchmark ===
Host: 127.0.0.1:3306
Total queries: 300
Successful: 300
Failed: 0
Total time: 89234.5ms
Avg: 892.34ms
Min: 567.89ms
Max: 2345.67ms
P50: 789.01ms
P95: 1890.12ms
P99: 2123.45ms
QPS: 3.4
```
