# MygramDB Configuration Reference

**Version**: 1.0
**Last Updated**: 2025-11-18

---

## Table of Contents

1. [Overview](#overview)
2. [Configuration File Format](#configuration-file-format)
3. [Configuration Sections](#configuration-sections)
   - [MySQL Connection](#mysql-connection)
   - [Table Configuration](#table-configuration)
   - [Index Build](#index-build)
   - [Replication](#replication)
   - [Memory Management](#memory-management)
   - [Dump Persistence](#dump-persistence)
   - [API Server](#api-server)
   - [Network Security](#network-security)
   - [Logging](#logging)
   - [Query Cache](#query-cache)
4. [Runtime Variables](#runtime-variables)
5. [Production Recommendations](#production-recommendations)
6. [Troubleshooting](#troubleshooting)

---

## Overview

MygramDB uses YAML or JSON configuration files to define server behavior, MySQL connection settings, table indexing parameters, and operational policies.

### Key Features

- **Multiple formats**: YAML (`.yaml`, `.yml`) or JSON (`.json`)
- **Schema validation**: Automatic validation against built-in JSON Schema
- **Runtime variables**: MySQL-style SET/SHOW VARIABLES for live updates without restart
- **Environment-specific**: Easy customization for development, staging, production

### Location

- **Default**: `config.yaml` in current directory
- **Custom**: Specify with `--config=/path/to/config.yaml` command-line option

---

## Configuration File Format

### YAML Format (Recommended)

```yaml
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "repl_user"
  password: "your_password"
  database: "mydb"

tables:
  - name: "articles"
    text_source:
      column: "content"
    ngram_size: 2
```

### JSON Format

```json
{
  "mysql": {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "repl_user",
    "password": "your_password",
    "database": "mydb"
  },
  "tables": [
    {
      "name": "articles",
      "text_source": {
        "column": "content"
      },
      "ngram_size": 2
    }
  ]
}
```

### Schema Validation

MygramDB automatically validates configuration files against a built-in JSON Schema. Invalid settings (typos, wrong types, unknown keys) are detected and reported at startup.

---

## Configuration Sections

### MySQL Connection

```yaml
mysql:
  host: "127.0.0.1"                 # MySQL server hostname or IP
  port: 3306                        # MySQL server port
  user: "repl_user"                 # MySQL username for replication
  password: "your_password_here"    # MySQL user password
  database: "mydb"                  # Database name
  use_gtid: true                    # Enable GTID-based replication (required)
  binlog_format: "ROW"              # Binary log format (required: ROW)
  binlog_row_image: "FULL"          # Row image format (required: FULL)
  connect_timeout_ms: 3000          # Connection timeout in milliseconds
  session_timeout_sec: 3600         # Session timeout in seconds (default: 3600 = 1 hour)
                                    # Prevents disconnection during long operations like snapshot building
  datetime_timezone: "+00:00"       # Timezone for DATETIME/DATE columns (default: "+00:00" UTC)
                                    # Format: [+-]HH:MM (e.g., "+09:00" for JST, "-05:00" for EST)

  # SSL/TLS settings (optional but recommended for production)
  ssl_enable: false                 # Enable SSL/TLS
  ssl_ca: "/path/to/ca-cert.pem"    # CA certificate
  ssl_cert: "/path/to/client-cert.pem"  # Client certificate
  ssl_key: "/path/to/client-key.pem"    # Client private key
  ssl_verify_server_cert: true      # Verify server certificate
```

#### Parameters

| Parameter | Type | Default | Description | Hot Reload |
|-----------|------|---------|-------------|------------|
| `host` | string | `127.0.0.1` | MySQL server hostname or IP address | ✅ Yes |
| `port` | integer | `3306` | MySQL server port | ✅ Yes |
| `user` | string | *required* | MySQL username (must have REPLICATION SLAVE, REPLICATION CLIENT privileges) | ✅ Yes |
| `password` | string | *required* | MySQL user password | ✅ Yes |
| `database` | string | *required* | MySQL database name | ❌ No (requires restart) |
| `use_gtid` | boolean | `true` | Enable GTID-based replication (required for replication) | ❌ No |
| `binlog_format` | string | `ROW` | Binary log format (must be ROW) | ❌ No |
| `binlog_row_image` | string | `FULL` | Row image format (must be FULL) | ❌ No |
| `connect_timeout_ms` | integer | `3000` | Connection timeout in milliseconds | ✅ Yes |
| `session_timeout_sec` | integer | `3600` | Session timeout in seconds - prevents disconnection during long operations like snapshot building | ✅ Yes |
| `datetime_timezone` | string | `+00:00` | Timezone for DATETIME/DATE columns. Format: `[+-]HH:MM`. TIMESTAMP columns are always UTC. | ❌ No (requires restart) |
| `ssl_enable` | boolean | `false` | Enable SSL/TLS connection | ✅ Yes |
| `ssl_ca` | string | `` | Path to CA certificate file | ✅ Yes |
| `ssl_cert` | string | `` | Path to client certificate file | ✅ Yes |
| `ssl_key` | string | `` | Path to client private key file | ✅ Yes |
| `ssl_verify_server_cert` | boolean | `true` | Verify server certificate | ✅ Yes |

#### MySQL User Privileges

The MySQL user must have the following privileges:

```sql
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_user'@'%';
FLUSH PRIVILEGES;
```

#### MySQL Server Requirements

- **GTID mode**: Must be enabled (`gtid_mode=ON`)
- **Binary log format**: Must be ROW (`binlog_format=ROW`)
- **Row image**: Must be FULL (`binlog_row_image=FULL`)

Verify with:

```sql
SHOW VARIABLES LIKE 'gtid_mode';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'binlog_row_image';
```

---

### Table Configuration

```yaml
tables:
  - name: "articles"                # Table name in MySQL database
    primary_key: "id"               # Primary key column name

    # Text Source Configuration
    text_source:
      column: "content"             # Single column to index
      # OR
      # concat: ["title", "body"]   # Multiple columns to concatenate
      # delimiter: " "              # Delimiter for concatenation

    # Required Filters (data existence conditions)
    required_filters:
      - name: "enabled"             # Column name
        type: "int"                 # Column type
        op: "="                     # Operator
        value: 1                    # Comparison value
        bitmap_index: false         # Enable bitmap index

      - name: "deleted_at"
        type: "datetime"
        op: "IS NULL"               # No value for IS NULL/IS NOT NULL
        bitmap_index: false

    # Optional Filters (search-time filtering)
    filters:
      - name: "status"              # Column name
        type: "int"                 # Column type
        dict_compress: false        # Dictionary compression
        bitmap_index: false         # Bitmap indexing

      - name: "category"
        type: "string"
        dict_compress: false
        bitmap_index: false

      - name: "created_at"
        type: "datetime"
        # bucket: "minute"          # Bucket datetime values

    # N-gram Configuration
    ngram_size: 2                   # N-gram size for ASCII (1=unigram, 2=bigram)
    kanji_ngram_size: 1             # N-gram size for CJK (0 = use ngram_size)

    # Posting List Configuration
    posting:
      block_size: 128               # Block size for delta encoding
      freq_bits: 0                  # Term frequency bits: 0, 4, or 8
      use_roaring: "auto"           # Roaring bitmap usage: auto, always, never
```

#### Table Parameters

| Parameter | Type | Default | Description | Hot Reload |
|-----------|------|---------|-------------|------------|
| `name` | string | *required* | MySQL table name | ❌ No |
| `primary_key` | string | `id` | Primary key column name (must be single-column PRIMARY KEY or UNIQUE KEY) | ❌ No |
| `text_source.column` | string | *required if concat not set* | Single column to index for full-text search | ❌ No |
| `text_source.concat` | array | *required if column not set* | Multiple columns to concatenate for indexing | ❌ No |
| `text_source.delimiter` | string | ` ` (space) | Delimiter for concatenating multiple columns | ❌ No |
| `ngram_size` | integer | `2` | N-gram size for ASCII/alphanumeric characters (1-4 recommended) | ⚠️ Partial (cache cleared) |
| `kanji_ngram_size` | integer | `0` | N-gram size for CJK characters (0 = use ngram_size, 1-2 recommended) | ⚠️ Partial (cache cleared) |

#### Required Filters vs Optional Filters

**Required Filters** (Data Existence Conditions):
- Define which rows are indexed
- Data NOT matching these conditions is **excluded from index**
- Changes trigger **add/remove** operations during binlog replication
- Use cases:
  - Index only published articles: `status = 'published'`
  - Exclude deleted records: `deleted_at IS NULL`
  - Index only enabled records: `enabled = 1`

**Optional Filters** (Search-Time Filtering):
- Used for filtering **during searches**
- Do NOT affect which data is indexed
- All rows (matching required_filters) are indexed
- Use cases:
  - Filter by category, status, date ranges during search
  - Sort by custom columns

#### Filter Types

| Type | MySQL Types | Description |
|------|-------------|-------------|
| `tinyint` | TINYINT | Signed 8-bit integer (-128 to 127) |
| `tinyint_unsigned` | TINYINT UNSIGNED | Unsigned 8-bit integer (0 to 255) |
| `smallint` | SMALLINT | Signed 16-bit integer |
| `smallint_unsigned` | SMALLINT UNSIGNED | Unsigned 16-bit integer |
| `int` | INT | Signed 32-bit integer (legacy: also accepts "int") |
| `int_unsigned` | INT UNSIGNED | Unsigned 32-bit integer |
| `bigint` | BIGINT | Signed 64-bit integer |
| `float` | FLOAT | Single-precision floating-point |
| `double` | DOUBLE | Double-precision floating-point |
| `string`, `varchar`, `text` | VARCHAR, TEXT, CHAR | String values |
| `datetime` | DATETIME | Date/time values (timezone-aware, configurable via `datetime_timezone`) |
| `date` | DATE | Date values (timezone-aware, configurable via `datetime_timezone`) |
| `timestamp` | TIMESTAMP | Timestamp values (always UTC, not affected by `datetime_timezone`) |
| `time` | TIME | Time values (stored as seconds from 00:00:00, supports negative values) |

#### Required Filter Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=` | Equal | `status = 'published'` |
| `!=` | Not equal | `type != 'draft'` |
| `<` | Less than | `priority < 10` |
| `>` | Greater than | `score > 50` |
| `<=` | Less than or equal | `age <= 18` |
| `>=` | Greater than or equal | `rating >= 4.0` |
| `IS NULL` | Is NULL | `deleted_at IS NULL` |
| `IS NOT NULL` | Is not NULL | `published_at IS NOT NULL` |

#### N-gram Size Recommendations

| Language | Content Type | `ngram_size` | `kanji_ngram_size` |
|----------|--------------|--------------|---------------------|
| English | Articles, documents | `2` | `0` or `2` |
| Japanese | Mixed (Kanji/Kana/ASCII) | `2` | `1` |
| Chinese | Mixed (Hanzi/ASCII) | `2` | `1` |
| Code | Source code | `3` | `0` or `3` |

**Trade-offs**:
- **Smaller n-grams (1)**: Higher recall, more false positives, larger index
- **Larger n-grams (3-4)**: Higher precision, fewer results, smaller index

#### Posting List Configuration

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `block_size` | integer | `128` | Block size for delta encoding compression (64-256 recommended) |
| `freq_bits` | integer | `0` | Term frequency bits: `0` (boolean), `4`, or `8` (ranking support) |
| `use_roaring` | string | `auto` | Roaring bitmap usage: `auto` (threshold-based), `always`, `never` |

**Roaring Bitmap Threshold**: Automatically switches to Roaring bitmaps when posting list density exceeds 18% (configurable via `memory.roaring_threshold`).

---

### Index Build

```yaml
build:
  mode: "select_snapshot"           # Build mode (currently only select_snapshot)
  batch_size: 5000                  # Rows per batch during snapshot
  parallelism: 2                    # Number of parallel build threads
  throttle_ms: 0                    # Throttle delay between batches (ms)
```

#### Parameters

| Parameter | Type | Default | Description | Hot Reload |
|-----------|------|---------|-------------|------------|
| `mode` | string | `select_snapshot` | Build mode (currently only `select_snapshot`) | ❌ No |
| `batch_size` | integer | `5000` | Number of rows to process per batch during snapshot | ❌ No |
| `parallelism` | integer | `2` | Number of parallel build threads | ❌ No |
| `throttle_ms` | integer | `0` | Throttle delay between batches in milliseconds (0 = no throttle) | ❌ No |

**Performance Tuning**:
- **Larger `batch_size` (10000+)**: Faster initial snapshot, higher memory usage
- **Smaller `batch_size` (1000-2000)**: Slower snapshot, lower memory usage
- **Higher `parallelism`**: Faster on multi-core systems, higher memory usage
- **Non-zero `throttle_ms`**: Reduces MySQL load during snapshot (useful for production databases)

---

### Replication

```yaml
replication:
  enable: true                      # Enable binlog replication
  auto_initial_snapshot: false      # Automatically build snapshot on startup
  server_id: 12345                  # MySQL server ID (must be unique)
  start_from: "snapshot"            # Replication start position
  queue_size: 10000                 # Binlog event queue size
  reconnect_backoff_min_ms: 500     # Minimum reconnect backoff delay
  reconnect_backoff_max_ms: 10000   # Maximum reconnect backoff delay
```

#### Parameters

| Parameter | Type | Default | Description | Hot Reload |
|-----------|------|---------|-------------|------------|
| `enable` | boolean | `true` | Enable binlog replication | ❌ No |
| `auto_initial_snapshot` | boolean | `false` | Automatically build snapshot on startup | ❌ No |
| `server_id` | integer | *required* | MySQL server ID (must be non-zero and unique in replication topology) | ❌ No |
| `start_from` | string | `snapshot` | Replication start position: `snapshot`, `latest`, or `gtid=<UUID:txn>` | ❌ No |
| `queue_size` | integer | `10000` | Binlog event queue size (buffer between reader and processor) | ❌ No |
| `reconnect_backoff_min_ms` | integer | `500` | Minimum reconnect backoff delay in milliseconds | ❌ No |
| `reconnect_backoff_max_ms` | integer | `10000` | Maximum reconnect backoff delay in milliseconds | ❌ No |

#### Replication Start Position

| Value | Description | Use Case |
|-------|-------------|----------|
| `snapshot` | Start from snapshot GTID (recommended) | Ensures consistency with snapshot data |
| `latest` | Start from current GTID (only new changes) | Skip historical data, only track new changes |
| `gtid=<UUID:txn>` | Start from specific GTID | Resume from known position |

**Example**:
```yaml
start_from: "gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:1"
```

#### Server ID Requirements

- **Must be non-zero**
- **Must be unique** across all MySQL replicas in the topology
- **Recommended**: Generate a random number or use a unique value per environment

**Generate a random server ID**:
```bash
echo $((RANDOM * RANDOM))
```

---

### Memory Management

```yaml
memory:
  hard_limit_mb: 8192               # Hard memory limit in MB
  soft_target_mb: 4096              # Soft memory target in MB
  arena_chunk_mb: 64                # Arena chunk size in MB
  roaring_threshold: 0.18           # Roaring bitmap threshold (density)
  minute_epoch: true                # Use minute-precision epoch

  # Text Normalization
  normalize:
    nfkc: true                      # NFKC normalization
    width: "narrow"                 # Width conversion: keep, narrow, wide
    lower: false                    # Lowercase conversion
```

#### Parameters

| Parameter | Type | Default | Description | Hot Reload |
|-----------|------|---------|-------------|------------|
| `hard_limit_mb` | integer | `8192` | Hard memory limit in MB (OOM protection) | ❌ No |
| `soft_target_mb` | integer | `4096` | Soft memory target in MB (eviction trigger) | ❌ No |
| `arena_chunk_mb` | integer | `64` | Arena chunk size in MB (memory allocator) | ❌ No |
| `roaring_threshold` | float | `0.18` | Posting list density threshold for Roaring bitmap (0.0-1.0) | ❌ No |
| `minute_epoch` | boolean | `true` | Use minute-precision epoch for timestamps | ❌ No |
| `normalize.nfkc` | boolean | `true` | Apply NFKC normalization (Unicode compatibility) | ❌ No |
| `normalize.width` | string | `narrow` | Width conversion: `keep`, `narrow`, `wide` | ❌ No |
| `normalize.lower` | boolean | `false` | Convert text to lowercase | ❌ No |

#### Text Normalization

**NFKC Normalization** (`normalize.nfkc`):
- Recommended for **Japanese** and **CJK** content
- Normalizes Unicode compatibility characters
- Example: `㍻` (U+337B) → `平成` (U+5E73 U+6210)

**Width Conversion** (`normalize.width`):
- `keep`: No conversion
- `narrow`: Full-width → Half-width (e.g., `Ａ` → `A`)
- `wide`: Half-width → Full-width (e.g., `A` → `Ａ`)

**Lowercase Conversion** (`normalize.lower`):
- `true`: Convert to lowercase (case-insensitive search)
- `false`: Preserve case (case-sensitive search)

---

### Dump Persistence

```yaml
dump:
  dir: "/var/lib/mygramdb/dumps"    # Dump directory path
  default_filename: "mygramdb.dmp"  # Default filename for manual dumps
  interval_sec: 0                   # Auto-save interval (0 = disabled)
  retain: 3                         # Number of auto-saved dumps to retain
```

#### Parameters

| Parameter | Type | Default | Description | Hot Reload |
|-----------|------|---------|-------------|------------|
| `dir` | string | `/var/lib/mygramdb/dumps` | Dump directory path (created automatically) | ❌ No |
| `default_filename` | string | `mygramdb.dmp` | Default filename for manual `DUMP SAVE` | ❌ No |
| `interval_sec` | integer | `0` | Auto-save interval in seconds (0 = disabled) | ✅ Yes |
| `retain` | integer | `3` | Number of auto-saved dumps to retain (cleanup) | ✅ Yes |

#### Auto-save Behavior

**When `interval_sec > 0`**:
- Automatically saves snapshots every N seconds
- Filenames: `auto_YYYYMMDD_HHMMSS.dmp`
- Older auto-saved files are cleaned up (keeps latest `retain` files)
- Manual dumps (via `DUMP SAVE`) are **not affected** by cleanup

**Recommended Values**:
- **Development**: `0` (disabled)
- **Production**: `7200` (2 hours)

**Similar to**:
- Redis RDB persistence

---

### API Server

```yaml
api:
  tcp:
    bind: "127.0.0.1"               # TCP bind address
    port: 11016                     # TCP port
    max_connections: 10000          # Maximum concurrent connections

  http:
    enable: false                   # Enable HTTP/JSON API
    bind: "127.0.0.1"               # HTTP bind address
    port: 8080                      # HTTP port
    enable_cors: false              # Enable CORS headers
    cors_allow_origin: ""           # Origin allowed when CORS enabled

  default_limit: 100                # Default LIMIT when not specified
  max_query_length: 128             # Max query expression length

  # Rate Limiting (optional)
  rate_limiting:
    enable: false                   # Enable rate limiting
    capacity: 100                   # Maximum tokens per client (burst)
    refill_rate: 10                 # Tokens added per second per client
    max_clients: 10000              # Maximum number of tracked clients
```

#### TCP Server Parameters

| Parameter | Type | Default | Description | Hot Reload |
|-----------|------|---------|-------------|------------|
| `tcp.bind` | string | `127.0.0.1` | TCP bind address (use `0.0.0.0` for all interfaces) | ❌ No |
| `tcp.port` | integer | `11016` | TCP port | ❌ No |
| `tcp.max_connections` | integer | `10000` | Maximum concurrent connections (prevents file descriptor exhaustion) | ❌ No |

**Security Recommendation**:
- **Development**: `127.0.0.1` (localhost only)
- **Production**: Use `network.allow_cidrs` to restrict access

#### HTTP Server Parameters

| Parameter | Type | Default | Description | Hot Reload |
|-----------|------|---------|-------------|------------|
| `http.enable` | boolean | `false` | Enable HTTP/JSON API (disabled by default) | ❌ No |
| `http.bind` | string | `127.0.0.1` | HTTP bind address | ❌ No |
| `http.port` | integer | `8080` | HTTP port | ❌ No |
| `http.enable_cors` | boolean | `false` | Enable CORS headers for browser clients | ❌ No |
| `http.cors_allow_origin` | string | `` | Origin allowed when CORS enabled (e.g., `https://app.example.com`) | ❌ No |

**HTTP Endpoints**:
- `POST /{table}/search`: Search query
- `GET /{table}/:id`: Get document by primary key
- `GET /info`: Server information
- `GET /health`: Health check (Kubernetes-ready)

#### Query Parameters

| Parameter | Type | Default | Description | Hot Reload |
|-----------|------|---------|-------------|------------|
| `default_limit` | integer | `100` | Default LIMIT when not specified (5-1000) | ⚠️ Partial |
| `max_query_length` | integer | `128` | Maximum query expression length (0 = unlimited) | ⚠️ Partial |

#### Rate Limiting (Token Bucket Algorithm)

| Parameter | Type | Default | Description | Hot Reload |
|-----------|------|---------|-------------|------------|
| `rate_limiting.enable` | boolean | `false` | Enable rate limiting per client IP | ⚠️ Partial |
| `rate_limiting.capacity` | integer | `100` | Maximum tokens per client (burst size) | ⚠️ Partial |
| `rate_limiting.refill_rate` | integer | `10` | Tokens added per second per client (sustained rate) | ⚠️ Partial |
| `rate_limiting.max_clients` | integer | `10000` | Maximum number of tracked clients (memory management) | ⚠️ Partial |

**How it works**:
- Each client IP has a token bucket with `capacity` tokens
- Each request consumes 1 token
- Tokens refill at `refill_rate` per second
- When bucket is empty, requests are rate-limited (HTTP 429)

**Example**:
```yaml
rate_limiting:
  enable: true
  capacity: 100       # Allow burst of 100 requests
  refill_rate: 10     # Sustained rate: 10 req/s per IP
```

---

### Network Security

```yaml
network:
  allow_cidrs:                      # Allowed CIDR list (fail-closed)
    - "127.0.0.1/32"                # Localhost only (most secure)
    # - "192.168.1.0/24"            # Local network
    # - "10.0.0.0/8"                # Private network
    # - "0.0.0.0/0"                 # WARNING: Allow all (NOT RECOMMENDED)
```

#### Parameters

| Parameter | Type | Default | Description | Hot Reload |
|-----------|------|---------|-------------|------------|
| `allow_cidrs` | array | *required* | Allowed CIDR list (empty = **deny all**) | ❌ No |

**Security Policy**:
- **Fail-closed**: Empty `allow_cidrs` denies all connections
- **Explicit allowlist**: You must explicitly configure allowed IP ranges
- **Applied to**: Both TCP and HTTP APIs

**Docker Environment Variable**:
When using Docker, you can configure this via the `NETWORK_ALLOW_CIDRS` environment variable (comma-separated):
```bash
# Single CIDR
NETWORK_ALLOW_CIDRS=192.168.1.0/24

# Multiple CIDRs
NETWORK_ALLOW_CIDRS=10.0.0.0/8,172.16.0.0/12,192.168.0.0/16

# Allow all (development only)
NETWORK_ALLOW_CIDRS=0.0.0.0/0
```

**Examples**:
```yaml
# Localhost only (most secure)
allow_cidrs:
  - "127.0.0.1/32"
  - "::1/128"  # IPv6 localhost

# Local network
allow_cidrs:
  - "192.168.1.0/24"

# Private networks
allow_cidrs:
  - "10.0.0.0/8"
  - "172.16.0.0/12"
  - "192.168.0.0/16"

# Allow all (NOT RECOMMENDED for production)
allow_cidrs:
  - "0.0.0.0/0"
  - "::/0"
```

---

### Logging

```yaml
logging:
  level: "info"                     # Log level
  format: "json"                    # Log output format: "json" or "text"
  file: ""                          # Log file path (empty = stdout)
```

#### Parameters

| Parameter | Type | Default | Description | Hot Reload |
|-----------|------|---------|-------------|------------|
| `level` | string | `info` | Log level: `debug`, `info`, `warn`, `error` | ✅ Yes |
| `format` | string | `json` | Log output format: `json` (structured) or `text` (human-readable) | ❌ No |
| `file` | string | `` | Log file path (empty = stdout, required for daemon mode) | ❌ No |

**Log Levels**:
- **`debug`**: Verbose logging (development only)
- **`info`**: Standard operational messages (recommended for production)
- **`warn`**: Warnings (anomalies that don't require immediate action)
- **`error`**: Errors (require attention)

**Log Formats**:
- **`json`**: Structured JSON format (recommended for production, easier to parse)
- **`text`**: Human-readable key=value format (recommended for development)

**Log Output**:
- **Empty `file`**: Log to stdout (recommended for Docker/systemd)
- **Path**: Log to file (e.g., `/var/log/mygramdb/mygramdb.log`)
- **Daemon mode**: `file` is **required** when using `-d/--daemon`

---

### Query Cache

```yaml
cache:
  enabled: true                     # Enable query result cache
  max_memory_mb: 32                 # Maximum cache memory in MB
  min_query_cost_ms: 10.0           # Minimum query cost to cache (ms)
  ttl_seconds: 3600                 # Cache entry TTL (0 = no TTL)
  invalidation_strategy: "ngram"    # Invalidation strategy

  # Advanced tuning
  compression_enabled: true         # Enable LZ4 compression
  eviction_batch_size: 10           # Eviction batch size

  # Invalidation queue
  invalidation:
    batch_size: 1000                # Process after N unique pairs
    max_delay_ms: 100               # Max delay before processing (ms)
```

#### Parameters

| Parameter | Type | Default | Description | Hot Reload |
|-----------|------|---------|-------------|------------|
| `enabled` | boolean | `true` | Enable query result cache | ⚠️ Partial (cache cleared) |
| `max_memory_mb` | integer | `32` | Maximum cache memory in MB | ⚠️ Partial |
| `min_query_cost_ms` | float | `10.0` | Minimum query cost to cache in milliseconds | ⚠️ Partial |
| `ttl_seconds` | integer | `3600` | Cache entry TTL in seconds (0 = no TTL) | ⚠️ Partial |
| `invalidation_strategy` | string | `ngram` | Invalidation strategy: `ngram`, `table` | ⚠️ Partial |
| `compression_enabled` | boolean | `true` | Enable LZ4 compression for cached results | ⚠️ Partial |
| `eviction_batch_size` | integer | `10` | Number of entries to evict at once | ⚠️ Partial |
| `invalidation.batch_size` | integer | `1000` | Process invalidation after N unique (table, ngram) pairs | ⚠️ Partial |
| `invalidation.max_delay_ms` | integer | `100` | Maximum delay before processing invalidation | ⚠️ Partial |

#### Invalidation Strategies

**`ngram` (Recommended)**:
- **Precision**: Only invalidates queries using changed n-grams
- **Efficiency**: Minimal cache invalidation
- **Use case**: Production environments

**`table`**:
- **Aggressive**: Invalidates entire table cache on any change
- **Simple**: No n-gram tracking overhead
- **Use case**: Very high write rate, small cache

#### Cache Tuning

**`min_query_cost_ms`**:
- Only cache queries that take longer than this threshold
- **Higher value**: Fewer cached queries, lower memory usage
- **Lower value**: More cached queries, higher memory usage
- **Recommended**: 10-50ms

**`ttl_seconds`**:
- Time-to-live for cache entries
- **0**: No TTL (cache until invalidated or evicted)
- **3600**: 1 hour (recommended for frequently changing data)

**`compression_enabled`**:
- LZ4 compression for cached results
- **true**: Lower memory usage, slight CPU overhead
- **false**: Higher memory usage, no CPU overhead

---

## Runtime Variables

MygramDB supports **live configuration changes** without restart using MySQL-compatible SET and SHOW VARIABLES commands.

### Basic Usage

```sql
-- Show all runtime variables
SHOW VARIABLES;

-- Show specific variable pattern
SHOW VARIABLES LIKE 'mysql%';
SHOW VARIABLES LIKE 'cache%';

-- Set a single variable
SET logging.level = 'debug';
SET mysql.host = '192.168.1.100';

-- Set multiple variables
SET api.default_limit = 200, api.max_query_length = 256;
```

### Variable Categories

#### Mutable Variables (Runtime Changeable)

These variables can be changed at runtime using `SET` commands:

| Variable | Type | Description | Example |
|----------|------|-------------|---------|
| **Logging** ||||
| `logging.level` | string | Log level: debug, info, warn, error | `SET logging.level = 'debug'` |
| `logging.format` | string | Log format: json, text | `SET logging.format = 'json'` |
| **MySQL Failover** ||||
| `mysql.host` | string | MySQL server hostname or IP | `SET mysql.host = '192.168.1.100'` |
| `mysql.port` | integer | MySQL server port | `SET mysql.port = 3307` |
| **Cache** ||||
| `cache.enabled` | boolean | Enable/disable query cache | `SET cache.enabled = true` |
| `cache.min_query_cost_ms` | float | Minimum query cost to cache (ms) | `SET cache.min_query_cost_ms = 20.0` |
| `cache.ttl_seconds` | integer | Cache entry TTL (0 = no TTL) | `SET cache.ttl_seconds = 7200` |
| **API** ||||
| `api.default_limit` | integer | Default LIMIT when not specified | `SET api.default_limit = 200` |
| `api.max_query_length` | integer | Maximum query expression length | `SET api.max_query_length = 256` |
| **Rate Limiting** ||||
| `rate_limiting.capacity` | integer | Maximum tokens per client (burst) | `SET rate_limiting.capacity = 200` |
| `rate_limiting.refill_rate` | integer | Tokens per second per client | `SET rate_limiting.refill_rate = 20` |

#### Immutable Variables (Restart Required)

These variables cannot be changed at runtime and require server restart:

| Category | Variables | Reason |
|----------|-----------|--------|
| **MySQL** | `database`, `user`, `password`, `use_gtid`, `binlog_format`, `binlog_row_image`, `datetime_timezone` | Core replication configuration |
| **Tables** | `tables[*].*` | Table schema and index structure |
| **Build** | `build.*` | Build configuration is startup-only |
| **Replication** | `enable`, `server_id`, `start_from`, `queue_size` | Replication initialization |
| **Memory** | `memory.*` | Memory allocator configuration |
| **API** | `tcp.bind`, `tcp.port`, `http.bind`, `http.port`, `max_connections` | Network socket binding |
| **Network** | `allow_cidrs` | Network security policy |
| **Logging** | `file` | Log output destination |

### MySQL Failover Example

When your MySQL primary fails, switch to a replica without restarting MygramDB:

```sql
-- Check current MySQL connection
SHOW VARIABLES LIKE 'mysql%';

-- Switch to new MySQL primary (GTID position is preserved)
SET mysql.host = '192.168.1.101', mysql.port = 3306;

-- Verify reconnection
SHOW VARIABLES LIKE 'mysql%';
```

**How it works**:
1. MygramDB saves current GTID position
2. Stops binlog reader
3. Closes old connection and opens new connection
4. Validates new MySQL server (GTID mode, binlog format)
5. Restarts binlog reader from saved GTID position

**Requirements**:
- New MySQL server must have GTID mode enabled
- New server must have same binlog format (ROW)
- GTID set must contain the saved position

### Cache Control Example

```sql
-- Disable cache during maintenance
SET cache.enabled = false;

-- Re-enable cache after maintenance
SET cache.enabled = true;

-- Adjust cache behavior
SET cache.min_query_cost_ms = 50.0;  -- Only cache slow queries
SET cache.ttl_seconds = 3600;        -- 1 hour TTL
```

### Variable Validation

SET commands validate values before applying:

```sql
-- Invalid value type (error)
SET api.default_limit = 'invalid';
ERROR: Invalid value for api.default_limit: must be integer

-- Out of range (error)
SET api.default_limit = 99999;
ERROR: Invalid value for api.default_limit: must be between 5 and 1000

-- Unknown variable (error)
SET unknown.variable = 'value';
ERROR: Unknown variable: unknown.variable

-- Immutable variable (error)
SET mysql.database = 'newdb';
ERROR: Variable mysql.database is immutable (requires restart)
```

### Checking Variable Values

```sql
-- Show all variables
SHOW VARIABLES;

-- Show variables by prefix
SHOW VARIABLES LIKE 'cache%';
SHOW VARIABLES LIKE 'mysql%';
SHOW VARIABLES LIKE 'api%';

-- Show specific pattern
SHOW VARIABLES LIKE '%_limit';
SHOW VARIABLES LIKE '%port%';
```

Output format (MySQL-compatible table):
```
+-------------------------+-----------------+
| Variable_name           | Value           |
+-------------------------+-----------------+
| cache.enabled           | true            |
| cache.min_query_cost_ms | 10.0            |
| cache.ttl_seconds       | 3600            |
+-------------------------+-----------------+
```

---

## Production Recommendations

### Security

1. **MySQL Connection**:
   - ✅ Enable SSL/TLS (`ssl_enable: true`)
   - ✅ Use strong passwords
   - ✅ Restrict MySQL user privileges (SELECT, REPLICATION SLAVE, REPLICATION CLIENT only)

2. **Network Security**:
   - ✅ Configure `network.allow_cidrs` (fail-closed by default)
   - ✅ Use `tcp.bind: 127.0.0.1` for localhost-only access
   - ✅ Use reverse proxy (nginx, haproxy) for internet-facing deployments

3. **Rate Limiting**:
   - ✅ Enable `rate_limiting` to prevent DoS attacks
   - ✅ Set `tcp.max_connections` based on system `ulimit -n`

### Performance

1. **Memory**:
   - ✅ Set `memory.hard_limit_mb` to 50-70% of system RAM
   - ✅ Set `memory.soft_target_mb` to 50% of `hard_limit_mb`

2. **Cache**:
   - ✅ Enable cache (`cache.enabled: true`)
   - ✅ Use `ngram` invalidation strategy
   - ✅ Set `min_query_cost_ms` to 10-50ms

3. **Dump Persistence**:
   - ✅ Enable auto-save (`dump.interval_sec: 7200` for 2 hours)
   - ✅ Retain 3-7 snapshots (`dump.retain: 3-7`)

4. **Replication**:
   - ✅ Set `queue_size: 10000` (default is sufficient)
   - ✅ Monitor queue size via `REPLICATION STATUS`

### Monitoring

1. **Logging**:
   - ✅ Use JSON logging (`logging.format: "json"`)
   - ✅ Send logs to centralized logging (ELK, Loki, etc.)

2. **Health Checks**:
   - ✅ Use HTTP `/health` endpoint for Kubernetes probes
   - ✅ Monitor `INFO` statistics

3. **Metrics**:
   - ✅ Track cache hit rate
   - ✅ Monitor replication lag
   - ✅ Alert on connection failures

### High Availability

1. **Dump Persistence**:
   - ✅ Enable auto-save for fast restarts
   - ✅ Backup dump files to S3/GCS

2. **Replication**:
   - ✅ Use `start_from: "snapshot"` for consistency
   - ✅ Monitor binlog reader status

3. **Failover**:
   - ✅ MygramDB automatically reconnects to MySQL on connection loss
   - ✅ Use MySQL replication (master-slave) for failover

---

## Troubleshooting

### Configuration Validation Errors

**Problem**: Configuration file has invalid syntax or unknown keys

**Solution**:
```bash
# Validate configuration
mygramdb --config=/path/to/config.yaml --validate

# Check JSON Schema validation errors
mygramdb --config=/path/to/config.yaml 2>&1 | grep -A5 "Schema validation failed"
```

### MySQL Connection Errors

**Problem**: Cannot connect to MySQL

**Diagnosis**:
```bash
# Check MySQL connection
mysql -h <host> -P <port> -u <user> -p<password>

# Verify GTID mode
mysql> SHOW VARIABLES LIKE 'gtid_mode';

# Verify binlog format
mysql> SHOW VARIABLES LIKE 'binlog_format';

# Verify user privileges
mysql> SHOW GRANTS FOR 'repl_user'@'%';
```

**Solution**:
- Enable GTID mode in MySQL
- Set binlog format to ROW
- Grant REPLICATION SLAVE, REPLICATION CLIENT privileges

### Runtime Variables Not Working

**Problem**: SET commands return errors or don't apply changes

**Diagnosis**:
```sql
-- Test basic SET command
SET logging.level = 'debug';

-- Verify variable value changed
SHOW VARIABLES LIKE 'logging%';

-- Check server logs
```

**Solution**:
- Ensure variable name is correct (case-sensitive after the dot)
- Check value type matches (string, integer, float, boolean)
- Verify variable is mutable (not immutable)
- Check server logs for detailed error messages

### Cache Not Working

**Problem**: Cache hit rate is 0%

**Diagnosis**:
```bash
# Check cache statistics
CACHE STATS

# Verify cache is enabled
grep "cache.enabled" config.yaml
```

**Solution**:
- Enable cache: `cache.enabled: true`
- Increase `max_memory_mb`
- Lower `min_query_cost_ms`

---

## References

- [Architecture Reference](architecture.md)
- [API Reference](api.md)
- [Deployment Guide](deployment.md)
- [Development Guide](development.md)
