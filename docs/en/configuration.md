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
4. [Hot Reload](#hot-reload)
5. [Production Recommendations](#production-recommendations)
6. [Troubleshooting](#troubleshooting)

---

## Overview

MygramDB uses YAML or JSON configuration files to define server behavior, MySQL connection settings, table indexing parameters, and operational policies.

### Key Features

- **Multiple formats**: YAML (`.yaml`, `.yml`) or JSON (`.json`)
- **Schema validation**: Automatic validation against built-in JSON Schema
- **Hot reload**: SIGHUP signal support for live configuration updates
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
| `datetime`, `date`, `timestamp` | DATETIME, DATE, TIMESTAMP | Date/time values |

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
  json: true                        # JSON format output
  file: ""                          # Log file path (empty = stdout)
```

#### Parameters

| Parameter | Type | Default | Description | Hot Reload |
|-----------|------|---------|-------------|------------|
| `level` | string | `info` | Log level: `debug`, `info`, `warn`, `error` | ✅ Yes |
| `json` | boolean | `true` | JSON format output (recommended for production) | ❌ No |
| `file` | string | `` | Log file path (empty = stdout, required for daemon mode) | ❌ No |

**Log Levels**:
- **`debug`**: Verbose logging (development only)
- **`info`**: Standard operational messages (recommended for production)
- **`warn`**: Warnings (anomalies that don't require immediate action)
- **`error`**: Errors (require attention)

**JSON Logging**:
- Recommended for **production** (easier to parse, structured)
- Disable for **development** (human-readable)

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

## Hot Reload

### Supported via SIGHUP

MygramDB supports **live configuration reload** without restart by sending a `SIGHUP` signal:

```bash
# Find process ID
ps aux | grep mygramdb

# Send SIGHUP signal
kill -HUP <pid>

# Or use systemd
systemctl reload mygramdb
```

### Reloadable Settings

| Section | Setting | Reload Behavior |
|---------|---------|-----------------|
| **Logging** | `level` | ✅ Applied immediately |
| **MySQL** | `host`, `port`, `user`, `password` | ✅ Reconnects to new MySQL server |
| **MySQL** | `ssl_*` | ✅ Reconnects with new SSL settings |
| **Dump** | `interval_sec`, `retain` | ✅ Updates scheduler settings |
| **Cache** | All settings | ⚠️ Cache cleared, new settings applied |
| **API** | `default_limit`, `max_query_length` | ⚠️ Applied to new queries |
| **API** | `rate_limiting.*` | ⚠️ Rate limiter reset |

### Non-Reloadable Settings (Require Restart)

| Section | Setting | Reason |
|---------|---------|--------|
| **MySQL** | `database` | Database connection cannot change |
| **MySQL** | `use_gtid`, `binlog_format`, `binlog_row_image` | Replication mode cannot change |
| **Tables** | All settings | Table schema and index structure cannot change |
| **Build** | All settings | Build configuration is startup-only |
| **Replication** | `enable`, `server_id`, `start_from`, `queue_size` | Replication configuration is startup-only |
| **Memory** | All settings | Memory allocator cannot change |
| **API** | `tcp.bind`, `tcp.port`, `http.bind`, `http.port` | Sockets cannot be rebound |
| **Network** | `allow_cidrs` | Network security policy cannot change |
| **Logging** | `json`, `file` | Log output cannot change |

### Reload Workflow

1. **Edit configuration file**:
   ```bash
   vim /etc/mygramdb/config.yaml
   ```

2. **Validate configuration** (optional):
   ```bash
   mygramdb --config=/etc/mygramdb/config.yaml --validate
   ```

3. **Send SIGHUP signal**:
   ```bash
   kill -HUP $(cat /var/run/mygramdb.pid)
   ```

4. **Verify reload**:
   ```bash
   tail -f /var/log/mygramdb/mygramdb.log
   ```

   Expected output:
   ```
   Configuration reload requested (SIGHUP received)
   Logging level changed: info -> debug
   Configuration reload completed successfully
   ```

### Reload Failure Handling

If configuration reload fails:
- **Current configuration continues** (no downtime)
- **Error logged** with details
- **Server continues operating** with old configuration

```
Failed to reload configuration: Invalid YAML syntax at line 42
Continuing with current configuration
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
   - ✅ Use JSON logging (`logging.json: true`)
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

### Hot Reload Not Working

**Problem**: SIGHUP signal does not trigger configuration reload

**Diagnosis**:
```bash
# Check process is running
ps aux | grep mygramdb

# Send SIGHUP
kill -HUP <pid>

# Check logs
tail -f /var/log/mygramdb/mygramdb.log
```

**Solution**:
- Ensure configuration file has no syntax errors
- Verify file path is correct
- Check log output for error details

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
