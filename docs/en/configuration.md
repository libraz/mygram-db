# Configuration Guide

MygramDB supports both **YAML** and **JSON** configuration formats with automatic **JSON Schema validation**. This guide explains all available configuration options.

## Configuration File Formats

MygramDB automatically detects the configuration format based on the file extension:

- `.yaml` or `.yml` → YAML format
- `.json` → JSON format

All configurations are automatically validated against a built-in JSON Schema at startup, ensuring that invalid settings (typos, wrong types, unknown keys) are caught immediately.

## Configuration File Structure

Create a configuration file in YAML or JSON format:

### YAML Format (config.yaml)

```yaml
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "repl_user"
  password: "your_password_here"
  database: "mydb"
  use_gtid: true
  binlog_format: "ROW"
  binlog_row_image: "FULL"
  connect_timeout_ms: 3000

tables:
  - name: "articles"
    primary_key: "id"
    text_source:
      column: "content"
    required_filters: []
    filters: []
    ngram_size: 2
    kanji_ngram_size: 1
    posting:
      block_size: 128
      freq_bits: 0
      use_roaring: "auto"

build:
  mode: "select_snapshot"
  batch_size: 5000
  parallelism: 2
  throttle_ms: 0

replication:
  enable: true
  auto_initial_snapshot: false
  server_id: 12345
  start_from: "snapshot"
  queue_size: 10000
  reconnect_backoff_min_ms: 500
  reconnect_backoff_max_ms: 10000

memory:
  hard_limit_mb: 8192
  soft_target_mb: 4096
  arena_chunk_mb: 64
  roaring_threshold: 0.18
  minute_epoch: true
  normalize:
    nfkc: true
    width: "narrow"
    lower: false

dump:
  dir: "/var/lib/mygramdb/dumps"
  default_filename: "mygramdb.dmp"
  interval_sec: 600
  retain: 3

api:
  tcp:
    bind: "0.0.0.0"
    port: 11016
  http:
    enable: true
    bind: "127.0.0.1"
    port: 8080

network:
  allow_cidrs: []

logging:
  level: "info"
  json: true
```

### JSON Format (config.json)

The same configuration in JSON format:

```json
{
  "mysql": {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "repl_user",
    "password": "your_password_here",
    "database": "mydb",
    "use_gtid": true,
    "binlog_format": "ROW",
    "binlog_row_image": "FULL",
    "connect_timeout_ms": 3000
  },
  "tables": [
    {
      "name": "articles",
      "primary_key": "id",
      "text_source": {
        "column": "content"
      },
      "required_filters": [],
      "filters": [],
      "ngram_size": 2,
      "kanji_ngram_size": 1,
      "posting": {
        "block_size": 128,
        "freq_bits": 0,
        "use_roaring": "auto"
      }
    }
  ],
  "build": {
    "mode": "select_snapshot",
    "batch_size": 5000,
    "parallelism": 2,
    "throttle_ms": 0
  },
  "replication": {
    "enable": true,
    "server_id": 12345,
    "start_from": "snapshot",
    "queue_size": 10000,
    "reconnect_backoff_min_ms": 500,
    "reconnect_backoff_max_ms": 10000
  },
  "memory": {
    "hard_limit_mb": 8192,
    "soft_target_mb": 4096,
    "arena_chunk_mb": 64,
    "roaring_threshold": 0.18,
    "minute_epoch": true,
    "normalize": {
      "nfkc": true,
      "width": "narrow",
      "lower": false
    }
  },
  "dump": {
    "dir": "/var/lib/mygramdb/dumps",
    "default_filename": "mygramdb.dmp",
    "interval_sec": 600,
    "retain": 3
  },
  "api": {
    "tcp": {
      "bind": "0.0.0.0",
      "port": 11016
    },
    "http": {
      "enable": true,
      "bind": "127.0.0.1",
      "port": 8080
    }
  },
  "network": {
    "allow_cidrs": []
  },
  "logging": {
    "level": "info",
    "json": true
  }
}
```

**Note:** All examples in this guide use YAML format for readability, but all settings can be used in JSON format as well.

## MySQL Section

Connection settings for MySQL server:

- **host**: MySQL server hostname or IP address (default: `127.0.0.1`)
- **port**: MySQL server port (default: `3306`)
- **user**: MySQL username for replication (required)
- **password**: MySQL user password (required)
- **database**: Database name (required)
- **use_gtid**: Enable GTID-based replication (default: `true`, required for replication)
- **binlog_format**: Binary log format (default: `ROW`, required for replication)
- **binlog_row_image**: Row image format (default: `FULL`, required for replication)
- **connect_timeout_ms**: Connection timeout in milliseconds (default: `3000`)

## Tables Section

Table configuration (supports multiple tables):

### Basic Settings

- **name**: Table name in MySQL database (required)
- **primary_key**: Primary key column name (default: `id`, must be single-column)
- **ngram_size**: N-gram size for ASCII/alphanumeric characters (default: `2`)
  - 1 = unigram, 2 = bigram, etc.
  - For mixed-language content: recommended 2
  - For English-only: use 3 or more
- **kanji_ngram_size**: N-gram size for CJK (kanji/kana/hanzi) characters (default: `0`)
  - Set to 0 or omit to use `ngram_size` value for all characters
  - For Japanese/Chinese text: recommended 1 (unigram)
  - Allows hybrid tokenization: different n-gram sizes for ASCII vs CJK text

### Text Source

Define which column(s) to index for full-text search:

**Single column:**

```yaml
text_source:
  column: "content"
  delimiter: " "                    # Default: " " (used when concat is specified)
```

**Multiple columns (concatenation):**

```yaml
text_source:
  concat: ["title", "body"]
  delimiter: " "
```

### Required Filters (Data Existence Conditions)

Required filters define conditions that data must satisfy to be indexed. Data that does not match these conditions will **NOT be indexed at all**.

During binlog replication:
- Data transitioning **OUT** of these conditions will be **REMOVED** from the index
- Data transitioning **INTO** these conditions will be **ADDED** to the index
- Data that remains within these conditions will be updated normally

```yaml
required_filters:
  - name: "enabled"                 # Column name
    type: "int"                     # Column type (see types below)
    op: "="                         # Operator
    value: 1                        # Comparison value
    bitmap_index: false             # Enable bitmap index for search-time filtering

  - name: "deleted_at"
    type: "datetime"
    op: "IS NULL"                   # Only index non-deleted records
    bitmap_index: false
```

**Supported operators:**

- Comparison: `=`, `!=`, `<`, `>`, `<=`, `>=`
- NULL checks: `IS NULL`, `IS NOT NULL`

**Important notes:**

- All `required_filters` conditions are combined with AND logic
- The `value` field should be omitted for `IS NULL` and `IS NOT NULL` operators
- Columns used in `required_filters` must be included in the table schema
- These filters are evaluated during both snapshot build and binlog replication

### Optional Filters (Search-Time Filtering)

Optional filters are used for filtering during searches. They do **NOT** affect which data is indexed.

```yaml
filters:
  - name: "status"
    type: "int"
    dict_compress: false            # Default: false
    bitmap_index: false             # Default: false

  - name: "category"
    type: "string"
    dict_compress: false
    bitmap_index: false

  - name: "created_at"
    type: "datetime"
    bucket: "minute"                # Optional: "minute", "hour", "day"
```

**Filter types:**

- Integer types: `tinyint`, `tinyint_unsigned`, `smallint`, `smallint_unsigned`, `int`, `int_unsigned`, `mediumint`, `mediumint_unsigned`, `bigint`
- Float types: `float`, `double`
- String types: `string`, `varchar`, `text`
- Date types: `datetime`, `date`, `timestamp`

**Filter options:**

- **dict_compress**: Enable dictionary compression (recommended for low-cardinality columns)
- **bitmap_index**: Enable bitmap indexing for faster filtering
- **bucket**: Datetime bucketing (`minute`, `hour`, or `day`) to reduce cardinality

### Posting List Configuration

Control how posting lists are stored:

```yaml
posting:
  block_size: 128                   # Default: 128
  freq_bits: 0                      # 0=boolean, 4 or 8 for term frequency (default: 0)
  use_roaring: "auto"               # "auto", "always", "never" (default: auto)
```

## Build Section

Index build configuration:

- **mode**: Build mode (default: `select_snapshot`, currently only option)
- **batch_size**: Number of rows per batch during snapshot (default: `5000`)
- **parallelism**: Number of parallel build threads (default: `2`)
- **throttle_ms**: Delay between batches in milliseconds (default: `0`)

## Replication Section

MySQL binlog replication settings:

- **enable**: Enable binlog replication (default: `true`)
- **auto_initial_snapshot**: Automatically build snapshot on startup (default: `false`)
  - `false`: Manual snapshot synchronization using `SYNC` command (recommended, safe by default)
  - `true`: Legacy behavior - automatically build snapshot on startup
  - Setting to `false` prevents unexpected MySQL load on startup
  - See [SYNC Command Guide](sync_command.md) for manual synchronization
- **server_id**: MySQL server ID (required, must be non-zero and unique in replication topology)
  - Generate a random number or use a unique value for your environment
  - Example: `12345`
- **start_from**: Replication start position (default: `snapshot`)
  - `snapshot`: Start from snapshot GTID (recommended)
  - `latest`: Start from current GTID
  - `gtid=<UUID:txn>`: Start from specific GTID (e.g., `gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:1`)
- **queue_size**: Binlog event queue size (default: `10000`)
- **reconnect_backoff_min_ms**: Minimum reconnect backoff delay (default: `500`)
- **reconnect_backoff_max_ms**: Maximum reconnect backoff delay (default: `10000`)

## Memory Section

Memory management configuration:

- **hard_limit_mb**: Hard memory limit in MB (default: `8192`)
- **soft_target_mb**: Soft memory target in MB (default: `4096`)
- **arena_chunk_mb**: Arena chunk size in MB (default: `64`)
- **roaring_threshold**: Roaring bitmap threshold (default: `0.18`)
- **minute_epoch**: Use minute-precision epoch (default: `true`)

### Text Normalization

```yaml
normalize:
  nfkc: true                        # Default: true, NFKC normalization
  width: "narrow"                   # "keep", "narrow", "wide" (default: narrow)
  lower: false                      # Lowercase conversion (default: false)
```

## Dump Section

Dump (snapshot) persistence settings with automatic backup:

- **dir**: Dump directory path (default: `/var/lib/mygramdb/dumps`)
  - Directory is created automatically on startup if it doesn't exist
  - Write permissions are verified at startup
- **default_filename**: Default filename for manual `DUMP SAVE` commands (default: `mygramdb.dmp`)
- **interval_sec**: Auto-save interval in seconds (default: `600`, `0` = disabled)
  - Automatic dumps are saved with timestamp-based filenames: `auto_YYYYMMDD_HHMMSS.dmp`
  - Similar to Redis RDB persistence
- **retain**: Number of auto-saved dumps to retain (default: `3`)
  - Older auto-saved files are automatically cleaned up
  - Manual dumps (via `DUMP SAVE` command) are not affected by cleanup

## API Section

API server configuration:

### TCP API

```yaml
api:
  tcp:
    bind: "0.0.0.0"                 # Default: 0.0.0.0 (all interfaces)
    port: 11016                     # Default: 11016
```

### HTTP API (Optional)

```yaml
api:
  http:
    enable: true                    # Default: true
    bind: "127.0.0.1"               # Default: 127.0.0.1 (localhost only)
    port: 8080                      # Default: 8080
```

## Network Section (Optional)

Network security configuration:

- **allow_cidrs**: Allow CIDR list (default: `[]` = allow all)
  - When empty, all IP addresses are allowed
  - When specified, only connections from these IP ranges are accepted
  - Supports standard CIDR notation (e.g., `192.168.1.0/24`, `10.0.0.0/8`)
  - Multiple CIDR ranges can be specified

```yaml
network:
  allow_cidrs:
    - "192.168.1.0/24"
    - "10.0.0.0/8"
    - "172.16.0.0/16"
```

**Common CIDR ranges:**

- Private networks:
  - `10.0.0.0/8` - Class A private network
  - `172.16.0.0/12` - Class B private network
  - `192.168.0.0/16` - Class C private network
- Localhost: `127.0.0.1/32`
- Single IP: `192.168.1.100/32`

## Logging Section

Logging configuration:

- **level**: Log level (default: `info`)
  - Options: `debug`, `info`, `warn`, `error`
- **json**: JSON format output (default: `true`)

```yaml
logging:
  level: "info"
  json: true
```

## Cache Section

Query result cache configuration:

```yaml
cache:
  enabled: true                       # Default: true (enabled)
  max_memory_mb: 32                   # Default: 32MB
  min_query_cost_ms: 10.0             # Default: 10.0ms
  ttl_seconds: 3600                   # Default: 3600 (1 hour)
  invalidation_strategy: "ngram"      # Default: "ngram"
  compression_enabled: true           # Default: true (LZ4)
  eviction_batch_size: 10             # Default: 10
  invalidation:
    batch_size: 1000                  # Default: 1000
    max_delay_ms: 100                 # Default: 100ms
```

**Settings:**

- **enabled**: Enable/disable query cache (default: `true`)
- **max_memory_mb**: Maximum cache memory in MB (default: `32`)
  - Cache automatically evicts old entries when memory limit is reached
- **min_query_cost_ms**: Minimum query execution time to cache (default: `10.0`)
  - Only queries taking longer than this threshold are cached
  - Prevents caching very fast queries that don't benefit from caching
- **ttl_seconds**: Time-to-live for cache entries in seconds (default: `3600`, 0 = no TTL)
  - Entries are automatically expired after this duration
- **invalidation_strategy**: How to invalidate cache on data changes (default: `"ngram"`)
  - `"ngram"`: Invalidate entries matching affected n-grams (precise, recommended)
  - `"table"`: Invalidate all entries for the table (simple, less efficient)
- **compression_enabled**: Enable LZ4 compression for cached results (default: `true`)
  - Reduces memory usage for large result sets
  - Small CPU overhead for compression/decompression
- **eviction_batch_size**: Number of entries to evict at once when memory is full (default: `10`)

**Invalidation Queue Settings:**

- **invalidation.batch_size**: Process invalidations after N unique (table, ngram) pairs (default: `1000`)
- **invalidation.max_delay_ms**: Maximum delay before processing invalidations (default: `100`)

**How it works:**

1. Query results are cached after first execution if query time exceeds `min_query_cost_ms`
2. Subsequent identical queries return cached results instantly (sub-millisecond)
3. When data changes via binlog replication:
   - Affected n-grams are identified
   - Cache entries matching those n-grams are invalidated
   - Next query will rebuild the cache entry
4. Entries expire after `ttl_seconds` even if data hasn't changed

**Performance impact:**

- Cache hit: < 1ms (memory lookup + optional decompression)
- Cache miss: Normal query time + small caching overhead
- Memory usage: Controlled by `max_memory_mb` with automatic eviction

**When to use:**

- Frequently repeated queries (search suggestions, popular searches)
- High query cost queries (complex filters, large result sets)
- Read-heavy workloads with some repeated patterns

**When to disable:**

- Highly unique query patterns (little repetition)
- Very fast queries (< 10ms consistently)
- Tight memory constraints

## Automatic Validation

MygramDB automatically validates all configuration files (YAML and JSON) using a built-in JSON Schema at startup. This validation ensures:

- **Syntax correctness**: Valid YAML/JSON format
- **Type checking**: Correct data types for all fields
- **Required fields**: All mandatory settings are present
- **Value constraints**: Values are within valid ranges and enums
- **Unknown keys**: Detects typos and unsupported options

If validation fails, MygramDB will display a detailed error message pointing to the exact problem.

## Runtime Configuration Help

MygramDB provides runtime commands to explore configuration options, view current settings, and verify configuration files without restarting the server.

### Getting Configuration Help

You can query configuration help at runtime using the `CONFIG HELP` command:

```bash
# Show all configuration sections
echo "CONFIG HELP" | nc localhost 11016
```

**Example output:**
```
+OK
Available configuration sections:
  mysql        - MySQL connection settings
  tables       - Table configuration (supports multiple tables)
  build        - Index build configuration
  replication  - Replication configuration
  memory       - Memory management
  ...
```

### Viewing Detailed Help

Get help for specific configuration options:

```bash
# Help for MySQL section
echo "CONFIG HELP mysql" | nc localhost 11016

# Help for a specific property
echo "CONFIG HELP mysql.port" | nc localhost 11016
```

This displays:
- Property type (string, integer, boolean, etc.)
- Default value
- Valid range or allowed values
- Description
- Whether the field is required

### Viewing Current Configuration

View the running configuration with sensitive fields masked:

```bash
# Show entire configuration
echo "CONFIG SHOW" | nc localhost 11016

# Show specific section
echo "CONFIG SHOW mysql" | nc localhost 11016

# Show specific property
echo "CONFIG SHOW mysql.port" | nc localhost 11016
```

**Note**: Sensitive fields (passwords, secrets, keys, tokens) are automatically masked as `***` in the output.

### Verifying Configuration Files

Verify a configuration file before deploying it:

```bash
echo "CONFIG VERIFY /path/to/config.yaml" | nc localhost 11016
```

This validates the configuration without loading it into the running server. If the configuration is valid, you'll see a summary:

```
+OK
Configuration is valid
  Tables: 2 (articles, products)
  MySQL: repl_user@127.0.0.1:3306
```

If invalid, you'll see detailed error messages:

```
-ERR Configuration validation failed:
  - mysql.port: value 99999 exceeds maximum 65535
  - tables[0].name: missing required field
```

For complete CONFIG command syntax, see the [Protocol Reference](protocol.md#config-commands).

---

## Testing Configuration

Before starting the server, you can test your configuration file:

```bash
# Test YAML configuration
./build/bin/mygramdb -t config.yaml

# Test JSON configuration
./build/bin/mygramdb -t config.json

# Or use long option
./build/bin/mygramdb --config-test config.yaml
```

This will:

1. Parse the configuration file
2. Validate against JSON Schema
3. Display configuration summary if valid

If valid, it displays:

- MySQL connection settings
- Table configurations
- API server settings
- Replication status
- Logging level

### Custom Schema (Advanced)

For testing or extending the configuration, you can validate against a custom schema:

```bash
./build/bin/mygramdb config.yaml --schema custom-schema.json
```

## Example Configurations

Complete configuration examples with all available options:
- YAML: `examples/config.yaml`
- JSON: `examples/config.json`

Minimal configuration examples to get started quickly:
- YAML: `examples/config-minimal.yaml`
- JSON: `examples/config-minimal.json`

See `examples/README.md` for more information about each example file.

## Usage

Once you have a configuration file, you can start MygramDB:

```bash
# Show help
./build/bin/mygramdb --help

# Show version
./build/bin/mygramdb --version

# Test configuration
./build/bin/mygramdb -t config.yaml

# Start server (both formats supported)
./build/bin/mygramdb -c config.yaml
# or
./build/bin/mygramdb config.yaml
```

**Command-line Options:**
- `-c, --config <file>` - Configuration file path
- `-t, --config-test` - Test configuration and exit
- `-h, --help` - Show help message
- `-v, --version` - Show version information
- `-s, --schema <file>` - Use custom JSON Schema (advanced)
