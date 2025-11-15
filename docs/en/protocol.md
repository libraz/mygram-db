# Protocol Reference

MygramDB uses a simple text-based protocol over TCP (similar to memcached).

## Connection

Connect to MygramDB via TCP:

```bash
telnet localhost 11016
```

Or use the CLI client:

```bash
./build/bin/mygram-cli -h localhost -p 11016
```

## Command Format

Commands are text-based, one command per line. Responses are terminated with newline.

---

## SEARCH Command

Search for documents containing specified text.

### Syntax

```
SEARCH <table> <text> [OPTIONS]
```

### Basic Examples

Simple search:
```
SEARCH articles hello
```

With filters and pagination:
```
SEARCH articles tech FILTER status = 1 LIMIT 10
```

### Response

```
OK RESULTS <total_count> <id1> <id2> <id3> ...
```

**For detailed query syntax, boolean operators, filters, sorting, and advanced features, see [Query Syntax Guide](query_syntax.md).**

---

## COUNT Command

Count documents matching search criteria (without returning IDs).

### Syntax

```
COUNT <table> <text> [OPTIONS]
```

### Example

```
COUNT articles tech AND AI FILTER status = 1
```

### Response

```
OK COUNT <number>
```

**For full query syntax, see [Query Syntax Guide](query_syntax.md).**

---

## GET Command

Retrieve a document by primary key.

### Syntax

```
GET <table> <primary_key>
```

### Example

```
GET articles 12345
```

### Response

```
DOC <primary_key> <filter1=value1> <filter2=value2> ...
```

Example:
```
DOC 12345 status=1 category=tech created_at=2024-01-15T10:30:00
```

Not found:
```
(error) Document not found
```

---

## INFO Command

Get comprehensive server information and statistics (Redis-style format).

### Syntax

```
INFO
```

### Response

Returns server information in Redis-style key-value format with multiple sections:

```
OK INFO

# Server
version: 1.0.0
uptime_seconds: 3600

# Stats
total_commands_processed: 10000
total_connections_received: 150
total_requests: 10000

# Commandstats
cmd_search: 8500
cmd_count: 1000
cmd_get: 500

# Memory
used_memory_bytes: 524288000
used_memory_human: 500.00 MB
used_memory_peak_bytes: 629145600
used_memory_peak_human: 600.00 MB
used_memory_index: 400.00 MB
used_memory_documents: 100.00 MB
memory_fragmentation_ratio: 1.20
total_system_memory: 16.00 GB
available_system_memory: 8.50 GB
system_memory_usage_ratio: 0.47
process_rss: 520.00 MB
process_rss_peak: 600.00 MB
memory_health: HEALTHY

# Index
total_documents: 1000000
total_terms: 1500000
total_postings: 5000000
avg_postings_per_term: 3.33
delta_encoded_lists: 1200000
roaring_bitmap_lists: 300000
optimization_status: idle

# Tables
tables: products, users, articles

# Clients
connected_clients: 5

# Replication
replication_inserts_applied: 50000
replication_updates_applied: 10000
replication_deletes_applied: 5000
```

### Memory Health Status

- **HEALTHY**: >20% system memory available
- **WARNING**: 10-20% system memory available
- **CRITICAL**: <10% system memory available (OPTIMIZE will be rejected)
- **UNKNOWN**: Unable to determine status

---

## CONFIG Commands

The CONFIG command family provides runtime configuration help, inspection, and verification.

### CONFIG HELP [path]

Display help for configuration options.

**Syntax:**
```
CONFIG HELP [path]
```

**Parameters:**
- `path` (optional): Dot-separated configuration path (e.g., `mysql.port`)

**Examples:**

Show all top-level configuration sections:
```
CONFIG HELP
```

Response:
```
+OK
Available configuration sections:
  mysql        - MySQL connection settings
  tables       - Table configuration (supports multiple tables)
  build        - Index build configuration
  replication  - Replication configuration
  memory       - Memory management
  dump         - Dump persistence (automatic backup)
  api          - API server configuration
  network      - Network security (optional)
  logging      - Logging configuration
  cache        - Query cache configuration

Use "CONFIG HELP <section>" for detailed information.
```

Show help for a specific section:
```
CONFIG HELP mysql
```

Response:
```
+OK
mysql - MySQL connection settings

Properties:
  host (string, default: "127.0.0.1")
    MySQL server hostname or IP

  port (integer, default: 3306, range: 1-65535)
    MySQL server port

  user (string, REQUIRED)
    MySQL username for replication

  password (string)
    MySQL user password

  database (string, REQUIRED)
    Database name

  use_gtid (boolean, default: true)
    Enable GTID-based replication

  ...
```

Show help for a specific property:
```
CONFIG HELP mysql.port
```

Response:
```
+OK
mysql.port

Type: integer
Default: 3306
Range: 1 - 65535
Description: MySQL server port
```

---

### CONFIG SHOW [path]

Display current configuration values. Sensitive fields (passwords, secrets) are masked with `***`.

**Syntax:**
```
CONFIG SHOW [path]
```

**Parameters:**
- `path` (optional): Dot-separated configuration path to show only specific section

**Examples:**

Show entire current configuration:
```
CONFIG SHOW
```

Response:
```
+OK
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "repl_user"
  password: "***"
  database: "mydb"
  use_gtid: true
  ...

tables:
  - name: "articles"
    primary_key: "id"
    ...

replication:
  enable: true
  server_id: 12345
  ...
```

Show specific section:
```
CONFIG SHOW mysql
```

Response:
```
+OK
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "repl_user"
  password: "***"
  database: "mydb"
  use_gtid: true
  ...
```

Show specific property:
```
CONFIG SHOW mysql.port
```

Response:
```
+OK
3306
```

---

### CONFIG VERIFY <filepath>

Verify a configuration file without loading it.

**Syntax:**
```
CONFIG VERIFY <filepath>
```

**Parameters:**
- `filepath` (required): Path to configuration file (YAML or JSON)

**Examples:**

Verify valid config:
```
CONFIG VERIFY /etc/mygramdb/config.yaml
```

Response (success):
```
+OK
Configuration is valid
  Tables: 2 (articles, products)
  MySQL: repl_user@127.0.0.1:3306
```

Verify invalid config:
```
CONFIG VERIFY /tmp/invalid.yaml
```

Response (error):
```
-ERR Configuration validation failed:
  - mysql.port: value 99999 exceeds maximum 65535
  - tables[0].name: missing required field
```

---


## DUMP Commands

The DUMP command family provides unified snapshot management with integrity verification.

### DUMP SAVE

Save complete snapshot to single binary file (`.dmp`).

**Syntax:**
```
DUMP SAVE [<filepath>] [--with-stats]
```

**Example:**
```
DUMP SAVE /backup/mygramdb.dmp --with-stats
```

### DUMP LOAD

Load snapshot from binary file.

**Syntax:**
```
DUMP LOAD [<filepath>]
```

**Example:**
```
DUMP LOAD /backup/mygramdb.dmp
```

### DUMP VERIFY

Verify snapshot file integrity without loading data.

**Syntax:**
```
DUMP VERIFY [<filepath>]
```

**Example:**
```
DUMP VERIFY /backup/mygramdb.dmp
```

### DUMP INFO

Display snapshot file metadata (version, GTID, tables, size, flags).

**Syntax:**
```
DUMP INFO [<filepath>]
```

**Example:**
```
DUMP INFO /backup/mygramdb.dmp
```

**For detailed snapshot management, integrity protection, best practices, and troubleshooting, see [Snapshot Guide](snapshot.md).**

---

## REPLICATION STATUS

Get current replication status.

### Syntax

```
REPLICATION STATUS
```

### Response

```
OK REPLICATION status=<running|stopped> gtid=<current_gtid>
```

Example:
```
OK REPLICATION status=running gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100
```

---

## REPLICATION STOP

Stop binlog replication (index becomes read-only).

### Syntax

```
REPLICATION STOP
```

### Response

```
OK REPLICATION STOPPED
```

---

## REPLICATION START

Resume binlog replication.

### Syntax

```
REPLICATION START
```

### Response

```
OK REPLICATION STARTED
```

---

## SYNC

Manually trigger snapshot synchronization from MySQL to MygramDB for a specific table.

### Syntax

```
SYNC <table_name>
```

### Parameters

- **table_name**: Name of the table to synchronize (must be configured in config file)

### Response (Success)

```
OK SYNC STARTED table=<table_name> job_id=1
```

### Response (Error)

```
ERROR SYNC already in progress for table '<table_name>'
ERROR Memory critically low. Cannot start SYNC. Check system memory.
ERROR Table '<table_name>' not found in configuration
```

### Behavior

- Runs asynchronously in the background
- Returns immediately after starting
- Builds snapshot from MySQL using SELECT query
- Captures GTID at snapshot time
- Automatically starts binlog replication with captured GTID when complete

### Conflicts

- **DUMP LOAD**: Blocked during SYNC (prevents data corruption)
- **REPLICATION START**: Blocked during SYNC (SYNC auto-starts replication)
- **SYNC** (same table): Blocked if already in progress

### Example

```bash
# Using CLI
mygram-cli SYNC articles

# Using telnet
echo "SYNC articles" | nc localhost 11016
```

---

## SYNC STATUS

Check the progress and status of SYNC operations.

### Syntax

```
SYNC STATUS
```

### Response Examples

**In Progress:**
```
table=articles status=IN_PROGRESS progress=10000/25000 rows (40.0%) rate=5000 rows/s
```

**Completed:**
```
table=articles status=COMPLETED rows=25000 time=5.2s gtid=uuid:123 replication=STARTED
```

**Failed:**
```
table=articles status=FAILED rows=5000 error="MySQL connection lost"
```

**Idle:**
```
status=IDLE message="No sync operation performed"
```

### Status Fields

| Field | Description |
|-------|-------------|
| `table` | Table name being synced |
| `status` | `IN_PROGRESS`, `COMPLETED`, `FAILED`, `IDLE`, `CANCELLED` |
| `progress` | Current/total rows processed |
| `rate` | Processing rate (rows/s) |
| `rows` | Total rows processed |
| `time` | Total processing time |
| `gtid` | Captured snapshot GTID |
| `replication` | Replication status: `STARTED`, `ALREADY_RUNNING`, `DISABLED`, `FAILED` |
| `error` | Error message (if failed) |

### Example

```bash
# Using CLI
mygram-cli SYNC STATUS

# Using telnet
echo "SYNC STATUS" | nc localhost 11016
```

### See Also

- [SYNC Command Guide](sync_command.md) - Detailed usage guide
- [Replication Guide](replication.md) - Manual snapshot synchronization setup

---

## OPTIMIZE Command

Optimize index posting lists (convert Delta Encoding to Roaring Bitmap based on density).

### Syntax

```
OPTIMIZE
```

### How it works

- **Pre-execution checks**: Memory health and availability verification
- Temporarily stops binlog replication
- Copies and optimizes posting lists in batches
- Query processing continues (using old index)
- Atomically switches after optimization completes
- Resumes binlog replication

### Memory Safety

**Pre-execution Memory Checks:**
- Rejects execution if system memory health is **CRITICAL** (<10% available)
- Estimates required memory based on index size and batch size
- Requires: `available_memory >= estimated_memory + 10% safety margin`
- Typical memory overhead: ~5-15% of index size during optimization

**Memory Usage Pattern:**
- **Index portion only** temporarily doubles (document store unchanged)
- Overall memory usage increases by approximately 1.05-1.15x
- Memory is freed gradually through batch processing

### Global Exclusion

- **Only one OPTIMIZE operation** can run at a time across all tables
- New OPTIMIZE commands are rejected while optimization is in progress
- Check `optimization_status` via `INFO` command

### Performance

- Small indexes (<10K terms): <1 second
- Medium indexes (10K-100K terms): 1-10 seconds
- Large indexes (>100K terms): 10+ seconds
- Concurrent searches: minimal impact (short lock durations)
- Concurrent updates: safe but may see brief contention

### Response

**Success:**
```
OK OPTIMIZED terms=<total> delta=<count> roaring=<count> memory=<size>
```

Example:
```
OK OPTIMIZED terms=1500000 delta=1200000 roaring=300000 memory=450.00 MB
```

**Errors:**

Already optimizing:
```
ERROR Another OPTIMIZE operation is already in progress
```

Memory critically low:
```
ERROR Memory critically low. Cannot start optimization: available=1.50 GB total=8.00 GB
```

Insufficient memory:
```
ERROR Insufficient memory for optimization: estimated=2.50 GB available=1.80 GB
```

---

## DEBUG Command

Enable or disable debug mode for the current connection to see detailed query execution metrics.

### Syntax

```
DEBUG ON
DEBUG OFF
```

### How it works

- **Per-Connection State**: Debug mode is enabled/disabled for the current connection only
- **Query Timing**: Shows execution time breakdown (index search, filtering)
- **Search Details**: Displays n-grams generated, posting list sizes, and candidate counts
- **Optimization Visibility**: Reports which optimization strategies were applied
- **Performance Impact**: Minimal overhead, only collects metrics when enabled

### Response

```
OK DEBUG_ON
```

or

```
OK DEBUG_OFF
```

### Debug Output Format

When debug mode is enabled, SEARCH and COUNT commands return additional debug information:

```
OK RESULTS <count> <id1> <id2> ...

# DEBUG
query_time: <ms>
index_time: <ms>
filter_time: <ms>
terms: <n>
ngrams: <n>
candidates: <n>
after_intersection: <n>
after_not: <n>
after_filters: <n>
final: <n>
optimization: <strategy>
order_by: <column> <direction>
limit: <value> [(default)]
offset: <value> [(default)]
```

### Debug Metrics Explained

- **query_time**: Total query execution time in milliseconds
- **index_time**: Time spent searching the index
- **filter_time**: Time spent applying filters (if any)
- **terms**: Number of search terms
- **ngrams**: Total n-grams generated from search terms
- **candidates**: Initial candidate documents from index
- **after_intersection**: Results after AND term intersection
- **after_not**: Results after NOT term filtering (if NOT used)
- **after_filters**: Results after FILTER conditions (if filters used)
- **final**: Total matching documents (before LIMIT/OFFSET)
- **optimization**: Strategy used (e.g., `merge_join`, `early_exit`, `none`)
- **order_by**: Applied sorting (column and direction)
- **limit**: Maximum results returned (shows "(default)" if not explicitly specified)
- **offset**: Result offset for pagination (shows "(default)" if not explicitly specified)

---

## Error Response

All errors follow this format:

```
ERROR <error_message>
```

Examples:
```
ERROR Unknown command
ERROR Table not found: products
ERROR Invalid GTID format
```

---

## CLI Client Features

The CLI client (`mygram-cli`) provides an interactive shell with:

- **Tab Completion**: Press TAB to autocomplete command names (requires GNU Readline)
- **Command History**: Use ↑/↓ arrow keys to navigate history (requires GNU Readline)
- **Line Editing**: Full line editing with Ctrl+A, Ctrl+E, etc. (requires GNU Readline)
- **Error Handling**: Graceful error messages (does not crash)

### Interactive Mode

```bash
./build/bin/mygram-cli
> SEARCH articles hello
OK RESULTS 5 1 2 3 4 5
> quit
```

### Single Command Mode

```bash
./build/bin/mygram-cli SEARCH articles "hello world"
```

### Help Command

In interactive mode, type `help` to see available commands:

```
> help
Available commands:
  SEARCH, COUNT, GET              - Search and retrieval
  INFO, CONFIG                    - Server information and configuration
  DUMP SAVE/LOAD/VERIFY/INFO      - Snapshot management
  REPLICATION STATUS/STOP/START   - Replication control
  OPTIMIZE                        - Index optimization
  DEBUG ON/OFF                    - Enable/disable debug mode
  quit, exit                      - Exit client
```

---

## See Also

- [Query Syntax Guide](query_syntax.md) - Detailed SEARCH/COUNT query syntax
- [Snapshot Guide](snapshot.md) - Snapshot management and best practices
- [Configuration Guide](configuration.md) - Server configuration
- [Replication Setup](replication.md) - MySQL replication configuration
