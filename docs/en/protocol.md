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
SEARCH articles tech FILTER status=1 LIMIT 10
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
COUNT articles tech AND AI FILTER status=1
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
OK DOC <primary_key> <filter1=value1> <filter2=value2> ...
```

Example:
```
OK DOC 12345 status=1 category=tech created_at=2024-01-15T10:30:00
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

## CONFIG Command

Get current server configuration (all settings).

### Syntax

```
CONFIG
```

### Response

Returns a YAML-style formatted configuration showing:
- MySQL connection settings
- Table configurations (name, primary_key, ngram_size, filters count)
- API server settings (bind address and port)
- Replication settings (enable, server_id, start_from)
- Memory configuration (limits, thresholds)
- Snapshot directory
- Logging level
- Runtime status (connections, uptime, read_only mode)

---

## CONFIG VERIFY

Validate current configuration and check system status.

### Syntax

```
CONFIG VERIFY
```

### Response

```
OK CONFIG VERIFIED
tables: <count>

table: <table_name>
  primary_key: <column>
  text_source: <source>
  ngram_size: <size>
  filters: <count>
  required_filters: <count>
  status: loaded|not_loaded
  documents: <count>
  terms: <count>

replication:
  status: running|stopped
  gtid: <gtid>

END
```

---

## DUMP Commands

The DUMP command family provides unified snapshot management with integrity verification.

### DUMP SAVE

Save complete snapshot to single binary file (`.dmp`).

**Syntax:**
```
DUMP SAVE [<filepath>] [WITH STATISTICS]
```

**Example:**
```
DUMP SAVE /backup/mygramdb.dmp WITH STATISTICS
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
  INFO, CONFIG, CONFIG VERIFY     - Server information and validation
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
