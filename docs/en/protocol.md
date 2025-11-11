# Protocol Reference

MygramDB uses a simple text-based protocol over TCP (similar to memcached).

## Connection

Connect to MygramDB via TCP:

```bash
telnet localhost 11311
```

Or use the CLI client:

```bash
./build/bin/mygram-cli -h localhost -p 11311
```

## Command Format

Commands are text-based, one command per line. Responses are terminated with newline.

## SEARCH Command

Search for documents containing specified text.

### Syntax

```
SEARCH <table> <text> [AND <term>...] [NOT <term>...] [FILTER <col=val>...] [LIMIT <n>] [OFFSET <n>]
```

### Parameters

- **table**: Table name
- **text**: Search text or phrase
- **AND term**: Additional required terms (multiple allowed)
- **NOT term**: Excluded terms (multiple allowed)
- **FILTER col=val**: Filter by column value (multiple allowed)
- **LIMIT n**: Maximum number of results (default: 1000)
- **OFFSET n**: Result offset for pagination (default: 0)

### Examples

Basic search:
```
SEARCH articles hello
```

Phrase search (quoted):
```
SEARCH articles "live streaming" LIMIT 100
```

Multiple required terms:
```
SEARCH articles tech AND AI
```

Combine AND and NOT:
```
SEARCH articles news AND breaking NOT old
```

With filters:
```
SEARCH articles news NOT old FILTER status=1
```

Pagination:
```
SEARCH articles tech FILTER category=AI LIMIT 50 OFFSET 100
```

### Search Syntax Features

**Quoted strings:**
- Use single `'` or double `"` quotes for phrase searches
- Example: `"hello world"` searches for the exact phrase
- Escape sequences supported: `\n`, `\t`, `\r`, `\\`, `\"`, `\'`

**AND operator:**
- Search for documents containing all specified terms
- Example: `term1 AND term2 AND term3`

**NOT operator:**
- Exclude documents containing specific terms
- Example: `term1 NOT excluded`

### Response

```
OK RESULTS <total_count> <id1> <id2> <id3> ...
```

Example:
```
OK RESULTS 3 101 205 387
```

## COUNT Command

Count documents matching search criteria (without returning IDs).

### Syntax

```
COUNT <table> <text> [AND <term>...] [NOT <term>...] [FILTER <col=val>...]
```

### Examples

Basic count:
```
COUNT articles hello
```

Count with multiple terms:
```
COUNT articles tech AND AI
```

Count with filters:
```
COUNT articles news NOT old FILTER status=1
```

### Response

```
OK COUNT <number>
```

Example:
```
OK COUNT 42
```

## GET Command

Retrieve a document by primary key.

### Syntax

```
GET <table> <primary_key>
```

### Examples

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

## INFO Command

Get server information and statistics.

### Syntax

```
INFO
```

### Response

```
OK INFO version=<version> uptime=<seconds> total_requests=<count> connections=<count> index_size=<bytes> doc_count=<count>
```

Example:
```
OK INFO version=1.0.0 uptime=3600 total_requests=10000 connections=5 index_size=1048576 doc_count=1000000
```

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
- Replication settings (enable, server_id, start_from, state_file)
- Memory configuration (limits, thresholds)
- Snapshot directory
- Logging level
- Runtime status (connections, uptime, read_only mode)

Example:
```
CONFIG
OK CONFIG
  mysql:
    host: 127.0.0.1
    port: 3306
    user: repl_user
    database: mydb
    use_gtid: true
  tables: 1
    - name: articles
      primary_key: id
      ngram_size: 1
      filters: 3
  api:
    tcp.bind: 0.0.0.0
    tcp.port: 11311
  replication:
    enable: true
    server_id: 12345
    start_from: snapshot
    state_file: ./mygramdb_replication.state
  memory:
    hard_limit_mb: 8192
    soft_target_mb: 4096
    roaring_threshold: 0.18
  snapshot:
    dir: /var/lib/mygramdb/snapshots
  logging:
    level: info
  runtime:
    connections: 5
    max_connections: 1000
    read_only: false
    uptime: 3600s
```

## SAVE Command

Save current index snapshot to disk.

### Syntax

```
SAVE [<filepath>]
```

### Examples

Save to default location:
```
SAVE
```

Save to specific file:
```
SAVE /path/to/snapshot.bin
```

### Response

```
OK SAVED <filepath>
```

## LOAD Command

Load index snapshot from disk.

### Syntax

```
LOAD <filepath>
```

### Examples

```
LOAD /path/to/snapshot.bin
```

### Response

```
OK LOADED <filepath> docs=<count>
```

## REPLICATION STATUS Command

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

## REPLICATION STOP Command

Stop binlog replication (index becomes read-only).

### Syntax

```
REPLICATION STOP
```

### Response

```
OK REPLICATION STOPPED
```

## REPLICATION START Command

Resume binlog replication.

### Syntax

```
REPLICATION START
```

### Response

```
OK REPLICATION STARTED
```

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
  SEARCH, COUNT, GET    - Search and retrieval
  INFO, CONFIG          - Server information
  SAVE, LOAD            - Snapshot management
  REPLICATION STATUS/STOP/START - Replication control
  quit, exit            - Exit client
```
