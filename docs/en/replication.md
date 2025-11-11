# MySQL Replication Guide

MygramDB supports real-time replication from MySQL using GTID-based binlog streaming with guaranteed data consistency.

## Prerequisites

### MySQL Server Requirements

MygramDB requires:
- **MySQL Version**: 5.7.6+ or 8.0+ (tested with MySQL 8.0 and 8.4)
- **GTID Mode**: Must be enabled
- **Binary Log Format**: ROW format required
- **Privileges**: Replication user needs specific privileges

### Enable GTID Mode

Check if GTID mode is enabled:

```sql
SHOW VARIABLES LIKE 'gtid_mode';
```

If GTID mode is OFF, enable it:

```sql
-- Enable GTID mode (requires server restart in MySQL 5.7)
SET GLOBAL gtid_mode = ON;
SET GLOBAL enforce_gtid_consistency = ON;
```

### Configure Binary Log

Ensure binary logging is enabled with ROW format:

```sql
-- Check binary log format
SHOW VARIABLES LIKE 'binlog_format';

-- Set to ROW format (add to my.cnf and restart)
SET GLOBAL binlog_format = ROW;
```

### Create Replication User

Create a user with replication privileges:

```sql
-- Create replication user
CREATE USER 'repl_user'@'%' IDENTIFIED BY 'your_password';

-- Grant replication privileges
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_user'@'%';

-- Apply changes
FLUSH PRIVILEGES;
```

## Replication Start Options

Configure `replication.start_from` in your config file:

### snapshot (Recommended)

Starts from GTID captured during initial snapshot build:

```yaml
replication:
  start_from: "snapshot"
```

**How it works:**
- Uses `START TRANSACTION WITH CONSISTENT SNAPSHOT` for data consistency
- Captures `@@global.gtid_executed` at exact snapshot moment
- Guarantees no data loss between snapshot and binlog replication

**When to use:**
- Initial setup (recommended for most cases)
- When you need a consistent point-in-time view
- When starting from scratch

### latest

Starts from current GTID position (ignores historical data):

```yaml
replication:
  start_from: "latest"
```

**How it works:**
- Uses `SHOW BINARY LOG STATUS` to get latest GTID
- Only captures changes after MygramDB starts

**When to use:**
- When you only need real-time changes
- When historical data is not important

### gtid=UUID:txn

Starts from specific GTID position:

```yaml
replication:
  start_from: "gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:100"
```

**When to use:**
- Manual recovery from specific point
- Testing or debugging

### state_file

Resumes from saved GTID state file:

```yaml
replication:
  start_from: "state_file"
  state_file: "./mygramdb_replication.state"
```

**How it works:**
- Reads GTID from state file (created automatically)
- Enables crash recovery and resume

**When to use:**
- Restarting MygramDB after shutdown
- Automatic resume after crash

## Supported Operations

### DML Operations

MygramDB automatically handles:

- **INSERT** (WRITE_ROWS events)
  - Adds new documents to index and store
- **UPDATE** (UPDATE_ROWS events)
  - Updates document content and filters
  - Re-indexes text if changed
- **DELETE** (DELETE_ROWS events)
  - Removes document from index and store

### DDL Operations

MygramDB handles these DDL operations:

#### TRUNCATE TABLE

Automatically clears index and document store for the target table:

```sql
TRUNCATE TABLE articles;
```

MygramDB will:
- Clear all documents from the table
- Clear all posting lists
- Reset document ID counter

#### DROP TABLE

Clears all data and logs an error:

```sql
DROP TABLE articles;
```

MygramDB will:
- Clear all data
- Log error message
- **Require manual restart/reconfiguration**

#### ALTER TABLE

Logs a warning about potential schema inconsistency:

```sql
ALTER TABLE articles ADD COLUMN new_col VARCHAR(100);
ALTER TABLE articles MODIFY COLUMN content TEXT;
```

**Important notes:**
- Type changes (e.g., VARCHAR to TEXT) may cause replication issues
- Column additions/removals affecting `text_source` or `filters` require MygramDB restart
- **Recommendation**: Rebuild MygramDB snapshot after schema changes

## Supported Column Types

MygramDB can replicate these MySQL column types:

### Integer Types
- TINYINT, SMALLINT, INT, MEDIUMINT, BIGINT (signed/unsigned)

### String Types
- VARCHAR, CHAR, TEXT, BLOB, ENUM, SET

### Date/Time Types
- DATE, TIME, DATETIME, TIMESTAMP (with fractional seconds)

### Numeric Types
- DECIMAL, FLOAT, DOUBLE

### Special Types
- JSON, BIT, NULL

## Replication Features

### GTID Consistency

- Snapshot and binlog replication are coordinated via consistent snapshot transaction
- No data loss between snapshot and replication

### GTID Position Tracking

- Atomic persistence with state file
- Automatic save on shutdown
- Resume on restart

### Automatic Validation

- Checks GTID mode on startup
- Clear error messages if not configured

### Automatic Reconnection

- Handles connection loss gracefully
- Exponential backoff retry (configurable)
- Continues from last GTID position

### Multi-threaded Processing

- Thread pool architecture for efficient request handling
- Configurable queue size for performance tuning

## Monitoring Replication

### Check Replication Status

Use the CLI or TCP protocol:

```bash
# Using CLI
./build/bin/mygram-cli REPLICATION STATUS

# Using telnet
echo "REPLICATION STATUS" | nc localhost 11311
```

Response:
```
OK REPLICATION status=running gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100
```

### Stop Replication

Stop binlog replication (index becomes read-only):

```bash
./build/bin/mygram-cli REPLICATION STOP
```

### Start Replication

Resume binlog replication:

```bash
./build/bin/mygram-cli REPLICATION START
```

## Troubleshooting

### "GTID mode is not enabled on MySQL server"

**Solution**: Enable GTID mode on MySQL server:

```sql
SET GLOBAL gtid_mode = ON;
SET GLOBAL enforce_gtid_consistency = ON;
```

Then restart MygramDB.

### "Binary log format is not ROW"

**Solution**: Set binary log format to ROW:

```sql
SET GLOBAL binlog_format = ROW;
```

Or add to `my.cnf` and restart MySQL:

```ini
[mysqld]
binlog_format = ROW
```

### "Replication lag is high"

**Possible causes:**
- High write volume on MySQL
- Insufficient MygramDB resources
- Network latency

**Solutions:**
- Increase `replication.queue_size` in config
- Increase `build.parallelism` for faster processing
- Add more MygramDB replicas

### "Lost connection to MySQL server during query"

MygramDB will automatically reconnect with exponential backoff:

```yaml
replication:
  reconnect_backoff_min_ms: 500
  reconnect_backoff_max_ms: 10000
```

### "Schema mismatch after ALTER TABLE"

**Solution**: Rebuild snapshot after schema changes:

1. Stop MygramDB
2. Update config file to match new schema
3. Restart MygramDB (will rebuild snapshot)

## Best Practices

1. **Always use GTID mode** for consistent replication
2. **Use `snapshot` start mode** for initial setup
3. **Monitor replication lag** regularly
4. **Rebuild snapshot** after significant schema changes
5. **Test configuration** before deploying to production
6. **Keep state file** for crash recovery
7. **Use multiple replicas** for high availability

## Configuration Example

Complete replication configuration:

```yaml
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "repl_user"
  password: "your_password"
  database: "mydb"
  use_gtid: true
  binlog_format: "ROW"
  binlog_row_image: "FULL"

replication:
  enable: true
  server_id: 0                    # 0 = auto-generate
  start_from: "snapshot"          # snapshot|latest|gtid=<UUID:txn>|state_file
  state_file: "./mygramdb_replication.state"
  queue_size: 10000
  reconnect_backoff_min_ms: 500
  reconnect_backoff_max_ms: 10000
```

## See Also

- [Configuration Guide](configuration.md) - Full configuration reference
- [Protocol Reference](protocol.md) - REPLICATION commands
