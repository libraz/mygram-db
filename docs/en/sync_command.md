# SYNC Command Usage

## Overview

The `SYNC` command allows manual control over snapshot synchronization from MySQL to MygramDB. This prevents unexpected load on the MySQL master server during startup.

## Configuration

### Default Behavior (Safe by Default)

Starting from version 1.X.X, MygramDB **no longer automatically builds snapshots on startup by default**:

```yaml
replication:
  enable: true
  auto_initial_snapshot: false  # Default: false (safe by default)
  server_id: 12345
  start_from: "snapshot"
```

### Legacy Behavior (Auto Snapshot on Startup)

To restore the previous automatic snapshot behavior:

```yaml
replication:
  enable: true
  auto_initial_snapshot: true   # Restore legacy behavior
  server_id: 12345
  start_from: "snapshot"
```

## Commands

### SYNC - Trigger Snapshot Synchronization

Manually trigger snapshot synchronization for a specific table.

**Syntax:**
```
SYNC <table_name>
```

**Example:**
```bash
# Using CLI
mygram-cli SYNC articles

# Using telnet/nc
echo "SYNC articles" | nc localhost 11016
```

**Response (Success):**
```
OK SYNC STARTED table=articles job_id=1
```

**Response (Error):**
```
ERROR SYNC already in progress for table 'articles'
ERROR Memory critically low. Cannot start SYNC. Check system memory.
ERROR Table 'products' not found in configuration
```

**Behavior:**
- Runs asynchronously in the background
- Returns immediately after starting
- Builds snapshot from MySQL SELECT query
- Captures GTID at snapshot time
- Automatically starts binlog replication with captured GTID when complete

### SYNC STATUS - Check Synchronization Progress

Check the progress and status of SYNC operations.

**Syntax:**
```
SYNC STATUS
```

**Example:**
```bash
# Using CLI
mygram-cli SYNC STATUS

# Using telnet/nc
echo "SYNC STATUS" | nc localhost 11016
```

**Response Examples:**

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

**Idle (No sync performed):**
```
status=IDLE message="No sync operation performed"
```

### Status Fields

| Field | Description | Example |
|-------|-------------|---------|
| `table` | Table name being synced | `articles` |
| `status` | Current status | `IN_PROGRESS`, `COMPLETED`, `FAILED`, `IDLE`, `CANCELLED` |
| `progress` | Current/total rows processed | `10000/25000 rows (40.0%)` |
| `rate` | Processing rate | `5000 rows/s` |
| `rows` | Total rows processed | `25000` |
| `time` | Total processing time | `5.2s` |
| `gtid` | Captured snapshot GTID | `uuid:123` |
| `replication` | Replication status | `STARTED`, `ALREADY_RUNNING`, `DISABLED`, `FAILED` |
| `error` | Error message (if failed) | `MySQL connection lost` |

### Replication Status Values

- **STARTED**: Binlog replication started from snapshot GTID (success)
- **ALREADY_RUNNING**: Replication was already running (GTID not updated)
- **DISABLED**: Replication is disabled in configuration
- **FAILED**: Snapshot succeeded but replication failed to start (check logs)

## Command Conflicts

### Operations Blocked During SYNC

| Command | Behavior | Reason |
|---------|----------|--------|
| `DUMP LOAD` | ❌ **Blocked** | Cannot load dump while SYNC is in progress (prevents data corruption) |
| `REPLICATION START` | ❌ **Blocked** | SYNC automatically starts replication when complete |
| `SYNC` (same table) | ❌ **Blocked** | SYNC already in progress for this table |

### Operations Allowed During SYNC

| Command | Behavior | Notes |
|---------|----------|-------|
| `SEARCH` | ✅ Allowed | Returns partial results (data loaded so far) |
| `COUNT` | ✅ Allowed | Returns current count (increases as sync progresses) |
| `GET` | ✅ Allowed | Returns document if loaded, error if not yet loaded |
| `INFO` | ✅ Allowed | Shows current server state including sync progress |
| `DUMP SAVE` | ⚠️ **Warning** | Allowed but logs warning (saving incomplete snapshot) |
| `SYNC` (different table) | ✅ Allowed | Multi-table support, independent operations |
| `REPLICATION STOP` | ✅ Allowed | Independent operation |

## Usage Scenarios

### Scenario 1: New Server Startup

```bash
# 1. Start MygramDB (auto_initial_snapshot=false)
./mygramdb --config config.yaml

# 2. Server starts without building snapshot (no MySQL load)
# Log: "Skipping automatic snapshot build for table: articles (auto_initial_snapshot=false)"

# 3. When ready, manually trigger SYNC
mygram-cli SYNC articles

# 4. Monitor progress
mygram-cli SYNC STATUS
# Output: table=articles status=IN_PROGRESS progress=5000/10000 rows (50.0%) rate=2000 rows/s

# 5. Wait for completion
mygram-cli SYNC STATUS
# Output: table=articles status=COMPLETED rows=10000 time=5.0s gtid=uuid:456 replication=STARTED
```

### Scenario 2: Multi-Table Sync

```yaml
# config.yaml
tables:
  - name: "articles"
    # ...
  - name: "products"
    # ...
  - name: "users"
    # ...

replication:
  enable: true
  auto_initial_snapshot: false
```

```bash
# Sync all tables independently
mygram-cli SYNC articles
mygram-cli SYNC products
mygram-cli SYNC users

# Check all sync statuses
mygram-cli SYNC STATUS
# Output:
# table=articles status=COMPLETED rows=10000 time=5.0s gtid=uuid:123 replication=STARTED
# table=products status=IN_PROGRESS progress=5000/20000 rows (25.0%) rate=3000 rows/s
# table=users status=COMPLETED rows=5000 time=2.5s gtid=uuid:123 replication=ALREADY_RUNNING
```

### Scenario 3: Scheduled Maintenance

```bash
# Schedule SYNC during off-peak hours (e.g., 2 AM)
# Add to cron:
0 2 * * * echo "SYNC articles" | nc localhost 11016
```

## Graceful Shutdown

When MygramDB receives a shutdown signal (SIGTERM/SIGINT) during SYNC:

1. **Active SYNC operations are cancelled**
   - `SnapshotBuilder::Cancel()` is called
   - Status changes to `CANCELLED`

2. **Server waits for cleanup**
   - Maximum wait time: 30 seconds
   - Background threads terminate gracefully

3. **Server shuts down**
   - MySQL connections closed
   - Resources released

**Example Log:**
```
INFO: Stopping TCP server...
INFO: Cancelling SYNC for table: articles
INFO: SYNC cancelled for table articles due to shutdown
WARN: Timeout waiting for SYNC operations to complete
INFO: TCP server stopped
```

## Best Practices

1. **Use `auto_initial_snapshot: false` in production**
   - Prevents unexpected MySQL master load on startup
   - Allows operators to control when sync occurs

2. **Monitor SYNC progress**
   - Use `SYNC STATUS` to track progress
   - Check server logs for detailed information

3. **Avoid concurrent DUMP LOAD**
   - Wait for SYNC to complete before loading dumps
   - Check `SYNC STATUS` first

4. **Schedule SYNC during off-peak hours**
   - Use cron or other schedulers
   - Reduce impact on MySQL master

5. **Monitor memory before SYNC**
   - Check available system memory
   - SYNC checks memory health automatically

## Troubleshooting

### SYNC Fails to Start

**Error:** `Memory critically low. Cannot start SYNC. Check system memory.`

**Solution:**
- Check available system memory
- Increase memory limits in configuration
- Reduce batch size in build configuration

### SYNC Fails During Execution

**Error:** `status=FAILED error="MySQL connection lost"`

**Solution:**
- Check MySQL server connectivity
- Verify MySQL credentials
- Check MySQL server logs
- Ensure replication user has sufficient privileges

### Replication Fails to Start

**Status:** `replication=FAILED`

**Solution:**
- Check server logs for detailed error message
- Verify MySQL GTID is enabled: `SHOW VARIABLES LIKE 'gtid_mode';`
- Verify binlog format is ROW: `SHOW VARIABLES LIKE 'binlog_format';`
- Check replication user privileges: `SHOW GRANTS FOR 'repl_user'@'%';`

## See Also

- [Replication Guide](replication.md)
- [Configuration Reference](configuration.md)
- [Performance Tuning](performance.md)
