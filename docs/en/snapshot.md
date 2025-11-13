# Snapshot Commands

MygramDB provides snapshot commands for backing up and restoring your database state. Snapshots capture the entire database including all indexed data, configuration, and replication position (GTID).

## Overview

A snapshot is a single binary file (`.dmp`) that contains:
- Complete database state (all tables, documents, and indexes)
- Configuration settings
- Replication position (GTID) for seamless recovery
- Integrity checksums (CRC32) for corruption detection

## Commands

### DUMP SAVE - Create Snapshot

Save the current database state to a snapshot file.

**Syntax:**
```
DUMP SAVE <filepath> [WITH STATISTICS]
```

**Parameters:**
- `<filepath>`: Path where the snapshot file will be saved
- `WITH STATISTICS` (optional): Include performance statistics in the snapshot

**Examples:**
```sql
-- Basic snapshot
DUMP SAVE /var/lib/mygramdb/dumps/mygramdb.dmp

-- Snapshot with statistics
DUMP SAVE /var/lib/mygramdb/dumps/mygramdb.dmp WITH STATISTICS
```

**Response:**
```
OK SAVE /var/lib/mygramdb/dumps/mygramdb.dmp
tables: 2
size: 1234567 bytes
gtid: 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10
```

### DUMP LOAD - Restore Snapshot

Load a snapshot file and restore the database state.

**Syntax:**
```
DUMP LOAD <filepath>
```

**Parameters:**
- `<filepath>`: Path to the snapshot file to load

**Examples:**
```sql
DUMP LOAD /var/lib/mygramdb/dumps/mygramdb.dmp
```

**Response:**
```
OK LOAD /var/lib/mygramdb/dumps/mygramdb.dmp
tables: 2
documents: 10000
gtid: 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10
```

**Important Notes:**
- Loading a snapshot **replaces** all current data
- The database will resume replication from the GTID stored in the snapshot
- All tables must be configured in the current config file

### DUMP VERIFY - Check Integrity

Verify the integrity of a snapshot file without loading it.

**Syntax:**
```
DUMP VERIFY <filepath>
```

**Parameters:**
- `<filepath>`: Path to the snapshot file to verify

**Examples:**
```sql
DUMP VERIFY /var/lib/mygramdb/dumps/mygramdb.dmp
```

**Response (Success):**
```
OK VERIFY /var/lib/mygramdb/dumps/mygramdb.dmp
status: valid
crc: verified
size: verified
```

**Response (Failure):**
```
ERROR CRC mismatch: file may be corrupted
expected: 0x12345678
actual: 0x87654321
```

### DUMP INFO - Show Snapshot Metadata

Display metadata about a snapshot file without loading it.

**Syntax:**
```
DUMP INFO <filepath>
```

**Parameters:**
- `<filepath>`: Path to the snapshot file

**Examples:**
```sql
DUMP INFO /var/lib/mygramdb/dumps/mygramdb.dmp
```

**Response:**
```
OK INFO /var/lib/mygramdb/dumps/mygramdb.dmp
version: 1
gtid: 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10
tables: 2
flags: 0x18
  statistics: yes
size: 1234567 bytes
timestamp: 1672531200 (2023-01-01 00:00:00 UTC)
```

## Integrity Protection

### CRC32 Checksums

All snapshot files include CRC32 checksums at multiple levels:
- **File-level CRC**: Detects any corruption in the entire file
- **Section-level CRC**: Validates individual sections (config, statistics, index, document store)

### File Size Validation

The snapshot header includes the expected file size:
- Detects incomplete writes
- Catches network transfer failures
- Identifies truncated files

### Verification Workflow

Always verify snapshot integrity before loading:

```sql
-- 1. Verify integrity
DUMP VERIFY mygramdb.dmp

-- 2. Check metadata
DUMP INFO mygramdb.dmp

-- 3. Load if everything looks good
DUMP LOAD mygramdb.dmp
```

## Error Handling

### CRC Mismatch

If CRC verification fails, the snapshot file is corrupted:

```
ERROR CRC32 mismatch: expected 0x12345678, got 0x87654321 (file may be corrupted)
```

**Actions:**
1. Do not load the corrupted snapshot
2. Try restoring from a backup copy
3. Check disk health if corruption occurs frequently

### File Truncation

If the file size doesn't match the expected size:

```
ERROR File size mismatch: expected 1234567 bytes, got 1000000 bytes (file may be truncated or corrupted)
```

**Actions:**
1. The file was not completely written
2. Retry the SAVE operation
3. Check available disk space

### Version Mismatch

If the snapshot version is too new:

```
ERROR Snapshot file version 2 is newer than supported version 1
```

**Actions:**
1. Upgrade MygramDB to a newer version
2. Or use a snapshot created with a compatible version

## Best Practices

### Automatic Snapshots

MygramDB supports automatic periodic snapshots for continuous backup:

```yaml
# config.yaml
dump:
  dir: /var/lib/mygramdb/dumps    # Directory for dump files
  interval_sec: 600                 # Auto-save every 10 minutes (0 = disabled)
  retain: 3                         # Keep last 3 auto-saved dumps
```

**Features:**
- Automatic snapshots are saved with timestamp-based filenames (`auto_YYYYMMDD_HHMMSS.dmp`)
- Old auto-saved files are automatically cleaned up based on `retain` count
- Manual snapshots (via `DUMP SAVE`) are not affected by auto-cleanup
- Directory permissions are verified on startup

### Manual Backups

You can also schedule manual snapshots for disaster recovery:

```bash
# Example: Daily backup script
#!/bin/bash
DATE=$(date +%Y%m%d)
echo "DUMP SAVE /var/lib/mygramdb/dumps/manual_${DATE}.dmp WITH STATISTICS" | mygram-cli
```

### Retention Policy

Keep multiple snapshot versions:

```yaml
# config.yaml
dump:
  dir: /var/lib/mygramdb/dumps
  default_filename: mygramdb.dmp
  interval_sec: 600      # Save every 10 minutes
  retain: 3              # Keep last 3 snapshots
```

### Verify Before Loading

Always verify integrity before loading:

```bash
# Verify first
echo "DUMP VERIFY mygramdb.dmp" | mygram-cli

# Then load
echo "DUMP LOAD mygramdb.dmp" | mygram-cli
```

### Secure Storage

Snapshots contain sensitive data:
- MySQL connection credentials
- All document content
- Replication position

**Recommendations:**
1. Set file permissions to 600 (owner read/write only)
2. Store snapshots on encrypted volumes
3. Use secure transfer protocols (SFTP, SCP) for remote storage
4. Audit access logs

### Monitor Snapshot Size

Track snapshot file sizes over time:

```bash
# Check snapshot metadata
echo "DUMP INFO mygramdb.dmp" | mygram-cli
```

If snapshot size grows unexpectedly:
- Check for index bloat
- Review document retention policies
- Consider data archival

## Performance Characteristics

### SAVE Performance

| Documents | Avg Text Length | Save Time | File Size |
|-----------|----------------|-----------|-----------|
| 100K | 100 bytes | ~1-2 sec | ~20 MB |
| 1M | 100 bytes | ~10-15 sec | ~200 MB |
| 10M | 100 bytes | ~2-3 min | ~2 GB |

### LOAD Performance

| Documents | Avg Text Length | Load Time |
|-----------|----------------|-----------|
| 100K | 100 bytes | ~2-3 sec |
| 1M | 100 bytes | ~15-20 sec |
| 10M | 100 bytes | ~3-5 min |

*Performance varies based on hardware, disk I/O, and data characteristics*

## Configuration

Dump behavior can be configured in `config.yaml`:

```yaml
dump:
  # Directory for dump files
  dir: /var/lib/mygramdb/dumps

  # Default filename when not specified (for manual DUMP SAVE commands)
  default_filename: mygramdb.dmp

  # Automatic dump interval in seconds (0 = disabled)
  interval_sec: 600  # Auto-save every 10 minutes

  # Number of auto-saved dumps to retain (manual dumps are not affected)
  retain: 3  # Keep last 3 auto-saved dumps
```

**Startup Checks:**
- MygramDB verifies dump directory permissions on startup
- If the directory doesn't exist, it will be created automatically
- If the directory is not writable, the server will fail to start with an error

## Replication Recovery

When loading a snapshot, MygramDB automatically resumes replication:

1. **GTID Extraction**: Reads replication position from snapshot
2. **BinlogReader Resume**: Continues from the stored GTID
3. **Catch-up**: Processes any missed transactions
4. **Normal Operation**: Returns to real-time replication

**Example:**

```
# 1. Server crashes at GTID 1-100
# 2. Load snapshot from GTID 1-80
DUMP LOAD mygramdb.dmp

# 3. MygramDB automatically processes transactions 81-100
# 4. Resumes real-time replication from 101+
```

## Troubleshooting

### "Failed to open snapshot file"

**Cause:** File doesn't exist or insufficient permissions

**Solution:**
```bash
# Check file exists
ls -l /path/to/snapshot.dmp

# Check permissions
chmod 600 /path/to/snapshot.dmp
```

### "Invalid snapshot version"

**Cause:** Snapshot created with incompatible MygramDB version

**Solution:**
- Check MygramDB version compatibility
- Use snapshots created with the same major version

### "Table not found in config"

**Cause:** Snapshot contains tables not defined in current config

**Solution:**
- Update `config.yaml` to include all tables from the snapshot
- Or create a new snapshot with the current configuration

### "CRC mismatch during load"

**Cause:** Snapshot file is corrupted

**Solution:**
1. Run DUMP VERIFY to identify corrupted section
2. Restore from backup copy
3. Check disk health and network reliability

## Related Commands

- **SEARCH**: Query indexed documents
- **STATUS**: Check database status and replication position
- **CONFIG RELOAD**: Reload configuration without restart

## See Also

- [Configuration Guide](configuration.md)
- [Replication Setup](replication.md)
- [Performance Tuning](performance.md)
