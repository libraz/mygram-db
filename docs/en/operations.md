# Configuration Hot Reload & MySQL Failover Detection - Operations Guide

**Date**: 2025-11-17

## Overview

This guide covers two critical operational resilience features:

1. **Configuration Hot Reload (SIGHUP)**: Reload configuration without server restart
2. **MySQL Failover Detection**: Automatic validation and failover detection for MySQL replication

These features enable zero-downtime configuration updates and safe handling of MySQL master failover scenarios.

## 1. Configuration Hot Reload (SIGHUP)

The server can reload configuration at runtime by sending a `SIGHUP` signal. This eliminates the need to restart the server for configuration changes.

### Supported Configuration Changes

#### ✅ Automatically Applied

- **Logging Level**: Changes take effect immediately
- **MySQL Connection Settings**: If changed, the server will:
  - Stop the current BinlogReader
  - Close existing MySQL connections
  - Reconnect with new settings
  - Restart BinlogReader from the last GTID
  - Validate the new server

#### ⚠️ Requires Restart

The following configuration changes are **not** hot-reloadable and require a full server restart:

- Server port/bind address
- Table configuration (name, primary key, text columns)
- Index configuration (n-gram size, hybrid mode)
- Cache settings
- Worker thread pool size

### Usage

#### Send SIGHUP Signal

```bash
# Find the process ID
ps aux | grep mygramdb

# Send SIGHUP signal (replace <PID> with actual process ID)
kill -SIGHUP <PID>

# Or if running as daemon with PID file
kill -SIGHUP $(cat /var/run/mygramdb.pid)
```

#### Verify Reload

Check the server logs for confirmation:

```log
[info] Configuration reload requested (SIGHUP received)
[info] Logging level changed from 'info' to 'debug'
[info] MySQL connection settings changed, reconnecting...
[info] Stopping binlog reader...
[info] Binlog reader stopped. Processed 12345 events
[info] Creating new MySQL connection with updated settings...
[info] Binlog connection validated successfully
[info] Binlog reader started from GTID: <last-gtid>
[info] Configuration reload completed successfully
```

#### Error Handling

If configuration reload fails, the server continues with the **current configuration**:

```log
[error] Failed to reload configuration: Invalid YAML syntax at line 42
[warn] Continuing with current configuration
```

This graceful degradation ensures the server remains operational even if the new configuration is invalid.

### Example: Change MySQL Connection

```yaml
# config.yaml
mysql:
  host: "mysql-master-new.example.com"  # Changed from old host
  port: 3306
  user: "replicator"
  password: "new-password"  # Updated password
  database: "myapp"
```

```bash
# Apply changes without restart
kill -SIGHUP $(pidof mygramdb)
```

### Monitoring Configuration Reload

Monitor configuration reload events using structured logs:

```bash
# Filter for config reload events
tail -f /var/log/mygramdb/server.log | grep "Configuration reload"

# Check for failures
tail -f /var/log/mygramdb/server.log | grep -E "(reload.*failed|Continuing with current)"
```

## 2. MySQL Failover Detection

The server automatically validates MySQL connections to detect:

- **Failover**: MySQL master server changed (different server UUID)
- **Invalid Servers**: Missing tables, GTID mode disabled, inconsistent state
- **Connection Loss**: Automatic reconnection with validation

This prevents replicating from incorrect servers after network changes or failover events.

### Validation Checks

When connecting or reconnecting to MySQL, the server performs:

1. **GTID Mode Check**: Ensures `gtid_mode=ON`
2. **Server UUID Tracking**: Detects server changes (failover)
3. **Table Existence**: Verifies all required tables exist
4. **GTID Consistency**: Checks replication state consistency

### Failover Detection Behavior

#### Scenario 1: Normal Reconnection (Same Server)

```log
[info] [binlog worker] Reconnected successfully
[info] [binlog worker] Connection validated successfully after reconnect
```

- **Action**: Continue replication normally
- **Impact**: None

#### Scenario 2: Failover Detected (Different Server)

```log
[info] [binlog worker] Reconnected successfully
[warn] [binlog validation] Server UUID changed: a1b2c3d4-... -> e5f6g7h8-... (failover detected)
{"event":"mysql_failover_detected","old_uuid":"a1b2c3d4-...","new_uuid":"e5f6g7h8-..."}
[info] [binlog worker] Connection validated successfully after reconnect
```

- **Action**: Continue replication with **warning logged**
- **Impact**: Operator alerted to failover event
- **Next Steps**:
  - Verify failover was intentional
  - Check replication lag
  - Monitor for data consistency

#### Scenario 3: Invalid Server (Missing Tables)

```log
[info] [binlog worker] Reconnected successfully
{"event":"binlog_connection_validation_failed","gtid":"<current-gtid>","error":"Required tables are missing: users, messages"}
[error] [binlog worker] Connection validation failed after reconnect: Required tables are missing: users, messages
[info] Binlog reader stopped. Processed 12345 events
```

- **Action**: **Stop replication** (prevents data corruption)
- **Impact**: Replication halted until operator intervenes
- **Next Steps**:
  1. Verify MySQL server is correct
  2. Check table schema
  3. Fix configuration or restore correct server
  4. Restart mygramdb

#### Scenario 4: GTID Mode Disabled

```log
{"event":"connection_validation_failed","reason":"gtid_disabled","error":"GTID mode is not enabled"}
[error] [binlog worker] Connection validation failed after reconnect: GTID mode is not enabled
```

- **Action**: **Stop replication**
- **Impact**: Server will not start binlog reader
- **Next Steps**: Enable GTID mode on MySQL server

### Monitoring Failover Events

#### Structured Log Queries

```bash
# Detect failover events
grep 'mysql_failover_detected' /var/log/mygramdb/server.log

# Check validation failures
grep 'connection_validation_failed' /var/log/mygramdb/server.log

# Monitor server UUID changes
grep 'Server UUID changed' /var/log/mygramdb/server.log
```

#### Example Structured Log Output

```json
{
  "event": "mysql_failover_detected",
  "old_uuid": "a1b2c3d4-e5f6-1234-5678-90abcdef1234",
  "new_uuid": "e5f6g7h8-i9j0-5678-9012-34567890abcd",
  "timestamp": "2025-11-17T10:30:45Z"
}
```

### Operational Procedures

#### Planned MySQL Failover

1. **Before Failover**:
   - Ensure new master has GTID enabled
   - Verify all required tables exist on new master
   - Check replication lag

2. **During Failover**:
   - Update DNS/IP for MySQL connection
   - Send SIGHUP to mygramdb (hot reload config)
   - Monitor logs for failover detection

3. **After Failover**:
   ```bash
   # Verify connection to new master
   grep "Connection validated successfully" /var/log/mygramdb/server.log | tail -1

   # Check for failover warning
   grep "mysql_failover_detected" /var/log/mygramdb/server.log | tail -1

   # Verify replication continues
   # Use mygramdb health endpoint
   curl http://localhost:8080/health/detail
   ```

#### Unplanned Failover (Emergency)

1. **Detect Issue**:
   ```bash
   # Check if replication stopped
   grep "Binlog reader stopped" /var/log/mygramdb/server.log | tail -1

   # Check validation failures
   grep "validation_failed" /var/log/mygramdb/server.log | tail -5
   ```

2. **Diagnose**:
   - Is server UUID different? → Failover occurred
   - Missing tables? → Wrong server or schema issue
   - GTID disabled? → Configuration problem

3. **Recover**:
   ```bash
   # If wrong server, update config and reload
   vim /etc/mygramdb/config.yaml  # Fix MySQL host/port
   kill -SIGHUP $(pidof mygramdb)

   # If correct server but validation failed, restart mygramdb
   systemctl restart mygramdb
   ```

## 3. Health Monitoring Integration

### Health Check Endpoints

Use the health endpoint to monitor replication status:

```bash
# Basic health check
curl http://localhost:8080/health/live
# Returns: 200 OK (server is running)

# Readiness check
curl http://localhost:8080/health/ready
# Returns: 200 if ready, 503 if loading

# Detailed health status
curl http://localhost:8080/health/detail
```

**Detailed Health Response Example**:

```json
{
  "status": "healthy",
  "uptime_seconds": 3600,
  "binlog_reader": {
    "running": true,
    "current_gtid": "a1b2c3d4-e5f6-1234-5678-90abcdef1234:1-12345",
    "processed_events": 12345,
    "queue_size": 42
  },
  "mysql_connection": {
    "connected": true,
    "server_uuid": "a1b2c3d4-e5f6-1234-5678-90abcdef1234"
  }
}
```

### Alerting Rules

Recommended monitoring alerts:

1. **Failover Detected** (INFO)
   - Trigger: `event:mysql_failover_detected`
   - Action: Notify operator, verify failover was intentional

2. **Validation Failed** (CRITICAL)
   - Trigger: `event:connection_validation_failed`
   - Action: Page on-call, replication stopped

3. **Replication Stopped** (CRITICAL)
   - Trigger: `binlog_reader.running = false` in health endpoint
   - Action: Page on-call, investigate immediately

4. **Config Reload Failed** (WARNING)
   - Trigger: `Failed to reload configuration`
   - Action: Check config syntax, notify operator

## 4. Testing & Validation

### Testing SIGHUP Reload

```bash
# 1. Start server
./bin/mygramdb --config config.yaml

# 2. Change configuration
vim config.yaml  # Modify logging level

# 3. Send SIGHUP
kill -SIGHUP $(pidof mygramdb)

# 4. Verify logs
tail -f /var/log/mygramdb/server.log
# Expected: "Configuration reload completed successfully"
```

### Testing Failover Detection

**Unit Tests** (No MySQL Required):

```bash
# Run validator unit tests
cd build
./bin/connection_validator_test --gtest_filter="ConnectionValidatorUnitTest.*"
```

**Integration Tests** (Requires MySQL with GTID):

```bash
# Set environment
export ENABLE_MYSQL_INTEGRATION_TESTS=1
export MYSQL_HOST=127.0.0.1
export MYSQL_USER=root
export MYSQL_PASSWORD=test
export MYSQL_DATABASE=test

# Run integration tests
ctest -R ConnectionValidatorIntegrationTest -V
```

### Manual Failover Simulation

```bash
# 1. Start mygramdb connected to mysql-server-1
./bin/mygramdb --config config.yaml

# 2. Change config to point to mysql-server-2
vim config.yaml
# Change: host: "mysql-server-2.example.com"

# 3. Trigger reload
kill -SIGHUP $(pidof mygramdb)

# 4. Check logs for failover detection
tail -100 /var/log/mygramdb/server.log | grep -E "(failover|UUID changed)"
```

## 5. Troubleshooting

### Issue: Config Reload Fails

**Symptoms**:
```log
[error] Failed to reload configuration: <error message>
[warn] Continuing with current configuration
```

**Solutions**:
1. Check YAML syntax: `yamllint config.yaml`
2. Verify file permissions: `ls -la config.yaml`
3. Check JSON schema validation errors in log
4. Test config manually: `./bin/mygramdb --config config.yaml --validate-config`

### Issue: Replication Stopped After Reconnect

**Symptoms**:
```log
[error] Connection validation failed: Required tables are missing
[info] Binlog reader stopped
```

**Solutions**:
1. Verify MySQL server is correct: Check `mysql.host` in config
2. Check tables exist:
   ```sql
   SHOW TABLES FROM myapp;
   ```
3. Verify GTID mode:
   ```sql
   SHOW VARIABLES LIKE 'gtid_mode';
   ```
4. Check server UUID:
   ```sql
   SELECT @@server_uuid;
   ```

### Issue: Failover Warning But Replication Works

**Symptoms**:
```log
[warn] Server UUID changed: <old> -> <new> (failover detected)
[info] Connection validated successfully
```

**This is normal** if:
- Planned MySQL failover occurred
- DNS/IP switched to new master
- All validation checks pass

**Action Required**:
- Document the failover event
- Verify replication lag
- Monitor for consistency issues

### Issue: Repeated Reconnection Attempts

**Symptoms**:
```log
[info] Reconnect attempt #5, waiting 5000 ms
[error] [binlog worker] Connection validation failed: <error>
[info] Reconnect attempt #6, waiting 6000 ms
```

**Solutions**:
1. Check MySQL server availability
2. Verify network connectivity
3. Check credentials in config
4. Review firewall rules
5. Consider increasing `mysql.connect_timeout_ms` in config

## 6. Performance Considerations

### SIGHUP Reload Impact

- **Logging Level Change**: No impact, immediate
- **MySQL Reconnection**: Brief interruption (1-5 seconds)
  - Binlog reader stops gracefully
  - Existing queries complete normally
  - Search/insert operations continue (uses existing connection)
  - Replication resumes from last GTID

**Recommendation**: Perform during low-traffic periods if possible

### Validation Overhead

- **Initial Connection**: ~100-200ms validation time
- **Reconnection**: ~50-100ms additional overhead
- **Impact**: Negligible (occurs only during connect/reconnect)

**Cost Breakdown**:
1. GTID mode check: 1 query (~10ms)
2. Server UUID check: 1 query (~10ms)
3. Table existence: N queries (~10ms each, N = table count)
4. GTID consistency: 1 query (~20ms)

## 7. Best Practices

### Configuration Management

1. **Version Control**: Keep `config.yaml` in git
2. **Validation**: Test config changes in staging first
3. **Rollback Plan**: Keep previous config backup
4. **Documentation**: Document all config changes

### Monitoring

1. **Log Aggregation**: Send logs to centralized logging (ELK, Splunk)
2. **Structured Logs**: Use JSON parsing for alerts
3. **Health Checks**: Monitor `/health/detail` endpoint every 30s
4. **Metrics**: Track GTID lag, event processing rate

### Failover Procedures

1. **Document MySQL Topology**: Keep diagram of master/replica setup
2. **Automate DNS/IP Updates**: Use orchestration tools
3. **Test Failover Regularly**: Quarterly failover drills
4. **Monitor Lag**: Alert if replication lag > 60s

### Security

1. **SIGHUP Access**: Only allow trusted users to send signals
2. **Config File Permissions**: `chmod 600 config.yaml` (sensitive credentials)
3. **Log Rotation**: Prevent disk space exhaustion
4. **Audit Trail**: Log all config changes and failovers

## 8. Implementation Details

### Source Files

**SIGHUP Handler**:
- `src/main.cpp:36-618` - Signal handler and config reload logic

**ConnectionValidator**:
- `src/mysql/connection_validator.h` - Validation interface
- `src/mysql/connection_validator.cpp` - Validation implementation

**BinlogReader Integration**:
- `src/mysql/binlog_reader.h:187-188,255` - UUID tracking members
- `src/mysql/binlog_reader.cpp:196-201,315-322,386-393,452-459,749-799` - Validation calls

**Tests**:
- `tests/mysql/connection_validator_test.cpp` - Comprehensive test suite

### Configuration Schema

Relevant configuration fields for hot reload and failover detection:

```yaml
logging:
  level: "info"  # Hot-reloadable via SIGHUP

mysql:
  host: "127.0.0.1"           # Hot-reloadable
  port: 3306                   # Hot-reloadable
  user: "replicator"           # Hot-reloadable
  password: "password"         # Hot-reloadable
  database: "myapp"            # Hot-reloadable
  connect_timeout_ms: 10000    # Hot-reloadable
  read_timeout_ms: 30000       # Hot-reloadable
  write_timeout_ms: 30000      # Hot-reloadable
```

## Appendix: Quick Reference

### Commands

```bash
# Reload configuration
kill -SIGHUP $(pidof mygramdb)

# Check process status
ps aux | grep mygramdb

# Monitor logs
tail -f /var/log/mygramdb/server.log

# Health check
curl http://localhost:8080/health/detail

# Run validator tests
./bin/connection_validator_test
```

### Log Events

| Event | Severity | Meaning |
|-------|----------|---------|
| `Configuration reload requested` | INFO | SIGHUP received |
| `Configuration reload completed successfully` | INFO | Config applied |
| `Failed to reload configuration` | ERROR | Config invalid, using old config |
| `mysql_failover_detected` | WARNING | Server UUID changed |
| `connection_validation_failed` | ERROR | Validation failed, replication stopped |
| `Connection validated successfully` | INFO | Validation passed |

### Error Codes

| Error | Code | Action |
|-------|------|--------|
| GTID mode disabled | `gtid_disabled` | Enable GTID on MySQL |
| Missing tables | `table_validation_failed` | Check schema |
| Server UUID changed | `failover_detected` | Verify failover |
| Connection lost | `connection_failed` | Check network |

**Last Updated**: 2025-11-17
