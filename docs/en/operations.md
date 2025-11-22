# Runtime Variables & MySQL Failover - Operations Guide

**Date**: 2025-11-22

## Overview

This guide covers two critical operational resilience features:

1. **Runtime Variables (SET/SHOW VARIABLES)**: Change configuration at runtime without server restart using MySQL-compatible commands
2. **MySQL Failover**: Switch MySQL servers at runtime while preserving GTID position

These features enable zero-downtime configuration updates and safe handling of MySQL master failover scenarios.

## 1. Runtime Variables (SET/SHOW VARIABLES)

MygramDB supports MySQL-compatible SET and SHOW VARIABLES commands for runtime configuration changes without server restart.

### Overview

Runtime variables allow you to:
- Change logging levels on the fly
- Switch MySQL servers during failover (GTID position preserved)
- Adjust cache behavior
- Modify API and rate limiting parameters

### Basic Commands

```sql
-- Show all variables
SHOW VARIABLES;

-- Show specific pattern
SHOW VARIABLES LIKE 'mysql%';
SHOW VARIABLES LIKE 'cache%';

-- Set single variable
SET logging.level = 'debug';
SET mysql.host = '192.168.1.100';

-- Set multiple variables
SET api.default_limit = 200, cache.enabled = true;
```

### Mutable vs Immutable Variables

#### ✅ Mutable (Runtime Changeable)

| Variable | Description | Example |
|----------|-------------|---------|
| `logging.level` | Log level (debug/info/warn/error) | `SET logging.level = 'debug'` |
| `logging.format` | Log format (json/text) | `SET logging.format = 'json'` |
| `mysql.host` | MySQL server hostname | `SET mysql.host = '192.168.1.101'` |
| `mysql.port` | MySQL server port | `SET mysql.port = 3306` |
| `cache.enabled` | Enable/disable query cache | `SET cache.enabled = false` |
| `cache.min_query_cost_ms` | Minimum query cost to cache | `SET cache.min_query_cost_ms = 20.0` |
| `cache.ttl_seconds` | Cache TTL in seconds | `SET cache.ttl_seconds = 7200` |
| `api.default_limit` | Default LIMIT value | `SET api.default_limit = 200` |
| `api.max_query_length` | Max query expression length | `SET api.max_query_length = 256` |
| `rate_limiting.capacity` | Token bucket capacity | `SET rate_limiting.capacity = 200` |
| `rate_limiting.refill_rate` | Tokens per second | `SET rate_limiting.refill_rate = 20` |

#### ⚠️ Immutable (Requires Restart)

- Server port/bind address (`tcp.bind`, `tcp.port`, `http.bind`, `http.port`)
- Table configuration (name, primary key, text columns)
- MySQL database name, user, password
- Replication settings (`server_id`, `start_from`)
- Memory allocator settings
- Network security (`allow_cidrs`)

### Usage Examples

#### Change Logging Level

```sql
-- Increase verbosity for debugging
SET logging.level = 'debug';

-- Verify change
SHOW VARIABLES LIKE 'logging%';

-- Return to normal
SET logging.level = 'info';
```

#### Disable Cache During Maintenance

```sql
-- Disable cache before maintenance
SET cache.enabled = false;

-- Perform maintenance operations...

-- Re-enable cache
SET cache.enabled = true;
```

#### Adjust API Limits

```sql
-- Temporarily increase limits for bulk operations
SET api.default_limit = 1000;
SET api.max_query_length = 512;

-- Check current values
SHOW VARIABLES LIKE 'api%';
```

### Validation and Error Handling

SET commands validate values before applying:

```sql
-- Type mismatch error
SET api.default_limit = 'invalid';
ERROR: Invalid value for api.default_limit: must be integer

-- Out of range error
SET api.default_limit = 99999;
ERROR: Invalid value for api.default_limit: must be between 5 and 1000

-- Unknown variable error
SET unknown.variable = 'value';
ERROR: Unknown variable: unknown.variable

-- Immutable variable error
SET mysql.database = 'newdb';
ERROR: Variable mysql.database is immutable (requires restart)
```

Variables are validated **before** being applied, ensuring the server remains in a consistent state even if invalid values are provided.

### Monitoring Variable Changes

Monitor variable changes using server logs:

```bash
# Watch for variable changes
tail -f /var/log/mygramdb/server.log | grep -i "variable"

# Check current configuration
mygramclient -c "SHOW VARIABLES;"
```

## 2. MySQL Failover with Runtime Variables

MygramDB supports zero-downtime MySQL failover using SET commands. When switching to a new MySQL server, the GTID position is preserved.

### Failover Workflow

```sql
-- 1. Check current MySQL connection
SHOW VARIABLES LIKE 'mysql%';

-- 2. Switch to new MySQL server (replica promoted to primary)
SET mysql.host = '192.168.1.101', mysql.port = 3306;

-- 3. Verify reconnection succeeded
SHOW VARIABLES LIKE 'mysql%';
```

### How It Works

When you execute `SET mysql.host/port`:

1. **Save GTID Position**: Current GTID position is saved
2. **Stop Binlog Reader**: Gracefully stop reading from old server
3. **Close Old Connection**: Close connection to old MySQL server
4. **Create New Connection**: Connect to new MySQL server
5. **Validate New Server**: Check GTID mode, binlog format, table existence
6. **Resume from GTID**: Restart binlog reader from saved GTID position

### Requirements

The new MySQL server must:
- Have GTID mode enabled (`gtid_mode=ON`)
- Use ROW binlog format (`binlog_format=ROW`)
- Contain the saved GTID position in its GTID set
- Have all required tables defined in configuration

### Example: Planned Failover

```sql
-- Before failover: connected to mysql-primary-1
SHOW VARIABLES LIKE 'mysql.host';
-- Output: mysql-primary-1.example.com

-- Promote replica to new primary
-- (Done externally via MySQL replication management)

-- Switch MygramDB to new primary
SET mysql.host = 'mysql-primary-2.example.com';

-- Verify failover
SHOW VARIABLES LIKE 'mysql.host';
-- Output: mysql-primary-2.example.com
```

Server logs will show:

```log
[info] Reconnecting to MySQL: mysql-primary-2.example.com:3306
[info] Stopping binlog reader...
[info] Binlog reader stopped. Processed 12345 events
[info] Creating new MySQL connection: mysql-primary-2.example.com:3306
[info] Connection validated successfully
[info] Binlog reader started from GTID: <saved-gtid>
```

### Error Handling

If failover fails, the server stops replication and logs detailed errors:

```log
[error] Failed to reconnect to new MySQL server: Connection refused
[error] Binlog replication stopped. Manual intervention required.
```

In this case:
1. Check new MySQL server is accessible
2. Verify network connectivity
3. Check MySQL credentials
4. Retry SET command or restart server

## 3. MySQL Failover Detection

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
   - Use SET command to switch MySQL server
   - `SET mysql.host = 'new-primary.example.com';`
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
   ```sql
   -- If wrong server, use SET to switch to correct server
   SET mysql.host = 'correct-server.example.com';

   -- Verify connection
   SHOW VARIABLES LIKE 'mysql%';
   ```

   ```bash
   # If validation failed, check logs and restart if needed
   systemctl restart mygramdb
   ```

## 4. Health Monitoring Integration

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

## 5. Testing & Validation

### Testing Runtime Variables

```bash
# 1. Start server
./bin/mygramdb --config config.yaml

# 2. Connect with client
mygramclient

# 3. Test variable changes
SET logging.level = 'debug';

# 4. Verify change applied
SHOW VARIABLES LIKE 'logging%';

# 5. Check logs
tail -f /var/log/mygramdb/server.log
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

# 2. Connect with client
mygramclient

# 3. Switch to mysql-server-2
SET mysql.host = 'mysql-server-2.example.com';

# 4. Check logs for failover detection
tail -100 /var/log/mygramdb/server.log | grep -E "(failover|UUID changed|Reconnecting)"
```

## 6. Troubleshooting

### Issue: SET Variable Fails

**Symptoms**:
```sql
SET mysql.host = 'new-host';
ERROR: Failed to reconnect to MySQL server
```

**Solutions**:
1. Check MySQL server is accessible: `mysql -h new-host -u user -p`
2. Verify network connectivity: `ping new-host`
3. Check server logs for detailed error message
4. Verify GTID mode is enabled on new server
5. Check credentials are correct

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

## 7. Performance Considerations

### Runtime Variable Change Impact

- **Logging Level Change**: No impact, immediate
- **MySQL Failover (SET mysql.host)**: Brief interruption (1-5 seconds)
  - Binlog reader stops gracefully
  - Existing queries complete normally
  - Search operations continue normally
  - Replication resumes from saved GTID
- **Cache Toggle**: Immediate, no query interruption
- **API Parameter Changes**: Applied to new requests only

**Recommendation**: MySQL failover should be performed during low-traffic periods if possible

### Validation Overhead

- **Initial Connection**: ~100-200ms validation time
- **Reconnection**: ~50-100ms additional overhead
- **Impact**: Negligible (occurs only during connect/reconnect)

**Cost Breakdown**:
1. GTID mode check: 1 query (~10ms)
2. Server UUID check: 1 query (~10ms)
3. Table existence: N queries (~10ms each, N = table count)
4. GTID consistency: 1 query (~20ms)

## 8. Best Practices

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

1. **Client Access Control**: Use `network.allow_cidrs` to restrict client access
2. **Config File Permissions**: `chmod 600 config.yaml` (sensitive credentials)
3. **Log Rotation**: Prevent disk space exhaustion
4. **Audit Trail**: Log all variable changes and failovers

## 9. Implementation Details

### Source Files

**Runtime Variables**:
- `src/config/runtime_variable_manager.h` - Variable management interface
- `src/config/runtime_variable_manager.cpp` - SET/SHOW implementation
- `src/server/handlers/variable_handler.h` - Query handler
- `src/app/mysql_reconnection_handler.h` - MySQL failover logic

**ConnectionValidator**:
- `src/mysql/connection_validator.h` - Validation interface
- `src/mysql/connection_validator.cpp` - Validation implementation

**BinlogReader Integration**:
- `src/mysql/binlog_reader.h:187-188,255` - UUID tracking members
- `src/mysql/binlog_reader.cpp:196-201,315-322,386-393,452-459,749-799` - Validation calls

**Tests**:
- `tests/mysql/connection_validator_test.cpp` - Comprehensive test suite

### Runtime Variable Categories

Variables that can be changed at runtime using SET commands:

```yaml
logging:
  level: "info"      # Mutable via SET
  format: "json"     # Mutable via SET

mysql:
  host: "127.0.0.1"  # Mutable via SET (triggers reconnection)
  port: 3306         # Mutable via SET (triggers reconnection)

cache:
  enabled: true                # Mutable via SET
  min_query_cost_ms: 10.0      # Mutable via SET
  ttl_seconds: 3600            # Mutable via SET

api:
  default_limit: 100           # Mutable via SET
  max_query_length: 128        # Mutable via SET

rate_limiting:
  capacity: 100                # Mutable via SET
  refill_rate: 10              # Mutable via SET
```

## Appendix: Quick Reference

### Commands

```sql
-- Change logging level
SET logging.level = 'debug';

-- Switch MySQL server
SET mysql.host = 'new-primary.example.com';

-- Disable cache
SET cache.enabled = false;

-- Show all variables
SHOW VARIABLES;

-- Show specific variables
SHOW VARIABLES LIKE 'mysql%';
```

```bash
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
| `mysql_failover_detected` | WARNING | Server UUID changed during reconnection |
| `connection_validation_failed` | ERROR | Validation failed, replication stopped |
| Variable change logs | INFO | Runtime variable modified via SET |
| `Connection validated successfully` | INFO | Validation passed |

### Error Codes

| Error | Code | Action |
|-------|------|--------|
| GTID mode disabled | `gtid_disabled` | Enable GTID on MySQL |
| Missing tables | `table_validation_failed` | Check schema |
| Server UUID changed | `failover_detected` | Verify failover |
| Connection lost | `connection_failed` | Check network |

**Last Updated**: 2025-11-17
