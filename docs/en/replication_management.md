# Replication Management

This document describes how MygramDB manages MySQL replication across different operations.

## Overview

MygramDB automatically manages replication start/stop to ensure data consistency during critical operations. Manual replication control is blocked when automatic management is in progress.

## Automatic Replication Management

### DUMP SAVE Operations

When executing `DUMP SAVE`:

1. **Pre-operation**: If replication is running, it is automatically stopped
   - Sets `replication_paused_for_dump` flag to `true`
   - Logs: `"Stopped replication before DUMP SAVE (will auto-restart after completion)"`

2. **During operation**: Dump file is created with consistent snapshot

3. **Post-operation**: Replication is automatically restarted (regardless of save success/failure)
   - Clears `replication_paused_for_dump` flag
   - Logs: `"Auto-restarted replication after DUMP SAVE"` (on success)
   - Logs: `"replication_restart_failed"` event (on failure, but DUMP SAVE still succeeds)

**Rationale**: DUMP SAVE creates a consistent snapshot. Stopping replication ensures no binlog events are processed during the snapshot creation, maintaining GTID consistency with the dumped data.

### DUMP LOAD Operations

When executing `DUMP LOAD`:

1. **Pre-operation**: If replication is running, it is automatically stopped
   - Sets `replication_paused_for_dump` flag to `true`
   - Logs: `"Stopped replication before DUMP LOAD (will auto-restart after completion)"`

2. **During operation**: All data is cleared and reloaded from dump file

3. **Post-operation**: Replication is automatically restarted with updated GTID
   - If load succeeded and dump contains GTID: Updates replication to loaded GTID position
   - Clears `replication_paused_for_dump` flag
   - Logs: `"Auto-restarted replication after DUMP LOAD"` with new GTID (on success)

**Rationale**: DUMP LOAD replaces all data. Running replication during load would cause:
- Data corruption (applying binlog events to incomplete data)
- GTID inconsistency (current position doesn't match loaded data)

### SYNC Operations

When executing `SYNC` for a table:

1. **Pre-operation**: SYNC operation checks if replication is running
   - If running with different GTID, replication is stopped
   - Performs full table synchronization

2. **Post-operation**: Replication is automatically started after SYNC completes
   - Managed by `SyncOperationManager`

**Rationale**: SYNC performs full table scan and rebuild. Replication must be stopped to avoid applying incremental changes while full sync is in progress.

### SET VARIABLES mysql.host / mysql.port

When changing MySQL connection settings:

1. **Pre-operation**: Replication is automatically stopped
   - Sets `mysql_reconnecting` flag to `true`
   - Current GTID position is saved
   - Old MySQL connection is closed
   - Logs: `"Stopping replication before MySQL reconnection"`

2. **Reconnection**: New MySQL connection is established
   - Connection is validated (GTID mode, binlog format)
   - GTID position is restored

3. **Post-operation**: Replication is automatically restarted from saved GTID
   - Clears `mysql_reconnecting` flag (on success or failure)
   - Logs: `"Restarted replication after MySQL reconnection"` (on success)

**Rationale**: Changing MySQL server connection requires stopping replication, reconnecting, and resuming from the same GTID position to maintain consistency.

**Implementation**: Handled by `MysqlReconnectionHandler::Reconnect()`
**Location**: `src/app/mysql_reconnection_handler.cpp:32-133`

## Manual Replication Control Blocking

### REPLICATION START Blocking

Manual `REPLICATION START` is blocked in the following scenarios:

| Scenario | Flag Checked | Error Message |
|----------|--------------|---------------|
| MySQL reconnection in progress | `mysql_reconnecting` | "Cannot start replication while MySQL reconnection is in progress. Replication will automatically restart after reconnection completes." |
| DUMP SAVE/LOAD in progress | `replication_paused_for_dump` | "Cannot start replication while DUMP SAVE/LOAD is in progress. Replication will automatically restart after DUMP completes." |
| DUMP SAVE in progress | `read_only` | "Cannot start replication while DUMP SAVE is in progress. Please wait for save to complete." |
| DUMP LOAD in progress | `loading` | "Cannot start replication while DUMP LOAD is in progress. Please wait for load to complete." |
| SYNC in progress | `sync_manager->IsAnySyncing()` | "Cannot start replication while SYNC is in progress. SYNC will automatically start replication when complete." |

**Implementation**: `src/server/handlers/replication_handler.cpp:45-78`

### SET VARIABLES Blocking

Changing `mysql.host` or `mysql.port` is blocked during SYNC:

```
Cannot change 'mysql.host' while SYNC is in progress. Please wait for SYNC to complete.
```

**Implementation**: `src/server/handlers/variable_handler.cpp:48-57`

## State Flags

### `mysql_reconnecting` (atomic<bool>)

- **Purpose**: Indicates MySQL reconnection is in progress (when changing mysql.host/port)
- **Set to `true`**: At start of `MysqlReconnectionHandler::Reconnect()`
- **Set to `false`**: After reconnection completes (success or failure)
- **Used by**:
  - `MysqlReconnectionHandler` to manage automatic reconnection
  - `ReplicationHandler` to block manual `REPLICATION START`

### `replication_paused_for_dump` (atomic<bool>)

- **Purpose**: Indicates replication was automatically paused for DUMP operation
- **Set to `true`**: Before DUMP SAVE/LOAD when stopping replication
- **Set to `false`**: After DUMP SAVE/LOAD when restarting replication
- **Used by**:
  - `DumpHandler` to manage automatic pause/resume
  - `ReplicationHandler` to block manual `REPLICATION START`

### `read_only` (atomic<bool>)

- **Purpose**: Indicates DUMP SAVE is in progress (read-only mode)
- **Set by**: `FlagGuard` in `DumpHandler::HandleDumpSave()`
- **Blocks**: REPLICATION START, DUMP LOAD, concurrent DUMP SAVE

### `loading` (atomic<bool>)

- **Purpose**: Indicates DUMP LOAD is in progress
- **Set by**: `FlagGuard` in `DumpHandler::HandleDumpLoad()`
- **Blocks**: REPLICATION START, DUMP SAVE, concurrent DUMP LOAD, OPTIMIZE

## Testing

### Unit Tests

1. **Replication START Blocking Tests** (`tests/server/replication_handler_test.cpp`)
   - `ReplicationStartBlockedDuringDumpLoad` - Verifies blocking during DUMP LOAD
   - `ReplicationStartBlockedDuringDumpSave` - Verifies blocking during DUMP SAVE
   - `ReplicationStartBlockedWhenPausedForDump` - Verifies `replication_paused_for_dump` flag blocks manual restart
   - `BlockReplicationStartDuringSYNC` - Verifies blocking during SYNC operations

2. **Dump Handler Tests** (`tests/server/dump_handler_test.cpp`)
   - All 32 tests updated with `replication_paused_for_dump` and `mysql_reconnecting` flags
   - Tests verify proper flag initialization and cleanup

3. **Server Component Tests** (`tests/server/*_test.cpp`)
   - All server test files updated with both new flags
   - ServerLifecycleManager tests verify flag references are correctly passed

### Integration Tests

Integration tests with actual MySQL replication are located in:
- `tests/integration/mysql/gtid_dump_test.cpp` - GTID consistency across DUMP operations
- `tests/integration/mysql/failover_test.cpp` - Replication failover and reconnection

**Note**: BinlogReader methods are not virtual, so unit tests cannot use mocks. Replication auto-stop/restart behavior is verified through integration tests with real MySQL instances.

## Implementation Details

### File Locations

| Component | File |
|-----------|------|
| DUMP SAVE/LOAD replication management | `src/server/handlers/dump_handler.cpp:120-130, 194-218, 280-290, 335-366` |
| Manual REPLICATION START blocking | `src/server/handlers/replication_handler.cpp:45-78` |
| SET VARIABLES mysql.host/port blocking | `src/server/handlers/variable_handler.cpp:48-57` |
| MySQL reconnection with replication restart | `src/app/mysql_reconnection_handler.cpp:32-133` |
| HandlerContext flag definitions | `src/server/server_types.h:103-107` |
| TcpServer flag ownership | `src/server/tcp_server.h:106-107, 282-283` |
| ServerLifecycleManager flag passing | `src/server/server_lifecycle_manager.h:43, 95-97` |

### Structured Logging Events

All replication state changes are logged with structured events:

```cpp
// Replication paused
StructuredLog()
    .Event("replication_paused")
    .Field("operation", "dump_save")
    .Field("reason", "automatic_pause_for_consistency")
    .Info();

// Replication resumed
StructuredLog()
    .Event("replication_resumed")
    .Field("operation", "dump_load")
    .Field("reason", "automatic_restart_after_completion")
    .Field("gtid", gtid)
    .Info();

// Restart failed (operation still succeeds)
StructuredLog()
    .Event("replication_restart_failed")
    .Field("operation", "dump_save")
    .Field("error", error_message)
    .Error();
```

## Summary

| Operation | Replication Management | Manual Control |
|-----------|------------------------|----------------|
| DUMP SAVE | Auto-stop → Auto-restart | Blocked during operation |
| DUMP LOAD | Auto-stop → Auto-restart with new GTID | Blocked during operation |
| SYNC | Auto-stop → Auto-restart after completion | Blocked during SYNC |
| SET mysql.host/port | Auto-stop → Reconnect → Auto-restart | Manual REPLICATION START blocked during reconnection |
| REPLICATION START | N/A | Blocked during DUMP/SYNC/MySQL reconnection |

All automatic replication management ensures data consistency and GTID integrity across operations.

## Key Design Decisions

1. **Auto-restart on all code paths**: Replication is restarted whether operations succeed or fail (with error logging on failure)
2. **Flag-based blocking**: All blocking uses atomic flags checked at operation start
3. **User-friendly error messages**: All blocking errors explain why and suggest waiting for auto-restart
4. **GTID preservation**: All operations maintain GTID continuity across stop/restart cycles
5. **No manual intervention during automatic operations**: Users cannot manually restart replication when auto-management is in progress
