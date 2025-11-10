/**
 * @file binlog_event_types.h
 * @brief MySQL binlog event type definitions
 *
 * Extracted from MySQL 8.4.7 source code (libbinlogevents)
 * https://github.com/mysql/mysql-server
 *
 * License: GPL v2.0 with Universal FOSS Exception
 */

#pragma once

namespace mygramdb {
namespace mysql {

/**
 * @brief MySQL binlog event types
 *
 * Based on enum Log_event_type from MySQL 8.4.7
 */
enum class MySQLBinlogEventType {
  UNKNOWN_EVENT = 0,
  START_EVENT_V3 = 1,             // Deprecated
  QUERY_EVENT = 2,
  STOP_EVENT = 3,
  ROTATE_EVENT = 4,
  INTVAR_EVENT = 5,
  SLAVE_EVENT = 7,                    // Legacy name from binlog protocol (replica event)
  APPEND_BLOCK_EVENT = 9,
  DELETE_FILE_EVENT = 11,
  RAND_EVENT = 13,
  USER_VAR_EVENT = 14,
  FORMAT_DESCRIPTION_EVENT = 15,
  XID_EVENT = 16,
  BEGIN_LOAD_QUERY_EVENT = 17,
  EXECUTE_LOAD_QUERY_EVENT = 18,

  TABLE_MAP_EVENT = 19,           // Contains table metadata

  // V1 events (obsolete since 8.4.0)
  OBSOLETE_WRITE_ROWS_EVENT_V1 = 23,
  OBSOLETE_UPDATE_ROWS_EVENT_V1 = 24,
  OBSOLETE_DELETE_ROWS_EVENT_V1 = 25,

  INCIDENT_EVENT = 26,
  HEARTBEAT_LOG_EVENT = 27,
  IGNORABLE_LOG_EVENT = 28,
  ROWS_QUERY_LOG_EVENT = 29,

  // V2 Row events (current format)
  WRITE_ROWS_EVENT = 30,          // INSERT operations
  UPDATE_ROWS_EVENT = 31,         // UPDATE operations
  DELETE_ROWS_EVENT = 32,         // DELETE operations

  GTID_LOG_EVENT = 33,            // GTID transaction marker
  ANONYMOUS_GTID_LOG_EVENT = 34,
  PREVIOUS_GTIDS_LOG_EVENT = 35,
  TRANSACTION_CONTEXT_EVENT = 36,
  VIEW_CHANGE_EVENT = 37,
  XA_PREPARE_LOG_EVENT = 38,
  PARTIAL_UPDATE_ROWS_EVENT = 39,
  TRANSACTION_PAYLOAD_EVENT = 40,
  HEARTBEAT_LOG_EVENT_V2 = 41,
  GTID_TAGGED_LOG_EVENT = 42,

  ENUM_END_EVENT = 255            // End marker
};

/**
 * @brief Get event type name as string
 */
inline const char* GetEventTypeName(MySQLBinlogEventType type) {
  switch (type) {
    case MySQLBinlogEventType::UNKNOWN_EVENT: return "UNKNOWN_EVENT";
    case MySQLBinlogEventType::QUERY_EVENT: return "QUERY_EVENT";
    case MySQLBinlogEventType::STOP_EVENT: return "STOP_EVENT";
    case MySQLBinlogEventType::ROTATE_EVENT: return "ROTATE_EVENT";
    case MySQLBinlogEventType::FORMAT_DESCRIPTION_EVENT: return "FORMAT_DESCRIPTION_EVENT";
    case MySQLBinlogEventType::XID_EVENT: return "XID_EVENT";
    case MySQLBinlogEventType::TABLE_MAP_EVENT: return "TABLE_MAP_EVENT";
    case MySQLBinlogEventType::WRITE_ROWS_EVENT: return "WRITE_ROWS_EVENT";
    case MySQLBinlogEventType::UPDATE_ROWS_EVENT: return "UPDATE_ROWS_EVENT";
    case MySQLBinlogEventType::DELETE_ROWS_EVENT: return "DELETE_ROWS_EVENT";
    case MySQLBinlogEventType::GTID_LOG_EVENT: return "GTID_LOG_EVENT";
    case MySQLBinlogEventType::HEARTBEAT_LOG_EVENT: return "HEARTBEAT_LOG_EVENT";
    case MySQLBinlogEventType::ROWS_QUERY_LOG_EVENT: return "ROWS_QUERY_LOG_EVENT";
    case MySQLBinlogEventType::PREVIOUS_GTIDS_LOG_EVENT: return "PREVIOUS_GTIDS_LOG_EVENT";
    default: return "UNKNOWN";
  }
}

}  // namespace mysql
}  // namespace mygramdb
