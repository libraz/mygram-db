/**
 * @file binlog_event_disposition.h
 * @brief Fail-closed classification of every binlog event type on the wire.
 *
 * The reader may treat a wire event in exactly three ways: decode and apply it,
 * skip it because it is provably data-neutral, or stop replication. This header
 * owns the single place where that decision is recorded, so an event type added
 * to MySQLBinlogEventType is a compile error (-Werror=switch) until it has been
 * classified here.
 */

#pragma once

#include <cstdint>
#include <string>

#include "mysql/binlog_event_types.h"

namespace mygramdb::mysql {

/**
 * @brief How the reader is allowed to treat one binlog event type.
 */
enum class BinlogEventDisposition : uint8_t {
  kDecodeAndApply,  ///< Carries replicated data or position; the reader decodes it.
  kDataNeutral,     ///< Informational; skipping it cannot lose a row or a position.
  kFailClosed,      ///< Cannot be applied and cannot be proven harmless to skip.
};

/**
 * @brief Recovery sentence shared by every fail-closed binlog event.
 *
 * Changing whatever server setting produced the event only stops new ones: the
 * event that stopped the stream stays in the binlog, so replication can never
 * resume from a position before it. Recovery means rebuilding from a snapshot
 * taken after it, and it has to cover every replicated table because the first
 * SYNC moves the shared stream past the event.
 */
inline constexpr const char* kUnreplayableEventRecovery =
    ". The event stays in the binlog, so replication cannot resume from before it: run SYNC for every "
    "replicated table to rebuild past it.";

/**
 * @brief Classify a wire event type.
 *
 * The switch has no default label on purpose. Every enumerator carries the
 * reason its disposition is safe, and a wire byte that matches no enumerator
 * falls through to the fail-closed return below the switch.
 */
inline BinlogEventDisposition ClassifyBinlogEventDisposition(MySQLBinlogEventType type) {
  switch (type) {
    // Decoded and applied. The reader consumes the position events itself and
    // hands the row, statement and commit events to BinlogEventParser.
    case MySQLBinlogEventType::QUERY_EVENT:
    case MySQLBinlogEventType::XID_EVENT:
    case MySQLBinlogEventType::TABLE_MAP_EVENT:
    case MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1:
    case MySQLBinlogEventType::OBSOLETE_UPDATE_ROWS_EVENT_V1:
    case MySQLBinlogEventType::OBSOLETE_DELETE_ROWS_EVENT_V1:
    case MySQLBinlogEventType::WRITE_ROWS_EVENT:
    case MySQLBinlogEventType::UPDATE_ROWS_EVENT:
    case MySQLBinlogEventType::DELETE_ROWS_EVENT:
    case MySQLBinlogEventType::GTID_LOG_EVENT:
    case MySQLBinlogEventType::MARIADB_GTID_EVENT:
    case MySQLBinlogEventType::MARIADB_GTID_LIST_EVENT:
      return BinlogEventDisposition::kDecodeAndApply;

    // Skipped. Each of these is informational: it carries no row image, and the
    // position of the transaction containing it is established by the GTID and
    // commit events the reader already tracks.

    // Written when the source shut down cleanly. The next binlog file continues
    // the same GTID stream.
    case MySQLBinlogEventType::STOP_EVENT:
    // Names the next binlog file. Positions are tracked by GTID, not by file.
    case MySQLBinlogEventType::ROTATE_EVENT:
    // Describes the format of the file that follows. Nothing in the reader
    // depends on its post-header lengths.
    case MySQLBinlogEventType::FORMAT_DESCRIPTION_EVENT:
    // Keepalive frames carrying no transaction.
    case MySQLBinlogEventType::HEARTBEAT_LOG_EVENT:
    case MySQLBinlogEventType::HEARTBEAT_LOG_EVENT_V2:
    // Statement-based execution context (AUTO_INCREMENT, RAND seeds, user
    // variables). They only ever precede a QUERY_EVENT, and the statement they
    // parameterize is judged separately: any statement that can carry row data
    // stops replication at that QUERY_EVENT.
    case MySQLBinlogEventType::INTVAR_EVENT:
    case MySQLBinlogEventType::RAND_EVENT:
    case MySQLBinlogEventType::USER_VAR_EVENT:
    // Stage the contents of a file for LOAD DATA INFILE. They mutate no table
    // by themselves; the statement that applies them is EXECUTE_LOAD_QUERY_EVENT,
    // which fails closed below.
    case MySQLBinlogEventType::BEGIN_LOAD_QUERY_EVENT:
    case MySQLBinlogEventType::APPEND_BLOCK_EVENT:
    case MySQLBinlogEventType::DELETE_FILE_EVENT:
    // Marked ignorable by the source, which is the protocol's guarantee that a
    // replica that does not understand the payload loses nothing by skipping it.
    case MySQLBinlogEventType::IGNORABLE_LOG_EVENT:
    // The original SQL text of the row events that follow, logged for operators
    // when binlog_rows_query_log_events is on. The rows themselves carry the data.
    case MySQLBinlogEventType::ROWS_QUERY_LOG_EVENT:
    // Lists the GTIDs already contained in earlier files. It restates history
    // rather than adding a transaction.
    case MySQLBinlogEventType::PREVIOUS_GTIDS_LOG_EVENT:
    // Group Replication bookkeeping: the certification write set and a change of
    // group membership. Neither carries a row image, and both are logged inside
    // a transaction whose GTID and commit the reader already tracks.
    case MySQLBinlogEventType::TRANSACTION_CONTEXT_EVENT:
    case MySQLBinlogEventType::VIEW_CHANGE_EVENT:
    // MariaDB: the SQL text behind row events, an XA crash-recovery checkpoint,
    // and the marker of an encrypted binlog file. The source decrypts events
    // before sending them, so the marker has no effect on a replica.
    case MySQLBinlogEventType::MARIADB_ANNOTATE_ROWS_EVENT:
    case MySQLBinlogEventType::MARIADB_BINLOG_CHECKPOINT_EVENT:
    case MySQLBinlogEventType::MARIADB_START_ENCRYPTION_EVENT:
      return BinlogEventDisposition::kDataNeutral;

    // Fail closed.
    case MySQLBinlogEventType::UNKNOWN_EVENT:
    case MySQLBinlogEventType::START_EVENT_V3:
    case MySQLBinlogEventType::SLAVE_EVENT:
    case MySQLBinlogEventType::EXECUTE_LOAD_QUERY_EVENT:
    case MySQLBinlogEventType::INCIDENT_EVENT:
    case MySQLBinlogEventType::ANONYMOUS_GTID_LOG_EVENT:
    case MySQLBinlogEventType::XA_PREPARE_LOG_EVENT:
    case MySQLBinlogEventType::PARTIAL_UPDATE_ROWS_EVENT:
    case MySQLBinlogEventType::TRANSACTION_PAYLOAD_EVENT:
    case MySQLBinlogEventType::GTID_TAGGED_LOG_EVENT:
    case MySQLBinlogEventType::MARIADB_QUERY_COMPRESSED_EVENT:
    case MySQLBinlogEventType::MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
    case MySQLBinlogEventType::MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
    case MySQLBinlogEventType::MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
    case MySQLBinlogEventType::MARIADB_WRITE_ROWS_COMPRESSED_EVENT:
    case MySQLBinlogEventType::MARIADB_UPDATE_ROWS_COMPRESSED_EVENT:
    case MySQLBinlogEventType::MARIADB_DELETE_ROWS_COMPRESSED_EVENT:
    case MySQLBinlogEventType::ENUM_END_EVENT:
      return BinlogEventDisposition::kFailClosed;
  }

  // A type byte outside the enumeration means the stream is not the protocol
  // this build decodes. Nothing about its payload is known, so it cannot be
  // skipped safely.
  return BinlogEventDisposition::kFailClosed;
}

/**
 * @brief Operator-facing reason a fail-closed event stopped replication.
 *
 * The returned text always ends with the shared recovery sentence, because
 * every one of these events survives in the binlog and is redelivered on any
 * reconnect from a position before it.
 */
inline std::string UnreplayableEventRemediation(MySQLBinlogEventType type) {
  std::string reason;
  switch (type) {
    case MySQLBinlogEventType::TRANSACTION_PAYLOAD_EVENT:
      reason =
          "Received TRANSACTION_PAYLOAD_EVENT: binlog_transaction_compression was enabled on the server after "
          "initial validation. Compressed events cannot be decoded. Disable compression with: SET GLOBAL "
          "binlog_transaction_compression=OFF";
      break;
    case MySQLBinlogEventType::PARTIAL_UPDATE_ROWS_EVENT:
      reason =
          "Received PARTIAL_UPDATE_ROWS_EVENT: binlog_row_value_options=PARTIAL_JSON was enabled on the server after "
          "initial validation. Partial JSON updates cannot be decoded. Disable with: SET GLOBAL "
          "binlog_row_value_options=''";
      break;
    case MySQLBinlogEventType::XA_PREPARE_LOG_EVENT:
      reason =
          "Received XA_PREPARE_LOG_EVENT. XA transactions are unsupported because prepared rows cannot be published "
          "before a later XA COMMIT or discarded on XA ROLLBACK";
      break;
    case MySQLBinlogEventType::MARIADB_QUERY_COMPRESSED_EVENT:
    case MySQLBinlogEventType::MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
    case MySQLBinlogEventType::MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
    case MySQLBinlogEventType::MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
    case MySQLBinlogEventType::MARIADB_WRITE_ROWS_COMPRESSED_EVENT:
    case MySQLBinlogEventType::MARIADB_UPDATE_ROWS_COMPRESSED_EVENT:
    case MySQLBinlogEventType::MARIADB_DELETE_ROWS_COMPRESSED_EVENT:
      reason =
          "Received a MariaDB compressed binlog event while log_bin_compress is enabled. Compressed events cannot be "
          "decoded. Disable compression with: SET GLOBAL log_bin_compress=OFF";
      break;
    case MySQLBinlogEventType::ANONYMOUS_GTID_LOG_EVENT:
      reason =
          "Received ANONYMOUS_GTID_LOG_EVENT: the source committed a transaction without a GTID, so gtid_mode is no "
          "longer ON. The transaction has no position to record, and applying its rows would attribute them to the "
          "previous transaction's GTID. Set gtid_mode=ON on the source";
      break;
    case MySQLBinlogEventType::INCIDENT_EVENT:
      reason =
          "Received INCIDENT_EVENT: the source declared that it lost events, so the binlog contains a gap. The "
          "transactions inside the gap were never written and cannot be replayed from any position";
      break;
    case MySQLBinlogEventType::EXECUTE_LOAD_QUERY_EVENT:
      reason =
          "Received EXECUTE_LOAD_QUERY_EVENT: a statement-based LOAD DATA INFILE. Its rows are never materialized by "
          "the row decoder, so accepting it would leave the index missing every loaded row";
      break;
    case MySQLBinlogEventType::START_EVENT_V3:
      reason =
          "Received START_EVENT_V3: the stream uses the binlog format of a server older than MySQL 5.0, which this "
          "build cannot decode";
      break;
    case MySQLBinlogEventType::UNKNOWN_EVENT:
    case MySQLBinlogEventType::SLAVE_EVENT:
    case MySQLBinlogEventType::ENUM_END_EVENT:
      reason = std::string("Received ") + GetEventTypeName(type) +
               ", which no supported server writes to a binlog. The stream does not match the binlog protocol this "
               "build decodes";
      break;
    default:
      reason = std::string("Received ") + GetEventTypeName(type) +
               ", whose payload this build cannot decode. Accepting it would advance the replication position past "
               "data that was never applied";
      break;
  }
  return reason + kUnreplayableEventRecovery;
}

}  // namespace mygramdb::mysql
