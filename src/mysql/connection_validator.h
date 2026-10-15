/**
 * @file connection_validator.h
 * @brief MySQL connection validator for failover detection
 */

#pragma once

#ifdef USE_MYSQL

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "mysql/server_flavor.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql {

// Forward declaration
class Connection;

/**
 * @brief Connection validation result
 */
struct ValidationResult {
  bool valid = false;
  bool failover_detected = false;  ///< True when server UUID changed
  std::string error_message;
  std::optional<mygram::utils::ErrorCode> error_code;
  std::vector<std::string> warnings;
  std::optional<std::string> server_uuid;  ///< Detected server UUID

  /**
   * @brief Check if validation passed
   */
  explicit operator bool() const { return valid; }
};

/**
 * @brief MySQL connection validator
 *
 * Validates MySQL server connections to detect:
 * - Failover scenarios (server UUID change)
 * - Invalid servers (missing tables, GTID disabled, inconsistent state)
 */
class ConnectionValidator {
 public:
  struct RequiredTable {
    std::string database;
    std::string name;

    [[nodiscard]] std::string DisplayName() const;
  };

  static bool IsValidIdentifier(std::string_view identifier);
  static bool IsSupportedBinlogFormatValue(std::string_view value);
  static bool IsSupportedBinlogChecksumValue(std::string_view value);
  static bool IsSupportedBinlogRowMetadataValue(std::string_view value);

  /// True when a MariaDB gtid_slave_pos value shows replicated transactions were applied.
  static bool HasAppliedReplicatedGtids(std::string_view gtid_slave_pos);

  /// True when a MariaDB binlog_storage_engine value names an engine.
  static bool IsBinlogStorageEngineEnabled(std::string_view value);

  /**
   * @brief True when any GTID entry carries a tag, in any position of the entry
   *
   * A set the GTID grammar rejects counts as tagged: it cannot be shown to be untagged.
   */
  static bool ContainsTaggedGtid(std::string_view gtid_set);

  /**
   * @brief Decide whether a replica keeps replicated writes in its own binary log
   *
   * @param is_replica Whether the replica status statement returned rows
   * @param value Value of log_replica_updates (MySQL) / log_slave_updates (MariaDB); nullopt if undefined
   * @return kMySQLBinlogError when a replica does not log replicated writes
   */
  static mygram::utils::Expected<void, mygram::utils::Error> EvaluateLogReplicaUpdates(
      ServerFlavor flavor, bool is_replica, const std::optional<std::string>& value);

  /**
   * @brief Validate MySQL server connection
   *
   * @param conn MySQL connection to validate
   * @param required_tables Tables that must exist on the server
   * @param expected_uuid Expected server UUID (nullopt to skip UUID check)
   * @return ValidationResult with validation status and details
   */
  static ValidationResult ValidateServer(Connection& conn, const std::vector<std::string>& required_tables,
                                         const std::optional<std::string>& expected_uuid = std::nullopt,
                                         const std::optional<std::string>& last_gtid = std::nullopt);

  static ValidationResult ValidateServer(Connection& conn, const std::vector<RequiredTable>& required_tables,
                                         const std::optional<std::string>& expected_uuid = std::nullopt,
                                         const std::optional<std::string>& last_gtid = std::nullopt);

 private:
  /**
   * @brief Check if GTID mode is enabled
   * @return Expected<void, Error> - success or error with kMySQLGTIDNotEnabled
   */
  static mygram::utils::Expected<void, mygram::utils::Error> CheckGTIDEnabled(Connection& conn);

  /**
   * @brief Check if required tables exist
   * @return Expected<void, Error> - success or error with kMySQLTableNotFound listing missing tables
   */
  static mygram::utils::Expected<void, mygram::utils::Error> CheckTablesExist(Connection& conn,
                                                                              const std::vector<RequiredTable>& tables);

  /**
   * @brief Check server UUID and detect failover
   * @return Expected<std::string, Error> - actual UUID or error
   */
  static mygram::utils::Expected<std::string, mygram::utils::Error> CheckServerUUID(
      Connection& conn, const std::optional<std::string>& expected_uuid, std::vector<std::string>& warnings);

  /**
   * @brief Check GTID consistency
   *
   * Validates that the server's GTID state is consistent with expected state.
   * This helps detect scenarios where a server has diverged or been reset.
   *
   * @return Expected<void, Error> - success or error describing inconsistency
   */
  static mygram::utils::Expected<void, mygram::utils::Error> CheckGTIDConsistency(
      Connection& conn, const std::optional<std::string>& last_gtid);

  /**
   * @brief Reject a MariaDB position whose transactions the server has purged
   *
   * @param processed_position Position replication resumes from
   * @param purge_boundary GTID position at the start of the oldest binlog
   *        (BINLOG_GTID_POS on its first event)
   * @return kMySQLReplicationError when any boundary domain is ahead of, or
   *         missing from, the processed position
   */
  static mygram::utils::Expected<void, mygram::utils::Error> CheckMariaDBPositionNotPurged(
      const std::string& processed_position, const std::string& purge_boundary);

  /**
   * @brief Check if binlog transaction compression is enabled
   *
   * TRANSACTION_PAYLOAD_EVENT (MySQL) and compressed query/row events
   * (MariaDB log_bin_compress) are not supported. Reject either setting.
   *
   * @return Expected<void, Error> - success or error with kMySQLBinlogError
   */
  static mygram::utils::Expected<void, mygram::utils::Error> CheckBinlogCompression(Connection& conn);

  /**
   * @brief Check if binlog_row_image is set to FULL
   *
   * MygramDB requires binlog_row_image=FULL because the NULL bitmap and column
   * data parsing assumes all columns are present. With MINIMAL or NOBLOB, the
   * bitmap size and indexing are different, causing silent data corruption.
   *
   * @return Expected<void, Error> - success or error with kMySQLBinlogError
   */
  static mygram::utils::Expected<void, mygram::utils::Error> CheckBinlogRowImage(Connection& conn);

  /**
   * @brief Check if binlog_format is set to ROW
   *
   * MygramDB requires binlog_format=ROW because it relies on row-level events
   * for data replication. STATEMENT or MIXED formats are not supported.
   *
   * @return Expected<void, Error> - success or error with kMySQLBinlogError
   */
  static mygram::utils::Expected<void, mygram::utils::Error> CheckBinlogFormat(Connection& conn);

  /**
   * @brief Check if binlog_checksum is set to CRC32
   *
   * The reader follows the algorithm each binlog file declares, so files
   * written earlier with NONE still stream. The requirement keeps every event
   * written from now on under integrity verification: with NONE a corrupted
   * event would be applied unnoticed.
   *
   * @return Expected<void, Error> - success or error with kMySQLBinlogError
   */
  static mygram::utils::Expected<void, mygram::utils::Error> CheckBinlogChecksum(Connection& conn);

  /**
   * @brief Check if partial JSON update mode is enabled
   *
   * binlog_row_value_options=PARTIAL_JSON causes PARTIAL_UPDATE_ROWS_EVENT
   * which is not yet supported.
   */
  static mygram::utils::Expected<void, mygram::utils::Error> CheckPartialJsonMode(Connection& conn);

  /**
   * @brief Check that binlog_row_metadata is FULL
   *
   * TABLE_MAP events carry column names only under FULL; the reader takes names
   * from them rather than from a schema query that can race with DDL.
   *
   * @return Expected<void, Error> - success or error with kMySQLBinlogError
   */
  static mygram::utils::Expected<void, mygram::utils::Error> CheckBinlogRowMetadata(Connection& conn);

  /**
   * @brief Reject MariaDB's engine-integrated binary log (binlog_storage_engine)
   *
   * That log has no per-event checksum and BINLOG_GTID_POS cannot probe its
   * purge boundary. Not applicable to MySQL.
   */
  static mygram::utils::Expected<void, mygram::utils::Error> CheckBinlogStorageEngine(Connection& conn);

  /**
   * @brief Require log_replica_updates / log_slave_updates when the server is a replica
   *
   * Without it, writes applied by the replication threads never reach this
   * server's binary log and the index silently stops changing. A failed replica
   * status query (missing privilege) fails validation.
   */
  static mygram::utils::Expected<void, mygram::utils::Error> CheckLogReplicaUpdates(Connection& conn);

  /**
   * @brief Check for tagged GTID usage
   *
   * MySQL 8.4+ supports tagged GTIDs (UUID:TAG:GNO).
   * Detect usage and reject it until GTID encode/decode supports tags end-to-end.
   */
  static mygram::utils::Expected<void, mygram::utils::Error> CheckTaggedGTIDSupport(Connection& conn);
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
