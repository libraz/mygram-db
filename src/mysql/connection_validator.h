/**
 * @file connection_validator.h
 * @brief MySQL connection validator for failover detection
 */

#pragma once

#ifdef USE_MYSQL

#include <optional>
#include <string>
#include <vector>

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

 private:
  /**
   * @brief Check if GTID mode is enabled
   */
  static bool CheckGTIDEnabled(Connection& conn, std::string& error);

  /**
   * @brief Check if required tables exist
   */
  static bool CheckTablesExist(Connection& conn, const std::vector<std::string>& tables,
                               std::vector<std::string>& missing_tables);

  /**
   * @brief Check server UUID and detect failover
   */
  static bool CheckServerUUID(Connection& conn, const std::optional<std::string>& expected_uuid,
                              std::string& actual_uuid, std::vector<std::string>& warnings);

  /**
   * @brief Check GTID consistency
   *
   * Validates that the server's GTID state is consistent with expected state.
   * This helps detect scenarios where a server has diverged or been reset.
   */
  static bool CheckGTIDConsistency(Connection& conn, const std::optional<std::string>& last_gtid, std::string& error);

  /**
   * @brief Check if binlog transaction compression is enabled
   *
   * TRANSACTION_PAYLOAD_EVENT (type 40) from binlog_transaction_compression=ON
   * is not supported. Reject connections with compression enabled.
   */
  static bool CheckBinlogCompression(Connection& conn, std::string& error);

  /**
   * @brief Check if binlog_row_image is set to FULL
   *
   * MygramDB requires binlog_row_image=FULL because the NULL bitmap and column
   * data parsing assumes all columns are present. With MINIMAL or NOBLOB, the
   * bitmap size and indexing are different, causing silent data corruption.
   */
  static bool CheckBinlogRowImage(Connection& conn, std::string& error);

  /**
   * @brief Check if binlog_format is set to ROW
   *
   * MygramDB requires binlog_format=ROW because it relies on row-level events
   * for data replication. STATEMENT or MIXED formats are not supported.
   */
  static bool CheckBinlogFormat(Connection& conn, std::string& error);

  /**
   * @brief Check if partial JSON update mode is enabled
   *
   * binlog_row_value_options=PARTIAL_JSON causes PARTIAL_UPDATE_ROWS_EVENT
   * which is not yet supported. Issue a warning.
   */
  static bool CheckPartialJsonMode(Connection& conn, std::vector<std::string>& warnings);

  /**
   * @brief Check for tagged GTID usage
   *
   * MySQL 8.4+ supports tagged GTIDs (UUID:TAG:GNO).
   * Detect usage and warn about experimental support.
   */
  static bool CheckTaggedGTIDSupport(Connection& conn, std::vector<std::string>& warnings);
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
