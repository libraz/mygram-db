/**
 * @file connection_validator.h
 * @brief MySQL connection validator for failover detection
 */

#pragma once

#ifdef USE_MYSQL

#include <optional>
#include <string>
#include <vector>

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
   * @return Expected<void, Error> - success or error with kMySQLGTIDNotEnabled
   */
  static mygram::utils::Expected<void, mygram::utils::Error> CheckGTIDEnabled(Connection& conn);

  /**
   * @brief Check if required tables exist
   * @return Expected<void, Error> - success or error with kMySQLTableNotFound listing missing tables
   */
  static mygram::utils::Expected<void, mygram::utils::Error> CheckTablesExist(Connection& conn,
                                                                              const std::vector<std::string>& tables);

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
   * @brief Check if binlog transaction compression is enabled
   *
   * TRANSACTION_PAYLOAD_EVENT (type 40) from binlog_transaction_compression=ON
   * is not supported. Reject connections with compression enabled.
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
