/**
 * @file connection_validator.cpp
 * @brief MySQL connection validator implementation
 */

#ifdef USE_MYSQL

#include "mysql/connection_validator.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>

#include "mysql/connection.h"
#include "mysql/server_flavor.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/structured_log.h"

namespace mygramdb::mysql {

ValidationResult ConnectionValidator::ValidateServer(Connection& conn, const std::vector<std::string>& required_tables,
                                                     const std::optional<std::string>& expected_uuid,
                                                     const std::optional<std::string>& last_gtid) {
  ValidationResult result;

  // Check connection status
  if (!conn.IsConnected()) {
    result.error_message = "Connection is not active";
    return result;
  }

  // 1. Check GTID mode
  auto gtid_check = CheckGTIDEnabled(conn);
  if (!gtid_check) {
    result.error_message = gtid_check.error().message();
    mygram::utils::StructuredLog()
        .Event("connection_validation_failed")
        .Field("reason", "gtid_disabled")
        .Field("error", result.error_message)
        .Error();
    return result;
  }

  // 2. Check server UUID and detect failover
  auto uuid_check = CheckServerUUID(conn, expected_uuid, result.warnings);
  if (!uuid_check) {
    result.error_message = uuid_check.error().message();
    mygram::utils::StructuredLog().Event("connection_validation_failed").Field("reason", "uuid_check_failed").Error();
    return result;
  }
  std::string actual_uuid = *uuid_check;
  result.server_uuid = actual_uuid;

  // Detect failover (server UUID/ID changed)
  if (expected_uuid && *expected_uuid != actual_uuid) {
    result.failover_detected = true;

    // Verify current GTID position is valid on the new server
    // GTID_SUBSET is MySQL-specific; MariaDB doesn't have this function
    if (conn.GetFlavor() != ServerFlavor::kMariaDB && last_gtid && !last_gtid->empty()) {
      // Validate GTID format before using in SQL
      const std::string& gtid_str = *last_gtid;
      bool valid_format = true;
      for (char chr : gtid_str) {
        if (std::isxdigit(static_cast<unsigned char>(chr)) == 0 && chr != '-' && chr != ':' && chr != ',' &&
            chr != ' ' && chr != '\n' && chr != '\r') {
          valid_format = false;
          break;
        }
      }

      if (valid_format) {
        std::string query = "SELECT GTID_SUBSET('" + gtid_str + "', @@GLOBAL.gtid_executed) AS is_subset";
        auto subset_result = conn.Execute(query);
        if (subset_result) {
          MYSQL_ROW row = mysql_fetch_row(subset_result->get());
          if (row != nullptr && row[0] != nullptr && std::string(row[0]) == "0") {
            result.error_message =
                "Failover detected but current GTID position is not a subset of new server's gtid_executed. "
                "GTID position: " +
                gtid_str +
                ". "
                "Manual intervention required: run SYNC command to establish a new position.";
            mygram::utils::StructuredLog()
                .Event("connection_validation_failed")
                .Field("reason", "failover_gtid_mismatch")
                .Field("gtid", gtid_str)
                .Error();
            return result;
          }
        }
      }
    }
  }

  // 3. Check required tables exist
  auto tables_check = CheckTablesExist(conn, required_tables);
  if (!tables_check) {
    result.error_message = tables_check.error().message();
    mygram::utils::StructuredLog()
        .Event("connection_validation_failed")
        .Field("reason", "missing_tables")
        .Field("error", result.error_message)
        .Error();
    return result;
  }

  // 4. Check GTID consistency (if we have an expected state)
  auto gtid_consistency_check = CheckGTIDConsistency(conn, last_gtid);
  if (!gtid_consistency_check) {
    result.warnings.push_back("GTID consistency check: " + gtid_consistency_check.error().message());
  }

  // 5. Check binlog compression (TRANSACTION_PAYLOAD_EVENT not supported)
  auto compression_check = CheckBinlogCompression(conn);
  if (!compression_check) {
    result.error_message = compression_check.error().message();
    mygram::utils::StructuredLog()
        .Event("connection_validation_failed")
        .Field("reason", "binlog_compression_enabled")
        .Error();
    return result;
  }

  // 6. Check binlog_row_image=FULL (required for correct NULL bitmap parsing)
  auto row_image_check = CheckBinlogRowImage(conn);
  if (!row_image_check) {
    result.error_message = row_image_check.error().message();
    mygram::utils::StructuredLog()
        .Event("connection_validation_failed")
        .Field("reason", "binlog_row_image_not_full")
        .Error();
    return result;
  }

  // 7. Check binlog_format=ROW (required for row-level replication)
  auto format_check = CheckBinlogFormat(conn);
  if (!format_check) {
    result.error_message = format_check.error().message();
    mygram::utils::StructuredLog()
        .Event("connection_validation_failed")
        .Field("reason", "binlog_format_not_row")
        .Error();
    return result;
  }

  // 8. Check partial JSON mode (warning only)
  CheckPartialJsonMode(conn, result.warnings);

  // 9. Check tagged GTID support (warning only)
  CheckTaggedGTIDSupport(conn, result.warnings);

  // All checks passed
  result.valid = true;

  if (!result.warnings.empty()) {
    mygram::utils::StructuredLog()
        .Event("connection_validation_succeeded_with_warnings")
        .Field("warning_count", static_cast<int64_t>(result.warnings.size()))
        .Warn();
  }

  return result;
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckGTIDEnabled(Connection& conn) {
  auto gtid_mode_enabled = conn.IsGTIDModeEnabled();
  if (!gtid_mode_enabled) {
    return mygram::utils::MakeUnexpected(gtid_mode_enabled.error());
  }
  if (!*gtid_mode_enabled) {
    return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
        mygram::utils::ErrorCode::kMySQLGTIDNotEnabled, "GTID mode is not enabled on MySQL server (gtid_mode != ON)"));
  }
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckTablesExist(
    Connection& conn, const std::vector<std::string>& tables) {
  std::vector<std::string> missing_tables;

  for (const auto& table : tables) {
    // Validate table name to prevent SQL injection
    // MySQL table names can contain: letters, digits, underscore, dollar sign, hyphen
    // Also reject null bytes which could truncate the name
    bool valid_name = !table.empty();
    for (char chr : table) {
      if (chr == '\0' ||
          (std::isalnum(static_cast<unsigned char>(chr)) == 0 && chr != '_' && chr != '$' && chr != '-')) {
        valid_name = false;
        break;
      }
    }
    if (!valid_name) {
      mygram::utils::StructuredLog()
          .Event("connection_validation_warning")
          .Field("reason", "invalid_table_name")
          .Field("table", table)
          .Warn();
      missing_tables.push_back(table);
      continue;
    }

    // Use mysql_real_escape_string for defense-in-depth SQL injection prevention
    MYSQL* handle = conn.GetHandle();
    std::string escaped_table(table.size() * 2 + 1, '\0');
    auto escaped_len = mysql_real_escape_string(handle, escaped_table.data(), table.c_str(), table.length());
    escaped_table.resize(escaped_len);

    // Query INFORMATION_SCHEMA to check if table exists
    std::string query = "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '" +
                        escaped_table + "' LIMIT 1";

    auto result = conn.Execute(query);
    if (!result) {
      // Query failed - consider table as missing
      missing_tables.push_back(table);
      continue;
    }

    // Check if result has rows
    MYSQL_ROW row = mysql_fetch_row(result->get());
    if (row == nullptr) {
      // No rows - table doesn't exist
      missing_tables.push_back(table);
    }
  }

  if (!missing_tables.empty()) {
    std::string error_msg = "Required tables are missing: ";
    for (size_t i = 0; i < missing_tables.size(); ++i) {
      if (i > 0) {
        error_msg += ", ";
      }
      error_msg += missing_tables[i];
    }
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLTableNotFound, error_msg));
  }

  return {};
}

mygram::utils::Expected<std::string, mygram::utils::Error> ConnectionValidator::CheckServerUUID(
    Connection& conn, const std::optional<std::string>& expected_uuid, std::vector<std::string>& warnings) {
  auto uuid_opt = conn.GetServerUUID();
  if (!uuid_opt) {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLQueryFailed, "Failed to retrieve server UUID"));
  }

  std::string actual_uuid = *uuid_opt;

  // Check if UUID matches expected (failover detection)
  if (expected_uuid && *expected_uuid != actual_uuid) {
    std::string warning = "Server UUID changed: " + *expected_uuid + " -> " + actual_uuid + " (failover detected)";
    warnings.push_back(warning);

    mygram::utils::StructuredLog()
        .Event("mysql_failover_detected")
        .Field("old_uuid", *expected_uuid)
        .Field("new_uuid", actual_uuid)
        .Warn();
  }

  return actual_uuid;
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckGTIDConsistency(
    Connection& conn, const std::optional<std::string>& last_gtid) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // Get current executed GTID set
  auto executed_gtid = conn.GetExecutedGTID();
  if (!executed_gtid) {
    return MakeUnexpected(executed_gtid.error());
  }

  // MariaDB doesn't have gtid_purged or GTID_SUBSET function.
  // Skip purge check for MariaDB; the binlog stream will report an error
  // if the requested position has been purged.
  if (conn.GetFlavor() == ServerFlavor::kMariaDB) {
    mygram::utils::StructuredLog()
        .Event("gtid_consistency_check")
        .Field("executed_gtid", *executed_gtid)
        .Field("flavor", "MariaDB")
        .Debug();
    return {};
  }

  // Get purged GTID set (MySQL only)
  auto purged_gtid = conn.GetPurgedGTID();
  if (!purged_gtid) {
    return MakeUnexpected(purged_gtid.error());
  }

  // If we have a last GTID, check if it has been purged using MySQL's GTID_SUBSET function
  if (last_gtid && !last_gtid->empty() && !purged_gtid->empty()) {
    // Validate GTID format to prevent SQL injection
    // Valid GTID format: UUID:GNO or UUID:TAG:GNO or UUID:GNO-GNO2
    // Characters allowed: hex digits, hyphens, colons, commas, spaces, newlines
    const std::string& gtid_str = *last_gtid;
    for (char chr : gtid_str) {  // NOLINT(readability-identifier-length)
      if (std::isxdigit(static_cast<unsigned char>(chr)) == 0 && chr != '-' && chr != ':' && chr != ',' && chr != ' ' &&
          chr != '\n' && chr != '\r') {
        return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID,
                                        "Invalid GTID format: contains illegal character '" + std::string(1, chr) + "'",
                                        gtid_str));
      }
    }

    // Use GTID_SUBSET to check if our start position has been purged
    std::string query = "SELECT GTID_SUBSET('" + gtid_str + "', @@GLOBAL.gtid_purged) AS is_purged";
    auto result = conn.Execute(query);
    if (result) {
      MYSQL_ROW row = mysql_fetch_row(result->get());
      if (row != nullptr && row[0] != nullptr && std::string(row[0]) == "1") {
        return MakeUnexpected(MakeError(ErrorCode::kMySQLReplicationError,
                                        "Start GTID '" + gtid_str +
                                            "' has been purged from server binlog. "
                                            "Available positions start after purged set: " +
                                            *purged_gtid +
                                            ". "
                                            "Run SYNC command to establish a new position.",
                                        gtid_str));
      }
    }
  }

  // Log for debugging
  mygram::utils::StructuredLog()
      .Event("gtid_consistency_check")
      .Field("executed_gtid", *executed_gtid)
      .Field("purged_gtid", *purged_gtid)
      .Debug();

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckBinlogCompression(Connection& conn) {
  // MariaDB doesn't support binlog_transaction_compression
  if (conn.GetFlavor() == ServerFlavor::kMariaDB) {
    return {};
  }

  auto result = conn.Execute("SHOW VARIABLES LIKE 'binlog_transaction_compression'");
  if (!result) {
    // Variable doesn't exist (MySQL < 8.0.20) - OK
    return {};
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if (row == nullptr) {
    // Variable not found - OK (MySQL < 8.0.20)
    return {};
  }

  // row[0] = variable name, row[1] = value
  if (row[1] != nullptr && std::string(row[1]) == "ON") {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError,
                                 "binlog_transaction_compression=ON is not supported. "
                                 "TRANSACTION_PAYLOAD_EVENT (compressed binlog events) cannot be decoded. "
                                 "Disable compression with: SET GLOBAL binlog_transaction_compression=OFF"));
  }

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckBinlogRowImage(Connection& conn) {
  auto result = conn.Execute("SHOW VARIABLES LIKE 'binlog_row_image'");
  if (!result) {
    // Cannot determine - assume FULL (variable always exists in MySQL 5.6+)
    return {};
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if (row == nullptr) {
    // Variable not found - shouldn't happen but assume FULL
    return {};
  }

  if (row[1] != nullptr) {
    std::string value(row[1]);
    std::string upper_value = value;
    std::transform(upper_value.begin(), upper_value.end(), upper_value.begin(), ::toupper);
    if (upper_value != "FULL") {
      return mygram::utils::MakeUnexpected(
          mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError,
                                   "binlog_row_image=" + value +
                                       " is not supported. "
                                       "MygramDB requires binlog_row_image=FULL for correct NULL bitmap parsing. "
                                       "Set it with: SET GLOBAL binlog_row_image=FULL"));
    }
  }

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckBinlogFormat(Connection& conn) {
  auto result = conn.Execute("SHOW VARIABLES LIKE 'binlog_format'");
  if (!result) {
    // Cannot determine - assume ROW (variable always exists in MySQL 5.6+)
    return {};
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if (row == nullptr) {
    // Variable not found - shouldn't happen but assume ROW
    return {};
  }

  if (row[1] != nullptr) {
    std::string value(row[1]);
    std::string upper_value = value;
    std::transform(upper_value.begin(), upper_value.end(), upper_value.begin(), ::toupper);
    if (upper_value != "ROW") {
      return mygram::utils::MakeUnexpected(
          mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError,
                                   "binlog_format=" + value +
                                       " is not supported. "
                                       "MygramDB requires binlog_format=ROW for row-level replication. "
                                       "Set it with: SET GLOBAL binlog_format=ROW"));
    }
  }

  return {};
}

bool ConnectionValidator::CheckPartialJsonMode(Connection& conn, std::vector<std::string>& warnings) {
  // MariaDB doesn't have binlog_row_value_options
  if (conn.GetFlavor() == ServerFlavor::kMariaDB) {
    return true;
  }

  auto result = conn.Execute("SHOW VARIABLES LIKE 'binlog_row_value_options'");
  if (!result) {
    return true;
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if (row == nullptr) {
    return true;
  }

  if (row[1] != nullptr) {
    std::string value(row[1]);
    // Check if PARTIAL_JSON is in the value (case-insensitive)
    std::string upper_value = value;
    std::transform(upper_value.begin(), upper_value.end(), upper_value.begin(), ::toupper);
    if (upper_value.find("PARTIAL_JSON") != std::string::npos) {
      warnings.emplace_back(
          "binlog_row_value_options contains PARTIAL_JSON. "
          "PARTIAL_UPDATE_ROWS_EVENT is not supported and will be skipped. "
          "JSON column updates may be lost. Consider: "
          "SET GLOBAL binlog_row_value_options=''");
    }
  }

  return true;
}

bool ConnectionValidator::CheckTaggedGTIDSupport(Connection& conn, std::vector<std::string>& warnings) {
  // Tagged GTIDs are a MySQL 8.4+ feature, not applicable to MariaDB
  if (conn.GetFlavor() == ServerFlavor::kMariaDB) {
    return true;
  }

  auto executed = conn.GetExecutedGTID();
  if (!executed) {
    return true;
  }

  // Tagged GTIDs use format UUID:TAG:GNO where TAG contains no hyphens
  // The '::' pattern indicates tag separator in GTID sets (empty tag)
  // MySQL 8.4 tagged GTID format: UUID:tag:GNO
  // Non-tagged format: UUID:GNO or UUID:GNO-GNO2

  // Check if the server version supports tagged GTIDs (MySQL 8.4+)
  auto result = conn.Execute("SELECT VERSION()");
  if (result) {
    MYSQL_ROW row = mysql_fetch_row(result->get());
    if (row != nullptr && row[0] != nullptr) {
      std::string version(row[0]);
      // MySQL 8.4+ supports tagged GTIDs
      int major = 0;
      int minor = 0;
      // NOLINTNEXTLINE(cert-err34-c,cppcoreguidelines-pro-type-vararg)
      if (sscanf(version.c_str(), "%d.%d", &major, &minor) == 2) {
        constexpr int kMinMajorVersion = 8;
        constexpr int kMinMinorVersion = 4;
        if (major > kMinMajorVersion || (major == kMinMajorVersion && minor >= kMinMinorVersion)) {
          // MySQL 8.4+ - tagged GTIDs are possible
          // Check if any are actually in use
          if (executed->find("::") != std::string::npos) {
            warnings.emplace_back(
                "Tagged GTIDs detected in executed GTID set. "
                "Tagged GTID support is experimental.");
          }
        }
      }
    }
  }

  return true;
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
