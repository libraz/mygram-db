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
#include "utils/numeric_parse.h"
#include "utils/structured_log.h"

namespace mygramdb::mysql {

namespace {

bool IsGtidIntervalToken(std::string_view token) {
  auto dash_pos = token.find('-');
  if (dash_pos == std::string_view::npos) {
    return mygram::utils::ParseNumeric<uint64_t>(token).has_value();
  }
  if (dash_pos == 0 || dash_pos + 1 >= token.size()) {
    return false;
  }
  return mygram::utils::ParseNumeric<uint64_t>(token.substr(0, dash_pos)).has_value() &&
         mygram::utils::ParseNumeric<uint64_t>(token.substr(dash_pos + 1)).has_value();
}

}  // namespace

std::string ConnectionValidator::RequiredTable::DisplayName() const {
  return database.empty() ? name : database + "." + name;
}

bool ConnectionValidator::IsValidIdentifier(std::string_view identifier) {
  if (identifier.empty()) {
    return false;
  }
  for (char chr : identifier) {
    if (chr == '\0' || (std::isalnum(static_cast<unsigned char>(chr)) == 0 && chr != '_' && chr != '$' && chr != '-')) {
      return false;
    }
  }
  return true;
}

bool ConnectionValidator::IsSupportedBinlogChecksumValue(std::string_view value) {
  std::string upper_value(value);
  std::transform(upper_value.begin(), upper_value.end(), upper_value.begin(), ::toupper);
  return upper_value == "CRC32";
}

bool ConnectionValidator::ContainsTaggedGtid(std::string_view gtid_set) {
  while (!gtid_set.empty()) {
    auto comma_pos = gtid_set.find(',');
    std::string_view entry = gtid_set.substr(0, comma_pos);
    while (!entry.empty() && std::isspace(static_cast<unsigned char>(entry.front())) != 0) {
      entry.remove_prefix(1);
    }
    while (!entry.empty() && std::isspace(static_cast<unsigned char>(entry.back())) != 0) {
      entry.remove_suffix(1);
    }

    auto first_colon = entry.find(':');
    if (first_colon != std::string_view::npos) {
      std::string_view remainder = entry.substr(first_colon + 1);
      auto second_colon = remainder.find(':');
      if (second_colon != std::string_view::npos) {
        std::string_view first_token = remainder.substr(0, second_colon);
        if (!IsGtidIntervalToken(first_token)) {
          return true;
        }
      }
    }

    if (comma_pos == std::string_view::npos) {
      break;
    }
    gtid_set.remove_prefix(comma_pos + 1);
  }
  return false;
}

ValidationResult ConnectionValidator::ValidateServer(Connection& conn, const std::vector<std::string>& required_tables,
                                                     const std::optional<std::string>& expected_uuid,
                                                     const std::optional<std::string>& last_gtid) {
  std::vector<RequiredTable> qualified_tables;
  qualified_tables.reserve(required_tables.size());
  for (const auto& table : required_tables) {
    qualified_tables.push_back({conn.GetConfig().database, table});
  }
  return ValidateServer(conn, qualified_tables, expected_uuid, last_gtid);
}

ValidationResult ConnectionValidator::ValidateServer(Connection& conn,
                                                     const std::vector<RequiredTable>& required_tables,
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
        if (std::isalnum(static_cast<unsigned char>(chr)) == 0 && chr != '-' && chr != '_' && chr != ':' &&
            chr != ',' && chr != ' ' && chr != '\n' && chr != '\r') {
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

  // 8. Check binlog_checksum=CRC32 (required for event boundary and CRC verification)
  auto checksum_check = CheckBinlogChecksum(conn);
  if (!checksum_check) {
    result.error_message = checksum_check.error().message();
    mygram::utils::StructuredLog()
        .Event("connection_validation_failed")
        .Field("reason", "binlog_checksum_not_crc32")
        .Error();
    return result;
  }

  // 9. Check partial JSON mode (unsupported)
  auto partial_json_check = CheckPartialJsonMode(conn);
  if (!partial_json_check) {
    result.error_message = partial_json_check.error().message();
    mygram::utils::StructuredLog()
        .Event("connection_validation_failed")
        .Field("reason", "partial_json_enabled")
        .Error();
    return result;
  }

  // 10. Check tagged GTID support (unsupported)
  auto tagged_gtid_check = CheckTaggedGTIDSupport(conn);
  if (!tagged_gtid_check) {
    result.error_message = tagged_gtid_check.error().message();
    mygram::utils::StructuredLog()
        .Event("connection_validation_failed")
        .Field("reason", "tagged_gtid_unsupported")
        .Error();
    return result;
  }

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
    Connection& conn, const std::vector<RequiredTable>& tables) {
  std::vector<std::string> missing_tables;

  for (const auto& table : tables) {
    // Validate identifiers to prevent SQL injection
    // MySQL table names can contain: letters, digits, underscore, dollar sign, hyphen
    // Also reject null bytes which could truncate the name
    if (!IsValidIdentifier(table.database) || !IsValidIdentifier(table.name)) {
      mygram::utils::StructuredLog()
          .Event("connection_validation_warning")
          .Field("reason", "invalid_table_name")
          .Field("table", table.DisplayName())
          .Warn();
      missing_tables.push_back(table.DisplayName());
      continue;
    }

    // Use mysql_real_escape_string for defense-in-depth SQL injection prevention
    MYSQL* handle = conn.GetHandle();
    std::string escaped_db(table.database.size() * 2 + 1, '\0');
    std::string escaped_table(table.name.size() * 2 + 1, '\0');
    auto escaped_db_len =
        mysql_real_escape_string(handle, escaped_db.data(), table.database.c_str(), table.database.length());
    auto escaped_table_len =
        mysql_real_escape_string(handle, escaped_table.data(), table.name.c_str(), table.name.length());
    escaped_db.resize(escaped_db_len);
    escaped_table.resize(escaped_table_len);

    // Query INFORMATION_SCHEMA to check if table exists
    std::string query = "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '" + escaped_db +
                        "' AND TABLE_NAME = '" + escaped_table + "' LIMIT 1";

    auto result = conn.Execute(query);
    if (!result) {
      // Query failed - consider table as missing
      missing_tables.push_back(table.DisplayName());
      continue;
    }

    // Check if result has rows
    MYSQL_ROW row = mysql_fetch_row(result->get());
    if (row == nullptr) {
      // No rows - table doesn't exist
      missing_tables.push_back(table.DisplayName());
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
  auto uuid_result = conn.GetServerUUID();
  if (!uuid_result) {
    return mygram::utils::MakeUnexpected(uuid_result.error());
  }

  std::string actual_uuid = *uuid_result;

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

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckBinlogChecksum(Connection& conn) {
  auto result = conn.Execute("SHOW VARIABLES LIKE 'binlog_checksum'");
  if (!result) {
    return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
        mygram::utils::ErrorCode::kMySQLBinlogError,
        "Unable to determine binlog_checksum. MygramDB requires binlog_checksum=CRC32 because binlog event parsing "
        "expects a trailing 4-byte CRC32 checksum."));
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if (row == nullptr || row[1] == nullptr) {
    return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
        mygram::utils::ErrorCode::kMySQLBinlogError,
        "binlog_checksum is unavailable. MygramDB requires binlog_checksum=CRC32 because binlog event parsing expects "
        "a trailing 4-byte CRC32 checksum."));
  }

  std::string value(row[1]);
  if (!IsSupportedBinlogChecksumValue(value)) {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError,
                                 "binlog_checksum=" + value +
                                     " is not supported. MygramDB requires binlog_checksum=CRC32 for binlog event "
                                     "boundary handling and CRC verification. Set it with: SET GLOBAL "
                                     "binlog_checksum=CRC32"));
  }

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckPartialJsonMode(Connection& conn) {
  // MariaDB doesn't have binlog_row_value_options
  if (conn.GetFlavor() == ServerFlavor::kMariaDB) {
    return {};
  }

  auto result = conn.Execute("SHOW VARIABLES LIKE 'binlog_row_value_options'");
  if (!result) {
    return {};
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if (row == nullptr) {
    return {};
  }

  if (row[1] != nullptr) {
    std::string value(row[1]);
    // Check if PARTIAL_JSON is in the value (case-insensitive)
    std::string upper_value = value;
    std::transform(upper_value.begin(), upper_value.end(), upper_value.begin(), ::toupper);
    if (upper_value.find("PARTIAL_JSON") != std::string::npos) {
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
          mygram::utils::ErrorCode::kMySQLBinlogError,
          "binlog_row_value_options contains PARTIAL_JSON. PARTIAL_UPDATE_ROWS_EVENT is not supported and cannot be "
          "decoded safely. Disable it with: SET GLOBAL binlog_row_value_options=''"));
    }
  }

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckTaggedGTIDSupport(Connection& conn) {
  // Tagged GTIDs are a MySQL 8.4+ feature, not applicable to MariaDB
  if (conn.GetFlavor() == ServerFlavor::kMariaDB) {
    return {};
  }

  auto executed = conn.GetExecutedGTID();
  if (!executed) {
    return {};
  }

  if (ContainsTaggedGtid(*executed)) {
    return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
        mygram::utils::ErrorCode::kMySQLInvalidGTID,
        "Tagged GTIDs are not supported. MygramDB cannot encode UUID:TAG:GNO positions for MySQL binlog reconnect. "
        "Use an untagged GTID source or run a full resync from a compatible server."));
  }

  return {};
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
