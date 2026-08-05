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
#include "mysql/mariadb_gtid.h"
#include "mysql/server_flavor.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/numeric_parse.h"
#include "utils/string_utils.h"
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
  // MySQL permits quoted identifiers containing spaces, dots, non-ASCII
  // characters, and punctuation. These names are used as escaped string
  // values in the INFORMATION_SCHEMA query below, not interpolated as SQL
  // identifiers, so only empty and NUL-containing values are invalid here.
  return !identifier.empty() && identifier.find('\0') == std::string_view::npos;
}

bool ConnectionValidator::IsSupportedBinlogFormatValue(std::string_view value) {
  std::string upper_value(value);
  std::transform(upper_value.begin(), upper_value.end(), upper_value.begin(), ::toupper);
  return upper_value == "ROW";
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

  // 2. Check server UUID. A changed upstream is never accepted implicitly:
  // credentials and replication state belong to the originally validated
  // server, and explicit operator reconfiguration is required for failover.
  auto uuid_check = CheckServerUUID(conn, expected_uuid, result.warnings);
  if (!uuid_check) {
    result.error_message = uuid_check.error().message();
    result.error_code = uuid_check.error().code();
    mygram::utils::StructuredLog().Event("connection_validation_failed").Field("reason", "uuid_check_failed").Error();
    return result;
  }
  std::string actual_uuid = *uuid_check;
  result.server_uuid = actual_uuid;

  // 3. Check required tables exist
  auto tables_check = CheckTablesExist(conn, required_tables);
  if (!tables_check) {
    result.error_message = tables_check.error().message();
    result.error_code = tables_check.error().code();
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
    result.error_message = "GTID consistency check failed: " + gtid_consistency_check.error().message();
    mygram::utils::StructuredLog()
        .Event("connection_validation_failed")
        .Field("reason", "gtid_consistency")
        .Field("error", result.error_message)
        .Error();
    return result;
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
    // Reject only values that cannot represent a MySQL name. Punctuation and
    // non-ASCII names are safe because both values are escaped below before
    // being embedded as INFORMATION_SCHEMA string literals.
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
      // A transport/query failure says nothing about table existence. Preserve
      // it so reconnect recovery can retry instead of declaring the schema
      // permanently invalid.
      return mygram::utils::MakeUnexpected(result.error());
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
  static_cast<void>(warnings);
  auto uuid_result = conn.GetServerUUID();
  if (!uuid_result) {
    return mygram::utils::MakeUnexpected(uuid_result.error());
  }

  std::string actual_uuid = *uuid_result;

  // A UUID mismatch proves this is a different MySQL server. Do not continue
  // with the existing credential set or apply its binlog to this index.
  if (expected_uuid && *expected_uuid != actual_uuid) {
    mygram::utils::StructuredLog()
        .Event("mysql_server_uuid_mismatch")
        .Field("old_uuid", *expected_uuid)
        .Field("new_uuid", actual_uuid)
        .Error();
    return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
        mygram::utils::ErrorCode::kMySQLReplicationError,
        "MySQL server UUID changed from " + *expected_uuid + " to " + actual_uuid + "; refusing implicit failover"));
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

  if (!last_gtid || last_gtid->empty()) {
    return {};
  }

  // MariaDB has no GTID_SUBSET function. Compare the highest sequence for
  // every required domain; server IDs are allowed to change on failover.
  if (conn.GetFlavor() == ServerFlavor::kMariaDB) {
    auto covered = MariaDBGTID::PositionCovers(*last_gtid, *executed_gtid);
    if (!covered) {
      return MakeUnexpected(covered.error());
    }
    if (!*covered) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLReplicationError,
                                      "MariaDB candidate does not cover the processed GTID position. Required: " +
                                          *last_gtid + "; available: " + *executed_gtid,
                                      *last_gtid));
    }
    mygram::utils::StructuredLog()
        .Event("gtid_consistency_check")
        .Field("executed_gtid", *executed_gtid)
        .Field("flavor", "MariaDB")
        .Debug();
    return {};
  }

  // Validate GTID format before embedding the internally captured position in
  // SQL. Tagged GTID characters are allowed here and rejected separately when
  // the server advertises unsupported tagged transactions.
  const std::string& gtid_str = *last_gtid;
  for (char chr : gtid_str) {  // NOLINT(readability-identifier-length)
    if (std::isalnum(static_cast<unsigned char>(chr)) == 0 && chr != '-' && chr != '_' && chr != ':' && chr != ',' &&
        chr != ' ' && chr != '\n' && chr != '\r') {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID,
                                      "Invalid GTID format: contains illegal character '" + std::string(1, chr) + "'",
                                      gtid_str));
    }
  }

  // A safe hand-off requires both: the candidate has executed everything we
  // processed, and it has not purged any transaction absent from our position.
  // Query/result failures are hard errors; failover validation must never
  // silently continue when safety cannot be proven.
  std::string query = "SELECT GTID_SUBSET('" + gtid_str +
                      "', @@GLOBAL.gtid_executed) AS is_subset, "
                      "GTID_SUBTRACT(@@GLOBAL.gtid_purged, '" +
                      gtid_str + "') AS unavailable";
  auto result = conn.Execute(query);
  if (!result) {
    return MakeUnexpected(result.error());
  }
  MYSQL_ROW row = mysql_fetch_row(result->get());
  if (row == nullptr || row[0] == nullptr || row[1] == nullptr) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLQueryFailed, "GTID consistency query returned no usable row"));
  }
  if (std::string(row[0]) != "1") {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLReplicationError,
                                    "Candidate does not cover the processed GTID position: " + gtid_str, gtid_str));
  }
  const std::string unavailable(row[1]);
  if (!unavailable.empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLReplicationError,
                                    "Candidate has purged GTIDs absent from the processed position: " + unavailable +
                                        ". Run SYNC to establish a new position.",
                                    gtid_str));
  }

  // Log for debugging
  mygram::utils::StructuredLog()
      .Event("gtid_consistency_check")
      .Field("executed_gtid", *executed_gtid)
      .Field("unavailable_purged_gtid", unavailable)
      .Debug();

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckBinlogCompression(Connection& conn) {
  if (conn.GetFlavor() == ServerFlavor::kMariaDB) {
    auto result = conn.Execute("SHOW VARIABLES LIKE 'log_bin_compress'");
    if (!result) {
      return mygram::utils::MakeUnexpected(result.error());
    }
    MYSQL_ROW row = mysql_fetch_row(result->get());
    if (row != nullptr && row[1] == nullptr) {
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
          mygram::utils::ErrorCode::kMySQLBinlogError, "Unable to determine MariaDB log_bin_compress value."));
    }
    if (row != nullptr && row[1] != nullptr && mygram::utils::ToUpper(row[1]) == "ON") {
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
          mygram::utils::ErrorCode::kMySQLBinlogError,
          "MariaDB log_bin_compress=ON is not supported. Disable it before starting MygramDB."));
    }
    return {};
  }

  auto result = conn.Execute("SHOW VARIABLES LIKE 'binlog_transaction_compression'");
  if (!result) {
    // A missing variable is represented by a successful empty result. A query
    // error means capability validation could not be completed.
    return mygram::utils::MakeUnexpected(result.error());
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if (row == nullptr) {
    // Variable not found - OK (MySQL < 8.0.20)
    return {};
  }
  if (row[1] == nullptr) {
    return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
        mygram::utils::ErrorCode::kMySQLBinlogError, "Unable to determine binlog_transaction_compression value."));
  }

  // row[0] = variable name, row[1] = value
  if (std::string(row[1]) == "ON") {
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
    return mygram::utils::MakeUnexpected(result.error());
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if (row == nullptr || row[1] == nullptr) {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError,
                                 "Unable to determine binlog_row_image. MygramDB requires binlog_row_image=FULL."));
  }

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

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckBinlogFormat(Connection& conn) {
  auto result = conn.Execute("SHOW VARIABLES LIKE 'binlog_format'");
  if (!result) {
    return mygram::utils::MakeUnexpected(result.error());
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if (row == nullptr || row[1] == nullptr) {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError,
                                 "Unable to determine binlog_format. MygramDB requires binlog_format=ROW."));
  }

  std::string value(row[1]);
  if (!IsSupportedBinlogFormatValue(value)) {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError,
                                 "binlog_format=" + value +
                                     " is not supported. "
                                     "MygramDB requires binlog_format=ROW for row-level replication. "
                                     "Set it with: SET GLOBAL binlog_format=ROW"));
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
    return mygram::utils::MakeUnexpected(result.error());
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if (row == nullptr) {
    return {};
  }
  if (row[1] == nullptr) {
    return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
        mygram::utils::ErrorCode::kMySQLBinlogError, "Unable to determine binlog_row_value_options value."));
  }

  {
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
    // A transient query failure must not turn an unsupported tagged GTID set
    // into a successful validation. Runtime parsing also fails closed, but
    // rejecting here keeps startup/failover transactional and predictable.
    return mygram::utils::MakeUnexpected(executed.error());
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
