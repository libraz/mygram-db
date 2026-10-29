/**
 * @file connection_validator.cpp
 * @brief MySQL connection validator implementation
 */

#ifdef USE_MYSQL

#include "mysql/connection_validator.h"

#include <spdlog/spdlog.h>

#include <initializer_list>

#include "mybinlog/mariadb_gtid.h"
#include "mybinlog/mysql_gtid.h"
#include "mybinlog/server_settings.h"
#include "mysql/connection.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/sql_utils.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::mysql {

namespace {

using mybinlog::RuleId;
using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::MakeError;

/// SHOW VARIABLES for exactly one name.
std::string ShowVariableQuery(std::string_view name) {
  // LIKE takes only a plain quoted literal; WHERE takes the encoded expression and has no wildcards.
  return "SHOW VARIABLES WHERE Variable_name = " + mygramdb::utils::EncodeMySQLStringLiteral(name);
}

/// Read one server variable; nullopt when the server does not define it.
mygram::utils::Expected<std::optional<std::string>, Error> ReadServerVariable(Connection& conn, std::string_view name) {
  auto result = conn.Execute(ShowVariableQuery(name));
  if (!result) {
    return mygram::utils::MakeUnexpected(result.error());
  }
  MYSQL_ROW row = mysql_fetch_row(result->get());
  if (row == nullptr || row[1] == nullptr) {
    return std::optional<std::string>{};
  }
  return std::optional<std::string>{row[1]};
}

/// Lookup context for EvaluateServerSettings that queries a live server.
struct LiveVariableLookup {
  Connection* conn = nullptr;
  std::optional<Error> error;  ///< A failed query or a NULL value; the rule cannot be judged.
};

bool LookupLiveVariable(void* ctx, std::string_view variable, std::string* value) {
  auto* lookup = static_cast<LiveVariableLookup*>(ctx);
  auto result = lookup->conn->Execute(ShowVariableQuery(variable));
  if (!result) {
    lookup->error = result.error();
    return false;
  }
  MYSQL_ROW row = mysql_fetch_row(result->get());
  if (row == nullptr) {
    return false;
  }
  if (row[1] == nullptr) {
    lookup->error = MakeError(ErrorCode::kMySQLBinlogError, "Unable to determine " + std::string(variable) + " value.");
    return false;
  }
  *value = row[1];
  return true;
}

/// Lookup that answers every variable with one value already read; nullopt is an absent variable.
bool LookupKnownValue(void* ctx, std::string_view /*variable*/, std::string* value) {
  const auto* known = static_cast<const std::optional<std::string>*>(ctx);
  if (!known->has_value()) {
    return false;
  }
  *value = **known;
  return true;
}

/// The error MygramDB reports for a violated server-setting rule.
Error ViolationError(const mybinlog::Violation& violation) {
  const std::string& name = violation.variable;
  const std::string& value = violation.observed;
  switch (violation.id) {
    case RuleId::kLogBin:
      return MakeError(ErrorCode::kMySQLBinlogError,
                       violation.absent ? "Unable to determine log_bin. MygramDB requires log_bin=ON."
                                        : "log_bin=" + value +
                                              " is not supported. MygramDB reads the binary log; enable it in the "
                                              "server configuration and restart the server.");
    case RuleId::kGtidMode:
      return MakeError(ErrorCode::kMySQLGTIDNotEnabled, "GTID mode is not enabled on MySQL server (gtid_mode != ON)");
    case RuleId::kBinlogFormat:
      return MakeError(ErrorCode::kMySQLBinlogError,
                       violation.absent ? "Unable to determine binlog_format. MygramDB requires binlog_format=ROW."
                                        : "binlog_format=" + value +
                                              " is not supported. "
                                              "MygramDB requires binlog_format=ROW for row-level replication. "
                                              "Set it with: SET GLOBAL binlog_format=ROW");
    case RuleId::kBinlogRowImage:
      return MakeError(ErrorCode::kMySQLBinlogError,
                       violation.absent
                           ? "Unable to determine binlog_row_image. MygramDB requires binlog_row_image=FULL."
                           : "binlog_row_image=" + value +
                                 " is not supported. "
                                 "MygramDB requires binlog_row_image=FULL for correct NULL bitmap parsing. "
                                 "Set it with: SET GLOBAL binlog_row_image=FULL");
    case RuleId::kNoTransactionCompression:
      return MakeError(ErrorCode::kMySQLBinlogError,
                       "binlog_transaction_compression=ON is not supported. "
                       "TRANSACTION_PAYLOAD_EVENT (compressed binlog events) cannot be decoded. "
                       "Disable compression with: SET GLOBAL binlog_transaction_compression=OFF");
    case RuleId::kNoPartialJson:
      return MakeError(ErrorCode::kMySQLBinlogError,
                       "binlog_row_value_options contains PARTIAL_JSON. PARTIAL_UPDATE_ROWS_EVENT is not supported "
                       "and cannot be decoded safely. Disable it with: SET GLOBAL binlog_row_value_options=''");
    case RuleId::kNoLogBinCompress:
      return MakeError(ErrorCode::kMySQLBinlogError,
                       "MariaDB log_bin_compress=ON is not supported. Disable it before starting MygramDB.");
    case RuleId::kChecksumCrc32:
      return MakeError(ErrorCode::kMySQLBinlogError,
                       violation.absent ? "binlog_checksum is unavailable. MygramDB requires binlog_checksum=CRC32 so "
                                          "that every replicated event is integrity-checked."
                                        : "binlog_checksum=" + value +
                                              " is not supported. MygramDB requires binlog_checksum=CRC32 so that "
                                              "every replicated event is integrity-checked. Set it with: SET GLOBAL "
                                              "binlog_checksum=CRC32");
    case RuleId::kRowMetadataFull:
      return MakeError(ErrorCode::kMySQLBinlogError,
                       violation.absent
                           ? "Unable to determine binlog_row_metadata. MygramDB requires binlog_row_metadata=FULL."
                           : "binlog_row_metadata=" + value +
                                 " is not supported. MygramDB requires binlog_row_metadata=FULL so that TABLE_MAP "
                                 "events carry column names. Set it with: SET GLOBAL binlog_row_metadata=FULL");
    case RuleId::kLogReplicaUpdates:
      return MakeError(
          ErrorCode::kMySQLBinlogError,
          violation.absent
              ? "Unable to determine " + name + " on a server that is itself a replica."
              : name + "=" + value +
                    " is not supported on a server that is itself a replica. Replicated writes never reach its "
                    "binary log, so MygramDB would stop seeing changes. Enable it in the server configuration and "
                    "restart the server.");
  }
  return MakeError(ErrorCode::kMySQLBinlogError,
                   std::string("Server setting rule ") + mybinlog::RuleIdName(violation.id) + " is not satisfied.");
}

/// Evaluate one rule; the first violation, or nullopt when it holds or does not apply.
std::optional<mybinlog::Violation> EvaluateRule(RuleId rule, mybinlog::ServerFlavor flavor,
                                                mybinlog::VariableLookupFn lookup, void* ctx) {
  std::vector<mybinlog::Violation> violations;
  mybinlog::EvaluateServerSettings(lookup, ctx, flavor, &rule, 1, &violations);
  if (violations.empty()) {
    return std::nullopt;
  }
  return violations.front();
}

/// Evaluate the given rules against the server in order and report the first that fails.
mygram::utils::Expected<void, Error> CheckServerSettings(Connection& conn, std::initializer_list<RuleId> rules) {
  for (const RuleId rule : rules) {
    LiveVariableLookup lookup{&conn, std::nullopt};
    auto violation = EvaluateRule(rule, conn.GetFlavor(), &LookupLiveVariable, &lookup);
    if (lookup.error.has_value()) {
      return mygram::utils::MakeUnexpected(*lookup.error);
    }
    if (violation.has_value()) {
      return mygram::utils::MakeUnexpected(ViolationError(*violation));
    }
  }
  return {};
}

/// Whether a value read for a rule's variable satisfies the rule.
bool SatisfiesRule(RuleId rule, std::string_view value) {
  std::optional<std::string> known{std::string(value)};
  // The rules judged this way apply to both flavors.
  return !EvaluateRule(rule, mybinlog::ServerFlavor::kMySQL, &LookupKnownValue, &known).has_value();
}

/// MariaDB PositionCovers over position text; kMariaDBInvalidGTID when either side does not parse.
mygram::utils::Expected<bool, Error> MariaDBPositionCovers(const std::string& required, const std::string& available) {
  auto invalid = [](const std::string& text, mybinlog::Status status) {
    return mygram::utils::MakeUnexpected(
        MakeError(ErrorCode::kMariaDBInvalidGTID,
                  std::string("Invalid MariaDB GTID position (") + mybinlog::StatusName(status) + ")", text));
  };
  mybinlog::MariaDbGtidPosition required_position;
  mybinlog::MariaDbGtidPosition available_position;
  if (const auto status = mybinlog::MariaDbGtidPosition::Parse(required, &required_position);
      status != mybinlog::Status::kOk) {
    return invalid(required, status);
  }
  if (const auto status = mybinlog::MariaDbGtidPosition::Parse(available, &available_position);
      status != mybinlog::Status::kOk) {
    return invalid(available, status);
  }
  return mybinlog::PositionCovers(required_position, available_position);
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
  return SatisfiesRule(RuleId::kBinlogFormat, value);
}

bool ConnectionValidator::IsSupportedBinlogChecksumValue(std::string_view value) {
  return SatisfiesRule(RuleId::kChecksumCrc32, value);
}

bool ConnectionValidator::IsSupportedBinlogRowMetadataValue(std::string_view value) {
  return SatisfiesRule(RuleId::kRowMetadataFull, value);
}

bool ConnectionValidator::HasAppliedReplicatedGtids(std::string_view gtid_slave_pos) {
  return !mygram::utils::TrimAsciiWhitespaceView(gtid_slave_pos).empty();
}

bool ConnectionValidator::IsBinlogStorageEngineEnabled(std::string_view value) {
  return !mygram::utils::TrimAsciiWhitespaceView(value).empty();
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::EvaluateLogReplicaUpdates(
    mybinlog::ServerFlavor flavor, bool is_replica, const std::optional<std::string>& value) {
  if (!is_replica) {
    return {};
  }
  std::optional<std::string> known = value;
  auto violation = EvaluateRule(RuleId::kLogReplicaUpdates, flavor, &LookupKnownValue, &known);
  if (violation.has_value()) {
    return mygram::utils::MakeUnexpected(ViolationError(*violation));
  }
  return {};
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
    result.error_code = mygram::utils::ErrorCode::kMySQLDisconnected;
    return result;
  }

  // 1. Check the binary log and GTID mode
  auto gtid_check = CheckGTIDEnabled(conn);
  if (!gtid_check) {
    result.error_message = gtid_check.error().message();
    result.error_code = gtid_check.error().code();
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
    result.error_code = gtid_consistency_check.error().code();
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
    result.error_code = compression_check.error().code();
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
    result.error_code = row_image_check.error().code();
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
    result.error_code = format_check.error().code();
    mygram::utils::StructuredLog()
        .Event("connection_validation_failed")
        .Field("reason", "binlog_format_not_row")
        .Error();
    return result;
  }

  // 8. Check binlog_checksum=CRC32 (every new event must be integrity-checked)
  auto checksum_check = CheckBinlogChecksum(conn);
  if (!checksum_check) {
    result.error_message = checksum_check.error().message();
    result.error_code = checksum_check.error().code();
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
    result.error_code = partial_json_check.error().code();
    mygram::utils::StructuredLog()
        .Event("connection_validation_failed")
        .Field("reason", "partial_json_enabled")
        .Error();
    return result;
  }

  // 10. Check binlog_row_metadata=FULL (column names must come from TABLE_MAP)
  auto row_metadata_check = CheckBinlogRowMetadata(conn);
  if (!row_metadata_check) {
    result.error_message = row_metadata_check.error().message();
    result.error_code = row_metadata_check.error().code();
    mygram::utils::StructuredLog()
        .Event("connection_validation_failed")
        .Field("reason", "binlog_row_metadata_not_full")
        .Error();
    return result;
  }

  // 11. Check MariaDB engine-integrated binlog (unsupported)
  auto storage_engine_check = CheckBinlogStorageEngine(conn);
  if (!storage_engine_check) {
    result.error_message = storage_engine_check.error().message();
    result.error_code = storage_engine_check.error().code();
    mygram::utils::StructuredLog()
        .Event("connection_validation_failed")
        .Field("reason", "binlog_storage_engine_enabled")
        .Error();
    return result;
  }

  // 12. Check that a replica source logs replicated writes
  auto log_updates_check = CheckLogReplicaUpdates(conn);
  if (!log_updates_check) {
    result.error_message = log_updates_check.error().message();
    result.error_code = log_updates_check.error().code();
    mygram::utils::StructuredLog()
        .Event("connection_validation_failed")
        .Field("reason", "log_replica_updates_off")
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
  return CheckServerSettings(conn, {RuleId::kLogBin, RuleId::kGtidMode});
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
    auto covered = MariaDBPositionCovers(*last_gtid, *executed_gtid);
    if (!covered) {
      return MakeUnexpected(covered.error());
    }
    if (!*covered) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLReplicationError,
                                      "MariaDB candidate does not cover the processed GTID position. Required: " +
                                          *last_gtid + "; available: " + *executed_gtid,
                                      *last_gtid));
    }

    // MariaDB has no gtid_purged. The position at the start of the oldest
    // binlog is the newest transaction the server can no longer send.
    std::string oldest_binlog;
    {
      auto logs = conn.Execute("SHOW BINARY LOGS");
      if (!logs) {
        return MakeUnexpected(logs.error());
      }
      MYSQL_ROW row = mysql_fetch_row(logs->get());
      if (row == nullptr || row[0] == nullptr) {
        return MakeUnexpected(MakeError(ErrorCode::kMySQLQueryFailed, "SHOW BINARY LOGS returned no binlog file"));
      }
      oldest_binlog = row[0];
    }
    auto boundary_result =
        conn.Execute("SELECT BINLOG_GTID_POS(" + mygramdb::utils::EncodeMySQLStringLiteral(oldest_binlog) + ", 4)");
    if (!boundary_result) {
      return MakeUnexpected(boundary_result.error());
    }
    MYSQL_ROW boundary_row = mysql_fetch_row(boundary_result->get());
    if (boundary_row == nullptr || boundary_row[0] == nullptr) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLQueryFailed,
                                      "BINLOG_GTID_POS returned no position for the oldest binlog " + oldest_binlog));
    }
    const std::string purge_boundary(boundary_row[0]);
    auto not_purged = CheckMariaDBPositionNotPurged(*last_gtid, purge_boundary);
    if (!not_purged) {
      return not_purged;
    }

    mygram::utils::StructuredLog()
        .Event("gtid_consistency_check")
        .Field("executed_gtid", *executed_gtid)
        .Field("purge_boundary", purge_boundary)
        .Field("flavor", "MariaDB")
        .Debug();
    return {};
  }

  auto canonical_position = CanonicalMySQLGtidPosition(*last_gtid);
  if (!canonical_position) {
    return MakeUnexpected(canonical_position.error());
  }
  const std::string& gtid_str = *canonical_position;

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

mygram::utils::Expected<std::string, mygram::utils::Error> ConnectionValidator::CanonicalMySQLGtidPosition(
    std::string_view position) {
  mybinlog::GtidSet parsed;
  if (const auto status = mybinlog::GtidSet::Parse(position, &parsed); status != mybinlog::Status::kOk) {
    return mygram::utils::MakeUnexpected(
        MakeError(ErrorCode::kMySQLInvalidGTID, std::string("Invalid GTID set (") + mybinlog::StatusName(status) + ")",
                  std::string(position)));
  }
  return FormatGtidSet(parsed);
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckMariaDBPositionNotPurged(
    const std::string& processed_position, const std::string& purge_boundary) {
  auto covered = MariaDBPositionCovers(purge_boundary, processed_position);
  if (!covered) {
    return mygram::utils::MakeUnexpected(covered.error());
  }
  if (!*covered) {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLReplicationError,
                                 "Candidate has purged GTIDs absent from the processed position: binlogs before " +
                                     purge_boundary + " are gone. Run SYNC to establish a new position.",
                                 processed_position));
  }
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckBinlogCompression(Connection& conn) {
  return CheckServerSettings(conn, {RuleId::kNoTransactionCompression, RuleId::kNoLogBinCompress});
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckBinlogRowImage(Connection& conn) {
  return CheckServerSettings(conn, {RuleId::kBinlogRowImage});
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckBinlogFormat(Connection& conn) {
  return CheckServerSettings(conn, {RuleId::kBinlogFormat});
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckBinlogChecksum(Connection& conn) {
  return CheckServerSettings(conn, {RuleId::kChecksumCrc32});
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckPartialJsonMode(Connection& conn) {
  return CheckServerSettings(conn, {RuleId::kNoPartialJson});
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckBinlogRowMetadata(Connection& conn) {
  return CheckServerSettings(conn, {RuleId::kRowMetadataFull});
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckBinlogStorageEngine(Connection& conn) {
  if (conn.GetFlavor() != ServerFlavor::kMariaDB) {
    return {};
  }
  auto value = ReadServerVariable(conn, "binlog_storage_engine");
  if (!value) {
    return mygram::utils::MakeUnexpected(value.error());
  }
  if (value->has_value() && IsBinlogStorageEngineEnabled(**value)) {
    return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
        mygram::utils::ErrorCode::kMySQLBinlogError,
        "binlog_storage_engine=" + **value +
            " is not supported. The engine-integrated binary log has no per-event checksum and cannot be probed "
            "for purged positions. Remove binlog_storage_engine from the server configuration."));
  }
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ConnectionValidator::CheckLogReplicaUpdates(Connection& conn) {
  const bool mariadb = conn.GetFlavor() == ServerFlavor::kMariaDB;
  bool is_replica = false;

  if (mariadb) {
    // The SQL thread records every replicated GTID group in gtid_slave_pos, in
    // GTID and in file/position mode alike, and reading the variable needs no
    // privilege (SHOW ALL SLAVES STATUS needs REPLICA MONITOR).
    auto slave_pos = conn.Execute("SELECT @@GLOBAL.gtid_slave_pos");
    if (!slave_pos) {
      return mygram::utils::MakeUnexpected(slave_pos.error());
    }
    MYSQL_ROW row = mysql_fetch_row(slave_pos->get());
    is_replica = row != nullptr && row[0] != nullptr && HasAppliedReplicatedGtids(row[0]);
  } else {
    // SHOW REPLICA STATUS needs REPLICATION CLIENT. When it is refused the
    // replica role cannot be ruled out, so validation fails.
    auto status = conn.Execute("SHOW REPLICA STATUS");
    if (!status) {
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
          status.error().code(), "Unable to determine whether the server is a replica: " + status.error().message() +
                                     " (SHOW REPLICA STATUS needs the REPLICATION CLIENT privilege)"));
    }
    is_replica = mysql_fetch_row(status->get()) != nullptr;
  }
  if (!is_replica) {
    return {};
  }

  const auto& rule = mybinlog::GetRule(RuleId::kLogReplicaUpdates);
  auto value = ReadServerVariable(conn, mariadb ? rule.mariadb_variable : rule.mysql_variable);
  if (!value) {
    return mygram::utils::MakeUnexpected(value.error());
  }
  return EvaluateLogReplicaUpdates(conn.GetFlavor(), is_replica, *value);
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
