/**
 * @file connection_validator.cpp
 * @brief MySQL connection validator implementation
 */

#ifdef USE_MYSQL

#include "mysql/connection_validator.h"

#include <spdlog/spdlog.h>

#include "mysql/connection.h"
#include "utils/structured_log.h"

namespace mygramdb::mysql {

ValidationResult ConnectionValidator::ValidateServer(Connection& conn, const std::vector<std::string>& required_tables,
                                                     const std::optional<std::string>& expected_uuid) {
  ValidationResult result;

  // Check connection status
  if (!conn.IsConnected()) {
    result.error_message = "Connection is not active";
    return result;
  }

  // 1. Check GTID mode
  std::string gtid_error;
  if (!CheckGTIDEnabled(conn, gtid_error)) {
    result.error_message = gtid_error;
    mygram::utils::StructuredLog()
        .Event("connection_validation_failed")
        .Field("reason", "gtid_disabled")
        .Field("error", gtid_error)
        .Error();
    return result;
  }

  // 2. Check server UUID and detect failover
  std::string actual_uuid;
  if (!CheckServerUUID(conn, expected_uuid, actual_uuid, result.warnings)) {
    result.error_message = "Failed to retrieve server UUID";
    mygram::utils::StructuredLog().Event("connection_validation_failed").Field("reason", "uuid_check_failed").Error();
    return result;
  }
  result.server_uuid = actual_uuid;

  // 3. Check required tables exist
  std::vector<std::string> missing_tables;
  if (!CheckTablesExist(conn, required_tables, missing_tables)) {
    result.error_message = "Required tables are missing: ";
    for (size_t i = 0; i < missing_tables.size(); ++i) {
      if (i > 0) {
        result.error_message += ", ";
      }
      result.error_message += missing_tables[i];
    }
    mygram::utils::StructuredLog()
        .Event("connection_validation_failed")
        .Field("reason", "missing_tables")
        .Field("missing_count", static_cast<int64_t>(missing_tables.size()))
        .Error();
    return result;
  }

  // 4. Check GTID consistency (if we have an expected state)
  std::string gtid_consistency_error;
  if (!CheckGTIDConsistency(conn, std::nullopt, gtid_consistency_error)) {
    result.warnings.push_back("GTID consistency check: " + gtid_consistency_error);
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

bool ConnectionValidator::CheckGTIDEnabled(Connection& conn, std::string& error) {
  if (!conn.IsGTIDModeEnabled()) {
    error = "GTID mode is not enabled on MySQL server (gtid_mode != ON)";
    return false;
  }
  return true;
}

bool ConnectionValidator::CheckTablesExist(Connection& conn, const std::vector<std::string>& tables,
                                           std::vector<std::string>& missing_tables) {
  missing_tables.clear();

  for (const auto& table : tables) {
    // Query INFORMATION_SCHEMA to check if table exists
    std::string query = "SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '" +
                        table + "' LIMIT 1";

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

  return missing_tables.empty();
}

bool ConnectionValidator::CheckServerUUID(Connection& conn, const std::optional<std::string>& expected_uuid,
                                          std::string& actual_uuid, std::vector<std::string>& warnings) {
  auto uuid_opt = conn.GetServerUUID();
  if (!uuid_opt) {
    return false;
  }

  actual_uuid = *uuid_opt;

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

  return true;
}

bool ConnectionValidator::CheckGTIDConsistency(Connection& conn, const std::optional<std::string>& last_gtid,
                                               std::string& error) {
  // Get current executed GTID set
  auto executed_gtid = conn.GetExecutedGTID();
  if (!executed_gtid) {
    error = "Failed to retrieve executed GTID set";
    return false;
  }

  // Get purged GTID set
  auto purged_gtid = conn.GetPurgedGTID();
  if (!purged_gtid) {
    error = "Failed to retrieve purged GTID set";
    return false;
  }

  // If we have a last GTID, check if it's in the purged set
  // This would indicate we can't continue replication from where we left off
  if (last_gtid && !purged_gtid->empty() && !last_gtid->empty()) {
    // Simple check: if purged set is not empty, warn that some GTIDs may be unavailable
    // A more sophisticated check would parse the GTID sets and compare ranges
    mygram::utils::StructuredLog()
        .Event("gtid_consistency_check")
        .Field("executed_gtid", *executed_gtid)
        .Field("purged_gtid", *purged_gtid)
        .Debug();
  }

  return true;
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
