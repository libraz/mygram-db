/**
 * @file server_flavor.h
 * @brief MySQL/MariaDB server flavor detection
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>

namespace mygramdb::mysql {

/**
 * @brief Database server flavor
 */
enum class ServerFlavor : uint8_t {
  kMySQL,    ///< MySQL (including Percona Server)
  kMariaDB,  ///< MariaDB
};

/**
 * @brief Get human-readable name for server flavor
 */
inline const char* GetServerFlavorName(ServerFlavor flavor) {
  switch (flavor) {
    case ServerFlavor::kMySQL:
      return "MySQL";
    case ServerFlavor::kMariaDB:
      return "MariaDB";
    default:
      return "Unknown";
  }
}

/**
 * Return the query for the GTID position actually available in this server's
 * binary log. MariaDB gtid_current_pos also includes gtid_slave_pos, which may
 * describe transactions this server cannot serve to a binlog client.
 */
inline const char* GetBinlogExecutedPositionQuery(ServerFlavor flavor) {
  return flavor == ServerFlavor::kMariaDB ? "SELECT @@GLOBAL.gtid_binlog_pos" : "SELECT @@GLOBAL.gtid_executed";
}

inline constexpr unsigned int kMySQLUnknownSystemVariableError = 1193;

/**
 * Classify a probe for MariaDB's source-binlog GTID variable. A successful
 * query identifies MariaDB; MySQL reports ER_UNKNOWN_SYSTEM_VARIABLE. Any
 * other query or result-read failure remains unknown so callers fail closed.
 */
inline std::optional<ServerFlavor> ClassifyServerFlavorCapabilityProbe(bool query_succeeded, bool result_read_succeeded,
                                                                       unsigned int error_number) {
  if (query_succeeded) {
    return result_read_succeeded ? std::optional<ServerFlavor>(ServerFlavor::kMariaDB) : std::nullopt;
  }
  return error_number == kMySQLUnknownSystemVariableError ? std::optional<ServerFlavor>(ServerFlavor::kMySQL)
                                                          : std::nullopt;
}

}  // namespace mygramdb::mysql
