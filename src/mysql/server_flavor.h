/**
 * @file server_flavor.h
 * @brief MySQL/MariaDB server flavor, as libmybinlog defines it
 */

#pragma once

#include <optional>

#include "mybinlog/server_flavor.h"

namespace mygramdb::mysql {

/// Database server flavor.
using ServerFlavor = mybinlog::ServerFlavor;

/**
 * @brief Get human-readable name for server flavor
 */
inline const char* GetServerFlavorName(ServerFlavor flavor) {
  return mybinlog::ServerFlavorName(flavor);
}

/**
 * Return the query for the GTID position actually available in this server's
 * binary log (defined in connection.cpp).
 */
const char* GetBinlogExecutedPositionQuery(ServerFlavor flavor);

inline constexpr unsigned int kMySQLUnknownSystemVariableError = mybinlog::kErUnknownSystemVariable;

/**
 * Classify a probe for MariaDB's source-binlog GTID variable; see mybinlog::ClassifyFlavorProbe.
 */
inline std::optional<ServerFlavor> ClassifyServerFlavorCapabilityProbe(bool query_succeeded, bool result_read_succeeded,
                                                                       unsigned int error_number) {
  return mybinlog::ClassifyFlavorProbe(query_succeeded, result_read_succeeded, error_number);
}

}  // namespace mygramdb::mysql
