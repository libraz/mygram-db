/**
 * @file server_flavor.h
 * @brief MySQL/MariaDB server flavor detection
 */

#pragma once

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
 * @brief Detect server flavor from VERSION() string
 *
 * MariaDB's VERSION() contains "MariaDB" (e.g., "10.11.6-MariaDB", "11.4.0-MariaDB-1:11.4.0+maria~ubu2404").
 * MySQL's VERSION() is purely numeric (e.g., "8.4.7", "9.0.1").
 *
 * @param version_string Result of SELECT VERSION()
 * @return Detected server flavor
 */
inline ServerFlavor DetectServerFlavor(const std::string& version_string) {
  // MariaDB always includes "MariaDB" (case-insensitive check for robustness)
  // but in practice it's always capitalized as "MariaDB"
  if (version_string.find("MariaDB") != std::string::npos || version_string.find("mariadb") != std::string::npos) {
    return ServerFlavor::kMariaDB;
  }
  return ServerFlavor::kMySQL;
}

}  // namespace mygramdb::mysql
