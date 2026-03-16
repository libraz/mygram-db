/**
 * @file mysql_test_helpers.h
 * @brief Shared helper functions for MySQL integration tests
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdlib>
#include <string>

#include "mysql/connection.h"

namespace mygramdb::mysql::testing {

/**
 * @brief Check whether MySQL integration tests should run.
 * @return true if the environment variable ENABLE_MYSQL_INTEGRATION_TESTS is
 *         set to "1".
 */
inline bool ShouldRunMySQLIntegrationTests() {
  const char* env = std::getenv("ENABLE_MYSQL_INTEGRATION_TESTS");
  return env != nullptr && std::string(env) == "1";
}

/**
 * @brief Build a Connection::Config from environment variables.
 *
 * Reads the following environment variables (with defaults):
 *   - MYSQL_HOST     (default: "127.0.0.1")
 *   - MYSQL_USER     (default: "root")
 *   - MYSQL_PASSWORD (default: "")
 *   - MYSQL_DATABASE (default: "test")
 *
 * @return A Connection::Config populated from the environment.
 */
inline Connection::Config GetMySQLTestConfig() {
  Connection::Config config;

  const char* host = std::getenv("MYSQL_HOST");
  config.host = host != nullptr ? host : "127.0.0.1";

  config.port = 3306;

  const char* user = std::getenv("MYSQL_USER");
  config.user = user != nullptr ? user : "root";

  const char* password = std::getenv("MYSQL_PASSWORD");
  config.password = password != nullptr ? password : "";

  const char* database = std::getenv("MYSQL_DATABASE");
  config.database = database != nullptr ? database : "test";

  return config;
}

}  // namespace mygramdb::mysql::testing

#endif  // USE_MYSQL
