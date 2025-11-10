/**
 * @file connection_integration_test.cpp
 * @brief Integration tests for MySQL connection (requires MySQL server)
 *
 * These tests require a running MySQL server and should be run separately
 * from unit tests. They are disabled by default and can be enabled with
 * the ENABLE_MYSQL_INTEGRATION_TESTS environment variable.
 */

#include "mysql/connection.h"
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <cstdlib>

#ifdef USE_MYSQL

using namespace mygramdb::mysql;

/**
 * @brief Check if integration tests should run
 */
bool ShouldRunIntegrationTests() {
  const char* env = std::getenv("ENABLE_MYSQL_INTEGRATION_TESTS");
  return env != nullptr && std::string(env) == "1";
}

/**
 * @brief Base class for MySQL integration tests
 */
class MySQLIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!ShouldRunIntegrationTests()) {
      GTEST_SKIP() << "MySQL integration tests are disabled. "
                   << "Set ENABLE_MYSQL_INTEGRATION_TESTS=1 to enable.";
    }
  }
};

/**
 * @brief Test actual MySQL connection
 */
TEST_F(MySQLIntegrationTest, ConnectToMySQL) {
  Connection::Config config;
  const char* host = std::getenv("MYSQL_HOST");
  config.host = host ? host : "127.0.0.1";
  config.port = 3306;
  const char* user = std::getenv("MYSQL_USER");
  config.user = user ? user : "root";
  const char* password = std::getenv("MYSQL_PASSWORD");
  config.password = password ? password : "";
  const char* database = std::getenv("MYSQL_DATABASE");
  config.database = database ? database : "test";

  Connection conn(config);

  EXPECT_TRUE(conn.Connect());
  EXPECT_TRUE(conn.IsConnected());

  // Note: Connection doesn't have explicit Disconnect(), it disconnects in destructor
}

/**
 * @brief Test MySQL ping
 */
TEST_F(MySQLIntegrationTest, PingMySQL) {
  Connection::Config config;
  const char* host = std::getenv("MYSQL_HOST");
  config.host = host ? host : "127.0.0.1";
  config.port = 3306;
  const char* user = std::getenv("MYSQL_USER");
  config.user = user ? user : "root";
  const char* password = std::getenv("MYSQL_PASSWORD");
  config.password = password ? password : "";

  Connection conn(config);
  ASSERT_TRUE(conn.Connect());

  EXPECT_TRUE(conn.Ping());
}

/**
 * @brief Test MySQL server UUID retrieval
 */
TEST_F(MySQLIntegrationTest, GetServerUUID) {
  Connection::Config config;
  const char* host = std::getenv("MYSQL_HOST");
  config.host = host ? host : "127.0.0.1";
  config.port = 3306;
  const char* user = std::getenv("MYSQL_USER");
  config.user = user ? user : "root";
  const char* password = std::getenv("MYSQL_PASSWORD");
  config.password = password ? password : "";

  Connection conn(config);
  ASSERT_TRUE(conn.Connect());

  auto uuid = conn.GetServerUUID();
  EXPECT_TRUE(uuid.has_value());
}

/**
 * @brief Test MySQL reconnection
 */
TEST_F(MySQLIntegrationTest, Reconnect) {
  Connection::Config config;
  const char* host = std::getenv("MYSQL_HOST");
  config.host = host ? host : "127.0.0.1";
  config.port = 3306;
  const char* user = std::getenv("MYSQL_USER");
  config.user = user ? user : "root";
  const char* password = std::getenv("MYSQL_PASSWORD");
  config.password = password ? password : "";

  Connection conn(config);
  ASSERT_TRUE(conn.Connect());

  EXPECT_TRUE(conn.Reconnect());
  EXPECT_TRUE(conn.IsConnected());
}

/**
 * @brief Test getting latest GTID from SHOW MASTER STATUS
 */
TEST_F(MySQLIntegrationTest, GetLatestGTID) {
  Connection::Config config;
  const char* host = std::getenv("MYSQL_HOST");
  config.host = host ? host : "127.0.0.1";
  config.port = 3306;
  const char* user = std::getenv("MYSQL_USER");
  config.user = user ? user : "root";
  const char* password = std::getenv("MYSQL_PASSWORD");
  config.password = password ? password : "";

  Connection conn(config);
  ASSERT_TRUE(conn.Connect());

  // Note: This test may fail if MySQL server doesn't have GTID enabled
  // or if the user doesn't have REPLICATION CLIENT privilege
  auto latest_gtid = conn.GetLatestGTID();

  // We don't assert true here because GTID might not be enabled on test server
  // Just verify the function doesn't crash
  if (latest_gtid.has_value()) {
    spdlog::info("Latest GTID: {}", latest_gtid.value());
    // Should be in format like "uuid:1-N" or empty
    EXPECT_FALSE(latest_gtid.value().empty());
  }
}

#endif  // USE_MYSQL
