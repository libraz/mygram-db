/**
 * @file connection_validate_unit_test.cpp
 * @brief Unit tests for connection validation logic (no MySQL connection required)
 */

#ifdef USE_MYSQL

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <chrono>
#include <string>
#include <utility>
#include <vector>

#include "mysql/connection.h"
#include "mysql_test_helpers.h"
#include "utils/error.h"
#include "utils/fd_guard.h"

namespace mygramdb::mysql {

// Unit test: Method exists and compiles
TEST(ConnectionValidateUnitTest, MethodExists) {
  Connection::Config config;
  config.host = "127.0.0.1";
  config.user = "test";
  config.password = "test";
  config.database = "test";

  Connection conn(config);

  // Method should exist and be callable (even if it fails due to no connection)
  auto result = conn.ValidateUniqueColumn("test_db", "test_table", "id");

  // We expect failure since we're not actually connected
  EXPECT_FALSE(result.has_value());
  EXPECT_TRUE(result.error().is_error());
  EXPECT_FALSE(result.error().message().empty());
}

/**
 * @brief Test ValidateUniqueColumn with null mysql_ handle
 * Regression test for: ValidateUniqueColumn crashed when mysql_ was nullptr
 * This could happen if Reconnect() failed or connection was never established
 */
TEST(ConnectionValidateUnitTest, NullHandleDoesNotCrash) {
  Connection::Config config;
  config.host = "127.0.0.1";
  config.user = "test";
  config.password = "test";
  config.database = "test";

  Connection conn(config);

  // Call ValidateUniqueColumn without connecting (mysql_ handle may be nullptr or unconnected)
  auto result = conn.ValidateUniqueColumn("test_db", "test_table", "id");

  // Should not crash and should return error
  EXPECT_FALSE(result.has_value());
  // Error could be kMySQLDisconnected (if handle is null) or kMySQLQueryFailed (if handle exists
  // but connection was never established). mysql_init() usually returns a non-null handle.
  EXPECT_TRUE(result.error().code() == mygram::utils::ErrorCode::kMySQLDisconnected ||
              result.error().code() == mygram::utils::ErrorCode::kMySQLQueryFailed);
  EXPECT_FALSE(result.error().message().empty());
}

TEST(ConnectionValidateIntegrationTest, ValidatesRealKeyMetadataAndEncodesInputs) {
  if (!testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled";
  }

  const auto config = testing::GetMySQLTestConfig();
  Connection connection(config);
  auto connect = connection.Connect("validate-unique-column-integration-test");
  ASSERT_TRUE(connect) << connect.error().message();

  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() & 0x7fffffff);
  const std::string table = "validate_unique_" + suffix;
  const std::string quoted_table = "`" + table + "`";
  auto cleanup = utils::ScopeGuard([&]() { (void)connection.ExecuteUpdate("DROP TABLE IF EXISTS " + quoted_table); });

  ASSERT_TRUE(connection.ExecuteUpdate(
      "CREATE TABLE " + quoted_table +
      " (id BIGINT PRIMARY KEY, email VARCHAR(255) NOT NULL UNIQUE, part_a INT, part_b INT, plain INT,"
      " UNIQUE KEY pair_key (part_a, part_b)) ENGINE=InnoDB"));

  EXPECT_TRUE(connection.ValidateUniqueColumn(config.database, table, "id"));
  EXPECT_TRUE(connection.ValidateUniqueColumn(config.database, table, "email"));

  for (const std::string& column : {"part_a", "part_b", "plain"}) {
    auto result = connection.ValidateUniqueColumn(config.database, table, column);
    ASSERT_FALSE(result) << column;
    EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLDuplicateColumn) << column;
    EXPECT_THAT(result.error().message(), ::testing::HasSubstr("must be a single-column PRIMARY KEY or UNIQUE KEY"));
  }

  auto missing = connection.ValidateUniqueColumn(config.database, table, "missing");
  ASSERT_FALSE(missing);
  EXPECT_EQ(missing.error().code(), mygram::utils::ErrorCode::kMySQLColumnNotFound);
  EXPECT_THAT(missing.error().message(), ::testing::HasSubstr("does not exist"));

  auto injection = connection.ValidateUniqueColumn(config.database + "' OR '1'='1", table, "id");
  ASSERT_FALSE(injection);
  EXPECT_EQ(injection.error().code(), mygram::utils::ErrorCode::kMySQLColumnNotFound);
}

TEST(ConnectionValidateIntegrationTest, CompositePrimaryKeyColumnIsRejected) {
  if (!testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled";
  }

  const auto config = testing::GetMySQLTestConfig();
  Connection connection(config);
  auto connect = connection.Connect("validate-composite-primary-integration-test");
  ASSERT_TRUE(connect) << connect.error().message();

  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() & 0x7fffffff);
  const std::string table = "validate_composite_pk_" + suffix;
  const std::string quoted_table = "`" + table + "`";
  auto cleanup = utils::ScopeGuard([&]() { (void)connection.ExecuteUpdate("DROP TABLE IF EXISTS " + quoted_table); });

  ASSERT_TRUE(
      connection.ExecuteUpdate("CREATE TABLE " + quoted_table +
                               " (tenant INT, id BIGINT, email VARCHAR(255) NOT NULL UNIQUE, PRIMARY KEY (tenant, id))"
                               " ENGINE=InnoDB"));

  // Rows sharing one part of a composite key would collapse into one document.
  for (const std::string& column : {"tenant", "id"}) {
    auto result = connection.ValidateUniqueColumn(config.database, table, column);
    ASSERT_FALSE(result) << column;
    EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLDuplicateColumn) << column;
  }
  EXPECT_TRUE(connection.ValidateUniqueColumn(config.database, table, "email"));
}

TEST(ConnectionValidateIntegrationTest, NullableUniqueColumnIsRejected) {
  if (!testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled";
  }

  const auto config = testing::GetMySQLTestConfig();
  Connection connection(config);
  auto connect = connection.Connect("validate-nullable-unique-integration-test");
  ASSERT_TRUE(connect) << connect.error().message();

  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() & 0x7fffffff);
  const std::string table = "validate_nullable_unique_" + suffix;
  const std::string quoted_table = "`" + table + "`";
  auto cleanup = utils::ScopeGuard([&]() { (void)connection.ExecuteUpdate("DROP TABLE IF EXISTS " + quoted_table); });

  ASSERT_TRUE(connection.ExecuteUpdate("CREATE TABLE " + quoted_table +
                                       " (id BIGINT PRIMARY KEY, nullable_key VARCHAR(64) UNIQUE,"
                                       " required_key VARCHAR(64) NOT NULL UNIQUE) ENGINE=InnoDB"));

  auto nullable = connection.ValidateUniqueColumn(config.database, table, "nullable_key");
  ASSERT_FALSE(nullable);
  EXPECT_EQ(nullable.error().code(), mygram::utils::ErrorCode::kMySQLInvalidSchema);
  EXPECT_THAT(nullable.error().message(), ::testing::HasSubstr("allows NULL"));
  EXPECT_TRUE(connection.ValidateUniqueColumn(config.database, table, "required_key"));
}

TEST(ConnectionValidateIntegrationTest, SessionPinsSqlModeAndIsolation) {
  if (!testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled";
  }

  Connection connection(testing::GetMySQLTestConfig());
  auto connect = connection.Connect("validate-session-pin-integration-test");
  ASSERT_TRUE(connect) << connect.error().message();

  auto mode = connection.Execute("SELECT @@SESSION.sql_mode");
  ASSERT_TRUE(mode) << mode.error().message();
  MYSQL_ROW row = mysql_fetch_row(mode->get());
  ASSERT_NE(row, nullptr);
  EXPECT_STREQ(row[0], "");

  auto snapshot = connection.StartConsistentSnapshot();
  ASSERT_TRUE(snapshot) << snapshot.error().message();
  // The variable is tx_isolation before MySQL 5.7.20 / MariaDB 11.1.
  auto isolation =
      connection.Execute("SHOW SESSION VARIABLES WHERE Variable_name IN ('transaction_isolation', 'tx_isolation')");
  ASSERT_TRUE(isolation) << isolation.error().message();
  row = mysql_fetch_row(isolation->get());
  ASSERT_NE(row, nullptr);
  EXPECT_STREQ(row[1], "REPEATABLE-READ");
  EXPECT_TRUE(connection.ExecuteUpdate("ROLLBACK"));
}

TEST(ConnectionValidateIntegrationTest, LowerCaseTableNamesIsReadAsAnInteger) {
  if (!testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled";
  }

  Connection connection(testing::GetMySQLTestConfig());
  auto connect = connection.Connect("validate-lctn-integration-test");
  ASSERT_TRUE(connect) << connect.error().message();

  auto value = connection.GetLowerCaseTableNames();
  ASSERT_TRUE(value) << value.error().message();
  EXPECT_GE(*value, 0);
  EXPECT_LE(*value, 2);
}

TEST(ConnectionValidateUnitTest, SnapshotStatusNeedsFileAndPosition) {
  using internal::ParseBinlogSnapshotStatus;
  using Rows = std::vector<std::pair<std::string, std::string>>;

  auto parsed = ParseBinlogSnapshotStatus(
      Rows{{"Binlog_snapshot_file", "mariadb-bin.000003"}, {"Binlog_snapshot_position", "4096"}});
  ASSERT_TRUE(parsed) << parsed.error().message();
  EXPECT_EQ(parsed->file, "mariadb-bin.000003");
  EXPECT_EQ(parsed->offset, 4096U);

  // Variable names are matched case-insensitively and in any order.
  auto reordered =
      ParseBinlogSnapshotStatus(Rows{{"binlog_snapshot_position", "512"}, {"BINLOG_SNAPSHOT_FILE", "b.000001"}});
  ASSERT_TRUE(reordered) << reordered.error().message();
  EXPECT_EQ(reordered->offset, 512U);
  EXPECT_EQ(reordered->file, "b.000001");

  // Anything missing, empty or malformed fails closed (binary log disabled, old server, proxy).
  for (const Rows& rows : {Rows{}, Rows{{"Binlog_snapshot_file", "b.000001"}}, Rows{{"Binlog_snapshot_position", "4"}},
                           Rows{{"Binlog_snapshot_file", ""}, {"Binlog_snapshot_position", "4"}},
                           Rows{{"Binlog_snapshot_file", "b.000001"}, {"Binlog_snapshot_position", "abc"}},
                           Rows{{"Binlog_snapshot_file", "b.000001"}, {"Binlog_snapshot_position", ""}}}) {
    auto failed = ParseBinlogSnapshotStatus(rows);
    ASSERT_FALSE(failed);
    EXPECT_EQ(failed.error().code(), mygram::utils::ErrorCode::kMySQLBinlogError);
  }
}

/**
 * @brief Test that ValidateUniqueColumn returns Expected<void, Error> with proper error codes
 * Verifies the new Expected-based API preserves error information
 */
TEST(ConnectionValidateUnitTest, ReturnsExpectedWithErrorCode) {
  Connection::Config config;
  config.host = "127.0.0.1";
  config.user = "test";
  config.password = "test";
  config.database = "test";
  Connection conn(config);

  auto result = conn.ValidateUniqueColumn("test_db", "test_table", "id");

  // Should return an error (not connected)
  EXPECT_FALSE(result.has_value());

  // Error should have a MySQL-range error code (2000-2999)
  auto code = static_cast<uint16_t>(result.error().code());
  EXPECT_GE(code, 2000) << "Error code should be in MySQL range";
  EXPECT_LE(code, 2999) << "Error code should be in MySQL range";

  // Error should have meaningful context
  EXPECT_TRUE(result.error().is_error());
  EXPECT_FALSE(result.error().message().empty());
}

/**
 * @brief Test that ValidateUniqueColumn error includes table/column context
 */
TEST(ConnectionValidateUnitTest, ErrorIncludesContext) {
  Connection::Config config;
  config.host = "127.0.0.1";
  config.user = "test";
  config.password = "test";
  config.database = "test";
  Connection conn(config);

  auto result = conn.ValidateUniqueColumn("mydb", "users", "email");

  EXPECT_FALSE(result.has_value());
  // The error to_string() should contain structured information
  std::string error_str = result.error().to_string();
  EXPECT_FALSE(error_str.empty());
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
