/**
 * @file connection_test.cpp
 * @brief Unit tests for MySQL connection wrapper
 */

#include "mysql/connection.h"

#include <gtest/gtest.h>

#ifdef USE_MYSQL

using namespace mygramdb::mysql;

/**
 * @brief Test GTID parsing - basic format
 */
TEST(MySQLConnectionTest, GTIDParseBasic) {
  auto gtid = GTID::Parse("3E11FA47-71CA-11E1-9E33-C80AA9429562:1");

  ASSERT_TRUE(gtid.has_value());
  EXPECT_EQ(gtid->server_uuid, "3E11FA47-71CA-11E1-9E33-C80AA9429562");
  EXPECT_EQ(gtid->transaction_id, 1);
}

/**
 * @brief Test GTID parsing - range format
 */
TEST(MySQLConnectionTest, GTIDParseRange) {
  auto gtid = GTID::Parse("3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100");

  ASSERT_TRUE(gtid.has_value());
  EXPECT_EQ(gtid->server_uuid, "3E11FA47-71CA-11E1-9E33-C80AA9429562");
  EXPECT_EQ(gtid->transaction_id, 100);  // Should parse end of range
}

/**
 * @brief Test GTID parsing - large transaction ID
 */
TEST(MySQLConnectionTest, GTIDParseLargeID) {
  auto gtid = GTID::Parse("3E11FA47-71CA-11E1-9E33-C80AA9429562:1000000");

  ASSERT_TRUE(gtid.has_value());
  EXPECT_EQ(gtid->transaction_id, 1000000);
}

/**
 * @brief Test GTID parsing - invalid format (no colon)
 */
TEST(MySQLConnectionTest, GTIDParseInvalidNoColon) {
  auto gtid = GTID::Parse("3E11FA47-71CA-11E1-9E33-C80AA9429562");

  EXPECT_FALSE(gtid.has_value());
}

/**
 * @brief Test GTID parsing - invalid format (non-numeric ID)
 */
TEST(MySQLConnectionTest, GTIDParseInvalidNonNumeric) {
  auto gtid = GTID::Parse("3E11FA47-71CA-11E1-9E33-C80AA9429562:abc");

  EXPECT_FALSE(gtid.has_value());
}

/**
 * @brief Test GTID parsing - empty string
 */
TEST(MySQLConnectionTest, GTIDParseEmpty) {
  auto gtid = GTID::Parse("");

  EXPECT_FALSE(gtid.has_value());
}

/**
 * @brief Test GTID ToString
 */
TEST(MySQLConnectionTest, GTIDToString) {
  GTID gtid;
  gtid.server_uuid = "3E11FA47-71CA-11E1-9E33-C80AA9429562";
  gtid.transaction_id = 42;

  EXPECT_EQ(gtid.ToString(), "3E11FA47-71CA-11E1-9E33-C80AA9429562:42");
}

/**
 * @brief Test GTID equality
 */
TEST(MySQLConnectionTest, GTIDEquality) {
  GTID gtid1;
  gtid1.server_uuid = "3E11FA47-71CA-11E1-9E33-C80AA9429562";
  gtid1.transaction_id = 42;

  GTID gtid2;
  gtid2.server_uuid = "3E11FA47-71CA-11E1-9E33-C80AA9429562";
  gtid2.transaction_id = 42;

  GTID gtid3;
  gtid3.server_uuid = "DIFFERENT-UUID";
  gtid3.transaction_id = 42;

  EXPECT_EQ(gtid1, gtid2);
  EXPECT_NE(gtid1, gtid3);
}

/**
 * @brief Test GTID round-trip (Parse -> ToString)
 */
TEST(MySQLConnectionTest, GTIDRoundTrip) {
  std::string original = "3E11FA47-71CA-11E1-9E33-C80AA9429562:123";

  auto gtid = GTID::Parse(original);
  ASSERT_TRUE(gtid.has_value());

  std::string result = gtid->ToString();
  EXPECT_EQ(result, original);
}

/**
 * @brief Test Connection construction
 */
TEST(MySQLConnectionTest, ConnectionConstruct) {
  Connection::Config config;
  config.host = "localhost";
  config.port = 3306;
  config.user = "test";
  config.password = "test";
  config.database = "testdb";

  Connection conn(config);

  // Should construct successfully (not connected yet)
  EXPECT_FALSE(conn.IsConnected());
}

/**
 * @brief Test Connection move constructor
 */
TEST(MySQLConnectionTest, ConnectionMove) {
  Connection::Config config;
  config.host = "localhost";

  Connection conn1(config);
  Connection conn2(std::move(conn1));

  // conn2 should take ownership
  EXPECT_FALSE(conn2.IsConnected());
}

/**
 * @brief Test IsGTIDModeEnabled without connection
 *
 * This tests that IsGTIDModeEnabled returns false when not connected,
 * rather than crashing. Actual GTID mode detection is tested in integration tests.
 */
TEST(MySQLConnectionTest, IsGTIDModeEnabledWithoutConnection) {
  Connection::Config config;
  config.host = "localhost";
  config.user = "test";
  config.password = "test";

  Connection conn(config);

  // Should return false when not connected (doesn't crash)
  EXPECT_FALSE(conn.IsGTIDModeEnabled());
}

#endif  // USE_MYSQL
