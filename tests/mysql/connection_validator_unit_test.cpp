/**
 * @file connection_validator_unit_test.cpp
 * @brief Unit tests for ConnectionValidator (no MySQL connection required)
 */

#ifdef USE_MYSQL

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "mybinlog/disposition.h"
#include "mybinlog/event_types.h"
#include "mysql/connection.h"
#include "utils/error.h"
#include "utils/expected.h"

// The purge-boundary decision is private; reach it the way the binlog reader
// tests reach reader internals (tests/mysql/binlog_test_fixtures.h).
#define private public
#include "mysql/connection_validator.h"
#undef private

namespace mygramdb::mysql {

// ============================================================================
// Unit Tests (no MySQL connection required)
// ============================================================================

TEST(ConnectionValidatorUnitTest, ValidationResultDefaultState) {
  ValidationResult result;
  EXPECT_FALSE(result.valid);
  EXPECT_TRUE(result.error_message.empty());
  EXPECT_TRUE(result.warnings.empty());
  EXPECT_FALSE(result.server_uuid.has_value());
}

TEST(ConnectionValidatorUnitTest, ValidationResultBoolConversion) {
  ValidationResult success;
  success.valid = true;
  ValidationResult failure;
  failure.valid = false;
  EXPECT_TRUE(static_cast<bool>(success));
  EXPECT_FALSE(static_cast<bool>(failure));
  if (success) {
    SUCCEED() << "Bool conversion works for success";
  } else {
    FAIL() << "Bool conversion failed for success";
  }
  if (!failure) {
    SUCCEED() << "Bool conversion works for failure";
  } else {
    FAIL() << "Bool conversion failed for failure";
  }
}

TEST(ConnectionValidatorUnitTest, ValidationResultWithError) {
  ValidationResult result;
  result.valid = false;
  result.error_message = "GTID mode is not enabled";
  EXPECT_FALSE(result.valid);
  EXPECT_EQ(result.error_message, "GTID mode is not enabled");
  EXPECT_FALSE(static_cast<bool>(result));
}

TEST(ConnectionValidatorUnitTest, ValidationResultPreservesFailureCode) {
  ValidationResult result;
  result.error_code = mygramdb::utils::ErrorCode::kMySQLTimeout;
  ASSERT_TRUE(result.error_code.has_value());
  EXPECT_EQ(*result.error_code, mygramdb::utils::ErrorCode::kMySQLTimeout);
}

TEST(ConnectionValidatorUnitTest, ValidationResultWithWarnings) {
  ValidationResult result;
  result.valid = true;
  result.warnings.push_back("Server UUID changed (failover detected)");
  result.warnings.push_back("GTID consistency check warning");
  EXPECT_TRUE(result.valid);
  EXPECT_TRUE(result.error_message.empty());
  EXPECT_EQ(result.warnings.size(), 2);
  EXPECT_TRUE(static_cast<bool>(result));
}

TEST(ConnectionValidatorUnitTest, ValidationResultWithServerUUID) {
  ValidationResult result;
  result.valid = true;
  result.server_uuid = "a1b2c3d4-e5f6-1234-5678-90abcdef1234";
  EXPECT_TRUE(result.valid);
  EXPECT_TRUE(result.server_uuid.has_value());
  EXPECT_EQ(*result.server_uuid, "a1b2c3d4-e5f6-1234-5678-90abcdef1234");
}

TEST(ConnectionValidatorUnitTest, ValidationResultFailoverDetectedDefault) {
  ValidationResult result;
  EXPECT_FALSE(result.failover_detected);
}

TEST(ConnectionValidatorUnitTest, ValidationResultFailoverDetectedSet) {
  ValidationResult result;
  result.failover_detected = true;
  EXPECT_TRUE(result.failover_detected);
}

TEST(ConnectionValidatorUnitTest, ValidTableNamePatterns) {
  const std::vector<std::string> valid_names = {"articles", "user_profiles", "tbl$1",        "test-table",
                                                "Table123", "注文 履歴",     "archive.2026", "quote'name"};
  for (const auto& name : valid_names) {
    EXPECT_TRUE(ConnectionValidator::IsValidIdentifier(name)) << "Table name '" << name << "' should be valid";
  }
}

TEST(ConnectionValidatorUnitTest, InvalidTableNamePatterns) {
  const std::vector<std::string> invalid_names = {"", std::string("table\0name", 10)};
  for (const auto& name : invalid_names) {
    EXPECT_FALSE(ConnectionValidator::IsValidIdentifier(name));
  }
}

TEST(ConnectionValidatorUnitTest, RequiredTableDisplayNameIncludesDatabase) {
  ConnectionValidator::RequiredTable table{"archive_db", "articles"};
  EXPECT_EQ(table.DisplayName(), "archive_db.articles");

  ConnectionValidator::RequiredTable legacy_table{"", "articles"};
  EXPECT_EQ(legacy_table.DisplayName(), "articles");
}

TEST(ConnectionValidatorUnitTest, IdentifierValidationCoversDatabaseAndTableNames) {
  EXPECT_TRUE(ConnectionValidator::IsValidIdentifier("app_db"));
  EXPECT_TRUE(ConnectionValidator::IsValidIdentifier("articles-2026"));
  EXPECT_TRUE(ConnectionValidator::IsValidIdentifier("tenant$1"));
  EXPECT_TRUE(ConnectionValidator::IsValidIdentifier("app.db"));
  EXPECT_TRUE(ConnectionValidator::IsValidIdentifier("app db"));
  EXPECT_TRUE(ConnectionValidator::IsValidIdentifier("顧客データ"));
  EXPECT_TRUE(ConnectionValidator::IsValidIdentifier("app';DROP"));

  EXPECT_FALSE(ConnectionValidator::IsValidIdentifier(""));
  EXPECT_FALSE(ConnectionValidator::IsValidIdentifier(std::string("app\0db", 6)));
}

TEST(ConnectionValidatorUnitTest, BinlogChecksumValidationRequiresCRC32) {
  EXPECT_TRUE(ConnectionValidator::IsSupportedBinlogChecksumValue("CRC32"));
  EXPECT_TRUE(ConnectionValidator::IsSupportedBinlogChecksumValue("crc32"));

  EXPECT_FALSE(ConnectionValidator::IsSupportedBinlogChecksumValue("NONE"));
  EXPECT_FALSE(ConnectionValidator::IsSupportedBinlogChecksumValue(""));
}

TEST(ConnectionValidatorUnitTest, BinlogFormatValidationRequiresRow) {
  EXPECT_TRUE(ConnectionValidator::IsSupportedBinlogFormatValue("ROW"));
  EXPECT_TRUE(ConnectionValidator::IsSupportedBinlogFormatValue("row"));

  EXPECT_FALSE(ConnectionValidator::IsSupportedBinlogFormatValue("STATEMENT"));
  EXPECT_FALSE(ConnectionValidator::IsSupportedBinlogFormatValue("MIXED"));
  EXPECT_FALSE(ConnectionValidator::IsSupportedBinlogFormatValue(""));
}

TEST(ConnectionValidatorUnitTest, MariaDBCompressedEventsAreExplicitlyFailClosed) {
  using mybinlog::EventType;
  const auto is_compressed = [](EventType type) {
    return mybinlog::ClassifyEvent(type, ServerFlavor::kMariaDB).disposition == mybinlog::Disposition::kCompressed;
  };

  EXPECT_TRUE(is_compressed(EventType::kMariaDBQueryCompressedEvent));
  EXPECT_TRUE(is_compressed(EventType::kMariaDBWriteRowsCompressedEventV1));
  EXPECT_TRUE(is_compressed(EventType::kMariaDBUpdateRowsCompressedEventV1));
  EXPECT_TRUE(is_compressed(EventType::kMariaDBDeleteRowsCompressedEventV1));
  EXPECT_TRUE(is_compressed(EventType::kMariaDBWriteRowsCompressedEvent));
  EXPECT_TRUE(is_compressed(EventType::kMariaDBUpdateRowsCompressedEvent));
  EXPECT_TRUE(is_compressed(EventType::kMariaDBDeleteRowsCompressedEvent));
  EXPECT_FALSE(is_compressed(EventType::kMariaDBGtidEvent));
  EXPECT_FALSE(is_compressed(EventType::kWriteRowsEvent));

  EXPECT_STREQ(mybinlog::EventTypeName(EventType::kMariaDBWriteRowsCompressedEvent),
               "MARIADB_WRITE_ROWS_COMPRESSED_EVENT");
  EXPECT_STREQ(mybinlog::EventTypeName(EventType::kMariaDBWriteRowsCompressedEventV1),
               "MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1");
}

/**
 * @brief A tagged executed GTID set, as MySQL 8.4 prints it, is a valid position for the consistency check
 */
TEST(ConnectionValidatorUnitTest, TaggedExecutedGtidSetPassesTheConsistencyCheck) {
  auto tagged = ConnectionValidator::CanonicalMySQLGtidPosition(
      "01020304-0506-0708-090a-0b0c0d0e0f10:1-100:TAG:1-5,11111111-2222-3333-4444-555555555555:7");
  ASSERT_TRUE(tagged.has_value()) << tagged.error().message();
  EXPECT_EQ(*tagged,
            "01020304-0506-0708-090a-0b0c0d0e0f10:1-100,01020304-0506-0708-090a-0b0c0d0e0f10:tag:1-5,"
            "11111111-2222-3333-4444-555555555555:7");

  auto invalid = ConnectionValidator::CanonicalMySQLGtidPosition("01020304-0506-0708-090a-0b0c0d0e0f10:my-tag:100");
  ASSERT_FALSE(invalid.has_value());
  EXPECT_EQ(invalid.error().code(), mygram::utils::ErrorCode::kMySQLInvalidGTID);
}

TEST(ConnectionValidatorUnitTest, ExecutedPositionQueryReadsTheSourceBinlogPosition) {
  EXPECT_STREQ(GetBinlogExecutedPositionQuery(ServerFlavor::kMariaDB), "SELECT @@GLOBAL.gtid_binlog_pos");
  EXPECT_STREQ(GetBinlogExecutedPositionQuery(ServerFlavor::kMySQL), "SELECT @@GLOBAL.gtid_executed");
}

TEST(ConnectionValidatorUnitTest, BinlogRowMetadataMustBeFull) {
  EXPECT_TRUE(ConnectionValidator::IsSupportedBinlogRowMetadataValue("FULL"));
  EXPECT_TRUE(ConnectionValidator::IsSupportedBinlogRowMetadataValue("full"));
  EXPECT_FALSE(ConnectionValidator::IsSupportedBinlogRowMetadataValue("MINIMAL"));
  EXPECT_FALSE(ConnectionValidator::IsSupportedBinlogRowMetadataValue("NO_LOG"));
  EXPECT_FALSE(ConnectionValidator::IsSupportedBinlogRowMetadataValue(""));
}

TEST(ConnectionValidatorUnitTest, BinlogStorageEngineIsEnabledOnlyWhenItNamesAnEngine) {
  EXPECT_FALSE(ConnectionValidator::IsBinlogStorageEngineEnabled(""));
  EXPECT_FALSE(ConnectionValidator::IsBinlogStorageEngineEnabled("  "));
  EXPECT_TRUE(ConnectionValidator::IsBinlogStorageEngineEnabled("innodb"));
}

TEST(ConnectionValidatorUnitTest, MariaDBReplicaRoleComesFromGtidSlavePos) {
  EXPECT_FALSE(ConnectionValidator::HasAppliedReplicatedGtids(""));
  EXPECT_FALSE(ConnectionValidator::HasAppliedReplicatedGtids("  "));
  EXPECT_TRUE(ConnectionValidator::HasAppliedReplicatedGtids("0-1-100"));
  EXPECT_TRUE(ConnectionValidator::HasAppliedReplicatedGtids("0-1-100,1-2-5"));
}

TEST(ConnectionValidatorUnitTest, ReplicaMustLogReplicatedWrites) {
  using mygram::utils::ErrorCode;

  // A server that is not a replica needs no setting.
  EXPECT_TRUE(ConnectionValidator::EvaluateLogReplicaUpdates(ServerFlavor::kMySQL, false, std::nullopt).has_value());
  EXPECT_TRUE(ConnectionValidator::EvaluateLogReplicaUpdates(ServerFlavor::kMariaDB, false, "OFF").has_value());

  EXPECT_TRUE(ConnectionValidator::EvaluateLogReplicaUpdates(ServerFlavor::kMySQL, true, "ON").has_value());
  EXPECT_TRUE(ConnectionValidator::EvaluateLogReplicaUpdates(ServerFlavor::kMariaDB, true, "ON").has_value());

  auto mysql_off = ConnectionValidator::EvaluateLogReplicaUpdates(ServerFlavor::kMySQL, true, "OFF");
  ASSERT_FALSE(mysql_off.has_value());
  EXPECT_EQ(mysql_off.error().code(), ErrorCode::kMySQLBinlogError);
  EXPECT_NE(mysql_off.error().message().find("log_replica_updates=OFF"), std::string::npos);

  auto mariadb_off = ConnectionValidator::EvaluateLogReplicaUpdates(ServerFlavor::kMariaDB, true, "OFF");
  ASSERT_FALSE(mariadb_off.has_value());
  EXPECT_NE(mariadb_off.error().message().find("log_slave_updates=OFF"), std::string::npos);

  // A replica whose setting cannot be read fails closed.
  EXPECT_FALSE(ConnectionValidator::EvaluateLogReplicaUpdates(ServerFlavor::kMySQL, true, std::nullopt).has_value());
}

/**
 * @brief A MariaDB position behind the oldest binlog's starting position is rejected before the stream opens.
 */
TEST(ConnectionValidatorUnitTest, MariaDBPositionBehindThePurgeBoundaryIsRejected) {
  const std::string boundary = "0-1-100,1-2-5";

  EXPECT_TRUE(ConnectionValidator::CheckMariaDBPositionNotPurged("0-1-100,1-2-5", boundary).has_value());
  // Server IDs change on failover; only the per-domain sequence matters.
  EXPECT_TRUE(ConnectionValidator::CheckMariaDBPositionNotPurged("0-3-150,1-4-9", boundary).has_value());
  EXPECT_TRUE(ConnectionValidator::CheckMariaDBPositionNotPurged("0-1-1", "").has_value());

  for (const char* position : {"0-1-99,1-2-5", "0-1-150"}) {
    auto purged = ConnectionValidator::CheckMariaDBPositionNotPurged(position, boundary);
    ASSERT_FALSE(purged.has_value()) << position;
    EXPECT_EQ(purged.error().code(), mygram::utils::ErrorCode::kMySQLReplicationError) << position;
    EXPECT_NE(purged.error().message().find("Run SYNC"), std::string::npos) << purged.error().message();
  }
}

// ============================================================================
// Error Tests (no MySQL connection required)
// ============================================================================

TEST(ConnectionValidatorErrorTest, ValidateServerNotConnected) {
  Connection::Config config;
  config.host = "127.0.0.1";
  config.user = "test";
  config.password = "test";
  config.database = "test";
  Connection conn(config);
  std::vector<std::string> required_tables = {"test_table"};
  auto result = ConnectionValidator::ValidateServer(conn, required_tables);
  EXPECT_FALSE(result.valid);
  EXPECT_FALSE(result.error_message.empty());
  EXPECT_THAT(result.error_message, ::testing::HasSubstr("Connection is not active"));
  ASSERT_TRUE(result.error_code.has_value());
  EXPECT_EQ(*result.error_code, mygram::utils::ErrorCode::kMySQLDisconnected);
}

TEST(ConnectionValidatorErrorTest, ValidateServerAcceptsDatabaseQualifiedRequiredTables) {
  Connection::Config config;
  config.host = "127.0.0.1";
  config.user = "test";
  config.password = "test";
  config.database = "default_db";
  Connection conn(config);
  std::vector<ConnectionValidator::RequiredTable> required_tables = {
      {"default_db", "articles"},
      {"archive_db", "articles"},
  };

  auto result = ConnectionValidator::ValidateServer(conn, required_tables);

  EXPECT_FALSE(result.valid);
  EXPECT_THAT(result.error_message, ::testing::HasSubstr("Connection is not active"));
}

/**
 * @brief Test that ValidateServer returns meaningful error for bad configurations
 * Verifies the Expected-based internal helpers propagate error messages correctly
 */
TEST(ConnectionValidatorErrorTest, ErrorMessagePropagation) {
  Connection::Config config;
  config.host = "127.0.0.1";
  config.user = "test";
  config.password = "test";
  config.database = "test";
  Connection conn(config);

  // ValidateServer with no tables should fail because connection is not active
  std::vector<std::string> tables = {"articles"};
  auto result = ConnectionValidator::ValidateServer(conn, tables);

  EXPECT_FALSE(result.valid);
  // Error message should be populated from the first failed check
  EXPECT_FALSE(result.error_message.empty());
  // Should not have failover detected on a non-connected server
  EXPECT_FALSE(result.failover_detected);
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
