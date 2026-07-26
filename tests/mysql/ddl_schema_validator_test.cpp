#include "mysql/ddl_schema_validator.h"

#include <gtest/gtest.h>

#include <cstdlib>

#ifdef USE_MYSQL

#include "mysql/connection.h"
#include "mysql_test_helpers.h"

namespace mygramdb::mysql {
namespace {

config::TableConfig MakeConfig() {
  config::TableConfig config;
  config.database = "testdb";
  config.name = "articles";
  config.primary_key = "id";
  config.text_source.column = "content";
  config.required_filters.push_back({"status", "int", "=", "1", false});
  config.filters.push_back({"category", "varchar", false, false, ""});
  return config;
}

std::vector<DDLColumnMetadata> MakeColumns() {
  return {
      {"id", "bigint unsigned", "", false, "PRI"},
      {"content", "longtext", "utf8mb4_0900_ai_ci", true, ""},
      {"status", "int", "", false, "MUL"},
      {"category", "varchar(64)", "utf8mb4_0900_ai_ci", true, ""},
  };
}

TEST(DDLSchemaValidatorTest, UnrelatedColumnAdditionKeepsConfiguredFingerprint) {
  const auto config = MakeConfig();
  auto before = DDLSchemaValidator::ValidateMetadata(config, MakeColumns(), true);
  ASSERT_TRUE(before) << before.error().message();

  auto columns = MakeColumns();
  columns.push_back({"unrelated", "json", "", true, ""});
  auto after = DDLSchemaValidator::ValidateMetadata(config, columns, true);
  ASSERT_TRUE(after) << after.error().message();
  EXPECT_TRUE(DDLSchemaValidator::Compare(*before, *after, config));
}

TEST(DDLSchemaValidatorTest, IdempotentCreateReplayKeepsConfiguredFingerprint) {
  const auto config = MakeConfig();
  auto startup = DDLSchemaValidator::ValidateMetadata(config, MakeColumns(), true);
  auto after_historical_create = DDLSchemaValidator::ValidateMetadata(config, MakeColumns(), true);
  ASSERT_TRUE(startup);
  ASSERT_TRUE(after_historical_create);
  EXPECT_TRUE(DDLSchemaValidator::Compare(*startup, *after_historical_create, config));
}

TEST(DDLSchemaValidatorTest, IncompatibleCreateReplayFailsClosed) {
  const auto config = MakeConfig();
  auto startup = DDLSchemaValidator::ValidateMetadata(config, MakeColumns(), true);
  auto recreated_columns = MakeColumns();
  recreated_columns[1].column_type = "varchar(255)";
  auto after_incompatible_create = DDLSchemaValidator::ValidateMetadata(config, recreated_columns, true);
  ASSERT_TRUE(startup);
  ASSERT_TRUE(after_incompatible_create);
  auto comparison = DDLSchemaValidator::Compare(*startup, *after_incompatible_create, config);
  ASSERT_FALSE(comparison);
  EXPECT_NE(comparison.error().message().find("'content' changed"), std::string::npos);
}

TEST(DDLSchemaValidatorTest, SecondaryIndexOnConfiguredColumnDoesNotChangeContract) {
  const auto config = MakeConfig();
  auto before = DDLSchemaValidator::ValidateMetadata(config, MakeColumns(), true);
  ASSERT_TRUE(before);
  auto columns = MakeColumns();
  columns[3].key = "MUL";
  auto after = DDLSchemaValidator::ValidateMetadata(config, columns, true);
  ASSERT_TRUE(after);
  EXPECT_TRUE(DDLSchemaValidator::Compare(*before, *after, config));
}

TEST(DDLSchemaValidatorTest, MissingTextColumnIsIncompatible) {
  const auto config = MakeConfig();
  auto columns = MakeColumns();
  columns.erase(columns.begin() + 1);
  auto result = DDLSchemaValidator::ValidateMetadata(config, columns, true);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLColumnNotFound);
}

TEST(DDLSchemaValidatorTest, PrimaryKeyMustRemainSingleColumnUnique) {
  auto result = DDLSchemaValidator::ValidateMetadata(MakeConfig(), MakeColumns(), false);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLInvalidSchema);
}

TEST(DDLSchemaValidatorTest, FilterTypeMustMatchConfiguredValueDomain) {
  auto columns = MakeColumns();
  columns[2].column_type = "varchar(10)";
  columns[2].collation = "utf8mb4_0900_ai_ci";
  auto result = DDLSchemaValidator::ValidateMetadata(MakeConfig(), columns, true);
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().message().find("expects type 'int'"), std::string::npos);
}

TEST(DDLSchemaValidatorTest, ConfiguredColumnTypeChangeChangesFingerprint) {
  const auto config = MakeConfig();
  auto before = DDLSchemaValidator::ValidateMetadata(config, MakeColumns(), true);
  ASSERT_TRUE(before);
  auto columns = MakeColumns();
  columns[0].column_type = "varchar(32)";
  columns[0].collation = "utf8mb4_0900_ai_ci";
  auto after = DDLSchemaValidator::ValidateMetadata(config, columns, true);
  ASSERT_TRUE(after);
  auto comparison = DDLSchemaValidator::Compare(*before, *after, config);
  ASSERT_FALSE(comparison);
  EXPECT_NE(comparison.error().message().find("'id' changed"), std::string::npos);
}

TEST(DDLSchemaValidatorTest, BooleanRequiresSignedTinyintOne) {
  auto config = MakeConfig();
  config.required_filters[0].type = "boolean";
  auto columns = MakeColumns();
  columns[2].column_type = "tinyint(1)";
  EXPECT_TRUE(DDLSchemaValidator::ValidateMetadata(config, columns, true));
  columns[2].column_type = "tinyint unsigned";
  EXPECT_FALSE(DDLSchemaValidator::ValidateMetadata(config, columns, true));
}

TEST(DDLSchemaValidatorTest, RejectsBinaryPrimaryKeyBeforeReplicationStarts) {
  auto columns = MakeColumns();
  columns[0].column_type = "binary(16)";
  auto result = DDLSchemaValidator::ValidateMetadata(MakeConfig(), columns, true);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLInvalidSchema);
  EXPECT_NE(result.error().message().find("uses binary type 'binary(16)'"), std::string::npos);
  EXPECT_EQ(result.error().context(), "testdb.articles.id");
}

TEST(DDLSchemaValidatorTest, RejectsBinaryTextAndFilterColumns) {
  auto columns = MakeColumns();
  columns[1].column_type = "longblob";
  columns[1].collation.clear();
  auto text_result = DDLSchemaValidator::ValidateMetadata(MakeConfig(), columns, true);
  ASSERT_FALSE(text_result);
  EXPECT_NE(text_result.error().message().find("snapshot and binlog paths"), std::string::npos);

  columns = MakeColumns();
  columns[3].column_type = "varbinary(64)";
  columns[3].collation.clear();
  auto filter_result = DDLSchemaValidator::ValidateMetadata(MakeConfig(), columns, true);
  ASSERT_FALSE(filter_result);
  EXPECT_NE(filter_result.error().message().find("uses binary type 'varbinary(64)'"), std::string::npos);
}

TEST(DDLSchemaValidatorTest, RejectsNonUtf8ConfiguredTextCharsets) {
  for (const std::string& collation :
       {"cp932_japanese_ci", "sjis_japanese_ci", "eucjpms_japanese_ci", "latin1_swedish_ci"}) {
    auto columns = MakeColumns();
    columns[1].collation = collation;
    auto result = DDLSchemaValidator::ValidateMetadata(MakeConfig(), columns, true);
    ASSERT_FALSE(result) << collation;
    EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLInvalidSchema);
    EXPECT_NE(result.error().message().find("unsupported collation '" + collation + "'"), std::string::npos);
    EXPECT_EQ(result.error().context(), "testdb.articles.content");
  }
}

TEST(DDLSchemaValidatorTest, AcceptsUtf8AliasesAndAsciiConfiguredTextCharsets) {
  for (const std::string& collation :
       {"utf8mb4_0900_ai_ci", "utf8mb3_general_ci", "utf8_general_ci", "ascii_general_ci"}) {
    auto columns = MakeColumns();
    columns[1].collation = collation;
    columns[3].collation = collation;
    auto result = DDLSchemaValidator::ValidateMetadata(MakeConfig(), columns, true);
    EXPECT_TRUE(result) << collation << ": " << (result ? "" : result.error().message());
  }
}

Connection::Config MakeIntegrationConnectionConfig() {
  Connection::Config config;
  config.host = std::getenv("MYSQL_HOST") == nullptr ? "127.0.0.1" : std::getenv("MYSQL_HOST");
  config.port =
      static_cast<uint16_t>(std::getenv("MYSQL_PORT") == nullptr ? 3306 : std::atoi(std::getenv("MYSQL_PORT")));
  config.user = std::getenv("MYSQL_USER") == nullptr ? "root" : std::getenv("MYSQL_USER");
  config.password = std::getenv("MYSQL_PASSWORD") == nullptr ? "" : std::getenv("MYSQL_PASSWORD");
  config.database = std::getenv("MYSQL_DATABASE") == nullptr ? "test" : std::getenv("MYSQL_DATABASE");
  return config;
}

TEST(DDLSchemaValidatorIntegrationTest, RejectsBinaryConfiguredColumnReportedByMySQL) {
  if (!mygramdb::mysql::testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled";
  }
  Connection connection(MakeIntegrationConnectionConfig());
  ASSERT_TRUE(connection.Connect("ddl-encoding-binary-test"));
  ASSERT_TRUE(connection.ExecuteUpdate("DROP TABLE IF EXISTS audit_f19_binary"));
  ASSERT_TRUE(connection.ExecuteUpdate(
      "CREATE TABLE audit_f19_binary (id BINARY(16) PRIMARY KEY, content TEXT CHARACTER SET utf8mb4 NOT NULL)"));

  config::TableConfig table_config;
  table_config.database = MakeIntegrationConnectionConfig().database;
  table_config.name = "audit_f19_binary";
  table_config.primary_key = "id";
  table_config.text_source.column = "content";
  auto result = DDLSchemaValidator::Capture(connection, table_config);
  EXPECT_TRUE(connection.ExecuteUpdate("DROP TABLE audit_f19_binary"));

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLInvalidSchema);
  EXPECT_NE(result.error().message().find("uses binary type 'binary(16)'"), std::string::npos);
}

TEST(DDLSchemaValidatorIntegrationTest, RejectsCp932ConfiguredColumnReportedByMySQL) {
  if (!mygramdb::mysql::testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled";
  }
  Connection connection(MakeIntegrationConnectionConfig());
  ASSERT_TRUE(connection.Connect("ddl-encoding-cp932-test"));
  ASSERT_TRUE(connection.ExecuteUpdate("DROP TABLE IF EXISTS audit_f20_cp932"));
  ASSERT_TRUE(connection.ExecuteUpdate(
      "CREATE TABLE audit_f20_cp932 (id BIGINT PRIMARY KEY, content TEXT CHARACTER SET cp932 NOT NULL)"));

  config::TableConfig table_config;
  table_config.database = MakeIntegrationConnectionConfig().database;
  table_config.name = "audit_f20_cp932";
  table_config.primary_key = "id";
  table_config.text_source.column = "content";
  auto result = DDLSchemaValidator::Capture(connection, table_config);
  EXPECT_TRUE(connection.ExecuteUpdate("DROP TABLE audit_f20_cp932"));

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMySQLInvalidSchema);
  EXPECT_NE(result.error().message().find("unsupported collation 'cp932_"), std::string::npos);
}

}  // namespace
}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
