#include "mysql/ddl_schema_validator.h"

#include <gtest/gtest.h>

#ifdef USE_MYSQL

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

}  // namespace
}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
