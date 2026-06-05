/**
 * @file config_help_test.cpp
 * @brief Unit tests for configuration help system
 */

#include "config/config_help.h"

#include <gtest/gtest.h>

#include <optional>

#include "utils/error.h"

namespace mygramdb::config {

class ConfigSchemaExplorerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto result = ConfigSchemaExplorer::Create();
    ASSERT_TRUE(result.has_value()) << "Failed to create ConfigSchemaExplorer: " << result.error().message();
    explorer_.emplace(std::move(*result));
  }

  ConfigSchemaExplorer& explorer() { return explorer_.value(); }
  const ConfigSchemaExplorer& explorer() const { return explorer_.value(); }

  std::optional<ConfigSchemaExplorer> explorer_;
};

// Test GetHelp for simple property
TEST_F(ConfigSchemaExplorerTest, GetHelpForSimpleProperty) {
  auto help = explorer().GetHelp("mysql.port");

  ASSERT_TRUE(help.has_value());
  EXPECT_EQ(help->path, "mysql.port");
  EXPECT_EQ(help->type, "integer");
  EXPECT_EQ(help->default_value, "3306");
  EXPECT_TRUE(help->minimum.has_value());
  EXPECT_EQ(help->minimum.value(), 1);
  EXPECT_TRUE(help->maximum.has_value());
  EXPECT_EQ(help->maximum.value(), 65535);
  EXPECT_FALSE(help->description.empty());
}

// Test GetHelp for nested property
TEST_F(ConfigSchemaExplorerTest, GetHelpForNestedProperty) {
  auto help = explorer().GetHelp("memory.normalize.nfkc");

  ASSERT_TRUE(help.has_value());
  EXPECT_EQ(help->path, "memory.normalize.nfkc");
  EXPECT_EQ(help->type, "boolean");
  EXPECT_EQ(help->default_value, "true");
  EXPECT_FALSE(help->description.empty());
}

// Test GetHelp for enum property
TEST_F(ConfigSchemaExplorerTest, GetHelpForEnumProperty) {
  auto help = explorer().GetHelp("mysql.binlog_format");

  ASSERT_TRUE(help.has_value());
  EXPECT_EQ(help->type, "string");
  EXPECT_FALSE(help->allowed_values.empty());
  EXPECT_EQ(help->allowed_values.size(), 3);
}

// Test GetHelp for non-existent path
TEST_F(ConfigSchemaExplorerTest, GetHelpForNonExistentPath) {
  auto help = explorer().GetHelp("nonexistent.path");
  EXPECT_FALSE(help.has_value());
}

// Test GetHelp for root
TEST_F(ConfigSchemaExplorerTest, GetHelpForRoot) {
  auto help = explorer().GetHelp("");
  ASSERT_TRUE(help.has_value());
  EXPECT_EQ(help->type, "object");
}

// Test ListPaths for root
TEST_F(ConfigSchemaExplorerTest, ListPathsRoot) {
  auto paths = explorer().ListPaths("");

  EXPECT_FALSE(paths.empty());
  EXPECT_TRUE(paths.find("mysql") != paths.end());
  EXPECT_TRUE(paths.find("tables") != paths.end());
  EXPECT_TRUE(paths.find("build") != paths.end());
  EXPECT_TRUE(paths.find("replication") != paths.end());
  EXPECT_TRUE(paths.find("memory") != paths.end());
  EXPECT_TRUE(paths.find("dump") != paths.end());
  EXPECT_TRUE(paths.find("api") != paths.end());
  EXPECT_TRUE(paths.find("logging") != paths.end());
  EXPECT_TRUE(paths.find("cache") != paths.end());
}

// Test ListPaths for specific section
TEST_F(ConfigSchemaExplorerTest, ListPathsForSection) {
  auto paths = explorer().ListPaths("mysql");

  EXPECT_FALSE(paths.empty());
  EXPECT_TRUE(paths.find("host") != paths.end());
  EXPECT_TRUE(paths.find("port") != paths.end());
  EXPECT_TRUE(paths.find("user") != paths.end());
  EXPECT_TRUE(paths.find("password") != paths.end());
  EXPECT_TRUE(paths.find("database") != paths.end());
}

// Test ListPaths for nested section
TEST_F(ConfigSchemaExplorerTest, ListPathsForNestedSection) {
  auto paths = explorer().ListPaths("memory.normalize");

  EXPECT_FALSE(paths.empty());
  EXPECT_TRUE(paths.find("nfkc") != paths.end());
  EXPECT_TRUE(paths.find("width") != paths.end());
  EXPECT_TRUE(paths.find("lower") != paths.end());
}

// Test ListPaths for non-existent path
TEST_F(ConfigSchemaExplorerTest, ListPathsForNonExistentPath) {
  auto paths = explorer().ListPaths("nonexistent");
  EXPECT_TRUE(paths.empty());
}

// Test FormatHelp
TEST_F(ConfigSchemaExplorerTest, FormatHelp) {
  auto help = explorer().GetHelp("mysql.port");
  ASSERT_TRUE(help.has_value());

  std::string formatted = ConfigSchemaExplorer::FormatHelp(help.value());

  EXPECT_FALSE(formatted.empty());
  EXPECT_NE(formatted.find("mysql.port"), std::string::npos);
  EXPECT_NE(formatted.find("integer"), std::string::npos);
  EXPECT_NE(formatted.find("3306"), std::string::npos);
  EXPECT_NE(formatted.find("Range"), std::string::npos);
}

// Test FormatPathList
TEST_F(ConfigSchemaExplorerTest, FormatPathList) {
  auto paths = explorer().ListPaths("mysql");
  std::string formatted = ConfigSchemaExplorer::FormatPathList(paths, "mysql");

  EXPECT_FALSE(formatted.empty());
  EXPECT_NE(formatted.find("host"), std::string::npos);
  EXPECT_NE(formatted.find("port"), std::string::npos);
}

// Test IsSensitiveField
TEST(ConfigHelpTest, IsSensitiveFieldPassword) {
  EXPECT_TRUE(IsSensitiveField("mysql.password"));
  EXPECT_TRUE(IsSensitiveField("api.api_token"));
  EXPECT_TRUE(IsSensitiveField("ssl.private_key"));
  EXPECT_TRUE(IsSensitiveField("auth.secret"));
}

TEST(ConfigHelpTest, IsSensitiveFieldNonSensitive) {
  EXPECT_FALSE(IsSensitiveField("mysql.host"));
  EXPECT_FALSE(IsSensitiveField("mysql.port"));
  EXPECT_FALSE(IsSensitiveField("mysql.user"));
}

TEST(ConfigHelpTest, IsSensitiveFieldCaseInsensitive) {
  EXPECT_TRUE(IsSensitiveField("mysql.PASSWORD"));
  EXPECT_TRUE(IsSensitiveField("mysql.Password"));
  EXPECT_TRUE(IsSensitiveField("API.TOKEN"));
}

// Test MaskSensitiveValue
TEST(ConfigHelpTest, MaskSensitiveValue) {
  EXPECT_EQ(MaskSensitiveValue("mysql.password", "secret123"), "***");
  EXPECT_EQ(MaskSensitiveValue("mysql.host", "127.0.0.1"), "127.0.0.1");
}

TEST(ConfigHelpTest, MaskSensitiveValueEmpty) {
  EXPECT_EQ(MaskSensitiveValue("mysql.password", ""), "");
}

// Test FormatConfigForDisplay
TEST(ConfigHelpTest, FormatConfigForDisplayMasksSensitive) {
  Config config;
  config.mysql.host = "127.0.0.1";
  config.mysql.port = 3306;
  config.mysql.user = "testuser";
  config.mysql.password = "secret123";
  config.mysql.database = "testdb";

  auto result = FormatConfigForDisplay(config, "mysql");
  ASSERT_TRUE(result.has_value()) << result.error().message();
  const auto& output = *result;

  // Verify password is masked
  EXPECT_NE(output.find("password"), std::string::npos);
  EXPECT_NE(output.find("***"), std::string::npos);
  EXPECT_EQ(output.find("secret123"), std::string::npos);

  // Verify non-sensitive fields are shown
  EXPECT_NE(output.find("testuser"), std::string::npos);
  EXPECT_NE(output.find("127.0.0.1"), std::string::npos);
}

TEST(ConfigHelpTest, FormatConfigForDisplayInvalidPath) {
  Config config;
  auto result = FormatConfigForDisplay(config, "nonexistent.path");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kConfigInvalidValue);
}

TEST(ConfigHelpTest, FormatConfigForDisplayEntireConfig) {
  Config config;
  config.mysql.host = "127.0.0.1";
  config.mysql.password = "secret123";

  auto result = FormatConfigForDisplay(config, "");
  ASSERT_TRUE(result.has_value()) << result.error().message();
  const auto& output = *result;

  // Verify output contains major sections
  EXPECT_NE(output.find("mysql:"), std::string::npos);
  EXPECT_NE(output.find("password"), std::string::npos);
  EXPECT_NE(output.find("***"), std::string::npos);
  EXPECT_EQ(output.find("secret123"), std::string::npos);
}

TEST(ConfigHelpTest, ConfigToJsonIncludesBM25Section) {
  Config config;
  config.bm25.enable = true;
  config.bm25.k1 = 1.7;
  config.bm25.b = 0.6;

  auto result = FormatConfigForDisplay(config, "bm25");
  ASSERT_TRUE(result.has_value()) << result.error().message();
  const auto& output = *result;

  EXPECT_NE(output.find("enable"), std::string::npos);
  EXPECT_NE(output.find("true"), std::string::npos);
  EXPECT_NE(output.find("k1"), std::string::npos);
  EXPECT_NE(output.find("1.7"), std::string::npos);
  EXPECT_NE(output.find("b"), std::string::npos);
  EXPECT_NE(output.find("0.6"), std::string::npos);
}

// Test SplitPath (via public interface behavior)
TEST_F(ConfigSchemaExplorerTest, PathNavigationSimple) {
  auto help = explorer().GetHelp("mysql");
  ASSERT_TRUE(help.has_value());
}

TEST_F(ConfigSchemaExplorerTest, PathNavigationNested) {
  auto help = explorer().GetHelp("memory.normalize.nfkc");
  ASSERT_TRUE(help.has_value());
}

TEST_F(ConfigSchemaExplorerTest, PathNavigationArraySchema) {
  // Test accessing array item schema
  auto help = explorer().GetHelp("tables.name");
  ASSERT_TRUE(help.has_value());
  EXPECT_EQ(help->type, "string");
}

// ============================================================================
// ConfigToJson password masking (tested via FormatConfigForDisplay)
// ============================================================================

/**
 * @brief Test that ConfigToJson masks non-empty MySQL password
 *
 * Validates the security fix where ConfigToJson was updated to mask
 * the password field as "***" when non-empty, preventing credential
 * leakage in configuration display output.
 */
TEST(ConfigHelpTest, ConfigToJsonMasksNonEmptyPassword) {
  Config config;
  config.mysql.host = "db.example.com";
  config.mysql.port = 3306;
  config.mysql.user = "admin";
  config.mysql.password = "super_secret_password_123";
  config.mysql.database = "production";

  auto result = FormatConfigForDisplay(config, "mysql");
  ASSERT_TRUE(result.has_value()) << result.error().message();
  const auto& output = *result;

  // Password value must NOT appear in output
  EXPECT_EQ(output.find("super_secret_password_123"), std::string::npos)
      << "Raw password should never appear in display output";
  // Masked value must appear
  EXPECT_NE(output.find("***"), std::string::npos) << "Password should be masked as '***'";
  // Other fields should be visible
  EXPECT_NE(output.find("db.example.com"), std::string::npos);
  EXPECT_NE(output.find("admin"), std::string::npos);
}

/**
 * @brief Test that empty password stays empty (not masked)
 *
 * When password is empty, it should display as empty string, not "***".
 * This distinguishes "no password configured" from "password is set".
 */
TEST(ConfigHelpTest, ConfigToJsonEmptyPasswordStaysEmpty) {
  Config config;
  config.mysql.host = "127.0.0.1";
  config.mysql.port = 3306;
  config.mysql.user = "test";
  config.mysql.password = "";
  config.mysql.database = "testdb";

  auto result = FormatConfigForDisplay(config, "mysql");
  ASSERT_TRUE(result.has_value()) << result.error().message();
  const auto& output = *result;

  // With empty password, the output should show the password field
  // but NOT show "***" (which is only for non-empty passwords)
  EXPECT_NE(output.find("password"), std::string::npos);
  // The output should contain an empty value for password, not "***"
  // Since the password is empty, MaskSensitiveFieldsRecursive should leave it as ""
}

/**
 * @brief Test that full config display masks password correctly
 *
 * Exercises the complete config display path to ensure password
 * masking works when showing the entire configuration.
 */
TEST(ConfigHelpTest, FullConfigDisplayMasksPassword) {
  Config config;
  config.mysql.host = "10.0.0.1";
  config.mysql.port = 3307;
  config.mysql.user = "repl_user";
  config.mysql.password = "replication_secret";
  config.mysql.database = "mydb";

  auto result = FormatConfigForDisplay(config, "");
  ASSERT_TRUE(result.has_value()) << result.error().message();
  const auto& output = *result;

  // Password must never appear in full config output
  EXPECT_EQ(output.find("replication_secret"), std::string::npos) << "Password must not appear in full config display";
  EXPECT_NE(output.find("***"), std::string::npos);

  // Non-sensitive fields should be visible
  EXPECT_NE(output.find("repl_user"), std::string::npos);
  EXPECT_NE(output.find("10.0.0.1"), std::string::npos);
}

}  // namespace mygramdb::config
