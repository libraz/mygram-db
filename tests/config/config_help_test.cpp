/**
 * @file config_help_test.cpp
 * @brief Unit tests for configuration help system
 */

#include "config/config_help.h"

#include <gtest/gtest.h>

namespace mygramdb::config {

class ConfigSchemaExplorerTest : public ::testing::Test {
 protected:
  ConfigSchemaExplorer explorer_;
};

// Test GetHelp for simple property
TEST_F(ConfigSchemaExplorerTest, GetHelpForSimpleProperty) {
  auto help = explorer_.GetHelp("mysql.port");

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
  auto help = explorer_.GetHelp("memory.normalize.nfkc");

  ASSERT_TRUE(help.has_value());
  EXPECT_EQ(help->path, "memory.normalize.nfkc");
  EXPECT_EQ(help->type, "boolean");
  EXPECT_EQ(help->default_value, "true");
  EXPECT_FALSE(help->description.empty());
}

// Test GetHelp for enum property
TEST_F(ConfigSchemaExplorerTest, GetHelpForEnumProperty) {
  auto help = explorer_.GetHelp("mysql.binlog_format");

  ASSERT_TRUE(help.has_value());
  EXPECT_EQ(help->type, "string");
  EXPECT_FALSE(help->allowed_values.empty());
  EXPECT_EQ(help->allowed_values.size(), 3);
}

// Test GetHelp for non-existent path
TEST_F(ConfigSchemaExplorerTest, GetHelpForNonExistentPath) {
  auto help = explorer_.GetHelp("nonexistent.path");
  EXPECT_FALSE(help.has_value());
}

// Test GetHelp for root
TEST_F(ConfigSchemaExplorerTest, GetHelpForRoot) {
  auto help = explorer_.GetHelp("");
  ASSERT_TRUE(help.has_value());
  EXPECT_EQ(help->type, "object");
}

// Test ListPaths for root
TEST_F(ConfigSchemaExplorerTest, ListPathsRoot) {
  auto paths = explorer_.ListPaths("");

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
  auto paths = explorer_.ListPaths("mysql");

  EXPECT_FALSE(paths.empty());
  EXPECT_TRUE(paths.find("host") != paths.end());
  EXPECT_TRUE(paths.find("port") != paths.end());
  EXPECT_TRUE(paths.find("user") != paths.end());
  EXPECT_TRUE(paths.find("password") != paths.end());
  EXPECT_TRUE(paths.find("database") != paths.end());
}

// Test ListPaths for nested section
TEST_F(ConfigSchemaExplorerTest, ListPathsForNestedSection) {
  auto paths = explorer_.ListPaths("memory.normalize");

  EXPECT_FALSE(paths.empty());
  EXPECT_TRUE(paths.find("nfkc") != paths.end());
  EXPECT_TRUE(paths.find("width") != paths.end());
  EXPECT_TRUE(paths.find("lower") != paths.end());
}

// Test ListPaths for non-existent path
TEST_F(ConfigSchemaExplorerTest, ListPathsForNonExistentPath) {
  auto paths = explorer_.ListPaths("nonexistent");
  EXPECT_TRUE(paths.empty());
}

// Test FormatHelp
TEST_F(ConfigSchemaExplorerTest, FormatHelp) {
  auto help = explorer_.GetHelp("mysql.port");
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
  auto paths = explorer_.ListPaths("mysql");
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

  std::string output = FormatConfigForDisplay(config, "mysql");

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
  EXPECT_THROW(FormatConfigForDisplay(config, "nonexistent.path"), std::runtime_error);
}

TEST(ConfigHelpTest, FormatConfigForDisplayEntireConfig) {
  Config config;
  config.mysql.host = "127.0.0.1";
  config.mysql.password = "secret123";

  std::string output = FormatConfigForDisplay(config, "");

  // Verify output contains major sections
  EXPECT_NE(output.find("mysql:"), std::string::npos);
  EXPECT_NE(output.find("password"), std::string::npos);
  EXPECT_NE(output.find("***"), std::string::npos);
  EXPECT_EQ(output.find("secret123"), std::string::npos);
}

// Test SplitPath (via public interface behavior)
TEST_F(ConfigSchemaExplorerTest, PathNavigationSimple) {
  auto help = explorer_.GetHelp("mysql");
  ASSERT_TRUE(help.has_value());
}

TEST_F(ConfigSchemaExplorerTest, PathNavigationNested) {
  auto help = explorer_.GetHelp("memory.normalize.nfkc");
  ASSERT_TRUE(help.has_value());
}

TEST_F(ConfigSchemaExplorerTest, PathNavigationArraySchema) {
  // Test accessing array item schema
  auto help = explorer_.GetHelp("tables.name");
  ASSERT_TRUE(help.has_value());
  EXPECT_EQ(help->type, "string");
}

}  // namespace mygramdb::config
