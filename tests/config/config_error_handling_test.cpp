/**
 * @file config_error_handling_test.cpp
 * @brief Tests for config error handling improvements
 *
 * Tests:
 * - ParseConfigFromJson error propagation
 * - ValidatePathNoTraversal returning Expected error
 * - ValidateBindAddress returning Expected error
 * - Invalid MYGRAM_MYSQL_PORT handling
 */

#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>
#include <string>

#include "config/config.h"
#include "config/config_internal.h"
#include "utils/error.h"
#include "utils/expected.h"

using namespace mygramdb::config;
using json = nlohmann::json;

namespace {

/**
 * @brief Create a temporary YAML config file for testing
 * @param yaml_content YAML content to write
 * @return Path to the temporary config file
 */
std::string CreateTempYamlConfig(const std::string& yaml_content) {
  char temp_buffer[256];
  std::snprintf(temp_buffer, sizeof(temp_buffer), "/tmp/mygramdb_err_test_XXXXXX");

  int fd = mkstemp(temp_buffer);
  if (fd == -1) {
    throw std::runtime_error("Failed to create temporary file");
  }
  close(fd);

  std::string temp_path = std::string(temp_buffer) + ".yaml";
  std::filesystem::rename(temp_buffer, temp_path);

  std::ofstream ofs(temp_path);
  ofs << yaml_content;
  ofs.close();
  return temp_path;
}

/**
 * @brief RAII helper to set/unset an environment variable for a test scope
 */
class ScopedEnvVar {
 public:
  ScopedEnvVar(const char* name, const char* value) : name_(name) {
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    const char* original = std::getenv(name);
    if (original != nullptr) {
      had_original_ = true;
      original_value_ = original;
    }
    // NOLINTNEXTLINE(concurrency-mt-unsafe)
    setenv(name, value, 1);
  }

  ~ScopedEnvVar() {
    if (had_original_) {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      setenv(name_.c_str(), original_value_.c_str(), 1);
    } else {
      // NOLINTNEXTLINE(concurrency-mt-unsafe)
      unsetenv(name_.c_str());
    }
  }

  ScopedEnvVar(const ScopedEnvVar&) = delete;
  ScopedEnvVar& operator=(const ScopedEnvVar&) = delete;

 private:
  std::string name_;
  bool had_original_ = false;
  std::string original_value_;
};

}  // namespace

// ===========================================================================
// ParseConfigFromJson error propagation
// ===========================================================================

TEST(ConfigErrorHandlingTest, ParseConfigFromJsonPropagatesTableError) {
  // JSON with a table missing the required "name" field
  json config_json = {{"tables", json::array({{{"primary_key", "id"}, {"text_source", {{"column", "content"}}}}})}};

  auto result = internal::ParseConfigFromJson(config_json);
  ASSERT_FALSE(result.has_value()) << "ParseConfigFromJson should fail for table missing name";
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kConfigMissingRequired);
}

TEST(ConfigErrorHandlingTest, ParseConfigFromJsonPropagatesMysqlSslPathError) {
  // JSON with path traversal in mysql.ssl_ca
  json config_json = {{"mysql", {{"ssl_ca", "/../../../etc/passwd"}}}};

  auto result = internal::ParseConfigFromJson(config_json);
  ASSERT_FALSE(result.has_value()) << "ParseConfigFromJson should fail for path traversal in ssl_ca";
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kConfigInvalidValue);
}

TEST(ConfigErrorHandlingTest, ParseConfigFromJsonRejectsSynonymsFilePathTraversal) {
  json config_json = {{"tables", json::array({{{"name", "test"},
                                               {"primary_key", "id"},
                                               {"text_source", {{"column", "body"}}},
                                               {"synonyms", {{"enable", true}, {"file", "../synonyms.tsv"}}}}})}};

  auto result = internal::ParseConfigFromJson(config_json);
  ASSERT_FALSE(result.has_value()) << "ParseConfigFromJson should fail for path traversal in synonyms.file";
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kConfigInvalidValue);
  EXPECT_NE(result.error().message().find("synonyms.file"), std::string::npos);
}

TEST(ConfigErrorHandlingTest, ParseConfigFromJsonSucceedsForValidMinimalConfig) {
  json config_json = {
      {"tables", json::array({{{"name", "test"}, {"primary_key", "id"}, {"text_source", {{"column", "body"}}}}})}};

  auto result = internal::ParseConfigFromJson(config_json);
  ASSERT_TRUE(result.has_value()) << "ParseConfigFromJson should succeed: " << result.error().message();
  EXPECT_EQ(result->tables.size(), 1);
  EXPECT_EQ(result->tables[0].name, "test");
}

TEST(ConfigErrorHandlingTest, LoadConfigAcceptsBigintUnsignedFilterTypes) {
  std::string config_path = CreateTempYamlConfig(
      "mysql:\n"
      "  host: \"127.0.0.1\"\n"
      "  port: 3306\n"
      "  user: \"test\"\n"
      "  password: \"test\"\n"
      "  database: \"test\"\n"
      "\n"
      "tables:\n"
      "  - name: \"test_table\"\n"
      "    primary_key: \"id\"\n"
      "    text_source:\n"
      "      column: \"content\"\n"
      "    required_filters:\n"
      "      - name: \"account_id\"\n"
      "        type: \"bigint_unsigned\"\n"
      "        op: \">=\"\n"
      "        value: \"18446744073709551614\"\n"
      "    filters:\n"
      "      - name: \"account_id\"\n"
      "        type: \"bigint_unsigned\"\n"
      "\n"
      "replication:\n"
      "  enable: false\n");

  auto result = LoadConfig(config_path);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  ASSERT_EQ(result->tables.size(), 1);
  ASSERT_EQ(result->tables[0].required_filters.size(), 1);
  ASSERT_EQ(result->tables[0].filters.size(), 1);
  EXPECT_EQ(result->tables[0].required_filters[0].type, "bigint_unsigned");
  EXPECT_EQ(result->tables[0].filters[0].type, "bigint_unsigned");

  std::filesystem::remove(config_path);
}

TEST(ConfigErrorHandlingTest, ParseConfigFromJsonPropagatesInvalidServerId) {
  json config_json = {{"replication", {{"enable", true}, {"server_id", 0}, {"start_from", "snapshot"}}}};

  auto result = internal::ParseConfigFromJson(config_json);
  ASSERT_FALSE(result.has_value()) << "ParseConfigFromJson should fail for zero server_id with replication enabled";
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kConfigValidationError);
}

// ===========================================================================
// ValidatePathNoTraversal returns Expected
// ===========================================================================

TEST(ConfigErrorHandlingTest, ValidatePathNoTraversalAcceptsNormalPath) {
  auto result = internal::ValidatePathNoTraversal("/var/data/mygramdb", "dump.dir");
  EXPECT_TRUE(result.has_value());
}

TEST(ConfigErrorHandlingTest, ValidatePathNoTraversalAcceptsEmptyPath) {
  auto result = internal::ValidatePathNoTraversal("", "dump.dir");
  EXPECT_TRUE(result.has_value());
}

TEST(ConfigErrorHandlingTest, ValidatePathNoTraversalRejectsDotDot) {
  auto result = internal::ValidatePathNoTraversal("/../etc/passwd", "dump.dir");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kConfigInvalidValue);
  EXPECT_NE(result.error().message().find("traversal"), std::string::npos);
}

TEST(ConfigErrorHandlingTest, ValidatePathNoTraversalRejectsBareDotDot) {
  auto result = internal::ValidatePathNoTraversal("..", "ssl_ca");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kConfigInvalidValue);
}

TEST(ConfigErrorHandlingTest, ValidatePathNoTraversalRejectsNullByte) {
  std::string path_with_null = std::string("/var/data") + '\0' + "/evil";
  auto result = internal::ValidatePathNoTraversal(path_with_null, "dump.dir");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kConfigInvalidValue);
  EXPECT_NE(result.error().message().find("null"), std::string::npos);
}

// ===========================================================================
// ValidateBindAddress returns Expected
// ===========================================================================

TEST(ConfigErrorHandlingTest, ValidateBindAddressAcceptsValidIPv4) {
  auto result = internal::ValidateBindAddress("127.0.0.1", "api.tcp.bind");
  EXPECT_TRUE(result.has_value());
}

TEST(ConfigErrorHandlingTest, ValidateBindAddressAcceptsEmpty) {
  auto result = internal::ValidateBindAddress("", "api.tcp.bind");
  EXPECT_TRUE(result.has_value());
}

TEST(ConfigErrorHandlingTest, ValidateBindAddressRejectsPathTraversal) {
  auto result = internal::ValidateBindAddress("127.0.0.1/../evil", "api.tcp.bind");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kConfigInvalidValue);
  EXPECT_NE(result.error().message().find(".."), std::string::npos);
}

TEST(ConfigErrorHandlingTest, ValidateBindAddressRejectsSlash) {
  auto result = internal::ValidateBindAddress("127.0.0.1/24", "api.tcp.bind");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kConfigInvalidValue);
}

TEST(ConfigErrorHandlingTest, ValidateBindAddressRejectsWhitespace) {
  auto result = internal::ValidateBindAddress("127.0.0.1 ", "api.tcp.bind");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kConfigInvalidValue);
  EXPECT_NE(result.error().message().find("whitespace"), std::string::npos);
}

TEST(ConfigErrorHandlingTest, ValidateBindAddressRejectsNullByte) {
  std::string addr_with_null = std::string("127.0.0.1") + '\0';
  auto result = internal::ValidateBindAddress(addr_with_null, "api.tcp.bind");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kConfigInvalidValue);
}

// ===========================================================================
// Invalid MYGRAM_MYSQL_PORT returns a clear config error
// ===========================================================================

TEST(ConfigErrorHandlingTest, InvalidMysqlPortEnvReturnsConfigError) {
  ScopedEnvVar env("MYGRAM_MYSQL_PORT", "not_a_number");

  std::string config_path = CreateTempYamlConfig(
      "mysql:\n"
      "  host: \"127.0.0.1\"\n"
      "  port: 3306\n"
      "  user: \"test\"\n"
      "  password: \"test\"\n"
      "  database: \"test\"\n"
      "\n"
      "tables:\n"
      "  - name: \"test_table\"\n"
      "    primary_key: \"id\"\n"
      "    text_source:\n"
      "      column: \"content\"\n"
      "\n"
      "replication:\n"
      "  enable: false\n");

  auto result = LoadConfig(config_path);
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kConfigInvalidValue);
  EXPECT_NE(result.error().message().find("MYGRAM_MYSQL_PORT"), std::string::npos);

  std::filesystem::remove(config_path);
}

TEST(ConfigErrorHandlingTest, ValidMysqlPortEnvOverridesConfig) {
  ScopedEnvVar env("MYGRAM_MYSQL_PORT", "3307");

  std::string config_path = CreateTempYamlConfig(
      "mysql:\n"
      "  host: \"127.0.0.1\"\n"
      "  port: 3306\n"
      "  user: \"test\"\n"
      "  password: \"test\"\n"
      "  database: \"test\"\n"
      "\n"
      "tables:\n"
      "  - name: \"test_table\"\n"
      "    primary_key: \"id\"\n"
      "    text_source:\n"
      "      column: \"content\"\n"
      "\n"
      "replication:\n"
      "  enable: false\n");

  auto result = LoadConfig(config_path);
  ASSERT_TRUE(result.has_value()) << result.error().message();
  EXPECT_EQ(result->mysql.port, 3307) << "Valid env port should override config";

  std::filesystem::remove(config_path);
}
