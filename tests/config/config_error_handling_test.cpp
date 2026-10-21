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
  // Supply the post-environment required values so parsing reaches the SSL
  // path check instead of correctly stopping at the earlier semantic guard.
  json config_json = {{"mysql", {{"user", "replicator"}, {"database", "testdb"}, {"ssl_ca", "/../../../etc/passwd"}}}};

  auto result = internal::ParseConfigFromJson(config_json);
  ASSERT_FALSE(result.has_value()) << "ParseConfigFromJson should fail for path traversal in ssl_ca";
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kConfigInvalidValue);
}

TEST(ConfigErrorHandlingTest, ParseConfigFromJsonRejectsSynonymsFilePathTraversal) {
  json config_json = {{"mysql", {{"user", "test_user"}, {"database", "test_db"}}},
                      {"tables", json::array({{{"name", "test"},
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
      {"mysql", {{"user", "test_user"}, {"database", "test_db"}}},
      {"replication", {{"enable", false}}},
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

/**
 * @brief A missing or empty YAML file reports the same error as the JSON equivalent
 */
TEST(ConfigErrorHandlingTest, MissingAndEmptyYamlMatchJsonErrors) {
  const auto dir = std::filesystem::temp_directory_path() / "mygramdb_missing_empty_config_test";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);

  const auto missing_yaml = LoadConfig((dir / "missing.yaml").string());
  const auto missing_json = LoadConfig((dir / "missing.json").string());
  ASSERT_FALSE(missing_yaml);
  ASSERT_FALSE(missing_json);
  EXPECT_EQ(missing_yaml.error().code(), mygram::utils::ErrorCode::kConfigFileNotFound);
  EXPECT_EQ(missing_yaml.error().code(), missing_json.error().code());
  EXPECT_NE(missing_yaml.error().message().find("Failed to open configuration file"), std::string::npos)
      << missing_yaml.error().message();

  std::ofstream(dir / "empty.yaml").close();
  std::ofstream(dir / "empty.json").close();
  const auto empty_yaml = LoadConfig((dir / "empty.yaml").string());
  const auto empty_json = LoadConfig((dir / "empty.json").string());
  ASSERT_FALSE(empty_yaml);
  ASSERT_FALSE(empty_json);
  EXPECT_EQ(empty_yaml.error().code(), empty_json.error().code());
  EXPECT_NE(empty_yaml.error().message().find("Configuration file is empty"), std::string::npos)
      << empty_yaml.error().message();

  std::filesystem::remove_all(dir);
}

/**
 * @brief Validation against loaded --schema text refuses what the loader refuses
 */
TEST(ConfigErrorHandlingTest, LoadConfigForValidationWithSchemaTextAppliesCustomSchema) {
  const auto dir = std::filesystem::temp_directory_path() / "mygramdb_validation_schema_text_test";
  std::filesystem::remove_all(dir);
  std::filesystem::create_directories(dir);
  const auto schema_path = dir / "schema.json";
  const std::string schema = R"({"properties":{"api":{"properties":{"max_query_length":{"maximum":256}}}}})";
  std::ofstream(schema_path) << schema;
  const auto write_config = [&dir](const std::string& name, int max_query_length) {
    const auto path = dir / name;
    std::ofstream(path) << "mysql:\n  user: \"test\"\n  database: \"test\"\n"
                        << "tables:\n  - name: \"docs\"\n    text_source:\n      column: \"body\"\n"
                        << "replication:\n  enable: false\n"
                        << "api:\n  max_query_length: " << max_query_length << "\n";
    return path.string();
  };
  const std::string within = write_config("within.yaml", 200);
  const std::string beyond = write_config("beyond.yaml", 4096);

  auto loaded = LoadConfig(within, schema_path.string());
  ASSERT_TRUE(loaded) << loaded.error().to_string();
  EXPECT_EQ(loaded->custom_schema, schema);

  EXPECT_TRUE(LoadConfigForValidationWithSchemaText(within, loaded->custom_schema));
  auto refused = LoadConfigForValidationWithSchemaText(beyond, loaded->custom_schema);
  ASSERT_FALSE(refused);
  auto refused_by_path = LoadConfigForValidation(beyond, schema_path.string());
  ASSERT_FALSE(refused_by_path);
  EXPECT_EQ(refused.error().code(), refused_by_path.error().code());
  EXPECT_TRUE(LoadConfigForValidationWithSchemaText(beyond, ""))
      << "an empty schema text applies the built-in schema only";

  std::filesystem::remove_all(dir);
}

TEST(ConfigErrorHandlingTest, RequiredFilterMissingTypeListsSchemaTypes) {
  json config_json = {
      {"mysql", {{"host", "127.0.0.1"}, {"user", "test"}, {"password", "test"}, {"database", "test"}}},
      {"tables",
       json::array({{{"name", "test_table"},
                     {"primary_key", "id"},
                     {"text_source", {{"column", "content"}}},
                     {"required_filters", json::array({{{"name", "created_at"}, {"op", "IS NOT NULL"}}})}}})}};

  auto result = internal::ParseConfigFromJson(config_json);

  ASSERT_FALSE(result.has_value());
  const std::string message = result.error().message();
  EXPECT_NE(message.find("time"), std::string::npos);
  EXPECT_EQ(message.find("bool"), std::string::npos);
  EXPECT_NE(message.find("bigint_unsigned"), std::string::npos);
}

TEST(ConfigErrorHandlingTest, ParseConfigFromJsonPropagatesInvalidServerId) {
  json config_json = {{"mysql", {{"user", "test_user"}, {"database", "test_db"}}},
                      {"replication", {{"enable", true}, {"server_id", 0}, {"start_from", "snapshot"}}}};

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
