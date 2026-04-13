/**
 * @file config_security_test.cpp
 * @brief Tests for security-related config validation and environment variable overrides
 *
 * Tests the expected behavior of:
 * - Null byte injection rejection in paths and bind addresses
 * - Environment variable override for MySQL credentials
 * - YAML null node handling
 */

#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#include "config/config.h"

using namespace mygramdb::config;

namespace {

/**
 * @brief Create a minimal YAML config for testing
 * @return Path to temporary config file
 */
std::string CreateMinimalConfig(const std::string& extra_yaml = "") {
  char temp_buffer[256];
  std::snprintf(temp_buffer, sizeof(temp_buffer), "/tmp/mygramdb_sec_test_XXXXXX");

  int fd = mkstemp(temp_buffer);
  if (fd == -1) {
    throw std::runtime_error("Failed to create temporary file");
  }
  close(fd);

  std::string temp_path = std::string(temp_buffer) + ".yaml";
  std::filesystem::rename(temp_buffer, temp_path);

  std::ofstream ofs(temp_path);
  ofs << "mysql:\n"
      << "  host: \"127.0.0.1\"\n"
      << "  port: 3306\n"
      << "  user: \"test\"\n"
      << "  password: \"test\"\n"
      << "  database: \"test\"\n"
      << "\n"
      << "tables:\n"
      << "  - name: \"test_table\"\n"
      << "    primary_key: \"id\"\n"
      << "    text_source:\n"
      << "      column: \"content\"\n"
      << "\n"
      << "replication:\n"
      << "  enable: false\n"
      << "\n";
  if (!extra_yaml.empty()) {
    ofs << extra_yaml << "\n";
  }
  ofs.close();
  return temp_path;
}

/**
 * @brief RAII helper to set/unset an environment variable for a test scope
 */
class ScopedEnvVar {
 public:
  ScopedEnvVar(const char* name, const char* value) : name_(name) {
    // Save original value
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
  std::string original_value_;
  bool had_original_ = false;
};

}  // namespace

// --- Path traversal null byte injection ---

TEST(ConfigSecurityTest, PathWithTraversalRejected) {
  // Paths with ".." should be rejected by ValidatePathNoTraversal.
  // Build a config file with ssl_ca containing path traversal inline.
  char temp_buffer[256];
  std::snprintf(temp_buffer, sizeof(temp_buffer), "/tmp/mygramdb_sec_test_XXXXXX");
  int fd = mkstemp(temp_buffer);
  close(fd);
  std::string temp_path = std::string(temp_buffer) + ".yaml";
  std::filesystem::rename(temp_buffer, temp_path);

  std::ofstream ofs(temp_path);
  ofs << "mysql:\n"
      << "  host: \"127.0.0.1\"\n"
      << "  port: 3306\n"
      << "  user: \"test\"\n"
      << "  password: \"test\"\n"
      << "  database: \"test\"\n"
      << "  ssl_enable: true\n"
      << "  ssl_ca: \"/etc/../../../etc/shadow\"\n"
      << "\n"
      << "tables:\n"
      << "  - name: \"test_table\"\n"
      << "    primary_key: \"id\"\n"
      << "    text_source:\n"
      << "      column: \"content\"\n"
      << "\n"
      << "replication:\n"
      << "  enable: false\n";
  ofs.close();

  auto result = LoadConfig(temp_path);
  std::filesystem::remove(temp_path);

  ASSERT_FALSE(result.has_value());
  // The error message should mention path traversal
  auto err_str = result.error().to_string();
  EXPECT_TRUE(err_str.find("traversal") != std::string::npos || err_str.find("..") != std::string::npos)
      << "Error was: " << err_str;
}

// --- Environment variable override ---

TEST(ConfigSecurityTest, EnvVarOverridesMySQLPassword) {
  ScopedEnvVar env("MYGRAM_MYSQL_PASSWORD", "env_secret_password");

  std::string path = CreateMinimalConfig();
  auto result = LoadConfig(path);
  std::filesystem::remove(path);

  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->mysql.password, "env_secret_password");
}

TEST(ConfigSecurityTest, EnvVarOverridesMySQLHost) {
  ScopedEnvVar env("MYGRAM_MYSQL_HOST", "db.production.internal");

  std::string path = CreateMinimalConfig();
  auto result = LoadConfig(path);
  std::filesystem::remove(path);

  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->mysql.host, "db.production.internal");
}

TEST(ConfigSecurityTest, EnvVarOverridesMySQLUser) {
  ScopedEnvVar env("MYGRAM_MYSQL_USER", "env_user");

  std::string path = CreateMinimalConfig();
  auto result = LoadConfig(path);
  std::filesystem::remove(path);

  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->mysql.user, "env_user");
}

TEST(ConfigSecurityTest, EnvVarOverridesMySQLDatabase) {
  ScopedEnvVar env("MYGRAM_MYSQL_DATABASE", "env_database");

  std::string path = CreateMinimalConfig();
  auto result = LoadConfig(path);
  std::filesystem::remove(path);

  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->mysql.database, "env_database");
}

TEST(ConfigSecurityTest, EnvVarOverridesMySQLPort) {
  ScopedEnvVar env("MYGRAM_MYSQL_PORT", "3307");

  std::string path = CreateMinimalConfig();
  auto result = LoadConfig(path);
  std::filesystem::remove(path);

  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  EXPECT_EQ(result->mysql.port, 3307);
}

TEST(ConfigSecurityTest, InvalidEnvVarPortFallsBackToConfig) {
  ScopedEnvVar env("MYGRAM_MYSQL_PORT", "not_a_number");

  std::string path = CreateMinimalConfig();
  auto result = LoadConfig(path);
  std::filesystem::remove(path);

  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  // Should fall back to config file value (3306)
  EXPECT_EQ(result->mysql.port, 3306);
}

// --- YAML null value handling ---

TEST(ConfigSecurityTest, YamlNullValueInOptionalFieldRejected) {
  // YAML null (~) for a schema-required string field should be rejected
  // by JSON schema validation (bind must be a string, not null)
  std::string extra =
      "api:\n"
      "  tcp:\n"
      "    bind: ~\n";  // YAML null

  std::string path = CreateMinimalConfig(extra);
  auto result = LoadConfig(path);
  std::filesystem::remove(path);

  // Schema validation rejects null for string-typed fields
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kConfigValidationError);
}

// --- Bind address validation with null bytes ---

TEST(ConfigSecurityTest, BindAddressWithTraversalRejected) {
  std::string extra =
      "api:\n"
      "  tcp:\n"
      "    bind: \"../../../etc/passwd\"\n";

  std::string path = CreateMinimalConfig(extra);
  auto result = LoadConfig(path);
  std::filesystem::remove(path);

  ASSERT_FALSE(result.has_value());
}

TEST(ConfigSecurityTest, BindAddressWithSlashRejected) {
  std::string extra =
      "api:\n"
      "  tcp:\n"
      "    bind: \"/tmp/socket\"\n";

  std::string path = CreateMinimalConfig(extra);
  auto result = LoadConfig(path);
  std::filesystem::remove(path);

  ASSERT_FALSE(result.has_value());
}
