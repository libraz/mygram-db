/**
 * @file bind_address_validation_test.cpp
 * @brief Unit tests for bind address validation in configuration
 */

#include <gtest/gtest.h>

#include <cstdio>
#include <filesystem>
#include <fstream>

#include "config/config.h"

using namespace mygramdb::config;

namespace {

/**
 * @brief Create a temporary YAML config file with API bind settings
 * @param tcp_bind TCP bind address (empty to omit)
 * @param http_bind HTTP bind address (empty to omit)
 * @param http_enable Whether to enable HTTP
 * @return Path to the temporary config file
 */
std::string CreateBindAddressConfig(const std::string& tcp_bind, const std::string& http_bind,
                                    bool http_enable = false) {
  char temp_buffer[256];
  std::snprintf(temp_buffer, sizeof(temp_buffer), "/tmp/mygramdb_bind_test_XXXXXX");

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
      << "\n"
      << "api:\n"
      << "  tcp:\n";

  if (!tcp_bind.empty()) {
    ofs << "    bind: \"" << tcp_bind << "\"\n";
  }
  ofs << "    port: 11016\n";

  if (http_enable || !http_bind.empty()) {
    ofs << "  http:\n";
    if (http_enable) {
      ofs << "    enable: true\n";
    }
    if (!http_bind.empty()) {
      ofs << "    bind: \"" << http_bind << "\"\n";
    }
    ofs << "    port: 8080\n";
  }

  ofs.close();
  return temp_path;
}

/**
 * @brief Create a temporary YAML config file using legacy server format
 * @param host Server host value
 * @return Path to the temporary config file
 */
std::string CreateLegacyServerConfig(const std::string& host) {
  char temp_buffer[256];
  std::snprintf(temp_buffer, sizeof(temp_buffer), "/tmp/mygramdb_bind_test_XXXXXX");

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
      << "\n"
      << "server:\n"
      << "  host: \"" << host << "\"\n"
      << "  port: 11016\n";

  ofs.close();
  return temp_path;
}

}  // namespace

// --- Valid bind addresses ---

TEST(BindAddressValidationTest, ValidIPv4Loopback) {
  std::string path = CreateBindAddressConfig("127.0.0.1", "");
  auto result = LoadConfig(path);
  EXPECT_TRUE(result) << "Failed: " << result.error().to_string();
  if (result) {
    EXPECT_EQ(result->api.tcp.bind, "127.0.0.1");
  }
  std::filesystem::remove(path);
}

TEST(BindAddressValidationTest, ValidIPv4AllInterfaces) {
  std::string path = CreateBindAddressConfig("0.0.0.0", "");
  auto result = LoadConfig(path);
  EXPECT_TRUE(result) << "Failed: " << result.error().to_string();
  if (result) {
    EXPECT_EQ(result->api.tcp.bind, "0.0.0.0");
  }
  std::filesystem::remove(path);
}

TEST(BindAddressValidationTest, ValidIPv6Loopback) {
  std::string path = CreateBindAddressConfig("::1", "");
  auto result = LoadConfig(path);
  EXPECT_TRUE(result) << "Failed: " << result.error().to_string();
  if (result) {
    EXPECT_EQ(result->api.tcp.bind, "::1");
  }
  std::filesystem::remove(path);
}

TEST(BindAddressValidationTest, ValidIPv6AllInterfaces) {
  std::string path = CreateBindAddressConfig("::", "");
  auto result = LoadConfig(path);
  EXPECT_TRUE(result) << "Failed: " << result.error().to_string();
  if (result) {
    EXPECT_EQ(result->api.tcp.bind, "::");
  }
  std::filesystem::remove(path);
}

TEST(BindAddressValidationTest, ValidIPv4Address) {
  std::string path = CreateBindAddressConfig("192.168.1.100", "");
  auto result = LoadConfig(path);
  EXPECT_TRUE(result) << "Failed: " << result.error().to_string();
  if (result) {
    EXPECT_EQ(result->api.tcp.bind, "192.168.1.100");
  }
  std::filesystem::remove(path);
}

TEST(BindAddressValidationTest, ValidHostname) {
  std::string path = CreateBindAddressConfig("myhost.local", "");
  auto result = LoadConfig(path);
  EXPECT_TRUE(result) << "Failed: " << result.error().to_string();
  if (result) {
    EXPECT_EQ(result->api.tcp.bind, "myhost.local");
  }
  std::filesystem::remove(path);
}

TEST(BindAddressValidationTest, ValidHttpBind) {
  std::string path = CreateBindAddressConfig("127.0.0.1", "0.0.0.0", true);
  auto result = LoadConfig(path);
  EXPECT_TRUE(result) << "Failed: " << result.error().to_string();
  if (result) {
    EXPECT_EQ(result->api.tcp.bind, "127.0.0.1");
    EXPECT_EQ(result->api.http.bind, "0.0.0.0");
  }
  std::filesystem::remove(path);
}

TEST(BindAddressValidationTest, ValidHttpBindIPv6) {
  std::string path = CreateBindAddressConfig("127.0.0.1", "::1", true);
  auto result = LoadConfig(path);
  EXPECT_TRUE(result) << "Failed: " << result.error().to_string();
  if (result) {
    EXPECT_EQ(result->api.http.bind, "::1");
  }
  std::filesystem::remove(path);
}

// --- Path traversal rejection ---

TEST(BindAddressValidationTest, RejectPathTraversalInTcpBind) {
  std::string path = CreateBindAddressConfig("../etc/passwd", "");
  auto result = LoadConfig(path);
  EXPECT_FALSE(result);
  if (!result) {
    EXPECT_TRUE(result.error().message().find("..") != std::string::npos);
  }
  std::filesystem::remove(path);
}

TEST(BindAddressValidationTest, RejectPathTraversalInHttpBind) {
  std::string path = CreateBindAddressConfig("127.0.0.1", "../../malicious", true);
  auto result = LoadConfig(path);
  EXPECT_FALSE(result);
  if (!result) {
    EXPECT_TRUE(result.error().message().find("..") != std::string::npos);
  }
  std::filesystem::remove(path);
}

TEST(BindAddressValidationTest, RejectSlashInTcpBind) {
  std::string path = CreateBindAddressConfig("/tmp/socket", "");
  auto result = LoadConfig(path);
  EXPECT_FALSE(result);
  if (!result) {
    EXPECT_TRUE(result.error().message().find("/") != std::string::npos ||
                result.error().message().find("bind") != std::string::npos);
  }
  std::filesystem::remove(path);
}

TEST(BindAddressValidationTest, RejectSlashInHttpBind) {
  std::string path = CreateBindAddressConfig("127.0.0.1", "/var/run/http.sock", true);
  auto result = LoadConfig(path);
  EXPECT_FALSE(result);
  if (!result) {
    EXPECT_TRUE(result.error().message().find("/") != std::string::npos ||
                result.error().message().find("bind") != std::string::npos);
  }
  std::filesystem::remove(path);
}

TEST(BindAddressValidationTest, RejectWhitespaceInBind) {
  std::string path = CreateBindAddressConfig("127.0.0.1 malicious", "");
  auto result = LoadConfig(path);
  EXPECT_FALSE(result);
  if (!result) {
    EXPECT_TRUE(result.error().message().find("whitespace") != std::string::npos);
  }
  std::filesystem::remove(path);
}

// --- Legacy server.host validation ---

TEST(BindAddressValidationTest, ValidLegacyServerHost) {
  std::string path = CreateLegacyServerConfig("0.0.0.0");
  auto result = LoadConfig(path);
  EXPECT_TRUE(result) << "Failed: " << result.error().to_string();
  if (result) {
    EXPECT_EQ(result->api.tcp.bind, "0.0.0.0");
  }
  std::filesystem::remove(path);
}

TEST(BindAddressValidationTest, RejectPathTraversalInLegacyServerHost) {
  std::string path = CreateLegacyServerConfig("../etc/passwd");
  auto result = LoadConfig(path);
  EXPECT_FALSE(result);
  if (!result) {
    EXPECT_TRUE(result.error().message().find("..") != std::string::npos);
  }
  std::filesystem::remove(path);
}

// --- Existing path validations still work ---

TEST(BindAddressValidationTest, ExistingPathTraversalInDumpDir) {
  char temp_buffer[256];
  std::snprintf(temp_buffer, sizeof(temp_buffer), "/tmp/mygramdb_bind_test_XXXXXX");
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
      << "  user: \"test\"\n"
      << "  password: \"test\"\n"
      << "  database: \"test\"\n"
      << "tables:\n"
      << "  - name: \"test_table\"\n"
      << "    text_source:\n"
      << "      column: \"content\"\n"
      << "replication:\n"
      << "  enable: false\n"
      << "dump:\n"
      << "  dir: \"../../../etc\"\n";
  ofs.close();

  auto result = LoadConfig(temp_path);
  EXPECT_FALSE(result);
  if (!result) {
    EXPECT_TRUE(result.error().message().find("Path traversal") != std::string::npos ||
                result.error().message().find("..") != std::string::npos);
  }
  std::filesystem::remove(temp_path);
}

TEST(BindAddressValidationTest, ExistingPathTraversalInLoggingFile) {
  char temp_buffer[256];
  std::snprintf(temp_buffer, sizeof(temp_buffer), "/tmp/mygramdb_bind_test_XXXXXX");
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
      << "  user: \"test\"\n"
      << "  password: \"test\"\n"
      << "  database: \"test\"\n"
      << "tables:\n"
      << "  - name: \"test_table\"\n"
      << "    text_source:\n"
      << "      column: \"content\"\n"
      << "replication:\n"
      << "  enable: false\n"
      << "logging:\n"
      << "  file: \"../../var/log/evil.log\"\n";
  ofs.close();

  auto result = LoadConfig(temp_path);
  EXPECT_FALSE(result);
  if (!result) {
    EXPECT_TRUE(result.error().message().find("Path traversal") != std::string::npos ||
                result.error().message().find("..") != std::string::npos);
  }
  std::filesystem::remove(temp_path);
}
