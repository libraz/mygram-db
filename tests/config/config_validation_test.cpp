/**
 * @file config_validation_test.cpp
 * @brief Unit tests for configuration validation rules
 */

#include <gtest/gtest.h>

#include <fstream>
#include <nlohmann/json.hpp>

#include "config/config.h"
#include "config/config_help.h"

using namespace mygramdb::config;
using json = nlohmann::json;

/**
 * @brief Test kanji_ngram_size negative value is rejected
 */
TEST(ConfigValidationTest, KanjiNgramSizeNegativeRejected) {
  // Create a minimal valid YAML config with negative kanji_ngram_size
  std::string config_path = "test_kanji_ngram_negative.yaml";
  {
    std::ofstream ofs(config_path);
    ofs << "tables:\n"
        << "  - name: test_table\n"
        << "    primary_key: id\n"
        << "    text_source:\n"
        << "      column: content\n"
        << "    kanji_ngram_size: -1\n";
  }

  auto result = LoadConfig(config_path);
  EXPECT_FALSE(result.has_value()) << "Negative kanji_ngram_size should be rejected";
  if (!result.has_value()) {
    EXPECT_NE(result.error().message().find("kanji_ngram_size"), std::string::npos)
        << "Error should mention kanji_ngram_size";
  }

  std::remove(config_path.c_str());
}

/**
 * @brief Test IsSensitiveField: "primary_key" should NOT be sensitive
 */
TEST(ConfigValidationTest, PrimaryKeyNotSensitive) {
  EXPECT_FALSE(IsSensitiveField("tables.primary_key")) << "primary_key should not be marked as sensitive";
  EXPECT_FALSE(IsSensitiveField("primary_key")) << "primary_key should not be marked as sensitive";
}

/**
 * @brief Test IsSensitiveField: "password" IS sensitive
 */
TEST(ConfigValidationTest, PasswordIsSensitive) {
  EXPECT_TRUE(IsSensitiveField("mysql.password")) << "password should be marked as sensitive";
}

/**
 * @brief Test IsSensitiveField: "ssl_key" IS sensitive
 */
TEST(ConfigValidationTest, SslKeyIsSensitive) {
  EXPECT_TRUE(IsSensitiveField("mysql.ssl_key")) << "ssl_key should be marked as sensitive";
}
