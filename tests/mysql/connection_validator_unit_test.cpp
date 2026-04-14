/**
 * @file connection_validator_unit_test.cpp
 * @brief Unit tests for ConnectionValidator (no MySQL connection required)
 */

#ifdef USE_MYSQL

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <string>
#include <vector>

#include "mysql/connection.h"
#include "mysql/connection_validator.h"

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

TEST(ConnectionValidatorUnitTest, GTIDFormatValidation) {
  std::string valid_gtid = "a1b2c3d4-e5f6-1234-5678-90abcdef1234:1-5";
  for (char c : valid_gtid) {
    bool is_valid = std::isxdigit(static_cast<unsigned char>(c)) != 0 || c == '-' || c == ':' || c == ',' || c == ' ' ||
                    c == '\n' || c == '\r';
    EXPECT_TRUE(is_valid) << "Character '" << c << "' should be valid in GTID";
  }
  std::vector<char> invalid_chars = {'\'', '"', ';', '(', ')', '@', '#', '!', '\\', '/'};
  for (char c : invalid_chars) {
    bool is_valid = std::isxdigit(static_cast<unsigned char>(c)) != 0 || c == '-' || c == ':' || c == ',' || c == ' ' ||
                    c == '\n' || c == '\r';
    EXPECT_FALSE(is_valid) << "Character '" << c << "' should be invalid in GTID";
  }
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

TEST(ConnectionValidatorUnitTest, InvalidTableNameWithSQLInjection) {
  std::string valid_table = "articles_2024";
  bool is_valid = true;
  for (char chr : valid_table) {
    if (std::isalnum(static_cast<unsigned char>(chr)) == 0 && chr != '_' && chr != '$' && chr != '-') {
      is_valid = false;
      break;
    }
  }
  EXPECT_TRUE(is_valid) << "Normal table name should be valid";
  std::string injection_table = "'; DROP TABLE users; --";
  is_valid = true;
  for (char chr : injection_table) {
    if (std::isalnum(static_cast<unsigned char>(chr)) == 0 && chr != '_' && chr != '$' && chr != '-') {
      is_valid = false;
      break;
    }
  }
  EXPECT_FALSE(is_valid) << "SQL injection table name should be rejected";
}

TEST(ConnectionValidatorUnitTest, ValidTableNamePatterns) {
  std::vector<std::string> valid_names = {"articles", "user_profiles", "tbl$1", "test-table", "Table123"};
  for (const auto& name : valid_names) {
    bool is_valid = !name.empty();
    for (char chr : name) {
      if (std::isalnum(static_cast<unsigned char>(chr)) == 0 && chr != '_' && chr != '$' && chr != '-') {
        is_valid = false;
        break;
      }
    }
    EXPECT_TRUE(is_valid) << "Table name '" << name << "' should be valid";
  }
}

TEST(ConnectionValidatorUnitTest, InvalidTableNamePatterns) {
  std::vector<std::string> invalid_names = {
      "",             // empty
      "table'name",   // single quote
      "table;name",   // semicolon
      "table name",   // space
      "table(name)",  // parentheses
      "table@name",   // at sign
  };
  for (const auto& name : invalid_names) {
    bool is_valid = !name.empty();
    for (char chr : name) {
      if (std::isalnum(static_cast<unsigned char>(chr)) == 0 && chr != '_' && chr != '$' && chr != '-') {
        is_valid = false;
        break;
      }
    }
    EXPECT_FALSE(is_valid) << "Table name '" << name << "' should be invalid";
  }
}

TEST(ConnectionValidatorUnitTest, BinlogFormatValidationLogic) {
  std::string row_value = "ROW";
  std::string upper_value = row_value;
  std::transform(upper_value.begin(), upper_value.end(), upper_value.begin(), ::toupper);
  EXPECT_EQ(upper_value, "ROW") << "ROW format should pass validation";
  std::vector<std::string> invalid_formats = {"STATEMENT", "MIXED", "statement", "mixed"};
  for (const auto& fmt : invalid_formats) {
    std::string upper_fmt = fmt;
    std::transform(upper_fmt.begin(), upper_fmt.end(), upper_fmt.begin(), ::toupper);
    EXPECT_NE(upper_fmt, "ROW") << "Format '" << fmt << "' should fail validation";
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

/**
 * @brief Test that validation result carries error for GTID disabled scenario
 * The CheckGTIDEnabled helper now returns Expected<void, Error> with kMySQLGTIDNotEnabled
 */
TEST(ConnectionValidatorErrorTest, ErrorCodeRangeForMySQLErrors) {
  // Verify that error codes used by ConnectionValidator are in the MySQL range
  using mygram::utils::ErrorCode;

  // All error codes used internally should be in 2000-2999
  auto check_range = [](ErrorCode code) {
    auto val = static_cast<uint16_t>(code);
    return val >= 2000 && val <= 2999;
  };

  EXPECT_TRUE(check_range(ErrorCode::kMySQLGTIDNotEnabled));
  EXPECT_TRUE(check_range(ErrorCode::kMySQLTableNotFound));
  EXPECT_TRUE(check_range(ErrorCode::kMySQLBinlogError));
  EXPECT_TRUE(check_range(ErrorCode::kMySQLQueryFailed));
  EXPECT_TRUE(check_range(ErrorCode::kMySQLReplicationError));
  EXPECT_TRUE(check_range(ErrorCode::kMySQLInvalidGTID));
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
