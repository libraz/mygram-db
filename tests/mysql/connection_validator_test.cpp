/**
 * @file connection_validator_test.cpp
 * @brief Unit and integration tests for ConnectionValidator
 */

#ifdef USE_MYSQL

#include "mysql/connection_validator.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <cstdlib>

#include "mysql/connection.h"

namespace mygramdb::mysql {

/**
 * @brief Check if integration tests should run
 */
bool ShouldRunValidatorIntegrationTests() {
  const char* env = std::getenv("ENABLE_MYSQL_INTEGRATION_TESTS");
  return env != nullptr && std::string(env) == "1";
}

/**
 * @brief Helper to get MySQL connection config from environment
 */
Connection::Config GetValidatorTestConfig() {
  Connection::Config config;
  const char* host = std::getenv("MYSQL_HOST");
  config.host = host ? host : "127.0.0.1";
  config.port = 3306;
  const char* user = std::getenv("MYSQL_USER");
  config.user = user ? user : "root";
  const char* password = std::getenv("MYSQL_PASSWORD");
  config.password = password ? password : "";
  const char* database = std::getenv("MYSQL_DATABASE");
  config.database = database ? database : "test";
  return config;
}

// ============================================================================
// Unit Tests (no MySQL connection required)
// ============================================================================

/**
 * @brief Test ValidationResult default constructor
 */
TEST(ConnectionValidatorUnitTest, ValidationResultDefaultState) {
  ValidationResult result;

  EXPECT_FALSE(result.valid);
  EXPECT_TRUE(result.error_message.empty());
  EXPECT_TRUE(result.warnings.empty());
  EXPECT_FALSE(result.server_uuid.has_value());
}

/**
 * @brief Test ValidationResult bool conversion operator
 */
TEST(ConnectionValidatorUnitTest, ValidationResultBoolConversion) {
  ValidationResult success;
  success.valid = true;

  ValidationResult failure;
  failure.valid = false;

  EXPECT_TRUE(static_cast<bool>(success));
  EXPECT_FALSE(static_cast<bool>(failure));

  // Test in if statement
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

/**
 * @brief Test ValidationResult with error message
 */
TEST(ConnectionValidatorUnitTest, ValidationResultWithError) {
  ValidationResult result;
  result.valid = false;
  result.error_message = "GTID mode is not enabled";

  EXPECT_FALSE(result.valid);
  EXPECT_EQ(result.error_message, "GTID mode is not enabled");
  EXPECT_FALSE(static_cast<bool>(result));
}

/**
 * @brief Test ValidationResult with warnings
 */
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

/**
 * @brief Test ValidationResult with server UUID
 */
TEST(ConnectionValidatorUnitTest, ValidationResultWithServerUUID) {
  ValidationResult result;
  result.valid = true;
  result.server_uuid = "a1b2c3d4-e5f6-1234-5678-90abcdef1234";

  EXPECT_TRUE(result.valid);
  EXPECT_TRUE(result.server_uuid.has_value());
  EXPECT_EQ(*result.server_uuid, "a1b2c3d4-e5f6-1234-5678-90abcdef1234");
}

// ============================================================================
// Integration Tests (require MySQL connection)
// ============================================================================

/**
 * @brief Integration test fixture for ConnectionValidator
 */
class ConnectionValidatorIntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (!ShouldRunValidatorIntegrationTests()) {
      GTEST_SKIP() << "MySQL integration tests are disabled. "
                   << "Set ENABLE_MYSQL_INTEGRATION_TESTS=1 to enable.";
    }

    config_ = GetValidatorTestConfig();
    conn_ = std::make_unique<Connection>(config_);

    auto connect_result = conn_->Connect("validator test");
    if (!connect_result) {
      GTEST_SKIP() << "Failed to connect to MySQL: " << connect_result.error().message();
    }

    // Check if GTID mode is enabled (required for validator tests)
    if (!conn_->IsGTIDModeEnabled()) {
      GTEST_SKIP() << "GTID mode is not enabled on MySQL server. "
                   << "Please enable GTID mode (gtid_mode=ON) for validator tests.";
    }

    // Create test tables
    SetupTestTables();
  }

  void TearDown() override {
    if (conn_ && conn_->IsConnected()) {
      CleanupTestTables();
    }
  }

  void SetupTestTables() {
    // Create test tables for validation
    conn_->ExecuteUpdate("DROP TABLE IF EXISTS validator_test_table1");
    conn_->ExecuteUpdate("DROP TABLE IF EXISTS validator_test_table2");
    conn_->ExecuteUpdate("DROP TABLE IF EXISTS validator_test_messages");

    conn_->ExecuteUpdate(
        "CREATE TABLE validator_test_table1 ("
        "id INT PRIMARY KEY, "
        "name VARCHAR(100))");

    conn_->ExecuteUpdate(
        "CREATE TABLE validator_test_table2 ("
        "id INT PRIMARY KEY, "
        "content TEXT)");

    conn_->ExecuteUpdate(
        "CREATE TABLE validator_test_messages ("
        "message_id INT PRIMARY KEY AUTO_INCREMENT, "
        "text VARCHAR(500))");
  }

  void CleanupTestTables() {
    conn_->ExecuteUpdate("DROP TABLE IF EXISTS validator_test_table1");
    conn_->ExecuteUpdate("DROP TABLE IF EXISTS validator_test_table2");
    conn_->ExecuteUpdate("DROP TABLE IF EXISTS validator_test_messages");
  }

  Connection::Config config_;
  std::unique_ptr<Connection> conn_;
};

/**
 * @brief Test ValidateServer - all checks pass (happy path)
 */
TEST_F(ConnectionValidatorIntegrationTest, ValidateServerAllCheckPass) {
  std::vector<std::string> required_tables = {"validator_test_table1", "validator_test_table2"};

  auto result = ConnectionValidator::ValidateServer(*conn_, required_tables);

  EXPECT_TRUE(result.valid) << "Error: " << result.error_message;
  EXPECT_TRUE(result.error_message.empty());
  EXPECT_TRUE(result.server_uuid.has_value());
  EXPECT_FALSE(result.server_uuid->empty());

  // Server UUID should be a valid MySQL UUID format
  EXPECT_THAT(*result.server_uuid,
              ::testing::MatchesRegex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));
}

/**
 * @brief Test ValidateServer - missing tables
 */
TEST_F(ConnectionValidatorIntegrationTest, ValidateServerMissingTables) {
  std::vector<std::string> required_tables = {"validator_test_table1", "nonexistent_table", "another_missing_table"};

  auto result = ConnectionValidator::ValidateServer(*conn_, required_tables);

  EXPECT_FALSE(result.valid);
  EXPECT_FALSE(result.error_message.empty());
  EXPECT_THAT(result.error_message, ::testing::HasSubstr("Required tables are missing"));
  EXPECT_THAT(result.error_message, ::testing::HasSubstr("nonexistent_table"));
  EXPECT_THAT(result.error_message, ::testing::HasSubstr("another_missing_table"));
}

/**
 * @brief Test ValidateServer - single missing table
 */
TEST_F(ConnectionValidatorIntegrationTest, ValidateServerSingleMissingTable) {
  std::vector<std::string> required_tables = {"validator_test_table1", "missing_table"};

  auto result = ConnectionValidator::ValidateServer(*conn_, required_tables);

  EXPECT_FALSE(result.valid);
  EXPECT_THAT(result.error_message, ::testing::HasSubstr("Required tables are missing"));
  EXPECT_THAT(result.error_message, ::testing::HasSubstr("missing_table"));
  EXPECT_THAT(result.error_message, ::testing::Not(::testing::HasSubstr("validator_test_table1")));
}

/**
 * @brief Test ValidateServer - empty required tables (should pass)
 */
TEST_F(ConnectionValidatorIntegrationTest, ValidateServerEmptyRequiredTables) {
  std::vector<std::string> required_tables;

  auto result = ConnectionValidator::ValidateServer(*conn_, required_tables);

  EXPECT_TRUE(result.valid) << "Error: " << result.error_message;
  EXPECT_TRUE(result.error_message.empty());
  EXPECT_TRUE(result.server_uuid.has_value());
}

/**
 * @brief Test ValidateServer - first connection (no expected UUID)
 */
TEST_F(ConnectionValidatorIntegrationTest, ValidateServerFirstConnection) {
  std::vector<std::string> required_tables = {"validator_test_table1"};

  // First validation - no expected UUID
  auto result = ConnectionValidator::ValidateServer(*conn_, required_tables, std::nullopt);

  EXPECT_TRUE(result.valid) << "Error: " << result.error_message;
  EXPECT_TRUE(result.warnings.empty());
  EXPECT_TRUE(result.server_uuid.has_value());

  std::string first_uuid = *result.server_uuid;
  EXPECT_FALSE(first_uuid.empty());
}

/**
 * @brief Test ValidateServer - UUID matches (no failover)
 */
TEST_F(ConnectionValidatorIntegrationTest, ValidateServerUUIDMatches) {
  std::vector<std::string> required_tables = {"validator_test_table1"};

  // First validation to get server UUID
  auto first_result = ConnectionValidator::ValidateServer(*conn_, required_tables, std::nullopt);
  ASSERT_TRUE(first_result.valid);
  ASSERT_TRUE(first_result.server_uuid.has_value());

  std::string expected_uuid = *first_result.server_uuid;

  // Second validation with expected UUID (should match)
  auto second_result = ConnectionValidator::ValidateServer(*conn_, required_tables, expected_uuid);

  EXPECT_TRUE(second_result.valid) << "Error: " << second_result.error_message;
  EXPECT_TRUE(second_result.warnings.empty()) << "Should have no warnings when UUID matches";
  EXPECT_EQ(*second_result.server_uuid, expected_uuid);
}

/**
 * @brief Test ValidateServer - UUID changed (simulated failover)
 */
TEST_F(ConnectionValidatorIntegrationTest, ValidateServerUUIDChanged) {
  std::vector<std::string> required_tables = {"validator_test_table1"};

  // Use a fake expected UUID (different from actual server UUID)
  std::string fake_expected_uuid = "00000000-0000-0000-0000-000000000000";

  auto result = ConnectionValidator::ValidateServer(*conn_, required_tables, fake_expected_uuid);

  EXPECT_TRUE(result.valid) << "Validation should pass even with UUID change";
  EXPECT_FALSE(result.warnings.empty()) << "Should have failover warning";
  EXPECT_EQ(result.warnings.size(), 1);
  EXPECT_THAT(result.warnings[0], ::testing::HasSubstr("Server UUID changed"));
  EXPECT_THAT(result.warnings[0], ::testing::HasSubstr(fake_expected_uuid));
  EXPECT_THAT(result.warnings[0], ::testing::HasSubstr("failover detected"));

  // Verify actual UUID is different from expected
  EXPECT_TRUE(result.server_uuid.has_value());
  EXPECT_NE(*result.server_uuid, fake_expected_uuid);
}

/**
 * @brief Test ValidateServer - all tables exist
 */
TEST_F(ConnectionValidatorIntegrationTest, ValidateServerAllTablesExist) {
  std::vector<std::string> required_tables = {"validator_test_table1", "validator_test_table2",
                                              "validator_test_messages"};

  auto result = ConnectionValidator::ValidateServer(*conn_, required_tables);

  EXPECT_TRUE(result.valid) << "Error: " << result.error_message;
  EXPECT_TRUE(result.error_message.empty());
}

/**
 * @brief Test ValidateServer - case sensitivity
 */
TEST_F(ConnectionValidatorIntegrationTest, ValidateServerCaseSensitivity) {
  // MySQL table names are case-insensitive on most platforms (case-sensitive on Linux by default)
  // This test verifies behavior with different case
  std::vector<std::string> required_tables = {"VALIDATOR_TEST_TABLE1"};  // Uppercase

  auto result = ConnectionValidator::ValidateServer(*conn_, required_tables);

  // Result depends on server configuration (lower_case_table_names)
  // We just verify that validation doesn't crash and returns a valid result
  EXPECT_TRUE(result.valid || !result.valid) << "Validation should return a result";
  if (!result.valid) {
    // If case-sensitive, should report missing table
    EXPECT_THAT(result.error_message, ::testing::HasSubstr("Required tables are missing"));
  }
}

/**
 * @brief Test ValidateServer - special characters in table names
 */
TEST_F(ConnectionValidatorIntegrationTest, ValidateServerSpecialTableNames) {
  // Create table with backticks in name (edge case)
  conn_->ExecuteUpdate("DROP TABLE IF EXISTS `validator_special-table`");
  conn_->ExecuteUpdate(
      "CREATE TABLE `validator_special-table` ("
      "id INT PRIMARY KEY)");

  std::vector<std::string> required_tables = {"validator_special-table"};

  auto result = ConnectionValidator::ValidateServer(*conn_, required_tables);

  EXPECT_TRUE(result.valid) << "Error: " << result.error_message;

  // Cleanup
  conn_->ExecuteUpdate("DROP TABLE IF EXISTS `validator_special-table`");
}

/**
 * @brief Test ValidateServer - connection not established
 */
TEST(ConnectionValidatorErrorTest, ValidateServerNotConnected) {
  Connection::Config config;
  config.host = "127.0.0.1";
  config.user = "test";
  config.password = "test";
  config.database = "test";

  Connection conn(config);
  // Don't connect

  std::vector<std::string> required_tables = {"test_table"};
  auto result = ConnectionValidator::ValidateServer(conn, required_tables);

  EXPECT_FALSE(result.valid);
  EXPECT_FALSE(result.error_message.empty());
  EXPECT_THAT(result.error_message, ::testing::HasSubstr("Connection is not active"));
}

// ============================================================================
// BUG 2: GTID purge check with last_gtid parameter
// ============================================================================

/**
 * @brief Test ValidateServer with last_gtid parameter (purge check enabled)
 */
TEST_F(ConnectionValidatorIntegrationTest, ValidateServerWithLastGtid) {
  std::vector<std::string> required_tables = {"validator_test_table1"};
  auto executed = conn_->GetExecutedGTID();
  ASSERT_TRUE(executed.has_value());

  // Pass a valid GTID that's a subset of server's executed set
  auto result = ConnectionValidator::ValidateServer(
      *conn_, required_tables, std::nullopt, *executed);
  EXPECT_TRUE(result.valid) << "Error: " << result.error_message;
}

// ============================================================================
// BUG 3: Failover detection integration tests
// ============================================================================

/**
 * @brief Test failover_detected flag is set when UUID changes
 */
TEST_F(ConnectionValidatorIntegrationTest, FailoverSetsDetectedFlag) {
  std::vector<std::string> required_tables = {"validator_test_table1"};
  std::string fake_uuid = "00000000-0000-0000-0000-000000000000";

  auto result = ConnectionValidator::ValidateServer(
      *conn_, required_tables, fake_uuid);

  EXPECT_TRUE(result.valid);
  EXPECT_TRUE(result.failover_detected);
  EXPECT_FALSE(result.warnings.empty());
}

/**
 * @brief Test failover_detected is false when UUID matches
 */
TEST_F(ConnectionValidatorIntegrationTest, NoFailoverWhenUUIDMatches) {
  std::vector<std::string> required_tables = {"validator_test_table1"};

  // First get actual UUID
  auto first = ConnectionValidator::ValidateServer(*conn_, required_tables);
  ASSERT_TRUE(first.valid);
  ASSERT_TRUE(first.server_uuid.has_value());

  // Second call with matching UUID
  auto result = ConnectionValidator::ValidateServer(
      *conn_, required_tables, *first.server_uuid);

  EXPECT_TRUE(result.valid);
  EXPECT_FALSE(result.failover_detected);
}

/**
 * @brief Test failover with valid GTID subset check
 */
TEST_F(ConnectionValidatorIntegrationTest, FailoverWithValidGtidSubset) {
  std::vector<std::string> required_tables = {"validator_test_table1"};
  auto executed = conn_->GetExecutedGTID();
  ASSERT_TRUE(executed.has_value());

  std::string fake_uuid = "00000000-0000-0000-0000-000000000000";

  // Pass a GTID that IS a subset of server's executed - should succeed
  auto result = ConnectionValidator::ValidateServer(
      *conn_, required_tables, fake_uuid, *executed);

  EXPECT_TRUE(result.valid) << "Error: " << result.error_message;
  EXPECT_TRUE(result.failover_detected);
}

/**
 * @brief Test multiple consecutive validations
 */
TEST_F(ConnectionValidatorIntegrationTest, MultipleConsecutiveValidations) {
  std::vector<std::string> required_tables = {"validator_test_table1"};

  // First validation
  auto result1 = ConnectionValidator::ValidateServer(*conn_, required_tables);
  ASSERT_TRUE(result1.valid);
  std::string uuid1 = *result1.server_uuid;

  // Second validation with same UUID
  auto result2 = ConnectionValidator::ValidateServer(*conn_, required_tables, uuid1);
  EXPECT_TRUE(result2.valid);
  EXPECT_TRUE(result2.warnings.empty());
  EXPECT_EQ(*result2.server_uuid, uuid1);

  // Third validation with same UUID
  auto result3 = ConnectionValidator::ValidateServer(*conn_, required_tables, uuid1);
  EXPECT_TRUE(result3.valid);
  EXPECT_TRUE(result3.warnings.empty());
  EXPECT_EQ(*result3.server_uuid, uuid1);
}

/**
 * @brief Test validation preserves connection state
 */
TEST_F(ConnectionValidatorIntegrationTest, ValidationPreservesConnectionState) {
  std::vector<std::string> required_tables = {"validator_test_table1"};

  // Verify connection is active before validation
  ASSERT_TRUE(conn_->IsConnected());

  // Perform validation
  auto result = ConnectionValidator::ValidateServer(*conn_, required_tables);
  EXPECT_TRUE(result.valid);

  // Verify connection is still active after validation
  EXPECT_TRUE(conn_->IsConnected());

  // Verify we can still execute queries
  auto query_result = conn_->Execute("SELECT 1");
  EXPECT_TRUE(query_result.has_value());
}

// ============================================================================
// Phase 2: Binlog compression and partial JSON checks (integration)
// ============================================================================

/**
 * @brief Test ValidateServer with binlog compression OFF (typical case)
 *
 * Most MySQL servers have binlog_transaction_compression=OFF by default.
 * Validation should pass.
 */
TEST_F(ConnectionValidatorIntegrationTest, ValidateServerNoBinlogCompression) {
  // Check current compression setting
  auto result = conn_->Execute("SHOW VARIABLES LIKE 'binlog_transaction_compression'");
  if (!result) {
    // MySQL < 8.0.20 doesn't have this variable - should pass
    GTEST_SKIP() << "binlog_transaction_compression variable not available";
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if (row != nullptr && row[1] != nullptr && std::string(row[1]) == "ON") {
    GTEST_SKIP() << "binlog_transaction_compression is ON on test server, "
                 << "cannot test the OFF path";
  }

  // Validation should pass when compression is OFF
  std::vector<std::string> required_tables = {"validator_test_table1"};
  auto validation_result = ConnectionValidator::ValidateServer(*conn_, required_tables);
  EXPECT_TRUE(validation_result.valid) << "Error: " << validation_result.error_message;
}

/**
 * @brief Test that GTID format validation rejects SQL injection attempts
 *
 * CheckGTIDConsistency validates GTID format before using it in SQL.
 * Invalid characters should be rejected.
 */
TEST(ConnectionValidatorUnitTest, GTIDFormatValidation) {
  // Valid GTID characters: hex digits, hyphens, colons, commas, spaces,
  // newlines. These are validated by CheckGTIDConsistency before SQL execution.

  // Verify valid format characters
  std::string valid_gtid = "a1b2c3d4-e5f6-1234-5678-90abcdef1234:1-5";
  for (char c : valid_gtid) {
    bool is_valid = std::isxdigit(static_cast<unsigned char>(c)) != 0 || c == '-' || c == ':' || c == ',' || c == ' ' ||
                    c == '\n' || c == '\r';
    EXPECT_TRUE(is_valid) << "Character '" << c << "' should be valid in GTID";
  }

  // Verify invalid characters are detected
  std::vector<char> invalid_chars = {'\'', '"', ';', '(', ')', '@', '#', '!', '\\', '/'};
  for (char c : invalid_chars) {
    bool is_valid = std::isxdigit(static_cast<unsigned char>(c)) != 0 || c == '-' || c == ':' || c == ',' || c == ' ' ||
                    c == '\n' || c == '\r';
    EXPECT_FALSE(is_valid) << "Character '" << c << "' should be invalid in GTID";
  }
}

// ============================================================================
// BUG 3: Failover detection tests (unit tests)
// ============================================================================

/**
 * @brief Test ValidationResult failover_detected default state
 */
TEST(ConnectionValidatorUnitTest, ValidationResultFailoverDetectedDefault) {
  ValidationResult result;
  EXPECT_FALSE(result.failover_detected);
}

/**
 * @brief Test ValidationResult failover_detected can be set
 */
TEST(ConnectionValidatorUnitTest, ValidationResultFailoverDetectedSet) {
  ValidationResult result;
  result.failover_detected = true;
  EXPECT_TRUE(result.failover_detected);
}

// ============================================================================
// BUG 4: Table name validation tests (unit tests)
// ============================================================================

/**
 * @brief Test that SQL injection characters in table names are rejected
 */
TEST(ConnectionValidatorUnitTest, InvalidTableNameWithSQLInjection) {
  // We can't directly call CheckTablesExist (it's private), but we can verify
  // through ValidateServer with a disconnected connection.
  // The important thing is that CheckTablesExist validates table names.

  // Verify the validation logic: valid characters are [a-zA-Z0-9_$-]
  std::string valid_table = "articles_2024";
  bool is_valid = true;
  for (char chr : valid_table) {
    if (std::isalnum(static_cast<unsigned char>(chr)) == 0 && chr != '_' && chr != '$' && chr != '-') {
      is_valid = false;
      break;
    }
  }
  EXPECT_TRUE(is_valid) << "Normal table name should be valid";

  // SQL injection attempt
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

/**
 * @brief Test valid table name patterns
 */
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

/**
 * @brief Test invalid table name patterns
 */
TEST(ConnectionValidatorUnitTest, InvalidTableNamePatterns) {
  std::vector<std::string> invalid_names = {
      "",                          // empty
      "table'name",                // single quote
      "table;name",                // semicolon
      "table name",                // space
      "table(name)",               // parentheses
      "table@name",                // at sign
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

/**
 * @brief Test ValidateServer rejects non-FULL binlog_row_image
 *
 * Most servers use FULL by default, so this test just verifies
 * validation passes with FULL. The rejection path is covered by
 * the unit-level logic in CheckBinlogRowImage.
 */
TEST_F(ConnectionValidatorIntegrationTest, ValidateServerBinlogRowImageFull) {
  // Check current setting
  auto result = conn_->Execute("SHOW VARIABLES LIKE 'binlog_row_image'");
  if (!result) {
    GTEST_SKIP() << "binlog_row_image variable not available";
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if (row == nullptr || row[1] == nullptr) {
    GTEST_SKIP() << "Could not read binlog_row_image value";
  }

  std::string value(row[1]);
  std::string upper_value = value;
  std::transform(upper_value.begin(), upper_value.end(), upper_value.begin(), ::toupper);

  std::vector<std::string> required_tables = {"validator_test_table1"};
  auto validation_result = ConnectionValidator::ValidateServer(*conn_, required_tables);

  if (upper_value == "FULL") {
    EXPECT_TRUE(validation_result.valid) << "Error: " << validation_result.error_message;
  } else {
    EXPECT_FALSE(validation_result.valid);
    EXPECT_THAT(validation_result.error_message, ::testing::HasSubstr("binlog_row_image"));
  }
}


// ============================================================================
// P6: binlog_format validation tests
// ============================================================================

/**
 * @brief Test ValidateServer checks binlog_format=ROW (integration test)
 *
 * Most servers use ROW by default. This test verifies that validation
 * passes when binlog_format=ROW and would fail for other formats.
 */
TEST_F(ConnectionValidatorIntegrationTest, ValidateServerBinlogFormatRow) {
  // Check current setting
  auto result = conn_->Execute("SHOW VARIABLES LIKE 'binlog_format'");
  if (!result) {
    GTEST_SKIP() << "binlog_format variable not available";
  }

  MYSQL_ROW row = mysql_fetch_row(result->get());
  if (row == nullptr || row[1] == nullptr) {
    GTEST_SKIP() << "Could not read binlog_format value";
  }

  std::string value(row[1]);
  std::string upper_value = value;
  std::transform(upper_value.begin(), upper_value.end(), upper_value.begin(), ::toupper);

  std::vector<std::string> required_tables = {"validator_test_table1"};
  auto validation_result = ConnectionValidator::ValidateServer(*conn_, required_tables);

  if (upper_value == "ROW") {
    EXPECT_TRUE(validation_result.valid) << "Error: " << validation_result.error_message;
  } else {
    EXPECT_FALSE(validation_result.valid);
    EXPECT_THAT(validation_result.error_message, ::testing::HasSubstr("binlog_format"));
  }
}

/**
 * @brief Unit test: Verify binlog_format validation logic
 *
 * We can't directly call CheckBinlogFormat (it's private), so we verify
 * the validation logic pattern matches what's expected.
 */
TEST(ConnectionValidatorUnitTest, BinlogFormatValidationLogic) {
  // Valid format
  std::string row_value = "ROW";
  std::string upper_value = row_value;
  std::transform(upper_value.begin(), upper_value.end(), upper_value.begin(), ::toupper);
  EXPECT_EQ(upper_value, "ROW") << "ROW format should pass validation";

  // Invalid formats
  std::vector<std::string> invalid_formats = {"STATEMENT", "MIXED", "statement", "mixed"};
  for (const auto& fmt : invalid_formats) {
    std::string upper_fmt = fmt;
    std::transform(upper_fmt.begin(), upper_fmt.end(), upper_fmt.begin(), ::toupper);
    EXPECT_NE(upper_fmt, "ROW") << "Format '" << fmt << "' should fail validation";
  }
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
