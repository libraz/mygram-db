/**
 * @file binlog_filter_validation_test.cpp
 * @brief Tests for filter condition input validation in BinlogReader
 *
 * SECURITY: Validates that invalid filter values (non-numeric strings,
 * out-of-range values, trailing garbage) are properly rejected without
 * throwing exceptions that could crash the server.
 */

#include <gtest/gtest.h>

#include "config/config.h"
#include "mysql/binlog_filter_evaluator.h"

namespace mygramdb::mysql {

class BinlogFilterValidationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // No setup needed
  }

  void TearDown() override {
    // No teardown needed
  }
};

// Test: Invalid integer filter value (non-numeric string)
TEST_F(BinlogFilterValidationTest, InvalidIntegerFilterValue) {
  config::RequiredFilterConfig filter;
  filter.name = "user_id";
  filter.type = "int";
  filter.op = "=";
  filter.value = "not_a_number";  // Invalid

  storage::FilterValue valid_value = static_cast<int64_t>(123);

  // CompareFilterValue should handle invalid filter.value gracefully
  // It will attempt to parse "not_a_number" as int64_t and fail
  // Expected behavior: returns false (doesn't match) without crashing
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(valid_value, filter));
}

// Test: Integer with trailing garbage
TEST_F(BinlogFilterValidationTest, IntegerWithTrailingGarbage) {
  config::RequiredFilterConfig filter;
  filter.name = "user_id";
  filter.type = "int";
  filter.op = "=";
  filter.value = "123abc";  // Valid number with trailing garbage

  storage::FilterValue valid_value = static_cast<int64_t>(123);

  // Should be rejected by the pos != length check
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(valid_value, filter));
}

// Test: Integer out of range
TEST_F(BinlogFilterValidationTest, IntegerOutOfRange) {
  config::RequiredFilterConfig filter;
  filter.name = "user_id";
  filter.type = "bigint";
  filter.op = "=";
  filter.value = "99999999999999999999999999";  // Too large for int64_t

  storage::FilterValue valid_value = static_cast<int64_t>(123);

  // Should be caught by std::out_of_range
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(valid_value, filter));
}

// Test: Invalid float filter value
TEST_F(BinlogFilterValidationTest, InvalidFloatFilterValue) {
  config::RequiredFilterConfig filter;
  filter.name = "price";
  filter.type = "double";
  filter.op = ">";
  filter.value = "not_a_float";  // Invalid

  storage::FilterValue valid_value = 123.45;

  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(valid_value, filter));
}

// Test: Float with trailing garbage
TEST_F(BinlogFilterValidationTest, FloatWithTrailingGarbage) {
  config::RequiredFilterConfig filter;
  filter.name = "price";
  filter.type = "double";
  filter.op = "=";
  filter.value = "123.45extra";  // Valid float with trailing garbage

  storage::FilterValue valid_value = 123.45;

  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(valid_value, filter));
}

// Test: Float out of range
TEST_F(BinlogFilterValidationTest, FloatOutOfRange) {
  config::RequiredFilterConfig filter;
  filter.name = "price";
  filter.type = "double";
  filter.op = "=";
  filter.value = "1e500";  // Too large for double

  storage::FilterValue valid_value = 123.45;

  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(valid_value, filter));
}

// Test: Invalid unsigned integer (negative value)
TEST_F(BinlogFilterValidationTest, InvalidUnsignedInteger) {
  config::RequiredFilterConfig filter;
  filter.name = "timestamp";
  filter.type = "bigint_unsigned";
  filter.op = "=";
  filter.value = "-123";  // Negative value for unsigned type

  storage::FilterValue valid_value = static_cast<uint64_t>(123);

  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(valid_value, filter));
}

// Test: Unsigned integer with trailing garbage
TEST_F(BinlogFilterValidationTest, UnsignedIntegerWithTrailingGarbage) {
  config::RequiredFilterConfig filter;
  filter.name = "timestamp";
  filter.type = "bigint_unsigned";
  filter.op = "=";
  filter.value = "12345xyz";  // Valid number with trailing garbage

  storage::FilterValue valid_value = static_cast<uint64_t>(12345);

  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(valid_value, filter));
}

// Test: Unsigned integer out of range
TEST_F(BinlogFilterValidationTest, UnsignedIntegerOutOfRange) {
  config::RequiredFilterConfig filter;
  filter.name = "timestamp";
  filter.type = "bigint_unsigned";
  filter.op = "=";
  filter.value = "99999999999999999999999999";  // Too large for uint64_t

  storage::FilterValue valid_value = static_cast<uint64_t>(123);

  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(valid_value, filter));
}

// Test: Empty filter value
TEST_F(BinlogFilterValidationTest, EmptyFilterValue) {
  config::RequiredFilterConfig filter;
  filter.name = "user_id";
  filter.type = "int";
  filter.op = "=";
  filter.value = "";  // Empty string

  storage::FilterValue valid_value = static_cast<int64_t>(123);

  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(valid_value, filter));
}

// Test: Whitespace-only filter value
TEST_F(BinlogFilterValidationTest, WhitespaceOnlyFilterValue) {
  config::RequiredFilterConfig filter;
  filter.name = "user_id";
  filter.type = "int";
  filter.op = "=";
  filter.value = "   ";  // Whitespace only

  storage::FilterValue valid_value = static_cast<int64_t>(123);

  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(valid_value, filter));
}

// Test: Special characters in numeric filter
TEST_F(BinlogFilterValidationTest, SpecialCharactersInNumericFilter) {
  config::RequiredFilterConfig filter;
  filter.name = "user_id";
  filter.type = "int";
  filter.op = "=";
  filter.value = "123$#@";  // Number with special characters

  storage::FilterValue valid_value = static_cast<int64_t>(123);

  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(valid_value, filter));
}

// Test: Valid integer filter value (positive test)
TEST_F(BinlogFilterValidationTest, ValidIntegerFilterValue) {
  config::RequiredFilterConfig filter;
  filter.name = "user_id";
  filter.type = "int";
  filter.op = "=";
  filter.value = "12345";  // Valid integer

  storage::FilterValue valid_value = static_cast<int64_t>(12345);

  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(valid_value, filter));
}

// Test: Valid negative integer
TEST_F(BinlogFilterValidationTest, ValidNegativeInteger) {
  config::RequiredFilterConfig filter;
  filter.name = "balance";
  filter.type = "int";
  filter.op = "<";
  filter.value = "-100";  // Valid negative integer

  storage::FilterValue valid_value = static_cast<int64_t>(-200);

  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(valid_value, filter));
}

// Test: Valid float filter value (positive test)
TEST_F(BinlogFilterValidationTest, ValidFloatFilterValue) {
  config::RequiredFilterConfig filter;
  filter.name = "price";
  filter.type = "double";
  filter.op = ">=";
  filter.value = "123.456";  // Valid float

  storage::FilterValue valid_value = 123.456;

  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(valid_value, filter));
}

// Test: Valid scientific notation
TEST_F(BinlogFilterValidationTest, ValidScientificNotation) {
  config::RequiredFilterConfig filter;
  filter.name = "price";
  filter.type = "double";
  filter.op = "=";
  filter.value = "1.23e10";  // Valid scientific notation

  storage::FilterValue valid_value = 1.23e10;

  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(valid_value, filter));
}

}  // namespace mygramdb::mysql
