/**
 * @file binlog_filter_evaluator_test.cpp
 * @brief Unit tests for BinlogFilterEvaluator class
 *
 * Tests filter value comparison and required_filters evaluation.
 */

#include <gtest/gtest.h>

#ifdef USE_MYSQL

#include <cmath>
#include <limits>
#include <string>
#include <unordered_map>

#include "config/config.h"
#include "mysql/binlog_filter_evaluator.h"
#include "storage/document_store.h"

using namespace mygramdb::mysql;
using namespace mygramdb::config;
using namespace mygramdb::storage;

namespace {

/**
 * @brief Helper to create a RequiredFilterConfig
 */
RequiredFilterConfig MakeFilter(const std::string& name, const std::string& type, const std::string& op,
                                const std::string& value) {
  RequiredFilterConfig filter;
  filter.name = name;
  filter.type = type;
  filter.op = op;
  filter.value = value;
  return filter;
}

/**
 * @brief Helper to create a TableConfig with required_filters
 */
TableConfig MakeTableConfig(const std::vector<RequiredFilterConfig>& required_filters) {
  TableConfig config;
  config.name = "test_table";
  config.primary_key = "id";
  config.required_filters = required_filters;
  return config;
}

}  // namespace

class BinlogFilterEvaluatorTest : public ::testing::Test {};

// ===========================================================================
// Empty required_filters tests
// ===========================================================================

TEST_F(BinlogFilterEvaluatorTest, EmptyRequiredFiltersAlwaysReturnsTrue) {
  TableConfig config = MakeTableConfig({});
  std::unordered_map<std::string, FilterValue> filters;

  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, config));
}

TEST_F(BinlogFilterEvaluatorTest, EmptyRequiredFiltersWithDataReturnsTrue) {
  TableConfig config = MakeTableConfig({});
  std::unordered_map<std::string, FilterValue> filters;
  filters["status"] = int64_t{1};

  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, config));
}

// ===========================================================================
// Integer comparison tests
// ===========================================================================

TEST_F(BinlogFilterEvaluatorTest, IntegerEqualityMatch) {
  auto filter = MakeFilter("status", "int", "=", "1");

  FilterValue value = int64_t{1};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(value, filter));
}

TEST_F(BinlogFilterEvaluatorTest, IntegerEqualityMismatch) {
  auto filter = MakeFilter("status", "int", "=", "1");

  FilterValue value = int64_t{2};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(value, filter));
}

TEST_F(BinlogFilterEvaluatorTest, IntegerNotEqual) {
  auto filter = MakeFilter("status", "int", "!=", "1");

  FilterValue match = int64_t{2};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(match, filter));

  FilterValue mismatch = int64_t{1};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(mismatch, filter));
}

TEST_F(BinlogFilterEvaluatorTest, IntegerLessThan) {
  auto filter = MakeFilter("age", "int", "<", "18");

  FilterValue match = int64_t{17};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(match, filter));

  FilterValue equal = int64_t{18};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(equal, filter));

  FilterValue greater = int64_t{19};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(greater, filter));
}

TEST_F(BinlogFilterEvaluatorTest, IntegerGreaterThan) {
  auto filter = MakeFilter("age", "int", ">", "18");

  FilterValue greater = int64_t{19};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(greater, filter));

  FilterValue equal = int64_t{18};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(equal, filter));
}

TEST_F(BinlogFilterEvaluatorTest, IntegerLessOrEqual) {
  auto filter = MakeFilter("age", "int", "<=", "18");

  FilterValue less = int64_t{17};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(less, filter));

  FilterValue equal = int64_t{18};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(equal, filter));

  FilterValue greater = int64_t{19};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(greater, filter));
}

TEST_F(BinlogFilterEvaluatorTest, IntegerGreaterOrEqual) {
  auto filter = MakeFilter("age", "int", ">=", "18");

  FilterValue greater = int64_t{19};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(greater, filter));

  FilterValue equal = int64_t{18};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(equal, filter));

  FilterValue less = int64_t{17};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(less, filter));
}

TEST_F(BinlogFilterEvaluatorTest, IntegerNegativeValues) {
  auto filter = MakeFilter("offset", "int", "=", "-100");

  FilterValue match = int64_t{-100};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(match, filter));

  FilterValue mismatch = int64_t{100};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(mismatch, filter));
}

// ===========================================================================
// Other integer types tests
// ===========================================================================

TEST_F(BinlogFilterEvaluatorTest, Int8Comparison) {
  auto filter = MakeFilter("tiny", "int", "=", "100");

  FilterValue value = int8_t{100};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(value, filter));
}

TEST_F(BinlogFilterEvaluatorTest, Uint8Comparison) {
  auto filter = MakeFilter("utiny", "int", "=", "200");

  FilterValue value = uint8_t{200};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(value, filter));
}

TEST_F(BinlogFilterEvaluatorTest, Int16Comparison) {
  auto filter = MakeFilter("small", "int", "=", "30000");

  FilterValue value = int16_t{30000};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(value, filter));
}

TEST_F(BinlogFilterEvaluatorTest, Uint16Comparison) {
  auto filter = MakeFilter("usmall", "int", "=", "60000");

  FilterValue value = uint16_t{60000};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(value, filter));
}

TEST_F(BinlogFilterEvaluatorTest, Int32Comparison) {
  auto filter = MakeFilter("medium", "int", "=", "1000000");

  FilterValue value = int32_t{1000000};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(value, filter));
}

TEST_F(BinlogFilterEvaluatorTest, Uint32Comparison) {
  auto filter = MakeFilter("umedium", "int", "=", "3000000000");

  FilterValue value = uint32_t{3000000000};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(value, filter));
}

// ===========================================================================
// Float/Double comparison tests
// ===========================================================================

TEST_F(BinlogFilterEvaluatorTest, DoubleEquality) {
  auto filter = MakeFilter("price", "float", "=", "19.99");

  FilterValue match = 19.99;
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(match, filter));

  // Test with epsilon tolerance (1e-9)
  // 19.990000001 - 19.99 = 1e-9 which is NOT < 1e-9, so it fails
  FilterValue close = 19.9900000001;  // Difference is 1e-10, which is < 1e-9
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(close, filter));
}

TEST_F(BinlogFilterEvaluatorTest, DoubleNotEqual) {
  auto filter = MakeFilter("price", "float", "!=", "19.99");

  FilterValue different = 20.00;
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(different, filter));

  FilterValue same = 19.99;
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(same, filter));
}

TEST_F(BinlogFilterEvaluatorTest, DoubleLessThan) {
  auto filter = MakeFilter("price", "float", "<", "100.0");

  FilterValue less = 99.99;
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(less, filter));

  FilterValue greater = 100.01;
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(greater, filter));
}

TEST_F(BinlogFilterEvaluatorTest, DoubleGreaterThan) {
  auto filter = MakeFilter("price", "float", ">", "0.0");

  FilterValue positive = 0.01;
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(positive, filter));

  FilterValue zero = 0.0;
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(zero, filter));
}

// ===========================================================================
// String comparison tests
// ===========================================================================

TEST_F(BinlogFilterEvaluatorTest, StringEquality) {
  auto filter = MakeFilter("status", "string", "=", "active");

  FilterValue match = std::string{"active"};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(match, filter));

  FilterValue mismatch = std::string{"inactive"};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(mismatch, filter));
}

TEST_F(BinlogFilterEvaluatorTest, StringNotEqual) {
  auto filter = MakeFilter("status", "string", "!=", "deleted");

  FilterValue active = std::string{"active"};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(active, filter));

  FilterValue deleted = std::string{"deleted"};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(deleted, filter));
}

TEST_F(BinlogFilterEvaluatorTest, StringLexicographicComparison) {
  auto filter = MakeFilter("name", "string", "<", "b");

  FilterValue match = std::string{"a"};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(match, filter));

  FilterValue mismatch = std::string{"c"};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(mismatch, filter));
}

TEST_F(BinlogFilterEvaluatorTest, StringCaseSensitive) {
  auto filter = MakeFilter("status", "string", "=", "Active");

  FilterValue lowercase = std::string{"active"};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(lowercase, filter));

  FilterValue exact = std::string{"Active"};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(exact, filter));
}

// ===========================================================================
// NULL value tests
// ===========================================================================

TEST_F(BinlogFilterEvaluatorTest, IsNullWithNullValue) {
  auto filter = MakeFilter("deleted_at", "datetime", "IS NULL", "");

  FilterValue null_value = std::monostate{};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(null_value, filter));
}

TEST_F(BinlogFilterEvaluatorTest, IsNullWithNonNullValue) {
  auto filter = MakeFilter("deleted_at", "datetime", "IS NULL", "");

  FilterValue non_null = int64_t{1234567890};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(non_null, filter));
}

TEST_F(BinlogFilterEvaluatorTest, IsNotNullWithNonNullValue) {
  auto filter = MakeFilter("created_at", "datetime", "IS NOT NULL", "");

  FilterValue non_null = int64_t{1234567890};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(non_null, filter));
}

TEST_F(BinlogFilterEvaluatorTest, IsNotNullWithNullValue) {
  auto filter = MakeFilter("created_at", "datetime", "IS NOT NULL", "");

  FilterValue null_value = std::monostate{};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(null_value, filter));
}

TEST_F(BinlogFilterEvaluatorTest, NullValueWithRegularOperatorReturnsFalse) {
  auto filter = MakeFilter("status", "int", "=", "1");

  FilterValue null_value = std::monostate{};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(null_value, filter));
}

// ===========================================================================
// TIME value tests
// ===========================================================================

TEST_F(BinlogFilterEvaluatorTest, TimeValueEquality) {
  auto filter = MakeFilter("duration", "time", "=", "3600");  // 1 hour in seconds

  FilterValue match = TimeValue{3600};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(match, filter));

  FilterValue mismatch = TimeValue{7200};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(mismatch, filter));
}

TEST_F(BinlogFilterEvaluatorTest, TimeValueComparison) {
  auto filter = MakeFilter("duration", "time", ">", "3600");

  FilterValue greater = TimeValue{7200};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(greater, filter));

  FilterValue less = TimeValue{1800};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(less, filter));
}

TEST_F(BinlogFilterEvaluatorTest, TimeValueNegative) {
  // TIME can be negative in MySQL
  auto filter = MakeFilter("offset", "time", "=", "-3600");

  FilterValue match = TimeValue{-3600};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(match, filter));
}

// ===========================================================================
// Invalid filter value tests
// ===========================================================================

TEST_F(BinlogFilterEvaluatorTest, InvalidIntegerFilterValue) {
  auto filter = MakeFilter("status", "int", "=", "not_a_number");

  FilterValue value = int64_t{1};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(value, filter));
}

TEST_F(BinlogFilterEvaluatorTest, IntegerFilterWithTrailingCharacters) {
  auto filter = MakeFilter("status", "int", "=", "123abc");

  FilterValue value = int64_t{123};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(value, filter));
}

TEST_F(BinlogFilterEvaluatorTest, InvalidFloatFilterValue) {
  auto filter = MakeFilter("price", "float", "=", "not_a_float");

  FilterValue value = 19.99;
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(value, filter));
}

TEST_F(BinlogFilterEvaluatorTest, FloatFilterWithTrailingCharacters) {
  auto filter = MakeFilter("price", "float", "=", "19.99xyz");

  FilterValue value = 19.99;
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(value, filter));
}

TEST_F(BinlogFilterEvaluatorTest, IntegerOutOfRange) {
  // Value too large for int64_t
  auto filter = MakeFilter("big", "int", "=", "99999999999999999999999999999");

  FilterValue value = int64_t{1};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(value, filter));
}

// ===========================================================================
// EvaluateRequiredFilters tests
// ===========================================================================

TEST_F(BinlogFilterEvaluatorTest, SingleRequiredFilterMatch) {
  TableConfig config = MakeTableConfig({MakeFilter("status", "int", "=", "1")});

  std::unordered_map<std::string, FilterValue> filters;
  filters["status"] = int64_t{1};

  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, config));
}

TEST_F(BinlogFilterEvaluatorTest, SingleRequiredFilterMismatch) {
  TableConfig config = MakeTableConfig({MakeFilter("status", "int", "=", "1")});

  std::unordered_map<std::string, FilterValue> filters;
  filters["status"] = int64_t{0};

  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, config));
}

TEST_F(BinlogFilterEvaluatorTest, MultipleRequiredFiltersAllMatch) {
  TableConfig config =
      MakeTableConfig({MakeFilter("status", "int", "=", "1"), MakeFilter("type", "string", "=", "article")});

  std::unordered_map<std::string, FilterValue> filters;
  filters["status"] = int64_t{1};
  filters["type"] = std::string{"article"};

  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, config));
}

TEST_F(BinlogFilterEvaluatorTest, MultipleRequiredFiltersOneMismatch) {
  TableConfig config =
      MakeTableConfig({MakeFilter("status", "int", "=", "1"), MakeFilter("type", "string", "=", "article")});

  std::unordered_map<std::string, FilterValue> filters;
  filters["status"] = int64_t{1};
  filters["type"] = std::string{"comment"};  // Mismatch

  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, config));
}

TEST_F(BinlogFilterEvaluatorTest, RequiredFilterColumnMissing) {
  TableConfig config = MakeTableConfig({MakeFilter("status", "int", "=", "1")});

  std::unordered_map<std::string, FilterValue> filters;
  // "status" column not present

  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, config));
}

TEST_F(BinlogFilterEvaluatorTest, ExtraColumnsInFiltersAreIgnored) {
  TableConfig config = MakeTableConfig({MakeFilter("status", "int", "=", "1")});

  std::unordered_map<std::string, FilterValue> filters;
  filters["status"] = int64_t{1};
  filters["extra_column"] = std::string{"ignored"};

  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, config));
}

// ===========================================================================
// Security: Filter value size limit test
// ===========================================================================

TEST_F(BinlogFilterEvaluatorTest, FilterValueSizeLimit) {
  // Create filter with value larger than 1MB limit
  std::string large_value(1024 * 1024 + 1, 'x');
  auto filter = MakeFilter("data", "string", "=", large_value);

  FilterValue value = std::string{"test"};
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(value, filter));
}

#endif  // USE_MYSQL
