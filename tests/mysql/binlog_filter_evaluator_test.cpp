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
#include "mysql/required_filter_predicate.h"
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
  FilterMap filters;

  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, UnconvertedFilterText{}, config));
}

TEST_F(BinlogFilterEvaluatorTest, EmptyRequiredFiltersWithDataReturnsTrue) {
  TableConfig config = MakeTableConfig({});
  FilterMap filters;
  filters["status"] = int64_t{1};

  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, UnconvertedFilterText{}, config));
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

TEST_F(BinlogFilterEvaluatorTest, Uint64BigintUnsignedComparison) {
  auto filter = MakeFilter("ubig", "bigint_unsigned", ">=", "18446744073709551614");

  FilterValue value = uint64_t{18446744073709551615ULL};
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(value, filter));
}

// ===========================================================================
// Float/Double comparison tests
// ===========================================================================

TEST_F(BinlogFilterEvaluatorTest, DoubleEquality) {
  auto filter = MakeFilter("price", "float", "=", "19.99");

  FilterValue match = 19.99;
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(match, filter));

  // A float required filter is compared at FLOAT precision, because that is
  // the precision the column itself carries: both operands round to 19.99f.
  FilterValue close = 19.9900000001;
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

TEST_F(BinlogFilterEvaluatorTest, ExtractedExplicitNullMatchesRequiredIsNull) {
  RowData row_data;
  row_data.null_columns.insert("deleted_at");
  TableConfig config = MakeTableConfig({MakeFilter("deleted_at", "datetime", "IS NULL", "")});

  UnconvertedFilterText filters_unconverted;
  auto filters = BinlogFilterEvaluator::ExtractAllFilters(row_data, config, "+00:00", filters_unconverted);

  ASSERT_TRUE(filters.contains("deleted_at"));
  ASSERT_TRUE(std::holds_alternative<std::monostate>(filters.at("deleted_at")));
  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, filters_unconverted, config));
}

TEST_F(BinlogFilterEvaluatorTest, ExtractedAbsentColumnDoesNotMatchRequiredIsNull) {
  RowData row_data;
  TableConfig config = MakeTableConfig({MakeFilter("deleted_at", "datetime", "IS NULL", "")});

  UnconvertedFilterText filters_unconverted;
  auto filters = BinlogFilterEvaluator::ExtractAllFilters(row_data, config, "+00:00", filters_unconverted);

  EXPECT_FALSE(filters.contains("deleted_at"));
  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, filters_unconverted, config));
}

TEST_F(BinlogFilterEvaluatorTest, ExtractedBooleanRoundTripsThroughRequiredEvaluation) {
  RowData row_data;
  row_data.columns["enabled"] = "1";
  TableConfig true_config = MakeTableConfig({MakeFilter("enabled", "boolean", "=", "true")});

  UnconvertedFilterText true_filters_unconverted;
  auto true_filters =
      BinlogFilterEvaluator::ExtractAllFilters(row_data, true_config, "+00:00", true_filters_unconverted);
  ASSERT_TRUE(true_filters.contains("enabled"));
  ASSERT_TRUE(std::holds_alternative<bool>(true_filters.at("enabled")));
  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(true_filters, true_filters_unconverted, true_config));

  row_data.columns["enabled"] = "0";
  TableConfig false_config = MakeTableConfig({MakeFilter("enabled", "boolean", "=", "0")});
  UnconvertedFilterText false_filters_unconverted;
  auto false_filters =
      BinlogFilterEvaluator::ExtractAllFilters(row_data, false_config, "+00:00", false_filters_unconverted);
  ASSERT_TRUE(false_filters.contains("enabled"));
  EXPECT_FALSE(std::get<bool>(false_filters.at("enabled")));
  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(false_filters, false_filters_unconverted, false_config));
  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(false_filters, false_filters_unconverted, true_config));
}

TEST_F(BinlogFilterEvaluatorTest, BooleanSupportsEqualityAndInequalityLiterals) {
  const FilterValue true_value = true;
  const FilterValue false_value = false;

  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(true_value, MakeFilter("enabled", "boolean", "=", "1")));
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(false_value, MakeFilter("enabled", "boolean", "!=", "true")));
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(true_value, MakeFilter("enabled", "boolean", "=", "yes")));
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

  FilterMap filters;
  filters["status"] = int64_t{1};

  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, UnconvertedFilterText{}, config));
}

TEST_F(BinlogFilterEvaluatorTest, SingleRequiredFilterMismatch) {
  TableConfig config = MakeTableConfig({MakeFilter("status", "int", "=", "1")});

  FilterMap filters;
  filters["status"] = int64_t{0};

  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, UnconvertedFilterText{}, config));
}

TEST_F(BinlogFilterEvaluatorTest, EmptyStringRequiredFilterMatchesOnlyEmptyValue) {
  TableConfig config = MakeTableConfig({MakeFilter("status", "varchar", "=", "")});

  FilterMap empty_filters;
  empty_filters["status"] = std::string{};
  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(empty_filters, UnconvertedFilterText{}, config));

  FilterMap nonempty_filters;
  nonempty_filters["status"] = std::string{"active"};
  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(nonempty_filters, UnconvertedFilterText{}, config));
}

TEST_F(BinlogFilterEvaluatorTest, MultipleRequiredFiltersAllMatch) {
  TableConfig config =
      MakeTableConfig({MakeFilter("status", "int", "=", "1"), MakeFilter("type", "string", "=", "article")});

  FilterMap filters;
  filters["status"] = int64_t{1};
  filters["type"] = std::string{"article"};

  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, UnconvertedFilterText{}, config));
}

TEST_F(BinlogFilterEvaluatorTest, MultipleRequiredFiltersOneMismatch) {
  TableConfig config =
      MakeTableConfig({MakeFilter("status", "int", "=", "1"), MakeFilter("type", "string", "=", "article")});

  FilterMap filters;
  filters["status"] = int64_t{1};
  filters["type"] = std::string{"comment"};  // Mismatch

  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, UnconvertedFilterText{}, config));
}

TEST_F(BinlogFilterEvaluatorTest, RequiredFilterColumnMissing) {
  TableConfig config = MakeTableConfig({MakeFilter("status", "int", "=", "1")});

  FilterMap filters;
  // "status" column not present

  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, UnconvertedFilterText{}, config));
}

// A required-filter value that does not decode is judged the way the initial-load
// WHERE clause judges the row, so load and replication admit the same rows.
TEST_F(BinlogFilterEvaluatorTest, ZeroDateIsNotNullAdmitsRowLikeTheInitialLoadPredicate) {
  RowData row_data;
  row_data.columns["published_at"] = "0000-00-00 00:00:00";

  TableConfig not_null_config = MakeTableConfig({MakeFilter("published_at", "datetime", "IS NOT NULL", "")});
  UnconvertedFilterText filters_unconverted;
  auto filters = BinlogFilterEvaluator::ExtractAllFilters(row_data, not_null_config, "+00:00", filters_unconverted);
  ASSERT_FALSE(filters.contains("published_at"));
  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, filters_unconverted, not_null_config));

  TableConfig null_config = MakeTableConfig({MakeFilter("published_at", "datetime", "IS NULL", "")});
  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, filters_unconverted, null_config));

  TableConfig threshold_config = MakeTableConfig({MakeFilter("published_at", "datetime", ">=", "2020-01-01 00:00:00")});
  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, filters_unconverted, threshold_config));
}

TEST_F(BinlogFilterEvaluatorTest, ZeroTimestampComparesAsEpochZero) {
  RowData row_data;
  row_data.columns["updated_at"] = "0000-00-00 00:00:00";

  TableConfig before_config = MakeTableConfig({MakeFilter("updated_at", "timestamp", "<", "1000")});
  UnconvertedFilterText filters_unconverted;
  auto filters = BinlogFilterEvaluator::ExtractAllFilters(row_data, before_config, "+00:00", filters_unconverted);
  ASSERT_FALSE(filters.contains("updated_at"));
  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, filters_unconverted, before_config));

  TableConfig after_config = MakeTableConfig({MakeFilter("updated_at", "timestamp", ">=", "1000")});
  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, filters_unconverted, after_config));
}

TEST_F(BinlogFilterEvaluatorTest, BooleanColumnOutsideZeroAndOneMatchesOnlyInequality) {
  RowData row_data;
  row_data.columns["enabled"] = "2";

  TableConfig equal_config = MakeTableConfig({MakeFilter("enabled", "boolean", "=", "1")});
  UnconvertedFilterText filters_unconverted;
  auto filters = BinlogFilterEvaluator::ExtractAllFilters(row_data, equal_config, "+00:00", filters_unconverted);
  ASSERT_FALSE(filters.contains("enabled"));
  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, filters_unconverted, equal_config));

  TableConfig not_equal_config = MakeTableConfig({MakeFilter("enabled", "boolean", "!=", "1")});
  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, filters_unconverted, not_equal_config));
}

// With the raw text, a zero or partly zero date is ordered as the server orders it: as a
// datetime by its components.
namespace {

bool AdmitsRawDatetime(const std::string& type, const std::string& op, const std::string& threshold,
                       const std::string& raw) {
  RowData row_data;
  row_data.columns["happened_at"] = raw;
  TableConfig config = MakeTableConfig({MakeFilter("happened_at", type, op, threshold)});
  UnconvertedFilterText unconverted;
  auto filters = BinlogFilterEvaluator::ExtractAllFilters(row_data, config, "+00:00", unconverted);
  EXPECT_FALSE(filters.contains("happened_at")) << raw;
  EXPECT_EQ(unconverted.count("happened_at"), 1U) << raw;
  return BinlogFilterEvaluator::EvaluateRequiredFilters(filters, unconverted, config);
}

}  // namespace

TEST_F(BinlogFilterEvaluatorTest, ZeroDateOrdersBeforeEveryRealInstant) {
  const std::string zero = "0000-00-00 00:00:00";
  const std::string threshold = "2024-01-01 00:00:00";
  EXPECT_TRUE(AdmitsRawDatetime("datetime", "<", threshold, zero));
  EXPECT_TRUE(AdmitsRawDatetime("datetime", "<=", threshold, zero));
  EXPECT_TRUE(AdmitsRawDatetime("datetime", "!=", threshold, zero));
  EXPECT_FALSE(AdmitsRawDatetime("datetime", "=", threshold, zero));
  EXPECT_FALSE(AdmitsRawDatetime("datetime", ">", threshold, zero));
  EXPECT_FALSE(AdmitsRawDatetime("datetime", ">=", threshold, zero));
  EXPECT_TRUE(AdmitsRawDatetime("date", "<", threshold, "0000-00-00"));
}

TEST_F(BinlogFilterEvaluatorTest, PartlyZeroDateOrdersByComponents) {
  const std::string partial = "2024-00-00 00:00:00";
  EXPECT_TRUE(AdmitsRawDatetime("datetime", "<", "2024-01-01 00:00:00", partial));
  EXPECT_FALSE(AdmitsRawDatetime("datetime", ">=", "2024-01-01 00:00:00", partial));
  EXPECT_FALSE(AdmitsRawDatetime("datetime", "=", "2024-01-01 00:00:00", partial));
  EXPECT_TRUE(AdmitsRawDatetime("datetime", "!=", "2024-01-01 00:00:00", partial));
}

// The rendered SQL turns <=, =, != and > into half-open bounds at x and x + 1; a value
// sorting between those bounds must get the verdict the SQL gives it.
TEST_F(BinlogFilterEvaluatorTest, PartlyZeroDateBetweenWholeSecondsFollowsHalfOpenBounds) {
  const std::string partial = "2024-00-00 00:00:00";
  const std::string just_before = "2023-12-31 23:59:59";
  EXPECT_TRUE(AdmitsRawDatetime("datetime", "<=", just_before, partial));
  EXPECT_FALSE(AdmitsRawDatetime("datetime", ">", just_before, partial));
  EXPECT_FALSE(AdmitsRawDatetime("datetime", "<", just_before, partial));
  EXPECT_TRUE(AdmitsRawDatetime("datetime", ">=", just_before, partial));
  EXPECT_TRUE(AdmitsRawDatetime("datetime", "=", just_before, partial));
  EXPECT_FALSE(AdmitsRawDatetime("datetime", "!=", just_before, partial));
}

TEST_F(BinlogFilterEvaluatorTest, ZeroTimestampOrdersBeforeEveryInstantByItsText) {
  EXPECT_TRUE(AdmitsRawDatetime("timestamp", "<", "2024-01-01 00:00:00", "0000-00-00 00:00:00"));
  EXPECT_FALSE(AdmitsRawDatetime("timestamp", ">=", "1970-01-01 00:00:00", "0000-00-00 00:00:00"));
}

TEST_F(BinlogFilterEvaluatorTest, ColumnMissingFromBothMapsIsRejectedWithRawTextOverload) {
  TableConfig config = MakeTableConfig({MakeFilter("happened_at", "datetime", "!=", "2024-01-01 00:00:00")});
  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(FilterMap{}, UnconvertedFilterText{}, config));
}

TEST_F(BinlogFilterEvaluatorTest, PredicateJudgesRawUndecodedTextDirectly) {
  auto predicate = RequiredFilterPredicate::Resolve(MakeFilter("c", "datetime", ">", "2024-01-01 00:00:00"), "+00:00");
  ASSERT_TRUE(predicate.has_value());
  EXPECT_FALSE(predicate->MatchesUndecodedValue("2024-00-00 00:00:00"));
  EXPECT_FALSE(predicate->MatchesUndecodedValue("garbage"));
}

TEST_F(BinlogFilterEvaluatorTest, UndecodedValueUnderUnusableFilterIsRejected) {
  TableConfig config = MakeTableConfig({MakeFilter("enabled", "boolean", "!=", "maybe")});

  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(FilterMap{}, UnconvertedFilterText{}, config));
}

TEST_F(BinlogFilterEvaluatorTest, ExtraColumnsInFiltersAreIgnored) {
  TableConfig config = MakeTableConfig({MakeFilter("status", "int", "=", "1")});

  FilterMap filters;
  filters["status"] = int64_t{1};
  filters["extra_column"] = std::string{"ignored"};

  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters, UnconvertedFilterText{}, config));
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
