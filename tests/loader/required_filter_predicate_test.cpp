/**
 * @file required_filter_predicate_test.cpp
 * @brief The initial-load WHERE clause and the binlog predicate must agree
 *
 * A required_filter decides membership twice: as SQL in the initial-load SELECT
 * and in C++ when a binlog row event arrives. When the two disagree, a row is
 * indexed at startup and then silently dropped on its first UPDATE (or the
 * reverse), so every predicate the query builder emits must select exactly the
 * rows BinlogFilterEvaluator accepts.
 *
 * Truth values that only a server can settle are recorded here as constants
 * measured on MySQL 8.4, named after the expression they came from.
 */

#ifdef USE_MYSQL

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#include "config/config.h"
#include "loader/initial_loader.h"
#include "mysql/binlog_filter_evaluator.h"
#include "storage/document_store.h"
#include "utils/datetime_converter.h"
#include "utils/sql_utils.h"

namespace mygramdb::loader {
namespace {

config::TableConfig TableWithRequiredFilter(const std::string& name, const std::string& type,
                                            const std::string& comparison_operator, const std::string& value) {
  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";
  table_config.text_source.column = "content";

  config::RequiredFilterConfig filter;
  filter.name = name;
  filter.type = type;
  filter.op = comparison_operator;
  filter.value = value;
  table_config.required_filters.push_back(filter);
  return table_config;
}

/// Membership decision the binlog path makes for one stored column value.
bool BinlogAccepts(const config::TableConfig& table_config, const storage::FilterValue& stored_value,
                   const std::string& datetime_timezone = "+00:00") {
  storage::FilterMap filters;
  filters[table_config.required_filters.front().name] = stored_value;
  return mysql::BinlogFilterEvaluator::EvaluateRequiredFilters(filters, table_config, datetime_timezone);
}

std::string Literal(const std::string& value) {
  return mygramdb::utils::EncodeMySQLStringLiteral(value);
}

// ===========================================================================
// Text filters
// ===========================================================================

/**
 * How MySQL 8.4 judges a stored value against the filter value "published",
 * for a VARCHAR(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci column
 * (the server default collation):
 *   collated:   `status` = _utf8mb4 X'7075626C6973686564'
 *   byte_exact: CAST(`status` AS BINARY) = _utf8mb4 X'7075626C6973686564'
 */
struct StringComparisonFact {
  const char* stored_value;
  bool collated;
  bool byte_exact;
};

constexpr StringComparisonFact kStringComparisonFacts[] = {
    {"published", true, true},
    {"Published", true, false},
    {"published ", false, false},
};

/**
 * @brief Byte equality, not the column collation, is what replication enforces.
 *
 * This fixes which of the two candidate SQL predicates the initial load has to
 * emit: the one whose truth values match this column.
 */
TEST(RequiredFilterPredicateTest, BinlogMembershipFollowsByteEqualityNotTheColumnCollation) {
  const auto table_config = TableWithRequiredFilter("status", "string", "=", "published");

  bool candidates_differ = false;
  for (const auto& fact : kStringComparisonFacts) {
    EXPECT_EQ(BinlogAccepts(table_config, std::string(fact.stored_value)), fact.byte_exact)
        << "stored value: " << fact.stored_value;
    candidates_differ = candidates_differ || fact.collated != fact.byte_exact;
  }
  EXPECT_TRUE(candidates_differ) << "the two candidate predicates must disagree somewhere for the choice to matter";
}

/**
 * @brief A text required filter is compared as bytes in the generated SELECT.
 *
 * Left to the column's collation, `status` = 'published' also admits
 * 'Published' under utf8mb4_0900_ai_ci, and that row is removed from the index
 * the first time it is UPDATEd.
 */
TEST(RequiredFilterPredicateTest, TextRequiredFilterIsComparedAsBytesInTheSelect) {
  for (const auto* type : {"string", "varchar", "text"}) {
    const auto query =
        internal::BuildInitialLoadSelectQuery(TableWithRequiredFilter("status", type, "=", "published"), {});
    ASSERT_FALSE(query.empty()) << type;
    EXPECT_NE(query.find("CAST(`status` AS BINARY) = _utf8mb4 X'7075626C6973686564'"), std::string::npos)
        << type << ": " << query;
  }
}

/**
 * @brief Ordering comparisons on text are byte-ordered too.
 *
 * The evaluator orders text with std::string's byte comparison, so a collated
 * ordering in SQL would draw the boundary somewhere else.
 */
TEST(RequiredFilterPredicateTest, TextOrderingComparisonsAreAlsoByteOrdered) {
  for (const auto* comparison_operator : {"!=", "<", ">", "<=", ">="}) {
    const auto query =
        internal::BuildInitialLoadSelectQuery(TableWithRequiredFilter("status", "text", comparison_operator, "b"), {});
    ASSERT_FALSE(query.empty()) << comparison_operator;
    EXPECT_NE(query.find("CAST(`status` AS BINARY) " + std::string(comparison_operator) + " " + Literal("b")),
              std::string::npos)
        << query;
  }
}

/**
 * @brief Only text comparisons are rewritten; other predicates keep the column.
 */
TEST(RequiredFilterPredicateTest, NonTextPredicatesKeepTheBareColumn) {
  const auto numeric = internal::BuildInitialLoadSelectQuery(TableWithRequiredFilter("enabled", "int", "=", "1"), {});
  EXPECT_NE(numeric.find("`enabled` = 1"), std::string::npos) << numeric;

  const auto is_null =
      internal::BuildInitialLoadSelectQuery(TableWithRequiredFilter("status", "varchar", "IS NULL", ""), {});
  EXPECT_NE(is_null.find("`status` IS NULL"), std::string::npos) << is_null;
}

// ===========================================================================
// TIME filters
// ===========================================================================

/**
 * Measured on MySQL 8.4 against a TIME column:
 *   `elapsed` = 32400      is true only for TIME'03:24:00' - a bare number is
 *                          read as packed HHMMSS, not as seconds
 *   `elapsed` = '09:00:00' is true only for TIME'09:00:00'
 */
constexpr int64_t kNineAmSeconds = 9 * 3600;
constexpr int64_t kSecondsMySQLReadsAs32400 = (3 * 3600) + (24 * 60);

/**
 * @brief A clock-formatted time filter is expressible as SQL at all.
 *
 * "09:00:00" is a value the evaluator accepts, so refusing to build the query
 * for it aborts the whole initial load over a configuration MySQL understands.
 */
TEST(RequiredFilterPredicateTest, ClockFormattedTimeFilterBuildsAQuery) {
  const auto query =
      internal::BuildInitialLoadSelectQuery(TableWithRequiredFilter("elapsed", "time", ">=", "09:00:00"), {});
  ASSERT_FALSE(query.empty()) << "a time value the evaluator accepts must also be expressible in the SELECT";
  EXPECT_NE(query.find("`elapsed` >= " + Literal("09:00:00")), std::string::npos) << query;
}

/**
 * @brief A time filter written in seconds is emitted as the same clock instant.
 */
TEST(RequiredFilterPredicateTest, TimeFilterInSecondsIsEmittedAsAClockLiteral) {
  const auto table_config = TableWithRequiredFilter("elapsed", "time", "=", "32400");

  const auto query = internal::BuildInitialLoadSelectQuery(table_config, {});
  ASSERT_FALSE(query.empty());
  EXPECT_NE(query.find("`elapsed` = " + Literal("09:00:00")), std::string::npos) << query;
  EXPECT_EQ(query.find("= 32400"), std::string::npos)
      << "MySQL reads a bare number as packed HHMMSS, which is a different instant: " << query;

  EXPECT_TRUE(BinlogAccepts(table_config, storage::TimeValue{kNineAmSeconds}));
  EXPECT_FALSE(BinlogAccepts(table_config, storage::TimeValue{kSecondsMySQLReadsAs32400}));
}

/**
 * @brief Negative and over-a-day times keep their clock form.
 */
TEST(RequiredFilterPredicateTest, NegativeAndOverdayTimeFiltersKeepTheirClockForm) {
  const auto negative_clock =
      internal::BuildInitialLoadSelectQuery(TableWithRequiredFilter("elapsed", "time", "=", "-10:20:30"), {});
  EXPECT_NE(negative_clock.find("`elapsed` = " + Literal("-10:20:30")), std::string::npos) << negative_clock;

  const auto negative_seconds =
      internal::BuildInitialLoadSelectQuery(TableWithRequiredFilter("elapsed", "time", "=", "-37230"), {});
  EXPECT_NE(negative_seconds.find("`elapsed` = " + Literal("-10:20:30")), std::string::npos) << negative_seconds;

  const auto overday =
      internal::BuildInitialLoadSelectQuery(TableWithRequiredFilter("elapsed", "time", "<", "100:00:00"), {});
  EXPECT_NE(overday.find("`elapsed` < " + Literal("100:00:00")), std::string::npos) << overday;
}

/**
 * @brief A time value MySQL cannot hold refuses the query instead of shipping.
 */
TEST(RequiredFilterPredicateTest, TimeFilterOutsideTheMySQLRangeRefusesTheQuery) {
  for (const auto* value : {"839:00:00", "3020400", "-3020400", "noon", ""}) {
    EXPECT_TRUE(
        internal::BuildInitialLoadSelectQuery(TableWithRequiredFilter("elapsed", "time", "=", value), {}).empty())
        << "a time value outside MySQL's TIME range reached the SELECT: " << value;
  }
}

TEST(RequiredFilterPredicateTest, TimeLiteralFormattingCoversTheWholeMySQLRange) {
  using mygramdb::utils::FormatMySQLTimeLiteral;
  EXPECT_EQ(FormatMySQLTimeLiteral(0), "00:00:00");
  EXPECT_EQ(FormatMySQLTimeLiteral(kNineAmSeconds), "09:00:00");
  EXPECT_EQ(FormatMySQLTimeLiteral(-37230), "-10:20:30");
  EXPECT_EQ(FormatMySQLTimeLiteral(3020399), "838:59:59");
  EXPECT_EQ(FormatMySQLTimeLiteral(-3020399), "-838:59:59");
  EXPECT_FALSE(FormatMySQLTimeLiteral(3020400).has_value());
  EXPECT_FALSE(FormatMySQLTimeLiteral(-3020400).has_value());
}

// ===========================================================================
// TIMESTAMP filters
// ===========================================================================

/**
 * Measured on MySQL 8.4: FROM_UNIXTIME() is NULL outside [0, 32536771199], and
 * a WHERE clause built around a NULL is UNKNOWN for every row, so the whole
 * initial load selects nothing and still reports success. UNIX_TIMESTAMP() on
 * the column returns the stored epoch, which is the value the binlog path
 * compares.
 */
constexpr const char* kTimestampBeforeEpoch = "1970-01-01 08:00:00";  // -3600 at +09:00

/**
 * @brief A pre-epoch timestamp bound still selects the rows above it.
 */
TEST(RequiredFilterPredicateTest, TimestampFilterBeforeTheEpochStillSelectsRows) {
  const auto table_config = TableWithRequiredFilter("published_at", "timestamp", ">=", kTimestampBeforeEpoch);
  config::MysqlConfig mysql_config;
  mysql_config.datetime_timezone = "+09:00";

  auto epoch = mygram::utils::ParseDatetimeValue(kTimestampBeforeEpoch, mysql_config.datetime_timezone);
  ASSERT_TRUE(epoch.has_value());
  ASSERT_LT(*epoch, 0);

  const auto query = internal::BuildInitialLoadSelectQuery(table_config, mysql_config);
  ASSERT_FALSE(query.empty());
  EXPECT_EQ(query.find("FROM_UNIXTIME"), std::string::npos)
      << "FROM_UNIXTIME is NULL for a negative epoch, which makes the WHERE clause UNKNOWN for every row: " << query;
  EXPECT_NE(query.find("UNIX_TIMESTAMP(`published_at`) >= " + std::to_string(*epoch)), std::string::npos) << query;

  EXPECT_TRUE(BinlogAccepts(table_config, static_cast<int64_t>(0), mysql_config.datetime_timezone));
}

/**
 * @brief An in-range timestamp bound compares the same epoch on both paths.
 */
TEST(RequiredFilterPredicateTest, TimestampFilterComparesTheStoredEpoch) {
  const auto table_config = TableWithRequiredFilter("published_at", "timestamp", "<=", "2026-01-01 00:30:00");
  config::MysqlConfig mysql_config;
  mysql_config.datetime_timezone = "+00:00";

  auto epoch = mygram::utils::ParseDatetimeValue("2026-01-01 00:30:00", mysql_config.datetime_timezone);
  ASSERT_TRUE(epoch.has_value());

  const auto query = internal::BuildInitialLoadSelectQuery(table_config, mysql_config);
  ASSERT_FALSE(query.empty());
  EXPECT_NE(query.find("UNIX_TIMESTAMP(`published_at`) <= " + std::to_string(*epoch)), std::string::npos) << query;

  EXPECT_TRUE(BinlogAccepts(table_config, *epoch, mysql_config.datetime_timezone));
  EXPECT_FALSE(BinlogAccepts(table_config, *epoch + 1, mysql_config.datetime_timezone));
}

}  // namespace
}  // namespace mygramdb::loader

#endif  // USE_MYSQL
