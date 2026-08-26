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
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include "config/config.h"
#include "loader/initial_loader.h"
#include "mysql/binlog_filter_evaluator.h"
#include "storage/document_store.h"
#include "utils/comparison_utils.h"
#include "utils/datetime_converter.h"
#include "utils/numeric_parse.h"
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

/// Offset the generated cases interpret DATETIME and DATE values in.
constexpr const char* kDifferentialTimezone = "+09:00";
constexpr int32_t kDifferentialTimezoneOffsetSeconds = 9 * 3600;

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

// ===========================================================================
// Cross-surface differential
// ===========================================================================

/**
 * The emitted WHERE conjunct, split at the comparison operator.
 */
struct EmittedConjunct {
  std::string left;
  std::string literal;
};

/// Pull the single WHERE conjunct out of a generated SELECT.
std::optional<EmittedConjunct> EmittedPredicate(const std::string& query, const std::string& comparison_operator) {
  const size_t where = query.find(" WHERE ");
  const size_t order = query.find(" ORDER BY ");
  if (where == std::string::npos || order == std::string::npos || order < where) {
    return std::nullopt;
  }
  const std::string conjunct = query.substr(where + 7, order - where - 7);
  const std::string separator = " " + comparison_operator + " ";
  const size_t split = conjunct.find(separator);
  if (split == std::string::npos) {
    // IS NULL and IS NOT NULL carry the whole comparison in the operator.
    const std::string suffix = " " + comparison_operator;
    if (conjunct.size() > suffix.size() &&
        conjunct.compare(conjunct.size() - suffix.size(), suffix.size(), suffix) == 0) {
      return EmittedConjunct{conjunct.substr(0, conjunct.size() - suffix.size()), ""};
    }
    return std::nullopt;
  }
  return EmittedConjunct{conjunct.substr(0, split), conjunct.substr(split + separator.size())};
}

/// Read back the bytes of a `_utf8mb4 X'..'` literal.
std::optional<std::string> DecodeByteLiteral(const std::string& literal) {
  constexpr std::string_view kPrefix = "_utf8mb4 X'";
  if (literal.size() < kPrefix.size() + 1 || literal.compare(0, kPrefix.size(), kPrefix) != 0 ||
      literal.back() != '\'') {
    return std::nullopt;
  }
  const std::string hex = literal.substr(kPrefix.size(), literal.size() - kPrefix.size() - 1);
  if (hex.size() % 2 != 0) {
    return std::nullopt;
  }
  std::string decoded;
  decoded.reserve(hex.size() / 2);
  for (size_t i = 0; i < hex.size(); i += 2) {
    decoded.push_back(static_cast<char>(std::stoi(hex.substr(i, 2), nullptr, 16)));
  }
  return decoded;
}

/// Read back the literal a `CAST(.. AS FLOAT)` wraps.
std::optional<std::string> UnwrapFloatCast(const std::string& literal) {
  constexpr std::string_view kPrefix = "CAST(";
  constexpr std::string_view kSuffix = " AS FLOAT)";
  if (literal.size() <= kPrefix.size() + kSuffix.size() || literal.compare(0, kPrefix.size(), kPrefix) != 0 ||
      literal.compare(literal.size() - kSuffix.size(), kSuffix.size(), kSuffix) != 0) {
    return std::nullopt;
  }
  return literal.substr(kPrefix.size(), literal.size() - kPrefix.size() - kSuffix.size());
}

template <typename T>
bool Holds(const storage::FilterValue& value) {
  return std::holds_alternative<T>(value);
}

/**
 * @brief What MySQL 8.4 makes of the emitted conjunct for one stored value.
 *
 * Every branch below reads the emitted text back through the inverse of the
 * server behaviour it was measured against, so a change in what the query
 * builder emits changes this verdict:
 *
 *   CAST(`c` AS BINARY) op _utf8mb4 X'..'  - byte-lexicographic on the bytes
 *                                            MySQL retrieves for the column
 *   `c` op <decimal>                       - numeric comparison
 *   `c` op CAST(<double> AS FLOAT)         - both sides widened from FLOAT;
 *                                            measured: `f`=0.1 is false while
 *                                            `f`=CAST(0.1e0 AS FLOAT) is true
 *   `c` op <double literal>                - measured: an exponent literal is
 *                                            read as DOUBLE, and 17 digits
 *                                            round-trip exactly
 *   `c` op _utf8mb4 X'<wall clock>'        - measured: a DATE column compares
 *                                            against a datetime-shaped string
 *                                            as a datetime, promoting the
 *                                            column to midnight
 *   UNIX_TIMESTAMP(`c`) op <epoch>         - the stored UTC epoch
 *   `c` op _utf8mb4 X'<HH:MM:SS>'          - measured: a bare number is read as
 *                                            packed HHMMSS, a clock string is
 *                                            read as a time
 *   `c` op 1 | 0                           - numeric comparison on TINYINT(1)
 *
 * @return The verdict, or std::nullopt when the emitted shape is not one this
 *         model was measured for - which is itself a failure.
 */
std::optional<bool> SqlVerdict(const std::string& type, const std::string& comparison_operator,
                               const std::string& quoted_column, const EmittedConjunct& emitted,
                               const storage::FilterValue& stored) {
  const bool is_null_check = comparison_operator == "IS NULL" || comparison_operator == "IS NOT NULL";
  if (is_null_check) {
    if (emitted.left != quoted_column) {
      return std::nullopt;
    }
    const bool is_null = Holds<std::monostate>(stored);
    return comparison_operator == "IS NULL" ? is_null : !is_null;
  }
  // A comparison against NULL is UNKNOWN, and an UNKNOWN conjunct excludes the
  // row from the result set.
  if (Holds<std::monostate>(stored)) {
    return false;
  }

  const auto compare = [&comparison_operator](auto lhs, auto rhs) {
    return mygram::utils::CompareValues(lhs, rhs, comparison_operator);
  };

  if (type == "string" || type == "varchar" || type == "text") {
    if (emitted.left != "CAST(" + quoted_column + " AS BINARY)" || !Holds<std::string>(stored)) {
      return std::nullopt;
    }
    auto target = DecodeByteLiteral(emitted.literal);
    if (!target) {
      return std::nullopt;
    }
    return compare(std::get<std::string>(stored), *target);
  }

  if (type == "float" || type == "double") {
    if (emitted.left != quoted_column || !Holds<double>(stored)) {
      return std::nullopt;
    }
    std::string literal = emitted.literal;
    if (type == "float") {
      auto unwrapped = UnwrapFloatCast(literal);
      if (!unwrapped) {
        return std::nullopt;
      }
      literal = *unwrapped;
    }
    auto target = mygram::utils::ParseNumeric<double>(literal);
    if (!target) {
      return std::nullopt;
    }
    if (type == "float") {
      return compare(static_cast<float>(std::get<double>(stored)), static_cast<float>(*target));
    }
    return compare(std::get<double>(stored), *target);
  }

  if (type == "boolean") {
    if (emitted.left != quoted_column || !Holds<bool>(stored)) {
      return std::nullopt;
    }
    if (emitted.literal != "1" && emitted.literal != "0") {
      return std::nullopt;
    }
    return compare(static_cast<int64_t>(std::get<bool>(stored) ? 1 : 0),
                   static_cast<int64_t>(emitted.literal == "1" ? 1 : 0));
  }

  if (type == "datetime" || type == "date") {
    if (emitted.left != quoted_column || !Holds<int64_t>(stored)) {
      return std::nullopt;
    }
    auto wall_clock = DecodeByteLiteral(emitted.literal);
    if (!wall_clock) {
      return std::nullopt;
    }
    auto target = mygram::utils::ConvertToEpoch(*wall_clock, kDifferentialTimezoneOffsetSeconds);
    if (!target) {
      return std::nullopt;
    }
    return compare(std::get<int64_t>(stored), *target);
  }

  if (type == "timestamp") {
    if (emitted.left != "UNIX_TIMESTAMP(" + quoted_column + ")" || !Holds<int64_t>(stored)) {
      return std::nullopt;
    }
    auto target = mygram::utils::ParseNumeric<int64_t>(emitted.literal);
    if (!target) {
      return std::nullopt;
    }
    return compare(std::get<int64_t>(stored), *target);
  }

  if (type == "time") {
    if (emitted.left != quoted_column || !Holds<storage::TimeValue>(stored)) {
      return std::nullopt;
    }
    auto clock = DecodeByteLiteral(emitted.literal);
    if (!clock) {
      return std::nullopt;
    }
    auto target = mygram::utils::DateTimeProcessor::TimeToSeconds(*clock);
    if (!target) {
      return std::nullopt;
    }
    return compare(std::get<storage::TimeValue>(stored).seconds, *target);
  }

  if (type == "bigint_unsigned") {
    if (emitted.left != quoted_column || !Holds<uint64_t>(stored)) {
      return std::nullopt;
    }
    auto target = mygram::utils::ParseNumeric<uint64_t>(emitted.literal);
    if (!target) {
      return std::nullopt;
    }
    return compare(std::get<uint64_t>(stored), *target);
  }

  // Every remaining type is an integer width MySQL compares as a signed number.
  if (emitted.left != quoted_column) {
    return std::nullopt;
  }
  auto target = mygram::utils::ParseNumeric<int64_t>(emitted.literal);
  if (!target) {
    return std::nullopt;
  }
  int64_t widened = 0;
  if (Holds<int8_t>(stored)) {
    widened = std::get<int8_t>(stored);
  } else if (Holds<int16_t>(stored)) {
    widened = std::get<int16_t>(stored);
  } else if (Holds<int32_t>(stored)) {
    widened = std::get<int32_t>(stored);
  } else if (Holds<int64_t>(stored)) {
    widened = std::get<int64_t>(stored);
  } else if (Holds<uint8_t>(stored)) {
    widened = std::get<uint8_t>(stored);
  } else if (Holds<uint16_t>(stored)) {
    widened = std::get<uint16_t>(stored);
  } else if (Holds<uint32_t>(stored)) {
    widened = std::get<uint32_t>(stored);
  } else {
    return std::nullopt;
  }
  return compare(widened, *target);
}

/// One type's interesting boundaries: every configured value crossed with
/// every stored column value the type can decode to.
struct TypeCases {
  const char* type;
  std::vector<std::string> values;
  std::vector<storage::FilterValue> stored;
};

std::vector<TypeCases> DifferentialCases() {
  using storage::FilterValue;
  return {
      {"string",
       {"", "published", "Published", "published ", "b"},
       {std::string(""), std::string("published"), std::string("Published"), std::string("published "),
        std::string("a"), std::string("b"), std::string("bb"), std::string("\xE6\x97\xA5")}},
      {"varchar", {"published"}, {std::string("published"), std::string("Published")}},
      {"text", {"b"}, {std::string("a"), std::string("b"), std::string("c")}},
      {"tinyint", {"0", "127", "-128"}, {int8_t{0}, int8_t{1}, int8_t{127}, int8_t{-128}}},
      {"tinyint_unsigned", {"0", "255"}, {uint8_t{0}, uint8_t{128}, uint8_t{255}}},
      {"smallint", {"-32768", "32767"}, {int16_t{-32768}, int16_t{0}, int16_t{32767}}},
      {"smallint_unsigned", {"0", "65535"}, {uint16_t{0}, uint16_t{65535}}},
      {"mediumint", {"-8388608", "8388607"}, {int32_t{-8388608}, int32_t{0}, int32_t{8388607}}},
      {"mediumint_unsigned", {"16777215"}, {uint32_t{0}, uint32_t{16777215}}},
      {"int", {"0", "1", "-1", "2147483647"}, {int32_t{-1}, int32_t{0}, int32_t{1}, int32_t{2147483647}}},
      {"int_unsigned", {"0", "4294967295"}, {uint32_t{0}, uint32_t{4294967295}}},
      {"bigint",
       {"0", "9223372036854775807", "-9223372036854775808"},
       {int64_t{0}, int64_t{-1}, int64_t{9223372036854775807LL}, int64_t{-9223372036854775807LL - 1}}},
      {"bigint_unsigned",
       {"0", "9223372036854775808", "18446744073709551615"},
       {uint64_t{0}, uint64_t{9223372036854775808ULL}, uint64_t{18446744073709551615ULL}}},
      // 0.100000001 is what a FLOAT column's binlog image decodes to for 0.1.
      {"float", {"0.1", "19.99", "0", "-1.5"}, {0.1, 0.100000001, 19.99, 0.0, -0.0, -1.5, 1.5}},
      {"double", {"0.1", "0.3", "0.30000000000000004", "1.23e10"}, {0.1, 0.3, 0.1 + 0.2, 1.23e10, 0.0, -0.0}},
      {"datetime",
       {"2024-01-01 12:00:00", "1704110400", "2024-01-01"},
       {int64_t{1704067200}, int64_t{1704110400}, int64_t{1704110401}, int64_t{0}, int64_t{-86400}}},
      {"date", {"2024-01-01", "1704067200"}, {int64_t{1704067200}, int64_t{1704153600}, int64_t{0}}},
      {"timestamp", {"2024-01-01 12:00:00", "1704110400"}, {int64_t{1704110400}, int64_t{1704110401}, int64_t{0}}},
      {"time",
       {"09:00:00", "32400", "-10:20:30", "0"},
       {storage::TimeValue{0}, storage::TimeValue{32400}, storage::TimeValue{12240}, storage::TimeValue{-37230}}},
      {"boolean", {"1", "0", "true", "false", "TRUE"}, {true, false}},
  };
}

std::vector<std::string> ValueOperators(const std::string& type) {
  if (type == "boolean") {
    return {"=", "!="};
  }
  return {"=", "!=", "<", ">", "<=", ">="};
}

/**
 * @brief The generated SELECT and the binlog evaluator agree on every value.
 *
 * The SQL side is judged by reading the emitted conjunct back through the
 * inverse of the server behaviour it was measured against, so this fails both
 * when the evaluator drifts and when the query builder emits something the
 * measurements do not cover.
 */
TEST(RequiredFilterPredicateTest, EveryFilterTypeDecidesMembershipTheSameWayOnBothSurfaces) {
  config::MysqlConfig mysql_config;
  mysql_config.datetime_timezone = kDifferentialTimezone;

  size_t compared = 0;
  for (const auto& type_cases : DifferentialCases()) {
    for (const auto& value : type_cases.values) {
      for (const auto& comparison_operator : ValueOperators(type_cases.type)) {
        const auto table_config = TableWithRequiredFilter("col", type_cases.type, comparison_operator, value);
        const auto query = internal::BuildInitialLoadSelectQuery(table_config, mysql_config);
        ASSERT_FALSE(query.empty()) << type_cases.type << " " << comparison_operator << " " << value;

        auto emitted = EmittedPredicate(query, comparison_operator);
        ASSERT_TRUE(emitted.has_value()) << query;

        for (const auto& stored : type_cases.stored) {
          auto sql = SqlVerdict(type_cases.type, comparison_operator, "`col`", *emitted, stored);
          ASSERT_TRUE(sql.has_value()) << "unmeasured predicate shape: " << emitted->left << " " << comparison_operator
                                       << " " << emitted->literal;
          EXPECT_EQ(*sql, BinlogAccepts(table_config, stored, kDifferentialTimezone))
              << type_cases.type << " " << comparison_operator << " '" << value << "' against " << stored.index()
              << ": " << emitted->left << " " << comparison_operator << " " << emitted->literal;
          ++compared;
        }
      }
    }
  }
  EXPECT_GT(compared, 1000U) << "the generated space collapsed";
}

/**
 * @brief NULL state is decided identically for every type.
 */
TEST(RequiredFilterPredicateTest, NullChecksAgreeForEveryFilterType) {
  config::MysqlConfig mysql_config;
  mysql_config.datetime_timezone = kDifferentialTimezone;

  for (const auto& type_cases : DifferentialCases()) {
    for (const auto* comparison_operator : {"IS NULL", "IS NOT NULL"}) {
      const auto table_config = TableWithRequiredFilter("col", type_cases.type, comparison_operator, "");
      const auto query = internal::BuildInitialLoadSelectQuery(table_config, mysql_config);
      ASSERT_FALSE(query.empty()) << type_cases.type << " " << comparison_operator;

      auto emitted = EmittedPredicate(query, comparison_operator);
      ASSERT_TRUE(emitted.has_value()) << query;

      std::vector<storage::FilterValue> stored_values = type_cases.stored;
      stored_values.emplace_back(std::monostate{});
      for (const auto& stored : stored_values) {
        auto sql = SqlVerdict(type_cases.type, comparison_operator, "`col`", *emitted, stored);
        ASSERT_TRUE(sql.has_value()) << query;
        EXPECT_EQ(*sql, BinlogAccepts(table_config, stored, kDifferentialTimezone))
            << type_cases.type << " " << comparison_operator;
      }
    }
  }
}

/**
 * @brief A value neither surface can compare stops the load and admits nothing.
 *
 * The two surfaces refuse differently - the initial load aborts, replication
 * rejects the row - but the outcome is the same: no row enters the index under
 * such a configuration, so the two never disagree about one.
 */
TEST(RequiredFilterPredicateTest, AValueThatCannotBeComparedIsRefusedOnBothSurfaces) {
  const std::vector<std::pair<std::string, std::string>> unusable = {
      {"int", "+5"},
      {"int", "5.0"},
      {"int", "1e5"},
      {"int", "0x41"},
      {"int", " 1"},
      {"bigint", "99999999999999999999"},
      {"bigint_unsigned", "-1"},
      {"tinyint", "abc"},
      {"float", "not_a_float"},
      {"float", "1e500"},
      {"double", "19.99xyz"},
      {"boolean", "2"},
      {"boolean", "yes"},
      {"datetime", "yesterday"},
      {"date", "2024-13-45"},
      {"timestamp", "noon"},
      {"time", "839:00:00"},
      {"time", "noon"},
  };

  const std::vector<storage::FilterValue> probes = {
      int64_t{5},       int64_t{0}, uint64_t{5}, 0.1, std::string("5"), true, false, storage::TimeValue{32400},
      std::monostate{},
  };

  for (const auto& [type, value] : unusable) {
    for (const auto* comparison_operator : {"=", "!=", "<", ">=", "<=", ">"}) {
      const auto table_config = TableWithRequiredFilter("col", type, comparison_operator, value);
      EXPECT_TRUE(internal::BuildInitialLoadSelectQuery(table_config, {}).empty())
          << "an uncomparable value reached the SELECT: " << type << " " << comparison_operator << " " << value;
      for (const auto& stored : probes) {
        EXPECT_FALSE(BinlogAccepts(table_config, stored))
            << "an uncomparable value admitted a row: " << type << " " << comparison_operator << " " << value;
      }
    }
  }
}

/**
 * @brief An operator outside the supported set is refused on both surfaces.
 */
TEST(RequiredFilterPredicateTest, AnUnsupportedOperatorIsRefusedOnBothSurfaces) {
  for (const auto* comparison_operator : {"<>", "LIKE", "IN", "= 1 OR 1", "", "IS NULL OR 1=1"}) {
    const auto table_config = TableWithRequiredFilter("col", "int", comparison_operator, "1");
    EXPECT_TRUE(internal::BuildInitialLoadSelectQuery(table_config, {}).empty()) << comparison_operator;
    EXPECT_FALSE(BinlogAccepts(table_config, int64_t{1})) << comparison_operator;
  }
}

// ===========================================================================
// Disagreements the shared declaration closes
// ===========================================================================

/**
 * @brief An epoch-valued datetime bound is a real instant in the SELECT.
 *
 * ParseDatetimeValue reads a bare number as epoch seconds, so the evaluator has
 * always accepted one. Passing the same digits to MySQL as a character literal
 * compares a DATETIME column against a string that is not a datetime, which is
 * NULL for every row, so the load selected nothing at all.
 */
TEST(RequiredFilterPredicateTest, AnEpochValuedDatetimeBoundIsEmittedAsAWallClock) {
  config::MysqlConfig mysql_config;
  mysql_config.datetime_timezone = "+09:00";

  for (const auto* type : {"datetime", "date"}) {
    const auto table_config = TableWithRequiredFilter("published_at", type, ">=", "1704067200");
    const auto query = internal::BuildInitialLoadSelectQuery(table_config, mysql_config);
    ASSERT_FALSE(query.empty()) << type;
    EXPECT_NE(query.find("`published_at` >= " + Literal("2024-01-01 09:00:00")), std::string::npos) << query;
    EXPECT_EQ(query.find(Literal("1704067200")), std::string::npos)
        << "an epoch reached MySQL as a character literal, which no DATETIME comparison reads: " << query;
  }
}

/**
 * @brief A datetime bound is written back in the offset it was read in.
 *
 * A DATETIME column's stored wall clock and the configured threshold are both
 * converted with mysql.datetime_timezone, so the offsets cancel. Emitting the
 * threshold in UTC instead would shift the boundary by the offset.
 */
TEST(RequiredFilterPredicateTest, ADatetimeBoundKeepsTheConfiguredOffsetsCancelled) {
  config::MysqlConfig mysql_config;
  mysql_config.datetime_timezone = "+09:00";

  const auto table_config = TableWithRequiredFilter("published_at", "datetime", ">=", "2024-01-01 12:00:00");
  const auto query = internal::BuildInitialLoadSelectQuery(table_config, mysql_config);
  ASSERT_FALSE(query.empty());
  EXPECT_NE(query.find("`published_at` >= " + Literal("2024-01-01 12:00:00")), std::string::npos) << query;
}

/**
 * @brief A boolean written as a word is expressible in the SELECT.
 *
 * The evaluator accepts "true" case-insensitively. Requiring the same value to
 * look like a number aborted the whole initial load over a configuration
 * replication was happy with.
 */
TEST(RequiredFilterPredicateTest, ABooleanWrittenAsAWordBuildsAQuery) {
  for (const auto& [value, expected] : std::vector<std::pair<std::string, std::string>>{
           {"true", "1"}, {"TRUE", "1"}, {"false", "0"}, {"False", "0"}, {"1", "1"}, {"0", "0"}}) {
    const auto table_config = TableWithRequiredFilter("enabled", "boolean", "=", value);
    const auto query = internal::BuildInitialLoadSelectQuery(table_config, {});
    ASSERT_FALSE(query.empty()) << value;
    EXPECT_NE(query.find("`enabled` = " + expected), std::string::npos) << query;
  }
}

/**
 * @brief A boolean spelling outside MySQL's own refuses the query.
 *
 * "2" satisfies a bare numeric literal check and would have selected rows that
 * cannot decode to a boolean at all, which replication then rejects.
 */
TEST(RequiredFilterPredicateTest, ABooleanSpellingOutsideTheRecognizedSetRefusesTheQuery) {
  for (const auto* value : {"2", "yes", "-1", "1.0"}) {
    EXPECT_TRUE(
        internal::BuildInitialLoadSelectQuery(TableWithRequiredFilter("enabled", "boolean", "=", value), {}).empty())
        << value;
  }
}

/**
 * A FLOAT column widens to double before any comparison, so a decimal no float
 * can hold never equals a stored value. Measured on MySQL 8.4 for a
 * FLOAT column holding 0.1:
 *   `f` = 0.1                        -> false
 *   `f` = 0.1e0                      -> false
 *   `f` = CAST(0.1e0 AS FLOAT)       -> true
 * The same widening decides the C++ side, which narrows both operands.
 */
TEST(RequiredFilterPredicateTest, AFloatBoundIsComparedAtFloatPrecisionOnBothSurfaces) {
  const auto table_config = TableWithRequiredFilter("score", "float", "=", "0.1");
  const auto query = internal::BuildInitialLoadSelectQuery(table_config, {});
  ASSERT_FALSE(query.empty());
  EXPECT_NE(query.find("`score` = CAST("), std::string::npos) << query;
  EXPECT_NE(query.find(" AS FLOAT)"), std::string::npos) << query;

  // What a FLOAT column carrying 0.1 decodes to on each ingest path.
  EXPECT_TRUE(BinlogAccepts(table_config, 0.1));
  EXPECT_TRUE(BinlogAccepts(table_config, 0.100000001));
  EXPECT_FALSE(BinlogAccepts(table_config, 0.2));
}

/**
 * @brief A double bound is compared exactly, as MySQL and query filters both do.
 *
 * Measured on MySQL 8.4: a DOUBLE column holding 0.1 equals both 0.1 and 0.1e0,
 * and a 17-digit exponent literal round-trips the same double.
 */
TEST(RequiredFilterPredicateTest, ADoubleBoundIsComparedExactly) {
  const auto table_config = TableWithRequiredFilter("score", "double", "=", "0.3");
  const auto query = internal::BuildInitialLoadSelectQuery(table_config, {});
  ASSERT_FALSE(query.empty());

  auto emitted = EmittedPredicate(query, "=");
  ASSERT_TRUE(emitted.has_value());
  auto target = mygram::utils::ParseNumeric<double>(emitted->literal);
  ASSERT_TRUE(target.has_value()) << emitted->literal;
  EXPECT_EQ(*target, 0.3) << "the emitted literal is not the double the evaluator parsed";

  EXPECT_TRUE(BinlogAccepts(table_config, 0.3));
  EXPECT_FALSE(BinlogAccepts(table_config, 0.1 + 0.2)) << "a near miss the server would not accept was admitted";
}

/**
 * @brief A double bound in exponent form is expressible in the SELECT.
 */
TEST(RequiredFilterPredicateTest, ADoubleBoundInExponentFormBuildsAQuery) {
  const auto table_config = TableWithRequiredFilter("score", "double", ">=", "1.23e10");
  const auto query = internal::BuildInitialLoadSelectQuery(table_config, {});
  ASSERT_FALSE(query.empty()) << "a value the evaluator accepts must also be expressible in the SELECT";
  EXPECT_TRUE(BinlogAccepts(table_config, 1.23e10));
}

TEST(RequiredFilterPredicateTest, DateTimeLiteralFormattingCoversTheMySQLRange) {
  using mygramdb::utils::FormatMySQLDateTimeLiteral;
  EXPECT_EQ(FormatMySQLDateTimeLiteral(0, 0), "1970-01-01 00:00:00");
  EXPECT_EQ(FormatMySQLDateTimeLiteral(0, 32400), "1970-01-01 09:00:00");
  EXPECT_EQ(FormatMySQLDateTimeLiteral(-1, 0), "1969-12-31 23:59:59");
  EXPECT_EQ(FormatMySQLDateTimeLiteral(1704110400, 0), "2024-01-01 12:00:00");
  // A leap day, and the last instant MySQL's DATETIME range holds.
  EXPECT_EQ(FormatMySQLDateTimeLiteral(1709164800, 0), "2024-02-29 00:00:00");
  EXPECT_EQ(FormatMySQLDateTimeLiteral(253402300799, 0), "9999-12-31 23:59:59");
  EXPECT_EQ(FormatMySQLDateTimeLiteral(-30610224000, 0), "1000-01-01 00:00:00");
  EXPECT_FALSE(FormatMySQLDateTimeLiteral(253402300800, 0).has_value());
  EXPECT_FALSE(FormatMySQLDateTimeLiteral(-30610224001, 0).has_value());
}

}  // namespace
}  // namespace mygramdb::loader

#endif  // USE_MYSQL
