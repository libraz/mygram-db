/**
 * @file required_filter_predicate.cpp
 * @brief Comparison domains shared by the initial-load SELECT and replication
 */

#include "mysql/required_filter_predicate.h"

#ifdef USE_MYSQL

#include <algorithm>
#include <cmath>
#include <limits>
#include <optional>

#include "storage/filter_index.h"
#include "utils/comparison_utils.h"
#include "utils/datetime_converter.h"
#include "utils/numeric_parse.h"
#include "utils/sql_utils.h"

namespace mygramdb::mysql {
namespace {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

Error InvalidValue(const config::RequiredFilterConfig& filter, const std::string& reason) {
  return MakeError(ErrorCode::kConfigInvalidValue,
                   "Required filter '" + filter.name + "' of type '" + filter.type +
                       "' cannot compare against value '" + filter.value + "': " + reason,
                   filter.name);
}

bool IsTextType(const std::string& type) {
  return type == "string" || type == "varchar" || type == "text";
}

bool IsUnsignedSixtyFourBitType(const std::string& type) {
  return type == "bigint_unsigned";
}

bool IsIntegerType(const std::string& type) {
  return type == "tinyint" || type == "tinyint_unsigned" || type == "smallint" || type == "smallint_unsigned" ||
         type == "mediumint" || type == "mediumint_unsigned" || type == "int" || type == "int_unsigned" ||
         type == "bigint";
}

/// Seconds since midnight from either a bare second count or "HH:MM:SS".
std::optional<int64_t> ParseTimeSeconds(const std::string& value) {
  if (auto seconds = mygram::utils::ParseNumeric<int64_t>(value)) {
    return seconds;
  }
  auto parsed = mygram::utils::DateTimeProcessor::TimeToSeconds(value);
  if (!parsed) {
    return std::nullopt;
  }
  return *parsed;
}

/// Widen any signed or narrow unsigned integer alternative to int64_t.
std::optional<int64_t> AsSignedInteger(const storage::FilterValue& value) {
  if (std::holds_alternative<int8_t>(value)) {
    return std::get<int8_t>(value);
  }
  if (std::holds_alternative<int16_t>(value)) {
    return std::get<int16_t>(value);
  }
  if (std::holds_alternative<int32_t>(value)) {
    return std::get<int32_t>(value);
  }
  if (std::holds_alternative<int64_t>(value)) {
    return std::get<int64_t>(value);
  }
  if (std::holds_alternative<uint8_t>(value)) {
    return std::get<uint8_t>(value);
  }
  if (std::holds_alternative<uint16_t>(value)) {
    return std::get<uint16_t>(value);
  }
  if (std::holds_alternative<uint32_t>(value)) {
    return static_cast<int64_t>(std::get<uint32_t>(value));
  }
  if (std::holds_alternative<uint64_t>(value)) {
    const uint64_t wide = std::get<uint64_t>(value);
    if (wide <= static_cast<uint64_t>(std::numeric_limits<int64_t>::max())) {
      return static_cast<int64_t>(wide);
    }
  }
  return std::nullopt;
}

/// Widen any unsigned integer alternative to uint64_t.
std::optional<uint64_t> AsUnsignedInteger(const storage::FilterValue& value) {
  if (std::holds_alternative<uint8_t>(value)) {
    return std::get<uint8_t>(value);
  }
  if (std::holds_alternative<uint16_t>(value)) {
    return std::get<uint16_t>(value);
  }
  if (std::holds_alternative<uint32_t>(value)) {
    return std::get<uint32_t>(value);
  }
  if (std::holds_alternative<uint64_t>(value)) {
    return std::get<uint64_t>(value);
  }
  return std::nullopt;
}

/// "YYYY-MM-DD HH:MM:SS" of a raw DATE / DATETIME / TIMESTAMP text, fraction dropped; zero parts are kept.
std::optional<std::string> WholeSecondWallClock(std::string_view raw) {
  constexpr size_t kDateLength = 10;
  constexpr size_t kDateTimeLength = 19;
  std::string text(raw);
  if (text.size() == kDateLength) {
    text += " 00:00:00";
  }
  if (text.size() < kDateTimeLength) {
    return std::nullopt;
  }
  if (text[kDateLength] == 'T') {
    text[kDateLength] = ' ';
  }
  text.resize(kDateTimeLength);
  constexpr std::string_view kShape = "dddd-dd-dd dd:dd:dd";
  for (size_t i = 0; i < kDateTimeLength; ++i) {
    const bool digit = text[i] >= '0' && text[i] <= '9';
    if (kShape[i] == 'd' ? !digit : text[i] != kShape[i]) {
      return std::nullopt;
    }
  }
  return text;
}

/// Truth of a bound that no column value can reach, for a column that is not NULL.
std::string ConstantBound(const std::string& column, bool holds) {
  return holds ? column + " IS NOT NULL" : "1 = 0";
}

/// `column op literal`, or, when the bound lies outside what the column can hold,
/// whether every non-NULL value satisfies it. @p beyond_end is which side the bound fell off.
std::string BoundOnColumn(const std::string& column, std::string_view comparison,
                          const std::optional<std::string>& text, bool beyond_end) {
  if (!text) {
    const bool lower_bound = comparison == ">=" || comparison == ">";
    return ConstantBound(column, lower_bound ? !beyond_end : beyond_end);
  }
  return column + " " + std::string(comparison) + " " + mygramdb::utils::EncodeMySQLStringLiteral(*text);
}

/// How a column value is cut down to the whole-second integer the binlog decode keeps.
enum class RangeAxis {
  kInstant,  ///< fraction dropped forward in time (DATETIME, DATE, TIMESTAMP)
  kClock     ///< fraction dropped toward zero (TIME)
};

/// One primitive bound on a truncated value: "truncated >= x" or "truncated < x".
struct TruncatedBound {
  bool at_least;
  int64_t x;
};

/// `truncate(value) <op> target` as one bound, or two joined by AND / OR.
struct TruncatedComparison {
  TruncatedBound first;
  std::optional<TruncatedBound> second;
  bool conjunctive;
};

/**
 * @brief Splits `truncate(value) <op> target` into half-open bounds at target and target + 1.
 *
 * The SQL rendering and the C++ check on undecoded text both go through this, so a
 * value ordered between two whole seconds (a partly zero date) gets one verdict.
 */
TruncatedComparison SplitTruncatedComparison(const std::string& op, int64_t target) {
  // Bounds past either end of the column's range are all the same constant.
  constexpr int64_t kFarBound = int64_t{1} << 60;
  target = std::clamp(target, -kFarBound, kFarBound);

  if (op == "=") {
    return {{true, target}, TruncatedBound{false, target + 1}, true};
  }
  if (op == "!=") {
    return {{false, target}, TruncatedBound{true, target + 1}, false};
  }
  if (op == "<") {
    return {{false, target}, std::nullopt, true};
  }
  if (op == "<=") {
    return {{false, target + 1}, std::nullopt, true};
  }
  if (op == ">") {
    return {{true, target + 1}, std::nullopt, true};
  }
  return {{true, target}, std::nullopt, true};
}

/**
 * @brief `truncate(column) <op> target` as comparisons on the bare column.
 *
 * Every temporal type is rendered here, so the SQL and the decode cannot cut a
 * fractional value differently and an index on the column stays usable. Each
 * operator becomes a combination of "truncated value >= x" and "truncated value
 * < x"; those are bounds on the column itself because truncation is monotonic.
 */
std::string RenderTruncatedRange(const std::string& column, const std::string& op, int64_t target, RangeAxis axis,
                                 int32_t offset_seconds) {
  const auto bound = [&](int64_t x, std::string_view comparison) {
    if (axis == RangeAxis::kInstant) {
      return BoundOnColumn(column, comparison, mygramdb::utils::FormatMySQLDateTimeLiteral(x, offset_seconds), x > 0);
    }
    return BoundOnColumn(column, comparison, mygramdb::utils::FormatMySQLTimeLiteral(x), x > 0);
  };
  const auto render = [&](const TruncatedBound& b) {
    // Toward zero, a non-positive x admits c > x - 1 (at least) or c <= x - 1 (below).
    if (axis == RangeAxis::kClock && b.x <= 0) {
      return bound(b.x - 1, b.at_least ? ">" : "<=");
    }
    return bound(b.x, b.at_least ? ">=" : "<");
  };

  const auto split = SplitTruncatedComparison(op, target);
  if (!split.second) {
    return render(split.first);
  }
  return "(" + render(split.first) + (split.conjunctive ? " AND " : " OR ") + render(*split.second) + ")";
}

}  // namespace

/**
 * @brief Renders the resolved target as the right-hand side of a WHERE conjunct.
 *
 * The left-hand side is the bare column unless the domain needs the server to
 * project it first: text is forced to a byte comparison.
 */
struct RequiredFilterPredicate::SqlRenderer {
  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members) - Visitor outlives nothing
  const std::string& column;
  const std::string& op;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)

  std::string Compare(const std::string& left, const std::string& right) const { return left + " " + op + " " + right; }

  std::string operator()(const NullState& /*target*/) const { return column + " " + op; }
  std::string operator()(const Bytes& target) const {
    return Compare("CAST(" + column + " AS BINARY)", mygramdb::utils::EncodeMySQLStringLiteral(target.value));
  }
  std::string operator()(const Signed& target) const { return Compare(column, std::to_string(target.value)); }
  std::string operator()(const Unsigned& target) const { return Compare(column, std::to_string(target.value)); }
  std::string operator()(const SinglePrecision& target) const {
    // A FLOAT column widens to double before comparison, so the target has to
    // make the same trip: an exact decimal that no float can hold would never
    // equal a stored value, however it was written.
    return Compare(column, "CAST(" + mygramdb::utils::FormatMySQLDoubleLiteral(target.value) + " AS FLOAT)");
  }
  std::string operator()(const DoublePrecision& target) const {
    return Compare(column, mygramdb::utils::FormatMySQLDoubleLiteral(target.value));
  }
  std::string operator()(const WallClock& target) const {
    return RenderTruncatedRange(column, op, target.epoch_seconds, RangeAxis::kInstant, target.offset_seconds);
  }
  std::string operator()(const UtcEpoch& target) const {
    return RenderTruncatedRange(column, op, target.epoch_seconds, RangeAxis::kInstant, 0);
  }
  std::string operator()(const Clock& target) const {
    return RenderTruncatedRange(column, op, target.seconds, RangeAxis::kClock, 0);
  }
  std::string operator()(const Boolean& target) const { return Compare(column, target.value ? "1" : "0"); }
};

/**
 * @brief Decides the same comparison in C++ for a decoded column value.
 *
 * A value that did not decode into the domain's own representation rejects the
 * row: a filter that cannot be evaluated must not admit anything.
 */
struct RequiredFilterPredicate::MembershipTest {
  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members) - Visitor outlives nothing
  const storage::FilterValue& value;
  const std::string& op;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)

  bool operator()(const NullState& /*target*/) const {
    const bool is_null = std::holds_alternative<std::monostate>(value);
    return op == "IS NULL" ? is_null : !is_null;
  }
  bool operator()(const Bytes& target) const {
    if (!std::holds_alternative<std::string>(value)) {
      return false;
    }
    return mygram::utils::CompareValues(std::get<std::string>(value), target.value, op);
  }
  bool operator()(const Signed& target) const {
    auto stored = AsSignedInteger(value);
    return stored && mygram::utils::CompareValues(*stored, target.value, op);
  }
  bool operator()(const Unsigned& target) const {
    auto stored = AsUnsignedInteger(value);
    return stored && mygram::utils::CompareValues(*stored, target.value, op);
  }
  bool operator()(const SinglePrecision& target) const {
    if (!std::holds_alternative<double>(value)) {
      return false;
    }
    return mygram::utils::CompareValues(static_cast<float>(std::get<double>(value)), target.value, op);
  }
  bool operator()(const DoublePrecision& target) const {
    if (!std::holds_alternative<double>(value)) {
      return false;
    }
    return mygram::utils::CompareValues(std::get<double>(value), target.value, op);
  }
  bool operator()(const WallClock& target) const {
    auto stored = AsSignedInteger(value);
    return stored && mygram::utils::CompareValues(*stored, target.epoch_seconds, op);
  }
  bool operator()(const UtcEpoch& target) const {
    auto stored = AsSignedInteger(value);
    return stored && mygram::utils::CompareValues(*stored, target.epoch_seconds, op);
  }
  bool operator()(const Clock& target) const {
    if (!std::holds_alternative<storage::TimeValue>(value)) {
      return false;
    }
    return mygram::utils::CompareValues(std::get<storage::TimeValue>(value).seconds, target.seconds, op);
  }
  bool operator()(const Boolean& target) const {
    if (!std::holds_alternative<bool>(value)) {
      return false;
    }
    return mygram::utils::CompareValues(std::get<bool>(value), target.value, op);
  }
};

/**
 * @brief The verdict the WHERE conjunct reaches for a non-NULL value that did not decode.
 *
 * Only the domains whose column can hold such a value are decided; for the rest
 * no stored value is undecodable, so the row is rejected.
 */
struct RequiredFilterPredicate::UndecodedValueTest {
  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members) - Visitor outlives nothing
  const std::string& op;
  std::string_view raw_text;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)

  /// The same half-open bounds the SQL renders, each decided by the zero-padded text order the
  /// server applies to a zero-part date.
  bool OrderedAgainst(int64_t epoch_seconds, int32_t offset_seconds) const {
    auto column = WholeSecondWallClock(raw_text);
    if (!column) {
      return false;
    }
    const auto holds = [&](const TruncatedBound& b) {
      auto literal = mygramdb::utils::FormatMySQLDateTimeLiteral(b.x, offset_seconds);
      if (!literal) {
        // Off the representable range: past the end for a positive x, before the start otherwise.
        return b.at_least ? b.x <= 0 : b.x > 0;
      }
      return b.at_least ? *column >= *literal : *column < *literal;
    };
    const auto split = SplitTruncatedComparison(op, epoch_seconds);
    if (!split.second) {
      return holds(split.first);
    }
    return split.conjunctive ? holds(split.first) && holds(*split.second) : holds(split.first) || holds(*split.second);
  }

  // The column is not NULL, only unrepresentable.
  bool operator()(const NullState& /*target*/) const { return op == "IS NOT NULL"; }
  bool operator()(const Bytes& /*target*/) const { return false; }
  bool operator()(const Signed& /*target*/) const { return false; }
  bool operator()(const Unsigned& /*target*/) const { return false; }
  bool operator()(const SinglePrecision& /*target*/) const { return false; }
  bool operator()(const DoublePrecision& /*target*/) const { return false; }
  bool operator()(const WallClock& target) const { return OrderedAgainst(target.epoch_seconds, target.offset_seconds); }
  bool operator()(const UtcEpoch& target) const { return OrderedAgainst(target.epoch_seconds, 0); }
  bool operator()(const Clock& /*target*/) const { return false; }
  // A value outside {0, 1} equals neither spelling of the target.
  bool operator()(const Boolean& /*target*/) const { return op == "!="; }
};

bool RequiredFilterPredicate::IsSupportedOperator(std::string_view comparison_operator) {
  return comparison_operator == "=" || comparison_operator == "!=" || comparison_operator == "<" ||
         comparison_operator == ">" || comparison_operator == "<=" || comparison_operator == ">=" ||
         comparison_operator == "IS NULL" || comparison_operator == "IS NOT NULL";
}

Expected<RequiredFilterPredicate, Error> RequiredFilterPredicate::Resolve(const config::RequiredFilterConfig& filter,
                                                                          const std::string& datetime_timezone) {
  if (!IsSupportedOperator(filter.op)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kConfigInvalidValue,
                  "Required filter '" + filter.name + "' uses unsupported operator '" + filter.op + "'", filter.name));
  }
  if (filter.op == "IS NULL" || filter.op == "IS NOT NULL") {
    return RequiredFilterPredicate(filter.op, NullState{});
  }

  // A binlog event can carry an arbitrarily large value; comparing against one
  // gains nothing and costs memory proportional to whatever was sent.
  if (filter.value.size() > kMaxValueSize) {
    return MakeUnexpected(InvalidValue(filter, "value exceeds " + std::to_string(kMaxValueSize) + " bytes"));
  }

  if (IsTextType(filter.type)) {
    return RequiredFilterPredicate(filter.op, Bytes{filter.value});
  }

  if (IsIntegerType(filter.type)) {
    auto target = mygram::utils::ParseNumeric<int64_t>(filter.value);
    if (!target) {
      return MakeUnexpected(InvalidValue(filter, "not a signed 64-bit integer"));
    }
    return RequiredFilterPredicate(filter.op, Signed{*target});
  }

  if (IsUnsignedSixtyFourBitType(filter.type)) {
    auto target = mygram::utils::ParseNumeric<uint64_t>(filter.value);
    if (!target) {
      return MakeUnexpected(InvalidValue(filter, "not an unsigned 64-bit integer"));
    }
    return RequiredFilterPredicate(filter.op, Unsigned{*target});
  }

  if (filter.type == "float") {
    auto target = mygram::utils::ParseNumeric<double>(filter.value);
    if (!target) {
      return MakeUnexpected(InvalidValue(filter, "not a finite number"));
    }
    const auto narrowed = static_cast<float>(*target);
    if (!std::isfinite(narrowed)) {
      return MakeUnexpected(InvalidValue(filter, "outside the FLOAT range"));
    }
    return RequiredFilterPredicate(filter.op, SinglePrecision{narrowed});
  }

  if (filter.type == "double") {
    auto target = mygram::utils::ParseNumeric<double>(filter.value);
    if (!target) {
      return MakeUnexpected(InvalidValue(filter, "not a finite number"));
    }
    return RequiredFilterPredicate(filter.op, DoublePrecision{*target});
  }

  if (filter.type == "boolean") {
    auto target = storage::ConvertFilterValue(filter.value, /*is_null=*/false, filter.type);
    if (!target) {
      return MakeUnexpected(InvalidValue(filter, "not one of 1, 0, true, false"));
    }
    return RequiredFilterPredicate(filter.op, Boolean{std::get<bool>(*target)});
  }

  if (filter.type == "datetime" || filter.type == "date") {
    auto offset = mygram::utils::ParseTimezoneOffset(datetime_timezone);
    if (!offset) {
      return MakeUnexpected(InvalidValue(filter, "timezone '" + datetime_timezone + "' is not an offset"));
    }
    auto epoch = mygram::utils::ParseDatetimeValue(filter.value, datetime_timezone);
    if (!epoch) {
      return MakeUnexpected(InvalidValue(filter, "not epoch seconds or an ISO 8601 datetime"));
    }
    // Written back as a wall clock in the same offset the column's values are
    // read in, so the offsets cancel on both sides exactly as they do in C++.
    auto literal = mygramdb::utils::FormatMySQLDateTimeLiteral(*epoch, *offset);
    if (!literal) {
      return MakeUnexpected(InvalidValue(filter, "outside the DATETIME year range of 1000-9999"));
    }
    return RequiredFilterPredicate(filter.op, WallClock{*epoch, *offset});
  }

  if (filter.type == "timestamp") {
    auto epoch = mygram::utils::ParseDatetimeValue(filter.value, datetime_timezone);
    if (!epoch) {
      return MakeUnexpected(InvalidValue(filter, "not epoch seconds or an ISO 8601 datetime"));
    }
    return RequiredFilterPredicate(filter.op, UtcEpoch{*epoch});
  }

  if (filter.type == "time") {
    auto seconds = ParseTimeSeconds(filter.value);
    if (!seconds) {
      return MakeUnexpected(InvalidValue(filter, "not a second count or an HH:MM:SS clock"));
    }
    // FormatMySQLTimeLiteral's range check is reused here to reject a value
    // no TIME column can hold.
    if (!mygramdb::utils::FormatMySQLTimeLiteral(*seconds)) {
      return MakeUnexpected(InvalidValue(filter, "outside the TIME range of +/-838:59:59"));
    }
    return RequiredFilterPredicate(filter.op, Clock{*seconds});
  }

  return MakeUnexpected(
      MakeError(ErrorCode::kConfigInvalidValue,
                "Required filter '" + filter.name + "' declares unsupported type '" + filter.type + "'", filter.name));
}

std::string RequiredFilterPredicate::SqlPredicate(const std::string& quoted_column) const {
  return std::visit(SqlRenderer{quoted_column, op_}, target_);
}

bool RequiredFilterPredicate::Matches(const storage::FilterValue& value) const {
  return std::visit(MembershipTest{value, op_}, target_);
}

bool RequiredFilterPredicate::MatchesUndecodedValue(std::string_view raw_text) const {
  return std::visit(UndecodedValueTest{op_, raw_text}, target_);
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
