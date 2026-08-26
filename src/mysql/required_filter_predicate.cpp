/**
 * @file required_filter_predicate.cpp
 * @brief Comparison domains shared by the initial-load SELECT and replication
 */

#include "mysql/required_filter_predicate.h"

#ifdef USE_MYSQL

#include <cmath>
#include <limits>

#include "utils/comparison_utils.h"
#include "utils/datetime_converter.h"
#include "utils/numeric_parse.h"
#include "utils/sql_utils.h"
#include "utils/string_utils.h"

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

}  // namespace

/**
 * @brief Renders the resolved target as the right-hand side of a WHERE conjunct.
 *
 * The left-hand side is the bare column unless the domain needs the server to
 * project it first: text is forced to a byte comparison, and a TIMESTAMP is
 * reduced to the UTC epoch replication decodes it to.
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
    return Compare(column, mygramdb::utils::EncodeMySQLStringLiteral(target.literal));
  }
  std::string operator()(const UtcEpoch& target) const {
    return Compare("UNIX_TIMESTAMP(" + column + ")", std::to_string(target.epoch_seconds));
  }
  std::string operator()(const Clock& target) const {
    return Compare(column, mygramdb::utils::EncodeMySQLStringLiteral(target.literal));
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
    const std::string spelling = mygram::utils::ToLower(filter.value);
    if (spelling == "1" || spelling == "true") {
      return RequiredFilterPredicate(filter.op, Boolean{true});
    }
    if (spelling == "0" || spelling == "false") {
      return RequiredFilterPredicate(filter.op, Boolean{false});
    }
    return MakeUnexpected(InvalidValue(filter, "not one of 1, 0, true, false"));
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
    return RequiredFilterPredicate(filter.op, WallClock{*epoch, std::move(*literal)});
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
    // MySQL reads a bare number compared against a TIME column as packed
    // HHMMSS, so the resolved seconds are written back as a clock literal.
    auto literal = mygramdb::utils::FormatMySQLTimeLiteral(*seconds);
    if (!literal) {
      return MakeUnexpected(InvalidValue(filter, "outside the TIME range of +/-838:59:59"));
    }
    return RequiredFilterPredicate(filter.op, Clock{*seconds, std::move(*literal)});
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

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
