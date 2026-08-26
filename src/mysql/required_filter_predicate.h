/**
 * @file required_filter_predicate.h
 * @brief The single declaration a required_filters membership test is built from
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>
#include <string>
#include <string_view>
#include <variant>

#include "config/config.h"
#include "storage/document_store.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/namespace_compat.h"

namespace mygramdb::mysql {

/**
 * @brief Membership rule of one `required_filters` entry.
 *
 * A required filter answers "does this row belong in the index?" twice: once as
 * a conjunct of the initial-load SELECT's WHERE clause, and once in C++ when a
 * binlog row event arrives. The two answers have to agree for every row, or a
 * row is admitted at load and then silently dropped on its first UPDATE.
 *
 * Resolve() reduces the configured (type, op, value) triple to one comparison
 * domain plus a target value already expressed in that domain. SqlPredicate()
 * and Matches() are the only ways to obtain either answer, and both are
 * produced by visiting that one domain, so a domain that can be rendered as SQL
 * but cannot decide membership - or the reverse - does not compile.
 */
class RequiredFilterPredicate {
 public:
  /// Largest configured value accepted, guarding against a multi-gigabyte value.
  static constexpr size_t kMaxValueSize = 1024 * 1024;

  /**
   * @brief Whether a comparison operator may take part in a membership test.
   *
   * The configuration schema already constrains this field to an enumeration.
   * Repeating the constraint here keeps the guarantee at the point where the
   * string becomes SQL, so a configuration reaching the loader by some other
   * route cannot inject through it.
   *
   * @param comparison_operator Operator text from the configuration.
   * @return true when the operator is one of the eight supported comparisons.
   */
  [[nodiscard]] static bool IsSupportedOperator(std::string_view comparison_operator);

  /**
   * @brief Resolve a configured required filter into its comparison domain.
   * @param filter Required filter configuration.
   * @param datetime_timezone Offset DATETIME and DATE values are read in.
   * @return Resolved predicate, or kConfigInvalidValue describing why the
   *         configured filter cannot decide membership at all.
   */
  static mygram::utils::Expected<RequiredFilterPredicate, mygram::utils::Error> Resolve(
      const config::RequiredFilterConfig& filter, const std::string& datetime_timezone);

  /**
   * @brief One conjunct of the initial-load SELECT's WHERE clause.
   * @param quoted_column Backtick-quoted filter column name.
   * @return SQL comparing the column against the resolved target.
   */
  [[nodiscard]] std::string SqlPredicate(const std::string& quoted_column) const;

  /**
   * @brief Membership verdict for one row's decoded column value.
   * @param value Filter value decoded from a binlog row event or snapshot row.
   * @return true when the row satisfies this filter.
   */
  [[nodiscard]] bool Matches(const storage::FilterValue& value) const;

 private:
  /// Tested for SQL NULL state; the operator carries the whole comparison.
  struct NullState {};
  /// Compared byte for byte, so the column's collation does not decide membership.
  struct Bytes {
    std::string value;
  };
  /// Compared as signed 64-bit; covers every integer width except BIGINT UNSIGNED.
  struct Signed {
    int64_t value;
  };
  /// Compared as unsigned 64-bit.
  struct Unsigned {
    uint64_t value;
  };
  /// Compared after narrowing both sides to FLOAT precision, as the server does.
  struct SinglePrecision {
    float value;
  };
  /// Compared as IEEE double, exactly.
  struct DoublePrecision {
    double value;
  };
  /// Compared as an instant, written back as the wall clock it was read as.
  struct WallClock {
    int64_t epoch_seconds;
    std::string literal;
  };
  /// Compared as UTC epoch seconds against UNIX_TIMESTAMP() of the column.
  struct UtcEpoch {
    int64_t epoch_seconds;
  };
  /// Compared as seconds since midnight, written back as a TIME literal.
  struct Clock {
    int64_t seconds;
    std::string literal;
  };
  /// Compared as MySQL's 1/0 spelling of a TINYINT(1).
  struct Boolean {
    bool value;
  };

  using Target = std::variant<NullState, Bytes, Signed, Unsigned, SinglePrecision, DoublePrecision, WallClock, UtcEpoch,
                              Clock, Boolean>;

  struct SqlRenderer;
  struct MembershipTest;

  RequiredFilterPredicate(std::string comparison_operator, Target target)
      : op_(std::move(comparison_operator)), target_(std::move(target)) {}

  std::string op_;
  Target target_;
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
