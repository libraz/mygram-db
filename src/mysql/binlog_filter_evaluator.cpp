/**
 * @file binlog_filter_evaluator.cpp
 * @brief Binlog filter evaluation implementation
 */

#include "mysql/binlog_filter_evaluator.h"

#ifdef USE_MYSQL

#include <spdlog/spdlog.h>

#include <algorithm>
#include <optional>

#include "mysql/required_filter_predicate.h"
#include "mysql/rows_parser.h"
#include "utils/structured_log.h"

namespace mygramdb::mysql {

namespace {

/// Resolve a required filter, logging why a configuration that cannot decide membership is refused.
std::optional<RequiredFilterPredicate> ResolveRequiredFilter(const config::RequiredFilterConfig& filter,
                                                             const std::string& datetime_timezone) {
  auto predicate = RequiredFilterPredicate::Resolve(filter, datetime_timezone);
  if (!predicate) {
    // Fail-closed: a filter whose configured value cannot be compared at all
    // must not admit rows. The initial load refuses to run against the same
    // configuration, so neither path indexes anything under it.
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_warning")
        .Field("type", "unusable_required_filter")
        .Field("column_name", filter.name)
        .Field("filter_type", filter.type)
        .Field("error", predicate.error().message())
        .Warn();
    return std::nullopt;
  }
  return std::move(*predicate);
}

}  // namespace

bool BinlogFilterEvaluator::EvaluateRequiredFilters(const storage::FilterMap& filters,
                                                    const UnconvertedFilterText& unconverted,
                                                    const config::TableConfig& table_config,
                                                    const std::string& datetime_timezone) {
  // If no required_filters, all data is accepted
  if (table_config.required_filters.empty()) {
    return true;
  }

  // Check each required filter condition
  return std::all_of(table_config.required_filters.begin(), table_config.required_filters.end(),
                     [&filters, &unconverted, &datetime_timezone](const auto& required_filter) {
                       auto filter_iter = filters.find(required_filter.name);
                       if (filter_iter != filters.end()) {
                         return CompareFilterValue(filter_iter->second, required_filter, datetime_timezone);
                       }
                       auto text_iter = unconverted.find(required_filter.name);
                       if (text_iter == unconverted.end()) {
                         mygram::utils::StructuredLog()
                             .Event("mysql_binlog_warning")
                             .Field("type", "required_filter_column_not_found")
                             .Field("column_name", required_filter.name)
                             .Warn();
                         return false;
                       }
                       // Row images are FULL, so a column that is present but not in the map held a value that
                       // did not decode into the filter's type. The initial load judged such a row by its WHERE
                       // clause.
                       mygram::utils::StructuredLog()
                           .Event("mysql_binlog_warning")
                           .Field("type", "required_filter_value_not_decoded")
                           .Field("column_name", required_filter.name)
                           .Warn();
                       auto predicate = ResolveRequiredFilter(required_filter, datetime_timezone);
                       return predicate.has_value() && predicate->MatchesUndecodedValue(text_iter->second);
                     });
}

bool BinlogFilterEvaluator::CompareFilterValue(const storage::FilterValue& value,
                                               const config::RequiredFilterConfig& filter,
                                               const std::string& datetime_timezone) {
  auto predicate = ResolveRequiredFilter(filter, datetime_timezone);
  return predicate.has_value() && predicate->Matches(value);
}

storage::FilterMap BinlogFilterEvaluator::ExtractAllFilters(const RowData& row_data,
                                                            const config::TableConfig& table_config,
                                                            const std::string& datetime_timezone,
                                                            UnconvertedFilterText& unconverted) {
  return ExtractFilters(row_data, config::BuildUnifiedFilterConfigs(table_config), datetime_timezone, unconverted);
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
