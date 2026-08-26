/**
 * @file binlog_filter_evaluator.h
 * @brief Binlog filter evaluation utilities
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>
#include <optional>
#include <string_view>
#include <unordered_map>

#include "config/config.h"
#include "mysql/rows_parser.h"
#include "storage/document_store.h"

namespace mygramdb::mysql {

/**
 * @brief Binlog filter evaluator
 *
 * Evaluates required_filters conditions and extracts filter values from binlog events
 */
class BinlogFilterEvaluator {
 public:
  /**
   * @brief Evaluate required_filters conditions for a binlog event
   * @param filters Filter values from binlog event
   * @param table_config Table configuration containing required_filters
   * @param datetime_timezone Timezone offset for DATETIME interpretation (e.g., "+09:00")
   * @return true if all required_filters conditions are satisfied
   */
  static bool EvaluateRequiredFilters(const storage::FilterMap& filters, const config::TableConfig& table_config,
                                      const std::string& datetime_timezone = "+00:00");

  /**
   * @brief Compare filter value against required filter condition
   * @param value Filter value from binlog
   * @param filter Required filter configuration
   * @param datetime_timezone Timezone offset for DATETIME interpretation (e.g., "+09:00")
   * @return true if condition is satisfied
   */
  static bool CompareFilterValue(const storage::FilterValue& value, const config::RequiredFilterConfig& filter,
                                 const std::string& datetime_timezone = "+00:00");

  /**
   * @brief Parse a `time`-typed filter value into seconds since midnight
   *
   * Accepts either a bare number of seconds or an "HH:MM:SS" clock string.
   * Exposed so that the initial-load query builder derives its SQL literal from
   * the same rule that decides membership during replication.
   *
   * @param value Configured filter value
   * @return Seconds since midnight, or std::nullopt for an unsupported format
   */
  static std::optional<int64_t> ParseTimeFilterSeconds(std::string_view value);

  /**
   * @brief Extract all filter columns (both required and optional) from row data
   * @param row_data Row data from binlog
   * @param table_config Table configuration to use for filter extraction
   * @param datetime_timezone Timezone offset for DATETIME interpretation (e.g., "+09:00")
   * @return Map of filter name to FilterValue
   */
  static storage::FilterMap ExtractAllFilters(const RowData& row_data, const config::TableConfig& table_config,
                                              const std::string& datetime_timezone = "+00:00");
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
