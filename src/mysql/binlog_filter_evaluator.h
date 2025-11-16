/**
 * @file binlog_filter_evaluator.h
 * @brief Binlog filter evaluation utilities
 */

#pragma once

#ifdef USE_MYSQL

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
   * @return true if all required_filters conditions are satisfied
   */
  static bool EvaluateRequiredFilters(const std::unordered_map<std::string, storage::FilterValue>& filters,
                                      const config::TableConfig& table_config);

  /**
   * @brief Compare filter value against required filter condition
   * @param value Filter value from binlog
   * @param filter Required filter configuration
   * @return true if condition is satisfied
   */
  static bool CompareFilterValue(const storage::FilterValue& value, const config::RequiredFilterConfig& filter);

  /**
   * @brief Extract all filter columns (both required and optional) from row data
   * @param row_data Row data from binlog
   * @param table_config Table configuration to use for filter extraction
   * @return Map of filter name to FilterValue
   */
  static std::unordered_map<std::string, storage::FilterValue> ExtractAllFilters(
      const RowData& row_data, const config::TableConfig& table_config);
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
