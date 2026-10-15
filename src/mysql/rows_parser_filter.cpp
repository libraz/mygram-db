/**
 * @file rows_parser_filter.cpp
 * @brief Filter extraction from parsed MySQL row data
 *
 * Contains ExtractFilters, extracted from rows_parser.cpp for translation
 * unit splitting.
 */

#include <spdlog/spdlog.h>

#include "mysql/rows_parser.h"
#include "storage/filter_index.h"
#include "utils/structured_log.h"

#ifdef USE_MYSQL

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

namespace mygramdb::mysql {

storage::FilterMap ExtractFilters(const RowData& row_data, const std::vector<config::FilterConfig>& filter_configs,
                                  const std::string& datetime_timezone, UnconvertedFilterText& unconverted) {
  storage::FilterMap filters;
  unconverted.clear();

  for (const auto& filter_config : filter_configs) {
    const bool is_null = row_data.IsColumnNull(filter_config.name);
    const std::string* value = row_data.FindColumnValue(filter_config.name);
    if (value == nullptr && !is_null) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_warning")
          .Field("type", "filter_column_not_found")
          .Field("column_name", filter_config.name)
          .Warn();
      continue;
    }
    const std::string_view value_view = value == nullptr ? std::string_view{} : std::string_view(*value);
    auto converted = storage::ConvertFilterValue(value_view, is_null, filter_config.type, datetime_timezone);
    if (converted.has_value()) {
      filters[filter_config.name] = std::move(*converted);
    } else {
      unconverted[filter_config.name] = std::string(value_view);
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_warning")
          .Field("type", "filter_conversion_failed")
          .Field("filter_type", filter_config.type)
          .Field("column_name", filter_config.name)
          .Warn();
    }
  }

  return filters;
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

#endif  // USE_MYSQL
