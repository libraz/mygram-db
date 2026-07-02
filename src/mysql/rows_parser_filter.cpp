/**
 * @file rows_parser_filter.cpp
 * @brief Filter extraction from parsed MySQL row data
 *
 * Contains ExtractFilters, extracted from rows_parser.cpp for translation
 * unit splitting.
 */

#include <spdlog/spdlog.h>

#include "mysql/rows_parser.h"
#include "utils/datetime_converter.h"
#include "utils/numeric_parse.h"
#include "utils/structured_log.h"

#ifdef USE_MYSQL

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

namespace mygramdb::mysql {

storage::FilterMap ExtractFilters(const RowData& row_data, const std::vector<config::FilterConfig>& filter_configs,
                                  const std::string& datetime_timezone) {
  storage::FilterMap filters;

  // Pre-create DateTimeProcessor once for all TIME-type filters (avoid per-row allocation)
  std::optional<mygram::utils::DateTimeProcessor> cached_processor;
  auto get_processor = [&]() -> mygram::utils::DateTimeProcessor* {
    if (!cached_processor.has_value()) {
      config::MysqlConfig temp_config;
      temp_config.datetime_timezone = datetime_timezone;
      auto result = temp_config.CreateDateTimeProcessor();
      if (result) {
        cached_processor.emplace(*result);
      } else {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_error")
            .Field("type", "datetime_processor_creation_failed")
            .Field("error", result.error().message())
            .Error();
        return nullptr;
      }
    }
    return &*cached_processor;
  };

  // Helper lambda: parse numeric type and assign to filters, logging on failure
  auto try_parse_numeric = [&](const auto& name, const std::string& value, auto tag) {
    using T = decltype(tag);
    auto parsed = mygram::utils::ParseNumeric<T>(value);
    if (parsed) {
      filters[name] = *parsed;
    } else {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "filter_parse_failed")
          .Field("value", value)
          .Field("column_name", name)
          .Error();
    }
  };

  for (const auto& filter_config : filter_configs) {
    // Check if column exists in row data
    const std::string* value = row_data.FindColumnValue(filter_config.name);
    if (value == nullptr) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_warning")
          .Field("type", "filter_column_not_found")
          .Field("column_name", filter_config.name)
          .Warn();
      continue;
    }

    const std::string& value_str = *value;

    // Skip explicit NULL values. Empty strings are valid filter values for string columns.
    if (row_data.IsColumnNull(filter_config.name)) {
      continue;
    }

    // Convert string to appropriate type based on filter config
    if (filter_config.type == "tinyint") {
      try_parse_numeric(filter_config.name, value_str, int8_t{});
    } else if (filter_config.type == "tinyint_unsigned") {
      try_parse_numeric(filter_config.name, value_str, uint8_t{});
    } else if (filter_config.type == "smallint") {
      try_parse_numeric(filter_config.name, value_str, int16_t{});
    } else if (filter_config.type == "smallint_unsigned") {
      try_parse_numeric(filter_config.name, value_str, uint16_t{});
    } else if (filter_config.type == "int" || filter_config.type == "mediumint") {
      try_parse_numeric(filter_config.name, value_str, int32_t{});
    } else if (filter_config.type == "int_unsigned" || filter_config.type == "mediumint_unsigned") {
      try_parse_numeric(filter_config.name, value_str, uint32_t{});
    } else if (filter_config.type == "bigint") {
      try_parse_numeric(filter_config.name, value_str, int64_t{});
    } else if (filter_config.type == "bigint_unsigned") {
      try_parse_numeric(filter_config.name, value_str, uint64_t{});
    } else if (filter_config.type == "float" || filter_config.type == "double") {
      try_parse_numeric(filter_config.name, value_str, double{});
    } else if (filter_config.type == "datetime" || filter_config.type == "date") {
      // DATETIME/DATE: Convert to epoch seconds using timezone
      auto epoch_opt = mygram::utils::ParseDatetimeValue(value_str, datetime_timezone);
      if (epoch_opt) {
        filters[filter_config.name] = *epoch_opt;
      } else {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_warning")
            .Field("type", "datetime_conversion_failed")
            .Field("value", value_str)
            .Field("column_name", filter_config.name)
            .Field("timezone", datetime_timezone)
            .Warn();
      }
    } else if (filter_config.type == "timestamp") {
      auto epoch_opt = mygram::utils::ParseDatetimeValue(value_str, "+00:00");
      if (epoch_opt) {
        filters[filter_config.name] = *epoch_opt;
      } else {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_warning")
            .Field("type", "timestamp_conversion_failed")
            .Field("value", value_str)
            .Field("column_name", filter_config.name)
            .Warn();
      }
    } else if (filter_config.type == "time") {
      // TIME: Convert to seconds since midnight using cached DateTimeProcessor
      auto* processor = get_processor();
      if (processor != nullptr) {
        auto seconds_result = processor->TimeToSeconds(value_str);
        if (seconds_result) {
          filters[filter_config.name] = storage::TimeValue{*seconds_result};
        } else {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_warning")
              .Field("type", "time_conversion_failed")
              .Field("value", value_str)
              .Field("column_name", filter_config.name)
              .Field("error", seconds_result.error().message())
              .Warn();
        }
      }
    } else if (filter_config.type == "string" || filter_config.type == "varchar" || filter_config.type == "text") {
      filters[filter_config.name] = value_str;
    } else if (filter_config.type == "boolean") {
      // Boolean: "1"/"true" = true, "0"/"false" = false
      filters[filter_config.name] = (value_str == "1" || value_str == "true");
    } else {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_warning")
          .Field("type", "unknown_filter_type")
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
