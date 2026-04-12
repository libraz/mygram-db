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
#include "utils/structured_log.h"

#ifdef USE_MYSQL

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

namespace mygramdb::mysql {

std::unordered_map<std::string, storage::FilterValue> ExtractFilters(
    const RowData& row_data, const std::vector<config::FilterConfig>& filter_configs,
    const std::string& datetime_timezone) {
  std::unordered_map<std::string, storage::FilterValue> filters;

  for (const auto& filter_config : filter_configs) {
    // Check if column exists in row data
    auto iterator = row_data.columns.find(filter_config.name);
    if (iterator == row_data.columns.end()) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_warning")
          .Field("type", "filter_column_not_found")
          .Field("column_name", filter_config.name)
          .Warn();
      continue;
    }

    const std::string& value_str = iterator->second;

    // Skip empty values (NULL)
    if (value_str.empty()) {
      continue;
    }

    try {
      // Convert string to appropriate type based on filter config
      if (filter_config.type == "tinyint") {
        filters[filter_config.name] = static_cast<int8_t>(std::stoi(value_str));
      } else if (filter_config.type == "tinyint_unsigned") {
        filters[filter_config.name] = static_cast<uint8_t>(std::stoul(value_str));
      } else if (filter_config.type == "smallint") {
        filters[filter_config.name] = static_cast<int16_t>(std::stoi(value_str));
      } else if (filter_config.type == "smallint_unsigned") {
        filters[filter_config.name] = static_cast<uint16_t>(std::stoul(value_str));
      } else if (filter_config.type == "int" || filter_config.type == "mediumint") {
        filters[filter_config.name] = static_cast<int32_t>(std::stoi(value_str));
      } else if (filter_config.type == "int_unsigned" || filter_config.type == "mediumint_unsigned") {
        filters[filter_config.name] = static_cast<uint32_t>(std::stoul(value_str));
      } else if (filter_config.type == "bigint") {
        filters[filter_config.name] = static_cast<int64_t>(std::stoll(value_str));
      } else if (filter_config.type == "float" || filter_config.type == "double") {
        filters[filter_config.name] = std::stod(value_str);
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
        // TIMESTAMP: Already in epoch seconds (UTC), no timezone conversion needed
        try {
          filters[filter_config.name] = static_cast<uint64_t>(std::stoull(value_str));
        } catch (const std::exception& e) {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "timestamp_conversion_failed")
              .Field("value", value_str)
              .Field("column_name", filter_config.name)
              .Field("error", e.what())
              .Error();
        }
      } else if (filter_config.type == "time") {
        // TIME: Convert to seconds since midnight using DateTimeProcessor
        // Create a temporary MysqlConfig to use DateTimeProcessor
        config::MysqlConfig temp_config;
        temp_config.datetime_timezone = datetime_timezone;
        auto processor_result = temp_config.CreateDateTimeProcessor();
        if (!processor_result) {
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "datetime_processor_creation_failed")
              .Field("column_name", filter_config.name)
              .Field("error", processor_result.error().message())
              .Error();
        } else {
          auto seconds_result = processor_result->TimeToSeconds(value_str);
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
    } catch (const std::exception& e) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_error")
          .Field("type", "filter_conversion_failed")
          .Field("value", value_str)
          .Field("column_name", filter_config.name)
          .Field("error", e.what())
          .Error();
    }
  }

  return filters;
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

#endif  // USE_MYSQL
