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
#include "utils/string_utils.h"
#include "utils/structured_log.h"

#ifdef USE_MYSQL

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers,readability-function-cognitive-complexity,readability-else-after-return)

namespace mygramdb::mysql {

namespace {

template <typename T>
std::optional<storage::FilterValue> ParseFilterNumeric(std::string_view value) {
  auto parsed = mygram::utils::ParseNumeric<T>(value);
  if (!parsed) {
    return std::nullopt;
  }
  return storage::FilterValue{*parsed};
}

}  // namespace

std::optional<storage::FilterValue> ConvertFilterValue(std::string_view value, bool is_null,
                                                       std::string_view filter_type,
                                                       const std::string& datetime_timezone) {
  if (is_null) {
    return storage::FilterValue{std::monostate{}};
  }

  if (filter_type == "tinyint") {
    return ParseFilterNumeric<int8_t>(value);
  }
  if (filter_type == "tinyint_unsigned") {
    return ParseFilterNumeric<uint8_t>(value);
  }
  if (filter_type == "smallint") {
    return ParseFilterNumeric<int16_t>(value);
  }
  if (filter_type == "smallint_unsigned") {
    return ParseFilterNumeric<uint16_t>(value);
  }
  if (filter_type == "int" || filter_type == "mediumint") {
    return ParseFilterNumeric<int32_t>(value);
  }
  if (filter_type == "int_unsigned" || filter_type == "mediumint_unsigned") {
    return ParseFilterNumeric<uint32_t>(value);
  }
  if (filter_type == "bigint") {
    return ParseFilterNumeric<int64_t>(value);
  }
  if (filter_type == "bigint_unsigned") {
    return ParseFilterNumeric<uint64_t>(value);
  }
  if (filter_type == "float" || filter_type == "double") {
    return ParseFilterNumeric<double>(value);
  }
  if (filter_type == "string" || filter_type == "varchar" || filter_type == "text") {
    return storage::FilterValue{std::string(value)};
  }
  if (filter_type == "boolean") {
    const std::string normalized = mygram::utils::ToLower(std::string(value));
    if (normalized == "1" || normalized == "true") {
      return storage::FilterValue{true};
    }
    if (normalized == "0" || normalized == "false") {
      return storage::FilterValue{false};
    }
    return std::nullopt;
  }
  if (filter_type == "datetime" || filter_type == "date") {
    auto epoch = mygram::utils::ParseDatetimeValue(std::string(value), datetime_timezone);
    return epoch ? std::optional<storage::FilterValue>{storage::FilterValue{*epoch}} : std::nullopt;
  }
  if (filter_type == "timestamp") {
    // TIMESTAMP is selected in UTC (Connection pins @@session.time_zone) and
    // decoded from binlog as UTC. DATETIME alone uses datetime_timezone.
    auto epoch = mygram::utils::ParseDatetimeValue(std::string(value), "+00:00");
    return epoch ? std::optional<storage::FilterValue>{storage::FilterValue{*epoch}} : std::nullopt;
  }
  if (filter_type == "time") {
    auto seconds = mygram::utils::DateTimeProcessor::TimeToSeconds(std::string(value));
    return seconds ? std::optional<storage::FilterValue>{storage::FilterValue{storage::TimeValue{*seconds}}}
                   : std::nullopt;
  }
  return std::nullopt;
}

storage::FilterMap ExtractFilters(const RowData& row_data, const std::vector<config::FilterConfig>& filter_configs,
                                  const std::string& datetime_timezone) {
  storage::FilterMap filters;

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
    auto converted = ConvertFilterValue(value_view, is_null, filter_config.type, datetime_timezone);
    if (converted.has_value()) {
      filters[filter_config.name] = std::move(*converted);
    } else {
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
