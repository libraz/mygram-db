/**
 * @file binlog_filter_evaluator.cpp
 * @brief Binlog filter evaluation implementation
 */

#include "mysql/binlog_filter_evaluator.h"

#ifdef USE_MYSQL

#include <spdlog/spdlog.h>

#include <algorithm>
#include <charconv>
#include <cmath>

#include "mysql/rows_parser.h"
#include "utils/comparison_utils.h"
#include "utils/constants.h"
#include "utils/datetime_converter.h"
#include "utils/structured_log.h"

// NOLINTBEGIN(cppcoreguidelines-avoid-*,readability-magic-numbers)

namespace mygramdb::mysql {

bool BinlogFilterEvaluator::EvaluateRequiredFilters(const storage::FilterMap& filters,
                                                    const config::TableConfig& table_config,
                                                    const std::string& datetime_timezone) {
  // If no required_filters, all data is accepted
  if (table_config.required_filters.empty()) {
    return true;
  }

  // Check each required filter condition
  return std::all_of(table_config.required_filters.begin(), table_config.required_filters.end(),
                     [&filters, &datetime_timezone](const auto& required_filter) {
                       auto filter_iter = filters.find(required_filter.name);
                       if (filter_iter == filters.end()) {
                         mygram::utils::StructuredLog()
                             .Event("mysql_binlog_warning")
                             .Field("type", "required_filter_column_not_found")
                             .Field("column_name", required_filter.name)
                             .Warn();
                         return false;
                       }
                       return CompareFilterValue(filter_iter->second, required_filter, datetime_timezone);
                     });
}

bool BinlogFilterEvaluator::CompareFilterValue(const storage::FilterValue& value,
                                               const config::RequiredFilterConfig& filter,
                                               const std::string& datetime_timezone) {
  // SECURITY: Limit filter value size to prevent memory exhaustion attacks
  // A malicious binlog event could contain multi-GB filter values
  constexpr size_t kMaxFilterValueSize = 1024 * 1024;  // 1MB
  if (filter.value.size() > kMaxFilterValueSize) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_warning")
        .Field("type", "filter_value_too_large")
        .Field("value_size", static_cast<uint64_t>(filter.value.size()))
        .Field("max_size", static_cast<uint64_t>(kMaxFilterValueSize))
        .Field("filter_name", filter.name)
        .Warn();
    return false;  // Fail-safe: reject oversized filter values
  }

  // Handle NULL checks
  if (filter.op == "IS NULL") {
    return std::holds_alternative<std::monostate>(value);
  }
  if (filter.op == "IS NOT NULL") {
    return !std::holds_alternative<std::monostate>(value);
  }

  // If value is NULL and operator is not IS NULL/IS NOT NULL, condition fails
  if (std::holds_alternative<std::monostate>(value)) {
    return false;
  }

  // Compare based on type
  if (std::holds_alternative<int64_t>(value)) {
    // Integer comparison
    int64_t val = std::get<int64_t>(value);
    int64_t target = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) - Required for from_chars range
    auto [ptr, ec] = std::from_chars(filter.value.data(), filter.value.data() + filter.value.size(), target);
    if (ec != std::errc() || ptr != filter.value.data() + filter.value.size()) {
      std::string reason = (ec == std::errc::result_out_of_range) ? "out_of_range" : "parse_error";
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_warning")
          .Field("type", "invalid_integer_filter")
          .Field("reason", reason)
          .Field("value", filter.value)
          .Field("column_name", filter.name)
          .Warn();
      return false;  // Fail-closed: reject document on invalid filter
    }

    return mygram::utils::CompareValues(val, target, filter.op);

  } else if (std::holds_alternative<double>(value)) {
    // Float comparison
    double val = std::get<double>(value);
    double target = 0.0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) - Required for from_chars range
    auto [ptr, ec] = std::from_chars(filter.value.data(), filter.value.data() + filter.value.size(), target);
    if (ec != std::errc() || ptr != filter.value.data() + filter.value.size()) {
      std::string reason = (ec == std::errc::result_out_of_range) ? "out_of_range" : "parse_error";
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_warning")
          .Field("type", "invalid_float_filter")
          .Field("reason", reason)
          .Field("value", filter.value)
          .Field("column_name", filter.name)
          .Warn();
      return false;  // Fail-closed: reject document on invalid filter
    }

    return mygram::utils::CompareDoubleValues(val, target, filter.op, mygram::constants::kFilterValueEpsilon);

  } else if (std::holds_alternative<std::string>(value)) {
    // String comparison
    const auto& val = std::get<std::string>(value);
    const std::string& target = filter.value;

    return mygram::utils::CompareValues(val, target, filter.op);

  } else if (std::holds_alternative<uint64_t>(value)) {
    // Datetime/timestamp (stored as uint64_t epoch)
    uint64_t val = std::get<uint64_t>(value);

    // Parse target value: support both epoch seconds and ISO8601 format
    auto target_opt = mygram::utils::ParseDatetimeValue(filter.value, datetime_timezone);
    if (!target_opt) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_warning")
          .Field("type", "invalid_datetime_filter")
          .Field("reason", "parse_error")
          .Field("value", filter.value)
          .Field("column_name", filter.name)
          .Field("timezone", datetime_timezone)
          .Warn();
      return false;  // Fail-closed: reject document on invalid filter
    }
    uint64_t target = *target_opt;

    // Perform comparison
    return mygram::utils::CompareValues(val, target, filter.op);

  } else if (std::holds_alternative<storage::TimeValue>(value)) {
    // TIME comparison (stored as TimeValue with seconds)
    storage::TimeValue val = std::get<storage::TimeValue>(value);

    // Parse target value: support both seconds (numeric) and HH:MM:SS format
    int64_t target = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) - Required for from_chars range
    auto [ptr_t, ec_t] = std::from_chars(filter.value.data(), filter.value.data() + filter.value.size(), target);
    if (ec_t != std::errc() || ptr_t != filter.value.data() + filter.value.size()) {
      // Not a pure integer, try HH:MM:SS format using DateTimeProcessor
      // TimeToSeconds is a static method that doesn't require timezone
      auto seconds_result = mygram::utils::DateTimeProcessor::TimeToSeconds(filter.value);
      if (seconds_result) {
        target = *seconds_result;
      } else {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_warning")
            .Field("type", "invalid_time_filter")
            .Field("reason", "unsupported_format")
            .Field("value", filter.value)
            .Field("column_name", filter.name)
            .Warn();
        return false;  // Fail-closed: reject document on invalid filter
      }
    }

    // Perform comparison
    return mygram::utils::CompareValues(val.seconds, target, filter.op);

  } else if (std::holds_alternative<int32_t>(value) || std::holds_alternative<uint32_t>(value) ||
             std::holds_alternative<int16_t>(value) || std::holds_alternative<uint16_t>(value) ||
             std::holds_alternative<int8_t>(value) || std::holds_alternative<uint8_t>(value)) {
    // Handle other integer types that might not be int64_t
    // Convert to int64_t for comparison
    int64_t val = 0;
    if (std::holds_alternative<int32_t>(value)) {
      val = std::get<int32_t>(value);
    } else if (std::holds_alternative<uint32_t>(value)) {
      val = static_cast<int64_t>(std::get<uint32_t>(value));
    } else if (std::holds_alternative<int16_t>(value)) {
      val = std::get<int16_t>(value);
    } else if (std::holds_alternative<uint16_t>(value)) {
      val = std::get<uint16_t>(value);
    } else if (std::holds_alternative<int8_t>(value)) {
      val = std::get<int8_t>(value);
    } else if (std::holds_alternative<uint8_t>(value)) {
      val = std::get<uint8_t>(value);
    }

    int64_t target = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) - Required for from_chars range
    auto [ptr_i, ec_i] = std::from_chars(filter.value.data(), filter.value.data() + filter.value.size(), target);
    if (ec_i != std::errc() || ptr_i != filter.value.data() + filter.value.size()) {
      std::string reason = (ec_i == std::errc::result_out_of_range) ? "out_of_range" : "parse_error";
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_warning")
          .Field("type", "invalid_integer_filter")
          .Field("reason", reason)
          .Field("value", filter.value)
          .Field("column_name", filter.name)
          .Warn();
      return false;  // Fail-closed: reject document on invalid filter
    }

    return mygram::utils::CompareValues(val, target, filter.op);
  }

  mygram::utils::StructuredLog()
      .Event("mysql_binlog_warning")
      .Field("type", "unsupported_filter_value_type")
      .Field("column_name", filter.name)
      .Warn();
  return false;
}

storage::FilterMap BinlogFilterEvaluator::ExtractAllFilters(const RowData& row_data,
                                                            const config::TableConfig& table_config,
                                                            const std::string& datetime_timezone) {
  storage::FilterMap all_filters;

  // Convert required_filters to FilterConfig format for extraction
  auto required_as_filters = config::ToFilterConfigs(table_config.required_filters);

  // Extract required_filters columns
  auto required_filters = ExtractFilters(row_data, required_as_filters, datetime_timezone);
  all_filters.insert(required_filters.begin(), required_filters.end());

  // Extract optional filters columns
  auto optional_filters = ExtractFilters(row_data, table_config.filters, datetime_timezone);
  all_filters.insert(optional_filters.begin(), optional_filters.end());

  return all_filters;
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-avoid-*,readability-magic-numbers)

#endif  // USE_MYSQL
