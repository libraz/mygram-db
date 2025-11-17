/**
 * @file binlog_filter_evaluator.cpp
 * @brief Binlog filter evaluation implementation
 */

#include "mysql/binlog_filter_evaluator.h"

#ifdef USE_MYSQL

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cmath>

#include "mysql/rows_parser.h"
#include "utils/structured_log.h"

// NOLINTBEGIN(cppcoreguidelines-avoid-*,readability-magic-numbers)

namespace mygramdb::mysql {

bool BinlogFilterEvaluator::EvaluateRequiredFilters(
    const std::unordered_map<std::string, storage::FilterValue>& filters, const config::TableConfig& table_config) {
  // If no required_filters, all data is accepted
  if (table_config.required_filters.empty()) {
    return true;
  }

  // Check each required filter condition
  return std::all_of(table_config.required_filters.begin(), table_config.required_filters.end(),
                     [&filters](const auto& required_filter) {
                       auto filter_iter = filters.find(required_filter.name);
                       if (filter_iter == filters.end()) {
                         mygram::utils::StructuredLog()
                             .Event("mysql_binlog_warning")
                             .Field("type", "required_filter_column_not_found")
                             .Field("column_name", required_filter.name)
                             .Warn();
                         return false;
                       }
                       return CompareFilterValue(filter_iter->second, required_filter);
                     });
}

bool BinlogFilterEvaluator::CompareFilterValue(const storage::FilterValue& value,
                                               const config::RequiredFilterConfig& filter) {
  // SECURITY: Limit filter value size to prevent memory exhaustion attacks
  // A malicious binlog event could contain multi-GB filter values
  constexpr size_t MAX_FILTER_VALUE_SIZE = 1024 * 1024;  // 1MB
  if (filter.value.size() > MAX_FILTER_VALUE_SIZE) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_warning")
        .Field("type", "filter_value_too_large")
        .Field("value_size", static_cast<uint64_t>(filter.value.size()))
        .Field("max_size", static_cast<uint64_t>(MAX_FILTER_VALUE_SIZE))
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
    try {
      size_t pos = 0;
      target = std::stoll(filter.value, &pos);
      // SECURITY: Validate that entire string was consumed (no trailing garbage)
      if (pos != filter.value.length()) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_warning")
            .Field("type", "invalid_integer_filter")
            .Field("reason", "trailing_characters")
            .Field("value", filter.value)
            .Field("column_name", filter.name)
            .Warn();
        return false;  // Fail-closed: reject document on invalid filter
      }
    } catch (const std::invalid_argument& e) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_warning")
          .Field("type", "invalid_integer_filter")
          .Field("reason", "parse_error")
          .Field("value", filter.value)
          .Field("column_name", filter.name)
          .Warn();
      return false;  // Fail-closed: reject document on invalid filter
    } catch (const std::out_of_range& e) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_warning")
          .Field("type", "invalid_integer_filter")
          .Field("reason", "out_of_range")
          .Field("value", filter.value)
          .Field("column_name", filter.name)
          .Warn();
      return false;  // Fail-closed: reject document on invalid filter
    }

    if (filter.op == "=") {
      return val == target;
    }
    if (filter.op == "!=") {
      return val != target;
    }
    if (filter.op == "<") {
      return val < target;
    }
    if (filter.op == ">") {
      return val > target;
    }
    if (filter.op == "<=") {
      return val <= target;
    }
    if (filter.op == ">=") {
      return val >= target;
    }

  } else if (std::holds_alternative<double>(value)) {
    // Float comparison
    double val = std::get<double>(value);
    double target = 0.0;
    try {
      size_t pos = 0;
      target = std::stod(filter.value, &pos);
      // SECURITY: Validate that entire string was consumed (no trailing garbage)
      if (pos != filter.value.length()) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_warning")
            .Field("type", "invalid_float_filter")
            .Field("reason", "trailing_characters")
            .Field("value", filter.value)
            .Field("column_name", filter.name)
            .Warn();
        return false;  // Fail-closed: reject document on invalid filter
      }
    } catch (const std::invalid_argument& e) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_warning")
          .Field("type", "invalid_float_filter")
          .Field("reason", "parse_error")
          .Field("value", filter.value)
          .Field("column_name", filter.name)
          .Warn();
      return false;  // Fail-closed: reject document on invalid filter
    } catch (const std::out_of_range& e) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_warning")
          .Field("type", "invalid_float_filter")
          .Field("reason", "out_of_range")
          .Field("value", filter.value)
          .Field("column_name", filter.name)
          .Warn();
      return false;  // Fail-closed: reject document on invalid filter
    }

    if (filter.op == "=") {
      return std::abs(val - target) < 1e-9;
    }
    if (filter.op == "!=") {
      return std::abs(val - target) >= 1e-9;
    }
    if (filter.op == "<") {
      return val < target;
    }
    if (filter.op == ">") {
      return val > target;
    }
    if (filter.op == "<=") {
      return val <= target;
    }
    if (filter.op == ">=") {
      return val >= target;
    }

  } else if (std::holds_alternative<std::string>(value)) {
    // String comparison
    const auto& val = std::get<std::string>(value);
    const std::string& target = filter.value;

    if (filter.op == "=") {
      return val == target;
    }
    if (filter.op == "!=") {
      return val != target;
    }
    if (filter.op == "<") {
      return val < target;
    }
    if (filter.op == ">") {
      return val > target;
    }
    if (filter.op == "<=") {
      return val <= target;
    }
    if (filter.op == ">=") {
      return val >= target;
    }

  } else if (std::holds_alternative<uint64_t>(value)) {
    // Datetime/timestamp (stored as uint64_t epoch)
    uint64_t val = std::get<uint64_t>(value);

    // For datetime comparison, we need to parse target value
    // For now, assume target is numeric (epoch timestamp)
    // TODO: Add proper datetime parsing if needed
    uint64_t target = 0;
    try {
      size_t pos = 0;
      target = std::stoull(filter.value, &pos);
      // SECURITY: Validate that entire string was consumed (no trailing garbage)
      if (pos != filter.value.length()) {
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_warning")
            .Field("type", "invalid_unsigned_integer_filter")
            .Field("reason", "trailing_characters")
            .Field("value", filter.value)
            .Field("column_name", filter.name)
            .Warn();
        return false;  // Fail-closed: reject document on invalid filter
      }
    } catch (const std::invalid_argument& e) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_warning")
          .Field("type", "invalid_unsigned_integer_filter")
          .Field("reason", "parse_error")
          .Field("value", filter.value)
          .Field("column_name", filter.name)
          .Warn();
      return false;  // Fail-closed: reject document on invalid filter
    } catch (const std::out_of_range& e) {
      mygram::utils::StructuredLog()
          .Event("mysql_binlog_warning")
          .Field("type", "invalid_unsigned_integer_filter")
          .Field("reason", "out_of_range")
          .Field("value", filter.value)
          .Field("column_name", filter.name)
          .Warn();
      return false;  // Fail-closed: reject document on invalid filter
    }

    if (filter.op == "=") {
      return val == target;
    }
    if (filter.op == "!=") {
      return val != target;
    }
    if (filter.op == "<") {
      return val < target;
    }
    if (filter.op == ">") {
      return val > target;
    }
    if (filter.op == "<=") {
      return val <= target;
    }
    if (filter.op == ">=") {
      return val >= target;
    }
  }

  mygram::utils::StructuredLog().Event("mysql_binlog_warning").Field("type", "unsupported_filter_value_type").Warn();
  return false;
}

std::unordered_map<std::string, storage::FilterValue> BinlogFilterEvaluator::ExtractAllFilters(
    const RowData& row_data, const config::TableConfig& table_config) {
  std::unordered_map<std::string, storage::FilterValue> all_filters;

  // Convert required_filters to FilterConfig format for extraction
  std::vector<config::FilterConfig> required_as_filters;
  for (const auto& req_filter : table_config.required_filters) {
    config::FilterConfig filter_config;
    filter_config.name = req_filter.name;
    filter_config.type = req_filter.type;
    filter_config.dict_compress = false;
    filter_config.bitmap_index = req_filter.bitmap_index;
    required_as_filters.push_back(filter_config);
  }

  // Extract required_filters columns
  auto required_filters = ExtractFilters(row_data, required_as_filters);
  all_filters.insert(required_filters.begin(), required_filters.end());

  // Extract optional filters columns
  auto optional_filters = ExtractFilters(row_data, table_config.filters);
  all_filters.insert(optional_filters.begin(), optional_filters.end());

  return all_filters;
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-avoid-*,readability-magic-numbers)

#endif  // USE_MYSQL
