/**
 * @file variable_handler.cpp
 * @brief Variable handler implementation
 */

#include "server/handlers/variable_handler.h"

#include <algorithm>
#include <iomanip>
#include <sstream>

#include "config/runtime_variable_manager.h"
#include "utils/structured_log.h"

namespace mygramdb::server {

using query::Query;
using query::QueryType;

namespace {
// Table column widths for formatting
constexpr size_t kMinNameColumnWidth = 20;
constexpr size_t kMinValueColumnWidth = 15;
constexpr size_t kMutableColumnWidth = 7;  // "Mutable" or "YES"/"NO"
constexpr size_t kColumnPadding = 2;       // Spaces before and after content
constexpr size_t kBorderWidth = 9;         // Width of "Mutable" column with borders
}  // namespace

std::string VariableHandler::Handle(const Query& query, ConnectionContext& /*conn_ctx*/) {
  if (query.type == QueryType::SET) {
    return HandleSet(query);
  }
  if (query.type == QueryType::SHOW_VARIABLES) {
    return HandleShowVariables(query);
  }

  return "-ERR Unknown variable command\r\n";
}

std::string VariableHandler::HandleSet(const Query& query) {
  if (ctx_.variable_manager == nullptr) {
    return "-ERR Runtime variable manager not initialized\r\n";
  }

  // Apply each variable assignment
  for (const auto& [variable_name, value] : query.variable_assignments) {
    auto result = ctx_.variable_manager->SetVariable(variable_name, value);
    if (!result) {
      // Return error for the first failed assignment
      std::ostringstream oss;
      oss << "-ERR Failed to set variable '" << variable_name << "': " << result.error().message() << "\r\n";
      return oss.str();
    }
  }

  // Success
  if (query.variable_assignments.size() == 1) {
    const auto& [variable_name, value] = query.variable_assignments[0];
    std::ostringstream oss;
    oss << "+OK Variable '" << variable_name << "' set to '" << value << "'\r\n";
    return oss.str();
  }

  std::ostringstream oss;
  oss << "+OK " << query.variable_assignments.size() << " variables set\r\n";
  return oss.str();
}

std::string VariableHandler::HandleShowVariables(const Query& query) {
  if (ctx_.variable_manager == nullptr) {
    return "-ERR Runtime variable manager not initialized\r\n";
  }

  // Get all variables (optionally filtered by LIKE pattern prefix)
  std::string prefix;
  if (!query.variable_like_pattern.empty()) {
    // Extract prefix from LIKE pattern (e.g., "logging%" -> "logging")
    // Note: This is a simplified implementation that only supports prefix patterns
    std::string pattern = query.variable_like_pattern;
    if (pattern.back() == '%') {
      prefix = pattern.substr(0, pattern.size() - 1);
    } else {
      prefix = pattern;  // Exact match
    }
  }

  auto variables = ctx_.variable_manager->GetAllVariables(prefix);

  // Apply full LIKE pattern matching if specified
  if (!query.variable_like_pattern.empty()) {
    std::map<std::string, config::VariableInfo> filtered;
    for (const auto& [name, info] : variables) {
      if (MatchLikePattern(name, query.variable_like_pattern)) {
        filtered[name] = info;
      }
    }
    variables = std::move(filtered);
  }

  // Format as MySQL table
  return FormatVariablesTable(variables);
}

std::string VariableHandler::FormatVariablesTable(const std::map<std::string, config::VariableInfo>& variables) {
  if (variables.empty()) {
    return "+OK 0 rows\r\n";
  }

  // Calculate column widths
  size_t max_name_width = std::string("Variable_name").size();
  size_t max_value_width = std::string("Value").size();

  for (const auto& [name, info] : variables) {
    max_name_width = std::max(max_name_width, name.size());
    max_value_width = std::max(max_value_width, info.value.size());
  }

  // Ensure minimum width for readability
  max_name_width = std::max(max_name_width, kMinNameColumnWidth);
  max_value_width = std::max(max_value_width, kMinValueColumnWidth);

  std::ostringstream oss;

  // Top border
  oss << "+";
  oss << std::string(max_name_width + kColumnPadding, '-') << "+";
  oss << std::string(max_value_width + kColumnPadding, '-') << "+";
  oss << std::string(kBorderWidth, '-') << "+\r\n";

  // Header
  oss << "| " << std::left << std::setw(static_cast<int>(max_name_width)) << "Variable_name" << " | ";
  oss << std::left << std::setw(static_cast<int>(max_value_width)) << "Value" << " | ";
  oss << std::left << std::setw(static_cast<int>(kMutableColumnWidth)) << "Mutable" << " |\r\n";

  // Header separator
  oss << "+";
  oss << std::string(max_name_width + kColumnPadding, '-') << "+";
  oss << std::string(max_value_width + kColumnPadding, '-') << "+";
  oss << std::string(kBorderWidth, '-') << "+\r\n";

  // Rows
  for (const auto& [name, info] : variables) {
    oss << "| " << std::left << std::setw(static_cast<int>(max_name_width)) << name << " | ";
    oss << std::left << std::setw(static_cast<int>(max_value_width)) << info.value << " | ";
    oss << std::left << std::setw(static_cast<int>(kMutableColumnWidth)) << (info.mutable_ ? "YES" : "NO") << " |\r\n";
  }

  // Bottom border
  oss << "+";
  oss << std::string(max_name_width + kColumnPadding, '-') << "+";
  oss << std::string(max_value_width + kColumnPadding, '-') << "+";
  oss << std::string(kBorderWidth, '-') << "+\r\n";

  // Footer
  oss << variables.size() << " row" << (variables.size() > 1 ? "s" : "") << " in set\r\n";

  return oss.str();
}

bool VariableHandler::MatchLikePattern(const std::string& value, const std::string& pattern) {
  // Simple LIKE pattern matching
  // Supports: % (any characters), _ (single character)
  // MySQL LIKE is case-insensitive by default

  size_t value_pos = 0;
  size_t pattern_pos = 0;

  while (pattern_pos < pattern.size()) {
    if (pattern[pattern_pos] == '%') {
      // % matches any sequence of characters (including empty)
      pattern_pos++;
      if (pattern_pos == pattern.size()) {
        return true;  // % at end matches rest
      }

      // Try to match rest of pattern at different positions
      while (value_pos <= value.size()) {
        if (MatchLikePattern(value.substr(value_pos), pattern.substr(pattern_pos))) {
          return true;
        }
        value_pos++;
      }
      return false;
    }

    if (pattern[pattern_pos] == '_') {
      // _ matches exactly one character
      if (value_pos >= value.size()) {
        return false;
      }
      value_pos++;
      pattern_pos++;
      continue;
    }

    // Regular character - must match exactly (case-insensitive)
    if (value_pos >= value.size()) {
      return false;
    }
    if (std::tolower(value[value_pos]) != std::tolower(pattern[pattern_pos])) {
      return false;
    }
    value_pos++;
    pattern_pos++;
  }

  // Pattern consumed - value must also be consumed
  return value_pos == value.size();
}

}  // namespace mygramdb::server
