/**
 * @file variable_handler.cpp
 * @brief Variable handler implementation
 */

#include "server/handlers/variable_handler.h"

#include <algorithm>
#include <iomanip>
#include <sstream>

#include "config/runtime_variable_manager.h"
#include "server/response_formatter.h"
#include "server/sync_operation_manager.h"
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

  return ResponseFormatter::FormatError("Unknown variable command");
}

std::string VariableHandler::HandleSet(const Query& query) {
  if (ctx_.variable_manager == nullptr) {
    return ResponseFormatter::FormatError("Runtime variable manager not initialized");
  }

#ifdef USE_MYSQL
  // Check if trying to change MySQL connection settings during SYNC
  for (const auto& [variable_name, value] : query.variable_assignments) {
    if (variable_name == "mysql.host" || variable_name == "mysql.port") {
      if (ctx_.sync_manager != nullptr && ctx_.sync_manager->IsAnySyncing()) {
        return ResponseFormatter::FormatError("Cannot change '" + variable_name +
                                              "' while SYNC is in progress. "
                                              "Please wait for SYNC to complete.");
      }
    }
  }
#endif

  // Apply each variable assignment
  for (const auto& [variable_name, value] : query.variable_assignments) {
    auto result = ctx_.variable_manager->SetVariable(variable_name, value);
    if (!result) {
      // Return error for the first failed assignment
      return ResponseFormatter::FormatError("Failed to set variable '" + variable_name +
                                            "': " + result.error().message());
    }
  }

  // Success
  if (query.variable_assignments.size() == 1) {
    const auto& [variable_name, value] = query.variable_assignments[0];
    std::ostringstream body;
    body << "Variable '" << variable_name << "' set to '" << value << "'";
    return ResponseFormatter::FormatOk(body.str()) + "\r\n";
  }

  std::ostringstream body;
  body << query.variable_assignments.size() << " variables set";
  return ResponseFormatter::FormatOk(body.str()) + "\r\n";
}

std::string VariableHandler::HandleShowVariables(const Query& query) {
  if (ctx_.variable_manager == nullptr) {
    return ResponseFormatter::FormatError("Runtime variable manager not initialized");
  }

  // Get all variables (optionally filtered by LIKE pattern prefix).
  // Extract the literal prefix up to the first wildcard character ('%' or '_')
  // so that GetAllVariables can narrow the candidate set before the full
  // LIKE match is applied.
  std::string prefix;
  if (!query.variable_like_pattern.empty()) {
    auto first_wild = query.variable_like_pattern.find_first_of("%_");
    if (first_wild == std::string::npos) {
      prefix = query.variable_like_pattern;  // No wildcards: exact match
    } else {
      prefix = query.variable_like_pattern.substr(0, first_wild);
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
    return ResponseFormatter::FormatOk("0 rows") + "\r\n";
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
  // Two-pointer LIKE pattern matching (O(n*m) worst-case, no allocations).
  // Supports: % (any sequence of characters), _ (single character).
  // MySQL LIKE is case-insensitive by default.
  size_t v = 0;
  size_t p = 0;
  size_t star = std::string::npos;
  size_t match = 0;

  while (v < value.size()) {
    if (p < pattern.size() && (pattern[p] == '_' || std::tolower(static_cast<unsigned char>(pattern[p])) ==
                                                        std::tolower(static_cast<unsigned char>(value[v])))) {
      ++v;
      ++p;
    } else if (p < pattern.size() && pattern[p] == '%') {
      star = p;
      ++p;
      match = v;
    } else if (star != std::string::npos) {
      p = star + 1;
      ++match;
      v = match;
    } else {
      return false;
    }
  }

  // Consume trailing '%' wildcards in the pattern.
  while (p < pattern.size() && pattern[p] == '%') {
    ++p;
  }
  return p == pattern.size();
}

}  // namespace mygramdb::server
