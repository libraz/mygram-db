/**
 * @file query_parser.cpp
 * @brief Query parser implementation
 */

#include "query/query_parser.h"
#include <spdlog/spdlog.h>
#include <sstream>
#include <algorithm>
#include <cctype>

namespace mygramdb {
namespace query {

namespace {

// Maximum LIMIT value (1000)
constexpr uint32_t MAX_LIMIT = 1000;

/**
 * @brief Convert string to uppercase
 */
std::string ToUpper(const std::string& str) {
  std::string result = str;
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  return result;
}

}  // namespace

bool Query::IsValid() const {
  if (type == QueryType::UNKNOWN) {
    return false;
  }

  // INFO, SAVE, LOAD, REPLICATION_* commands don't require a table
  if (type != QueryType::INFO && type != QueryType::SAVE &&
      type != QueryType::LOAD &&
      type != QueryType::REPLICATION_STATUS &&
      type != QueryType::REPLICATION_STOP &&
      type != QueryType::REPLICATION_START &&
      table.empty()) {
    return false;
  }

  if (type == QueryType::SEARCH || type == QueryType::COUNT) {
    if (search_text.empty()) {
      return false;
    }
  }

  if (type == QueryType::GET) {
    if (primary_key.empty()) {
      return false;
    }
  }

  if (type == QueryType::SEARCH) {
    if (limit == 0 || limit > MAX_LIMIT) {
      return false;
    }
  }

  return true;
}

Query QueryParser::Parse(const std::string& query_str) {
  error_.clear();

  auto tokens = Tokenize(query_str);
  if (tokens.empty()) {
    SetError("Empty query");
    return Query{};
  }

  std::string command = ToUpper(tokens[0]);

  if (command == "SEARCH") {
    return ParseSearch(tokens);
  } else if (command == "COUNT") {
    return ParseCount(tokens);
  } else if (command == "GET") {
    return ParseGet(tokens);
  } else if (command == "INFO") {
    Query query;
    query.type = QueryType::INFO;
    query.table = ""; // INFO doesn't need a table
    return query;
  } else if (command == "SAVE") {
    Query query;
    query.type = QueryType::SAVE;
    query.table = ""; // SAVE doesn't need a table
    // Optional filepath argument
    if (tokens.size() > 1) {
      query.filepath = tokens[1];
    }
    return query;
  } else if (command == "LOAD") {
    Query query;
    query.type = QueryType::LOAD;
    query.table = ""; // LOAD doesn't need a table
    // Optional filepath argument
    if (tokens.size() > 1) {
      query.filepath = tokens[1];
    }
    return query;
  } else if (command == "REPLICATION") {
    // REPLICATION STATUS | STOP | START
    if (tokens.size() < 2) {
      SetError("REPLICATION requires a subcommand (STATUS, STOP, START)");
      return Query{};
    }

    std::string subcommand = ToUpper(tokens[1]);
    Query query;
    query.table = ""; // REPLICATION doesn't need a table

    if (subcommand == "STATUS") {
      query.type = QueryType::REPLICATION_STATUS;
    } else if (subcommand == "STOP") {
      query.type = QueryType::REPLICATION_STOP;
    } else if (subcommand == "START") {
      query.type = QueryType::REPLICATION_START;
    } else {
      SetError("Unknown REPLICATION subcommand: " + subcommand);
      return Query{};
    }

    return query;
  } else {
    SetError("Unknown command: " + command);
    return Query{};
  }
}

Query QueryParser::ParseSearch(const std::vector<std::string>& tokens) {
  Query query;
  query.type = QueryType::SEARCH;

  // SEARCH <table> <text> [FILTER ...] [LIMIT n] [OFFSET n]
  if (tokens.size() < 3) {
    SetError("SEARCH requires at least table and search text");
    return query;
  }

  query.table = tokens[1];
  query.search_text = tokens[2];

  // Parse optional clauses
  size_t pos = 3;
  while (pos < tokens.size()) {
    std::string keyword = ToUpper(tokens[pos]);

    if (keyword == "NOT") {
      if (!ParseNot(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return query;
      }
    } else if (keyword == "FILTER") {
      if (!ParseFilters(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return query;
      }
    } else if (keyword == "LIMIT") {
      if (!ParseLimit(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return query;
      }
    } else if (keyword == "OFFSET") {
      if (!ParseOffset(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return query;
      }
    } else {
      SetError("Unknown keyword: " + keyword);
      query.type = QueryType::UNKNOWN;
      return query;
    }
  }

  // Validate limit
  if (query.limit > MAX_LIMIT) {
    SetError("LIMIT exceeds maximum of " + std::to_string(MAX_LIMIT));
    query.type = QueryType::UNKNOWN;
    return query;
  }

  return query;
}

Query QueryParser::ParseCount(const std::vector<std::string>& tokens) {
  Query query;
  query.type = QueryType::COUNT;

  // COUNT <table> <text> [FILTER ...]
  if (tokens.size() < 3) {
    SetError("COUNT requires at least table and search text");
    return query;
  }

  query.table = tokens[1];
  query.search_text = tokens[2];

  // Parse optional clauses
  size_t pos = 3;
  while (pos < tokens.size()) {
    std::string keyword = ToUpper(tokens[pos]);

    if (keyword == "NOT") {
      if (!ParseNot(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return query;
      }
    } else if (keyword == "FILTER") {
      if (!ParseFilters(tokens, pos, query)) {
        query.type = QueryType::UNKNOWN;
        return query;
      }
    } else {
      SetError("COUNT only supports NOT and FILTER clauses");
      query.type = QueryType::UNKNOWN;
      return query;
    }
  }

  return query;
}

Query QueryParser::ParseGet(const std::vector<std::string>& tokens) {
  Query query;
  query.type = QueryType::GET;

  // GET <table> <primary_key>
  if (tokens.size() != 3) {
    SetError("GET requires table and primary_key");
    return query;
  }

  query.table = tokens[1];
  query.primary_key = tokens[2];

  return query;
}

bool QueryParser::ParseNot(const std::vector<std::string>& tokens,
                           size_t& pos, Query& query) {
  // NOT <term>
  pos++; // Skip "NOT"

  if (pos >= tokens.size()) {
    SetError("NOT requires a term");
    return false;
  }

  query.not_terms.push_back(tokens[pos++]);
  return true;
}

bool QueryParser::ParseFilters(const std::vector<std::string>& tokens,
                               size_t& pos, Query& query) {
  // FILTER <col> <op> <value>
  pos++; // Skip "FILTER"

  if (pos + 2 >= tokens.size()) {
    SetError("FILTER requires column, operator, and value");
    return false;
  }

  FilterCondition filter;
  filter.column = tokens[pos++];

  auto op = ParseFilterOp(tokens[pos++]);
  if (!op.has_value()) {
    SetError("Invalid filter operator: " + tokens[pos - 1]);
    return false;
  }
  filter.op = op.value();

  filter.value = tokens[pos++];

  query.filters.push_back(filter);
  return true;
}

bool QueryParser::ParseLimit(const std::vector<std::string>& tokens,
                             size_t& pos, Query& query) {
  // LIMIT <n>
  pos++; // Skip "LIMIT"

  if (pos >= tokens.size()) {
    SetError("LIMIT requires a number");
    return false;
  }

  try {
    int limit = std::stoi(tokens[pos++]);
    if (limit <= 0) {
      SetError("LIMIT must be positive");
      return false;
    }
    query.limit = static_cast<uint32_t>(limit);
  } catch (const std::exception& e) {
    SetError("Invalid LIMIT value: " + tokens[pos - 1]);
    return false;
  }

  return true;
}

bool QueryParser::ParseOffset(const std::vector<std::string>& tokens,
                              size_t& pos, Query& query) {
  // OFFSET <n>
  pos++; // Skip "OFFSET"

  if (pos >= tokens.size()) {
    SetError("OFFSET requires a number");
    return false;
  }

  try {
    int offset = std::stoi(tokens[pos++]);
    if (offset < 0) {
      SetError("OFFSET must be non-negative");
      return false;
    }
    query.offset = static_cast<uint32_t>(offset);
  } catch (const std::exception& e) {
    SetError("Invalid OFFSET value: " + tokens[pos - 1]);
    return false;
  }

  return true;
}

std::vector<std::string> QueryParser::Tokenize(const std::string& str) {
  std::vector<std::string> tokens;
  std::istringstream iss(str);
  std::string token;

  while (iss >> token) {
    tokens.push_back(token);
  }

  return tokens;
}

std::optional<FilterOp> QueryParser::ParseFilterOp(const std::string& op_str) {
  std::string op = ToUpper(op_str);

  if (op == "=" || op == "EQ") return FilterOp::EQ;
  if (op == "!=" || op == "NE") return FilterOp::NE;
  if (op == ">" || op == "GT") return FilterOp::GT;
  if (op == ">=" || op == "GTE") return FilterOp::GTE;
  if (op == "<" || op == "LT") return FilterOp::LT;
  if (op == "<=" || op == "LTE") return FilterOp::LTE;

  return std::nullopt;
}

}  // namespace query
}  // namespace mygramdb
