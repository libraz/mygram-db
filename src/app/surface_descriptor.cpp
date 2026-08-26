/**
 * @file surface_descriptor.cpp
 * @brief Implementation of the static external surface rendering
 */

#include "app/surface_descriptor.h"

#include <algorithm>
#include <array>
#include <cstdint>
#include <locale>
#include <map>
#include <nlohmann/json.hpp>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "config/config.h"
#include "config/config_schema_embedded.h"
#include "config/runtime_variable_manager.h"
#include "index/index_format.h"
#include "query/query_parser.h"
#include "query/query_parser_internal.h"
#include "server/http_server.h"
#include "server/protocol_constants.h"
#include "server/reactor_connection.h"
#include "server/request_dispatcher.h"
#include "server/server_types.h"
#include "storage/dump_format.h"
#include "utils/constants.h"
#include "utils/error.h"

namespace mygramdb::app {

namespace {

using query::QueryType;

/// Number of QueryType values, kept in lockstep with the enum by the
/// static_assert below. UNKNOWN is the final member by construction.
constexpr size_t kQueryTypeCount = static_cast<size_t>(QueryType::UNKNOWN) + 1;

/**
 * @brief Where a command's execution lives.
 */
enum class HandlerBinding : uint8_t {
  kRegistered,        ///< Routed to a handler registered on RequestDispatcher.
  kDispatcherInline,  ///< Executed by RequestDispatcher itself.
  kNone,              ///< No execution path; the parser never yields this type.
};

/// Sentinel for commands that accept any number of trailing tokens.
constexpr int kUnboundedArgs = -1;

/**
 * @brief Static description of one text-protocol command.
 *
 * Argument counts exclude the command text itself. A max of kUnboundedArgs
 * means the parser accepts (and ignores) further tokens.
 */
struct CommandDescriptor {
  QueryType type;
  std::string_view text;
  int min_args;
  int max_args;
  HandlerBinding binding;
};

// Indexed by QueryType so a new enum member that is not described here shifts
// every following entry and trips the static_assert on the table size.
constexpr std::array<CommandDescriptor, kQueryTypeCount> kCommands = {{
    {QueryType::AUTH, "AUTH", 1, 1, HandlerBinding::kDispatcherInline},
    {QueryType::SEARCH, "SEARCH", 2, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::COUNT, "COUNT", 2, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::GET, "GET", 2, 2, HandlerBinding::kRegistered},
    {QueryType::INFO, "INFO", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::DUMP_SAVE, "DUMP SAVE", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::DUMP_LOAD, "DUMP LOAD", 1, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::DUMP_VERIFY, "DUMP VERIFY", 1, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::DUMP_INFO, "DUMP INFO", 1, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::DUMP_STATUS, "DUMP STATUS", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::SAVE, "SAVE", 0, 0, HandlerBinding::kNone},
    {QueryType::LOAD, "LOAD", 0, 0, HandlerBinding::kNone},
    {QueryType::REPLICATION_STATUS, "REPLICATION STATUS", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::REPLICATION_STOP, "REPLICATION STOP", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::REPLICATION_START, "REPLICATION START", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::SYNC, "SYNC", 1, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::SYNC_STATUS, "SYNC STATUS", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::SYNC_STOP, "SYNC STOP", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::CONFIG_HELP, "CONFIG HELP", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::CONFIG_SHOW, "CONFIG SHOW", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::CONFIG_VERIFY, "CONFIG VERIFY", 1, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::OPTIMIZE, "OPTIMIZE", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::DEBUG_ON, "DEBUG ON", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::DEBUG_OFF, "DEBUG OFF", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::CACHE_CLEAR, "CACHE CLEAR", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::CACHE_STATS, "CACHE STATS", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::CACHE_ENABLE, "CACHE ENABLE", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::CACHE_DISABLE, "CACHE DISABLE", 0, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::SET, "SET", 1, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::SHOW_VARIABLES, "SHOW VARIABLES", 0, 2, HandlerBinding::kRegistered},
    {QueryType::FACET, "FACET", 2, kUnboundedArgs, HandlerBinding::kRegistered},
    {QueryType::UNKNOWN, "", 0, 0, HandlerBinding::kNone},
}};

static_assert(kCommands.size() == kQueryTypeCount, "Every QueryType must have a command descriptor");

/// Compile-time check that the table is ordered by enum value.
constexpr bool CommandTableIsIndexedByEnum() {
  for (size_t i = 0; i < kCommands.size(); ++i) {
    if (static_cast<size_t>(kCommands[i].type) != i) {
      return false;
    }
  }
  return true;
}

static_assert(CommandTableIsIndexedByEnum(), "kCommands must be indexed by QueryType value");

std::string_view BindingToString(HandlerBinding binding) {
  switch (binding) {
    case HandlerBinding::kRegistered:
      return "registered";
    case HandlerBinding::kDispatcherInline:
      return "dispatcher-inline";
    case HandlerBinding::kNone:
      return "none";
  }
  return "none";
}

std::string ArgCountToString(int count) {
  return count == kUnboundedArgs ? "unbounded" : std::to_string(count);
}

void AppendCommands(std::ostringstream& out) {
  out << "[commands]\n";
  for (const auto& command : kCommands) {
    // UNKNOWN is the parser's sentinel for input that named no command, so it
    // has no wire representation to describe. The table still carries a row for
    // it because the static_asserts above require one per enumerator.
    if (command.type == QueryType::UNKNOWN) {
      continue;
    }
    out << "text=\"" << command.text << "\""
        << " min_args=" << ArgCountToString(command.min_args) << " max_args=" << ArgCountToString(command.max_args)
        << " administrative=" << (server::IsAdministrativeCommand(command.type) ? "yes" : "no")
        << " handler=" << BindingToString(command.binding) << "\n";
  }
}

void AppendQueryLimits(std::ostringstream& out) {
  out << "[query-limits]\n";
  // Sorted by name so the section order never depends on declaration order.
  const std::map<std::string, uint64_t> limits = {
      {"http.default_max_body_bytes", server::defaults::kHttpDefaultMaxBodyBytes},
      {"http.default_read_timeout_sec", static_cast<uint64_t>(config::defaults::kHttpTimeoutSec)},
      {"http.default_write_timeout_sec", static_cast<uint64_t>(config::defaults::kHttpTimeoutSec)},
      {"query.default_limit", static_cast<uint64_t>(config::defaults::kDefaultLimit)},
      {"query.default_max_query_length", static_cast<uint64_t>(config::defaults::kDefaultQueryLengthLimit)},
      {"query.max_filter_column_name_length", query::QueryParser::kMaxFilterColumnNameLength},
      {"query.max_filter_value_length", query::QueryParser::kMaxFilterValueLength},
      {"query.max_highlight_tag_length", query::QueryParser::kMaxHighlightTagLength},
      {"query.max_limit", static_cast<uint64_t>(query::internal::kMaxLimit)},
      {"query.max_term_count", query::QueryParser::kMaxTermCount},
      {"query.min_limit", static_cast<uint64_t>(config::defaults::kMinLimit)},
      {"tcp.client_recv_buffer_bytes", server::protocol::kDefaultClientRecvBufferSize},
      {"tcp.default_max_connections", static_cast<uint64_t>(server::kDefaultMaxConnections)},
      {"tcp.default_max_write_queue_bytes", server::ReactorConnection::kDefaultMaxWriteQueueBytes},
      {"tcp.max_pending_frame_bytes", server::ReactorConnection::kMaxPendingFrameBytes},
      {"tcp.max_pending_frames", server::ReactorConnection::kMaxPendingFrames},
      {"tcp.max_request_bytes", server::ReactorConnection::kMaxReadBufferBytes},
  };
  for (const auto& [name, value] : limits) {
    out << name << " = " << value << "\n";
  }
}

void AppendHttpRoutes(std::ostringstream& out) {
  out << "[http-routes]\n";
  // Registration order is behavioural (cpp-httplib matches in order), so this
  // section deliberately preserves it instead of sorting.
  for (const auto& route : server::HttpServer::Routes()) {
    out << (route.method == server::HttpServer::RouteMethod::kGet ? "GET" : "POST") << " " << route.pattern
        << " auth=" << (route.requires_admin_token ? "admin-token" : "none") << "\n";
  }
}

void AppendErrorCodes(std::ostringstream& out) {
  // The defined set is derived by scanning the whole uint16 domain and keeping
  // every value whose rendering differs from ErrorCodeToString's fallback, so
  // a newly added code appears here without being restated.
  const std::string_view fallback = mygram::utils::ErrorCodeToString(static_cast<mygram::utils::ErrorCode>(UINT16_MAX));

  std::vector<std::pair<uint32_t, std::string_view>> defined;
  for (uint32_t value = 0; value <= UINT16_MAX; ++value) {
    const std::string_view message =
        mygram::utils::ErrorCodeToString(static_cast<mygram::utils::ErrorCode>(static_cast<uint16_t>(value)));
    if (message != fallback) {
      defined.emplace_back(value, message);
    }
  }

  out << "[error-codes]\n";
  for (const auto& [code, message] : defined) {
    out << code << " = " << message << "\n";
  }

  // Per-module totals, so a code added to the wrong range is visible without
  // reading the full list.
  constexpr std::array<std::pair<uint32_t, uint32_t>, 9> kModuleRanges = {{
      {0, 999},
      {1000, 1999},
      {2000, 2999},
      {3000, 3999},
      {4000, 4999},
      {5000, 5999},
      {6000, 6999},
      {7000, 7999},
      {8000, 8999},
  }};
  for (const auto& [low, high] : kModuleRanges) {
    const auto count = std::count_if(defined.begin(), defined.end(), [low = low, high = high](const auto& entry) {
      return entry.first >= low && entry.first <= high;
    });
    out << "count " << low << "-" << high << " = " << count << "\n";
  }
  out << "count total = " << defined.size() << "\n";
}

/// One rendered configuration key, keyed by its dotted path.
using ConfigKeyLines = std::map<std::string, std::string>;

std::string DescribeConstraint(const nlohmann::json& node, const char* keyword) {
  if (!node.contains(keyword)) {
    return {};
  }
  return std::string(" ") + keyword + "=" + node.at(keyword).dump();
}

void WalkSchemaProperties(const nlohmann::json& node, const std::string& prefix, ConfigKeyLines& lines);

void DescribeSchemaNode(const nlohmann::json& node, const std::string& path, ConfigKeyLines& lines) {
  std::string type = "any";
  if (node.contains("type")) {
    type = node.at("type").is_string() ? node.at("type").get<std::string>() : node.at("type").dump();
  }

  std::string line = "type=" + type;
  if (node.contains("default")) {
    line += " default=" + node.at("default").dump();
  }
  line += DescribeConstraint(node, "minimum");
  line += DescribeConstraint(node, "maximum");
  line += DescribeConstraint(node, "enum");
  line += std::string(" mutable=") + (config::RuntimeVariableManager::IsMutable(path) ? "yes" : "no");
  lines.emplace(path, line);

  if (node.contains("properties")) {
    WalkSchemaProperties(node, path, lines);
  }
  // Array element schemas are addressed with a "[]" path segment; only object
  // elements carry further named keys.
  if (node.contains("items") && node.at("items").is_object() && node.at("items").contains("properties")) {
    WalkSchemaProperties(node.at("items"), path + "[]", lines);
  }
}

void WalkSchemaProperties(const nlohmann::json& node, const std::string& prefix, ConfigKeyLines& lines) {
  for (const auto& [name, child] : node.at("properties").items()) {
    if (!child.is_object()) {
      continue;
    }
    DescribeSchemaNode(child, prefix.empty() ? name : prefix + "." + name, lines);
  }
}

void AppendConfigKeys(std::ostringstream& out) {
  out << "[config-keys]\n";
  const nlohmann::json schema = nlohmann::json::parse(config::kConfigSchemaJson, nullptr, false);
  if (schema.is_discarded() || !schema.contains("properties")) {
    return;
  }

  ConfigKeyLines lines;
  WalkSchemaProperties(schema, "", lines);
  for (const auto& [path, description] : lines) {
    out << path << " " << description << "\n";
  }
}

void AppendPersistence(std::ostringstream& out) {
  out << "[persistence]\n";
  out << "dump.magic = "
      << std::string(storage::dump_format::kMagicNumber.begin(), storage::dump_format::kMagicNumber.end()) << "\n";
  out << "dump.fixed_header_bytes = " << storage::dump_format::kFixedHeaderSize << "\n";
  out << "dump.section_envelope_bytes = " << storage::dump_format::kSectionEnvelopeSize << "\n";
  out << "dump.container_version_written = " << storage::dump_format::kCurrentVersion << "\n";
  out << "dump.container_version_accepted_min = " << storage::dump_format::kMinSupportedVersion << "\n";
  out << "dump.container_version_accepted_max = " << storage::dump_format::kMaxSupportedVersion << "\n";
  out << "dump.container_version_rejected_at_or_below = " << storage::dump_format::kMinSupportedVersion - 1 << "\n";
  out << "dump.container_version_rejected_at_or_above = " << storage::dump_format::kMaxSupportedVersion + 1 << "\n";
  out << "index.magic = " << index::format::kMagic << "\n";
  out << "index.crc32_trailer_bytes = " << index::format::kCRC32Size << "\n";
  out << "index.serialization_version_written = " << index::format::kCurrentFormatVersion << "\n";
  out << "index.serialization_version_accepted_min = " << index::format::kMinSupportedVersion << "\n";
  out << "index.serialization_version_accepted_max = " << index::format::kMaxSupportedVersion << "\n";
  out << "index.serialization_version_rejected_at_or_below = " << index::format::kMinSupportedVersion - 1 << "\n";
  out << "index.serialization_version_rejected_at_or_above = " << index::format::kMaxSupportedVersion + 1 << "\n";
}

/**
 * @brief One server command-line flag.
 */
struct FlagDescriptor {
  std::string_view flags;       ///< Accepted spellings, comma separated.
  bool takes_argument;          ///< True when the next argv entry is consumed.
  std::string_view config_key;  ///< Config key the flag supplies, or empty.
};

void AppendCliFlags(std::ostringstream& out) {
  out << "[cli-flags]\n";
  // Sorted by the flag spelling so the section does not track parse order.
  constexpr std::array<FlagDescriptor, 7> kFlags = {{
      {"--print-surface", false, ""},
      {"-c,--config", true, ""},
      {"-d,--daemon", false, ""},
      {"-h,--help", false, ""},
      {"-s,--schema", true, ""},
      {"-t,--config-test", false, ""},
      {"-v,--version", false, ""},
  }};
  std::map<std::string_view, const FlagDescriptor*> sorted;
  for (const auto& flag : kFlags) {
    sorted.emplace(flag.flags, &flag);
  }
  for (const auto& [name, flag] : sorted) {
    out << name << " argument=" << (flag->takes_argument ? "yes" : "no")
        << " config_key=" << (flag->config_key.empty() ? "none" : flag->config_key) << "\n";
  }
}

}  // namespace

std::string RenderSurfaceSnapshot() {
  std::ostringstream out;
  out.imbue(std::locale::classic());
  out << "# surface-snapshot-format: 1\n";
  AppendCommands(out);
  AppendQueryLimits(out);
  AppendHttpRoutes(out);
  AppendErrorCodes(out);
  AppendConfigKeys(out);
  AppendPersistence(out);
  AppendCliFlags(out);
  return out.str();
}

}  // namespace mygramdb::app
