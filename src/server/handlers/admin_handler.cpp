/**
 * @file admin_handler.cpp
 * @brief Handler for administrative commands
 */

#include "server/handlers/admin_handler.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>

#include "config/config_help.h"
#include "server/statistics_service.h"
#include "server/table_catalog.h"
#include "utils/structured_log.h"

namespace mygramdb::server {

std::string AdminHandler::Handle(const query::Query& query, ConnectionContext& conn_ctx) {
  (void)conn_ctx;  // Unused for admin commands

  switch (query.type) {
    case query::QueryType::INFO: {
      {
        const auto& tables = ctx_.table_catalog->GetTables();
        // 1. Aggregate metrics (domain layer, pure function)
        auto metrics = StatisticsService::AggregateMetrics(tables);

        // 2. Update stats (domain layer, explicit side effect)
        StatisticsService::UpdateServerStatistics(ctx_.stats, metrics);

        // 3. Format response (presentation layer, pure function)
        return ResponseFormatter::FormatInfoResponse(metrics, ctx_.stats, tables, ctx_.binlog_reader,
                                                     ctx_.cache_manager);
      }
    }

    case query::QueryType::CONFIG_HELP:
      return HandleConfigHelp(query.filepath);

    case query::QueryType::CONFIG_SHOW:
      return HandleConfigShow(query.filepath);

    case query::QueryType::CONFIG_VERIFY:
      return HandleConfigVerify(query.filepath);

    default:
      return ResponseFormatter::FormatError("Invalid query type for AdminHandler");
  }
}

std::string AdminHandler::HandleConfigHelp(const std::string& path) {
  auto explorer_result = config::ConfigSchemaExplorer::Create();
  if (!explorer_result) {
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "config_help")
        .Field("error", explorer_result.error().message())
        .Error();
    return ResponseFormatter::FormatError(std::string("CONFIG HELP failed: ") + explorer_result.error().message());
  }
  auto& explorer = *explorer_result;

  if (path.empty()) {
    // Show top-level sections
    auto paths = explorer.ListPaths("");
    std::string result = config::ConfigSchemaExplorer::FormatPathList(paths, "");
    // Ensure result ends with \r\n for multi-line end-of-response detection
    if (result.size() < 2 || result[result.size() - 2] != '\r' || result[result.size() - 1] != '\n') {
      result.append("\r\n");
    }
    return "+OK\r\n" + result;
  }

  // Show help for specific path
  auto help_info = explorer.GetHelp(path);
  if (!help_info.has_value()) {
    return ResponseFormatter::FormatError("Configuration path not found: " + path);
  }

  std::string result = config::ConfigSchemaExplorer::FormatHelp(help_info.value());
  // Ensure result ends with \r\n for multi-line end-of-response detection
  if (result.size() < 2 || result[result.size() - 2] != '\r' || result[result.size() - 1] != '\n') {
    result.append("\r\n");
  }
  return "+OK\r\n" + result;
}

std::string AdminHandler::HandleConfigShow(const std::string& path) {
  if (ctx_.full_config == nullptr) {
    mygram::utils::StructuredLog()
        .Event("server_warning")
        .Field("operation", "config_show")
        .Field("reason", "config_not_available")
        .Warn();
    return ResponseFormatter::FormatError("Server configuration is not available");
  }

  auto format_result = config::FormatConfigForDisplay(*ctx_.full_config, path);
  if (!format_result) {
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "config_show")
        .Field("error", format_result.error().message())
        .Error();
    return ResponseFormatter::FormatError(std::string("CONFIG SHOW failed: ") + format_result.error().message());
  }

  std::string result = std::move(*format_result);
  // Ensure result ends with \r\n so the server's appended \r\n produces
  // the \r\n\r\n end-of-response marker that clients use to detect
  // multi-line response completion.
  if (result.size() < 2 || result[result.size() - 2] != '\r' || result[result.size() - 1] != '\n') {
    result.append("\r\n");
  }
  return "+OK\r\n" + result;
}

std::string AdminHandler::HandleConfigVerify(const std::string& filepath) {
  if (filepath.empty()) {
    return ResponseFormatter::FormatError("CONFIG VERIFY requires a filepath");
  }

  // Restrict to YAML/YML configuration files only to prevent reading
  // arbitrary filesystem paths via this command.
  {
    const auto dot_pos = filepath.rfind('.');
    if (dot_pos == std::string::npos) {
      return ResponseFormatter::FormatError("CONFIG VERIFY only accepts .yaml or .yml files");
    }
    std::string ext = filepath.substr(dot_pos);
    // Case-insensitive extension check
    std::transform(ext.begin(), ext.end(), ext.begin(), [](unsigned char c) { return std::tolower(c); });
    if (ext != ".yaml" && ext != ".yml") {
      return ResponseFormatter::FormatError("CONFIG VERIFY only accepts .yaml or .yml files");
    }
  }

  // Try to load and validate the configuration file
  auto config_result = config::LoadConfig(filepath);
  if (!config_result) {
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "config_verify")
        .Field("filepath", filepath)
        .Field("error", config_result.error().to_string())
        .Error();
    return ResponseFormatter::FormatError(std::string("Configuration validation failed:\r\n  ") +
                                          config_result.error().message());
  }

  config::Config test_config = *config_result;

  // Build summary information
  std::ostringstream summary;
  summary << "Configuration is valid\r\n";
  summary << "  Tables: " << test_config.tables.size();
  if (!test_config.tables.empty()) {
    summary << " (";
    for (size_t i = 0; i < test_config.tables.size(); ++i) {
      if (i > 0) {
        summary << ", ";
      }
      summary << test_config.tables[i].name;
    }
    summary << ")";
  }
  summary << "\r\n";
  summary << "  MySQL: " << test_config.mysql.user << "@" << test_config.mysql.host << ":" << test_config.mysql.port
          << "\r\n";

  return "+OK\r\n" + summary.str();
}

}  // namespace mygramdb::server
