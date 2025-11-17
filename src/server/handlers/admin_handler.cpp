/**
 * @file admin_handler.cpp
 * @brief Handler for administrative commands
 */

#include "server/handlers/admin_handler.h"

#include <spdlog/spdlog.h>

#include "config/config_help.h"
#include "server/statistics_service.h"
#include "utils/structured_log.h"

namespace mygramdb::server {

std::string AdminHandler::Handle(const query::Query& query, ConnectionContext& conn_ctx) {
  (void)conn_ctx;  // Unused for admin commands

  switch (query.type) {
    case query::QueryType::INFO: {
      // 1. Aggregate metrics (domain layer, pure function)
      auto metrics = StatisticsService::AggregateMetrics(ctx_.table_contexts);

      // 2. Update stats (domain layer, explicit side effect)
      StatisticsService::UpdateServerStatistics(ctx_.stats, metrics);

      // 3. Format response (presentation layer, pure function)
      return ResponseFormatter::FormatInfoResponse(metrics, ctx_.stats, ctx_.table_contexts, ctx_.binlog_reader,
                                                   ctx_.cache_manager);
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
  try {
    config::ConfigSchemaExplorer explorer;

    if (path.empty()) {
      // Show top-level sections
      auto paths = explorer.ListPaths("");
      std::string result = config::ConfigSchemaExplorer::FormatPathList(paths, "");
      return "+OK\n" + result;
    }

    // Show help for specific path
    auto help_info = explorer.GetHelp(path);
    if (!help_info.has_value()) {
      return ResponseFormatter::FormatError("Configuration path not found: " + path);
    }

    std::string result = config::ConfigSchemaExplorer::FormatHelp(help_info.value());
    return "+OK\n" + result;

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "config_help")
        .Field("error", e.what())
        .Error();
    return ResponseFormatter::FormatError(std::string("CONFIG HELP failed: ") + e.what());
  }
}

std::string AdminHandler::HandleConfigShow(const std::string& path) {
  try {
    if (ctx_.full_config == nullptr) {
      mygram::utils::StructuredLog()
          .Event("server_warning")
          .Field("operation", "config_show")
          .Field("reason", "config_not_available")
          .Warn();
      return ResponseFormatter::FormatError("Server configuration is not available");
    }

    std::string result = config::FormatConfigForDisplay(*ctx_.full_config, path);
    return "+OK\n" + result;
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "config_show")
        .Field("error", e.what())
        .Error();
    return ResponseFormatter::FormatError(std::string("CONFIG SHOW failed: ") + e.what());
  }
}

std::string AdminHandler::HandleConfigVerify(const std::string& filepath) {
  if (filepath.empty()) {
    return ResponseFormatter::FormatError("CONFIG VERIFY requires a filepath");
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
    return ResponseFormatter::FormatError(std::string("Configuration validation failed:\n  ") +
                                          config_result.error().message());
  }

  config::Config test_config = *config_result;

  // Build summary information
  std::ostringstream summary;
  summary << "Configuration is valid\n";
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
  summary << "\n";
  summary << "  MySQL: " << test_config.mysql.user << "@" << test_config.mysql.host << ":" << test_config.mysql.port;

  return "+OK\n" + summary.str();
}

}  // namespace mygramdb::server
