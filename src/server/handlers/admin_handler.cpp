/**
 * @file admin_handler.cpp
 * @brief Handler for administrative commands
 */

#include "server/handlers/admin_handler.h"

#include "server/statistics_service.h"

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

    case query::QueryType::CONFIG:
      return ResponseFormatter::FormatConfigResponse(ctx_.full_config, ctx_.stats.GetActiveConnections(),
                                                     0,  // max_connections (TODO: pass from server config)
                                                     ctx_.read_only, ctx_.stats.GetUptimeSeconds());

    default:
      return ResponseFormatter::FormatError("Invalid query type for AdminHandler");
  }
}

}  // namespace mygramdb::server
