/**
 * @file cache_handler.cpp
 * @brief Handler for CACHE commands
 */

#include "server/handlers/cache_handler.h"

#include <iomanip>
#include <sstream>

#include "cache/cache_manager.h"
#include "server/response_formatter.h"

namespace mygramdb::server {

std::string CacheHandler::Handle(const query::Query& query, ConnectionContext& conn_ctx) {
  (void)conn_ctx;  // Mark as intentionally unused

  switch (query.type) {
    case query::QueryType::CACHE_CLEAR:
      return HandleClear(query);
    case query::QueryType::CACHE_STATS:
      return HandleStats();
    case query::QueryType::CACHE_ENABLE:
      return HandleEnable();
    case query::QueryType::CACHE_DISABLE:
      return HandleDisable();
    default:
      return ResponseFormatter::FormatError("Invalid query type for CacheHandler");
  }
}

std::string CacheHandler::HandleClear(const query::Query& query) {
  // Check if cache manager exists
  if (ctx_.cache_manager == nullptr) {
    return ResponseFormatter::FormatError("Cache not configured");
  }

  if (query.table.empty()) {
    // CACHE CLEAR - clear all cache
    ctx_.cache_manager->Clear();
    return "OK CACHE_CLEARED";
  }

  // CACHE CLEAR <table> - clear table-specific cache
  ctx_.cache_manager->ClearTable(query.table);
  return "OK CACHE_CLEARED table=" + query.table;
}

std::string CacheHandler::HandleStats() {
  // Check if cache manager exists
  if (ctx_.cache_manager == nullptr) {
    return ResponseFormatter::FormatError("Cache not configured");
  }

  auto stats = ctx_.cache_manager->GetStatistics();

  std::ostringstream oss;
  oss << "OK CACHE_STATS\r\n\r\n";

  // Cache status
  oss << "# Cache\r\n";
  oss << "enabled: " << (ctx_.cache_manager->IsEnabled() ? "true" : "false") << "\r\n";
  oss << "total_queries: " << stats.total_queries << "\r\n";
  oss << "cache_hits: " << stats.cache_hits << "\r\n";
  oss << "cache_misses: " << stats.cache_misses << "\r\n";

  // Hit rate
  if (stats.total_queries > 0) {
    double hit_rate = static_cast<double>(stats.cache_hits) / static_cast<double>(stats.total_queries);
    oss << "hit_rate: " << std::fixed << std::setprecision(4) << hit_rate << "\r\n";
  } else {
    oss << "hit_rate: 0.0000\r\n";
  }

  // Memory usage
  oss << "current_entries: " << stats.current_entries << "\r\n";
  oss << "current_memory_bytes: " << stats.current_memory_bytes << "\r\n";
  oss << "evictions: " << stats.evictions << "\r\n";

  // Invalidation statistics
  oss << "invalidations_immediate: " << stats.invalidations_immediate << "\r\n";
  oss << "invalidations_deferred: " << stats.invalidations_deferred << "\r\n";
  oss << "invalidations_batches: " << stats.invalidations_batches << "\r\n";

  // Timing statistics
  if (stats.cache_hits > 0) {
    oss << "avg_cache_hit_time_ms: " << std::fixed << std::setprecision(3) << stats.AverageCacheHitLatency() << "\r\n";
  }
  if (stats.cache_misses > 0) {
    oss << "avg_cache_miss_time_ms: " << std::fixed << std::setprecision(3) << stats.AverageCacheMissLatency()
        << "\r\n";
  }
  oss << "total_time_saved_ms: " << std::fixed << std::setprecision(3) << stats.TotalTimeSaved() << "\r\n";

  oss << "\r\nEND";
  return oss.str();
}

std::string CacheHandler::HandleEnable() {
  // Check if cache manager exists
  if (ctx_.cache_manager == nullptr) {
    return ResponseFormatter::FormatError("Cache not configured");
  }

  // Attempt to enable cache
  if (!ctx_.cache_manager->Enable()) {
    return ResponseFormatter::FormatError(
        "Cache cannot be enabled: server was started with cache disabled. "
        "Please restart the server with cache.enabled = true in configuration.");
  }

  return "OK CACHE_ENABLED";
}

std::string CacheHandler::HandleDisable() {
  // Check if cache manager exists
  if (ctx_.cache_manager == nullptr) {
    return ResponseFormatter::FormatError("Cache not configured");
  }

  ctx_.cache_manager->Disable();
  return "OK CACHE_DISABLED";
}

}  // namespace mygramdb::server
