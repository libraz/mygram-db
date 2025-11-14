/**
 * @file response_formatter.cpp
 * @brief Response formatting utilities for TCP server
 */

#include "server/response_formatter.h"

#include <spdlog/spdlog.h>

#include <iomanip>
#include <sstream>

#include "server/tcp_server.h"
#include "utils/memory_utils.h"
#include "utils/string_utils.h"
#include "version.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#endif

namespace mygramdb::server {

std::string ResponseFormatter::FormatSearchResponse(const std::vector<index::DocId>& results, size_t total_results,
                                                    storage::DocumentStore* doc_store,
                                                    const query::DebugInfo* debug_info) {
  std::ostringstream oss;
  oss << "OK RESULTS " << total_results;

  // Results are already sorted and paginated by ResultSorter::SortAndPaginate
  // Just format them directly (no offset/limit needed)
  for (const auto& doc_id : results) {
    auto pk_opt = doc_store->GetPrimaryKey(static_cast<storage::DocId>(doc_id));
    if (pk_opt) {
      oss << " " << pk_opt.value();
    }
  }

  // Add debug information if provided
  if (debug_info != nullptr) {
    oss << "\r\n\r\n# DEBUG\r\n";
    oss << "query_time: " << std::fixed << std::setprecision(3) << debug_info->query_time_ms << "ms\r\n";
    oss << "index_time: " << debug_info->index_time_ms << "ms\r\n";
    if (debug_info->filter_time_ms > 0.0) {
      oss << "filter_time: " << debug_info->filter_time_ms << "ms\r\n";
    }
    oss << "terms: " << debug_info->search_terms.size() << "\r\n";
    oss << "ngrams: " << debug_info->ngrams_used.size() << "\r\n";
    oss << "candidates: " << debug_info->total_candidates << "\r\n";
    oss << "after_intersection: " << debug_info->after_intersection << "\r\n";
    if (debug_info->after_not > 0) {
      oss << "after_not: " << debug_info->after_not << "\r\n";
    }
    if (debug_info->after_filters > 0) {
      oss << "after_filters: " << debug_info->after_filters << "\r\n";
    }
    oss << "final: " << debug_info->final_results << "\r\n";
    if (!debug_info->optimization_used.empty()) {
      oss << "optimization: " << debug_info->optimization_used << "\r\n";
    }
    // Show applied SORT, LIMIT, OFFSET
    if (!debug_info->order_by_applied.empty()) {
      oss << "sort: " << debug_info->order_by_applied << "\r\n";
    }
    // Always show LIMIT (it has a default value of 100)
    oss << "limit: " << debug_info->limit_applied;
    if (!debug_info->limit_explicit) {
      oss << " (default)";
    }
    oss << "\r\n";
    // Show OFFSET if non-zero
    if (debug_info->offset_applied > 0) {
      oss << "offset: " << debug_info->offset_applied;
      if (!debug_info->offset_explicit) {
        oss << " (default)";
      }
      oss << "\r\n";
    }
  }

  return oss.str();
}

std::string ResponseFormatter::FormatCountResponse(uint64_t count, const query::DebugInfo* debug_info) {
  std::ostringstream oss;
  oss << "OK COUNT " << count;

  // Add debug information if provided
  if (debug_info != nullptr) {
    oss << "\r\n\r\n# DEBUG\r\n";
    oss << "query_time: " << std::fixed << std::setprecision(3) << debug_info->query_time_ms << "ms\r\n";
    oss << "index_time: " << debug_info->index_time_ms << "ms\r\n";
    oss << "terms: " << debug_info->search_terms.size() << "\r\n";
    oss << "ngrams: " << debug_info->ngrams_used.size() << "\r\n";
  }

  return oss.str();
}

std::string ResponseFormatter::FormatGetResponse(const std::optional<storage::Document>& doc) {
  if (!doc) {
    return FormatError("Document not found");
  }

  std::ostringstream oss;
  oss << "OK DOC " << doc->primary_key;

  // Add filters
  for (const auto& [name, value] : doc->filters) {
    oss << " " << name << "=";
    if (std::holds_alternative<int64_t>(value)) {
      oss << std::get<int64_t>(value);
    } else if (std::holds_alternative<std::string>(value)) {
      oss << std::get<std::string>(value);
    }
  }

  return oss.str();
}

std::string ResponseFormatter::FormatInfoResponse(const std::unordered_map<std::string, TableContext*>& table_contexts,
                                                  ServerStats& stats,
#ifdef USE_MYSQL
                                                  mysql::BinlogReader* binlog_reader
#else
                                                  void* binlog_reader
#endif
) {
  std::ostringstream oss;
  oss << "OK INFO\r\n\r\n";

  // Server information
  oss << "# Server\r\n";
  oss << "version: " << mygramdb::Version::FullString() << "\r\n";
  oss << "uptime_seconds: " << stats.GetUptimeSeconds() << "\r\n";
  oss << "\r\n";

  // Stats - Command counters
  oss << "# Stats\r\n";
  oss << "total_commands_processed: " << stats.GetTotalCommands() << "\r\n";
  oss << "total_connections_received: " << stats.GetStatistics().total_connections_received << "\r\n";
  oss << "total_requests: " << stats.GetTotalRequests() << "\r\n";
  oss << "\r\n";

  // Command counters
  oss << "# Commandstats\r\n";
  auto cmd_stats = stats.GetStatistics();
  if (cmd_stats.cmd_search > 0) {
    oss << "cmd_search: " << cmd_stats.cmd_search << "\r\n";
  }
  if (cmd_stats.cmd_count > 0) {
    oss << "cmd_count: " << cmd_stats.cmd_count << "\r\n";
  }
  if (cmd_stats.cmd_get > 0) {
    oss << "cmd_get: " << cmd_stats.cmd_get << "\r\n";
  }
  if (cmd_stats.cmd_info > 0) {
    oss << "cmd_info: " << cmd_stats.cmd_info << "\r\n";
  }
  if (cmd_stats.cmd_save > 0) {
    oss << "cmd_save: " << cmd_stats.cmd_save << "\r\n";
  }
  if (cmd_stats.cmd_load > 0) {
    oss << "cmd_load: " << cmd_stats.cmd_load << "\r\n";
  }
  if (cmd_stats.cmd_replication_status > 0) {
    oss << "cmd_replication_status: " << cmd_stats.cmd_replication_status << "\r\n";
  }
  if (cmd_stats.cmd_replication_stop > 0) {
    oss << "cmd_replication_stop: " << cmd_stats.cmd_replication_stop << "\r\n";
  }
  if (cmd_stats.cmd_replication_start > 0) {
    oss << "cmd_replication_start: " << cmd_stats.cmd_replication_start << "\r\n";
  }
  if (cmd_stats.cmd_config > 0) {
    oss << "cmd_config: " << cmd_stats.cmd_config << "\r\n";
  }
  oss << "\r\n";

  // Aggregate memory and index statistics across all tables
  size_t total_index_memory = 0;
  size_t total_doc_memory = 0;
  size_t total_documents = 0;
  size_t total_terms = 0;
  size_t total_postings = 0;
  size_t total_delta_encoded = 0;
  size_t total_roaring_bitmap = 0;
  bool any_optimizing = false;

  for (const auto& [table_name, ctx] : table_contexts) {
    total_index_memory += ctx->index->MemoryUsage();
    total_doc_memory += ctx->doc_store->MemoryUsage();
    total_documents += ctx->doc_store->Size();
    auto idx_stats = ctx->index->GetStatistics();
    total_terms += idx_stats.total_terms;
    total_postings += idx_stats.total_postings;
    total_delta_encoded += idx_stats.delta_encoded_lists;
    total_roaring_bitmap += idx_stats.roaring_bitmap_lists;
    if (ctx->index->IsOptimizing()) {
      any_optimizing = true;
    }
  }

  size_t total_memory = total_index_memory + total_doc_memory;

  // Memory statistics
  oss << "# Memory\r\n";

  // Update memory stats in real-time
  stats.UpdateMemoryUsage(total_memory);

  oss << "used_memory_bytes: " << total_memory << "\r\n";
  oss << "used_memory_human: " << utils::FormatBytes(total_memory) << "\r\n";
  oss << "used_memory_peak_bytes: " << stats.GetPeakMemoryUsage() << "\r\n";
  oss << "used_memory_peak_human: " << utils::FormatBytes(stats.GetPeakMemoryUsage()) << "\r\n";
  oss << "used_memory_index: " << utils::FormatBytes(total_index_memory) << "\r\n";
  oss << "used_memory_documents: " << utils::FormatBytes(total_doc_memory) << "\r\n";

  // Memory fragmentation estimate
  if (total_memory > 0) {
    size_t peak = stats.GetPeakMemoryUsage();
    double fragmentation = peak > 0 ? static_cast<double>(peak) / static_cast<double>(total_memory) : 1.0;
    oss << "memory_fragmentation_ratio: " << std::fixed << std::setprecision(2) << fragmentation << "\r\n";
  }

  // System memory information
  auto sys_info = utils::GetSystemMemoryInfo();
  if (sys_info) {
    oss << "total_system_memory: " << utils::FormatBytes(sys_info->total_physical_bytes) << "\r\n";
    oss << "available_system_memory: " << utils::FormatBytes(sys_info->available_physical_bytes) << "\r\n";
    if (sys_info->total_physical_bytes > 0) {
      double usage_ratio = 1.0 - static_cast<double>(sys_info->available_physical_bytes) /
                                     static_cast<double>(sys_info->total_physical_bytes);
      oss << "system_memory_usage_ratio: " << std::fixed << std::setprecision(2) << usage_ratio << "\r\n";
    }
  }

  // Process memory information
  auto proc_info = utils::GetProcessMemoryInfo();
  if (proc_info) {
    oss << "process_rss: " << utils::FormatBytes(proc_info->rss_bytes) << "\r\n";
    oss << "process_rss_peak: " << utils::FormatBytes(proc_info->peak_rss_bytes) << "\r\n";
  }

  // Memory health status
  auto health = utils::GetMemoryHealthStatus();
  oss << "memory_health: " << utils::MemoryHealthStatusToString(health) << "\r\n";

  oss << "\r\n";

  // Index statistics (aggregated)
  oss << "# Index\r\n";
  oss << "total_documents: " << total_documents << "\r\n";
  oss << "total_terms: " << total_terms << "\r\n";
  oss << "total_postings: " << total_postings << "\r\n";
  if (total_terms > 0) {
    double avg_postings = static_cast<double>(total_postings) / static_cast<double>(total_terms);
    oss << "avg_postings_per_term: " << std::fixed << std::setprecision(2) << avg_postings << "\r\n";
  }
  oss << "delta_encoded_lists: " << total_delta_encoded << "\r\n";
  oss << "roaring_bitmap_lists: " << total_roaring_bitmap << "\r\n";

  // Optimization status
  if (any_optimizing) {
    oss << "optimization_status: in_progress\r\n";
  } else {
    oss << "optimization_status: idle\r\n";
  }
  oss << "\r\n";

  // Tables
  oss << "# Tables\r\n";
  oss << "tables: ";
  size_t idx = 0;
  for (const auto& [name, unused_context] : table_contexts) {
    (void)unused_context;  // Mark as intentionally unused
    if (idx++ > 0) {
      oss << ",";
    }
    oss << name;
  }
  oss << "\r\n\r\n";

  // Clients
  oss << "# Clients\r\n";
  oss << "connected_clients: " << stats.GetActiveConnections() << "\r\n";
  oss << "\r\n";

  // Replication information
#ifdef USE_MYSQL
  oss << "# Replication\r\n";
  if (binlog_reader != nullptr) {
    oss << "replication_status: " << (binlog_reader->IsRunning() ? "running" : "stopped") << "\r\n";
    oss << "replication_gtid: " << binlog_reader->GetCurrentGTID() << "\r\n";
    oss << "replication_events: " << binlog_reader->GetProcessedEvents() << "\r\n";
  } else {
    oss << "replication_status: disabled\r\n";
  }

  // Event statistics (always shown, even when binlog_reader is null)
  oss << "replication_inserts_applied: " << stats.GetReplInsertsApplied() << "\r\n";
  oss << "replication_inserts_skipped: " << stats.GetReplInsertsSkipped() << "\r\n";
  oss << "replication_updates_applied: " << stats.GetReplUpdatesApplied() << "\r\n";
  oss << "replication_updates_added: " << stats.GetReplUpdatesAdded() << "\r\n";
  oss << "replication_updates_removed: " << stats.GetReplUpdatesRemoved() << "\r\n";
  oss << "replication_updates_modified: " << stats.GetReplUpdatesModified() << "\r\n";
  oss << "replication_updates_skipped: " << stats.GetReplUpdatesSkipped() << "\r\n";
  oss << "replication_deletes_applied: " << stats.GetReplDeletesApplied() << "\r\n";
  oss << "replication_deletes_skipped: " << stats.GetReplDeletesSkipped() << "\r\n";
  oss << "replication_ddl_executed: " << stats.GetReplDdlExecuted() << "\r\n";
  oss << "replication_events_skipped_other_tables: " << stats.GetReplEventsSkippedOtherTables() << "\r\n";
  oss << "\r\n";
#endif

  oss << "END";
  return oss.str();
}

std::string ResponseFormatter::FormatSaveResponse(const std::string& filepath) {
  return "OK SAVED " + filepath;
}

std::string ResponseFormatter::FormatLoadResponse(const std::string& filepath) {
  return "OK LOADED " + filepath;
}

std::string ResponseFormatter::FormatReplicationStatusResponse(
#ifdef USE_MYSQL
    mysql::BinlogReader* binlog_reader
#else
    void* binlog_reader
#endif
) {
#ifdef USE_MYSQL
  std::ostringstream oss;
  oss << "OK REPLICATION\r\n";

  if (binlog_reader != nullptr) {
    bool is_running = binlog_reader->IsRunning();
    oss << "status: " << (is_running ? "running" : "stopped") << "\r\n";
    oss << "current_gtid: " << binlog_reader->GetCurrentGTID() << "\r\n";
    oss << "processed_events: " << binlog_reader->GetProcessedEvents() << "\r\n";

    if (is_running) {
      oss << "queue_size: " << binlog_reader->GetQueueSize() << "\r\n";
    }
  } else {
    oss << "status: not_configured\r\n";
  }

  oss << "END";
  return oss.str();
#else
  (void)binlog_reader;  // Suppress unused parameter warning
  return FormatError("MySQL support not compiled");
#endif
}

std::string ResponseFormatter::FormatReplicationStopResponse() {
  return "OK REPLICATION_STOPPED";
}

std::string ResponseFormatter::FormatReplicationStartResponse() {
  return "OK REPLICATION_STARTED";
}

std::string ResponseFormatter::FormatConfigResponse(const config::Config* full_config, size_t connection_count,
                                                    int max_connections, bool read_only, uint64_t uptime_seconds) {
  std::ostringstream oss;
  oss << "OK CONFIG\n";

  if (full_config == nullptr) {
    oss << "  [Configuration not available]\n";
    return oss.str();
  }

  // MySQL configuration
  oss << "  mysql:\n";
  oss << "    host: " << full_config->mysql.host << "\n";
  oss << "    port: " << full_config->mysql.port << "\n";
  oss << "    user: " << full_config->mysql.user << "\n";
  oss << "    database: " << full_config->mysql.database << "\n";
  oss << "    use_gtid: " << (full_config->mysql.use_gtid ? "true" : "false") << "\n";

  // Tables configuration
  oss << "  tables: " << full_config->tables.size() << "\n";
  for (const auto& table : full_config->tables) {
    oss << "    - name: " << table.name << "\n";
    oss << "      primary_key: " << table.primary_key << "\n";
    oss << "      ngram_size: " << table.ngram_size << "\n";
    oss << "      filters: " << table.filters.size() << "\n";
  }

  // API configuration
  oss << "  api:\n";
  oss << "    tcp.bind: " << full_config->api.tcp.bind << "\n";
  oss << "    tcp.port: " << full_config->api.tcp.port << "\n";

  // Replication configuration
  oss << "  replication:\n";
  oss << "    enable: " << (full_config->replication.enable ? "true" : "false") << "\n";
  oss << "    server_id: " << full_config->replication.server_id << "\n";
  oss << "    start_from: " << full_config->replication.start_from << "\n";

  // Memory configuration
  oss << "  memory:\n";
  oss << "    hard_limit_mb: " << full_config->memory.hard_limit_mb << "\n";
  oss << "    soft_target_mb: " << full_config->memory.soft_target_mb << "\n";
  oss << "    roaring_threshold: " << full_config->memory.roaring_threshold << "\n";

  // Dump configuration
  oss << "  dump:\n";
  oss << "    dir: " << full_config->dump.dir << "\n";

  // Logging configuration
  oss << "  logging:\n";
  oss << "    level: " << full_config->logging.level << "\n";

  // Runtime status
  oss << "  runtime:\n";
  oss << "    connections: " << connection_count << "\n";
  oss << "    max_connections: " << max_connections << "\n";
  oss << "    read_only: " << (read_only ? "true" : "false") << "\n";
  oss << "    uptime: " << uptime_seconds << "s\n";

  return oss.str();
}

std::string ResponseFormatter::FormatPrometheusMetrics(
    const std::unordered_map<std::string, TableContext*>& table_contexts, ServerStats& stats,
#ifdef USE_MYSQL
    mysql::BinlogReader* binlog_reader
#else
    void* binlog_reader
#endif
) {
  std::ostringstream oss;

  // Aggregate metrics across all tables
  size_t total_index_memory = 0;
  size_t total_doc_memory = 0;
  size_t total_documents = 0;
  size_t total_terms = 0;
  size_t total_postings = 0;
  size_t total_delta_encoded = 0;
  size_t total_roaring_bitmap = 0;

  for (const auto& [table_name, ctx] : table_contexts) {
    total_index_memory += ctx->index->MemoryUsage();
    total_doc_memory += ctx->doc_store->MemoryUsage();
    total_documents += ctx->doc_store->Size();
    auto idx_stats = ctx->index->GetStatistics();
    total_terms += idx_stats.total_terms;
    total_postings += idx_stats.total_postings;
    total_delta_encoded += idx_stats.delta_encoded_lists;
    total_roaring_bitmap += idx_stats.roaring_bitmap_lists;
  }

  size_t total_memory = total_index_memory + total_doc_memory;
  stats.UpdateMemoryUsage(total_memory);

  // Server info (version as label)
  oss << "# HELP mygramdb_server_info MygramDB server information\n";
  oss << "# TYPE mygramdb_server_info gauge\n";
  oss << "mygramdb_server_info{version=\"" << mygramdb::Version::String() << "\"} 1\n";
  oss << "\n";

  // Server uptime
  oss << "# HELP mygramdb_server_uptime_seconds Server uptime in seconds\n";
  oss << "# TYPE mygramdb_server_uptime_seconds counter\n";
  oss << "mygramdb_server_uptime_seconds " << stats.GetUptimeSeconds() << "\n";
  oss << "\n";

  // Total commands processed
  auto cmd_stats = stats.GetStatistics();
  oss << "# HELP mygramdb_server_commands_total Total number of commands processed\n";
  oss << "# TYPE mygramdb_server_commands_total counter\n";
  oss << "mygramdb_server_commands_total " << stats.GetTotalCommands() << "\n";
  oss << "\n";

  // Command counters by type
  oss << "# HELP mygramdb_command_total Total number of commands executed by type\n";
  oss << "# TYPE mygramdb_command_total counter\n";
  if (cmd_stats.cmd_search > 0) {
    oss << "mygramdb_command_total{command=\"search\"} " << cmd_stats.cmd_search << "\n";
  }
  if (cmd_stats.cmd_count > 0) {
    oss << "mygramdb_command_total{command=\"count\"} " << cmd_stats.cmd_count << "\n";
  }
  if (cmd_stats.cmd_get > 0) {
    oss << "mygramdb_command_total{command=\"get\"} " << cmd_stats.cmd_get << "\n";
  }
  if (cmd_stats.cmd_info > 0) {
    oss << "mygramdb_command_total{command=\"info\"} " << cmd_stats.cmd_info << "\n";
  }
  if (cmd_stats.cmd_save > 0) {
    oss << "mygramdb_command_total{command=\"save\"} " << cmd_stats.cmd_save << "\n";
  }
  if (cmd_stats.cmd_load > 0) {
    oss << "mygramdb_command_total{command=\"load\"} " << cmd_stats.cmd_load << "\n";
  }
  if (cmd_stats.cmd_replication_status > 0) {
    oss << "mygramdb_command_total{command=\"replication_status\"} " << cmd_stats.cmd_replication_status << "\n";
  }
  if (cmd_stats.cmd_replication_stop > 0) {
    oss << "mygramdb_command_total{command=\"replication_stop\"} " << cmd_stats.cmd_replication_stop << "\n";
  }
  if (cmd_stats.cmd_replication_start > 0) {
    oss << "mygramdb_command_total{command=\"replication_start\"} " << cmd_stats.cmd_replication_start << "\n";
  }
  if (cmd_stats.cmd_config > 0) {
    oss << "mygramdb_command_total{command=\"config\"} " << cmd_stats.cmd_config << "\n";
  }
  oss << "\n";

  // Memory usage by type
  oss << "# HELP mygramdb_memory_used_bytes Current memory usage in bytes\n";
  oss << "# TYPE mygramdb_memory_used_bytes gauge\n";
  oss << "mygramdb_memory_used_bytes{type=\"index\"} " << total_index_memory << "\n";
  oss << "mygramdb_memory_used_bytes{type=\"documents\"} " << total_doc_memory << "\n";
  oss << "mygramdb_memory_used_bytes{type=\"total\"} " << total_memory << "\n";
  oss << "\n";

  // Peak memory
  oss << "# HELP mygramdb_memory_peak_bytes Peak memory usage since server start\n";
  oss << "# TYPE mygramdb_memory_peak_bytes gauge\n";
  oss << "mygramdb_memory_peak_bytes " << stats.GetPeakMemoryUsage() << "\n";
  oss << "\n";

  // Memory fragmentation ratio
  if (total_memory > 0) {
    size_t peak = stats.GetPeakMemoryUsage();
    double fragmentation = peak > 0 ? static_cast<double>(peak) / static_cast<double>(total_memory) : 1.0;
    oss << "# HELP mygramdb_memory_fragmentation_ratio Memory fragmentation ratio\n";
    oss << "# TYPE mygramdb_memory_fragmentation_ratio gauge\n";
    oss << "mygramdb_memory_fragmentation_ratio " << std::fixed << std::setprecision(2) << fragmentation << "\n";
    oss << "\n";
  }

  // System memory information
  auto sys_info = utils::GetSystemMemoryInfo();
  if (sys_info) {
    oss << "# HELP mygramdb_memory_system_total_bytes Total system physical memory\n";
    oss << "# TYPE mygramdb_memory_system_total_bytes gauge\n";
    oss << "mygramdb_memory_system_total_bytes " << sys_info->total_physical_bytes << "\n";
    oss << "\n";

    oss << "# HELP mygramdb_memory_system_available_bytes Available system physical memory\n";
    oss << "# TYPE mygramdb_memory_system_available_bytes gauge\n";
    oss << "mygramdb_memory_system_available_bytes " << sys_info->available_physical_bytes << "\n";
    oss << "\n";

    if (sys_info->total_physical_bytes > 0) {
      double usage_ratio = 1.0 - static_cast<double>(sys_info->available_physical_bytes) /
                                     static_cast<double>(sys_info->total_physical_bytes);
      oss << "# HELP mygramdb_memory_system_usage_ratio System memory usage ratio\n";
      oss << "# TYPE mygramdb_memory_system_usage_ratio gauge\n";
      oss << "mygramdb_memory_system_usage_ratio " << std::fixed << std::setprecision(2) << usage_ratio << "\n";
      oss << "\n";
    }
  }

  // Process memory information
  auto proc_info = utils::GetProcessMemoryInfo();
  if (proc_info) {
    oss << "# HELP mygramdb_memory_process_rss_bytes Process resident set size\n";
    oss << "# TYPE mygramdb_memory_process_rss_bytes gauge\n";
    oss << "mygramdb_memory_process_rss_bytes " << proc_info->rss_bytes << "\n";
    oss << "\n";

    oss << "# HELP mygramdb_memory_process_rss_peak_bytes Peak process RSS since start\n";
    oss << "# TYPE mygramdb_memory_process_rss_peak_bytes gauge\n";
    oss << "mygramdb_memory_process_rss_peak_bytes " << proc_info->peak_rss_bytes << "\n";
    oss << "\n";
  }

  // Memory health status (0=UNKNOWN, 1=HEALTHY, 2=WARNING, 3=CRITICAL)
  auto health = utils::GetMemoryHealthStatus();
  int health_value = 0;
  switch (health) {
    case utils::MemoryHealthStatus::HEALTHY:
      health_value = 1;
      break;
    case utils::MemoryHealthStatus::WARNING:
      health_value = 2;
      break;
    case utils::MemoryHealthStatus::CRITICAL:
      health_value = 3;
      break;
    case utils::MemoryHealthStatus::UNKNOWN:
    default:
      health_value = 0;
      break;
  }
  oss << "# HELP mygramdb_memory_health_status Memory health status (0=UNKNOWN, 1=HEALTHY, 2=WARNING, 3=CRITICAL)\n";
  oss << "# TYPE mygramdb_memory_health_status gauge\n";
  oss << "mygramdb_memory_health_status " << health_value << "\n";
  oss << "\n";

  // Aggregated index statistics
  oss << "# HELP mygramdb_index_documents_total Total number of documents in the index\n";
  oss << "# TYPE mygramdb_index_documents_total gauge\n";
  for (const auto& [table_name, ctx] : table_contexts) {
    oss << "mygramdb_index_documents_total{table=\"" << table_name << "\"} " << ctx->doc_store->Size() << "\n";
  }
  oss << "\n";

  oss << "# HELP mygramdb_index_terms_total Total number of unique terms\n";
  oss << "# TYPE mygramdb_index_terms_total gauge\n";
  for (const auto& [table_name, ctx] : table_contexts) {
    auto idx_stats = ctx->index->GetStatistics();
    oss << "mygramdb_index_terms_total{table=\"" << table_name << "\"} " << idx_stats.total_terms << "\n";
  }
  oss << "\n";

  oss << "# HELP mygramdb_index_postings_total Total number of postings\n";
  oss << "# TYPE mygramdb_index_postings_total gauge\n";
  for (const auto& [table_name, ctx] : table_contexts) {
    auto idx_stats = ctx->index->GetStatistics();
    oss << "mygramdb_index_postings_total{table=\"" << table_name << "\"} " << idx_stats.total_postings << "\n";
  }
  oss << "\n";

  oss << "# HELP mygramdb_index_postings_per_term_avg Average postings per term\n";
  oss << "# TYPE mygramdb_index_postings_per_term_avg gauge\n";
  for (const auto& [table_name, ctx] : table_contexts) {
    auto idx_stats = ctx->index->GetStatistics();
    if (idx_stats.total_terms > 0) {
      double avg = static_cast<double>(idx_stats.total_postings) / static_cast<double>(idx_stats.total_terms);
      oss << "mygramdb_index_postings_per_term_avg{table=\"" << table_name << "\"} " << std::fixed
          << std::setprecision(2) << avg << "\n";
    }
  }
  oss << "\n";

  oss << "# HELP mygramdb_index_delta_encoded_lists Delta-encoded posting lists count\n";
  oss << "# TYPE mygramdb_index_delta_encoded_lists gauge\n";
  for (const auto& [table_name, ctx] : table_contexts) {
    auto idx_stats = ctx->index->GetStatistics();
    oss << "mygramdb_index_delta_encoded_lists{table=\"" << table_name << "\"} " << idx_stats.delta_encoded_lists
        << "\n";
  }
  oss << "\n";

  oss << "# HELP mygramdb_index_roaring_bitmap_lists Roaring bitmap posting lists count\n";
  oss << "# TYPE mygramdb_index_roaring_bitmap_lists gauge\n";
  for (const auto& [table_name, ctx] : table_contexts) {
    auto idx_stats = ctx->index->GetStatistics();
    oss << "mygramdb_index_roaring_bitmap_lists{table=\"" << table_name << "\"} " << idx_stats.roaring_bitmap_lists
        << "\n";
  }
  oss << "\n";

  oss << "# HELP mygramdb_index_optimization_in_progress Index optimization in progress (0=idle, 1=running)\n";
  oss << "# TYPE mygramdb_index_optimization_in_progress gauge\n";
  for (const auto& [table_name, ctx] : table_contexts) {
    int optimizing = ctx->index->IsOptimizing() ? 1 : 0;
    oss << "mygramdb_index_optimization_in_progress{table=\"" << table_name << "\"} " << optimizing << "\n";
  }
  oss << "\n";

  // Client connections
  oss << "# HELP mygramdb_clients_connected Current number of connected clients\n";
  oss << "# TYPE mygramdb_clients_connected gauge\n";
  oss << "mygramdb_clients_connected " << stats.GetActiveConnections() << "\n";
  oss << "\n";

  oss << "# HELP mygramdb_clients_total Total number of client connections received\n";
  oss << "# TYPE mygramdb_clients_total counter\n";
  oss << "mygramdb_clients_total " << cmd_stats.total_connections_received << "\n";
  oss << "\n";

  // Replication statistics
#ifdef USE_MYSQL
  if (binlog_reader != nullptr) {
    oss << "# HELP mygramdb_replication_running Replication status (0=stopped, 1=running)\n";
    oss << "# TYPE mygramdb_replication_running gauge\n";
    oss << "mygramdb_replication_running " << (binlog_reader->IsRunning() ? 1 : 0) << "\n";
    oss << "\n";

    oss << "# HELP mygramdb_replication_events_processed Total number of binlog events processed\n";
    oss << "# TYPE mygramdb_replication_events_processed counter\n";
    oss << "mygramdb_replication_events_processed " << binlog_reader->GetProcessedEvents() << "\n";
    oss << "\n";
  }

  // Replication operation counters
  oss << "# HELP mygramdb_replication_inserts_total Total number of INSERT operations\n";
  oss << "# TYPE mygramdb_replication_inserts_total counter\n";
  oss << "mygramdb_replication_inserts_total{status=\"applied\"} " << stats.GetReplInsertsApplied() << "\n";
  oss << "mygramdb_replication_inserts_total{status=\"skipped\"} " << stats.GetReplInsertsSkipped() << "\n";
  oss << "\n";

  oss << "# HELP mygramdb_replication_updates_total Total number of UPDATE operations\n";
  oss << "# TYPE mygramdb_replication_updates_total counter\n";
  oss << "mygramdb_replication_updates_total{status=\"applied\"} " << stats.GetReplUpdatesApplied() << "\n";
  oss << "mygramdb_replication_updates_total{status=\"added\"} " << stats.GetReplUpdatesAdded() << "\n";
  oss << "mygramdb_replication_updates_total{status=\"removed\"} " << stats.GetReplUpdatesRemoved() << "\n";
  oss << "mygramdb_replication_updates_total{status=\"modified\"} " << stats.GetReplUpdatesModified() << "\n";
  oss << "mygramdb_replication_updates_total{status=\"skipped\"} " << stats.GetReplUpdatesSkipped() << "\n";
  oss << "\n";

  oss << "# HELP mygramdb_replication_deletes_total Total number of DELETE operations\n";
  oss << "# TYPE mygramdb_replication_deletes_total counter\n";
  oss << "mygramdb_replication_deletes_total{status=\"applied\"} " << stats.GetReplDeletesApplied() << "\n";
  oss << "mygramdb_replication_deletes_total{status=\"skipped\"} " << stats.GetReplDeletesSkipped() << "\n";
  oss << "\n";

  oss << "# HELP mygramdb_replication_ddl_total Total number of DDL operations executed\n";
  oss << "# TYPE mygramdb_replication_ddl_total counter\n";
  oss << "mygramdb_replication_ddl_total " << stats.GetReplDdlExecuted() << "\n";
  oss << "\n";
#else
  (void)binlog_reader;  // Suppress unused parameter warning
#endif

  return oss.str();
}

std::string ResponseFormatter::FormatError(const std::string& message) {
  return "ERROR " + message;
}

}  // namespace mygramdb::server
