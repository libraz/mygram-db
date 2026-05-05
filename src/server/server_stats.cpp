/**
 * @file server_stats.cpp
 * @brief Server statistics tracking implementation
 */

#include "server/server_stats.h"

#include <algorithm>

namespace mygramdb::server {

ServerStats::ServerStats() : start_time_(static_cast<uint64_t>(std::time(nullptr))) {}

// Per-command counters cover hot-path commands; less common commands accumulate
// in cmd_other_. Always update GetTotalCommands and aggregate stats together
// when adding a new specific counter.
void ServerStats::IncrementCommand(query::QueryType type) {
  // These are independent counters with no happens-before relationship.
  // relaxed ordering is sufficient (reviewed: no cross-counter invariant
  // requires sequential consistency).
  switch (type) {
    case query::QueryType::SEARCH:
      cmd_search_.fetch_add(1, std::memory_order_relaxed);
      break;
    case query::QueryType::COUNT:
      cmd_count_.fetch_add(1, std::memory_order_relaxed);
      break;
    case query::QueryType::GET:
      cmd_get_.fetch_add(1, std::memory_order_relaxed);
      break;
    case query::QueryType::INFO:
      cmd_info_.fetch_add(1, std::memory_order_relaxed);
      break;
    case query::QueryType::SAVE:
      cmd_save_.fetch_add(1, std::memory_order_relaxed);
      break;
    case query::QueryType::LOAD:
      cmd_load_.fetch_add(1, std::memory_order_relaxed);
      break;
    case query::QueryType::REPLICATION_STATUS:
      cmd_replication_status_.fetch_add(1, std::memory_order_relaxed);
      break;
    case query::QueryType::REPLICATION_STOP:
      cmd_replication_stop_.fetch_add(1, std::memory_order_relaxed);
      break;
    case query::QueryType::REPLICATION_START:
      cmd_replication_start_.fetch_add(1, std::memory_order_relaxed);
      break;
    case query::QueryType::CONFIG_HELP:
    case query::QueryType::CONFIG_SHOW:
    case query::QueryType::CONFIG_VERIFY:
      cmd_config_.fetch_add(1, std::memory_order_relaxed);
      break;
    case query::QueryType::UNKNOWN:
      cmd_unknown_.fetch_add(1, std::memory_order_relaxed);
      break;
    // Less-common commands aggregate into cmd_other_ so GetTotalCommands and
    // total_requests reflect their occurrence.
    case query::QueryType::DUMP_SAVE:
    case query::QueryType::DUMP_LOAD:
    case query::QueryType::DUMP_VERIFY:
    case query::QueryType::DUMP_INFO:
    case query::QueryType::DUMP_STATUS:
    case query::QueryType::SYNC:
    case query::QueryType::SYNC_STATUS:
    case query::QueryType::SYNC_STOP:
    case query::QueryType::OPTIMIZE:
    case query::QueryType::DEBUG_ON:
    case query::QueryType::DEBUG_OFF:
    case query::QueryType::CACHE_CLEAR:
    case query::QueryType::CACHE_STATS:
    case query::QueryType::CACHE_ENABLE:
    case query::QueryType::CACHE_DISABLE:
    case query::QueryType::SET:
    case query::QueryType::SHOW_VARIABLES:
    case query::QueryType::FACET:
      cmd_other_.fetch_add(1, std::memory_order_relaxed);
      break;
  }
}

void ServerStats::UpdateMemoryUsage(size_t current_bytes) {
  current_memory_.store(current_bytes);

  // Update peak if current is higher
  size_t current_peak = peak_memory_.load();
  while (current_bytes > current_peak) {
    if (peak_memory_.compare_exchange_weak(current_peak, current_bytes)) {
      break;
    }
  }
}

void ServerStats::IncrementConnections() {
  active_connections_++;
}

void ServerStats::DecrementConnections() {
  active_connections_--;
}

void ServerStats::IncrementTotalConnections() {
  total_connections_++;
}

void ServerStats::IncrementRequests() {
  total_requests_++;
}

Statistics ServerStats::GetStatistics() const {
  Statistics stats;

  // Command statistics
  stats.cmd_search = cmd_search_.load();
  stats.cmd_count = cmd_count_.load();
  stats.cmd_get = cmd_get_.load();
  stats.cmd_info = cmd_info_.load();
  stats.cmd_save = cmd_save_.load();
  stats.cmd_load = cmd_load_.load();
  stats.cmd_replication_status = cmd_replication_status_.load();
  stats.cmd_replication_stop = cmd_replication_stop_.load();
  stats.cmd_replication_start = cmd_replication_start_.load();
  stats.cmd_config = cmd_config_.load();
  stats.cmd_other = cmd_other_.load();
  stats.cmd_unknown = cmd_unknown_.load();

  stats.total_commands_processed = stats.cmd_search + stats.cmd_count + stats.cmd_get + stats.cmd_info +
                                   stats.cmd_save + stats.cmd_load + stats.cmd_replication_status +
                                   stats.cmd_replication_stop + stats.cmd_replication_start + stats.cmd_config +
                                   stats.cmd_other + stats.cmd_unknown;

  // Memory statistics
  stats.used_memory_bytes = current_memory_.load();
  stats.peak_memory_bytes = peak_memory_.load();

  // Connection statistics
  stats.active_connections = active_connections_.load();
  stats.total_connections_received = total_connections_.load();
  stats.total_requests = total_requests_.load();

  // Uptime
  stats.uptime_seconds = GetUptimeSeconds();

  // Replication event statistics
  stats.repl_inserts_applied = repl_inserts_applied_.load();
  stats.repl_inserts_skipped = repl_inserts_skipped_.load();
  stats.repl_updates_applied = repl_updates_applied_.load();
  stats.repl_updates_added = repl_updates_added_.load();
  stats.repl_updates_removed = repl_updates_removed_.load();
  stats.repl_updates_modified = repl_updates_modified_.load();
  stats.repl_updates_skipped = repl_updates_skipped_.load();
  stats.repl_deletes_applied = repl_deletes_applied_.load();
  stats.repl_deletes_skipped = repl_deletes_skipped_.load();
  stats.repl_ddl_executed = repl_ddl_executed_.load();
  stats.repl_events_skipped_other_tables = repl_events_skipped_other_tables_.load();

  return stats;
}

uint64_t ServerStats::GetUptimeSeconds() const {
  auto current_time = static_cast<uint64_t>(std::time(nullptr));
  return current_time - start_time_;
}

uint64_t ServerStats::GetTotalCommands() const {
  return cmd_search_.load() + cmd_count_.load() + cmd_get_.load() + cmd_info_.load() + cmd_save_.load() +
         cmd_load_.load() + cmd_replication_status_.load() + cmd_replication_stop_.load() +
         cmd_replication_start_.load() + cmd_config_.load() + cmd_other_.load() + cmd_unknown_.load();
}

uint64_t ServerStats::GetCommandCount(query::QueryType type) const {
  switch (type) {
    case query::QueryType::SEARCH:
      return cmd_search_.load();
    case query::QueryType::COUNT:
      return cmd_count_.load();
    case query::QueryType::GET:
      return cmd_get_.load();
    case query::QueryType::INFO:
      return cmd_info_.load();
    case query::QueryType::SAVE:
      return cmd_save_.load();
    case query::QueryType::LOAD:
      return cmd_load_.load();
    case query::QueryType::REPLICATION_STATUS:
      return cmd_replication_status_.load();
    case query::QueryType::REPLICATION_STOP:
      return cmd_replication_stop_.load();
    case query::QueryType::REPLICATION_START:
      return cmd_replication_start_.load();
    case query::QueryType::CONFIG_HELP:
    case query::QueryType::CONFIG_SHOW:
    case query::QueryType::CONFIG_VERIFY:
      return cmd_config_.load();
    case query::QueryType::UNKNOWN:
      return cmd_unknown_.load();
    // All "other" commands share cmd_other_; per-type breakdown is not tracked.
    case query::QueryType::DUMP_SAVE:
    case query::QueryType::DUMP_LOAD:
    case query::QueryType::DUMP_VERIFY:
    case query::QueryType::DUMP_INFO:
    case query::QueryType::DUMP_STATUS:
    case query::QueryType::SYNC:
    case query::QueryType::SYNC_STATUS:
    case query::QueryType::SYNC_STOP:
    case query::QueryType::OPTIMIZE:
    case query::QueryType::DEBUG_ON:
    case query::QueryType::DEBUG_OFF:
    case query::QueryType::CACHE_CLEAR:
    case query::QueryType::CACHE_STATS:
    case query::QueryType::CACHE_ENABLE:
    case query::QueryType::CACHE_DISABLE:
    case query::QueryType::SET:
    case query::QueryType::SHOW_VARIABLES:
    case query::QueryType::FACET:
      return cmd_other_.load();
  }
  return 0;
}

void ServerStats::Reset() {
  // Note: Reset() is not atomic across all counters. Concurrent readers may
  // observe a partially-reset state during the brief reset window. Callers
  // should account for this when using post-reset values as baselines.

  // Reset command counters
  cmd_search_.store(0);
  cmd_count_.store(0);
  cmd_get_.store(0);
  cmd_info_.store(0);
  cmd_save_.store(0);
  cmd_load_.store(0);
  cmd_replication_status_.store(0);
  cmd_replication_stop_.store(0);
  cmd_replication_start_.store(0);
  cmd_config_.store(0);
  cmd_other_.store(0);
  cmd_unknown_.store(0);

  // Reset memory statistics (current only; peak is a cumulative high-water mark)
  current_memory_.store(0);
  // Note: peak_memory_ is NOT reset — it is a cumulative high-water mark

  // Reset connection counters (cumulative only; active_connections_ represents
  // live state and must not be reset while connections are open)
  // Note: active_connections_ is NOT reset — it tracks live connection count
  total_connections_.store(0);
  total_requests_.store(0);

  // Reset replication event counters
  repl_inserts_applied_.store(0);
  repl_inserts_skipped_.store(0);
  repl_updates_applied_.store(0);
  repl_updates_added_.store(0);
  repl_updates_removed_.store(0);
  repl_updates_modified_.store(0);
  repl_updates_skipped_.store(0);
  repl_deletes_applied_.store(0);
  repl_deletes_skipped_.store(0);
  repl_ddl_executed_.store(0);
  repl_events_skipped_other_tables_.store(0);

  // Note: start_time_ is NOT reset
}

}  // namespace mygramdb::server
