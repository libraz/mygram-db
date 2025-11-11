/**
 * @file server_stats.cpp
 * @brief Server statistics tracking implementation
 */

#include "server/server_stats.h"

#include <algorithm>

namespace mygramdb {
namespace server {

ServerStats::ServerStats() : start_time_(static_cast<uint64_t>(std::time(nullptr))) {}

void ServerStats::IncrementCommand(query::QueryType type) {
  switch (type) {
    case query::QueryType::SEARCH:
      cmd_search_++;
      break;
    case query::QueryType::COUNT:
      cmd_count_++;
      break;
    case query::QueryType::GET:
      cmd_get_++;
      break;
    case query::QueryType::INFO:
      cmd_info_++;
      break;
    case query::QueryType::SAVE:
      cmd_save_++;
      break;
    case query::QueryType::LOAD:
      cmd_load_++;
      break;
    case query::QueryType::REPLICATION_STATUS:
      cmd_replication_status_++;
      break;
    case query::QueryType::REPLICATION_STOP:
      cmd_replication_stop_++;
      break;
    case query::QueryType::REPLICATION_START:
      cmd_replication_start_++;
      break;
    case query::QueryType::CONFIG:
      cmd_config_++;
      break;
    case query::QueryType::OPTIMIZE:
      // Optimize doesn't need a counter
      break;
    case query::QueryType::DEBUG_ON:
    case query::QueryType::DEBUG_OFF:
      // Debug commands don't need counters
      break;
    case query::QueryType::UNKNOWN:
      cmd_unknown_++;
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
  stats.cmd_unknown = cmd_unknown_.load();

  stats.total_commands_processed =
      stats.cmd_search + stats.cmd_count + stats.cmd_get + stats.cmd_info + stats.cmd_save +
      stats.cmd_load + stats.cmd_replication_status + stats.cmd_replication_stop +
      stats.cmd_replication_start + stats.cmd_config + stats.cmd_unknown;

  // Memory statistics
  stats.used_memory_bytes = current_memory_.load();
  stats.peak_memory_bytes = peak_memory_.load();

  // Connection statistics
  stats.active_connections = active_connections_.load();
  stats.total_connections_received = total_connections_.load();
  stats.total_requests = total_requests_.load();

  // Uptime
  stats.uptime_seconds = GetUptimeSeconds();

  return stats;
}

uint64_t ServerStats::GetUptimeSeconds() const {
  auto current_time = static_cast<uint64_t>(std::time(nullptr));
  return current_time - start_time_;
}

uint64_t ServerStats::GetTotalCommands() const {
  return cmd_search_.load() + cmd_count_.load() + cmd_get_.load() + cmd_info_.load() +
         cmd_save_.load() + cmd_load_.load() + cmd_replication_status_.load() +
         cmd_replication_stop_.load() + cmd_replication_start_.load() + cmd_config_.load() +
         cmd_unknown_.load();
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
    case query::QueryType::CONFIG:
      return cmd_config_.load();
    case query::QueryType::OPTIMIZE:
    case query::QueryType::DEBUG_ON:
    case query::QueryType::DEBUG_OFF:
      return 0;  // These commands don't have counters
    case query::QueryType::UNKNOWN:
      return cmd_unknown_.load();
  }
  return 0;
}

void ServerStats::Reset() {
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
  cmd_unknown_.store(0);

  // Reset memory statistics
  current_memory_.store(0);
  peak_memory_.store(0);

  // Reset connection statistics
  active_connections_.store(0);
  total_connections_.store(0);
  total_requests_.store(0);

  // Note: start_time_ is NOT reset
}

}  // namespace server
}  // namespace mygramdb
