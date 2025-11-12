/**
 * @file server_stats.h
 * @brief Server statistics tracking (Redis-style)
 */

#pragma once

#include <atomic>
#include <cstdint>
#include <ctime>

#include "query/query_parser.h"

namespace mygramdb::server {

/**
 * @brief Server statistics snapshot
 *
 * Contains all statistics at a point in time (for INFO command)
 */
struct Statistics {
  // Command statistics
  uint64_t total_commands_processed = 0;
  uint64_t cmd_search = 0;
  uint64_t cmd_count = 0;
  uint64_t cmd_get = 0;
  uint64_t cmd_info = 0;
  uint64_t cmd_save = 0;
  uint64_t cmd_load = 0;
  uint64_t cmd_replication_status = 0;
  uint64_t cmd_replication_stop = 0;
  uint64_t cmd_replication_start = 0;
  uint64_t cmd_config = 0;
  uint64_t cmd_unknown = 0;

  // Memory statistics (bytes)
  size_t used_memory_bytes = 0;
  size_t peak_memory_bytes = 0;
  size_t used_memory_index = 0;
  size_t used_memory_documents = 0;

  // Connection statistics
  size_t active_connections = 0;
  uint64_t total_connections_received = 0;
  uint64_t total_requests = 0;

  // Uptime
  uint64_t uptime_seconds = 0;

  // Index statistics
  size_t total_documents = 0;
  size_t total_terms = 0;
  size_t total_postings = 0;
  size_t delta_encoded_lists = 0;
  size_t roaring_bitmap_lists = 0;
};

/**
 * @brief Thread-safe server statistics tracker
 *
 * Tracks Redis-style statistics including:
 * - Command counters (per command type)
 * - Memory usage (current and peak)
 * - Connection statistics
 * - Request statistics
 * - Uptime
 */
class ServerStats {
 public:
  ServerStats();
  ~ServerStats() = default;

  // Disable copy and move
  ServerStats(const ServerStats&) = delete;
  ServerStats& operator=(const ServerStats&) = delete;
  ServerStats(ServerStats&&) = delete;
  ServerStats& operator=(ServerStats&&) = delete;

  /**
   * @brief Increment command counter
   * @param type Command type
   */
  void IncrementCommand(query::QueryType type);

  /**
   * @brief Update memory usage and track peak
   * @param current_bytes Current memory usage in bytes
   */
  void UpdateMemoryUsage(size_t current_bytes);

  /**
   * @brief Increment active connection count
   */
  void IncrementConnections();

  /**
   * @brief Decrement active connection count
   */
  void DecrementConnections();

  /**
   * @brief Increment total connection counter
   */
  void IncrementTotalConnections();

  /**
   * @brief Increment total request counter
   */
  void IncrementRequests();

  /**
   * @brief Get current statistics snapshot
   * @return Statistics structure with current values
   */
  Statistics GetStatistics() const;

  /**
   * @brief Get server start time (Unix timestamp)
   */
  uint64_t GetStartTime() const { return start_time_; }

  /**
   * @brief Get uptime in seconds
   */
  uint64_t GetUptimeSeconds() const;

  /**
   * @brief Get total commands processed
   */
  uint64_t GetTotalCommands() const;

  /**
   * @brief Get command count for specific type
   * @param type Command type
   */
  uint64_t GetCommandCount(query::QueryType type) const;

  /**
   * @brief Get current memory usage
   */
  size_t GetCurrentMemoryUsage() const { return current_memory_.load(); }

  /**
   * @brief Get peak memory usage
   */
  size_t GetPeakMemoryUsage() const { return peak_memory_.load(); }

  /**
   * @brief Get active connection count
   */
  size_t GetActiveConnections() const { return active_connections_.load(); }

  /**
   * @brief Get total requests processed
   */
  uint64_t GetTotalRequests() const { return total_requests_.load(); }

  /**
   * @brief Reset all statistics (except start_time)
   */
  void Reset();

 private:
  // Server start time (Unix timestamp)
  uint64_t start_time_;

  // Command counters
  std::atomic<uint64_t> cmd_search_{0};
  std::atomic<uint64_t> cmd_count_{0};
  std::atomic<uint64_t> cmd_get_{0};
  std::atomic<uint64_t> cmd_info_{0};
  std::atomic<uint64_t> cmd_save_{0};
  std::atomic<uint64_t> cmd_load_{0};
  std::atomic<uint64_t> cmd_replication_status_{0};
  std::atomic<uint64_t> cmd_replication_stop_{0};
  std::atomic<uint64_t> cmd_replication_start_{0};
  std::atomic<uint64_t> cmd_config_{0};
  std::atomic<uint64_t> cmd_unknown_{0};

  // Memory statistics
  std::atomic<size_t> current_memory_{0};
  std::atomic<size_t> peak_memory_{0};

  // Connection statistics
  std::atomic<size_t> active_connections_{0};
  std::atomic<uint64_t> total_connections_{0};
  std::atomic<uint64_t> total_requests_{0};
};

}  // namespace mygramdb::server
