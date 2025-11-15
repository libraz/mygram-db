/**
 * @file server_types.h
 * @brief Common server type definitions
 */

#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "server/server_stats.h"
#include "storage/document_store.h"
#include "utils/network_utils.h"

#ifdef USE_MYSQL
namespace mygramdb::mysql {
class BinlogReader;
}  // namespace mygramdb::mysql
#endif

namespace mygramdb::cache {
class CacheManager;
}  // namespace mygramdb::cache

namespace mygramdb::server {

// Forward declarations
class TableCatalog;

// Default constants
constexpr uint16_t kDefaultPort = 11016;       // memcached default port
constexpr int kDefaultMaxConnections = 10000;  // Maximum concurrent connections
constexpr int kDefaultRecvBufferSize = 4096;   // Receive buffer size
constexpr int kDefaultSendBufferSize = 65536;  // Send buffer size
constexpr int kDefaultLimit = 100;             // Default LIMIT for SEARCH queries (range: 5-1000)

/**
 * @brief TCP server configuration
 */
struct ServerConfig {
  std::string host = "127.0.0.1";
  uint16_t port = kDefaultPort;
  int max_connections = kDefaultMaxConnections;
  int worker_threads = 0;  // Number of worker threads (0 = CPU count)
  int recv_buffer_size = kDefaultRecvBufferSize;
  int send_buffer_size = kDefaultSendBufferSize;
  int default_limit = kDefaultLimit;  // Default LIMIT for SEARCH queries (range: 5-1000)
  int max_query_length = config::defaults::kDefaultQueryLengthLimit;  // Max characters for query expressions
  std::vector<std::string> allow_cidrs;
  std::vector<utils::CIDR> parsed_allow_cidrs;
};

/**
 * @brief Per-connection context
 */
struct ConnectionContext {
  int client_fd = -1;
  bool debug_mode = false;  // Debug mode flag
};

/**
 * @brief Table context managing resources for a single table
 */
struct TableContext {
  std::string name;
  config::TableConfig config;
  std::unique_ptr<index::Index> index;
  std::unique_ptr<storage::DocumentStore> doc_store;
  // Note: BinlogReader is shared across all tables (single GTID stream)
};

/**
 * @brief Context passed to command handlers
 *
 * Contains all necessary dependencies and state for command execution.
 * Reference members are intentional: this struct does not own the data,
 * it provides access to objects managed by TCPServer.
 */
struct HandlerContext {
  // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members) - Intentional design: context references external
  // state

  // Service-based access
  TableCatalog* table_catalog = nullptr;

  // Direct table access (maintained for backward compatibility)
  std::unordered_map<std::string, TableContext*>& table_contexts;

  ServerStats& stats;
  const config::Config* full_config;
  std::string dump_dir;
  std::atomic<bool>& loading;
  std::atomic<bool>& read_only;
  std::atomic<bool>& optimization_in_progress;
  // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
#ifdef USE_MYSQL
  mysql::BinlogReader* binlog_reader;
  std::unordered_set<std::string>&
      syncing_tables;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members) - External state reference
  std::mutex&
      syncing_tables_mutex;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members) - External state reference
#else
  void* binlog_reader;
#endif
  cache::CacheManager* cache_manager = nullptr;
};

}  // namespace mygramdb::server
