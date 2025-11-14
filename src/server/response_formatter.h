/**
 * @file response_formatter.h
 * @brief Response formatting utilities for TCP server
 */

#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "server/server_stats.h"
#include "server/server_types.h"
#include "storage/document_store.h"

#ifdef USE_MYSQL
namespace mygramdb::mysql {
class BinlogReader;
}  // namespace mygramdb::mysql
#endif

namespace mygramdb::server {

/**
 * @brief Utility class for formatting server responses
 *
 * All methods are static and stateless for easy testing and reuse.
 */
class ResponseFormatter {
 public:
  /**
   * @brief Format SEARCH response
   * @param results Search results (already sorted and paginated)
   * @param total_results Total number of results before pagination
   * @param doc_store Document store for retrieving primary keys
   * @param debug_info Optional debug information
   * @return Formatted response
   */
  static std::string FormatSearchResponse(const std::vector<index::DocId>& results, size_t total_results,
                                          storage::DocumentStore* doc_store,
                                          const query::DebugInfo* debug_info = nullptr);

  /**
   * @brief Format COUNT response
   * @param count Result count
   * @param debug_info Optional debug information
   * @return Formatted response
   */
  static std::string FormatCountResponse(uint64_t count, const query::DebugInfo* debug_info = nullptr);

  /**
   * @brief Format GET response
   * @param doc Document to format
   * @return Formatted response
   */
  static std::string FormatGetResponse(const std::optional<storage::Document>& doc);

  /**
   * @brief Format INFO response
   * @param table_contexts Map of table contexts
   * @param stats Server statistics
   * @param binlog_reader Optional binlog reader for replication info
   * @return Formatted response
   */
  static std::string FormatInfoResponse(const std::unordered_map<std::string, TableContext*>& table_contexts,
                                        ServerStats& stats,
#ifdef USE_MYSQL
                                        mysql::BinlogReader* binlog_reader = nullptr
#else
                                        void* binlog_reader = nullptr
#endif
  );

  /**
   * @brief Format SAVE response
   * @param filepath Path to saved snapshot
   * @return Formatted response
   */
  static std::string FormatSaveResponse(const std::string& filepath);

  /**
   * @brief Format LOAD response
   * @param filepath Path to loaded snapshot
   * @return Formatted response
   */
  static std::string FormatLoadResponse(const std::string& filepath);

  /**
   * @brief Format REPLICATION STATUS response
   * @param binlog_reader Binlog reader for status information
   * @return Formatted response
   */
  static std::string FormatReplicationStatusResponse(
#ifdef USE_MYSQL
      mysql::BinlogReader* binlog_reader
#else
      void* binlog_reader
#endif
  );

  /**
   * @brief Format REPLICATION STOP response
   * @return Formatted response
   */
  static std::string FormatReplicationStopResponse();

  /**
   * @brief Format REPLICATION START response
   * @return Formatted response
   */
  static std::string FormatReplicationStartResponse();

  /**
   * @brief Format CONFIG response
   * @param full_config Full configuration
   * @param connection_count Current connection count
   * @param max_connections Maximum connections allowed
   * @param read_only Read-only mode flag
   * @param uptime_seconds Server uptime in seconds
   * @return Formatted response
   */
  static std::string FormatConfigResponse(const config::Config* full_config, size_t connection_count,
                                          int max_connections, bool read_only, uint64_t uptime_seconds);

  /**
   * @brief Format Prometheus metrics response
   * @param table_contexts Map of table contexts
   * @param stats Server statistics
   * @param binlog_reader Optional binlog reader for replication info
   * @return Prometheus exposition format response
   */
  static std::string FormatPrometheusMetrics(const std::unordered_map<std::string, TableContext*>& table_contexts,
                                             ServerStats& stats,
#ifdef USE_MYSQL
                                             mysql::BinlogReader* binlog_reader = nullptr
#else
                                             void* binlog_reader = nullptr
#endif
  );

  /**
   * @brief Format error response
   * @param message Error message
   * @return Formatted response
   */
  static std::string FormatError(const std::string& message);
};

}  // namespace mygramdb::server
