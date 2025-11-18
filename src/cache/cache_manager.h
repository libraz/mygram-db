/**
 * @file cache_manager.h
 * @brief Unified cache manager integrating all cache components
 */

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "cache/invalidation_manager.h"
#include "cache/invalidation_queue.h"
#include "cache/query_cache.h"
#include "config/config.h"
#include "query/query_normalizer.h"
#include "query/query_parser.h"

namespace mygramdb::server {
struct TableContext;
}  // namespace mygramdb::server

namespace mygramdb::cache {

/**
 * @brief Cache lookup result with metadata
 */
struct CacheLookupResult {
  std::vector<DocId> results;                        ///< Cached search results
  double query_cost_ms = 0.0;                        ///< Original query execution time
  std::chrono::steady_clock::time_point created_at;  ///< When cache entry was created
};

/**
 * @brief Unified cache manager
 *
 * Integrates QueryCache, InvalidationManager, and InvalidationQueue
 * to provide a simple API for caching and invalidation.
 */
class CacheManager {
 public:
  /**
   * @brief Constructor
   * @param cache_config Cache configuration
   * @param table_contexts Map of table name to TableContext pointer (for per-table ngram settings)
   */
  CacheManager(const config::CacheConfig& cache_config,
               const std::unordered_map<std::string, server::TableContext*>& table_contexts);

  /**
   * @brief Destructor
   */
  ~CacheManager();

  // Non-copyable, non-movable
  CacheManager(const CacheManager&) = delete;
  CacheManager& operator=(const CacheManager&) = delete;
  CacheManager(CacheManager&&) = delete;
  CacheManager& operator=(CacheManager&&) = delete;

  /**
   * @brief Check if cache is enabled
   */
  [[nodiscard]] bool IsEnabled() const { return enabled_; }

  /**
   * @brief Lookup cached query result
   * @param query Parsed query
   * @return Cached result if found and valid, nullopt otherwise
   */
  [[nodiscard]] std::optional<std::vector<DocId>> Lookup(const query::Query& query);

  /**
   * @brief Lookup cached query result with metadata
   * @param query Parsed query
   * @return Cached result with metadata if found and valid, nullopt otherwise
   */
  [[nodiscard]] std::optional<CacheLookupResult> LookupWithMetadata(const query::Query& query);

  /**
   * @brief Insert query result into cache
   * @param query Parsed query
   * @param result Search result
   * @param ngrams Ngrams used in query (for invalidation tracking)
   * @param query_cost_ms Query execution time
   * @return true if cached, false otherwise
   */
  bool Insert(const query::Query& query, const std::vector<DocId>& result, const std::set<std::string>& ngrams,
              double query_cost_ms);

  /**
   * @brief Invalidate cache entries affected by data modification
   * @param table_name Table that was modified
   * @param old_text Previous text content (empty if INSERT)
   * @param new_text New text content (empty if DELETE)
   */
  void Invalidate(const std::string& table_name, const std::string& old_text, const std::string& new_text);

  /**
   * @brief Clear all cache entries
   */
  void Clear();

  /**
   * @brief Clear cache entries for specific table
   * @param table_name Table name
   */
  void ClearTable(const std::string& table_name);

  /**
   * @brief Get cache statistics
   */
  [[nodiscard]] CacheStatisticsSnapshot GetStatistics() const;

  /**
   * @brief Enable cache
   * @return true if cache was enabled, false if cache was not initialized at startup
   *
   * Note: Cache can only be enabled if it was initialized at startup (cache.enabled = true).
   * If the server was started with cache disabled, this operation will fail.
   */
  bool Enable();

  /**
   * @brief Disable cache
   */
  void Disable();

 private:
  bool enabled_;

  std::unique_ptr<QueryCache> query_cache_;
  std::unique_ptr<InvalidationManager> invalidation_mgr_;
  std::unique_ptr<InvalidationQueue> invalidation_queue_;
};

}  // namespace mygramdb::cache
