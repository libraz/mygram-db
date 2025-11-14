/**
 * @file table_catalog.h
 * @brief Centralized table resource catalog
 */

#pragma once

#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "index/index.h"
#include "server/server_types.h"
#include "server/statistics_service.h"
#include "storage/document_store.h"

namespace mygramdb::server {

/**
 * @brief Centralized table resource catalog
 *
 * This class encapsulates table context management and provides
 * a clean abstraction for common table operations. It eliminates
 * code duplication by providing reusable methods for table access,
 * conversion, and state management.
 *
 * Key responsibilities:
 * - Manage table contexts
 * - Provide encapsulated access patterns
 * - Convert table contexts for dump operations
 * - Centralize read_only and loading state management
 * - Delegate metric aggregation to StatisticsService
 *
 * Design principles:
 * - Single source of truth for table operations
 * - Thread-safe access using shared_mutex
 * - Consistent state management
 */
class TableCatalog {
 public:
  /**
   * @brief Construct a TableCatalog
   * @param tables Map of table contexts (ownership not transferred)
   */
  explicit TableCatalog(std::unordered_map<std::string, TableContext*> tables);

  // Disable copy and move (manages external resources)
  TableCatalog(const TableCatalog&) = delete;
  TableCatalog& operator=(const TableCatalog&) = delete;
  TableCatalog(TableCatalog&&) = delete;
  TableCatalog& operator=(TableCatalog&&) = delete;

  ~TableCatalog() = default;

  /**
   * @brief Get a table context by name
   * @param name Table name
   * @return Pointer to table context, or nullptr if not found
   */
  TableContext* GetTable(const std::string& name);

  /**
   * @brief Check if a table exists
   * @param name Table name
   * @return true if table exists
   */
  bool TableExists(const std::string& name) const;

  /**
   * @brief Get list of all table names
   * @return Vector of table names
   */
  std::vector<std::string> GetTableNames() const;

  /**
   * @brief Get dumpable contexts for snapshot operations
   *
   * This method eliminates the duplicated conversion loop that
   * appears in multiple locations (DumpHandler, AutoSaveThread, etc.)
   *
   * @return Map of table name to (Index*, DocumentStore*) pairs
   */
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> GetDumpableContexts() const;

  /**
   * @brief Set read-only mode for catalog
   * @param read_only Read-only flag
   */
  void SetReadOnly(bool read_only);

  /**
   * @brief Set loading mode for catalog
   * @param loading Loading flag
   */
  void SetLoading(bool loading);

  /**
   * @brief Check if catalog is in read-only mode
   * @return true if read-only
   */
  bool IsReadOnly() const { return read_only_.load(); }

  /**
   * @brief Check if catalog is in loading mode
   * @return true if loading
   */
  bool IsLoading() const { return loading_.load(); }

  /**
   * @brief Aggregate metrics across all tables
   *
   * This method delegates to StatisticsService for consistency.
   *
   * @return Aggregated metrics
   */
  AggregatedMetrics AggregateMetrics() const { return StatisticsService::AggregateMetrics(tables_); }

  /**
   * @brief Get direct access to table map (const only)
   *
   * This method is provided for cases where direct iteration is needed.
   * Prefer using the other methods when possible for better encapsulation.
   *
   * @return Const reference to table map
   */
  const std::unordered_map<std::string, TableContext*>& GetTables() const { return tables_; }

 private:
  std::unordered_map<std::string, TableContext*> tables_;
  std::atomic<bool> read_only_{false};
  std::atomic<bool> loading_{false};
  mutable std::shared_mutex mutex_;
};

}  // namespace mygramdb::server
