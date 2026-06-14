/**
 * @file table_catalog.h
 * @brief Centralized table resource catalog
 */

#pragma once

#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include "index/index.h"
#include "server/server_types.h"
#include "server/statistics_service.h"
#include "storage/document_store.h"

namespace mygramdb::server {

/**
 * @brief Resolve a (possibly bare) table identifier to a canonical map key.
 *
 * Tables are keyed by their qualified `database.table` identity. This shared
 * resolver lets both the TCP catalog and the HTTP server map a user-supplied
 * identifier onto the canonical key:
 *   - If @p tables contains @p name exactly, it is returned unchanged (covers
 *     already-qualified references and exact matches).
 *   - Otherwise, if @p name contains no '.', the map is scanned for keys of
 *     the form `<db>.<name>`. Exactly one match resolves to that key; zero or
 *     more than one (ambiguous) yields std::nullopt.
 *   - A qualified name that is not present yields std::nullopt.
 *
 * @param tables Map keyed by qualified `database.table` identity.
 * @param name   User-supplied table identifier (bare or qualified).
 * @return The canonical map key on success, std::nullopt otherwise.
 */
std::optional<std::string> ResolveTableKey(const std::unordered_map<std::string, TableContext*>& tables,
                                           std::string_view name);

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
 * - Immutable post-construction: the table map is built once at startup and
 *   never mutated, so read access requires no synchronization. Mutating
 *   per-catalog state (read-only / loading flags) uses std::atomic.
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
   * @brief Get a table context by name (const overload)
   *
   * Allows lookup through a `const TableCatalog&`. Returns a pointer-to-const
   * so callers cannot mutate the table context through this path.
   *
   * @param name Table name
   * @return Pointer to const table context, or nullptr if not found
   */
  const TableContext* GetTable(const std::string& name) const;

  /**
   * @brief Check if a table exists
   * @param name Table name
   * @return true if table exists
   */
  bool TableExists(const std::string& name) const;

  /**
   * @brief Resolve a (possibly bare) table identifier to its canonical key.
   *
   * Applies the same bare-to-qualified resolution as GetTable() but returns
   * the resolved qualified key instead of the context pointer. Useful where
   * downstream bookkeeping (e.g. SYNC status) compares against qualified keys.
   *
   * @param name Table identifier (bare or qualified).
   * @return The canonical qualified key, or std::nullopt if unresolved.
   */
  std::optional<std::string> ResolveName(const std::string& name) const;

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
   * @note Immutable post-construction; no synchronization needed for read
   *       access. If table hot-add/remove is ever implemented, this must
   *       return a snapshot copy instead, and the catalog must reintroduce
   *       a mutex (or other synchronization mechanism).
   *
   * @return Const reference to table map
   */
  const std::unordered_map<std::string, TableContext*>& GetTables() const { return tables_; }

 private:
  // tables_ is immutable post-construction. Access is safe without locking
  // from any thread; see class-level Doxygen.
  std::unordered_map<std::string, TableContext*> tables_;
  std::atomic<bool> read_only_{false};
  std::atomic<bool> loading_{false};
};

}  // namespace mygramdb::server
