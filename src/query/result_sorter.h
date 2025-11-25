/**
 * @file result_sorter.h
 * @brief Result sorting utilities for query results
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

#include "query/query_parser.h"
#include "storage/document_store.h"

namespace mygramdb::query {

using DocId = uint32_t;

/**
 * @brief Sort query results with performance optimization
 *
 * This class provides optimized sorting for query results:
 * - Uses partial_sort when LIMIT is specified (only sorts top N)
 * - Supports sorting by primary key or filter columns
 * - Applies LIMIT and OFFSET after sorting
 */
class ResultSorter {
 public:
  /**
   * @brief Sort and apply LIMIT/OFFSET to results
   *
   * Performance optimization:
   * - If LIMIT is specified: uses partial_sort (O(N*logK) where K=LIMIT)
   * - If no LIMIT: uses full sort (O(N*logN))
   * - Sorting happens BEFORE applying OFFSET/LIMIT
   * - Memory: in-place sorting, no additional memory allocation
   * - Thread-safe: uses read locks on DocumentStore
   *
   * Column validation:
   * - PRIMARY KEY: always valid
   * - Filter columns: sample-based validation (first 100 documents)
   * - Non-existent columns: treated as NULL values (logs warning)
   *
   * @param results Document IDs to sort (modified in-place)
   * @param doc_store Document store for retrieving sort values
   * @param query Query with ORDER BY, LIMIT, OFFSET clauses
   * @return Sorted and paginated document IDs
   */
  static mygram::utils::Expected<std::vector<DocId>, mygram::utils::Error> SortAndPaginate(
      std::vector<DocId>& results, const storage::DocumentStore& doc_store, const Query& query,
      const std::string& primary_key_column = "id");

 private:
  /**
   * @brief Threshold for using Schwartzian Transform optimization
   *
   * For result sets smaller than this threshold, the overhead of pre-computing
   * sort keys is not justified. Use traditional comparison-based sorting instead.
   *
   * Rationale:
   * - N < 100: ~664 comparisons → ~1,328 lookups (manageable)
   * - N >= 100: O(N log N) lookups vs O(N) pre-computation (significant win)
   */
  static constexpr size_t kSchwartzianTransformThreshold = 100;

  /**
   * @brief Maximum result size for Schwartzian Transform
   *
   * To prevent memory explosion, limit Schwartzian Transform to this size.
   * Beyond this, fall back to traditional sorting.
   *
   * Memory estimate: ~100 bytes per entry (DocId + string + overhead)
   * 5M entries ≈ 500MB temporary memory
   */
  static constexpr size_t kSchwartzianTransformMaxSize = 5'000'000;

  /**
   * @brief Entry for Schwartzian Transform (pre-computed sort key)
   */
  struct SortEntry {
    DocId doc_id;
    std::string sort_key;
  };

  /**
   * @brief Get sort value for a document
   *
   * @param doc_id Document ID
   * @param doc_store Document store
   * @param order_by ORDER BY clause
   * @param primary_key_column Primary key column name (for distinguishing PK sort from filter sort)
   * @return Sort key as string (for comparison)
   */
  static std::string GetSortKey(DocId doc_id, const storage::DocumentStore& doc_store, const OrderByClause& order_by,
                                const std::string& primary_key_column = "id");

  /**
   * @brief Sort using Schwartzian Transform (pre-compute sort keys)
   *
   * This optimization eliminates repeated GetPrimaryKey()/GetSortKey() calls
   * during sorting by pre-computing all sort keys once, then sorting based on
   * the pre-computed keys.
   *
   * Performance characteristics:
   * - Traditional sort: O(N log N) comparisons × O(1) lock+hash = O(N log N) lookups
   * - Schwartzian Transform: O(N) lookups + O(N log N) string comparisons
   *
   * Expected improvement: 30-50% reduction in sort time for N >= 10,000
   *
   * Memory overhead: ~50 bytes per entry × N (temporary allocation)
   *
   * @param results Document IDs to sort
   * @param doc_store Document store for retrieving sort keys
   * @param order_by ORDER BY clause (must be primary key)
   * @param primary_key_column Primary key column name (for distinguishing PK sort from filter sort)
   * @return Sorted document IDs
   */
  static std::vector<DocId> SortWithSchwartzianTransform(const std::vector<DocId>& results,
                                                         const storage::DocumentStore& doc_store,
                                                         const OrderByClause& order_by,
                                                         const std::string& primary_key_column = "id");

  /**
   * @brief Sort using Schwartzian Transform with partial_sort
   *
   * Combines Schwartzian Transform with partial_sort for optimal performance:
   * - Pre-computes sort keys once (O(N) lookups with single lock)
   * - Uses partial_sort (O(N log K) comparisons, no lock contention)
   *
   * This eliminates lock contention during parallel query execution.
   *
   * @param results Document IDs to sort (modified in-place)
   * @param doc_store Document store for retrieving sort keys
   * @param order_by ORDER BY clause
   * @param primary_key_column Primary key column name
   * @param top_k Number of elements to partially sort
   * @return Top K sorted document IDs
   */
  static std::vector<DocId> SortWithSchwartzianTransformPartial(const std::vector<DocId>& results,
                                                                const storage::DocumentStore& doc_store,
                                                                const OrderByClause& order_by,
                                                                const std::string& primary_key_column, size_t top_k);

  /**
   * @brief Compare function for sorting
   */
  struct SortComparator {
    const storage::DocumentStore& doc_store_;  // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    const OrderByClause& order_by_;            // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    const std::string& primary_key_column_;    // NOLINT(cppcoreguidelines-avoid-const-or-ref-data-members)
    bool ascending_;

    SortComparator(const storage::DocumentStore& doc_store, const OrderByClause& order_by,
                   const std::string& primary_key_column)
        : doc_store_(doc_store),
          order_by_(order_by),
          primary_key_column_(primary_key_column),
          ascending_(order_by.order == SortOrder::ASC) {}

    bool operator()(DocId lhs, DocId rhs) const;
  };
};

}  // namespace mygramdb::query
