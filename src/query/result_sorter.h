/**
 * @file result_sorter.h
 * @brief Result sorting utilities for query results
 */

#pragma once

#include "query/query_parser.h"
#include "storage/document_store.h"
#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>

namespace mygramdb {
namespace query {

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
  static std::vector<DocId> SortAndPaginate(
      std::vector<DocId>& results,
      const storage::DocumentStore& doc_store,
      const Query& query);

 private:
  /**
   * @brief Get sort value for a document
   *
   * @param doc_id Document ID
   * @param doc_store Document store
   * @param order_by ORDER BY clause
   * @return Sort key as string (for comparison)
   */
  static std::string GetSortKey(
      DocId doc_id,
      const storage::DocumentStore& doc_store,
      const OrderByClause& order_by);

  /**
   * @brief Compare function for sorting
   */
  struct SortComparator {
    const storage::DocumentStore& doc_store_;
    const OrderByClause& order_by_;
    bool ascending_;

    SortComparator(const storage::DocumentStore& ds,
                   const OrderByClause& ob)
        : doc_store_(ds), order_by_(ob),
          ascending_(ob.order == SortOrder::ASC) {}

    bool operator()(DocId a, DocId b) const;
  };
};

}  // namespace query
}  // namespace mygramdb
