/**
 * @file filter_index.h
 * @brief Bitmap-based filter index for fast filter evaluation
 *
 * Uses CRoaring bitmaps to pre-index filter column values,
 * enabling O(1) bitmap intersection instead of O(N) sequential scan.
 */

#pragma once

#include <roaring/roaring.h>

#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "storage/document_store.h"
#include "types/doc_id.h"
#include "utils/hash_utils.h"
#include "utils/roaring_bitmap_ptr.h"

namespace mygramdb::storage {

/**
 * @brief Bitmap-based filter index for EQ/NE filter acceleration
 *
 * Maintains per-(column, value) Roaring bitmaps for instant set operations.
 * Thread-safe: uses internal shared_mutex for concurrent reader/writer access.
 *
 * Memory overhead: ~500KB for 100K docs x 5 columns x 10 values.
 */
class FilterIndex {
 public:
  FilterIndex() = default;
  ~FilterIndex() = default;

  // Non-copyable (owns its bitmaps)
  FilterIndex(const FilterIndex&) = delete;
  FilterIndex& operator=(const FilterIndex&) = delete;
  FilterIndex(FilterIndex&&) = delete;
  FilterIndex& operator=(FilterIndex&&) = delete;

  /// Add doc_id to bitmaps for each filter value.
  /// A bitmap that cannot be allocated is reported instead of skipped: the
  /// entries this call had already applied are rolled back, so the document is
  /// either indexed under all of its filter values or under none of them.
  Expected<void, Error> AddDocument(DocId doc_id, const FilterMap& filters);

  /// Update bitmaps when filter values change.
  /// On failure the previous values are put back and the error is reported;
  /// the document is never left indexed under a partially applied new set.
  Expected<void, Error> UpdateDocument(DocId doc_id, const FilterMap& old_filters, const FilterMap& new_filters);

  /// Remove doc_id from all bitmaps for its filter values
  void RemoveDocument(DocId doc_id, const FilterMap& filters);

  /// Collect the doc ids indexed under (column, value) into @p doc_ids, ascending.
  /// The stored bitmap is read under the lock rather than copied, so there is no
  /// allocation whose failure could be mistaken for an unindexed value.
  /// @return true when the indexed value exists; false leaves @p doc_ids empty
  bool CollectEqDocIds(std::string_view column, std::string_view serialized_value, std::vector<DocId>& doc_ids) const;

  /// OR the stored bitmap for (column, value) directly into destination.
  /// Avoids copying a potentially large bitmap when callers probe and union
  /// several serialized type interpretations of the same query literal.
  /// @return true when the indexed value exists and was merged
  bool OrEqBitmapInto(std::string_view column, std::string_view serialized_value, roaring_bitmap_t* destination) const;

  /// @brief Check if a column exists in the filter index
  bool HasColumn(std::string_view column) const;

  /// Resolve a live column name with exact-match preference and otherwise
  /// unambiguous ASCII case-insensitive matching.
  [[nodiscard]] std::optional<std::string> ResolveColumnName(std::string_view column) const;

  /// Clear all bitmaps
  void Clear();

  /// Estimate memory usage
  [[nodiscard]] size_t MemoryUsage() const;

  /// Serialize FilterValue to a comparable string key (type tag + value bytes)
  static std::string SerializeFilterValue(const FilterValue& value);

  /// Get all (serialized_value, doc_count) pairs for a column, sorted by count DESC.
  /// Returns empty vector if column not found.
  [[nodiscard]] std::vector<std::pair<std::string, uint64_t>> GetColumnValueCounts(const std::string& column) const;

  /// Get (serialized_value, doc_count) pairs for a column, filtered by a result bitmap.
  /// Only includes values with non-zero count after filtering. Sorted by count DESC.
  /// @param column Filter column name
  /// @param filter_bitmap Roaring bitmap of allowed doc_ids (e.g., search results)
  [[nodiscard]] std::vector<std::pair<std::string, uint64_t>> GetColumnValueCountsFiltered(
      const std::string& column, const roaring_bitmap_t* filter_bitmap) const;

  /// Deserialize a serialized filter value back to a human-readable display string.
  /// The output matches the string format users see in FILTER clause values.
  static std::string DeserializeToDisplayString(const std::string& serialized);

 private:
  /// Add doc_id to bitmaps for given filters. Caller must hold unique_lock on mutex_.
  [[nodiscard]] Expected<void, Error> AddDocToBitmapsLocked(DocId doc_id, const FilterMap& filters);

  /// Remove doc_id from bitmaps for given filters. Caller must hold unique_lock on mutex_.
  void RemoveDocFromBitmapsLocked(DocId doc_id, const FilterMap& filters);

  /// Undo one (column, value) entry of a document. Caller must hold unique_lock on mutex_.
  void RemoveFilterEntryLocked(DocId doc_id, const std::string& column, const FilterValue& value);

  /// Undo the first @p visited entries of an interrupted add, in the same
  /// iteration order that applied them. Caller must hold unique_lock on mutex_.
  void RollbackPartialAddLocked(DocId doc_id, const FilterMap& filters, size_t visited);

  /// Protects all bitmap data from concurrent read/write access.
  /// Readers (CollectEqDocIds, OrEqBitmapInto, ResolveColumnName, MemoryUsage) take shared_lock;
  /// writers (AddDocument, UpdateDocument, RemoveDocument, Clear) take unique_lock.
  mutable std::shared_mutex mutex_;

  /// column_name -> { serialized_value -> owning bitmap handle }
  /// The mapped type owns its bitmap, so erasing an entry frees it.
  /// Uses transparent hash for heterogeneous lookup (string_view without allocation)
  using ValueBitmapMap =
      absl::flat_hash_map<std::string, mygram::utils::RoaringBitmapPtr, mygram::utils::TransparentStringHash,
                          mygram::utils::TransparentStringEqual>;
  absl::flat_hash_map<std::string, ValueBitmapMap, mygram::utils::TransparentStringHash,
                      mygram::utils::TransparentStringEqual>
      eq_bitmaps_;

  /// Number of live documents containing each column. Unlike eq_bitmaps_,
  /// this also tracks columns whose values are all NULL.
  absl::flat_hash_map<std::string, size_t, mygram::utils::TransparentStringHash, mygram::utils::TransparentStringEqual>
      column_ref_counts_;
};

}  // namespace mygramdb::storage
