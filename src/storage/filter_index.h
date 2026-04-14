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
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "storage/document_store.h"
#include "types/doc_id.h"

namespace mygramdb::storage {

/// RAII wrapper for roaring_bitmap_t* (auto-frees on destruction)
using RoaringBitmapPtr = std::unique_ptr<roaring_bitmap_t, decltype(&roaring_bitmap_free)>;

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
  ~FilterIndex();

  // Non-copyable (owns roaring_bitmap_t pointers)
  FilterIndex(const FilterIndex&) = delete;
  FilterIndex& operator=(const FilterIndex&) = delete;
  FilterIndex(FilterIndex&&) = delete;
  FilterIndex& operator=(FilterIndex&&) = delete;

  /// Add doc_id to bitmaps for each filter value
  void AddDocument(DocId doc_id, const FilterMap& filters);

  /// Update bitmaps when filter values change
  void UpdateDocument(DocId doc_id, const FilterMap& old_filters, const FilterMap& new_filters);

  /// Remove doc_id from all bitmaps for its filter values
  void RemoveDocument(DocId doc_id, const FilterMap& filters);

  /// Get a copy of bitmap for (column, value) pair. Returns null ptr if not found.
  /// The returned bitmap is an independent copy safe to use without holding any lock.
  [[nodiscard]] RoaringBitmapPtr GetEqBitmap(const std::string& column, const std::string& serialized_value) const;

  /// Clear all bitmaps
  void Clear();

  /// Estimate memory usage
  [[nodiscard]] size_t MemoryUsage() const;

  /// Serialize FilterValue to a comparable string key (type tag + value bytes)
  static std::string SerializeFilterValue(const FilterValue& value);

  /// Get all (serialized_value, doc_count) pairs for a column, sorted by count DESC.
  /// Returns empty vector if column not found.
  [[nodiscard]] std::vector<std::pair<std::string, uint64_t>> GetColumnValueCounts(
      const std::string& column) const;

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
  void AddDocToBitmapsLocked(DocId doc_id, const FilterMap& filters);

  /// Remove doc_id from bitmaps for given filters. Caller must hold unique_lock on mutex_.
  void RemoveDocFromBitmapsLocked(DocId doc_id, const FilterMap& filters);

  /// Protects all bitmap data from concurrent read/write access.
  /// Readers (GetEqBitmap, MemoryUsage) take shared_lock;
  /// writers (AddDocument, UpdateDocument, RemoveDocument, Clear) take unique_lock.
  mutable std::shared_mutex mutex_;

  /// column_name -> { serialized_value -> roaring_bitmap_t* }
  std::unordered_map<std::string, std::unordered_map<std::string, roaring_bitmap_t*>> eq_bitmaps_;
};

}  // namespace mygramdb::storage
