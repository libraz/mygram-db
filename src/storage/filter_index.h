/**
 * @file filter_index.h
 * @brief Bitmap-based filter index for fast filter evaluation
 *
 * Uses CRoaring bitmaps to pre-index filter column values,
 * enabling O(1) bitmap intersection instead of O(N) sequential scan.
 */

#pragma once

#include <roaring/roaring.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "absl/hash/hash.h"
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
  /// Bitmap storage is prepared before any membership changes, so allocation
  /// failure leaves the index unchanged and is reported to the caller.
  Expected<void, Error> AddDocument(DocId doc_id, const FilterMap& filters);

  /// Update bitmaps when filter values change.
  /// On failure the previous values remain untouched and the error is reported;
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
  static constexpr size_t kSerializedValuePayloadBytes = sizeof(double) > sizeof(uint64_t) ? sizeof(double)
                                                                                           : sizeof(uint64_t);
  using SerializedValueStorage = std::array<char, kSerializedValuePayloadBytes>;

  struct SerializedValueProbe {
    uint8_t tag = 0;
    std::string_view payload;
  };

  struct SerializedValueHash {
    using is_transparent = void;

    size_t operator()(std::string_view serialized) const {
      const uint8_t tag = serialized.empty() ? 0 : static_cast<uint8_t>(static_cast<unsigned char>(serialized[0]));
      return absl::HashOf(tag, serialized.empty() ? std::string_view{} : serialized.substr(1));
    }
    size_t operator()(const std::string& serialized) const { return (*this)(std::string_view(serialized)); }
    size_t operator()(SerializedValueProbe probe) const { return absl::HashOf(probe.tag, probe.payload); }
  };

  struct SerializedValueEqual {
    using is_transparent = void;

    bool operator()(std::string_view lhs, std::string_view rhs) const { return lhs == rhs; }
    bool operator()(std::string_view serialized, SerializedValueProbe probe) const {
      return serialized.size() == probe.payload.size() + 1 &&
             static_cast<uint8_t>(static_cast<unsigned char>(serialized[0])) == probe.tag &&
             serialized.substr(1) == probe.payload;
    }
    bool operator()(SerializedValueProbe probe, std::string_view serialized) const {
      return (*this)(serialized, probe);
    }
  };

  /// Prepare then atomically apply filter membership changes. Caller holds the unique lock.
  [[nodiscard]] Expected<void, Error> ApplyFilterChangesLocked(DocId doc_id, const FilterMap* old_filters,
                                                               const FilterMap* new_filters);
  static SerializedValueProbe MakeSerializedValueProbe(const FilterValue& value, SerializedValueStorage& storage);

  /// Protects all bitmap data from concurrent read/write access.
  /// Readers (CollectEqDocIds, OrEqBitmapInto, ResolveColumnName, MemoryUsage) take shared_lock;
  /// writers (AddDocument, UpdateDocument, RemoveDocument, Clear) take unique_lock.
  mutable std::shared_mutex mutex_;

  /// column_name -> { serialized_value -> owning bitmap handle }
  /// The mapped type owns its bitmap, so erasing an entry frees it.
  /// Uses transparent hashing for serialized views and allocation-free FilterValue probes.
  using ValueBitmapMap =
      absl::flat_hash_map<std::string, mygram::utils::RoaringBitmapPtr, SerializedValueHash, SerializedValueEqual>;
  absl::flat_hash_map<std::string, ValueBitmapMap, mygram::utils::TransparentStringHash,
                      mygram::utils::TransparentStringEqual>
      eq_bitmaps_;

  /// Number of live documents containing each column. Unlike eq_bitmaps_,
  /// this also tracks columns whose values are all NULL.
  absl::flat_hash_map<std::string, size_t, mygram::utils::TransparentStringHash, mygram::utils::TransparentStringEqual>
      column_ref_counts_;
};

/**
 * @brief Convert one column value to the FilterValue of a configured filter type
 *
 * The single conversion shared by the initial load, replication and query-side
 * filter literals, so all three agree on what a value means and on which text
 * cannot be converted.
 *
 * @param value Column value (or query filter literal) as text
 * @param is_null True when the column is SQL NULL; yields a monostate
 * @param filter_type Configured filter type (e.g. "bigint", "boolean", "datetime")
 * @param datetime_timezone Offset DATETIME and DATE values are read in (e.g. "+09:00")
 * @return The converted value, or std::nullopt when @p value is not valid for @p filter_type
 */
std::optional<FilterValue> ConvertFilterValue(std::string_view value, bool is_null, std::string_view filter_type,
                                              const std::string& datetime_timezone = "+00:00");

}  // namespace mygramdb::storage
