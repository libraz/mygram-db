/**
 * @file result_sorter.cpp
 * @brief Result sorting implementation
 */

#include "query/result_sorter.h"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstring>
#include <iterator>
#include <optional>
#include <variant>

#include "query/query_parser_internal.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/structured_log.h"

namespace mygramdb::query {

using internal::EqualsIgnoreCase;

namespace {

// Format widths for zero-padded strings
constexpr int kDocIdWidth = 10;
constexpr int kNumericWidth = 20;
constexpr int kDoubleWidth = 20;

// Buffer sizes for string formatting (width + null terminator + safety margin)
constexpr size_t kDocIdBufferSize = kDocIdWidth + 2;  // Ensure enough space for width + null
constexpr size_t kNumericBufferSize = 32;

// Buffer size for std::to_chars: max uint64_t is 20 digits + null terminator
constexpr size_t kToCharsBufferSize = 21;

// Buffer size for std::from_chars parsing
constexpr size_t kFromCharsBufferSize = 21;

// Partial sort threshold: use partial_sort when needed elements < 50% of total
constexpr double kPartialSortThreshold = 0.5;

bool CompareDocIdTie(DocId lhs, DocId rhs, bool ascending) {
  return ascending ? (lhs < rhs) : (lhs > rhs);
}

template <typename SortEntryLike>
bool CompareSortEntries(const SortEntryLike& lhs, const SortEntryLike& rhs, bool ascending) {
  int cmp = lhs.sort_key.compare(rhs.sort_key);
  if (cmp != 0) {
    return ascending ? (cmp < 0) : (cmp > 0);
  }
  return CompareDocIdTie(lhs.doc_id, rhs.doc_id, ascending);
}

/**
 * @brief Convert uint64_t to zero-padded string using std::to_chars (locale-independent)
 *
 * This is ~10x faster than snprintf for parallel execution because:
 * - No locale lookup (localeconv_l)
 * - No os_unfair_lock contention
 *
 * @param num Number to convert
 * @param width Target width (will be zero-padded)
 * @return Zero-padded string
 */
inline std::string ToZeroPaddedString(uint64_t num, int width) {
  // Buffer large enough for max uint64_t (20 digits)
  std::array<char, kToCharsBufferSize> buf{};
  // std::to_chars requires pointer range; buf.data() + buf.size() is idiomatic end pointer
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  auto [ptr, ec] = std::to_chars(buf.data(), buf.data() + buf.size(), num);

  if (ec != std::errc()) {
    // Fallback (should never happen for valid uint64_t)
    return std::to_string(num);
  }

  auto num_digits = static_cast<size_t>(ptr - buf.data());
  auto target_width = static_cast<size_t>(width);

  if (num_digits >= target_width) {
    return {buf.data(), num_digits};
  }

  // Zero-pad on the left
  std::string result(target_width, '0');
  // std::copy destination requires pointer arithmetic for offset positioning
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  std::copy(buf.data(), ptr, result.data() + (target_width - num_digits));
  return result;
}

/**
 * @brief Convert uint32_t to zero-padded string using std::to_chars
 */
inline std::string ToZeroPaddedString(uint32_t num, int width) {
  return ToZeroPaddedString(static_cast<uint64_t>(num), width);
}

/**
 * @brief Convert signed integer to zero-padded string using sign-bit XOR
 *
 * Uses XOR with (1ULL << 63) to flip the sign bit, mapping:
 *   INT64_MIN → 0x0000000000000000 (smallest)
 *   -1        → 0x7FFFFFFFFFFFFFFF
 *   0         → 0x8000000000000000
 *   INT64_MAX → 0xFFFFFFFFFFFFFFFF (largest)
 *
 * This preserves total order and avoids signed overflow (undefined behavior).
 *
 * @param num Signed number to convert
 * @param width Target width (will be zero-padded)
 * @return Zero-padded string
 */
inline std::string ToZeroPaddedSignedString(int64_t num, int width) {
  auto adjusted = static_cast<uint64_t>(static_cast<uint64_t>(num) ^ (1ULL << 63));
  return ToZeroPaddedString(adjusted, width);
}

/**
 * @brief Convert double to a sort key string using IEEE 754 bit-level transformation
 *
 * Uses a bit-level transformation that preserves total order for doubles:
 * 1. Bit-cast the double to uint64_t via memcpy
 * 2. If negative (sign bit set): flip all bits (~bits)
 * 3. If positive (sign bit clear): flip only the sign bit (bits ^ (1ULL << 63))
 *
 * This maps negative doubles to lower uint64_t values and positive doubles to
 * higher values, preserving correct ordering within each group.
 *
 * @param value Double value to convert
 * @param width Target total width for zero-padded output
 * @return Zero-padded string representation that sorts correctly for all doubles
 */
inline std::string ToZeroPaddedDoubleString(double value, int width) {
  uint64_t bits = 0;
  std::memcpy(&bits, &value, sizeof(bits));

  if ((bits & (1ULL << 63)) != 0) {
    // Negative: flip all bits
    bits = ~bits;
  } else {
    // Positive (including +0): flip sign bit
    bits ^= (1ULL << 63);
  }

  return ToZeroPaddedString(bits, width);
}

/**
 * @brief Parse a numeric primary key string and return zero-padded sort key
 *
 * Uses std::from_chars for no-throw, no-allocation numeric parsing.
 *
 * @param pk_str Primary key string (must be all-digit)
 * @return Zero-padded string if successfully parsed, std::nullopt otherwise
 */
std::optional<std::string> ParseNumericPrimaryKey(std::string_view pk_str) {
  uint64_t num = 0;
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  auto [ptr, ec] = std::from_chars(pk_str.data(), pk_str.data() + pk_str.size(), num);
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  if (ec == std::errc() && ptr == pk_str.data() + pk_str.size()) {
    return ToZeroPaddedString(num, kNumericWidth);
  }
  return std::nullopt;
}

/**
 * @brief Convert a FilterValue variant to a sort-key string
 *
 * Centralizes the conversion logic used by GetSortKey, SortWithSchwartzianTransform,
 * and SortWithSchwartzianTransformPartial.
 *
 * Performance note: This converts numeric types to zero-padded strings for
 * lexicographic comparison. While direct numeric comparison would avoid O(N)
 * string allocations, the Schwartzian Transform requires a uniform key type
 * for the pre-computed key array. Supporting mixed-type sort keys (string vs
 * int64_t vs double) would require a tagged union or std::variant<string,
 * int64_t, double> with a custom comparator, adding complexity for marginal
 * gain. The bit-level transformations (sign-bit XOR for integers, IEEE 754
 * flip for doubles) preserve total order correctly in the string domain.
 * The SortComparator fallback path already uses direct numeric comparison
 * for primary key ordering (see operator()).
 */
static std::string FilterValueToSortKey(const storage::FilterValue& val) {
  return std::visit(
      [](const auto& arg) -> std::string {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          return "";  // NULL
        } else if constexpr (std::is_same_v<T, bool>) {
          return arg ? "1" : "0";
        } else if constexpr (std::is_same_v<T, std::string>) {
          return arg;
        } else if constexpr (std::is_same_v<T, storage::TimeValue>) {
          return ToZeroPaddedSignedString(arg.seconds, kNumericWidth);
        } else {
          if constexpr (std::is_same_v<T, double>) {
            return ToZeroPaddedDoubleString(arg, kDoubleWidth);
          } else if constexpr (std::is_signed_v<T>) {
            return ToZeroPaddedSignedString(static_cast<int64_t>(arg), kNumericWidth);
          } else {
            return ToZeroPaddedString(static_cast<uint64_t>(arg), kNumericWidth);
          }
        }
      },
      val);
}

/**
 * @brief Apply offset and limit to a sorted results vector
 *
 * @param results Sorted document IDs
 * @param offset Number of results to skip
 * @param limit Maximum number of results to return
 * @return Paginated slice of results
 */
static std::vector<DocId> ApplyOffsetLimit(const std::vector<DocId>& results, uint32_t offset, uint32_t limit) {
  size_t start = std::min(static_cast<size_t>(offset), results.size());
  size_t end = std::min(start + static_cast<size_t>(limit), results.size());
  return std::vector<DocId>(results.begin() + static_cast<std::ptrdiff_t>(start),
                            results.begin() + static_cast<std::ptrdiff_t>(end));
}

}  // namespace

std::string ResultSorter::GetSortKey(DocId doc_id, const storage::DocumentStore& doc_store,
                                     const OrderByClause& order_by, const std::string& primary_key_column) {
  // If ordering by primary key (empty column name or explicit primary key column name)
  if (order_by.IsPrimaryKey() || EqualsIgnoreCase(order_by.column, primary_key_column)) {
    // Get primary key as sort key
    auto pk_opt = doc_store.GetPrimaryKey(doc_id);
    if (pk_opt.has_value()) {
      const auto& pk_str = pk_opt.value();

      // Fast path: Numeric primary keys
      // Try to parse as numeric and convert to zero-padded string for correct lexicographic ordering
      if (!pk_str.empty() &&
          std::all_of(pk_str.begin(), pk_str.end(), [](unsigned char chr) { return std::isdigit(chr) != 0; })) {
        auto padded = ParseNumericPrimaryKey(pk_str);
        if (padded.has_value()) {
          return padded.value();
        }
      }

      // String primary keys: use as-is
      return pk_str;
    }
    // Fallback: use DocID itself (numeric)
    // Pre-pad to avoid repeated string allocation in comparator
    // Using std::to_chars (locale-independent, no lock contention)
    return ToZeroPaddedString(doc_id, kDocIdWidth);
  }

  // Ordering by filter column
  auto filter_val = doc_store.GetFilterValue(doc_id, order_by.column);
  if (!filter_val.has_value()) {
    // Filter column not found: treat as NULL (empty string)
    // NULL values sort first in ascending order, last in descending order
    // Note: PK column name match is already handled by the first if-block above.
    return "";
  }

  // Convert FilterValue to string for comparison
  return FilterValueToSortKey(filter_val.value());
}

void ResultSorter::PrecomputeSortKeys(const std::vector<DocId>& results, const storage::DocumentStore& doc_store,
                                      const OrderByClause& order_by, const std::string& primary_key_column,
                                      std::vector<SortEntry>& entries) {
  bool is_pk_order = order_by.IsPrimaryKey() || EqualsIgnoreCase(order_by.column, primary_key_column);

  if (is_pk_order) {
    // Batch primary key lookup: single lock acquisition for all keys
    auto primary_keys = doc_store.GetPrimaryKeysBatch(results);
    for (size_t i = 0; i < results.size(); ++i) {
      std::string sort_key;
      const auto& pk_str = primary_keys[i];
      if (!pk_str.empty()) {
        if (std::all_of(pk_str.begin(), pk_str.end(), [](unsigned char chr) { return std::isdigit(chr) != 0; })) {
          auto padded = ParseNumericPrimaryKey(pk_str);
          sort_key = padded.has_value() ? padded.value() : pk_str;
        } else {
          sort_key = pk_str;
        }
      } else {
        sort_key = ToZeroPaddedString(results[i], kDocIdWidth);
      }
      entries.push_back({results[i], std::move(sort_key)});
    }
  } else {
    // Batch filter value lookup: single lock acquisition
    auto batch_values = doc_store.GetFilterValuesBatch(results, order_by.column);
    for (size_t i = 0; i < results.size(); ++i) {
      std::string sort_key;
      if (batch_values[i].has_value()) {
        sort_key = FilterValueToSortKey(batch_values[i].value());
      }
      entries.push_back({results[i], std::move(sort_key)});
    }
  }
}

std::vector<DocId> ResultSorter::SortWithSchwartzianTransform(const std::vector<DocId>& results,
                                                              const storage::DocumentStore& doc_store,
                                                              const OrderByClause& order_by,
                                                              const std::string& primary_key_column) {
  // Schwartzian Transform: Pre-compute sort keys once, then sort
  // This eliminates repeated GetPrimaryKey() calls during O(N log N) comparisons
  //
  // Performance improvement:
  // - Before: N log N comparisons × 2 GetPrimaryKey() calls = 2N log N lock acquisitions
  // - After:  N GetPrimaryKey() calls + N log N string comparisons (no locks)
  // - For N=10,000: ~265,000 lock acquisitions → 10,000 lock acquisitions (96% reduction)

  std::vector<SortEntry> entries;
  try {
    entries.reserve(results.size());
  } catch (const std::bad_alloc&) {
    // Memory allocation failed - fall back to traditional sort
    mygram::utils::StructuredLog()
        .Event("sort_fallback")
        .Field("reason", "memory_allocation_failed")
        .Field("size", static_cast<uint64_t>(results.size()))
        .Warn();

    // Fallback: use traditional in-place sort (no pre-allocation needed)
    std::vector<DocId> sorted_results = results;
    SortComparator comparator(doc_store, order_by, primary_key_column);
    std::sort(sorted_results.begin(), sorted_results.end(), comparator);
    return sorted_results;
  }

  // Step 1: Pre-compute sort keys for all DocIDs (O(N) lookups)
  PrecomputeSortKeys(results, doc_store, order_by, primary_key_column, entries);

  // Step 2: Sort by pre-computed keys (O(N log N) string comparisons, no lock acquisitions)
  bool ascending = (order_by.order == SortOrder::ASC);
  std::sort(entries.begin(), entries.end(), [ascending](const SortEntry& lhs, const SortEntry& rhs) {
    return CompareSortEntries(lhs, rhs, ascending);
  });

  // Step 3: Extract sorted DocIDs (O(N))
  std::vector<DocId> sorted_results;
  sorted_results.reserve(entries.size());
  for (const auto& entry : entries) {
    sorted_results.push_back(entry.doc_id);
  }

  return sorted_results;
}

std::vector<DocId> ResultSorter::SortWithSchwartzianTransformPartial(const std::vector<DocId>& results,
                                                                     const storage::DocumentStore& doc_store,
                                                                     const OrderByClause& order_by,
                                                                     const std::string& primary_key_column,
                                                                     size_t top_k) {
  // Schwartzian Transform + partial_sort: eliminates lock contention during sorting
  //
  // Performance improvement for parallel execution:
  // - Before: N log K comparisons × 2 GetPrimaryKey() calls = 2N log K lock acquisitions per query
  // - After:  1 batch lookup (1 lock) + N log K string comparisons (no locks)
  // - For 100 parallel queries with N=10,000, K=100:
  //   Before: ~265,000 × 100 = 26.5M lock acquisitions total
  //   After:  100 lock acquisitions total (99.9996% reduction)

  if (results.empty() || top_k == 0) {
    return {};
  }

  // Clamp top_k to results size
  top_k = std::min(top_k, results.size());

  std::vector<SortEntry> entries;
  try {
    entries.reserve(results.size());
  } catch (const std::bad_alloc&) {
    // Memory allocation failed - OOM condition
    mygram::utils::StructuredLog()
        .Event("sort_fallback")
        .Field("reason", "memory_allocation_failed")
        .Field("size", static_cast<uint64_t>(results.size()))
        .Error();
    return {};  // Let caller handle fallback
  }

  // Step 1: Pre-compute sort keys for all DocIDs
  PrecomputeSortKeys(results, doc_store, order_by, primary_key_column, entries);

  // Step 2: partial_sort by pre-computed keys (O(N log K), no lock acquisitions)
  bool ascending = (order_by.order == SortOrder::ASC);
  std::partial_sort(
      entries.begin(), entries.begin() + static_cast<std::ptrdiff_t>(top_k), entries.end(),
      [ascending](const SortEntry& lhs, const SortEntry& rhs) { return CompareSortEntries(lhs, rhs, ascending); });

  // Step 3: Extract top K sorted DocIDs
  std::vector<DocId> sorted_results;
  sorted_results.reserve(top_k);
  for (size_t i = 0; i < top_k; ++i) {
    sorted_results.push_back(entries[i].doc_id);
  }

  return sorted_results;
}

std::vector<DocId> ResultSorter::SortWithBatchedSchwartzianTransformPartial(const std::vector<DocId>& results,
                                                                            const storage::DocumentStore& doc_store,
                                                                            const OrderByClause& order_by,
                                                                            const std::string& primary_key_column,
                                                                            size_t top_k) {
  if (results.empty() || top_k == 0) {
    return {};
  }

  top_k = std::min(top_k, results.size());
  const bool ascending = (order_by.order == SortOrder::ASC);
  const auto entry_less = [ascending](const SortEntry& lhs, const SortEntry& rhs) {
    return CompareSortEntries(lhs, rhs, ascending);
  };

  std::vector<SortEntry> top_entries;
  try {
    top_entries.reserve(top_k);
  } catch (const std::bad_alloc&) {
    mygram::utils::StructuredLog()
        .Event("sort_fallback")
        .Field("reason", "batched_topk_allocation_failed")
        .Field("top_k", static_cast<uint64_t>(top_k))
        .Error();
    return {};
  }

  for (size_t offset = 0; offset < results.size(); offset += kSchwartzianBatchSize) {
    size_t batch_size = std::min(kSchwartzianBatchSize, results.size() - offset);
    std::vector<DocId> batch(results.begin() + static_cast<std::ptrdiff_t>(offset),
                             results.begin() + static_cast<std::ptrdiff_t>(offset + batch_size));

    std::vector<SortEntry> batch_entries;
    try {
      batch_entries.reserve(batch.size());
    } catch (const std::bad_alloc&) {
      mygram::utils::StructuredLog()
          .Event("sort_fallback")
          .Field("reason", "batched_sort_allocation_failed")
          .Field("batch_size", static_cast<uint64_t>(batch.size()))
          .Error();
      return {};
    }

    PrecomputeSortKeys(batch, doc_store, order_by, primary_key_column, batch_entries);
    top_entries.insert(top_entries.end(), std::make_move_iterator(batch_entries.begin()),
                       std::make_move_iterator(batch_entries.end()));

    if (top_entries.size() > top_k) {
      std::partial_sort(top_entries.begin(), top_entries.begin() + static_cast<std::ptrdiff_t>(top_k),
                        top_entries.end(), entry_less);
      top_entries.resize(top_k);
    }
  }

  std::sort(top_entries.begin(), top_entries.end(), entry_less);

  std::vector<DocId> sorted_results;
  sorted_results.reserve(top_entries.size());
  for (const auto& entry : top_entries) {
    sorted_results.push_back(entry.doc_id);
  }

  return sorted_results;
}

bool ResultSorter::SortComparator::operator()(DocId lhs, DocId rhs) const {
  // Optimization: for primary key ordering, try numeric comparison first
  // This avoids string allocation for numeric primary keys
  // Check both empty column (shorthand) and explicit primary key column name
  if (order_by_.IsPrimaryKey() || EqualsIgnoreCase(order_by_.column, primary_key_column_)) {
    auto pk_lhs = doc_store_.GetPrimaryKey(lhs);
    auto pk_rhs = doc_store_.GetPrimaryKey(rhs);

    if (pk_lhs.has_value() && pk_rhs.has_value()) {
      const auto& str_lhs = pk_lhs.value();
      const auto& str_rhs = pk_rhs.value();

      // Fast path: both are pure numeric strings — use std::from_chars (locale-independent, lock-free)
      if (!str_lhs.empty() && !str_rhs.empty()) {
        uint64_t num_lhs = 0;
        uint64_t num_rhs = 0;
        auto [ptr_lhs, ec_lhs] = std::from_chars(str_lhs.data(), str_lhs.data() + str_lhs.size(), num_lhs);
        auto [ptr_rhs, ec_rhs] = std::from_chars(str_rhs.data(), str_rhs.data() + str_rhs.size(), num_rhs);
        if (ec_lhs == std::errc{} && ptr_lhs == str_lhs.data() + str_lhs.size() && ec_rhs == std::errc{} &&
            ptr_rhs == str_rhs.data() + str_rhs.size()) {
          if (num_lhs == num_rhs) {
            return CompareDocIdTie(lhs, rhs, ascending_);
          }
          return ascending_ ? (num_lhs < num_rhs) : (num_lhs > num_rhs);
        }
        // Parse failure (non-numeric or overflow): fall through to string comparison
      }

      // String comparison for non-numeric primary keys
      int cmp = str_lhs.compare(str_rhs);
      if (cmp == 0) {
        return CompareDocIdTie(lhs, rhs, ascending_);
      }
      return ascending_ ? (cmp < 0) : (cmp > 0);
    }

    // Fallback: use DocId if primary key not available
    return ascending_ ? (lhs < rhs) : (lhs > rhs);
  }

  // For filter columns, we need to get the filter values
  // This generates strings for numeric types, but it's unavoidable
  std::string key_lhs = GetSortKey(lhs, doc_store_, order_by_, primary_key_column_);
  std::string key_rhs = GetSortKey(rhs, doc_store_, order_by_, primary_key_column_);

  // String comparison
  int cmp = key_lhs.compare(key_rhs);
  if (cmp == 0) {
    return CompareDocIdTie(lhs, rhs, ascending_);
  }
  return ascending_ ? (cmp < 0) : (cmp > 0);
}

mygram::utils::Expected<std::vector<DocId>, mygram::utils::Error> ResultSorter::SortAndPaginate(
    std::vector<DocId>& results, const storage::DocumentStore& doc_store, const Query& query,
    const std::string& primary_key_column) {
  using mygram::utils::Error;
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  // No results to sort
  if (results.empty()) {
    return std::vector<DocId>{};
  }

  // Determine ORDER BY clause (default: primary key DESC)
  OrderByClause order_by;
  if (query.order_by.has_value()) {
    order_by = query.order_by.value();
  } else {
    // Default: primary key DESC
    order_by.column = "";  // Empty = primary key
    order_by.order = SortOrder::DESC;
  }

  if (order_by.IsScoreSort()) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "SORT _score requires BM25-aware search path"));
  }

  // Column validation: lightweight check (only first few documents)
  // - Primary key (empty column name or explicit primary key column name): always valid
  // - Filter column: check existence in a sample of documents
  // - Performance: O(min(N, sample_size)) instead of O(N)
  // - Thread safety: uses read lock, but column may be added/removed between check and sort
  //   (This is acceptable - non-existent columns are treated as NULL)
  bool is_primary_key_order = order_by.IsPrimaryKey() || EqualsIgnoreCase(order_by.column, primary_key_column);
  if (!is_primary_key_order && !results.empty()) {
    // Use HasFilterColumn for O(1) validation instead of sampling documents
    bool column_found_as_filter = doc_store.HasFilterColumn(order_by.column);
    bool column_found_as_primary_key = false;

    // If not found as filter column, check if the column name matches the primary key column
    // This allows sorting by primary key column name (e.g., SORT id DESC)
    if (!column_found_as_filter) {
      column_found_as_primary_key = (EqualsIgnoreCase(order_by.column, primary_key_column));
    }

    if (!column_found_as_filter && !column_found_as_primary_key) {
      // Error: column not found
      // Return error since this likely indicates a typo in the column name
      mygram::utils::StructuredLog()
          .Event("query_error")
          .Field("type", "order_by_column_not_found")
          .Field("column", order_by.column)
          .Error();

      return MakeUnexpected(MakeError(
          ErrorCode::kInvalidArgument,
          "Sort column '" + order_by.column +
              "' not found. Column does not exist as filter column or primary key. Check column name spelling."));
    }
    if (!column_found_as_filter && column_found_as_primary_key) {
      // Info: sorting by primary key column name (not filter column)
      mygram::utils::StructuredLog().Event("sort_by_primary_key_column").Field("column", order_by.column).Debug();
    }
  }

  // Performance optimization: use partial_sort when LIMIT is specified
  // This only sorts the top K elements instead of the entire array
  // For large datasets (e.g., 1M docs with 800K hits), this is critical
  // Check for overflow in offset + limit and clamp to results.size()
  uint64_t total_needed_64 = static_cast<uint64_t>(query.offset) + static_cast<uint64_t>(query.limit);
  // Clamp to min(total_needed_64, results.size()) to avoid out-of-bounds access
  size_t total_needed = (total_needed_64 > results.size()) ? results.size() : static_cast<size_t>(total_needed_64);

  // Use partial_sort aggressively when total_needed is significantly smaller than result size
  // Threshold: if we need less than 50% of results, use partial_sort
  // For 800K results with LIMIT 100, partial_sort is ~3x faster (O(N*log(K)) vs O(N*log(N)))
  auto results_size_double = static_cast<double>(results.size());
  auto total_needed_double = static_cast<double>(total_needed);
  bool use_partial_sort =
      (total_needed < results.size() && total_needed_double < results_size_double * kPartialSortThreshold);

  // Decide whether to use Schwartzian Transform optimization
  // Benefits: Eliminates repeated GetSortKey() calls (96%+ reduction in lock acquisitions)
  // Trade-offs: Requires O(N) temporary memory
  //
  // Use Schwartzian Transform when:
  // 1. Result set is large enough (N >= 100) to justify overhead
  // 2. Result set is not too large (N <= 5M) to avoid memory explosion
  //
  // Schwartzian Transform now supports both full sort and partial_sort.
  bool use_schwartzian =
      (results.size() >= kSchwartzianTransformThreshold && results.size() <= kSchwartzianTransformMaxSize);

  if (use_schwartzian && use_partial_sort) {
    // Schwartzian Transform + partial_sort: Best for parallel execution
    // Eliminates lock contention by pre-computing all sort keys
    // Memory: ~100 bytes per entry × N (temporary allocation)
    auto sorted_results =
        SortWithSchwartzianTransformPartial(results, doc_store, order_by, primary_key_column, total_needed);

    if (!sorted_results.empty()) {
      // Success - return directly (already paginated to total_needed)
      mygram::utils::StructuredLog()
          .Event("result_sort_strategy")
          .Field("strategy", "schwartzian_partial_sort")
          .Field("needed", static_cast<uint64_t>(total_needed))
          .Field("result_count", static_cast<uint64_t>(results.size()))
          .Trace();

      // Apply OFFSET within the partial-sorted results, with limit clamped to remaining size
      return ApplyOffsetLimit(sorted_results, query.offset, query.limit);
    }
    // Fall through to traditional partial_sort if Schwartzian failed
    mygram::utils::StructuredLog()
        .Event("result_sort_strategy")
        .Field("strategy", "schwartzian_failed_fallback")
        .Trace();
  } else if (use_schwartzian) {
    // Schwartzian Transform: Pre-compute sort keys, then full sort
    // Expected: 30-50% reduction in sort time for N >= 10,000
    // Memory: ~100 bytes per entry × N (temporary allocation)
    auto sorted_results = SortWithSchwartzianTransform(results, doc_store, order_by, primary_key_column);
    results = std::move(sorted_results);

    mygram::utils::StructuredLog()
        .Event("result_sort_strategy")
        .Field("strategy", "schwartzian_full_sort")
        .Field("result_count", static_cast<uint64_t>(results.size()))
        .Trace();
  } else if (use_partial_sort) {
    auto sorted_results =
        SortWithBatchedSchwartzianTransformPartial(results, doc_store, order_by, primary_key_column, total_needed);
    if (!sorted_results.empty()) {
      mygram::utils::StructuredLog()
          .Event("result_sort_strategy")
          .Field("strategy", "schwartzian_batched_partial_sort")
          .Field("needed", static_cast<uint64_t>(total_needed))
          .Field("result_count", static_cast<uint64_t>(results.size()))
          .Trace();
      return ApplyOffsetLimit(sorted_results, query.offset, query.limit);
    }

    SortComparator comparator(doc_store, order_by, primary_key_column);
    std::partial_sort(results.begin(), results.begin() + static_cast<std::ptrdiff_t>(total_needed), results.end(),
                      comparator);

    mygram::utils::StructuredLog()
        .Event("result_sort_strategy")
        .Field("strategy", "traditional_partial_sort")
        .Field("needed", static_cast<uint64_t>(total_needed))
        .Field("result_count", static_cast<uint64_t>(results.size()))
        .Trace();
  } else {
    // Full sort: O(N * log(N))
    // Use when result set is too small for Schwartzian Transform (< 100)
    // Lock contention is minimal for small result sets
    SortComparator comparator(doc_store, order_by, primary_key_column);
    std::sort(results.begin(), results.end(), comparator);

    mygram::utils::StructuredLog()
        .Event("result_sort_strategy")
        .Field("strategy", "traditional_full_sort")
        .Field("result_count", static_cast<uint64_t>(results.size()))
        .Trace();
  }

  // Apply OFFSET and LIMIT after sorting
  return ApplyOffsetLimit(results, query.offset, query.limit);
}

std::vector<DocId> ResultSorter::SortByScore(const std::vector<DocId>& results, const std::vector<double>& scores,
                                             SortOrder order, uint32_t limit, uint32_t offset) {
  if (results.empty()) {
    return {};
  }

  // Build index-score pairs
  struct ScoreEntry {
    size_t index;
    DocId doc_id;
    double score;
  };

  std::vector<ScoreEntry> entries;
  entries.reserve(results.size());
  for (size_t i = 0; i < results.size(); ++i) {
    entries.push_back({i, results[i], scores[i]});
  }

  bool descending = (order == SortOrder::DESC);
  auto comparator = [descending](const ScoreEntry& lhs, const ScoreEntry& rhs) {
    if (lhs.score == rhs.score) {
      return descending ? (lhs.doc_id > rhs.doc_id) : (lhs.doc_id < rhs.doc_id);
    }
    return descending ? (lhs.score > rhs.score) : (lhs.score < rhs.score);
  };

  // Compute total needed for partial sort
  uint64_t total_needed_64 = static_cast<uint64_t>(offset) + static_cast<uint64_t>(limit);
  size_t total_needed =
      (limit == 0 || total_needed_64 > entries.size()) ? entries.size() : static_cast<size_t>(total_needed_64);

  auto entries_size_double = static_cast<double>(entries.size());
  auto total_needed_double = static_cast<double>(total_needed);
  bool use_partial =
      (total_needed < entries.size() && total_needed_double < entries_size_double * kPartialSortThreshold);

  if (use_partial) {
    std::partial_sort(entries.begin(), entries.begin() + static_cast<std::ptrdiff_t>(total_needed), entries.end(),
                      comparator);
  } else {
    std::sort(entries.begin(), entries.end(), comparator);
  }

  // Apply offset and limit
  size_t start = std::min(static_cast<size_t>(offset), entries.size());
  size_t end = (limit == 0) ? entries.size() : std::min(start + static_cast<size_t>(limit), entries.size());

  std::vector<DocId> sorted_results;
  sorted_results.reserve(end - start);
  for (size_t i = start; i < end; ++i) {
    sorted_results.push_back(results[entries[i].index]);
  }

  return sorted_results;
}

}  // namespace mygramdb::query
