/**
 * @file result_sorter.cpp
 * @brief Result sorting implementation
 */

#include "query/result_sorter.h"

#include <spdlog/spdlog.h>

#include <array>
#include <charconv>
#include <variant>

#include "utils/error.h"
#include "utils/expected.h"
#include "utils/structured_log.h"

namespace mygramdb::query {

namespace {

// Format widths for zero-padded strings
constexpr int kDocIdWidth = 10;
constexpr int kNumericWidth = 20;
constexpr int kDoubleWidth = 20;
constexpr int kDoublePrecision = 6;

// Buffer sizes for string formatting (width + null terminator + safety margin)
constexpr size_t kDocIdBufferSize = kDocIdWidth + 2;  // Ensure enough space for width + null
constexpr size_t kNumericBufferSize = 32;

// Buffer size for std::to_chars: max uint64_t is 20 digits + null terminator
constexpr size_t kToCharsBufferSize = 21;

// Signed integer offset to make all values positive for sorting
constexpr long long kSignedOffset = (1LL << 60);

// Partial sort threshold: use partial_sort when needed elements < 50% of total
constexpr double kPartialSortThreshold = 0.5;

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

}  // namespace

std::string ResultSorter::GetSortKey(DocId doc_id, const storage::DocumentStore& doc_store,
                                     const OrderByClause& order_by, const std::string& primary_key_column) {
  // If ordering by primary key (empty column name or explicit primary key column name)
  if (order_by.IsPrimaryKey() || order_by.column == primary_key_column) {
    // Get primary key as sort key
    auto pk_opt = doc_store.GetPrimaryKey(doc_id);
    if (pk_opt.has_value()) {
      const auto& pk_str = pk_opt.value();

      // Fast path: Numeric primary keys
      // Try to parse as numeric and convert to zero-padded string for correct lexicographic ordering
      if (!pk_str.empty() &&
          std::all_of(pk_str.begin(), pk_str.end(), [](unsigned char chr) { return std::isdigit(chr) != 0; })) {
        try {
          uint64_t num = std::stoull(pk_str);
          // Convert to zero-padded string (20 digits) for lexicographic comparison
          // This ensures: "00000000000000000001" < "00000000000000000002" < "00000000000000000010"
          // NOLINTBEGIN(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
          char buf[kNumericBufferSize];
          // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
          snprintf(buf, sizeof(buf), "%020llu", static_cast<unsigned long long>(num));
          // NOLINTEND(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
          return {buf};  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        } catch (const std::exception&) {
          // Overflow or parse error - fall through to string comparison
        }
      }

      // String primary keys: use as-is
      return pk_str;
    }
    // Fallback: use DocID itself (numeric)
    // Pre-pad to avoid repeated string allocation in comparator
    // Note: Using C-style buffer and snprintf for optimal performance in hot path
    // This sorting function is called millions of times, std::format is too slow
    char buf[kDocIdBufferSize];  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    snprintf(buf, sizeof(buf), "%0*u", kDocIdWidth, doc_id);
    return {buf};  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
  }

  // Ordering by filter column
  auto filter_val = doc_store.GetFilterValue(doc_id, order_by.column);
  if (!filter_val.has_value()) {
    // Filter column not found - check if the column name matches the primary key column
    // This allows sorting by primary key column name (e.g., SORT id DESC)
    if (order_by.column == primary_key_column) {
      // Treat as primary key sort
      auto pk_opt = doc_store.GetPrimaryKey(doc_id);
      if (pk_opt.has_value()) {
        const auto& pk_str = pk_opt.value();

        // Numeric primary keys: pad with zeros for proper lexicographic ordering
        if (!pk_str.empty() &&
            std::all_of(pk_str.begin(), pk_str.end(), [](unsigned char chr) { return std::isdigit(chr) != 0; })) {
          try {
            uint64_t num = std::stoull(pk_str);
            char buf[kNumericBufferSize];  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            snprintf(buf, sizeof(buf), "%020llu", static_cast<unsigned long long>(num));
            return {buf};  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
          } catch (const std::exception&) {
            // Overflow or parse error - fall through to string comparison
          }
        }

        // String primary key or numeric overflow: use as-is
        return pk_str;
      }
    }

    // Filter column not found and not primary key column: treat as NULL (empty string)
    // NULL values sort first in ascending order, last in descending order
    return "";
  }

  // Convert FilterValue to string for comparison
  // Use static buffer to reduce allocations for numeric types
  return std::visit(
      [](auto&& arg) -> std::string {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, std::monostate>) {
          return "";  // NULL
        } else if constexpr (std::is_same_v<T, bool>) {
          return arg ? "1" : "0";
        } else if constexpr (std::is_same_v<T, std::string>) {
          return arg;
        } else if constexpr (std::is_same_v<T, storage::TimeValue>) {
          // TimeValue: sort by seconds (can be negative)
          // Add offset to make all values positive for sorting
          char buf[kNumericBufferSize];  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
          // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
          snprintf(buf, sizeof(buf), "%0*lld", kNumericWidth, arg.seconds + kSignedOffset);
          return {buf};  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        } else {
          // Numeric types: pad with zeros for lexicographic comparison
          // This ensures proper numeric ordering
          // Note: Using C-style buffer and snprintf for optimal performance
          // This lambda is called millions of times in sorting, std::format is too slow
          char buf[kNumericBufferSize];  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
          if constexpr (std::is_same_v<T, double>) {
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            snprintf(buf, sizeof(buf), "%0*.*f", kDoubleWidth, kDoublePrecision, arg);
          } else if constexpr (std::is_signed_v<T>) {
            // Signed: add offset to make all values positive for sorting
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            snprintf(buf, sizeof(buf), "%0*lld", kNumericWidth, static_cast<long long>(arg) + kSignedOffset);
          } else {
            // Unsigned
            // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
            snprintf(buf, sizeof(buf), "%0*llu", kNumericWidth, static_cast<unsigned long long>(arg));
          }
          return {buf};  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
        }
      },
      filter_val.value());
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

  // Phase 1: Pre-compute sort keys for all DocIDs (O(N) lookups)
  // GetSortKey() handles both primary keys and filter columns
  for (const auto& doc_id : results) {
    std::string sort_key = GetSortKey(doc_id, doc_store, order_by, primary_key_column);
    entries.push_back({doc_id, std::move(sort_key)});
  }

  // Phase 2: Sort by pre-computed keys (O(N log N) string comparisons, no lock acquisitions)
  bool ascending = (order_by.order == SortOrder::ASC);
  std::sort(entries.begin(), entries.end(), [ascending](const SortEntry& lhs, const SortEntry& rhs) {
    int cmp = lhs.sort_key.compare(rhs.sort_key);
    return ascending ? (cmp < 0) : (cmp > 0);
  });

  // Phase 3: Extract sorted DocIDs (O(N))
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
    // Memory allocation failed - this shouldn't happen as caller checks size
    mygram::utils::StructuredLog()
        .Event("sort_fallback")
        .Field("reason", "memory_allocation_failed")
        .Field("size", static_cast<uint64_t>(results.size()))
        .Warn();
    return {};  // Let caller handle fallback
  }

  // Phase 1: Pre-compute sort keys for all DocIDs
  // Use batch lookup for primary key ordering (single lock acquisition)
  bool is_primary_key_order = order_by.IsPrimaryKey() || order_by.column == primary_key_column;

  if (is_primary_key_order) {
    // Batch primary key lookup: single lock acquisition for all keys
    auto primary_keys = doc_store.GetPrimaryKeysBatch(results);

    for (size_t i = 0; i < results.size(); ++i) {
      const auto& pk_str = primary_keys[i];
      std::string sort_key;

      if (!pk_str.empty()) {
        // Numeric primary keys: pad with zeros for lexicographic ordering
        if (std::all_of(pk_str.begin(), pk_str.end(), [](unsigned char chr) { return std::isdigit(chr) != 0; })) {
          try {
            uint64_t num = std::stoull(pk_str);
            // Use std::to_chars (locale-independent, no lock contention)
            sort_key = ToZeroPaddedString(num, kNumericWidth);
          } catch (const std::exception&) {
            sort_key = pk_str;  // Overflow - use as string
          }
        } else {
          sort_key = pk_str;  // String primary key
        }
      } else {
        // Fallback: use DocID as sort key (locale-independent)
        sort_key = ToZeroPaddedString(results[i], kDocIdWidth);
      }

      entries.push_back({results[i], std::move(sort_key)});
    }
  } else {
    // Filter column: individual lookups (still benefits from pre-computation)
    for (const auto& doc_id : results) {
      std::string sort_key = GetSortKey(doc_id, doc_store, order_by, primary_key_column);
      entries.push_back({doc_id, std::move(sort_key)});
    }
  }

  // Phase 2: partial_sort by pre-computed keys (O(N log K), no lock acquisitions)
  bool ascending = (order_by.order == SortOrder::ASC);
  std::partial_sort(entries.begin(), entries.begin() + static_cast<std::ptrdiff_t>(top_k), entries.end(),
                    [ascending](const SortEntry& lhs, const SortEntry& rhs) {
                      int cmp = lhs.sort_key.compare(rhs.sort_key);
                      return ascending ? (cmp < 0) : (cmp > 0);
                    });

  // Phase 3: Extract top K sorted DocIDs
  std::vector<DocId> sorted_results;
  sorted_results.reserve(top_k);
  for (size_t i = 0; i < top_k; ++i) {
    sorted_results.push_back(entries[i].doc_id);
  }

  return sorted_results;
}

bool ResultSorter::SortComparator::operator()(DocId lhs, DocId rhs) const {
  // Optimization: for primary key ordering, try numeric comparison first
  // This avoids string allocation for numeric primary keys
  // Check both empty column (shorthand) and explicit primary key column name
  if (order_by_.IsPrimaryKey() || order_by_.column == primary_key_column_) {
    auto pk_lhs = doc_store_.GetPrimaryKey(lhs);
    auto pk_rhs = doc_store_.GetPrimaryKey(rhs);

    if (pk_lhs.has_value() && pk_rhs.has_value()) {
      const auto& str_lhs = pk_lhs.value();
      const auto& str_rhs = pk_rhs.value();

      // Fast path: both are pure numeric strings
      if (!str_lhs.empty() && !str_rhs.empty() &&
          std::all_of(str_lhs.begin(), str_lhs.end(), [](unsigned char chr) { return std::isdigit(chr) != 0; }) &&
          std::all_of(str_rhs.begin(), str_rhs.end(), [](unsigned char chr) { return std::isdigit(chr) != 0; })) {
        try {
          uint64_t num_lhs = std::stoull(str_lhs);
          uint64_t num_rhs = std::stoull(str_rhs);
          return ascending_ ? (num_lhs < num_rhs) : (num_lhs > num_rhs);
        } catch (...) {
          // Overflow, fall through to string comparison
        }
      }

      // String comparison for non-numeric primary keys
      int cmp = str_lhs.compare(str_rhs);
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

  // Column validation: lightweight check (only first few documents)
  // - Primary key (empty column name or explicit primary key column name): always valid
  // - Filter column: check existence in a sample of documents
  // - Performance: O(min(N, sample_size)) instead of O(N)
  // - Thread safety: uses read lock, but column may be added/removed between check and sort
  //   (This is acceptable - non-existent columns are treated as NULL)
  bool is_primary_key_order = order_by.IsPrimaryKey() || order_by.column == primary_key_column;
  if (!is_primary_key_order && !results.empty()) {
    // Sample-based validation: check first 100 documents (or all if fewer)
    // This is a heuristic - if column doesn't exist in first 100, likely doesn't exist
    constexpr size_t kSampleSize = 100;
    size_t check_count = std::min(results.size(), kSampleSize);

    bool column_found_as_filter = false;
    bool column_found_as_primary_key = false;

    for (size_t i = 0; i < check_count; ++i) {
      auto filter_val = doc_store.GetFilterValue(results[i], order_by.column);
      if (filter_val.has_value()) {
        column_found_as_filter = true;
        break;
      }
    }

    // If not found as filter column, check if the column name matches the primary key column
    // This allows sorting by primary key column name (e.g., SORT id DESC)
    if (!column_found_as_filter) {
      column_found_as_primary_key = (order_by.column == primary_key_column);
    }

    if (!column_found_as_filter && !column_found_as_primary_key) {
      // Error: column not found in sample
      // Return error since this likely indicates a typo in the column name
      mygram::utils::StructuredLog()
          .Event("query_error")
          .Field("type", "order_by_column_not_found")
          .Field("column", order_by.column)
          .Field("check_count", static_cast<uint64_t>(check_count))
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
      spdlog::trace("Used Schwartzian Transform + partial_sort for {} out of {} results", total_needed, results.size());

      // Apply OFFSET (skip first query.offset elements)
      auto start = std::min(static_cast<size_t>(query.offset), sorted_results.size());
      auto end = sorted_results.size();
      auto start_iter = sorted_results.begin() + static_cast<ptrdiff_t>(start);
      auto end_iter = sorted_results.begin() + static_cast<ptrdiff_t>(end);
      return std::vector<DocId>{start_iter, end_iter};
    }
    // Fall through to traditional partial_sort if Schwartzian failed
    spdlog::trace("Schwartzian Transform failed, falling back to traditional partial_sort");
  } else if (use_schwartzian) {
    // Schwartzian Transform: Pre-compute sort keys, then full sort
    // Expected: 30-50% reduction in sort time for N >= 10,000
    // Memory: ~100 bytes per entry × N (temporary allocation)
    auto sorted_results = SortWithSchwartzianTransform(results, doc_store, order_by, primary_key_column);
    results = std::move(sorted_results);

    spdlog::trace("Used Schwartzian Transform for {} results", results.size());
  } else if (use_partial_sort) {
    // Traditional partial_sort: O(N * log(K)) where K = total_needed
    // Note: This path has lock contention issues with parallel execution
    // Used only for very large result sets (> 5M) to avoid memory explosion
    SortComparator comparator(doc_store, order_by, primary_key_column);
    std::partial_sort(results.begin(), results.begin() + static_cast<std::ptrdiff_t>(total_needed), results.end(),
                      comparator);

    spdlog::trace("Used traditional partial_sort for {} out of {} results (large dataset fallback)", total_needed,
                  results.size());
  } else {
    // Full sort: O(N * log(N))
    // Use when result set is too small for Schwartzian Transform (< 100)
    // Lock contention is minimal for small result sets
    SortComparator comparator(doc_store, order_by, primary_key_column);
    std::sort(results.begin(), results.end(), comparator);

    spdlog::trace("Used full sort for {} results", results.size());
  }

  // Apply OFFSET and LIMIT after sorting
  auto start = std::min(static_cast<size_t>(query.offset), results.size());
  auto end = std::min(start + static_cast<size_t>(query.limit), results.size());

  // Return paginated slice (minimal copy, only final results)
  auto start_iter = results.begin() + static_cast<ptrdiff_t>(start);
  auto end_iter = results.begin() + static_cast<ptrdiff_t>(end);
  return std::vector<DocId>{start_iter, end_iter};
}

}  // namespace mygramdb::query
