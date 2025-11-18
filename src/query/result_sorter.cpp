/**
 * @file result_sorter.cpp
 * @brief Result sorting implementation
 */

#include "query/result_sorter.h"

#include <spdlog/spdlog.h>

#include <variant>

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

// Signed integer offset to make all values positive for sorting
constexpr long long kSignedOffset = (1LL << 60);

// Partial sort threshold: use partial_sort when needed elements < 50% of total
constexpr double kPartialSortThreshold = 0.5;

}  // namespace

std::string ResultSorter::GetSortKey(DocId doc_id, const storage::DocumentStore& doc_store,
                                     const OrderByClause& order_by) {
  // If ordering by primary key (empty column name)
  if (order_by.IsPrimaryKey()) {
    // Get primary key as sort key
    auto pk_opt = doc_store.GetPrimaryKey(doc_id);
    if (pk_opt.has_value()) {
      return pk_opt.value();
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
    // NULL value: treat as empty string (sorts first)
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
                                                              const OrderByClause& order_by) {
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
    SortComparator comparator(doc_store, order_by);
    std::sort(sorted_results.begin(), sorted_results.end(), comparator);
    return sorted_results;
  }

  // Phase 1: Pre-compute sort keys for all DocIDs (O(N) lookups)
  for (const auto& doc_id : results) {
    if (!order_by.IsPrimaryKey()) {
      // Currently only primary key sorting is optimized
      // For filter columns, fall back to traditional sort
      // TODO(performance): Extend Schwartzian Transform to filter columns in Phase 2
      mygram::utils::StructuredLog()
          .Event("sort_fallback")
          .Field("reason", "filter_column_not_yet_optimized")
          .Field("column", order_by.column)
          .Warn();

      std::vector<DocId> sorted_results = results;
      SortComparator comparator(doc_store, order_by);
      std::sort(sorted_results.begin(), sorted_results.end(), comparator);
      return sorted_results;
    }

    // Get primary key
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
          snprintf(buf, sizeof(buf), "%020llu", num);
          // NOLINTEND(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
          entries.push_back({doc_id, std::string(buf)});  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
          continue;
        } catch (const std::exception&) {
          // Overflow or parse error - fall through to string comparison
        }
      }

      // String primary keys: use as-is
      entries.push_back({doc_id, pk_str});
    } else {
      // No primary key: use DocId itself (fallback)
      // NOLINTBEGIN(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
      char buf[kDocIdBufferSize];
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
      snprintf(buf, sizeof(buf), "%0*u", kDocIdWidth, doc_id);
      // NOLINTEND(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
      entries.push_back({doc_id, std::string(buf)});  // NOLINT(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    }
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

bool ResultSorter::SortComparator::operator()(DocId lhs, DocId rhs) const {
  // Optimization: for primary key ordering, try numeric comparison first
  // This avoids string allocation for numeric primary keys
  if (order_by_.IsPrimaryKey()) {
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
  std::string key_lhs = GetSortKey(lhs, doc_store_, order_by_);
  std::string key_rhs = GetSortKey(rhs, doc_store_, order_by_);

  // String comparison
  int cmp = key_lhs.compare(key_rhs);
  return ascending_ ? (cmp < 0) : (cmp > 0);
}

std::vector<DocId> ResultSorter::SortAndPaginate(std::vector<DocId>& results, const storage::DocumentStore& doc_store,
                                                 const Query& query) {
  // No results to sort
  if (results.empty()) {
    return {};
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
  // - Primary key (empty column name): always valid
  // - Filter column: check existence in a sample of documents
  // - Performance: O(min(N, sample_size)) instead of O(N)
  // - Thread safety: uses read lock, but column may be added/removed between check and sort
  //   (This is acceptable - non-existent columns are treated as NULL)
  if (!order_by.IsPrimaryKey() && !results.empty()) {
    // Sample-based validation: check first 100 documents (or all if fewer)
    // This is a heuristic - if column doesn't exist in first 100, likely doesn't exist
    constexpr size_t kSampleSize = 100;
    size_t check_count = std::min(results.size(), kSampleSize);

    bool column_found = false;
    for (size_t i = 0; i < check_count; ++i) {
      auto filter_val = doc_store.GetFilterValue(results[i], order_by.column);
      if (filter_val.has_value()) {
        column_found = true;
        break;
      }
    }

    if (!column_found) {
      // Warning: column not found in sample
      // Note: This is not a hard error - documents without the column will be sorted as NULL
      mygram::utils::StructuredLog()
          .Event("query_warning")
          .Field("type", "order_by_column_not_found")
          .Field("column", order_by.column)
          .Field("check_count", static_cast<uint64_t>(check_count))
          .Warn();
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
  // Benefits: Eliminates repeated GetPrimaryKey() calls (96% reduction in lock acquisitions)
  // Trade-offs: Requires O(N) temporary memory, only beneficial for N >= threshold
  //
  // Use Schwartzian Transform when:
  // 1. Result set is large enough (N >= 100) to justify overhead
  // 2. Sorting by primary key (currently only primary key is optimized)
  // 3. NOT using partial_sort (Schwartzian Transform returns new vector, incompatible with in-place partial_sort)
  //
  // TODO(performance): Combine Schwartzian Transform with partial_sort in future
  bool use_schwartzian =
      (results.size() >= kSchwartzianTransformThreshold && order_by.IsPrimaryKey() && !use_partial_sort);

  if (use_schwartzian) {
    // Schwartzian Transform: Pre-compute sort keys, then sort
    // Expected: 30-50% reduction in sort time for N >= 10,000
    // Memory: ~50 bytes per entry × N (temporary allocation)
    auto sorted_results = SortWithSchwartzianTransform(results, doc_store, order_by);
    results = std::move(sorted_results);

    spdlog::trace("Used Schwartzian Transform for {} results", results.size());
  } else if (use_partial_sort) {
    // partial_sort: O(N * log(K)) where K = total_needed
    // For 800K results with K=100: ~800K * log2(100) ≈ 5.3M operations
    // vs full sort: 800K * log2(800K) ≈ 15.9M operations
    // Memory: in-place, no additional allocation
    SortComparator comparator(doc_store, order_by);
    std::partial_sort(results.begin(), results.begin() + static_cast<std::ptrdiff_t>(total_needed), results.end(),
                      comparator);

    spdlog::trace("Used partial_sort for {} out of {} results", total_needed, results.size());
  } else {
    // Full sort: O(N * log(N))
    // Use when we need most of the results anyway OR result set is too small for Schwartzian Transform
    // Memory: in-place, no additional allocation
    SortComparator comparator(doc_store, order_by);
    std::sort(results.begin(), results.end(), comparator);

    spdlog::trace("Used full sort for {} results", results.size());
  }

  // Apply OFFSET and LIMIT after sorting
  auto start = std::min(static_cast<size_t>(query.offset), results.size());
  auto end = std::min(start + static_cast<size_t>(query.limit), results.size());

  // Return paginated slice (minimal copy, only final results)
  auto start_iter = results.begin() + static_cast<ptrdiff_t>(start);
  auto end_iter = results.begin() + static_cast<ptrdiff_t>(end);
  return {start_iter, end_iter};
}

}  // namespace mygramdb::query
