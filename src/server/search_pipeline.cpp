/**
 * @file search_pipeline.cpp
 * @brief Shared search pipeline implementation
 */

#include "server/search_pipeline.h"

#include <roaring/roaring.h>

#include <algorithm>
#include <charconv>
#include <cmath>
#include <limits>
#include <memory>

#include "cache/cache_manager.h"
#include "config/config.h"
#include "storage/filter_index.h"
#include "utils/comparison_utils.h"
#include "utils/constants.h"
#include "utils/string_utils.h"

namespace mygramdb::server::search_pipeline {

std::vector<SearchTermInfo> GenerateTermInfos(const std::vector<std::string>& search_terms, index::Index* current_index,
                                              int ngram_size, int kanji_ngram_size, bool cross_boundary_ngrams) {
  std::vector<SearchTermInfo> term_infos;
  term_infos.reserve(search_terms.size());

  for (const auto& search_term : search_terms) {
    std::string normalized = current_index->NormalizeText(search_term);
    auto ngrams = mygram::utils::GenerateQueryNgrams(normalized, ngram_size, kanji_ngram_size, cross_boundary_ngrams);

    // Deduplicate n-grams to avoid redundant PostingList lookups
    mygram::utils::DeduplicateSorted(ngrams);

    // Estimate result size by checking the smallest posting list (thread-safe)
    size_t min_size = std::numeric_limits<size_t>::max();
    for (const auto& ngram : ngrams) {
      uint64_t posting_size = current_index->EstimatePostingSize(ngram);
      if (posting_size > 0) {
        min_size = std::min(min_size, static_cast<size_t>(posting_size));
      } else {
        min_size = 0;
        break;
      }
    }

    term_infos.push_back({std::move(ngrams), min_size});
  }

  return term_infos;
}

SearchPipelineResult Execute(const query::Query& query, const std::vector<SearchTermInfo>& term_infos,
                             const std::vector<std::string>& all_search_terms, index::Index* current_index,
                             storage::DocumentStore* current_doc_store, const config::Config* full_config,
                             int ngram_size, int kanji_ngram_size, bool cross_boundary, size_t filter_threshold) {
  SearchPipelineResult result;

  // Check for empty term (early exit)
  if (!term_infos.empty() && term_infos[0].estimated_size == 0) {
    result.empty_term_detected = true;
    return result;
  }

  // Perform intersection (AND of all terms)
  if (!term_infos.empty()) {
    result.results = current_index->SearchAnd(term_infos[0].ngrams);
    for (size_t i = 1; i < term_infos.size() && !result.results.empty(); ++i) {
      // Use filter approach when candidate set is small enough
      if (result.results.size() <= filter_threshold) {
        result.results = current_index->FilterByNgrams(result.results, term_infos[i].ngrams);
      } else {
        auto and_results = current_index->SearchAnd(term_infos[i].ngrams);
        std::vector<storage::DocId> intersection;
        intersection.reserve(std::min(result.results.size(), and_results.size()));
        std::set_intersection(result.results.begin(), result.results.end(), and_results.begin(), and_results.end(),
                              std::back_inserter(intersection));
        result.results = std::move(intersection);
      }
    }
  }

  // Apply NOT filter
  if (!query.not_terms.empty()) {
    result.results =
        ApplyNotFilter(result.results, query.not_terms, current_index, ngram_size, kanji_ngram_size, cross_boundary);
  }

  // Apply filter conditions
  if (!query.filters.empty()) {
    result.results = ApplyFiltersWithBitmap(result.results, query.filters, current_doc_store);
  }

  // Apply verify_text post-filter
  result.results =
      ApplyVerifyTextFilter(std::move(result.results), all_search_terms, current_index, current_doc_store, full_config);

  return result;
}

std::vector<storage::DocId> ApplyNotFilter(const std::vector<storage::DocId>& results,
                                           const std::vector<std::string>& not_terms, index::Index* current_index,
                                           int ngram_size, int kanji_ngram_size, bool cross_boundary_ngrams) {
  // Generate NOT term n-grams
  std::vector<std::string> not_ngrams;
  for (const auto& not_term : not_terms) {
    std::string norm_not = current_index->NormalizeText(not_term);
    auto ngrams = mygram::utils::GenerateQueryNgrams(norm_not, ngram_size, kanji_ngram_size, cross_boundary_ngrams);
    not_ngrams.insert(not_ngrams.end(), ngrams.begin(), ngrams.end());
  }

  // Deduplicate n-grams to avoid redundant PostingList lookups in SearchNot
  mygram::utils::DeduplicateSorted(not_ngrams);

  return current_index->SearchNot(results, not_ngrams);
}

namespace {

/// @brief Convert FilterOp enum to the string representation used by CompareValues
inline std::string_view FilterOpToString(query::FilterOp op) {
  switch (op) {
    case query::FilterOp::EQ:
      return "=";
    case query::FilterOp::NE:
      return "!=";
    case query::FilterOp::GT:
      return ">";
    case query::FilterOp::GTE:
      return ">=";
    case query::FilterOp::LT:
      return "<";
    case query::FilterOp::LTE:
      return "<=";
    default:
      return "";
  }
}

/// @brief RAII deleter for raw roaring_bitmap_t pointers
struct RoaringBitmapDeleter {
  void operator()(roaring_bitmap_t* p) const {
    if (p)
      roaring_bitmap_free(p);
  }
};
using RoaringBitmapPtr = std::unique_ptr<roaring_bitmap_t, RoaringBitmapDeleter>;

// Pre-parsed filter values to avoid repeated string parsing in the inner loop
struct ParsedFilterValue {
  double double_val = 0.0;
  int64_t int64_val = 0;
  uint64_t uint64_val = 0;
  bool bool_val = false;
  bool double_valid = false;
  bool int64_valid = false;
  bool uint64_valid = false;
};

inline ParsedFilterValue ParseFilterValue(const std::string& value) {
  ParsedFilterValue parsed_value;

  // Parse as bool
  parsed_value.bool_val = (value == "1" || value == "true");

  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic) - Required for from_chars range
  const char* end = value.data() + value.size();

  // Parse as double (locale-independent, no exceptions)
  {
    double result = 0.0;
    auto [ptr, ec] = std::from_chars(value.data(), end, result);
    if (ec == std::errc() && ptr == end) {
      parsed_value.double_val = result;
      parsed_value.double_valid = true;
    }
  }

  // Parse as int64_t (locale-independent, no exceptions)
  {
    int64_t result = 0;
    auto [ptr, ec] = std::from_chars(value.data(), end, result);
    if (ec == std::errc() && ptr == end) {
      parsed_value.int64_val = result;
      parsed_value.int64_valid = true;
    }
  }

  // Parse as uint64_t (locale-independent, no exceptions)
  {
    uint64_t result = 0;
    auto [ptr, ec] = std::from_chars(value.data(), end, result);
    if (ec == std::errc() && ptr == end) {
      parsed_value.uint64_val = result;
      parsed_value.uint64_valid = true;
    }
  }

  return parsed_value;
}

/// Check if all filter conditions can be accelerated with bitmap index
bool AllFiltersHaveBitmapSupport(const std::vector<query::FilterCondition>& filters) {
  for (const auto& filter : filters) {
    if (filter.op != query::FilterOp::EQ && filter.op != query::FilterOp::NE) {
      return false;
    }
  }
  return true;
}

/// Build a bitmap union of all type interpretations of a filter value string
RoaringBitmapPtr BuildTypeUnionBitmap(const storage::FilterIndex* filter_index, const std::string& column,
                                      const std::string& value) {
  RoaringBitmapPtr union_bm(roaring_bitmap_create());

  auto try_add = [&](const storage::FilterValue& fv) {
    std::string key = storage::FilterIndex::SerializeFilterValue(fv);
    auto bm = filter_index->GetEqBitmap(column, key);
    if (bm != nullptr) {
      roaring_bitmap_or_inplace(union_bm.get(), bm.get());
    }
  };

  // Try string
  try_add(storage::FilterValue{value});

  // Try bool
  if (value == "1" || value == "true") {
    try_add(storage::FilterValue{true});
  } else if (value == "0" || value == "false") {
    try_add(storage::FilterValue{false});
  }

  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  const char* end = value.data() + value.size();

  // Try int64_t and narrower signed types
  {
    int64_t val = 0;
    auto [ptr, ec] = std::from_chars(value.data(), end, val);
    if (ec == std::errc() && ptr == end) {
      try_add(storage::FilterValue{val});
      if (val >= INT8_MIN && val <= INT8_MAX) {
        try_add(storage::FilterValue{static_cast<int8_t>(val)});
      }
      if (val >= INT16_MIN && val <= INT16_MAX) {
        try_add(storage::FilterValue{static_cast<int16_t>(val)});
      }
      if (val >= INT32_MIN && val <= INT32_MAX) {
        try_add(storage::FilterValue{static_cast<int32_t>(val)});
      }
      // Try TimeValue (TIME columns stored as seconds)
      try_add(storage::FilterValue{storage::TimeValue{val}});
    }
  }

  // Try uint64_t and narrower unsigned types
  {
    uint64_t val = 0;
    auto [ptr, ec] = std::from_chars(value.data(), end, val);
    if (ec == std::errc() && ptr == end) {
      try_add(storage::FilterValue{val});
      if (val <= UINT8_MAX) {
        try_add(storage::FilterValue{static_cast<uint8_t>(val)});
      }
      if (val <= UINT16_MAX) {
        try_add(storage::FilterValue{static_cast<uint16_t>(val)});
      }
      if (val <= UINT32_MAX) {
        try_add(storage::FilterValue{static_cast<uint32_t>(val)});
      }
    }
  }

  // Try double
  {
    double val = 0.0;
    auto [ptr, ec] = std::from_chars(value.data(), end, val);
    if (ec == std::errc() && ptr == end) {
      try_add(storage::FilterValue{val});
    }
  }

  return union_bm;
}

}  // namespace

std::vector<storage::DocId> ApplyFilters(const std::vector<storage::DocId>& results,
                                         const std::vector<query::FilterCondition>& filters,
                                         storage::DocumentStore* doc_store) {
  std::vector<storage::DocId> filtered_results;
  filtered_results.reserve(results.size());

  // Pre-parse all filter values once before the main loop
  std::vector<ParsedFilterValue> parsed_values;
  parsed_values.reserve(filters.size());
  for (const auto& filter_cond : filters) {
    parsed_values.push_back(ParseFilterValue(filter_cond.value));
  }

  // Pre-fetch all filter values in batch (one lock acquisition per filter column)
  // instead of per-doc * per-filter individual GetFilterValue calls
  std::vector<std::vector<std::optional<storage::FilterValue>>> batch_filter_values;
  batch_filter_values.reserve(filters.size());
  for (const auto& filter_cond : filters) {
    batch_filter_values.push_back(doc_store->GetFilterValuesBatch(results, filter_cond.column));
  }

  for (size_t doc_idx = 0; doc_idx < results.size(); ++doc_idx) {
    bool matches_all_filters = true;

    for (size_t i = 0; i < filters.size(); ++i) {
      const auto& filter_cond = filters[i];
      const auto& parsed_value = parsed_values[i];
      const auto& stored_value = batch_filter_values[i][doc_idx];

      // NULL values: only match for NE operator
      if (!stored_value) {
        if (filter_cond.op != query::FilterOp::NE) {
          matches_all_filters = false;
          break;
        }
        continue;  // NULL != anything is true
      }

      // Evaluate filter condition based on operator
      auto op_str = FilterOpToString(filter_cond.op);
      bool matches = std::visit(
          [&](const auto& val) -> bool {
            using T = std::decay_t<decltype(val)>;
            if constexpr (std::is_same_v<T, std::monostate>) {
              // NULL value: handled above
              return filter_cond.op == query::FilterOp::NE;
            } else if constexpr (std::is_same_v<T, std::string>) {
              return mygram::utils::CompareValues(val, filter_cond.value, op_str);
            } else if constexpr (std::is_same_v<T, bool>) {
              // Boolean: only EQ/NE are meaningful
              return mygram::utils::CompareValues(val, parsed_value.bool_val, op_str);
            } else if constexpr (std::is_same_v<T, double>) {
              if (!parsed_value.double_valid) {
                return false;  // Invalid number
              }
              return mygram::utils::CompareDoubleValues(val, parsed_value.double_val, op_str,
                                                        mygram::constants::kFilterValueEpsilon);
            } else if constexpr (std::is_same_v<T, storage::TimeValue>) {
              if (!parsed_value.int64_valid) {
                return false;  // Invalid number
              }
              return mygram::utils::CompareValues(val.seconds, parsed_value.int64_val, op_str);
            } else if constexpr (std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t> ||
                                 std::is_same_v<T, uint16_t> || std::is_same_v<T, uint8_t>) {
              if (!parsed_value.uint64_valid) {
                return false;  // Invalid number
              }
              return mygram::utils::CompareValues(static_cast<uint64_t>(val), parsed_value.uint64_val, op_str);
            } else {
              if (!parsed_value.int64_valid) {
                return false;  // Invalid number
              }
              return mygram::utils::CompareValues(static_cast<int64_t>(val), parsed_value.int64_val, op_str);
            }
          },
          stored_value.value());

      if (!matches) {
        matches_all_filters = false;
        break;
      }
    }

    if (matches_all_filters) {
      filtered_results.push_back(results[doc_idx]);
    }
  }

  return filtered_results;
}

std::vector<storage::DocId> ApplyFiltersWithBitmap(const std::vector<storage::DocId>& results,
                                                   const std::vector<query::FilterCondition>& filters,
                                                   storage::DocumentStore* doc_store) {
  // Take a shared_ptr snapshot of filter_index -- keeps it alive even if
  // a concurrent writer replaces doc_store's filter_index_ pointer.
  auto filter_index = doc_store->GetFilterIndex();

  // Check if all filters can use bitmap acceleration
  if (filter_index == nullptr || !AllFiltersHaveBitmapSupport(filters)) {
    return ApplyFilters(results, filters, doc_store);
  }

  // Convert results vector to a temporary Roaring bitmap
  RoaringBitmapPtr result_bm(roaring_bitmap_create());
  roaring_bitmap_add_many(result_bm.get(), results.size(), results.data());

  for (const auto& filter : filters) {
    if (filter.op == query::FilterOp::EQ) {
      RoaringBitmapPtr match_bm = BuildTypeUnionBitmap(filter_index.get(), filter.column, filter.value);
      roaring_bitmap_and_inplace(result_bm.get(), match_bm.get());
    } else if (filter.op == query::FilterOp::NE) {
      RoaringBitmapPtr exclude_bm = BuildTypeUnionBitmap(filter_index.get(), filter.column, filter.value);
      roaring_bitmap_andnot_inplace(result_bm.get(), exclude_bm.get());
    }
  }

  // Convert bitmap back to sorted vector
  uint64_t cardinality = roaring_bitmap_get_cardinality(result_bm.get());
  std::vector<storage::DocId> filtered_results(cardinality);
  roaring_bitmap_to_uint32_array(result_bm.get(), filtered_results.data());

  return filtered_results;
}

std::vector<storage::DocId> PostFilterByText(const std::vector<storage::DocId>& candidates,
                                             const std::vector<std::string>& normalized_terms,
                                             storage::DocumentStore* doc_store) {
  std::vector<storage::DocId> verified;
  verified.reserve(candidates.size());
  for (auto doc_id : candidates) {
    auto text = doc_store->GetNormalizedText(doc_id);
    if (!text.has_value()) {
      // Text unavailable (e.g. after snapshot restore) -- include to avoid false negatives
      verified.push_back(doc_id);
      continue;
    }
    bool all_found = true;
    for (const auto& term : normalized_terms) {
      if (text->find(term) == std::string::npos) {
        all_found = false;
        break;
      }
    }
    if (all_found) {
      verified.push_back(doc_id);
    }
  }
  return verified;
}

std::vector<storage::DocId> ApplyVerifyTextFilter(std::vector<storage::DocId> results,
                                                  const std::vector<std::string>& search_terms,
                                                  index::Index* current_index, storage::DocumentStore* doc_store,
                                                  const config::Config* full_config) {
  if (results.empty() || full_config == nullptr) {
    return results;
  }
  const auto& verify_mode = full_config->memory.verify_text;
  if (verify_mode == "off") {
    return results;
  }
  // Check if verification should be applied based on mode
  bool should_verify = (verify_mode == "all");
  if (verify_mode == "ascii") {
    // Verify only if all search terms are ASCII
    should_verify = true;
    for (const auto& term : search_terms) {
      for (unsigned char ch : term) {
        if (ch >= mygram::constants::kFirstNonAsciiByte) {
          should_verify = false;
          break;
        }
      }
      if (!should_verify) {
        break;
      }
    }
  }
  if (!should_verify) {
    return results;
  }
  // Normalize each search term using the same settings as index
  std::vector<std::string> normalized_terms;
  normalized_terms.reserve(search_terms.size());
  for (const auto& term : search_terms) {
    normalized_terms.push_back(current_index->NormalizeText(term));
  }
  return PostFilterByText(results, normalized_terms, doc_store);
}

bool IsCacheStale(const std::vector<storage::DocId>& results, storage::DocumentStore* doc_store) {
  if (results.empty()) {
    return false;
  }
  // Sample doc IDs and check in batch (one lock acquisition instead of N individual ones)
  size_t sample_size = std::min(results.size(), std::max(size_t{10}, results.size() / 10));
  size_t step = std::max(size_t{1}, results.size() / sample_size);
  std::vector<storage::DocId> sampled_ids;
  sampled_ids.reserve(sample_size);
  for (size_t i = 0; i < results.size() && i / step < sample_size; i += step) {
    sampled_ids.push_back(results[i]);
  }
  auto pks = doc_store->GetPrimaryKeysBatch(sampled_ids);
  for (const auto& pk : pks) {
    if (pk.empty()) {
      return true;
    }
  }
  return false;
}

void InsertToCache(cache::CacheManager* cache_manager, const query::Query& query,
                   const std::vector<storage::DocId>& results, const std::vector<SearchTermInfo>& term_infos,
                   double query_time_ms, int ngram_size, int kanji_ngram_size, bool cross_boundary) {
  if (cache_manager == nullptr || !cache_manager->IsEnabled()) {
    return;
  }
  // Merge already-sorted ngram vectors from term_infos using set_union (O(N) merge)
  std::vector<std::string> all_ngrams;
  for (const auto& term_info : term_infos) {
    std::vector<std::string> merged;
    merged.reserve(all_ngrams.size() + term_info.ngrams.size());
    std::set_union(all_ngrams.begin(), all_ngrams.end(), term_info.ngrams.begin(), term_info.ngrams.end(),
                   std::back_inserter(merged));
    all_ngrams = std::move(merged);
  }
  cache_manager->Insert(query, results, all_ngrams, query_time_ms, ngram_size, kanji_ngram_size, cross_boundary);
}

}  // namespace mygramdb::server::search_pipeline
