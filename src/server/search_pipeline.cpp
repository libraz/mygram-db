/**
 * @file search_pipeline.cpp
 * @brief Shared search pipeline implementation
 */

#include "server/search_pipeline.h"

#include <roaring/roaring.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <limits>
#include <memory>
#include <queue>

#include "cache/cache_key.h"
#include "cache/cache_manager.h"
#include "config/config.h"
#include "query/query_ast.h"
#include "query/query_normalizer.h"
#include "query/synonym_dictionary.h"
#include "server/server_types.h"
#include "storage/filter_index.h"
#include "utils/comparison_utils.h"
#include "utils/constants.h"
#include "utils/edit_distance.h"
#include "utils/roaring_bitmap_ptr.h"
#include "utils/string_utils.h"

namespace mygramdb::server::search_pipeline {

namespace {

/// Check if verify_text should be applied based on mode and search terms
/// @param verify_mode The verify_text config value ("off", "ascii", "all")
/// @param terms_begin Iterator to first term
/// @param terms_end Iterator past last term
/// @return true if verify_text should be applied
template <typename Iter>
bool ShouldApplyVerifyText(const std::string& verify_mode, Iter terms_begin, Iter terms_end) {
  if (verify_mode == "off") {
    return false;
  }
  if (verify_mode == "all") {
    return true;
  }
  if (verify_mode == "ascii") {
    for (auto it = terms_begin; it != terms_end; ++it) {
      for (unsigned char ch : *it) {
        if (ch >= mygram::constants::kFirstNonAsciiByte) {
          return false;
        }
      }
    }
    return true;
  }
  return false;
}

/// Overload for a flat vector of terms
bool ShouldApplyVerifyText(const std::string& verify_mode, const std::vector<std::string>& terms) {
  return ShouldApplyVerifyText(verify_mode, terms.begin(), terms.end());
}

/// Overload for synonym groups: flattens normalized_terms and delegates to the
/// template version to avoid duplicating the off/all/ascii logic.
bool ShouldApplyVerifyTextSynonyms(const std::string& verify_mode, const std::vector<SynonymTermGroup>& groups) {
  if (verify_mode == "off") {
    return false;
  }
  if (verify_mode == "all") {
    return true;
  }
  // "ascii" check: iterate all terms across all groups
  if (verify_mode == "ascii") {
    for (const auto& group : groups) {
      // Delegate per-group check to the template (reuses the same byte check logic)
      if (!ShouldApplyVerifyText(verify_mode, group.normalized_terms.begin(), group.normalized_terms.end())) {
        return false;
      }
    }
    return true;
  }
  return false;
}

bool ContainsBooleanSyntax(const std::string& search_text) {
  query::Tokenizer tokenizer(search_text);
  auto tokens = tokenizer.Tokenize();
  if (!tokenizer.GetError().empty()) {
    return false;
  }

  return std::any_of(tokens.begin(), tokens.end(), [](const query::Token& token) {
    return token.type == query::TokenType::AND || token.type == query::TokenType::OR ||
           token.type == query::TokenType::NOT || token.type == query::TokenType::LPAREN ||
           token.type == query::TokenType::RPAREN;
  });
}

void CollectAstTerms(const query::QueryNode& node, std::vector<std::string>& terms) {
  if (node.type == query::NodeType::TERM) {
    terms.push_back(node.term);
    return;
  }
  for (const auto& child : node.children) {
    if (child != nullptr) {
      CollectAstTerms(*child, terms);
    }
  }
}

/// Intersect accumulator with new sorted results (AND semantics).
/// On first call (is_first=true), moves new_results into accumulator.
/// Returns false if accumulator becomes empty (and is_first is false).
bool IntersectSorted(std::vector<storage::DocId>& accumulator, std::vector<storage::DocId>&& new_results,
                     bool& is_first) {
  if (is_first) {
    accumulator = std::move(new_results);
    is_first = false;
  } else if (!accumulator.empty() && !new_results.empty()) {
    std::vector<storage::DocId> intersection;
    intersection.reserve(std::min(accumulator.size(), new_results.size()));
    std::set_intersection(accumulator.begin(), accumulator.end(), new_results.begin(), new_results.end(),
                          std::back_inserter(intersection));
    accumulator = std::move(intersection);
  } else {
    accumulator.clear();
  }
  return !accumulator.empty();
}

/// Apply NOT filter and column filters (shared by all Execute* functions)
void ApplyNotAndFilters(SearchPipelineResult& result, const query::Query& query, index::Index* current_index,
                        storage::DocumentStore* current_doc_store, int ngram_size, int kanji_ngram_size,
                        bool cross_boundary) {
  if (!query.not_terms.empty()) {
    result.results =
        ApplyNotFilter(result.results, query.not_terms, current_index, ngram_size, kanji_ngram_size, cross_boundary);
  }
  if (!query.filters.empty()) {
    result.results = ApplyFiltersWithBitmap(result.results, query.filters, current_doc_store);
  }
}

query::Query WithCanonicalCacheKey(const query::Query& query, const FullPipelineParams& params) {
  query::Query cache_query = query;
  cache_query.cache_key.reset();

  auto text_normalizer = [&params](std::string_view text) {
    return params.current_index != nullptr ? params.current_index->NormalizeText(text) : std::string(text);
  };
  const std::string normalized =
      cache::QueryNormalizer::Normalize(cache_query, params.primary_key_column, text_normalizer);
  if (!normalized.empty()) {
    const cache::CacheKey key = cache::CacheKeyGenerator::Generate(normalized);
    cache_query.cache_key = std::make_pair(key.hash_high, key.hash_low);
    cache_query.cache_key_is_canonical = true;
  }

  return cache_query;
}

}  // namespace

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

    uint64_t term_doc_freq = 0;
    if (!ngrams.empty() && min_size > 0 && min_size != std::numeric_limits<size_t>::max()) {
      term_doc_freq = static_cast<uint64_t>(current_index->SearchAnd(ngrams).size());
    }

    term_infos.push_back({std::move(ngrams), min_size, term_doc_freq, std::move(normalized)});
  }

  return term_infos;
}

std::vector<std::string> MergeSortedTermNgramsForCache(const std::vector<SearchTermInfo>& term_infos) {
  struct HeapEntry {
    size_t term_index = 0;
    size_t ngram_index = 0;
  };

  struct Greater {
    const std::vector<SearchTermInfo>* term_infos = nullptr;

    bool operator()(const HeapEntry& lhs, const HeapEntry& rhs) const {
      return (*term_infos)[lhs.term_index].ngrams[lhs.ngram_index] >
             (*term_infos)[rhs.term_index].ngrams[rhs.ngram_index];
    }
  };

  size_t total_ngrams = 0;
  for (const auto& term_info : term_infos) {
    total_ngrams += term_info.ngrams.size();
  }

  std::vector<std::string> all_ngrams;
  all_ngrams.reserve(total_ngrams);

  std::priority_queue<HeapEntry, std::vector<HeapEntry>, Greater> heap(Greater{&term_infos});
  for (size_t term_index = 0; term_index < term_infos.size(); ++term_index) {
    if (!term_infos[term_index].ngrams.empty()) {
      heap.push(HeapEntry{term_index, 0});
    }
  }

  while (!heap.empty()) {
    HeapEntry entry = heap.top();
    heap.pop();

    const auto& ngram = term_infos[entry.term_index].ngrams[entry.ngram_index];
    if (all_ngrams.empty() || all_ngrams.back() != ngram) {
      all_ngrams.push_back(ngram);
    }

    ++entry.ngram_index;
    if (entry.ngram_index < term_infos[entry.term_index].ngrams.size()) {
      heap.push(entry);
    }
  }

  return all_ngrams;
}

SearchPipelineResult Execute(const query::Query& query, const std::vector<SearchTermInfo>& term_infos,
                             const std::vector<std::string>& all_search_terms, index::Index* current_index,
                             storage::DocumentStore* current_doc_store, const config::Config* full_config,
                             int ngram_size, int kanji_ngram_size, bool cross_boundary, size_t filter_threshold) {
  SearchPipelineResult result;

  // Check ALL terms for empty n-gram results (not just the first).
  // If any term has zero or max estimated size, the intersection result
  // is guaranteed to be empty, so skip the expensive search.
  for (const auto& ti : term_infos) {
    if (ti.estimated_size == 0 || ti.estimated_size == std::numeric_limits<size_t>::max()) {
      result.empty_term_detected = true;
      return result;
    }
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

  // Apply column filters
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
  // Reserve estimated capacity to avoid reallocation during NOT n-gram collection
  not_ngrams.reserve(not_terms.size() * static_cast<size_t>(ngram_size));
  for (const auto& not_term : not_terms) {
    std::string norm_not = current_index->NormalizeText(not_term);
    auto ngrams = mygram::utils::GenerateQueryNgrams(norm_not, ngram_size, kanji_ngram_size, cross_boundary_ngrams);
    not_ngrams.insert(not_ngrams.end(), std::make_move_iterator(ngrams.begin()), std::make_move_iterator(ngrams.end()));
  }

  // Deduplicate n-grams to avoid redundant PostingList lookups in SearchNot
  mygram::utils::DeduplicateSorted(not_ngrams);

  return current_index->SearchNot(results, not_ngrams);
}

namespace {

/// @brief Convert FilterOp enum to the string representation used by CompareValues.
/// NOTE: A separate FilterOpToString exists in QueryNormalizer for cache-key
/// normalization, with a different default ("=" vs "" here). These serve
/// different purposes and are intentionally separate.
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

// Reuse the shared RAII bitmap pointer from utils so this TU does not need
// a private copy of the deleter.
using mygram::utils::MakeEmptyRoaring;
using mygram::utils::MakeRoaringFromVector;
using mygram::utils::RoaringBitmapPtr;

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
  RoaringBitmapPtr union_bm = MakeEmptyRoaring();

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

  // Pre-fetch all filter values in a single lock acquisition (one shared lock
  // for all columns) instead of per-column locking
  std::vector<std::string> columns;
  columns.reserve(filters.size());
  for (const auto& filter_cond : filters) {
    columns.push_back(filter_cond.column);
  }
  auto batch_filter_values = doc_store->GetFilterValuesBatchMultiColumn(results, columns);

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
  RoaringBitmapPtr result_bm = MakeRoaringFromVector(results);

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
  // Batch fetch all texts in single lock acquisition (PERF-3)
  auto texts = doc_store->GetNormalizedTextBatch(candidates);

  std::vector<storage::DocId> verified;
  verified.reserve(candidates.size());
  for (size_t i = 0; i < candidates.size(); ++i) {
    if (!texts[i].has_value()) {
      // Text unavailable (e.g. after snapshot restore) -- include to avoid false negatives
      verified.push_back(candidates[i]);
      continue;
    }
    bool all_found = true;
    for (const auto& term : normalized_terms) {
      if (texts[i]->find(term) == std::string::npos) {
        all_found = false;
        break;
      }
    }
    if (all_found) {
      verified.push_back(candidates[i]);
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
  if (!ShouldApplyVerifyText(verify_mode, search_terms)) {
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

std::optional<CacheLookupResult> TryCacheLookup(const query::Query& query, cache::CacheManager* cache_manager,
                                                storage::DocumentStore* doc_store, CacheMissReason* miss_reason) {
  auto set_reason = [&](CacheMissReason r) {
    if (miss_reason != nullptr) {
      *miss_reason = r;
    }
  };

  if (cache_manager == nullptr || !cache_manager->IsEnabled()) {
    set_reason(CacheMissReason::kDisabled);
    return std::nullopt;
  }

  auto cached_lookup = cache_manager->LookupWithMetadata(query);
  if (!cached_lookup.has_value()) {
    set_reason(CacheMissReason::kNotFound);
    return std::nullopt;
  }

  // Check staleness against a const-ref to avoid copying the results vector
  // if the entry turns out to be stale (reviewed: not a dangling reference
  // since cached_lookup owns the data until the function returns).
  const auto& cached_entry = cached_lookup.value();
  if (IsCacheStale(cached_entry.results, doc_store)) {
    set_reason(CacheMissReason::kStale);
    return std::nullopt;
  }

  CacheLookupResult result;
  result.results = std::move(cached_lookup->results);
  auto now = std::chrono::steady_clock::now();
  result.cache_age_ms = std::chrono::duration<double, std::milli>(now - cached_entry.created_at).count();
  result.cache_saved_ms = cached_entry.query_cost_ms;
  set_reason(CacheMissReason::kHit);
  return result;
}

bool IsCacheStale(const std::vector<storage::DocId>& results, storage::DocumentStore* doc_store) {
  if (results.empty()) {
    return false;
  }
  // Sample doc IDs and check in batch (one lock acquisition instead of N individual ones)
  constexpr size_t kCacheStaleMinSamples = 10;     // Minimum number of samples to check
  constexpr size_t kCacheStaleSampleDivisor = 10;  // Sample ~10% of results
  size_t sample_size =
      std::min(results.size(), std::max(kCacheStaleMinSamples, results.size() / kCacheStaleSampleDivisor));
  size_t step = std::max(size_t{1}, results.size() / sample_size);
  std::vector<storage::DocId> sampled_ids;
  sampled_ids.reserve(sample_size);
  size_t count = 0;
  for (size_t i = 0; count < sample_size && i < results.size(); i += step, ++count) {
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
                   double query_time_ms, int ngram_size, int kanji_ngram_size, bool cross_boundary,
                   std::optional<uint64_t> data_version) {
  if (cache_manager == nullptr || !cache_manager->IsEnabled()) {
    return;
  }
  auto all_ngrams = MergeSortedTermNgramsForCache(term_infos);
  if (data_version.has_value()) {
    cache_manager->InsertIfVersion(query, results, all_ngrams, query_time_ms, *data_version, ngram_size,
                                   kanji_ngram_size, cross_boundary);
  } else {
    cache_manager->Insert(query, results, all_ngrams, query_time_ms, ngram_size, kanji_ngram_size, cross_boundary);
  }
}

std::vector<SynonymTermGroup> ExpandTermsWithSynonyms(const std::vector<std::string>& search_terms,
                                                      const query::SynonymDictionary* synonym_dict,
                                                      index::Index* current_index, int ngram_size, int kanji_ngram_size,
                                                      bool cross_boundary_ngrams) {
  std::vector<SynonymTermGroup> groups;
  groups.reserve(search_terms.size());

  for (const auto& term : search_terms) {
    SynonymTermGroup group;
    std::string normalized = current_index->NormalizeText(term);

    // Expand to include synonyms
    auto synonyms = synonym_dict->Expand(normalized);

    for (const auto& synonym : synonyms) {
      auto ngrams = mygram::utils::GenerateQueryNgrams(synonym, ngram_size, kanji_ngram_size, cross_boundary_ngrams);
      mygram::utils::DeduplicateSorted(ngrams);

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

      uint64_t term_doc_freq = 0;
      if (!ngrams.empty() && min_size > 0 && min_size != std::numeric_limits<size_t>::max()) {
        term_doc_freq = static_cast<uint64_t>(current_index->SearchAnd(ngrams).size());
      }

      group.variants.push_back({std::move(ngrams), min_size, term_doc_freq, synonym});
      group.normalized_terms.push_back(synonym);
    }

    groups.push_back(std::move(group));
  }

  return groups;
}

SearchPipelineResult ExecuteWithBooleanAst(const query::Query& query, const query::QueryNode& ast,
                                           index::Index* current_index, storage::DocumentStore* current_doc_store,
                                           int ngram_size, int kanji_ngram_size, bool cross_boundary) {
  SearchPipelineResult result;
  result.results = ast.Evaluate(*current_index, *current_doc_store);

  for (const auto& and_term : query.and_terms) {
    auto term_infos = GenerateTermInfos({and_term}, current_index, ngram_size, kanji_ngram_size, cross_boundary);
    if (term_infos.empty() || term_infos[0].ngrams.empty() || term_infos[0].estimated_size == 0) {
      result.results.clear();
      result.empty_term_detected = true;
      return result;
    }

    auto and_results = current_index->SearchAnd(term_infos[0].ngrams);
    std::vector<storage::DocId> intersection;
    std::set_intersection(result.results.begin(), result.results.end(), and_results.begin(), and_results.end(),
                          std::back_inserter(intersection));
    result.results = std::move(intersection);
    if (result.results.empty()) {
      break;
    }
  }

  ApplyNotAndFilters(result, query, current_index, current_doc_store, ngram_size, kanji_ngram_size, cross_boundary);
  return result;
}

SearchPipelineResult ExecuteWithSynonyms(const query::Query& query, const std::vector<SynonymTermGroup>& synonym_groups,
                                         index::Index* current_index, storage::DocumentStore* current_doc_store,
                                         const config::Config* full_config, int ngram_size, int kanji_ngram_size,
                                         bool cross_boundary, size_t filter_threshold) {
  (void)filter_threshold;  // Reserved for future optimization
  SearchPipelineResult result;
  bool first_group = true;

  for (const auto& group : synonym_groups) {
    // Union within this synonym group (OR semantics)
    std::vector<storage::DocId> group_results;

    for (const auto& variant : group.variants) {
      if (variant.ngrams.empty() || variant.estimated_size == 0) {
        continue;
      }

      auto var_results = current_index->SearchAnd(variant.ngrams);
      if (group_results.empty()) {
        group_results = std::move(var_results);
      } else {
        std::vector<storage::DocId> merged;
        merged.reserve(group_results.size() + var_results.size());
        std::set_union(group_results.begin(), group_results.end(), var_results.begin(), var_results.end(),
                       std::back_inserter(merged));
        group_results = std::move(merged);
      }
    }

    // Intersect across groups (AND semantics)
    IntersectSorted(result.results, std::move(group_results), first_group);
  }

  if (first_group) {
    result.empty_term_detected = true;
    return result;
  }

  // Apply NOT filter and column filters
  ApplyNotAndFilters(result, query, current_index, current_doc_store, ngram_size, kanji_ngram_size, cross_boundary);

  // Apply synonym-aware verify_text
  result.results =
      PostFilterByTextWithSynonyms(result.results, synonym_groups, current_index, current_doc_store, full_config);

  return result;
}

std::vector<storage::DocId> PostFilterByTextWithSynonyms(const std::vector<storage::DocId>& candidates,
                                                         const std::vector<SynonymTermGroup>& synonym_groups,
                                                         index::Index* current_index, storage::DocumentStore* doc_store,
                                                         const config::Config* full_config) {
  (void)current_index;  // Available for future normalization needs

  if (candidates.empty() || full_config == nullptr) {
    return candidates;
  }

  const auto& verify_mode = full_config->memory.verify_text;
  if (!ShouldApplyVerifyTextSynonyms(verify_mode, synonym_groups)) {
    return candidates;
  }

  // Batch fetch all texts in single lock acquisition
  auto texts = doc_store->GetNormalizedTextBatch(candidates);

  std::vector<storage::DocId> verified;
  verified.reserve(candidates.size());

  for (size_t i = 0; i < candidates.size(); ++i) {
    if (!texts[i].has_value()) {
      verified.push_back(candidates[i]);
      continue;
    }

    bool all_groups_match = true;
    for (const auto& group : synonym_groups) {
      bool any_synonym_found = false;
      for (const auto& term : group.normalized_terms) {
        if (texts[i]->find(term) != std::string::npos) {
          any_synonym_found = true;
          break;
        }
      }
      if (!any_synonym_found) {
        all_groups_match = false;
        break;
      }
    }

    if (all_groups_match) {
      verified.push_back(candidates[i]);
    }
  }

  return verified;
}

SearchPipelineResult ExecuteWithFuzzy(const query::Query& query, const std::vector<SearchTermInfo>& term_infos,
                                      const std::vector<std::string>& all_search_terms, uint32_t max_distance,
                                      index::Index* current_index, storage::DocumentStore* current_doc_store,
                                      const config::Config* full_config, int ngram_size, int kanji_ngram_size,
                                      bool cross_boundary, size_t filter_threshold) {
  (void)filter_threshold;  // Reserved for future optimization
  SearchPipelineResult result;

  if (term_infos.empty()) {
    result.empty_term_detected = true;
    return result;
  }

  bool first_term = true;
  for (const auto& ti : term_infos) {
    if (ti.ngrams.empty()) {
      // Term too short for n-gram generation -- no candidates can match it
      result.results.clear();
      result.empty_term_detected = true;
      first_term = false;
      break;
    }

    // Compute effective n-gram size per-term: use kanji_ngram_size for CJK-dominant terms
    int effective_ngram_size_for_term = ngram_size > 0 ? ngram_size : 2;
    if (kanji_ngram_size > 0) {
      // If most ngrams are short (CJK unigram), use kanji_ngram_size
      size_t short_count = 0;
      for (const auto& ng : ti.ngrams) {
        if (ng.size() <= 3) {
          ++short_count;  // CJK chars are up to 3 bytes in UTF-8
        }
      }
      if (short_count > ti.ngrams.size() / 2) {
        effective_ngram_size_for_term = kanji_ngram_size;
      }
    }

    // Compute threshold: max(1, |ngrams| - max_distance * ngram_size)
    size_t ngram_count = ti.ngrams.size();
    size_t drop = static_cast<size_t>(max_distance) * static_cast<size_t>(effective_ngram_size_for_term);
    size_t threshold = (ngram_count > drop) ? (ngram_count - drop) : 1;

    auto term_results = current_index->SearchByThreshold(ti.ngrams, threshold);

    // AND across terms
    IntersectSorted(result.results, std::move(term_results), first_term);
  }

  if (first_term) {
    // No terms were processed at all (shouldn't happen since we check term_infos.empty() above)
    result.empty_term_detected = true;
    return result;
  }

  // Apply NOT filter and column filters
  ApplyNotAndFilters(result, query, current_index, current_doc_store, ngram_size, kanji_ngram_size, cross_boundary);

  // Apply fuzzy verify_text post-filter
  if (!result.results.empty() && full_config != nullptr) {
    const auto& verify_mode = full_config->memory.verify_text;
    if (ShouldApplyVerifyText(verify_mode, all_search_terms)) {
      std::vector<std::string> normalized_terms;
      normalized_terms.reserve(all_search_terms.size());
      for (const auto& term : all_search_terms) {
        normalized_terms.push_back(current_index->NormalizeText(term));
      }
      result.results = PostFilterByFuzzyText(result.results, normalized_terms, max_distance, current_doc_store);
    }
  }

  return result;
}

std::vector<storage::DocId> PostFilterByFuzzyText(const std::vector<storage::DocId>& candidates,
                                                  const std::vector<std::string>& normalized_terms,
                                                  uint32_t max_distance, storage::DocumentStore* doc_store) {
  // Batch fetch all texts in single lock acquisition (PERF-3)
  auto texts = doc_store->GetNormalizedTextBatch(candidates);

  std::vector<storage::DocId> verified;
  verified.reserve(candidates.size());

  for (size_t i = 0; i < candidates.size(); ++i) {
    if (!texts[i].has_value()) {
      // Text unavailable -- include to avoid false negatives
      verified.push_back(candidates[i]);
      continue;
    }

    bool all_terms_match = true;
    for (const auto& term : normalized_terms) {
      // First try exact substring match (faster than edit distance)
      if (texts[i]->find(term) != std::string::npos) {
        continue;
      }
      // Fall back to fuzzy word-level matching
      if (!mygram::utils::ContainsFuzzyMatch(*texts[i], term, max_distance)) {
        all_terms_match = false;
        break;
      }
    }

    if (all_terms_match) {
      verified.push_back(candidates[i]);
    }
  }

  return verified;
}

FullPipelineOutput ExecuteFullPipeline(const query::Query& query, const FullPipelineParams& params) {
  FullPipelineOutput output;

  // Validate required parameters
  if (params.current_index == nullptr) {
    output.success = false;
    output.error_message = "Index not available";
    return output;
  }
  if (params.current_doc_store == nullptr) {
    output.success = false;
    output.error_message = "Document store not available";
    return output;
  }

  const query::Query cache_query = WithCanonicalCacheKey(query, params);

  // Try cache lookup first (skip if caller already checked)
  auto cache_lookup_start = std::chrono::high_resolution_clock::now();
  if (params.skip_cache_lookup) {
    // Caller already performed cache lookup; skip to avoid redundant hash
    // computation and lock acquisition on every cache miss. Treat as
    // "disabled" from this pipeline's perspective: the lookup outcome is
    // owned by the caller and not represented here.
    output.cache_miss_reason = CacheMissReason::kDisabled;
  } else {
    CacheMissReason miss_reason = CacheMissReason::kDisabled;
    auto cache_result = TryCacheLookup(cache_query, params.cache_manager, params.current_doc_store, &miss_reason);
    output.cache_miss_reason = miss_reason;
    if (cache_result) {
      auto cache_lookup_end = std::chrono::high_resolution_clock::now();
      output.results = std::move(cache_result->results);
      output.cache_hit = true;
      output.cache_age_ms = cache_result->cache_age_ms;
      output.cache_saved_ms = cache_result->cache_saved_ms;
      output.path_taken = PipelinePath::CACHE_HIT;
      output.query_time_ms = std::chrono::duration<double, std::milli>(cache_lookup_end - cache_lookup_start).count();

      // Collect search terms for downstream use (highlighting, etc.)
      if (!query.search_text.empty()) {
        output.all_search_terms.push_back(query.search_text);
      }
      output.all_search_terms.insert(output.all_search_terms.end(), query.and_terms.begin(), query.and_terms.end());
      return output;
    }
  }
  const std::optional<uint64_t> cache_data_version =
      (params.cache_manager != nullptr && params.cache_manager->IsEnabled())
          ? std::optional<uint64_t>{params.cache_manager->CaptureDataVersion()}
          : std::nullopt;

  // Start timing
  auto start_time = std::chrono::high_resolution_clock::now();

  // Collect all search terms (main + AND terms)
  if (!query.search_text.empty()) {
    output.all_search_terms.push_back(query.search_text);
  }
  output.all_search_terms.insert(output.all_search_terms.end(), query.and_terms.begin(), query.and_terms.end());

  bool cross_boundary = params.cross_boundary_ngrams;

  query::QueryASTParser ast_parser;
  auto boolean_ast = ast_parser.Parse(query.search_text);
  if (!boolean_ast && ContainsBooleanSyntax(query.search_text)) {
    output.success = false;
    output.error_message = "Invalid boolean search expression: " + ast_parser.GetError();
    return output;
  }

  if (boolean_ast && boolean_ast->type != query::NodeType::TERM) {
    output.path_taken = PipelinePath::REGULAR;

    std::vector<std::string> boolean_terms;
    CollectAstTerms(*boolean_ast, boolean_terms);
    output.all_search_terms = boolean_terms;
    output.all_search_terms.insert(output.all_search_terms.end(), query.and_terms.begin(), query.and_terms.end());
    output.term_infos = GenerateTermInfos(output.all_search_terms, params.current_index, params.ngram_size,
                                          params.kanji_ngram_size, cross_boundary);

    auto pipeline_result = ExecuteWithBooleanAst(query, *boolean_ast, params.current_index, params.current_doc_store,
                                                 params.ngram_size, params.kanji_ngram_size, cross_boundary);

    output.empty_term_detected = pipeline_result.empty_term_detected;
    if (pipeline_result.empty_term_detected) {
      output.results.clear();
    } else {
      output.results = std::move(pipeline_result.results);
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    output.query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    if (!output.empty_term_detected) {
      InsertToCache(params.cache_manager, cache_query, output.results, output.term_infos, output.query_time_ms,
                    params.ngram_size, params.kanji_ngram_size, cross_boundary, cache_data_version);
    }
    return output;
  }

  // Fuzzy search path (takes precedence over synonyms)
  if (query.fuzzy_max_distance.has_value()) {
    output.path_taken = PipelinePath::FUZZY;
    output.term_infos = GenerateTermInfos(output.all_search_terms, params.current_index, params.ngram_size,
                                          params.kanji_ngram_size, cross_boundary);

    auto pipeline_result =
        ExecuteWithFuzzy(query, output.term_infos, output.all_search_terms, *query.fuzzy_max_distance,
                         params.current_index, params.current_doc_store, params.full_config, params.ngram_size,
                         params.kanji_ngram_size, cross_boundary, params.filter_threshold);

    output.empty_term_detected = pipeline_result.empty_term_detected;
    if (pipeline_result.empty_term_detected) {
      output.results.clear();
    } else {
      output.results = std::move(pipeline_result.results);
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    output.query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    // Do not cache early-exit (empty posting list) results: a missing posting
    // list during one query may not be missing for the next, and a cached
    // empty entry would mask future legitimate results until invalidated.
    if (!output.empty_term_detected) {
      InsertToCache(params.cache_manager, cache_query, output.results, output.term_infos, output.query_time_ms,
                    params.ngram_size, params.kanji_ngram_size, cross_boundary, cache_data_version);
    }
    return output;
  }

  // Synonym-aware search path
  if (params.synonym_dict != nullptr) {
    output.path_taken = PipelinePath::SYNONYM;
    auto synonym_groups = ExpandTermsWithSynonyms(output.all_search_terms, params.synonym_dict, params.current_index,
                                                  params.ngram_size, params.kanji_ngram_size, cross_boundary);

    auto pipeline_result =
        ExecuteWithSynonyms(query, synonym_groups, params.current_index, params.current_doc_store, params.full_config,
                            params.ngram_size, params.kanji_ngram_size, cross_boundary, params.filter_threshold);

    output.empty_term_detected = pipeline_result.empty_term_detected;
    if (pipeline_result.empty_term_detected) {
      output.results.clear();
    } else {
      output.results = std::move(pipeline_result.results);
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    output.query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

    // Skip caching for early-exit (empty posting list) -- see fuzzy path comment.
    if (!output.empty_term_detected) {
      // Collect all n-grams for cache invalidation
      std::vector<SearchTermInfo> all_term_infos;
      for (const auto& group : synonym_groups) {
        for (const auto& variant : group.variants) {
          all_term_infos.push_back(variant);
        }
      }
      InsertToCache(params.cache_manager, cache_query, output.results, all_term_infos, output.query_time_ms,
                    params.ngram_size, params.kanji_ngram_size, cross_boundary, cache_data_version);
    }
    return output;
  }

  // Standard search path: generate n-grams for each term and estimate result sizes
  output.path_taken = PipelinePath::REGULAR;
  output.term_infos = GenerateTermInfos(output.all_search_terms, params.current_index, params.ngram_size,
                                        params.kanji_ngram_size, cross_boundary);

  // Sort terms by estimated size (smallest first for faster intersection)
  std::sort(
      output.term_infos.begin(), output.term_infos.end(),
      [](const SearchTermInfo& lhs, const SearchTermInfo& rhs) { return lhs.estimated_size < rhs.estimated_size; });

  // Execute the core search pipeline (intersection, NOT filter, filters, verify_text)
  auto pipeline_result =
      Execute(query, output.term_infos, output.all_search_terms, params.current_index, params.current_doc_store,
              params.full_config, params.ngram_size, params.kanji_ngram_size, cross_boundary, params.filter_threshold);

  output.empty_term_detected = pipeline_result.empty_term_detected;
  if (pipeline_result.empty_term_detected) {
    output.results.clear();
  } else {
    output.results = std::move(pipeline_result.results);
  }

  // Calculate query execution time
  auto end_time = std::chrono::high_resolution_clock::now();
  output.query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();

  // Store in cache if enabled. Skip on early-exit (empty posting list); see
  // the fuzzy path for the rationale.
  if (!output.empty_term_detected) {
    InsertToCache(params.cache_manager, cache_query, output.results, output.term_infos, output.query_time_ms,
                  params.ngram_size, params.kanji_ngram_size, cross_boundary, cache_data_version);
  }

  return output;
}

FullPipelineParams BuildPipelineParamsFromContext(const TableContext& table_ctx, const config::Config* full_config,
                                                  cache::CacheManager* cache_manager, size_t filter_threshold,
                                                  bool attach_bm25_stats) {
  FullPipelineParams params;
  params.current_index = table_ctx.index.get();
  params.current_doc_store = table_ctx.doc_store.get();
  params.full_config = full_config;
  params.cache_manager = cache_manager;
  params.ngram_size = table_ctx.config.ngram_size;
  params.kanji_ngram_size = table_ctx.config.kanji_ngram_size;
  params.cross_boundary_ngrams = table_ctx.config.cross_boundary_ngrams;
  params.filter_threshold = filter_threshold;
  params.primary_key_column = table_ctx.config.primary_key;

  // BM25 stats are only relevant to the SEARCH path (used for `_score`
  // sorting). COUNT-style callers leave them null to keep params minimal and
  // surface accidental wiring at compile time.
  if (attach_bm25_stats) {
    params.bm25_stats = &table_ctx.bm25_stats;
  }

  // Skip empty synonym dictionaries: passing one would force
  // ExecuteFullPipeline down the synonym path even when there's nothing to
  // expand, costing a redundant traversal.
  if (table_ctx.synonym_dict && !table_ctx.synonym_dict->IsEmpty()) {
    params.synonym_dict = table_ctx.synonym_dict.get();
  }

  return params;
}

}  // namespace mygramdb::server::search_pipeline
