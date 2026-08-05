/**
 * @file query_normalizer.cpp
 * @brief Query normalization implementation
 */

#include "query/query_normalizer.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <cstdint>
#include <string>
#include <tuple>
#include <vector>

namespace mygramdb::cache {

namespace {

// Bumping this prefix creates a new cache namespace. Query cache entries are
// currently process-local, but keeping the version in the hashed material also
// makes future persistence safe across serializer changes.
constexpr std::array<char, 5> kSerializerPrefix = {'M', 'G', 'Q', 'K', '\x04'};

enum class FieldTag : uint8_t {
  kCommand = 1,
  kTable = 2,
  kSearchText = 3,
  kAndTerm = 4,
  kNotTerm = 5,
  kFilter = 6,
  kFuzzyDistance = 7,
  kExecutionMode = 8,
  kVerificationPolicy = 9,
  kSynonymRevision = 10,
};

enum class FilterFieldTag : uint8_t { kColumn = 1, kOperator = 2, kValue = 3 };

void AppendUint64(std::string& output, uint64_t value) {
  for (int shift = 56; shift >= 0; shift -= 8) {
    output.push_back(static_cast<char>((value >> shift) & 0xffU));
  }
}

void AppendField(std::string& output, uint8_t tag, std::string_view value) {
  output.push_back(static_cast<char>(tag));
  AppendUint64(output, static_cast<uint64_t>(value.size()));
  output.append(value.data(), value.size());
}

void AppendField(std::string& output, FieldTag tag, std::string_view value) {
  AppendField(output, static_cast<uint8_t>(tag), value);
}

std::string OneByteValue(uint8_t value) {
  return std::string(1, static_cast<char>(value));
}

}  // namespace

std::string QueryNormalizer::Normalize(const query::Query& query, const TextNormalizer& text_normalizer,
                                       const CacheSemanticContext& semantic_context) {
  uint8_t command = 0;
  switch (query.type) {
    case query::QueryType::SEARCH:
    case query::QueryType::FACET:
      // FACET caches the underlying DocId set, not the aggregated facet
      // values. Share the SEARCH namespace so identical predicates reuse the
      // same posting-list result regardless of which surface warmed it.
      command = 1;
      break;
    case query::QueryType::COUNT:
      command = 2;
      break;
    default:
      // Only SEARCH, FACET, and COUNT queries are cacheable.
      return "";
  }

  std::string result;
  result.reserve(128);
  result.append(kSerializerPrefix.data(), kSerializerPrefix.size());

  AppendField(result, FieldTag::kCommand, OneByteValue(command));
  AppendField(result, FieldTag::kTable, query.table);
  AppendField(result, FieldTag::kExecutionMode, OneByteValue(static_cast<uint8_t>(semantic_context.execution_mode)));
  AppendField(result, FieldTag::kVerificationPolicy, semantic_context.verification_policy);
  std::string encoded_revision;
  AppendUint64(encoded_revision, semantic_context.synonym_revision);
  AppendField(result, FieldTag::kSynonymRevision, encoded_revision);
  AppendField(result, FieldTag::kSearchText,
              NormalizeSearchText(query.search_expression.empty() ? query.search_text : query.search_expression,
                                  text_normalizer));

  std::vector<std::string> and_terms;
  and_terms.reserve(query.and_terms.size());
  for (const auto& term : query.and_terms) {
    and_terms.push_back(NormalizeSearchText(term, text_normalizer));
  }
  std::sort(and_terms.begin(), and_terms.end());
  for (const auto& term : and_terms) {
    AppendField(result, FieldTag::kAndTerm, term);
  }

  std::vector<std::string> not_terms;
  not_terms.reserve(query.not_terms.size());
  for (const auto& term : query.not_terms) {
    not_terms.push_back(NormalizeSearchText(term, text_normalizer));
  }
  std::sort(not_terms.begin(), not_terms.end());
  for (const auto& term : not_terms) {
    AppendField(result, FieldTag::kNotTerm, term);
  }

  std::vector<query::FilterCondition> filters = query.filters;
  std::sort(filters.begin(), filters.end(), [](const auto& lhs, const auto& rhs) {
    return std::tie(lhs.column, lhs.op, lhs.value) < std::tie(rhs.column, rhs.op, rhs.value);
  });
  for (const auto& filter : filters) {
    std::string encoded_filter;
    encoded_filter.reserve(filter.column.size() + filter.value.size() + 32);
    AppendField(encoded_filter, static_cast<uint8_t>(FilterFieldTag::kColumn), filter.column);
    AppendField(encoded_filter, static_cast<uint8_t>(FilterFieldTag::kOperator),
                OneByteValue(static_cast<uint8_t>(filter.op)));
    AppendField(encoded_filter, static_cast<uint8_t>(FilterFieldTag::kValue), filter.value);
    AppendField(result, FieldTag::kFilter, encoded_filter);
  }

  if (query.fuzzy_max_distance.has_value()) {
    const uint32_t distance = *query.fuzzy_max_distance;
    std::string encoded_distance;
    encoded_distance.reserve(sizeof(distance));
    for (int shift = 24; shift >= 0; shift -= 8) {
      encoded_distance.push_back(static_cast<char>((distance >> shift) & 0xffU));
    }
    AppendField(result, FieldTag::kFuzzyDistance, encoded_distance);
  }

  // Note: LIMIT, OFFSET, and SORT are intentionally excluded from cache key.
  // The cache stores full unsorted results, and presentation concerns are
  // applied by the request handler after lookup.

  return result;
}

std::string QueryNormalizer::NormalizeSearchText(const std::string& text, const TextNormalizer& text_normalizer) {
  if (text_normalizer) {
    return text_normalizer(text);
  }
  return text;
}

}  // namespace mygramdb::cache
