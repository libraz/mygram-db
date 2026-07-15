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

#include "utils/string_utils.h"

namespace mygramdb::cache {

namespace {

// Bumping this prefix creates a new cache namespace. Query cache entries are
// currently process-local, but keeping the version in the hashed material also
// makes future persistence safe across serializer changes.
constexpr std::array<char, 5> kSerializerPrefix = {'M', 'G', 'Q', 'K', '\x02'};

enum class FieldTag : uint8_t {
  kCommand = 1,
  kTable = 2,
  kSearchText = 3,
  kAndTerm = 4,
  kNotTerm = 5,
  kFilter = 6,
  kFuzzyDistance = 7,
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

std::string QueryNormalizer::Normalize(const query::Query& query, const TextNormalizer& text_normalizer) {
  uint8_t command = 0;
  switch (query.type) {
    case query::QueryType::SEARCH:
      command = 1;
      break;
    case query::QueryType::COUNT:
      command = 2;
      break;
    default:
      // Only SEARCH and COUNT queries are cacheable.
      return "";
  }

  std::string result;
  result.reserve(128);
  result.append(kSerializerPrefix.data(), kSerializerPrefix.size());

  AppendField(result, FieldTag::kCommand, OneByteValue(command));
  AppendField(result, FieldTag::kTable, query.table);
  AppendField(result, FieldTag::kSearchText, NormalizeSearchText(query.search_text, text_normalizer));

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
  // Normalize whitespace: collapse multiple spaces (including Unicode spaces) to single space
  std::string normalized;
  normalized.reserve(text.size());

  bool prev_was_space = false;
  for (size_t i = 0; i < text.size(); ++i) {
    size_t ws_len = 0;
    bool is_space = mygram::utils::IsUnicodeWhitespace(text, i, ws_len);

    if (is_space) {
      // Skip extra bytes of multi-byte whitespace character (loop will ++i)
      i += ws_len - 1;
      if (!prev_was_space && !normalized.empty()) {
        normalized += ' ';
        prev_was_space = true;
      }
    } else {
      normalized += text[i];
      prev_was_space = false;
    }
  }

  // Remove trailing space if any
  if (!normalized.empty() && normalized.back() == ' ') {
    normalized.pop_back();
  }

  if (text_normalizer) {
    return text_normalizer(normalized);
  }

  return normalized;
}

}  // namespace mygramdb::cache
