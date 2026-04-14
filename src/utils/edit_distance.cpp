/**
 * @file edit_distance.cpp
 * @brief Levenshtein edit distance implementation for fuzzy search
 */

#include "utils/edit_distance.h"

#include <algorithm>
#include <cstdlib>
#include <numeric>
#include <string>
#include <vector>

#include "utils/string_utils.h"

namespace mygram::utils {

namespace {

/// @brief Whitespace characters used for word splitting
constexpr char kWhitespace[] = " \t\r\n";

/// @brief Replace common Unicode whitespace sequences with ASCII space
/// Handles U+3000 (ideographic space) and U+00A0 (no-break space)
std::string NormalizeUnicodeWhitespace(std::string_view text) {
  std::string result;
  result.reserve(text.size());
  size_t i = 0;
  while (i < text.size()) {
    auto byte = static_cast<unsigned char>(text[i]);
    // U+3000 ideographic space: 0xE3 0x80 0x80
    if (byte == 0xE3 && i + 2 < text.size() && static_cast<unsigned char>(text[i + 1]) == 0x80 &&
        static_cast<unsigned char>(text[i + 2]) == 0x80) {
      result += ' ';
      i += 3;
      continue;
    }
    // U+00A0 no-break space: 0xC2 0xA0
    if (byte == 0xC2 && i + 1 < text.size() && static_cast<unsigned char>(text[i + 1]) == 0xA0) {
      result += ' ';
      i += 2;
      continue;
    }
    result += text[i];
    ++i;
  }
  return result;
}

/// @brief Stack DP array size limit (avoids heap allocation for short strings)
constexpr size_t kStackDpLimit = 256;

/// @brief Check if all bytes in a string are ASCII (< 0x80)
bool IsAscii(std::string_view s) {
  for (unsigned char c : s) {
    if (c >= 0x80) {
      return false;
    }
  }
  return true;
}

/// @brief Compute Levenshtein distance over raw element sequences
/// @tparam CharT Element type (uint32_t for codepoints, unsigned char for
/// ASCII)
/// @param a_data Pointer to first sequence
/// @param a_len Length of first sequence
/// @param b_data Pointer to second sequence
/// @param b_len Length of second sequence
/// @param max_distance Early termination threshold
/// @return Edit distance, or max_distance+1 if exceeded
template <typename CharT>
uint32_t ComputeDistanceImpl(const CharT* a_data, uint32_t a_len, const CharT* b_data, uint32_t b_len,
                             uint32_t max_distance) {
  // Ensure a is the shorter string for O(min(m,n)) space
  if (a_len > b_len) {
    std::swap(a_data, b_data);
    std::swap(a_len, b_len);
  }

  // Length difference alone exceeds max_distance
  if (b_len - a_len > max_distance) {
    return max_distance + 1;
  }

  // Handle empty strings
  if (a_len == 0) {
    return b_len;
  }

  // 1D DP array: use stack allocation for short strings, heap for long ones
  uint32_t dp_stack[kStackDpLimit];  // NOLINT(modernize-avoid-c-arrays)
  std::vector<uint32_t> dp_heap;
  uint32_t* dp = nullptr;
  if (a_len + 1 <= kStackDpLimit) {
    dp = dp_stack;
  } else {
    dp_heap.resize(a_len + 1);
    dp = dp_heap.data();
  }
  std::iota(dp, dp + a_len + 1, static_cast<uint32_t>(0));

  for (uint32_t i = 0; i < b_len; ++i) {
    uint32_t prev = dp[0];
    dp[0] = i + 1;
    uint32_t row_min = dp[0];

    for (uint32_t j = 0; j < a_len; ++j) {
      uint32_t cost = (a_data[j] == b_data[i]) ? 0 : 1;
      uint32_t insert_cost = dp[j + 1] + 1;
      uint32_t delete_cost = dp[j] + 1;
      uint32_t replace_cost = prev + cost;

      prev = dp[j + 1];
      dp[j + 1] = std::min({insert_cost, delete_cost, replace_cost});
      row_min = std::min(row_min, dp[j + 1]);
    }

    // Early termination: if minimum value in this row exceeds max_distance,
    // no future computation can bring it back below max_distance
    if (row_min > max_distance) {
      return max_distance + 1;
    }
  }

  return dp[a_len] <= max_distance ? dp[a_len] : max_distance + 1;
}

}  // namespace

uint32_t LevenshteinDistance(std::string_view a, std::string_view b, uint32_t max_distance) {
  // Fast path for identical strings
  if (a == b) {
    return 0;
  }

  // Fast path for empty strings
  if (a.empty()) {
    uint32_t b_len = IsAscii(b) ? static_cast<uint32_t>(b.size()) : CountCodePoints(b);
    return b_len <= max_distance ? b_len : max_distance + 1;
  }
  if (b.empty()) {
    uint32_t a_len = IsAscii(a) ? static_cast<uint32_t>(a.size()) : CountCodePoints(a);
    return a_len <= max_distance ? a_len : max_distance + 1;
  }

  // Optimization: for pure-ASCII strings, operate directly on bytes
  // to avoid Utf8ToCodepoints allocation
  if (IsAscii(a) && IsAscii(b)) {
    const auto* a_bytes = reinterpret_cast<const unsigned char*>(a.data());
    const auto* b_bytes = reinterpret_cast<const unsigned char*>(b.data());
    return ComputeDistanceImpl(a_bytes, static_cast<uint32_t>(a.size()), b_bytes, static_cast<uint32_t>(b.size()),
                               max_distance);
  }

  // General Unicode path
  std::vector<uint32_t> a_cp = Utf8ToCodepoints(a);
  std::vector<uint32_t> b_cp = Utf8ToCodepoints(b);
  return ComputeDistanceImpl(a_cp.data(), static_cast<uint32_t>(a_cp.size()), b_cp.data(),
                             static_cast<uint32_t>(b_cp.size()), max_distance);
}

bool ContainsFuzzyMatch(std::string_view text, std::string_view term, uint32_t max_distance) {
  if (term.empty()) {
    return true;
  }
  if (text.empty()) {
    return false;
  }

  // Normalize Unicode whitespace to ASCII space for consistent word splitting
  std::string normalized_text = NormalizeUnicodeWhitespace(text);
  std::string_view normalized_view = normalized_text;

  // Pre-decode term once to avoid repeated UTF-8 scanning
  bool term_is_ascii = IsAscii(term);
  std::vector<uint32_t> term_codepoints;
  if (!term_is_ascii) {
    term_codepoints = Utf8ToCodepoints(term);
  }
  uint32_t term_len =
      term_is_ascii ? static_cast<uint32_t>(term.size()) : static_cast<uint32_t>(term_codepoints.size());

  // Iterate through whitespace-delimited words in text
  size_t pos = 0;
  while (pos < normalized_view.size()) {
    // Skip leading whitespace
    size_t word_start = normalized_view.find_first_not_of(kWhitespace, pos);
    if (word_start == std::string_view::npos) {
      break;
    }

    // Find end of word
    size_t word_end = normalized_view.find_first_of(kWhitespace, word_start);
    if (word_end == std::string_view::npos) {
      word_end = normalized_view.size();
    }

    std::string_view word = normalized_view.substr(word_start, word_end - word_start);

    // Decode word and compute distance directly, avoiding redundant UTF-8 scans
    bool word_is_ascii = IsAscii(word);

    if (term_is_ascii && word_is_ascii) {
      // Both ASCII: operate directly on bytes, no allocation
      uint32_t word_len = static_cast<uint32_t>(word.size());
      uint32_t len_diff = (word_len > term_len) ? (word_len - term_len) : (term_len - word_len);
      if (len_diff <= max_distance) {
        const auto* w_bytes = reinterpret_cast<const unsigned char*>(word.data());
        const auto* t_bytes = reinterpret_cast<const unsigned char*>(term.data());
        uint32_t distance = ComputeDistanceImpl(w_bytes, word_len, t_bytes, term_len, max_distance);
        if (distance <= max_distance) {
          return true;
        }
      }
    } else {
      // At least one is non-ASCII: decode word to codepoints
      std::vector<uint32_t> word_codepoints;
      const uint32_t* word_data = nullptr;
      uint32_t word_len = 0;

      if (word_is_ascii) {
        // Word is ASCII but term is not; still need uint32_t for
        // ComputeDistance
        word_codepoints.assign(word.begin(), word.end());
      } else {
        word_codepoints = Utf8ToCodepoints(word);
      }
      word_data = word_codepoints.data();
      word_len = static_cast<uint32_t>(word_codepoints.size());

      // Term data: use pre-decoded codepoints, or convert ASCII term on the fly
      const uint32_t* term_data = nullptr;
      std::vector<uint32_t> term_ascii_codepoints;
      if (term_is_ascii) {
        term_ascii_codepoints.assign(term.begin(), term.end());
        term_data = term_ascii_codepoints.data();
      } else {
        term_data = term_codepoints.data();
      }

      uint32_t len_diff = (word_len > term_len) ? (word_len - term_len) : (term_len - word_len);
      if (len_diff <= max_distance) {
        uint32_t distance = ComputeDistanceImpl(word_data, word_len, term_data, term_len, max_distance);
        if (distance <= max_distance) {
          return true;
        }
      }
    }

    pos = word_end;
  }

  return false;
}

}  // namespace mygram::utils
