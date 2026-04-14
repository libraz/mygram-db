/**
 * @file edit_distance.cpp
 * @brief Levenshtein edit distance implementation for fuzzy search
 */

#include "utils/edit_distance.h"

#include <algorithm>
#include <cstdlib>
#include <numeric>
#include <vector>

#include "utils/string_utils.h"

namespace mygram::utils {

namespace {

/// @brief Check if all bytes in a string are ASCII (< 0x80)
bool IsAscii(std::string_view s) {
  for (unsigned char c : s) {
    if (c >= 0x80) {
      return false;
    }
  }
  return true;
}

/// @brief Compute Levenshtein distance over raw codepoint sequences
/// @param a_data Pointer to first codepoint sequence
/// @param a_len Length of first sequence
/// @param b_data Pointer to second codepoint sequence
/// @param b_len Length of second sequence
/// @param max_distance Early termination threshold
/// @return Edit distance, or max_distance+1 if exceeded
uint32_t ComputeDistance(const uint32_t* a_data, uint32_t a_len,
                         const uint32_t* b_data, uint32_t b_len,
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

  // 1D DP array: dp[j] represents the edit distance for the first j characters
  // of a
  std::vector<uint32_t> dp(a_len + 1);
  std::iota(dp.begin(), dp.end(), 0);

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

uint32_t LevenshteinDistance(std::string_view a, std::string_view b,
                             uint32_t max_distance) {
  // Fast path for identical strings
  if (a == b) {
    return 0;
  }

  // Fast path for empty strings
  if (a.empty()) {
    uint32_t b_len =
        IsAscii(b) ? static_cast<uint32_t>(b.size()) : CountCodePoints(b);
    return b_len <= max_distance ? b_len : max_distance + 1;
  }
  if (b.empty()) {
    uint32_t a_len =
        IsAscii(a) ? static_cast<uint32_t>(a.size()) : CountCodePoints(a);
    return a_len <= max_distance ? a_len : max_distance + 1;
  }

  // Optimization: for pure-ASCII strings, treat bytes as codepoints directly
  // to avoid Utf8ToCodepoints allocation
  if (IsAscii(a) && IsAscii(b)) {
    // Reinterpret ASCII bytes as uint32_t on the stack or in a temporary vector
    std::vector<uint32_t> a_cp(a.begin(), a.end());
    std::vector<uint32_t> b_cp(b.begin(), b.end());
    return ComputeDistance(a_cp.data(), static_cast<uint32_t>(a_cp.size()),
                           b_cp.data(), static_cast<uint32_t>(b_cp.size()),
                           max_distance);
  }

  // General Unicode path
  std::vector<uint32_t> a_cp = Utf8ToCodepoints(a);
  std::vector<uint32_t> b_cp = Utf8ToCodepoints(b);
  return ComputeDistance(a_cp.data(), static_cast<uint32_t>(a_cp.size()),
                         b_cp.data(), static_cast<uint32_t>(b_cp.size()),
                         max_distance);
}

bool ContainsFuzzyMatch(std::string_view text, std::string_view term,
                        uint32_t max_distance) {
  if (term.empty()) {
    return true;
  }
  if (text.empty()) {
    return false;
  }

  // Pre-compute term codepoint length once
  uint32_t term_len = IsAscii(term) ? static_cast<uint32_t>(term.size())
                                    : CountCodePoints(term);

  // Iterate through whitespace-delimited words in text
  size_t pos = 0;
  while (pos < text.size()) {
    // Skip leading spaces
    size_t word_start = text.find_first_not_of(' ', pos);
    if (word_start == std::string_view::npos) {
      break;
    }

    // Find end of word
    size_t word_end = text.find(' ', word_start);
    if (word_end == std::string_view::npos) {
      word_end = text.size();
    }

    std::string_view word = text.substr(word_start, word_end - word_start);

    // Length-difference pre-filter: skip words where |word_len - term_len| >
    // max_distance
    uint32_t word_len = IsAscii(word) ? static_cast<uint32_t>(word.size())
                                      : CountCodePoints(word);

    uint32_t len_diff =
        (word_len > term_len) ? (word_len - term_len) : (term_len - word_len);
    if (len_diff <= max_distance) {
      uint32_t distance = LevenshteinDistance(word, term, max_distance);
      if (distance <= max_distance) {
        return true;
      }
    }

    pos = word_end;
  }

  return false;
}

}  // namespace mygram::utils
