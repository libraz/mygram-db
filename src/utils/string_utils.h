/**
 * @file string_utils.h
 * @brief String utility functions for text normalization and processing
 */

#pragma once

#include <algorithm>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace mygram::utils {

/**
 * @brief Normalize text according to configuration
 *
 * Applies NFKC normalization, width conversion, and case conversion
 * Uses ICU library if available, otherwise uses simple fallback
 *
 * @param text Input text
 * @param nfkc Apply NFKC normalization
 * @param width Width conversion: "keep", "narrow", or "wide"
 * @param lower Convert to lowercase
 * @return Normalized text
 */
std::string NormalizeText(std::string_view text, bool nfkc = true, std::string_view width = "narrow",
                          bool lower = false);

#ifdef USE_ICU
/**
 * @brief Normalize text using ICU
 *
 * @param text Input text
 * @param nfkc Apply NFKC normalization
 * @param width Width conversion: "keep", "narrow", or "wide"
 * @param lower Convert to lowercase
 * @return Normalized text
 */
std::string NormalizeTextICU(std::string_view text, bool nfkc, std::string_view width, bool lower);
#endif

/**
 * @brief Generate n-grams from text
 *
 * @param text Input text (should be normalized)
 * @param n N-gram size (typically 1 for unigrams)
 * @return Vector of n-gram strings
 */
std::vector<std::string> GenerateNgrams(std::string_view text, int n = 1);

/**
 * @brief Generate hybrid n-grams with configurable sizes
 *
 * CJK Ideographs (漢字) are tokenized with kanji_ngram_size,
 * while other characters are tokenized with ascii_ngram_size.
 * This provides flexibility for different language requirements.
 *
 * @param text Input text (should be normalized)
 * @param ascii_ngram_size N-gram size for ASCII/alphanumeric characters (default: 2)
 * @param kanji_ngram_size N-gram size for CJK characters (default: 1)
 * @param cross_boundary_ngrams Generate N-grams spanning CJK/non-CJK boundaries (default: true)
 * @return Vector of n-gram strings
 */
std::vector<std::string> GenerateHybridNgrams(std::string_view text, int ascii_ngram_size = 2, int kanji_ngram_size = 1,
                                              bool cross_boundary_ngrams = true);

/**
 * @brief Convert UTF-8 string to codepoint vector
 *
 * @param text UTF-8 encoded string
 * @return Vector of Unicode codepoints
 */
std::vector<uint32_t> Utf8ToCodepoints(std::string_view text);

/**
 * @brief Convert codepoint vector to UTF-8 string
 *
 * @param codepoints Vector of Unicode codepoints
 * @return UTF-8 encoded string
 */
std::string CodepointsToUtf8(const std::vector<uint32_t>& codepoints);

/**
 * @brief Convert a range of codepoints to UTF-8 string
 *
 * Avoids heap allocation of a temporary vector when converting a sub-range.
 *
 * @param begin Iterator to first codepoint
 * @param end Iterator past last codepoint
 * @return UTF-8 encoded string
 */
std::string CodepointsToUtf8(const uint32_t* begin, const uint32_t* end);

/**
 * @brief Format bytes to human-readable string (e.g., "1.5MB", "500KB")
 *
 * @param bytes Number of bytes
 * @return Human-readable string
 */
std::string FormatBytes(size_t bytes);

/**
 * @brief Check if a string is valid UTF-8
 *
 * @param text String to validate
 * @return true if valid UTF-8, false otherwise
 */
bool IsValidUtf8(std::string_view text);

/**
 * @brief Sanitize a string by replacing invalid UTF-8 sequences with U+FFFD
 *
 * Invalid bytes are replaced with the Unicode replacement character (U+FFFD).
 * Valid UTF-8 sequences are preserved unchanged.
 *
 * @param text String to sanitize
 * @return Sanitized UTF-8 string
 */
std::string SanitizeUtf8(std::string_view text);

/**
 * @brief Convert a string to uppercase (ASCII only)
 *
 * @param str Input string
 * @return Uppercase copy of the string
 */
std::string ToUpper(std::string_view str);

/// Replace all occurrences of `from` with `to` in the string
std::string ReplaceAll(std::string_view str, std::string_view from, std::string_view to);

/// Split string on first occurrence of delimiter, returns {before, after}
/// If delimiter not found, returns {str, ""}
std::pair<std::string, std::string> SplitOnFirst(std::string_view str, std::string_view delimiter);

/// @brief Sort and remove duplicate elements from a vector in-place.
template <typename T>
inline void DeduplicateSorted(std::vector<T>& vec) {
  std::sort(vec.begin(), vec.end());
  vec.erase(std::unique(vec.begin(), vec.end()), vec.end());
}

}  // namespace mygram::utils
