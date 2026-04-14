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
 * @brief Convert UTF-8 string to codepoints, writing to a caller-provided buffer
 *
 * Avoids heap allocation when the buffer is large enough. If the text has more
 * codepoints than buffer_capacity, returns 0 and the caller should fall back
 * to the vector-returning overload.
 *
 * @param text UTF-8 encoded string
 * @param buffer Output buffer for codepoints
 * @param buffer_capacity Maximum number of codepoints the buffer can hold
 * @return Number of codepoints written, or 0 if buffer was too small
 */
size_t Utf8ToCodepoints(std::string_view text, uint32_t* buffer, size_t buffer_capacity);

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

/**
 * @brief Generate n-grams for a query term using the appropriate strategy
 *
 * Encapsulates the 3-branch n-gram selection logic:
 * - kanji_ngram_size > 0: Use GenerateHybridNgrams with both sizes
 * - ngram_size == 0: Use GenerateHybridNgrams with default parameters
 * - Otherwise: Use GenerateNgrams with the given ngram_size
 *
 * @param normalized Normalized text to generate n-grams from
 * @param ngram_size N-gram size for ASCII/alphanumeric characters
 * @param kanji_ngram_size N-gram size for CJK characters (0 = not configured)
 * @param cross_boundary_ngrams Generate n-grams spanning CJK/non-CJK boundaries
 * @return Vector of n-gram strings
 */
std::vector<std::string> GenerateQueryNgrams(std::string_view normalized, int ngram_size, int kanji_ngram_size,
                                             bool cross_boundary_ngrams);

/**
 * @brief Count UTF-8 code points in a string
 * @param text UTF-8 encoded text
 * @return Number of Unicode code points
 */
size_t CountCodePoints(std::string_view text);

/**
 * @brief Check if a Unicode whitespace character starts at the given position
 *
 * Detects ASCII whitespace and Unicode whitespace characters including:
 * U+00A0 (No-Break Space), U+1680 (Ogham Space Mark), U+2000-U+200B,
 * U+2028 (Line Separator), U+2029 (Paragraph Separator), U+202F (Narrow No-Break Space),
 * U+205F (Medium Mathematical Space), U+3000 (Ideographic Space).
 *
 * @param text Input text
 * @param pos Position to check
 * @param[out] char_len Set to the number of bytes consumed by the whitespace character (1-3)
 * @return true if a whitespace character starts at pos
 */
bool IsUnicodeWhitespace(std::string_view text, size_t pos, size_t& char_len);

}  // namespace mygram::utils
