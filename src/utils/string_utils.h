/**
 * @file string_utils.h
 * @brief String utility functions for text normalization and processing
 */

#pragma once

#include <cstdint>
#include <string>
#include <vector>

namespace mygramdb::utils {

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
std::string NormalizeText(const std::string& text, bool nfkc = true, const std::string& width = "narrow",
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
std::string NormalizeTextICU(const std::string& text, bool nfkc, const std::string& width, bool lower);
#endif

/**
 * @brief Generate n-grams from text
 *
 * @param text Input text (should be normalized)
 * @param n N-gram size (typically 1 for unigrams)
 * @return Vector of n-gram strings
 */
std::vector<std::string> GenerateNgrams(const std::string& text, int n = 1);

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
 * @return Vector of n-gram strings
 */
std::vector<std::string> GenerateHybridNgrams(const std::string& text, int ascii_ngram_size = 2,
                                              int kanji_ngram_size = 1);

/**
 * @brief Convert UTF-8 string to codepoint vector
 *
 * @param text UTF-8 encoded string
 * @return Vector of Unicode codepoints
 */
std::vector<uint32_t> Utf8ToCodepoints(const std::string& text);

/**
 * @brief Convert codepoint vector to UTF-8 string
 *
 * @param codepoints Vector of Unicode codepoints
 * @return UTF-8 encoded string
 */
std::string CodepointsToUtf8(const std::vector<uint32_t>& codepoints);

/**
 * @brief Format bytes to human-readable string (e.g., "1.5MB", "500KB")
 *
 * @param bytes Number of bytes
 * @return Human-readable string
 */
std::string FormatBytes(size_t bytes);

}  // namespace mygramdb::utils
