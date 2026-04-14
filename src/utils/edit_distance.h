/**
 * @file edit_distance.h
 * @brief Levenshtein edit distance for fuzzy search
 */

#pragma once

#include <cstdint>
#include <string_view>

namespace mygram::utils {

/**
 * @brief Compute Levenshtein edit distance between two UTF-8 strings
 *
 * Operates at codepoint level (not byte level) for correct Unicode handling.
 * Uses O(min(m,n)) space with 1D DP array and early termination.
 *
 * @param a First string (UTF-8)
 * @param b Second string (UTF-8)
 * @param max_distance Maximum distance to compute (returns max_distance+1 if
 * exceeded)
 * @return Edit distance, or max_distance+1 if distance exceeds max_distance
 */
uint32_t LevenshteinDistance(std::string_view a, std::string_view b,
                             uint32_t max_distance);

/**
 * @brief Check if any whitespace-delimited word in text is within edit distance
 * of term
 *
 * Splits text by whitespace, checks each word against term.
 * Optimized with length-difference pre-filter (skips words where
 * |len(word) - len(term)| > max_distance).
 *
 * @param text Normalized text to search in (space-delimited words)
 * @param term Search term to match against
 * @param max_distance Maximum edit distance for a match
 * @return true if any word in text is within max_distance of term
 */
bool ContainsFuzzyMatch(std::string_view text, std::string_view term,
                        uint32_t max_distance);

}  // namespace mygram::utils
