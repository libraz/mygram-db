/**
 * @file synonym_dictionary.h
 * @brief Synonym dictionary for search term expansion
 */

#pragma once

#include <functional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::query {

/**
 * @brief In-memory synonym dictionary for search term expansion
 *
 * Stores bidirectional synonym groups loaded from TSV files.
 * All terms are stored in normalized form matching index normalization.
 * Thread-safe for concurrent reads during search.
 */
class SynonymDictionary {
 public:
  SynonymDictionary() = default;
  ~SynonymDictionary() = default;

  SynonymDictionary(const SynonymDictionary&) = delete;
  SynonymDictionary& operator=(const SynonymDictionary&) = delete;
  SynonymDictionary(SynonymDictionary&& other) noexcept;
  SynonymDictionary& operator=(SynonymDictionary&& other) noexcept;

  /**
   * @brief Load synonym groups from a TSV file
   *
   * Each line contains tab-separated terms that are synonyms of each other.
   * Empty lines and lines starting with '#' are skipped.
   * Terms are normalized using the provided function.
   *
   * @param filepath Path to the TSV file
   * @param normalizer Function to normalize terms (should match index normalization)
   * @return void or Error
   */
  mygram::utils::Expected<void, mygram::utils::Error> LoadFromFile(
      const std::string& filepath, std::function<std::string(std::string_view)> normalizer);

  /**
   * @brief Expand a normalized term to include all its synonyms
   *
   * Returns a vector containing the term plus all its synonyms.
   * If the term has no synonyms, returns a vector containing only the term.
   * Thread-safe (shared lock).
   *
   * @param normalized_term Normalized search term
   * @return Vector of normalized synonyms (always includes the input term)
   */
  [[nodiscard]] std::vector<std::string> Expand(const std::string& normalized_term) const;

  [[nodiscard]] bool IsEmpty() const;
  [[nodiscard]] size_t GroupCount() const;
  [[nodiscard]] size_t TermCount() const;
  void Clear();

  /// Serialize dictionary to stream (for dump persistence)
  [[nodiscard]] bool SaveToStream(std::ostream& output_stream) const;

  /// Deserialize dictionary from stream
  [[nodiscard]] bool LoadFromStream(std::istream& input_stream);

 private:
  /// Synonym groups: each group is a vector of normalized terms
  std::vector<std::vector<std::string>> groups_;

  /// Reverse index: normalized_term -> index into groups_
  std::unordered_map<std::string, size_t> term_to_group_;

  mutable std::shared_mutex mutex_;

  static constexpr size_t kMaxGroupSize = 20;  ///< Max synonyms per group
};

}  // namespace mygramdb::query
