/**
 * @file index.h
 * @brief N-gram inverted index
 */

#pragma once

#include "index/posting_list.h"
#include <cstdint>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>

namespace mygramdb {
namespace index {

/**
 * @brief N-gram inverted index
 *
 * Maintains posting lists for each n-gram term
 */
class Index {
 public:
  /**
   * @brief Construct index
   * @param ngram_size N-gram size (typically 1 for unigrams)
   * @param roaring_threshold Density threshold for Roaring bitmaps
   */
  explicit Index(int ngram_size = 1, double roaring_threshold = 0.18);

  ~Index() = default;

  /**
   * @brief Add document to index
   *
   * @param doc_id Document ID
   * @param text Normalized text content
   */
  void AddDocument(DocId doc_id, const std::string& text);

  /**
   * @brief Update document in index
   *
   * @param doc_id Document ID
   * @param old_text Old text content
   * @param new_text New text content
   */
  void UpdateDocument(DocId doc_id, const std::string& old_text,
                     const std::string& new_text);

  /**
   * @brief Remove document from index
   *
   * @param doc_id Document ID
   * @param text Text content
   */
  void RemoveDocument(DocId doc_id, const std::string& text);

  /**
   * @brief Search for documents containing all terms (AND)
   *
   * @param terms Search terms
   * @return Vector of document IDs
   */
  std::vector<DocId> SearchAnd(const std::vector<std::string>& terms) const;

  /**
   * @brief Search for documents containing any term (OR)
   *
   * @param terms Search terms
   * @return Vector of document IDs
   */
  std::vector<DocId> SearchOr(const std::vector<std::string>& terms) const;

  /**
   * @brief Search excluding documents containing any term (NOT)
   *
   * @param all_docs All document IDs in the collection
   * @param terms Terms to exclude
   * @return Vector of document IDs not containing any of the terms
   */
  std::vector<DocId> SearchNot(const std::vector<DocId>& all_docs,
                               const std::vector<std::string>& terms) const;

  /**
   * @brief Count documents containing term
   *
   * @param term Search term
   * @return Document count
   */
  uint64_t Count(const std::string& term) const;

  /**
   * @brief Get total number of unique terms
   */
  size_t TermCount() const { return term_postings_.size(); }

  /**
   * @brief Get total memory usage
   */
  size_t MemoryUsage() const;

  /**
   * @brief Optimize all posting lists
   * @param total_docs Total document count
   */
  void Optimize(uint64_t total_docs);

  /**
   * @brief Serialize index to file
   * @param filepath Output file path
   * @return true if successful
   */
  bool SaveToFile(const std::string& filepath) const;

  /**
   * @brief Deserialize index from file
   * @param filepath Input file path
   * @return true if successful
   */
  bool LoadFromFile(const std::string& filepath);

 private:
  int ngram_size_;
  double roaring_threshold_;

  // Term -> Posting list mapping
  std::unordered_map<std::string, std::unique_ptr<PostingList>> term_postings_;

  /**
   * @brief Get or create posting list for term
   */
  PostingList* GetOrCreatePostingList(const std::string& term);

  /**
   * @brief Get posting list for term (read-only)
   */
  const PostingList* GetPostingList(const std::string& term) const;
};

}  // namespace index
}  // namespace mygramdb
