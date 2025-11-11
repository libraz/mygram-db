/**
 * @file index.h
 * @brief N-gram inverted index
 */

#pragma once

#include "index/posting_list.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

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
   * @param ngram_size N-gram size for ASCII/alphanumeric characters (typically 2 for bigrams)
   * @param kanji_ngram_size N-gram size for CJK characters (0 = use ngram_size)
   * @param roaring_threshold Density threshold for Roaring bitmaps
   */
  explicit Index(int ngram_size = 2,
                  int kanji_ngram_size = 1,
                  double roaring_threshold = 0.18);

  ~Index() = default;

  // Delete copy constructor and assignment (contains unique_ptr)
  Index(const Index&) = delete;
  Index& operator=(const Index&) = delete;

  // Delete move constructor and assignment (contains mutex)
  Index(Index&&) = delete;
  Index& operator=(Index&&) = delete;

  /**
   * @brief Document item for batch processing
   */
  struct DocumentItem {
    DocId doc_id;
    std::string text;  // Normalized text content
  };

  /**
   * @brief Add document to index
   *
   * @param doc_id Document ID
   * @param text Normalized text content
   */
  void AddDocument(DocId doc_id, const std::string& text);

  /**
   * @brief Add multiple documents to index (batch operation, thread-safe)
   *
   * This method is optimized for bulk insertions during snapshot builds.
   * It processes documents in batches to reduce lock contention and improve
   * cache locality.
   *
   * @param documents Vector of documents to add
   * @note This method is thread-safe and can be called from multiple threads
   */
  void AddDocumentBatch(const std::vector<DocumentItem>& documents);

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
  [[nodiscard]] std::vector<DocId> SearchAnd(const std::vector<std::string>& terms) const;

  /**
   * @brief Search for documents containing any term (OR)
   *
   * @param terms Search terms
   * @return Vector of document IDs
   */
  [[nodiscard]] std::vector<DocId> SearchOr(const std::vector<std::string>& terms) const;

  /**
   * @brief Search excluding documents containing any term (NOT)
   *
   * @param all_docs All document IDs in the collection
   * @param terms Terms to exclude
   * @return Vector of document IDs not containing any of the terms
   */
  [[nodiscard]] std::vector<DocId> SearchNot(
      const std::vector<DocId>& all_docs,
      const std::vector<std::string>& terms) const;

  /**
   * @brief Count documents containing term
   *
   * @param term Search term
   * @return Document count
   */
  [[nodiscard]] uint64_t Count(const std::string& term) const;

  /**
   * @brief Get total number of unique terms
   */
  [[nodiscard]] size_t TermCount() const { return term_postings_.size(); }

  /**
   * @brief Get total memory usage
   */
  [[nodiscard]] size_t MemoryUsage() const;

  /**
   * @brief Index statistics
   */
  struct IndexStatistics {
    size_t total_terms = 0;
    size_t total_postings = 0;
    size_t delta_encoded_lists = 0;
    size_t roaring_bitmap_lists = 0;
    size_t memory_usage_bytes = 0;
  };

  /**
   * @brief Get index statistics
   * @return Index statistics structure
   */
  [[nodiscard]] IndexStatistics GetStatistics() const;

  /**
   * @brief Optimize all posting lists
   * @param total_docs Total document count
   */
  void Optimize(uint64_t total_docs);

  /**
   * @brief Optimize posting lists in batches (thread-safe, minimal memory overhead)
   * @param total_docs Total document count
   * @param batch_size Number of terms to optimize per batch (default: 10000)
   * @return true if optimization started, false if already in progress
   */
  bool OptimizeInBatches(uint64_t total_docs, size_t batch_size = 10000);

  /**
   * @brief Check if optimization is currently running
   */
  bool IsOptimizing() const { return is_optimizing_.load(); }

  /**
   * @brief Clear all data from index
   */
  void Clear();

  /**
   * @brief Serialize index to file
   * @param filepath Output file path
   * @return true if successful
   */
  [[nodiscard]] bool SaveToFile(const std::string& filepath) const;

  /**
   * @brief Deserialize index from file
   * @param filepath Input file path
   * @return true if successful
   */
  [[nodiscard]] bool LoadFromFile(const std::string& filepath);

  /**
   * @brief Get posting list for term (read-only)
   * @param term Search term
   * @return Pointer to posting list, or nullptr if not found
   */
  [[nodiscard]] const PostingList* GetPostingList(const std::string& term) const;

  /**
   * @brief Get n-gram size for regular text
   * @return N-gram size (0 for hybrid mode)
   */
  [[nodiscard]] int GetNgramSize() const { return ngram_size_; }

  /**
   * @brief Get n-gram size for Kanji characters
   * @return Kanji n-gram size
   */
  [[nodiscard]] int GetKanjiNgramSize() const { return kanji_ngram_size_; }

 private:
  int ngram_size_;
  int kanji_ngram_size_;
  double roaring_threshold_;

  // Term -> Posting list mapping
  std::unordered_map<std::string, std::unique_ptr<PostingList>> term_postings_;

  // Mutex for protecting term_postings_ during batch optimization
  mutable std::mutex postings_mutex_;

  // Flag to prevent concurrent optimization
  std::atomic<bool> is_optimizing_{false};

  /**
   * @brief Get or create posting list for term
   */
  PostingList* GetOrCreatePostingList(const std::string& term);

  /**
   * @brief Internal search methods (no locking, assumes caller holds lock)
   */
  [[nodiscard]] std::vector<DocId> SearchOrInternal(const std::vector<std::string>& terms) const;
};

}  // namespace index
}  // namespace mygramdb
