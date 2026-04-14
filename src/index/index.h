/**
 * @file index.h
 * @brief N-gram inverted index
 */

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <iosfwd>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include "absl/container/flat_hash_map.h"
#include "index/posting_list.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/hash_utils.h"
#include "utils/string_utils.h"

namespace mygramdb::index {

// Import transparent hash utilities from common header
using mygram::utils::TransparentStringEqual;
using mygram::utils::TransparentStringHash;

// Default n-gram sizes for different character types
constexpr int kDefaultNgramSize = 2;       // Bigrams for ASCII/alphanumeric
constexpr int kDefaultKanjiNgramSize = 1;  // Unigrams for CJK characters

// Default batch size for posting list optimization
constexpr size_t kDefaultOptimizeBatchSize = 10000;

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
   * @param cross_boundary_ngrams Generate N-grams spanning CJK/non-CJK boundaries
   */
  explicit Index(int ngram_size = kDefaultNgramSize, int kanji_ngram_size = kDefaultKanjiNgramSize,
                 double roaring_threshold = kDefaultRoaringThreshold, bool cross_boundary_ngrams = true,
                 bool normalize_nfkc = true, const std::string& normalize_width = "keep", bool normalize_lower = true);

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
   * @return true if document produced N-grams and was indexed, false if text
   *         was too short
   */
  bool AddDocument(DocId doc_id, std::string_view text);

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
  void UpdateDocument(DocId doc_id, std::string_view old_text, std::string_view new_text);

  /**
   * @brief Remove document from index
   *
   * @param doc_id Document ID
   * @param text Text content
   */
  void RemoveDocument(DocId doc_id, std::string_view text);

  /**
   * @brief Search for documents containing all terms (AND)
   *
   * @param terms Search terms
   * @param limit Maximum number of results (0 = unlimited)
   * @param reverse Return results in reverse order (highest DocIds first)
   * @return Vector of document IDs
   */
  [[nodiscard]] std::vector<DocId> SearchAnd(const std::vector<std::string>& terms, size_t limit = 0,
                                             bool reverse = false) const;

  /**
   * @brief Filter candidate documents by checking membership in posting lists
   *
   * More efficient than full intersection when candidates set is small.
   * @param candidates Candidate document IDs to check
   * @param terms N-gram terms that candidates must match
   * @return Filtered vector of document IDs matching all terms
   */
  [[nodiscard]] std::vector<DocId> FilterByNgrams(const std::vector<DocId>& candidates,
                                                  const std::vector<std::string>& terms) const;

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
  [[nodiscard]] std::vector<DocId> SearchNot(const std::vector<DocId>& all_docs,
                                             const std::vector<std::string>& terms) const;

  /**
   * @brief Search for documents appearing in at least threshold posting lists
   *
   * Relaxed version of SearchAnd: instead of requiring ALL n-gram posting lists
   * to contain a document, requires at least `threshold` of them.
   * When threshold == terms.size(), equivalent to SearchAnd.
   *
   * Uses k-way sorted merge with counting for efficient threshold matching.
   *
   * @param terms N-gram terms to search
   * @param threshold Minimum number of posting lists a document must appear in
   * @return Sorted vector of document IDs appearing in >= threshold posting lists
   */
  [[nodiscard]] std::vector<DocId> SearchByThreshold(const std::vector<std::string>& terms, size_t threshold) const;

  /**
   * @brief Count documents containing term
   *
   * @param term Search term
   * @return Document count
   */
  [[nodiscard]] uint64_t Count(std::string_view term) const;

  /**
   * @brief Get total number of unique terms
   */
  [[nodiscard]] size_t TermCount() const {
    std::shared_lock<std::shared_mutex> lock(postings_mutex_);
    return term_postings_.size();
  }

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
   * @param batch_size Number of terms to optimize per batch
   * @return true if optimization started, false if already in progress
   */
  bool OptimizeInBatches(uint64_t total_docs, size_t batch_size = kDefaultOptimizeBatchSize);

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
   * @return Success or error with context
   */
  [[nodiscard]] mygram::utils::Expected<void, mygram::utils::Error> SaveToFile(const std::string& filepath) const;

  /**
   * @brief Serialize index to output stream
   * @param output_stream Output stream
   * @return Success or error with context
   */
  [[nodiscard]] mygram::utils::Expected<void, mygram::utils::Error> SaveToStream(std::ostream& output_stream) const;

  /**
   * @brief Deserialize index from file
   * @param filepath Input file path
   * @return Success or error with context
   */
  [[nodiscard]] mygram::utils::Expected<void, mygram::utils::Error> LoadFromFile(const std::string& filepath);

  /**
   * @brief Deserialize index from input stream
   * @param input_stream Input stream
   * @return Success or error with context
   */
  [[nodiscard]] mygram::utils::Expected<void, mygram::utils::Error> LoadFromStream(std::istream& input_stream);

  /**
   * @brief Get posting list for term (read-only)
   * @param term Search term
   * @return Pointer to posting list, or nullptr if not found
   */
  [[nodiscard]] const PostingList* GetPostingList(std::string_view term) const;

  /**
   * @brief Estimate posting list size for a term (thread-safe)
   * @param term Search term
   * @return Estimated size of the posting list, or 0 if term not found
   */
  [[nodiscard]] uint64_t EstimatePostingSize(std::string_view term) const;

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

  /**
   * @brief Get cross-boundary N-gram generation setting
   * @return true if cross-boundary N-grams are generated
   */
  [[nodiscard]] bool GetCrossBoundaryNgrams() const { return cross_boundary_ngrams_; }

  /** @brief Get NFKC normalization setting */
  [[nodiscard]] bool GetNormalizeNfkc() const { return normalize_nfkc_; }

  /** @brief Get width normalization setting */
  [[nodiscard]] const std::string& GetNormalizeWidth() const { return normalize_width_; }

  /** @brief Get lowercase normalization setting */
  [[nodiscard]] bool GetNormalizeLower() const { return normalize_lower_; }

  /** @brief Normalize text using this index's normalization settings */
  [[nodiscard]] std::string NormalizeText(std::string_view text) const {
    return mygram::utils::NormalizeText(text, normalize_nfkc_, normalize_width_, normalize_lower_);
  }

 private:
  int ngram_size_;
  int kanji_ngram_size_;
  double roaring_threshold_;
  bool cross_boundary_ngrams_;
  bool normalize_nfkc_;
  std::string normalize_width_;
  bool normalize_lower_;

  // Term -> Posting list mapping
  // Note: Using shared_ptr instead of unique_ptr to safely handle concurrent access
  // during optimization (Optimize() creates snapshots that need reference counting)
  // Using absl::flat_hash_map with TransparentStringHash for heterogeneous lookup
  // to avoid std::string allocation on every lookup (C++17 compatible)
  absl::flat_hash_map<std::string, std::shared_ptr<PostingList>, TransparentStringHash, TransparentStringEqual>
      term_postings_;

  // Shared mutex for read/write protection
  // - Readers (Search): shared_lock (multiple concurrent readers allowed)
  // - Writers (Add/Update/Remove/Optimize): unique_lock (exclusive access)
  // Lock ordering (acquire in this order to prevent deadlock):
  //   postings_mutex_ → PostingList::mutex_
  // When postings_mutex_ is held (shared or exclusive), it is safe to call
  // PostingList's lock-free accessors (SizeApprox, MemoryUsageApprox).
  mutable std::shared_mutex postings_mutex_;

  // Flag to prevent concurrent optimization
  std::atomic<bool> is_optimizing_{false};

  /**
   * @brief Get or create posting list for term
   */
  PostingList* GetOrCreatePostingList(std::string_view term);

  /**
   * @brief RCU snapshot helpers for lock-free search
   *
   * These methods take a short lock to copy shared_ptrs, then release the lock.
   * The actual search operations can then proceed without holding any lock.
   */

  /**
   * @brief Take snapshot of posting lists for given terms (RCU pattern)
   * @param terms Search terms
   * @return Vector of shared_ptr to posting lists (empty ptr if term not found)
   */
  [[nodiscard]] std::vector<std::shared_ptr<PostingList>> TakePostingSnapshots(
      const std::vector<std::string>& terms) const;

  /**
   * @brief Take snapshot of a single posting list (RCU pattern)
   * @param term Search term
   * @return shared_ptr to posting list (nullptr if term not found)
   */
  [[nodiscard]] std::shared_ptr<PostingList> TakePostingSnapshot(std::string_view term) const;
};

}  // namespace mygramdb::index
