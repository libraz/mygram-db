/**
 * @file posting_list.h
 * @brief Posting list implementation with delta encoding and Roaring bitmaps
 */

#pragma once

#include <roaring/roaring.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "types/doc_id.h"

namespace mygramdb::index {

/**
 * @brief Default density threshold for Roaring bitmap strategy (18%)
 */
constexpr double kDefaultRoaringThreshold = 0.18;

// DocId is now defined in types/doc_id.h and re-exported via namespace

/**
 * @brief Posting list storage strategies
 */
enum class PostingStrategy : uint8_t {
  kDeltaCompressed,  // Delta-encoded varint array (sparse)
  kRoaringBitmap     // Roaring bitmap (dense)
};

/**
 * @brief Posting list for a single term
 *
 * Stores document IDs in one of two formats:
 * - Delta-compressed varint array for sparse postings
 * - Roaring bitmap for dense postings (auto-selected based on threshold)
 */
class PostingList {
  friend class Index;  // Allow Index to call lock-free accessors

 public:
  /**
   * @brief Construct empty posting list
   * @param roaring_threshold Density threshold for Roaring bitmap (0.0-1.0)
   */
  explicit PostingList(double roaring_threshold = kDefaultRoaringThreshold);

  /**
   * @brief Destructor
   */
  ~PostingList();

  // Disable copy (use move or Clone)
  PostingList(const PostingList&) = delete;
  PostingList& operator=(const PostingList&) = delete;

  // Enable move
  PostingList(PostingList&& other) noexcept;
  PostingList& operator=(PostingList&& other) noexcept;

  /**
   * @brief Add document ID to posting list
   * @param doc_id Document ID to add
   */
  void Add(DocId doc_id);

  /**
   * @brief Add multiple document IDs (sorted)
   * @param doc_ids Sorted vector of document IDs
   */
  void AddBatch(const std::vector<DocId>& doc_ids);

  /**
   * @brief Remove document ID from posting list
   * @param doc_id Document ID to remove
   */
  void Remove(DocId doc_id);

  /**
   * @brief Check if document ID exists
   * @param doc_id Document ID to check
   * @return true if exists
   */
  [[nodiscard]] bool Contains(DocId doc_id) const;

  /**
   * @brief Get all document IDs
   * @return Vector of document IDs (sorted)
   */
  [[nodiscard]] std::vector<DocId> GetAll() const;

  /**
   * @brief Get top N document IDs with optional reverse order
   *
   * Performance optimization for queries with LIMIT and ORDER BY:
   * - Returns up to 'limit' document IDs
   * - Reverse order enables efficient "ORDER BY primary_key DESC LIMIT N" queries
   * - For Roaring bitmaps: uses reverse iterator (no full materialization)
   * - For delta-compressed: decodes delta-compressed list and returns the last N elements
   *
   * @param limit Maximum number of documents to return (0 = all documents)
   * @param reverse If true, returns highest DocIds first (descending order)
   * @return Vector of document IDs (up to 'limit' elements)
   */
  [[nodiscard]] std::vector<DocId> GetTopN(size_t limit, bool reverse = false) const;

  /**
   * @brief Get document count
   * @return Number of documents in posting list
   */
  [[nodiscard]] uint64_t Size() const;

  /**
   * @brief Get mutation version counter
   *
   * Incremented on every Add/AddBatch/Remove operation. Used by Index::Optimize()
   * to detect concurrent modifications (including balanced Remove+Add that leave
   * size unchanged).
   *
   * @return Current version number
   */
  [[nodiscard]] uint64_t Version() const { return version_.load(std::memory_order_acquire); }

  /**
   * @brief Get memory usage in bytes
   * @return Memory used by this posting list
   */
  [[nodiscard]] size_t MemoryUsage() const;

  /**
   * @brief Get current strategy
   */
  [[nodiscard]] PostingStrategy GetStrategy() const { return strategy_.load(std::memory_order_acquire); }

  /**
   * @brief Intersect with another posting list
   * @param other Other posting list
   * @return New posting list with intersection
   */
  [[nodiscard]] std::unique_ptr<PostingList> Intersect(const PostingList& other) const;

  /**
   * @brief Union with another posting list
   * @param other Other posting list
   * @return New posting list with union
   */
  [[nodiscard]] std::unique_ptr<PostingList> Union(const PostingList& other) const;

  /**
   * @brief Optimize storage (convert to Roaring if beneficial)
   * @param total_docs Total number of documents (for density calculation)
   */
  void Optimize(uint64_t total_docs);

  /**
   * @brief Create optimized copy of this posting list
   * @param total_docs Total number of documents (for density calculation)
   * @return New posting list with optimized storage
   */
  [[nodiscard]] std::shared_ptr<PostingList> Clone(uint64_t total_docs) const;

  /**
   * @brief Serialize posting list to buffer
   * @param buffer Output buffer
   * @return true if serialization succeeded, false if data is too large
   */
  bool Serialize(std::vector<uint8_t>& buffer) const;

  /**
   * @brief Deserialize posting list from buffer
   * @param buffer Input buffer
   * @param offset Current offset (will be updated)
   * @return true if successful
   */
  bool Deserialize(const std::vector<uint8_t>& buffer, size_t& offset);

  /**
   * @brief Get approximate document count without acquiring mutex
   *
   * Returns the cached doc_count_ via atomic load. This avoids the overhead
   * of acquiring the per-PostingList shared_mutex, making it suitable for
   * use in hot paths (e.g., query planning in SearchAnd) where the caller
   * already holds a higher-level lock or operates on an immutable snapshot.
   *
   * @return Approximate document count
   */
  [[nodiscard]] uint64_t SizeApprox() const;

  /**
   * @brief Get approximate memory usage without acquiring mutex
   *
   * Returns memory usage estimate via atomic strategy check. Avoids the
   * overhead of acquiring the per-PostingList shared_mutex.
   *
   * @return Approximate memory usage in bytes
   */
  [[nodiscard]] size_t MemoryUsageApprox() const;

 private:
  std::atomic<PostingStrategy> strategy_{PostingStrategy::kDeltaCompressed};
  double roaring_threshold_;

  // Delta-compressed storage
  std::vector<uint32_t> delta_compressed_;

  // Cached last DocId for O(1) fast-path append in Add()
  DocId last_doc_id_ = 0;

  // Roaring bitmap storage
  roaring_bitmap_t* roaring_bitmap_ = nullptr;

  // Approximate document count for lock-free reads (H-14).
  // Updated atomically inside mutation methods that already hold the exclusive lock.
  std::atomic<uint64_t> doc_count_{0};

  // Cached memory usage for lock-free reads in MemoryUsageApprox().
  // Updated atomically inside mutation methods that already hold the exclusive lock.
  std::atomic<size_t> cached_memory_size_{0};

  // Mutation version counter for change detection in Index::Optimize().
  // Atomic (not protected by mutex_) - incremented inside mutation methods
  // that already hold the exclusive lock.
  std::atomic<uint64_t> version_{0};

  // Protects all data members for thread-safe read/write operations
  // Uses shared_mutex to allow concurrent reads while serializing writes
  mutable std::shared_mutex mutex_;

  /**
   * @brief Update doc_count_ and increment version_ after a mutation
   *
   * Consolidates the repeated pattern of reading the current strategy,
   * storing the appropriate cardinality into doc_count_, and bumping version_.
   *
   * @note Caller must already hold mutex_ exclusively
   */
  void UpdateCountsAndVersion();

  /**
   * @brief Recompute and cache last_doc_id_ from delta_compressed_
   * @note Caller must already hold mutex_ exclusively
   */
  void RecomputeLastDocId();

  /**
   * @brief Convert from delta to Roaring
   */
  void ConvertToRoaring();

  /**
   * @brief Convert from Roaring to delta
   */
  void ConvertToDelta();

  /**
   * @brief Encode document IDs with delta compression
   */
  static std::vector<uint32_t> EncodeDelta(const std::vector<DocId>& doc_ids);

  /**
   * @brief Decode delta-compressed document IDs
   */
  static std::vector<DocId> DecodeDelta(const std::vector<uint32_t>& encoded);
};

}  // namespace mygramdb::index
