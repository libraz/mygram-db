/**
 * @file posting_list.h
 * @brief Posting list implementation with delta encoding and Roaring bitmaps
 */

#pragma once

#include <roaring/roaring.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

namespace mygramdb::index {

/**
 * @brief Default density threshold for Roaring bitmap strategy (18%)
 */
constexpr double kDefaultRoaringThreshold = 0.18;

/**
 * @brief Document ID type (32-bit, supports up to 4B documents)
 */
using DocId = uint32_t;

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
   * - Returns up to 'limit' document IDs without materializing entire posting list
   * - Reverse order enables efficient "ORDER BY primary_key DESC LIMIT N" queries
   * - For Roaring bitmaps: uses reverse iterator (no full materialization)
   * - For delta-compressed: decodes and returns last N elements
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
   * @brief Get memory usage in bytes
   * @return Memory used by this posting list
   */
  [[nodiscard]] size_t MemoryUsage() const;

  /**
   * @brief Get current strategy
   */
  [[nodiscard]] PostingStrategy GetStrategy() const { return strategy_; }

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
   */
  void Serialize(std::vector<uint8_t>& buffer) const;

  /**
   * @brief Deserialize posting list from buffer
   * @param buffer Input buffer
   * @param offset Current offset (will be updated)
   * @return true if successful
   */
  bool Deserialize(const std::vector<uint8_t>& buffer, size_t& offset);

 private:
  PostingStrategy strategy_ = PostingStrategy::kDeltaCompressed;
  double roaring_threshold_;

  // Delta-compressed storage
  std::vector<uint32_t> delta_compressed_;

  // Roaring bitmap storage
  roaring_bitmap_t* roaring_bitmap_ = nullptr;

  // Protects all data members for thread-safe read/write operations
  // Uses shared_mutex to allow concurrent reads while serializing writes
  mutable std::shared_mutex mutex_;

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
