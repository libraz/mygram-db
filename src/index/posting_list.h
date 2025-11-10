/**
 * @file posting_list.h
 * @brief Posting list implementation with delta encoding and Roaring bitmaps
 */

#pragma once

#include <cstdint>
#include <vector>
#include <memory>
#include <roaring/roaring.h>

namespace mygramdb {
namespace index {

/**
 * @brief Document ID type (32-bit)
 */
using DocId = uint32_t;

/**
 * @brief Posting list storage strategies
 */
enum class PostingStrategy {
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
  explicit PostingList(double roaring_threshold = 0.18);

  /**
   * @brief Destructor
   */
  ~PostingList();

  // Disable copy (use move or Clone)
  PostingList(const PostingList&) = delete;
  PostingList& operator=(const PostingList&) = delete;

  // Enable move
  PostingList(PostingList&&) noexcept;
  PostingList& operator=(PostingList&&) noexcept;

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
  bool Contains(DocId doc_id) const;

  /**
   * @brief Get all document IDs
   * @return Vector of document IDs (sorted)
   */
  std::vector<DocId> GetAll() const;

  /**
   * @brief Get document count
   * @return Number of documents in posting list
   */
  uint64_t Size() const;

  /**
   * @brief Get memory usage in bytes
   * @return Memory used by this posting list
   */
  size_t MemoryUsage() const;

  /**
   * @brief Get current strategy
   */
  PostingStrategy GetStrategy() const { return strategy_; }

  /**
   * @brief Intersect with another posting list
   * @param other Other posting list
   * @return New posting list with intersection
   */
  std::unique_ptr<PostingList> Intersect(const PostingList& other) const;

  /**
   * @brief Union with another posting list
   * @param other Other posting list
   * @return New posting list with union
   */
  std::unique_ptr<PostingList> Union(const PostingList& other) const;

  /**
   * @brief Optimize storage (convert to Roaring if beneficial)
   * @param total_docs Total number of documents (for density calculation)
   */
  void Optimize(uint64_t total_docs);

 private:
  PostingStrategy strategy_;
  double roaring_threshold_;

  // Delta-compressed storage
  std::vector<uint32_t> delta_compressed_;

  // Roaring bitmap storage
  roaring_bitmap_t* roaring_bitmap_;

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

}  // namespace index
}  // namespace mygramdb
