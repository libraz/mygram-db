/**
 * @file posting_list.cpp
 * @brief Posting list implementation
 */

#include "index/posting_list.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstdlib>

#include "utils/structured_log.h"

namespace mygramdb::index {

// Hysteresis factor to prevent oscillation between delta and roaring formats
constexpr double kHysteresisFactor = 0.5;

// Binary serialization constants
constexpr uint32_t kBitsPerByte = 8;
constexpr uint32_t kShift16Bits = 16;
constexpr uint32_t kShift24Bits = 24;
constexpr uint32_t kByteMask = 0xFF;

namespace {

/**
 * @brief Helper to get char* pointer for Roaring Bitmap serialization
 *
 * Roaring Bitmap C API requires char* for serialization output.
 * This helper encapsulates the required type conversion and pointer arithmetic.
 *
 * Why reinterpret_cast and pointer arithmetic are necessary:
 * - roaring_bitmap_portable_serialize() requires char* as output buffer
 * - We use std::vector<uint8_t> for type-safe memory management
 * - uint8_t* and char* are binary-compatible but different types
 * - Pointer arithmetic is needed to write at specific buffer offsets
 * - This is the standard pattern for C library integration
 *
 * @param buffer Vector to write serialized data
 * @param offset Offset in the buffer where serialization should start
 * @return char* pointer to the offset position
 */
inline char* GetSerializationPointer(std::vector<uint8_t>& buffer, size_t offset) {
  // Suppressing clang-tidy warnings for Roaring Bitmap C API compatibility
  // Both casts are required and safe for binary-compatible types
  return reinterpret_cast<char*>(  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
      buffer.data() + offset);     // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

/**
 * @brief Helper to get const char* pointer for Roaring Bitmap deserialization
 *
 * Roaring Bitmap C API requires const char* for deserialization input.
 * This helper encapsulates the required type conversion and pointer arithmetic.
 *
 * Why reinterpret_cast and pointer arithmetic are necessary:
 * - roaring_bitmap_portable_deserialize() requires const char* as input
 * - We use std::vector<uint8_t> for type-safe memory management
 * - const uint8_t* and const char* are binary-compatible but different types
 * - Pointer arithmetic is needed to read from specific buffer offsets
 * - This is the standard pattern for C library integration
 *
 * @param buffer Vector containing serialized data
 * @param offset Offset in the buffer where deserialization should start
 * @return const char* pointer to the offset position
 */
inline const char* GetDeserializationPointer(const std::vector<uint8_t>& buffer, size_t offset) {
  // Suppressing clang-tidy warnings for Roaring Bitmap C API compatibility
  // Both casts are required and safe for binary-compatible types
  return reinterpret_cast<const char*>(  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
      buffer.data() + offset);           // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
}

}  // namespace

PostingList::PostingList(double roaring_threshold) : roaring_threshold_(roaring_threshold) {}

PostingList::~PostingList() {
  if (roaring_bitmap_ != nullptr) {
    roaring_bitmap_free(roaring_bitmap_);
  }
}

PostingList::PostingList(PostingList&& other) noexcept
    : strategy_(other.strategy_),
      roaring_threshold_(other.roaring_threshold_),
      delta_compressed_(std::move(other.delta_compressed_)),
      roaring_bitmap_(other.roaring_bitmap_) {
  other.roaring_bitmap_ = nullptr;
}

PostingList& PostingList::operator=(PostingList&& other) noexcept {
  if (this != &other) {
    if (roaring_bitmap_ != nullptr) {
      roaring_bitmap_free(roaring_bitmap_);
    }
    strategy_ = other.strategy_;
    roaring_threshold_ = other.roaring_threshold_;
    delta_compressed_ = std::move(other.delta_compressed_);
    roaring_bitmap_ = other.roaring_bitmap_;
    other.roaring_bitmap_ = nullptr;
  }
  return *this;
}

void PostingList::Add(DocId doc_id) {
  std::unique_lock lock(mutex_);  // Exclusive access for write
  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    // For delta-compressed strategy, decode, modify, and re-encode
    // This is simpler and more maintainable than in-place delta manipulation
    auto docs = DecodeDelta(delta_compressed_);
    auto iter = std::lower_bound(docs.begin(), docs.end(), doc_id);
    if (iter == docs.end() || *iter != doc_id) {
      docs.insert(iter, doc_id);
      delta_compressed_ = EncodeDelta(docs);
    }
  } else {
    roaring_bitmap_add(roaring_bitmap_, doc_id);
  }
}

void PostingList::AddBatch(const std::vector<DocId>& doc_ids) {
  if (doc_ids.empty()) {
    return;
  }

  std::unique_lock lock(mutex_);  // Exclusive access for write
  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    // Merge sorted arrays
    auto existing = DecodeDelta(delta_compressed_);
    std::vector<DocId> merged;
    merged.reserve(existing.size() + doc_ids.size());
    std::set_union(existing.begin(), existing.end(), doc_ids.begin(), doc_ids.end(), std::back_inserter(merged));
    delta_compressed_ = EncodeDelta(merged);
  } else {
    roaring_bitmap_add_many(roaring_bitmap_, doc_ids.size(), doc_ids.data());
  }
}

void PostingList::Remove(DocId doc_id) {
  std::unique_lock lock(mutex_);  // Exclusive access for write
  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    // For delta-compressed strategy, decode, modify, and re-encode
    // This is simpler and more maintainable than in-place delta manipulation
    auto docs = DecodeDelta(delta_compressed_);
    auto iter = std::find(docs.begin(), docs.end(), doc_id);
    if (iter != docs.end()) {
      docs.erase(iter);
      delta_compressed_ = EncodeDelta(docs);
    }
  } else {
    roaring_bitmap_remove(roaring_bitmap_, doc_id);
  }
}

bool PostingList::Contains(DocId doc_id) const {
  std::shared_lock lock(mutex_);  // Protect read access
  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    if (delta_compressed_.empty()) {
      return false;
    }

    // Quick check for first element
    if (delta_compressed_[0] == doc_id) {
      return true;
    }
    if (delta_compressed_[0] > doc_id) {
      return false;
    }

    // For small arrays, linear search is faster than repeated accumulation
    // Threshold chosen based on performance profiling
    constexpr size_t kLinearSearchThreshold = 16;
    if (delta_compressed_.size() <= kLinearSearchThreshold) {
      DocId current = 0;
      for (const auto& delta : delta_compressed_) {
        current += delta;
        if (current == doc_id) {
          return true;
        }
        if (current > doc_id) {
          return false;
        }
      }
      return false;
    }

    // Binary search for larger arrays
    // Cache accumulated values during search to avoid redundant computation
    // For large posting lists, decode fully first for O(n) + O(log n) instead of O(n log n)
    // Threshold: 64 elements (empirically determined for best performance)
    constexpr size_t kDecodeThreshold = 64;

    if (delta_compressed_.size() > kDecodeThreshold) {
      // Decode delta-compressed array to absolute values
      std::vector<DocId> decoded;
      decoded.reserve(delta_compressed_.size());
      DocId cumulative = 0;
      for (DocId delta : delta_compressed_) {
        cumulative += delta;
        decoded.push_back(cumulative);
      }

      // Binary search on decoded array (O(log n))
      return std::binary_search(decoded.begin(), decoded.end(), doc_id);
    }

    // For small lists, use linear scan (simpler and faster for small sizes)
    DocId cumulative = 0;
    for (DocId delta : delta_compressed_) {
      cumulative += delta;
      if (cumulative == doc_id) {
        return true;
      }
      if (cumulative > doc_id) {
        return false;  // Passed target, not found
      }
    }

    return false;
  }
  return roaring_bitmap_contains(roaring_bitmap_, doc_id);
}

std::vector<DocId> PostingList::GetAll() const {
  std::shared_lock lock(mutex_);  // Protect read access
  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    return DecodeDelta(delta_compressed_);
  }
  uint64_t size = roaring_bitmap_get_cardinality(roaring_bitmap_);
  std::vector<DocId> result(size);
  roaring_bitmap_to_uint32_array(roaring_bitmap_, result.data());
  return result;
}

std::vector<DocId> PostingList::GetTopN(size_t limit, bool reverse) const {
  std::shared_lock lock(mutex_);  // Protect read access

  // If limit is 0, return all documents
  if (limit == 0) {
    std::vector<DocId> result;
    if (strategy_ == PostingStrategy::kDeltaCompressed) {
      result = DecodeDelta(delta_compressed_);
    } else {
      uint64_t size = roaring_bitmap_get_cardinality(roaring_bitmap_);
      result.resize(size);
      roaring_bitmap_to_uint32_array(roaring_bitmap_, result.data());
    }
    if (reverse) {
      std::reverse(result.begin(), result.end());
    }
    return result;
  }

  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    // Delta-compressed: decode and extract top N
    auto all_docs = DecodeDelta(delta_compressed_);
    size_t actual_limit = std::min(limit, all_docs.size());

    std::vector<DocId> result;
    result.reserve(actual_limit);

    if (reverse) {
      // Return last N elements in reverse order (highest DocIds first)
      auto start_it = all_docs.rbegin();
      auto end_it = all_docs.rbegin() + static_cast<std::vector<DocId>::difference_type>(actual_limit);
      result.assign(start_it, end_it);
    } else {
      // Return first N elements (lowest DocIds first)
      auto end_it = all_docs.begin() + static_cast<std::vector<DocId>::difference_type>(actual_limit);
      result.assign(all_docs.begin(), end_it);
    }
    return result;
  }

  // Roaring bitmap: use iterator for efficient top-N retrieval
  uint64_t total_size = roaring_bitmap_get_cardinality(roaring_bitmap_);
  size_t actual_limit = std::min(limit, static_cast<size_t>(total_size));

  std::vector<DocId> result;
  result.reserve(actual_limit);

  if (reverse) {
    // For reverse order: use reverse iterator to get last N elements efficiently
    // CRoaring library requires manual memory management for iterators
    // Use RAII via unique_ptr with custom deleter for exception safety
    // NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cppcoreguidelines-no-malloc)
    auto* raw_iter = static_cast<roaring_uint32_iterator_t*>(malloc(sizeof(roaring_uint32_iterator_t)));
    if (raw_iter != nullptr) {
      // RAII wrapper ensures free() is called even if push_back throws
      auto deleter = [](roaring_uint32_iterator_t* ptr) {
        // NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cppcoreguidelines-no-malloc)
        free(ptr);
      };
      std::unique_ptr<roaring_uint32_iterator_t, decltype(deleter)> iter(raw_iter, deleter);

      roaring_iterator_init_last(roaring_bitmap_, iter.get());
      size_t count = 0;
      while (count < actual_limit && iter->has_value) {
        result.push_back(iter->current_value);
        roaring_uint32_iterator_previous(iter.get());
        count++;
      }
      // iter automatically freed by unique_ptr destructor
    }
  } else {
    // For forward order: use iterator to get first N
    // Use RAII via unique_ptr with custom deleter for exception safety
    auto deleter = [](roaring_uint32_iterator_t* ptr) { roaring_uint32_iterator_free(ptr); };
    std::unique_ptr<roaring_uint32_iterator_t, decltype(deleter)> iter(roaring_iterator_create(roaring_bitmap_),
                                                                       deleter);
    if (iter != nullptr) {
      size_t count = 0;
      while (count < actual_limit && iter->has_value) {
        result.push_back(iter->current_value);
        roaring_uint32_iterator_advance(iter.get());
        count++;
      }
      // iter automatically freed by unique_ptr destructor
    }
  }

  return result;
}

uint64_t PostingList::Size() const {
  std::shared_lock lock(mutex_);  // Protect read access
  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    return delta_compressed_.size();
  }
  return roaring_bitmap_get_cardinality(roaring_bitmap_);
}

size_t PostingList::MemoryUsage() const {
  std::shared_lock lock(mutex_);  // Protect read access
  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    return delta_compressed_.size() * sizeof(uint32_t);
  }
  return roaring_bitmap_portable_size_in_bytes(roaring_bitmap_);
}

std::unique_ptr<PostingList> PostingList::Intersect(const PostingList& other) const {
  std::shared_lock lock1(mutex_);        // Protect read access to this
  std::shared_lock lock2(other.mutex_);  // Protect read access to other

  auto result = std::make_unique<PostingList>(roaring_threshold_);

  if (strategy_ == PostingStrategy::kRoaringBitmap && other.strategy_ == PostingStrategy::kRoaringBitmap) {
    // Both Roaring: use fast bitmap AND
    result->strategy_ = PostingStrategy::kRoaringBitmap;
    result->roaring_bitmap_ = roaring_bitmap_and(roaring_bitmap_, other.roaring_bitmap_);
  } else {
    // At least one is delta: fall back to sorted array intersection
    // Note: GetAll() would try to acquire the lock again, so we inline the logic
    std::vector<DocId> docs1;
    if (strategy_ == PostingStrategy::kDeltaCompressed) {
      docs1 = DecodeDelta(delta_compressed_);
    } else {
      uint64_t size1 = roaring_bitmap_get_cardinality(roaring_bitmap_);
      docs1.resize(size1);
      roaring_bitmap_to_uint32_array(roaring_bitmap_, docs1.data());
    }

    std::vector<DocId> docs2;
    if (other.strategy_ == PostingStrategy::kDeltaCompressed) {
      docs2 = DecodeDelta(other.delta_compressed_);
    } else {
      uint64_t size2 = roaring_bitmap_get_cardinality(other.roaring_bitmap_);
      docs2.resize(size2);
      roaring_bitmap_to_uint32_array(other.roaring_bitmap_, docs2.data());
    }

    std::vector<DocId> intersection;
    std::set_intersection(docs1.begin(), docs1.end(), docs2.begin(), docs2.end(), std::back_inserter(intersection));
    result->delta_compressed_ = EncodeDelta(intersection);
  }

  return result;
}

std::unique_ptr<PostingList> PostingList::Union(const PostingList& other) const {
  std::shared_lock lock1(mutex_);        // Protect read access to this
  std::shared_lock lock2(other.mutex_);  // Protect read access to other

  auto result = std::make_unique<PostingList>(roaring_threshold_);

  if (strategy_ == PostingStrategy::kRoaringBitmap && other.strategy_ == PostingStrategy::kRoaringBitmap) {
    // Both Roaring: use fast bitmap OR
    result->strategy_ = PostingStrategy::kRoaringBitmap;
    result->roaring_bitmap_ = roaring_bitmap_or(roaring_bitmap_, other.roaring_bitmap_);
  } else {
    // At least one is delta: fall back to sorted array union
    // Note: GetAll() would try to acquire the lock again, so we inline the logic
    std::vector<DocId> docs1;
    if (strategy_ == PostingStrategy::kDeltaCompressed) {
      docs1 = DecodeDelta(delta_compressed_);
    } else {
      uint64_t size1 = roaring_bitmap_get_cardinality(roaring_bitmap_);
      docs1.resize(size1);
      roaring_bitmap_to_uint32_array(roaring_bitmap_, docs1.data());
    }

    std::vector<DocId> docs2;
    if (other.strategy_ == PostingStrategy::kDeltaCompressed) {
      docs2 = DecodeDelta(other.delta_compressed_);
    } else {
      uint64_t size2 = roaring_bitmap_get_cardinality(other.roaring_bitmap_);
      docs2.resize(size2);
      roaring_bitmap_to_uint32_array(other.roaring_bitmap_, docs2.data());
    }

    std::vector<DocId> union_result;
    std::set_union(docs1.begin(), docs1.end(), docs2.begin(), docs2.end(), std::back_inserter(union_result));
    result->delta_compressed_ = EncodeDelta(union_result);
  }

  return result;
}

void PostingList::Optimize(uint64_t total_docs) {
  std::unique_lock lock(mutex_);  // Exclusive access for write
  if (total_docs == 0) {
    return;
  }

  // Calculate size without calling Size() to avoid recursive locking
  uint64_t size = 0;
  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    size = delta_compressed_.size();
  } else {
    size = roaring_bitmap_get_cardinality(roaring_bitmap_);
  }

  double density = static_cast<double>(size) / static_cast<double>(total_docs);

  if (density >= roaring_threshold_ && strategy_ == PostingStrategy::kDeltaCompressed) {
    // Convert to Roaring for high density
    ConvertToRoaring();
    mygram::utils::StructuredLog()
        .Event("posting_list_converted")
        .Field("to", "roaring")
        .Field("density", density)
        .Debug();
  } else if (density < roaring_threshold_ * kHysteresisFactor && strategy_ == PostingStrategy::kRoaringBitmap) {
    // Convert back to delta for low density (with hysteresis)
    ConvertToDelta();
    mygram::utils::StructuredLog()
        .Event("posting_list_converted")
        .Field("to", "delta")
        .Field("density", density)
        .Debug();
  }
}

std::shared_ptr<PostingList> PostingList::Clone(uint64_t total_docs) const {
  std::shared_lock lock(mutex_);  // Protect read access to internal state

  auto cloned = std::make_shared<PostingList>(roaring_threshold_);

  // Get all document IDs from current posting list
  // Note: GetAll() is called with the lock already held
  std::vector<DocId> docs;
  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    docs = DecodeDelta(delta_compressed_);
  } else {
    uint64_t size = roaring_bitmap_get_cardinality(roaring_bitmap_);
    docs.resize(size);
    roaring_bitmap_to_uint32_array(roaring_bitmap_, docs.data());
  }

  lock.unlock();  // Release lock before expensive operations

  // Build the cloned posting list (no longer needs lock on original)
  if (!docs.empty()) {
    cloned->AddBatch(docs);
  }

  // Optimize the cloned posting list based on density
  if (total_docs > 0) {
    cloned->Optimize(total_docs);
  }

  return cloned;
}

void PostingList::ConvertToRoaring() {
  if (strategy_ == PostingStrategy::kRoaringBitmap) {
    return;
  }

  auto docs = DecodeDelta(delta_compressed_);
  roaring_bitmap_ = roaring_bitmap_create();
  if (!docs.empty()) {
    roaring_bitmap_add_many(roaring_bitmap_, docs.size(), docs.data());
  }
  roaring_bitmap_run_optimize(roaring_bitmap_);

  delta_compressed_.clear();
  delta_compressed_.shrink_to_fit();
  strategy_ = PostingStrategy::kRoaringBitmap;
}

void PostingList::ConvertToDelta() {
  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    return;
  }

  auto docs = GetAll();
  delta_compressed_ = EncodeDelta(docs);

  roaring_bitmap_free(roaring_bitmap_);
  roaring_bitmap_ = nullptr;
  strategy_ = PostingStrategy::kDeltaCompressed;
}

std::vector<uint32_t> PostingList::EncodeDelta(const std::vector<DocId>& doc_ids) {
  if (doc_ids.empty()) {
    return {};
  }

  std::vector<uint32_t> encoded;
  encoded.reserve(doc_ids.size());

  // First value as-is
  encoded.push_back(doc_ids[0]);

  // Rest as deltas
  for (size_t i = 1; i < doc_ids.size(); ++i) {
    encoded.push_back(doc_ids[i] - doc_ids[i - 1]);
  }

  return encoded;
}

std::vector<DocId> PostingList::DecodeDelta(const std::vector<uint32_t>& encoded) {
  if (encoded.empty()) {
    return {};
  }

  std::vector<DocId> decoded;
  decoded.reserve(encoded.size());

  // First value as-is
  decoded.push_back(encoded[0]);

  // Reconstruct from deltas
  for (size_t i = 1; i < encoded.size(); ++i) {
    decoded.push_back(decoded[i - 1] + encoded[i]);
  }

  return decoded;
}

void PostingList::Serialize(std::vector<uint8_t>& buffer) const {
  std::shared_lock lock(mutex_);  // Protect read access

  // Format:
  // [1 byte: strategy] [4 bytes: size] [data...]

  // Write strategy
  buffer.push_back(static_cast<uint8_t>(strategy_));

  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    // Write size
    auto size = static_cast<uint32_t>(delta_compressed_.size());
    buffer.push_back((size >> kShift24Bits) & kByteMask);
    buffer.push_back((size >> kShift16Bits) & kByteMask);
    buffer.push_back((size >> kBitsPerByte) & kByteMask);
    buffer.push_back(size & kByteMask);

    // Write delta-compressed data
    for (uint32_t val : delta_compressed_) {
      buffer.push_back((val >> kShift24Bits) & kByteMask);
      buffer.push_back((val >> kShift16Bits) & kByteMask);
      buffer.push_back((val >> kBitsPerByte) & kByteMask);
      buffer.push_back(val & kByteMask);
    }
  } else {
    // Roaring bitmap: serialize using roaring's native format
    size_t roaring_size = roaring_bitmap_portable_size_in_bytes(roaring_bitmap_);

    // Write size
    buffer.push_back((roaring_size >> kShift24Bits) & kByteMask);
    buffer.push_back((roaring_size >> kShift16Bits) & kByteMask);
    buffer.push_back((roaring_size >> kBitsPerByte) & kByteMask);
    buffer.push_back(roaring_size & kByteMask);

    // Write roaring bitmap data
    size_t old_size = buffer.size();
    buffer.resize(old_size + roaring_size);
    roaring_bitmap_portable_serialize(roaring_bitmap_, GetSerializationPointer(buffer, old_size));
  }
}

bool PostingList::Deserialize(const std::vector<uint8_t>& buffer, size_t& offset) {
  std::unique_lock lock(mutex_);  // Exclusive access for write

  if (offset >= buffer.size()) {
    return false;
  }

  // Read strategy
  strategy_ = static_cast<PostingStrategy>(buffer[offset++]);

  if (offset + 4 > buffer.size()) {
    return false;
  }

  // Read size
  uint32_t size = (static_cast<uint32_t>(buffer[offset]) << kShift24Bits) |
                  (static_cast<uint32_t>(buffer[offset + 1]) << kShift16Bits) |
                  (static_cast<uint32_t>(buffer[offset + 2]) << kBitsPerByte) |
                  static_cast<uint32_t>(buffer[offset + 3]);
  offset += 4;

  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    // Read delta-compressed data
    if (offset + (size * 4) > buffer.size()) {
      return false;
    }

    delta_compressed_.clear();
    delta_compressed_.reserve(size);

    for (uint32_t i = 0; i < size; ++i) {
      uint32_t val = (static_cast<uint32_t>(buffer[offset]) << kShift24Bits) |
                     (static_cast<uint32_t>(buffer[offset + 1]) << kShift16Bits) |
                     (static_cast<uint32_t>(buffer[offset + 2]) << kBitsPerByte) |
                     static_cast<uint32_t>(buffer[offset + 3]);
      delta_compressed_.push_back(val);
      offset += 4;
    }

    if (roaring_bitmap_ != nullptr) {
      roaring_bitmap_free(roaring_bitmap_);
      roaring_bitmap_ = nullptr;
    }
  } else {
    // Read roaring bitmap
    if (offset + size > buffer.size()) {
      return false;
    }

    if (roaring_bitmap_ != nullptr) {
      roaring_bitmap_free(roaring_bitmap_);
    }

    roaring_bitmap_ = roaring_bitmap_portable_deserialize(GetDeserializationPointer(buffer, offset));

    if (roaring_bitmap_ == nullptr) {
      return false;
    }

    offset += size;
    delta_compressed_.clear();
  }

  return true;
}

}  // namespace mygramdb::index
