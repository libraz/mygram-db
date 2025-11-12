/**
 * @file posting_list.cpp
 * @brief Posting list implementation
 */

#include "index/posting_list.h"

#include <spdlog/spdlog.h>

#include <algorithm>

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
  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    // Get current docs, add new one, re-encode
    auto docs = DecodeDelta(delta_compressed_);
    auto iterator = std::lower_bound(docs.begin(), docs.end(), doc_id);
    if (iterator == docs.end() || *iterator != doc_id) {
      docs.insert(iterator, doc_id);
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
  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    auto docs = DecodeDelta(delta_compressed_);
    auto iterator = std::find(docs.begin(), docs.end(), doc_id);
    if (iterator != docs.end()) {
      docs.erase(iterator);
      delta_compressed_ = EncodeDelta(docs);
    }
  } else {
    roaring_bitmap_remove(roaring_bitmap_, doc_id);
  }
}

bool PostingList::Contains(DocId doc_id) const {
  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    auto docs = DecodeDelta(delta_compressed_);
    return std::binary_search(docs.begin(), docs.end(), doc_id);
  }
  return roaring_bitmap_contains(roaring_bitmap_, doc_id);
}

std::vector<DocId> PostingList::GetAll() const {
  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    return DecodeDelta(delta_compressed_);
  }
  uint64_t size = roaring_bitmap_get_cardinality(roaring_bitmap_);
  std::vector<DocId> result(size);
  roaring_bitmap_to_uint32_array(roaring_bitmap_, result.data());
  return result;
}

uint64_t PostingList::Size() const {
  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    return DecodeDelta(delta_compressed_).size();
  }
  return roaring_bitmap_get_cardinality(roaring_bitmap_);
}

size_t PostingList::MemoryUsage() const {
  if (strategy_ == PostingStrategy::kDeltaCompressed) {
    return delta_compressed_.size() * sizeof(uint32_t);
  }
  return roaring_bitmap_portable_size_in_bytes(roaring_bitmap_);
}

std::unique_ptr<PostingList> PostingList::Intersect(const PostingList& other) const {
  auto result = std::make_unique<PostingList>(roaring_threshold_);

  if (strategy_ == PostingStrategy::kRoaringBitmap && other.strategy_ == PostingStrategy::kRoaringBitmap) {
    // Both Roaring: use fast bitmap AND
    result->strategy_ = PostingStrategy::kRoaringBitmap;
    result->roaring_bitmap_ = roaring_bitmap_and(roaring_bitmap_, other.roaring_bitmap_);
  } else {
    // At least one is delta: fall back to sorted array intersection
    auto docs1 = GetAll();
    auto docs2 = other.GetAll();
    std::vector<DocId> intersection;
    std::set_intersection(docs1.begin(), docs1.end(), docs2.begin(), docs2.end(), std::back_inserter(intersection));
    result->delta_compressed_ = EncodeDelta(intersection);
  }

  return result;
}

std::unique_ptr<PostingList> PostingList::Union(const PostingList& other) const {
  auto result = std::make_unique<PostingList>(roaring_threshold_);

  if (strategy_ == PostingStrategy::kRoaringBitmap && other.strategy_ == PostingStrategy::kRoaringBitmap) {
    // Both Roaring: use fast bitmap OR
    result->strategy_ = PostingStrategy::kRoaringBitmap;
    result->roaring_bitmap_ = roaring_bitmap_or(roaring_bitmap_, other.roaring_bitmap_);
  } else {
    // At least one is delta: fall back to sorted array union
    auto docs1 = GetAll();
    auto docs2 = other.GetAll();
    std::vector<DocId> union_result;
    std::set_union(docs1.begin(), docs1.end(), docs2.begin(), docs2.end(), std::back_inserter(union_result));
    result->delta_compressed_ = EncodeDelta(union_result);
  }

  return result;
}

void PostingList::Optimize(uint64_t total_docs) {
  if (total_docs == 0) {
    return;
  }

  double density = static_cast<double>(Size()) / static_cast<double>(total_docs);

  if (density >= roaring_threshold_ && strategy_ == PostingStrategy::kDeltaCompressed) {
    // Convert to Roaring for high density
    ConvertToRoaring();
    spdlog::debug("Converted posting list to Roaring (density={:.2f})", density);
  } else if (density < roaring_threshold_ * kHysteresisFactor && strategy_ == PostingStrategy::kRoaringBitmap) {
    // Convert back to delta for low density (with hysteresis)
    ConvertToDelta();
    spdlog::debug("Converted posting list to delta (density={:.2f})", density);
  }
}

std::unique_ptr<PostingList> PostingList::Clone(uint64_t total_docs) const {
  auto cloned = std::make_unique<PostingList>(roaring_threshold_);

  // Get all document IDs from current posting list
  auto docs = GetAll();

  // Build the cloned posting list
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
