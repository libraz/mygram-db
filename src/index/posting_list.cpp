/**
 * @file posting_list.cpp
 * @brief Posting list implementation
 */

#include "index/posting_list.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <limits>

#include "utils/structured_log.h"

namespace mygramdb::index {

// Hysteresis factor to prevent oscillation between delta and roaring formats
constexpr double kHysteresisFactor = 0.5;

namespace {

/**
 * @brief Write a uint32_t in little-endian byte order to a buffer
 */
inline void WriteUint32LE(std::vector<uint8_t>& buf, uint32_t val) {
  buf.push_back(static_cast<uint8_t>(val & 0xFF));
  buf.push_back(static_cast<uint8_t>((val >> 8) & 0xFF));
  buf.push_back(static_cast<uint8_t>((val >> 16) & 0xFF));
  buf.push_back(static_cast<uint8_t>((val >> 24) & 0xFF));
}

/**
 * @brief Read a uint32_t in little-endian byte order from a buffer
 */
inline uint32_t ReadUint32LE(const std::vector<uint8_t>& buf, size_t& offset) {
  assert(offset + 4 <= buf.size() && "ReadUint32LE: buffer overflow");
  uint32_t val = static_cast<uint32_t>(buf[offset]) | (static_cast<uint32_t>(buf[offset + 1]) << 8) |
                 (static_cast<uint32_t>(buf[offset + 2]) << 16) | (static_cast<uint32_t>(buf[offset + 3]) << 24);
  offset += 4;
  return val;
}

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

// Precondition: moved-from object must not be concurrently accessed.
// Move operations do not acquire mutex_ on either object.
PostingList::PostingList(PostingList&& other) noexcept
    : strategy_(other.strategy_.load(std::memory_order_relaxed)),
      roaring_threshold_(other.roaring_threshold_),
      delta_compressed_(std::move(other.delta_compressed_)),
      last_doc_id_(other.last_doc_id_),
      roaring_bitmap_(other.roaring_bitmap_),
      doc_count_(other.doc_count_.load(std::memory_order_relaxed)),
      cached_memory_size_(other.cached_memory_size_.load(std::memory_order_relaxed)),
      version_(other.version_.load(std::memory_order_relaxed)) {
  other.roaring_bitmap_ = nullptr;
  other.last_doc_id_ = 0;
  other.doc_count_.store(0, std::memory_order_relaxed);
  other.cached_memory_size_.store(0, std::memory_order_relaxed);
}

// Precondition: moved-from object must not be concurrently accessed.
// Move operations do not acquire mutex_ on either object.
PostingList& PostingList::operator=(PostingList&& other) noexcept {
  if (this != &other) {
    if (roaring_bitmap_ != nullptr) {
      roaring_bitmap_free(roaring_bitmap_);
    }
    strategy_.store(other.strategy_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    roaring_threshold_ = other.roaring_threshold_;
    delta_compressed_ = std::move(other.delta_compressed_);
    last_doc_id_ = other.last_doc_id_;
    roaring_bitmap_ = other.roaring_bitmap_;
    doc_count_.store(other.doc_count_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    cached_memory_size_.store(other.cached_memory_size_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    version_.store(other.version_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    other.roaring_bitmap_ = nullptr;
    other.last_doc_id_ = 0;
    other.doc_count_.store(0, std::memory_order_relaxed);
    other.cached_memory_size_.store(0, std::memory_order_relaxed);
  }
  return *this;
}

void PostingList::Add(DocId doc_id) {
  std::unique_lock lock(mutex_);  // Exclusive access for write
  auto strategy = strategy_.load(std::memory_order_relaxed);
  if (strategy == PostingStrategy::kDeltaCompressed) {
    if (delta_compressed_.empty()) {
      // First entry: store doc_id as-is (delta encoding stores first value raw)
      delta_compressed_.push_back(doc_id);
      last_doc_id_ = doc_id;
    } else {
      if (doc_id > last_doc_id_) {
        // Fast path: monotonically increasing insertion (O(1) append)
        // Common case during binlog replication where DocIds arrive in order
        delta_compressed_.push_back(doc_id - last_doc_id_);
        last_doc_id_ = doc_id;
      } else if (doc_id != last_doc_id_) {
        // Slow path: out-of-order insertion requires full decode-sort-encode
        auto docs = DecodeDelta(delta_compressed_);
        auto iter = std::lower_bound(docs.begin(), docs.end(), doc_id);
        if (iter == docs.end() || *iter != doc_id) {
          docs.insert(iter, doc_id);
          delta_compressed_ = EncodeDelta(docs);
          // last_doc_id_ unchanged since new doc_id < last_doc_id_
        }
      }
      // If doc_id == last_doc_id_, it's a duplicate; skip silently
    }
  } else {
    roaring_bitmap_add(roaring_bitmap_, doc_id);
  }
  UpdateCountsAndVersion();
}

void PostingList::AddBatch(const std::vector<DocId>& doc_ids) {
  if (doc_ids.empty()) {
    return;
  }

  assert(std::is_sorted(doc_ids.begin(), doc_ids.end()));

  std::unique_lock lock(mutex_);  // Exclusive access for write
  auto strategy = strategy_.load(std::memory_order_relaxed);
  if (strategy == PostingStrategy::kDeltaCompressed) {
    // Merge sorted arrays
    auto existing = DecodeDelta(delta_compressed_);
    std::vector<DocId> merged;
    merged.reserve(existing.size() + doc_ids.size());
    std::set_union(existing.begin(), existing.end(), doc_ids.begin(), doc_ids.end(), std::back_inserter(merged));
    delta_compressed_ = EncodeDelta(merged);
    if (!merged.empty()) {
      last_doc_id_ = merged.back();
    }
  } else {
    roaring_bitmap_add_many(roaring_bitmap_, doc_ids.size(), doc_ids.data());
  }
  UpdateCountsAndVersion();
}

void PostingList::Remove(DocId doc_id) {
  std::unique_lock lock(mutex_);  // Exclusive access for write
  auto strategy = strategy_.load(std::memory_order_relaxed);
  if (strategy == PostingStrategy::kDeltaCompressed) {
    // For delta-compressed strategy, decode, modify, and re-encode
    // This is simpler and more maintainable than in-place delta manipulation
    auto docs = DecodeDelta(delta_compressed_);
    auto iter = std::lower_bound(docs.begin(), docs.end(), doc_id);
    if (iter != docs.end() && *iter == doc_id) {
      docs.erase(iter);
      delta_compressed_ = EncodeDelta(docs);
      last_doc_id_ = docs.empty() ? 0 : docs.back();
    }
  } else {
    roaring_bitmap_remove(roaring_bitmap_, doc_id);
  }
  UpdateCountsAndVersion();
}

bool PostingList::Contains(DocId doc_id) const {
  std::shared_lock lock(mutex_);  // Protect read access
  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
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

    // Streaming decode with early exit - O(n) time, O(1) memory
    // More efficient than full decode + binary search for all sizes because
    // it avoids vector allocation and exits early when target is passed.
    // Since delta values are non-negative and cumulative is monotonically
    // increasing, we can stop as soon as cumulative exceeds doc_id.
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
  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
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
    if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
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

  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
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

  // CRoaring iterator lifecycle: use malloc + init/init_last + free consistently
  // for both forward and reverse directions. This avoids mixing
  // roaring_iterator_create()/roaring_uint32_iterator_free() (which uses
  // roaring_malloc/roaring_free internally) with malloc()/free().
  // NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cppcoreguidelines-no-malloc)
  auto* raw_iter = static_cast<roaring_uint32_iterator_t*>(malloc(sizeof(roaring_uint32_iterator_t)));
  if (raw_iter != nullptr) {
    // RAII wrapper ensures free() is called even if push_back throws
    auto deleter = [](roaring_uint32_iterator_t* ptr) {
      // NOLINTNEXTLINE(cppcoreguidelines-owning-memory,cppcoreguidelines-no-malloc)
      free(ptr);
    };
    std::unique_ptr<roaring_uint32_iterator_t, decltype(deleter)> iter(raw_iter, deleter);

    if (reverse) {
      roaring_iterator_init_last(roaring_bitmap_, iter.get());
    } else {
      roaring_iterator_init(roaring_bitmap_, iter.get());
    }

    size_t count = 0;
    while (count < actual_limit && iter->has_value) {
      result.push_back(iter->current_value);
      if (reverse) {
        roaring_uint32_iterator_previous(iter.get());
      } else {
        roaring_uint32_iterator_advance(iter.get());
      }
      count++;
    }
    // iter automatically freed by unique_ptr destructor
  }

  return result;
}

uint64_t PostingList::Size() const {
  std::shared_lock lock(mutex_);  // Protect read access
  return SizeApprox();
}

uint64_t PostingList::SizeApprox() const {
  return doc_count_.load(std::memory_order_acquire);
}

size_t PostingList::MemoryUsage() const {
  std::shared_lock lock(mutex_);  // Protect read access
  return MemoryUsageApprox();
}

size_t PostingList::MemoryUsageApprox() const {
  return cached_memory_size_.load(std::memory_order_acquire);
}

void PostingList::UpdateCountsAndVersion() {
  // Use memory_order_release for doc_count_ and cached_memory_size_ so that
  // SizeApprox() and MemoryUsageApprox() (which read with acquire) form proper
  // release-acquire pairs, ensuring visibility on weakly-ordered architectures
  // (e.g., ARM). The version_ increment provides additional ordering for
  // callers that check version_ first.
  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
    doc_count_.store(delta_compressed_.size(), std::memory_order_release);
    cached_memory_size_.store(delta_compressed_.capacity() * sizeof(uint32_t) + sizeof(std::vector<uint32_t>),
                              std::memory_order_release);
  } else {
    doc_count_.store(roaring_bitmap_get_cardinality(roaring_bitmap_), std::memory_order_release);
    cached_memory_size_.store(roaring_bitmap_ != nullptr ? roaring_bitmap_portable_size_in_bytes(roaring_bitmap_) : 0,
                              std::memory_order_release);
  }
  version_.fetch_add(1, std::memory_order_release);
}

void PostingList::RecomputeLastDocId() {
  if (delta_compressed_.empty()) {
    last_doc_id_ = 0;
    return;
  }
  DocId id = 0;
  for (DocId delta : delta_compressed_) {
    id += delta;
  }
  last_doc_id_ = id;
}

std::unique_ptr<PostingList> PostingList::Intersect(const PostingList& other) const {
  // Self-intersection guard: avoid UB from locking the same shared_mutex twice.
  // A & A == A, so return a copy of this list.
  if (&other == this) {
    auto result = std::make_unique<PostingList>(roaring_threshold_);
    std::vector<DocId> docs;
    {
      std::shared_lock lock(mutex_);
      if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
        docs = DecodeDelta(delta_compressed_);
      } else {
        uint64_t size = roaring_bitmap_get_cardinality(roaring_bitmap_);
        docs.resize(size);
        roaring_bitmap_to_uint32_array(roaring_bitmap_, docs.data());
      }
    }  // lock released here
    if (!docs.empty()) {
      result->AddBatch(docs);
    }
    return result;
  }

  std::shared_lock lock1(mutex_, std::defer_lock);        // Protect read access to this
  std::shared_lock lock2(other.mutex_, std::defer_lock);  // Protect read access to other
  std::lock(lock1, lock2);                                // Deadlock-safe acquisition

  auto result = std::make_unique<PostingList>(roaring_threshold_);

  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kRoaringBitmap &&
      other.strategy_.load(std::memory_order_relaxed) == PostingStrategy::kRoaringBitmap) {
    // Both Roaring: use fast bitmap AND
    roaring_bitmap_t* intersected = roaring_bitmap_and(roaring_bitmap_, other.roaring_bitmap_);
    if (intersected == nullptr) {
      // OOM: return empty list with delta strategy (safe fallback)
      result->doc_count_.store(0, std::memory_order_relaxed);
      return result;
    }
    result->strategy_.store(PostingStrategy::kRoaringBitmap, std::memory_order_relaxed);
    result->roaring_bitmap_ = intersected;
    uint64_t card = roaring_bitmap_get_cardinality(result->roaring_bitmap_);
    result->doc_count_.store(card, std::memory_order_relaxed);
    if (card > 0) {
      result->last_doc_id_ = roaring_bitmap_maximum(result->roaring_bitmap_);
    }
  } else {
    // At least one is delta: fall back to sorted array intersection
    // Note: GetAll() would try to acquire the lock again, so we inline the logic
    std::vector<DocId> docs1;
    if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
      docs1 = DecodeDelta(delta_compressed_);
    } else {
      uint64_t size1 = roaring_bitmap_get_cardinality(roaring_bitmap_);
      docs1.resize(size1);
      roaring_bitmap_to_uint32_array(roaring_bitmap_, docs1.data());
    }

    std::vector<DocId> docs2;
    if (other.strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
      docs2 = DecodeDelta(other.delta_compressed_);
    } else {
      uint64_t size2 = roaring_bitmap_get_cardinality(other.roaring_bitmap_);
      docs2.resize(size2);
      roaring_bitmap_to_uint32_array(other.roaring_bitmap_, docs2.data());
    }

    std::vector<DocId> intersection;
    std::set_intersection(docs1.begin(), docs1.end(), docs2.begin(), docs2.end(), std::back_inserter(intersection));
    result->delta_compressed_ = EncodeDelta(intersection);
    result->doc_count_.store(intersection.size(), std::memory_order_relaxed);
    if (!intersection.empty()) {
      result->last_doc_id_ = intersection.back();
    }
  }

  return result;
}

std::unique_ptr<PostingList> PostingList::Union(const PostingList& other) const {
  // Self-union guard: avoid UB from locking the same shared_mutex twice.
  // A | A == A, so return a copy of this list.
  if (&other == this) {
    auto result = std::make_unique<PostingList>(roaring_threshold_);
    std::vector<DocId> docs;
    {
      std::shared_lock lock(mutex_);
      if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
        docs = DecodeDelta(delta_compressed_);
      } else {
        uint64_t size = roaring_bitmap_get_cardinality(roaring_bitmap_);
        docs.resize(size);
        roaring_bitmap_to_uint32_array(roaring_bitmap_, docs.data());
      }
    }  // lock released here
    if (!docs.empty()) {
      result->AddBatch(docs);
    }
    return result;
  }

  std::shared_lock lock1(mutex_, std::defer_lock);        // Protect read access to this
  std::shared_lock lock2(other.mutex_, std::defer_lock);  // Protect read access to other
  std::lock(lock1, lock2);                                // Deadlock-safe acquisition

  auto result = std::make_unique<PostingList>(roaring_threshold_);

  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kRoaringBitmap &&
      other.strategy_.load(std::memory_order_relaxed) == PostingStrategy::kRoaringBitmap) {
    // Both Roaring: use fast bitmap OR
    roaring_bitmap_t* united = roaring_bitmap_or(roaring_bitmap_, other.roaring_bitmap_);
    if (united == nullptr) {
      // OOM: return empty list with delta strategy (safe fallback)
      result->doc_count_.store(0, std::memory_order_relaxed);
      return result;
    }
    result->strategy_.store(PostingStrategy::kRoaringBitmap, std::memory_order_relaxed);
    result->roaring_bitmap_ = united;
    uint64_t card = roaring_bitmap_get_cardinality(result->roaring_bitmap_);
    result->doc_count_.store(card, std::memory_order_relaxed);
    if (card > 0) {
      result->last_doc_id_ = roaring_bitmap_maximum(result->roaring_bitmap_);
    }
  } else {
    // At least one is delta: fall back to sorted array union
    // Note: GetAll() would try to acquire the lock again, so we inline the logic
    std::vector<DocId> docs1;
    if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
      docs1 = DecodeDelta(delta_compressed_);
    } else {
      uint64_t size1 = roaring_bitmap_get_cardinality(roaring_bitmap_);
      docs1.resize(size1);
      roaring_bitmap_to_uint32_array(roaring_bitmap_, docs1.data());
    }

    std::vector<DocId> docs2;
    if (other.strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
      docs2 = DecodeDelta(other.delta_compressed_);
    } else {
      uint64_t size2 = roaring_bitmap_get_cardinality(other.roaring_bitmap_);
      docs2.resize(size2);
      roaring_bitmap_to_uint32_array(other.roaring_bitmap_, docs2.data());
    }

    std::vector<DocId> union_result;
    std::set_union(docs1.begin(), docs1.end(), docs2.begin(), docs2.end(), std::back_inserter(union_result));
    result->delta_compressed_ = EncodeDelta(union_result);
    result->doc_count_.store(union_result.size(), std::memory_order_relaxed);
    if (!union_result.empty()) {
      result->last_doc_id_ = union_result.back();
    }
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
  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
    size = delta_compressed_.size();
  } else {
    size = roaring_bitmap_get_cardinality(roaring_bitmap_);
  }

  double density = static_cast<double>(size) / static_cast<double>(total_docs);

  if (density >= roaring_threshold_ && strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
    // Convert to Roaring for high density
    ConvertToRoaring();
    mygram::utils::StructuredLog()
        .Event("posting_list_converted")
        .Field("to", "roaring")
        .Field("density", density)
        .Debug();
  } else if (density < roaring_threshold_ * kHysteresisFactor &&
             strategy_.load(std::memory_order_relaxed) == PostingStrategy::kRoaringBitmap) {
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
  auto cloned = std::make_shared<PostingList>(roaring_threshold_);

  // Get all document IDs from current posting list under lock
  std::vector<DocId> docs;
  {
    std::shared_lock lock(mutex_);  // Protect read access to internal state
    if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
      docs = DecodeDelta(delta_compressed_);
    } else {
      uint64_t size = roaring_bitmap_get_cardinality(roaring_bitmap_);
      docs.resize(size);
      roaring_bitmap_to_uint32_array(roaring_bitmap_, docs.data());
    }
  }
  // Lock released here

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
  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kRoaringBitmap) {
    return;
  }

  auto docs = DecodeDelta(delta_compressed_);
  roaring_bitmap_ = roaring_bitmap_create();
  if (roaring_bitmap_ == nullptr) {
    // OOM: keep delta-compressed strategy, log error
    mygram::utils::StructuredLog()
        .Event("posting_list_roaring_alloc_failed")
        .Field("doc_count", static_cast<uint64_t>(docs.size()))
        .Error();
    return;
  }
  if (!docs.empty()) {
    roaring_bitmap_add_many(roaring_bitmap_, docs.size(), docs.data());
  }
  roaring_bitmap_run_optimize(roaring_bitmap_);

  delta_compressed_.clear();
  delta_compressed_.shrink_to_fit();
  last_doc_id_ = 0;  // Not used for Roaring strategy
  strategy_.store(PostingStrategy::kRoaringBitmap, std::memory_order_release);
  doc_count_.store(roaring_bitmap_get_cardinality(roaring_bitmap_), std::memory_order_relaxed);
  cached_memory_size_.store(roaring_bitmap_portable_size_in_bytes(roaring_bitmap_), std::memory_order_relaxed);
}

void PostingList::ConvertToDelta() {
  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
    return;
  }

  // Access roaring bitmap directly instead of calling GetAll(),
  // because the caller (Optimize()) already holds a unique_lock on mutex_.
  // Calling GetAll() would try to acquire a shared_lock, causing undefined behavior.
  uint64_t size = roaring_bitmap_get_cardinality(roaring_bitmap_);
  std::vector<DocId> docs(size);
  roaring_bitmap_to_uint32_array(roaring_bitmap_, docs.data());
  delta_compressed_ = EncodeDelta(docs);
  last_doc_id_ = docs.empty() ? 0 : docs.back();

  roaring_bitmap_free(roaring_bitmap_);
  roaring_bitmap_ = nullptr;
  strategy_.store(PostingStrategy::kDeltaCompressed, std::memory_order_release);
  doc_count_.store(delta_compressed_.size(), std::memory_order_relaxed);
  cached_memory_size_.store(delta_compressed_.capacity() * sizeof(uint32_t) + sizeof(std::vector<uint32_t>),
                            std::memory_order_relaxed);
}

std::vector<uint32_t> PostingList::EncodeDelta(const std::vector<DocId>& doc_ids) {
  if (doc_ids.empty()) {
    return {};
  }

  // Debug assertion: input must be strictly sorted (no duplicates)
  assert(std::is_sorted(doc_ids.begin(), doc_ids.end()) && "EncodeDelta: input must be sorted");
  assert(std::adjacent_find(doc_ids.begin(), doc_ids.end()) == doc_ids.end() &&
         "EncodeDelta: input must not contain duplicates");

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

bool PostingList::Serialize(std::vector<uint8_t>& buffer) const {
  std::shared_lock lock(mutex_);  // Protect read access

  // Format:
  // [1 byte: strategy] [4 bytes: size] [data...]

  // Write strategy
  buffer.push_back(static_cast<uint8_t>(strategy_.load(std::memory_order_relaxed)));

  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
    // Write size
    if (delta_compressed_.size() > std::numeric_limits<uint32_t>::max()) {
      spdlog::warn("Cannot serialize delta list larger than 4G entries (size={})", delta_compressed_.size());
      return false;
    }
    auto size = static_cast<uint32_t>(delta_compressed_.size());
    WriteUint32LE(buffer, size);

    // Write delta-compressed data
    for (uint32_t val : delta_compressed_) {
      WriteUint32LE(buffer, val);
    }
  } else {
    // Roaring bitmap: serialize using roaring's native format
    size_t roaring_size = roaring_bitmap_portable_size_in_bytes(roaring_bitmap_);

    if (roaring_size > std::numeric_limits<uint32_t>::max()) {
      spdlog::warn("Cannot serialize bitmap larger than 4GB (size={})", roaring_size);
      return false;
    }
    auto roaring_size_u32 = static_cast<uint32_t>(roaring_size);

    // Write size
    WriteUint32LE(buffer, roaring_size_u32);

    // Write roaring bitmap data
    size_t old_size = buffer.size();
    buffer.resize(old_size + roaring_size);
    roaring_bitmap_portable_serialize(roaring_bitmap_, GetSerializationPointer(buffer, old_size));
  }

  return true;
}

bool PostingList::Deserialize(const std::vector<uint8_t>& buffer, size_t& offset) {
  std::unique_lock lock(mutex_);  // Exclusive access for write

  if (offset >= buffer.size()) {
    return false;
  }

  // Read and validate strategy byte
  uint8_t strategy_byte = buffer[offset++];
  if (strategy_byte > static_cast<uint8_t>(PostingStrategy::kRoaringBitmap)) {
    return false;
  }
  strategy_ = static_cast<PostingStrategy>(strategy_byte);

  if (offset + 4 > buffer.size()) {
    return false;
  }

  // Read size
  uint32_t size = ReadUint32LE(buffer, offset);

  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kDeltaCompressed) {
    // Read delta-compressed data
    if (offset + (static_cast<size_t>(size) * 4) > buffer.size()) {
      return false;
    }

    delta_compressed_.clear();
    delta_compressed_.reserve(size);

    for (uint32_t i = 0; i < size; ++i) {
      uint32_t val = ReadUint32LE(buffer, offset);
      delta_compressed_.push_back(val);
    }

    RecomputeLastDocId();

    doc_count_.store(delta_compressed_.size(), std::memory_order_relaxed);
    cached_memory_size_.store(delta_compressed_.capacity() * sizeof(uint32_t) + sizeof(std::vector<uint32_t>),
                              std::memory_order_relaxed);

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

    roaring_bitmap_ = roaring_bitmap_portable_deserialize_safe(GetDeserializationPointer(buffer, offset), size);

    if (roaring_bitmap_ == nullptr) {
      return false;
    }

    offset += size;
    delta_compressed_.clear();
    doc_count_.store(roaring_bitmap_get_cardinality(roaring_bitmap_), std::memory_order_relaxed);
    cached_memory_size_.store(roaring_bitmap_portable_size_in_bytes(roaring_bitmap_), std::memory_order_relaxed);
  }

  version_.fetch_add(1, std::memory_order_release);
  return true;
}

}  // namespace mygramdb::index
