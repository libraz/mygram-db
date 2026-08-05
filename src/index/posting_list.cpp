/**
 * @file posting_list.cpp
 * @brief Posting list implementation
 */

#include "index/posting_list.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <limits>
#include <thread>

#include "utils/structured_log.h"

namespace mygramdb::index {

// Hysteresis factor to prevent oscillation between delta and roaring formats
constexpr double kHysteresisFactor = 0.5;
constexpr size_t kAutoRoaringEntryThreshold = 4096;

namespace {

#ifdef MYGRAMDB_INDEX_TEST_HOOKS
std::atomic<bool> g_fail_next_roaring_create{false};
std::atomic<bool> g_fail_next_roaring_and{false};
std::atomic<bool> g_fail_next_roaring_or{false};
std::atomic<uint64_t> g_roaring_and_operation_count{0};
std::atomic<bool> g_pause_next_remove{false};
std::atomic<bool> g_remove_paused{false};
std::atomic<bool> g_release_paused_remove{false};
#endif

utils::RoaringBitmapPtr CreateRoaringBitmap() {
#ifdef MYGRAMDB_INDEX_TEST_HOOKS
  if (g_fail_next_roaring_create.exchange(false, std::memory_order_acq_rel)) {
    return {};
  }
#endif
  return utils::RoaringBitmapPtr(roaring_bitmap_create());
}

utils::RoaringBitmapPtr AndRoaringBitmaps(const roaring_bitmap_t* lhs, const roaring_bitmap_t* rhs) {
#ifdef MYGRAMDB_INDEX_TEST_HOOKS
  g_roaring_and_operation_count.fetch_add(1, std::memory_order_relaxed);
  if (g_fail_next_roaring_and.exchange(false, std::memory_order_acq_rel)) {
    return {};
  }
#endif
  return utils::RoaringBitmapPtr(roaring_bitmap_and(lhs, rhs));
}

utils::RoaringBitmapPtr OrRoaringBitmaps(const roaring_bitmap_t* lhs, const roaring_bitmap_t* rhs) {
#ifdef MYGRAMDB_INDEX_TEST_HOOKS
  if (g_fail_next_roaring_or.exchange(false, std::memory_order_acq_rel)) {
    return {};
  }
#endif
  return utils::RoaringBitmapPtr(roaring_bitmap_or(lhs, rhs));
}

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

bool IsValidDeltaEncoding(const std::vector<uint32_t>& encoded) {
  if (encoded.empty()) {
    return true;
  }

  uint64_t cumulative = encoded.front();
  for (size_t i = 1; i < encoded.size(); ++i) {
    if (encoded[i] == 0) {
      return false;
    }
    cumulative += encoded[i];
    if (cumulative > std::numeric_limits<DocId>::max()) {
      return false;
    }
  }
  return true;
}

}  // namespace

PostingList::PostingList(double roaring_threshold) : roaring_threshold_(roaring_threshold) {}

#ifdef MYGRAMDB_INDEX_TEST_HOOKS
void PostingList::FailNextRoaringOperationForTest(TestRoaringFault fault) {
  switch (fault) {
    case TestRoaringFault::kCreate:
      g_fail_next_roaring_create.store(true, std::memory_order_release);
      break;
    case TestRoaringFault::kAnd:
      g_fail_next_roaring_and.store(true, std::memory_order_release);
      break;
    case TestRoaringFault::kOr:
      g_fail_next_roaring_or.store(true, std::memory_order_release);
      break;
  }
}

void PostingList::ResetRoaringAndOperationCountForTesting() {
  g_roaring_and_operation_count.store(0, std::memory_order_release);
}

uint64_t PostingList::RoaringAndOperationCountForTesting() {
  return g_roaring_and_operation_count.load(std::memory_order_acquire);
}

const void* PostingList::DeltaStorageAddressForTesting() const {
  std::shared_lock lock(mutex_);
  return delta_encoded_.data();
}

void PostingList::PauseNextRemoveForTesting() {
  g_remove_paused.store(false, std::memory_order_release);
  g_release_paused_remove.store(false, std::memory_order_release);
  g_pause_next_remove.store(true, std::memory_order_release);
}

bool PostingList::IsRemovePausedForTesting() {
  return g_remove_paused.load(std::memory_order_acquire);
}

void PostingList::ReleasePausedRemoveForTesting() {
  g_release_paused_remove.store(true, std::memory_order_release);
}
#endif

// Precondition: moved-from object must not be concurrently accessed.
// Move operations do not acquire mutex_ on either object.
PostingList::PostingList(PostingList&& other) noexcept
    : strategy_(other.strategy_.load(std::memory_order_relaxed)),
      roaring_threshold_(other.roaring_threshold_),
      delta_encoded_(std::move(other.delta_encoded_)),
      last_doc_id_(other.last_doc_id_),
      roaring_bitmap_(std::move(other.roaring_bitmap_)),
      doc_count_(other.doc_count_.load(std::memory_order_relaxed)),
      cached_memory_size_(other.cached_memory_size_.load(std::memory_order_relaxed)),
      version_(other.version_.load(std::memory_order_relaxed)) {
  other.strategy_.store(PostingStrategy::kFixedWidthDelta, std::memory_order_relaxed);
  other.delta_encoded_.clear();
  other.last_doc_id_ = 0;
  other.doc_count_.store(0, std::memory_order_relaxed);
  other.cached_memory_size_.store(0, std::memory_order_relaxed);
  other.version_.store(0, std::memory_order_relaxed);
}

// Precondition: moved-from object must not be concurrently accessed.
// Move operations do not acquire mutex_ on either object.
PostingList& PostingList::operator=(PostingList&& other) noexcept {
  if (this != &other) {
    strategy_.store(other.strategy_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    roaring_threshold_ = other.roaring_threshold_;
    delta_encoded_ = std::move(other.delta_encoded_);
    last_doc_id_ = other.last_doc_id_;
    roaring_bitmap_ = std::move(other.roaring_bitmap_);
    doc_count_.store(other.doc_count_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    cached_memory_size_.store(other.cached_memory_size_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    version_.store(other.version_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    other.strategy_.store(PostingStrategy::kFixedWidthDelta, std::memory_order_relaxed);
    other.delta_encoded_.clear();
    other.last_doc_id_ = 0;
    other.doc_count_.store(0, std::memory_order_relaxed);
    other.cached_memory_size_.store(0, std::memory_order_relaxed);
    other.version_.store(0, std::memory_order_relaxed);
  }
  return *this;
}

void PostingList::Add(DocId doc_id) {
  std::unique_lock lock(mutex_);  // Exclusive access for write
  auto strategy = strategy_.load(std::memory_order_relaxed);
  if (strategy == PostingStrategy::kFixedWidthDelta) {
    if (delta_encoded_.empty()) {
      // First entry: store doc_id as-is (delta encoding stores first value raw)
      delta_encoded_.push_back(doc_id);
      last_doc_id_ = doc_id;
    } else {
      if (doc_id > last_doc_id_) {
        // Fast path: monotonically increasing insertion (O(1) append)
        // Common case during binlog replication where DocIds arrive in order
        delta_encoded_.push_back(doc_id - last_doc_id_);
        last_doc_id_ = doc_id;
      } else if (doc_id != last_doc_id_) {
        // Patch the two neighboring deltas in place. This is still O(n) for
        // vector insertion, but avoids two full temporary vectors.
        DocId cumulative = delta_encoded_.front();
        if (doc_id == cumulative) {
          // Duplicate of the first entry.
        } else if (doc_id < cumulative) {
          delta_encoded_.insert(delta_encoded_.begin(), doc_id);
          delta_encoded_[1] = cumulative - doc_id;
        } else {
          for (size_t i = 1; i < delta_encoded_.size(); ++i) {
            const DocId previous = cumulative;
            cumulative += delta_encoded_[i];
            if (doc_id == cumulative) {
              break;
            }
            if (doc_id < cumulative) {
              delta_encoded_[i] = doc_id - previous;
              delta_encoded_.insert(delta_encoded_.begin() + static_cast<std::ptrdiff_t>(i + 1), cumulative - doc_id);
              break;
            }
          }
        }
      }
      // If doc_id == last_doc_id_, it's a duplicate; skip silently
    }
    MaybeConvertLargeDeltaToRoaring();
  } else {
    roaring_bitmap_add(roaring_bitmap_.get(), doc_id);
  }
  UpdateCountsAndVersion();
}

void PostingList::AddBatch(const std::vector<DocId>& doc_ids) {
  if (doc_ids.empty()) {
    return;
  }

  const std::vector<DocId>* normalized_doc_ids = &doc_ids;
  std::vector<DocId> deduped_doc_ids;
  if (!std::is_sorted(doc_ids.begin(), doc_ids.end()) ||
      std::adjacent_find(doc_ids.begin(), doc_ids.end()) != doc_ids.end()) {
    deduped_doc_ids = doc_ids;
    std::sort(deduped_doc_ids.begin(), deduped_doc_ids.end());
    deduped_doc_ids.erase(std::unique(deduped_doc_ids.begin(), deduped_doc_ids.end()), deduped_doc_ids.end());
    normalized_doc_ids = &deduped_doc_ids;
  }

  std::unique_lock lock(mutex_);  // Exclusive access for write
  auto strategy = strategy_.load(std::memory_order_relaxed);
  if (strategy == PostingStrategy::kFixedWidthDelta) {
    // Merge sorted arrays
    auto existing = DecodeDelta(delta_encoded_);
    std::vector<DocId> merged;
    merged.reserve(existing.size() + normalized_doc_ids->size());
    std::set_union(existing.begin(), existing.end(), normalized_doc_ids->begin(), normalized_doc_ids->end(),
                   std::back_inserter(merged));
    delta_encoded_ = EncodeDelta(merged);
    if (!merged.empty()) {
      last_doc_id_ = merged.back();
    }
    MaybeConvertLargeDeltaToRoaring();
  } else {
    roaring_bitmap_add_many(roaring_bitmap_.get(), normalized_doc_ids->size(), normalized_doc_ids->data());
  }
  UpdateCountsAndVersion();
}

void PostingList::Remove(DocId doc_id) {
  std::unique_lock lock(mutex_);  // Exclusive access for write
#ifdef MYGRAMDB_INDEX_TEST_HOOKS
  if (g_pause_next_remove.exchange(false, std::memory_order_acq_rel)) {
    g_remove_paused.store(true, std::memory_order_release);
    while (!g_release_paused_remove.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }
    g_remove_paused.store(false, std::memory_order_release);
  }
#endif
  auto strategy = strategy_.load(std::memory_order_relaxed);
  if (strategy == PostingStrategy::kFixedWidthDelta) {
    if (!delta_encoded_.empty()) {
      DocId cumulative = delta_encoded_.front();
      if (doc_id == cumulative) {
        if (delta_encoded_.size() == 1) {
          delta_encoded_.clear();
          last_doc_id_ = 0;
        } else {
          delta_encoded_[0] += delta_encoded_[1];
          delta_encoded_.erase(delta_encoded_.begin() + 1);
        }
      } else if (doc_id > cumulative) {
        for (size_t i = 1; i < delta_encoded_.size(); ++i) {
          const DocId previous = cumulative;
          cumulative += delta_encoded_[i];
          if (doc_id == cumulative) {
            if (i + 1 < delta_encoded_.size()) {
              delta_encoded_[i + 1] += delta_encoded_[i];
            } else {
              last_doc_id_ = previous;
            }
            delta_encoded_.erase(delta_encoded_.begin() + static_cast<std::ptrdiff_t>(i));
            break;
          }
          if (doc_id < cumulative) {
            break;
          }
        }
      }
    }
  } else {
    roaring_bitmap_remove(roaring_bitmap_.get(), doc_id);
  }
  UpdateCountsAndVersion();
}

bool PostingList::Contains(DocId doc_id) const {
  std::shared_lock lock(mutex_);  // Protect read access
  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kFixedWidthDelta) {
    if (delta_encoded_.empty()) {
      return false;
    }

    // Quick check for first element
    if (delta_encoded_[0] == doc_id) {
      return true;
    }
    if (delta_encoded_[0] > doc_id) {
      return false;
    }

    // Streaming decode with early exit - O(n) time, O(1) memory
    // More efficient than full decode + binary search for all sizes because
    // it avoids vector allocation and exits early when target is passed.
    // For fixed-width delta encoded lists (pre-Roaring threshold, typically <1000 entries),
    // this linear scan with early exit outperforms decode+binary_search because
    // it avoids O(n) vector allocation. Lists exceeding the Roaring density
    // threshold are automatically converted, bounding the O(n) scan by the
    // threshold size.
    // Since delta values are non-negative and cumulative is monotonically
    // increasing, we can stop as soon as cumulative exceeds doc_id.
    // Start from index 1 since delta_encoded_[0] (first doc_id) was already
    // checked in the quick check above.
    DocId cumulative = delta_encoded_[0];
    for (size_t i = 1; i < delta_encoded_.size(); ++i) {
      cumulative += delta_encoded_[i];
      if (cumulative == doc_id) {
        return true;
      }
      if (cumulative > doc_id) {
        return false;  // Passed target, not found
      }
    }

    return false;
  }
  return roaring_bitmap_contains(roaring_bitmap_.get(), doc_id);
}

std::vector<DocId> PostingList::GetAll() const {
  std::shared_lock lock(mutex_);  // Protect read access
  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kFixedWidthDelta) {
    return DecodeDelta(delta_encoded_);
  }
  uint64_t size = roaring_bitmap_get_cardinality(roaring_bitmap_.get());
  std::vector<DocId> result(size);
  roaring_bitmap_to_uint32_array(roaring_bitmap_.get(), result.data());
  return result;
}

std::vector<DocId> PostingList::GetTopN(size_t limit, bool reverse) const {
  std::shared_lock lock(mutex_);  // Protect read access

  // If limit is 0, return all documents
  if (limit == 0) {
    std::vector<DocId> result;
    if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kFixedWidthDelta) {
      result = DecodeDelta(delta_encoded_);
    } else {
      uint64_t size = roaring_bitmap_get_cardinality(roaring_bitmap_.get());
      result.resize(size);
      roaring_bitmap_to_uint32_array(roaring_bitmap_.get(), result.data());
    }
    if (reverse) {
      std::reverse(result.begin(), result.end());
    }
    return result;
  }

  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kFixedWidthDelta) {
    // Fixed-width delta encoded: decode and extract top N
    auto all_docs = DecodeDelta(delta_encoded_);
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
  uint64_t total_size = roaring_bitmap_get_cardinality(roaring_bitmap_.get());
  size_t actual_limit = std::min(limit, static_cast<size_t>(total_size));

  std::vector<DocId> result;
  result.reserve(actual_limit);

  // CRoaring exposes an in-place iterator initializer, so use stack storage.
  // This removes the allocation-failure path that previously returned an
  // indistinguishable empty result for a non-empty posting list.
  roaring_uint32_iterator_t iter;
  if (reverse) {
    roaring_iterator_init_last(roaring_bitmap_.get(), &iter);
  } else {
    roaring_iterator_init(roaring_bitmap_.get(), &iter);
  }

  size_t count = 0;
  while (count < actual_limit && iter.has_value) {
    result.push_back(iter.current_value);
    if (reverse) {
      roaring_uint32_iterator_previous(&iter);
    } else {
      roaring_uint32_iterator_advance(&iter);
    }
    count++;
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
  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kFixedWidthDelta) {
    doc_count_.store(delta_encoded_.size(), std::memory_order_release);
    cached_memory_size_.store(delta_encoded_.capacity() * sizeof(uint32_t) + sizeof(std::vector<uint32_t>),
                              std::memory_order_release);
  } else {
    doc_count_.store(roaring_bitmap_get_cardinality(roaring_bitmap_.get()), std::memory_order_release);
    cached_memory_size_.store(
        roaring_bitmap_ != nullptr ? roaring_bitmap_portable_size_in_bytes(roaring_bitmap_.get()) : 0,
        std::memory_order_release);
  }
  version_.fetch_add(1, std::memory_order_release);
}

void PostingList::RecomputeLastDocId() {
  if (delta_encoded_.empty()) {
    last_doc_id_ = 0;
    return;
  }
  DocId id = 0;
  for (DocId delta : delta_encoded_) {
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
      if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kFixedWidthDelta) {
        docs = DecodeDelta(delta_encoded_);
      } else {
        uint64_t size = roaring_bitmap_get_cardinality(roaring_bitmap_.get());
        docs.resize(size);
        roaring_bitmap_to_uint32_array(roaring_bitmap_.get(), docs.data());
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
  auto get_docs_locked = [](const PostingList& list) {
    std::vector<DocId> docs;
    if (list.strategy_.load(std::memory_order_relaxed) == PostingStrategy::kFixedWidthDelta) {
      docs = DecodeDelta(list.delta_encoded_);
    } else {
      uint64_t size = roaring_bitmap_get_cardinality(list.roaring_bitmap_.get());
      docs.resize(size);
      roaring_bitmap_to_uint32_array(list.roaring_bitmap_.get(), docs.data());
    }
    return docs;
  };

  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kRoaringBitmap &&
      other.strategy_.load(std::memory_order_relaxed) == PostingStrategy::kRoaringBitmap) {
    // Both Roaring: use fast bitmap AND
    auto intersected = AndRoaringBitmaps(roaring_bitmap_.get(), other.roaring_bitmap_.get());
    if (intersected == nullptr) {
      std::vector<DocId> docs1 = get_docs_locked(*this);
      std::vector<DocId> docs2 = get_docs_locked(other);
      std::vector<DocId> intersection;
      std::set_intersection(docs1.begin(), docs1.end(), docs2.begin(), docs2.end(), std::back_inserter(intersection));
      result->delta_encoded_ = EncodeDelta(intersection);
      result->doc_count_.store(intersection.size(), std::memory_order_relaxed);
      if (!intersection.empty()) {
        result->last_doc_id_ = intersection.back();
      }
      result->UpdateCountsAndVersion();
      return result;
    }
    result->strategy_.store(PostingStrategy::kRoaringBitmap, std::memory_order_relaxed);
    result->roaring_bitmap_ = std::move(intersected);
    uint64_t card = roaring_bitmap_get_cardinality(result->roaring_bitmap_.get());
    result->doc_count_.store(card, std::memory_order_relaxed);
    if (card > 0) {
      result->last_doc_id_ = roaring_bitmap_maximum(result->roaring_bitmap_.get());
    }
  } else {
    // At least one is delta: fall back to sorted array intersection
    // Note: GetAll() would try to acquire the lock again, so we inline the logic
    std::vector<DocId> docs1 = get_docs_locked(*this);
    std::vector<DocId> docs2 = get_docs_locked(other);

    std::vector<DocId> intersection;
    std::set_intersection(docs1.begin(), docs1.end(), docs2.begin(), docs2.end(), std::back_inserter(intersection));
    result->delta_encoded_ = EncodeDelta(intersection);
    result->doc_count_.store(intersection.size(), std::memory_order_relaxed);
    if (!intersection.empty()) {
      result->last_doc_id_ = intersection.back();
    }
  }

  result->UpdateCountsAndVersion();

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
      if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kFixedWidthDelta) {
        docs = DecodeDelta(delta_encoded_);
      } else {
        uint64_t size = roaring_bitmap_get_cardinality(roaring_bitmap_.get());
        docs.resize(size);
        roaring_bitmap_to_uint32_array(roaring_bitmap_.get(), docs.data());
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
  auto get_docs_locked = [](const PostingList& list) {
    std::vector<DocId> docs;
    if (list.strategy_.load(std::memory_order_relaxed) == PostingStrategy::kFixedWidthDelta) {
      docs = DecodeDelta(list.delta_encoded_);
    } else {
      uint64_t size = roaring_bitmap_get_cardinality(list.roaring_bitmap_.get());
      docs.resize(size);
      roaring_bitmap_to_uint32_array(list.roaring_bitmap_.get(), docs.data());
    }
    return docs;
  };

  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kRoaringBitmap &&
      other.strategy_.load(std::memory_order_relaxed) == PostingStrategy::kRoaringBitmap) {
    // Both Roaring: use fast bitmap OR
    auto united = OrRoaringBitmaps(roaring_bitmap_.get(), other.roaring_bitmap_.get());
    if (united == nullptr) {
      std::vector<DocId> docs1 = get_docs_locked(*this);
      std::vector<DocId> docs2 = get_docs_locked(other);
      std::vector<DocId> union_result;
      std::set_union(docs1.begin(), docs1.end(), docs2.begin(), docs2.end(), std::back_inserter(union_result));
      result->delta_encoded_ = EncodeDelta(union_result);
      result->doc_count_.store(union_result.size(), std::memory_order_relaxed);
      if (!union_result.empty()) {
        result->last_doc_id_ = union_result.back();
      }
      result->UpdateCountsAndVersion();
      return result;
    }
    result->strategy_.store(PostingStrategy::kRoaringBitmap, std::memory_order_relaxed);
    result->roaring_bitmap_ = std::move(united);
    uint64_t card = roaring_bitmap_get_cardinality(result->roaring_bitmap_.get());
    result->doc_count_.store(card, std::memory_order_relaxed);
    if (card > 0) {
      result->last_doc_id_ = roaring_bitmap_maximum(result->roaring_bitmap_.get());
    }
  } else {
    // At least one is delta: fall back to sorted array union
    // Note: GetAll() would try to acquire the lock again, so we inline the logic
    std::vector<DocId> docs1 = get_docs_locked(*this);
    std::vector<DocId> docs2 = get_docs_locked(other);

    std::vector<DocId> union_result;
    std::set_union(docs1.begin(), docs1.end(), docs2.begin(), docs2.end(), std::back_inserter(union_result));
    result->delta_encoded_ = EncodeDelta(union_result);
    result->doc_count_.store(union_result.size(), std::memory_order_relaxed);
    if (!union_result.empty()) {
      result->last_doc_id_ = union_result.back();
    }
  }

  result->UpdateCountsAndVersion();

  return result;
}

void PostingList::Optimize(uint64_t total_docs) {
  std::unique_lock lock(mutex_);  // Exclusive access for write
  if (total_docs == 0) {
    return;
  }

  // Calculate size without calling Size() to avoid recursive locking
  uint64_t size = 0;
  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kFixedWidthDelta) {
    size = delta_encoded_.size();
  } else {
    size = roaring_bitmap_get_cardinality(roaring_bitmap_.get());
  }

  double density = static_cast<double>(size) / static_cast<double>(total_docs);

  if (density >= roaring_threshold_ && strategy_.load(std::memory_order_relaxed) == PostingStrategy::kFixedWidthDelta) {
    // Convert to Roaring for high density
    ConvertToRoaring();
    mygram::utils::StructuredLog()
        .Event("posting_list_converted")
        .Field("to", "roaring")
        .Field("density", density)
        .Debug();
  } else if (size <= kAutoRoaringEntryThreshold && density < roaring_threshold_ * kHysteresisFactor &&
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
    if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kFixedWidthDelta) {
      docs = DecodeDelta(delta_encoded_);
    } else {
      uint64_t size = roaring_bitmap_get_cardinality(roaring_bitmap_.get());
      docs.resize(size);
      roaring_bitmap_to_uint32_array(roaring_bitmap_.get(), docs.data());
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

  auto docs = DecodeDelta(delta_encoded_);
  roaring_bitmap_ = CreateRoaringBitmap();
  if (roaring_bitmap_ == nullptr) {
    // OOM: keep fixed-width delta encoded strategy, log error
    mygram::utils::StructuredLog()
        .Event("posting_list_roaring_alloc_failed")
        .Field("doc_count", static_cast<uint64_t>(docs.size()))
        .Error();
    return;
  }
  if (!docs.empty()) {
    roaring_bitmap_add_many(roaring_bitmap_.get(), docs.size(), docs.data());
  }
  roaring_bitmap_run_optimize(roaring_bitmap_.get());

  delta_encoded_.clear();
  delta_encoded_.shrink_to_fit();
  last_doc_id_ = 0;  // Not used for Roaring strategy
  strategy_.store(PostingStrategy::kRoaringBitmap, std::memory_order_release);
  doc_count_.store(roaring_bitmap_get_cardinality(roaring_bitmap_.get()), std::memory_order_relaxed);
  cached_memory_size_.store(roaring_bitmap_portable_size_in_bytes(roaring_bitmap_.get()), std::memory_order_relaxed);
}

void PostingList::ConvertToDelta() {
  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kFixedWidthDelta) {
    return;
  }

  // Access roaring bitmap directly instead of calling GetAll(),
  // because the caller (Optimize()) already holds a unique_lock on mutex_.
  // Calling GetAll() would try to acquire a shared_lock, causing undefined behavior.
  uint64_t size = roaring_bitmap_get_cardinality(roaring_bitmap_.get());
  std::vector<DocId> docs(size);
  roaring_bitmap_to_uint32_array(roaring_bitmap_.get(), docs.data());
  delta_encoded_ = EncodeDelta(docs);
  last_doc_id_ = docs.empty() ? 0 : docs.back();

  roaring_bitmap_.reset();
  strategy_.store(PostingStrategy::kFixedWidthDelta, std::memory_order_release);
  doc_count_.store(delta_encoded_.size(), std::memory_order_relaxed);
  cached_memory_size_.store(delta_encoded_.capacity() * sizeof(uint32_t) + sizeof(std::vector<uint32_t>),
                            std::memory_order_relaxed);
}

void PostingList::MaybeConvertLargeDeltaToRoaring() {
  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kFixedWidthDelta &&
      delta_encoded_.size() > kAutoRoaringEntryThreshold) {
    ConvertToRoaring();
  }
}

std::vector<uint32_t> PostingList::EncodeDelta(const std::vector<DocId>& doc_ids) {
  if (doc_ids.empty()) {
    return {};
  }

  const std::vector<DocId>* normalized_doc_ids = &doc_ids;
  std::vector<DocId> sorted_unique_doc_ids;
  if (!std::is_sorted(doc_ids.begin(), doc_ids.end()) ||
      std::adjacent_find(doc_ids.begin(), doc_ids.end()) != doc_ids.end()) {
    sorted_unique_doc_ids = doc_ids;
    std::sort(sorted_unique_doc_ids.begin(), sorted_unique_doc_ids.end());
    sorted_unique_doc_ids.erase(std::unique(sorted_unique_doc_ids.begin(), sorted_unique_doc_ids.end()),
                                sorted_unique_doc_ids.end());
    normalized_doc_ids = &sorted_unique_doc_ids;
  }

  std::vector<uint32_t> encoded;
  encoded.reserve(normalized_doc_ids->size());

  // First value as-is
  encoded.push_back((*normalized_doc_ids)[0]);

  // Rest as deltas
  for (size_t i = 1; i < normalized_doc_ids->size(); ++i) {
    encoded.push_back((*normalized_doc_ids)[i] - (*normalized_doc_ids)[i - 1]);
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

  if (strategy_.load(std::memory_order_relaxed) == PostingStrategy::kFixedWidthDelta) {
    // Write size
    if (delta_encoded_.size() > std::numeric_limits<uint32_t>::max()) {
      mygram::utils::StructuredLog()
          .Event("posting_list_serialize_failed")
          .Field("reason", "delta_list_too_large")
          .Field("size", static_cast<uint64_t>(delta_encoded_.size()))
          .Warn();
      return false;
    }
    auto size = static_cast<uint32_t>(delta_encoded_.size());
    WriteUint32LE(buffer, size);

    // Write fixed-width delta encoded data
    for (uint32_t val : delta_encoded_) {
      WriteUint32LE(buffer, val);
    }
  } else {
    // Roaring bitmap: serialize using roaring's native format
    size_t roaring_size = roaring_bitmap_portable_size_in_bytes(roaring_bitmap_.get());

    if (roaring_size > std::numeric_limits<uint32_t>::max()) {
      mygram::utils::StructuredLog()
          .Event("posting_list_serialize_failed")
          .Field("reason", "bitmap_too_large")
          .Field("size", static_cast<uint64_t>(roaring_size))
          .Warn();
      return false;
    }
    auto roaring_size_u32 = static_cast<uint32_t>(roaring_size);

    // Write size
    WriteUint32LE(buffer, roaring_size_u32);

    // Write roaring bitmap data
    size_t old_size = buffer.size();
    buffer.resize(old_size + roaring_size);
    roaring_bitmap_portable_serialize(roaring_bitmap_.get(), GetSerializationPointer(buffer, old_size));
  }

  return true;
}

bool PostingList::Deserialize(const std::vector<uint8_t>& buffer, size_t& offset) {
  std::unique_lock lock(mutex_);  // Exclusive access for write

  size_t cursor = offset;
  if (cursor >= buffer.size()) {
    return false;
  }

  // Read and validate strategy byte
  uint8_t strategy_byte = buffer[cursor++];
  if (strategy_byte > static_cast<uint8_t>(PostingStrategy::kRoaringBitmap)) {
    return false;
  }
  const auto parsed_strategy = static_cast<PostingStrategy>(strategy_byte);

  if (buffer.size() - cursor < sizeof(uint32_t)) {
    return false;
  }

  // Read size
  uint32_t size = ReadUint32LE(buffer, cursor);

  if (parsed_strategy == PostingStrategy::kFixedWidthDelta) {
    // Read fixed-width delta encoded data
    if (static_cast<size_t>(size) > (buffer.size() - cursor) / sizeof(uint32_t)) {
      return false;
    }

    std::vector<uint32_t> decoded_delta;
    decoded_delta.reserve(size);

    for (uint32_t i = 0; i < size; ++i) {
      uint32_t val = ReadUint32LE(buffer, cursor);
      decoded_delta.push_back(val);
    }

    if (!IsValidDeltaEncoding(decoded_delta)) {
      return false;
    }

    // Commit only after the complete body has validated.
    strategy_.store(parsed_strategy, std::memory_order_relaxed);
    delta_encoded_ = std::move(decoded_delta);
    RecomputeLastDocId();

    doc_count_.store(delta_encoded_.size(), std::memory_order_relaxed);
    cached_memory_size_.store(delta_encoded_.capacity() * sizeof(uint32_t) + sizeof(std::vector<uint32_t>),
                              std::memory_order_relaxed);

    roaring_bitmap_.reset();
    MaybeConvertLargeDeltaToRoaring();
  } else {
    // Read roaring bitmap
    if (static_cast<size_t>(size) > buffer.size() - cursor) {
      return false;
    }

    utils::RoaringBitmapPtr decoded_bitmap(
        roaring_bitmap_portable_deserialize_safe(GetDeserializationPointer(buffer, cursor), size));
    if (decoded_bitmap == nullptr) {
      return false;
    }
    if (!roaring_bitmap_internal_validate(decoded_bitmap.get(), nullptr)) {
      return false;
    }

    cursor += size;
    strategy_.store(parsed_strategy, std::memory_order_relaxed);
    roaring_bitmap_ = std::move(decoded_bitmap);
    delta_encoded_.clear();
    doc_count_.store(roaring_bitmap_get_cardinality(roaring_bitmap_.get()), std::memory_order_relaxed);
    cached_memory_size_.store(roaring_bitmap_portable_size_in_bytes(roaring_bitmap_.get()), std::memory_order_relaxed);
  }

  offset = cursor;
  version_.fetch_add(1, std::memory_order_release);
  return true;
}

}  // namespace mygramdb::index
