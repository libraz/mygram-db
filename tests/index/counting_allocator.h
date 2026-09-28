/**
 * @file counting_allocator.h
 * @brief Replace the global allocator with one that tracks live and peak bytes.
 *
 * Allocation volume counted this way is deterministic and machine-independent.
 * Both operator new and Roaring's own allocation hook are routed through the
 * counter, so a change of posting representation cannot look like a saving.
 *
 * The replacement operators are ordinary definitions: include this header from
 * exactly one translation unit of a test executable.
 */

#pragma once

#include <roaring/memory.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdlib>
#include <cstring>
#include <new>

namespace {

// Every allocation carries a header holding its size and, for the aligned
// forms, the base pointer, so a release can subtract exactly what it returns.
// Tracking live bytes rather than cumulative bytes is what distinguishes
// "materializes every list at once" from "visits every list in turn".
constexpr size_t kHeaderBytes = 32;

std::atomic<size_t> g_live_bytes{0};
std::atomic<size_t> g_peak_bytes{0};

void NoteAllocation(size_t bytes) {
  const size_t live = g_live_bytes.fetch_add(bytes, std::memory_order_relaxed) + bytes;
  size_t peak = g_peak_bytes.load(std::memory_order_relaxed);
  while (live > peak && !g_peak_bytes.compare_exchange_weak(peak, live, std::memory_order_relaxed)) {
  }
}

void NoteRelease(size_t bytes) {
  g_live_bytes.fetch_sub(bytes, std::memory_order_relaxed);
}

void* TrackedAllocate(size_t size) {
  auto* base = static_cast<unsigned char*>(std::malloc(size + kHeaderBytes));
  if (base == nullptr) {
    return nullptr;
  }
  auto* user = base + kHeaderBytes;
  std::memcpy(user - sizeof(size_t), &size, sizeof(size_t));
  NoteAllocation(size);
  return user;
}

void TrackedRelease(void* ptr) {
  if (ptr == nullptr) {
    return;
  }
  auto* user = static_cast<unsigned char*>(ptr);
  size_t size = 0;
  std::memcpy(&size, user - sizeof(size_t), sizeof(size_t));
  NoteRelease(size);
  std::free(user - kHeaderBytes);
}

size_t TrackedSizeOf(void* ptr) {
  size_t size = 0;
  std::memcpy(&size, static_cast<unsigned char*>(ptr) - sizeof(size_t), sizeof(size_t));
  return size;
}

}  // namespace

void* operator new(size_t size) {
  void* ptr = TrackedAllocate(size == 0 ? 1 : size);
  if (ptr == nullptr) {
    throw std::bad_alloc();
  }
  return ptr;
}

void* operator new[](size_t size) {
  return ::operator new(size);
}

void* operator new(size_t size, const std::nothrow_t&) noexcept {
  return TrackedAllocate(size == 0 ? 1 : size);
}

void* operator new[](size_t size, const std::nothrow_t& tag) noexcept {
  return ::operator new(size, tag);
}

void operator delete(void* ptr) noexcept {
  TrackedRelease(ptr);
}
void operator delete[](void* ptr) noexcept {
  TrackedRelease(ptr);
}
void operator delete(void* ptr, size_t) noexcept {
  TrackedRelease(ptr);
}
void operator delete[](void* ptr, size_t) noexcept {
  TrackedRelease(ptr);
}
void operator delete(void* ptr, const std::nothrow_t&) noexcept {
  TrackedRelease(ptr);
}
void operator delete[](void* ptr, const std::nothrow_t&) noexcept {
  TrackedRelease(ptr);
}

namespace {

// Roaring bitmaps allocate through their own hook rather than operator new, so
// both allocators must be accounted for or a change of representation would
// look like a saving.
void* CountedMalloc(size_t size) {
  return TrackedAllocate(size);
}

void* CountedCalloc(size_t count, size_t size) {
  void* ptr = TrackedAllocate(count * size);
  if (ptr != nullptr) {
    std::memset(ptr, 0, count * size);
  }
  return ptr;
}

void* CountedRealloc(void* ptr, size_t size) {
  if (ptr == nullptr) {
    return TrackedAllocate(size);
  }
  const size_t previous = TrackedSizeOf(ptr);
  void* fresh = TrackedAllocate(size);
  if (fresh == nullptr) {
    return nullptr;
  }
  std::memcpy(fresh, ptr, std::min(previous, size));
  TrackedRelease(ptr);
  return fresh;
}

void CountedFree(void* ptr) {
  TrackedRelease(ptr);
}

void* CountedAlignedMalloc(size_t alignment, size_t size) {
  const size_t padding = std::max(alignment, kHeaderBytes);
  void* base = nullptr;
  if (posix_memalign(&base, alignment, size + padding) != 0) {
    return nullptr;
  }
  auto* user = static_cast<unsigned char*>(base) + padding;
  std::memcpy(user - sizeof(size_t), &size, sizeof(size_t));
  std::memcpy(user - (2 * sizeof(size_t)), &base, sizeof(void*));
  NoteAllocation(size);
  return user;
}

void CountedAlignedFree(void* ptr) {
  if (ptr == nullptr) {
    return;
  }
  auto* user = static_cast<unsigned char*>(ptr);
  size_t size = 0;
  void* base = nullptr;
  std::memcpy(&size, user - sizeof(size_t), sizeof(size_t));
  std::memcpy(&base, user - (2 * sizeof(size_t)), sizeof(void*));
  NoteRelease(size);
  std::free(base);
}

void InstallRoaringAllocationCounter() {
  roaring_memory_t hooks{};
  hooks.malloc = CountedMalloc;
  hooks.realloc = CountedRealloc;
  hooks.calloc = CountedCalloc;
  hooks.free = CountedFree;
  hooks.aligned_malloc = CountedAlignedMalloc;
  hooks.aligned_free = CountedAlignedFree;
  roaring_init_memory_hook(hooks);
}

}  // namespace
