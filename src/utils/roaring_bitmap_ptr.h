/**
 * @file roaring_bitmap_ptr.h
 * @brief RAII smart pointer for CRoaring's `roaring_bitmap_t`
 *
 * Provides a unique-ownership wrapper for the C-style `roaring_bitmap_t`
 * handles produced by `roaring_bitmap_create()`. Centralising the deleter
 * here lets call sites (search pipeline, facet handler, future filter
 * helpers) share a single RAII type instead of re-declaring the same
 * `unique_ptr<..., decltype(&roaring_bitmap_free)>` boilerplate.
 *
 * The header pulls in `<roaring/roaring.h>`, so any TU that includes it
 * must also be allowed to depend on the CRoaring library.
 */

#pragma once

#include <roaring/roaring.h>

#include <cstdint>
#include <memory>
#include <vector>

namespace mygram::utils {

/**
 * @brief Custom deleter that frees a `roaring_bitmap_t` via
 * `roaring_bitmap_free`.
 *
 * `noexcept` because `roaring_bitmap_free` is a C function that accepts a
 * null pointer (no-op) and never throws.
 */
struct RoaringBitmapDeleter {
  void operator()(roaring_bitmap_t* bm) const noexcept {
    if (bm != nullptr) {
      roaring_bitmap_free(bm);
    }
  }
};

/**
 * @brief Unique-ownership pointer for a heap-allocated `roaring_bitmap_t`.
 *
 * Use `MakeRoaringFromVector` / `MakeEmptyRoaring` to construct one.
 */
using RoaringBitmapPtr = std::unique_ptr<roaring_bitmap_t, RoaringBitmapDeleter>;

/**
 * @brief Construct an empty Roaring bitmap.
 *
 * @return Owning pointer to a newly-allocated empty bitmap.
 */
inline RoaringBitmapPtr MakeEmptyRoaring() {
  return RoaringBitmapPtr(roaring_bitmap_create());
}

/**
 * @brief Construct a Roaring bitmap from a vector of doc ids.
 *
 * Equivalent to `MakeEmptyRoaring()` followed by `roaring_bitmap_add_many`.
 * Empty inputs return an empty bitmap.
 *
 * @param doc_ids Sorted-or-unsorted set of doc ids to seed the bitmap with.
 *                The bitmap uses set semantics, so duplicates are merged.
 * @return Owning pointer to the new bitmap.
 */
inline RoaringBitmapPtr MakeRoaringFromVector(const std::vector<uint32_t>& doc_ids) {
  RoaringBitmapPtr bm = MakeEmptyRoaring();
  if (!doc_ids.empty()) {
    roaring_bitmap_add_many(bm.get(), doc_ids.size(), doc_ids.data());
  }
  return bm;
}

}  // namespace mygram::utils
