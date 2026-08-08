/**
 * @file index_optimization.cpp
 * @brief Index optimization implementations (Optimize, OptimizeInBatches)
 */

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <thread>

#include "absl/container/flat_hash_map.h"
#include "index/index.h"
#include "utils/constants.h"
#include "utils/structured_log.h"

namespace mygramdb::index {

namespace {

/**
 * @brief RAII guard to ensure the optimization flag is cleared on scope exit
 */
struct OptimizationGuard {
  std::atomic<bool>& flag;
  explicit OptimizationGuard(std::atomic<bool>& flag_ref) : flag(flag_ref) {}
  ~OptimizationGuard() { flag.store(false); }
  OptimizationGuard(const OptimizationGuard&) = delete;
  OptimizationGuard& operator=(const OptimizationGuard&) = delete;
  OptimizationGuard(OptimizationGuard&&) = delete;
  OptimizationGuard& operator=(OptimizationGuard&&) = delete;
};

}  // namespace

void Index::Optimize(uint64_t total_docs) {
  // Check if already optimizing (prevent concurrent Optimize() calls)
  bool expected = false;
  if (!is_optimizing_.compare_exchange_strong(expected, true)) {
    mygram::utils::StructuredLog().Event("index_optimization_skipped").Field("reason", "already in progress").Warn();
    return;
  }

  OptimizationGuard guard(is_optimizing_);

  // Capture load generation before snapshotting — if LoadFromStream runs
  // concurrently and replaces term_postings_, our snapshot is stale.
  const uint64_t gen_before = load_generation_.load(std::memory_order_acquire);

  // Step 1a: Take snapshot of posting list VERSIONS and pointers (brief shared_lock)
  // IMPORTANT: We store both versions and shared_ptrs:
  // - shared_ptr copies keep posting lists alive during optimization
  // - Version snapshots capture state at T0, unaffected by concurrent mutations
  // - Version-based detection catches balanced Remove+Add (size unchanged but data changed)
  absl::flat_hash_map<std::string, std::shared_ptr<PostingList>> snapshot;
  absl::flat_hash_map<std::string, uint64_t> snapshot_versions;
  {
    std::shared_lock<std::shared_mutex> lock(postings_mutex_);
    for (const auto& [term, posting] : term_postings_) {
      snapshot[term] = posting;                      // Copy shared_ptr (reference counting)
      snapshot_versions[term] = posting->Version();  // Capture version at snapshot time
    }
  }
  // Lock released - AddDocument/RemoveDocument can now proceed

  // Step 1b: Create optimized copies outside the lock (CPU-intensive work)
  // This doesn't block any operations - searches and writes continue normally
  // The snapshot keeps posting lists alive via shared_ptr reference counting
  absl::flat_hash_map<std::string, std::shared_ptr<PostingList>> optimized_postings;
  for (const auto& [term, posting] : snapshot) {
    // Clone creates an optimized copy without modifying the original
    optimized_postings[term] = posting->Clone(total_docs);
  }

  // Step 2: Atomically swap the old index with the new optimized index
  // Brief exclusive lock to update the map
  size_t term_count = 0;
  size_t merged_count = 0;
  {
    std::unique_lock<std::shared_mutex> lock(postings_mutex_);

    // If LoadFromStream replaced term_postings_ since we took our snapshot,
    // discard all optimization results to avoid overwriting fresh data.
    if (load_generation_.load(std::memory_order_acquire) != gen_before) {
      mygram::utils::StructuredLog()
          .Event("index_optimization_discarded")
          .Field("reason", "load_generation_changed")
          .Info();
      // term_count and merged_count stay 0 — skip the loop entirely
    } else {
      term_count = optimized_postings.size();

      // Update only terms that still exist in the index
      // This preserves concurrent modifications:
      // - Terms removed during Step 1: won't be re-added (not in term_postings_)
      // - Terms added during Step 1: won't be optimized (not in optimized_postings)
      // - Terms modified during Step 1: keep current version (source of truth),
      //   skip optimization for this term
      for (auto& [term, optimized_posting] : optimized_postings) {
        auto current_it = term_postings_.find(term);
        if (current_it != term_postings_.end()) {
          const auto& current_posting = current_it->second;
          auto snapshot_version_it = snapshot_versions.find(term);

          // The term may have been erased and recreated while we optimized its
          // old posting list. A fresh PostingList starts its version counter at
          // zero, so a version-only check has an ABA hole. Require both the
          // original object identity and its captured version.
          const auto snapshot_posting_it = snapshot.find(term);
          if (snapshot_version_it == snapshot_versions.end() || snapshot_posting_it == snapshot.end() ||
              current_posting != snapshot_posting_it->second ||
              current_posting->Version() != snapshot_version_it->second) {
            // Posting list was modified during optimization.
            // Keep current_posting as-is (source of truth) rather than Union,
            // which would resurrect documents removed during optimization.
            // This term will be optimized in the next optimization cycle.
            merged_count++;
          } else {
            // No changes: use optimized version as-is
            term_postings_[term] = std::move(optimized_posting);
          }
        }
        // If term was removed, don't re-add it
      }
    }
  }

  if (merged_count > 0) {
    mygram::utils::StructuredLog()
        .Event("index_optimization_merge")
        .Field("skipped_terms", static_cast<uint64_t>(merged_count))
        .Debug();
  }

  size_t final_term_count = 0;
  {
    std::shared_lock<std::shared_mutex> lock(postings_mutex_);
    final_term_count = term_postings_.size();
  }
  mygram::utils::StructuredLog()
      .Event("index_optimized")
      .Field("terms_optimized", static_cast<uint64_t>(term_count))
      .Field("terms_final", static_cast<uint64_t>(final_term_count))
      .Field("memory_mb", static_cast<uint64_t>(MemoryUsage() / mygram::constants::kBytesPerMegabyte))
      .Info();
}

bool Index::OptimizeInBatches(uint64_t total_docs, size_t batch_size) {
  // Check if already optimizing
  bool expected = false;
  if (!is_optimizing_.compare_exchange_strong(expected, true)) {
    mygram::utils::StructuredLog()
        .Event("index_batch_optimization_skipped")
        .Field("reason", "already in progress")
        .Warn();
    return false;
  }

  OptimizationGuard guard(is_optimizing_);

  // Capture load generation before starting — if LoadFromStream runs
  // concurrently and replaces term_postings_, our snapshots are stale.
  const uint64_t gen_before = load_generation_.load(std::memory_order_acquire);

  size_t initial_term_count;
  {
    std::shared_lock<std::shared_mutex> lock(postings_mutex_);
    initial_term_count = term_postings_.size();
  }

  mygram::utils::StructuredLog()
      .Event("index_batch_optimization_starting")
      .Field("terms", static_cast<uint64_t>(initial_term_count))
      .Field("batch_size", static_cast<uint64_t>(batch_size))
      .Info();

  auto start_time = std::chrono::steady_clock::now();

  // Snapshot the term names so that concurrent inserts and erases cannot
  // invalidate the iteration. Holding one std::string per term costs 24 bytes
  // of object overhead plus an allocation for every term above the small-string
  // bound; a single character arena with offsets holds the same names in two
  // allocations regardless of term count.
  std::string term_arena;
  std::vector<size_t> term_offsets;  // term j spans [term_offsets[j], term_offsets[j + 1])
  {
    std::shared_lock<std::shared_mutex> lock(postings_mutex_);
    size_t arena_bytes = 0;
    for (const auto& [term, posting] : term_postings_) {
      (void)posting;  // Suppress unused variable warning
      arena_bytes += term.size();
    }
    term_arena.reserve(arena_bytes);
    term_offsets.reserve(term_postings_.size() + 1);
    term_offsets.push_back(0);
    for (const auto& [term, posting] : term_postings_) {
      (void)posting;  // Suppress unused variable warning
      term_arena.append(term);
      term_offsets.push_back(term_arena.size());
    }
  }

  const std::string_view terms_view(term_arena);
  auto term_at = [&terms_view, &term_offsets](size_t index) {
    return terms_view.substr(term_offsets[index], term_offsets[index + 1] - term_offsets[index]);
  };

  size_t total_terms = term_offsets.size() - 1;
  size_t converted_count = 0;

  // Process in batches to allow periodic updates
  for (size_t i = 0; i < total_terms; i += batch_size) {
    size_t batch_end = std::min(i + batch_size, total_terms);

    // Step 1a: Take snapshot of posting list VERSIONS for this batch (brief shared_lock)
    // IMPORTANT: We store versions (not sizes) to detect all concurrent mutations:
    // - Version-based detection catches balanced Remove+Add (size unchanged but data changed)
    // - shared_ptr copies keep posting lists alive during optimization
    // Indexed by the batch-relative position so that no term string is copied
    // again for bookkeeping.
    const size_t batch_span = batch_end - i;
    std::vector<uint64_t> batch_snapshot_versions(batch_span, 0);
    std::vector<std::shared_ptr<PostingList>> batch_snapshot_ptrs(batch_span);
    {
      std::shared_lock<std::shared_mutex> lock(postings_mutex_);
      for (size_t j = i; j < batch_end; ++j) {
        auto iter = term_postings_.find(term_at(j));
        if (iter != term_postings_.end()) {
          batch_snapshot_versions[j - i] = iter->second->Version();  // Capture version at snapshot time
          batch_snapshot_ptrs[j - i] = iter->second;                 // Keep pointer for optimization
        }
      }
    }
    // Lock released - AddDocument/RemoveDocument can proceed

    // Step 1b: Create optimized copies for this batch (CPU-intensive, outside lock)
    std::vector<std::shared_ptr<PostingList>> optimized_postings(batch_span);
    for (size_t j = i; j < batch_end; ++j) {
      const auto& posting = batch_snapshot_ptrs[j - i];
      if (!posting) {
        continue;  // Term was removed
      }

      auto old_strategy = posting->GetStrategy();

      // Clone and optimize (CPU-intensive, outside lock)
      auto optimized = posting->Clone(total_docs);

      // Track if strategy changed
      if (optimized->GetStrategy() != old_strategy) {
        converted_count++;
      }

      optimized_postings[j - i] = std::move(optimized);
    }

    if (before_batch_optimization_publish_hook_for_test_) {
      before_batch_optimization_publish_hook_for_test_();
    }

    // Step 2: Atomically swap the optimized batch (brief exclusive lock)
    {
      std::unique_lock<std::shared_mutex> lock(postings_mutex_);

      // If LoadFromStream replaced term_postings_ since we started,
      // discard remaining optimization results to avoid overwriting fresh data.
      if (load_generation_.load(std::memory_order_acquire) != gen_before) {
        mygram::utils::StructuredLog()
            .Event("index_batch_optimization_discarded")
            .Field("reason", "load_generation_changed")
            .Field("batch_offset", static_cast<uint64_t>(i))
            .Info();
        break;  // Exit the batch loop entirely
      }

      // Update only terms that still exist in the index
      // This preserves concurrent modifications:
      // - Terms removed during Step 1: won't be re-added (not in term_postings_)
      // - Terms added during Step 1: won't be optimized (not in optimized_postings)
      // - Terms modified during Step 1: keep current version (source of truth),
      //   skip optimization for this term
      for (size_t j = i; j < batch_end; ++j) {
        auto& optimized = optimized_postings[j - i];
        if (!optimized) {
          continue;  // Term wasn't optimized
        }

        auto current_it = term_postings_.find(term_at(j));
        if (current_it != term_postings_.end()) {
          const auto& current_posting = current_it->second;

          // Require pointer identity as well as the captured version. A term
          // can be erased and recreated between the snapshot and publish;
          // the replacement's version may coincidentally match the old one.
          if (current_posting != batch_snapshot_ptrs[j - i] ||
              current_posting->Version() != batch_snapshot_versions[j - i]) {
            // Posting list was modified during optimization.
            // Keep current_posting as-is (source of truth) rather than Union,
            // which would resurrect documents removed during optimization.
            // This term will be optimized in the next optimization cycle.
          } else {
            // No changes: use optimized version as-is
            current_it->second = std::move(optimized);
          }
        }
        // If term was removed, don't re-add it
      }
    }
    // Lock released - brief pause allows other operations to proceed
    if (batch_end < total_terms) {
      std::this_thread::yield();
    }

    // Log progress every 10% or at the end
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    // 100, 10: Standard percentage calculation values
    size_t progress = ((batch_end) * 100) / total_terms;
    if (progress % 10 == 0 || batch_end == total_terms) {
      // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
      mygram::utils::StructuredLog()
          .Event("index_optimization_progress")
          .Field("processed", static_cast<uint64_t>(batch_end))
          .Field("total", static_cast<uint64_t>(total_terms))
          .Field("percent", static_cast<uint64_t>(progress))
          .Info();
    }
  }

  auto end_time = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  // 1000.0: Standard conversion factor from milliseconds to seconds
  size_t final_term_count = 0;
  {
    std::shared_lock<std::shared_mutex> lock(postings_mutex_);
    final_term_count = term_postings_.size();
  }
  mygram::utils::StructuredLog()
      .Event("index_batch_optimization_completed")
      .Field("terms_processed", static_cast<uint64_t>(total_terms))
      .Field("terms_final", static_cast<uint64_t>(final_term_count))
      .Field("strategy_changes", static_cast<uint64_t>(converted_count))
      .Field("elapsed_sec", static_cast<double>(duration) / 1000.0)
      .Info();
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)

  return true;
}

}  // namespace mygramdb::index
