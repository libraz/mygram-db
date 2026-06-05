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

          // Check if posting list was modified during optimization
          // IMPORTANT: Compare with snapshot VERSION, not size. Version-based detection
          // catches balanced Remove+Add operations where size stays the same but data changed.
          if (snapshot_version_it != snapshot_versions.end() &&
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

  // Collect term names for batch processing
  std::vector<std::string> terms;
  {
    std::shared_lock<std::shared_mutex> lock(postings_mutex_);
    terms.reserve(term_postings_.size());
    for (const auto& [term, posting] : term_postings_) {
      (void)posting;  // Suppress unused variable warning
      terms.push_back(term);
    }
  }

  size_t total_terms = terms.size();
  size_t converted_count = 0;

  // Process in batches to allow periodic updates
  for (size_t i = 0; i < total_terms; i += batch_size) {
    size_t batch_end = std::min(i + batch_size, total_terms);

    // Step 1a: Take snapshot of posting list VERSIONS for this batch (brief shared_lock)
    // IMPORTANT: We store versions (not sizes) to detect all concurrent mutations:
    // - Version-based detection catches balanced Remove+Add (size unchanged but data changed)
    // - shared_ptr copies keep posting lists alive during optimization
    absl::flat_hash_map<std::string, uint64_t> batch_snapshot_versions;
    absl::flat_hash_map<std::string, std::shared_ptr<PostingList>> batch_snapshot_ptrs;
    {
      std::shared_lock<std::shared_mutex> lock(postings_mutex_);
      for (size_t j = i; j < batch_end; ++j) {
        const auto& term = terms[j];
        auto iter = term_postings_.find(term);
        if (iter != term_postings_.end()) {
          batch_snapshot_versions[term] = iter->second->Version();  // Capture version at snapshot time
          batch_snapshot_ptrs[term] = iter->second;                 // Keep pointer for optimization
        }
      }
    }
    // Lock released - AddDocument/RemoveDocument can proceed

    // Step 1b: Create optimized copies for this batch (CPU-intensive, outside lock)
    absl::flat_hash_map<std::string, std::shared_ptr<PostingList>> optimized_postings;
    for (size_t j = i; j < batch_end; ++j) {
      const auto& term = terms[j];

      // Find the posting list in batch snapshot
      auto iterator = batch_snapshot_ptrs.find(term);
      if (iterator == batch_snapshot_ptrs.end()) {
        continue;  // Term was removed
      }

      const auto& posting = iterator->second;
      auto old_strategy = posting->GetStrategy();

      // Clone and optimize (CPU-intensive, outside lock)
      auto optimized = posting->Clone(total_docs);

      // Track if strategy changed
      if (optimized->GetStrategy() != old_strategy) {
        converted_count++;
      }

      optimized_postings[term] = std::move(optimized);
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
        const auto& term = terms[j];
        auto opt_it = optimized_postings.find(term);
        if (opt_it == optimized_postings.end()) {
          continue;  // Term wasn't optimized
        }

        auto current_it = term_postings_.find(term);
        if (current_it != term_postings_.end()) {
          const auto& current_posting = current_it->second;
          auto snapshot_version_it = batch_snapshot_versions.find(term);

          // Check if posting list was modified during this batch's optimization
          // IMPORTANT: Compare with snapshot VERSION, not size. Version-based detection
          // catches balanced Remove+Add operations where size stays the same but data changed.
          if (snapshot_version_it != batch_snapshot_versions.end() &&
              current_posting->Version() != snapshot_version_it->second) {
            // Posting list was modified during optimization.
            // Keep current_posting as-is (source of truth) rather than Union,
            // which would resurrect documents removed during optimization.
            // This term will be optimized in the next optimization cycle.
          } else {
            // No changes: use optimized version as-is
            term_postings_[term] = std::move(opt_it->second);
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
