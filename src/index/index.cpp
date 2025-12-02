/**
 * @file index.cpp
 * @brief N-gram inverted index implementation
 */

#include "index/index.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstring>
#include <fstream>

#include "utils/string_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::index {

Index::Index(int ngram_size, int kanji_ngram_size, double roaring_threshold)
    : ngram_size_(ngram_size),
      kanji_ngram_size_(kanji_ngram_size > 0 ? kanji_ngram_size : ngram_size),
      roaring_threshold_(roaring_threshold) {}

void Index::AddDocument(DocId doc_id, std::string_view text) {
  // Generate n-grams using hybrid mode (no lock needed for this CPU-intensive operation)
  std::vector<std::string> ngrams = utils::GenerateHybridNgrams(text, ngram_size_, kanji_ngram_size_);

  // Remove duplicates by sorting and using unique (more efficient than unordered_set)
  std::sort(ngrams.begin(), ngrams.end());
  ngrams.erase(std::unique(ngrams.begin(), ngrams.end()), ngrams.end());

  // Acquire exclusive lock for modifying posting lists
  std::unique_lock<std::shared_mutex> lock(postings_mutex_);

  // Add document to posting list for each unique n-gram
  for (const auto& ngram : ngrams) {
    auto* posting = GetOrCreatePostingList(ngram);
    posting->Add(doc_id);
  }

  const char* mode = (ngram_size_ == 0) ? "hybrid" : "regular";
  mygram::utils::StructuredLog()
      .Event("document_added")
      .Field("doc_id", static_cast<uint64_t>(doc_id))
      .Field("text_length", static_cast<uint64_t>(text.size()))
      .Field("unique_ngrams", static_cast<uint64_t>(ngrams.size()))
      .Field("ngram_size", static_cast<int64_t>(ngram_size_))
      .Field("mode", mode)
      .Debug();
}

void Index::AddDocumentBatch(const std::vector<DocumentItem>& documents) {
  if (documents.empty()) {
    return;
  }

  // Phase 1: Generate n-grams for all documents (no locking, CPU-intensive)
  // Map: term -> vector of doc_ids containing that term
  std::unordered_map<std::string, std::vector<DocId>> term_to_docs;

  for (const auto& doc : documents) {
    // Generate n-grams
    std::vector<std::string> ngrams;
    ngrams = utils::GenerateHybridNgrams(doc.text, ngram_size_, kanji_ngram_size_);

    // Remove duplicates by sorting and using unique (more efficient than unordered_set)
    std::sort(ngrams.begin(), ngrams.end());
    ngrams.erase(std::unique(ngrams.begin(), ngrams.end()), ngrams.end());

    // Build term->docs mapping
    for (const auto& ngram : ngrams) {
      term_to_docs[ngram].push_back(doc.doc_id);
    }
  }

  // Phase 2: Sort doc_ids for each term (enables batch insertion optimization)
  for (auto& [term, doc_ids] : term_to_docs) {
    std::sort(doc_ids.begin(), doc_ids.end());
  }

  // Phase 3: Add to posting lists (with exclusive lock, minimal lock time)
  // Use PostingList::AddBatch() for better performance
  std::unique_lock<std::shared_mutex> lock(postings_mutex_);

  for (const auto& [term, doc_ids] : term_to_docs) {
    auto* posting = GetOrCreatePostingList(term);
    posting->AddBatch(doc_ids);
  }
}

void Index::UpdateDocument(DocId doc_id, std::string_view old_text, std::string_view new_text) {
  // Generate n-grams for both texts (no lock needed for CPU-intensive operation)
  std::vector<std::string> old_ngrams = utils::GenerateHybridNgrams(old_text, ngram_size_, kanji_ngram_size_);
  std::vector<std::string> new_ngrams = utils::GenerateHybridNgrams(new_text, ngram_size_, kanji_ngram_size_);

  // Sort and remove duplicates (more efficient than unordered_set)
  std::sort(old_ngrams.begin(), old_ngrams.end());
  old_ngrams.erase(std::unique(old_ngrams.begin(), old_ngrams.end()), old_ngrams.end());

  std::sort(new_ngrams.begin(), new_ngrams.end());
  new_ngrams.erase(std::unique(new_ngrams.begin(), new_ngrams.end()), new_ngrams.end());

  // Calculate set differences using sorted arrays
  std::vector<std::string> to_remove;
  std::vector<std::string> to_add;
  std::set_difference(old_ngrams.begin(), old_ngrams.end(), new_ngrams.begin(), new_ngrams.end(),
                      std::back_inserter(to_remove));
  std::set_difference(new_ngrams.begin(), new_ngrams.end(), old_ngrams.begin(), old_ngrams.end(),
                      std::back_inserter(to_add));

  // Acquire exclusive lock for modifying posting lists
  std::unique_lock<std::shared_mutex> lock(postings_mutex_);

  // Remove doc from n-grams that are no longer present
  for (const auto& ngram : to_remove) {
    auto* posting = GetOrCreatePostingList(ngram);
    posting->Remove(doc_id);
  }

  // Add doc to new n-grams
  for (const auto& ngram : to_add) {
    auto* posting = GetOrCreatePostingList(ngram);
    posting->Add(doc_id);
  }

  mygram::utils::StructuredLog()
      .Event("document_updated")
      .Field("doc_id", static_cast<uint64_t>(doc_id))
      .Field("ngrams_removed", static_cast<uint64_t>(to_remove.size()))
      .Field("ngrams_added", static_cast<uint64_t>(to_add.size()))
      .Debug();
}

void Index::RemoveDocument(DocId doc_id, std::string_view text) {
  // Generate n-grams (no lock needed for CPU-intensive operation)
  std::vector<std::string> ngrams = utils::GenerateHybridNgrams(text, ngram_size_, kanji_ngram_size_);

  // Remove duplicates by sorting and using unique (more efficient than unordered_set)
  std::sort(ngrams.begin(), ngrams.end());
  ngrams.erase(std::unique(ngrams.begin(), ngrams.end()), ngrams.end());

  // Acquire exclusive lock for modifying posting lists
  std::unique_lock<std::shared_mutex> lock(postings_mutex_);

  // Remove document from posting list for each n-gram
  for (const auto& ngram : ngrams) {
    auto* posting = GetOrCreatePostingList(ngram);
    posting->Remove(doc_id);
  }

  mygram::utils::StructuredLog()
      .Event("document_removed")
      .Field("doc_id", static_cast<uint64_t>(doc_id))
      .Field("ngrams_removed", static_cast<uint64_t>(ngrams.size()))
      .Debug();
}

std::vector<DocId> Index::SearchAnd(const std::vector<std::string>& terms, size_t limit, bool reverse) const {
  // RCU pattern: Take snapshot of posting lists under short lock, then search without lock
  // This reduces lock contention under high concurrency

  if (terms.empty()) {
    return {};
  }

  // Take snapshot of all posting lists (short lock)
  auto snapshots = TakePostingSnapshots(terms);

  // Check if any term is missing
  for (const auto& snapshot : snapshots) {
    if (snapshot == nullptr) {
      return {};  // No documents if any term is missing
    }
  }

  // From here, no lock is held - search operates on immutable snapshots

  // Optimization: Single term with limit and reverse
  // This is common for "ORDER BY primary_key DESC LIMIT N" queries
  if (terms.size() == 1 && limit > 0 && reverse) {
    return snapshots[0]->GetTopN(limit, true);
  }

  // NEW Optimization: Multi-term with limit and reverse (for multi-ngram queries)
  // Query planning: Use statistics to choose the best execution strategy
  if (terms.size() > 1 && limit > 0 && reverse) {
    // Step 1: Gather statistics (cheap: O(N) where N = number of terms)
    std::vector<std::pair<size_t, const PostingList*>> term_info;
    term_info.reserve(terms.size());

    for (const auto& snapshot : snapshots) {
      term_info.emplace_back(snapshot->Size(), snapshot.get());
    }

    // Find min and max sizes for selectivity estimation
    auto min_it = std::min_element(term_info.begin(), term_info.end(),
                                   [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });
    auto max_it = std::max_element(term_info.begin(), term_info.end(),
                                   [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    size_t min_size = min_it->first;
    size_t max_size = max_it->first;

    // Step 2: Estimate intersection selectivity
    // selectivity = min_size / max_size
    // High selectivity (close to 1.0) means terms are highly correlated (e.g., CJK bigrams)
    // Low selectivity (close to 0.0) means terms are independent
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    // Suppressing magic-numbers warning: query optimization thresholds are defined as named constants below
    double selectivity = (max_size > 0) ? static_cast<double>(min_size) / static_cast<double>(max_size) : 0.0;

    // Step 3: Query planning - choose execution strategy
    // Strategy 1: Streaming intersection (when selectivity is high)
    //   - Pros: Avoids materializing large result sets, early termination
    //   - Cons: Contains() lookups can be expensive
    //   - Best for: High selectivity (>50%), need only top-N results
    //
    // Strategy 2: Standard intersection (when selectivity is low)
    //   - Pros: Efficient set intersection, no redundant lookups
    //   - Cons: Materializes entire intersection result
    //   - Best for: Low selectivity (<50%), or when result set is small anyway

    constexpr double kSelectivityThreshold = 0.5;  // 50% threshold
    constexpr size_t kMinSizeThreshold = 10000;    // Don't optimize for tiny lists

    bool use_streaming = (selectivity >= kSelectivityThreshold) && (min_size >= kMinSizeThreshold);
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)

    if (use_streaming) {
      // Merge join optimization (DESC order)
      // Algorithm: Simultaneously walk backwards through all sorted posting lists
      // This is a classic merge join algorithm adapted for reverse iteration
      //
      // Performance: O(M) where M = size of posting lists
      // Much faster than binary_search approach: O(M + K*N*log(M))
      //
      // Example with 2 lists (DESC order):
      //   list1: [800K, 799K, ..., 3, 2, 1]
      //   list2: [800K, 799K, ..., 3, 2, 1]
      //   Walk both from end, when values match -> add to result

      // Get all posting lists (sorted ASC)
      std::vector<std::vector<DocId>> all_postings;
      all_postings.reserve(term_info.size());
      for (const auto& [size, posting] : term_info) {
        all_postings.push_back(posting->GetAll());
      }

      std::vector<DocId> result;
      result.reserve(limit);

      if (term_info.size() == 2) {
        // Optimized 2-way merge join
        const auto& list1 = all_postings[0];
        const auto& list2 = all_postings[1];

        auto it1 = list1.rbegin();
        auto it2 = list2.rbegin();

        while (result.size() < limit && it1 != list1.rend() && it2 != list2.rend()) {
          if (*it1 == *it2) {
            // Match found
            result.push_back(*it1);
            ++it1;
            ++it2;
          } else if (*it1 > *it2) {
            // it1 is ahead, advance it
            ++it1;
          } else {
            // it2 is ahead, advance it
            ++it2;
          }
        }
      } else {
        // N-way merge join (for 3+ terms)
        // Use iterators for each list
        std::vector<std::vector<DocId>::const_reverse_iterator> iters;
        std::vector<std::vector<DocId>::const_reverse_iterator> ends;
        iters.reserve(all_postings.size());
        ends.reserve(all_postings.size());

        for (const auto& list : all_postings) {
          iters.push_back(list.rbegin());
          ends.push_back(list.rend());
        }

        while (result.size() < limit) {
          // Check if any iterator is exhausted
          bool any_exhausted = false;
          for (size_t idx = 0; idx < iters.size(); ++idx) {
            if (iters[idx] == ends[idx]) {
              any_exhausted = true;
              break;
            }
          }
          if (any_exhausted) {
            break;
          }

          // Find maximum value among current positions
          DocId max_val = *iters[0];
          for (size_t idx = 1; idx < iters.size(); ++idx) {
            if (*iters[idx] > max_val) {
              max_val = *iters[idx];
            }
          }

          // Check if all iterators point to max_val
          bool all_match = true;
          for (const auto& iter : iters) {
            if (*iter != max_val) {
              all_match = false;
              break;
            }
          }

          if (all_match) {
            // All match - add to result
            result.push_back(max_val);
            // Advance all iterators
            for (auto& iter : iters) {
              ++iter;
            }
          } else {
            // Not all match - advance iterators pointing to max_val
            for (auto& iter : iters) {
              if (*iter == max_val) {
                ++iter;
              }
            }
          }
        }
      }

      // Merge join always produces exact results (or all available)
      mygram::utils::StructuredLog()
          .Event("merge_join_search")
          .Field("terms", static_cast<uint64_t>(terms.size()))
          .Field("selectivity", selectivity)
          .Field("min_size", static_cast<uint64_t>(min_size))
          .Field("max_size", static_cast<uint64_t>(max_size))
          .Field("found", static_cast<uint64_t>(result.size()))
          .Debug();
      return result;
    }
    mygram::utils::StructuredLog()
        .Event("standard_intersection_search")
        .Field("selectivity", selectivity)
        .Field("min_size", static_cast<uint64_t>(min_size))
        .Field("max_size", static_cast<uint64_t>(max_size))
        .Debug();
    // Fall through to standard path
  }

  // Standard path: Get all documents from all terms and intersect
  // Note: snapshots are already validated above (no nullptr)

  // Start with first term's documents
  auto result = snapshots[0]->GetAll();

  // Intersect with each subsequent term
  for (size_t i = 1; i < snapshots.size(); ++i) {
    auto term_docs = snapshots[i]->GetAll();
    std::vector<DocId> intersection;
    std::set_intersection(result.begin(), result.end(), term_docs.begin(), term_docs.end(),
                          std::back_inserter(intersection));
    result = std::move(intersection);

    if (result.empty()) {
      break;  // Early termination if no matches
    }
  }

  // Note: limit and reverse are applied by ResultSorter layer, not here
  // This is because we don't know the offset, and for multi-term queries
  // the intersection result size is unpredictable
  return result;
}

std::vector<DocId> Index::SearchOrInternal(const std::vector<std::string>& terms) const {
  // Internal implementation (assumes postings_mutex_ is already locked)
  std::vector<DocId> result;

  for (const auto& term : terms) {
    const auto* posting = GetPostingList(term);
    if (posting != nullptr) {
      auto term_docs = posting->GetAll();
      std::vector<DocId> union_result;
      std::set_union(result.begin(), result.end(), term_docs.begin(), term_docs.end(),
                     std::back_inserter(union_result));
      result = std::move(union_result);
    }
  }

  return result;
}

std::vector<DocId> Index::SearchOr(const std::vector<std::string>& terms) const {
  // RCU pattern: Take snapshot of posting lists under short lock, then search without lock
  if (terms.empty()) {
    return {};
  }

  // Take snapshot of all posting lists (short lock)
  auto snapshots = TakePostingSnapshots(terms);

  // From here, no lock is held - search operates on immutable snapshots
  std::vector<DocId> result;

  for (const auto& snapshot : snapshots) {
    if (snapshot != nullptr) {
      auto term_docs = snapshot->GetAll();
      std::vector<DocId> union_result;
      std::set_union(result.begin(), result.end(), term_docs.begin(), term_docs.end(),
                     std::back_inserter(union_result));
      result = std::move(union_result);
    }
  }

  return result;
}

std::vector<DocId> Index::SearchNot(const std::vector<DocId>& all_docs, const std::vector<std::string>& terms) const {
  if (terms.empty()) {
    return all_docs;
  }

  // RCU pattern: Take snapshot of posting lists under short lock, then search without lock
  auto snapshots = TakePostingSnapshots(terms);

  // From here, no lock is held - search operates on immutable snapshots
  // Get union of all documents containing any of the NOT terms
  std::vector<DocId> excluded_docs;

  for (const auto& snapshot : snapshots) {
    if (snapshot != nullptr) {
      auto term_docs = snapshot->GetAll();
      std::vector<DocId> union_result;
      std::set_union(excluded_docs.begin(), excluded_docs.end(), term_docs.begin(), term_docs.end(),
                     std::back_inserter(union_result));
      excluded_docs = std::move(union_result);
    }
  }

  // Return set difference: all_docs - excluded_docs
  std::vector<DocId> result;
  std::set_difference(all_docs.begin(), all_docs.end(), excluded_docs.begin(), excluded_docs.end(),
                      std::back_inserter(result));

  return result;
}

uint64_t Index::Count(std::string_view term) const {
  // RCU pattern: Take snapshot under short lock, then access without lock
  auto snapshot = TakePostingSnapshot(term);
  return (snapshot != nullptr) ? snapshot->Size() : 0;
}

size_t Index::MemoryUsage() const {
  // Acquire shared lock for read-only access (allows concurrent readers)
  std::shared_lock<std::shared_mutex> lock(postings_mutex_);
  size_t total = 0;
  for (const auto& [term, posting] : term_postings_) {
    total += term.size();             // Term string
    total += posting->MemoryUsage();  // Posting list
  }
  return total;
}

Index::IndexStatistics Index::GetStatistics() const {
  // Acquire shared lock for read-only access (allows concurrent readers)
  std::shared_lock<std::shared_mutex> lock(postings_mutex_);
  IndexStatistics stats;
  stats.total_terms = term_postings_.size();
  stats.total_postings = 0;
  stats.delta_encoded_lists = 0;
  stats.roaring_bitmap_lists = 0;
  stats.memory_usage_bytes = 0;

  for (const auto& [term, posting] : term_postings_) {
    // Count postings
    stats.total_postings += posting->Size();

    // Count strategy types
    if (posting->GetStrategy() == PostingStrategy::kDeltaCompressed) {
      stats.delta_encoded_lists++;
    } else if (posting->GetStrategy() == PostingStrategy::kRoaringBitmap) {
      stats.roaring_bitmap_lists++;
    }

    // Memory usage
    stats.memory_usage_bytes += term.size();             // Term string
    stats.memory_usage_bytes += posting->MemoryUsage();  // Posting list
  }

  return stats;
}

void Index::Optimize(uint64_t total_docs) {
  // Check if already optimizing (prevent concurrent Optimize() calls)
  bool expected = false;
  if (!is_optimizing_.compare_exchange_strong(expected, true)) {
    mygram::utils::StructuredLog().Event("index_optimization_skipped").Field("reason", "already in progress").Warn();
    return;
  }

  // RAII guard to ensure flag is cleared
  struct OptimizationGuard {
    std::atomic<bool>& flag;
    explicit OptimizationGuard(std::atomic<bool>& flag_ref) : flag(flag_ref) {}
    ~OptimizationGuard() { flag = false; }
    OptimizationGuard(const OptimizationGuard&) = delete;
    OptimizationGuard& operator=(const OptimizationGuard&) = delete;
    OptimizationGuard(OptimizationGuard&&) = delete;
    OptimizationGuard& operator=(OptimizationGuard&&) = delete;
  };
  OptimizationGuard guard(is_optimizing_);

  // Phase 1a: Take snapshot of posting list SIZES and pointers (brief shared_lock)
  // IMPORTANT: We store both sizes and shared_ptrs:
  // - shared_ptr copies keep posting lists alive during optimization
  // - Size snapshots capture state at T0, unaffected by concurrent AddDocument()
  std::unordered_map<std::string, std::shared_ptr<PostingList>> snapshot;
  std::unordered_map<std::string, size_t> snapshot_sizes;
  {
    std::shared_lock<std::shared_mutex> lock(postings_mutex_);
    for (const auto& [term, posting] : term_postings_) {
      snapshot[term] = posting;                // Copy shared_ptr (reference counting)
      snapshot_sizes[term] = posting->Size();  // Capture size at snapshot time
    }
  }
  // Lock released - AddDocument/RemoveDocument can now proceed

  // Phase 1b: Create optimized copies outside the lock (CPU-intensive work)
  // This doesn't block any operations - searches and writes continue normally
  // The snapshot keeps posting lists alive via shared_ptr reference counting
  std::unordered_map<std::string, std::shared_ptr<PostingList>> optimized_postings;
  for (const auto& [term, posting] : snapshot) {
    // Clone creates an optimized copy without modifying the original
    optimized_postings[term] = posting->Clone(total_docs);
  }

  // Phase 2: Atomically swap the old index with the new optimized index
  // Brief exclusive lock to update the map
  size_t term_count = 0;
  size_t merged_count = 0;
  {
    std::unique_lock<std::shared_mutex> lock(postings_mutex_);

    term_count = optimized_postings.size();

    // Update only terms that still exist in the index
    // This preserves concurrent modifications:
    // - Terms removed during Phase 1: won't be re-added (not in term_postings_)
    // - Terms added during Phase 1: won't be optimized (not in optimized_postings)
    // - Terms modified during Phase 1: merge new additions with optimized version
    for (auto& [term, optimized_posting] : optimized_postings) {
      auto current_it = term_postings_.find(term);
      if (current_it != term_postings_.end()) {
        const auto& current_posting = current_it->second;
        auto snapshot_size_it = snapshot_sizes.find(term);

        // Check if documents were added during optimization
        // IMPORTANT: Compare with snapshot SIZE, not snapshot object's current size
        // The snapshot shared_ptr points to the same mutable object that AddDocument() modifies
        if (snapshot_size_it != snapshot_sizes.end() && current_posting->Size() > snapshot_size_it->second) {
          // New documents were added: merge them with the optimized version
          auto merged = optimized_posting->Union(*current_posting);
          merged->Optimize(total_docs);  // Re-optimize after merge
          term_postings_[term] = std::move(merged);
          merged_count++;
        } else {
          // No changes or only removals: use optimized version as-is
          term_postings_[term] = std::move(optimized_posting);
        }
      }
      // If term was removed, don't re-add it
    }
  }

  if (merged_count > 0) {
    mygram::utils::StructuredLog()
        .Event("index_optimization_merge")
        .Field("merged_terms", static_cast<uint64_t>(merged_count))
        .Debug();
  }

  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  // 1024: Standard conversion factor for bytes to KB to MB
  size_t final_term_count = 0;
  {
    std::shared_lock<std::shared_mutex> lock(postings_mutex_);
    final_term_count = term_postings_.size();
  }
  mygram::utils::StructuredLog()
      .Event("index_optimized")
      .Field("terms_optimized", static_cast<uint64_t>(term_count))
      .Field("terms_final", static_cast<uint64_t>(final_term_count))
      .Field("memory_mb", static_cast<uint64_t>(MemoryUsage() / (1024 * 1024)))
      .Info();
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
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

  // RAII guard to ensure flag is cleared even if exception occurs
  struct OptimizationGuard {
    std::atomic<bool>& flag;
    explicit OptimizationGuard(std::atomic<bool>& flag_ref) : flag(flag_ref) {}
    OptimizationGuard(const OptimizationGuard&) = delete;
    OptimizationGuard& operator=(const OptimizationGuard&) = delete;
    OptimizationGuard(OptimizationGuard&&) = delete;
    OptimizationGuard& operator=(OptimizationGuard&&) = delete;
    ~OptimizationGuard() { flag.store(false); }
  };
  OptimizationGuard guard(is_optimizing_);

  mygram::utils::StructuredLog()
      .Event("index_batch_optimization_starting")
      .Field("terms", static_cast<uint64_t>(term_postings_.size()))
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

    // Phase 1a: Take snapshot of posting list SIZES for this batch (brief shared_lock)
    // IMPORTANT: We only store sizes, not shared_ptrs, because:
    // - shared_ptr copies would point to the same mutable objects
    // - AddDocument() modifies the objects in-place, invalidating the "snapshot"
    // - Storing sizes ensures we can detect concurrent additions accurately
    std::unordered_map<std::string, size_t> batch_snapshot_sizes;
    std::unordered_map<std::string, std::shared_ptr<PostingList>> batch_snapshot_ptrs;
    {
      std::shared_lock<std::shared_mutex> lock(postings_mutex_);
      for (size_t j = i; j < batch_end; ++j) {
        const auto& term = terms[j];
        auto iter = term_postings_.find(term);
        if (iter != term_postings_.end()) {
          batch_snapshot_sizes[term] = iter->second->Size();  // Capture size at snapshot time
          batch_snapshot_ptrs[term] = iter->second;           // Keep pointer for optimization
        }
      }
    }
    // Lock released - AddDocument/RemoveDocument can proceed

    // Phase 1b: Create optimized copies for this batch (CPU-intensive, outside lock)
    std::unordered_map<std::string, std::shared_ptr<PostingList>> optimized_postings;
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

    // Phase 2: Atomically swap the optimized batch (brief exclusive lock)
    {
      std::unique_lock<std::shared_mutex> lock(postings_mutex_);

      // Update only terms that still exist in the index
      // This preserves concurrent modifications:
      // - Terms removed during Phase 1: won't be re-added (not in term_postings_)
      // - Terms added during Phase 1: won't be optimized (not in optimized_postings)
      // - Terms modified during Phase 1: merge new additions with optimized version
      for (size_t j = i; j < batch_end; ++j) {
        const auto& term = terms[j];
        auto opt_it = optimized_postings.find(term);
        if (opt_it == optimized_postings.end()) {
          continue;  // Term wasn't optimized
        }

        auto current_it = term_postings_.find(term);
        if (current_it != term_postings_.end()) {
          const auto& current_posting = current_it->second;
          auto snapshot_size_it = batch_snapshot_sizes.find(term);

          // Check if documents were added during this batch's optimization
          // IMPORTANT: Compare with snapshot SIZE, not snapshot object, because:
          // - The snapshot shared_ptr points to the same mutable object
          // - AddDocument() modifies it in-place, invalidating size comparisons
          if (snapshot_size_it != batch_snapshot_sizes.end() && current_posting->Size() > snapshot_size_it->second) {
            // New documents were added: merge them with the optimized version
            auto merged = opt_it->second->Union(*current_posting);
            merged->Optimize(total_docs);
            term_postings_[term] = std::move(merged);
          } else {
            // No changes or only removals: use optimized version as-is
            term_postings_[term] = std::move(opt_it->second);
          }
        }
        // If term was removed, don't re-add it
      }
    }
    // Lock released - brief pause allows other operations to proceed

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

void Index::Clear() {
  // Acquire exclusive lock for modifying posting lists
  std::unique_lock<std::shared_mutex> lock(postings_mutex_);
  term_postings_.clear();
  mygram::utils::StructuredLog().Event("index_cleared").Info();
}

PostingList* Index::GetOrCreatePostingList(std::string_view term) {
  // C++17: unordered_map doesn't support heterogeneous lookup, convert to std::string
  std::string term_str(term);
  auto iterator = term_postings_.find(term_str);
  if (iterator != term_postings_.end()) {
    return iterator->second.get();
  }

  // Create new posting list
  auto posting = std::make_shared<PostingList>(roaring_threshold_);
  auto* ptr = posting.get();
  term_postings_[std::move(term_str)] = std::move(posting);
  return ptr;
}

const PostingList* Index::GetPostingList(std::string_view term) const {
  // NOTE: This method assumes postings_mutex_ is already locked by the caller

  // TECHNICAL DEBT: String copy required for C++17 std::unordered_map lookup
  // C++17's std::unordered_map does NOT support heterogeneous lookup (is_transparent)
  // This was added in C++20 (P0919R3). Current impact:
  //   - ~20-50ns allocation + copy overhead per lookup
  //   - Called 2-10 times per query (multi-term searches)
  //   - At 1000 QPS: 2,000-10,000 allocations/second
  //
  // FUTURE OPTIMIZATION (when upgrading to C++20):
  //   1. Add TransparentStringHash and TransparentStringEqual functors with is_transparent tag
  //   2. Change term_postings_ type to:
  //      std::unordered_map<std::string, std::shared_ptr<PostingList>,
  //                         TransparentStringHash, TransparentStringEqual>
  //   3. Remove std::string(term) conversion below
  //   Expected improvement: 5-10% latency reduction for multi-term searches
  //
  // ALTERNATIVE (if C++20 upgrade is not feasible):
  //   - Use absl::flat_hash_map (requires external dependency)
  //   - Switch to std::map + std::less<> (zero-copy but O(log N) lookup)
  auto iterator = term_postings_.find(std::string(term));
  return iterator != term_postings_.end() ? iterator->second.get() : nullptr;
}

std::vector<std::shared_ptr<PostingList>> Index::TakePostingSnapshots(const std::vector<std::string>& terms) const {
  // RCU pattern: Take short lock to copy shared_ptrs, then release
  // This allows search to proceed without holding any lock
  std::vector<std::shared_ptr<PostingList>> snapshots;
  snapshots.reserve(terms.size());

  {
    std::shared_lock<std::shared_mutex> lock(postings_mutex_);
    for (const auto& term : terms) {
      auto iter = term_postings_.find(term);
      if (iter != term_postings_.end()) {
        snapshots.push_back(iter->second);  // Copy shared_ptr (ref count increment)
      } else {
        snapshots.push_back(nullptr);  // Term not found
      }
    }
  }  // Lock released here

  return snapshots;
}

std::shared_ptr<PostingList> Index::TakePostingSnapshot(std::string_view term) const {
  // RCU pattern: Take short lock to copy shared_ptr, then release
  std::shared_lock<std::shared_mutex> lock(postings_mutex_);
  auto iter = term_postings_.find(std::string(term));
  return (iter != term_postings_.end()) ? iter->second : nullptr;
}

bool Index::SaveToFile(const std::string& filepath) const {
  try {
    std::ofstream ofs(filepath, std::ios::binary);
    if (!ofs) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "file_open_failed")
          .Field("operation", "save")
          .Field("filepath", filepath)
          .Error();
      return false;
    }

    // File format:
    // [4 bytes: magic "MGIX"] [4 bytes: version] [4 bytes: ngram_size]
    // [8 bytes: term_count] [terms and posting lists...]

    // Write magic number
    ofs.write("MGIX", 4);

    // Write version
    uint32_t version = 1;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
    ofs.write(reinterpret_cast<const char*>(&version), sizeof(version));

    // Write ngram_size
    auto ngram = static_cast<uint32_t>(ngram_size_);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
    ofs.write(reinterpret_cast<const char*>(&ngram), sizeof(ngram));

    uint64_t term_count = 0;
    // Lock scope: iterate and serialize posting lists
    {
      std::scoped_lock lock(postings_mutex_);

      // Write term count
      term_count = term_postings_.size();
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
      ofs.write(reinterpret_cast<const char*>(&term_count), sizeof(term_count));

      // Write each term and its posting list
      for (const auto& [term, posting] : term_postings_) {
        // Write term length and term
        auto term_len = static_cast<uint32_t>(term.size());
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
        ofs.write(reinterpret_cast<const char*>(&term_len), sizeof(term_len));
        ofs.write(term.data(), static_cast<std::streamsize>(term_len));

        // Serialize posting list to buffer
        std::vector<uint8_t> posting_data;
        posting->Serialize(posting_data);

        // Write posting list size and data
        uint64_t posting_size = posting_data.size();
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
        ofs.write(reinterpret_cast<const char*>(&posting_size), sizeof(posting_size));
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
        ofs.write(reinterpret_cast<const char*>(posting_data.data()), static_cast<std::streamsize>(posting_size));
      }
    }

    ofs.close();
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    // 1024: Standard conversion factor for bytes to KB to MB
    mygram::utils::StructuredLog()
        .Event("index_saved")
        .Field("path", filepath)
        .Field("terms", term_count)
        .Field("memory_mb", static_cast<uint64_t>(MemoryUsage() / (1024 * 1024)))
        .Info();
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    return true;
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("index_io_error")
        .Field("type", "exception_during_save")
        .Field("operation", "save")
        .Field("error", e.what())
        .Error();
    return false;
  }
}

bool Index::SaveToStream(std::ostream& output_stream) const {
  try {
    // File format:
    // [4 bytes: magic "MGIX"] [4 bytes: version] [4 bytes: ngram_size]
    // [8 bytes: term_count] [terms and posting lists...]

    // Write magic number
    output_stream.write("MGIX", 4);

    // Write version
    uint32_t version = 1;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
    output_stream.write(reinterpret_cast<const char*>(&version), sizeof(version));

    // Write ngram_size
    auto ngram = static_cast<uint32_t>(ngram_size_);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
    output_stream.write(reinterpret_cast<const char*>(&ngram), sizeof(ngram));

    uint64_t term_count = 0;
    // Lock scope: iterate and serialize posting lists
    {
      std::scoped_lock lock(postings_mutex_);

      // Write term count
      term_count = term_postings_.size();
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
      output_stream.write(reinterpret_cast<const char*>(&term_count), sizeof(term_count));

      // Write each term and its posting list
      for (const auto& [term, posting] : term_postings_) {
        // Write term length and term
        auto term_len = static_cast<uint32_t>(term.size());
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
        output_stream.write(reinterpret_cast<const char*>(&term_len), sizeof(term_len));
        output_stream.write(term.data(), static_cast<std::streamsize>(term_len));

        // Serialize posting list to buffer
        std::vector<uint8_t> posting_data;
        posting->Serialize(posting_data);

        // Write posting list size and data
        uint64_t posting_size = posting_data.size();
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
        output_stream.write(reinterpret_cast<const char*>(&posting_size), sizeof(posting_size));
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
        output_stream.write(reinterpret_cast<const char*>(posting_data.data()),
                            static_cast<std::streamsize>(posting_size));
      }
    }

    if (!output_stream.good()) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "stream_error")
          .Field("operation", "save_to_stream")
          .Error();
      return false;
    }

    mygram::utils::StructuredLog().Event("index_saved_to_stream").Field("terms", term_count).Debug();
    return true;
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("index_io_error")
        .Field("type", "exception_during_save")
        .Field("operation", "save_to_stream")
        .Field("error", e.what())
        .Error();
    return false;
  }
}

bool Index::LoadFromFile(const std::string& filepath) {
  try {
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "file_open_failed")
          .Field("operation", "load")
          .Field("filepath", filepath)
          .Error();
      return false;
    }

    // Read and verify magic number
    std::array<char, 4> magic{};
    ifs.read(magic.data(), magic.size());
    if (std::memcmp(magic.data(), "MGIX", 4) != 0) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "invalid_format")
          .Field("operation", "load")
          .Field("error", "bad_magic_number")
          .Field("filepath", filepath)
          .Error();
      return false;
    }

    // Read version
    uint32_t version = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
    ifs.read(reinterpret_cast<char*>(&version), sizeof(version));
    if (version != 1) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "unsupported_version")
          .Field("operation", "load")
          .Field("version", std::to_string(version))
          .Field("filepath", filepath)
          .Error();
      return false;
    }

    // Read ngram_size
    uint32_t ngram = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
    ifs.read(reinterpret_cast<char*>(&ngram), sizeof(ngram));
    if (static_cast<int>(ngram) != ngram_size_) {
      mygram::utils::StructuredLog()
          .Event("index_ngram_mismatch")
          .Field("file_ngram", static_cast<uint64_t>(ngram))
          .Field("current_ngram", static_cast<uint64_t>(ngram_size_))
          .Warn();
      // Continue anyway, but this might cause issues
    }

    // Read term count
    uint64_t term_count = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
    ifs.read(reinterpret_cast<char*>(&term_count), sizeof(term_count));

    // Load into a new map to minimize lock time
    std::unordered_map<std::string, std::shared_ptr<PostingList>> new_postings;

    // Read each term and its posting list
    for (uint64_t i = 0; i < term_count; ++i) {
      // Read term length and term
      uint32_t term_len = 0;
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
      ifs.read(reinterpret_cast<char*>(&term_len), sizeof(term_len));

      std::string term(term_len, '\0');
      ifs.read(term.data(), static_cast<std::streamsize>(term_len));

      // Read posting list size and data
      uint64_t posting_size = 0;
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
      ifs.read(reinterpret_cast<char*>(&posting_size), sizeof(posting_size));

      std::vector<uint8_t> posting_data(posting_size);
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
      ifs.read(reinterpret_cast<char*>(posting_data.data()), static_cast<std::streamsize>(posting_size));

      // Deserialize posting list
      auto posting = std::make_shared<PostingList>(roaring_threshold_);
      size_t offset = 0;
      if (!posting->Deserialize(posting_data, offset)) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "deserialization_failed")
            .Field("operation", "load")
            .Field("term", term)
            .Field("filepath", filepath)
            .Error();
        return false;
      }

      new_postings[term] = std::move(posting);
    }

    ifs.close();

    // Swap the loaded data in with minimal lock time
    {
      std::scoped_lock lock(postings_mutex_);
      term_postings_ = std::move(new_postings);
    }

    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    // 1024: Standard conversion factor for bytes to KB to MB
    mygram::utils::StructuredLog()
        .Event("index_loaded")
        .Field("path", filepath)
        .Field("terms", term_count)
        .Field("memory_mb", static_cast<uint64_t>(MemoryUsage() / (1024 * 1024)))
        .Info();
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    return true;
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("index_io_error")
        .Field("type", "exception_during_load")
        .Field("operation", "load")
        .Field("error", e.what())
        .Error();
    return false;
  }
}

bool Index::LoadFromStream(std::istream& input_stream) {
  try {
    // Read and verify magic number
    std::array<char, 4> magic{};
    input_stream.read(magic.data(), magic.size());
    if (std::memcmp(magic.data(), "MGIX", 4) != 0) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "invalid_format")
          .Field("operation", "load_from_stream")
          .Field("error", "bad_magic_number")
          .Error();
      return false;
    }

    // Read version
    uint32_t version = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
    input_stream.read(reinterpret_cast<char*>(&version), sizeof(version));
    if (version != 1) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "unsupported_version")
          .Field("operation", "load_from_stream")
          .Field("version", std::to_string(version))
          .Error();
      return false;
    }

    // Read ngram_size
    uint32_t ngram = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
    input_stream.read(reinterpret_cast<char*>(&ngram), sizeof(ngram));
    if (static_cast<int>(ngram) != ngram_size_) {
      mygram::utils::StructuredLog()
          .Event("index_ngram_mismatch")
          .Field("stream_ngram", static_cast<uint64_t>(ngram))
          .Field("current_ngram", static_cast<uint64_t>(ngram_size_))
          .Warn();
      // Continue anyway, but this might cause issues
    }

    // Read term count
    uint64_t term_count = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
    input_stream.read(reinterpret_cast<char*>(&term_count), sizeof(term_count));

    // Load into a new map to minimize lock time
    std::unordered_map<std::string, std::shared_ptr<PostingList>> new_postings;

    // Read each term and its posting list
    for (uint64_t i = 0; i < term_count; ++i) {
      // Read term length and term
      uint32_t term_len = 0;
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
      input_stream.read(reinterpret_cast<char*>(&term_len), sizeof(term_len));

      std::string term(term_len, '\0');
      input_stream.read(term.data(), static_cast<std::streamsize>(term_len));

      // Read posting list size and data
      uint64_t posting_size = 0;
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
      input_stream.read(reinterpret_cast<char*>(&posting_size), sizeof(posting_size));

      std::vector<uint8_t> posting_data(posting_size);
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
      input_stream.read(reinterpret_cast<char*>(posting_data.data()), static_cast<std::streamsize>(posting_size));

      // Deserialize posting list
      auto posting = std::make_shared<PostingList>(roaring_threshold_);
      size_t offset = 0;
      if (!posting->Deserialize(posting_data, offset)) {
        mygram::utils::StructuredLog()
            .Event("index_io_error")
            .Field("type", "deserialization_failed")
            .Field("operation", "load_from_stream")
            .Field("term", term)
            .Error();
        return false;
      }

      new_postings[term] = std::move(posting);
    }

    if (!input_stream.good()) {
      mygram::utils::StructuredLog()
          .Event("index_io_error")
          .Field("type", "stream_error")
          .Field("operation", "load_from_stream")
          .Error();
      return false;
    }

    // Swap the loaded data in with minimal lock time
    {
      std::scoped_lock lock(postings_mutex_);
      term_postings_ = std::move(new_postings);
    }

    mygram::utils::StructuredLog().Event("index_loaded_from_stream").Field("terms", term_count).Debug();
    return true;
  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("index_io_error")
        .Field("type", "exception_during_load")
        .Field("operation", "load_from_stream")
        .Field("error", e.what())
        .Error();
    return false;
  }
}

}  // namespace mygramdb::index
