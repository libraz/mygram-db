/**
 * @file index.cpp
 * @brief N-gram inverted index implementation (core operations)
 */

#include "index/index.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <queue>
#include <tuple>

#include "utils/string_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::index {

namespace {

// Query optimization thresholds for streaming vs standard intersection
constexpr double kSelectivityThreshold = 0.5;  // 50% threshold
constexpr size_t kMinSizeThreshold = 10000;    // Don't optimize for tiny lists

}  // namespace

Index::Index(int ngram_size, int kanji_ngram_size, double roaring_threshold, bool cross_boundary_ngrams,
             bool normalize_nfkc, const std::string& normalize_width, bool normalize_lower)
    : ngram_size_(ngram_size),
      kanji_ngram_size_(kanji_ngram_size > 0 ? kanji_ngram_size : ngram_size),
      roaring_threshold_(roaring_threshold),
      cross_boundary_ngrams_(cross_boundary_ngrams),
      normalize_nfkc_(normalize_nfkc),
      normalize_width_(normalize_width),
      normalize_lower_(normalize_lower) {}

bool Index::AddDocument(DocId doc_id, std::string_view text) {
  // Generate n-grams using hybrid mode (no lock needed for this CPU-intensive operation)
  std::vector<std::string> ngrams =
      mygram::utils::GenerateHybridNgrams(text, ngram_size_, kanji_ngram_size_, cross_boundary_ngrams_);

  // Remove duplicates by sorting and using unique (more efficient than unordered_set)
  mygram::utils::DeduplicateSorted(ngrams);

  // Warn when empty text produces no index entries (document will exist in
  // DocumentStore but will never appear in search results)
  if (ngrams.empty()) {
    mygram::utils::StructuredLog()
        .Event("empty_document_skipped")
        .Field("doc_id", static_cast<uint64_t>(doc_id))
        .Field("text_length", static_cast<uint64_t>(text.size()))
        .Message("Document has no indexable n-grams; it will not appear in search results")
        .Warn();
    return false;
  }

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
  return true;
}

void Index::AddDocumentBatch(const std::vector<DocumentItem>& documents) {
  if (documents.empty()) {
    return;
  }

  // Phase 1: Generate n-grams for all documents (no locking, CPU-intensive)
  // Map: term -> vector of doc_ids containing that term
  absl::flat_hash_map<std::string, std::vector<DocId>> term_to_docs;

  for (const auto& doc : documents) {
    // Generate n-grams
    std::vector<std::string> ngrams;
    ngrams = mygram::utils::GenerateHybridNgrams(doc.text, ngram_size_, kanji_ngram_size_, cross_boundary_ngrams_);

    // Remove duplicates by sorting and using unique (more efficient than unordered_set)
    mygram::utils::DeduplicateSorted(ngrams);

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
  std::vector<std::string> old_ngrams =
      mygram::utils::GenerateHybridNgrams(old_text, ngram_size_, kanji_ngram_size_, cross_boundary_ngrams_);
  std::vector<std::string> new_ngrams =
      mygram::utils::GenerateHybridNgrams(new_text, ngram_size_, kanji_ngram_size_, cross_boundary_ngrams_);

  // Sort and remove duplicates (more efficient than unordered_set)
  mygram::utils::DeduplicateSorted(old_ngrams);
  mygram::utils::DeduplicateSorted(new_ngrams);

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
  size_t empty_lists_removed = 0;
  for (const auto& ngram : to_remove) {
    auto posting_iter = term_postings_.find(ngram);
    if (posting_iter != term_postings_.end()) {
      posting_iter->second->Remove(doc_id);
      // Remove empty posting lists to prevent memory leak
      if (posting_iter->second->SizeApprox() == 0) {
        term_postings_.erase(posting_iter);
        empty_lists_removed++;
      }
    }
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
      .Field("empty_lists_removed", static_cast<uint64_t>(empty_lists_removed))
      .Debug();
}

void Index::RemoveDocument(DocId doc_id, std::string_view text) {
  // Generate n-grams (no lock needed for CPU-intensive operation)
  std::vector<std::string> ngrams =
      mygram::utils::GenerateHybridNgrams(text, ngram_size_, kanji_ngram_size_, cross_boundary_ngrams_);

  // Remove duplicates by sorting and using unique (more efficient than unordered_set)
  mygram::utils::DeduplicateSorted(ngrams);

  // Acquire exclusive lock for modifying posting lists
  std::unique_lock<std::shared_mutex> lock(postings_mutex_);

  // Remove document from posting list for each n-gram
  size_t empty_lists_removed = 0;
  for (const auto& ngram : ngrams) {
    auto posting_iter = term_postings_.find(ngram);
    if (posting_iter != term_postings_.end()) {
      posting_iter->second->Remove(doc_id);
      // Remove empty posting lists to prevent memory leak
      if (posting_iter->second->SizeApprox() == 0) {
        term_postings_.erase(posting_iter);
        empty_lists_removed++;
      }
    }
  }

  mygram::utils::StructuredLog()
      .Event("document_removed")
      .Field("doc_id", static_cast<uint64_t>(doc_id))
      .Field("ngrams_removed", static_cast<uint64_t>(ngrams.size()))
      .Field("empty_lists_removed", static_cast<uint64_t>(empty_lists_removed))
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
      // Use SizeApprox() to avoid acquiring per-PostingList mutex on each
      // snapshot. After TakePostingSnapshots(), the shared_ptrs keep the
      // PostingLists alive and the atomic doc_count_ is sufficient for
      // query planning decisions.
      term_info.emplace_back(snapshot->SizeApprox(), snapshot.get());
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

    bool use_streaming = (selectivity >= kSelectivityThreshold) && (min_size >= kMinSizeThreshold);

    if (use_streaming) {
      // Optimized intersection using PostingList::Intersect() chain
      // This avoids materializing all documents by:
      // 1. Using Roaring bitmap's native AND operation (memory efficient)
      // 2. Only materializing the top N results at the end via GetTopN()
      //
      // Previous implementation called GetAll() on all posting lists,
      // which would allocate 100MB+ per query for large datasets.

      // Start with the smallest posting list and chain intersections
      // Sort by size (smallest first) for optimal intersection order
      std::vector<std::pair<size_t, const PostingList*>> sorted_info = term_info;
      std::sort(sorted_info.begin(), sorted_info.end(),
                [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

      // Chain intersections: result = P1 ∩ P2 ∩ P3 ∩ ...
      std::unique_ptr<PostingList> intersected = sorted_info[0].second->Intersect(*sorted_info[1].second);

      for (size_t i = 2; i < sorted_info.size(); ++i) {
        intersected = intersected->Intersect(*sorted_info[i].second);
        // Early termination if intersection becomes empty
        if (intersected->SizeApprox() == 0) {
          break;
        }
      }

      // Get only the top N results (reverse order for DESC)
      auto result = intersected->GetTopN(limit, true);

      mygram::utils::StructuredLog()
          .Event("intersect_chain_search")
          .Field("terms", static_cast<uint64_t>(terms.size()))
          .Field("selectivity", selectivity)
          .Field("min_size", static_cast<uint64_t>(min_size))
          .Field("max_size", static_cast<uint64_t>(max_size))
          .Field("intersected_size", intersected->SizeApprox())
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

  // Apply limit if specified (optimization: avoid returning more results than needed)
  // Note: offset is applied by ResultSorter, but limit can be applied here to reduce
  // the amount of data passed to the sorting layer.
  if (limit > 0 && result.size() > limit) {
    if (reverse) {
      // Take the last `limit` elements (highest DocIDs)
      result.erase(result.begin(), result.begin() + static_cast<std::ptrdiff_t>(result.size() - limit));
    } else {
      result.resize(limit);
    }
  }
  return result;
}

std::vector<DocId> Index::FilterByNgrams(const std::vector<DocId>& candidates,
                                         const std::vector<std::string>& terms) const {
  // Take snapshots of posting lists (RCU pattern, same as SearchAnd)
  auto snapshots = TakePostingSnapshots(terms);

  std::vector<DocId> result;
  result.reserve(candidates.size());

  for (const auto& doc_id : candidates) {
    bool match = true;
    for (size_t i = 0; i < terms.size(); ++i) {
      if (!snapshots[i] || !snapshots[i]->Contains(doc_id)) {
        match = false;
        break;
      }
    }
    if (match) {
      result.push_back(doc_id);
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
  std::vector<DocId> temp;

  for (const auto& snapshot : snapshots) {
    if (snapshot != nullptr) {
      auto term_docs = snapshot->GetAll();
      temp.clear();
      temp.reserve(result.size() + term_docs.size());
      std::set_union(result.begin(), result.end(), term_docs.begin(), term_docs.end(), std::back_inserter(temp));
      result.swap(temp);
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
  std::vector<DocId> temp;

  for (const auto& snapshot : snapshots) {
    if (snapshot != nullptr) {
      auto term_docs = snapshot->GetAll();
      temp.clear();
      temp.reserve(excluded_docs.size() + term_docs.size());
      std::set_union(excluded_docs.begin(), excluded_docs.end(), term_docs.begin(), term_docs.end(),
                     std::back_inserter(temp));
      excluded_docs.swap(temp);
    }
  }

  // Return set difference: all_docs - excluded_docs
  std::vector<DocId> result;
  std::set_difference(all_docs.begin(), all_docs.end(), excluded_docs.begin(), excluded_docs.end(),
                      std::back_inserter(result));

  return result;
}

std::vector<DocId> Index::SearchByThreshold(const std::vector<std::string>& terms, size_t threshold) const {
  if (terms.empty() || threshold == 0) {
    return {};
  }

  // Delegate to SearchAnd when threshold equals term count
  if (threshold >= terms.size()) {
    return SearchAnd(terms);
  }

  // RCU pattern: Take snapshot of posting lists under short lock
  auto snapshots = TakePostingSnapshots(terms);

  // Collect non-null snapshots (missing n-grams don't count toward threshold)
  std::vector<std::shared_ptr<PostingList>> valid_snapshots;
  valid_snapshots.reserve(snapshots.size());
  for (auto& snapshot : snapshots) {
    if (snapshot != nullptr) {
      valid_snapshots.push_back(std::move(snapshot));
    }
  }

  // If fewer valid posting lists than threshold, no document can meet it
  if (valid_snapshots.size() < threshold) {
    return {};
  }

  // Get all posting lists as sorted vectors
  std::vector<std::vector<DocId>> all_docs;
  all_docs.reserve(valid_snapshots.size());
  for (const auto& snapshot : valid_snapshots) {
    all_docs.push_back(snapshot->GetAll());
  }

  // K-way merge with counting using min-heap
  // Heap element: (doc_id, list_index, position_in_list)
  using HeapEntry = std::tuple<DocId, size_t, size_t>;
  std::priority_queue<HeapEntry, std::vector<HeapEntry>, std::greater<HeapEntry>> heap;

  // Initialize heap with first element of each list
  for (size_t i = 0; i < all_docs.size(); ++i) {
    if (!all_docs[i].empty()) {
      heap.emplace(all_docs[i][0], i, 0);
    }
  }

  std::vector<DocId> result;
  DocId current_doc = 0;
  size_t current_count = 0;
  bool has_current = false;

  while (!heap.empty()) {
    auto [doc_id, list_idx, pos] = heap.top();
    heap.pop();

    if (!has_current || doc_id != current_doc) {
      // Emit previous document if it met threshold
      if (has_current && current_count >= threshold) {
        result.push_back(current_doc);
      }
      current_doc = doc_id;
      current_count = 1;
      has_current = true;
    } else {
      ++current_count;
    }

    // Advance in this list
    size_t next_pos = pos + 1;
    if (next_pos < all_docs[list_idx].size()) {
      heap.emplace(all_docs[list_idx][next_pos], list_idx, next_pos);
    }
  }

  // Don't forget the last document
  if (has_current && current_count >= threshold) {
    result.push_back(current_doc);
  }

  return result;
}

uint64_t Index::Count(std::string_view term) const {
  // RCU pattern: Take snapshot under short lock, then access without lock
  auto snapshot = TakePostingSnapshot(term);
  return (snapshot != nullptr) ? snapshot->Size() : 0;
}

size_t Index::MemoryUsage() const {
  // Acquire shared lock for read-only access (allows concurrent readers).
  // Use SizeApprox/MemoryUsageApprox to avoid redundant inner lock acquisition
  // on each PostingList — the outer shared lock on postings_mutex_ already
  // prevents structural changes to the map and its entries.
  std::shared_lock<std::shared_mutex> lock(postings_mutex_);
  size_t total = 0;
  for (const auto& [term, posting] : term_postings_) {
    total += term.size();                   // Term string
    total += posting->MemoryUsageApprox();  // Posting list (no inner lock)
  }
  return total;
}

Index::IndexStatistics Index::GetStatistics() const {
  // Acquire shared lock for read-only access (allows concurrent readers).
  // Use SizeApprox/MemoryUsageApprox to avoid redundant inner lock acquisition
  // on each PostingList — the outer shared lock prevents structural changes.
  std::shared_lock<std::shared_mutex> lock(postings_mutex_);
  IndexStatistics stats;
  stats.total_terms = term_postings_.size();
  stats.total_postings = 0;
  stats.delta_encoded_lists = 0;
  stats.roaring_bitmap_lists = 0;
  stats.memory_usage_bytes = 0;

  for (const auto& [term, posting] : term_postings_) {
    // Count postings (no inner lock — outer shared lock is sufficient)
    stats.total_postings += posting->SizeApprox();

    // Count strategy types
    if (posting->GetStrategy() == PostingStrategy::kDeltaCompressed) {
      stats.delta_encoded_lists++;
    } else if (posting->GetStrategy() == PostingStrategy::kRoaringBitmap) {
      stats.roaring_bitmap_lists++;
    }

    // Memory usage (no inner lock — outer shared lock is sufficient)
    stats.memory_usage_bytes += term.size();
    stats.memory_usage_bytes += posting->MemoryUsageApprox();
  }

  return stats;
}

void Index::Clear() {
  // Acquire exclusive lock for modifying posting lists
  std::unique_lock<std::shared_mutex> lock(postings_mutex_);
  term_postings_.clear();
  mygram::utils::StructuredLog().Event("index_cleared").Info();
}

PostingList* Index::GetOrCreatePostingList(std::string_view term) {
  // absl::flat_hash_map with TransparentStringHash supports heterogeneous lookup
  auto iterator = term_postings_.find(term);
  if (iterator != term_postings_.end()) {
    return iterator->second.get();
  }

  // Create new posting list - only allocate std::string on the insert path
  auto posting = std::make_shared<PostingList>(roaring_threshold_);
  auto* ptr = posting.get();
  term_postings_[std::string(term)] = std::move(posting);
  return ptr;
}

const PostingList* Index::GetPostingList(std::string_view term) const {
  // NOTE: This method assumes postings_mutex_ is already locked by the caller

  // Heterogeneous lookup via absl::flat_hash_map with TransparentStringHash —
  // no std::string allocation required.
  auto iterator = term_postings_.find(term);
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
  auto iter = term_postings_.find(term);
  return (iter != term_postings_.end()) ? iter->second : nullptr;
}

uint64_t Index::EstimatePostingSize(std::string_view term) const {
  auto snapshot = TakePostingSnapshot(term);
  return snapshot ? snapshot->Size() : 0;
}

}  // namespace mygramdb::index
