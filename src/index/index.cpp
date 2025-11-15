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

namespace mygramdb::index {

Index::Index(int ngram_size, int kanji_ngram_size, double roaring_threshold)
    : ngram_size_(ngram_size),
      kanji_ngram_size_(kanji_ngram_size > 0 ? kanji_ngram_size : ngram_size),
      roaring_threshold_(roaring_threshold) {}

void Index::AddDocument(DocId doc_id, const std::string& text) {
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
  spdlog::debug("Added document {} with {} unique {}-grams ({})", doc_id, ngrams.size(), ngram_size_, mode);
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

void Index::UpdateDocument(DocId doc_id, const std::string& old_text, const std::string& new_text) {
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

  spdlog::debug("Updated document {}", doc_id);
}

void Index::RemoveDocument(DocId doc_id, const std::string& text) {
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

  spdlog::debug("Removed document {}", doc_id);
}

std::vector<DocId> Index::SearchAnd(const std::vector<std::string>& terms, size_t limit, bool reverse) const {
  // Acquire shared lock for read-only access (allows concurrent readers)
  std::shared_lock<std::shared_mutex> lock(postings_mutex_);

  if (terms.empty()) {
    return {};
  }

  // Optimization: Single term with limit and reverse
  // This is common for "ORDER BY primary_key DESC LIMIT N" queries
  if (terms.size() == 1 && limit > 0 && reverse) {
    const auto* posting = GetPostingList(terms[0]);
    if (posting == nullptr) {
      return {};
    }
    return posting->GetTopN(limit, true);
  }

  // NEW Optimization: Multi-term with limit and reverse (for multi-ngram queries)
  // Query planning: Use statistics to choose the best execution strategy
  if (terms.size() > 1 && limit > 0 && reverse) {
    // Step 1: Gather statistics (cheap: O(N) where N = number of terms)
    std::vector<std::pair<size_t, const PostingList*>> term_info;
    term_info.reserve(terms.size());

    for (const auto& term : terms) {
      const auto* posting = GetPostingList(term);
      if (posting == nullptr) {
        return {};  // No documents if any term is missing
      }
      term_info.emplace_back(posting->Size(), posting);
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
      spdlog::debug("Merge join: {} terms, selectivity={:.2f}, min={}, max={}, found={}", terms.size(), selectivity,
                    min_size, max_size, result.size());
      return result;
    }
    spdlog::debug("Using standard intersection: selectivity={:.2f}, min={}, max={}", selectivity, min_size, max_size);
    // Fall through to standard path
  }

  // Standard path: Get all documents from all terms and intersect
  const auto* first_posting = GetPostingList(terms[0]);
  if (first_posting == nullptr) {
    return {};
  }

  // Start with first term's documents
  auto result = first_posting->GetAll();

  // Intersect with each subsequent term
  for (size_t i = 1; i < terms.size(); ++i) {
    const auto* posting = GetPostingList(terms[i]);
    if (posting == nullptr) {
      return {};  // No documents if any term is missing
    }

    auto term_docs = posting->GetAll();
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
  // Acquire shared lock for read-only access (allows concurrent readers)
  std::shared_lock<std::shared_mutex> lock(postings_mutex_);
  return SearchOrInternal(terms);
}

std::vector<DocId> Index::SearchNot(const std::vector<DocId>& all_docs, const std::vector<std::string>& terms) const {
  if (terms.empty()) {
    return all_docs;
  }

  // Acquire shared lock for read-only access (allows concurrent readers)
  std::shared_lock<std::shared_mutex> lock(postings_mutex_);

  // Get union of all documents containing any of the NOT terms
  std::vector<DocId> excluded_docs = SearchOrInternal(terms);

  // Return set difference: all_docs - excluded_docs
  std::vector<DocId> result;
  std::set_difference(all_docs.begin(), all_docs.end(), excluded_docs.begin(), excluded_docs.end(),
                      std::back_inserter(result));

  return result;
}

uint64_t Index::Count(const std::string& term) const {
  // Acquire shared lock for read-only access (allows concurrent readers)
  std::shared_lock<std::shared_mutex> lock(postings_mutex_);
  const auto* posting = GetPostingList(term);
  return (posting != nullptr) ? posting->Size() : 0;
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
  for (auto& [term, posting] : term_postings_) {
    posting->Optimize(total_docs);
  }
  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  // 1024: Standard conversion factor for bytes to KB to MB
  spdlog::info("Optimized index: {} terms, {} MB", term_postings_.size(), MemoryUsage() / (1024 * 1024));
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
}

bool Index::OptimizeInBatches(uint64_t total_docs, size_t batch_size) {
  // Check if already optimizing
  bool expected = false;
  if (!is_optimizing_.compare_exchange_strong(expected, true)) {
    spdlog::warn("Optimization already in progress, ignoring request");
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

  spdlog::info("Starting batch optimization: {} terms, batch_size={}", term_postings_.size(), batch_size);

  auto start_time = std::chrono::steady_clock::now();

  // Acquire exclusive lock for the entire optimization operation
  // This blocks all concurrent writes (Add/Update/Remove) during optimization
  std::unique_lock<std::shared_mutex> lock(postings_mutex_);

  // Collect all term names to iterate over
  std::vector<std::string> terms;
  terms.reserve(term_postings_.size());
  for (const auto& [term, posting] : term_postings_) {
    (void)posting;  // Suppress unused variable warning
    terms.push_back(term);
  }

  size_t total_terms = terms.size();
  size_t converted_count = 0;

  // Process in batches
  for (size_t i = 0; i < total_terms; i += batch_size) {
    size_t batch_end = std::min(i + batch_size, total_terms);

    // Create optimized copies for this batch
    std::unordered_map<std::string, std::unique_ptr<PostingList>> optimized_batch;

    for (size_t j = i; j < batch_end; ++j) {
      const auto& term = terms[j];

      // Find the posting list
      auto iterator = term_postings_.find(term);
      if (iterator == term_postings_.end()) {
        continue;  // Term was removed
      }

      const auto& posting = iterator->second;
      auto old_strategy = posting->GetStrategy();

      // Clone and optimize
      auto optimized = posting->Clone(total_docs);

      // Track if strategy changed
      if (optimized->GetStrategy() != old_strategy) {
        converted_count++;
      }

      optimized_batch[term] = std::move(optimized);
    }

    // Swap in the optimized batch
    for (auto& [term, optimized] : optimized_batch) {
      auto iterator = term_postings_.find(term);
      if (iterator != term_postings_.end()) {
        iterator->second = std::move(optimized);
      }
    }

    // Log progress every 10% or at the end
    // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    // 100, 10: Standard percentage calculation values
    size_t progress = ((batch_end) * 100) / total_terms;
    if (progress % 10 == 0 || batch_end == total_terms) {
      // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
      spdlog::info("Optimization progress: {}/{} terms ({}%)", batch_end, total_terms, progress);
    }
  }

  auto end_time = std::chrono::steady_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

  // NOLINTBEGIN(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  // 1000.0: Standard conversion factor from milliseconds to seconds
  spdlog::info(
      "Batch optimization completed: {} terms processed, "
      "{} strategy changes, {:.2f}s elapsed",
      total_terms, converted_count, static_cast<double>(duration) / 1000.0);
  // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)

  return true;
}

void Index::Clear() {
  // Acquire exclusive lock for modifying posting lists
  std::unique_lock<std::shared_mutex> lock(postings_mutex_);
  term_postings_.clear();
  spdlog::info("Cleared index");
}

PostingList* Index::GetOrCreatePostingList(const std::string& term) {
  auto iterator = term_postings_.find(term);
  if (iterator != term_postings_.end()) {
    return iterator->second.get();
  }

  // Create new posting list
  auto posting = std::make_unique<PostingList>(roaring_threshold_);
  auto* ptr = posting.get();
  term_postings_[term] = std::move(posting);
  return ptr;
}

const PostingList* Index::GetPostingList(const std::string& term) const {
  // NOTE: This method assumes postings_mutex_ is already locked by the caller
  auto iterator = term_postings_.find(term);
  return iterator != term_postings_.end() ? iterator->second.get() : nullptr;
}

bool Index::SaveToFile(const std::string& filepath) const {
  try {
    std::ofstream ofs(filepath, std::ios::binary);
    if (!ofs) {
      spdlog::error("Failed to open file for writing: {}", filepath);
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
    spdlog::info("Saved index to {}: {} terms, {} MB", filepath, term_count, MemoryUsage() / (1024 * 1024));
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    return true;
  } catch (const std::exception& e) {
    spdlog::error("Exception while saving index: {}", e.what());
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
      spdlog::error("Stream error while saving index");
      return false;
    }

    spdlog::debug("Saved index to stream: {} terms", term_count);
    return true;
  } catch (const std::exception& e) {
    spdlog::error("Exception while saving index to stream: {}", e.what());
    return false;
  }
}

bool Index::LoadFromFile(const std::string& filepath) {
  try {
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs) {
      spdlog::error("Failed to open file for reading: {}", filepath);
      return false;
    }

    // Read and verify magic number
    std::array<char, 4> magic{};
    ifs.read(magic.data(), magic.size());
    if (std::memcmp(magic.data(), "MGIX", 4) != 0) {
      spdlog::error("Invalid index file format (bad magic number)");
      return false;
    }

    // Read version
    uint32_t version = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
    ifs.read(reinterpret_cast<char*>(&version), sizeof(version));
    if (version != 1) {
      spdlog::error("Unsupported index file version: {}", version);
      return false;
    }

    // Read ngram_size
    uint32_t ngram = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
    ifs.read(reinterpret_cast<char*>(&ngram), sizeof(ngram));
    if (static_cast<int>(ngram) != ngram_size_) {
      spdlog::warn("Index ngram_size mismatch: file={}, current={}", ngram, ngram_size_);
      // Continue anyway, but this might cause issues
    }

    // Read term count
    uint64_t term_count = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
    ifs.read(reinterpret_cast<char*>(&term_count), sizeof(term_count));

    // Load into a new map to minimize lock time
    std::unordered_map<std::string, std::unique_ptr<PostingList>> new_postings;

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
      auto posting = std::make_unique<PostingList>(roaring_threshold_);
      size_t offset = 0;
      if (!posting->Deserialize(posting_data, offset)) {
        spdlog::error("Failed to deserialize posting list for term: {}", term);
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
    spdlog::info("Loaded index from {}: {} terms, {} MB", filepath, term_count, MemoryUsage() / (1024 * 1024));
    // NOLINTEND(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
    return true;
  } catch (const std::exception& e) {
    spdlog::error("Exception while loading index: {}", e.what());
    return false;
  }
}

bool Index::LoadFromStream(std::istream& input_stream) {
  try {
    // Read and verify magic number
    std::array<char, 4> magic{};
    input_stream.read(magic.data(), magic.size());
    if (std::memcmp(magic.data(), "MGIX", 4) != 0) {
      spdlog::error("Invalid index stream format (bad magic number)");
      return false;
    }

    // Read version
    uint32_t version = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
    input_stream.read(reinterpret_cast<char*>(&version), sizeof(version));
    if (version != 1) {
      spdlog::error("Unsupported index stream version: {}", version);
      return false;
    }

    // Read ngram_size
    uint32_t ngram = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
    input_stream.read(reinterpret_cast<char*>(&ngram), sizeof(ngram));
    if (static_cast<int>(ngram) != ngram_size_) {
      spdlog::warn("Index ngram_size mismatch: stream={}, current={}", ngram, ngram_size_);
      // Continue anyway, but this might cause issues
    }

    // Read term count
    uint64_t term_count = 0;
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for binary I/O
    input_stream.read(reinterpret_cast<char*>(&term_count), sizeof(term_count));

    // Load into a new map to minimize lock time
    std::unordered_map<std::string, std::unique_ptr<PostingList>> new_postings;

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
      auto posting = std::make_unique<PostingList>(roaring_threshold_);
      size_t offset = 0;
      if (!posting->Deserialize(posting_data, offset)) {
        spdlog::error("Failed to deserialize posting list for term: {}", term);
        return false;
      }

      new_postings[term] = std::move(posting);
    }

    if (!input_stream.good()) {
      spdlog::error("Stream error while loading index");
      return false;
    }

    // Swap the loaded data in with minimal lock time
    {
      std::scoped_lock lock(postings_mutex_);
      term_postings_ = std::move(new_postings);
    }

    spdlog::debug("Loaded index from stream: {} terms", term_count);
    return true;
  } catch (const std::exception& e) {
    spdlog::error("Exception while loading index from stream: {}", e.what());
    return false;
  }
}

}  // namespace mygramdb::index
