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
#include <unordered_set>

#include "utils/string_utils.h"

namespace mygramdb {
namespace index {

Index::Index(int ngram_size, int kanji_ngram_size, double roaring_threshold)
    : ngram_size_(ngram_size),
      kanji_ngram_size_(kanji_ngram_size > 0 ? kanji_ngram_size : ngram_size),
      roaring_threshold_(roaring_threshold) {}

void Index::AddDocument(DocId doc_id, const std::string& text) {
  // Generate n-grams using hybrid mode
  std::vector<std::string> ngrams =
      utils::GenerateHybridNgrams(text, ngram_size_, kanji_ngram_size_);

  // Remove duplicates while preserving uniqueness
  std::unordered_set<std::string> unique_ngrams(ngrams.begin(), ngrams.end());

  // Add document to posting list for each unique n-gram
  for (const auto& ngram : unique_ngrams) {
    auto* posting = GetOrCreatePostingList(ngram);
    posting->Add(doc_id);
  }

  const char* mode = (ngram_size_ == 0) ? "hybrid" : "regular";
  spdlog::debug("Added document {} with {} unique {}-grams ({})", doc_id, unique_ngrams.size(),
                ngram_size_, mode);
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

    // Remove duplicates
    std::unordered_set<std::string> unique_ngrams(ngrams.begin(), ngrams.end());

    // Build term->docs mapping
    for (const auto& ngram : unique_ngrams) {
      term_to_docs[ngram].push_back(doc.doc_id);
    }
  }

  // Phase 2: Sort doc_ids for each term (enables batch insertion optimization)
  for (auto& [term, doc_ids] : term_to_docs) {
    std::sort(doc_ids.begin(), doc_ids.end());
  }

  // Phase 3: Add to posting lists (with locking, minimal lock time)
  // Use PostingList::AddBatch() for better performance
  std::scoped_lock<std::mutex> lock(postings_mutex_);

  for (const auto& [term, doc_ids] : term_to_docs) {
    auto* posting = GetOrCreatePostingList(term);
    posting->AddBatch(doc_ids);
  }

  spdlog::debug("Added batch of {} documents with {} unique terms", documents.size(),
                term_to_docs.size());
}

void Index::UpdateDocument(DocId doc_id, const std::string& old_text, const std::string& new_text) {
  // Generate n-grams for both texts
  std::vector<std::string> old_ngrams =
      utils::GenerateHybridNgrams(old_text, ngram_size_, kanji_ngram_size_);
  std::vector<std::string> new_ngrams =
      utils::GenerateHybridNgrams(new_text, ngram_size_, kanji_ngram_size_);

  std::unordered_set<std::string> old_set(old_ngrams.begin(), old_ngrams.end());
  std::unordered_set<std::string> new_set(new_ngrams.begin(), new_ngrams.end());

  // Remove doc from n-grams that are no longer present
  for (const auto& ngram : old_set) {
    if (new_set.find(ngram) == new_set.end()) {
      auto* posting = GetOrCreatePostingList(ngram);
      posting->Remove(doc_id);
    }
  }

  // Add doc to new n-grams
  for (const auto& ngram : new_set) {
    if (old_set.find(ngram) == old_set.end()) {
      auto* posting = GetOrCreatePostingList(ngram);
      posting->Add(doc_id);
    }
  }

  spdlog::debug("Updated document {}", doc_id);
}

void Index::RemoveDocument(DocId doc_id, const std::string& text) {
  // Generate n-grams
  std::vector<std::string> ngrams =
      utils::GenerateHybridNgrams(text, ngram_size_, kanji_ngram_size_);
  std::unordered_set<std::string> unique_ngrams(ngrams.begin(), ngrams.end());

  // Remove document from posting list for each n-gram
  for (const auto& ngram : unique_ngrams) {
    auto* posting = GetOrCreatePostingList(ngram);
    posting->Remove(doc_id);
  }

  spdlog::debug("Removed document {}", doc_id);
}

std::vector<DocId> Index::SearchAnd(const std::vector<std::string>& terms) const {
  std::scoped_lock lock(postings_mutex_);

  if (terms.empty()) {
    return {};
  }

  // Get posting list for first term
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
      break;  // Early termination
    }
  }

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
  std::scoped_lock lock(postings_mutex_);
  return SearchOrInternal(terms);
}

std::vector<DocId> Index::SearchNot(const std::vector<DocId>& all_docs,
                                    const std::vector<std::string>& terms) const {
  if (terms.empty()) {
    return all_docs;
  }

  std::scoped_lock lock(postings_mutex_);

  // Get union of all documents containing any of the NOT terms
  std::vector<DocId> excluded_docs = SearchOrInternal(terms);

  // Return set difference: all_docs - excluded_docs
  std::vector<DocId> result;
  std::set_difference(all_docs.begin(), all_docs.end(), excluded_docs.begin(), excluded_docs.end(),
                      std::back_inserter(result));

  return result;
}

uint64_t Index::Count(const std::string& term) const {
  std::scoped_lock lock(postings_mutex_);
  const auto* posting = GetPostingList(term);
  return (posting != nullptr) ? posting->Size() : 0;
}

size_t Index::MemoryUsage() const {
  std::scoped_lock lock(postings_mutex_);
  size_t total = 0;
  for (const auto& [term, posting] : term_postings_) {
    total += term.size();             // Term string
    total += posting->MemoryUsage();  // Posting list
  }
  return total;
}

Index::IndexStatistics Index::GetStatistics() const {
  std::scoped_lock lock(postings_mutex_);
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
  spdlog::info("Optimized index: {} terms, {} MB", term_postings_.size(),
               MemoryUsage() / (1024 * 1024));
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

  spdlog::info("Starting batch optimization: {} terms, batch_size={}", term_postings_.size(),
               batch_size);

  auto start_time = std::chrono::steady_clock::now();

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

      // Find the posting list (check if still exists)
      auto iterator = term_postings_.find(term);
      if (iterator == term_postings_.end()) {
        continue;  // Term was removed during optimization
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

    // Swap in the optimized batch (short lock duration)
    {
      std::scoped_lock lock(postings_mutex_);
      for (auto& [term, optimized] : optimized_batch) {
        // Check if term still exists before replacing
        auto iterator = term_postings_.find(term);
        if (iterator != term_postings_.end()) {
          iterator->second = std::move(optimized);
        }
      }
    }

    // Log progress every 10% or at the end
    size_t progress = ((batch_end) * 100) / total_terms;
    if (progress % 10 == 0 || batch_end == total_terms) {
      spdlog::info("Optimization progress: {}/{} terms ({}%)", batch_end, total_terms, progress);
    }
  }

  auto end_time = std::chrono::steady_clock::now();
  auto duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();

  spdlog::info(
      "Batch optimization completed: {} terms processed, "
      "{} strategy changes, {:.2f}s elapsed",
      total_terms, converted_count, static_cast<double>(duration) / 1000.0);

  return true;
}

void Index::Clear() {
  std::scoped_lock lock(postings_mutex_);
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
    ofs.write(reinterpret_cast<const char*>(&version), sizeof(version));

    // Write ngram_size
    auto ngram = static_cast<uint32_t>(ngram_size_);
    ofs.write(reinterpret_cast<const char*>(&ngram), sizeof(ngram));

    uint64_t term_count = 0;
    // Lock scope: iterate and serialize posting lists
    {
      std::scoped_lock lock(postings_mutex_);

      // Write term count
      term_count = term_postings_.size();
      ofs.write(reinterpret_cast<const char*>(&term_count), sizeof(term_count));

      // Write each term and its posting list
      for (const auto& [term, posting] : term_postings_) {
        // Write term length and term
        auto term_len = static_cast<uint32_t>(term.size());
        ofs.write(reinterpret_cast<const char*>(&term_len), sizeof(term_len));
        ofs.write(term.data(), static_cast<std::streamsize>(term_len));

        // Serialize posting list to buffer
        std::vector<uint8_t> posting_data;
        posting->Serialize(posting_data);

        // Write posting list size and data
        uint64_t posting_size = posting_data.size();
        ofs.write(reinterpret_cast<const char*>(&posting_size), sizeof(posting_size));
        ofs.write(reinterpret_cast<const char*>(posting_data.data()),
                  static_cast<std::streamsize>(posting_size));
      }
    }

    ofs.close();
    spdlog::info("Saved index to {}: {} terms, {} MB", filepath, term_count,
                 MemoryUsage() / (1024 * 1024));
    return true;
  } catch (const std::exception& e) {
    spdlog::error("Exception while saving index: {}", e.what());
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
    ifs.read(reinterpret_cast<char*>(&version), sizeof(version));
    if (version != 1) {
      spdlog::error("Unsupported index file version: {}", version);
      return false;
    }

    // Read ngram_size
    uint32_t ngram = 0;
    ifs.read(reinterpret_cast<char*>(&ngram), sizeof(ngram));
    if (static_cast<int>(ngram) != ngram_size_) {
      spdlog::warn("Index ngram_size mismatch: file={}, current={}", ngram, ngram_size_);
      // Continue anyway, but this might cause issues
    }

    // Read term count
    uint64_t term_count = 0;
    ifs.read(reinterpret_cast<char*>(&term_count), sizeof(term_count));

    // Load into a new map to minimize lock time
    std::unordered_map<std::string, std::unique_ptr<PostingList>> new_postings;

    // Read each term and its posting list
    for (uint64_t i = 0; i < term_count; ++i) {
      // Read term length and term
      uint32_t term_len = 0;
      ifs.read(reinterpret_cast<char*>(&term_len), sizeof(term_len));

      std::string term(term_len, '\0');
      ifs.read(term.data(), static_cast<std::streamsize>(term_len));

      // Read posting list size and data
      uint64_t posting_size = 0;
      ifs.read(reinterpret_cast<char*>(&posting_size), sizeof(posting_size));

      std::vector<uint8_t> posting_data(posting_size);
      ifs.read(reinterpret_cast<char*>(posting_data.data()),
               static_cast<std::streamsize>(posting_size));

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

    spdlog::info("Loaded index from {}: {} terms, {} MB", filepath, term_count,
                 MemoryUsage() / (1024 * 1024));
    return true;
  } catch (const std::exception& e) {
    spdlog::error("Exception while loading index: {}", e.what());
    return false;
  }
}

}  // namespace index
}  // namespace mygramdb
