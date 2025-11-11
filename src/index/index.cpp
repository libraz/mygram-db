/**
 * @file index.cpp
 * @brief N-gram inverted index implementation
 */

#include "index/index.h"
#include "utils/string_utils.h"

#include <algorithm>
#include <array>
#include <cstring>
#include <fstream>
#include <spdlog/spdlog.h>
#include <unordered_set>

namespace mygramdb {
namespace index {

Index::Index(int ngram_size, double roaring_threshold)
    : ngram_size_(ngram_size), roaring_threshold_(roaring_threshold) {}

void Index::AddDocument(DocId doc_id, const std::string& text) {
  // Generate n-grams (hybrid mode if ngram_size_ == 0)
  std::vector<std::string> ngrams;
  if (ngram_size_ == 0) {
    ngrams = utils::GenerateHybridNgrams(text);
  } else {
    ngrams = utils::GenerateNgrams(text, ngram_size_);
  }

  // Remove duplicates while preserving uniqueness
  std::unordered_set<std::string> unique_ngrams(ngrams.begin(), ngrams.end());

  // Add document to posting list for each unique n-gram
  for (const auto& ngram : unique_ngrams) {
    auto* posting = GetOrCreatePostingList(ngram);
    posting->Add(doc_id);
  }

  const char* mode = (ngram_size_ == 0) ? "hybrid" : "regular";
  spdlog::debug("Added document {} with {} unique {}-grams ({})", doc_id,
                unique_ngrams.size(), ngram_size_, mode);
}

void Index::UpdateDocument(DocId doc_id, const std::string& old_text,
                          const std::string& new_text) {
  // Generate n-grams for both texts (hybrid mode if ngram_size_ == 0)
  std::vector<std::string> old_ngrams;
  std::vector<std::string> new_ngrams;
  if (ngram_size_ == 0) {
    old_ngrams = utils::GenerateHybridNgrams(old_text);
    new_ngrams = utils::GenerateHybridNgrams(new_text);
  } else {
    old_ngrams = utils::GenerateNgrams(old_text, ngram_size_);
    new_ngrams = utils::GenerateNgrams(new_text, ngram_size_);
  }

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
  // Generate n-grams (hybrid mode if ngram_size_ == 0)
  std::vector<std::string> ngrams;
  if (ngram_size_ == 0) {
    ngrams = utils::GenerateHybridNgrams(text);
  } else {
    ngrams = utils::GenerateNgrams(text, ngram_size_);
  }
  std::unordered_set<std::string> unique_ngrams(ngrams.begin(), ngrams.end());

  // Remove document from posting list for each n-gram
  for (const auto& ngram : unique_ngrams) {
    auto* posting = GetOrCreatePostingList(ngram);
    posting->Remove(doc_id);
  }

  spdlog::debug("Removed document {}", doc_id);
}

std::vector<DocId> Index::SearchAnd(const std::vector<std::string>& terms) const {
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
    std::set_intersection(result.begin(), result.end(),
                         term_docs.begin(), term_docs.end(),
                         std::back_inserter(intersection));
    result = std::move(intersection);

    if (result.empty()) {
      break;  // Early termination
    }
  }

  return result;
}

std::vector<DocId> Index::SearchOr(const std::vector<std::string>& terms) const {
  std::vector<DocId> result;

  for (const auto& term : terms) {
    const auto* posting = GetPostingList(term);
    if (posting != nullptr) {
      auto term_docs = posting->GetAll();
      std::vector<DocId> union_result;
      std::set_union(result.begin(), result.end(),
                    term_docs.begin(), term_docs.end(),
                    std::back_inserter(union_result));
      result = std::move(union_result);
    }
  }

  return result;
}

std::vector<DocId> Index::SearchNot(const std::vector<DocId>& all_docs,
                                    const std::vector<std::string>& terms) const {
  if (terms.empty()) {
    return all_docs;
  }

  // Get union of all documents containing any of the NOT terms
  std::vector<DocId> excluded_docs = SearchOr(terms);

  // Return set difference: all_docs - excluded_docs
  std::vector<DocId> result;
  std::set_difference(all_docs.begin(), all_docs.end(),
                     excluded_docs.begin(), excluded_docs.end(),
                     std::back_inserter(result));

  return result;
}

uint64_t Index::Count(const std::string& term) const {
  const auto* posting = GetPostingList(term);
  return (posting != nullptr) ? posting->Size() : 0;
}

size_t Index::MemoryUsage() const {
  size_t total = 0;
  for (const auto& [term, posting] : term_postings_) {
    total += term.size();  // Term string
    total += posting->MemoryUsage();  // Posting list
  }
  return total;
}

void Index::Optimize(uint64_t total_docs) {
  for (auto& [term, posting] : term_postings_) {
    posting->Optimize(total_docs);
  }
  spdlog::info("Optimized index: {} terms, {} MB", term_postings_.size(),
               MemoryUsage() / (1024 * 1024));
}

void Index::Clear() {
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

    // Write term count
    uint64_t term_count = term_postings_.size();
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

    ofs.close();
    spdlog::info("Saved index to {}: {} terms, {} MB",
                 filepath, term_count, MemoryUsage() / (1024 * 1024));
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

    // Clear existing data
    term_postings_.clear();

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

      term_postings_[term] = std::move(posting);
    }

    ifs.close();
    spdlog::info("Loaded index from {}: {} terms, {} MB",
                 filepath, term_count, MemoryUsage() / (1024 * 1024));
    return true;
  } catch (const std::exception& e) {
    spdlog::error("Exception while loading index: {}", e.what());
    return false;
  }
}

}  // namespace index
}  // namespace mygramdb
