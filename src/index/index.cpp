/**
 * @file index.cpp
 * @brief N-gram inverted index implementation
 */

#include "index/index.h"
#include "utils/string_utils.h"
#include <spdlog/spdlog.h>
#include <unordered_set>

namespace mygramdb {
namespace index {

Index::Index(int ngram_size, double roaring_threshold)
    : ngram_size_(ngram_size), roaring_threshold_(roaring_threshold) {}

void Index::AddDocument(DocId doc_id, const std::string& text) {
  // Generate n-grams
  auto ngrams = utils::GenerateNgrams(text, ngram_size_);

  // Remove duplicates while preserving uniqueness
  std::unordered_set<std::string> unique_ngrams(ngrams.begin(), ngrams.end());

  // Add document to posting list for each unique n-gram
  for (const auto& ngram : unique_ngrams) {
    auto* posting = GetOrCreatePostingList(ngram);
    posting->Add(doc_id);
  }

  spdlog::debug("Added document {} with {} unique {}-grams", doc_id,
                unique_ngrams.size(), ngram_size_);
}

void Index::UpdateDocument(DocId doc_id, const std::string& old_text,
                          const std::string& new_text) {
  // Generate n-grams for both texts
  auto old_ngrams = utils::GenerateNgrams(old_text, ngram_size_);
  auto new_ngrams = utils::GenerateNgrams(new_text, ngram_size_);

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
  auto ngrams = utils::GenerateNgrams(text, ngram_size_);
  std::unordered_set<std::string> unique_ngrams(ngrams.begin(), ngrams.end());

  // Remove document from posting list for each n-gram
  for (const auto& ngram : unique_ngrams) {
    auto* posting = GetOrCreatePostingList(ngram);
    posting->Remove(doc_id);
  }

  spdlog::debug("Removed document {}", doc_id);
}

std::vector<DocId> Index::SearchAnd(const std::vector<std::string>& terms) const {
  if (terms.empty()) return {};

  // Get posting list for first term
  const auto* first_posting = GetPostingList(terms[0]);
  if (first_posting == nullptr) return {};

  // Start with first term's documents
  auto result = first_posting->GetAll();

  // Intersect with each subsequent term
  for (size_t i = 1; i < terms.size(); ++i) {
    const auto* posting = GetPostingList(terms[i]);
    if (posting == nullptr) return {};  // No documents if any term is missing

    auto term_docs = posting->GetAll();
    std::vector<DocId> intersection;
    std::set_intersection(result.begin(), result.end(),
                         term_docs.begin(), term_docs.end(),
                         std::back_inserter(intersection));
    result = std::move(intersection);

    if (result.empty()) break;  // Early termination
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
  if (terms.empty()) return all_docs;

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
  return posting ? posting->Size() : 0;
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

PostingList* Index::GetOrCreatePostingList(const std::string& term) {
  auto it = term_postings_.find(term);
  if (it != term_postings_.end()) {
    return it->second.get();
  }

  // Create new posting list
  auto posting = std::make_unique<PostingList>(roaring_threshold_);
  auto* ptr = posting.get();
  term_postings_[term] = std::move(posting);
  return ptr;
}

const PostingList* Index::GetPostingList(const std::string& term) const {
  auto it = term_postings_.find(term);
  return it != term_postings_.end() ? it->second.get() : nullptr;
}

}  // namespace index
}  // namespace mygramdb
