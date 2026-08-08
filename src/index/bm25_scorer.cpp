/**
 * @file bm25_scorer.cpp
 * @brief BM25 relevance scoring implementation
 */

#include "index/bm25_scorer.h"

#include <cmath>

#include "utils/string_utils.h"

namespace mygramdb::index {

double BM25Scorer::ComputeIDF(uint64_t total_docs, uint64_t doc_freq) {
  if (total_docs == 0) {
    return 0.0;
  }
  // Clamp doc_freq to total_docs to avoid negative values
  if (doc_freq > total_docs) {
    doc_freq = total_docs;
  }
  auto n = static_cast<double>(total_docs);
  auto df = static_cast<double>(doc_freq);
  return std::log((n - df + 0.5) / (df + 0.5) + 1.0);
}

uint32_t BM25Scorer::CountTermOccurrences(std::string_view text, std::string_view term) {
  if (text.empty() || term.empty()) {
    return 0;
  }
  if (term.size() > text.size()) {
    return 0;
  }
  uint32_t count = 0;
  size_t pos = 0;
  while (pos <= text.size() - term.size()) {
    auto found = text.find(term, pos);
    if (found == std::string_view::npos) {
      break;
    }
    ++count;
    pos = found + term.size();  // Non-overlapping: advance past the match
  }
  return count;
}

mygram::utils::Expected<std::vector<ScoredDoc>, mygram::utils::Error> BM25Scorer::ScoreDocuments(
    const std::vector<storage::DocId>& candidates, const std::vector<std::string>& search_terms,
    const std::vector<uint64_t>& term_doc_freqs, const storage::DocumentStore& doc_store, uint64_t total_docs,
    double avg_doc_length, const BM25Params& params) {
  if (search_terms.size() != term_doc_freqs.size()) {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInvalidArgument,
                                 "BM25 search_terms and term_doc_freqs must have identical lengths"));
  }

  std::vector<ScoredDoc> results;
  results.reserve(candidates.size());

  // Pre-compute IDF for each term
  std::vector<double> idfs;
  idfs.reserve(search_terms.size());
  for (size_t i = 0; i < search_terms.size(); ++i) {
    idfs.push_back(ComputeIDF(total_docs, term_doc_freqs[i]));
  }

  // Ranking needs a score for every candidate, so the candidate set cannot be
  // truncated before scoring. Materializing all of their texts at once would
  // copy a large fraction of the corpus for a broad query, so they are visited
  // in bounded chunks instead.
  doc_store.VisitNormalizedTextsFor(candidates, storage::DocumentStore::kSelectedNormalizedTextChunkSize,
                                    [&](size_t /*index*/, storage::DocId doc_id, const std::string* text) {
                                      double score = 0.0;
                                      if (text != nullptr && !text->empty()) {
                                        auto doc_length = static_cast<double>(mygram::utils::CountCodePoints(*text));

                                        for (size_t i = 0; i < search_terms.size(); ++i) {
                                          auto tf = static_cast<double>(CountTermOccurrences(*text, search_terms[i]));
                                          if (tf > 0.0) {
                                            double length_norm =
                                                1.0 - params.b + params.b * doc_length / std::max(avg_doc_length, 1.0);
                                            double numerator = tf * (params.k1 + 1.0);
                                            double denominator = tf + params.k1 * length_norm;
                                            score += idfs[i] * numerator / denominator;
                                          }
                                        }
                                      }

                                      results.push_back({doc_id, score});
                                      return true;
                                    });

  if (results.size() != candidates.size()) {
    return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
        mygram::utils::ErrorCode::kInternalError, "DocumentStore visited an unexpected number of scoring candidates"));
  }

  return results;
}

}  // namespace mygramdb::index
