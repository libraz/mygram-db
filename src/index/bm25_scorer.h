/**
 * @file bm25_scorer.h
 * @brief BM25 relevance scoring for search results
 */

#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "storage/document_store.h"
#include "types/doc_id.h"

namespace mygramdb::index {

/**
 * @brief BM25 tuning parameters
 */
struct BM25Params {
  double k1 = 1.2;  ///< Term frequency saturation (higher = more TF influence)
  double b = 0.75;  ///< Document length normalization (0 = no normalization, 1 = full)
};

/**
 * @brief Document with BM25 score
 */
struct ScoredDoc {
  storage::DocId doc_id;
  double score = 0.0;
};

/**
 * @brief BM25 relevance scoring engine
 *
 * Computes BM25 scores at the search-term level (not n-gram level).
 * TF is computed at query time by counting search term occurrences
 * in each document's normalized text.
 */
class BM25Scorer {
 public:
  /**
   * @brief Compute Inverse Document Frequency
   * @param total_docs Total documents in corpus (N)
   * @param doc_freq Documents containing the term (df)
   * @return IDF value: ln((N - df + 0.5) / (df + 0.5) + 1)
   */
  static double ComputeIDF(uint64_t total_docs, uint64_t doc_freq);

  /**
   * @brief Count non-overlapping occurrences of a term in text
   * @param text Text to search in (normalized)
   * @param term Term to count occurrences of (normalized)
   * @return Number of non-overlapping occurrences
   */
  static uint32_t CountTermOccurrences(std::string_view text, std::string_view term);

  /**
   * @brief Compute BM25 scores for candidate documents
   *
   * For each candidate document:
   * 1. Retrieves normalized text from DocumentStore
   * 2. Counts term occurrences (TF) and document length
   * 3. Computes BM25 score using IDF, TF, and length normalization
   *
   * @param candidates Document IDs to score
   * @param search_terms Normalized search terms (original keywords, not n-grams)
   * @param term_doc_freqs Document frequency for each search term (parallel to search_terms)
   * @param doc_store DocumentStore for normalized text access
   * @param total_docs Total documents in corpus
   * @param avg_doc_length Average document length (code points)
   * @param params BM25 parameters (k1, b)
   * @return Scored documents (same order as candidates)
   */
  static std::vector<ScoredDoc> ScoreDocuments(const std::vector<storage::DocId>& candidates,
                                               const std::vector<std::string>& search_terms,
                                               const std::vector<uint64_t>& term_doc_freqs,
                                               const storage::DocumentStore& doc_store, uint64_t total_docs,
                                               double avg_doc_length, const BM25Params& params);
};

}  // namespace mygramdb::index
