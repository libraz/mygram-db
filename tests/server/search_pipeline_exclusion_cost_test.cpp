/**
 * @file search_pipeline_exclusion_cost_test.cpp
 * @brief What an exclusion costs, written as a clause and as a boolean node.
 *
 * `a NOT b` and `a AND NOT b` select the same documents, so they should cost
 * the same. A boolean NOT is a complement, and evaluating it literally means
 * producing the whole corpus first; the clause form instead subtracts the
 * excluded documents from the matches it already has. The difference is only
 * visible on a corpus large enough for the complement to be expensive, so both
 * forms are run over the same fixture and their durations compared as a ratio.
 *
 * Timings are noisy, so the assertion is on agreement of results plus a ratio
 * with generous headroom rather than on any absolute duration.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <iomanip>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "server/search_pipeline.h"
#include "storage/document_store.h"

namespace mygramdb::server::search_pipeline {
namespace {

constexpr size_t kCorpusSize = 200000;
constexpr int kRepeats = 5;

double MedianOf(std::vector<double> samples) {
  std::sort(samples.begin(), samples.end());
  return samples[samples.size() / 2];
}

}  // namespace

class ExclusionCostTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    index_ = new index::Index(
        /*ngram_size=*/2, /*kanji_ngram_size=*/2,
        /*roaring_threshold=*/0.1, /*cross_boundary_ngrams=*/false,
        /*normalize_nfkc=*/true, /*normalize_width=*/"half", /*normalize_lower=*/true);
    doc_store_ = new storage::DocumentStore();

    // One document in ten mentions the excluded term; one in four mentions the
    // term being searched for, so neither side of the exclusion is degenerate.
    for (size_t i = 0; i < kCorpusSize; ++i) {
      std::string text = "record " + std::to_string(i);
      if (i % 4 == 0) {
        text += " tokyo";
      }
      if (i % 10 == 0) {
        text += " spam";
      }
      const std::string normalized = index_->NormalizeText(text);
      auto id = doc_store_->AddDocument("pk" + std::to_string(i), {}, normalized);
      ASSERT_TRUE(id.has_value());
      index_->AddDocument(*id, normalized);
    }
  }

  static void TearDownTestSuite() {
    delete doc_store_;
    delete index_;
    doc_store_ = nullptr;
    index_ = nullptr;
  }

  FullPipelineParams MakeParams() const {
    FullPipelineParams params;
    params.current_index = index_;
    params.current_doc_store = doc_store_;
    params.full_config = &config_;
    params.ngram_size = 2;
    params.kanji_ngram_size = 2;
    params.cross_boundary_ngrams = false;
    params.filter_threshold = 1000;
    params.primary_key_column = "id";
    return params;
  }

  static query::Query MakeQuery(const std::string& search_text) {
    query::Query query;
    query.type = query::QueryType::SEARCH;
    query.table = "test";
    query.search_text = search_text;
    query.limit = 100;
    return query;
  }

  config::Config config_;
  static index::Index* index_;
  static storage::DocumentStore* doc_store_;
};

index::Index* ExclusionCostTest::index_ = nullptr;
storage::DocumentStore* ExclusionCostTest::doc_store_ = nullptr;

/**
 * @brief A boolean NOT must cost what the equivalent clause costs.
 *
 * Both forms select "tokyo, excluding spam" over the same corpus. If evaluating
 * the boolean form materializes and orders every document, its cost tracks the
 * corpus rather than the match set and the ratio blows out.
 */
TEST_F(ExclusionCostTest, BooleanNotCostsWhatTheEquivalentClauseCosts) {
  auto params = MakeParams();

  auto clause_query = MakeQuery("tokyo");
  clause_query.not_terms = {"spam"};
  const auto boolean_query = MakeQuery("tokyo AND NOT spam");

  std::vector<storage::DocId> clause_results;
  std::vector<storage::DocId> boolean_results;
  std::vector<double> clause_ms;
  std::vector<double> boolean_ms;

  for (int i = 0; i < kRepeats; ++i) {
    auto start = std::chrono::steady_clock::now();
    auto clause_output = ExecuteFullPipeline(clause_query, params);
    auto end = std::chrono::steady_clock::now();
    ASSERT_TRUE(clause_output.has_value()) << clause_output.error().message();
    clause_ms.push_back(std::chrono::duration<double, std::milli>(end - start).count());
    clause_results = clause_output->results;

    start = std::chrono::steady_clock::now();
    auto boolean_output = ExecuteFullPipeline(boolean_query, params);
    end = std::chrono::steady_clock::now();
    ASSERT_TRUE(boolean_output.has_value()) << boolean_output.error().message();
    boolean_ms.push_back(std::chrono::duration<double, std::milli>(end - start).count());
    boolean_results = boolean_output->results;
  }

  const double clause_median = MedianOf(clause_ms);
  const double boolean_median = MedianOf(boolean_ms);

  std::cout << "\nExclusion over " << kCorpusSize << " documents (median of " << kRepeats << ")\n";
  std::cout << "  " << std::left << std::setw(24) << "tokyo NOT spam" << std::right << std::setw(10) << std::fixed
            << std::setprecision(3) << clause_median << " ms\n";
  std::cout << "  " << std::left << std::setw(24) << "tokyo AND NOT spam" << std::right << std::setw(10)
            << boolean_median << " ms\n";
  std::cout << "  ratio x" << std::setprecision(2) << (boolean_median / clause_median) << "\n";

  // Same query, same answer: the two forms must not diverge.
  EXPECT_EQ(clause_results, boolean_results);

  // Headroom for scheduling noise on a shared machine, but far below the gap a
  // whole-corpus materialization opens up.
  EXPECT_LT(boolean_median, clause_median * 4.0 + 1.0);
}

/**
 * @brief A top-level NOT still needs the corpus, and must stay correct.
 *
 * Nothing bounds "everything except spam" other than the corpus itself, so this
 * form is expected to be expensive. It is measured to keep the comparison above
 * honest about where the cost legitimately comes from.
 */
TEST_F(ExclusionCostTest, TopLevelNotSelectsTheComplement) {
  auto params = MakeParams();
  auto output = ExecuteFullPipeline(MakeQuery("NOT spam"), params);
  ASSERT_TRUE(output.has_value()) << output.error().message();

  auto excluded = ExecuteFullPipeline(MakeQuery("spam"), params);
  ASSERT_TRUE(excluded.has_value()) << excluded.error().message();

  EXPECT_EQ(output->results.size(), kCorpusSize - excluded->results.size());
}

}  // namespace mygramdb::server::search_pipeline
