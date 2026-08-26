/**
 * @file search_pipeline_not_verification_test.cpp
 * @brief NOT terms must be verified against stored text like positive terms
 *
 * A document whose n-grams happen to cover a NOT term without containing it as
 * a substring may only be excluded when the configured verification policy has
 * confirmed the term occurs, and the flat NOT clause must agree with the
 * equivalent boolean NOT node.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "server/search_pipeline.h"
#include "storage/document_store.h"

namespace mygramdb::server::search_pipeline {

namespace {

bool Contains(const std::vector<storage::DocId>& docs, storage::DocId id) {
  return std::find(docs.begin(), docs.end(), id) != docs.end();
}

}  // namespace

class NotTermVerificationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    index_ = std::make_unique<index::Index>(
        /*ngram_size=*/2, /*kanji_ngram_size=*/2,
        /*roaring_threshold=*/0.1, /*cross_boundary_ngrams=*/false,
        /*normalize_nfkc=*/true, /*normalize_width=*/"half", /*normalize_lower=*/true);
    doc_store_ = std::make_unique<storage::DocumentStore>();

    // Bigrams "ab" and "bc" both occur, the substring "abc" does not.
    ngram_only_doc_ = AddAndIndex("pk1", "crab bcx report");
    substring_doc_ = AddAndIndex("pk2", "abc report");
    unrelated_doc_ = AddAndIndex("pk3", "plain report");
  }

  storage::DocId AddAndIndex(const std::string& pk, const std::string& text) {
    const std::string normalized = index_->NormalizeText(text);
    auto id = doc_store_->AddDocument(pk, {}, normalized);
    EXPECT_TRUE(id.has_value());
    index_->AddDocument(*id, normalized);
    return *id;
  }

  FullPipelineParams MakeParams() {
    FullPipelineParams params;
    params.current_index = index_.get();
    params.current_doc_store = doc_store_.get();
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
  std::unique_ptr<index::Index> index_;
  std::unique_ptr<storage::DocumentStore> doc_store_;
  storage::DocId ngram_only_doc_ = 0;
  storage::DocId substring_doc_ = 0;
  storage::DocId unrelated_doc_ = 0;
};

TEST_F(NotTermVerificationTest, NotClauseKeepsDocumentWithoutTheSubstring) {
  config_.memory.verify_text = "all";

  auto query = MakeQuery("report");
  query.not_terms = {"abc"};

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << output.error().message();
  EXPECT_TRUE(Contains(output->results, ngram_only_doc_));
  EXPECT_FALSE(Contains(output->results, substring_doc_));
  EXPECT_TRUE(Contains(output->results, unrelated_doc_));
}

TEST_F(NotTermVerificationTest, BooleanNotNodeKeepsDocumentWithoutTheSubstring) {
  config_.memory.verify_text = "all";

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(MakeQuery("report AND NOT abc"), params);

  ASSERT_TRUE(output.has_value()) << output.error().message();
  EXPECT_TRUE(Contains(output->results, ngram_only_doc_));
  EXPECT_FALSE(Contains(output->results, substring_doc_));
  EXPECT_TRUE(Contains(output->results, unrelated_doc_));
}

TEST_F(NotTermVerificationTest, NotClauseAgreesWithBooleanNotNode) {
  config_.memory.verify_text = "all";

  auto not_clause_query = MakeQuery("report");
  not_clause_query.not_terms = {"abc"};

  auto params = MakeParams();
  auto not_clause_output = ExecuteFullPipeline(not_clause_query, params);
  ASSERT_TRUE(not_clause_output.has_value()) << not_clause_output.error().message();

  auto boolean_output = ExecuteFullPipeline(MakeQuery("report AND NOT abc"), params);
  ASSERT_TRUE(boolean_output.has_value()) << boolean_output.error().message();

  EXPECT_EQ(not_clause_output->results, boolean_output->results);
}

TEST_F(NotTermVerificationTest, VerifyTextOffKeepsNgramOnlyExclusion) {
  config_.memory.verify_text = "off";

  auto query = MakeQuery("report");
  query.not_terms = {"abc"};

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << output.error().message();
  EXPECT_FALSE(Contains(output->results, ngram_only_doc_));
  EXPECT_FALSE(Contains(output->results, substring_doc_));
  EXPECT_TRUE(Contains(output->results, unrelated_doc_));
}

}  // namespace mygramdb::server::search_pipeline
