/**
 * @file bm25_scorer_test.cpp
 * @brief Unit tests for BM25 relevance scoring
 */

#include "index/bm25_scorer.h"

#include <gtest/gtest.h>

#include <cmath>

#include "storage/document_store.h"

using mygramdb::index::BM25Params;
using mygramdb::index::BM25Scorer;
using mygramdb::index::ScoredDoc;
using mygramdb::storage::DocId;
using mygramdb::storage::DocumentStore;

// Test ComputeIDF
TEST(BM25ScorerTest, ComputeIDFBasic) {
  // N=100, df=10 => ln((100-10+0.5)/(10+0.5) + 1) = ln(90.5/10.5 + 1)
  double idf = BM25Scorer::ComputeIDF(100, 10);
  EXPECT_NEAR(idf, std::log(90.5 / 10.5 + 1.0), 1e-10);
}

TEST(BM25ScorerTest, ComputeIDFZeroDocFreq) {
  EXPECT_NEAR(BM25Scorer::ComputeIDF(100, 0), std::log((100.0 + 0.5) / 0.5 + 1.0), 1e-10);
  EXPECT_GT(BM25Scorer::ComputeIDF(100, 0), BM25Scorer::ComputeIDF(100, 1));
}

TEST(BM25ScorerTest, ComputeIDFZeroTotalDocs) {
  EXPECT_DOUBLE_EQ(BM25Scorer::ComputeIDF(0, 10), 0.0);
}

TEST(BM25ScorerTest, ComputeIDFDocFreqExceedsTotalDocs) {
  // Should clamp df to N
  double idf = BM25Scorer::ComputeIDF(10, 20);
  double expected = BM25Scorer::ComputeIDF(10, 10);
  EXPECT_DOUBLE_EQ(idf, expected);
}

TEST(BM25ScorerTest, ComputeIDFRareTerm) {
  // Rare terms should have higher IDF
  double idf_rare = BM25Scorer::ComputeIDF(1000, 1);
  double idf_common = BM25Scorer::ComputeIDF(1000, 500);
  EXPECT_GT(idf_rare, idf_common);
}

// Test CountTermOccurrences
TEST(BM25ScorerTest, CountTermOccurrencesSingle) {
  EXPECT_EQ(BM25Scorer::CountTermOccurrences("hello world", "hello"), 1);
}

TEST(BM25ScorerTest, CountTermOccurrencesMultiple) {
  EXPECT_EQ(BM25Scorer::CountTermOccurrences("hello hello hello", "hello"), 3);
}

TEST(BM25ScorerTest, CountTermOccurrencesNotFound) {
  EXPECT_EQ(BM25Scorer::CountTermOccurrences("hello world", "foo"), 0);
}

TEST(BM25ScorerTest, CountTermOccurrencesEmpty) {
  EXPECT_EQ(BM25Scorer::CountTermOccurrences("", "hello"), 0);
  EXPECT_EQ(BM25Scorer::CountTermOccurrences("hello", ""), 0);
}

TEST(BM25ScorerTest, CountTermOccurrencesNonOverlapping) {
  // "aa" in "aaa" should count 1 (non-overlapping)
  EXPECT_EQ(BM25Scorer::CountTermOccurrences("aaa", "aa"), 1);
  EXPECT_EQ(BM25Scorer::CountTermOccurrences("aaaa", "aa"), 2);
}

TEST(BM25ScorerTest, CountTermOccurrencesTermLongerThanText) {
  EXPECT_EQ(BM25Scorer::CountTermOccurrences("hi", "hello world test"), 0u);
}

TEST(BM25ScorerTest, CountTermOccurrencesCJK) {
  EXPECT_EQ(BM25Scorer::CountTermOccurrences(
                "\xe6\x9d\xb1\xe4\xba\xac\xe3\x81\xaf\xe6\x97\xa5\xe6\x9c\xac\xe3\x81\xae\xe9\xa6\x96\xe9\x83\xbd\xe3"
                "\x81\xa7\xe3\x81\x99\xe3\x80\x82\xe6\x9d\xb1\xe4\xba\xac\xe3\x82\xbf\xe3\x83\xaf\xe3\x83\xbc\xe3\x81"
                "\xaf\xe6\x9c\x89\xe5\x90\x8d\xe3\x81\xa7\xe3\x81\x99\xe3\x80\x82",
                "\xe6\x9d\xb1\xe4\xba\xac"),
            2);
}

// Test ScoreDocuments
TEST(BM25ScorerTest, ScoreDocumentsSingleDocSingleTerm) {
  DocumentStore store;
  auto result = store.AddDocument("doc1", {}, "hello world hello");
  ASSERT_TRUE(result.has_value());
  DocId doc_id = result.value();

  std::vector<DocId> candidates = {doc_id};
  std::vector<std::string> terms = {"hello"};
  std::vector<uint64_t> dfs = {1};
  BM25Params params{1.2, 0.75};
  double avgdl = 3.0;

  auto scored = BM25Scorer::ScoreDocuments(candidates, terms, dfs, store, 1, avgdl, params);
  ASSERT_EQ(scored.size(), 1);
  EXPECT_GT(scored[0].score, 0.0);
}

TEST(BM25ScorerTest, ScoreDocumentsHigherTFScoresHigher) {
  DocumentStore store;
  auto r1 = store.AddDocument("doc1", {}, "hello");
  auto r2 = store.AddDocument("doc2", {}, "hello hello hello");
  ASSERT_TRUE(r1.has_value());
  ASSERT_TRUE(r2.has_value());

  std::vector<DocId> candidates = {r1.value(), r2.value()};
  std::vector<std::string> terms = {"hello"};
  std::vector<uint64_t> dfs = {2};
  BM25Params params{1.2, 0.0};  // b=0 disables length normalization
  double avgdl = 10.0;

  auto scored = BM25Scorer::ScoreDocuments(candidates, terms, dfs, store, 2, avgdl, params);
  ASSERT_EQ(scored.size(), 2);
  EXPECT_LT(scored[0].score, scored[1].score);  // doc2 has higher TF
}

TEST(BM25ScorerTest, ScoreDocumentsMissingText) {
  DocumentStore store;
  auto r1 = store.AddDocument("doc1", {});
  ASSERT_TRUE(r1.has_value());

  std::vector<DocId> candidates = {r1.value()};
  std::vector<std::string> terms = {"hello"};
  std::vector<uint64_t> dfs = {1};
  BM25Params params;

  auto scored = BM25Scorer::ScoreDocuments(candidates, terms, dfs, store, 1, 10.0, params);
  ASSERT_EQ(scored.size(), 1);
  EXPECT_DOUBLE_EQ(scored[0].score, 0.0);
}

TEST(BM25ScorerTest, ScoreDocumentsEmptyCandidates) {
  DocumentStore store;
  std::vector<DocId> candidates;
  std::vector<std::string> terms = {"hello"};
  std::vector<uint64_t> dfs = {1};
  BM25Params params;

  auto scored = BM25Scorer::ScoreDocuments(candidates, terms, dfs, store, 0, 0.0, params);
  EXPECT_TRUE(scored.empty());
}

TEST(BM25ScorerTest, ScoreDocumentsK1ZeroIsPureIDF) {
  DocumentStore store;
  auto r1 = store.AddDocument("doc1", {}, "hello");
  auto r2 = store.AddDocument("doc2", {}, "hello hello hello hello hello");
  ASSERT_TRUE(r1.has_value());
  ASSERT_TRUE(r2.has_value());

  std::vector<DocId> candidates = {r1.value(), r2.value()};
  std::vector<std::string> terms = {"hello"};
  std::vector<uint64_t> dfs = {2};
  BM25Params params{0.0, 0.75};  // k1=0: TF has no influence
  double avgdl = 10.0;

  auto scored = BM25Scorer::ScoreDocuments(candidates, terms, dfs, store, 2, avgdl, params);
  ASSERT_EQ(scored.size(), 2);
  // With k1=0: numerator = tf*(0+1) = tf, denominator = tf + 0*(...) = tf
  // So score = IDF * tf/tf = IDF for any tf>0. Both should be equal.
  EXPECT_NEAR(scored[0].score, scored[1].score, 1e-10);
}
