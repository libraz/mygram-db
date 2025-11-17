/**
 * @file query_ast_test.cpp
 * @brief Unit tests for query AST parser
 */

#include "query/query_ast.h"

#include <gtest/gtest.h>

#include "index/index.h"
#include "storage/document_store.h"
#include "utils/string_utils.h"

namespace mygramdb {
namespace query {

// ============================================================================
// Basic Term Tests
// ============================================================================

TEST(QueryASTTest, SingleTerm) {
  QueryASTParser parser;
  auto ast = parser.Parse("golang");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::TERM);
  EXPECT_EQ(ast->term, "golang");
  EXPECT_EQ(parser.GetError(), "");
}

TEST(QueryASTTest, QuotedTerm) {
  QueryASTParser parser;
  auto ast = parser.Parse("\"hello world\"");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::TERM);
  EXPECT_EQ(ast->term, "hello world");
}

TEST(QueryASTTest, SingleQuotedTerm) {
  QueryASTParser parser;
  auto ast = parser.Parse("'hello world'");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::TERM);
  EXPECT_EQ(ast->term, "hello world");
}

TEST(QueryASTTest, EscapeSequencesInQuotes) {
  QueryASTParser parser;
  auto ast = parser.Parse("\"hello\\nworld\\t!\"");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::TERM);
  EXPECT_EQ(ast->term, "hello\nworld\t!");
}

// ============================================================================
// AND Operator Tests
// ============================================================================

TEST(QueryASTTest, SimpleAnd) {
  QueryASTParser parser;
  auto ast = parser.Parse("golang AND python");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::AND);
  ASSERT_EQ(ast->children.size(), 2);

  EXPECT_EQ(ast->children[0]->type, NodeType::TERM);
  EXPECT_EQ(ast->children[0]->term, "golang");

  EXPECT_EQ(ast->children[1]->type, NodeType::TERM);
  EXPECT_EQ(ast->children[1]->term, "python");
}

TEST(QueryASTTest, MultipleAnd) {
  QueryASTParser parser;
  auto ast = parser.Parse("a AND b AND c");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::AND);

  // Should be left-associative: (a AND b) AND c
  EXPECT_EQ(ast->children[0]->type, NodeType::AND);
  EXPECT_EQ(ast->children[1]->type, NodeType::TERM);
  EXPECT_EQ(ast->children[1]->term, "c");

  auto left = ast->children[0].get();
  EXPECT_EQ(left->children[0]->type, NodeType::TERM);
  EXPECT_EQ(left->children[0]->term, "a");
  EXPECT_EQ(left->children[1]->type, NodeType::TERM);
  EXPECT_EQ(left->children[1]->term, "b");
}

// ============================================================================
// OR Operator Tests
// ============================================================================

TEST(QueryASTTest, SimpleOr) {
  QueryASTParser parser;
  auto ast = parser.Parse("golang OR python");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::OR);
  ASSERT_EQ(ast->children.size(), 2);

  EXPECT_EQ(ast->children[0]->type, NodeType::TERM);
  EXPECT_EQ(ast->children[0]->term, "golang");

  EXPECT_EQ(ast->children[1]->type, NodeType::TERM);
  EXPECT_EQ(ast->children[1]->term, "python");
}

TEST(QueryASTTest, MultipleOr) {
  QueryASTParser parser;
  auto ast = parser.Parse("a OR b OR c");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::OR);

  // Should be left-associative: (a OR b) OR c
  EXPECT_EQ(ast->children[0]->type, NodeType::OR);
  EXPECT_EQ(ast->children[1]->type, NodeType::TERM);
  EXPECT_EQ(ast->children[1]->term, "c");
}

// ============================================================================
// NOT Operator Tests
// ============================================================================

TEST(QueryASTTest, SimpleNot) {
  QueryASTParser parser;
  auto ast = parser.Parse("NOT spam");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::NOT);
  ASSERT_EQ(ast->children.size(), 1);

  EXPECT_EQ(ast->children[0]->type, NodeType::TERM);
  EXPECT_EQ(ast->children[0]->term, "spam");
}

TEST(QueryASTTest, DoubleNot) {
  QueryASTParser parser;
  auto ast = parser.Parse("NOT NOT term");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::NOT);
  EXPECT_EQ(ast->children[0]->type, NodeType::NOT);
  EXPECT_EQ(ast->children[0]->children[0]->type, NodeType::TERM);
  EXPECT_EQ(ast->children[0]->children[0]->term, "term");
}

// ============================================================================
// Operator Precedence Tests (NOT > AND > OR)
// ============================================================================

TEST(QueryASTTest, NotAndPrecedence) {
  // NOT has higher precedence than AND
  // "NOT a AND b" should be parsed as "(NOT a) AND b"
  QueryASTParser parser;
  auto ast = parser.Parse("NOT a AND b");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::AND);
  EXPECT_EQ(ast->children[0]->type, NodeType::NOT);
  EXPECT_EQ(ast->children[0]->children[0]->term, "a");
  EXPECT_EQ(ast->children[1]->type, NodeType::TERM);
  EXPECT_EQ(ast->children[1]->term, "b");
}

TEST(QueryASTTest, AndOrPrecedence) {
  // AND has higher precedence than OR
  // "a OR b AND c" should be parsed as "a OR (b AND c)"
  QueryASTParser parser;
  auto ast = parser.Parse("a OR b AND c");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::OR);
  EXPECT_EQ(ast->children[0]->type, NodeType::TERM);
  EXPECT_EQ(ast->children[0]->term, "a");

  EXPECT_EQ(ast->children[1]->type, NodeType::AND);
  EXPECT_EQ(ast->children[1]->children[0]->term, "b");
  EXPECT_EQ(ast->children[1]->children[1]->term, "c");
}

TEST(QueryASTTest, ComplexPrecedence) {
  // "a AND b OR c AND d" should be "(a AND b) OR (c AND d)"
  QueryASTParser parser;
  auto ast = parser.Parse("a AND b OR c AND d");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::OR);

  EXPECT_EQ(ast->children[0]->type, NodeType::AND);
  EXPECT_EQ(ast->children[0]->children[0]->term, "a");
  EXPECT_EQ(ast->children[0]->children[1]->term, "b");

  EXPECT_EQ(ast->children[1]->type, NodeType::AND);
  EXPECT_EQ(ast->children[1]->children[0]->term, "c");
  EXPECT_EQ(ast->children[1]->children[1]->term, "d");
}

// ============================================================================
// Parentheses Tests
// ============================================================================

TEST(QueryASTTest, SimpleParentheses) {
  QueryASTParser parser;
  auto ast = parser.Parse("(golang)");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::TERM);
  EXPECT_EQ(ast->term, "golang");
}

TEST(QueryASTTest, ParenthesesOverridePrecedence) {
  // "(a OR b) AND c" should respect parentheses
  QueryASTParser parser;
  auto ast = parser.Parse("(a OR b) AND c");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::AND);

  EXPECT_EQ(ast->children[0]->type, NodeType::OR);
  EXPECT_EQ(ast->children[0]->children[0]->term, "a");
  EXPECT_EQ(ast->children[0]->children[1]->term, "b");

  EXPECT_EQ(ast->children[1]->type, NodeType::TERM);
  EXPECT_EQ(ast->children[1]->term, "c");
}

TEST(QueryASTTest, NestedParentheses) {
  // "((a OR b) AND c) OR d"
  QueryASTParser parser;
  auto ast = parser.Parse("((a OR b) AND c) OR d");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::OR);

  auto left = ast->children[0].get();
  EXPECT_EQ(left->type, NodeType::AND);

  EXPECT_EQ(left->children[0]->type, NodeType::OR);
  EXPECT_EQ(left->children[0]->children[0]->term, "a");
  EXPECT_EQ(left->children[0]->children[1]->term, "b");

  EXPECT_EQ(left->children[1]->type, NodeType::TERM);
  EXPECT_EQ(left->children[1]->term, "c");

  EXPECT_EQ(ast->children[1]->type, NodeType::TERM);
  EXPECT_EQ(ast->children[1]->term, "d");
}

TEST(QueryASTTest, MultipleNestedParentheses) {
  // "(((term)))"
  QueryASTParser parser;
  auto ast = parser.Parse("(((term)))");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::TERM);
  EXPECT_EQ(ast->term, "term");
}

TEST(QueryASTTest, ComplexNestedExpression) {
  // "((a AND b) OR (c AND d)) AND NOT e"
  QueryASTParser parser;
  auto ast = parser.Parse("((a AND b) OR (c AND d)) AND NOT e");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::AND);

  // Left side: (a AND b) OR (c AND d)
  auto left = ast->children[0].get();
  EXPECT_EQ(left->type, NodeType::OR);
  EXPECT_EQ(left->children[0]->type, NodeType::AND);
  EXPECT_EQ(left->children[1]->type, NodeType::AND);

  // Right side: NOT e
  auto right = ast->children[1].get();
  EXPECT_EQ(right->type, NodeType::NOT);
  EXPECT_EQ(right->children[0]->term, "e");
}

// ============================================================================
// Error Cases
// ============================================================================

TEST(QueryASTTest, EmptyQuery) {
  QueryASTParser parser;
  auto ast = parser.Parse("");

  EXPECT_EQ(ast, nullptr);
  EXPECT_NE(parser.GetError(), "");
}

TEST(QueryASTTest, WhitespaceOnly) {
  QueryASTParser parser;
  auto ast = parser.Parse("   ");

  EXPECT_EQ(ast, nullptr);
  EXPECT_NE(parser.GetError(), "");
}

TEST(QueryASTTest, UnclosedParenthesis) {
  QueryASTParser parser;
  auto ast = parser.Parse("(a AND b");

  EXPECT_EQ(ast, nullptr);
  EXPECT_NE(parser.GetError(), "");
}

TEST(QueryASTTest, ExtraClosingParenthesis) {
  QueryASTParser parser;
  auto ast = parser.Parse("a AND b)");

  EXPECT_EQ(ast, nullptr);
  EXPECT_NE(parser.GetError(), "");
}

TEST(QueryASTTest, EmptyParentheses) {
  QueryASTParser parser;
  auto ast = parser.Parse("()");

  EXPECT_EQ(ast, nullptr);
  EXPECT_NE(parser.GetError(), "");
}

TEST(QueryASTTest, MismatchedParentheses) {
  QueryASTParser parser;
  auto ast = parser.Parse("((a AND b)");

  EXPECT_EQ(ast, nullptr);
  EXPECT_NE(parser.GetError(), "");
}

TEST(QueryASTTest, UnclosedQuote) {
  QueryASTParser parser;
  auto ast = parser.Parse("\"unclosed");

  EXPECT_EQ(ast, nullptr);
  EXPECT_NE(parser.GetError(), "");
}

TEST(QueryASTTest, OperatorWithoutOperand) {
  QueryASTParser parser;
  auto ast = parser.Parse("AND");

  EXPECT_EQ(ast, nullptr);
  EXPECT_NE(parser.GetError(), "");
}

TEST(QueryASTTest, NotWithoutOperand) {
  QueryASTParser parser;
  auto ast = parser.Parse("NOT");

  EXPECT_EQ(ast, nullptr);
  EXPECT_NE(parser.GetError(), "");
}

TEST(QueryASTTest, TrailingOperator) {
  QueryASTParser parser;
  auto ast = parser.Parse("a AND");

  EXPECT_EQ(ast, nullptr);
  EXPECT_NE(parser.GetError(), "");
}

// ============================================================================
// Case Insensitivity Tests
// ============================================================================

TEST(QueryASTTest, CaseInsensitiveAnd) {
  QueryASTParser parser;
  auto ast = parser.Parse("a and b");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::AND);
}

TEST(QueryASTTest, CaseInsensitiveOr) {
  QueryASTParser parser;
  auto ast = parser.Parse("a or b");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::OR);
}

TEST(QueryASTTest, CaseInsensitiveNot) {
  QueryASTParser parser;
  auto ast = parser.Parse("not a");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::NOT);
}

TEST(QueryASTTest, MixedCase) {
  QueryASTParser parser;
  auto ast = parser.Parse("a AnD b Or c AnD NoT d");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::OR);
}

// ============================================================================
// ToString Tests
// ============================================================================

TEST(QueryASTTest, ToStringSimpleTerm) {
  QueryASTParser parser;
  auto ast = parser.Parse("golang");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->ToString(), "TERM(\"golang\")");
}

TEST(QueryASTTest, ToStringAnd) {
  QueryASTParser parser;
  auto ast = parser.Parse("a AND b");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->ToString(), "AND(TERM(\"a\"), TERM(\"b\"))");
}

TEST(QueryASTTest, ToStringComplexExpression) {
  QueryASTParser parser;
  auto ast = parser.Parse("(a OR b) AND NOT c");

  ASSERT_NE(ast, nullptr);
  std::string result = ast->ToString();
  EXPECT_NE(result.find("AND"), std::string::npos);
  EXPECT_NE(result.find("OR"), std::string::npos);
  EXPECT_NE(result.find("NOT"), std::string::npos);
}

// ============================================================================
// Real-world Query Tests
// ============================================================================

TEST(QueryASTTest, RealWorldQuery1) {
  // "(golang OR python) AND tutorial AND NOT beginner"
  QueryASTParser parser;
  auto ast = parser.Parse("(golang OR python) AND tutorial AND NOT beginner");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::AND);
}

TEST(QueryASTTest, RealWorldQuery2) {
  // "database AND (mysql OR postgresql) AND NOT sqlite"
  QueryASTParser parser;
  auto ast = parser.Parse("database AND (mysql OR postgresql) AND NOT sqlite");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::AND);
}

TEST(QueryASTTest, RealWorldQuery3) {
  // "\"machine learning\" AND (python OR R) AND NOT tensorflow"
  QueryASTParser parser;
  auto ast = parser.Parse("\"machine learning\" AND (python OR R) AND NOT tensorflow");

  ASSERT_NE(ast, nullptr);
  EXPECT_EQ(ast->type, NodeType::AND);
  EXPECT_EQ(ast->children[0]->type, NodeType::AND);
}

// ============================================================================
// AST Evaluation Tests
// ============================================================================

// Simple evaluation test using unigrams
TEST(QueryASTEvaluationTest, SimpleEvaluation) {
  index::Index idx(1);  // unigram
  storage::DocumentStore doc_store;

  auto doc1 = *doc_store.AddDocument("1");
  auto doc2 = *doc_store.AddDocument("2");
  auto doc3 = *doc_store.AddDocument("3");

  idx.AddDocument(doc1, "abc");
  idx.AddDocument(doc2, "bcd");
  idx.AddDocument(doc3, "cde");

  QueryASTParser parser;

  // Test single term
  auto ast1 = parser.Parse("b");
  ASSERT_NE(ast1, nullptr);
  auto results1 = ast1->Evaluate(idx, doc_store);
  EXPECT_EQ(results1.size(), 2);  // doc1 and doc2

  // Test AND
  auto ast2 = parser.Parse("a AND b");
  ASSERT_NE(ast2, nullptr);
  auto results2 = ast2->Evaluate(idx, doc_store);
  EXPECT_EQ(results2.size(), 1);  // Only doc1

  // Test OR
  auto ast3 = parser.Parse("a OR e");
  ASSERT_NE(ast3, nullptr);
  auto results3 = ast3->Evaluate(idx, doc_store);
  EXPECT_EQ(results3.size(), 2);  // doc1 and doc3

  // Test NOT
  auto ast4 = parser.Parse("NOT a");
  ASSERT_NE(ast4, nullptr);
  auto results4 = ast4->Evaluate(idx, doc_store);
  EXPECT_EQ(results4.size(), 2);  // doc2 and doc3
}

TEST(QueryASTEvaluationTest, ComplexEvaluation) {
  index::Index idx(1);  // unigram
  storage::DocumentStore doc_store;

  auto doc1 = *doc_store.AddDocument("1");
  auto doc2 = *doc_store.AddDocument("2");
  auto doc3 = *doc_store.AddDocument("3");
  auto doc4 = *doc_store.AddDocument("4");

  idx.AddDocument(doc1, "abc");
  idx.AddDocument(doc2, "abd");
  idx.AddDocument(doc3, "cde");
  idx.AddDocument(doc4, "xyz");

  QueryASTParser parser;

  // Test: (a OR c) AND b
  auto ast = parser.Parse("(a OR c) AND b");
  ASSERT_NE(ast, nullptr);
  auto results = ast->Evaluate(idx, doc_store);
  EXPECT_EQ(results.size(), 2);  // doc1 and doc2 (have both 'a' or 'c' AND 'b')
}

/**
 * @brief Test 1-character terms with bigram index (should return no results)
 */
TEST(QueryASTEvaluationTest, SingleCharTermWithBigrams) {
  index::Index idx(2);  // bigram
  storage::DocumentStore doc_store;

  // Add documents
  auto doc1 = *doc_store.AddDocument("1");
  idx.AddDocument(doc1, "a");

  auto doc2 = *doc_store.AddDocument("2");
  idx.AddDocument(doc2, "ab");

  auto doc3 = *doc_store.AddDocument("3");
  idx.AddDocument(doc3, "abc");

  QueryASTParser parser;

  // Single 1-char term - should return empty (no bigrams from 'a')
  auto ast1 = parser.Parse("a");
  ASSERT_NE(ast1, nullptr);
  auto results1 = ast1->Evaluate(idx, doc_store);
  EXPECT_EQ(results1.size(), 0);  // No results

  // 1-char OR 2-char - should return results from 2-char term only
  auto ast2 = parser.Parse("a OR ab");
  ASSERT_NE(ast2, nullptr);
  auto results2 = ast2->Evaluate(idx, doc_store);
  EXPECT_EQ(results2.size(), 2);  // doc2 and doc3 (match "ab")

  // (1-char OR 3-char) AND 2-char
  // 'a' returns empty, 'abc' returns doc3, so OR = {doc3}
  // 'ab' returns {doc2, doc3}, so AND = {doc3}
  auto ast3 = parser.Parse("(a OR abc) AND ab");
  ASSERT_NE(ast3, nullptr);
  auto results3 = ast3->Evaluate(idx, doc_store);
  EXPECT_EQ(results3.size(), 1);  // Only doc3

  // 1-char AND 2-char - should return empty (no bigrams from 'a')
  auto ast4 = parser.Parse("a AND ab");
  ASSERT_NE(ast4, nullptr);
  auto results4 = ast4->Evaluate(idx, doc_store);
  EXPECT_EQ(results4.size(), 0);  // No results

  // NOT 1-char - should return all documents (NOT empty = all)
  auto ast5 = parser.Parse("NOT a");
  ASSERT_NE(ast5, nullptr);
  auto results5 = ast5->Evaluate(idx, doc_store);
  EXPECT_EQ(results5.size(), 3);  // All documents (NOT empty = all)
}

// ============================================================================
// Japanese/CJK Text Normalization Tests
// ============================================================================

/**
 * @brief Test Japanese keyword search with proper text normalization
 *
 * This test verifies the fix for the Japanese search bug where text
 * normalization parameters differed between indexing and querying.
 * The bug caused Japanese keywords to return 0 results.
 *
 * Fix: Ensure both index and query use:
 * - NormalizeText(text, true, "keep", true)
 * - GenerateHybridNgrams (not GenerateNgrams)
 */
TEST(QueryASTEvaluationTest, JapaneseTextNormalization) {
  // Use hybrid mode: ASCII bigram (2), CJK unigram (1)
  index::Index idx(2, 1);
  storage::DocumentStore doc_store;

  // Add documents with Japanese text (normalized before indexing)
  auto doc1 = *doc_store.AddDocument("1");
  idx.AddDocument(doc1, utils::NormalizeText("二次創作", true, "keep", true));  // "Derivative work"

  auto doc2 = *doc_store.AddDocument("2");
  idx.AddDocument(doc2, utils::NormalizeText("東方Project", true, "keep", true));  // "Touhou Project"

  auto doc3 = *doc_store.AddDocument("3");
  idx.AddDocument(doc3, utils::NormalizeText("艦これ", true, "keep", true));  // "KanColle"

  auto doc4 = *doc_store.AddDocument("4");
  idx.AddDocument(doc4, utils::NormalizeText("test", true, "keep", true));  // English control

  QueryASTParser parser;

  // Test Japanese keyword "二次" (2-char)
  auto ast1 = parser.Parse("二次");
  ASSERT_NE(ast1, nullptr);
  auto results1 = ast1->Evaluate(idx, doc_store);
  EXPECT_EQ(results1.size(), 1);  // Should match doc1
  EXPECT_EQ(results1[0], doc1);

  // Test Japanese keyword "東方" (2-char)
  auto ast2 = parser.Parse("東方");
  ASSERT_NE(ast2, nullptr);
  auto results2 = ast2->Evaluate(idx, doc_store);
  EXPECT_EQ(results2.size(), 1);  // Should match doc2
  EXPECT_EQ(results2[0], doc2);

  // Test Japanese keyword "艦これ" (3-char, should use unigrams)
  auto ast3 = parser.Parse("艦これ");
  ASSERT_NE(ast3, nullptr);
  auto results3 = ast3->Evaluate(idx, doc_store);
  EXPECT_EQ(results3.size(), 1);  // Should match doc3
  EXPECT_EQ(results3[0], doc3);

  // Test English still works
  auto ast4 = parser.Parse("test");
  ASSERT_NE(ast4, nullptr);
  auto results4 = ast4->Evaluate(idx, doc_store);
  EXPECT_EQ(results4.size(), 1);  // Should match doc4
  EXPECT_EQ(results4[0], doc4);

  // Test mixed Japanese/English query
  auto ast5 = parser.Parse("東方 OR test");
  ASSERT_NE(ast5, nullptr);
  auto results5 = ast5->Evaluate(idx, doc_store);
  EXPECT_EQ(results5.size(), 2);  // Should match doc2 and doc4
}

/**
 * @brief Test that hybrid ngrams are used for mixed CJK/ASCII text
 */
TEST(QueryASTEvaluationTest, HybridNgramConsistency) {
  // ASCII bigram (2), CJK unigram (1)
  index::Index idx(2, 1);
  storage::DocumentStore doc_store;

  // Add document with mixed text (normalized before indexing)
  auto doc1 = *doc_store.AddDocument("1");
  idx.AddDocument(doc1, utils::NormalizeText("東方project", true, "keep", true));  // Mixed: CJK + ASCII

  QueryASTParser parser;

  // Search for CJK part
  auto ast1 = parser.Parse("東方");
  ASSERT_NE(ast1, nullptr);
  auto results1 = ast1->Evaluate(idx, doc_store);
  EXPECT_EQ(results1.size(), 1);  // Should find doc1

  // Search for ASCII part (bigram "pr" from "project")
  auto ast2 = parser.Parse("pr");
  ASSERT_NE(ast2, nullptr);
  auto results2 = ast2->Evaluate(idx, doc_store);
  EXPECT_EQ(results2.size(), 1);  // Should find doc1

  // Combined search
  auto ast3 = parser.Parse("東方 AND pr");
  ASSERT_NE(ast3, nullptr);
  auto results3 = ast3->Evaluate(idx, doc_store);
  EXPECT_EQ(results3.size(), 1);  // Should find doc1
}

/**
 * @brief Test normalization parameter consistency
 *
 * This specifically tests that the same normalization parameters
 * (nfkc=true, width="keep", lower=true) are used in both indexing
 * and query evaluation.
 */
TEST(QueryASTEvaluationTest, NormalizationParameterConsistency) {
  // Use default configuration from production
  index::Index idx(2, 1);
  storage::DocumentStore doc_store;

  // Add documents with text that requires normalization
  // NOTE: In production, text is normalized before being added to index (snapshot_builder.cpp)
  // We must do the same in tests
  auto doc1 = *doc_store.AddDocument("1");
  std::string normalized1 = utils::NormalizeText("Test", true, "keep", true);  // -> "test"
  idx.AddDocument(doc1, normalized1);

  auto doc2 = *doc_store.AddDocument("2");
  std::string normalized2 = utils::NormalizeText("テスト", true, "keep", true);  // Full-width katakana
  idx.AddDocument(doc2, normalized2);

  auto doc3 = *doc_store.AddDocument("3");
  std::string normalized3 = utils::NormalizeText("ﾃｽﾄ", true, "keep", true);  // Half-width katakana
  idx.AddDocument(doc3, normalized3);

  QueryASTParser parser;

  // Search with lowercase (should match doc1 due to lower=true)
  auto ast1 = parser.Parse("test");
  ASSERT_NE(ast1, nullptr);
  auto results1 = ast1->Evaluate(idx, doc_store);
  EXPECT_EQ(results1.size(), 1);  // Should match doc1

  // Both full-width and half-width katakana are normalized to same form with nfkc=true
  // NFKC converts half-width katakana to full-width, so both queries should match both docs
  auto ast2 = parser.Parse("テスト");
  ASSERT_NE(ast2, nullptr);
  auto results2 = ast2->Evaluate(idx, doc_store);
  EXPECT_EQ(results2.size(), 2);  // Should match both doc2 and doc3 (NFKC normalizes to same form)

  auto ast3 = parser.Parse("ﾃｽﾄ");
  ASSERT_NE(ast3, nullptr);
  auto results3 = ast3->Evaluate(idx, doc_store);
  EXPECT_EQ(results3.size(), 2);  // Should match both doc2 and doc3 (NFKC normalizes to same form)
}

}  // namespace query
}  // namespace mygramdb
