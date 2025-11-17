/**
 * @file search_expression_test.cpp
 * @brief Unit tests for search expression parser
 */

#include "client/search_expression.h"

#include <gtest/gtest.h>

using namespace mygramdb::client;

/**
 * @brief Test simple required term with +
 */
TEST(SearchExpressionTest, SimpleRequiredTerm) {
  auto result = ParseSearchExpression("+golang");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 1);
  EXPECT_EQ(expr.required_terms[0], "golang");
  EXPECT_TRUE(expr.excluded_terms.empty());
  EXPECT_TRUE(expr.optional_terms.empty());
}

/**
 * @brief Test simple excluded term with -
 */
TEST(SearchExpressionTest, SimpleExcludedTerm) {
  auto result = ParseSearchExpression("-old");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_TRUE(expr.required_terms.empty());
  EXPECT_EQ(expr.excluded_terms.size(), 1);
  EXPECT_EQ(expr.excluded_terms[0], "old");
  EXPECT_TRUE(expr.optional_terms.empty());
}

/**
 * @brief Test single term (no prefix - treated as required with implicit AND)
 */
TEST(SearchExpressionTest, OptionalTerm) {
  auto result = ParseSearchExpression("tutorial");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 1);
  EXPECT_EQ(expr.required_terms[0], "tutorial");
  EXPECT_TRUE(expr.excluded_terms.empty());
}

/**
 * @brief Test multiple terms with implicit AND
 */
TEST(SearchExpressionTest, RequiredAndOptional) {
  auto result = ParseSearchExpression("golang tutorial");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 2);
  EXPECT_EQ(expr.required_terms[0], "golang");
  EXPECT_EQ(expr.required_terms[1], "tutorial");
  EXPECT_TRUE(expr.excluded_terms.empty());
}

/**
 * @brief Test required and excluded
 */
TEST(SearchExpressionTest, RequiredAndExcluded) {
  auto result = ParseSearchExpression("+golang -old");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 1);
  EXPECT_EQ(expr.required_terms[0], "golang");
  EXPECT_EQ(expr.excluded_terms.size(), 1);
  EXPECT_EQ(expr.excluded_terms[0], "old");
  EXPECT_TRUE(expr.optional_terms.empty());
}

/**
 * @brief Test multiple required terms
 */
TEST(SearchExpressionTest, MultipleRequired) {
  auto result = ParseSearchExpression("+golang +tutorial +2024");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 3);
  EXPECT_EQ(expr.required_terms[0], "golang");
  EXPECT_EQ(expr.required_terms[1], "tutorial");
  EXPECT_EQ(expr.required_terms[2], "2024");
}

/**
 * @brief Test OR expression
 */
TEST(SearchExpressionTest, OrExpression) {
  auto result = ParseSearchExpression("python OR ruby");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_TRUE(expr.HasComplexExpression());  // OR is a complex expression
  EXPECT_FALSE(expr.raw_expression.empty());
  EXPECT_TRUE(expr.required_terms.empty());
  EXPECT_TRUE(expr.excluded_terms.empty());
}

/**
 * @brief Test parenthesized expression
 */
TEST(SearchExpressionTest, ParenthesizedExpression) {
  auto result = ParseSearchExpression("(tutorial OR guide)");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_TRUE(expr.HasComplexExpression());
  EXPECT_FALSE(expr.raw_expression.empty());
}

/**
 * @brief Test required with parenthesized OR
 */
TEST(SearchExpressionTest, RequiredWithParenthesizedOr) {
  auto result = ParseSearchExpression("+golang +(tutorial OR guide)");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 2);
  EXPECT_EQ(expr.required_terms[0], "golang");
  EXPECT_TRUE(expr.HasComplexExpression());
}

/**
 * @brief Test complex expression
 */
TEST(SearchExpressionTest, ComplexExpression) {
  auto result = ParseSearchExpression("+golang +(tutorial OR guide) -old -deprecated");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 2);
  EXPECT_EQ(expr.required_terms[0], "golang");
  EXPECT_EQ(expr.excluded_terms.size(), 2);
  EXPECT_EQ(expr.excluded_terms[0], "old");
  EXPECT_EQ(expr.excluded_terms[1], "deprecated");
  EXPECT_TRUE(expr.HasComplexExpression());
}

/**
 * @brief Test ToQueryString with required terms
 */
TEST(SearchExpressionTest, ToQueryStringRequired) {
  auto result = ParseSearchExpression("+golang +tutorial");
  ASSERT_TRUE(result);

  auto& expr = *result;
  std::string query = expr.ToQueryString();
  EXPECT_EQ(query, "golang AND tutorial");
}

/**
 * @brief Test ToQueryString with excluded terms
 */
TEST(SearchExpressionTest, ToQueryStringExcluded) {
  auto result = ParseSearchExpression("+golang -old");
  ASSERT_TRUE(result);

  auto& expr = *result;
  std::string query = expr.ToQueryString();
  EXPECT_EQ(query, "golang AND NOT old");
}

/**
 * @brief Test ToQueryString with multiple terms (implicit AND)
 */
TEST(SearchExpressionTest, ToQueryStringOptional) {
  auto result = ParseSearchExpression("python ruby");
  ASSERT_TRUE(result);

  auto& expr = *result;
  std::string query = expr.ToQueryString();
  // Multiple terms without prefix become implicit AND
  EXPECT_EQ(query, "python AND ruby");
}

/**
 * @brief Test ConvertSearchExpression convenience function
 */
TEST(SearchExpressionTest, ConvertSearchExpression) {
  auto result = ConvertSearchExpression("+golang -old");
  ASSERT_TRUE(result);  // Success

  std::string query = *result;
  EXPECT_FALSE(query.empty());
  EXPECT_TRUE(query.find("golang") != std::string::npos);
  EXPECT_TRUE(query.find("NOT old") != std::string::npos);
}

/**
 * @brief Test SimplifySearchExpression
 */
TEST(SearchExpressionTest, SimplifySearchExpression) {
  std::string main_term;
  std::vector<std::string> and_terms;
  std::vector<std::string> not_terms;

  bool success = SimplifySearchExpression("golang tutorial -old", main_term, and_terms, not_terms);

  EXPECT_TRUE(success);
  EXPECT_EQ(main_term, "golang");
  EXPECT_EQ(and_terms.size(), 1);
  EXPECT_EQ(and_terms[0], "tutorial");
  EXPECT_EQ(not_terms.size(), 1);
  EXPECT_EQ(not_terms[0], "old");
}

/**
 * @brief Test empty expression
 */
TEST(SearchExpressionTest, EmptyExpression) {
  auto result = ParseSearchExpression("");
  ASSERT_TRUE(!result);

  std::string error = result.error().message();
  EXPECT_FALSE(error.empty());
}

/**
 * @brief Test invalid syntax - missing term after +
 */
TEST(SearchExpressionTest, InvalidMissingTermAfterPlus) {
  auto result = ParseSearchExpression("+");
  ASSERT_TRUE(!result);

  std::string error = result.error().message();
  EXPECT_TRUE(error.find("Expected term after") != std::string::npos || !error.empty());
}

/**
 * @brief Test invalid syntax - unbalanced parentheses
 */
TEST(SearchExpressionTest, InvalidUnbalancedParens) {
  auto result = ParseSearchExpression("(golang tutorial");
  ASSERT_TRUE(!result);

  std::string error = result.error().message();
  EXPECT_TRUE(error.find("Unbalanced") != std::string::npos || !error.empty());
}

/**
 * @brief Test whitespace handling
 */
TEST(SearchExpressionTest, WhitespaceHandling) {
  auto result = ParseSearchExpression("  +golang   -old   tutorial  ");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 2);
  EXPECT_EQ(expr.required_terms[0], "golang");
  EXPECT_EQ(expr.required_terms[1], "tutorial");
  EXPECT_EQ(expr.excluded_terms.size(), 1);
  EXPECT_EQ(expr.excluded_terms[0], "old");
}

/**
 * @brief Test Japanese/CJK terms
 */
TEST(SearchExpressionTest, JapaneseTerms) {
  auto result = ParseSearchExpression("+日本語 -古い チュートリアル");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 2);
  EXPECT_EQ(expr.required_terms[0], "日本語");
  EXPECT_EQ(expr.required_terms[1], "チュートリアル");
  EXPECT_EQ(expr.excluded_terms.size(), 1);
  EXPECT_EQ(expr.excluded_terms[0], "古い");
}

/**
 * @brief Test quoted phrase search
 */
TEST(SearchExpressionTest, QuotedPhrase) {
  auto result = ParseSearchExpression("\"machine learning\" tutorial");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 2);
  EXPECT_EQ(expr.required_terms[0], "\"machine learning\"");
  EXPECT_EQ(expr.required_terms[1], "tutorial");
}

/**
 * @brief Test quoted phrase with exclusion
 */
TEST(SearchExpressionTest, QuotedPhraseWithExclusion) {
  auto result = ParseSearchExpression("\"deep learning\" -tensorflow");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 1);
  EXPECT_EQ(expr.required_terms[0], "\"deep learning\"");
  EXPECT_EQ(expr.excluded_terms.size(), 1);
  EXPECT_EQ(expr.excluded_terms[0], "tensorflow");
}

/**
 * @brief Test quoted phrase in OR expression
 */
TEST(SearchExpressionTest, QuotedPhraseWithOr) {
  auto result = ParseSearchExpression("\"machine learning\" OR \"deep learning\"");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_TRUE(expr.HasComplexExpression());
  EXPECT_FALSE(expr.raw_expression.empty());
  EXPECT_TRUE(expr.raw_expression.find("\"machine learning\"") != std::string::npos);
  EXPECT_TRUE(expr.raw_expression.find("\"deep learning\"") != std::string::npos);
}

/**
 * @brief Test full-width space as delimiter
 */
TEST(SearchExpressionTest, FullWidthSpace) {
  // "golang　tutorial" with full-width space (U+3000)
  auto result = ParseSearchExpression("golang　tutorial");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 2);
  EXPECT_EQ(expr.required_terms[0], "golang");
  EXPECT_EQ(expr.required_terms[1], "tutorial");
}

/**
 * @brief Test mixed ASCII and full-width spaces
 */
TEST(SearchExpressionTest, MixedSpaces) {
  // "golang tutorial　日本語" with mixed spaces
  auto result = ParseSearchExpression("golang tutorial　日本語");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 3);
  EXPECT_EQ(expr.required_terms[0], "golang");
  EXPECT_EQ(expr.required_terms[1], "tutorial");
  EXPECT_EQ(expr.required_terms[2], "日本語");
}

/**
 * @brief Test converting quoted phrase to query string
 */
TEST(SearchExpressionTest, QuotedPhraseToQueryString) {
  auto result = ConvertSearchExpression("\"machine learning\" tutorial");
  ASSERT_TRUE(result);  // Success

  std::string query = *result;
  EXPECT_EQ(query, "\"machine learning\" AND tutorial");
}

/**
 * @brief Test emoji in search expression
 */
TEST(SearchExpressionTest, EmojiInExpression) {
  auto result = ParseSearchExpression("😀 tutorial");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 2);
  EXPECT_EQ(expr.required_terms[0], "😀");
  EXPECT_EQ(expr.required_terms[1], "tutorial");
}

/**
 * @brief Test multiple emojis
 */
TEST(SearchExpressionTest, MultipleEmojis) {
  auto result = ParseSearchExpression("😀 🎉 👍");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 3);
  EXPECT_EQ(expr.required_terms[0], "😀");
  EXPECT_EQ(expr.required_terms[1], "🎉");
  EXPECT_EQ(expr.required_terms[2], "👍");
}

/**
 * @brief Test emoji with prefix operators
 */
TEST(SearchExpressionTest, EmojiWithPrefixOperators) {
  auto result = ParseSearchExpression("+😀 -🎉");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 1);
  EXPECT_EQ(expr.required_terms[0], "😀");
  EXPECT_EQ(expr.excluded_terms.size(), 1);
  EXPECT_EQ(expr.excluded_terms[0], "🎉");
}

/**
 * @brief Test emoji in quoted phrase
 */
TEST(SearchExpressionTest, EmojiInQuotedPhrase) {
  auto result = ParseSearchExpression("\"Hello 😀 World\"");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 1);
  EXPECT_EQ(expr.required_terms[0], "\"Hello 😀 World\"");
}

/**
 * @brief Test emoji with OR expression
 */
TEST(SearchExpressionTest, EmojiWithOr) {
  auto result = ParseSearchExpression("😀 OR 🎉");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_TRUE(expr.HasComplexExpression());
  EXPECT_TRUE(expr.raw_expression.find("😀") != std::string::npos);
  EXPECT_TRUE(expr.raw_expression.find("🎉") != std::string::npos);
}

/**
 * @brief Test mixed emoji and Japanese text
 */
TEST(SearchExpressionTest, EmojiWithJapanese) {
  auto result = ParseSearchExpression("楽しい😀チュートリアル🎉");
  ASSERT_TRUE(result);

  auto& expr = *result;
  EXPECT_EQ(expr.required_terms.size(), 1);
  EXPECT_EQ(expr.required_terms[0], "楽しい😀チュートリアル🎉");
}

/**
 * @brief Test emoji to query string conversion
 */
TEST(SearchExpressionTest, EmojiToQueryString) {
  auto result = ConvertSearchExpression("😀 tutorial -🎉");
  ASSERT_TRUE(result);  // Success

  std::string query = *result;
  EXPECT_TRUE(query.find("😀") != std::string::npos);
  EXPECT_TRUE(query.find("tutorial") != std::string::npos);
  EXPECT_TRUE(query.find("NOT 🎉") != std::string::npos);
}
