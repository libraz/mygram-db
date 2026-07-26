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

TEST(SearchExpressionTest, OrSubstringInsideOrdinaryWordIsNotComplex) {
  const std::vector<std::string> ordinary_terms = {
      "ORANGE",
      "ORDER",
      "FLOOR",
      "orchestra",
  };

  for (const auto& term : ordinary_terms) {
    auto result = ParseSearchExpression(term);
    ASSERT_TRUE(result) << term;
    EXPECT_FALSE(result->HasComplexExpression()) << term;
    ASSERT_EQ(result->required_terms.size(), 1U) << term;
    EXPECT_EQ(result->required_terms.front(), term);
  }
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
  auto simplified = SimplifySearchExpression("golang tutorial -old");

  ASSERT_TRUE(simplified) << simplified.error().message();
  EXPECT_EQ(simplified->main_term, "golang");
  ASSERT_EQ(simplified->and_terms.size(), 1);
  EXPECT_EQ(simplified->and_terms[0], "tutorial");
  ASSERT_EQ(simplified->not_terms.size(), 1);
  EXPECT_EQ(simplified->not_terms[0], "old");
}

/**
 * @brief Test empty expression
 */
TEST(SearchExpressionTest, EmptyExpression) {
  auto result = ParseSearchExpression("");
  ASSERT_TRUE(!result);

  EXPECT_EQ(result.error().code(), mygramdb::utils::ErrorCode::kClientExpressionParseError);
  std::string error = result.error().message();
  EXPECT_FALSE(error.empty());
}

/**
 * @brief Test invalid syntax - missing term after +
 */
TEST(SearchExpressionTest, InvalidMissingTermAfterPlus) {
  auto result = ParseSearchExpression("+");
  ASSERT_TRUE(!result);

  EXPECT_EQ(result.error().code(), mygramdb::utils::ErrorCode::kClientExpressionParseError);
  std::string error = result.error().message();
  EXPECT_TRUE(error.find("Expected term after") != std::string::npos || !error.empty());
}

/**
 * @brief Test invalid syntax - unbalanced parentheses
 */
TEST(SearchExpressionTest, InvalidUnbalancedParens) {
  auto result = ParseSearchExpression("(golang tutorial");
  ASSERT_TRUE(!result);

  EXPECT_EQ(result.error().code(), mygramdb::utils::ErrorCode::kClientExpressionParseError);
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

/**
 * @brief Test SimplifySearchExpression with OR-only expression
 *
 * Regression test: previously this returned false because there were no
 * required terms, even though raw_expression contained the OR sub-expression.
 */
TEST(SearchExpressionTest, SimplifyOrOnly) {
  auto simplified = SimplifySearchExpression("python OR ruby");
  ASSERT_TRUE(simplified) << simplified.error().message();
  EXPECT_FALSE(simplified->main_term.empty());
  EXPECT_NE(simplified->main_term.find("python"), std::string::npos);
  EXPECT_NE(simplified->main_term.find("ruby"), std::string::npos);
  EXPECT_NE(simplified->main_term.find("OR"), std::string::npos);
  EXPECT_TRUE(simplified->and_terms.empty());
  EXPECT_TRUE(simplified->not_terms.empty());
}

/**
 * @brief Test SimplifySearchExpression with parenthesized OR expression
 */
TEST(SearchExpressionTest, SimplifyParenthesizedOnly) {
  auto simplified = SimplifySearchExpression("(python OR ruby)");
  ASSERT_TRUE(simplified) << simplified.error().message();
  EXPECT_FALSE(simplified->main_term.empty());
  // main_term should already be parenthesized.
  EXPECT_EQ(simplified->main_term.front(), '(');
  EXPECT_EQ(simplified->main_term.back(), ')');
  EXPECT_NE(simplified->main_term.find("python"), std::string::npos);
  EXPECT_NE(simplified->main_term.find("ruby"), std::string::npos);
}

TEST(SearchExpressionTest, ParenthesizedImplicitAndPreservesWhitespace) {
  auto converted = ConvertSearchExpression("+(machine learning)");
  ASSERT_TRUE(converted) << converted.error().message();
  EXPECT_EQ(*converted, "(machine learning)");

  converted = ConvertSearchExpression("+(a \"quoted phrase\" b)");
  ASSERT_TRUE(converted) << converted.error().message();
  EXPECT_EQ(*converted, "(a \"quoted phrase\" b)");
}

/**
 * @brief Test SimplifySearchExpression with mixed required + OR sub-expression
 */
TEST(SearchExpressionTest, SimplifyMixed) {
  auto simplified = SimplifySearchExpression("+golang (tutorial OR guide)");
  ASSERT_FALSE(simplified);
  EXPECT_EQ(simplified.error().code(), mygramdb::utils::ErrorCode::kClientExpressionParseError);
}

TEST(SearchExpressionTest, MalformedOrChainsAreRejected) {
  const std::vector<std::string> expressions = {
      "a OR -b",
      "a b OR",
      "foo OR",
  };

  for (const auto& expression : expressions) {
    auto result = ConvertSearchExpression(expression);
    EXPECT_FALSE(result.has_value()) << expression;
    if (!result.has_value()) {
      EXPECT_EQ(result.error().code(), mygramdb::utils::ErrorCode::kClientExpressionParseError) << expression;
    }
  }
}

TEST(SearchExpressionTest, PlusAndMinusInsideTermsAreLiteralText) {
  const std::vector<std::string> terms = {"COVID-19", "e-mail", "UTF-8", "C++"};
  for (const auto& term : terms) {
    auto parsed = ParseSearchExpression(term);
    ASSERT_TRUE(parsed) << term << ": " << parsed.error().message();
    ASSERT_EQ(parsed->required_terms.size(), 1U) << term;
    EXPECT_EQ(parsed->required_terms.front(), term);
    EXPECT_TRUE(parsed->excluded_terms.empty());

    auto converted = ConvertSearchExpression(term);
    ASSERT_TRUE(converted) << term;
    EXPECT_EQ(*converted, term);
  }
}

TEST(SearchExpressionTest, LeadingPlusAndMinusRemainUnaryOperators) {
  auto parsed = ParseSearchExpression("+COVID-19 -e-mail C++");
  ASSERT_TRUE(parsed) << parsed.error().message();
  ASSERT_EQ(parsed->required_terms.size(), 2U);
  EXPECT_EQ(parsed->required_terms[0], "COVID-19");
  EXPECT_EQ(parsed->required_terms[1], "C++");
  ASSERT_EQ(parsed->excluded_terms.size(), 1U);
  EXPECT_EQ(parsed->excluded_terms[0], "e-mail");
}

/**
 * @brief Test ToQueryString wraps OR sub-expressions in parentheses
 *
 * Doc-vs-behavior regression: docstring used to claim no parens were added,
 * but the implementation always wrapped raw_expression in parens. Lock the
 * actual behaviour with a test so future doc/code drift is caught.
 */
TEST(SearchExpressionTest, ToQueryStringWrapsOrInParens) {
  auto result = ConvertSearchExpression("python OR ruby");
  ASSERT_TRUE(result);

  std::string query = *result;
  EXPECT_EQ(query, "(python OR ruby)");
}
