/**
 * @file query_normalizer_test.cpp
 * @brief Unit tests for QueryNormalizer
 */

#include "query/query_normalizer.h"

#include <gtest/gtest.h>

#include "query/query_parser.h"

namespace mygramdb::cache {

/**
 * @brief Test whitespace normalization
 */
TEST(QueryNormalizerTest, WhitespaceNormalization) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "hello   world";
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "hello world";
  query2.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  // Multiple spaces should be normalized to single space
  EXPECT_EQ(normalized1, normalized2);
}

/**
 * @brief Test tab character normalization
 */
TEST(QueryNormalizerTest, TabNormalization) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "hello\t\tworld";  // Tabs
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "hello world";  // Single space
  query2.limit = 100;

  query::Query query3;
  query3.type = query::QueryType::SEARCH;
  query3.table = "posts";
  query3.search_text = "hello \t world";  // Mixed space and tab
  query3.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);
  std::string normalized3 = QueryNormalizer::Normalize(query3);

  // Tabs should be normalized to single space
  EXPECT_EQ(normalized1, normalized2);
  EXPECT_EQ(normalized2, normalized3);
}

/**
 * @brief Test full-width space (U+3000) normalization
 */
TEST(QueryNormalizerTest, FullWidthSpaceNormalization) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "hello　　world";  // Full-width spaces (U+3000)
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "hello world";  // Half-width space
  query2.limit = 100;

  query::Query query3;
  query3.type = query::QueryType::SEARCH;
  query3.table = "posts";
  query3.search_text = "hello　world";  // Single full-width space
  query3.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);
  std::string normalized3 = QueryNormalizer::Normalize(query3);

  // Full-width spaces should be normalized to single half-width space
  EXPECT_EQ(normalized1, normalized2);
  EXPECT_EQ(normalized2, normalized3);
}

/**
 * @brief Test mixed whitespace normalization
 */
TEST(QueryNormalizerTest, MixedWhitespaceNormalization) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "  hello \t　world  \n";  // Leading/trailing spaces, tab, full-width space, newline
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "hello world";
  query2.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  // All whitespace types should be normalized and trimmed
  EXPECT_EQ(normalized1, normalized2);
}

/**
 * @brief Test filter ordering normalization
 */
TEST(QueryNormalizerTest, FilterOrdering) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "test";
  query1.filters = {
      {"user_id", query::FilterOp::EQ, "123"},
      {"status", query::FilterOp::EQ, "active"},
  };
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "test";
  query2.filters = {
      {"status", query::FilterOp::EQ, "active"},
      {"user_id", query::FilterOp::EQ, "123"},
  };
  query2.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  // Different filter order should produce same normalized query
  EXPECT_EQ(normalized1, normalized2);
}

/**
 * @brief Test default limit uses actual value (different values)
 */
TEST(QueryNormalizerTest, DefaultLimitDifferent) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "test";
  query1.limit = 100;
  query1.limit_explicit = false;  // Default (set by api.default_limit)

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "test";
  query2.limit = 50;
  query2.limit_explicit = false;  // Default (different api.default_limit)

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  // Even with default limits, different actual limit values should produce different cache keys
  // This prevents cache hits from returning incorrect number of results
  EXPECT_NE(normalized1, normalized2);
}

/**
 * @brief Test default limit uses actual value (same values)
 */
TEST(QueryNormalizerTest, DefaultLimitSame) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "test";
  query1.limit = 50;
  query1.limit_explicit = false;  // Default (set by api.default_limit)

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "test";
  query2.limit = 50;
  query2.limit_explicit = false;  // Default (same api.default_limit)

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  // Same limit value should produce same cache key regardless of limit_explicit flag
  EXPECT_EQ(normalized1, normalized2);
}

/**
 * @brief Test explicit limit preservation
 */
TEST(QueryNormalizerTest, ExplicitLimit) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "test";
  query1.limit = 100;
  query1.limit_explicit = true;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "test";
  query2.limit = 50;
  query2.limit_explicit = true;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  // Explicit limits should be preserved and differ
  EXPECT_NE(normalized1, normalized2);
}

/**
 * @brief Test SORT clause normalization
 */
TEST(QueryNormalizerTest, SortClause) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "test";
  query1.order_by = query::OrderByClause{"created_at", query::SortOrder::DESC};
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "test";
  query2.order_by = query::OrderByClause{"created_at", query::SortOrder::ASC};
  query2.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  // Different SORT order should produce different normalized queries
  EXPECT_NE(normalized1, normalized2);
}

/**
 * @brief Test AND terms normalization
 */
TEST(QueryNormalizerTest, AndTerms) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "golang";
  query1.and_terms = {"programming", "tutorial"};
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "golang";
  query2.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  // Queries with different AND terms should differ
  EXPECT_NE(normalized1, normalized2);
}

/**
 * @brief Test AND terms ordering normalization
 */
TEST(QueryNormalizerTest, AndTermsOrdering) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "golang";
  query1.and_terms = {"programming", "tutorial", "beginner"};
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "golang";
  query2.and_terms = {"tutorial", "beginner", "programming"};
  query2.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  // Different AND term order should produce same normalized query (sorted alphabetically)
  EXPECT_EQ(normalized1, normalized2);
}

/**
 * @brief Test NOT terms ordering normalization
 */
TEST(QueryNormalizerTest, NotTermsOrdering) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "golang";
  query1.not_terms = {"deprecated", "old", "archived"};
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "golang";
  query2.not_terms = {"old", "archived", "deprecated"};
  query2.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  // Different NOT term order should produce same normalized query (sorted alphabetically)
  EXPECT_EQ(normalized1, normalized2);
}

/**
 * @brief Test table name case insensitivity
 */
TEST(QueryNormalizerTest, TableNameCaseInsensitive) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "Posts";
  query1.search_text = "test";
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "test";
  query2.limit = 100;

  query::Query query3;
  query3.type = query::QueryType::SEARCH;
  query3.table = "POSTS";
  query3.search_text = "test";
  query3.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);
  std::string normalized3 = QueryNormalizer::Normalize(query3);

  // Different table name case should produce same normalized query (lowercase)
  EXPECT_EQ(normalized1, normalized2);
  EXPECT_EQ(normalized2, normalized3);
}

/**
 * @brief Test empty search text
 */
TEST(QueryNormalizerTest, EmptySearchText) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "";
  query1.filters = {{"status", query::FilterOp::EQ, "active"}};
  query1.limit = 100;

  std::string normalized = QueryNormalizer::Normalize(query1);

  // Should produce valid normalized query without search text
  EXPECT_FALSE(normalized.empty());
  EXPECT_NE(normalized.find("SEARCH posts"), std::string::npos);
  EXPECT_NE(normalized.find("FILTER status = active"), std::string::npos);
}

/**
 * @brief Test Unicode characters in search text
 */
TEST(QueryNormalizerTest, UnicodeSearchText) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "日本語検索";
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "日本語検索";
  query2.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  // Same Unicode text should produce same normalized query
  EXPECT_EQ(normalized1, normalized2);
  EXPECT_NE(normalized1.find("日本語検索"), std::string::npos);
}

/**
 * @brief Test special characters in filter values
 */
TEST(QueryNormalizerTest, SpecialCharactersInFilterValues) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "test";
  query1.filters = {{"title", query::FilterOp::EQ, "LIMIT 100"}};
  query1.limit = 100;

  std::string normalized = QueryNormalizer::Normalize(query1);

  // Should handle filter values containing keywords
  EXPECT_FALSE(normalized.empty());
  EXPECT_NE(normalized.find("FILTER title = LIMIT 100"), std::string::npos);
}

/**
 * @brief Test COUNT query normalization
 */
TEST(QueryNormalizerTest, CountQuery) {
  query::Query query1;
  query1.type = query::QueryType::COUNT;
  query1.table = "posts";
  query1.search_text = "golang";
  query1.limit = 100;

  std::string normalized = QueryNormalizer::Normalize(query1);

  // COUNT queries should start with "COUNT"
  EXPECT_FALSE(normalized.empty());
  EXPECT_EQ(normalized.find("COUNT"), 0);
}

/**
 * @brief Test non-cacheable query types
 */
TEST(QueryNormalizerTest, NonCacheableQuery) {
  query::Query query1;
  query1.type = query::QueryType::GET;
  query1.table = "posts";
  query1.primary_key = "123";

  std::string normalized = QueryNormalizer::Normalize(query1);

  // GET queries should return empty string (not cacheable)
  EXPECT_TRUE(normalized.empty());
}

/**
 * @brief Test very long normalized query
 */
TEST(QueryNormalizerTest, LongNormalizedQuery) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "golang programming tutorial for beginners with examples";

  // Add many AND terms
  for (int i = 0; i < 20; ++i) {
    query1.and_terms.push_back("term" + std::to_string(i));
  }

  // Add many filters
  for (int i = 0; i < 10; ++i) {
    query1.filters.push_back({"col" + std::to_string(i), query::FilterOp::EQ, "val" + std::to_string(i)});
  }

  query1.limit = 100;

  std::string normalized = QueryNormalizer::Normalize(query1);

  // Should handle long queries without issues
  EXPECT_FALSE(normalized.empty());
  EXPECT_GT(normalized.length(), 100);
}

}  // namespace mygramdb::cache
