/**
 * @file query_normalizer_test.cpp
 * @brief Unit tests for QueryNormalizer
 */

#include "cache/query_normalizer.h"

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

}  // namespace mygramdb::cache
