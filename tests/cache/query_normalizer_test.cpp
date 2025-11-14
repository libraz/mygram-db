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
 * @brief Test default limit normalization
 */
TEST(QueryNormalizerTest, DefaultLimit) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "test";
  query1.limit = 100;
  query1.limit_explicit = false;  // Default

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "test";
  query2.limit = 50;
  query2.limit_explicit = false;  // Default

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  // Default limits should be normalized to same value
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
