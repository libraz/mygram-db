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
 * @brief Test LIMIT is excluded from cache key
 *
 * LIMIT and OFFSET are excluded from cache keys because the cache stores
 * full results (before pagination). Different LIMIT values should produce
 * the same cache key, and pagination is applied when retrieving from cache.
 */
TEST(QueryNormalizerTest, LimitExcludedFromCacheKey) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "test";
  query1.limit = 100;
  query1.limit_explicit = false;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "test";
  query2.limit = 50;
  query2.limit_explicit = false;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  // Different LIMIT values should produce the SAME cache key
  // (LIMIT is excluded from normalization)
  EXPECT_EQ(normalized1, normalized2);
}

/**
 * @brief Test OFFSET is excluded from cache key
 *
 * OFFSET is excluded from cache keys for the same reason as LIMIT.
 * Different pages of the same query should share the same cache entry.
 */
TEST(QueryNormalizerTest, OffsetExcludedFromCacheKey) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  query1.search_text = "test";
  query1.limit = 20;
  query1.offset = 0;  // Page 1

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "test";
  query2.limit = 20;
  query2.offset = 20;  // Page 2

  query::Query query3;
  query3.type = query::QueryType::SEARCH;
  query3.table = "posts";
  query3.search_text = "test";
  query3.limit = 20;
  query3.offset = 100;  // Page 6

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);
  std::string normalized3 = QueryNormalizer::Normalize(query3);

  // Different OFFSET values should produce the SAME cache key
  // (OFFSET is excluded from normalization)
  EXPECT_EQ(normalized1, normalized2);
  EXPECT_EQ(normalized2, normalized3);
}

/**
 * @brief Test explicit limit is also excluded from cache key
 *
 * Even explicit LIMIT values are excluded from cache keys.
 * The cache stores full results, and LIMIT is applied on retrieval.
 */
TEST(QueryNormalizerTest, ExplicitLimitAlsoExcluded) {
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

  // Even explicit LIMIT values should produce the SAME cache key
  EXPECT_EQ(normalized1, normalized2);
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
  EXPECT_NE(normalized.find("Q posts"), std::string::npos);
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

  // COUNT queries should use unified "Q" prefix (same as SEARCH)
  EXPECT_FALSE(normalized.empty());
  EXPECT_EQ(normalized.find("Q"), 0);
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

// =============================================================================
// M7: Unicode space normalization
// =============================================================================

/**
 * @brief Test No-Break Space (U+00A0) normalization
 */
TEST(QueryNormalizerTest, NoBreakingSpaceNormalized) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  // U+00A0 is 0xC2 0xA0 in UTF-8
  query1.search_text = "hello\xC2\xA0world";
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "hello world";
  query2.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  EXPECT_EQ(normalized1, normalized2) << "No-Break Space (U+00A0) should be normalized to regular space";
}

/**
 * @brief Test various Unicode spaces normalization
 */
TEST(QueryNormalizerTest, VariousUnicodeSpacesNormalized) {
  // U+2003 (Em Space) = 0xE2 0x80 0x83
  query::Query query_em;
  query_em.type = query::QueryType::SEARCH;
  query_em.table = "posts";
  query_em.search_text = "hello\xE2\x80\x83world";
  query_em.limit = 100;

  // U+2002 (En Space) = 0xE2 0x80 0x82
  query::Query query_en;
  query_en.type = query::QueryType::SEARCH;
  query_en.table = "posts";
  query_en.search_text = "hello\xE2\x80\x82world";
  query_en.limit = 100;

  // U+200B (Zero Width Space) = 0xE2 0x80 0x8B
  query::Query query_zw;
  query_zw.type = query::QueryType::SEARCH;
  query_zw.table = "posts";
  query_zw.search_text = "hello\xE2\x80\x8Bworld";
  query_zw.limit = 100;

  // U+202F (Narrow No-Break Space) = 0xE2 0x80 0xAF
  query::Query query_nnbs;
  query_nnbs.type = query::QueryType::SEARCH;
  query_nnbs.table = "posts";
  query_nnbs.search_text = "hello\xE2\x80\xAFworld";
  query_nnbs.limit = 100;

  // U+205F (Medium Mathematical Space) = 0xE2 0x81 0x9F
  query::Query query_mms;
  query_mms.type = query::QueryType::SEARCH;
  query_mms.table = "posts";
  query_mms.search_text = "hello\xE2\x81\x9Fworld";
  query_mms.limit = 100;

  // Reference: normal space
  query::Query query_ref;
  query_ref.type = query::QueryType::SEARCH;
  query_ref.table = "posts";
  query_ref.search_text = "hello world";
  query_ref.limit = 100;

  std::string ref = QueryNormalizer::Normalize(query_ref);

  EXPECT_EQ(QueryNormalizer::Normalize(query_em), ref) << "Em Space (U+2003) should normalize";
  EXPECT_EQ(QueryNormalizer::Normalize(query_en), ref) << "En Space (U+2002) should normalize";
  EXPECT_EQ(QueryNormalizer::Normalize(query_zw), ref) << "Zero Width Space (U+200B) should normalize";
  EXPECT_EQ(QueryNormalizer::Normalize(query_nnbs), ref) << "Narrow No-Break Space (U+202F) should normalize";
  EXPECT_EQ(QueryNormalizer::Normalize(query_mms), ref) << "Medium Mathematical Space (U+205F) should normalize";
}

/**
 * @brief Test Ogham Space Mark (U+1680) normalization
 */
TEST(QueryNormalizerTest, OghamSpaceMarkNormalized) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  // U+1680 is 0xE1 0x9A 0x80 in UTF-8
  query1.search_text = "hello\xE1\x9A\x80world";
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "hello world";
  query2.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  EXPECT_EQ(normalized1, normalized2) << "Ogham Space Mark (U+1680) should be normalized to regular space";
}

/**
 * @brief Test Line Separator (U+2028) normalization
 */
TEST(QueryNormalizerTest, LineSeparatorNormalized) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  // U+2028 is 0xE2 0x80 0xA8 in UTF-8
  query1.search_text = "hello\xE2\x80\xA8world";
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "hello world";
  query2.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  EXPECT_EQ(normalized1, normalized2) << "Line Separator (U+2028) should be normalized to regular space";
}

/**
 * @brief Test Paragraph Separator (U+2029) normalization
 */
TEST(QueryNormalizerTest, ParagraphSeparatorNormalized) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "posts";
  // U+2029 is 0xE2 0x80 0xA9 in UTF-8
  query1.search_text = "hello\xE2\x80\xA9world";
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "posts";
  query2.search_text = "hello world";
  query2.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  EXPECT_EQ(normalized1, normalized2) << "Paragraph Separator (U+2029) should be normalized to regular space";
}

// =============================================================================
// Bug 5: Primary key normalization in sort clause
// =============================================================================

/**
 * @brief Sort by PK column should produce the same key as default sort
 */
TEST(QueryNormalizerTest, SortByPKProducesSameKeyAsDefault) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "articles";
  query1.search_text = "test";
  query1.limit = 100;
  // No sort specified (default)

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "articles";
  query2.search_text = "test";
  query2.order_by = query::OrderByClause{"article_id", query::SortOrder::DESC};
  query2.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1, "article_id");
  std::string normalized2 = QueryNormalizer::Normalize(query2, "article_id");

  EXPECT_EQ(normalized1, normalized2)
      << "Default sort and explicit PK sort should produce the same cache key";
}

/**
 * @brief Sort by non-PK column should produce a different key from default
 */
TEST(QueryNormalizerTest, SortByNonPKProducesDifferentKey) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "articles";
  query1.search_text = "test";
  query1.limit = 100;
  // No sort specified (default)

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "articles";
  query2.search_text = "test";
  query2.order_by = query::OrderByClause{"created_at", query::SortOrder::DESC};
  query2.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1, "article_id");
  std::string normalized2 = QueryNormalizer::Normalize(query2, "article_id");

  EXPECT_NE(normalized1, normalized2)
      << "Default sort and non-PK sort should produce different cache keys";
}

/**
 * @brief PK column should be replaced with __pk__ placeholder in output
 */
TEST(QueryNormalizerTest, PKPlaceholderInOutput) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "articles";
  query.search_text = "test";
  query.limit = 100;
  // No sort specified (default)

  std::string normalized = QueryNormalizer::Normalize(query, "article_id");

  EXPECT_NE(normalized.find("__pk__"), std::string::npos)
      << "Normalized output should contain __pk__ placeholder";
  EXPECT_EQ(normalized.find("article_id"), std::string::npos)
      << "Normalized output should not contain the actual PK column name";
}

// =============================================================================
// Bug 8: Sort column name case sensitivity
// =============================================================================

/**
 * @brief "SORT ID DESC" and "SORT id DESC" should produce the same cache key
 *
 * MySQL column names are case-insensitive, so different cases of the same
 * PK column name should be normalized to the same __pk__ placeholder.
 */
TEST(QueryNormalizerTest, SortColumnCaseInsensitive) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "articles";
  query1.search_text = "test";
  query1.order_by = query::OrderByClause{"ID", query::SortOrder::DESC};
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::SEARCH;
  query2.table = "articles";
  query2.search_text = "test";
  query2.order_by = query::OrderByClause{"id", query::SortOrder::DESC};
  query2.limit = 100;

  query::Query query3;
  query3.type = query::QueryType::SEARCH;
  query3.table = "articles";
  query3.search_text = "test";
  query3.order_by = query::OrderByClause{"Id", query::SortOrder::DESC};
  query3.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1, "id");
  std::string normalized2 = QueryNormalizer::Normalize(query2, "id");
  std::string normalized3 = QueryNormalizer::Normalize(query3, "id");

  // All case variants should produce the same cache key
  EXPECT_EQ(normalized1, normalized2) << "ID vs id should produce same cache key";
  EXPECT_EQ(normalized2, normalized3) << "id vs Id should produce same cache key";

  // Should use __pk__ placeholder
  EXPECT_NE(normalized1.find("__pk__"), std::string::npos)
      << "Should normalize PK column name to __pk__ placeholder";
}

// =============================================================================
// Bug 6: SEARCH and COUNT produce the same cache key
// =============================================================================

/**
 * @brief SEARCH and COUNT for the same query should produce identical cache keys
 */
TEST(QueryNormalizerTest, SearchAndCountProduceSameNormalizedString) {
  query::Query query1;
  query1.type = query::QueryType::SEARCH;
  query1.table = "articles";
  query1.search_text = "test";
  query1.limit = 100;

  query::Query query2;
  query2.type = query::QueryType::COUNT;
  query2.table = "articles";
  query2.search_text = "test";
  query2.limit = 100;

  std::string normalized1 = QueryNormalizer::Normalize(query1);
  std::string normalized2 = QueryNormalizer::Normalize(query2);

  EXPECT_EQ(normalized1, normalized2)
      << "SEARCH and COUNT should produce the same normalized cache key";
}

/**
 * @brief Both SEARCH and COUNT should use the unified "Q" prefix
 */
TEST(QueryNormalizerTest, UnifiedPrefixIsQ) {
  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "articles";
  query.search_text = "test";
  query.limit = 100;

  std::string normalized = QueryNormalizer::Normalize(query);

  EXPECT_EQ(normalized.substr(0, 2), "Q ") << "Normalized string should start with 'Q '";
}

}  // namespace mygramdb::cache
