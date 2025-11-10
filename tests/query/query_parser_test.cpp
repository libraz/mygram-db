/**
 * @file query_parser_test.cpp
 * @brief Unit tests for query parser
 */

#include "query/query_parser.h"
#include <gtest/gtest.h>

using namespace mygramdb::query;

/**
 * @brief Test basic SEARCH query
 */
TEST(QueryParserTest, SearchBasic) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.table, "articles");
  EXPECT_EQ(query.search_text, "hello");
  EXPECT_EQ(query.limit, 100);  // Default
  EXPECT_EQ(query.offset, 0);   // Default
  EXPECT_TRUE(query.IsValid());
  EXPECT_TRUE(parser.GetError().empty());
}

/**
 * @brief Test SEARCH with LIMIT
 */
TEST(QueryParserTest, SearchWithLimit) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT 50");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.table, "articles");
  EXPECT_EQ(query.search_text, "hello");
  EXPECT_EQ(query.limit, 50);
  EXPECT_EQ(query.offset, 0);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test SEARCH with OFFSET
 */
TEST(QueryParserTest, SearchWithOffset) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello OFFSET 100");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.limit, 100);
  EXPECT_EQ(query.offset, 100);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test SEARCH with LIMIT and OFFSET
 */
TEST(QueryParserTest, SearchWithLimitAndOffset) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT 50 OFFSET 200");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.limit, 50);
  EXPECT_EQ(query.offset, 200);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test SEARCH with maximum LIMIT
 */
TEST(QueryParserTest, SearchWithMaxLimit) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT 1000");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.limit, 1000);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test SEARCH exceeding maximum LIMIT
 */
TEST(QueryParserTest, SearchExceedMaxLimit) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT 1001");

  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_FALSE(parser.GetError().empty());
  EXPECT_NE(parser.GetError().find("maximum"), std::string::npos);
}

/**
 * @brief Test SEARCH with filter
 */
TEST(QueryParserTest, SearchWithFilter) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello FILTER status = 1");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.filters.size(), 1);
  EXPECT_EQ(query.filters[0].column, "status");
  EXPECT_EQ(query.filters[0].op, FilterOp::EQ);
  EXPECT_EQ(query.filters[0].value, "1");
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test SEARCH with multiple keywords
 */
TEST(QueryParserTest, SearchWithMultipleKeywords) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello FILTER status = 1 LIMIT 50 OFFSET 100");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.filters.size(), 1);
  EXPECT_EQ(query.limit, 50);
  EXPECT_EQ(query.offset, 100);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test COUNT basic query
 */
TEST(QueryParserTest, CountBasic) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles hello");

  EXPECT_EQ(query.type, QueryType::COUNT);
  EXPECT_EQ(query.table, "articles");
  EXPECT_EQ(query.search_text, "hello");
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test COUNT with filter
 */
TEST(QueryParserTest, CountWithFilter) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles hello FILTER status = 1");

  EXPECT_EQ(query.type, QueryType::COUNT);
  EXPECT_EQ(query.filters.size(), 1);
  EXPECT_EQ(query.filters[0].column, "status");
  EXPECT_EQ(query.filters[0].op, FilterOp::EQ);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test GET query
 */
TEST(QueryParserTest, GetBasic) {
  QueryParser parser;
  auto query = parser.Parse("GET articles 12345");

  EXPECT_EQ(query.type, QueryType::GET);
  EXPECT_EQ(query.table, "articles");
  EXPECT_EQ(query.primary_key, "12345");
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test filter operators
 */
TEST(QueryParserTest, FilterOperators) {
  QueryParser parser;

  // EQ
  auto query1 = parser.Parse("SEARCH articles hello FILTER status = 1");
  EXPECT_EQ(query1.filters[0].op, FilterOp::EQ);

  // NE
  auto query2 = parser.Parse("SEARCH articles hello FILTER status != 1");
  EXPECT_EQ(query2.filters[0].op, FilterOp::NE);

  // GT
  auto query3 = parser.Parse("SEARCH articles hello FILTER status > 1");
  EXPECT_EQ(query3.filters[0].op, FilterOp::GT);

  // GTE
  auto query4 = parser.Parse("SEARCH articles hello FILTER status >= 1");
  EXPECT_EQ(query4.filters[0].op, FilterOp::GTE);

  // LT
  auto query5 = parser.Parse("SEARCH articles hello FILTER status < 1");
  EXPECT_EQ(query5.filters[0].op, FilterOp::LT);

  // LTE
  auto query6 = parser.Parse("SEARCH articles hello FILTER status <= 1");
  EXPECT_EQ(query6.filters[0].op, FilterOp::LTE);
}

/**
 * @brief Test case insensitivity
 */
TEST(QueryParserTest, CaseInsensitive) {
  QueryParser parser;

  auto query1 = parser.Parse("search articles hello");
  EXPECT_EQ(query1.type, QueryType::SEARCH);

  auto query2 = parser.Parse("SEARCH articles hello limit 50");
  EXPECT_EQ(query2.limit, 50);

  auto query3 = parser.Parse("Search articles hello Limit 50 Offset 100");
  EXPECT_EQ(query3.limit, 50);
  EXPECT_EQ(query3.offset, 100);
}

/**
 * @brief Test empty query
 */
TEST(QueryParserTest, EmptyQuery) {
  QueryParser parser;
  auto query = parser.Parse("");

  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_FALSE(parser.GetError().empty());
}

/**
 * @brief Test unknown command
 */
TEST(QueryParserTest, UnknownCommand) {
  QueryParser parser;
  auto query = parser.Parse("INVALID articles hello");

  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("Unknown command"), std::string::npos);
}

/**
 * @brief Test SEARCH missing arguments
 */
TEST(QueryParserTest, SearchMissingArgs) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_FALSE(query.IsValid());
  EXPECT_FALSE(parser.GetError().empty());
}

/**
 * @brief Test COUNT missing arguments
 */
TEST(QueryParserTest, CountMissingArgs) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles");

  EXPECT_EQ(query.type, QueryType::COUNT);
  EXPECT_FALSE(query.IsValid());
  EXPECT_FALSE(parser.GetError().empty());
}

/**
 * @brief Test GET missing arguments
 */
TEST(QueryParserTest, GetMissingArgs) {
  QueryParser parser;

  auto query1 = parser.Parse("GET articles");
  EXPECT_FALSE(query1.IsValid());

  auto query2 = parser.Parse("GET");
  EXPECT_FALSE(query2.IsValid());
}

/**
 * @brief Test invalid LIMIT value
 */
TEST(QueryParserTest, InvalidLimitValue) {
  QueryParser parser;

  auto query1 = parser.Parse("SEARCH articles hello LIMIT abc");
  EXPECT_FALSE(query1.IsValid());
  EXPECT_FALSE(parser.GetError().empty());

  auto query2 = parser.Parse("SEARCH articles hello LIMIT 0");
  EXPECT_FALSE(query2.IsValid());

  auto query3 = parser.Parse("SEARCH articles hello LIMIT -10");
  EXPECT_FALSE(query3.IsValid());
}

/**
 * @brief Test invalid OFFSET value
 */
TEST(QueryParserTest, InvalidOffsetValue) {
  QueryParser parser;

  auto query1 = parser.Parse("SEARCH articles hello OFFSET abc");
  EXPECT_FALSE(query1.IsValid());
  EXPECT_FALSE(parser.GetError().empty());

  auto query2 = parser.Parse("SEARCH articles hello OFFSET -10");
  EXPECT_FALSE(query2.IsValid());
}

/**
 * @brief Test missing LIMIT value
 */
TEST(QueryParserTest, MissingLimitValue) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT");

  EXPECT_FALSE(query.IsValid());
  EXPECT_FALSE(parser.GetError().empty());
}

/**
 * @brief Test missing OFFSET value
 */
TEST(QueryParserTest, MissingOffsetValue) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello OFFSET");

  EXPECT_FALSE(query.IsValid());
  EXPECT_FALSE(parser.GetError().empty());
}

/**
 * @brief Test invalid filter format
 */
TEST(QueryParserTest, InvalidFilterFormat) {
  QueryParser parser;

  auto query1 = parser.Parse("SEARCH articles hello FILTER status");
  EXPECT_FALSE(query1.IsValid());

  auto query2 = parser.Parse("SEARCH articles hello FILTER status =");
  EXPECT_FALSE(query2.IsValid());
}

/**
 * @brief Test invalid filter operator
 */
TEST(QueryParserTest, InvalidFilterOperator) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello FILTER status ~~ 1");

  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("operator"), std::string::npos);
}

/**
 * @brief Test COUNT with unsupported clause
 */
TEST(QueryParserTest, CountUnsupportedClause) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles hello LIMIT 50");

  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("FILTER"), std::string::npos);
}

/**
 * @brief Test SEARCH with unknown keyword
 */
TEST(QueryParserTest, SearchUnknownKeyword) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello UNKNOWN keyword");

  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("Unknown keyword"), std::string::npos);
}

/**
 * @brief Test Japanese search text
 */
TEST(QueryParserTest, JapaneseSearchText) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles ライブ LIMIT 50");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.search_text, "ライブ");
  EXPECT_EQ(query.limit, 50);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test large OFFSET value
 */
TEST(QueryParserTest, LargeOffsetValue) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello OFFSET 1000000");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.offset, 1000000);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test SEARCH with NOT clause
 */
TEST(QueryParserTest, SearchWithNot) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello NOT world");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.table, "articles");
  EXPECT_EQ(query.search_text, "hello");
  EXPECT_EQ(query.not_terms.size(), 1);
  EXPECT_EQ(query.not_terms[0], "world");
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test SEARCH with multiple NOT clauses
 */
TEST(QueryParserTest, SearchWithMultipleNots) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello NOT world NOT test");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.not_terms.size(), 2);
  EXPECT_EQ(query.not_terms[0], "world");
  EXPECT_EQ(query.not_terms[1], "test");
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test SEARCH with NOT and FILTER
 */
TEST(QueryParserTest, SearchWithNotAndFilter) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello NOT world FILTER status = 1");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.not_terms.size(), 1);
  EXPECT_EQ(query.not_terms[0], "world");
  EXPECT_EQ(query.filters.size(), 1);
  EXPECT_EQ(query.filters[0].column, "status");
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test SEARCH with NOT, FILTER, LIMIT, and OFFSET
 */
TEST(QueryParserTest, SearchWithNotFilterLimitOffset) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello NOT world FILTER status = 1 LIMIT 50 OFFSET 100");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.not_terms.size(), 1);
  EXPECT_EQ(query.not_terms[0], "world");
  EXPECT_EQ(query.filters.size(), 1);
  EXPECT_EQ(query.limit, 50);
  EXPECT_EQ(query.offset, 100);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test COUNT with NOT clause
 */
TEST(QueryParserTest, CountWithNot) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles hello NOT world");

  EXPECT_EQ(query.type, QueryType::COUNT);
  EXPECT_EQ(query.table, "articles");
  EXPECT_EQ(query.search_text, "hello");
  EXPECT_EQ(query.not_terms.size(), 1);
  EXPECT_EQ(query.not_terms[0], "world");
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test COUNT with NOT and FILTER
 */
TEST(QueryParserTest, CountWithNotAndFilter) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles hello NOT world FILTER status = 1");

  EXPECT_EQ(query.type, QueryType::COUNT);
  EXPECT_EQ(query.not_terms.size(), 1);
  EXPECT_EQ(query.filters.size(), 1);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test NOT without term
 */
TEST(QueryParserTest, NotWithoutTerm) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello NOT");

  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("NOT requires"), std::string::npos);
}

/**
 * @brief Test COUNT with LIMIT (unsupported)
 */
TEST(QueryParserTest, CountWithLimitStillUnsupported) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles hello NOT world LIMIT 50");

  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("FILTER"), std::string::npos);
}
