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
 * @brief Test SEARCH with unknown keyword (treated as search text)
 *
 * With parentheses-aware parsing, unknown keywords are consumed as search text
 * until a known keyword is encountered. This is more user-friendly and allows
 * flexible search expressions without worrying about keyword conflicts.
 */
TEST(QueryParserTest, SearchUnknownKeyword) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello UNKNOWN keyword");

  // UNKNOWN and keyword are treated as part of search text
  EXPECT_TRUE(query.IsValid());
  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.table, "articles");
  EXPECT_EQ(query.search_text, "hello UNKNOWN keyword");
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
}

/**
 * @brief Test quoted string with double quotes
 */
TEST(QueryParserTest, QuotedStringDouble) {
  QueryParser parser;
  auto query = parser.Parse(R"(SEARCH articles "hello world" LIMIT 10)");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.table, "articles");
  EXPECT_EQ(query.search_text, "hello world");
  EXPECT_EQ(query.limit, 10);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test quoted string with single quotes
 */
TEST(QueryParserTest, QuotedStringSingle) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles 'hello world' LIMIT 10");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.table, "articles");
  EXPECT_EQ(query.search_text, "hello world");
  EXPECT_EQ(query.limit, 10);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test quoted string with mixed quotes
 */
TEST(QueryParserTest, QuotedStringMixed) {
  QueryParser parser;
  auto query = parser.Parse(R"(SEARCH articles "it's working" LIMIT 10)");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.search_text, "it's working");
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test unclosed double quote
 */
TEST(QueryParserTest, UnclosedDoubleQuote) {
  QueryParser parser;
  auto query = parser.Parse(R"(SEARCH articles "hello world LIMIT 10)");

  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("Unclosed quote"), std::string::npos);
}

/**
 * @brief Test unclosed single quote
 */
TEST(QueryParserTest, UnclosedSingleQuote) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles 'hello world LIMIT 10");

  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("Unclosed quote"), std::string::npos);
}

/**
 * @brief Test escaped quote inside quoted string
 */
TEST(QueryParserTest, EscapedQuoteInString) {
  QueryParser parser;
  auto query = parser.Parse(R"(SEARCH articles "hello \"world\"" LIMIT 10)");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.search_text, "hello \"world\"");
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test escaped backslash
 */
TEST(QueryParserTest, EscapedBackslash) {
  QueryParser parser;
  auto query = parser.Parse(R"(SEARCH articles "hello\\world" LIMIT 10)");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.search_text, "hello\\world");
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test empty quoted string
 */
TEST(QueryParserTest, EmptyQuotedString) {
  QueryParser parser;
  auto query = parser.Parse(R"(SEARCH articles "" LIMIT 10)");

  // Empty quoted string results in UNKNOWN type due to missing args
  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_FALSE(parser.GetError().empty());
}

/**
 * @brief Test SEARCH with AND clause
 */
TEST(QueryParserTest, SearchWithAnd) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello AND world");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.table, "articles");
  EXPECT_EQ(query.search_text, "hello");
  EXPECT_EQ(query.and_terms.size(), 1);
  EXPECT_EQ(query.and_terms[0], "world");
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test SEARCH with multiple AND clauses
 */
TEST(QueryParserTest, SearchWithMultipleAnds) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello AND world AND test");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.and_terms.size(), 2);
  EXPECT_EQ(query.and_terms[0], "world");
  EXPECT_EQ(query.and_terms[1], "test");
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test SEARCH with AND and NOT
 */
TEST(QueryParserTest, SearchWithAndAndNot) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello AND world NOT test");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.and_terms.size(), 1);
  EXPECT_EQ(query.and_terms[0], "world");
  EXPECT_EQ(query.not_terms.size(), 1);
  EXPECT_EQ(query.not_terms[0], "test");
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test COUNT with AND clause
 */
TEST(QueryParserTest, CountWithAnd) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles hello AND world");

  EXPECT_EQ(query.type, QueryType::COUNT);
  EXPECT_EQ(query.table, "articles");
  EXPECT_EQ(query.search_text, "hello");
  EXPECT_EQ(query.and_terms.size(), 1);
  EXPECT_EQ(query.and_terms[0], "world");
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test AND without term
 */
TEST(QueryParserTest, AndWithoutTerm) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello AND");

  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("AND requires"), std::string::npos);
}

/**
 * @brief Test Japanese quoted string
 */
TEST(QueryParserTest, JapaneseQuotedString) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles \"漫画 アニメ\" LIMIT 10");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.search_text, "漫画 アニメ");
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test complex query with quoted string, AND, NOT, and FILTER
 */
TEST(QueryParserTest, ComplexQueryWithQuotesAndNot) {
  QueryParser parser;
  auto query = parser.Parse(R"(SEARCH articles "hello world" AND test NOT bad FILTER status = 1 LIMIT 50 OFFSET 100)");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.search_text, "hello world");
  EXPECT_EQ(query.and_terms.size(), 1);
  EXPECT_EQ(query.and_terms[0], "test");
  EXPECT_EQ(query.not_terms.size(), 1);
  EXPECT_EQ(query.not_terms[0], "bad");
  EXPECT_EQ(query.filters.size(), 1);
  EXPECT_EQ(query.limit, 50);
  EXPECT_EQ(query.offset, 100);
  EXPECT_TRUE(query.IsValid());
}

// DEBUG Command Tests
TEST(QueryParserTest, DebugOn) {
  QueryParser parser;
  auto query = parser.Parse("DEBUG ON");

  EXPECT_EQ(query.type, QueryType::DEBUG_ON);
  EXPECT_TRUE(query.IsValid());
}

TEST(QueryParserTest, DebugOff) {
  QueryParser parser;
  auto query = parser.Parse("DEBUG OFF");

  EXPECT_EQ(query.type, QueryType::DEBUG_OFF);
  EXPECT_TRUE(query.IsValid());
}

TEST(QueryParserTest, DebugCaseInsensitive) {
  QueryParser parser;
  auto query1 = parser.Parse("debug on");
  auto query2 = parser.Parse("DeBuG oFf");

  EXPECT_EQ(query1.type, QueryType::DEBUG_ON);
  EXPECT_EQ(query2.type, QueryType::DEBUG_OFF);
  EXPECT_TRUE(query1.IsValid());
  EXPECT_TRUE(query2.IsValid());
}

TEST(QueryParserTest, DebugMissingMode) {
  QueryParser parser;
  auto query = parser.Parse("DEBUG");

  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_FALSE(parser.GetError().empty());
}

TEST(QueryParserTest, DebugInvalidMode) {
  QueryParser parser;
  auto query = parser.Parse("DEBUG INVALID");

  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_FALSE(parser.GetError().empty());
}

// ORDER BY Tests
TEST(QueryParserTest, SearchWithOrderByDesc) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello ORDER BY created_at DESC LIMIT 10");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.table, "articles");
  EXPECT_EQ(query.search_text, "hello");
  EXPECT_TRUE(query.order_by.has_value());
  EXPECT_EQ(query.order_by->column, "created_at");
  EXPECT_EQ(query.order_by->order, SortOrder::DESC);
  EXPECT_EQ(query.limit, 10);
  EXPECT_TRUE(query.IsValid());
}

TEST(QueryParserTest, SearchWithOrderByAsc) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello ORDER BY created_at ASC LIMIT 10");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_TRUE(query.order_by.has_value());
  EXPECT_EQ(query.order_by->column, "created_at");
  EXPECT_EQ(query.order_by->order, SortOrder::ASC);
  EXPECT_TRUE(query.IsValid());
}

TEST(QueryParserTest, SearchWithOrderByDefaultDesc) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello ORDER BY created_at");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_TRUE(query.order_by.has_value());
  EXPECT_EQ(query.order_by->column, "created_at");
  EXPECT_EQ(query.order_by->order, SortOrder::DESC);  // Default
  EXPECT_TRUE(query.IsValid());
}

TEST(QueryParserTest, SearchWithOrderByPrimaryKey) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello ORDER BY id DESC");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_TRUE(query.order_by.has_value());
  EXPECT_EQ(query.order_by->column, "id");
  EXPECT_FALSE(query.order_by->IsPrimaryKey());  // id is a column name, not empty
  EXPECT_TRUE(query.IsValid());
}

TEST(QueryParserTest, SearchWithOrderByCaseInsensitive) {
  QueryParser parser;
  auto query1 = parser.Parse("SEARCH articles hello order by created_at asc");
  auto query2 = parser.Parse("SEARCH articles hello OrDeR By score DeSc");

  EXPECT_EQ(query1.type, QueryType::SEARCH);
  EXPECT_TRUE(query1.order_by.has_value());
  EXPECT_EQ(query1.order_by->order, SortOrder::ASC);

  EXPECT_EQ(query2.type, QueryType::SEARCH);
  EXPECT_TRUE(query2.order_by.has_value());
  EXPECT_EQ(query2.order_by->order, SortOrder::DESC);
}

TEST(QueryParserTest, SearchWithOrderByAndFilter) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello FILTER status = published ORDER BY created_at DESC LIMIT 20");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.filters.size(), 1);
  EXPECT_TRUE(query.order_by.has_value());
  EXPECT_EQ(query.order_by->column, "created_at");
  EXPECT_EQ(query.order_by->order, SortOrder::DESC);
  EXPECT_EQ(query.limit, 20);
  EXPECT_TRUE(query.IsValid());
}

TEST(QueryParserTest, SearchComplexWithOrderBy) {
  QueryParser parser;
  auto query = parser.Parse(
      "SEARCH articles golang AND tutorial NOT beginner FILTER status = 1 ORDER BY score DESC "
      "LIMIT 10 OFFSET 20");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.search_text, "golang");
  EXPECT_EQ(query.and_terms.size(), 1);
  EXPECT_EQ(query.not_terms.size(), 1);
  EXPECT_EQ(query.filters.size(), 1);
  EXPECT_TRUE(query.order_by.has_value());
  EXPECT_EQ(query.order_by->column, "score");
  EXPECT_EQ(query.order_by->order, SortOrder::DESC);
  EXPECT_EQ(query.limit, 10);
  EXPECT_EQ(query.offset, 20);
  EXPECT_TRUE(query.IsValid());
}

TEST(QueryParserTest, OrderByWithoutBy) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello ORDER created_at");

  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("BY"), std::string::npos);
}

TEST(QueryParserTest, OrderByWithoutColumn) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello ORDER BY");

  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("column name"), std::string::npos);
}

/**
 * @brief Test ORDER BY ASC shorthand (primary key)
 */
TEST(QueryParserTest, SearchWithOrderByAscShorthand) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello ORDER BY ASC LIMIT 10");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.table, "articles");
  EXPECT_EQ(query.search_text, "hello");
  EXPECT_TRUE(query.order_by.has_value());
  EXPECT_EQ(query.order_by->column, "");  // Empty = primary key
  EXPECT_TRUE(query.order_by->IsPrimaryKey());
  EXPECT_EQ(query.order_by->order, SortOrder::ASC);
  EXPECT_EQ(query.limit, 10);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test ORDER BY DESC shorthand (primary key)
 */
TEST(QueryParserTest, SearchWithOrderByDescShorthand) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello ORDER BY DESC LIMIT 10");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_TRUE(query.order_by.has_value());
  EXPECT_EQ(query.order_by->column, "");  // Empty = primary key
  EXPECT_TRUE(query.order_by->IsPrimaryKey());
  EXPECT_EQ(query.order_by->order, SortOrder::DESC);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test ORDER ASC shorthand (without BY)
 */
TEST(QueryParserTest, SearchWithOrderAscShorthand) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello ORDER ASC LIMIT 10");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_TRUE(query.order_by.has_value());
  EXPECT_EQ(query.order_by->column, "");  // Empty = primary key
  EXPECT_TRUE(query.order_by->IsPrimaryKey());
  EXPECT_EQ(query.order_by->order, SortOrder::ASC);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test ORDER DESC shorthand (without BY)
 */
TEST(QueryParserTest, SearchWithOrderDescShorthand) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello ORDER DESC LIMIT 10");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_TRUE(query.order_by.has_value());
  EXPECT_EQ(query.order_by->column, "");  // Empty = primary key
  EXPECT_TRUE(query.order_by->IsPrimaryKey());
  EXPECT_EQ(query.order_by->order, SortOrder::DESC);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test ORDER DESC shorthand with filters
 */
TEST(QueryParserTest, SearchWithOrderDescShorthandAndFilter) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello FILTER status = 1 ORDER DESC LIMIT 10");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.filters.size(), 1);
  EXPECT_TRUE(query.order_by.has_value());
  EXPECT_TRUE(query.order_by->IsPrimaryKey());
  EXPECT_EQ(query.order_by->order, SortOrder::DESC);
  EXPECT_TRUE(query.IsValid());
}

TEST(QueryParserTest, SearchWithoutOrderBy) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT 10");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_FALSE(query.order_by.has_value());  // No ORDER BY specified
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test ORDER BY with parenthesized search expression (no quotes needed!)
 *
 * The parser now tracks parentheses depth, so OR inside parentheses
 * is not interpreted as a keyword
 */
TEST(QueryParserTest, SearchWithParenthesesAndOrderBy) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH threads (golang OR python) AND tutorial ORDER DESC LIMIT 10");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.table, "threads");
  // Parenthesized expression is extracted as search_text
  EXPECT_EQ(query.search_text, "(golang OR python)");
  // AND after closing paren is recognized as keyword
  EXPECT_EQ(query.and_terms.size(), 1);
  EXPECT_EQ(query.and_terms[0], "tutorial");
  EXPECT_TRUE(query.order_by.has_value());
  EXPECT_EQ(query.order_by->order, SortOrder::DESC);
  EXPECT_TRUE(query.order_by->IsPrimaryKey());
  EXPECT_EQ(query.limit, 10);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test ORDER BY with nested parentheses and quoted phrase
 */
TEST(QueryParserTest, SearchWithComplexExpressionAndOrderBy) {
  QueryParser parser;
  auto query =
      parser.Parse(R"(SEARCH posts ((mysql OR postgresql) AND "hello world") NOT sqlite ORDER BY score ASC LIMIT 20)");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_EQ(query.table, "posts");
  // The entire complex expression up to NOT (quotes are removed by tokenizer)
  EXPECT_EQ(query.search_text, "((mysql OR postgresql) AND hello world)");
  EXPECT_EQ(query.not_terms.size(), 1);
  EXPECT_EQ(query.not_terms[0], "sqlite");
  EXPECT_TRUE(query.order_by.has_value());
  EXPECT_EQ(query.order_by->column, "score");
  EXPECT_EQ(query.order_by->order, SortOrder::ASC);
  EXPECT_EQ(query.limit, 20);
  EXPECT_TRUE(query.IsValid());
}

/**
 * @brief Test COUNT with parentheses
 */
TEST(QueryParserTest, CountWithParentheses) {
  QueryParser parser;
  auto query = parser.Parse("COUNT threads (golang OR python) FILTER status = 1");

  EXPECT_EQ(query.type, QueryType::COUNT);
  EXPECT_EQ(query.table, "threads");
  EXPECT_EQ(query.search_text, "(golang OR python)");
  EXPECT_EQ(query.filters.size(), 1);
  EXPECT_TRUE(query.IsValid());
}

// ============================================================================
// Syntax Error Tests
// ============================================================================

/**
 * @brief Test SEARCH with unclosed parenthesis
 */
TEST(QueryParserTest, SearchUnclosedParenthesis) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH threads (golang OR python LIMIT 10");

  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("Unclosed parenthesis"), std::string::npos);
}

/**
 * @brief Test SEARCH with unmatched closing parenthesis
 */
TEST(QueryParserTest, SearchUnmatchedClosingParenthesis) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH threads golang OR python) LIMIT 10");

  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("Unmatched closing parenthesis"), std::string::npos);
}

/**
 * @brief Test SEARCH with multiple unclosed parentheses
 */
TEST(QueryParserTest, SearchMultipleUnclosedParentheses) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH threads ((golang OR python) AND (rust ORDER BY id DESC");

  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("Unclosed parenthesis"), std::string::npos);
}

/**
 * @brief Test SEARCH with nested parentheses - one unclosed
 */
TEST(QueryParserTest, SearchNestedUnclosedParenthesis) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH threads ((golang OR python) AND rust LIMIT 10");

  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("Unclosed parenthesis"), std::string::npos);
}

/**
 * @brief Test SEARCH with quoted string containing unbalanced parentheses
 *
 * Note: After tokenization, quotes are removed, so the tokenized result
 * contains an unbalanced parenthesis. This is detected as an error because
 * the parenthesis balance check happens after tokenization.
 *
 * Users should either balance parentheses even inside quotes, or use
 * different delimiters for such searches.
 */
TEST(QueryParserTest, SearchQuotedParentheses) {
  QueryParser parser;
  auto query = parser.Parse(R"(SEARCH threads "hello (world" LIMIT 10)");

  // Unbalanced parenthesis detected after tokenization
  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("parenthesis"), std::string::npos);
}

/**
 * @brief Test COUNT with unclosed parenthesis
 */
TEST(QueryParserTest, CountUnclosedParenthesis) {
  QueryParser parser;
  auto query = parser.Parse("COUNT threads (golang OR python");

  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("Unclosed parenthesis"), std::string::npos);
}

/**
 * @brief Test COUNT with unmatched closing parenthesis
 */
TEST(QueryParserTest, CountUnmatchedClosingParenthesis) {
  QueryParser parser;
  auto query = parser.Parse("COUNT threads golang OR python)");

  EXPECT_EQ(query.type, QueryType::UNKNOWN);
  EXPECT_FALSE(query.IsValid());
  EXPECT_NE(parser.GetError().find("Unmatched closing parenthesis"), std::string::npos);
}

/**
 * @brief Test SEARCH with complex nested parentheses - properly balanced
 */
TEST(QueryParserTest, SearchComplexNestedParenthesesBalanced) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH threads ((golang OR python) AND (rust OR cpp)) LIMIT 10");

  EXPECT_EQ(query.type, QueryType::SEARCH);
  EXPECT_TRUE(query.IsValid());
  EXPECT_EQ(query.search_text, "((golang OR python) AND (rust OR cpp))");
}
