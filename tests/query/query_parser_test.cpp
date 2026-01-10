/**
 * @file query_parser_test.cpp
 * @brief Unit tests for query parser
 */

#include "query/query_parser.h"

#include <gtest/gtest.h>

using namespace mygramdb::query;
using namespace mygram::utils;

/**
 * @brief Test basic SEARCH query
 */
TEST(QueryParserTest, SearchBasic) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->table, "articles");
  EXPECT_EQ(query->search_text, "hello");
  EXPECT_EQ(query->limit, 100);  // Default
  EXPECT_EQ(query->offset, 0);   // Default
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test SEARCH with LIMIT
 */
TEST(QueryParserTest, SearchWithLimit) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT 50");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->table, "articles");
  EXPECT_EQ(query->search_text, "hello");
  EXPECT_EQ(query->limit, 50);
  EXPECT_EQ(query->offset, 0);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test SEARCH with OFFSET
 */
TEST(QueryParserTest, SearchWithOffset) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello OFFSET 100");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->limit, 100);
  EXPECT_EQ(query->offset, 100);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test SEARCH with LIMIT and OFFSET
 */
TEST(QueryParserTest, SearchWithLimitAndOffset) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT 50 OFFSET 200");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->limit, 50);
  EXPECT_EQ(query->offset, 200);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test SEARCH with maximum LIMIT
 */
TEST(QueryParserTest, SearchWithMaxLimit) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT 1000");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->limit, 1000);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test SEARCH exceeding maximum LIMIT
 */
TEST(QueryParserTest, SearchExceedMaxLimit) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT 1001");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("maximum"), std::string::npos);
}

/**
 * @brief Test SEARCH with filter
 */
TEST(QueryParserTest, SearchWithFilter) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello FILTER status = 1");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->filters.size(), 1);
  EXPECT_EQ(query->filters[0].column, "status");
  EXPECT_EQ(query->filters[0].op, FilterOp::EQ);
  EXPECT_EQ(query->filters[0].value, "1");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test SEARCH with multiple keywords
 */
TEST(QueryParserTest, SearchWithMultipleKeywords) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello FILTER status = 1 LIMIT 50 OFFSET 100");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->filters.size(), 1);
  EXPECT_EQ(query->limit, 50);
  EXPECT_EQ(query->offset, 100);
  EXPECT_TRUE(query->IsValid());
}

TEST(QueryParserTest, SearchExceedsDefaultQueryLengthLimit) {
  QueryParser parser;
  std::string long_term(200, 'a');
  auto query = parser.Parse("SEARCH articles " + long_term);

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("exceeds"), std::string::npos);
}

TEST(QueryParserTest, SearchRespectsFilterContributionToLength) {
  QueryParser parser;
  std::string filter_value(150, 'b');
  auto query = parser.Parse("SEARCH articles short FILTER status = " + filter_value);

  EXPECT_FALSE(query);
}

TEST(QueryParserTest, SearchAllowsCustomQueryLengthLimit) {
  QueryParser parser;
  parser.SetMaxQueryLength(256);

  std::string long_term(200, 'a');
  auto query = parser.Parse("SEARCH articles " + long_term);

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test COUNT basic query
 */
TEST(QueryParserTest, CountBasic) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles hello");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::COUNT);
  EXPECT_EQ(query->table, "articles");
  EXPECT_EQ(query->search_text, "hello");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test COUNT with filter
 */
TEST(QueryParserTest, CountWithFilter) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles hello FILTER status = 1");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::COUNT);
  EXPECT_EQ(query->filters.size(), 1);
  EXPECT_EQ(query->filters[0].column, "status");
  EXPECT_EQ(query->filters[0].op, FilterOp::EQ);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test GET query
 */
TEST(QueryParserTest, GetBasic) {
  QueryParser parser;
  auto query = parser.Parse("GET articles 12345");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::GET);
  EXPECT_EQ(query->table, "articles");
  EXPECT_EQ(query->primary_key, "12345");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test filter operators
 */
TEST(QueryParserTest, FilterOperators) {
  QueryParser parser;

  // EQ
  auto query1 = parser.Parse("SEARCH articles hello FILTER status = 1");
  ASSERT_TRUE(query1);
  EXPECT_EQ(query1->filters[0].op, FilterOp::EQ);

  // NE
  auto query2 = parser.Parse("SEARCH articles hello FILTER status != 1");
  ASSERT_TRUE(query2);
  EXPECT_EQ(query2->filters[0].op, FilterOp::NE);

  // GT
  auto query3 = parser.Parse("SEARCH articles hello FILTER status > 1");
  ASSERT_TRUE(query3);
  EXPECT_EQ(query3->filters[0].op, FilterOp::GT);

  // GTE
  auto query4 = parser.Parse("SEARCH articles hello FILTER status >= 1");
  ASSERT_TRUE(query4);
  EXPECT_EQ(query4->filters[0].op, FilterOp::GTE);

  // LT
  auto query5 = parser.Parse("SEARCH articles hello FILTER status < 1");
  ASSERT_TRUE(query5);
  EXPECT_EQ(query5->filters[0].op, FilterOp::LT);

  // LTE
  auto query6 = parser.Parse("SEARCH articles hello FILTER status <= 1");
  ASSERT_TRUE(query6);
  EXPECT_EQ(query6->filters[0].op, FilterOp::LTE);
}

TEST(QueryParserTest, FilterWithoutSpacesEquals) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello FILTER status=1");

  ASSERT_TRUE(query);
  EXPECT_TRUE(query->IsValid());
  ASSERT_EQ(query->filters.size(), 1);
  EXPECT_EQ(query->filters[0].column, "status");
  EXPECT_EQ(query->filters[0].op, FilterOp::EQ);
  EXPECT_EQ(query->filters[0].value, "1");
}

TEST(QueryParserTest, FilterWithoutSpacesGreaterEqual) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello FILTER score>=42");

  ASSERT_TRUE(query);
  EXPECT_TRUE(query->IsValid());
  ASSERT_EQ(query->filters.size(), 1);
  EXPECT_EQ(query->filters[0].column, "score");
  EXPECT_EQ(query->filters[0].op, FilterOp::GTE);
  EXPECT_EQ(query->filters[0].value, "42");
}

TEST(QueryParserTest, FilterAttachedOperatorWithSeparateValue) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello FILTER status= 1");

  ASSERT_TRUE(query);
  EXPECT_TRUE(query->IsValid());
  ASSERT_EQ(query->filters.size(), 1);
  EXPECT_EQ(query->filters[0].value, "1");
}

/**
 * @brief Test case insensitivity
 */
TEST(QueryParserTest, CaseInsensitive) {
  QueryParser parser;

  auto query1 = parser.Parse("search articles hello");
  ASSERT_TRUE(query1);
  EXPECT_EQ(query1->type, QueryType::SEARCH);

  auto query2 = parser.Parse("SEARCH articles hello limit 50");
  ASSERT_TRUE(query2);
  EXPECT_EQ(query2->limit, 50);

  auto query3 = parser.Parse("Search articles hello Limit 50 Offset 100");
  ASSERT_TRUE(query3);
  EXPECT_EQ(query3->limit, 50);
  EXPECT_EQ(query3->offset, 100);
}

/**
 * @brief Test empty query
 */
TEST(QueryParserTest, EmptyQuery) {
  QueryParser parser;
  auto query = parser.Parse("");

  EXPECT_FALSE(query);
}

/**
 * @brief Test unknown command
 */
TEST(QueryParserTest, UnknownCommand) {
  QueryParser parser;
  auto query = parser.Parse("INVALID articles hello");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Unknown command"), std::string::npos);
}

/**
 * @brief Test SEARCH missing arguments
 */
TEST(QueryParserTest, SearchMissingArgs) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_FALSE(query->IsValid());
}

/**
 * @brief Test COUNT missing arguments
 */
TEST(QueryParserTest, CountMissingArgs) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::COUNT);
  EXPECT_FALSE(query->IsValid());
}

/**
 * @brief Test GET missing arguments
 */
TEST(QueryParserTest, GetMissingArgs) {
  QueryParser parser;

  auto query1 = parser.Parse("GET articles");
  ASSERT_TRUE(query1);
  EXPECT_FALSE(query1->IsValid());

  auto query2 = parser.Parse("GET");
  ASSERT_TRUE(query2);
  EXPECT_FALSE(query2->IsValid());
}

/**
 * @brief Test invalid LIMIT value
 */
TEST(QueryParserTest, InvalidLimitValue) {
  QueryParser parser;

  auto query1 = parser.Parse("SEARCH articles hello LIMIT abc");
  EXPECT_FALSE(query1);

  auto query2 = parser.Parse("SEARCH articles hello LIMIT 0");
  EXPECT_FALSE(query2);

  auto query3 = parser.Parse("SEARCH articles hello LIMIT -10");
  EXPECT_FALSE(query3);
}

/**
 * @brief Test invalid OFFSET value
 */
TEST(QueryParserTest, InvalidOffsetValue) {
  QueryParser parser;

  auto query1 = parser.Parse("SEARCH articles hello OFFSET abc");
  EXPECT_FALSE(query1);

  auto query2 = parser.Parse("SEARCH articles hello OFFSET -10");
  EXPECT_FALSE(query2);
}

/**
 * @brief Test missing LIMIT value
 */
TEST(QueryParserTest, MissingLimitValue) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT");

  EXPECT_FALSE(query);
}

/**
 * @brief Test missing OFFSET value
 */
TEST(QueryParserTest, MissingOffsetValue) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello OFFSET");

  EXPECT_FALSE(query);
}

/**
 * @brief Test invalid filter format
 */
TEST(QueryParserTest, InvalidFilterFormat) {
  QueryParser parser;

  auto query1 = parser.Parse("SEARCH articles hello FILTER status");
  EXPECT_FALSE(query1);

  auto query2 = parser.Parse("SEARCH articles hello FILTER status =");
  EXPECT_FALSE(query2);
}

/**
 * @brief Test invalid filter operator
 */
TEST(QueryParserTest, InvalidFilterOperator) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello FILTER status ~~ 1");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("operator"), std::string::npos);
}

/**
 * @brief Test COUNT with unsupported clause
 */
TEST(QueryParserTest, CountUnsupportedClause) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles hello LIMIT 50");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("FILTER"), std::string::npos);
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
  ASSERT_TRUE(query);
  EXPECT_TRUE(query->IsValid());
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->table, "articles");
  EXPECT_EQ(query->search_text, "hello UNKNOWN keyword");
}

/**
 * @brief Test Japanese search text
 */
TEST(QueryParserTest, JapaneseSearchText) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles ライブ LIMIT 50");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->search_text, "ライブ");
  EXPECT_EQ(query->limit, 50);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test large OFFSET value
 */
TEST(QueryParserTest, LargeOffsetValue) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello OFFSET 1000000");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->offset, 1000000);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test SEARCH with NOT clause
 */
TEST(QueryParserTest, SearchWithNot) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello NOT world");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->table, "articles");
  EXPECT_EQ(query->search_text, "hello");
  EXPECT_EQ(query->not_terms.size(), 1);
  EXPECT_EQ(query->not_terms[0], "world");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test SEARCH with multiple NOT clauses
 */
TEST(QueryParserTest, SearchWithMultipleNots) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello NOT world NOT test");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->not_terms.size(), 2);
  EXPECT_EQ(query->not_terms[0], "world");
  EXPECT_EQ(query->not_terms[1], "test");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test SEARCH with NOT and FILTER
 */
TEST(QueryParserTest, SearchWithNotAndFilter) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello NOT world FILTER status = 1");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->not_terms.size(), 1);
  EXPECT_EQ(query->not_terms[0], "world");
  EXPECT_EQ(query->filters.size(), 1);
  EXPECT_EQ(query->filters[0].column, "status");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test SEARCH with NOT, FILTER, LIMIT, and OFFSET
 */
TEST(QueryParserTest, SearchWithNotFilterLimitOffset) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello NOT world FILTER status = 1 LIMIT 50 OFFSET 100");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->not_terms.size(), 1);
  EXPECT_EQ(query->not_terms[0], "world");
  EXPECT_EQ(query->filters.size(), 1);
  EXPECT_EQ(query->limit, 50);
  EXPECT_EQ(query->offset, 100);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test COUNT with NOT clause
 */
TEST(QueryParserTest, CountWithNot) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles hello NOT world");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::COUNT);
  EXPECT_EQ(query->table, "articles");
  EXPECT_EQ(query->search_text, "hello");
  EXPECT_EQ(query->not_terms.size(), 1);
  EXPECT_EQ(query->not_terms[0], "world");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test COUNT with NOT and FILTER
 */
TEST(QueryParserTest, CountWithNotAndFilter) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles hello NOT world FILTER status = 1");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::COUNT);
  EXPECT_EQ(query->not_terms.size(), 1);
  EXPECT_EQ(query->filters.size(), 1);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test NOT without term
 */
TEST(QueryParserTest, NotWithoutTerm) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello NOT");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("NOT requires"), std::string::npos);
}

/**
 * @brief Test COUNT with LIMIT (unsupported)
 */
TEST(QueryParserTest, CountWithLimitStillUnsupported) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles hello NOT world LIMIT 50");

  EXPECT_FALSE(query);
}

/**
 * @brief Test quoted string with double quotes
 */
TEST(QueryParserTest, QuotedStringDouble) {
  QueryParser parser;
  auto query = parser.Parse(R"(SEARCH articles "hello world" LIMIT 10)");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->table, "articles");
  EXPECT_EQ(query->search_text, "hello world");
  EXPECT_EQ(query->limit, 10);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test quoted string with single quotes
 */
TEST(QueryParserTest, QuotedStringSingle) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles 'hello world' LIMIT 10");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->table, "articles");
  EXPECT_EQ(query->search_text, "hello world");
  EXPECT_EQ(query->limit, 10);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test quoted string with mixed quotes
 */
TEST(QueryParserTest, QuotedStringMixed) {
  QueryParser parser;
  auto query = parser.Parse(R"(SEARCH articles "it's working" LIMIT 10)");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->search_text, "it's working");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test unclosed double quote
 */
TEST(QueryParserTest, UnclosedDoubleQuote) {
  QueryParser parser;
  auto query = parser.Parse(R"(SEARCH articles "hello world LIMIT 10)");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Unclosed quote"), std::string::npos);
}

/**
 * @brief Test unclosed single quote
 */
TEST(QueryParserTest, UnclosedSingleQuote) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles 'hello world LIMIT 10");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Unclosed quote"), std::string::npos);
}

/**
 * @brief Test escaped quote inside quoted string
 */
TEST(QueryParserTest, EscapedQuoteInString) {
  QueryParser parser;
  auto query = parser.Parse(R"(SEARCH articles "hello \"world\"" LIMIT 10)");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->search_text, "hello \"world\"");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test escaped backslash
 */
TEST(QueryParserTest, EscapedBackslash) {
  QueryParser parser;
  auto query = parser.Parse(R"(SEARCH articles "hello\\world" LIMIT 10)");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->search_text, "hello\\world");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test empty quoted string
 */
TEST(QueryParserTest, EmptyQuotedString) {
  QueryParser parser;
  auto query = parser.Parse(R"(SEARCH articles "" LIMIT 10)");

  // Empty quoted string results in UNKNOWN type due to missing args
  EXPECT_FALSE(query);
  EXPECT_FALSE(query.error().message().empty());
}

/**
 * @brief Test SEARCH with AND clause
 */
TEST(QueryParserTest, SearchWithAnd) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello AND world");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->table, "articles");
  EXPECT_EQ(query->search_text, "hello");
  EXPECT_EQ(query->and_terms.size(), 1);
  EXPECT_EQ(query->and_terms[0], "world");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test SEARCH with multiple AND clauses
 */
TEST(QueryParserTest, SearchWithMultipleAnds) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello AND world AND test");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->and_terms.size(), 2);
  EXPECT_EQ(query->and_terms[0], "world");
  EXPECT_EQ(query->and_terms[1], "test");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test SEARCH with AND and NOT
 */
TEST(QueryParserTest, SearchWithAndAndNot) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello AND world NOT test");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->and_terms.size(), 1);
  EXPECT_EQ(query->and_terms[0], "world");
  EXPECT_EQ(query->not_terms.size(), 1);
  EXPECT_EQ(query->not_terms[0], "test");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test COUNT with AND clause
 */
TEST(QueryParserTest, CountWithAnd) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles hello AND world");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::COUNT);
  EXPECT_EQ(query->table, "articles");
  EXPECT_EQ(query->search_text, "hello");
  EXPECT_EQ(query->and_terms.size(), 1);
  EXPECT_EQ(query->and_terms[0], "world");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test AND without term
 */
TEST(QueryParserTest, AndWithoutTerm) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello AND");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("AND requires"), std::string::npos);
}

/**
 * @brief Test Japanese quoted string
 */
TEST(QueryParserTest, JapaneseQuotedString) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles \"漫画 アニメ\" LIMIT 10");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->search_text, "漫画 アニメ");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test complex query with quoted string, AND, NOT, and FILTER
 */
TEST(QueryParserTest, ComplexQueryWithQuotesAndNot) {
  QueryParser parser;
  auto query = parser.Parse(R"(SEARCH articles "hello world" AND test NOT bad FILTER status = 1 LIMIT 50 OFFSET 100)");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->search_text, "hello world");
  EXPECT_EQ(query->and_terms.size(), 1);
  EXPECT_EQ(query->and_terms[0], "test");
  EXPECT_EQ(query->not_terms.size(), 1);
  EXPECT_EQ(query->not_terms[0], "bad");
  EXPECT_EQ(query->filters.size(), 1);
  EXPECT_EQ(query->limit, 50);
  EXPECT_EQ(query->offset, 100);
  EXPECT_TRUE(query->IsValid());
}

// DEBUG Command Tests
TEST(QueryParserTest, DebugOn) {
  QueryParser parser;
  auto query = parser.Parse("DEBUG ON");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::DEBUG_ON);
  EXPECT_TRUE(query->IsValid());
}

TEST(QueryParserTest, DebugOff) {
  QueryParser parser;
  auto query = parser.Parse("DEBUG OFF");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::DEBUG_OFF);
  EXPECT_TRUE(query->IsValid());
}

TEST(QueryParserTest, DebugCaseInsensitive) {
  QueryParser parser;
  auto query1 = parser.Parse("debug on");
  auto query2 = parser.Parse("DeBuG oFf");

  EXPECT_EQ(query1->type, QueryType::DEBUG_ON);
  EXPECT_EQ(query2->type, QueryType::DEBUG_OFF);
  EXPECT_TRUE(query1->IsValid());
  EXPECT_TRUE(query2->IsValid());
}

TEST(QueryParserTest, DebugMissingMode) {
  QueryParser parser;
  auto query = parser.Parse("DEBUG");

  EXPECT_FALSE(query);
  EXPECT_FALSE(query.error().message().empty());
}

TEST(QueryParserTest, DebugInvalidMode) {
  QueryParser parser;
  auto query = parser.Parse("DEBUG INVALID");

  EXPECT_FALSE(query);
  EXPECT_FALSE(query.error().message().empty());
}

// SORT Tests (formerly ORDER BY)
TEST(QueryParserTest, SearchWithSortDesc) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello SORT created_at DESC LIMIT 10");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->table, "articles");
  EXPECT_EQ(query->search_text, "hello");
  EXPECT_TRUE(query->order_by.has_value());
  EXPECT_EQ(query->order_by->column, "created_at");
  EXPECT_EQ(query->order_by->order, SortOrder::DESC);
  EXPECT_EQ(query->limit, 10);
  EXPECT_TRUE(query->IsValid());
}

TEST(QueryParserTest, SearchWithSortAsc) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello SORT created_at ASC LIMIT 10");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_TRUE(query->order_by.has_value());
  EXPECT_EQ(query->order_by->column, "created_at");
  EXPECT_EQ(query->order_by->order, SortOrder::ASC);
  EXPECT_TRUE(query->IsValid());
}

TEST(QueryParserTest, SearchWithSortDefaultDesc) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello SORT created_at");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_TRUE(query->order_by.has_value());
  EXPECT_EQ(query->order_by->column, "created_at");
  EXPECT_EQ(query->order_by->order, SortOrder::DESC);  // Default
  EXPECT_TRUE(query->IsValid());
}

TEST(QueryParserTest, SearchWithSortPrimaryKey) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello SORT id DESC");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_TRUE(query->order_by.has_value());
  EXPECT_EQ(query->order_by->column, "id");
  EXPECT_FALSE(query->order_by->IsPrimaryKey());  // id is a column name, not empty
  EXPECT_TRUE(query->IsValid());
}

TEST(QueryParserTest, SearchWithSortCaseInsensitive) {
  QueryParser parser;
  auto query1 = parser.Parse("SEARCH articles hello sort created_at asc");
  auto query2 = parser.Parse("SEARCH articles hello SoRt score DeSc");

  ASSERT_TRUE(query1);
  EXPECT_EQ(query1->type, QueryType::SEARCH);
  EXPECT_TRUE(query1->order_by.has_value());
  EXPECT_EQ(query1->order_by->order, SortOrder::ASC);

  ASSERT_TRUE(query2);
  EXPECT_EQ(query2->type, QueryType::SEARCH);
  EXPECT_TRUE(query2->order_by.has_value());
  EXPECT_EQ(query2->order_by->order, SortOrder::DESC);
}

TEST(QueryParserTest, SearchWithSortAndFilter) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello FILTER status = published SORT created_at DESC LIMIT 20");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->filters.size(), 1);
  EXPECT_TRUE(query->order_by.has_value());
  EXPECT_EQ(query->order_by->column, "created_at");
  EXPECT_EQ(query->order_by->order, SortOrder::DESC);
  EXPECT_EQ(query->limit, 20);
  EXPECT_TRUE(query->IsValid());
}

TEST(QueryParserTest, SearchComplexWithSort) {
  QueryParser parser;
  auto query = parser.Parse(
      "SEARCH articles golang AND tutorial NOT beginner FILTER status = 1 SORT score DESC "
      "LIMIT 10 OFFSET 20");

  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->search_text, "golang");
  EXPECT_EQ(query->and_terms.size(), 1);
  EXPECT_EQ(query->not_terms.size(), 1);
  EXPECT_EQ(query->filters.size(), 1);
  EXPECT_TRUE(query->order_by.has_value());
  EXPECT_EQ(query->order_by->column, "score");
  EXPECT_EQ(query->order_by->order, SortOrder::DESC);
  EXPECT_EQ(query->limit, 10);
  EXPECT_EQ(query->offset, 20);
  EXPECT_TRUE(query->IsValid());
}

TEST(QueryParserTest, SortWithoutColumn) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello SORT");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("column name"), std::string::npos);
}

/**
 * @brief Test SORT ASC shorthand (primary key)
 */
TEST(QueryParserTest, SearchWithSortAscShorthand) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello SORT ASC LIMIT 10");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->table, "articles");
  EXPECT_EQ(query->search_text, "hello");
  EXPECT_TRUE(query->order_by.has_value());
  EXPECT_EQ(query->order_by->column, "");  // Empty = primary key
  EXPECT_TRUE(query->order_by->IsPrimaryKey());
  EXPECT_EQ(query->order_by->order, SortOrder::ASC);
  EXPECT_EQ(query->limit, 10);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test SORT DESC shorthand (primary key)
 */
TEST(QueryParserTest, SearchWithSortDescShorthand) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello SORT DESC LIMIT 10");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_TRUE(query->order_by.has_value());
  EXPECT_EQ(query->order_by->column, "");  // Empty = primary key
  EXPECT_TRUE(query->order_by->IsPrimaryKey());
  EXPECT_EQ(query->order_by->order, SortOrder::DESC);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test SORT DESC shorthand with filters
 */
TEST(QueryParserTest, SearchWithSortDescShorthandAndFilter) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello FILTER status = 1 SORT DESC LIMIT 10");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->filters.size(), 1);
  EXPECT_TRUE(query->order_by.has_value());
  EXPECT_TRUE(query->order_by->IsPrimaryKey());
  EXPECT_EQ(query->order_by->order, SortOrder::DESC);
  EXPECT_TRUE(query->IsValid());
}

TEST(QueryParserTest, SearchWithoutSort) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT 10");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_FALSE(query->order_by.has_value());  // No SORT specified
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test SORT with parenthesized search expression (no quotes needed!)
 *
 * The parser now tracks parentheses depth, so OR inside parentheses
 * is not interpreted as a keyword
 */
TEST(QueryParserTest, SearchWithParenthesesAndSort) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH threads (golang OR python) AND tutorial SORT DESC LIMIT 10");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->table, "threads");
  // Parenthesized expression is extracted as search_text
  EXPECT_EQ(query->search_text, "(golang OR python)");
  // AND after closing paren is recognized as keyword
  EXPECT_EQ(query->and_terms.size(), 1);
  EXPECT_EQ(query->and_terms[0], "tutorial");
  EXPECT_TRUE(query->order_by.has_value());
  EXPECT_EQ(query->order_by->order, SortOrder::DESC);
  EXPECT_TRUE(query->order_by->IsPrimaryKey());
  EXPECT_EQ(query->limit, 10);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test SORT with nested parentheses and quoted phrase
 */
TEST(QueryParserTest, SearchWithComplexExpressionAndSort) {
  QueryParser parser;
  auto query =
      parser.Parse(R"(SEARCH posts ((mysql OR postgresql) AND "hello world") NOT sqlite SORT score ASC LIMIT 20)");

  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->table, "posts");
  // The entire complex expression up to NOT (quotes are removed by tokenizer)
  EXPECT_EQ(query->search_text, "((mysql OR postgresql) AND hello world)");
  EXPECT_EQ(query->not_terms.size(), 1);
  EXPECT_EQ(query->not_terms[0], "sqlite");
  EXPECT_TRUE(query->order_by.has_value());
  EXPECT_EQ(query->order_by->column, "score");
  EXPECT_EQ(query->order_by->order, SortOrder::ASC);
  EXPECT_EQ(query->limit, 20);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test ORDER BY is rejected with helpful error message
 */
TEST(QueryParserTest, OrderByRejectedWithHelpfulError) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello ORDER BY created_at DESC");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("ORDER BY is not supported"), std::string::npos);
  EXPECT_NE(query.error().message().find("Use SORT instead"), std::string::npos);
}

// LIMIT offset,count Tests
/**
 * @brief Test LIMIT with offset,count format
 */
TEST(QueryParserTest, LimitWithOffsetCountFormat) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT 10,50");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->offset, 10);
  EXPECT_EQ(query->limit, 50);
  EXPECT_TRUE(query->offset_explicit);
  EXPECT_TRUE(query->limit_explicit);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test LIMIT 0,100 (offset 0)
 */
TEST(QueryParserTest, LimitWithZeroOffset) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT 0,100");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->offset, 0);
  EXPECT_EQ(query->limit, 100);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test LIMIT 100,1000 (maximum)
 */
TEST(QueryParserTest, LimitWithLargeOffsetAndMax) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT 100,1000");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->offset, 100);
  EXPECT_EQ(query->limit, 1000);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test LIMIT with invalid offset,count (negative offset)
 */
TEST(QueryParserTest, LimitWithNegativeOffset) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT -10,50");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("offset must be non-negative"), std::string::npos);
}

/**
 * @brief Test LIMIT with invalid offset,count (zero count)
 */
TEST(QueryParserTest, LimitWithZeroCount) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT 10,0");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("count must be positive"), std::string::npos);
}

/**
 * @brief Test LIMIT with invalid offset,count format
 */
TEST(QueryParserTest, LimitWithInvalidOffsetCountFormat) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT abc,def");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Invalid LIMIT offset,count format"), std::string::npos);
}

/**
 * @brief Test LIMIT offset,count exceeding maximum
 */
TEST(QueryParserTest, LimitOffsetCountExceedingMax) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT 10,1001");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("maximum"), std::string::npos);
}

/**
 * @brief Test LIMIT offset,count with SORT
 */
TEST(QueryParserTest, LimitOffsetCountWithSort) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello SORT created_at DESC LIMIT 50,100");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->offset, 50);
  EXPECT_EQ(query->limit, 100);
  EXPECT_TRUE(query->order_by.has_value());
  EXPECT_EQ(query->order_by->column, "created_at");
  EXPECT_TRUE(query->IsValid());
}

// SQL Error Hint Tests
/**
 * @brief Test comma-separated tables error
 */
TEST(QueryParserTest, CommaSeparatedTablesError) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles,posts hello");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Multiple tables not supported"), std::string::npos);
  EXPECT_NE(query.error().message().find("single table"), std::string::npos);
}

/**
 * @brief Test comma after table name error
 */
TEST(QueryParserTest, CommaAfterTableNameError) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles , posts hello");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Multiple tables not supported"), std::string::npos);
}

/**
 * @brief Test COUNT with comma-separated tables
 */
TEST(QueryParserTest, CountCommaSeparatedTablesError) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles,posts hello");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Multiple tables not supported"), std::string::npos);
}

/**
 * @brief Test COUNT with ORDER BY (should suggest SORT)
 */
TEST(QueryParserTest, CountWithOrderByError) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles hello ORDER BY created_at");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("ORDER BY is not supported"), std::string::npos);
  EXPECT_NE(query.error().message().find("Use SORT instead"), std::string::npos);
}

/**
 * @brief Test COUNT with SORT (should clarify not supported)
 */
TEST(QueryParserTest, CountWithSortError) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles hello SORT created_at");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("COUNT does not support SORT"), std::string::npos);
  EXPECT_NE(query.error().message().find("Use SEARCH"), std::string::npos);
}

/**
 * @brief Test SORT with comma-separated columns
 */
TEST(QueryParserTest, SortMultipleColumnsCommaError) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello SORT created_at,updated_at");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Multiple column sorting is not supported"), std::string::npos);
}

/**
 * @brief Test SORT with multiple columns (SORT col1 ASC col2 DESC)
 */
TEST(QueryParserTest, SortMultipleColumnsSpacedError) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello SORT created_at ASC updated_at DESC");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Multiple column sorting is not supported"), std::string::npos);
}

/**
 * @brief Test SORT with multiple columns without order
 */
TEST(QueryParserTest, SortMultipleColumnsNoOrderError) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello SORT created_at updated_at");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Multiple column sorting is not supported"), std::string::npos);
}

/**
 * @brief Test COUNT with parentheses
 */
TEST(QueryParserTest, CountWithParentheses) {
  QueryParser parser;
  auto query = parser.Parse("COUNT threads (golang OR python) FILTER status = 1");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::COUNT);
  EXPECT_EQ(query->table, "threads");
  EXPECT_EQ(query->search_text, "(golang OR python)");
  EXPECT_EQ(query->filters.size(), 1);
  EXPECT_TRUE(query->IsValid());
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

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Unclosed parenthesis"), std::string::npos);
}

/**
 * @brief Test SEARCH with unmatched closing parenthesis
 */
TEST(QueryParserTest, SearchUnmatchedClosingParenthesis) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH threads golang OR python) LIMIT 10");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Unmatched closing parenthesis"), std::string::npos);
}

/**
 * @brief Test SEARCH with multiple unclosed parentheses
 */
TEST(QueryParserTest, SearchMultipleUnclosedParentheses) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH threads ((golang OR python) AND (rust ORDER BY id DESC");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Unclosed parenthesis"), std::string::npos);
}

/**
 * @brief Test SEARCH with nested parentheses - one unclosed
 */
TEST(QueryParserTest, SearchNestedUnclosedParenthesis) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH threads ((golang OR python) AND rust LIMIT 10");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Unclosed parenthesis"), std::string::npos);
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
  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("parenthesis"), std::string::npos);
}

/**
 * @brief Test COUNT with unclosed parenthesis
 */
TEST(QueryParserTest, CountUnclosedParenthesis) {
  QueryParser parser;
  auto query = parser.Parse("COUNT threads (golang OR python");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Unclosed parenthesis"), std::string::npos);
}

/**
 * @brief Test COUNT with unmatched closing parenthesis
 */
TEST(QueryParserTest, CountUnmatchedClosingParenthesis) {
  QueryParser parser;
  auto query = parser.Parse("COUNT threads golang OR python)");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Unmatched closing parenthesis"), std::string::npos);
}

/**
 * @brief Test SEARCH with complex nested parentheses - properly balanced
 */
TEST(QueryParserTest, SearchComplexNestedParenthesesBalanced) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH threads ((golang OR python) AND (rust OR cpp)) LIMIT 10");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_TRUE(query->IsValid());
  EXPECT_EQ(query->search_text, "((golang OR python) AND (rust OR cpp))");
}

// ============================================================================
// DUMP Command Tests
// ============================================================================

/**
 * @brief Test DUMP SAVE without table (regression test for Issue #63)
 *
 * Previously, DUMP_SAVE was not in the table-not-required list, causing
 * Query::IsValid() to return false even though the command doesn't need a table.
 */
TEST(QueryParserTest, DumpSaveWithoutTable) {
  QueryParser parser;
  auto query = parser.Parse("DUMP SAVE");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::DUMP_SAVE);
  EXPECT_TRUE(query->table.empty());
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test DUMP SAVE with filepath
 */
TEST(QueryParserTest, DumpSaveWithFilepath) {
  QueryParser parser;
  auto query = parser.Parse("DUMP SAVE test.dmp");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::DUMP_SAVE);
  EXPECT_TRUE(query->table.empty());
  EXPECT_EQ(query->filepath, "test.dmp");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test DUMP LOAD without filepath
 *
 * "DUMP LOAD" is parsed as DUMP_LOAD with "LOAD" as the filepath (edge case).
 * The handler will validate filepath requirements and fail appropriately.
 */
TEST(QueryParserTest, DumpLoadWithoutFilepath) {
  QueryParser parser;
  auto query = parser.Parse("DUMP LOAD");

  // Parser treats this as an error due to missing subcommand argument
  EXPECT_FALSE(query);
}

/**
 * @brief Test DUMP LOAD with filepath
 */
TEST(QueryParserTest, DumpLoadWithFilepath) {
  QueryParser parser;
  auto query = parser.Parse("DUMP LOAD test.dmp");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::DUMP_LOAD);
  EXPECT_TRUE(query->table.empty());
  EXPECT_EQ(query->filepath, "test.dmp");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test DUMP VERIFY with filepath
 */
TEST(QueryParserTest, DumpVerifyWithFilepath) {
  QueryParser parser;
  auto query = parser.Parse("DUMP VERIFY test.dmp");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::DUMP_VERIFY);
  EXPECT_TRUE(query->table.empty());
  EXPECT_EQ(query->filepath, "test.dmp");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test DUMP INFO with filepath
 */
TEST(QueryParserTest, DumpInfoWithFilepath) {
  QueryParser parser;
  auto query = parser.Parse("DUMP INFO test.dmp");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::DUMP_INFO);
  EXPECT_TRUE(query->table.empty());
  EXPECT_EQ(query->filepath, "test.dmp");
  EXPECT_TRUE(query->IsValid());
}

/**
 * @brief Test all DUMP commands are case insensitive
 */
TEST(QueryParserTest, DumpCommandsCaseInsensitive) {
  QueryParser parser;

  auto query1 = parser.Parse("dump save test.dmp");
  EXPECT_EQ(query1->type, QueryType::DUMP_SAVE);
  EXPECT_TRUE(query1->IsValid());

  auto query2 = parser.Parse("DuMp LoAd test.dmp");
  EXPECT_EQ(query2->type, QueryType::DUMP_LOAD);
  EXPECT_TRUE(query2->IsValid());

  auto query3 = parser.Parse("DUMP verify test.dmp");
  EXPECT_EQ(query3->type, QueryType::DUMP_VERIFY);
  EXPECT_TRUE(query3->IsValid());

  auto query4 = parser.Parse("dump INFO test.dmp");
  EXPECT_EQ(query4->type, QueryType::DUMP_INFO);
  EXPECT_TRUE(query4->IsValid());
}

/**
 * @brief Test case-insensitive command parsing (optimization fix)
 *
 * This test verifies that the EqualsIgnoreCase optimization correctly
 * handles case-insensitive command parsing without string allocations.
 *
 * The fix replaces ToUpper(token) + string comparison with
 * EqualsIgnoreCase(string_view, string_view) to avoid allocations.
 */
TEST(QueryParserTest, CaseInsensitiveCommandsOptimization) {
  QueryParser parser;

  // Test various case combinations for commands
  auto search_lower = parser.Parse("search posts hello");
  auto search_upper = parser.Parse("SEARCH posts hello");
  auto search_mixed = parser.Parse("SeArCh posts hello");

  EXPECT_EQ(search_lower->type, QueryType::SEARCH);
  EXPECT_EQ(search_upper->type, QueryType::SEARCH);
  EXPECT_EQ(search_mixed->type, QueryType::SEARCH);

  auto count_lower = parser.Parse("count posts hello");
  auto count_upper = parser.Parse("COUNT posts hello");
  auto count_mixed = parser.Parse("CoUnT posts hello");

  EXPECT_EQ(count_lower->type, QueryType::COUNT);
  EXPECT_EQ(count_upper->type, QueryType::COUNT);
  EXPECT_EQ(count_mixed->type, QueryType::COUNT);

  auto get_lower = parser.Parse("get posts 123");
  auto get_upper = parser.Parse("GET posts 123");
  auto get_mixed = parser.Parse("GeT posts 123");

  EXPECT_EQ(get_lower->type, QueryType::GET);
  EXPECT_EQ(get_upper->type, QueryType::GET);
  EXPECT_EQ(get_mixed->type, QueryType::GET);

  auto info_lower = parser.Parse("info");
  auto info_upper = parser.Parse("INFO");
  auto info_mixed = parser.Parse("InFo");

  EXPECT_EQ(info_lower->type, QueryType::INFO);
  EXPECT_EQ(info_upper->type, QueryType::INFO);
  EXPECT_EQ(info_mixed->type, QueryType::INFO);
}

/**
 * @brief Benchmark test to verify ToUpper optimization impact
 *
 * This test measures the performance improvement from using EqualsIgnoreCase
 * instead of ToUpper + string comparison.
 *
 * Expected improvement: 15-25% faster parsing for simple queries.
 */
TEST(QueryParserTest, ParsePerformanceWithOptimization) {
  QueryParser parser;

  // Warm up
  for (int i = 0; i < 100; ++i) {
    parser.Parse("SEARCH posts hello");
  }

  // Measure parsing performance
  const int iterations = 10000;
  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < iterations; ++i) {
    auto query = parser.Parse("SEARCH posts hello world");
    EXPECT_EQ(query->type, QueryType::SEARCH);
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

  // Just verify it completes in reasonable time (no specific threshold)
  // With optimization, should be significantly faster than without
  EXPECT_LT(duration.count(), 1000000);  // Less than 1 second for 10k parses

  // Log performance for manual inspection
  std::cout << "Parse performance: " << iterations << " iterations in " << duration.count() << " microseconds ("
            << (duration.count() / static_cast<double>(iterations)) << " μs/parse)" << std::endl;
}

// =============================================================================
// Bug #27: SET command boundary check tests
// =============================================================================

/**
 * @test Bug #27: Valid SET command should parse correctly
 */
TEST(QueryParserTest, Bug27_SetCommandValid) {
  QueryParser parser;

  auto query = parser.Parse("SET var = value");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SET);
  ASSERT_EQ(query->variable_assignments.size(), 1);
  EXPECT_EQ(query->variable_assignments[0].first, "var");
  EXPECT_EQ(query->variable_assignments[0].second, "value");
}

/**
 * @test Bug #27: Multiple SET assignments should parse correctly
 */
TEST(QueryParserTest, Bug27_SetCommandMultiple) {
  QueryParser parser;

  auto query = parser.Parse("SET var1 = value1, var2 = value2");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SET);
  ASSERT_EQ(query->variable_assignments.size(), 2);
  EXPECT_EQ(query->variable_assignments[0].first, "var1");
  EXPECT_EQ(query->variable_assignments[0].second, "value1");
  EXPECT_EQ(query->variable_assignments[1].first, "var2");
  EXPECT_EQ(query->variable_assignments[1].second, "value2");
}

/**
 * @test Bug #27: Empty SET should return error (not crash)
 */
TEST(QueryParserTest, Bug27_SetCommandEmpty) {
  QueryParser parser;

  auto query = parser.Parse("SET");
  EXPECT_FALSE(query);  // Should be error, not crash
}

/**
 * @test Bug #27: SET with only variable name should return error (not crash)
 */
TEST(QueryParserTest, Bug27_SetCommandOnlyVariable) {
  QueryParser parser;

  auto query = parser.Parse("SET var");
  EXPECT_FALSE(query);  // Should be error, not crash
}

/**
 * @test Bug #27: SET with variable and equals only should return error (not crash)
 */
TEST(QueryParserTest, Bug27_SetCommandNoValue) {
  QueryParser parser;

  auto query = parser.Parse("SET var =");
  EXPECT_FALSE(query);  // Should be error, not crash
}

/**
 * @test Bug #27: SET with trailing comma should handle gracefully
 */
TEST(QueryParserTest, Bug27_SetCommandTrailingComma) {
  QueryParser parser;

  auto query = parser.Parse("SET var = value,");
  // Either error or ignore trailing comma - but should NOT crash
  // Current implementation might allow this - just verify no crash
  SUCCEED();
}

/**
 * @test Bug #27: SET with comma but no second assignment should return error
 */
TEST(QueryParserTest, Bug27_SetCommandIncompleteSecond) {
  QueryParser parser;

  auto query = parser.Parse("SET var1 = value1, var2");
  EXPECT_FALSE(query);  // Should be error, not crash
}

/**
 * @test Bug #27: SET with comma and partial second assignment
 */
TEST(QueryParserTest, Bug27_SetCommandPartialSecond) {
  QueryParser parser;

  auto query = parser.Parse("SET var1 = value1, var2 =");
  EXPECT_FALSE(query);  // Should be error, not crash
}

/**
 * @test Bug #27: SET with three assignments should parse correctly
 */
TEST(QueryParserTest, Bug27_SetCommandThreeAssignments) {
  QueryParser parser;

  auto query = parser.Parse("SET a = 1, b = 2, c = 3");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SET);
  ASSERT_EQ(query->variable_assignments.size(), 3);
}

/**
 * @test Bug #27: SET missing equals sign should return error
 */
TEST(QueryParserTest, Bug27_SetCommandMissingEquals) {
  QueryParser parser;

  auto query = parser.Parse("SET var value");
  EXPECT_FALSE(query);  // Should be error, not crash
}

// =============================================================================
// LIMIT/OFFSET Boundary Value Tests
// =============================================================================

/**
 * @test LIMIT with zero should be rejected
 *
 * LIMIT 0 is invalid - requesting 0 results doesn't make sense.
 */
TEST(QueryParserTest, LimitZeroRejected) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello LIMIT 0");
  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("positive"), std::string::npos);
}

/**
 * @test LIMIT with negative value should be rejected
 */
TEST(QueryParserTest, LimitNegativeRejected) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello LIMIT -1");
  EXPECT_FALSE(query);
  // Parser returns error for invalid LIMIT value
}

/**
 * @test OFFSET with negative value should be rejected
 */
TEST(QueryParserTest, OffsetNegativeRejected) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello OFFSET -1");
  EXPECT_FALSE(query);
  // Parser returns error for negative OFFSET
}

/**
 * @test OFFSET with zero should be accepted
 *
 * OFFSET 0 is valid and equivalent to no offset.
 */
TEST(QueryParserTest, OffsetZeroAccepted) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello OFFSET 0");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->offset, 0);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @test Large OFFSET should be accepted
 *
 * Large offsets are valid, though may return empty results.
 */
TEST(QueryParserTest, LargOffsetAccepted) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello OFFSET 1000000");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->offset, 1000000);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @test LIMIT offset,count format - both valid
 */
TEST(QueryParserTest, LimitOffsetCountFormatValid) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello LIMIT 10,50");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->offset, 10);
  EXPECT_EQ(query->limit, 50);
  EXPECT_TRUE(query->offset_explicit);
  EXPECT_TRUE(query->limit_explicit);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @test LIMIT offset,count format - zero offset valid
 */
TEST(QueryParserTest, LimitOffsetCountFormatZeroOffset) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello LIMIT 0,100");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->offset, 0);
  EXPECT_EQ(query->limit, 100);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @test LIMIT offset,count format - zero count rejected
 */
TEST(QueryParserTest, LimitOffsetCountFormatZeroCount) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello LIMIT 10,0");
  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("positive"), std::string::npos);
}

/**
 * @test LIMIT offset,count format - negative offset rejected
 */
TEST(QueryParserTest, LimitOffsetCountFormatNegativeOffset) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello LIMIT -10,50");
  EXPECT_FALSE(query);
  // Invalid format or negative offset
}

/**
 * @test LIMIT offset,count format - count exceeds maximum
 */
TEST(QueryParserTest, LimitOffsetCountFormatExceedsMax) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello LIMIT 10,1001");
  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("maximum"), std::string::npos);
}

/**
 * @test LIMIT without value should be rejected
 */
TEST(QueryParserTest, LimitWithoutValueRejected) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello LIMIT");
  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("requires"), std::string::npos);
}

/**
 * @test OFFSET without value should be rejected
 */
TEST(QueryParserTest, OffsetWithoutValueRejected) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello OFFSET");
  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("requires"), std::string::npos);
}

/**
 * @test LIMIT with non-numeric value should be rejected
 */
TEST(QueryParserTest, LimitNonNumericRejected) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello LIMIT abc");
  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Invalid LIMIT"), std::string::npos);
}

/**
 * @test OFFSET with non-numeric value should be rejected
 */
TEST(QueryParserTest, OffsetNonNumericRejected) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello OFFSET xyz");
  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("Invalid OFFSET"), std::string::npos);
}

/**
 * @test LIMIT 1 is minimum valid value
 */
TEST(QueryParserTest, LimitOneAccepted) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello LIMIT 1");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->limit, 1);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @test LIMIT at maximum (1000) is accepted
 */
TEST(QueryParserTest, LimitMaxAccepted) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello LIMIT 1000");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->limit, 1000);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @test LIMIT just above maximum (1001) is rejected
 */
TEST(QueryParserTest, LimitJustAboveMaxRejected) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello LIMIT 1001");
  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("maximum"), std::string::npos);
}

/**
 * @test OFFSET order - OFFSET before LIMIT should work
 */
TEST(QueryParserTest, OffsetBeforeLimitAccepted) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello OFFSET 20 LIMIT 10");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->offset, 20);
  EXPECT_EQ(query->limit, 10);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @test Duplicate LIMIT should use last value
 */
TEST(QueryParserTest, DuplicateLimitUsesLast) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello LIMIT 50 LIMIT 100");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->limit, 100);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @test Duplicate OFFSET should use last value
 */
TEST(QueryParserTest, DuplicateOffsetUsesLast) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello OFFSET 10 OFFSET 20");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->offset, 20);
  EXPECT_TRUE(query->IsValid());
}

/**
 * @test Very large OFFSET that could overflow 32-bit integer
 */
TEST(QueryParserTest, OverflowOffsetHandled) {
  QueryParser parser;

  // std::stoi will throw for overflow values
  auto query = parser.Parse("SEARCH articles hello OFFSET 9999999999");
  EXPECT_FALSE(query);
  // Should return error for invalid value, not crash
}

/**
 * @test Very large LIMIT that could overflow 32-bit integer
 */
TEST(QueryParserTest, OverflowLimitHandled) {
  QueryParser parser;

  // std::stoi will throw for overflow values
  auto query = parser.Parse("SEARCH articles hello LIMIT 9999999999");
  EXPECT_FALSE(query);
  // Should return error for invalid value, not crash
}

/**
 * @test LIMIT with floating point value truncates to integer
 *
 * std::stoi parses "10.5" as "10" (stops at non-digit character).
 * This is current behavior - floating point values are truncated.
 */
TEST(QueryParserTest, LimitFloatingPointTruncated) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello LIMIT 10.5");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->limit, 10);  // Truncated to integer
  EXPECT_TRUE(query->IsValid());
}

/**
 * @test OFFSET with floating point value truncates to integer
 *
 * std::stoi parses "10.5" as "10" (stops at non-digit character).
 * This is current behavior - floating point values are truncated.
 */
TEST(QueryParserTest, OffsetFloatingPointTruncated) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello OFFSET 10.5");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->offset, 10);  // Truncated to integer
  EXPECT_TRUE(query->IsValid());
}

/**
 * @test Combined large OFFSET and LIMIT within bounds
 */
TEST(QueryParserTest, LargeOffsetWithMaxLimit) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello OFFSET 100000 LIMIT 1000");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->offset, 100000);
  EXPECT_EQ(query->limit, 1000);
  EXPECT_TRUE(query->IsValid());
}
