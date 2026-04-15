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

  // After BUG-2 fix: error paths now return MakeUnexpected instead of partial Query
  EXPECT_FALSE(query.has_value());
}

/**
 * @brief Test COUNT missing arguments
 */
TEST(QueryParserTest, CountMissingArgs) {
  QueryParser parser;
  auto query = parser.Parse("COUNT articles");

  EXPECT_FALSE(query.has_value());
}

/**
 * @brief Test GET missing arguments
 */
TEST(QueryParserTest, GetMissingArgs) {
  QueryParser parser;

  auto query1 = parser.Parse("GET articles");
  EXPECT_FALSE(query1.has_value());

  auto query2 = parser.Parse("GET");
  EXPECT_FALSE(query2.has_value());
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
 * @brief Test DUMP SAVE with empty quoted string does not cause UB
 *
 * Regression test: an empty quoted argument (e.g., DUMP SAVE "") previously
 * caused undefined behavior by accessing token[0] on an empty string.
 */
TEST(QueryParserTest, DumpSaveEmptyQuotedString) {
  QueryParser parser;
  auto query = parser.Parse("DUMP SAVE \"\"");

  // Should parse successfully - empty string is treated as empty filepath
  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::DUMP_SAVE);
}

/**
 * @brief Test DUMP SAVE with empty token from consecutive spaces
 */
TEST(QueryParserTest, DumpSaveWithOnlyFlags) {
  QueryParser parser;
  auto query = parser.Parse("DUMP SAVE --with-stats");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::DUMP_SAVE);
  EXPECT_TRUE(query->dump_with_stats);
  EXPECT_TRUE(query->filepath.empty());
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
  // The parser strips the trailing comma from the value and continues the loop.
  // Since no more tokens follow, the loop exits with one valid assignment.
  ASSERT_TRUE(query.has_value());
  ASSERT_EQ(query->variable_assignments.size(), 1u);
  EXPECT_EQ(query->variable_assignments[0].first, "var");
  EXPECT_EQ(query->variable_assignments[0].second, "value");
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
 * @test LIMIT with floating point value is rejected
 *
 * std::from_chars requires the entire string to be a valid integer.
 * "10.5" is not a valid uint32_t, so it is rejected with an error.
 * (Previously, std::stoul silently truncated "10.5" to 10.)
 */
TEST(QueryParserTest, LimitFloatingPointRejected) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello LIMIT 10.5");
  EXPECT_FALSE(query);  // Rejected: not a valid integer
}

/**
 * @test OFFSET with floating point value is rejected
 *
 * std::from_chars requires the entire string to be a valid integer.
 * "10.5" is not a valid uint32_t, so it is rejected with an error.
 * (Previously, std::stoul silently truncated "10.5" to 10.)
 */
TEST(QueryParserTest, OffsetFloatingPointRejected) {
  QueryParser parser;

  auto query = parser.Parse("SEARCH articles hello OFFSET 10.5");
  EXPECT_FALSE(query);  // Rejected: not a valid integer
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

// =============================================================================
// Unicode whitespace tokenization tests (#4)
// =============================================================================

/**
 * @brief Test tokenization with full-width space (U+3000)
 */
TEST(QueryParserTest, TokenizeFullWidthSpace) {
  QueryParser parser;
  // U+3000 (Ideographic Space) = 0xE3 0x80 0x80
  auto query = parser.Parse("SEARCH articles hello\xE3\x80\x80world");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->table, "articles");
  // Full-width space should separate tokens, so "hello" becomes search_text
  // and "world" may become an additional term depending on parser behavior
  // At minimum, the search text should not contain the raw full-width space bytes
  EXPECT_EQ(query->search_text, "hello world");
}

/**
 * @brief Test tokenization with No-Break Space (U+00A0)
 */
TEST(QueryParserTest, TokenizeNoBreakSpace) {
  QueryParser parser;
  // U+00A0 (No-Break Space) = 0xC2 0xA0
  auto query = parser.Parse("SEARCH articles hello\xC2\xA0world");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->table, "articles");
  EXPECT_EQ(query->search_text, "hello world");
}

/**
 * @brief Test tokenization with Em Space (U+2003)
 */
TEST(QueryParserTest, TokenizeEmSpace) {
  QueryParser parser;
  // U+2003 (Em Space) = 0xE2 0x80 0x83
  auto query = parser.Parse("SEARCH articles hello\xE2\x80\x83world");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->table, "articles");
  EXPECT_EQ(query->search_text, "hello world");
}

/**
 * @brief Test tokenization with Ogham Space Mark (U+1680)
 */
TEST(QueryParserTest, TokenizeOghamSpaceMark) {
  QueryParser parser;
  // U+1680 (Ogham Space Mark) = 0xE1 0x9A 0x80
  auto query = parser.Parse("SEARCH articles hello\xE1\x9A\x80world");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SEARCH);
  EXPECT_EQ(query->table, "articles");
  EXPECT_EQ(query->search_text, "hello world");
}

TEST(QueryParserTest, FilterColumnNameTooLong) {
  QueryParser parser;
  parser.SetMaxQueryLength(0);  // Disable query length limit for this test
  std::string long_column(129, 'x');
  auto query = parser.Parse("SEARCH articles hello FILTER " + long_column + " = 1");

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("column name exceeds maximum length"), std::string::npos);
}

TEST(QueryParserTest, FilterValueTooLong) {
  QueryParser parser;
  parser.SetMaxQueryLength(0);  // Disable query length limit for this test
  std::string long_value(1025, 'y');
  auto query = parser.Parse("SEARCH articles hello FILTER status = " + long_value);

  EXPECT_FALSE(query);
  EXPECT_NE(query.error().message().find("value exceeds maximum length"), std::string::npos);
}

TEST(QueryParserTest, FilterColumnNameAtLimit) {
  QueryParser parser;
  parser.SetMaxQueryLength(0);  // Disable query length limit for this test
  std::string column_at_limit(128, 'x');
  auto query = parser.Parse("SEARCH articles hello FILTER " + column_at_limit + " = 1");

  ASSERT_TRUE(query);
  EXPECT_EQ(query->filters.size(), 1);
  EXPECT_EQ(query->filters[0].column, column_at_limit);
  EXPECT_EQ(query->filters[0].value, "1");
}

TEST(QueryParserTest, FilterValueAtLimit) {
  QueryParser parser;
  parser.SetMaxQueryLength(0);  // Disable query length limit for this test
  std::string value_at_limit(1024, 'y');
  auto query = parser.Parse("SEARCH articles hello FILTER status = " + value_at_limit);

  ASSERT_TRUE(query);
  EXPECT_EQ(query->filters.size(), 1);
  EXPECT_EQ(query->filters[0].column, "status");
  EXPECT_EQ(query->filters[0].value, value_at_limit);
}

// =============================================================================
// CountParensInToken (detail namespace) tests
// =============================================================================

/**
 * @brief Test CountParensInToken with no parentheses
 */
TEST(CountParensInTokenTest, NoParentheses) {
  auto [open, close] = mygramdb::query::detail::CountParensInToken("hello");
  EXPECT_EQ(open, 0);
  EXPECT_EQ(close, 0);
}

/**
 * @brief Test CountParensInToken with empty string
 */
TEST(CountParensInTokenTest, EmptyString) {
  auto [open, close] = mygramdb::query::detail::CountParensInToken("");
  EXPECT_EQ(open, 0);
  EXPECT_EQ(close, 0);
}

/**
 * @brief Test CountParensInToken with simple balanced parentheses
 */
TEST(CountParensInTokenTest, SimpleBalanced) {
  auto [open, close] = mygramdb::query::detail::CountParensInToken("(hello)");
  EXPECT_EQ(open, 1);
  EXPECT_EQ(close, 1);
}

/**
 * @brief Test CountParensInToken with nested parentheses
 */
TEST(CountParensInTokenTest, NestedParentheses) {
  auto [open, close] = mygramdb::query::detail::CountParensInToken("((a))");
  EXPECT_EQ(open, 2);
  EXPECT_EQ(close, 2);
}

/**
 * @brief Test CountParensInToken ignores parentheses inside double quotes
 */
TEST(CountParensInTokenTest, DoubleQuotedParensIgnored) {
  auto [open, close] = mygramdb::query::detail::CountParensInToken("\"(hello)\"");
  EXPECT_EQ(open, 0);
  EXPECT_EQ(close, 0);
}

/**
 * @brief Test CountParensInToken ignores parentheses inside single quotes
 */
TEST(CountParensInTokenTest, SingleQuotedParensIgnored) {
  auto [open, close] = mygramdb::query::detail::CountParensInToken("'(hello)'");
  EXPECT_EQ(open, 0);
  EXPECT_EQ(close, 0);
}

/**
 * @brief Test CountParensInToken with mixed quoted and unquoted parentheses
 */
TEST(CountParensInTokenTest, MixedQuotedAndUnquoted) {
  // ( outside quote, ")" inside quote, "(" inside quote, then bare (
  auto [open, close] = mygramdb::query::detail::CountParensInToken("(\")(\"(");
  EXPECT_EQ(open, 2);
  EXPECT_EQ(close, 0);
}

// ============================================================================
// LIMIT/OFFSET boundary tests
// ============================================================================

/**
 * @brief Test LIMIT with value at INT_MAX+1 (2147483648) — should be valid
 */
TEST(QueryParserTest, LimitAboveIntMax) {
  QueryParser parser;
  // 2147483648 exceeds INT_MAX but fits in uint32_t; however it exceeds kMaxLimit (1000)
  // so the query should be parsed but fail validation
  auto query = parser.Parse("SEARCH articles hello LIMIT 2147483648");
  // The LIMIT value exceeds kMaxLimit (1000), so it should fail
  EXPECT_FALSE(query);
}

/**
 * @brief Test LIMIT with negative value — should fail
 */
TEST(QueryParserTest, LimitNegativeValue) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT -1");
  EXPECT_FALSE(query);
}

/**
 * @brief Test LIMIT 0 — should fail
 */
TEST(QueryParserTest, LimitZero) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT 0");
  EXPECT_FALSE(query);
}

/**
 * @brief Test OFFSET with very large value — should be handled
 */
TEST(QueryParserTest, OffsetVeryLargeValue) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello OFFSET 4294967295");  // UINT32_MAX
  ASSERT_TRUE(query);
  EXPECT_EQ(query->offset, 4294967295U);
}

/**
 * @brief Test OFFSET with value exceeding uint32_t — should fail
 */
TEST(QueryParserTest, OffsetExceedsUint32Max) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello OFFSET 4294967296");  // UINT32_MAX + 1
  EXPECT_FALSE(query);
}

/**
 * @brief Test OFFSET with negative value — should fail
 */
TEST(QueryParserTest, OffsetNegativeValue) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello OFFSET -10");
  EXPECT_FALSE(query);
}

/**
 * @brief Test LIMIT with comma format and large values
 */
TEST(QueryParserTest, LimitCommaFormatLargeOffset) {
  QueryParser parser;
  // Large offset with small count — offset exceeds kMaxLimit only applies to count/limit
  auto query = parser.Parse("SEARCH articles hello LIMIT 100000,10");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->offset, 100000U);
  EXPECT_EQ(query->limit, 10U);
}

/**
 * @brief Test LIMIT comma format with negative offset — should fail
 */
TEST(QueryParserTest, LimitCommaFormatNegativeOffset) {
  QueryParser parser;
  auto query = parser.Parse("SEARCH articles hello LIMIT -1,10");
  EXPECT_FALSE(query);
}

/**
 * @brief Test case-insensitive CONFIG command
 */
TEST(QueryParserTest, ConfigCaseInsensitive) {
  QueryParser parser;
  auto query = parser.Parse("config show");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::CONFIG_SHOW);
}

/**
 * @brief Test case-insensitive REPLICATION command
 */
TEST(QueryParserTest, ReplicationCaseInsensitive) {
  QueryParser parser;
  auto query = parser.Parse("replication status");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::REPLICATION_STATUS);
}

/**
 * @brief Test case-insensitive SYNC command
 */
TEST(QueryParserTest, SyncCaseInsensitive) {
  QueryParser parser;
  auto query = parser.Parse("sync mytable");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::SYNC);
}

/**
 * @brief Test case-insensitive OPTIMIZE command
 */
TEST(QueryParserTest, OptimizeCaseInsensitive) {
  QueryParser parser;
  auto query = parser.Parse("optimize mytable");
  ASSERT_TRUE(query);
  EXPECT_EQ(query->type, QueryType::OPTIMIZE);
}

/**
 * @brief Test CountParensInToken with double-backslash before quote
 */
TEST(CountParensInTokenTest, DoubleBackslashBeforeQuote) {
  // \\\\" means backslash is escaped, quote should toggle
  // Input: \\"( — the \\ is an escaped backslash, " starts a quote, ( is inside
  // Actually, token = R"(\\\"()" with double backslash before quote means
  // the backslash is escaped and the quote IS significant
  auto [open, close] = mygramdb::query::detail::CountParensInToken(R"(\\"()");
  // \\ = escaped backslash, " = opens quote, ( = inside quote, no open parens counted
  EXPECT_EQ(open, 0);
  EXPECT_EQ(close, 0);
}

/**
 * @brief Test CountParensInToken with only open parentheses
 */
TEST(CountParensInTokenTest, OnlyOpenParens) {
  auto [open, close] = mygramdb::query::detail::CountParensInToken("((");
  EXPECT_EQ(open, 2);
  EXPECT_EQ(close, 0);
}

/**
 * @brief Test CountParensInToken with only close parentheses
 */
TEST(CountParensInTokenTest, OnlyCloseParens) {
  auto [open, close] = mygramdb::query::detail::CountParensInToken("))");
  EXPECT_EQ(open, 0);
  EXPECT_EQ(close, 2);
}

/**
 * @brief Test CountParensInToken with escaped quotes (backslash does not end quote)
 */
TEST(CountParensInTokenTest, EscapedQuoteDoesNotEndQuoteState) {
  // "\\\"(" - the \" is escaped, so the quote is not closed; ( is still inside quotes
  auto [open, close] = mygramdb::query::detail::CountParensInToken("\"\\\"(\"");
  EXPECT_EQ(open, 0);
  EXPECT_EQ(close, 0);
}

// ============================================================================
// Error return validation tests (BUG-2 fixes)
// ============================================================================

TEST(QueryParserBugFixTest, SearchMissingArgsReturnsError) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH");
  EXPECT_FALSE(result.has_value());
  auto result2 = parser.Parse("SEARCH articles");
  EXPECT_FALSE(result2.has_value());
}

TEST(QueryParserBugFixTest, CountMissingArgsReturnsError) {
  QueryParser parser;
  auto result = parser.Parse("COUNT");
  EXPECT_FALSE(result.has_value());
  auto result2 = parser.Parse("COUNT articles");
  EXPECT_FALSE(result2.has_value());
}

TEST(QueryParserBugFixTest, GetMissingArgsReturnsError) {
  QueryParser parser;
  auto result = parser.Parse("GET");
  EXPECT_FALSE(result.has_value());
  auto result2 = parser.Parse("GET articles");
  EXPECT_FALSE(result2.has_value());
}

TEST(QueryParserBugFixTest, SearchTooManyAndTermsRejected) {
  QueryParser parser;
  parser.SetMaxQueryLength(4096);
  std::string query = "SEARCH articles hello";
  for (int i = 0; i < 65; ++i) {
    query += " AND t" + std::to_string(i);
  }
  auto result = parser.Parse(query);
  EXPECT_FALSE(result.has_value());
}

TEST(QueryParserBugFixTest, SearchTooManyNotTermsRejected) {
  QueryParser parser;
  parser.SetMaxQueryLength(4096);
  std::string query = "SEARCH articles hello";
  for (int i = 0; i < 65; ++i) {
    query += " NOT t" + std::to_string(i);
  }
  auto result = parser.Parse(query);
  EXPECT_FALSE(result.has_value());
}

TEST(QueryParserBugFixTest, SearchAtLimitAndTermsAccepted) {
  QueryParser parser;
  parser.SetMaxQueryLength(4096);  // Allow long queries for this test
  std::string query = "SEARCH articles hello";
  for (int i = 0; i < 64; ++i) {
    query += " AND t" + std::to_string(i);
  }
  auto result = parser.Parse(query);
  EXPECT_TRUE(result.has_value());
}

// ============================================================
// HIGHLIGHT clause tests
// ============================================================

TEST(QueryParserHighlightTest, HighlightBasic) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH articles hello HIGHLIGHT");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->highlight.has_value());
  EXPECT_EQ(result->highlight->open_tag, "<em>");
  EXPECT_EQ(result->highlight->close_tag, "</em>");
  EXPECT_EQ(result->highlight->snippet_length, 100);
  EXPECT_EQ(result->highlight->max_fragments, 3);
}

TEST(QueryParserHighlightTest, HighlightWithCustomTags) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH articles hello HIGHLIGHT TAG <b> </b>");
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->highlight.has_value());
  EXPECT_EQ(result->highlight->open_tag, "<b>");
  EXPECT_EQ(result->highlight->close_tag, "</b>");
}

TEST(QueryParserHighlightTest, HighlightWithSnippetLen) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH articles hello HIGHLIGHT SNIPPET_LEN 200");
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->highlight.has_value());
  EXPECT_EQ(result->highlight->snippet_length, 200);
}

TEST(QueryParserHighlightTest, HighlightWithMaxFragments) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH articles hello HIGHLIGHT MAX_FRAGMENTS 5");
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->highlight.has_value());
  EXPECT_EQ(result->highlight->max_fragments, 5);
}

TEST(QueryParserHighlightTest, HighlightWithAllOptions) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH articles hello HIGHLIGHT TAG <mark> </mark> SNIPPET_LEN 50 MAX_FRAGMENTS 2");
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(result->highlight.has_value());
  EXPECT_EQ(result->highlight->open_tag, "<mark>");
  EXPECT_EQ(result->highlight->close_tag, "</mark>");
  EXPECT_EQ(result->highlight->snippet_length, 50);
  EXPECT_EQ(result->highlight->max_fragments, 2);
}

TEST(QueryParserHighlightTest, HighlightWithOtherClauses) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH articles hello AND world HIGHLIGHT LIMIT 10");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->highlight.has_value());
  EXPECT_EQ(result->and_terms.size(), 1);
  EXPECT_EQ(result->and_terms[0], "world");
  EXPECT_EQ(result->limit, 10);
}

TEST(QueryParserHighlightTest, HighlightAfterSort) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH articles hello SORT _score DESC HIGHLIGHT");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->highlight.has_value());
  EXPECT_TRUE(result->order_by.has_value());
  EXPECT_TRUE(result->order_by->IsScoreSort());
}

TEST(QueryParserHighlightTest, NoHighlightByDefault) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH articles hello");
  ASSERT_TRUE(result.has_value());
  EXPECT_FALSE(result->highlight.has_value());
}

TEST(QueryParserHighlightTest, HighlightInvalidSnippetLen) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH articles hello HIGHLIGHT SNIPPET_LEN 0");
  EXPECT_FALSE(result.has_value());
}

TEST(QueryParserHighlightTest, HighlightInvalidMaxFragments) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH articles hello HIGHLIGHT MAX_FRAGMENTS 0");
  EXPECT_FALSE(result.has_value());
}

TEST(QueryParserHighlightTest, HighlightTagMissingArgs) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH articles hello HIGHLIGHT TAG <b>");
  EXPECT_FALSE(result.has_value());
}

/**
 * @test HIGHLIGHT SNIPPET_LEN with floating point value is rejected
 *
 * std::from_chars requires the entire string to be a valid integer.
 * "10.5" is not a valid uint32_t, so it is rejected with an error.
 */
TEST(QueryParserHighlightTest, HighlightSnippetLenRejectsFloat) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH articles hello HIGHLIGHT SNIPPET_LEN 10.5");
  EXPECT_FALSE(result.has_value());
}

/**
 * @test HIGHLIGHT MAX_FRAGMENTS with floating point value is rejected
 */
TEST(QueryParserHighlightTest, HighlightMaxFragmentsRejectsFloat) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH articles hello HIGHLIGHT MAX_FRAGMENTS 3.7");
  EXPECT_FALSE(result.has_value());
}

// ============================================================
// FUZZY clause tests
// ============================================================

TEST(QueryParserFuzzyTest, FuzzyBasic) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH t \"term\" FUZZY");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->fuzzy_max_distance.has_value());
  EXPECT_EQ(*result->fuzzy_max_distance, 1);
}

TEST(QueryParserFuzzyTest, FuzzyWithDistance1) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH t \"term\" FUZZY 1");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->fuzzy_max_distance.has_value());
  EXPECT_EQ(*result->fuzzy_max_distance, 1);
}

TEST(QueryParserFuzzyTest, FuzzyWithDistance2) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH t \"term\" FUZZY 2");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->fuzzy_max_distance.has_value());
  EXPECT_EQ(*result->fuzzy_max_distance, 2);
}

TEST(QueryParserFuzzyTest, FuzzyInvalidDistance3) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH t \"term\" FUZZY 3");
  EXPECT_FALSE(result.has_value());
}

TEST(QueryParserFuzzyTest, FuzzyInvalidDistance0) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH t \"term\" FUZZY 0");
  EXPECT_FALSE(result.has_value());
}

TEST(QueryParserFuzzyTest, NoFuzzyByDefault) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH t \"term\"");
  ASSERT_TRUE(result.has_value());
  EXPECT_FALSE(result->fuzzy_max_distance.has_value());
}

TEST(QueryParserFuzzyTest, FuzzyWithOtherClauses) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH t \"term\" FUZZY 1 FILTER status = active LIMIT 10");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->fuzzy_max_distance.has_value());
  EXPECT_EQ(*result->fuzzy_max_distance, 1);
  EXPECT_FALSE(result->filters.empty());
  EXPECT_EQ(result->limit, 10);
}

TEST(QueryParserFuzzyTest, FuzzyWithHighlight) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH t \"term\" FUZZY 2 HIGHLIGHT");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->fuzzy_max_distance.has_value());
  EXPECT_EQ(*result->fuzzy_max_distance, 2);
  EXPECT_TRUE(result->highlight.has_value());
}

TEST(QueryParserFuzzyTest, FuzzyAfterAnd) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH t \"term\" AND \"other\" FUZZY 1");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->fuzzy_max_distance.has_value());
  EXPECT_EQ(*result->fuzzy_max_distance, 1);
  EXPECT_EQ(result->and_terms.size(), 1);
  EXPECT_EQ(result->and_terms[0], "other");
}

TEST(QueryParserFuzzyTest, FuzzyInvalidNonNumeric) {
  QueryParser parser;
  auto result = parser.Parse("SEARCH t \"term\" FUZZY abc");
  EXPECT_FALSE(result.has_value());
}

// BUG-4: from_chars instead of stoi - non-numeric after FUZZY is an error
TEST(QueryParserFuzzyTest, FuzzyNonNumericIsError) {
  // Non-numeric token after FUZZY should not be consumed as a number.
  // "nonsense" is not a valid keyword, so parser should error.
  QueryParser parser;
  auto result = parser.Parse("SEARCH t hello FUZZY nonsense");
  EXPECT_FALSE(result.has_value());
}

// ============================================================
// FACET command tests
// ============================================================

TEST(QueryParserFacetTest, FacetBasic) {
  QueryParser parser;
  auto result = parser.Parse("FACET t column");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->type, QueryType::FACET);
  EXPECT_EQ(result->table, "t");
  EXPECT_EQ(result->facet_column, "column");
  EXPECT_TRUE(result->search_text.empty());
}

TEST(QueryParserFacetTest, FacetWithSearch) {
  QueryParser parser;
  auto result = parser.Parse("FACET t column \"hello\"");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->type, QueryType::FACET);
  EXPECT_EQ(result->table, "t");
  EXPECT_EQ(result->facet_column, "column");
  EXPECT_EQ(result->search_text, "hello");
}

TEST(QueryParserFacetTest, FacetWithFilter) {
  QueryParser parser;
  auto result = parser.Parse("FACET t column FILTER status = active");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->type, QueryType::FACET);
  EXPECT_EQ(result->facet_column, "column");
  EXPECT_EQ(result->filters.size(), 1);
  EXPECT_EQ(result->filters[0].column, "status");
  EXPECT_EQ(result->filters[0].op, FilterOp::EQ);
  EXPECT_EQ(result->filters[0].value, "active");
}

TEST(QueryParserFacetTest, FacetWithLimit) {
  QueryParser parser;
  auto result = parser.Parse("FACET t column LIMIT 5");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->type, QueryType::FACET);
  EXPECT_EQ(result->limit, 5);
  EXPECT_TRUE(result->limit_explicit);
}

TEST(QueryParserFacetTest, FacetMissingColumn) {
  QueryParser parser;
  auto result = parser.Parse("FACET t");
  EXPECT_FALSE(result.has_value());
}

TEST(QueryParserFacetTest, FacetMissingTable) {
  QueryParser parser;
  auto result = parser.Parse("FACET");
  EXPECT_FALSE(result.has_value());
}

TEST(QueryParserFacetTest, FacetWithSearchAndFilter) {
  QueryParser parser;
  auto result = parser.Parse("FACET t column \"hello\" FILTER status = active LIMIT 10");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->type, QueryType::FACET);
  EXPECT_EQ(result->table, "t");
  EXPECT_EQ(result->facet_column, "column");
  EXPECT_EQ(result->search_text, "hello");
  EXPECT_EQ(result->filters.size(), 1);
  EXPECT_EQ(result->filters[0].column, "status");
  EXPECT_EQ(result->filters[0].value, "active");
  EXPECT_EQ(result->limit, 10);
  EXPECT_TRUE(result->limit_explicit);
}

TEST(QueryParserFacetTest, FacetWithAndNot) {
  QueryParser parser;
  auto result = parser.Parse("FACET t column \"hello\" AND \"world\" NOT \"bad\"");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->type, QueryType::FACET);
  EXPECT_EQ(result->search_text, "hello");
  EXPECT_EQ(result->and_terms.size(), 1);
  EXPECT_EQ(result->and_terms[0], "world");
  EXPECT_EQ(result->not_terms.size(), 1);
  EXPECT_EQ(result->not_terms[0], "bad");
}

// DESIGN-2: facet_column length validation
TEST(QueryParserFacetTest, FacetColumnNameTooLong) {
  QueryParser parser;
  std::string long_name(200, 'x');
  auto result = parser.Parse("FACET t " + long_name);
  EXPECT_FALSE(result.has_value());
  EXPECT_NE(result.error().message().find("maximum length"), std::string::npos);
}
