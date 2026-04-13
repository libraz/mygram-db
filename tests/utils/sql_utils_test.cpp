/**
 * @file sql_utils_test.cpp
 * @brief Unit tests for SQL parsing utility functions
 */

#include "utils/sql_utils.h"

#include <gtest/gtest.h>

#include <string>

using namespace mygram::utils;

// --- StripSQLComments Tests ---

TEST(StripSQLCommentsTest, NoComments) {
  std::string sql = "SELECT * FROM users";
  EXPECT_EQ(StripSQLComments(sql), sql);
}

TEST(StripSQLCommentsTest, BlockComment) {
  std::string sql = "SELECT /* column list */ * FROM users";
  std::string result = StripSQLComments(sql);
  EXPECT_EQ(result, "SELECT  * FROM users");
}

TEST(StripSQLCommentsTest, LineComment) {
  std::string sql = "SELECT * FROM users -- fetch all\nWHERE id = 1";
  std::string result = StripSQLComments(sql);
  EXPECT_EQ(result, "SELECT * FROM users WHERE id = 1");
}

TEST(StripSQLCommentsTest, MultipleComments) {
  std::string sql = "/* start */ SELECT * /* mid */ FROM users -- end";
  std::string result = StripSQLComments(sql);
  // Block comments add space for word boundary preservation
  EXPECT_NE(result.find("SELECT"), std::string::npos);
  EXPECT_NE(result.find("FROM users"), std::string::npos);
}

TEST(StripSQLCommentsTest, UnterminatedBlockComment) {
  std::string sql = "SELECT /* no end";
  std::string result = StripSQLComments(sql);
  EXPECT_EQ(result, "SELECT ");
}

TEST(StripSQLCommentsTest, EmptyInput) {
  EXPECT_EQ(StripSQLComments(""), "");
}

// --- NormalizeWhitespace Tests ---

TEST(NormalizeWhitespaceTest, CollapseSpaces) {
  EXPECT_EQ(NormalizeWhitespace("a   b"), "a b");
}

TEST(NormalizeWhitespaceTest, CollapseTabs) {
  EXPECT_EQ(NormalizeWhitespace("a\t\tb"), "a b");
}

TEST(NormalizeWhitespaceTest, CollapseNewlines) {
  EXPECT_EQ(NormalizeWhitespace("a\n\n\nb"), "a b");
}

TEST(NormalizeWhitespaceTest, MixedWhitespace) {
  EXPECT_EQ(NormalizeWhitespace("a \t\n b"), "a b");
}

TEST(NormalizeWhitespaceTest, NoChange) {
  EXPECT_EQ(NormalizeWhitespace("a b c"), "a b c");
}

TEST(NormalizeWhitespaceTest, EmptyInput) {
  EXPECT_EQ(NormalizeWhitespace(""), "");
}

// --- SkipWhitespace Tests ---

TEST(SkipWhitespaceTest, SkipsSpaces) {
  std::string str = "   hello";
  size_t pos = 0;
  EXPECT_TRUE(SkipWhitespace(str, pos));
  EXPECT_EQ(pos, 3U);
}

TEST(SkipWhitespaceTest, NoWhitespace) {
  std::string str = "hello";
  size_t pos = 0;
  EXPECT_TRUE(SkipWhitespace(str, pos));
  EXPECT_EQ(pos, 0U);
}

TEST(SkipWhitespaceTest, AllWhitespace) {
  std::string str = "   ";
  size_t pos = 0;
  EXPECT_FALSE(SkipWhitespace(str, pos));
  EXPECT_EQ(pos, 3U);
}

TEST(SkipWhitespaceTest, EmptyString) {
  std::string str;
  size_t pos = 0;
  EXPECT_FALSE(SkipWhitespace(str, pos));
}

// --- MatchKeyword Tests ---

TEST(MatchKeywordTest, MatchAtStart) {
  std::string str = "SELECT * FROM";
  size_t pos = 0;
  EXPECT_TRUE(MatchKeyword(str, pos, "SELECT"));
  EXPECT_EQ(pos, 6U);
}

TEST(MatchKeywordTest, MatchAtMiddle) {
  std::string str = "SELECT * FROM USERS";
  size_t pos = 9;
  EXPECT_TRUE(MatchKeyword(str, pos, "FROM"));
  EXPECT_EQ(pos, 13U);
}

TEST(MatchKeywordTest, NoMatchPartial) {
  std::string str = "SELECTED";
  size_t pos = 0;
  // "SELECT" is a prefix of "SELECTED" so it should not match
  // (next char is not whitespace or backtick)
  EXPECT_FALSE(MatchKeyword(str, pos, "SELECT"));
  EXPECT_EQ(pos, 0U);
}

TEST(MatchKeywordTest, MatchAtEnd) {
  std::string str = "INSERT INTO USERS";
  size_t pos = 12;
  EXPECT_TRUE(MatchKeyword(str, pos, "USERS"));
  EXPECT_EQ(pos, 17U);
}

TEST(MatchKeywordTest, MatchFollowedByBacktick) {
  std::string str = "FROM`users`";
  size_t pos = 0;
  EXPECT_TRUE(MatchKeyword(str, pos, "FROM"));
  EXPECT_EQ(pos, 4U);
}

TEST(MatchKeywordTest, NotEnoughChars) {
  std::string str = "SEL";
  size_t pos = 0;
  EXPECT_FALSE(MatchKeyword(str, pos, "SELECT"));
}

// --- MatchTableName Tests ---

TEST(MatchTableNameTest, SimpleMatch) {
  std::string str = "USERS WHERE";
  size_t pos = 0;
  EXPECT_TRUE(MatchTableName(str, pos, "USERS"));
  EXPECT_EQ(pos, 5U);
}

TEST(MatchTableNameTest, BacktickQuoted) {
  std::string str = "`USERS` WHERE";
  size_t pos = 0;
  EXPECT_TRUE(MatchTableName(str, pos, "USERS"));
  EXPECT_EQ(pos, 7U);
}

TEST(MatchTableNameTest, PrefixDoesNotMatch) {
  std::string str = "USERS_ARCHIVE";
  size_t pos = 0;
  EXPECT_FALSE(MatchTableName(str, pos, "USERS"));
}

TEST(MatchTableNameTest, MatchAtEnd) {
  std::string str = "USERS";
  size_t pos = 0;
  EXPECT_TRUE(MatchTableName(str, pos, "USERS"));
  EXPECT_EQ(pos, 5U);
}

TEST(MatchTableNameTest, FollowedBySemicolon) {
  std::string str = "USERS;";
  size_t pos = 0;
  EXPECT_TRUE(MatchTableName(str, pos, "USERS"));
  EXPECT_EQ(pos, 5U);
}

// BUG-3 fix: pos restoration on failure
TEST(MatchTableNameBugFixTest, PosRestoredOnBacktickFailure) {
  std::string str = "`other_table` WHERE";
  size_t pos = 0;
  EXPECT_FALSE(MatchTableName(str, pos, "my_table"));
  EXPECT_EQ(pos, 0U) << "pos must be restored on backtick mismatch";
}

TEST(MatchTableNameBugFixTest, PosRestoredOnPlainFailure) {
  std::string str = "other_table WHERE";
  size_t pos = 0;
  EXPECT_FALSE(MatchTableName(str, pos, "my_table"));
  EXPECT_EQ(pos, 0U) << "pos must be restored on plain mismatch";
}

TEST(MatchTableNameBugFixTest, PosRestoredOnPrefixMatch) {
  std::string str = "users_extended WHERE";
  size_t pos = 0;
  EXPECT_FALSE(MatchTableName(str, pos, "users"));
  EXPECT_EQ(pos, 0U) << "pos must be restored when name is prefix of longer identifier";
}
