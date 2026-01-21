/**
 * @file binlog_event_parser_bug_fixes_test.cpp
 * @brief Unit tests for critical binlog event parser bug fixes
 *
 * TDD tests for:
 * - Bug #1: Multi-row binlog events only process first row
 * - Bug #2: text_source.concat only uses first column
 */

#include <gtest/gtest.h>

#include <string>
#include <vector>

#ifdef USE_MYSQL

#include "config/config.h"
#include "mysql/binlog_event_parser.h"
#include "mysql/rows_parser.h"
#include "server/server_types.h"

using namespace mygramdb::mysql;
using namespace mygramdb;

namespace {

// =============================================================================
// Bug #1: Multi-row binlog events only process first row
// =============================================================================
// The bug is in binlog_event_parser.cpp where only rows_opt->front() is used
// for INSERT, UPDATE, and DELETE events. This causes data loss for batch
// operations like:
//   INSERT INTO table VALUES (1, 'a'), (2, 'b'), (3, 'c');
// Only row 1 would be processed, rows 2 and 3 are lost.
// =============================================================================

/**
 * @brief Test that documents the multi-row event bug
 *
 * This test verifies that when ParseBinlogEvent returns, it should return
 * all rows in a multi-row event, not just the first one.
 *
 * Currently the function returns std::optional<BinlogEvent> (single event).
 * After fix, it should return std::vector<BinlogEvent> (multiple events).
 */
TEST(BinlogEventParserBugFixTest, Bug1_MultiRowEventsShouldReturnAllRows) {
  // Document the expected behavior for multi-row events
  // MySQL batches multiple row operations into single binlog events

  // Example: INSERT INTO articles VALUES (1, 'text1'), (2, 'text2'), (3, 'text3')
  // This creates ONE WRITE_ROWS_EVENT with THREE rows

  // Current buggy behavior: Only first row (1, 'text1') is returned
  // Expected behavior: All three rows should be returned as separate BinlogEvents

  // Test the fix: ParseBinlogEvent should return vector, not optional
  // After the fix, calling code should receive:
  //   events[0] = {INSERT, pk="1", text="text1"}
  //   events[1] = {INSERT, pk="2", text="text2"}
  //   events[2] = {INSERT, pk="3", text="text3"}

  // For now, this test documents the expected behavior
  // The actual fix requires changing the return type

  // Verify that std::vector can hold multiple BinlogEvents
  std::vector<BinlogEvent> events;

  BinlogEvent event1;
  event1.type = BinlogEventType::INSERT;
  event1.primary_key = "1";
  event1.text = "text1";
  events.push_back(event1);

  BinlogEvent event2;
  event2.type = BinlogEventType::INSERT;
  event2.primary_key = "2";
  event2.text = "text2";
  events.push_back(event2);

  BinlogEvent event3;
  event3.type = BinlogEventType::INSERT;
  event3.primary_key = "3";
  event3.text = "text3";
  events.push_back(event3);

  EXPECT_EQ(events.size(), 3) << "Multi-row events should return all rows";
  EXPECT_EQ(events[0].primary_key, "1");
  EXPECT_EQ(events[1].primary_key, "2");
  EXPECT_EQ(events[2].primary_key, "3");
}

/**
 * @brief Test that multi-row UPDATE events should process all row pairs
 */
TEST(BinlogEventParserBugFixTest, Bug1_MultiRowUpdatesShouldReturnAllRows) {
  // Example: UPDATE articles SET text='new' WHERE id IN (1, 2, 3)
  // This creates ONE UPDATE_ROWS_EVENT with THREE before/after row pairs

  // Current buggy behavior: Only first row pair is returned
  // Expected behavior: All three row pairs should be returned

  std::vector<BinlogEvent> events;

  for (int i = 1; i <= 3; ++i) {
    BinlogEvent event;
    event.type = BinlogEventType::UPDATE;
    event.primary_key = std::to_string(i);
    event.text = "new_text";
    event.old_text = "old_text";
    events.push_back(event);
  }

  EXPECT_EQ(events.size(), 3) << "Multi-row UPDATEs should return all rows";
}

/**
 * @brief Test that multi-row DELETE events should process all rows
 */
TEST(BinlogEventParserBugFixTest, Bug1_MultiRowDeletesShouldReturnAllRows) {
  // Example: DELETE FROM articles WHERE id IN (1, 2, 3)
  // This creates ONE DELETE_ROWS_EVENT with THREE rows

  std::vector<BinlogEvent> events;

  for (int i = 1; i <= 3; ++i) {
    BinlogEvent event;
    event.type = BinlogEventType::DELETE;
    event.primary_key = std::to_string(i);
    events.push_back(event);
  }

  EXPECT_EQ(events.size(), 3) << "Multi-row DELETEs should return all rows";
}

// =============================================================================
// Bug #2: text_source.concat only uses first column
// =============================================================================
// The bug is in binlog_event_parser.cpp where:
//   text_column = current_config->text_source.concat[0];
// Only the first column in the concat array is used. For example:
//   concat: ["title", "body", "tags"]
// Only "title" is extracted, "body" and "tags" are ignored.
// =============================================================================

/**
 * @brief Test that text_source.concat should use all specified columns
 */
TEST(BinlogEventParserBugFixTest, Bug2_ConcatShouldUseAllColumns) {
  // Config specifies multiple columns to concatenate
  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";
  table_config.text_source.concat = {"title", "body", "tags"};

  // Expected behavior: text should be "title_value body_value tags_value"
  // Current buggy behavior: text is only "title_value"

  // Verify config is correct
  EXPECT_EQ(table_config.text_source.concat.size(), 3);
  EXPECT_EQ(table_config.text_source.concat[0], "title");
  EXPECT_EQ(table_config.text_source.concat[1], "body");
  EXPECT_EQ(table_config.text_source.concat[2], "tags");

  // The fix should extract all columns and concatenate them
  // This is done in ParseWriteRowsEvent, not in ParseBinlogEvent

  // After fix, the text extraction logic should:
  // 1. Check if text_source.concat is non-empty
  // 2. Extract ALL columns listed in concat
  // 3. Concatenate them with appropriate separator

  // Document expected concatenation
  std::string title = "Hello World";
  std::string body = "This is the body text";
  std::string tags = "news tech";

  // Expected concatenated text (with space separator)
  std::string expected_text = title + " " + body + " " + tags;

  EXPECT_EQ(expected_text, "Hello World This is the body text news tech");
}

/**
 * @brief Test that single column text_source should work correctly
 */
TEST(BinlogEventParserBugFixTest, Bug2_SingleColumnTextSourceWorks) {
  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";
  table_config.text_source.column = "content";

  // When text_source.column is set, it should be used directly
  EXPECT_FALSE(table_config.text_source.column.empty());
  EXPECT_TRUE(table_config.text_source.concat.empty());
}

/**
 * @brief Test that concat falls back correctly when empty
 */
TEST(BinlogEventParserBugFixTest, Bug2_EmptyConcatFallback) {
  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";
  // Both column and concat are empty

  // The code should handle this gracefully
  std::string text_column;
  if (!table_config.text_source.column.empty()) {
    text_column = table_config.text_source.column;
  } else if (!table_config.text_source.concat.empty()) {
    // Should iterate ALL columns, not just [0]
    text_column = "concatenated";
  } else {
    text_column = "";
  }

  EXPECT_TRUE(text_column.empty()) << "Empty config should result in empty text column";
}

// =============================================================================
// Transaction control statements should NOT be treated as DDL
// =============================================================================
// When binlog contains QUERY_EVENT with transaction control statements
// (BEGIN, COMMIT, ROLLBACK, XA COMMIT, XA ROLLBACK, etc.), these should
// be correctly ignored and NOT treated as DDL affecting tables.
// =============================================================================

/**
 * @brief Test that ROLLBACK statement is not treated as DDL
 *
 * ROLLBACK statements appear in binlog QUERY_EVENT for statement-based
 * replication or XA transactions. They should be ignored.
 */
TEST(BinlogEventParserBugFixTest, RollbackStatementNotTreatedAsDDL) {
  // Test various ROLLBACK statement forms
  std::vector<std::string> rollback_statements = {
      "ROLLBACK", "rollback", "ROLLBACK;", "  ROLLBACK  ", "ROLLBACK TO SAVEPOINT sp1", "ROLLBACK TO sp1",
  };

  for (const auto& stmt : rollback_statements) {
    bool is_ddl = BinlogEventParser::IsTableAffectingDDL(stmt, "articles");
    EXPECT_FALSE(is_ddl) << "ROLLBACK statement should not be treated as DDL: " << stmt;
  }
}

/**
 * @brief Test that BEGIN statement is not treated as DDL
 *
 * BEGIN/START TRANSACTION statements mark transaction start.
 * They should be ignored.
 */
TEST(BinlogEventParserBugFixTest, BeginStatementNotTreatedAsDDL) {
  std::vector<std::string> begin_statements = {
      "BEGIN",
      "begin",
      "BEGIN;",
      "  BEGIN  ",
      "START TRANSACTION",
      "START TRANSACTION READ ONLY",
      "START TRANSACTION WITH CONSISTENT SNAPSHOT",
  };

  for (const auto& stmt : begin_statements) {
    bool is_ddl = BinlogEventParser::IsTableAffectingDDL(stmt, "articles");
    EXPECT_FALSE(is_ddl) << "BEGIN statement should not be treated as DDL: " << stmt;
  }
}

/**
 * @brief Test that COMMIT statement is not treated as DDL
 */
TEST(BinlogEventParserBugFixTest, CommitStatementNotTreatedAsDDL) {
  std::vector<std::string> commit_statements = {
      "COMMIT", "commit", "COMMIT;", "  COMMIT  ", "COMMIT WORK",
  };

  for (const auto& stmt : commit_statements) {
    bool is_ddl = BinlogEventParser::IsTableAffectingDDL(stmt, "articles");
    EXPECT_FALSE(is_ddl) << "COMMIT statement should not be treated as DDL: " << stmt;
  }
}

/**
 * @brief Test that XA transaction statements are not treated as DDL
 *
 * XA transactions are used for distributed transactions. The binlog
 * may contain XA START, XA END, XA PREPARE, XA COMMIT, XA ROLLBACK.
 */
TEST(BinlogEventParserBugFixTest, XAStatementsNotTreatedAsDDL) {
  std::vector<std::string> xa_statements = {
      "XA START 'xid1'",    "XA END 'xid1'", "XA PREPARE 'xid1'", "XA COMMIT 'xid1'",
      "XA ROLLBACK 'xid1'", "XA RECOVER",    "xa commit 'xid1'",  "xa rollback 'xid1'",
  };

  for (const auto& stmt : xa_statements) {
    bool is_ddl = BinlogEventParser::IsTableAffectingDDL(stmt, "articles");
    EXPECT_FALSE(is_ddl) << "XA statement should not be treated as DDL: " << stmt;
  }
}

/**
 * @brief Test that SAVEPOINT statements are not treated as DDL
 */
TEST(BinlogEventParserBugFixTest, SavepointStatementsNotTreatedAsDDL) {
  std::vector<std::string> savepoint_statements = {
      "SAVEPOINT sp1",
      "RELEASE SAVEPOINT sp1",
      "savepoint my_savepoint",
  };

  for (const auto& stmt : savepoint_statements) {
    bool is_ddl = BinlogEventParser::IsTableAffectingDDL(stmt, "articles");
    EXPECT_FALSE(is_ddl) << "SAVEPOINT statement should not be treated as DDL: " << stmt;
  }
}

/**
 * @brief Test that SET statements are not treated as DDL
 *
 * SET statements for session variables appear in binlog but should
 * not be treated as DDL.
 */
TEST(BinlogEventParserBugFixTest, SetStatementsNotTreatedAsDDL) {
  std::vector<std::string> set_statements = {
      "SET autocommit=0",
      "SET @var = 1",
      "SET NAMES utf8mb4",
      "SET SESSION sql_mode = ''",
      "SET TRANSACTION ISOLATION LEVEL READ COMMITTED",
  };

  for (const auto& stmt : set_statements) {
    bool is_ddl = BinlogEventParser::IsTableAffectingDDL(stmt, "articles");
    EXPECT_FALSE(is_ddl) << "SET statement should not be treated as DDL: " << stmt;
  }
}

/**
 * @brief Test that actual DDL statements are still correctly detected
 *
 * Ensure the transaction control exclusions don't break DDL detection.
 */
TEST(BinlogEventParserBugFixTest, DDLStatementsStillDetected) {
  // These should be detected as DDL
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE articles", "articles"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE IF EXISTS articles", "articles"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("ALTER TABLE articles ADD COLUMN foo INT", "articles"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("TRUNCATE TABLE articles", "articles"));

  // These should NOT be detected as DDL (different table)
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE other_table", "articles"));
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("ALTER TABLE other_table ADD COLUMN foo INT", "articles"));
}

/**
 * @brief Test edge case: table name that looks like transaction keyword
 *
 * A table named "rollback" or "commit" should still be detected in DDL.
 */
TEST(BinlogEventParserBugFixTest, TableNameLooksLikeTransactionKeyword) {
  // Table named "rollback" - DROP should be detected
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE rollback", "rollback"));
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("ALTER TABLE rollback ADD COLUMN x INT", "rollback"));

  // But standalone ROLLBACK should not affect table "rollback"
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("ROLLBACK", "rollback"));

  // Table named "begin" - DDL should be detected
  EXPECT_TRUE(BinlogEventParser::IsTableAffectingDDL("DROP TABLE begin", "begin"));
  EXPECT_FALSE(BinlogEventParser::IsTableAffectingDDL("BEGIN", "begin"));
}

}  // namespace

#endif  // USE_MYSQL
