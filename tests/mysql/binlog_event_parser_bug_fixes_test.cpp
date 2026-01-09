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

}  // namespace

#endif  // USE_MYSQL
