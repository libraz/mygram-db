/**
 * @file binlog_reader_events_test.cpp
 * @brief Unit tests for binlog reader - Event processing (INSERT/UPDATE/DELETE/DDL)
 */

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>

#define private public
#define protected public
#include "mysql/binlog_reader.h"
#undef private
#undef protected

#ifdef USE_MYSQL

#include "mock_connection.h"
#include "mysql/binlog_filter_evaluator.h"
#include "server/server_stats.h"
#include "server/server_types.h"

using namespace mygramdb::mysql;
using namespace mygramdb;

namespace {

/**
 * @brief Helper that creates a default table configuration for tests
 */
config::TableConfig MakeDefaultTableConfig() {
  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";
  table_config.text_source.column = "content";

  config::RequiredFilterConfig required_filter;
  required_filter.name = "status";
  required_filter.type = "int";
  required_filter.op = "=";
  required_filter.value = "1";
  table_config.required_filters.push_back(required_filter);

  config::FilterConfig optional_filter;
  optional_filter.name = "category";
  optional_filter.type = "string";
  table_config.filters.push_back(optional_filter);

  return table_config;
}

/**
 * @brief BinlogReader test fixture providing in-memory dependencies
 */
class BinlogReaderFixture : public ::testing::Test {
 protected:
  BinlogReaderFixture() : connection_(connection_config_), index_(2) {}

  void SetUp() override {
    table_config_ = MakeDefaultTableConfig();
    reader_config_.start_gtid = "uuid:1";
    reader_config_.queue_size = 2;
    reader_config_.reconnect_delay_ms = 10;
    reader_config_.server_id = 12345;  // Test server ID

    index_.Clear();
    doc_store_.Clear();
    ResetReader();
  }

  void TearDown() override {
    reader_.reset();
    doc_store_.Clear();
    index_.Clear();
  }

  /**
   * @brief Recreate BinlogReader with current configuration
   */
  void ResetReader() {
    config::MysqlConfig mysql_config;  // Use default (UTC timezone)
    reader_ =
        std::make_unique<BinlogReader>(connection_, index_, doc_store_, table_config_, mysql_config, reader_config_);
  }

  /**
   * @brief Utility to build a fully populated event for tests
   */
  BinlogEvent MakeEvent(BinlogEventType type, const std::string& pk, int status, const std::string& text = "hello") {
    BinlogEvent event;
    event.type = type;
    event.table_name = table_config_.name;
    event.primary_key = pk;
    event.text = text;
    event.gtid = "uuid:" + pk;
    event.filters["status"] = static_cast<int64_t>(status);
    event.filters["category"] = std::string("news");
    return event;
  }

  Connection::Config connection_config_;
  Connection connection_;
  index::Index index_;
  storage::DocumentStore doc_store_;
  config::TableConfig table_config_;
  BinlogReader::Config reader_config_;
  std::unique_ptr<BinlogReader> reader_;
};

}  // namespace

/**
 * @brief Validate INSERT events create documents when filters match
 */
TEST_F(BinlogReaderFixture, ProcessInsertAddsDocument) {
  BinlogEvent insert_event = MakeEvent(BinlogEventType::INSERT, "42", 1, "Breaking news");
  ASSERT_TRUE(reader_->ProcessEvent(insert_event));

  auto doc_id = doc_store_.GetDocId("42");
  ASSERT_TRUE(doc_id.has_value());
  auto stored_doc = doc_store_.GetDocument(doc_id.value());
  ASSERT_TRUE(stored_doc.has_value());
  EXPECT_EQ(std::get<std::string>(stored_doc->filters["category"]), "news");
}

/**
 * @brief Ensure UPDATE removes rows when they no longer satisfy required filters
 */
TEST_F(BinlogReaderFixture, ProcessUpdateRemovesWhenFiltersFail) {
  ASSERT_TRUE(reader_->ProcessEvent(MakeEvent(BinlogEventType::INSERT, "90", 1, "Initial")));

  BinlogEvent update_event = MakeEvent(BinlogEventType::UPDATE, "90", 0, "Updated text");
  ASSERT_TRUE(reader_->ProcessEvent(update_event));
  EXPECT_FALSE(doc_store_.GetDocId("90").has_value());
}

/**
 * @brief Test UPDATE properly updates full-text index when text changes
 *
 * Verifies that when an UPDATE event changes the text content:
 * 1. The old text is removed from the index using old_text field
 * 2. The new text is added to the index
 * 3. Document store filters are updated
 */
TEST_F(BinlogReaderFixture, ProcessUpdateUpdatesIndexWithTextChange) {
  // Insert initial document with text "hello world"
  ASSERT_TRUE(reader_->ProcessEvent(MakeEvent(BinlogEventType::INSERT, "100", 1, "hello world")));

  auto doc_id = doc_store_.GetDocId("100");
  ASSERT_TRUE(doc_id.has_value());

  // Verify initial text is in the index (bigram "he" from "hello")
  EXPECT_GT(index_.Count("he"), 0);

  // Create UPDATE event with new text "goodbye universe"
  BinlogEvent update_event = MakeEvent(BinlogEventType::UPDATE, "100", 1, "goodbye universe");
  update_event.old_text = "hello world";  // Set old_text for index update

  ASSERT_TRUE(reader_->ProcessEvent(update_event));

  // Verify document still exists (not removed and re-added)
  auto updated_doc_id = doc_store_.GetDocId("100");
  ASSERT_TRUE(updated_doc_id.has_value());
  EXPECT_EQ(updated_doc_id.value(), doc_id.value());

  // Verify old text was removed from index (bigram "he" from "hello" should be gone)
  EXPECT_EQ(index_.Count("he"), 0);

  // Verify new text was added to index (bigram "go" from "goodbye" should exist)
  EXPECT_GT(index_.Count("go"), 0);

  // Verify filters were updated
  auto stored_doc = doc_store_.GetDocument(doc_id.value());
  ASSERT_TRUE(stored_doc.has_value());
  EXPECT_EQ(std::get<std::string>(stored_doc->filters["category"]), "news");
  EXPECT_EQ(std::get<int64_t>(stored_doc->filters["status"]), 1);
}

/**
 * @brief Test UPDATE handles empty old_text gracefully
 *
 * Ensures that if old_text is empty (shouldn't happen in practice with proper
 * before image parsing, but defensive), the update still works and adds new text.
 */
TEST_F(BinlogReaderFixture, ProcessUpdateHandlesEmptyOldText) {
  // Insert initial document
  ASSERT_TRUE(reader_->ProcessEvent(MakeEvent(BinlogEventType::INSERT, "101", 1, "original text")));

  auto doc_id = doc_store_.GetDocId("101");
  ASSERT_TRUE(doc_id.has_value());

  // Create UPDATE event with empty old_text
  BinlogEvent update_event = MakeEvent(BinlogEventType::UPDATE, "101", 1, "newtext");
  update_event.old_text = "";  // Empty old_text

  // Should still process successfully
  ASSERT_TRUE(reader_->ProcessEvent(update_event));

  // Verify document still exists
  auto updated_doc_id = doc_store_.GetDocId("101");
  ASSERT_TRUE(updated_doc_id.has_value());
  EXPECT_EQ(updated_doc_id.value(), doc_id.value());

  // Verify new text was added to index (bigram "ne" from "newtext")
  EXPECT_GT(index_.Count("ne"), 0);

  // Verify filters are preserved
  auto stored_doc = doc_store_.GetDocument(doc_id.value());
  ASSERT_TRUE(stored_doc.has_value());
  EXPECT_EQ(std::get<std::string>(stored_doc->filters["category"]), "news");
}

/**
 * @brief Test UPDATE when only filters change (no text change)
 *
 * Verifies that UPDATE correctly handles cases where only filter values change
 * but the text content remains the same. Index should update (remove old, add same)
 * but content remains searchable.
 */
TEST_F(BinlogReaderFixture, ProcessUpdateOnlyFiltersChange) {
  // Insert initial document
  BinlogEvent insert_event = MakeEvent(BinlogEventType::INSERT, "102", 1, "sametext");
  insert_event.filters["category"] = std::string("sports");
  ASSERT_TRUE(reader_->ProcessEvent(insert_event));

  auto doc_id = doc_store_.GetDocId("102");
  ASSERT_TRUE(doc_id.has_value());

  // Verify initial category and text is indexed (bigram "sa" from "sametext")
  auto initial_doc = doc_store_.GetDocument(doc_id.value());
  ASSERT_TRUE(initial_doc.has_value());
  EXPECT_EQ(std::get<std::string>(initial_doc->filters["category"]), "sports");
  EXPECT_GT(index_.Count("sa"), 0);

  // Update with same text but different filter
  BinlogEvent update_event = MakeEvent(BinlogEventType::UPDATE, "102", 1, "sametext");
  update_event.old_text = "sametext";                      // Same text
  update_event.filters["category"] = std::string("news");  // Different category

  ASSERT_TRUE(reader_->ProcessEvent(update_event));

  // Verify document still exists (same doc_id)
  auto updated_doc_id = doc_store_.GetDocId("102");
  ASSERT_TRUE(updated_doc_id.has_value());
  EXPECT_EQ(updated_doc_id.value(), doc_id.value());

  // Verify text is still in index (same text was removed and re-added)
  EXPECT_GT(index_.Count("sa"), 0);

  // Verify filters were updated
  auto stored_doc = doc_store_.GetDocument(doc_id.value());
  ASSERT_TRUE(stored_doc.has_value());
  EXPECT_EQ(std::get<std::string>(stored_doc->filters["category"]), "news");
}

/**
 * @brief Verify DELETE events remove documents and index entries
 */
TEST_F(BinlogReaderFixture, ProcessDeleteRemovesDocument) {
  ASSERT_TRUE(reader_->ProcessEvent(MakeEvent(BinlogEventType::INSERT, "77", 1, "Row")));

  BinlogEvent delete_event = MakeEvent(BinlogEventType::DELETE, "77", 1, "Row");
  ASSERT_TRUE(reader_->ProcessEvent(delete_event));
  EXPECT_FALSE(doc_store_.GetDocId("77").has_value());
}

/**
 * @brief Validate DDL TRUNCATE clears index and store
 */
TEST_F(BinlogReaderFixture, ProcessDdlTruncateClearsState) {
  ASSERT_TRUE(reader_->ProcessEvent(MakeEvent(BinlogEventType::INSERT, "5", 1, "Body")));
  EXPECT_EQ(doc_store_.Size(), 1);

  BinlogEvent ddl_event;
  ddl_event.type = BinlogEventType::DDL;
  ddl_event.table_name = table_config_.name;
  ddl_event.text = "TRUNCATE TABLE articles";
  ASSERT_TRUE(reader_->ProcessEvent(ddl_event));
  EXPECT_EQ(doc_store_.Size(), 0);
}

/**
 * @brief Confirm events missing required filters are skipped
 */
TEST_F(BinlogReaderFixture, SkipsEventsMissingRequiredFilters) {
  BinlogEvent insert_event = MakeEvent(BinlogEventType::INSERT, "21", 1, "Text");
  insert_event.filters.erase("status");
  ASSERT_TRUE(reader_->ProcessEvent(insert_event));
  EXPECT_FALSE(doc_store_.GetDocId("21").has_value());
}

/**
 * @brief Exercise GTID setters/getters
 */
TEST_F(BinlogReaderFixture, TracksGtidUpdates) {
  reader_->SetCurrentGTID("uuid:10");
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:10");
  reader_->UpdateCurrentGTID("uuid:11");
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:11");
}

/**
 * @brief Test BinlogEvent with filters
 */
TEST(BinlogReaderTest, EventWithFilters) {
  BinlogEvent event;
  event.type = BinlogEventType::INSERT;
  event.table_name = "articles";
  event.primary_key = "456";
  event.text = "article text";

  // Add filters
  event.filters["status"] = static_cast<int64_t>(1);
  event.filters["category"] = std::string("news");

  EXPECT_EQ(event.filters.size(), 2);

  auto status = std::get<int64_t>(event.filters["status"]);
  auto category = std::get<std::string>(event.filters["category"]);

  EXPECT_EQ(status, 1);
  EXPECT_EQ(category, "news");
}

/**
 * @brief Test multiple event types
 */
TEST(BinlogReaderTest, MultipleEventTypes) {
  BinlogEvent insert_event;
  insert_event.type = BinlogEventType::INSERT;
  insert_event.primary_key = "1";

  BinlogEvent update_event;
  update_event.type = BinlogEventType::UPDATE;
  update_event.primary_key = "2";

  BinlogEvent delete_event;
  delete_event.type = BinlogEventType::DELETE;
  delete_event.primary_key = "3";

  EXPECT_EQ(insert_event.type, BinlogEventType::INSERT);
  EXPECT_EQ(update_event.type, BinlogEventType::UPDATE);
  EXPECT_EQ(delete_event.type, BinlogEventType::DELETE);

  EXPECT_NE(insert_event.primary_key, update_event.primary_key);
  EXPECT_NE(update_event.primary_key, delete_event.primary_key);
}

/**
 * @brief Test DDL event type
 */
TEST(BinlogReaderTest, DDLEventType) {
  BinlogEvent ddl_event;
  ddl_event.type = BinlogEventType::DDL;
  ddl_event.table_name = "test_table";
  ddl_event.text = "TRUNCATE TABLE test_table";

  EXPECT_EQ(ddl_event.type, BinlogEventType::DDL);
  EXPECT_EQ(ddl_event.table_name, "test_table");
  EXPECT_EQ(ddl_event.text, "TRUNCATE TABLE test_table");

  // DDL events should be distinct from other event types
  EXPECT_NE(BinlogEventType::DDL, BinlogEventType::INSERT);
  EXPECT_NE(BinlogEventType::DDL, BinlogEventType::UPDATE);
  EXPECT_NE(BinlogEventType::DDL, BinlogEventType::DELETE);
  EXPECT_NE(BinlogEventType::DDL, BinlogEventType::UNKNOWN);
}

/**
 * @brief Test TRUNCATE TABLE DDL event
 */
TEST(BinlogReaderTest, TruncateTableEvent) {
  BinlogEvent event;
  event.type = BinlogEventType::DDL;
  event.table_name = "articles";
  event.text = "TRUNCATE TABLE articles";

  EXPECT_EQ(event.type, BinlogEventType::DDL);
  EXPECT_NE(event.text.find("TRUNCATE"), std::string::npos);
}

/**
 * @brief Test ALTER TABLE DDL event
 */
TEST(BinlogReaderTest, AlterTableEvent) {
  BinlogEvent event;
  event.type = BinlogEventType::DDL;
  event.table_name = "users";
  event.text = "ALTER TABLE users ADD COLUMN email VARCHAR(255)";

  EXPECT_EQ(event.type, BinlogEventType::DDL);
  EXPECT_NE(event.text.find("ALTER"), std::string::npos);
}

/**
 * @brief Test DROP TABLE DDL event
 */
TEST(BinlogReaderTest, DropTableEvent) {
  BinlogEvent event;
  event.type = BinlogEventType::DDL;
  event.table_name = "temp_table";
  event.text = "DROP TABLE temp_table";

  EXPECT_EQ(event.type, BinlogEventType::DDL);
  EXPECT_NE(event.text.find("DROP"), std::string::npos);
}

/**
 * @brief Test DDL event with GTID
 */
TEST(BinlogReaderTest, DDLEventWithGTID) {
  BinlogEvent event;
  event.type = BinlogEventType::DDL;
  event.table_name = "products";
  event.text = "TRUNCATE TABLE products";
  event.gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:150";

  EXPECT_EQ(event.type, BinlogEventType::DDL);
  EXPECT_EQ(event.gtid, "3E11FA47-71CA-11E1-9E33-C80AA9429562:150");
  EXPECT_FALSE(event.gtid.empty());
}

/**
 * @brief Test various DDL statement formats
 */
TEST(BinlogReaderTest, VariousDDLFormats) {
  // Test case variations
  BinlogEvent truncate_upper;
  truncate_upper.type = BinlogEventType::DDL;
  truncate_upper.text = "TRUNCATE TABLE MY_TABLE";
  EXPECT_NE(truncate_upper.text.find("TRUNCATE"), std::string::npos);

  BinlogEvent truncate_lower;
  truncate_lower.type = BinlogEventType::DDL;
  truncate_lower.text = "truncate table my_table";
  EXPECT_NE(truncate_lower.text.find("truncate"), std::string::npos);

  BinlogEvent alter_add_column;
  alter_add_column.type = BinlogEventType::DDL;
  alter_add_column.text = "ALTER TABLE users ADD COLUMN status INT";
  EXPECT_NE(alter_add_column.text.find("ALTER"), std::string::npos);

  BinlogEvent alter_modify_column;
  alter_modify_column.type = BinlogEventType::DDL;
  alter_modify_column.text = "ALTER TABLE users MODIFY COLUMN name VARCHAR(100)";
  EXPECT_NE(alter_modify_column.text.find("MODIFY"), std::string::npos);

  BinlogEvent drop_if_exists;
  drop_if_exists.type = BinlogEventType::DDL;
  drop_if_exists.text = "DROP TABLE IF EXISTS temp_table";
  EXPECT_NE(drop_if_exists.text.find("DROP"), std::string::npos);
}

/**
 * @brief Test DDL event distinguishing from DML events
 */
TEST(BinlogReaderTest, DDLvsDMLEvents) {
  BinlogEvent dml_insert;
  dml_insert.type = BinlogEventType::INSERT;
  dml_insert.primary_key = "100";
  dml_insert.text = "new record text";

  BinlogEvent ddl_truncate;
  ddl_truncate.type = BinlogEventType::DDL;
  ddl_truncate.text = "TRUNCATE TABLE test_table";

  // DDL events don't have primary keys (they affect entire table)
  EXPECT_FALSE(dml_insert.primary_key.empty());
  EXPECT_TRUE(ddl_truncate.primary_key.empty());

  // DDL events store SQL query in text field
  EXPECT_EQ(dml_insert.type, BinlogEventType::INSERT);
  EXPECT_EQ(ddl_truncate.type, BinlogEventType::DDL);
}

/**
 * @brief Test exception handling in filter value parsing
 * Regression test for: std::stod() and std::stoull() had no exception handling
 */
TEST(BinlogReaderFilterTest, InvalidFilterValueExceptionHandling) {
  // Test invalid float value (stod exception)
  config::RequiredFilterConfig float_filter;
  float_filter.name = "score";
  float_filter.type = "double";
  float_filter.op = "=";
  float_filter.value = "not_a_number";  // Invalid float

  storage::FilterValue test_value = 3.14;

  // Should not crash, should return false
  bool result = BinlogFilterEvaluator::CompareFilterValue(test_value, float_filter);
  EXPECT_FALSE(result);

  // Test invalid datetime value (stoull exception)
  config::RequiredFilterConfig datetime_filter;
  datetime_filter.name = "created_at";
  datetime_filter.type = "unsigned";
  datetime_filter.op = "=";
  datetime_filter.value = "invalid_timestamp";  // Invalid uint64

  storage::FilterValue datetime_value = uint64_t(1234567890);

  // Should not crash, should return false
  result = BinlogFilterEvaluator::CompareFilterValue(datetime_value, datetime_filter);
  EXPECT_FALSE(result);
}

/**
 * @brief Test filter value size validation (security: memory exhaustion protection)
 */
TEST_F(BinlogReaderFixture, FilterValueSizeValidation) {
  // Normal size filter value should work
  config::RequiredFilterConfig normal_filter;
  normal_filter.name = "status";
  normal_filter.op = "=";
  normal_filter.value = "active";  // Small value

  storage::FilterValue test_value = std::string("active");
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(test_value, normal_filter))
      << "Normal-sized filter value should be accepted";

  // Large but acceptable filter value (< 1MB)
  config::RequiredFilterConfig large_filter;
  large_filter.name = "description";
  large_filter.op = "=";
  large_filter.value = std::string(100 * 1024, 'x');  // 100KB

  storage::FilterValue large_test_value = large_filter.value;
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(large_test_value, large_filter))
      << "Large filter value (100KB) should be accepted";

  // Oversized filter value (> 1MB) should be rejected
  config::RequiredFilterConfig oversized_filter;
  oversized_filter.name = "malicious";
  oversized_filter.op = "=";
  oversized_filter.value = std::string(2 * 1024 * 1024, 'x');  // 2MB (exceeds limit)

  storage::FilterValue oversized_test_value = std::string("test");
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(oversized_test_value, oversized_filter))
      << "Oversized filter value (2MB) should be rejected for security";

  // Edge case: exactly at limit (1MB)
  config::RequiredFilterConfig edge_filter;
  edge_filter.name = "edge_case";
  edge_filter.op = "=";
  edge_filter.value = std::string(1024 * 1024, 'y');  // Exactly 1MB

  storage::FilterValue edge_test_value = edge_filter.value;
  EXPECT_TRUE(BinlogFilterEvaluator::CompareFilterValue(edge_test_value, edge_filter))
      << "Filter value at exact limit (1MB) should be accepted";

  // Just over limit (1MB + 1 byte)
  config::RequiredFilterConfig just_over_filter;
  just_over_filter.name = "just_over";
  just_over_filter.op = "=";
  just_over_filter.value = std::string(1024 * 1024 + 1, 'z');  // 1MB + 1 byte

  storage::FilterValue just_over_test_value = std::string("test");
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(just_over_test_value, just_over_filter))
      << "Filter value just over limit (1MB+1) should be rejected";
}

/**
 * @brief Test filter value size validation with different data types
 */
TEST_F(BinlogReaderFixture, FilterValueSizeValidationTypes) {
  // Integer filter with oversized string representation
  config::RequiredFilterConfig int_filter;
  int_filter.name = "number";
  int_filter.op = "=";
  int_filter.value = std::string(2 * 1024 * 1024, '9');  // 2MB of '9's

  storage::FilterValue int_value = int64_t(999);
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(int_value, int_filter))
      << "Oversized integer filter value string should be rejected";

  // Double filter with oversized string representation
  config::RequiredFilterConfig double_filter;
  double_filter.name = "price";
  double_filter.op = "=";
  double_filter.value = std::string(2 * 1024 * 1024, '1');  // 2MB

  storage::FilterValue double_value = 123.45;
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(double_value, double_filter))
      << "Oversized double filter value string should be rejected";

  // Datetime filter with oversized string representation
  config::RequiredFilterConfig datetime_filter;
  datetime_filter.name = "created_at";
  datetime_filter.op = "=";
  datetime_filter.value = std::string(2 * 1024 * 1024, '2');  // 2MB

  storage::FilterValue datetime_value = uint64_t(1234567890);
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(datetime_value, datetime_filter))
      << "Oversized datetime filter value string should be rejected";
}

/**
 * @brief Test that NULL checks work regardless of filter value size
 */
TEST_F(BinlogReaderFixture, FilterValueSizeValidationNullChecks) {
  // IS NULL should work even with oversized filter value
  config::RequiredFilterConfig null_filter;
  null_filter.name = "deleted_at";
  null_filter.op = "IS NULL";
  null_filter.value = std::string(2 * 1024 * 1024, 'x');  // Oversized (but ignored for IS NULL)

  storage::FilterValue null_value = std::monostate{};
  // IS NULL doesn't use filter.value, so size check happens but doesn't affect NULL check
  // The function returns false early due to size check before reaching NULL logic
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(null_value, null_filter))
      << "Oversized filter value should be rejected even for NULL checks";

  // IS NOT NULL should also be affected by size validation
  config::RequiredFilterConfig not_null_filter;
  not_null_filter.name = "updated_at";
  not_null_filter.op = "IS NOT NULL";
  not_null_filter.value = std::string(2 * 1024 * 1024, 'y');  // Oversized

  storage::FilterValue non_null_value = uint64_t(1234567890);
  EXPECT_FALSE(BinlogFilterEvaluator::CompareFilterValue(non_null_value, not_null_filter))
      << "Oversized filter value should be rejected even for NOT NULL checks";
}

#endif  // USE_MYSQL
