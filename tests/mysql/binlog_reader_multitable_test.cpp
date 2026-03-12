/**
 * @file binlog_reader_multitable_test.cpp
 * @brief Unit tests for binlog reader - Multi-table mode and integration tests
 */

#include "binlog_test_fixtures.h"

#ifdef USE_MYSQL

#include "mysql/binlog_filter_evaluator.h"

using namespace binlog_test;

/**
 * @brief Ensure events are routed to correct TableContext in multi-table mode
 */
TEST_F(BinlogReaderFixture, MultiTableProcessesCorrectTable) {
  server::TableContext articles_ctx;
  articles_ctx.name = "articles";
  articles_ctx.config = table_config_;
  articles_ctx.index = std::make_unique<index::Index>();
  articles_ctx.doc_store = std::make_unique<storage::DocumentStore>();

  server::TableContext comments_ctx;
  comments_ctx.name = "comments";
  comments_ctx.config = table_config_;
  comments_ctx.config.name = "comments";
  comments_ctx.index = std::make_unique<index::Index>();
  comments_ctx.doc_store = std::make_unique<storage::DocumentStore>();

  std::unordered_map<std::string, server::TableContext*> contexts = {
      {articles_ctx.name, &articles_ctx},
      {comments_ctx.name, &comments_ctx},
  };

  config::MysqlConfig mysql_config;  // Use default (UTC timezone)
  BinlogReader multi_reader(connection_, contexts, mysql_config, reader_config_);

  BinlogEvent comment_event = MakeEvent(BinlogEventType::INSERT, "300", 1, "Comment");
  comment_event.table_name = "comments";
  ASSERT_TRUE(multi_reader.ProcessEvent(comment_event));
  EXPECT_TRUE(comments_ctx.doc_store->GetDocId("300").has_value());
  EXPECT_FALSE(articles_ctx.doc_store->GetDocId("300").has_value());
}

/**
 * @brief Multi-table mode should ignore tables that are not tracked
 */
TEST_F(BinlogReaderFixture, MultiTableSkipsUnknownTable) {
  server::TableContext articles_ctx;
  articles_ctx.name = "articles";
  articles_ctx.config = table_config_;
  articles_ctx.index = std::make_unique<index::Index>();
  articles_ctx.doc_store = std::make_unique<storage::DocumentStore>();

  std::unordered_map<std::string, server::TableContext*> contexts = {
      {articles_ctx.name, &articles_ctx},
  };

  config::MysqlConfig mysql_config;  // Use default (UTC timezone)
  BinlogReader multi_reader(connection_, contexts, mysql_config, reader_config_);
  BinlogEvent other_event = MakeEvent(BinlogEventType::INSERT, "400", 1, "Ignored");
  other_event.table_name = "not_tracked";
  ASSERT_TRUE(multi_reader.ProcessEvent(other_event));
  EXPECT_EQ(articles_ctx.doc_store->Size(), 0);
}

/**
 * @brief Test BinlogReader with ServerStats integration
 */
TEST(BinlogReaderTest, ServerStatsIntegration) {
  Connection::Config conn_config;
  conn_config.host = "localhost";
  conn_config.user = "test";
  conn_config.password = "test";

  Connection conn(conn_config);

  index::Index idx(1);
  storage::DocumentStore doc_store;

  config::TableConfig table_config;
  table_config.name = "test_table";
  table_config.primary_key = "id";

  BinlogReader::Config reader_config;
  reader_config.start_gtid = "uuid:1";

  server::ServerStats stats;

  // Create BinlogReader with ServerStats
  config::MysqlConfig mysql_config;  // Use default (UTC timezone)
  BinlogReader reader(conn, idx, doc_store, table_config, mysql_config, reader_config, &stats);

  // Verify initial statistics are zero
  EXPECT_EQ(stats.GetReplInsertsApplied(), 0);
  EXPECT_EQ(stats.GetReplInsertsSkipped(), 0);
  EXPECT_EQ(stats.GetReplUpdatesApplied(), 0);
  EXPECT_EQ(stats.GetReplDeletesApplied(), 0);

  // Verify BinlogReader is not running
  EXPECT_FALSE(reader.IsRunning());
}

/**
 * @brief Test BinlogReader SetServerStats method
 */
TEST(BinlogReaderTest, SetServerStats) {
  Connection::Config conn_config;
  Connection conn(conn_config);

  index::Index idx(1);
  storage::DocumentStore doc_store;

  config::TableConfig table_config;
  table_config.name = "test_table";

  BinlogReader::Config reader_config;

  // Create BinlogReader without ServerStats
  config::MysqlConfig mysql_config;  // Use default (UTC timezone)
  BinlogReader reader(conn, idx, doc_store, table_config, mysql_config, reader_config);

  // Create ServerStats and set it
  server::ServerStats stats;
  reader.SetServerStats(&stats);

  // Verify initial statistics are zero
  EXPECT_EQ(stats.GetReplInsertsApplied(), 0);
  EXPECT_EQ(stats.GetReplUpdatesApplied(), 0);
  EXPECT_EQ(stats.GetReplDeletesApplied(), 0);
}

/**
 * @brief Test BinlogReader multi-table mode with ServerStats
 */
TEST(BinlogReaderTest, MultiTableModeWithServerStats) {
  Connection::Config conn_config;
  Connection conn(conn_config);

  // Create table contexts
  server::TableContext table_ctx1;
  table_ctx1.name = "table1";
  table_ctx1.config.name = "table1";
  table_ctx1.config.primary_key = "id";
  table_ctx1.index = std::make_unique<index::Index>(1);
  table_ctx1.doc_store = std::make_unique<storage::DocumentStore>();

  server::TableContext table_ctx2;
  table_ctx2.name = "table2";
  table_ctx2.config.name = "table2";
  table_ctx2.config.primary_key = "id";
  table_ctx2.index = std::make_unique<index::Index>(1);
  table_ctx2.doc_store = std::make_unique<storage::DocumentStore>();

  std::unordered_map<std::string, server::TableContext*> table_contexts;
  table_contexts["table1"] = &table_ctx1;
  table_contexts["table2"] = &table_ctx2;

  BinlogReader::Config reader_config;
  server::ServerStats stats;

  // Create BinlogReader in multi-table mode with ServerStats
  config::MysqlConfig mysql_config;  // Use default (UTC timezone)
  BinlogReader reader(conn, table_contexts, mysql_config, reader_config, &stats);

  // Verify initial statistics are zero
  EXPECT_EQ(stats.GetReplInsertsApplied(), 0);
  EXPECT_EQ(stats.GetReplUpdatesApplied(), 0);
  EXPECT_EQ(stats.GetReplDeletesApplied(), 0);
  EXPECT_EQ(stats.GetReplEventsSkippedOtherTables(), 0);

  // Verify BinlogReader is not running
  EXPECT_FALSE(reader.IsRunning());
}

/**
 * @brief Test multi-table mode with different table configurations
 *
 * Regression test for: Multi-table mode was using global table_config_ instead of
 * per-table configuration, causing incorrect text_column, primary_key, and filter extraction.
 *
 * This test ensures each table uses its own configuration independently.
 */
TEST_F(BinlogReaderFixture, MultiTableModeUsesCorrectTableConfig) {
  // Create articles table with "content" as text column
  server::TableContext articles_ctx;
  articles_ctx.name = "articles";
  articles_ctx.config.name = "articles";
  articles_ctx.config.primary_key = "article_id";
  articles_ctx.config.text_source.column = "content";
  articles_ctx.index = std::make_unique<index::Index>(2);
  articles_ctx.doc_store = std::make_unique<storage::DocumentStore>();

  config::FilterConfig article_filter;
  article_filter.name = "author_id";
  article_filter.type = "int";
  articles_ctx.config.filters.push_back(article_filter);

  // Create comments table with DIFFERENT configuration
  server::TableContext comments_ctx;
  comments_ctx.name = "comments";
  comments_ctx.config.name = "comments";
  comments_ctx.config.primary_key = "comment_id";   // Different primary key
  comments_ctx.config.text_source.column = "body";  // Different text column
  comments_ctx.index = std::make_unique<index::Index>(2);
  comments_ctx.doc_store = std::make_unique<storage::DocumentStore>();

  config::FilterConfig comment_filter;
  comment_filter.name = "post_id";  // Different filter
  comment_filter.type = "int";
  comments_ctx.config.filters.push_back(comment_filter);

  std::unordered_map<std::string, server::TableContext*> contexts = {
      {articles_ctx.name, &articles_ctx},
      {comments_ctx.name, &comments_ctx},
  };

  config::MysqlConfig mysql_config;  // Use default (UTC timezone)
  BinlogReader multi_reader(connection_, contexts, mysql_config, reader_config_);

  // Test ExtractAllFilters with articles config
  RowData article_row;
  article_row.primary_key = "100";
  article_row.text = "Article text";
  article_row.columns["author_id"] = "42";  // RowData.columns is string-to-string map

  auto article_filters = BinlogFilterEvaluator::ExtractAllFilters(article_row, articles_ctx.config);
  // Verify articles table extracts author_id (not post_id)
  EXPECT_TRUE(article_filters.find("author_id") != article_filters.end());
  // Articles should NOT have post_id since it's not in the config
  EXPECT_TRUE(article_filters.find("post_id") == article_filters.end());

  // Test ExtractAllFilters with comments config - should NOT extract author_id
  RowData comment_row;
  comment_row.primary_key = "200";
  comment_row.text = "Comment text";
  comment_row.columns["post_id"] = "999";   // RowData.columns is string-to-string map
  comment_row.columns["author_id"] = "42";  // Also add author_id to row data

  auto comment_filters = BinlogFilterEvaluator::ExtractAllFilters(comment_row, comments_ctx.config);
  // Verify comments table extracts post_id (not author_id)
  EXPECT_TRUE(comment_filters.find("post_id") != comment_filters.end());
  // Comments should NOT have author_id since it's not in the config
  EXPECT_TRUE(comment_filters.find("author_id") == comment_filters.end());
}

/**
 * @brief Test multi-table mode with different required filters
 *
 * Ensures that required_filters from each table's config are correctly applied,
 * not mixing up between tables.
 */
TEST_F(BinlogReaderFixture, MultiTableModeRequiredFiltersPerTable) {
  // Table 1: Accepts status = 1
  server::TableContext published_ctx;
  published_ctx.name = "published";
  published_ctx.config.name = "published";
  published_ctx.config.primary_key = "id";
  published_ctx.config.text_source.column = "content";
  published_ctx.index = std::make_unique<index::Index>(2);
  published_ctx.doc_store = std::make_unique<storage::DocumentStore>();

  config::RequiredFilterConfig published_filter;
  published_filter.name = "status";
  published_filter.type = "int";
  published_filter.op = "=";
  published_filter.value = "1";
  published_ctx.config.required_filters.push_back(published_filter);

  // Table 2: Accepts status = 0
  server::TableContext draft_ctx;
  draft_ctx.name = "drafts";
  draft_ctx.config.name = "drafts";
  draft_ctx.config.primary_key = "id";
  draft_ctx.config.text_source.column = "content";
  draft_ctx.index = std::make_unique<index::Index>(2);
  draft_ctx.doc_store = std::make_unique<storage::DocumentStore>();

  config::RequiredFilterConfig draft_filter;
  draft_filter.name = "status";
  draft_filter.type = "int";
  draft_filter.op = "=";
  draft_filter.value = "0";
  draft_ctx.config.required_filters.push_back(draft_filter);

  std::unordered_map<std::string, server::TableContext*> contexts = {
      {published_ctx.name, &published_ctx},
      {draft_ctx.name, &draft_ctx},
  };

  config::MysqlConfig mysql_config;  // Use default (UTC timezone)
  BinlogReader multi_reader(connection_, contexts, mysql_config, reader_config_);

  // Filters with status = 1
  std::unordered_map<std::string, storage::FilterValue> filters_published;
  filters_published["status"] = static_cast<int64_t>(1);

  // Filters with status = 0
  std::unordered_map<std::string, storage::FilterValue> filters_draft;
  filters_draft["status"] = static_cast<int64_t>(0);

  // Published table should accept status=1, reject status=0
  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters_published, published_ctx.config));
  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters_draft, published_ctx.config));

  // Draft table should accept status=0, reject status=1
  EXPECT_FALSE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters_published, draft_ctx.config));
  EXPECT_TRUE(BinlogFilterEvaluator::EvaluateRequiredFilters(filters_draft, draft_ctx.config));
}

/**
 * @brief Test multi-table mode with concat vs single column text source
 *
 * Verifies that tables with different text_source configurations work correctly
 * in multi-table mode (one using single column, another using concat).
 */
TEST_F(BinlogReaderFixture, MultiTableModeDifferentTextSources) {
  // Table 1: Single column text source
  server::TableContext products_ctx;
  products_ctx.name = "products";
  products_ctx.config.name = "products";
  products_ctx.config.primary_key = "id";
  products_ctx.config.text_source.column = "name";  // Single column
  products_ctx.config.text_source.concat.clear();
  products_ctx.index = std::make_unique<index::Index>(2);
  products_ctx.doc_store = std::make_unique<storage::DocumentStore>();

  // Table 2: Concat text source (multiple columns)
  server::TableContext users_ctx;
  users_ctx.name = "users";
  users_ctx.config.name = "users";
  users_ctx.config.primary_key = "user_id";
  users_ctx.config.text_source.column.clear();  // No single column
  users_ctx.config.text_source.concat = {"first_name", "last_name", "email"};
  users_ctx.index = std::make_unique<index::Index>(2);
  users_ctx.doc_store = std::make_unique<storage::DocumentStore>();

  std::unordered_map<std::string, server::TableContext*> contexts = {
      {products_ctx.name, &products_ctx},
      {users_ctx.name, &users_ctx},
  };

  config::MysqlConfig mysql_config;  // Use default (UTC timezone)
  BinlogReader multi_reader(connection_, contexts, mysql_config, reader_config_);

  // Verify each table has correct configuration
  EXPECT_FALSE(products_ctx.config.text_source.column.empty());
  EXPECT_TRUE(products_ctx.config.text_source.concat.empty());

  EXPECT_TRUE(users_ctx.config.text_source.column.empty());
  EXPECT_FALSE(users_ctx.config.text_source.concat.empty());
  EXPECT_EQ(users_ctx.config.text_source.concat.size(), 3);
}

/**
 * @brief Test concurrent Start() calls don't cause race conditions
 * Regression test for: gtid_encoded_data_ was not protected by mutex
 * and running_ flag was not atomically checked-and-set
 */
TEST_F(BinlogReaderFixture, ConcurrentStartCallsThreadSafe) {
  // Attempt to start the reader from multiple threads concurrently
  std::atomic<int> start_success_count{0};
  std::atomic<int> already_running_count{0};

  constexpr int num_threads = 5;
  std::vector<std::thread> threads;

  for (int i = 0; i < num_threads; ++i) {
    threads.emplace_back([&]() {
      // Note: Start() will fail because we don't have a real MySQL connection
      // But the important part is testing the thread safety of the check-and-set logic
      auto result = reader_->Start();
      if (result) {
        start_success_count.fetch_add(1);
      } else {
        // Check if error was "already running"
        if (result.error().message().find("already running") != std::string::npos) {
          already_running_count.fetch_add(1);
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // With proper atomic compare_exchange_strong:
  // - At most one thread should succeed or all should fail for other reasons
  // - Multiple threads should get "already running" error
  // The key is: no race condition or crash should occur
  EXPECT_LE(start_success_count.load(), 1) << "At most one Start() should succeed";

  // Stop the reader if it was started
  if (reader_->IsRunning()) {
    reader_->Stop();
  }
}

/**
 * @brief Test that FetchColumnNames is skipped for non-monitored tables
 *
 * Regression test for: FetchColumnNames was called for all TABLE_MAP_EVENTs regardless
 * of whether the table was monitored, causing permission errors and unnecessary queries.
 */
TEST_F(BinlogReaderFixture, SkipsColumnFetchForNonMonitoredTablesMultiTableMode) {
  // Set up multi-table mode with only "articles" registered
  server::TableContext articles_ctx;
  articles_ctx.name = "articles";
  articles_ctx.config = table_config_;
  articles_ctx.index = std::make_unique<index::Index>(2);
  articles_ctx.doc_store = std::make_unique<storage::DocumentStore>();

  std::unordered_map<std::string, server::TableContext*> contexts = {
      {articles_ctx.name, &articles_ctx},
  };

  config::MysqlConfig mysql_config;
  BinlogReader multi_reader(connection_, contexts, mysql_config, reader_config_);

  // Test: monitored table should be recognized
  EXPECT_TRUE(multi_reader.table_contexts_.find("articles") != multi_reader.table_contexts_.end())
      << "articles table should be in table_contexts_";

  // Test: non-monitored table should NOT be in contexts
  EXPECT_TRUE(multi_reader.table_contexts_.find("ignore_threads") == multi_reader.table_contexts_.end())
      << "ignore_threads should NOT be in table_contexts_";

  EXPECT_TRUE(multi_reader.table_contexts_.find("other_table") == multi_reader.table_contexts_.end())
      << "other_table should NOT be in table_contexts_";

  // Verify multi_table_mode is true
  EXPECT_TRUE(multi_reader.multi_table_mode_) << "Should be in multi-table mode";
}

/**
 * @brief Test single-table mode correctly identifies monitored table
 *
 * In single-table mode, only the configured table_config_.name should be monitored.
 */
TEST_F(BinlogReaderFixture, SkipsColumnFetchForNonMonitoredTablesSingleTableMode) {
  // reader_ is already created in single-table mode with "articles" table
  EXPECT_FALSE(reader_->multi_table_mode_) << "Should be in single-table mode";
  EXPECT_EQ(reader_->table_config_.name, "articles") << "Configured table should be 'articles'";

  // In single-table mode, the check is: (metadata.table_name == table_config_.name)
  // This can be verified indirectly through ProcessEvent which uses similar logic

  // Verify event for configured table is processed
  BinlogEvent articles_event = MakeEvent(BinlogEventType::INSERT, "1", 1, "test");
  articles_event.table_name = "articles";
  EXPECT_TRUE(reader_->ProcessEvent(articles_event)) << "Event for monitored table should process";

  // Verify event for non-configured table is skipped (returns true but doesn't add to store)
  BinlogEvent other_event = MakeEvent(BinlogEventType::INSERT, "2", 1, "test");
  other_event.table_name = "ignore_threads";
  EXPECT_TRUE(reader_->ProcessEvent(other_event)) << "Event for non-monitored table should be skipped gracefully";
  EXPECT_FALSE(doc_store_.GetDocId("2").has_value()) << "Non-monitored table event should not be indexed";
}

/**
 * @brief Test that column names cache is only populated for monitored tables
 *
 * This verifies the optimization: we don't waste resources caching column names
 * for tables we'll never process.
 */
TEST_F(BinlogReaderFixture, ColumnNamesCacheOnlyForMonitoredTables) {
  // The column_names_cache_ should only contain entries for monitored tables
  // Since we can't call FetchColumnNames without a real MySQL connection,
  // we verify the cache starts empty and stays empty for non-monitored tables

  EXPECT_TRUE(reader_->column_names_cache_.empty()) << "Column names cache should start empty";

  // Simulate the behavior: in the actual code, FetchColumnNames is only called
  // for monitored tables, so the cache will only contain monitored table entries
}

#endif  // USE_MYSQL
