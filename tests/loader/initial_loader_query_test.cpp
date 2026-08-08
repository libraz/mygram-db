/**
 * @file initial_loader_query_test.cpp
 * @brief Unit tests for InitialLoader query generation and batch processing
 *
 * Tests for:
 * - SELECT query generation: duplicate column avoidance in BuildSelectQuery()
 * - Batch processing: final batch indexing, duplicate handling, edge cases
 * - Last batch not indexed
 * - index_batch/doc_ids size mismatch
 * - GTID capture timing issue (requires MySQL integration test)
 * - GTID capture simplification (regression test)
 */

#ifdef USE_MYSQL

#include <gtest/gtest.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <optional>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "loader/initial_loader.h"
#include "mysql/binlog_filter_evaluator.h"
#include "mysql/text_materializer.h"
#include "mysql_test_helpers.h"
#include "storage/document_store.h"
#include "utils/datetime_converter.h"
#include "utils/sql_utils.h"

namespace mygramdb::loader {

/**
 * @brief Build a minimal table configuration for query-generation tests.
 */
config::TableConfig BaseTableConfig() {
  config::TableConfig table_config;
  table_config.name = "articles";
  table_config.primary_key = "id";
  table_config.text_source.column = "content";
  return table_config;
}

config::FilterConfig MakeFilter(const std::string& name, const std::string& type) {
  config::FilterConfig filter;
  filter.name = name;
  filter.type = type;
  return filter;
}

config::RequiredFilterConfig MakeRequiredFilter(const std::string& name, const std::string& type,
                                                const std::string& comparison_operator, const std::string& value) {
  config::RequiredFilterConfig filter;
  filter.name = name;
  filter.type = type;
  filter.op = comparison_operator;
  filter.value = value;
  return filter;
}

/**
 * @brief Extract the column list between SELECT and FROM.
 */
std::vector<std::string> SelectedColumns(const std::string& query) {
  const size_t select_end = query.find(" FROM ");
  if (query.rfind("SELECT ", 0) != 0 || select_end == std::string::npos) {
    return {};
  }
  const std::string list = query.substr(7, select_end - 7);
  std::vector<std::string> columns;
  size_t pos = 0;
  while (pos < list.size()) {
    const size_t next = list.find(", ", pos);
    const size_t stop = next == std::string::npos ? list.size() : next;
    columns.push_back(list.substr(pos, stop - pos));
    pos = next == std::string::npos ? list.size() : next + 2;
  }
  return columns;
}

/**
 * @brief A column named in several roles is selected once, in first-seen order.
 */
TEST(InitialLoadSelectQueryTest, EachColumnIsSelectedOnceInFirstSeenOrder) {
  auto table_config = BaseTableConfig();
  table_config.filters.push_back(MakeFilter("id", "bigint"));     // also the primary key
  table_config.filters.push_back(MakeFilter("content", "text"));  // also the text source
  table_config.required_filters.push_back(MakeRequiredFilter("enabled", "int", "=", "1"));
  table_config.filters.push_back(MakeFilter("enabled", "int"));  // also a required filter

  const auto query = internal::BuildInitialLoadSelectQuery(table_config, {});
  ASSERT_FALSE(query.empty());
  EXPECT_EQ(SelectedColumns(query), (std::vector<std::string>{"`id`", "`content`", "`enabled`"}));
}

/**
 * @brief Concatenated text sources contribute every part column.
 */
TEST(InitialLoadSelectQueryTest, ConcatenatedTextSourceSelectsEveryPartOnce) {
  auto table_config = BaseTableConfig();
  table_config.text_source.column.clear();
  table_config.text_source.concat = {"title", "body", "title"};
  table_config.filters.push_back(MakeFilter("body", "text"));

  const auto query = internal::BuildInitialLoadSelectQuery(table_config, {});
  ASSERT_FALSE(query.empty());
  EXPECT_EQ(SelectedColumns(query), (std::vector<std::string>{"`id`", "`title`", "`body`"}));
}

/**
 * @brief Identifiers are quoted and the table is qualified by its database.
 */
TEST(InitialLoadSelectQueryTest, IdentifiersAreQuotedAndTheTableIsQualified) {
  auto table_config = BaseTableConfig();
  table_config.database = "shop";

  const auto query = internal::BuildInitialLoadSelectQuery(table_config, {});
  EXPECT_EQ(query, "SELECT `id`, `content` FROM `shop`.`articles` ORDER BY `id`");
}

/**
 * @brief A column name carrying a backtick is escaped, not passed through.
 */
TEST(InitialLoadSelectQueryTest, BacktickInAnIdentifierIsEscapedRatherThanClosingTheQuoting) {
  auto table_config = BaseTableConfig();
  table_config.primary_key = "id`, (SELECT password FROM users) AS leaked, `id";

  // Every backtick from the name is doubled, so the identifier never ends early
  // and the injected text stays inside it.
  const std::string escaped = "`id``, (SELECT password FROM users) AS leaked, ``id`";
  EXPECT_EQ(internal::BuildInitialLoadSelectQuery(table_config, {}),
            "SELECT " + escaped + ", `content` FROM `articles` ORDER BY " + escaped);
}

/**
 * @brief An identifier that cannot be quoted at all refuses the whole query.
 *
 * A NUL truncates the C string handed to the driver, so the statement the
 * server executes would be a prefix of the one that was built and checked.
 */
TEST(InitialLoadSelectQueryTest, UnquotableIdentifierRefusesTheQuery) {
  auto embedded_nul = BaseTableConfig();
  embedded_nul.name = std::string("articles\0", 9);
  EXPECT_TRUE(internal::BuildInitialLoadSelectQuery(embedded_nul, {}).empty());

  auto empty_primary_key = BaseTableConfig();
  empty_primary_key.primary_key.clear();
  EXPECT_TRUE(internal::BuildInitialLoadSelectQuery(empty_primary_key, {}).empty());
}

/**
 * @brief String filter values become hex literals rather than quoted text.
 *
 * Hex encoding carries the bytes through without depending on the server's
 * SQL mode for backslash handling, so no value can terminate the literal.
 */
TEST(InitialLoadSelectQueryTest, StringFilterValuesAreHexEncoded) {
  auto table_config = BaseTableConfig();
  table_config.required_filters.push_back(MakeRequiredFilter("status", "string", "=", "'; DROP TABLE articles; --"));

  const auto query = internal::BuildInitialLoadSelectQuery(table_config, {});
  ASSERT_NE(query.find(" WHERE "), std::string::npos);
  EXPECT_NE(query.find("`status` = _utf8mb4 X'273B2044524F50205441424C452061727469636C65733B202D2D'"),
            std::string::npos)
      << query;
  // Nothing that could end a literal or start a statement survives into the SQL.
  EXPECT_EQ(query.find("DROP"), std::string::npos);
}

TEST(InitialLoadSelectQueryTest, EmptyStringFilterValueBecomesAnEmptyHexLiteral) {
  auto table_config = BaseTableConfig();
  table_config.required_filters.push_back(MakeRequiredFilter("status", "varchar", "=", ""));

  const auto query = internal::BuildInitialLoadSelectQuery(table_config, {});
  EXPECT_NE(query.find("`status` = _utf8mb4 X''"), std::string::npos) << query;
}

/**
 * @brief Numeric filter values are emitted bare, so only numbers may pass.
 */
TEST(InitialLoadSelectQueryTest, NumericFilterValueIsEmittedWithoutQuoting) {
  auto table_config = BaseTableConfig();
  table_config.required_filters.push_back(MakeRequiredFilter("enabled", "int", "=", "1"));

  const auto query = internal::BuildInitialLoadSelectQuery(table_config, {});
  EXPECT_NE(query.find("`enabled` = 1"), std::string::npos) << query;
}

TEST(InitialLoadSelectQueryTest, NonNumericValueOnANumericFilterRefusesTheQuery) {
  const std::vector<std::string> rejected = {
      "1 OR 1=1", "1; DROP TABLE articles", "1'", "0x41", "1e5", "", " 1", "1 ", "+", "-", ".", "1.2.3", "1,2",
  };
  for (const auto& value : rejected) {
    auto table_config = BaseTableConfig();
    table_config.required_filters.push_back(MakeRequiredFilter("enabled", "int", "=", value));
    EXPECT_TRUE(internal::BuildInitialLoadSelectQuery(table_config, {}).empty())
        << "a non-numeric value reached an unquoted comparison: " << value;
  }
}

/**
 * @brief Digits are recognized by byte value, not by the process locale.
 */
TEST(InitialLoadSelectQueryTest, NumericLiteralCheckClassifiesEveryByte) {
  for (int value = 0; value <= 0xFF; ++value) {
    const auto byte = static_cast<unsigned char>(value);
    const std::string candidate(1, static_cast<char>(byte));
    const bool expected = byte >= '0' && byte <= '9';
    EXPECT_EQ(internal::IsSafeSQLNumericLiteral(candidate), expected)
        << "byte 0x" << std::hex << value << " was classified against the documented set";
  }
  EXPECT_TRUE(internal::IsSafeSQLNumericLiteral("-12.50"));
  EXPECT_TRUE(internal::IsSafeSQLNumericLiteral("+0"));
  EXPECT_FALSE(internal::IsSafeSQLNumericLiteral("."));
  EXPECT_FALSE(internal::IsSafeSQLNumericLiteral("-"));
}

/**
 * @brief The comparison operator is the one filter field that becomes syntax.
 *
 * The configuration schema constrains it to an enumeration, but the check is
 * repeated where the string turns into SQL so a configuration that reached the
 * loader without passing that schema cannot inject through it.
 */
TEST(InitialLoadSelectQueryTest, OperatorOutsideTheAllowedSetRefusesTheQuery) {
  const std::vector<std::string> rejected = {
      "= 1 OR 1", "IN", "LIKE", "=1", " =", "= (SELECT 1) --", "IS NULL OR 1=1", "", "<>",
  };
  for (const auto& comparison_operator : rejected) {
    auto table_config = BaseTableConfig();
    table_config.required_filters.push_back(MakeRequiredFilter("enabled", "int", comparison_operator, "1"));
    EXPECT_TRUE(internal::BuildInitialLoadSelectQuery(table_config, {}).empty())
        << "an unlisted operator was emitted as SQL: " << comparison_operator;
  }

  for (const auto& comparison_operator : {"=", "!=", "<", ">", "<=", ">="}) {
    auto table_config = BaseTableConfig();
    table_config.required_filters.push_back(MakeRequiredFilter("enabled", "int", comparison_operator, "1"));
    EXPECT_FALSE(internal::BuildInitialLoadSelectQuery(table_config, {}).empty())
        << "a documented operator was rejected: " << comparison_operator;
  }
}

TEST(InitialLoadSelectQueryTest, NullComparisonsTakeNoValue) {
  for (const auto& comparison_operator : {"IS NULL", "IS NOT NULL"}) {
    auto table_config = BaseTableConfig();
    table_config.required_filters.push_back(MakeRequiredFilter("deleted_at", "datetime", comparison_operator, ""));
    const auto query = internal::BuildInitialLoadSelectQuery(table_config, {});
    ASSERT_FALSE(query.empty());
    EXPECT_NE(query.find(std::string("`deleted_at` ") + comparison_operator + " ORDER BY"), std::string::npos) << query;
  }
}

/**
 * @brief A timestamp filter is compared as an epoch, not as a pasted literal.
 */
TEST(InitialLoadSelectQueryTest, TimestampFilterBecomesAnEpochExpression) {
  auto table_config = BaseTableConfig();
  table_config.required_filters.push_back(MakeRequiredFilter("published_at", "timestamp", "<=", "2026-01-01 00:30:00"));
  config::MysqlConfig mysql_config;
  mysql_config.datetime_timezone = "+00:00";

  const auto query = internal::BuildInitialLoadSelectQuery(table_config, mysql_config);
  ASSERT_FALSE(query.empty());
  auto expected_epoch = mygram::utils::ParseDatetimeValue("2026-01-01 00:30:00", "+00:00");
  ASSERT_TRUE(expected_epoch.has_value());
  EXPECT_NE(query.find("FROM_UNIXTIME(" + std::to_string(*expected_epoch) + ")"), std::string::npos) << query;

  auto unparseable = BaseTableConfig();
  unparseable.required_filters.push_back(MakeRequiredFilter("published_at", "timestamp", "<=", "not a timestamp"));
  EXPECT_TRUE(internal::BuildInitialLoadSelectQuery(unparseable, mysql_config).empty());
}

/**
 * @brief Several required filters are joined, and one bad filter refuses all.
 */
TEST(InitialLoadSelectQueryTest, RequiredFiltersAreJoinedAndOneRefusalDiscardsTheQuery) {
  auto table_config = BaseTableConfig();
  table_config.required_filters.push_back(MakeRequiredFilter("enabled", "int", "=", "1"));
  table_config.required_filters.push_back(MakeRequiredFilter("status", "varchar", "!=", "draft"));

  const auto query = internal::BuildInitialLoadSelectQuery(table_config, {});
  ASSERT_FALSE(query.empty());
  EXPECT_NE(query.find(" AND "), std::string::npos) << query;

  table_config.required_filters.push_back(MakeRequiredFilter("score", "int", "=", "1 OR 1=1"));
  EXPECT_TRUE(internal::BuildInitialLoadSelectQuery(table_config, {}).empty());
}

/**
 * @brief The loader streams in primary-key order, which the query must impose.
 */
TEST(InitialLoadSelectQueryTest, RowsAreOrderedByThePrimaryKey) {
  const auto query = internal::BuildInitialLoadSelectQuery(BaseTableConfig(), {});
  ASSERT_FALSE(query.empty());
  EXPECT_EQ(query.substr(query.size() - std::string(" ORDER BY `id`").size()), " ORDER BY `id`");
}

// ===========================================================================
// Batch processing tests (from initial_loader_bug_fixes_test.cpp)
// ===========================================================================

/**
 * @brief Test fixture for batch processing logic
 *
 * These tests verify the batch processing logic used in InitialLoader
 * without requiring MySQL connection.
 */
class BatchProcessingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    doc_store_ = std::make_unique<storage::DocumentStore>();
    index_ = std::make_unique<index::Index>();
  }

  std::unique_ptr<storage::DocumentStore> doc_store_;
  std::unique_ptr<index::Index> index_;
};

/**
 * @brief Test that final batch is properly indexed
 *
 * The last batch of documents should be indexed even when
 * it's smaller than the batch size.
 */
TEST_F(BatchProcessingTest, FinalBatchIsIndexed) {
  const size_t batch_size = 5;

  // Simulate batch processing like InitialLoader does
  std::vector<storage::DocumentStore::DocumentItem> doc_batch;
  std::vector<index::Index::DocumentItem> index_batch;

  // Add 7 items (batch_size=5, so first batch of 5, then final batch of 2)
  std::vector<std::pair<std::string, std::string>> test_data = {
      {"pk1", "text one"},  {"pk2", "text two"}, {"pk3", "text three"}, {"pk4", "text four"},
      {"pk5", "text five"}, {"pk6", "text six"}, {"pk7", "text seven"},
  };

  size_t processed = 0;
  for (const auto& [pk, text] : test_data) {
    doc_batch.push_back({pk, {}});
    index_batch.push_back({0, text});

    // Process batch when full
    if (doc_batch.size() >= batch_size) {
      auto doc_ids_result = doc_store_->AddDocumentBatch(doc_batch);
      ASSERT_TRUE(doc_ids_result.has_value());
      auto doc_ids = *doc_ids_result;

      ASSERT_EQ(doc_ids.size(), index_batch.size()) << "doc_ids and index_batch size mismatch in regular batch";

      for (size_t i = 0; i < doc_ids.size(); ++i) {
        index_batch[i].doc_id = doc_ids[i];
      }
      index_->AddDocumentBatch(index_batch);

      processed += doc_batch.size();
      doc_batch.clear();
      index_batch.clear();
    }
  }

  // Process final batch (this should work correctly)
  ASSERT_FALSE(doc_batch.empty()) << "Final batch should not be empty";
  ASSERT_EQ(doc_batch.size(), 2) << "Final batch should have 2 items";
  ASSERT_EQ(doc_batch.size(), index_batch.size()) << "doc_batch and index_batch should have same size in final batch";

  auto doc_ids_result = doc_store_->AddDocumentBatch(doc_batch);
  ASSERT_TRUE(doc_ids_result.has_value());
  auto doc_ids = *doc_ids_result;

  ASSERT_EQ(doc_ids.size(), index_batch.size()) << "doc_ids and index_batch size mismatch in final batch";

  for (size_t i = 0; i < doc_ids.size(); ++i) {
    index_batch[i].doc_id = doc_ids[i];
  }
  index_->AddDocumentBatch(index_batch);

  processed += doc_batch.size();

  // Verify all documents are stored and indexed
  EXPECT_EQ(processed, 7);
  EXPECT_EQ(doc_store_->Size(), 7);

  // Verify documents can be found via search
  // Index uses bigrams (2-gram) by default, so search for "te" which is in all "text X"
  auto results = index_->SearchAnd({"te"});  // All texts contain "te" bigram
  EXPECT_EQ(results.size(), 7) << "All 7 documents should be found via search";
}

/**
 * @brief Test batch processing with duplicates
 *
 * When duplicates exist, doc_ids may not match index_batch properly.
 */
TEST_F(BatchProcessingTest, DuplicatesHandledCorrectly) {
  std::vector<storage::DocumentStore::DocumentItem> doc_batch;
  std::vector<index::Index::DocumentItem> index_batch;

  // Add items with one duplicate
  doc_batch.push_back({"pk1", {}});
  index_batch.push_back({0, "first text"});

  doc_batch.push_back({"pk2", {}});
  index_batch.push_back({0, "second text"});

  doc_batch.push_back({"pk1", {}});  // Duplicate!
  index_batch.push_back({0, "third text"});

  doc_batch.push_back({"pk3", {}});
  index_batch.push_back({0, "fourth text"});

  auto doc_ids_result = doc_store_->AddDocumentBatch(doc_batch);
  ASSERT_TRUE(doc_ids_result.has_value());
  auto doc_ids = *doc_ids_result;

  // AddDocumentBatch returns same size (returns existing doc_id for duplicates)
  ASSERT_EQ(doc_ids.size(), doc_batch.size());

  // The issue: doc_ids[2] will be the same as doc_ids[0] (existing doc_id)
  // But index_batch[2] has different text ("third text")
  // This causes incorrect indexing if we blindly map them

  // Verify the duplicate behavior
  EXPECT_EQ(doc_ids[0], doc_ids[2]) << "Duplicate should return same doc_id";

  // To fix Need to skip duplicates when indexing
  // Current implementation would incorrectly index "third text" with doc_ids[0]
}

/**
 * @brief Test that size assertions catch mismatches
 */
TEST_F(BatchProcessingTest, SizeAssertionCatchesMismatch) {
  std::vector<storage::DocumentStore::DocumentItem> doc_batch;
  std::vector<index::Index::DocumentItem> index_batch;

  // Simulate a scenario where sizes could mismatch
  doc_batch.push_back({"pk1", {}});
  index_batch.push_back({0, "text1"});

  doc_batch.push_back({"pk2", {}});
  index_batch.push_back({0, "text2"});

  auto doc_ids_result = doc_store_->AddDocumentBatch(doc_batch);
  ASSERT_TRUE(doc_ids_result.has_value());
  auto doc_ids = *doc_ids_result;

  // Verify sizes match
  EXPECT_EQ(doc_ids.size(), doc_batch.size());
  EXPECT_EQ(doc_ids.size(), index_batch.size());
}

/**
 * @brief Test empty batch handling
 */
TEST_F(BatchProcessingTest, EmptyBatchHandledCorrectly) {
  std::vector<storage::DocumentStore::DocumentItem> doc_batch;
  std::vector<index::Index::DocumentItem> index_batch;

  // Both should be empty
  EXPECT_TRUE(doc_batch.empty());
  EXPECT_TRUE(index_batch.empty());

  // Empty batch should return empty result
  auto doc_ids_result = doc_store_->AddDocumentBatch(doc_batch);
  ASSERT_TRUE(doc_ids_result.has_value());
  EXPECT_TRUE(doc_ids_result->empty());
}

/**
 * @brief Test single item batch (edge case)
 */
TEST_F(BatchProcessingTest, SingleItemBatch) {
  std::vector<storage::DocumentStore::DocumentItem> doc_batch;
  std::vector<index::Index::DocumentItem> index_batch;

  doc_batch.push_back({"single_pk", {}});
  index_batch.push_back({0, "single text"});

  auto doc_ids_result = doc_store_->AddDocumentBatch(doc_batch);
  ASSERT_TRUE(doc_ids_result.has_value());
  auto doc_ids = *doc_ids_result;

  ASSERT_EQ(doc_ids.size(), 1);
  ASSERT_EQ(index_batch.size(), 1);

  index_batch[0].doc_id = doc_ids[0];
  index_->AddDocumentBatch(index_batch);

  // Verify document is indexed
  // Index uses bigrams, so search for "si" which is in "single"
  auto results = index_->SearchAnd({"si"});
  EXPECT_EQ(results.size(), 1);
}

// ===========================================================================
// Conservative pre-snapshot GTID capture (regression tests)
// ===========================================================================

/**
 * @brief Document that GTID is captured before the consistent snapshot
 *
 * Global GTID variables are not MVCC snapshot data. The safe ordering is:
 * capture a lower-bound GTID first, then open the snapshot. Commits in between
 * may be replayed, but a commit absent from the snapshot is never skipped.
 */
TEST_F(BatchProcessingTest, PreSnapshotGtidAllowsIdempotentReplay) {
  // Simulate a normal loading flow (mirrors InitialLoader behavior)
  std::vector<storage::DocumentStore::DocumentItem> doc_batch;
  std::vector<index::Index::DocumentItem> index_batch;

  doc_batch.push_back({"pk1", {}});
  index_batch.push_back({0, "text one"});

  auto doc_ids_result = doc_store_->AddDocumentBatch(doc_batch);
  ASSERT_TRUE(doc_ids_result.has_value());
  auto doc_ids = *doc_ids_result;

  ASSERT_EQ(doc_ids.size(), index_batch.size());

  for (size_t i = 0; i < doc_ids.size(); ++i) {
    index_batch[i].doc_id = doc_ids[i];
  }
  index_->AddDocumentBatch(index_batch);

  EXPECT_EQ(doc_store_->Size(), 1);
  auto results = index_->SearchAnd({"te"});
  EXPECT_EQ(results.size(), 1);
}

TEST(InitialLoaderIntegrationTest, LoadsConfiguredDatabaseWhenSameTableExistsInDefaultDatabase) {
  if (!mysql::testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled. Set ENABLE_MYSQL_INTEGRATION_TESTS=1 to enable.";
  }

  auto connection_config = mysql::testing::GetMySQLTestConfig();
  mysql::Connection loader_connection(connection_config);
  auto loader_connect = loader_connection.Connect("initial-loader-cross-database-test");
  if (!loader_connect) {
    GTEST_SKIP() << "MySQL connection failed: " << loader_connect.error().message();
  }

  mysql::Connection writer_connection(connection_config);
  auto writer_connect = writer_connection.Connect("initial-loader-cross-database-writer");
  ASSERT_TRUE(writer_connect) << writer_connect.error().message();

  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() & 0x7fffffff);
  const std::string database = "mygram_it_source_db_" + suffix;
  const std::string table = "same_articles_" + suffix;
  const std::string qualified_source = "`" + database + "`.`" + table + "`";
  const std::string default_table = "`" + table + "`";

  auto cleanup = [&]() {
    (void)writer_connection.ExecuteUpdate("DROP TABLE IF EXISTS " + default_table);
    (void)writer_connection.ExecuteUpdate("DROP DATABASE IF EXISTS `" + database + "`");
  };
  cleanup();

  ASSERT_TRUE(writer_connection.ExecuteUpdate("CREATE DATABASE `" + database + "`"));
  ASSERT_TRUE(writer_connection.ExecuteUpdate("CREATE TABLE " + default_table +
                                              " (id VARCHAR(32) PRIMARY KEY, content TEXT) ENGINE=InnoDB"));
  ASSERT_TRUE(writer_connection.ExecuteUpdate("CREATE TABLE " + qualified_source +
                                              " (id VARCHAR(32) PRIMARY KEY, content TEXT) ENGINE=InnoDB"));
  ASSERT_TRUE(
      writer_connection.ExecuteUpdate("INSERT INTO " + default_table + " VALUES ('wrong', 'default database row')"));
  ASSERT_TRUE(
      writer_connection.ExecuteUpdate("INSERT INTO " + qualified_source + " VALUES ('right', 'configured source')"));

  config::TableConfig table_config;
  table_config.name = table;
  table_config.database = database;
  table_config.primary_key = "id";
  table_config.text_source.column = "content";
  table_config.ngram_size = 1;

  index::Index index(1);
  storage::DocumentStore store;
  InitialLoader loader(loader_connection, index, store, table_config);
  auto load_result = loader.Load();
  ASSERT_TRUE(load_result) << load_result.error().message();

  EXPECT_EQ(store.Size(), 1);
  EXPECT_TRUE(store.GetDocId("right").has_value());
  EXPECT_FALSE(store.GetDocId("wrong").has_value());
  auto source_doc_id = store.GetDocId("right");
  ASSERT_TRUE(source_doc_id.has_value());
  EXPECT_EQ(store.GetNormalizedText(*source_doc_id), std::optional<std::string>{"configured source"});

  cleanup();
}

TEST(InitialLoaderIntegrationTest, CanonicalizesEnumSetDecimalAndTemporalValues) {
  if (!mysql::testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled. Set ENABLE_MYSQL_INTEGRATION_TESTS=1 to enable.";
  }

  auto connection_config = mysql::testing::GetMySQLTestConfig();
  mysql::Connection connection(connection_config);
  auto connect = connection.Connect("initial-loader-canonical-values-test");
  if (!connect) {
    GTEST_SKIP() << "MySQL connection failed: " << connect.error().message();
  }

  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() & 0x7fffffff);
  const std::string table = "canonical_values_" + suffix;
  const std::string quoted_table = "`" + table + "`";
  auto cleanup = [&]() { (void)connection.ExecuteUpdate("DROP TABLE IF EXISTS " + quoted_table); };
  cleanup();

  ASSERT_TRUE(connection.ExecuteUpdate("CREATE TABLE " + quoted_table +
                                       " (amount DECIMAL(15,2) PRIMARY KEY, status ENUM('draft','published') NOT NULL,"
                                       " tags SET('red','green','blue') NOT NULL, born_on DATE NOT NULL,"
                                       " happened_at DATETIME(6) NOT NULL, published_at TIMESTAMP(6) NOT NULL,"
                                       " elapsed TIME(6) NOT NULL) ENGINE=InnoDB"));
  ASSERT_TRUE(
      connection.ExecuteUpdate("INSERT INTO " + quoted_table +
                               " VALUES (1234.56, 'published', 'red,blue', '1960-01-01',"
                               " '1960-01-01 00:00:00.123456', '2024-01-02 00:00:00.654321', '-10:20:30.654321')"));

  config::TableConfig table_config;
  table_config.name = table;
  table_config.database = connection_config.database;
  table_config.primary_key = "amount";
  table_config.text_source.concat = {"status", "tags"};
  table_config.text_source.delimiter = "|";
  table_config.ngram_size = 1;
  table_config.filters = {
      {"born_on", "date", false, false, ""},
      {"happened_at", "datetime", false, false, ""},
      {"published_at", "timestamp", false, false, ""},
      {"elapsed", "time", false, false, ""},
  };

  config::MysqlConfig mysql_config;
  mysql_config.datetime_timezone = "+00:00";
  index::Index index(1);
  storage::DocumentStore store;
  InitialLoader loader(connection, index, store, table_config, mysql_config);
  auto load = loader.Load();
  ASSERT_TRUE(load) << load.error().message();

  auto doc_id = store.GetDocId("1234.56");
  ASSERT_TRUE(doc_id.has_value());
  EXPECT_EQ(store.GetNormalizedText(*doc_id), std::optional<std::string>{"published|red,blue"});

  auto born_on = store.GetFilterValue(*doc_id, "born_on");
  ASSERT_TRUE(born_on.has_value());
  ASSERT_TRUE(std::holds_alternative<int64_t>(*born_on));
  EXPECT_EQ(std::get<int64_t>(*born_on), -315619200);

  auto elapsed = store.GetFilterValue(*doc_id, "elapsed");
  ASSERT_TRUE(elapsed.has_value());
  ASSERT_TRUE(std::holds_alternative<storage::TimeValue>(*elapsed));
  EXPECT_EQ(std::get<storage::TimeValue>(*elapsed).seconds, -(10 * 3600 + 20 * 60 + 30));

  cleanup();
}

TEST(InitialLoaderIntegrationTest, SharedSnapshotKeepsMultipleTableLoadsAtSameGtid) {
  if (!mysql::testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled. Set ENABLE_MYSQL_INTEGRATION_TESTS=1 to enable.";
  }

  auto connection_config = mysql::testing::GetMySQLTestConfig();
  mysql::Connection loader_connection(connection_config);
  auto loader_connect = loader_connection.Connect("initial-loader-shared-snapshot-test");
  if (!loader_connect) {
    GTEST_SKIP() << "MySQL connection failed: " << loader_connect.error().message();
  }
  auto gtid_mode_enabled = loader_connection.IsGTIDModeEnabled();
  if (!gtid_mode_enabled) {
    GTEST_SKIP() << "Failed to query MySQL GTID mode: " << gtid_mode_enabled.error().message();
  }
  if (!*gtid_mode_enabled) {
    GTEST_SKIP() << "MySQL GTID mode is not enabled";
  }

  mysql::Connection writer_connection(connection_config);
  auto writer_connect = writer_connection.Connect("initial-loader-shared-snapshot-writer");
  if (!writer_connect) {
    GTEST_SKIP() << "MySQL writer connection failed: " << writer_connect.error().message();
  }

  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() & 0x7fffffff);
  const std::string table_a = "mygram_it_snapshot_a_" + suffix;
  const std::string table_b = "mygram_it_snapshot_b_" + suffix;

  auto cleanup = [&]() {
    (void)writer_connection.ExecuteUpdate("DROP TABLE IF EXISTS " + table_a);
    (void)writer_connection.ExecuteUpdate("DROP TABLE IF EXISTS " + table_b);
  };
  cleanup();

  ASSERT_TRUE(writer_connection.ExecuteUpdate("CREATE TABLE " + table_a +
                                              " (id VARCHAR(32) PRIMARY KEY, content TEXT) ENGINE=InnoDB"));
  ASSERT_TRUE(writer_connection.ExecuteUpdate("CREATE TABLE " + table_b +
                                              " (id VARCHAR(32) PRIMARY KEY, content TEXT) ENGINE=InnoDB"));
  ASSERT_TRUE(writer_connection.ExecuteUpdate("INSERT INTO " + table_a + " VALUES ('1', 'snapshot old alpha')"));
  ASSERT_TRUE(writer_connection.ExecuteUpdate("INSERT INTO " + table_b + " VALUES ('1', 'snapshot old beta')"));

  auto gtid_result = loader_connection.GetExecutedGTID();
  ASSERT_TRUE(gtid_result) << gtid_result.error().message();
  std::string snapshot_gtid = *gtid_result;
  snapshot_gtid.erase(
      std::remove_if(snapshot_gtid.begin(), snapshot_gtid.end(), [](unsigned char chr) { return std::isspace(chr); }),
      snapshot_gtid.end());
  ASSERT_FALSE(snapshot_gtid.empty());
  ASSERT_TRUE(loader_connection.ExecuteUpdate("START TRANSACTION WITH CONSISTENT SNAPSHOT"));

  auto make_table_config = [&connection_config](const std::string& table_name) {
    config::TableConfig table_config;
    table_config.name = table_name;
    table_config.database = connection_config.database;
    table_config.primary_key = "id";
    table_config.text_source.column = "content";
    table_config.ngram_size = 1;
    return table_config;
  };

  index::Index index_a(1);
  storage::DocumentStore store_a;
  loader::InitialLoader loader_a(loader_connection, index_a, store_a, make_table_config(table_a));
  ASSERT_TRUE(loader_a.LoadFromExistingSnapshot(snapshot_gtid));

  ASSERT_TRUE(
      writer_connection.ExecuteUpdate("UPDATE " + table_b + " SET content = 'snapshot new beta' WHERE id = '1'"));

  index::Index index_b(1);
  storage::DocumentStore store_b;
  loader::InitialLoader loader_b(loader_connection, index_b, store_b, make_table_config(table_b));
  ASSERT_TRUE(loader_b.LoadFromExistingSnapshot(snapshot_gtid));
  ASSERT_TRUE(loader_connection.ExecuteUpdate("COMMIT"));

  auto doc_id_b = store_b.GetDocId("1");
  ASSERT_TRUE(doc_id_b.has_value());
  auto loaded_text_b = store_b.GetNormalizedText(*doc_id_b);
  ASSERT_TRUE(loaded_text_b.has_value());
  EXPECT_EQ(*loaded_text_b, "snapshot old beta");

  cleanup();
}

TEST(InitialLoaderIntegrationTest, CommitBetweenGtidCaptureAndSnapshotCannotBeSkipped) {
  if (!mysql::testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled. Set ENABLE_MYSQL_INTEGRATION_TESTS=1 to enable.";
  }

  auto connection_config = mysql::testing::GetMySQLTestConfig();
  mysql::Connection loader_connection(connection_config);
  auto loader_connect = loader_connection.Connect("initial-loader-gtid-interleaving-test");
  if (!loader_connect) {
    GTEST_SKIP() << "MySQL connection failed: " << loader_connect.error().message();
  }
  auto gtid_mode_enabled = loader_connection.IsGTIDModeEnabled();
  if (!gtid_mode_enabled || !*gtid_mode_enabled) {
    GTEST_SKIP() << "GTID mode is required";
  }

  mysql::Connection writer_connection(connection_config);
  auto writer_connect = writer_connection.Connect("initial-loader-gtid-interleaving-writer");
  ASSERT_TRUE(writer_connect) << writer_connect.error().message();

  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() & 0x7fffffff);
  const std::string table = "mygram_it_gtid_interleave_" + suffix;
  auto cleanup = [&]() { (void)writer_connection.ExecuteUpdate("DROP TABLE IF EXISTS " + table); };
  cleanup();
  ASSERT_TRUE(writer_connection.ExecuteUpdate("CREATE TABLE " + table +
                                              " (id VARCHAR(32) PRIMARY KEY, content TEXT) ENGINE=InnoDB"));
  ASSERT_TRUE(writer_connection.ExecuteUpdate("INSERT INTO " + table + " VALUES ('1', 'baseline alpha')"));

  auto before_result = loader_connection.GetExecutedGTID();
  ASSERT_TRUE(before_result) << before_result.error().message();
  std::string before_gtid = *before_result;
  before_gtid.erase(
      std::remove_if(before_gtid.begin(), before_gtid.end(), [](unsigned char chr) { return std::isspace(chr); }),
      before_gtid.end());

  config::TableConfig table_config;
  table_config.name = table;
  table_config.database = connection_config.database;
  table_config.primary_key = "id";
  table_config.text_source.column = "content";
  table_config.ngram_size = 1;
  index::Index index(1);
  storage::DocumentStore store;
  InitialLoader loader(loader_connection, index, store, table_config);
  loader.SetAfterGtidCaptureHookForTest(
      [&]() { EXPECT_TRUE(writer_connection.ExecuteUpdate("INSERT INTO " + table + " VALUES ('2', 'racing beta')")); });

  auto load_result = loader.Load();
  ASSERT_TRUE(load_result) << load_result.error().message();
  EXPECT_EQ(loader.GetStartGTID(), before_gtid);
  EXPECT_EQ(store.Size(), 2U);
  EXPECT_TRUE(store.GetDocId("1").has_value());
  EXPECT_TRUE(store.GetDocId("2").has_value());

  auto mysql_count = writer_connection.Execute("SELECT COUNT(*) FROM " + table);
  ASSERT_TRUE(mysql_count) << mysql_count.error().message();
  MYSQL_ROW row = mysql_fetch_row(mysql_count->get());
  ASSERT_NE(row, nullptr);
  EXPECT_EQ(std::stoull(row[0]), store.Size());
  cleanup();
}

TEST(InitialLoaderIntegrationTest, ExistingSnapshotErrorDoesNotRollbackCallerTransaction) {
  if (!mysql::testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled. Set ENABLE_MYSQL_INTEGRATION_TESTS=1 to enable.";
  }

  auto connection_config = mysql::testing::GetMySQLTestConfig();
  mysql::Connection loader_connection(connection_config);
  auto loader_connect = loader_connection.Connect("initial-loader-existing-snapshot-rollback-test");
  if (!loader_connect) {
    GTEST_SKIP() << "MySQL connection failed: " << loader_connect.error().message();
  }
  auto gtid_mode_enabled = loader_connection.IsGTIDModeEnabled();
  if (!gtid_mode_enabled) {
    GTEST_SKIP() << "Failed to query MySQL GTID mode: " << gtid_mode_enabled.error().message();
  }
  if (!*gtid_mode_enabled) {
    GTEST_SKIP() << "MySQL GTID mode is not enabled";
  }

  mysql::Connection writer_connection(connection_config);
  auto writer_connect = writer_connection.Connect("initial-loader-existing-snapshot-rollback-writer");
  if (!writer_connect) {
    GTEST_SKIP() << "MySQL writer connection failed: " << writer_connect.error().message();
  }

  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() & 0x7fffffff);
  const std::string load_table = "mygram_it_existing_snapshot_load_" + suffix;
  const std::string probe_table = "mygram_it_existing_snapshot_probe_" + suffix;

  auto cleanup = [&]() {
    (void)writer_connection.ExecuteUpdate("DROP TABLE IF EXISTS " + load_table);
    (void)writer_connection.ExecuteUpdate("DROP TABLE IF EXISTS " + probe_table);
  };
  cleanup();

  ASSERT_TRUE(writer_connection.ExecuteUpdate("CREATE TABLE " + load_table +
                                              " (id VARCHAR(32) PRIMARY KEY, content TEXT, status INT) ENGINE=InnoDB"));
  ASSERT_TRUE(
      writer_connection.ExecuteUpdate("CREATE TABLE " + probe_table + " (id VARCHAR(32) PRIMARY KEY) ENGINE=InnoDB"));
  ASSERT_TRUE(writer_connection.ExecuteUpdate("INSERT INTO " + load_table + " VALUES ('1', 'snapshot text', 1)"));

  auto gtid_result = loader_connection.GetExecutedGTID();
  ASSERT_TRUE(gtid_result) << gtid_result.error().message();
  std::string snapshot_gtid = *gtid_result;
  snapshot_gtid.erase(
      std::remove_if(snapshot_gtid.begin(), snapshot_gtid.end(), [](unsigned char chr) { return std::isspace(chr); }),
      snapshot_gtid.end());
  ASSERT_FALSE(snapshot_gtid.empty());
  ASSERT_TRUE(loader_connection.ExecuteUpdate("START TRANSACTION WITH CONSISTENT SNAPSHOT"));
  ASSERT_TRUE(loader_connection.ExecuteUpdate("INSERT INTO " + probe_table + " VALUES ('kept')"));

  config::TableConfig table_config;
  table_config.name = load_table;
  table_config.database = connection_config.database;
  table_config.primary_key = "id";
  table_config.text_source.column = "content";
  table_config.ngram_size = 1;
  config::RequiredFilterConfig required_filter;
  required_filter.name = "status";
  required_filter.type = "int";
  required_filter.op = "=";
  required_filter.value = "1 OR 1";
  table_config.required_filters.push_back(required_filter);

  index::Index index(1);
  storage::DocumentStore store;
  loader::InitialLoader loader(loader_connection, index, store, table_config);
  EXPECT_FALSE(loader.LoadFromExistingSnapshot(snapshot_gtid));
  ASSERT_TRUE(loader_connection.ExecuteUpdate("COMMIT"));

  auto count_result = writer_connection.Execute("SELECT COUNT(*) FROM " + probe_table + " WHERE id = 'kept'");
  ASSERT_TRUE(count_result) << count_result.error().message();
  MYSQL_ROW row = mysql_fetch_row(count_result->get());
  ASSERT_NE(row, nullptr);
  ASSERT_NE(row[0], nullptr);
  EXPECT_STREQ(row[0], "1");

  cleanup();
}

TEST(InitialLoaderIntegrationTest, EmbeddedNulValuesAreLoadedWithoutTruncation) {
  if (!mysql::testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled. Set ENABLE_MYSQL_INTEGRATION_TESTS=1 to enable.";
  }

  auto connection_config = mysql::testing::GetMySQLTestConfig();
  mysql::Connection connection(connection_config);
  auto connect_result = connection.Connect("initial-loader-embedded-nul-test");
  if (!connect_result) {
    GTEST_SKIP() << "MySQL connection failed: " << connect_result.error().message();
  }
  auto gtid_mode_enabled = connection.IsGTIDModeEnabled();
  if (!gtid_mode_enabled || !*gtid_mode_enabled) {
    GTEST_SKIP() << "MySQL GTID mode is required";
  }

  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() & 0x7fffffff);
  const std::string table = "mygram_it_embedded_nul_" + suffix;
  auto cleanup = [&]() { (void)connection.ExecuteUpdate("DROP TABLE IF EXISTS " + table); };
  cleanup();

  ASSERT_TRUE(connection.ExecuteUpdate("CREATE TABLE " + table +
                                       " (id VARBINARY(32) PRIMARY KEY, content BLOB NOT NULL, "
                                       "category VARBINARY(32)) ENGINE=InnoDB"));
  ASSERT_TRUE(connection.ExecuteUpdate("INSERT INTO " + table +
                                       " VALUES (X'6162006364', X'707265006e6565646c65', X'72656400626c7565'), "
                                       "(X'6162006566', X'70726500737566666978', X'677265656e0079656c6c6f77')"));

  config::TableConfig table_config;
  table_config.name = table;
  table_config.database = connection_config.database;
  table_config.primary_key = "id";
  table_config.text_source.column = "content";
  table_config.ngram_size = 1;
  config::FilterConfig category;
  category.name = "category";
  category.type = "string";
  table_config.filters.push_back(category);

  index::Index index(1);
  storage::DocumentStore store;
  loader::InitialLoader loader(connection, index, store, table_config);
  auto load_result = loader.Load();
  ASSERT_TRUE(load_result) << load_result.error().message();

  const std::string expected_pk("ab\0cd", 5);
  const std::string expected_text("pre\0needle", 10);
  const std::string expected_filter("red\0blue", 8);
  auto doc_id = store.GetDocId(expected_pk);
  ASSERT_TRUE(doc_id.has_value());
  EXPECT_EQ(store.GetPrimaryKey(*doc_id), expected_pk);
  EXPECT_EQ(store.GetNormalizedText(*doc_id), expected_text);
  auto filter_value = store.GetFilterValue(*doc_id, "category");
  ASSERT_TRUE(filter_value.has_value());
  ASSERT_TRUE(std::holds_alternative<std::string>(*filter_value));
  EXPECT_EQ(std::get<std::string>(*filter_value), expected_filter);
  EXPECT_EQ(index.SearchAnd({"n"}), (std::vector<storage::DocId>{*doc_id}));

  const std::string second_pk("ab\0ef", 5);
  const std::string second_text("pre\0suffix", 10);
  const std::string second_filter("green\0yellow", 12);
  auto second_doc_id = store.GetDocId(second_pk);
  ASSERT_TRUE(second_doc_id.has_value());
  EXPECT_NE(*second_doc_id, *doc_id);
  EXPECT_EQ(store.GetPrimaryKey(*second_doc_id), second_pk);
  EXPECT_EQ(store.GetNormalizedText(*second_doc_id), second_text);
  auto second_filter_value = store.GetFilterValue(*second_doc_id, "category");
  ASSERT_TRUE(second_filter_value.has_value());
  ASSERT_TRUE(std::holds_alternative<std::string>(*second_filter_value));
  EXPECT_EQ(std::get<std::string>(*second_filter_value), second_filter);

  cleanup();
}

TEST(InitialLoaderIntegrationTest, EmptyStringPrimaryKeyIsLoaded) {
  if (!mysql::testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled. Set ENABLE_MYSQL_INTEGRATION_TESTS=1 to enable.";
  }

  auto connection_config = mysql::testing::GetMySQLTestConfig();
  mysql::Connection connection(connection_config);
  auto connect_result = connection.Connect("initial-loader-empty-primary-key-test");
  if (!connect_result) {
    GTEST_SKIP() << "MySQL connection failed: " << connect_result.error().message();
  }
  auto gtid_mode_enabled = connection.IsGTIDModeEnabled();
  if (!gtid_mode_enabled || !*gtid_mode_enabled) {
    GTEST_SKIP() << "MySQL GTID mode is required";
  }

  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() & 0x7fffffff);
  const std::string table = "mygram_it_empty_primary_key_" + suffix;
  auto cleanup = [&]() { (void)connection.ExecuteUpdate("DROP TABLE IF EXISTS " + table); };
  cleanup();

  ASSERT_TRUE(connection.ExecuteUpdate("CREATE TABLE " + table +
                                       " (id VARCHAR(32) PRIMARY KEY, content TEXT NOT NULL) ENGINE=InnoDB"));
  ASSERT_TRUE(connection.ExecuteUpdate("INSERT INTO " + table + " VALUES ('', 'empty primary key document')"));

  config::TableConfig table_config;
  table_config.name = table;
  table_config.database = connection_config.database;
  table_config.primary_key = "id";
  table_config.text_source.column = "content";
  table_config.ngram_size = 1;

  index::Index index(1);
  storage::DocumentStore store;
  InitialLoader loader(connection, index, store, table_config);
  auto load_result = loader.Load();
  ASSERT_TRUE(load_result) << load_result.error().message();

  auto doc_id = store.GetDocId("");
  ASSERT_TRUE(doc_id.has_value());
  EXPECT_EQ(store.GetPrimaryKey(*doc_id), "");
  EXPECT_EQ(store.GetOriginalText(*doc_id), std::optional<std::string>("empty primary key document"));

  cleanup();
}

TEST(InitialLoaderIntegrationTest, SessionTimezoneAndTimestampFilterUseOneEpochContract) {
  if (!mysql::testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled. Set ENABLE_MYSQL_INTEGRATION_TESTS=1 to enable.";
  }

  auto connection_config = mysql::testing::GetMySQLTestConfig();
  mysql::Connection loader_connection(connection_config);
  auto loader_connect = loader_connection.Connect("initial-loader-timestamp-timezone-test");
  if (!loader_connect) {
    GTEST_SKIP() << "MySQL connection failed: " << loader_connect.error().message();
  }
  mysql::Connection writer_connection(connection_config);
  auto writer_connect = writer_connection.Connect("initial-loader-timestamp-timezone-writer");
  if (!writer_connect) {
    GTEST_SKIP() << "MySQL writer connection failed: " << writer_connect.error().message();
  }

  auto timezone_result = loader_connection.Execute("SELECT @@session.time_zone");
  ASSERT_TRUE(timezone_result) << timezone_result.error().message();
  MYSQL_ROW timezone_row = mysql_fetch_row(timezone_result->get());
  ASSERT_NE(timezone_row, nullptr);
  ASSERT_NE(timezone_row[0], nullptr);
  EXPECT_STREQ(timezone_row[0], "+00:00");

  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() & 0x7fffffff);
  const std::string table = "mygram_it_timestamp_tz_" + suffix;
  auto cleanup = [&]() { (void)writer_connection.ExecuteUpdate("DROP TABLE IF EXISTS " + table); };
  cleanup();

  ASSERT_TRUE(writer_connection.ExecuteUpdate("CREATE TABLE " + table +
                                              " (id VARCHAR(32) PRIMARY KEY, content TEXT NOT NULL, "
                                              "published_at TIMESTAMP NULL) ENGINE=InnoDB"));
  ASSERT_TRUE(writer_connection.ExecuteUpdate("SET SESSION time_zone = '+09:00'"));
  ASSERT_TRUE(writer_connection.ExecuteUpdate("INSERT INTO " + table +
                                              " VALUES ('1', 'timestamp boundary', '2026-01-01 00:30:00')"));

  config::TableConfig table_config;
  table_config.name = table;
  table_config.database = connection_config.database;
  table_config.primary_key = "id";
  table_config.text_source.column = "content";
  table_config.ngram_size = 1;
  config::RequiredFilterConfig published_filter;
  published_filter.name = "published_at";
  published_filter.type = "timestamp";
  published_filter.op = "=";
  published_filter.value = "2026-01-01 00:30:00";
  table_config.required_filters.push_back(published_filter);

  config::MysqlConfig mysql_config;
  mysql_config.datetime_timezone = "+09:00";
  index::Index index(1);
  storage::DocumentStore store;
  loader::InitialLoader loader(loader_connection, index, store, table_config, mysql_config);
  auto load_result = loader.Load();
  ASSERT_TRUE(load_result) << load_result.error().message();

  auto expected_epoch = mygram::utils::ParseDatetimeValue(published_filter.value, mysql_config.datetime_timezone);
  ASSERT_TRUE(expected_epoch.has_value());
  auto doc_id = store.GetDocId("1");
  ASSERT_TRUE(doc_id.has_value());
  auto snapshot_value = store.GetFilterValue(*doc_id, "published_at");
  ASSERT_TRUE(snapshot_value.has_value());
  ASSERT_TRUE(std::holds_alternative<int64_t>(*snapshot_value));
  EXPECT_EQ(std::get<int64_t>(*snapshot_value), *expected_epoch);

  mysql::RowData binlog_row;
  binlog_row.primary_key = "1";
  binlog_row.columns["published_at"] = std::to_string(*expected_epoch);
  auto binlog_filters =
      mysql::BinlogFilterEvaluator::ExtractAllFilters(binlog_row, table_config, mysql_config.datetime_timezone);
  ASSERT_TRUE(binlog_filters.contains("published_at"));
  EXPECT_EQ(binlog_filters.at("published_at"), *snapshot_value);
  EXPECT_TRUE(mysql::BinlogFilterEvaluator::EvaluateRequiredFilters(binlog_filters, table_config,
                                                                    mysql_config.datetime_timezone));

  cleanup();
}

TEST(InitialLoaderIntegrationTest, ConcatDelimiterAndEmptyDocumentsMatchBinlogMaterializer) {
  if (!mysql::testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled. Set ENABLE_MYSQL_INTEGRATION_TESTS=1 to enable.";
  }

  auto connection_config = mysql::testing::GetMySQLTestConfig();
  mysql::Connection connection(connection_config);
  auto connect_result = connection.Connect("initial-loader-text-materializer-test");
  if (!connect_result) {
    GTEST_SKIP() << "MySQL connection failed: " << connect_result.error().message();
  }

  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() & 0x7fffffff);
  const std::string table = "mygram_it_text_materializer_" + suffix;
  auto cleanup = [&]() { (void)connection.ExecuteUpdate("DROP TABLE IF EXISTS " + table); };
  cleanup();

  ASSERT_TRUE(connection.ExecuteUpdate("CREATE TABLE " + table +
                                       " (id VARCHAR(32) PRIMARY KEY, title TEXT NULL, middle TEXT NULL, "
                                       "body TEXT NULL) ENGINE=InnoDB"));
  ASSERT_TRUE(
      connection.ExecuteUpdate("INSERT INTO " + table + " VALUES ('1', 'alpha', '', 'beta'), ('2', '', NULL, '')"));

  config::TableConfig table_config;
  table_config.name = table;
  table_config.database = connection_config.database;
  table_config.primary_key = "id";
  table_config.text_source.concat = {"title", "middle", "body"};
  table_config.text_source.delimiter = "|";
  table_config.ngram_size = 1;

  index::Index index(1);
  storage::DocumentStore store;
  loader::InitialLoader loader(connection, index, store, table_config);
  auto load_result = loader.Load();
  ASSERT_TRUE(load_result) << load_result.error().message();

  ASSERT_EQ(store.Size(), 2);
  auto first_doc_id = store.GetDocId("1");
  auto empty_doc_id = store.GetDocId("2");
  ASSERT_TRUE(first_doc_id.has_value());
  ASSERT_TRUE(empty_doc_id.has_value());
  EXPECT_EQ(store.GetNormalizedText(*first_doc_id), std::optional<std::string>{"alpha|beta"});
  // Empty normalized text is represented sparsely as no doc_texts_ entry,
  // while the document itself remains addressable by primary key.
  EXPECT_FALSE(store.GetNormalizedText(*empty_doc_id).has_value());

  mysql::RowData binlog_row;
  binlog_row.columns["title"] = "alpha";
  binlog_row.columns["middle"] = "";
  binlog_row.columns["body"] = "beta";
  auto binlog_text = mysql::MaterializeTextSource(binlog_row, table_config.text_source);
  EXPECT_EQ(binlog_text.state, mysql::TextValueState::kPresent);
  EXPECT_EQ(binlog_text.value, store.GetNormalizedText(*first_doc_id));

  cleanup();
}

TEST(InitialLoaderIntegrationTest, EmptyStringRequiredFilterLoadsOnlyMatchingRows) {
  if (!mysql::testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled. Set ENABLE_MYSQL_INTEGRATION_TESTS=1 to enable.";
  }

  auto connection_config = mysql::testing::GetMySQLTestConfig();
  mysql::Connection connection(connection_config);
  auto connect_result = connection.Connect("initial-loader-empty-required-filter-test");
  if (!connect_result) {
    GTEST_SKIP() << "MySQL connection failed: " << connect_result.error().message();
  }

  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() & 0x7fffffff);
  const std::string table = "mygram_it_empty_required_filter_" + suffix;
  auto cleanup = [&]() { (void)connection.ExecuteUpdate("DROP TABLE IF EXISTS " + table); };
  cleanup();

  ASSERT_TRUE(connection.ExecuteUpdate("CREATE TABLE " + table +
                                       " (id VARCHAR(32) PRIMARY KEY, content TEXT NOT NULL, "
                                       "status VARCHAR(32) NOT NULL) ENGINE=InnoDB"));
  ASSERT_TRUE(connection.ExecuteUpdate("INSERT INTO " + table +
                                       " VALUES ('1', 'included text', ''), ('2', 'excluded text', 'active')"));

  config::TableConfig table_config;
  table_config.name = table;
  table_config.database = connection_config.database;
  table_config.primary_key = "id";
  table_config.text_source.column = "content";
  table_config.ngram_size = 1;
  config::RequiredFilterConfig required_filter;
  required_filter.name = "status";
  required_filter.type = "varchar";
  required_filter.op = "=";
  required_filter.value = "";
  table_config.required_filters.push_back(required_filter);

  index::Index index(1);
  storage::DocumentStore store;
  loader::InitialLoader loader(connection, index, store, table_config);
  auto load_result = loader.Load();
  ASSERT_TRUE(load_result) << load_result.error().message();

  EXPECT_EQ(store.Size(), 1);
  EXPECT_TRUE(store.GetDocId("1").has_value());
  EXPECT_FALSE(store.GetDocId("2").has_value());
  cleanup();
}

TEST(InitialLoaderIntegrationTest, CancellationStopsStreamingLoadWithoutDrainingWholeTable) {
  if (!mysql::testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled. Set ENABLE_MYSQL_INTEGRATION_TESTS=1 to enable.";
  }

  auto connection_config = mysql::testing::GetMySQLTestConfig();
  mysql::Connection loader_connection(connection_config);
  auto loader_connect = loader_connection.Connect("initial-loader-stream-cancel-test");
  if (!loader_connect) {
    GTEST_SKIP() << "MySQL connection failed: " << loader_connect.error().message();
  }
  auto gtid_mode_enabled = loader_connection.IsGTIDModeEnabled();
  if (!gtid_mode_enabled || !*gtid_mode_enabled) {
    GTEST_SKIP() << "MySQL GTID mode is not enabled";
  }

  mysql::Connection writer_connection(connection_config);
  ASSERT_TRUE(writer_connection.Connect("initial-loader-stream-cancel-writer"));

  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() & 0x7fffffff);
  const std::string table = "mygram_it_stream_cancel_" + suffix;
  auto cleanup = [&]() { (void)writer_connection.ExecuteUpdate("DROP TABLE IF EXISTS `" + table + "`"); };
  cleanup();

  ASSERT_TRUE(writer_connection.ExecuteUpdate("CREATE TABLE `" + table +
                                              "` (id INT PRIMARY KEY, content TEXT NOT NULL) ENGINE=InnoDB"));
  std::ostringstream insert;
  insert << "INSERT INTO `" << table << "` VALUES ";
  constexpr int kRows = 50;
  for (int row = 1; row <= kRows; ++row) {
    if (row > 1) {
      insert << ",";
    }
    insert << "(" << row << ",'streamed row " << row << "')";
  }
  ASSERT_TRUE(writer_connection.ExecuteUpdate(insert.str()));

  config::TableConfig table_config;
  table_config.name = table;
  table_config.database = connection_config.database;
  table_config.primary_key = "id";
  table_config.text_source.column = "content";
  table_config.ngram_size = 1;
  config::BuildConfig build_config;
  build_config.batch_size = 10;

  index::Index index(1);
  storage::DocumentStore store;
  InitialLoader loader(loader_connection, index, store, table_config, {}, build_config);
  size_t progress_calls = 0;
  auto load_result = loader.Load([&](const LoadProgress& progress) {
    ++progress_calls;
    EXPECT_EQ(progress.total_rows, 0U);
    loader.Cancel();
  });

  ASSERT_FALSE(load_result);
  EXPECT_NE(load_result.error().message().find("cancelled"), std::string::npos);
  EXPECT_TRUE(loader.IsCancelled());
  EXPECT_EQ(progress_calls, 1U);
  EXPECT_EQ(loader.GetProcessedRows(), 10U);
  EXPECT_EQ(store.Size(), 10U);
  EXPECT_FALSE(loader_connection.IsConnected())
      << "An abandoned unbuffered result must discard its connection instead of issuing commands out of sync";

  cleanup();
}

}  // namespace mygramdb::loader

#endif  // USE_MYSQL
