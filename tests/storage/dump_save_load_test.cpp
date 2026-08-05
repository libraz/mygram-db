/**
 * @file dump_save_load_test.cpp
 * @brief MySQL-backed snapshot component round-trip integration test
 */

#ifdef USE_MYSQL

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <string>

#include "config/config.h"
#include "index/index.h"
#include "loader/initial_loader.h"
#include "mysql/connection.h"
#include "mysql_test_helpers.h"
#include "storage/document_store.h"
#include "utils/fd_guard.h"

namespace mygramdb::storage {
namespace {

TEST(DumpSaveLoadIntegrationTest, RoundTripsFilteredInitialSnapshot) {
  if (!mysql::testing::ShouldRunMySQLIntegrationTests()) {
    GTEST_SKIP() << "MySQL integration tests are disabled. Set ENABLE_MYSQL_INTEGRATION_TESTS=1 to enable.";
  }

  const auto connection_config = mysql::testing::GetMySQLTestConfig();
  mysql::Connection connection(connection_config);
  auto connect = connection.Connect("dump-save-load-integration-test");
  ASSERT_TRUE(connect) << connect.error().message();

  const auto suffix = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count() & 0x7fffffff);
  const std::string table = "dump_roundtrip_" + suffix;
  const std::string quoted_table = "`" + table + "`";
  const auto temp_dir = std::filesystem::temp_directory_path();
  const std::string index_file = (temp_dir / ("mygramdb_index_" + suffix + ".dat")).string();
  const std::string doc_store_file = (temp_dir / ("mygramdb_docstore_" + suffix + ".dat")).string();

  auto cleanup = utils::ScopeGuard([&]() {
    (void)connection.ExecuteUpdate("DROP TABLE IF EXISTS " + quoted_table);
    std::error_code error;
    (void)std::filesystem::remove(index_file, error);
    error.clear();
    (void)std::filesystem::remove(doc_store_file, error);
  });

  ASSERT_TRUE(connection.ExecuteUpdate("DROP TABLE IF EXISTS " + quoted_table));
  ASSERT_TRUE(connection.ExecuteUpdate("CREATE TABLE " + quoted_table +
                                       " (id BIGINT PRIMARY KEY, name VARCHAR(255) NOT NULL, enabled INT NOT NULL,"
                                       " comic_type_id INT NOT NULL) ENGINE=InnoDB"));
  ASSERT_TRUE(connection.ExecuteUpdate("INSERT INTO " + quoted_table +
                                       " VALUES (100, 'snapshot fixture', 1, 7), (200, 'disabled fixture', 0, 8),"
                                       " (20000, 'outside range fixture', 1, 9)"));

  config::TableConfig table_config;
  table_config.database = connection_config.database;
  table_config.name = table;
  table_config.primary_key = "id";
  table_config.text_source.column = "name";
  table_config.ngram_size = 2;
  table_config.required_filters = {
      {"enabled", "int", "=", "1", false},
      {"id", "int", "<", "10000", false},
  };
  table_config.filters = {
      {"comic_type_id", "int", true, true, ""},
  };

  index::Index index(table_config.ngram_size);
  DocumentStore doc_store;
  loader::InitialLoader initial_loader(connection, index, doc_store, table_config);
  auto load = initial_loader.Load();
  ASSERT_TRUE(load) << load.error().message();
  ASSERT_EQ(initial_loader.GetProcessedRows(), 1U);
  ASSERT_FALSE(initial_loader.GetStartGTID().empty());

  const auto original_doc_id = doc_store.GetDocId("100");
  ASSERT_TRUE(original_doc_id.has_value());
  EXPECT_FALSE(doc_store.GetDocId("200").has_value());
  EXPECT_FALSE(doc_store.GetDocId("20000").has_value());
  EXPECT_EQ(index.SearchAnd({"sn"}), std::vector<DocId>{*original_doc_id});

  auto save_index = index.SaveToFile(index_file);
  ASSERT_TRUE(save_index) << save_index.error().message();
  auto save_doc_store = doc_store.SaveToFile(doc_store_file, initial_loader.GetStartGTID());
  ASSERT_TRUE(save_doc_store) << save_doc_store.error().message();

  index::Index loaded_index(table_config.ngram_size);
  DocumentStore loaded_doc_store;
  auto load_index = loaded_index.LoadFromFile(index_file);
  ASSERT_TRUE(load_index) << load_index.error().message();
  std::string loaded_gtid;
  auto load_doc_store = loaded_doc_store.LoadFromFile(doc_store_file, &loaded_gtid);
  ASSERT_TRUE(load_doc_store) << load_doc_store.error().message();

  EXPECT_EQ(loaded_gtid, initial_loader.GetStartGTID());
  EXPECT_EQ(loaded_doc_store.Size(), 1U);
  EXPECT_EQ(loaded_doc_store.GetDocId("100"), original_doc_id);
  EXPECT_FALSE(loaded_doc_store.GetDocId("200").has_value());
  EXPECT_FALSE(loaded_doc_store.GetDocId("20000").has_value());
  EXPECT_EQ(loaded_index.SearchAnd({"sn"}), std::vector<DocId>{*original_doc_id});
}

}  // namespace
}  // namespace mygramdb::storage

#endif  // USE_MYSQL
