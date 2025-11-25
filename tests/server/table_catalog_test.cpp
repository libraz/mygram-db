/**
 * @file table_catalog_test.cpp
 * @brief Unit tests for TableCatalog class
 *
 * Tests table context management, lookup, and state flag operations.
 */

#include "server/table_catalog.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "index/index.h"
#include "server/server_types.h"
#include "storage/document_store.h"

using namespace mygramdb::server;
using namespace mygramdb::index;
using namespace mygramdb::storage;
using namespace mygramdb::config;

namespace {

/**
 * @brief Helper to create a minimal TableContext for testing
 */
std::unique_ptr<TableContext> CreateTableContext(const std::string& name) {
  auto ctx = std::make_unique<TableContext>();
  ctx->name = name;
  ctx->config.name = name;
  ctx->config.primary_key = "id";
  ctx->index = std::make_unique<Index>();
  ctx->doc_store = std::make_unique<DocumentStore>();
  return ctx;
}

}  // namespace

class TableCatalogTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create table contexts
    table1_ = CreateTableContext("articles");
    table2_ = CreateTableContext("comments");
    table3_ = CreateTableContext("users");

    // Build table map (raw pointers)
    tables_["articles"] = table1_.get();
    tables_["comments"] = table2_.get();
    tables_["users"] = table3_.get();
  }

  std::unique_ptr<TableContext> table1_;
  std::unique_ptr<TableContext> table2_;
  std::unique_ptr<TableContext> table3_;
  std::unordered_map<std::string, TableContext*> tables_;
};

// ===========================================================================
// Constructor tests
// ===========================================================================

TEST_F(TableCatalogTest, ConstructWithEmptyMap) {
  std::unordered_map<std::string, TableContext*> empty_tables;
  TableCatalog catalog(empty_tables);

  EXPECT_TRUE(catalog.GetTableNames().empty());
}

TEST_F(TableCatalogTest, ConstructWithMultipleTables) {
  TableCatalog catalog(tables_);

  auto names = catalog.GetTableNames();
  EXPECT_EQ(names.size(), 3u);
}

// ===========================================================================
// GetTable tests
// ===========================================================================

TEST_F(TableCatalogTest, GetTableExisting) {
  TableCatalog catalog(tables_);

  auto* result = catalog.GetTable("articles");
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->name, "articles");
}

TEST_F(TableCatalogTest, GetTableNonExisting) {
  TableCatalog catalog(tables_);

  auto* result = catalog.GetTable("nonexistent");
  EXPECT_EQ(result, nullptr);
}

TEST_F(TableCatalogTest, GetTableEmptyName) {
  TableCatalog catalog(tables_);

  auto* result = catalog.GetTable("");
  EXPECT_EQ(result, nullptr);
}

TEST_F(TableCatalogTest, GetTableReturnsCorrectContext) {
  TableCatalog catalog(tables_);

  auto* articles = catalog.GetTable("articles");
  auto* comments = catalog.GetTable("comments");

  ASSERT_NE(articles, nullptr);
  ASSERT_NE(comments, nullptr);
  EXPECT_NE(articles, comments);
  EXPECT_EQ(articles, table1_.get());
  EXPECT_EQ(comments, table2_.get());
}

// ===========================================================================
// TableExists tests
// ===========================================================================

TEST_F(TableCatalogTest, TableExistsReturnsTrue) {
  TableCatalog catalog(tables_);

  EXPECT_TRUE(catalog.TableExists("articles"));
  EXPECT_TRUE(catalog.TableExists("comments"));
  EXPECT_TRUE(catalog.TableExists("users"));
}

TEST_F(TableCatalogTest, TableExistsReturnsFalse) {
  TableCatalog catalog(tables_);

  EXPECT_FALSE(catalog.TableExists("nonexistent"));
  EXPECT_FALSE(catalog.TableExists(""));
  EXPECT_FALSE(catalog.TableExists("ARTICLES"));  // Case-sensitive
}

// ===========================================================================
// GetTableNames tests
// ===========================================================================

TEST_F(TableCatalogTest, GetTableNamesReturnsAllNames) {
  TableCatalog catalog(tables_);

  auto names = catalog.GetTableNames();
  EXPECT_EQ(names.size(), 3u);

  // Check all expected names are present (order may vary)
  std::unordered_set<std::string> name_set(names.begin(), names.end());
  EXPECT_TRUE(name_set.count("articles"));
  EXPECT_TRUE(name_set.count("comments"));
  EXPECT_TRUE(name_set.count("users"));
}

TEST_F(TableCatalogTest, GetTableNamesEmptyCatalog) {
  std::unordered_map<std::string, TableContext*> empty_tables;
  TableCatalog catalog(empty_tables);

  auto names = catalog.GetTableNames();
  EXPECT_TRUE(names.empty());
}

// ===========================================================================
// GetDumpableContexts tests
// ===========================================================================

TEST_F(TableCatalogTest, GetDumpableContextsReturnsAllContexts) {
  TableCatalog catalog(tables_);

  auto dumpable = catalog.GetDumpableContexts();
  EXPECT_EQ(dumpable.size(), 3u);

  // Check each table has correct index and doc_store pointers
  ASSERT_TRUE(dumpable.count("articles"));
  EXPECT_EQ(dumpable["articles"].first, table1_->index.get());
  EXPECT_EQ(dumpable["articles"].second, table1_->doc_store.get());

  ASSERT_TRUE(dumpable.count("comments"));
  EXPECT_EQ(dumpable["comments"].first, table2_->index.get());
  EXPECT_EQ(dumpable["comments"].second, table2_->doc_store.get());
}

TEST_F(TableCatalogTest, GetDumpableContextsEmptyCatalog) {
  std::unordered_map<std::string, TableContext*> empty_tables;
  TableCatalog catalog(empty_tables);

  auto dumpable = catalog.GetDumpableContexts();
  EXPECT_TRUE(dumpable.empty());
}

// ===========================================================================
// ReadOnly flag tests
// ===========================================================================

TEST_F(TableCatalogTest, ReadOnlyInitiallyFalse) {
  TableCatalog catalog(tables_);

  EXPECT_FALSE(catalog.IsReadOnly());
}

TEST_F(TableCatalogTest, SetReadOnlyTrue) {
  TableCatalog catalog(tables_);

  catalog.SetReadOnly(true);
  EXPECT_TRUE(catalog.IsReadOnly());
}

TEST_F(TableCatalogTest, SetReadOnlyFalse) {
  TableCatalog catalog(tables_);

  catalog.SetReadOnly(true);
  catalog.SetReadOnly(false);
  EXPECT_FALSE(catalog.IsReadOnly());
}

TEST_F(TableCatalogTest, SetReadOnlyToggle) {
  TableCatalog catalog(tables_);

  for (int i = 0; i < 10; ++i) {
    bool expected = (i % 2 == 0);
    catalog.SetReadOnly(expected);
    EXPECT_EQ(catalog.IsReadOnly(), expected);
  }
}

// ===========================================================================
// Loading flag tests
// ===========================================================================

TEST_F(TableCatalogTest, LoadingInitiallyFalse) {
  TableCatalog catalog(tables_);

  EXPECT_FALSE(catalog.IsLoading());
}

TEST_F(TableCatalogTest, SetLoadingTrue) {
  TableCatalog catalog(tables_);

  catalog.SetLoading(true);
  EXPECT_TRUE(catalog.IsLoading());
}

TEST_F(TableCatalogTest, SetLoadingFalse) {
  TableCatalog catalog(tables_);

  catalog.SetLoading(true);
  catalog.SetLoading(false);
  EXPECT_FALSE(catalog.IsLoading());
}

// ===========================================================================
// GetTables tests
// ===========================================================================

TEST_F(TableCatalogTest, GetTablesReturnsConstReference) {
  TableCatalog catalog(tables_);

  const auto& tables_ref = catalog.GetTables();
  EXPECT_EQ(tables_ref.size(), 3u);
  EXPECT_EQ(tables_ref.at("articles"), table1_.get());
}

// ===========================================================================
// Thread safety tests
// ===========================================================================

TEST_F(TableCatalogTest, ConcurrentReadAccess) {
  TableCatalog catalog(tables_);

  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  // Launch multiple readers concurrently
  for (int i = 0; i < 10; ++i) {
    threads.emplace_back([&catalog, &success_count]() {
      for (int j = 0; j < 100; ++j) {
        auto* table = catalog.GetTable("articles");
        if (table != nullptr) {
          ++success_count;
        }
        auto exists = catalog.TableExists("comments");
        if (exists) {
          ++success_count;
        }
        auto names = catalog.GetTableNames();
        if (names.size() == 3) {
          ++success_count;
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // All reads should succeed
  EXPECT_EQ(success_count.load(), 10 * 100 * 3);
}

TEST_F(TableCatalogTest, ConcurrentFlagAccess) {
  TableCatalog catalog(tables_);

  std::vector<std::thread> threads;
  std::atomic<bool> error_detected{false};

  // Writers toggle flags
  for (int i = 0; i < 5; ++i) {
    threads.emplace_back([&catalog, &error_detected]() {
      for (int j = 0; j < 100; ++j) {
        catalog.SetReadOnly(j % 2 == 0);
        catalog.SetLoading(j % 3 == 0);
      }
    });
  }

  // Readers check flags
  for (int i = 0; i < 5; ++i) {
    threads.emplace_back([&catalog, &error_detected]() {
      for (int j = 0; j < 100; ++j) {
        // Just ensure no crash - value is indeterminate due to concurrent writes
        bool ro = catalog.IsReadOnly();
        bool ld = catalog.IsLoading();
        (void)ro;
        (void)ld;
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  EXPECT_FALSE(error_detected.load());
}

// ===========================================================================
// Edge cases
// ===========================================================================

TEST_F(TableCatalogTest, SingleTableCatalog) {
  std::unordered_map<std::string, TableContext*> single_table;
  single_table["only_table"] = table1_.get();

  TableCatalog catalog(single_table);

  EXPECT_TRUE(catalog.TableExists("only_table"));
  EXPECT_FALSE(catalog.TableExists("articles"));
  EXPECT_EQ(catalog.GetTableNames().size(), 1u);
}

TEST_F(TableCatalogTest, TableNameWithSpecialCharacters) {
  auto special_table = CreateTableContext("table-with_special.chars");
  std::unordered_map<std::string, TableContext*> tables;
  tables["table-with_special.chars"] = special_table.get();

  TableCatalog catalog(tables);

  EXPECT_TRUE(catalog.TableExists("table-with_special.chars"));
  EXPECT_NE(catalog.GetTable("table-with_special.chars"), nullptr);
}
