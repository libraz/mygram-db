/**
 * @file table_metadata_test.cpp
 * @brief Unit tests for TableMetadataCache class
 */

#ifdef USE_MYSQL

#include "mysql/table_metadata.h"

#include <gtest/gtest.h>

namespace mygramdb::mysql {

class TableMetadataCacheTest : public ::testing::Test {
 protected:
  TableMetadataCache cache_;

  TableMetadata CreateTestMetadata(uint64_t id, const std::string& db, const std::string& table) {
    TableMetadata metadata;
    metadata.table_id = id;
    metadata.database_name = db;
    metadata.table_name = table;
    return metadata;
  }
};

// ===========================================================================
// Basic operations
// ===========================================================================

TEST_F(TableMetadataCacheTest, AddAndGet) {
  auto metadata = CreateTestMetadata(100, "testdb", "users");
  metadata.columns.push_back({ColumnType::LONG, "id", 0, false, false});
  metadata.columns.push_back({ColumnType::VARCHAR, "name", 255, true, false});

  cache_.Add(100, metadata);

  const TableMetadata* retrieved = cache_.Get(100);
  ASSERT_NE(retrieved, nullptr);
  EXPECT_EQ(retrieved->table_id, 100);
  EXPECT_EQ(retrieved->database_name, "testdb");
  EXPECT_EQ(retrieved->table_name, "users");
  EXPECT_EQ(retrieved->columns.size(), 2);
}

TEST_F(TableMetadataCacheTest, GetNonExistent) {
  const TableMetadata* retrieved = cache_.Get(999);
  EXPECT_EQ(retrieved, nullptr);
}

TEST_F(TableMetadataCacheTest, GetFromEmptyCache) {
  const TableMetadata* retrieved = cache_.Get(1);
  EXPECT_EQ(retrieved, nullptr);
}

TEST_F(TableMetadataCacheTest, AddMultipleTables) {
  cache_.Add(1, CreateTestMetadata(1, "db1", "table1"));
  cache_.Add(2, CreateTestMetadata(2, "db1", "table2"));
  cache_.Add(3, CreateTestMetadata(3, "db2", "table1"));

  EXPECT_NE(cache_.Get(1), nullptr);
  EXPECT_NE(cache_.Get(2), nullptr);
  EXPECT_NE(cache_.Get(3), nullptr);
  EXPECT_EQ(cache_.Get(4), nullptr);

  EXPECT_EQ(cache_.Get(1)->table_name, "table1");
  EXPECT_EQ(cache_.Get(2)->table_name, "table2");
  EXPECT_EQ(cache_.Get(3)->database_name, "db2");
}

TEST_F(TableMetadataCacheTest, UpdateExistingEntry) {
  cache_.Add(1, CreateTestMetadata(1, "db1", "old_name"));
  EXPECT_EQ(cache_.Get(1)->table_name, "old_name");

  // Update with new metadata
  cache_.Add(1, CreateTestMetadata(1, "db1", "new_name"));
  EXPECT_EQ(cache_.Get(1)->table_name, "new_name");
}

// ===========================================================================
// Remove operations
// ===========================================================================

TEST_F(TableMetadataCacheTest, Remove) {
  cache_.Add(1, CreateTestMetadata(1, "db", "table"));
  EXPECT_NE(cache_.Get(1), nullptr);

  cache_.Remove(1);
  EXPECT_EQ(cache_.Get(1), nullptr);
}

TEST_F(TableMetadataCacheTest, RemoveNonExistent) {
  // Should not crash
  cache_.Remove(999);
  EXPECT_EQ(cache_.Get(999), nullptr);
}

TEST_F(TableMetadataCacheTest, RemoveFromEmptyCache) {
  // Should not crash
  cache_.Remove(1);
  EXPECT_EQ(cache_.Get(1), nullptr);
}

TEST_F(TableMetadataCacheTest, RemoveDoesNotAffectOthers) {
  cache_.Add(1, CreateTestMetadata(1, "db", "table1"));
  cache_.Add(2, CreateTestMetadata(2, "db", "table2"));
  cache_.Add(3, CreateTestMetadata(3, "db", "table3"));

  cache_.Remove(2);

  EXPECT_NE(cache_.Get(1), nullptr);
  EXPECT_EQ(cache_.Get(2), nullptr);
  EXPECT_NE(cache_.Get(3), nullptr);
}

// ===========================================================================
// Clear operations
// ===========================================================================

TEST_F(TableMetadataCacheTest, Clear) {
  cache_.Add(1, CreateTestMetadata(1, "db", "table1"));
  cache_.Add(2, CreateTestMetadata(2, "db", "table2"));
  cache_.Add(3, CreateTestMetadata(3, "db", "table3"));

  cache_.Clear();

  EXPECT_EQ(cache_.Get(1), nullptr);
  EXPECT_EQ(cache_.Get(2), nullptr);
  EXPECT_EQ(cache_.Get(3), nullptr);
}

TEST_F(TableMetadataCacheTest, ClearEmptyCache) {
  // Should not crash
  cache_.Clear();
  EXPECT_EQ(cache_.Get(1), nullptr);
}

TEST_F(TableMetadataCacheTest, AddAfterClear) {
  cache_.Add(1, CreateTestMetadata(1, "db", "table1"));
  cache_.Clear();

  cache_.Add(2, CreateTestMetadata(2, "db", "table2"));
  EXPECT_EQ(cache_.Get(1), nullptr);
  EXPECT_NE(cache_.Get(2), nullptr);
}

// ===========================================================================
// Column metadata tests
// ===========================================================================

TEST_F(TableMetadataCacheTest, ColumnTypes) {
  TableMetadata metadata;
  metadata.table_id = 1;
  metadata.database_name = "test";
  metadata.table_name = "all_types";

  // Add various column types
  metadata.columns.push_back({ColumnType::TINY, "tiny_col", 0, false, false});
  metadata.columns.push_back({ColumnType::SHORT, "short_col", 0, false, true});
  metadata.columns.push_back({ColumnType::LONG, "int_col", 0, true, false});
  metadata.columns.push_back({ColumnType::LONGLONG, "bigint_col", 0, false, true});
  metadata.columns.push_back({ColumnType::FLOAT, "float_col", 0, true, false});
  metadata.columns.push_back({ColumnType::DOUBLE, "double_col", 0, true, false});
  metadata.columns.push_back({ColumnType::VARCHAR, "varchar_col", 255, true, false});
  metadata.columns.push_back({ColumnType::BLOB, "text_col", 0, true, false});
  metadata.columns.push_back({ColumnType::DATETIME, "datetime_col", 0, true, false});
  metadata.columns.push_back({ColumnType::DATETIME2, "datetime2_col", 6, true, false});
  metadata.columns.push_back({ColumnType::JSON, "json_col", 0, true, false});

  cache_.Add(1, metadata);

  const TableMetadata* retrieved = cache_.Get(1);
  ASSERT_NE(retrieved, nullptr);
  ASSERT_EQ(retrieved->columns.size(), 11);

  // Verify specific columns
  EXPECT_EQ(retrieved->columns[0].type, ColumnType::TINY);
  EXPECT_FALSE(retrieved->columns[0].is_nullable);
  EXPECT_FALSE(retrieved->columns[0].is_unsigned);

  EXPECT_EQ(retrieved->columns[1].type, ColumnType::SHORT);
  EXPECT_TRUE(retrieved->columns[1].is_unsigned);

  EXPECT_EQ(retrieved->columns[6].type, ColumnType::VARCHAR);
  EXPECT_EQ(retrieved->columns[6].metadata, 255);

  EXPECT_EQ(retrieved->columns[9].type, ColumnType::DATETIME2);
  EXPECT_EQ(retrieved->columns[9].metadata, 6);  // Fractional seconds precision
}

TEST_F(TableMetadataCacheTest, ColumnBitmaps) {
  TableMetadata metadata;
  metadata.table_id = 1;
  metadata.database_name = "test";
  metadata.table_name = "bitmap_test";

  // Set up column bitmaps
  metadata.columns_before_image = {0xFF, 0x0F};  // 12 columns used for before image
  metadata.columns_after_image = {0xFF, 0xFF};   // 16 columns used for after image

  cache_.Add(1, metadata);

  const TableMetadata* retrieved = cache_.Get(1);
  ASSERT_NE(retrieved, nullptr);
  ASSERT_EQ(retrieved->columns_before_image.size(), 2);
  ASSERT_EQ(retrieved->columns_after_image.size(), 2);
  EXPECT_EQ(retrieved->columns_before_image[0], 0xFF);
  EXPECT_EQ(retrieved->columns_before_image[1], 0x0F);
  EXPECT_EQ(retrieved->columns_after_image[0], 0xFF);
  EXPECT_EQ(retrieved->columns_after_image[1], 0xFF);
}

// ===========================================================================
// Large table ID tests
// ===========================================================================

TEST_F(TableMetadataCacheTest, LargeTableId) {
  uint64_t large_id = 0xFFFFFFFFFFFFFFFF;
  cache_.Add(large_id, CreateTestMetadata(large_id, "db", "table"));

  const TableMetadata* retrieved = cache_.Get(large_id);
  ASSERT_NE(retrieved, nullptr);
  EXPECT_EQ(retrieved->table_id, large_id);
}

TEST_F(TableMetadataCacheTest, ZeroTableId) {
  cache_.Add(0, CreateTestMetadata(0, "db", "table"));

  const TableMetadata* retrieved = cache_.Get(0);
  ASSERT_NE(retrieved, nullptr);
  EXPECT_EQ(retrieved->table_id, 0);
}

// ===========================================================================
// Edge cases
// ===========================================================================

TEST_F(TableMetadataCacheTest, EmptyStrings) {
  TableMetadata metadata;
  metadata.table_id = 1;
  metadata.database_name = "";
  metadata.table_name = "";

  cache_.Add(1, metadata);

  const TableMetadata* retrieved = cache_.Get(1);
  ASSERT_NE(retrieved, nullptr);
  EXPECT_TRUE(retrieved->database_name.empty());
  EXPECT_TRUE(retrieved->table_name.empty());
}

TEST_F(TableMetadataCacheTest, UnicodeTableNames) {
  TableMetadata metadata;
  metadata.table_id = 1;
  metadata.database_name = "test_db";
  metadata.table_name = "users_table";  // Regular ASCII name

  cache_.Add(1, metadata);

  const TableMetadata* retrieved = cache_.Get(1);
  ASSERT_NE(retrieved, nullptr);
  EXPECT_EQ(retrieved->table_name, "users_table");
}

TEST_F(TableMetadataCacheTest, MultipleAddRemoveCycles) {
  for (int cycle = 0; cycle < 3; ++cycle) {
    // Add entries
    for (uint64_t i = 1; i <= 10; ++i) {
      cache_.Add(i, CreateTestMetadata(i, "db", "table" + std::to_string(i)));
    }

    // Verify all entries
    for (uint64_t i = 1; i <= 10; ++i) {
      EXPECT_NE(cache_.Get(i), nullptr);
    }

    // Clear
    cache_.Clear();

    // Verify all entries removed
    for (uint64_t i = 1; i <= 10; ++i) {
      EXPECT_EQ(cache_.Get(i), nullptr);
    }
  }
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
