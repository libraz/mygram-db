/**
 * @file schema_change_detection_test.cpp
 * @brief A cached table's decoding properties are compared, not just its shape
 *
 * A TABLE_MAP event restates a table's columns before every batch of rows. The
 * cache answers whether anything that decides how those rows decode has moved,
 * and that answer drives the diagnostic an operator sees and the refetch of the
 * properties the wire does not carry.
 */

#include <gtest/gtest.h>

#include <cstdint>
#include <string>

#ifdef USE_MYSQL

#include "mysql/table_metadata.h"

namespace mygramdb::mysql {
namespace {

TableMetadata MakeMetadata(bool is_unsigned) {
  TableMetadata metadata;
  metadata.table_id = 7;
  metadata.database_name = "testdb";
  metadata.table_name = "articles";
  metadata.columns.push_back({ColumnType::LONGLONG, "id", 0, false, is_unsigned, {}});
  metadata.columns.push_back({ColumnType::BLOB, "content", 2, true, false, {}});
  metadata.RebuildColumnOrdinals();
  return metadata;
}

/**
 * @brief A signedness change is a schema change.
 *
 * A TABLE_MAP carries the same type code for both signed and unsigned columns,
 * so signedness is one of the properties fetched separately and cached. An
 * ALTER that changes only it leaves every field width identical and would
 * otherwise be reported as an unchanged table, skipping both the operator
 * diagnostic and the refetch that keeps the cached property true.
 */
TEST(SchemaChangeDetectionTest, ChangingOnlySignednessIsReportedAsASchemaChange) {
  TableMetadataCache cache;
  ASSERT_EQ(cache.AddOrUpdate(7, MakeMetadata(false)), TableMetadataCache::AddResult::kAdded);

  EXPECT_EQ(cache.AddOrUpdate(7, MakeMetadata(true)), TableMetadataCache::AddResult::kSchemaChanged);
  EXPECT_EQ(cache.AddOrUpdate(7, MakeMetadata(true)), TableMetadataCache::AddResult::kUpdated);
  EXPECT_EQ(cache.AddOrUpdate(7, MakeMetadata(false)), TableMetadataCache::AddResult::kSchemaChanged);
}

/**
 * @brief The same change is reported when the table is remapped to a new id.
 *
 * A reconnect or a table reopen gives the same table a new TABLE_MAP id, and
 * the entry for the previous id is replaced. The comparison has to survive that
 * replacement, or a signedness change hidden behind a reconnect is silent.
 */
TEST(SchemaChangeDetectionTest, SignednessIsComparedAcrossATableIdChange) {
  TableMetadataCache cache;
  ASSERT_EQ(cache.AddOrUpdate(7, MakeMetadata(false)), TableMetadataCache::AddResult::kAdded);

  auto remapped = MakeMetadata(true);
  remapped.table_id = 8;
  EXPECT_EQ(cache.AddOrUpdate(8, remapped), TableMetadataCache::AddResult::kSchemaChanged);
  EXPECT_EQ(cache.Size(), 1U);
}

/**
 * @brief Restating an identical table is not a schema change.
 */
TEST(SchemaChangeDetectionTest, AnIdenticalRestatementIsNotASchemaChange) {
  TableMetadataCache cache;
  ASSERT_EQ(cache.AddOrUpdate(7, MakeMetadata(true)), TableMetadataCache::AddResult::kAdded);
  EXPECT_EQ(cache.AddOrUpdate(7, MakeMetadata(true)), TableMetadataCache::AddResult::kUpdated);
}

}  // namespace
}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
