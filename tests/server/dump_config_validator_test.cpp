/**
 * @file dump_config_validator_test.cpp
 * @brief Table identity matching in the shared dump/config compatibility check
 */

#include "server/dump_config_validator.h"

#include <gtest/gtest.h>

#include <string>

#include "config/config.h"

namespace {

using mygramdb::config::Config;
using mygramdb::config::TableConfig;
using mygramdb::server::FindDumpConfigMismatch;

/// @param kanji_ngram_size Zero mirrors ngram_size, the way configuration load
///        resolves an omitted value; pass a value to vary it on its own.
TableConfig MakeTable(const std::string& database, const std::string& name, int ngram_size, int kanji_ngram_size = 0) {
  TableConfig table;
  table.database = database;
  table.name = name;
  table.ngram_size = ngram_size;
  table.kanji_ngram_size = (kanji_ngram_size != 0) ? kanji_ngram_size : ngram_size;
  return table;
}

}  // namespace

TEST(DumpConfigValidatorTest, NgramSizeOfTheSameQualifiedTableIsCompared) {
  Config live_config;
  live_config.tables.push_back(MakeTable("db1", "articles", 3));
  live_config.tables.push_back(MakeTable("db2", "articles", 2));

  Config loaded_config;
  loaded_config.tables.push_back(MakeTable("db2", "articles", 3));

  const auto mismatch = FindDumpConfigMismatch(loaded_config, live_config);
  ASSERT_TRUE(mismatch.has_value()) << "a dump for db2.articles must be compared against db2.articles";
  EXPECT_NE(mismatch->find("db2.articles"), std::string::npos) << *mismatch;
  EXPECT_NE(mismatch->find("ngram_size mismatch"), std::string::npos) << *mismatch;
}

TEST(DumpConfigValidatorTest, MatchingQualifiedTableIsAcceptedWhenAnotherDatabaseDiffers) {
  Config live_config;
  live_config.tables.push_back(MakeTable("db1", "articles", 2));
  live_config.tables.push_back(MakeTable("db2", "articles", 3));

  Config loaded_config;
  loaded_config.tables.push_back(MakeTable("db2", "articles", 3));

  EXPECT_FALSE(FindDumpConfigMismatch(loaded_config, live_config).has_value());
}

TEST(DumpConfigValidatorTest, SameBareNameInTwoDatabasesWithEqualNgramSizesIsAccepted) {
  Config live_config;
  live_config.tables.push_back(MakeTable("db1", "articles", 2));
  live_config.tables.push_back(MakeTable("db2", "articles", 2));

  Config loaded_config;
  loaded_config.tables.push_back(MakeTable("db1", "articles", 2));
  loaded_config.tables.push_back(MakeTable("db2", "articles", 2));

  EXPECT_FALSE(FindDumpConfigMismatch(loaded_config, live_config).has_value());
}

TEST(DumpConfigValidatorTest, CrossBoundaryNgramsOfTheSameQualifiedTableIsCompared) {
  Config live_config;
  live_config.tables.push_back(MakeTable("db1", "articles", 2));
  live_config.tables.push_back(MakeTable("db2", "articles", 2));
  live_config.tables[1].cross_boundary_ngrams = false;

  Config loaded_config;
  loaded_config.tables.push_back(MakeTable("db2", "articles", 2));

  const auto mismatch = FindDumpConfigMismatch(loaded_config, live_config);
  ASSERT_TRUE(mismatch.has_value());
  EXPECT_NE(mismatch->find("cross_boundary_ngrams mismatch"), std::string::npos) << *mismatch;
}

TEST(DumpConfigValidatorTest, SingleDatabaseMatchStillCompares) {
  Config live_config;
  live_config.tables.push_back(MakeTable("shop", "articles", 2));

  Config matching_dump;
  matching_dump.tables.push_back(MakeTable("shop", "articles", 2));
  EXPECT_FALSE(FindDumpConfigMismatch(matching_dump, live_config).has_value());

  Config diverging_dump;
  diverging_dump.tables.push_back(MakeTable("shop", "articles", 3));
  const auto mismatch = FindDumpConfigMismatch(diverging_dump, live_config);
  ASSERT_TRUE(mismatch.has_value());
  EXPECT_NE(mismatch->find("ngram_size mismatch"), std::string::npos) << *mismatch;
}

TEST(DumpConfigValidatorTest, TableAbsentFromTheRunningConfigIsSkipped) {
  Config live_config;
  live_config.tables.push_back(MakeTable("db1", "articles", 2));

  Config loaded_config;
  loaded_config.tables.push_back(MakeTable("db2", "reviews", 3));

  EXPECT_FALSE(FindDumpConfigMismatch(loaded_config, live_config).has_value());
}

// ngram_size is compared first, so a case that varies both settings together
// would pass even if this comparison were deleted.
TEST(DumpConfigValidatorTest, KanjiNgramSizeIsComparedIndependentlyOfNgramSize) {
  Config live_config;
  live_config.tables.push_back(MakeTable("shop", "articles", 2, 2));

  Config loaded_config;
  loaded_config.tables.push_back(MakeTable("shop", "articles", 2, 3));

  const auto mismatch = FindDumpConfigMismatch(loaded_config, live_config);
  ASSERT_TRUE(mismatch.has_value());
  EXPECT_NE(mismatch->find("kanji_ngram_size mismatch"), std::string::npos) << *mismatch;
}

// The one behavior this check changed: a dump entry naming a database that is
// not configured is skipped, where matching on the bare name would have compared
// it against a same-named table in a different database.
TEST(DumpConfigValidatorTest, DumpTableInAnUnconfiguredDatabaseIsNotComparedAgainstTheSameBareName) {
  Config live_config;
  live_config.tables.push_back(MakeTable("db1", "articles", 2));

  Config loaded_config;
  loaded_config.tables.push_back(MakeTable("db2", "articles", 3));

  EXPECT_FALSE(FindDumpConfigMismatch(loaded_config, live_config).has_value())
      << "db2.articles is not configured, so it must not be compared against db1.articles";
}

TEST(DumpConfigValidatorTest, DumpEntryWithoutADatabaseFallsBackToTheBareName) {
  Config live_config;
  live_config.tables.push_back(MakeTable("shop", "articles", 2));

  Config matching_dump;
  matching_dump.tables.push_back(MakeTable("", "articles", 2));
  EXPECT_FALSE(FindDumpConfigMismatch(matching_dump, live_config).has_value());

  Config diverging_dump;
  diverging_dump.tables.push_back(MakeTable("", "articles", 3));
  const auto mismatch = FindDumpConfigMismatch(diverging_dump, live_config);
  ASSERT_TRUE(mismatch.has_value());
  EXPECT_NE(mismatch->find("ngram_size mismatch"), std::string::npos) << *mismatch;
}
