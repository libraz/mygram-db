/**
 * @file ddl_classifier_test.cpp
 * @brief Classification of binlog QUERY_EVENT text into table DDL actions
 *
 * Every spelling MySQL and MariaDB accept for a table DDL has to reach the same
 * action, whatever comments surround it and however its names are written.
 * A statement that is missed falls through to the fail-open path for
 * unrelated DDL, so the GTID advances while the index keeps the old rows.
 */

#ifdef USE_MYSQL

#include "mysql/ddl_classifier.h"

#include <gtest/gtest.h>

#include <string>
#include <unordered_map>
#include <vector>

#include "binlog_event_builder.h"
#include "mysql/binlog_event_parser.h"

namespace {

using mygramdb::config::TableConfig;
using mygramdb::mysql::BinlogEventParser;
using mygramdb::mysql::BinlogEventType;
using mygramdb::mysql::ClassifyQueryEvent;
using mygramdb::mysql::DDLType;
using mygramdb::mysql::FindConfiguredTableDDL;
using mygramdb::mysql::LexQueryStatements;
using mygramdb::mysql::SqlToken;
using mygramdb::mysql::TableMetadataCache;

TableConfig MakeTableConfig(const std::string& name, const std::string& database = "") {
  TableConfig config;
  config.name = name;
  config.database = database;
  return config;
}

void ExpectDDL(const std::string& query, DDLType expected, const TableConfig& table_config,
               const std::string& event_database = "") {
  const auto match = FindConfiguredTableDDL(ClassifyQueryEvent(query), event_database, table_config);
  ASSERT_TRUE(match.has_value()) << "expected a DDL match for: " << query;
  EXPECT_EQ(*match, expected) << "wrong DDL type for: " << query;
}

void ExpectNoDDL(const std::string& query, const TableConfig& table_config, const std::string& event_database = "") {
  EXPECT_FALSE(FindConfiguredTableDDL(ClassifyQueryEvent(query), event_database, table_config).has_value())
      << "unexpected DDL match for: " << query;
}

/// Token texts of every statement, joined by spaces, one string per statement.
std::vector<std::string> ExecutedTokens(const std::string& query) {
  std::vector<std::string> statements;
  for (const auto& tokens : LexQueryStatements(query)) {
    std::string joined;
    for (const SqlToken& token : tokens) {
      if (!joined.empty()) {
        joined += ' ';
      }
      joined += token.text;
    }
    statements.push_back(joined);
  }
  return statements;
}

TEST(DdlClassifierTest, TruncateDropAndRenameAcceptEveryTableKeywordSpelling) {
  const TableConfig config = MakeTableConfig("articles");

  ExpectDDL("TRUNCATE articles", DDLType::kTruncate, config);
  ExpectDDL("TRUNCATE TABLE articles", DDLType::kTruncate, config);
  ExpectDDL("truncate `articles`", DDLType::kTruncate, config);
  ExpectDDL("DROP TABLES users, articles", DDLType::kDrop, config);
  ExpectDDL("DROP TEMPORARY TABLES IF EXISTS users, articles", DDLType::kDrop, config);
  ExpectDDL("RENAME TABLES articles TO articles_old, a TO b", DDLType::kRename, config);
  ExpectDDL("RENAME TABLE IF EXISTS a TO b, staging TO articles", DDLType::kRename, config);
  ExpectDDL("RENAME TABLE articles NOWAIT TO articles_old", DDLType::kRename, config);
  ExpectDDL("ALTER ONLINE IGNORE TABLE IF EXISTS articles ADD COLUMN c INT", DDLType::kAlter, config);

  ExpectNoDDL("TRUNCATE articles_backup", config);
  ExpectNoDDL("DROP TABLES users, logs", config);
}

TEST(DdlClassifierTest, ParsedQueryEventCarriesItsGtidSoTheDdlIsNotSkipped) {
  const TableConfig config = MakeTableConfig("articles", "app");
  TableMetadataCache cache;
  const std::unordered_map<std::string, mygramdb::server::TableContext*> table_contexts;

  const std::vector<std::pair<std::string, DDLType>> cases = {
      {"TRUNCATE articles", DDLType::kTruncate},
      {"TRUNCATE TABLE articles", DDLType::kTruncate},
      {"DROP TABLES users, articles", DDLType::kDrop},
      {"DROP TEMPORARY TABLES articles", DDLType::kDrop},
      {"RENAME TABLES articles TO old_articles", DDLType::kRename},
  };
  for (const auto& [query, expected] : cases) {
    const auto event = mygramdb::mysql::test::BinlogEventBuilder::BuildQueryEvent("app", query);
    const auto events = BinlogEventParser::ParseBinlogEvent(event.data(), event.size(), "uuid:41", cache,
                                                            table_contexts, &config, false);
    ASSERT_EQ(events.size(), 1U) << query;
    EXPECT_EQ(events[0].type, BinlogEventType::DDL) << query;
    EXPECT_EQ(events[0].ddl_type, expected) << query;
    EXPECT_EQ(events[0].gtid, "uuid:41") << query;
  }
}

TEST(DdlClassifierTest, DropSchemaMatchesTheConfiguredDatabaseCaseInsensitively) {
  const TableConfig config = MakeTableConfig("articles", "shop");

  ExpectDDL("DROP DATABASE Shop", DDLType::kDropDatabase, config);
  ExpectDDL("DrOp ScHeMa IF EXISTS `SHOP`", DDLType::kDropDatabase, config);
  ExpectDDL("DROP DATABASE shop; CREATE DATABASE x", DDLType::kDropDatabase, config);
  ExpectNoDDL("DROP DATABASE shop_archive", config);
  ExpectNoDDL("DROP DATABASE shop", MakeTableConfig("articles"));
}

TEST(DdlClassifierTest, UnquotedNonAsciiAndDollarNamesAreRead) {
  const TableConfig japanese = MakeTableConfig("記事");
  ExpectDDL("DROP TABLE 記事", DDLType::kDrop, japanese);
  ExpectDDL("TRUNCATE TABLE 記事", DDLType::kTruncate, japanese);
  ExpectDDL("RENAME TABLE 記事 TO 記事_old", DDLType::kRename, japanese);
  ExpectDDL("ALTER TABLE 記事 ADD COLUMN c INT", DDLType::kAlter, japanese);
  ExpectNoDDL("DROP TABLE 記事_old", japanese);

  const TableConfig dollar = MakeTableConfig("price$list");
  ExpectDDL("DROP TABLE price$list", DDLType::kDrop, dollar);
  ExpectDDL("TRUNCATE TABLE price$list", DDLType::kTruncate, dollar);
  ExpectDDL("RENAME TABLE price$list TO old", DDLType::kRename, dollar);
  ExpectDDL("ALTER TABLE price$list ADD COLUMN c INT", DDLType::kAlter, dollar);
  ExpectNoDDL("DROP TABLE price", dollar);

  ExpectDDL("DROP TABLE `we``ird`", DDLType::kDrop, MakeTableConfig("we`ird"));
}

TEST(DdlClassifierTest, LexerKeepsExactlyTheTokensMySqlExecutes) {
  // `#` comments, `-- ` comments, and `--` without a following space.
  EXPECT_EQ(ExecutedTokens("# maintenance\nTRUNCATE articles"), std::vector<std::string>{"TRUNCATE articles"});
  EXPECT_EQ(ExecutedTokens("-- note\nINSERT INTO t VALUES (1)"),
            std::vector<std::string>{"INSERT INTO t VALUES ( 1 )"});
  EXPECT_EQ(ExecutedTokens("SELECT 1--1"), std::vector<std::string>{"SELECT 1 - - 1"});
  EXPECT_EQ(ExecutedTokens("SELECT 1 --"), std::vector<std::string>{"SELECT 1"});
  // Executable comments run; ordinary block comments do not.
  EXPECT_EQ(ExecutedTokens("/*!40000 TRUNCATE TABLE articles */"), std::vector<std::string>{"TRUNCATE TABLE articles"});
  EXPECT_EQ(ExecutedTokens("/*M!100100 DROP TABLE articles */"), std::vector<std::string>{"DROP TABLE articles"});
  EXPECT_EQ(ExecutedTokens("/* DROP TABLE articles */ SELECT 1"), std::vector<std::string>{"SELECT 1"});
  // Comment markers and separators inside quoted spans are content.
  EXPECT_EQ(ExecutedTokens("RENAME TABLE `a--b` TO articles"),
            std::vector<std::string>{"RENAME TABLE a--b TO articles"});
  EXPECT_EQ(ExecutedTokens("ALTER TABLE t COMMENT 'x # y; -- z /* w'"),
            std::vector<std::string>{"ALTER TABLE t COMMENT x # y; -- z /* w"});
  EXPECT_EQ(ExecutedTokens("DROP TABLE a; DROP TABLE b;"), (std::vector<std::string>{"DROP TABLE a", "DROP TABLE b"}));
}

TEST(DdlClassifierTest, CommentFormsDoNotHideConfiguredTableDdl) {
  const TableConfig config = MakeTableConfig("articles");

  ExpectDDL("# rotate\nTRUNCATE articles", DDLType::kTruncate, config);
  ExpectDDL("-- rotate\nDROP TABLE articles", DDLType::kDrop, config);
  ExpectDDL("/*!40000 TRUNCATE TABLE articles */", DDLType::kTruncate, config);
  ExpectDDL("DROP TABLE IF EXISTS `articles` /* generated by server */", DDLType::kDrop, config);
  ExpectDDL("RENAME TABLE `a--b` TO articles", DDLType::kRename, config);
  ExpectNoDDL("ALTER TABLE t COMMENT 'TRUNCATE articles; DROP TABLE articles'", config);
}

TEST(DdlClassifierTest, DdlThatMovesRowsWithoutRowEventsIsRowsReplaced) {
  const TableConfig config = MakeTableConfig("articles");

  ExpectDDL("ALTER TABLE articles DROP PARTITION p0", DDLType::kRowsReplaced, config);
  ExpectDDL("ALTER TABLE articles TRUNCATE PARTITION p0, p1", DDLType::kRowsReplaced, config);
  ExpectDDL("ALTER TABLE articles EXCHANGE PARTITION p0 WITH TABLE staging", DDLType::kRowsReplaced, config);
  ExpectDDL("ALTER TABLE other EXCHANGE PARTITION p0 WITH TABLE articles", DDLType::kRowsReplaced, config);
  ExpectDDL("ALTER TABLE other CONVERT PARTITION p0 TO TABLE articles", DDLType::kRowsReplaced, config);
  ExpectDDL("ALTER TABLE other CONVERT TABLE articles TO PARTITION p1 VALUES LESS THAN (10)", DDLType::kRowsReplaced,
            config);
  ExpectDDL("ALTER TABLE articles DISCARD TABLESPACE", DDLType::kRowsReplaced, config);
  ExpectDDL("ALTER TABLE articles IMPORT PARTITION p0 TABLESPACE", DDLType::kRowsReplaced, config);
  ExpectDDL("CREATE OR REPLACE TABLE articles (id BIGINT PRIMARY KEY)", DDLType::kRowsReplaced, config);

  // Repartitioning and maintenance keep every row.
  ExpectDDL("ALTER TABLE articles REORGANIZE PARTITION p0 INTO (PARTITION p1 VALUES LESS THAN (5))", DDLType::kAlter,
            config);
  ExpectDDL("ALTER TABLE articles ADD COLUMN discard INT", DDLType::kAlter, config);
  ExpectDDL("ALTER TABLE articles DROP COLUMN `partition`", DDLType::kAlter, config);
  ExpectDDL("CREATE TABLE articles (id BIGINT PRIMARY KEY)", DDLType::kCreate, config);
}

TEST(DdlClassifierTest, UnreadableTableDdlIsMalformedNotIgnored) {
  for (const char* query : {"TRUNCATE TABLE", "DROP TABLE IF EXISTS", "RENAME TABLE", "CREATE TABLE"}) {
    const auto statements = ClassifyQueryEvent(query);
    ASSERT_EQ(statements.size(), 1U) << query;
    EXPECT_TRUE(statements[0].malformed) << query;
    EXPECT_EQ(statements[0].type, DDLType::kUnknown) << query;
    EXPECT_FALSE(BinlogEventParser::IsSafeIgnoredQuery(query)) << query;
  }
}

TEST(DdlClassifierTest, NameListCutShortIsMalformed) {
  for (const char* query :
       {"RENAME TABLE a TO", "RENAME TABLE a TO b,", "RENAME TABLE a", "RENAME TABLE a TO b c", "DROP TABLE x,",
        "DROP TABLE x, 'articles'", "DROP TABLE x y", "TRUNCATE TABLE x y", "DROP DATABASE shop extra",
        "ALTER TABLE staging RENAME TO", "ALTER TABLE staging EXCHANGE PARTITION p0 WITH TABLE"}) {
    const auto statements = ClassifyQueryEvent(query);
    ASSERT_EQ(statements.size(), 1U) << query;
    EXPECT_TRUE(statements[0].malformed) << query;
    EXPECT_EQ(statements[0].type, DDLType::kUnknown) << query;
    EXPECT_FALSE(BinlogEventParser::IsSafeIgnoredQuery(query)) << query;
  }
}

TEST(DdlClassifierTest, CompleteNameListWithTrailingClausesIsNotMalformed) {
  for (const char* query : {"DROP TABLE x, y RESTRICT", "DROP TABLE x CASCADE", "DROP TABLE x NOWAIT",
                            "DROP TABLE x WAIT 5 RESTRICT", "TRUNCATE TABLE x WAIT 5", "RENAME TABLE a TO b, c TO d"}) {
    const auto statements = ClassifyQueryEvent(query);
    ASSERT_EQ(statements.size(), 1U) << query;
    EXPECT_FALSE(statements[0].malformed) << query;
    EXPECT_NE(statements[0].type, DDLType::kUnknown) << query;
  }
}

// Under ANSI_QUOTES the server logs a double-quoted name, which names a table.
TEST(DdlClassifierTest, DoubleQuotedNameIsAnIdentifier) {
  const TableConfig config = MakeTableConfig("articles");
  ExpectDDL(R"(RENAME TABLE staging TO "articles")", DDLType::kRename, config);
  ExpectDDL(R"(DROP TABLE x, "articles")", DDLType::kDrop, config);
  ExpectDDL(R"(TRUNCATE TABLE "articles")", DDLType::kTruncate, config);
  ExpectDDL(R"(ALTER TABLE staging RENAME TO "articles")", DDLType::kRename, config);
  ExpectDDL(R"(DROP TABLE "app"."articles")", DDLType::kDrop, MakeTableConfig("articles", "app"));
  ExpectNoDDL(R"(DROP TABLE x, "articles_old")", config);
}

}  // namespace

#endif  // USE_MYSQL
