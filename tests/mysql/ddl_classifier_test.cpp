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
using mygramdb::mysql::QueryLexOptions;
using mygramdb::mysql::SqlToken;
using mygramdb::mysql::TableMetadataCache;

TableConfig MakeTableConfig(const std::string& name, const std::string& database = "") {
  TableConfig config;
  config.name = name;
  config.database = database;
  return config;
}

void ExpectDDL(const std::string& query, DDLType expected, const TableConfig& table_config,
               const std::string& event_database = "", bool names_case_insensitive = false) {
  const auto match =
      FindConfiguredTableDDL(ClassifyQueryEvent(query), event_database, table_config, names_case_insensitive);
  ASSERT_TRUE(match.has_value()) << "expected a DDL match for: " << query;
  EXPECT_EQ(*match, expected) << "wrong DDL type for: " << query;
}

void ExpectNoDDL(const std::string& query, const TableConfig& table_config, const std::string& event_database = "",
                 bool names_case_insensitive = false) {
  EXPECT_FALSE(FindConfiguredTableDDL(ClassifyQueryEvent(query), event_database, table_config, names_case_insensitive)
                   .has_value())
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

TEST(DdlClassifierTest, DropSchemaMatchesTheConfiguredDatabaseAsTheSourceFoldsCase) {
  const TableConfig config = MakeTableConfig("articles", "shop");

  ExpectDDL("DROP DATABASE Shop", DDLType::kDropDatabase, config, "", true);
  ExpectDDL("DrOp ScHeMa IF EXISTS `SHOP`", DDLType::kDropDatabase, config, "", true);
  ExpectNoDDL("DROP DATABASE Shop", config);
  ExpectDDL("DrOp ScHeMa IF EXISTS `shop`", DDLType::kDropDatabase, config);
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

TEST(DdlClassifierTest, TemporaryTableDdlNeverNamesABaseTable) {
  const TableConfig config = MakeTableConfig("articles");

  for (const char* query : {"DROP TEMPORARY TABLE articles", "DROP TEMPORARY TABLES IF EXISTS users, articles",
                            "CREATE TEMPORARY TABLE articles (id BIGINT)", "CREATE TEMPORARY TABLE articles SELECT 1",
                            "CREATE OR REPLACE TEMPORARY TABLE articles (id BIGINT)"}) {
    ExpectNoDDL(query, config);
    const auto statements = ClassifyQueryEvent(query);
    ASSERT_EQ(statements.size(), 1U) << query;
    EXPECT_FALSE(statements[0].malformed) << query;
    EXPECT_TRUE(BinlogEventParser::IsSafeIgnoredQuery(query)) << query;
  }
  // The base-table form of the same names still classifies.
  ExpectDDL("DROP TABLE articles", DDLType::kDrop, config);
  ExpectDDL("CREATE TABLE articles (id BIGINT)", DDLType::kCreate, config);
}

TEST(DdlClassifierTest, StoredProgramDefinitionIsOneStatementWhoseBodyNeverClassifies) {
  const TableConfig config = MakeTableConfig("articles");

  for (const char* query : {
           "CREATE PROCEDURE p() BEGIN SELECT 1; TRUNCATE TABLE articles; END",
           "CREATE DEFINER=`root`@`%` PROCEDURE p() BEGIN DROP TABLE articles; END",
           "CREATE OR REPLACE DEFINER=CURRENT_USER() FUNCTION f() RETURNS INT BEGIN TRUNCATE articles; RETURN 1; END",
           "CREATE AGGREGATE FUNCTION IF NOT EXISTS db.agg(x INT) RETURNS INT BEGIN TRUNCATE articles; RETURN x; END",
           "CREATE TRIGGER t AFTER INSERT ON a FOR EACH ROW BEGIN UPDATE articles SET n = 1; TRUNCATE articles; END",
           "CREATE EVENT e ON SCHEDULE EVERY 1 DAY DO BEGIN TRUNCATE articles; DELETE FROM articles; END",
           "ALTER DEFINER='a'@'b' EVENT e DO BEGIN TRUNCATE articles; END",
           "CREATE PACKAGE BODY pkg AS PROCEDURE p() IS BEGIN TRUNCATE TABLE articles; END; END",
       }) {
    SCOPED_TRACE(query);
    EXPECT_EQ(LexQueryStatements(query).size(), 1U);
    EXPECT_EQ(ClassifyQueryEvent(query).size(), 1U);
    ExpectNoDDL(query, config);
    EXPECT_TRUE(BinlogEventParser::IsSafeIgnoredQuery(query));
  }
}

TEST(DdlClassifierTest, StatementsBeforeAStoredProgramDefinitionStillClassify) {
  const TableConfig config = MakeTableConfig("articles");

  ExpectDDL("DROP TABLE articles; CREATE PROCEDURE p() BEGIN SELECT 1; END", DDLType::kDrop, config);
  EXPECT_EQ(ClassifyQueryEvent("DROP TABLE x; CREATE PROCEDURE p() BEGIN SELECT 1; SELECT 2; END").size(), 2U);
  EXPECT_FALSE(BinlogEventParser::IsSafeIgnoredQuery("DELETE FROM articles; CREATE PROCEDURE p() BEGIN SELECT 1; END"));
}

TEST(DdlClassifierTest, StoredProgramDefinitionIsTheWholeEvent) {
  const TableConfig config = MakeTableConfig("articles");

  // Constructs that look like block ends or openers inside a body never end the definition.
  for (const char* query : {
           "CREATE PROCEDURE p() BEGIN IF NOT EXISTS (SELECT 1) THEN SELECT 1; END IF; TRUNCATE TABLE articles; END",
           "CREATE PROCEDURE p() BEGIN SELECT start, end FROM r; TRUNCATE TABLE articles; END",
           "CREATE PROCEDURE p() BEGIN SET @end=1; TRUNCATE TABLE articles; END",
           "CREATE PROCEDURE p() BEGIN FOR i IN 1..3 DO SELECT 1; END FOR; TRUNCATE TABLE articles; END",
       }) {
    SCOPED_TRACE(query);
    EXPECT_EQ(ClassifyQueryEvent(query).size(), 1U);
    ExpectNoDDL(query, config);
    EXPECT_TRUE(BinlogEventParser::IsSafeIgnoredQuery(query));
  }

  // A header that names a monitored table is still only a definition.
  ExpectNoDDL("CREATE TRIGGER t AFTER INSERT ON articles FOR EACH ROW SET @x=1", config);
  EXPECT_TRUE(BinlogEventParser::IsSafeIgnoredQuery("CREATE TRIGGER t AFTER INSERT ON articles FOR EACH ROW SET @x=1"));

  // Documented limit: what follows the body inside the same event is part of the definition.
  EXPECT_TRUE(BinlogEventParser::IsSafeIgnoredQuery("CREATE PROCEDURE p() BEGIN SELECT 1; END; DELETE FROM articles"));
  ExpectNoDDL("CREATE PROCEDURE p() BEGIN SELECT 1; END; TRUNCATE TABLE articles", config);

  // Statements before the definition classify normally.
  ExpectDDL("TRUNCATE TABLE articles; CREATE PROCEDURE p() BEGIN SELECT 1; END", DDLType::kTruncate, config);
}

TEST(DdlClassifierTest, DefinitionsWithoutABodyDoNotSwallowTheStatementsAfterThem) {
  const TableConfig config = MakeTableConfig("articles");

  // A loadable function and ALTER PROCEDURE have no body.
  ExpectDDL("CREATE FUNCTION f RETURNS INT SONAME 'f.so'; TRUNCATE TABLE articles", DDLType::kTruncate, config);
  ExpectDDL("CREATE AGGREGATE FUNCTION f RETURNS REAL SONAME 'f.so'; DROP TABLE articles", DDLType::kDrop, config);
  ExpectDDL("ALTER PROCEDURE p COMMENT 'x'; TRUNCATE TABLE articles", DDLType::kTruncate, config);
  ExpectDDL("CREATE TABLE event (id INT); TRUNCATE TABLE articles", DDLType::kTruncate, config);
}

TEST(DdlClassifierTest, AnsiQuotesMakesADoubleQuotedSpanAnIdentifier) {
  const TableConfig config = MakeTableConfig("articles");

  const auto ansi = LexQueryStatements(R"(TRUNCATE TABLE "articles")", QueryLexOptions{true, false});
  ASSERT_EQ(ansi.size(), 1U);
  ASSERT_EQ(ansi[0].size(), 3U);
  EXPECT_EQ(ansi[0][2].kind, SqlToken::Kind::kQuotedIdentifier);
  EXPECT_FALSE(ansi[0][2].double_quoted);

  const auto plain = LexQueryStatements(R"(TRUNCATE TABLE "articles")", QueryLexOptions{false, false});
  ASSERT_EQ(plain[0].size(), 3U);
  EXPECT_EQ(plain[0][2].kind, SqlToken::Kind::kString);
  EXPECT_TRUE(plain[0][2].double_quoted);

  // A doubled quote is one quote character; a backslash is not an escape in an identifier.
  EXPECT_EQ(LexQueryStatements(R"(DROP TABLE "a""b")", QueryLexOptions{true, false})[0][2].text, "a\"b");
  const std::string backslash = R"(DROP TABLE "a\", articles)";
  EXPECT_TRUE(
      FindConfiguredTableDDL(ClassifyQueryEvent(backslash, QueryLexOptions{true, false}), "", config).has_value());
  EXPECT_FALSE(
      FindConfiguredTableDDL(ClassifyQueryEvent(backslash, QueryLexOptions{false, false}), "", config).has_value());

  // A name left half-read stays malformed in either mode.
  for (const bool ansi_quotes : {false, true}) {
    const auto statements = ClassifyQueryEvent(R"(RENAME TABLE "a" TO)", QueryLexOptions{ansi_quotes, false});
    ASSERT_EQ(statements.size(), 1U);
    EXPECT_TRUE(statements[0].malformed);
  }
  EXPECT_FALSE(BinlogEventParser::IsSafeIgnoredQuery(R"(RENAME TABLE "a" TO)", {}, QueryLexOptions{true, false}));
  EXPECT_TRUE(
      BinlogEventParser::IsTableAffectingDDL(R"(TRUNCATE TABLE "articles")", "articles", QueryLexOptions{true, false}));
}

TEST(DdlClassifierTest, NoBackslashEscapesMakesABackslashOrdinaryInAStringLiteral) {
  const TableConfig config = MakeTableConfig("articles");

  // Under NO_BACKSLASH_ESCAPES the literal 'C:\' ends at its second quote, so the
  // DROP that follows it is a statement of its own; with the escape on, the
  // literal swallows the rest of the query.
  const std::string query = R"(SET @path = 'C:\'; DROP TABLE articles; SELECT '')";
  const auto plain = LexQueryStatements(query, QueryLexOptions{false, false});
  EXPECT_EQ(plain.size(), 1U);
  EXPECT_FALSE(FindConfiguredTableDDL(ClassifyQueryEvent(query, QueryLexOptions{false, false}), "", config));
  const auto literal_backslash = LexQueryStatements(query, QueryLexOptions{false, true});
  EXPECT_EQ(literal_backslash.size(), 3U);
  EXPECT_EQ(FindConfiguredTableDDL(ClassifyQueryEvent(query, QueryLexOptions{false, true}), "", config),
            DDLType::kDrop);

  // Identifiers never honor a backslash, so the option changes nothing for them.
  EXPECT_EQ(LexQueryStatements(R"(DROP TABLE `a\`)", QueryLexOptions{false, true})[0][2].text, "a\\");
  EXPECT_EQ(LexQueryStatements(R"(DROP TABLE `a\`)", QueryLexOptions{false, false})[0][2].text, "a\\");
  EXPECT_EQ(LexQueryStatements(R"(SELECT 'a\'b')", QueryLexOptions{false, false})[0][1].text, "a'b");
}

TEST(DdlClassifierTest, TableNamesMatchByteExactlyUnlessTheSourceFoldsCase) {
  const TableConfig config = MakeTableConfig("Articles");

  const auto upper = ClassifyQueryEvent("TRUNCATE TABLE articles");
  EXPECT_FALSE(FindConfiguredTableDDL(upper, "", config, false));
  EXPECT_EQ(FindConfiguredTableDDL(upper, "", config, true), DDLType::kTruncate);
  EXPECT_EQ(FindConfiguredTableDDL(ClassifyQueryEvent("TRUNCATE TABLE Articles"), "", config, false),
            DDLType::kTruncate);

  TableConfig with_database = MakeTableConfig("Articles");
  with_database.database = "AppDb";
  const auto statements = ClassifyQueryEvent("TRUNCATE TABLE articles");
  EXPECT_FALSE(FindConfiguredTableDDL(statements, "appdb", with_database, false));
  EXPECT_EQ(FindConfiguredTableDDL(statements, "appdb", with_database, true), DDLType::kTruncate);
  EXPECT_EQ(FindConfiguredTableDDL(ClassifyQueryEvent("DROP DATABASE appdb"), "", with_database, true),
            DDLType::kDropDatabase);
  EXPECT_FALSE(FindConfiguredTableDDL(ClassifyQueryEvent("DROP DATABASE appdb"), "", with_database, false));
}

TEST(DdlClassifierTest, CreateTableStartTransactionOpensATransaction) {
  const auto opens = [](const std::string& query, bool ansi_quotes = false) {
    const auto statements = ClassifyQueryEvent(query, QueryLexOptions{ansi_quotes, false});
    return !statements.empty() && statements.front().starts_transaction;
  };

  EXPECT_TRUE(opens("CREATE TABLE `articles` (\n  `id` int NOT NULL\n) START TRANSACTION ENGINE=InnoDB"));
  EXPECT_TRUE(opens(R"(CREATE TABLE "articles" ("id" int) START TRANSACTION)", true));
  EXPECT_TRUE(opens("create table articles (id int) start  transaction"));

  EXPECT_FALSE(opens("CREATE TABLE `articles` (`id` int) ENGINE=InnoDB"));
  EXPECT_FALSE(opens("CREATE TABLE t (`start` int DEFAULT 1, c VARCHAR(5) COMMENT 'START TRANSACTION')"));
  EXPECT_FALSE(opens("CREATE TABLE t (id int, START TRANSACTION)"));
  EXPECT_FALSE(opens("CREATE TABLE t (id int); START TRANSACTION"));
  EXPECT_FALSE(opens("START TRANSACTION"));

  // It is still the CREATE of the named table.
  ExpectDDL("CREATE TABLE `articles` (`id` int) START TRANSACTION ENGINE=InnoDB", DDLType::kCreate,
            MakeTableConfig("articles"));
}

}  // namespace

#endif  // USE_MYSQL
