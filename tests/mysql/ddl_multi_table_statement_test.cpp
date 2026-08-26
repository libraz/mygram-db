/**
 * @file ddl_multi_table_statement_test.cpp
 * @brief DDL detection when a statement names several tables
 *
 * A single DDL statement may name many tables (DROP TABLE a, b, c;
 * RENAME TABLE a TO b, c TO d). Detection of a configured table must not
 * depend on its position in that list.
 */

#ifdef USE_MYSQL

#include <gtest/gtest.h>

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "binlog_event_builder.h"
#include "config/config.h"
#include "mysql/binlog_event_parser.h"
#include "mysql/binlog_reader.h"

namespace mygramdb::mysql {

// Defined in binlog_event_parser.cpp. The parser's public surface exposes only
// a boolean predicate, so the DDL type carried by a match is reached directly.
struct MatchedConfiguredDDL {
  DDLType ddl_type = DDLType::kUnknown;
};

std::optional<MatchedConfiguredDDL> FindTableAffectingConfiguredDDL(const std::string& query,
                                                                    const std::string& event_database,
                                                                    const config::TableConfig& table_config);

}  // namespace mygramdb::mysql

namespace {

using mygramdb::config::TableConfig;
using mygramdb::mysql::BinlogEventParser;
using mygramdb::mysql::BinlogEventType;
using mygramdb::mysql::DDLType;
using mygramdb::mysql::FindTableAffectingConfiguredDDL;
using mygramdb::mysql::MySQLBinlogEventType;
using mygramdb::mysql::TableMetadataCache;

TableConfig MakeTableConfig(const std::string& name, const std::string& database = "") {
  TableConfig config;
  config.name = name;
  config.database = database;
  return config;
}

void ExpectDDLMatch(const std::string& query, DDLType expected, const TableConfig& table_config,
                    const std::string& event_database = "") {
  const auto match = FindTableAffectingConfiguredDDL(query, event_database, table_config);
  ASSERT_TRUE(match.has_value()) << "expected a DDL match for: " << query;
  EXPECT_EQ(match->ddl_type, expected) << "wrong DDL type for: " << query;
}

void ExpectNoDDLMatch(const std::string& query, const TableConfig& table_config,
                      const std::string& event_database = "") {
  const auto match = FindTableAffectingConfiguredDDL(query, event_database, table_config);
  EXPECT_FALSE(match.has_value()) << "unexpected DDL match for: " << query;
}

std::vector<uint8_t> BuildQueryEvent(const std::string& database, const std::string& query) {
  auto event = mygramdb::mysql::test::BinlogEventBuilder::BuildHeader(MySQLBinlogEventType::QUERY_EVENT);
  mygramdb::mysql::test::BinlogEventBuilder::AppendLittleEndian32(event, 1);  // thread_id
  mygramdb::mysql::test::BinlogEventBuilder::AppendLittleEndian32(event, 0);  // query_exec_time
  event.push_back(static_cast<uint8_t>(database.size()));
  mygramdb::mysql::test::BinlogEventBuilder::AppendLittleEndian16(event, 0);  // error_code
  mygramdb::mysql::test::BinlogEventBuilder::AppendLittleEndian16(event, 0);  // status_vars_len
  event.insert(event.end(), database.begin(), database.end());
  event.push_back(0x00);
  event.insert(event.end(), query.begin(), query.end());
  mygramdb::mysql::test::BinlogEventBuilder::AppendLittleEndian32(event, 0);  // checksum placeholder
  mygramdb::mysql::test::BinlogEventBuilder::FixEventSizeWithChecksum(event);
  return event;
}

TEST(DDLMultiTableStatementTest, DropListMatchesConfiguredTableAtAnyPosition) {
  const TableConfig config = MakeTableConfig("articles");

  ExpectDDLMatch("DROP TABLE articles, users", DDLType::kDrop, config);
  ExpectDDLMatch("DROP TABLE users, articles", DDLType::kDrop, config);
  ExpectDDLMatch("DROP TABLE users, articles, logs", DDLType::kDrop, config);
  ExpectDDLMatch("DROP TABLE users,articles,logs", DDLType::kDrop, config);
  ExpectDDLMatch("DROP TABLE  users ,  articles ,  logs ", DDLType::kDrop, config);
}

TEST(DDLMultiTableStatementTest, DropListMatchesQuotedAndQualifiedNames) {
  const TableConfig unqualified = MakeTableConfig("articles");

  ExpectDDLMatch("DROP TABLE `users`, `articles`", DDLType::kDrop, unqualified);
  ExpectDDLMatch("drop table users, articles", DDLType::kDrop, unqualified);
  ExpectDDLMatch("DrOp TaBlE users, ArTiClEs", DDLType::kDrop, unqualified);
  ExpectDDLMatch("DROP TABLE IF EXISTS users, articles", DDLType::kDrop, unqualified);
  ExpectDDLMatch("DROP TABLE users, articles RESTRICT", DDLType::kDrop, unqualified);
  ExpectDDLMatch("DROP TABLE users, articles CASCADE", DDLType::kDrop, unqualified);
  ExpectDDLMatch("/* comment */ DROP TABLE users, articles", DDLType::kDrop, unqualified);
  ExpectDDLMatch("DROP TABLE users, articles;", DDLType::kDrop, unqualified);

  const TableConfig qualified = MakeTableConfig("articles", "app");

  ExpectDDLMatch("DROP TABLE IF EXISTS `app`.`a`,`app`.`articles`,`app`.`b`", DDLType::kDrop, qualified, "app");
  ExpectDDLMatch("DROP TABLE `app`.`a`, `app`.`articles`", DDLType::kDrop, qualified, "app");
  ExpectDDLMatch("DROP TABLE app.a, app.articles, app.b", DDLType::kDrop, qualified, "app");
  ExpectDDLMatch("DROP TABLE other.a, articles", DDLType::kDrop, qualified, "app");
  ExpectDDLMatch("DROP TEMPORARY TABLE users, articles", DDLType::kDrop, unqualified);
}

TEST(DDLMultiTableStatementTest, DropListIgnoresTablesOutsideTheConfiguration) {
  const TableConfig config = MakeTableConfig("articles");

  ExpectNoDDLMatch("DROP TABLE users, logs", config);
  ExpectNoDDLMatch("DROP TABLE users, articles_backup, old_articles", config);
  ExpectNoDDLMatch("DROP TABLE `users`, `articles_backup`", config);

  const TableConfig qualified = MakeTableConfig("articles", "app");

  ExpectNoDDLMatch("DROP TABLE `other`.`a`, `other`.`articles`", qualified, "app");
  ExpectNoDDLMatch("DROP TABLE a, articles", qualified, "other");
}

TEST(DDLMultiTableStatementTest, DropListMatchesAcrossStatementSeparators) {
  const TableConfig config = MakeTableConfig("articles");

  ExpectDDLMatch("DROP TABLE users; DROP TABLE logs, articles", DDLType::kDrop, config);
  ExpectDDLMatch("DROP TABLE logs, articles; DROP TABLE users", DDLType::kDrop, config);
}

TEST(DDLMultiTableStatementTest, RenameListMatchesConfiguredTableAtAnyPosition) {
  const TableConfig config = MakeTableConfig("articles");

  ExpectDDLMatch("RENAME TABLE articles TO articles_old, a TO b", DDLType::kRename, config);
  ExpectDDLMatch("RENAME TABLE a TO b, articles TO articles_old", DDLType::kRename, config);
  ExpectDDLMatch("RENAME TABLE a TO b, articles_old TO articles, c TO d", DDLType::kRename, config);
  ExpectDDLMatch("RENAME TABLE `a` TO `b`, `c` TO `articles`", DDLType::kRename, config);
  ExpectNoDDLMatch("RENAME TABLE a TO b, c TO d", config);
}

TEST(DDLMultiTableStatementTest, TruncateAndAlterStillClassifyTheirOwnType) {
  const TableConfig config = MakeTableConfig("articles");

  ExpectDDLMatch("TRUNCATE TABLE articles", DDLType::kTruncate, config);
  ExpectDDLMatch("ALTER TABLE articles ADD COLUMN status INT", DDLType::kAlter, config);
  ExpectDDLMatch("CREATE TABLE articles (id INT PRIMARY KEY)", DDLType::kCreate, config);
  ExpectNoDDLMatch("ALTER TABLE users ADD COLUMN status INT", config);
}

/**
 * @brief A configured-table DROP must be caught before the fail-open fallback.
 *
 * IsSafeIgnoredQuery deliberately accepts unfamiliar DDL so that unrelated
 * maintenance statements do not halt replication. That fail-open behaviour is
 * only sound because configured-table DDL is detected first, so a multi-table
 * DROP naming a configured table must produce a match here.
 */
TEST(DDLMultiTableStatementTest, ConfiguredDropIsDetectedBeforeSafeIgnoredFallback) {
  const TableConfig config = MakeTableConfig("articles");
  const std::string query = "DROP TABLE users, articles";

  ExpectDDLMatch(query, DDLType::kDrop, config);
  EXPECT_TRUE(BinlogEventParser::IsSafeIgnoredQuery(query))
      << "the fallback is fail-open by design; detection above it carries the contract";
}

TEST(DDLMultiTableStatementTest, ParsedQueryEventEmitsDropForConfiguredTableInList) {
  const TableConfig config = MakeTableConfig("articles", "testdb");
  const auto event_buffer = BuildQueryEvent("testdb", "DROP TABLE users, articles");

  TableMetadataCache cache;
  const std::unordered_map<std::string, mygramdb::server::TableContext*> table_contexts;
  const auto events = BinlogEventParser::ParseBinlogEvent(event_buffer.data(), event_buffer.size(), "gtid:1", cache,
                                                          table_contexts, &config, false);

  ASSERT_EQ(events.size(), 1U);
  EXPECT_EQ(events[0].type, BinlogEventType::DDL);
  EXPECT_EQ(events[0].ddl_type, DDLType::kDrop);
  EXPECT_EQ(events[0].table_name, "articles");
}

}  // namespace

#endif  // USE_MYSQL
