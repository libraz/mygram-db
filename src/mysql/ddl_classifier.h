/**
 * @file ddl_classifier.h
 * @brief Classification of binlog QUERY_EVENT text into table DDL actions
 *
 * Every decision the replication path makes about a statement's effect on a
 * table reads it from ClassifyQueryEvent. The query is lexed once with MySQL's
 * comment and quoting rules, split into statements outside quoted spans, and
 * each statement is classified into a DDLType plus every table or schema it
 * names. A new statement spelling is therefore added here and nowhere else.
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "config/config.h"

namespace mygramdb::mysql {

/**
 * @brief What a DDL statement does to the tables it names
 *
 * The set is closed: consumers switch over it without a default label so a new
 * action cannot be ignored silently.
 */
enum class DDLType : uint8_t {
  kUnknown = 0,   ///< Not a table DDL statement
  kTruncate,      ///< TRUNCATE [TABLE] t
  kCreate,        ///< CREATE [TEMPORARY] TABLE t
  kAlter,         ///< ALTER TABLE t that keeps every row in place
  kDrop,          ///< DROP [TEMPORARY] TABLE|TABLES t [, ...]
  kRename,        ///< RENAME TABLE|TABLES a TO b [, ...], or ALTER TABLE a RENAME b
  kDropDatabase,  ///< DROP DATABASE|SCHEMA db
  kRowsReplaced,  ///< Removes, replaces or moves rows without row events (partition and tablespace
                  ///< maintenance, CREATE OR REPLACE TABLE)
};

/**
 * @brief One lexical token of a statement, comments already removed
 */
struct SqlToken {
  enum class Kind : uint8_t {
    kWord,              ///< Unquoted keyword or identifier
    kQuotedIdentifier,  ///< Backtick-quoted identifier, unescaped
    kString,            ///< Single- or double-quoted literal; text is the unescaped content
    kPunct,             ///< Any other single character
  };
  Kind kind = Kind::kPunct;
  std::string text;
  bool double_quoted = false;  ///< kString opened with `"`, an identifier under ANSI_QUOTES
};

/**
 * @brief A table (or, for kDropDatabase, a schema) a statement names
 */
struct DdlTableName {
  std::string database;  ///< Empty when the reference is unqualified
  std::string table;     ///< Empty for a schema-level statement
};

/**
 * @brief Classification of one statement
 */
struct DdlStatement {
  DDLType type = DDLType::kUnknown;
  std::vector<DdlTableName> names;  ///< Every table the statement names, both sides of a rename or exchange
  std::string verb;                 ///< First token, ASCII-uppercased, when it is a word
  std::string object;               ///< Second token, ASCII-uppercased, when it is a word
  bool malformed = false;           ///< Starts like a table DDL but its table names could not all be read
};

/**
 * @brief Lex a query into statements with MySQL's comment rules
 *
 * Removes `#` and `-- ` line comments and block comments, keeps the contents of
 * executable comments (a block comment whose body starts with `!` or `M!`),
 * never looks inside quoted spans, and splits statements on `;` outside them.
 *
 * @param query Query text as logged in the binlog
 * @return Non-empty statements in order
 */
std::vector<std::vector<SqlToken>> LexQueryStatements(std::string_view query);

/**
 * @brief Classify one lexed statement
 * @param tokens One entry of LexQueryStatements
 */
DdlStatement ClassifyStatement(const std::vector<SqlToken>& tokens);

/**
 * @brief Whether a lexed statement begins with the tokens of @p prefix
 *
 * Words compare ASCII case-insensitively. The last prefix token may stop
 * inside the statement's token, as a raw text prefix would.
 *
 * @param statement One entry of LexQueryStatements
 * @param prefix A single lexed statement prefix
 */
bool StartsWithStatementPrefix(const std::vector<SqlToken>& statement, const std::vector<SqlToken>& prefix);

/**
 * @brief Classify every statement of a QUERY_EVENT
 * @param query Query text as logged in the binlog
 * @return One entry per non-empty statement, in order
 */
std::vector<DdlStatement> ClassifyQueryEvent(std::string_view query);

/**
 * @brief Whether a classified statement names the configured table
 *
 * Names compare ASCII case-insensitively. An unqualified name belongs to the
 * event's default database; a configuration without a database matches the
 * table in any database, but never a schema-level statement.
 */
bool NamesConfiguredTable(const DdlStatement& statement, std::string_view event_database,
                          const config::TableConfig& table_config);

/**
 * @brief DDL type of the first statement that names the configured table
 * @return std::nullopt when no statement names it
 */
std::optional<DDLType> FindConfiguredTableDDL(const std::vector<DdlStatement>& statements,
                                              std::string_view event_database, const config::TableConfig& table_config);

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
