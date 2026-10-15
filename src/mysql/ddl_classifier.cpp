/**
 * @file ddl_classifier.cpp
 * @brief Classification of binlog QUERY_EVENT text into table DDL actions
 */

#include "mysql/ddl_classifier.h"

#ifdef USE_MYSQL

#include <cctype>

#include "utils/string_utils.h"

namespace mygramdb::mysql {

namespace {

/// MySQL accepts ASCII alphanumerics, `_`, `$` and every byte of a multi-byte character unquoted.
bool IsIdentifierByte(char chr) {
  const auto byte = static_cast<unsigned char>(chr);
  return std::isalnum(byte) != 0 || chr == '_' || chr == '$' || byte >= 0x80;
}

/// `--` opens a comment only when followed by whitespace, a control character, or the end of input.
bool StartsDashComment(std::string_view query, size_t pos) {
  if (pos + 1 >= query.size() || query[pos] != '-' || query[pos + 1] != '-') {
    return false;
  }
  if (pos + 2 >= query.size()) {
    return true;
  }
  const auto next = static_cast<unsigned char>(query[pos + 2]);
  return std::isspace(next) != 0 || std::iscntrl(next) != 0;
}

/**
 * @brief Read a quoted span starting at @p pos
 *
 * Backticks escape themselves by doubling; string literals also honour
 * backslash escapes. An unterminated span runs to the end of the input.
 */
size_t ReadQuoted(std::string_view query, size_t pos, std::string& content) {
  const char quote = query[pos++];
  while (pos < query.size()) {
    const char chr = query[pos];
    if (quote != '`' && chr == '\\' && pos + 1 < query.size()) {
      content.push_back(query[pos + 1]);
      pos += 2;
      continue;
    }
    if (chr == quote) {
      if (pos + 1 < query.size() && query[pos + 1] == quote) {
        content.push_back(quote);
        pos += 2;
        continue;
      }
      return pos + 1;
    }
    content.push_back(chr);
    ++pos;
  }
  return pos;
}

/**
 * @brief Cursor over one statement's tokens
 */
class StatementCursor {
 public:
  explicit StatementCursor(const std::vector<SqlToken>& tokens) : tokens_(tokens) {}

  [[nodiscard]] bool AtEnd() const { return pos_ >= tokens_.size(); }

  [[nodiscard]] bool IsKeyword(size_t ahead, std::string_view keyword) const {
    const size_t index = pos_ + ahead;
    return index < tokens_.size() && tokens_[index].kind == SqlToken::Kind::kWord &&
           mygram::utils::EqualsAsciiCaseInsensitive(tokens_[index].text, keyword);
  }

  bool Accept(std::string_view keyword) {
    if (!IsKeyword(0, keyword)) {
      return false;
    }
    ++pos_;
    return true;
  }

  bool AcceptPunct(char punct) {
    if (AtEnd() || tokens_[pos_].kind != SqlToken::Kind::kPunct || tokens_[pos_].text[0] != punct) {
      return false;
    }
    ++pos_;
    return true;
  }

  void AcceptIfExists() {
    if (IsKeyword(0, "IF") && IsKeyword(1, "EXISTS")) {
      pos_ += 2;
    }
  }

  void AcceptIfNotExists() {
    if (IsKeyword(0, "IF") && IsKeyword(1, "NOT") && IsKeyword(2, "EXISTS")) {
      pos_ += 3;
    }
  }

  /// MariaDB accepts a lock-wait clause after a table name in DROP, RENAME and TRUNCATE.
  void AcceptWaitClause() {
    if (Accept("NOWAIT")) {
      return;
    }
    if (Accept("WAIT")) {
      Skip(1);
    }
  }

  void Skip(size_t count) { pos_ += count; }

  /// A double-quoted span is read as a name: the server only logs one there under ANSI_QUOTES.
  std::optional<std::string> ReadIdentifier() {
    if (AtEnd()) {
      return std::nullopt;
    }
    const SqlToken& token = tokens_[pos_];
    if (token.kind != SqlToken::Kind::kWord && token.kind != SqlToken::Kind::kQuotedIdentifier &&
        !(token.kind == SqlToken::Kind::kString && token.double_quoted)) {
      return std::nullopt;
    }
    ++pos_;
    return token.text;
  }

  /// `[database.]table`
  std::optional<DdlTableName> ReadTableName() {
    auto first = ReadIdentifier();
    if (!first) {
      return std::nullopt;
    }
    if (!AcceptPunct('.')) {
      return DdlTableName{"", std::move(*first)};
    }
    auto second = ReadIdentifier();
    if (!second) {
      return std::nullopt;
    }
    return DdlTableName{std::move(*first), std::move(*second)};
  }

 private:
  const std::vector<SqlToken>& tokens_;
  size_t pos_ = 0;
};

/// Settle a statement whose grammar matched: without a readable name, or with a name list that
/// stopped short of its grammar, it is malformed.
DdlStatement Finish(DdlStatement statement, DDLType type, std::vector<DdlTableName> names, bool complete) {
  if (names.empty() || !complete) {
    statement.malformed = true;
    return statement;
  }
  statement.type = type;
  statement.names = std::move(names);
  return statement;
}

std::vector<DdlTableName> ReadOneName(StatementCursor& cursor) {
  std::vector<DdlTableName> names;
  if (auto name = cursor.ReadTableName()) {
    names.push_back(std::move(*name));
  }
  return names;
}

/// `name [, name] ...`; false when a name is missing where the list requires one.
bool ReadNameList(StatementCursor& cursor, std::vector<DdlTableName>& names) {
  do {
    auto name = cursor.ReadTableName();
    if (!name) {
      return false;
    }
    names.push_back(std::move(*name));
  } while (cursor.AcceptPunct(','));
  return true;
}

DdlStatement ClassifyAlterTable(DdlStatement statement, StatementCursor& cursor) {
  cursor.AcceptIfExists();
  auto table = cursor.ReadTableName();
  if (!table) {
    return Finish(std::move(statement), DDLType::kAlter, {}, false);
  }
  std::vector<DdlTableName> names{std::move(*table)};
  bool renamed = false;
  bool rows_replaced = false;
  bool complete = true;

  // Alter specifications are comma-separated and order-free, so scan them all.
  while (!cursor.AtEnd()) {
    if (cursor.Accept("RENAME")) {
      if (cursor.Accept("COLUMN") || cursor.Accept("INDEX") || cursor.Accept("KEY")) {
        continue;
      }
      if (!cursor.Accept("TO")) {
        cursor.Accept("AS");
      }
      auto target = cursor.ReadTableName();
      if (!target) {
        complete = false;
        break;
      }
      names.push_back(std::move(*target));
      renamed = true;
      continue;
    }
    if ((cursor.IsKeyword(0, "DROP") || cursor.IsKeyword(0, "TRUNCATE")) && cursor.IsKeyword(1, "PARTITION")) {
      rows_replaced = true;
      cursor.Skip(2);
      continue;
    }
    if ((cursor.IsKeyword(0, "DISCARD") || cursor.IsKeyword(0, "IMPORT")) &&
        (cursor.IsKeyword(1, "TABLESPACE") || cursor.IsKeyword(1, "PARTITION"))) {
      rows_replaced = true;
      cursor.Skip(2);
      continue;
    }
    // EXCHANGE PARTITION p WITH TABLE other; MariaDB CONVERT PARTITION p TO TABLE other
    // and CONVERT TABLE other TO PARTITION p. The other table loses or gains rows too.
    const bool exchange = cursor.IsKeyword(0, "EXCHANGE") && cursor.IsKeyword(1, "PARTITION");
    const bool convert_partition = cursor.IsKeyword(0, "CONVERT") && cursor.IsKeyword(1, "PARTITION");
    if (exchange || convert_partition) {
      rows_replaced = true;
      cursor.Skip(2);
      cursor.ReadIdentifier();
      const bool has_table = exchange ? cursor.Accept("WITH") : cursor.Accept("TO");
      auto other = has_table && cursor.Accept("TABLE") ? cursor.ReadTableName() : std::nullopt;
      if (!other) {
        complete = false;
        break;
      }
      names.push_back(std::move(*other));
      continue;
    }
    if (cursor.IsKeyword(0, "CONVERT") && cursor.IsKeyword(1, "TABLE")) {
      rows_replaced = true;
      cursor.Skip(2);
      auto other = cursor.ReadTableName();
      if (!other) {
        complete = false;
        break;
      }
      names.push_back(std::move(*other));
      continue;
    }
    cursor.Skip(1);
  }

  DDLType type = DDLType::kAlter;
  if (rows_replaced) {
    type = DDLType::kRowsReplaced;
  } else if (renamed) {
    type = DDLType::kRename;
  }
  return Finish(std::move(statement), type, std::move(names), complete);
}

DdlStatement ClassifyRenameTable(DdlStatement statement, StatementCursor& cursor) {
  cursor.AcceptIfExists();
  std::vector<DdlTableName> names;
  bool complete = false;
  do {
    complete = false;
    auto from = cursor.ReadTableName();
    if (!from) {
      break;
    }
    names.push_back(std::move(*from));
    cursor.AcceptWaitClause();
    if (!cursor.Accept("TO")) {
      break;
    }
    auto to = cursor.ReadTableName();
    if (!to) {
      break;
    }
    names.push_back(std::move(*to));
    complete = true;
  } while (cursor.AcceptPunct(','));
  return Finish(std::move(statement), DDLType::kRename, std::move(names), complete && cursor.AtEnd());
}

}  // namespace

DdlStatement ClassifyStatement(const std::vector<SqlToken>& tokens) {
  DdlStatement statement;
  if (!tokens.empty() && tokens[0].kind == SqlToken::Kind::kWord) {
    statement.verb = mygram::utils::ToUpper(tokens[0].text);
  }
  if (tokens.size() > 1 && tokens[1].kind == SqlToken::Kind::kWord) {
    statement.object = mygram::utils::ToUpper(tokens[1].text);
  }

  StatementCursor cursor(tokens);
  if (cursor.Accept("TRUNCATE")) {
    cursor.Accept("TABLE");
    auto names = ReadOneName(cursor);
    cursor.AcceptWaitClause();
    return Finish(std::move(statement), DDLType::kTruncate, std::move(names), cursor.AtEnd());
  }

  if (cursor.Accept("DROP")) {
    if (cursor.Accept("DATABASE") || cursor.Accept("SCHEMA")) {
      cursor.AcceptIfExists();
      std::vector<DdlTableName> names;
      if (auto database = cursor.ReadIdentifier()) {
        names.push_back(DdlTableName{std::move(*database), ""});
      }
      return Finish(std::move(statement), DDLType::kDropDatabase, std::move(names), cursor.AtEnd());
    }
    cursor.Accept("TEMPORARY");
    if (cursor.Accept("TABLE") || cursor.Accept("TABLES")) {
      cursor.AcceptIfExists();
      std::vector<DdlTableName> names;
      const bool listed = ReadNameList(cursor, names);
      cursor.AcceptWaitClause();
      if (!cursor.Accept("RESTRICT")) {
        cursor.Accept("CASCADE");
      }
      return Finish(std::move(statement), DDLType::kDrop, std::move(names), listed && cursor.AtEnd());
    }
    return statement;
  }

  if (cursor.Accept("CREATE")) {
    bool replace = false;
    if (cursor.Accept("OR")) {
      if (!cursor.Accept("REPLACE")) {
        return statement;
      }
      replace = true;
    }
    cursor.Accept("TEMPORARY");
    if (!cursor.Accept("TABLE")) {
      return statement;
    }
    cursor.AcceptIfNotExists();
    // CREATE OR REPLACE swaps out an existing table's rows without row events.
    return Finish(std::move(statement), replace ? DDLType::kRowsReplaced : DDLType::kCreate, ReadOneName(cursor), true);
  }

  if (cursor.Accept("ALTER")) {
    cursor.Accept("ONLINE");
    cursor.Accept("IGNORE");
    if (!cursor.Accept("TABLE")) {
      return statement;
    }
    return ClassifyAlterTable(std::move(statement), cursor);
  }

  if (cursor.Accept("RENAME")) {
    if (!cursor.Accept("TABLE") && !cursor.Accept("TABLES")) {
      return statement;
    }
    return ClassifyRenameTable(std::move(statement), cursor);
  }

  return statement;
}

std::vector<std::vector<SqlToken>> LexQueryStatements(std::string_view query) {
  std::vector<std::vector<SqlToken>> statements;
  std::vector<SqlToken> current;
  bool in_executable_comment = false;

  const auto end_statement = [&statements, &current]() {
    if (!current.empty()) {
      statements.push_back(std::move(current));
      current.clear();
    }
  };

  size_t pos = 0;
  while (pos < query.size()) {
    const char chr = query[pos];
    if (std::isspace(static_cast<unsigned char>(chr)) != 0) {
      ++pos;
      continue;
    }
    if (chr == '\'' || chr == '"' || chr == '`') {
      SqlToken token;
      token.kind = chr == '`' ? SqlToken::Kind::kQuotedIdentifier : SqlToken::Kind::kString;
      token.double_quoted = chr == '"';
      pos = ReadQuoted(query, pos, token.text);
      current.push_back(std::move(token));
      continue;
    }
    if (chr == '/' && pos + 1 < query.size() && query[pos + 1] == '*') {
      size_t body = pos + 2;
      size_t marker = 0;
      if (body < query.size() && query[body] == '!') {
        marker = 1;
      } else if (body + 1 < query.size() && query[body] == 'M' && query[body + 1] == '!') {
        marker = 2;
      }
      // The server executes an executable comment's body, so its tokens count.
      if (marker > 0 && !in_executable_comment) {
        body += marker;
        while (body < query.size() && std::isdigit(static_cast<unsigned char>(query[body])) != 0) {
          ++body;
        }
        in_executable_comment = true;
        pos = body;
        continue;
      }
      const size_t close = query.find("*/", pos + 2);
      pos = close == std::string_view::npos ? query.size() : close + 2;
      continue;
    }
    if (in_executable_comment && chr == '*' && pos + 1 < query.size() && query[pos + 1] == '/') {
      in_executable_comment = false;
      pos += 2;
      continue;
    }
    if (chr == '#' || StartsDashComment(query, pos)) {
      const size_t newline = query.find('\n', pos);
      pos = newline == std::string_view::npos ? query.size() : newline + 1;
      continue;
    }
    if (chr == ';') {
      end_statement();
      ++pos;
      continue;
    }
    if (IsIdentifierByte(chr)) {
      const size_t start = pos;
      while (pos < query.size() && IsIdentifierByte(query[pos])) {
        ++pos;
      }
      current.push_back(SqlToken{SqlToken::Kind::kWord, std::string(query.substr(start, pos - start))});
      continue;
    }
    current.push_back(SqlToken{SqlToken::Kind::kPunct, std::string(1, chr)});
    ++pos;
  }
  end_statement();
  return statements;
}

std::vector<DdlStatement> ClassifyQueryEvent(std::string_view query) {
  std::vector<DdlStatement> statements;
  for (const auto& tokens : LexQueryStatements(query)) {
    statements.push_back(ClassifyStatement(tokens));
  }
  return statements;
}

bool StartsWithStatementPrefix(const std::vector<SqlToken>& statement, const std::vector<SqlToken>& prefix) {
  if (prefix.empty() || prefix.size() > statement.size()) {
    return false;
  }
  for (size_t i = 0; i < prefix.size(); ++i) {
    const SqlToken& want = prefix[i];
    const SqlToken& have = statement[i];
    if (want.kind != have.kind || want.double_quoted != have.double_quoted) {
      return false;
    }
    const bool last = i + 1 == prefix.size();
    if (last ? have.text.size() < want.text.size() : have.text.size() != want.text.size()) {
      return false;
    }
    const std::string_view compared = std::string_view(have.text).substr(0, want.text.size());
    const bool equal = want.kind == SqlToken::Kind::kWord
                           ? mygram::utils::EqualsAsciiCaseInsensitive(compared, want.text)
                           : compared == want.text;
    if (!equal) {
      return false;
    }
  }
  return true;
}

bool NamesConfiguredTable(const DdlStatement& statement, std::string_view event_database,
                          const config::TableConfig& table_config) {
  for (const auto& name : statement.names) {
    if (statement.type == DDLType::kDropDatabase) {
      if (!table_config.database.empty() &&
          mygram::utils::EqualsAsciiCaseInsensitive(name.database, table_config.database)) {
        return true;
      }
      continue;
    }
    if (!mygram::utils::EqualsAsciiCaseInsensitive(name.table, table_config.name)) {
      continue;
    }
    const std::string_view database = name.database.empty() ? event_database : std::string_view(name.database);
    if (table_config.database.empty() || mygram::utils::EqualsAsciiCaseInsensitive(database, table_config.database)) {
      return true;
    }
  }
  return false;
}

std::optional<DDLType> FindConfiguredTableDDL(const std::vector<DdlStatement>& statements,
                                              std::string_view event_database,
                                              const config::TableConfig& table_config) {
  for (const auto& statement : statements) {
    if (statement.type != DDLType::kUnknown && NamesConfiguredTable(statement, event_database, table_config)) {
      return statement.type;
    }
  }
  return std::nullopt;
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
