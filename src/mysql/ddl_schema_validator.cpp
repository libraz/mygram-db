/**
 * @file ddl_schema_validator.cpp
 * @brief Validation of configured table columns across binlog DDL events
 */

#include "mysql/ddl_schema_validator.h"

#ifdef USE_MYSQL

#include <mysql.h>

#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include "mysql/column_type_support.h"
#include "mysql/connection.h"
#include "utils/sql_utils.h"
#include "utils/string_utils.h"

namespace mygramdb::mysql {
namespace {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

std::string Lower(const std::string& value) {
  return mygram::utils::ToLower(value);
}

std::string BaseType(const std::string& column_type) {
  const std::string lower = Lower(column_type);
  const size_t end = lower.find_first_of("( ");
  return lower.substr(0, end);
}

bool IsUnsigned(const std::string& column_type) {
  return Lower(column_type).find("unsigned") != std::string::npos;
}

bool IsBinaryStringType(const std::string& column_type) {
  static const std::unordered_set<std::string> kBinaryTypes = {
      "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob",
  };
  return kBinaryTypes.find(BaseType(column_type)) != kBinaryTypes.end();
}

bool IsCharacterStringType(const std::string& column_type) {
  static const std::unordered_set<std::string> kCharacterTypes = {
      "char", "varchar", "tinytext", "text", "mediumtext", "longtext", "enum", "set",
  };
  return kCharacterTypes.find(BaseType(column_type)) != kCharacterTypes.end();
}

bool IsZerofill(const std::string& column_type) {
  return Lower(column_type).find("zerofill") != std::string::npos;
}

bool IsSupportedTextCollation(const std::string& collation) {
  const std::string lower = Lower(collation);
  return lower.rfind("utf8mb4_", 0) == 0 || lower.rfind("utf8mb3_", 0) == 0 || lower.rfind("utf8_", 0) == 0 ||
         lower.rfind("ascii_", 0) == 0;
}

Expected<void, Error> ValidateConfiguredColumnEncoding(const DDLColumnMetadata& column, const std::string& table_name) {
  const std::string context = table_name + "." + column.name;
  if (IsBinaryStringType(column.column_type)) {
    return MakeUnexpected(MakeError(
        ErrorCode::kMySQLInvalidSchema,
        "Configured column '" + column.name + "' in '" + table_name + "' uses binary type '" + column.column_type +
            "'. BINARY, VARBINARY, and BLOB columns are unsupported because the snapshot and binlog paths do not "
            "currently share a lossless representation",
        context));
  }
  if (IsCharacterStringType(column.column_type) && !IsSupportedTextCollation(column.collation)) {
    const std::string reported_collation = column.collation.empty() ? "<none>" : column.collation;
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLInvalidSchema,
                  "Configured text column '" + column.name + "' in '" + table_name + "' uses unsupported collation '" +
                      reported_collation +
                      "'. Only utf8mb4, utf8/utf8mb3, and ascii character sets are supported for binlog replication",
                  context));
  }
  // ZEROFILL is a display attribute the server applies when it renders the
  // column as text. The initial snapshot receives the padded digits and the
  // binlog row image carries the unpadded number, so the two paths would key
  // and index the same row differently.
  if (IsZerofill(column.column_type)) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidSchema,
                                    "Configured column '" + column.name + "' in '" + table_name + "' is declared '" +
                                        column.column_type +
                                        "'. ZEROFILL pads the value the initial snapshot reads and not the value the "
                                        "binlog row image carries, so the two would disagree",
                                    context));
  }
  const std::string unusable = UnusableColumnTypeReason(column.column_type);
  if (!unusable.empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidSchema,
                                    "Configured column '" + column.name + "' in '" + table_name + "' uses " + unusable,
                                    context));
  }
  return {};
}

bool IsFilterTypeCompatible(const std::string& configured_type, const std::string& column_type) {
  const std::string actual = BaseType(column_type);
  const bool actual_unsigned = IsUnsigned(column_type);

  auto integer_matches = [&](const char* name, bool expected_unsigned) {
    return actual == name && actual_unsigned == expected_unsigned;
  };
  if (configured_type == "tinyint")
    return integer_matches("tinyint", false);
  if (configured_type == "tinyint_unsigned")
    return integer_matches("tinyint", true);
  if (configured_type == "smallint")
    return integer_matches("smallint", false);
  if (configured_type == "smallint_unsigned")
    return integer_matches("smallint", true);
  if (configured_type == "mediumint")
    return integer_matches("mediumint", false);
  if (configured_type == "mediumint_unsigned")
    return integer_matches("mediumint", true);
  if (configured_type == "int")
    return (actual == "int" || actual == "integer") && !actual_unsigned;
  if (configured_type == "int_unsigned")
    return (actual == "int" || actual == "integer") && actual_unsigned;
  if (configured_type == "bigint")
    return integer_matches("bigint", false);
  if (configured_type == "bigint_unsigned")
    return integer_matches("bigint", true);
  if (configured_type == "double")
    return actual == "double" || actual == "real";
  if (configured_type == "boolean") {
    const std::string lower = Lower(column_type);
    return actual == "tinyint" && !actual_unsigned && lower.find("tinyint(1)") == 0;
  }
  if (configured_type == "string" || configured_type == "varchar" || configured_type == "text") {
    // Binary string types never reach here: ValidateConfiguredColumnEncoding refuses them first.
    static const std::unordered_set<std::string> kStringTypes = {
        "char", "varchar", "tinytext", "text", "mediumtext", "longtext",
    };
    return kStringTypes.find(actual) != kStringTypes.end();
  }
  if (configured_type == "datetime")
    return actual == "datetime";
  if (configured_type == "date")
    return actual == "date";
  if (configured_type == "timestamp")
    return actual == "timestamp";
  if (configured_type == "time")
    return actual == "time";
  return false;
}

std::string Field(MYSQL_ROW row, unsigned long* lengths, size_t index) {
  if (row[index] == nullptr)
    return {};
  return std::string(row[index], lengths[index]);
}

bool SameColumn(const DDLColumnMetadata& lhs, const DDLColumnMetadata& rhs) {
  return lhs.name == rhs.name && lhs.column_type == rhs.column_type && lhs.collation == rhs.collation &&
         lhs.nullable == rhs.nullable && lhs.generation_kind == rhs.generation_kind &&
         lhs.generation_expression == rhs.generation_expression;
}

/// Fill generation_kind/generation_expression from information_schema.COLUMNS. SHOW COLUMNS
/// reports neither, yet MODIFY ... AS (expr) STORED rewrites values without any row event.
Expected<void, Error> CaptureGeneratedColumns(Connection& connection, const config::TableConfig& table_config,
                                              std::vector<DDLColumnMetadata>& columns) {
  const std::string query =
      "SELECT COLUMN_NAME, GENERATION_EXPRESSION, EXTRA FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = " +
      mygramdb::utils::EncodeMySQLStringLiteral(table_config.database) +
      " AND TABLE_NAME = " + mygramdb::utils::EncodeMySQLStringLiteral(table_config.name);
  auto result = connection.Execute(query);
  if (!result) {
    return MakeUnexpected(result.error());
  }

  std::unordered_map<std::string, std::pair<std::string, std::string>> generated;
  MYSQL_ROW row = nullptr;
  while ((row = mysql_fetch_row(result->get())) != nullptr) {
    unsigned long* lengths = mysql_fetch_lengths(result->get());
    if (lengths == nullptr) {
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLQueryFailed,
                    "Failed to read schema field lengths for '" + config::QualifiedTableName(table_config) + "'"));
    }
    generated.emplace(
        Field(row, lengths, 0),
        std::make_pair(DDLSchemaValidator::GenerationKindFromExtra(Field(row, lengths, 2)), Field(row, lengths, 1)));
  }

  for (auto& column : columns) {
    auto it = generated.find(column.name);
    if (it == generated.end()) {
      // The column vanished between the two statements: a DDL is in flight.
      return MakeUnexpected(MakeError(ErrorCode::kMySQLQueryFailed, "Column '" + column.name + "' of '" +
                                                                        config::QualifiedTableName(table_config) +
                                                                        "' changed while its schema was being read"));
    }
    column.generation_kind = it->second.first;
    column.generation_expression = it->second.second;
  }
  return {};
}

/// Reject a child-side foreign key whose cascade InnoDB applies without a row event.
Expected<void, Error> ValidateNoCascadingForeignKeys(Connection& connection, const config::TableConfig& table_config) {
  const std::string table_name = config::QualifiedTableName(table_config);
  const std::string query =
      "SELECT CONSTRAINT_NAME, UPDATE_RULE, DELETE_RULE FROM information_schema.REFERENTIAL_CONSTRAINTS "
      "WHERE CONSTRAINT_SCHEMA = " +
      mygramdb::utils::EncodeMySQLStringLiteral(table_config.database) +
      " AND TABLE_NAME = " + mygramdb::utils::EncodeMySQLStringLiteral(table_config.name);
  auto result = connection.Execute(query);
  if (!result) {
    return MakeUnexpected(result.error());
  }

  std::string cascading;
  MYSQL_ROW row = nullptr;
  while ((row = mysql_fetch_row(result->get())) != nullptr) {
    unsigned long* lengths = mysql_fetch_lengths(result->get());
    if (lengths == nullptr) {
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLQueryFailed, "Failed to read foreign key field lengths for '" + table_name + "'"));
    }
    const std::string update_rule = Field(row, lengths, 1);
    const std::string delete_rule = Field(row, lengths, 2);
    if (DDLSchemaValidator::IsRowRewritingReferentialRule(update_rule) ||
        DDLSchemaValidator::IsRowRewritingReferentialRule(delete_rule)) {
      cascading += (cascading.empty() ? "" : ", ") + Field(row, lengths, 0) + " (ON UPDATE " + update_rule +
                   ", ON DELETE " + delete_rule + ")";
    }
  }
  if (cascading.empty()) {
    return {};
  }

  // With SQL-layer foreign keys (MySQL 9.x, innodb_native_foreign_keys=OFF) the
  // server writes a row event for every child row a cascade touches.
  auto native = connection.Execute("SHOW GLOBAL VARIABLES LIKE 'innodb_native_foreign_keys'");
  if (!native) {
    return MakeUnexpected(native.error());
  }
  MYSQL_ROW native_row = mysql_fetch_row(native->get());
  if (native_row != nullptr && native_row[1] != nullptr &&
      (mygram::utils::EqualsAsciiCaseInsensitive(native_row[1], "OFF") || std::string_view(native_row[1]) == "0")) {
    return {};
  }

  return MakeUnexpected(
      MakeError(ErrorCode::kMySQLInvalidSchema,
                "Table '" + table_name + "' is the child of foreign keys that cascade: " + cascading +
                    ". InnoDB applies these changes without writing row events, so the index would silently diverge. "
                    "Use ON UPDATE/ON DELETE NO ACTION or RESTRICT on a monitored table, or run MySQL 9.x with "
                    "innodb_native_foreign_keys=OFF",
                table_name));
}

}  // namespace

Expected<ConfiguredTableSchema, Error> DDLSchemaValidator::Capture(Connection& connection,
                                                                   const config::TableConfig& table_config) {
  auto quoted_table = mygramdb::utils::QuoteQualifiedSQLIdentifier(table_config.database, table_config.name);
  if (!quoted_table) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidSchema,
                                    "Invalid configured table identifier '" + config::QualifiedTableName(table_config) +
                                        "': " + quoted_table.error().message(),
                                    config::QualifiedTableName(table_config)));
  }
  const std::string query = "SHOW FULL COLUMNS FROM " + *quoted_table;
  auto result = connection.Execute(query);
  if (!result) {
    return MakeUnexpected(result.error());
  }

  std::vector<DDLColumnMetadata> columns;
  MYSQL_ROW row = nullptr;
  while ((row = mysql_fetch_row(result->get())) != nullptr) {
    unsigned long* lengths = mysql_fetch_lengths(result->get());
    if (lengths == nullptr) {
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLQueryFailed,
                    "Failed to read schema field lengths for '" + config::QualifiedTableName(table_config) + "'"));
    }
    columns.push_back({Field(row, lengths, 0), Field(row, lengths, 1), Field(row, lengths, 2),
                       Field(row, lengths, 3) == "YES", Field(row, lengths, 4), "", ""});
  }
  if (auto generated = CaptureGeneratedColumns(connection, table_config, columns); !generated) {
    return MakeUnexpected(generated.error());
  }

  auto unique = connection.ValidateUniqueColumn(table_config.database, table_config.name, table_config.primary_key);
  if (!unique) {
    return MakeUnexpected(unique.error());
  }
  if (auto foreign_keys = ValidateNoCascadingForeignKeys(connection, table_config); !foreign_keys) {
    return MakeUnexpected(foreign_keys.error());
  }
  return ValidateMetadata(table_config, columns, true);
}

Expected<ConfiguredTableSchema, Error> DDLSchemaValidator::ValidateMetadata(
    const config::TableConfig& table_config, const std::vector<DDLColumnMetadata>& columns,
    bool primary_key_is_unique) {
  const std::string table_name = config::QualifiedTableName(table_config);
  if (!primary_key_is_unique) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidSchema,
                                    "Configured primary key column '" + table_config.primary_key +
                                        "' is no longer a single-column PRIMARY/UNIQUE key in '" + table_name + "'",
                                    table_name));
  }

  std::unordered_map<std::string, const DDLColumnMetadata*> by_name;
  for (const auto& column : columns) {
    if (!by_name.emplace(column.name, &column).second) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidSchema,
                                      "Duplicate column metadata for '" + column.name + "' in '" + table_name + "'",
                                      table_name));
    }
  }

  // Every column is sized from the row event, configured or not.
  for (const auto& column : columns) {
    const std::string unsizable = UnsizableColumnTypeReason(column.column_type);
    if (!unsizable.empty()) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidSchema,
                                      "Column '" + column.name + "' in '" + table_name + "' uses " + unsizable,
                                      table_name + "." + column.name));
    }
  }

  std::vector<std::string> required_names;
  std::unordered_set<std::string> seen;
  auto require = [&](const std::string& name) {
    if (!name.empty() && seen.insert(name).second)
      required_names.push_back(name);
  };
  require(table_config.primary_key);
  if (!table_config.text_source.column.empty()) {
    require(table_config.text_source.column);
  } else {
    for (const auto& name : table_config.text_source.concat)
      require(name);
  }
  const auto filters = config::BuildUnifiedFilterConfigs(table_config);
  for (const auto& filter : filters)
    require(filter.name);

  ConfiguredTableSchema schema;
  schema.columns.reserve(required_names.size());
  for (const auto& name : required_names) {
    auto it = by_name.find(name);
    if (it == by_name.end()) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLColumnNotFound,
                                      "Configured column '" + name + "' is missing from '" + table_name + "'",
                                      table_name + "." + name));
    }
    auto encoding = ValidateConfiguredColumnEncoding(*it->second, table_name);
    if (!encoding) {
      return MakeUnexpected(encoding.error());
    }
    schema.columns.push_back(*it->second);
  }

  for (const auto& filter : filters) {
    const auto* actual = by_name.at(filter.name);
    if (!IsFilterTypeCompatible(filter.type, actual->column_type)) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidSchema,
                                      "Configured filter column '" + filter.name + "' expects type '" + filter.type +
                                          "' but MySQL reports '" + actual->column_type + "' in '" + table_name + "'",
                                      table_name + "." + filter.name));
    }
  }
  return schema;
}

bool DDLSchemaValidator::IsRowRewritingReferentialRule(const std::string& rule) {
  return !rule.empty() && !mygram::utils::EqualsAsciiCaseInsensitive(rule, "NO ACTION") &&
         !mygram::utils::EqualsAsciiCaseInsensitive(rule, "RESTRICT");
}

std::string DDLSchemaValidator::GenerationKindFromExtra(const std::string& extra) {
  const std::string lower = Lower(extra);
  if (lower.find("stored generated") != std::string::npos) {
    return "STORED";
  }
  if (lower.find("virtual generated") != std::string::npos) {
    return "VIRTUAL";
  }
  return {};
}

Expected<void, Error> DDLSchemaValidator::Compare(const ConfiguredTableSchema& expected,
                                                  const ConfiguredTableSchema& actual,
                                                  const config::TableConfig& table_config) {
  if (expected.columns.size() != actual.columns.size()) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLInvalidSchema,
                  "Configured schema fingerprint changed for '" + config::QualifiedTableName(table_config) + "'"));
  }
  for (size_t i = 0; i < expected.columns.size(); ++i) {
    if (!SameColumn(expected.columns[i], actual.columns[i])) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidSchema,
                                      "Configured column '" + expected.columns[i].name + "' changed after DDL in '" +
                                          config::QualifiedTableName(table_config) + "'",
                                      config::QualifiedTableName(table_config) + "." + expected.columns[i].name));
    }
  }
  return {};
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
