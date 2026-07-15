/**
 * @file ddl_schema_validator.cpp
 * @brief Validation of configured table columns across binlog DDL events
 */

#include "mysql/ddl_schema_validator.h"

#ifdef USE_MYSQL

#include <mysql.h>

#include <algorithm>
#include <cctype>
#include <unordered_map>
#include <unordered_set>

#include "mysql/connection.h"

namespace mygramdb::mysql {
namespace {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

std::string Lower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char chr) { return static_cast<char>(std::tolower(chr)); });
  return value;
}

std::string BaseType(const std::string& column_type) {
  const std::string lower = Lower(column_type);
  const size_t end = lower.find_first_of("( ");
  return lower.substr(0, end);
}

bool IsUnsigned(const std::string& column_type) {
  return Lower(column_type).find("unsigned") != std::string::npos;
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
  if (configured_type == "float")
    return actual == "float";
  if (configured_type == "double")
    return actual == "double" || actual == "real";
  if (configured_type == "boolean") {
    const std::string lower = Lower(column_type);
    return actual == "tinyint" && !actual_unsigned && lower.find("tinyint(1)") == 0;
  }
  if (configured_type == "string" || configured_type == "varchar" || configured_type == "text") {
    static const std::unordered_set<std::string> kStringTypes = {
        "char",   "varchar",   "tinytext", "text", "mediumtext", "longtext",
        "binary", "varbinary", "tinyblob", "blob", "mediumblob", "longblob",
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

std::string EscapeIdentifier(const std::string& identifier) {
  std::string escaped;
  escaped.reserve(identifier.size());
  for (char chr : identifier) {
    escaped += chr;
    if (chr == '`')
      escaped += '`';
  }
  return escaped;
}

std::string Field(MYSQL_ROW row, unsigned long* lengths, size_t index) {
  if (row[index] == nullptr)
    return {};
  return std::string(row[index], lengths[index]);
}

bool SameColumn(const DDLColumnMetadata& lhs, const DDLColumnMetadata& rhs) {
  return lhs.name == rhs.name && lhs.column_type == rhs.column_type && lhs.collation == rhs.collation &&
         lhs.nullable == rhs.nullable;
}

}  // namespace

Expected<ConfiguredTableSchema, Error> DDLSchemaValidator::Capture(Connection& connection,
                                                                   const config::TableConfig& table_config) {
  const std::string query = "SHOW FULL COLUMNS FROM `" + EscapeIdentifier(table_config.database) + "`.`" +
                            EscapeIdentifier(table_config.name) + "`";
  auto result = connection.Execute(query);
  if (!result) {
    auto reconnect = connection.Reconnect(true /* silent */);
    if (reconnect)
      result = connection.Execute(query);
  }
  if (!result) {
    return MakeUnexpected(MakeError(
        ErrorCode::kMySQLQueryFailed,
        "Failed to read schema for '" + config::QualifiedTableName(table_config) + "': " + result.error().message(),
        config::QualifiedTableName(table_config)));
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
                       Field(row, lengths, 3) == "YES", Field(row, lengths, 4)});
  }

  auto unique = connection.ValidateUniqueColumn(table_config.database, table_config.name, table_config.primary_key);
  if (!unique) {
    return MakeUnexpected(unique.error());
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
