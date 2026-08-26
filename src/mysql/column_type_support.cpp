/**
 * @file column_type_support.cpp
 * @brief Declared-type lookup into the column type support table
 */

#include "mysql/column_type_support.h"

#ifdef USE_MYSQL

#include <algorithm>
#include <cctype>
#include <unordered_map>

namespace mygramdb::mysql {
namespace {

/// Leading word of a declared type, lowercased: `varchar(64)` -> `varchar`.
std::string DeclaredBaseType(std::string_view column_type) {
  const size_t end = column_type.find_first_of("( ");
  std::string base(column_type.substr(0, end));
  std::transform(base.begin(), base.end(), base.begin(),
                 [](unsigned char chr) { return static_cast<char>(std::tolower(chr)); });
  return base;
}

/**
 * @brief Declared type names, mapped to the code a row event uses for them.
 *
 * Synonyms are listed next to the name they stand for. A name absent from this
 * table is a type this build has never decoded, and is refused rather than
 * guessed at.
 */
const std::unordered_map<std::string, ColumnType>& DeclaredTypeCodes() {
  static const std::unordered_map<std::string, ColumnType> codes = {
      {"tinyint", ColumnType::TINY},
      {"bool", ColumnType::TINY},
      {"boolean", ColumnType::TINY},
      {"smallint", ColumnType::SHORT},
      {"mediumint", ColumnType::INT24},
      {"int", ColumnType::LONG},
      {"integer", ColumnType::LONG},
      {"bigint", ColumnType::LONGLONG},
      {"serial", ColumnType::LONGLONG},
      {"decimal", ColumnType::NEWDECIMAL},
      {"dec", ColumnType::NEWDECIMAL},
      {"numeric", ColumnType::NEWDECIMAL},
      {"fixed", ColumnType::NEWDECIMAL},
      {"float", ColumnType::FLOAT},
      {"double", ColumnType::DOUBLE},
      {"real", ColumnType::DOUBLE},
      {"bit", ColumnType::BIT},
      {"year", ColumnType::YEAR},
      {"date", ColumnType::DATE},
      {"datetime", ColumnType::DATETIME},
      {"timestamp", ColumnType::TIMESTAMP},
      {"time", ColumnType::TIME},
      {"char", ColumnType::STRING},
      {"character", ColumnType::STRING},
      {"nchar", ColumnType::STRING},
      {"binary", ColumnType::STRING},
      {"varchar", ColumnType::VARCHAR},
      {"nvarchar", ColumnType::VARCHAR},
      {"varbinary", ColumnType::VARCHAR},
      {"tinytext", ColumnType::TINY_BLOB},
      {"tinyblob", ColumnType::TINY_BLOB},
      {"text", ColumnType::BLOB},
      {"blob", ColumnType::BLOB},
      {"mediumtext", ColumnType::MEDIUM_BLOB},
      {"mediumblob", ColumnType::MEDIUM_BLOB},
      {"longtext", ColumnType::LONG_BLOB},
      {"longblob", ColumnType::LONG_BLOB},
      {"enum", ColumnType::ENUM},
      {"set", ColumnType::SET},
      {"json", ColumnType::JSON},
      {"vector", ColumnType::VECTOR},
      {"geometry", ColumnType::GEOMETRY},
      {"point", ColumnType::GEOMETRY},
      {"linestring", ColumnType::GEOMETRY},
      {"polygon", ColumnType::GEOMETRY},
      {"multipoint", ColumnType::GEOMETRY},
      {"multilinestring", ColumnType::GEOMETRY},
      {"multipolygon", ColumnType::GEOMETRY},
      {"geometrycollection", ColumnType::GEOMETRY},
      {"geomcollection", ColumnType::GEOMETRY},
  };
  return codes;
}

}  // namespace

std::optional<ColumnType> ColumnTypeFromDeclaredType(std::string_view column_type) {
  const auto& codes = DeclaredTypeCodes();
  const auto iterator = codes.find(DeclaredBaseType(column_type));
  if (iterator == codes.end()) {
    return std::nullopt;
  }
  return iterator->second;
}

std::string UnusableColumnTypeReason(std::string_view column_type) {
  const auto code = ColumnTypeFromDeclaredType(column_type);
  if (!code.has_value()) {
    return "type '" + std::string(column_type) + "' is not a type this build decodes from a binlog row image";
  }
  switch (DescribeColumnType(*code).acceptance) {
    case ColumnAcceptance::kAccepted:
      return {};
    case ColumnAcceptance::kRejectedPathsDisagree:
      return "type '" + std::string(column_type) +
             "' is rendered differently by the initial snapshot and the binlog row image, so the same row would be "
             "indexed as two documents";
    case ColumnAcceptance::kRejectedUndecodable:
      return "type '" + std::string(column_type) + "' cannot be decoded from a binlog row image";
  }
  return "type '" + std::string(column_type) + "' cannot be decoded from a binlog row image";
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
