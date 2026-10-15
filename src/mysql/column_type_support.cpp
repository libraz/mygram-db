/**
 * @file column_type_support.cpp
 * @brief Declared-type lookup into the column type support table
 */

#include "mysql/column_type_support.h"

#ifdef USE_MYSQL

#include <algorithm>
#include <unordered_map>

#include "utils/numeric_parse.h"
#include "utils/string_utils.h"

namespace mygramdb::mysql {
namespace {

/// Leading word of a declared type, lowercased: `varchar(64)` -> `varchar`.
std::string DeclaredBaseType(std::string_view column_type) {
  const size_t end = column_type.find_first_of("( ");
  std::string base(column_type.substr(0, end));
  std::transform(base.begin(), base.end(), base.begin(), mygram::utils::AsciiToLower);
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

/// The digits of a declared type's `(n)` suffix, e.g. 3 for `datetime(3) /* ... */`.
std::optional<int> DeclaredTypeArgument(std::string_view column_type) {
  const size_t open = column_type.find('(');
  const size_t close = column_type.find(')', open);
  if (open == std::string_view::npos || close == std::string_view::npos) {
    return std::nullopt;
  }
  return mygram::utils::ParseNumeric<int>(column_type.substr(open + 1, close - open - 1));
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
  if (DeclaredBaseType(column_type) == "year" && DeclaredTypeArgument(column_type) == 2) {
    return "type '" + std::string(column_type) +
           "' is read back as a two-digit year by the initial snapshot and decoded as a four-digit year by the binlog "
           "row image, so the same row would be indexed as two documents";
  }
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

std::string UnsizableColumnTypeReason(std::string_view column_type) {
  constexpr std::string_view kOldFormatMarker = "/* mariadb-5.3 */";
  const std::string base = DeclaredBaseType(column_type);
  if (base != "time" && base != "datetime" && base != "timestamp") {
    return {};
  }
  if (mygram::utils::ToLower(std::string(column_type)).find(kOldFormatMarker) == std::string::npos) {
    return {};
  }
  const auto precision = DeclaredTypeArgument(column_type);
  if (!precision.has_value() || *precision <= 0) {
    return {};
  }
  return "type '" + std::string(column_type) +
         "' is an old-format MariaDB temporal with fractional seconds, whose width the binlog TABLE_MAP event does not "
         "describe, so rows of the table cannot be sized. Rebuild the column in the current format with "
         "ALTER TABLE ... MODIFY";
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
