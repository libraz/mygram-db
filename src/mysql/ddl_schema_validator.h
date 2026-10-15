/**
 * @file ddl_schema_validator.h
 * @brief Validation of configured table columns across binlog DDL events
 */

#pragma once

#ifdef USE_MYSQL

#include <string>
#include <vector>

#include "config/config.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql {

class Connection;

struct DDLColumnMetadata {
  std::string name;
  std::string column_type;
  std::string collation;
  bool nullable = false;
  std::string key;
  std::string generation_kind;        ///< "STORED", "VIRTUAL", or empty for an ordinary column
  std::string generation_expression;  ///< information_schema GENERATION_EXPRESSION, empty if not generated
};

struct ConfiguredTableSchema {
  std::vector<DDLColumnMetadata> columns;
};

/**
 * Captures and compares only the columns that MygramDB is configured to use.
 * This makes unrelated ADD COLUMN / ADD INDEX operations compatible while
 * treating any semantic change to PK, text, or filter columns as requiring a
 * rebuild.
 *
 * Capture also refuses a table whose child-side foreign keys cascade, because
 * InnoDB applies those changes without writing row events.
 */
class DDLSchemaValidator {
 public:
  static mygram::utils::Expected<ConfiguredTableSchema, mygram::utils::Error> Capture(
      Connection& connection, const config::TableConfig& table_config);

  static mygram::utils::Expected<ConfiguredTableSchema, mygram::utils::Error> ValidateMetadata(
      const config::TableConfig& table_config, const std::vector<DDLColumnMetadata>& columns,
      bool primary_key_is_unique);

  /**
   * @brief Whether a REFERENTIAL_CONSTRAINTS UPDATE_RULE/DELETE_RULE rewrites child rows
   *
   * Anything but NO ACTION and RESTRICT does (CASCADE, SET NULL, SET DEFAULT).
   */
  static bool IsRowRewritingReferentialRule(const std::string& rule);

  /**
   * @brief Generation kind reported by information_schema.COLUMNS.EXTRA
   * @return "STORED", "VIRTUAL", or empty for an ordinary column
   */
  static std::string GenerationKindFromExtra(const std::string& extra);

  static mygram::utils::Expected<void, mygram::utils::Error> Compare(const ConfiguredTableSchema& expected,
                                                                     const ConfiguredTableSchema& actual,
                                                                     const config::TableConfig& table_config);
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
