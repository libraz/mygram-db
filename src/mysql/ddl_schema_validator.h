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
};

struct ConfiguredTableSchema {
  std::vector<DDLColumnMetadata> columns;
};

/**
 * Captures and compares only the columns that MygramDB is configured to use.
 * This makes unrelated ADD COLUMN / ADD INDEX operations compatible while
 * treating any semantic change to PK, text, or filter columns as requiring a
 * rebuild.
 */
class DDLSchemaValidator {
 public:
  static mygram::utils::Expected<ConfiguredTableSchema, mygram::utils::Error> Capture(
      Connection& connection, const config::TableConfig& table_config);

  static mygram::utils::Expected<ConfiguredTableSchema, mygram::utils::Error> ValidateMetadata(
      const config::TableConfig& table_config, const std::vector<DDLColumnMetadata>& columns,
      bool primary_key_is_unique);

  static mygram::utils::Expected<void, mygram::utils::Error> Compare(const ConfiguredTableSchema& expected,
                                                                     const ConfiguredTableSchema& actual,
                                                                     const config::TableConfig& table_config);
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
