/**
 * @file table_name_match.h
 * @brief Database and table name equality under the source's lower_case_table_names
 *
 * With lower_case_table_names != 0 the source folds names to lowercase in
 * TABLE_MAP events while a configuration may spell them otherwise. Every place
 * that matches a name from the binlog against a configured one goes through
 * here, so row events and DDL statements always agree.
 */

#pragma once

#ifdef USE_MYSQL

#include <algorithm>
#include <string>
#include <string_view>

#include "config/config.h"
#include "utils/string_utils.h"

namespace mygramdb::mysql {

/// Byte-exact when the source is case sensitive, ASCII case-insensitive otherwise.
inline bool TableNamesEqual(std::string_view lhs, std::string_view rhs, bool case_insensitive) noexcept {
  return case_insensitive ? mygram::utils::EqualsAsciiCaseInsensitive(lhs, rhs) : lhs == rhs;
}

/**
 * @brief Find the entry configured for a table named in the binlog
 *
 * Keys are the database-qualified identity ("db.table"), or the bare table
 * name for a configuration that names no database.
 *
 * @return contexts.end() when no entry matches
 */
template <typename Map>
typename Map::const_iterator FindTableContextEntry(const Map& contexts, const std::string& database,
                                                   const std::string& table, bool case_insensitive) {
  const std::string qualified = config::QualifiedTableName(database, table);
  auto iter = contexts.find(qualified);
  if (iter == contexts.end()) {
    iter = contexts.find(table);
  }
  if (iter != contexts.end() || !case_insensitive) {
    return iter;
  }
  return std::find_if(contexts.begin(), contexts.end(), [&](const auto& entry) {
    return TableNamesEqual(entry.first, qualified, true) || TableNamesEqual(entry.first, table, true);
  });
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
