/**
 * @file dump_config_validator.h
 * @brief Shared compatibility checks for manual and startup dump restores
 */

#pragma once

#include <optional>
#include <string>

#include "config/config.h"

namespace mygramdb::server {

/**
 * @brief Resolve the running-config entry a dump table entry refers to.
 *
 * A table's identity is its qualified `database.table` name: configuration
 * loading rejects duplicate `(database, name)` pairs and therefore accepts the
 * same bare name in two databases, and the dump table set is compared by
 * qualified name as well. Matching on the bare name here would compare a dump
 * entry against whichever database happened to be configured first.
 *
 * A dump entry that records no database at all falls back to the bare name.
 * This is a backstop, not the compatibility mechanism: a table entry written
 * before the database field existed decodes with it empty, but DeserializeConfig
 * fills it from the dump's own mysql.database before the entry reaches this
 * check, so an artifact an earlier release wrote arrives already qualified. The
 * fallback covers only an entry that reaches here unqualified anyway, where the
 * bare name is the sole identity the dump carries.
 *
 * @param config Running configuration to search.
 * @param dump_table Table entry decoded from the dump.
 * @return Matching running-config entry, or nullptr when the dump table is not
 *         configured.
 */
inline const config::TableConfig* FindDumpTableConfig(const config::Config& config,
                                                      const config::TableConfig& dump_table) {
  const auto dump_key = config::QualifiedTableName(dump_table);
  for (const auto& table : config.tables) {
    if (config::QualifiedTableName(table) == dump_key) {
      return &table;
    }
  }
  if (!dump_table.database.empty()) {
    return nullptr;
  }
  for (const auto& table : config.tables) {
    if (table.name == dump_table.name) {
      return &table;
    }
  }
  return nullptr;
}

inline std::optional<std::string> FindDumpConfigMismatch(const config::Config& loaded_config,
                                                         const config::Config& live_config) {
  if (loaded_config.memory.verify_text.empty()) {
    if (live_config.memory.verify_text != "off") {
      return "legacy dump is missing memory.verify_text compatibility metadata; load with verify_text=off or rebuild "
             "the dump from the source database";
    }
  } else if (loaded_config.memory.verify_text != live_config.memory.verify_text) {
    return "memory.verify_text mismatch between dump and running config";
  }
  if (loaded_config.memory.normalize.nfkc != live_config.memory.normalize.nfkc) {
    return "memory.normalize.nfkc mismatch between dump and running config";
  }
  if (loaded_config.memory.normalize.width != live_config.memory.normalize.width) {
    return "memory.normalize.width mismatch between dump and running config";
  }
  if (loaded_config.memory.normalize.lower != live_config.memory.normalize.lower) {
    return "memory.normalize.lower mismatch between dump and running config";
  }

  for (const auto& loaded_table : loaded_config.tables) {
    const auto* live_table = FindDumpTableConfig(live_config, loaded_table);
    if (live_table == nullptr) {
      continue;
    }
    const auto table_name = config::QualifiedTableName(*live_table);
    if (loaded_table.ngram_size != live_table->ngram_size) {
      return "table '" + table_name + "' ngram_size mismatch between dump and running config";
    }
    if (loaded_table.kanji_ngram_size != live_table->kanji_ngram_size) {
      return "table '" + table_name + "' kanji_ngram_size mismatch between dump and running config";
    }
    if (loaded_table.cross_boundary_ngrams != live_table->cross_boundary_ngrams) {
      return "table '" + table_name + "' cross_boundary_ngrams mismatch between dump and running config";
    }
  }

  return std::nullopt;
}

}  // namespace mygramdb::server
