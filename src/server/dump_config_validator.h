/**
 * @file dump_config_validator.h
 * @brief Shared compatibility checks for manual and startup dump restores
 */

#pragma once

#include <optional>
#include <string>

#include "config/config.h"

namespace mygramdb::server {

inline const config::TableConfig* FindDumpTableConfigByName(const config::Config& config,
                                                            const std::string& table_name) {
  for (const auto& table : config.tables) {
    if (table.name == table_name) {
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
    const auto* live_table = FindDumpTableConfigByName(live_config, loaded_table.name);
    if (live_table == nullptr) {
      continue;
    }
    if (loaded_table.ngram_size != live_table->ngram_size) {
      return "table '" + loaded_table.name + "' ngram_size mismatch between dump and running config";
    }
    if (loaded_table.kanji_ngram_size != live_table->kanji_ngram_size) {
      return "table '" + loaded_table.name + "' kanji_ngram_size mismatch between dump and running config";
    }
    if (loaded_table.cross_boundary_ngrams != live_table->cross_boundary_ngrams) {
      return "table '" + loaded_table.name + "' cross_boundary_ngrams mismatch between dump and running config";
    }
  }

  return std::nullopt;
}

}  // namespace mygramdb::server
