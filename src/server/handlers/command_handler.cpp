/**
 * @file command_handler.cpp
 * @brief Base class for command handlers
 */

#include "server/handlers/command_handler.h"

namespace mygramdb::server {

std::string CommandHandler::GetTableContext(const std::string& table_name, index::Index** index,
                                            storage::DocumentStore** doc_store, int* ngram_size,
                                            int* kanji_ngram_size) {
  auto table_iter = ctx_.table_contexts.find(table_name);
  if (table_iter == ctx_.table_contexts.end()) {
    return ResponseFormatter::FormatError("Table not found: " + table_name);
  }

  *index = table_iter->second->index.get();
  *doc_store = table_iter->second->doc_store.get();
  *ngram_size = table_iter->second->config.ngram_size;
  *kanji_ngram_size = table_iter->second->config.kanji_ngram_size;

  return "";  // Success
}

}  // namespace mygramdb::server
