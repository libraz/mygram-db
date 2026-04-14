/**
 * @file command_handler.cpp
 * @brief Base class for command handlers
 */

#include "server/handlers/command_handler.h"

#include "server/table_catalog.h"

namespace mygramdb::server {

std::string CommandHandler::GetTableContext(const std::string& table_name, index::Index** index,
                                            storage::DocumentStore** doc_store, int* ngram_size,
                                            int* kanji_ngram_size) {
  if (ctx_.table_catalog == nullptr) {
    return ResponseFormatter::FormatError("Table catalog not initialized");
  }

  auto* table_ctx = ctx_.table_catalog->GetTable(table_name);
  if (table_ctx == nullptr) {
    return ResponseFormatter::FormatError("Table not found: " + table_name);
  }

  *index = table_ctx->index.get();
  *doc_store = table_ctx->doc_store.get();
  *ngram_size = table_ctx->config.ngram_size;
  *kanji_ngram_size = table_ctx->config.kanji_ngram_size;

  return "";  // Success
}

}  // namespace mygramdb::server
