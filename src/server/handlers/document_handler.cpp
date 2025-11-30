/**
 * @file document_handler.cpp
 * @brief Handler for GET command
 */

#include "server/handlers/document_handler.h"

namespace mygramdb::server {

std::string DocumentHandler::Handle(const query::Query& query, ConnectionContext& conn_ctx) {
  (void)conn_ctx;  // Unused for GET command

  // Check if server is loading
  if (ctx_.dump_load_in_progress) {
    return ResponseFormatter::FormatError("Server is loading, please try again later");
  }

  // Get table context
  index::Index* current_index = nullptr;
  storage::DocumentStore* current_doc_store = nullptr;
  int current_ngram_size = 0;
  int current_kanji_ngram_size = 0;

  std::string error =
      GetTableContext(query.table, &current_index, &current_doc_store, &current_ngram_size, &current_kanji_ngram_size);
  if (!error.empty()) {
    return error;
  }

  // Verify doc_store is available
  if (current_doc_store == nullptr) {
    return ResponseFormatter::FormatError("Document store not available");
  }

  auto doc_id_opt = current_doc_store->GetDocId(query.primary_key);
  if (!doc_id_opt) {
    return ResponseFormatter::FormatError("Document not found");
  }

  auto doc = current_doc_store->GetDocument(doc_id_opt.value());
  return ResponseFormatter::FormatGetResponse(doc);
}

}  // namespace mygramdb::server
