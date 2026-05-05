/**
 * @file document_handler.cpp
 * @brief Handler for GET command
 */

#include "server/handlers/document_handler.h"

namespace mygramdb::server {

std::string DocumentHandler::Handle(const query::Query& query, ConnectionContext& conn_ctx) {
  (void)conn_ctx;  // Unused for GET command

  // Check if server is loading
  if (auto err = CheckNotLoading(); !err.empty()) {
    return err;
  }

  // Get table context
  auto table_ctx = GetTableContext(query.table);
  if (!table_ctx) {
    return ResponseFormatter::FormatError(table_ctx.error().message());
  }

  auto* current_doc_store = table_ctx->doc_store;

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
