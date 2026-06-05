/**
 * @file command_handler.cpp
 * @brief Base class for command handlers
 */

#include "server/handlers/command_handler.h"

#include "index/index.h"
#include "query/query_parser.h"
#include "server/table_catalog.h"
#include "storage/document_store.h"

namespace mygramdb::server {

mygram::utils::Expected<CommandHandler::TableContextResult, mygram::utils::Error> CommandHandler::GetTableContext(
    const std::string& table_name) {
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (ctx_.table_catalog == nullptr) {
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kCatalogNotInitialized, "Table catalog not initialized"));
  }

  auto* table_ctx = ctx_.table_catalog->GetTable(table_name);
  if (table_ctx == nullptr) {
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kTableNotFound, "Table not found: " + table_name));
  }

  return TableContextResult{
      .table_context = table_ctx,
      .index = table_ctx->index.get(),
      .doc_store = table_ctx->doc_store.get(),
      .ngram_size = table_ctx->config.ngram_size,
      .kanji_ngram_size = table_ctx->config.kanji_ngram_size,
  };
}

std::string CommandHandler::CheckNotLoading() const {
  if (ctx_.dump_load_in_progress) {
    return ResponseFormatter::FormatError("Server is loading, please try again later");
  }
  return {};
}

}  // namespace mygramdb::server
