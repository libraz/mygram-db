/**
 * @file command_handler.cpp
 * @brief Base class for command handlers
 */

#include "server/handlers/command_handler.h"

#include "index/index.h"
#include "query/query_parser.h"
#include "server/table_catalog.h"
#include "storage/document_store.h"

#ifdef USE_MYSQL
#include "server/sync_operation_manager.h"
#endif

namespace mygramdb::server {

mygram::utils::Expected<std::string, mygram::utils::Error> CommandHandler::ResolveTableName(
    const std::string& table_name) const {
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (ctx_.table_catalog == nullptr) {
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kCatalogNotInitialized, "Table catalog not initialized"));
  }

  // Multi-database configurations require qualified references; reject bare
  // identifiers up front to keep the helpful error message.
  if (config::RequiresQualifiedTableReferences(ctx_.full_config) &&
      !query::QueryParser::IsDatabaseQualifiedTableName(table_name)) {
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kTableNotFound,
                                    "Bare table names are not supported; use <database>.<table>: " + table_name));
  }

  auto resolved = ctx_.table_catalog->ResolveName(table_name);
  if (!resolved.has_value()) {
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kTableNotFound, "Table not found: " + table_name));
  }
  return *resolved;
}

mygram::utils::Expected<query::Query, mygram::utils::Error> CommandHandler::CanonicalizeQueryTable(
    const query::Query& query) const {
  auto resolved = ResolveTableName(query.table);
  if (!resolved) {
    return mygram::utils::MakeUnexpected(resolved.error());
  }
  query::Query canonical = query;
  canonical.table = *resolved;
  return canonical;
}

mygram::utils::Expected<CommandHandler::TableContextResult, mygram::utils::Error> CommandHandler::GetTableContext(
    const std::string& table_name) {
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  auto resolved = ResolveTableName(table_name);
  if (!resolved) {
    return MakeUnexpected(resolved.error());
  }

  auto* table_ctx = ctx_.table_catalog->GetTable(*resolved);
  if (table_ctx == nullptr) {
    return MakeUnexpected(MakeError(mygram::utils::ErrorCode::kTableNotFound, "Table not found: " + table_name));
  }

  std::shared_lock<std::shared_mutex> generation_lock(*table_ctx->generation_mutex);

  return TableContextResult{
      .table_context = table_ctx,
      .index = table_ctx->index.get(),
      .doc_store = table_ctx->doc_store.get(),
      .ngram_size = table_ctx->config.ngram_size,
      .kanji_ngram_size = table_ctx->config.kanji_ngram_size,
      .generation_lock = std::move(generation_lock),
  };
}

std::string CommandHandler::CheckNotLoading() const {
  if (ctx_.dump_load_in_progress) {
    return ResponseFormatter::FormatError(mygram::utils::MakeError(mygram::utils::ErrorCode::kServerLoading,
                                                                   "Server is loading, please try again later"));
  }
  return {};
}

std::string CommandHandler::CheckTableNotSyncing(const std::string& table_name) const {
#ifdef USE_MYSQL
  if (ctx_.sync_manager == nullptr || table_name.empty()) {
    return {};
  }

  // syncing_tables_ is keyed by the qualified `database.table` identity. Resolve
  // a (possibly bare) request name to that canonical key before comparing so
  // single-database bare references match an in-flight SYNC.
  std::string resolved = table_name;
  if (ctx_.table_catalog != nullptr) {
    if (auto key = ctx_.table_catalog->ResolveName(table_name); key.has_value()) {
      resolved = std::move(*key);
    }
  }

  auto syncing_tables = ctx_.sync_manager->GetSyncingTables();
  if (syncing_tables.find(resolved) == syncing_tables.end()) {
    return {};
  }

  return ResponseFormatter::FormatError(
      "Table '" + ResponseFormatter::SanitizeDelimitedField(resolved) + "' is synchronizing, please try again later",
      mygram::utils::ErrorCode::kServerNotReady);
#else
  (void)table_name;
  return {};
#endif
}

}  // namespace mygramdb::server
