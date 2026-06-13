/**
 * @file command_handler.h
 * @brief Base class for command handlers
 */

#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>

#include "config/config.h"
#include "server/response_formatter.h"
#include "server/server_stats.h"
#include "server/server_types.h"
#include "utils/error.h"
#include "utils/expected.h"

// Forward declarations for types only used as pointers / references in this
// header. NOTE: server_types.h still pulls in index/index.h and
// storage/document_store.h because TableContext holds unique_ptr to those
// (the implicit destructor needs the full type), so this is mostly a
// hygiene improvement — the heavy headers are not actually shed from
// downstream TUs until TableContext is restructured.
namespace mygramdb::index {
class Index;
}  // namespace mygramdb::index
namespace mygramdb::storage {
class DocumentStore;
}  // namespace mygramdb::storage
namespace mygramdb::query {
struct Query;
enum class QueryType : uint8_t;
}  // namespace mygramdb::query

#ifdef USE_MYSQL
namespace mygramdb::mysql {
class BinlogReader;
}  // namespace mygramdb::mysql
#endif

namespace mygramdb::server {

/**
 * @brief Base class for all command handlers
 *
 * Provides common utilities and interface for handling specific query types
 */
class CommandHandler {
 public:
  explicit CommandHandler(HandlerContext& ctx) : ctx_(ctx) {}
  virtual ~CommandHandler() = default;

  // Non-copyable and non-movable
  CommandHandler(const CommandHandler&) = delete;
  CommandHandler& operator=(const CommandHandler&) = delete;
  CommandHandler(CommandHandler&&) = delete;
  CommandHandler& operator=(CommandHandler&&) = delete;

  /**
   * @brief Handle a query
   * @param query Parsed query
   * @param conn_ctx Connection context (for debug mode, etc.)
   * @return Response string
   */
  virtual std::string Handle(const query::Query& query, ConnectionContext& conn_ctx) = 0;

 protected:
  // Protected access needed by derived handlers
  HandlerContext& ctx_;  // NOLINT(cppcoreguidelines-non-private-member-variables-in-classes)

  /**
   * @brief Result of GetTableContext lookup
   */
  struct TableContextResult {
    TableContext* table_context = nullptr;
    index::Index* index = nullptr;
    storage::DocumentStore* doc_store = nullptr;
    int ngram_size = 0;
    int kanji_ngram_size = 0;
  };

  /**
   * @brief Get table context for a query
   * @param table_name Table name
   * @return Table context result or error
   */
  mygram::utils::Expected<TableContextResult, mygram::utils::Error> GetTableContext(const std::string& table_name);

  /**
   * @brief Check whether a DUMP LOAD operation is in progress.
   *
   * Returns an empty string if the server is ready to handle requests, or a
   * pre-formatted ERROR response describing the loading state. Handlers should
   * call this helper at the start of request processing and return the result
   * directly when it is non-empty.
   *
   * @return Empty string when ready, or a formatted ERROR response otherwise.
   */
  std::string CheckNotLoading() const;

  /**
   * @brief Check whether the requested table is currently being rebuilt by SYNC.
   *
   * Returns an empty string if the table is ready, or a pre-formatted ERROR
   * response otherwise. This avoids silently serving empty or partial
   * index/doc_store state while SYNC has cleared and is rebuilding the table.
   */
  std::string CheckTableNotSyncing(const std::string& table_name) const;
};

}  // namespace mygramdb::server
