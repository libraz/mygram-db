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
#include "index/index.h"
#include "query/query_parser.h"
#include "server/response_formatter.h"
#include "server/server_stats.h"
#include "server/server_types.h"
#include "storage/document_store.h"
#include "utils/error.h"
#include "utils/expected.h"

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
};

}  // namespace mygramdb::server
