/**
 * @file debug_handler.cpp
 * @brief Handler for debug and maintenance commands
 */

#include "server/handlers/debug_handler.h"

#include <spdlog/spdlog.h>

#include <sstream>

namespace mygramdb::server {

std::string DebugHandler::Handle(const query::Query& query, ConnectionContext& conn_ctx) {
  switch (query.type) {
    case query::QueryType::DEBUG_ON: {
      conn_ctx.debug_mode = true;
      spdlog::debug("Debug mode enabled for connection {}", conn_ctx.client_fd);
      return "OK DEBUG_ON";
    }

    case query::QueryType::DEBUG_OFF: {
      conn_ctx.debug_mode = false;
      spdlog::debug("Debug mode disabled for connection {}", conn_ctx.client_fd);
      return "OK DEBUG_OFF";
    }

    case query::QueryType::OPTIMIZE: {
      // Get table context
      index::Index* current_index = nullptr;
      storage::DocumentStore* current_doc_store = nullptr;
      int current_ngram_size = 0;
      int current_kanji_ngram_size = 0;

      std::string error = GetTableContext(query.table, &current_index, &current_doc_store, &current_ngram_size,
                                          &current_kanji_ngram_size);
      if (!error.empty()) {
        return error;
      }

      // Verify index is available
      if (current_index == nullptr) {
        return ResponseFormatter::FormatError("Index not available");
      }

      // Check if optimization is already running
      if (current_index->IsOptimizing()) {
        return ResponseFormatter::FormatError("Optimization already in progress");
      }

      spdlog::info("Starting index optimization by user request");
      uint64_t total_docs = current_doc_store->Size();

      // Run optimization (this will block, but it's intentional for now)
      bool started = current_index->OptimizeInBatches(total_docs);

      if (started) {
        auto stats = current_index->GetStatistics();
        std::ostringstream oss;
        oss << "OK OPTIMIZED terms=" << stats.total_terms << " delta=" << stats.delta_encoded_lists
            << " roaring=" << stats.roaring_bitmap_lists;
        return oss.str();
      }
      return ResponseFormatter::FormatError("Failed to start optimization");
    }

    default:
      return ResponseFormatter::FormatError("Invalid query type for DebugHandler");
  }
}

}  // namespace mygramdb::server
