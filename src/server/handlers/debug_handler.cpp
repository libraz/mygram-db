/**
 * @file debug_handler.cpp
 * @brief Handler for debug and maintenance commands
 */

#include "server/handlers/debug_handler.h"

#include <spdlog/spdlog.h>

#include <sstream>

#include "server/sync_operation_manager.h"
#include "utils/memory_utils.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

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
#ifdef USE_MYSQL
      // Check if any table is currently syncing
      if (ctx_.sync_manager != nullptr && ctx_.sync_manager->IsAnySyncing()) {
        return ResponseFormatter::FormatError(
            "Cannot optimize while SYNC is in progress. "
            "Please wait for SYNC to complete.");
      }
#endif

      // Check if DUMP LOAD is in progress
      if (ctx_.dump_load_in_progress.load()) {
        return ResponseFormatter::FormatError(
            "Cannot optimize while DUMP LOAD is in progress. "
            "Please wait for load to complete.");
      }

      // Note: DUMP SAVE (dump_save_in_progress flag) is allowed during OPTIMIZE
      // to support auto-save functionality that runs in background

      // Check if another OPTIMIZE is already running globally
      bool expected = false;
      if (!ctx_.optimization_in_progress.compare_exchange_strong(expected, true)) {
        return ResponseFormatter::FormatError("Another OPTIMIZE operation is already in progress");
      }

      // RAII guard to ensure flag is cleared even if exception occurs
      struct OptimizationGuard {
        std::atomic<bool>& flag;
        explicit OptimizationGuard(std::atomic<bool>& flag_ref) : flag(flag_ref) {}
        OptimizationGuard(const OptimizationGuard&) = delete;
        OptimizationGuard& operator=(const OptimizationGuard&) = delete;
        OptimizationGuard(OptimizationGuard&&) = delete;
        OptimizationGuard& operator=(OptimizationGuard&&) = delete;
        ~OptimizationGuard() { flag.store(false); }
      };
      OptimizationGuard guard(ctx_.optimization_in_progress);

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

      // Check memory health before optimization
      auto memory_health = utils::GetMemoryHealthStatus();
      if (memory_health == utils::MemoryHealthStatus::CRITICAL) {
        auto sys_info = utils::GetSystemMemoryInfo();
        std::ostringstream oss;
        oss << "Memory critically low: ";
        if (sys_info) {
          oss << "available=" << utils::FormatBytes(sys_info->available_physical_bytes)
              << " total=" << utils::FormatBytes(sys_info->total_physical_bytes);
        }
        mygram::utils::StructuredLog()
            .Event("server_warning")
            .Field("type", "optimize_rejected")
            .Field("reason", "critical_memory_status")
            .Field("details", oss.str())
            .Warn();
        return ResponseFormatter::FormatError("Memory critically low. Cannot start optimization: " + oss.str());
      }

      // Estimate memory required for optimization
      uint64_t index_memory = current_index->MemoryUsage();
      uint64_t total_docs = current_doc_store->Size();
      constexpr size_t kDefaultBatchSize = 1000;
      uint64_t estimated_memory = utils::EstimateOptimizationMemory(index_memory, kDefaultBatchSize);

      // Check if estimated memory is available (with 10% safety margin)
      if (!utils::CheckMemoryAvailability(estimated_memory, utils::kDefaultMemorySafetyMargin)) {
        auto sys_info = utils::GetSystemMemoryInfo();
        std::ostringstream oss;
        oss << "Insufficient memory: estimated=" << utils::FormatBytes(estimated_memory);
        if (sys_info) {
          oss << " available=" << utils::FormatBytes(sys_info->available_physical_bytes);
        }
        mygram::utils::StructuredLog()
            .Event("server_warning")
            .Field("type", "optimize_rejected")
            .Field("reason", "insufficient_memory")
            .Field("details", oss.str())
            .Warn();
        return ResponseFormatter::FormatError("Insufficient memory for optimization: " + oss.str());
      }

      spdlog::info("Starting index optimization: memory_health={} estimated={} index_size={} docs={}",
                   utils::MemoryHealthStatusToString(memory_health), utils::FormatBytes(estimated_memory),
                   utils::FormatBytes(index_memory), total_docs);

      // Run optimization (this will block, but it's intentional for now)
      bool started = current_index->OptimizeInBatches(total_docs, kDefaultBatchSize);

      if (started) {
        auto stats = current_index->GetStatistics();
        std::ostringstream oss;
        oss << "OK OPTIMIZED terms=" << stats.total_terms << " delta=" << stats.delta_encoded_lists
            << " roaring=" << stats.roaring_bitmap_lists << " memory=" << utils::FormatBytes(stats.memory_usage_bytes);
        return oss.str();
      }
      return ResponseFormatter::FormatError("Failed to start optimization");
    }

    default:
      return ResponseFormatter::FormatError("Invalid query type for DebugHandler");
  }
}

}  // namespace mygramdb::server
