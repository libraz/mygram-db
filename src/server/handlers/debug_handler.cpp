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
      mygram::utils::StructuredLog()
          .Event("debug_mode_enabled")
          .Field("connection_fd", static_cast<int64_t>(conn_ctx.client_fd))
          .Debug();
      return ResponseFormatter::FormatStatus("DEBUG_ON");
    }

    case query::QueryType::DEBUG_OFF: {
      conn_ctx.debug_mode = false;
      mygram::utils::StructuredLog()
          .Event("debug_mode_disabled")
          .Field("connection_fd", static_cast<int64_t>(conn_ctx.client_fd))
          .Debug();
      return ResponseFormatter::FormatStatus("DEBUG_OFF");
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
      auto table_ctx = GetTableContext(query.table);
      if (!table_ctx) {
        return ResponseFormatter::FormatError(table_ctx.error().message());
      }
      auto* current_index = table_ctx->index;
      auto* current_doc_store = table_ctx->doc_store;

      // Verify index is available
      if (current_index == nullptr) {
        return ResponseFormatter::FormatError("Index not available");
      }

      // Check memory health before optimization
      auto memory_health = mygram::utils::GetMemoryHealthStatus();
      if (memory_health == mygram::utils::MemoryHealthStatus::CRITICAL) {
        auto sys_info = mygram::utils::GetSystemMemoryInfo();
        std::ostringstream oss;
        oss << "Memory critically low: ";
        if (sys_info) {
          oss << "available=" << mygram::utils::FormatBytes(sys_info->available_physical_bytes)
              << " total=" << mygram::utils::FormatBytes(sys_info->total_physical_bytes);
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
      uint64_t estimated_memory = mygram::utils::EstimateOptimizationMemory(index_memory, kDefaultBatchSize);

      // Check if estimated memory is available (with 10% safety margin)
      if (!mygram::utils::CheckMemoryAvailability(estimated_memory, mygram::utils::kDefaultMemorySafetyMargin)) {
        auto sys_info = mygram::utils::GetSystemMemoryInfo();
        std::ostringstream oss;
        oss << "Insufficient memory: estimated=" << mygram::utils::FormatBytes(estimated_memory);
        if (sys_info) {
          oss << " available=" << mygram::utils::FormatBytes(sys_info->available_physical_bytes);
        }
        mygram::utils::StructuredLog()
            .Event("server_warning")
            .Field("type", "optimize_rejected")
            .Field("reason", "insufficient_memory")
            .Field("details", oss.str())
            .Warn();
        return ResponseFormatter::FormatError("Insufficient memory for optimization: " + oss.str());
      }

      mygram::utils::StructuredLog()
          .Event("index_optimization_starting")
          .Field("memory_health", mygram::utils::MemoryHealthStatusToString(memory_health))
          .Field("estimated_memory", mygram::utils::FormatBytes(estimated_memory))
          .Field("index_size", mygram::utils::FormatBytes(index_memory))
          .Field("docs", total_docs)
          .Info();

      // Run optimization (this will block, but it's intentional for now)
      bool started = current_index->OptimizeInBatches(total_docs, kDefaultBatchSize);

      if (started) {
        auto stats = current_index->GetStatistics();
        std::ostringstream oss;
        oss << "OK OPTIMIZED terms=" << stats.total_terms << " delta=" << stats.delta_encoded_lists
            << " roaring=" << stats.roaring_bitmap_lists
            << " memory=" << mygram::utils::FormatBytes(stats.memory_usage_bytes);
        return oss.str();
      }
      return ResponseFormatter::FormatError("Failed to start optimization");
    }

    default:
      return ResponseFormatter::FormatError("Invalid query type for DebugHandler");
  }
}

}  // namespace mygramdb::server
