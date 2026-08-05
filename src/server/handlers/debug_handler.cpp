/**
 * @file debug_handler.cpp
 * @brief Handler for debug and maintenance commands
 */

#include "server/handlers/debug_handler.h"

#include <sstream>
#include <vector>

#include "server/log_field_names.h"
#include "server/operation_coordinator.h"
#include "server/operation_names.h"
#include "server/sync_operation_manager.h"
#include "server/table_catalog.h"
#include "utils/flag_guard.h"
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
          .Field(log_fields::kFieldFd, static_cast<int64_t>(conn_ctx.client_fd))
          .Debug();
      return ResponseFormatter::FormatStatus("DEBUG_ON");
    }

    case query::QueryType::DEBUG_OFF: {
      conn_ctx.debug_mode = false;
      mygram::utils::StructuredLog()
          .Event("debug_mode_disabled")
          .Field(log_fields::kFieldFd, static_cast<int64_t>(conn_ctx.client_fd))
          .Debug();
      return ResponseFormatter::FormatStatus("DEBUG_OFF");
    }

    case query::QueryType::OPTIMIZE: {
#ifdef USE_MYSQL
      // Check if any table is currently syncing
      if (ctx_.sync_manager != nullptr) {
        auto check = ctx_.sync_manager->CheckNoSyncInProgress(ops::kOptimize);
        if (!check) {
          return ResponseFormatter::FormatError(check.error().message());
        }
      }
#endif

      // Check if DUMP LOAD is in progress
      if (ctx_.dump_load_in_progress.load()) {
        return ResponseFormatter::FormatError(
            "Cannot optimize while DUMP LOAD is in progress. "
            "Please wait for load to complete.");
      }

      OperationCoordinator::Token operation_token;
      if (ctx_.operation_coordinator != nullptr) {
        const std::string operation_detail = query.table.empty() ? "all tables" : query.table;
        auto acquired = ctx_.operation_coordinator->TryAcquire(LongOperation::kOptimize, operation_detail);
        if (!acquired.has_value()) {
          return ResponseFormatter::FormatError("Cannot optimize while " +
                                                ctx_.operation_coordinator->DescribeActive() + " is in progress");
        }
        operation_token = std::move(*acquired);
      }

      // Atomic test-and-set with scope-bound release via OperationGuard::TryAcquire.
      // Same contract as DUMP SAVE / DUMP LOAD: prevents two concurrent OPTIMIZE
      // callers from both observing the flag false and racing each other inside
      // the index-rebuild critical section.
      auto guard = mygram::utils::OperationGuard::TryAcquire(ctx_.optimization_in_progress);
      if (!guard.engaged()) {
        return ResponseFormatter::FormatError("Another OPTIMIZE operation is already in progress");
      }

      if (ctx_.table_catalog == nullptr) {
        return ResponseFormatter::FormatError("Table catalog not initialized");
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
            .Event("optimize_rejected")
            .Field("reason", "critical_memory_status")
            .Field("details", oss.str())
            .Warn();
        return ResponseFormatter::FormatError("Memory critically low. Cannot start optimization: " + oss.str());
      }

      std::vector<std::string> table_names;
      if (query.table.empty()) {
        table_names = ctx_.table_catalog->GetTableNames();
        if (table_names.empty()) {
          return ResponseFormatter::FormatError("No tables are available for optimization");
        }
      } else {
        table_names.push_back(query.table);
      }

      constexpr size_t kDefaultBatchSize = 1000;
      size_t optimized_tables = 0;
      uint64_t total_terms = 0;
      uint64_t total_delta_lists = 0;
      uint64_t total_roaring_lists = 0;
      uint64_t total_memory = 0;

      for (const auto& table_name : table_names) {
        auto table_ctx = GetTableContext(table_name);
        if (!table_ctx) {
          return ResponseFormatter::FormatError(table_ctx.error().message());
        }
        auto* current_index = table_ctx->index;
        auto* current_doc_store = table_ctx->doc_store;
        if (current_index == nullptr || current_doc_store == nullptr) {
          return ResponseFormatter::FormatError("Index or document store not available for table: " + table_name);
        }

        const uint64_t index_memory = current_index->MemoryUsage();
        const uint64_t total_docs = current_doc_store->Size();
        const uint64_t estimated_memory = mygram::utils::EstimateOptimizationMemory(index_memory, kDefaultBatchSize);
        if (!mygram::utils::CheckMemoryAvailability(estimated_memory, mygram::utils::kDefaultMemorySafetyMargin)) {
          auto sys_info = mygram::utils::GetSystemMemoryInfo();
          std::ostringstream oss;
          oss << "Insufficient memory: estimated=" << mygram::utils::FormatBytes(estimated_memory);
          if (sys_info) {
            oss << " available=" << mygram::utils::FormatBytes(sys_info->available_physical_bytes);
          }
          mygram::utils::StructuredLog()
              .Event("optimize_rejected")
              .Field("reason", "insufficient_memory")
              .Field("details", oss.str())
              .Field("table", table_name)
              .Warn();
          return ResponseFormatter::FormatError("Insufficient memory for optimization: " + oss.str());
        }

        mygram::utils::StructuredLog()
            .Event("index_optimization_starting")
            .Field("table", table_name)
            .Field("memory_health", mygram::utils::MemoryHealthStatusToString(memory_health))
            .Field("estimated_memory", mygram::utils::FormatBytes(estimated_memory))
            .Field("index_size", mygram::utils::FormatBytes(index_memory))
            .Field("docs", total_docs)
            .Info();

        if (!current_index->OptimizeInBatches(total_docs, kDefaultBatchSize)) {
          return ResponseFormatter::FormatError("Failed to optimize table: " + table_name);
        }
        auto stats = current_index->GetStatistics();
        ++optimized_tables;
        total_terms += stats.total_terms;
        total_delta_lists += stats.delta_encoded_lists;
        total_roaring_lists += stats.roaring_bitmap_lists;
        total_memory += stats.memory_usage_bytes;
      }

      std::ostringstream body;
      body << "OPTIMIZED tables=" << optimized_tables << " terms=" << total_terms << " delta=" << total_delta_lists
           << " roaring=" << total_roaring_lists << " memory=" << mygram::utils::FormatBytes(total_memory);
      return ResponseFormatter::FormatStatus(body.str());
    }

    default:
      return ResponseFormatter::FormatError("Invalid query type for DebugHandler");
  }
}

}  // namespace mygramdb::server
