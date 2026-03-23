/**
 * @file binlog_event_processor.cpp
 * @brief Binlog event processor implementation
 */

#include "mysql/binlog_event_processor.h"

#ifdef USE_MYSQL

#include <spdlog/spdlog.h>

#include <algorithm>

#include "cache/cache_manager.h"
#include "mysql/binlog_filter_evaluator.h"
#include "server/tcp_server.h"  // For ServerStats
#include "utils/string_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::mysql {

bool BinlogEventProcessor::ProcessEvent(const BinlogEvent& event, index::Index& index,
                                        storage::DocumentStore& doc_store, const config::TableConfig& table_config,
                                        const config::MysqlConfig& mysql_config, server::ServerStats* stats,
                                        cache::CacheManager* cache_manager) {
  try {
    // Debug: log doc_store instance address and document count to verify correct instance is used
    // This helps diagnose BUG where replication uses different instance than SYNC populated
    mygram::utils::StructuredLog()
        .Event("binlog_process_event")
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for debug address logging
        .Field("doc_store_addr", reinterpret_cast<uint64_t>(&doc_store))
        .Field("doc_store_size", static_cast<uint64_t>(doc_store.Size()))
        .Field("event_type", static_cast<int64_t>(event.type))
        .Field("primary_key", event.primary_key)
        .Debug();

    // Evaluate required_filters to determine if data should exist in index
    bool matches_required =
        BinlogFilterEvaluator::EvaluateRequiredFilters(event.filters, table_config, mysql_config.datetime_timezone);

    // Check if document already exists in index
    auto doc_id_opt = doc_store.GetDocId(event.primary_key);
    bool exists = doc_id_opt.has_value();

    switch (event.type) {
      case BinlogEventType::INSERT: {
        if (exists) {
          // Document already exists (replay scenario) — skip to maintain idempotency
          mygram::utils::StructuredLog()
              .Event("binlog_insert")
              .Field("primary_key", event.primary_key)
              .Field("action", "skipped_duplicate")
              .Debug();
          if (stats != nullptr) {
            stats->IncrementReplInsertSkipped();
          }
          break;
        }
        if (matches_required) {
          // Condition satisfied -> add to index
          std::string normalized = index.NormalizeText(event.text);

          auto doc_id_result = doc_store.AddDocument(event.primary_key, event.filters, normalized);
          if (!doc_id_result) {
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_error")
                .Field("type", "add_document_failed")
                .Field("event_type", "insert")
                .Field("primary_key", event.primary_key)
                .Field("error", doc_id_result.error().message())
                .Error();
            return false;
          }
          storage::DocId doc_id = *doc_id_result;

          // Atomic operation: if index fails, rollback document store
          try {
            index.AddDocument(doc_id, normalized);
          } catch (const std::exception& e) {
            // Rollback: remove from document store to maintain consistency
            doc_store.RemoveDocument(doc_id);
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_error")
                .Field("type", "index_add_failed")
                .Field("event_type", "insert")
                .Field("primary_key", event.primary_key)
                .Field("doc_id", static_cast<uint64_t>(doc_id))
                .Field("error", e.what())
                .Error();
            return false;
          }

          mygram::utils::StructuredLog()
              .Event("binlog_insert")
              .Field("primary_key", event.primary_key)
              .Field("doc_id", static_cast<uint64_t>(doc_id))
              .Field("text_length", static_cast<uint64_t>(event.text.size()))
              .Field("action", "added_to_index")
              .Debug();
          if (stats != nullptr) {
            stats->IncrementReplInsertApplied();
          }
          if (cache_manager != nullptr) {
            cache_manager->Invalidate(event.table_name, "", event.text);
          }
        } else {
          // Condition not satisfied -> do not index
          mygram::utils::StructuredLog()
              .Event("binlog_insert")
              .Field("primary_key", event.primary_key)
              .Field("action", "skipped")
              .Debug();
          if (stats != nullptr) {
            stats->IncrementReplInsertSkipped();
          }
        }
        break;
      }

      case BinlogEventType::UPDATE: {
        if (exists && !matches_required) {
          // Transitioned out of required conditions -> DELETE from index
          storage::DocId doc_id = doc_id_opt.value();

          // Extract text to remove from index — use before-image (old_text) since
          // that's what was indexed, falling back to after-image (text) if unavailable
          const std::string& removal_text = event.old_text.empty() ? event.text : event.old_text;
          if (!removal_text.empty()) {
            std::string normalized = index.NormalizeText(removal_text);
            try {
              index.RemoveDocument(doc_id, normalized);
            } catch (const std::exception& e) {
              mygram::utils::StructuredLog()
                  .Event("mysql_binlog_error")
                  .Field("type", "index_remove_failed")
                  .Field("event_type", "update_remove")
                  .Field("primary_key", event.primary_key)
                  .Field("doc_id", static_cast<uint64_t>(doc_id))
                  .Field("error", e.what())
                  .Error();
              return false;
            }
          }

          if (!doc_store.RemoveDocument(doc_id)) {
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_warning")
                .Field("type", "document_store_remove_not_found")
                .Field("event_type", "update_remove")
                .Field("primary_key", event.primary_key)
                .Field("doc_id", static_cast<uint64_t>(doc_id))
                .Warn();
          }

          mygram::utils::StructuredLog()
              .Event("binlog_update_removed")
              .Field("primary_key", event.primary_key)
              .Field("doc_id", static_cast<uint64_t>(doc_id))
              .Debug();
          if (stats != nullptr) {
            stats->IncrementReplUpdateRemoved();
          }
          if (cache_manager != nullptr) {
            const std::string& old_text_for_cache = event.old_text.empty() ? event.text : event.old_text;
            cache_manager->Invalidate(event.table_name, old_text_for_cache, "");
          }

        } else if (!exists && matches_required) {
          // Transitioned into required conditions -> INSERT into index
          std::string normalized = index.NormalizeText(event.text);

          auto doc_id_result = doc_store.AddDocument(event.primary_key, event.filters, normalized);
          if (!doc_id_result) {
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_error")
                .Field("type", "add_document_failed")
                .Field("event_type", "update")
                .Field("primary_key", event.primary_key)
                .Field("error", doc_id_result.error().message())
                .Error();
            return false;
          }
          storage::DocId doc_id = *doc_id_result;

          // Atomic operation: if index fails, rollback document store
          try {
            index.AddDocument(doc_id, normalized);
          } catch (const std::exception& e) {
            // Rollback: remove from document store to maintain consistency
            doc_store.RemoveDocument(doc_id);
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_error")
                .Field("type", "index_add_failed")
                .Field("event_type", "update")
                .Field("primary_key", event.primary_key)
                .Field("doc_id", static_cast<uint64_t>(doc_id))
                .Field("error", e.what())
                .Error();
            return false;
          }

          mygram::utils::StructuredLog()
              .Event("binlog_update_added")
              .Field("primary_key", event.primary_key)
              .Field("doc_id", static_cast<uint64_t>(doc_id))
              .Field("text_length", static_cast<uint64_t>(event.text.size()))
              .Debug();
          if (stats != nullptr) {
            stats->IncrementReplUpdateAdded();
          }
          if (cache_manager != nullptr) {
            cache_manager->Invalidate(event.table_name, "", event.text);
          }

        } else if (exists && matches_required) {
          // Still matches conditions -> UPDATE
          storage::DocId doc_id = doc_id_opt.value();

          // Save old filters for rollback in case index update fails
          auto old_doc = doc_store.GetDocument(doc_id);
          std::unordered_map<std::string, storage::FilterValue> old_filters;
          if (old_doc.has_value()) {
            old_filters = std::move(old_doc->filters);
          }

          // Update document store filters (check return value for race condition)
          if (!doc_store.UpdateDocument(doc_id, event.filters)) {
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_warning")
                .Field("type", "update_document_not_found")
                .Field("event_type", "update")
                .Field("primary_key", event.primary_key)
                .Field("doc_id", static_cast<uint64_t>(doc_id))
                .Warn();
            // Document was concurrently removed - skip index update
            break;
          }

          // Update full-text index if text has changed
          bool text_changed = false;
          if (!event.old_text.empty() || !event.text.empty()) {
            try {
              // Use Index::UpdateDocument for atomic update when both texts are available
              if (!event.old_text.empty() && !event.text.empty()) {
                std::string old_normalized = index.NormalizeText(event.old_text);
                std::string new_normalized = index.NormalizeText(event.text);
                index.UpdateDocument(doc_id, old_normalized, new_normalized);
                doc_store.SetNormalizedText(doc_id, new_normalized);
                text_changed = true;
              } else if (!event.old_text.empty()) {
                // Only old text available - remove from index
                std::string old_normalized = index.NormalizeText(event.old_text);
                index.RemoveDocument(doc_id, old_normalized);
                doc_store.SetNormalizedText(doc_id, "");
                text_changed = true;
              } else if (!event.text.empty()) {
                // Only new text available - add to index
                std::string new_normalized = index.NormalizeText(event.text);
                index.AddDocument(doc_id, new_normalized);
                doc_store.SetNormalizedText(doc_id, new_normalized);
                text_changed = true;
              }
            } catch (const std::exception& e) {
              // Rollback doc_store filters to maintain consistency
              doc_store.UpdateDocument(doc_id, old_filters);
              mygram::utils::StructuredLog()
                  .Event("mysql_binlog_error")
                  .Field("type", "index_update_failed")
                  .Field("event_type", "update")
                  .Field("primary_key", event.primary_key)
                  .Field("doc_id", static_cast<uint64_t>(doc_id))
                  .Field("filter_rollback", "rolled_back")
                  .Field("index_state", "partially_updated")
                  .Field("error", e.what())
                  .Error();
              return false;
            }
          }

          mygram::utils::StructuredLog()
              .Event("binlog_update")
              .Field("primary_key", event.primary_key)
              .Field("doc_id", static_cast<uint64_t>(doc_id))
              .Field("text_changed", text_changed)
              .Debug();
          if (stats != nullptr) {
            stats->IncrementReplUpdateModified();
          }
          if (cache_manager != nullptr) {
            bool filter_changed = (old_filters != event.filters);
            cache_manager->Invalidate(event.table_name, event.old_text, event.text, filter_changed);
          }

        } else {
          // !exists && !matches_required -> do nothing
          mygram::utils::StructuredLog()
              .Event("binlog_update")
              .Field("primary_key", event.primary_key)
              .Field("action", "ignored")
              .Debug();
          if (stats != nullptr) {
            stats->IncrementReplUpdateSkipped();
          }
        }
        break;
      }

      case BinlogEventType::DELETE: {
        if (exists) {
          // Remove document from index
          storage::DocId doc_id = doc_id_opt.value();

          // For deletion, we extract text from binlog DELETE event (before image)
          // The rows_parser provides the deleted row data including text column
          if (!event.text.empty()) {
            std::string normalized = index.NormalizeText(event.text);
            try {
              index.RemoveDocument(doc_id, normalized);
            } catch (const std::exception& e) {
              mygram::utils::StructuredLog()
                  .Event("mysql_binlog_error")
                  .Field("type", "index_remove_failed")
                  .Field("event_type", "delete")
                  .Field("primary_key", event.primary_key)
                  .Field("doc_id", static_cast<uint64_t>(doc_id))
                  .Field("error", e.what())
                  .Error();
              return false;
            }
          }

          // Remove from document store
          if (!doc_store.RemoveDocument(doc_id)) {
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_warning")
                .Field("type", "document_store_remove_not_found")
                .Field("event_type", "delete")
                .Field("primary_key", event.primary_key)
                .Field("doc_id", static_cast<uint64_t>(doc_id))
                .Warn();
          }

          mygram::utils::StructuredLog()
              .Event("binlog_delete")
              .Field("primary_key", event.primary_key)
              .Field("doc_id", static_cast<uint64_t>(doc_id))
              .Debug();
          if (stats != nullptr) {
            stats->IncrementReplDeleteApplied();
          }
          if (cache_manager != nullptr) {
            cache_manager->Invalidate(event.table_name, event.text, "");
          }
        } else {
          // Not in index, nothing to do
          mygram::utils::StructuredLog()
              .Event("binlog_delete")
              .Field("primary_key", event.primary_key)
              .Field("action", "ignored")
              .Debug();
          if (stats != nullptr) {
            stats->IncrementReplDeleteSkipped();
          }
        }
        break;
      }

      case BinlogEventType::DDL: {
        // Handle DDL operations (TRUNCATE, ALTER, DROP)
        std::string query = event.text;
        std::string query_upper = query;
        std::transform(query_upper.begin(), query_upper.end(), query_upper.begin(), ::toupper);

        if (query_upper.find("TRUNCATE") != std::string::npos) {
          // TRUNCATE TABLE - clear all data
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_warning")
              .Field("type", "truncate_table_detected")
              .Field("table_name", event.table_name)
              .Field("query", query)
              .Warn();
          index.Clear();
          doc_store.Clear();
          if (cache_manager != nullptr) {
            cache_manager->ClearTable(event.table_name);
          }
          mygram::utils::StructuredLog().Event("binlog_truncate_applied").Field("table", event.table_name).Info();
        } else if (query_upper.find("ALTER") != std::string::npos) {
          // ALTER TABLE - log warning about potential schema mismatch
          // Check ALTER before DROP to avoid matching "ALTER TABLE ... DROP COLUMN"
          // as a DROP TABLE event
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_warning")
              .Field("type", "alter_table_detected")
              .Field("table_name", event.table_name)
              .Field("query", query)
              .Warn();
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_warning")
              .Field("type", "schema_change_warning")
              .Field("message", "Schema change may cause data inconsistency. Consider rebuilding from snapshot.")
              .Warn();
          // Note: We cannot automatically detect what changed (column type, name, etc.)
          // Users should manually rebuild if text column type or PK changed
        } else if (query_upper.find("DROP") != std::string::npos) {
          // DROP TABLE - clear all data and warn
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "drop_table_detected")
              .Field("table_name", event.table_name)
              .Field("query", query)
              .Error();
          index.Clear();
          doc_store.Clear();
          if (cache_manager != nullptr) {
            cache_manager->ClearTable(event.table_name);
          }
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "table_dropped")
              .Field("message", "Index and document store cleared. Please reconfigure or stop MygramDB.")
              .Error();
        }
        if (stats != nullptr) {
          stats->IncrementReplDdlExecuted();
        }
        break;
      }

      default:
        mygram::utils::StructuredLog()
            .Event("mysql_binlog_warning")
            .Field("type", "unknown_event_type")
            .Field("primary_key", event.primary_key)
            .Warn();
        return false;
    }

    return true;

  } catch (const std::exception& e) {
    mygram::utils::StructuredLog()
        .Event("mysql_binlog_error")
        .Field("type", "event_processing_exception")
        .Field("error", e.what())
        .Error();
    return false;
  }
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
