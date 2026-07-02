/**
 * @file binlog_event_processor.cpp
 * @brief Binlog event processor implementation
 */

#include "mysql/binlog_event_processor.h"

#ifdef USE_MYSQL

#include <spdlog/spdlog.h>

#include "cache/cache_manager.h"
#include "mysql/binlog_filter_evaluator.h"
#include "server/server_stats.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::mysql {

// Apply/search consistency contract:
// BinlogEventProcessor mutates DocumentStore and Index as a two-step per-table
// apply. The caller serializes events for a table. Concurrent search paths may
// observe the narrow midpoint between DocumentStore and Index updates; they
// must treat missing store/index material as a transient miss rather than
// undefined state. The next request observes the completed apply. This
// eventual-consistency contract avoids holding a global table lock across every
// search while preserving memory safety under ThreadSanitizer.

namespace {

std::string NormalizeForCacheInvalidation(const std::string& text, index::Index& index) {
  return text.empty() ? std::string{} : index.NormalizeText(text);
}

}  // namespace

bool BinlogEventProcessor::ProcessEvent(const BinlogEvent& event, index::Index& index,
                                        storage::DocumentStore& doc_store, const config::TableConfig& table_config,
                                        const config::MysqlConfig& mysql_config, server::ServerStats* stats,
                                        cache::CacheManager* cache_manager, server::BM25Stats* bm25_stats) {
  if (event.type == BinlogEventType::UPDATE && !event.old_primary_key.empty() &&
      event.old_primary_key != event.primary_key) {
    BinlogEvent delete_event = BinlogEvent::CreateDelete(
        event.table_name, event.old_primary_key, event.old_text.empty() ? event.text : event.old_text, event.gtid);
    delete_event.filters = event.filters;
    BinlogEvent insert_event = BinlogEvent::CreateInsert(event.table_name, event.primary_key, event.text, event.gtid);
    insert_event.filters = event.filters;

    return ProcessEvent(delete_event, index, doc_store, table_config, mysql_config, stats, cache_manager, bm25_stats) &&
           ProcessEvent(insert_event, index, doc_store, table_config, mysql_config, stats, cache_manager, bm25_stats);
  }

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

        index.AddDocument(doc_id, normalized);

        if (bm25_stats != nullptr && !normalized.empty()) {
          bm25_stats->AddDocument(mygram::utils::CountCodePoints(normalized));
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
          cache_manager->Invalidate(event.table_name, "", normalized);
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
          index.RemoveDocument(doc_id, normalized);

          if (bm25_stats != nullptr && !normalized.empty()) {
            bm25_stats->RemoveDocument(mygram::utils::CountCodePoints(normalized));
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
          cache_manager->Invalidate(event.table_name, NormalizeForCacheInvalidation(old_text_for_cache, index), "");
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

        index.AddDocument(doc_id, normalized);

        if (bm25_stats != nullptr && !normalized.empty()) {
          bm25_stats->AddDocument(mygram::utils::CountCodePoints(normalized));
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
          cache_manager->Invalidate(event.table_name, "", normalized);
        }

      } else if (exists && matches_required) {
        // Still matches conditions -> UPDATE
        storage::DocId doc_id = doc_id_opt.value();

        // Save old filters to detect filter changes for cache invalidation
        auto old_doc = doc_store.GetDocument(doc_id);
        storage::FilterMap old_filters;
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
          // Use Index::UpdateDocument for atomic update when both texts are available
          if (!event.old_text.empty() && !event.text.empty()) {
            std::string old_normalized = index.NormalizeText(event.old_text);
            std::string new_normalized = index.NormalizeText(event.text);
            index.UpdateDocument(doc_id, old_normalized, new_normalized);
            doc_store.SetNormalizedText(doc_id, new_normalized);
            if (bm25_stats != nullptr) {
              if (!old_normalized.empty()) {
                bm25_stats->RemoveDocument(mygram::utils::CountCodePoints(old_normalized));
              }
              if (!new_normalized.empty()) {
                bm25_stats->AddDocument(mygram::utils::CountCodePoints(new_normalized));
              }
            }
            text_changed = true;
          } else if (!event.old_text.empty()) {
            // Before-image text is present but the after-image text came back
            // empty for a document that STILL satisfies all required filters.
            // This is ambiguous: under binlog_row_image=FULL the after image
            // always carries the (unchanged) text column, so an empty
            // after-image text here most likely reflects an incomplete/absent
            // after-image value (e.g. a filter-only UPDATE whose text column was
            // not re-materialized) rather than a genuine content clear.
            // Removing the document from the index on this signal alone would
            // silently drop a still-qualifying row from search results (data
            // loss), so the existing index entry and stored text are preserved.
            mygram::utils::StructuredLog()
                .Event("binlog_update")
                .Field("primary_key", event.primary_key)
                .Field("doc_id", static_cast<uint64_t>(doc_id))
                .Field("action", "empty_after_text_index_preserved")
                .Debug();
          } else if (!event.text.empty()) {
            // Only new text available - add to index
            std::string new_normalized = index.NormalizeText(event.text);
            index.AddDocument(doc_id, new_normalized);
            doc_store.SetNormalizedText(doc_id, new_normalized);
            if (bm25_stats != nullptr && !new_normalized.empty()) {
              bm25_stats->AddDocument(mygram::utils::CountCodePoints(new_normalized));
            }
            text_changed = true;
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
          cache_manager->Invalidate(event.table_name, NormalizeForCacheInvalidation(event.old_text, index),
                                    NormalizeForCacheInvalidation(event.text, index), filter_changed);
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
          index.RemoveDocument(doc_id, normalized);

          if (bm25_stats != nullptr && !normalized.empty()) {
            bm25_stats->RemoveDocument(mygram::utils::CountCodePoints(normalized));
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
          cache_manager->Invalidate(event.table_name, NormalizeForCacheInvalidation(event.text, index), "");
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
      // Handle DDL operations using pre-classified DDL type
      const std::string& query = event.text;

      switch (event.ddl_type) {
        case DDLType::kTruncate: {
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
          break;
        }
        case DDLType::kAlter: {
          // ALTER TABLE - log warning about potential schema mismatch
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
          if (cache_manager != nullptr) {
            cache_manager->ClearTable(event.table_name);
          }
          // Note: We cannot automatically detect what changed (column type, name, etc.)
          // Users should manually rebuild if text column type or PK changed
          break;
        }
        case DDLType::kDrop: {
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
          break;
        }
        case DDLType::kRename:
        case DDLType::kUnknown:
          // Log unhandled DDL for visibility
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_warning")
              .Field("type", "unhandled_ddl")
              .Field("table_name", event.table_name)
              .Field("query", query)
              .Warn();
          break;
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
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
