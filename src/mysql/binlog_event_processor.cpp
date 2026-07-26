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

bool HasMaterializedAfterText(const BinlogEvent& event) {
  // Non-empty text remains an implicit-present compatibility path for older
  // tests/callers. Parsed FULL row images always set text_state explicitly,
  // which is what distinguishes a real empty value from an absent column.
  return event.text_state != TextValueState::kAbsent || !event.text.empty();
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
    delete_event.text_state = event.old_text_state;
    delete_event.filters = event.filters;
    BinlogEvent insert_event = BinlogEvent::CreateInsert(event.table_name, event.primary_key, event.text, event.gtid);
    insert_event.text_state = event.text_state;
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

        auto doc_id_result = doc_store.AddDocument(event.primary_key, event.filters, normalized, event.text);
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

        // The store is the exact materialized text currently represented by
        // postings; it is safer than inferring availability from empty strings.
        const std::string old_normalized = doc_store.GetNormalizedText(doc_id).value_or(std::string{});
        if (!old_normalized.empty()) {
          index.RemoveDocument(doc_id, old_normalized);

          if (bm25_stats != nullptr) {
            bm25_stats->RemoveDocument(mygram::utils::CountCodePoints(old_normalized));
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
          cache_manager->Invalidate(event.table_name, old_normalized, "");
        }

      } else if (!exists && matches_required) {
        // Transitioned into required conditions -> INSERT into index
        std::string normalized = index.NormalizeText(event.text);

        auto doc_id_result = doc_store.AddDocument(event.primary_key, event.filters, normalized, event.text);
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
        const std::string old_normalized = doc_store.GetNormalizedText(doc_id).value_or(std::string{});
        std::string new_normalized = old_normalized;

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

        bool text_changed = false;
        if (HasMaterializedAfterText(event)) {
          new_normalized = index.NormalizeText(event.text);
          doc_store.SetOriginalText(doc_id, event.text);
          if (old_normalized != new_normalized) {
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
          cache_manager->Invalidate(event.table_name, old_normalized, new_normalized, filter_changed);
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
        const std::string old_normalized = doc_store.GetNormalizedText(doc_id).value_or(std::string{});

        if (!old_normalized.empty()) {
          index.RemoveDocument(doc_id, old_normalized);

          if (bm25_stats != nullptr) {
            bm25_stats->RemoveDocument(mygram::utils::CountCodePoints(old_normalized));
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
          cache_manager->Invalidate(event.table_name, old_normalized, "");
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
          if (bm25_stats != nullptr) {
            bm25_stats->Reset();
          }
          if (cache_manager != nullptr) {
            cache_manager->ClearTable(event.table_name);
          }
          mygram::utils::StructuredLog().Event("binlog_truncate_applied").Field("table", event.table_name).Info();
          break;
        }
        case DDLType::kCreate:
        case DDLType::kAlter: {
          // CREATE/ALTER TABLE has already passed configured-schema validation.
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_warning")
              .Field("type", event.ddl_type == DDLType::kCreate ? "create_table_detected" : "alter_table_detected")
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
          if (bm25_stats != nullptr) {
            bm25_stats->Reset();
          }
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
