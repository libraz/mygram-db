/**
 * @file binlog_event_processor.cpp
 * @brief Binlog event processor implementation
 */

#include "mysql/binlog_event_processor.h"

#ifdef USE_MYSQL

#include <spdlog/spdlog.h>

#include <algorithm>

#include "mysql/binlog_filter_evaluator.h"
#include "server/tcp_server.h"  // For ServerStats
#include "utils/string_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::mysql {

bool BinlogEventProcessor::ProcessEvent(const BinlogEvent& event, index::Index& index,
                                        storage::DocumentStore& doc_store, const config::TableConfig& table_config,
                                        const config::MysqlConfig& mysql_config, server::ServerStats* stats) {
  try {
    // Evaluate required_filters to determine if data should exist in index
    bool matches_required =
        BinlogFilterEvaluator::EvaluateRequiredFilters(event.filters, table_config, mysql_config.datetime_timezone);

    // Check if document already exists in index
    auto doc_id_opt = doc_store.GetDocId(event.primary_key);
    bool exists = doc_id_opt.has_value();

    switch (event.type) {
      case BinlogEventType::INSERT: {
        if (matches_required) {
          // Condition satisfied -> add to index
          auto doc_id_result = doc_store.AddDocument(event.primary_key, event.filters);
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

          std::string normalized = utils::NormalizeText(event.text, true, "keep", true);

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

          // Extract text to remove from index
          if (!event.text.empty()) {
            std::string normalized = utils::NormalizeText(event.text, true, "keep", true);
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

          doc_store.RemoveDocument(doc_id);

          mygram::utils::StructuredLog()
              .Event("binlog_update_removed")
              .Field("primary_key", event.primary_key)
              .Field("doc_id", static_cast<uint64_t>(doc_id))
              .Debug();
          if (stats != nullptr) {
            stats->IncrementReplUpdateRemoved();
          }

        } else if (!exists && matches_required) {
          // Transitioned into required conditions -> INSERT into index
          auto doc_id_result = doc_store.AddDocument(event.primary_key, event.filters);
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

          std::string normalized = utils::NormalizeText(event.text, true, "keep", true);

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

        } else if (exists && matches_required) {
          // Still matches conditions -> UPDATE
          storage::DocId doc_id = doc_id_opt.value();

          // Update document store filters (check return value for race condition)
          if (!doc_store.UpdateDocument(doc_id, event.filters)) {
            mygram::utils::StructuredLog()
                .Event("mysql_binlog_warning")
                .Field("type", "update_document_not_found")
                .Field("event_type", "update")
                .Field("primary_key", event.primary_key)
                .Field("doc_id", static_cast<uint64_t>(doc_id))
                .Warn();
            // Continue with index update since the document may have been removed concurrently
          }

          // Update full-text index if text has changed
          bool text_changed = false;
          if (!event.old_text.empty() || !event.text.empty()) {
            try {
              // Use Index::UpdateDocument for atomic update when both texts are available
              if (!event.old_text.empty() && !event.text.empty()) {
                std::string old_normalized = utils::NormalizeText(event.old_text, true, "keep", true);
                std::string new_normalized = utils::NormalizeText(event.text, true, "keep", true);
                index.UpdateDocument(doc_id, old_normalized, new_normalized);
                text_changed = true;
              } else if (!event.old_text.empty()) {
                // Only old text available - remove from index
                std::string old_normalized = utils::NormalizeText(event.old_text, true, "keep", true);
                index.RemoveDocument(doc_id, old_normalized);
                text_changed = true;
              } else if (!event.text.empty()) {
                // Only new text available - add to index
                std::string new_normalized = utils::NormalizeText(event.text, true, "keep", true);
                index.AddDocument(doc_id, new_normalized);
                text_changed = true;
              }
            } catch (const std::exception& e) {
              mygram::utils::StructuredLog()
                  .Event("mysql_binlog_error")
                  .Field("type", "index_update_failed")
                  .Field("event_type", "update")
                  .Field("primary_key", event.primary_key)
                  .Field("doc_id", static_cast<uint64_t>(doc_id))
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
            std::string normalized = utils::NormalizeText(event.text, true, "keep", true);
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
          doc_store.RemoveDocument(doc_id);

          mygram::utils::StructuredLog()
              .Event("binlog_delete")
              .Field("primary_key", event.primary_key)
              .Field("doc_id", static_cast<uint64_t>(doc_id))
              .Debug();
          if (stats != nullptr) {
            stats->IncrementReplDeleteApplied();
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
          mygram::utils::StructuredLog().Event("binlog_truncate_applied").Field("table", event.table_name).Info();
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
          mygram::utils::StructuredLog()
              .Event("mysql_binlog_error")
              .Field("type", "table_dropped")
              .Field("message", "Index and document store cleared. Please reconfigure or stop MygramDB.")
              .Error();
        } else if (query_upper.find("ALTER") != std::string::npos) {
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
          // Note: We cannot automatically detect what changed (column type, name, etc.)
          // Users should manually rebuild if text column type or PK changed
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
