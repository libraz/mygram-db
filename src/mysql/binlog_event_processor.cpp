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

namespace mygramdb::mysql {

bool BinlogEventProcessor::ProcessEvent(const BinlogEvent& event, index::Index& index,
                                        storage::DocumentStore& doc_store, const config::TableConfig& table_config,
                                        server::ServerStats* stats) {
  try {
    // Evaluate required_filters to determine if data should exist in index
    bool matches_required = BinlogFilterEvaluator::EvaluateRequiredFilters(event.filters, table_config);

    // Check if document already exists in index
    auto doc_id_opt = doc_store.GetDocId(event.primary_key);
    bool exists = doc_id_opt.has_value();

    switch (event.type) {
      case BinlogEventType::INSERT: {
        if (matches_required) {
          // Condition satisfied -> add to index
          storage::DocId doc_id = doc_store.AddDocument(event.primary_key, event.filters);

          std::string normalized = utils::NormalizeText(event.text, true, "keep", true);
          index.AddDocument(doc_id, normalized);

          spdlog::debug("INSERT: pk={} (added to index)", event.primary_key);
          if (stats != nullptr) {
            stats->IncrementReplInsertApplied();
          }
        } else {
          // Condition not satisfied -> do not index
          spdlog::debug("INSERT: pk={} (skipped, does not match required_filters)", event.primary_key);
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
            index.RemoveDocument(doc_id, normalized);
          }

          doc_store.RemoveDocument(doc_id);

          spdlog::info("UPDATE: pk={} (removed from index, no longer matches required_filters)", event.primary_key);
          if (stats != nullptr) {
            stats->IncrementReplUpdateRemoved();
          }

        } else if (!exists && matches_required) {
          // Transitioned into required conditions -> INSERT into index
          storage::DocId doc_id = doc_store.AddDocument(event.primary_key, event.filters);

          std::string normalized = utils::NormalizeText(event.text, true, "keep", true);
          index.AddDocument(doc_id, normalized);

          spdlog::info("UPDATE: pk={} (added to index, now matches required_filters)", event.primary_key);
          if (stats != nullptr) {
            stats->IncrementReplUpdateAdded();
          }

        } else if (exists && matches_required) {
          // Still matches conditions -> UPDATE
          storage::DocId doc_id = doc_id_opt.value();

          // Update document store filters
          doc_store.UpdateDocument(doc_id, event.filters);

          // Update full-text index if text has changed
          bool text_changed = false;
          if (!event.old_text.empty() || !event.text.empty()) {
            // Remove old text from index if available
            if (!event.old_text.empty()) {
              std::string old_normalized = utils::NormalizeText(event.old_text, true, "keep", true);
              index.RemoveDocument(doc_id, old_normalized);
              text_changed = true;
            }

            // Add new text to index if available
            if (!event.text.empty()) {
              std::string new_normalized = utils::NormalizeText(event.text, true, "keep", true);
              index.AddDocument(doc_id, new_normalized);
              text_changed = true;
            }
          }

          if (text_changed) {
            spdlog::debug("UPDATE: pk={} (filters and text updated)", event.primary_key);
          } else {
            spdlog::debug("UPDATE: pk={} (filters updated)", event.primary_key);
          }
          if (stats != nullptr) {
            stats->IncrementReplUpdateModified();
          }

        } else {
          // !exists && !matches_required -> do nothing
          spdlog::debug("UPDATE: pk={} (ignored, not in index and does not match required_filters)", event.primary_key);
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
            index.RemoveDocument(doc_id, normalized);
          }

          // Remove from document store
          doc_store.RemoveDocument(doc_id);

          spdlog::debug("DELETE: pk={} (removed from index)", event.primary_key);
          if (stats != nullptr) {
            stats->IncrementReplDeleteApplied();
          }
        } else {
          // Not in index, nothing to do
          spdlog::debug("DELETE: pk={} (not in index, ignored)", event.primary_key);
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
          spdlog::warn("TRUNCATE TABLE detected for table {}: {}", event.table_name, query);
          index.Clear();
          doc_store.Clear();
          spdlog::info("Cleared index and document store due to TRUNCATE");
        } else if (query_upper.find("DROP") != std::string::npos) {
          // DROP TABLE - clear all data and warn
          spdlog::error("DROP TABLE detected for table {}: {}", event.table_name, query);
          index.Clear();
          doc_store.Clear();
          spdlog::error(
              "Table dropped! Index and document store cleared. Please reconfigure or stop "
              "MygramDB.");
        } else if (query_upper.find("ALTER") != std::string::npos) {
          // ALTER TABLE - log warning about potential schema mismatch
          spdlog::warn("ALTER TABLE detected for table {}: {}", event.table_name, query);
          spdlog::warn("Schema change may cause data inconsistency. Consider rebuilding from snapshot.");
          // Note: We cannot automatically detect what changed (column type, name, etc.)
          // Users should manually rebuild if text column type or PK changed
        }
        if (stats != nullptr) {
          stats->IncrementReplDdlExecuted();
        }
        break;
      }

      default:
        spdlog::warn("Unknown event type for pk={}", event.primary_key);
        return false;
    }

    return true;

  } catch (const std::exception& e) {
    spdlog::error("Exception processing event: {}", e.what());
    return false;
  }
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
