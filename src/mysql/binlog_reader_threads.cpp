/**
 * @file binlog_reader_threads.cpp
 * @brief BinlogReader thread functions and queue operations
 *
 * Contains ReaderThreadFunc, WorkerThreadFunc, PushEvent, and PopEvent,
 * extracted from binlog_reader.cpp for translation unit splitting.
 */

#include "mysql/binlog_reader.h"

#ifdef USE_MYSQL

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstring>
#include <utility>

#include "mysql/binlog_event_parser.h"
#include "mysql/binlog_event_processor.h"
#include "mysql/binlog_event_types.h"
#include "mysql/binlog_reader_internal.h"
#include "mysql/binlog_stream.h"
#include "mysql/gtid_encoder.h"
#include "mysql/mariadb_event_parser.h"
#include "server/server_types.h"  // For TableContext definition
#include "utils/constants.h"
#include "utils/crc32.h"
#include "utils/structured_log.h"

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

namespace mygramdb::mysql {

/// Log every Nth no-data occurrence to avoid spam
static constexpr int kLogSampleInterval = 100;

/// Polling interval when no binlog data is available (milliseconds)
static constexpr int kNoDataPollIntervalMs = 10;

bool IsRowsEventType(MySQLBinlogEventType event_type) {
  switch (event_type) {
    case MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1:
    case MySQLBinlogEventType::OBSOLETE_UPDATE_ROWS_EVENT_V1:
    case MySQLBinlogEventType::OBSOLETE_DELETE_ROWS_EVENT_V1:
    case MySQLBinlogEventType::WRITE_ROWS_EVENT:
    case MySQLBinlogEventType::UPDATE_ROWS_EVENT:
    case MySQLBinlogEventType::DELETE_ROWS_EVENT:
      return true;
    default:
      return false;
  }
}

uint64_t ExtractRowsEventTableId(const unsigned char* buffer) {
  uint64_t table_id = 0;
  const unsigned char* post_header = buffer + mygram::constants::kBinlogEventHeaderLen;
  for (int i = 0; i < 6; ++i) {
    table_id |= static_cast<uint64_t>(post_header[i]) << (i * 8);
  }
  return table_id;
}

void BinlogReader::ReaderThreadFunc() {
  mygram::utils::StructuredLog().Event("binlog_reader_thread_started").Info();

  // Get starting GTID
  std::string gtid_set;
  {
    std::scoped_lock lock(gtid_mutex_);
    if (!current_gtid_.empty()) {
      gtid_set = current_gtid_;
      mygram::utils::StructuredLog().Event("binlog_replication_start").Field("gtid", gtid_set).Info();
    }
  }

  // Local GTID tracking for reader thread only - NOT used for reconnection.
  // Only the worker thread updates current_gtid_ (via UpdateCurrentGTID) to
  // ensure reconnection uses the last *processed* position, not the last *received* position.
  std::string reader_last_gtid;
  {
    std::scoped_lock lock(gtid_mutex_);
    reader_last_gtid = current_gtid_;
  }

  // Main reconnection loop (infinite retries)
  int reconnect_attempt = 0;
  bool idle_timeout_reconnect = false;  // Track if reconnecting due to idle timeout

  // Helper lambda: sleep with backoff, check should_stop_, reconnect, validate.
  // Returns: 1 = success, 0 = reconnect failed (retry), -1 = should stop
  auto reconnect_with_backoff = [this, &reconnect_attempt](const std::string& reason, bool silent) -> int {
    reconnect_attempt = std::min(reconnect_attempt + 1, 10);
    int delay_ms = config_.reconnect_delay_ms * reconnect_attempt;
    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "retry_connection")
        .Field("reason", reason)
        .Field("delay_ms", static_cast<int64_t>(delay_ms))
        .Field("attempt", static_cast<int64_t>(reconnect_attempt))
        .Debug();
    std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));

    if (should_stop_) {
      mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "stop_requested_during_retry").Debug();
      return -1;
    }

    auto result = binlog_connection_->Reconnect(silent);
    if (!result) {
      mygram::utils::LogBinlogError("reconnect_failed", GetCurrentGTID(), result.error().message(), reconnect_attempt);
      return 0;  // Retry in next iteration
    }

    // Validate connection after reconnection (detect failover, invalid servers)
    if (!ValidateConnection()) {
      mygram::utils::StructuredLog()
          .Event("binlog_error")
          .Field("type", "connection_validation_failed")
          .Field("context", "after_reconnect")
          .Field("error", GetLastError())
          .Error();
      return -1;  // Stop replication
    }

    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "connection_validated_after_reconnect")
        .Debug();
    reconnect_attempt = 0;
    return 1;  // Success
  };

  while (!should_stop_) {
    // Setup session (CRC32 checksum, heartbeat, etc.) via protocol-specific stream
    auto setup_result = binlog_stream_->SetupSession(*binlog_connection_);
    if (!setup_result) {
      SetLastError(setup_result.error().message());

      // On first attempt (reconnect_attempt == 0), this is likely a read timeout recovery
      if (reconnect_attempt == 0) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "session_setup_failed_idle_reconnect")
            .Field("error", GetLastError())
            .Debug();
      } else {
        mygram::utils::LogBinlogError("session_setup_failed", GetCurrentGTID(), GetLastError(), reconnect_attempt);
      }

      bool silent = (reconnect_attempt == 0);
      int rc = reconnect_with_backoff("session_setup_failed", silent);
      if (rc == -1) {
        should_stop_.store(true, std::memory_order_release);
        break;
      }
      continue;
    }

    // Always use current_gtid_ (last processed by worker thread) as the
    // authoritative source for reconnection. Never use executed_gtid_set_
    // (from @@GLOBAL.gtid_executed) which may include events committed on
    // the server but not yet delivered to MygramDB, causing data loss.
    std::string current_gtid;
    {
      std::scoped_lock lock(gtid_mutex_);
      current_gtid = ConvertSingleGtidToRange(current_gtid_);
    }

    // Open binlog stream via protocol-specific implementation
    auto open_result = binlog_stream_->Open(*binlog_connection_, current_gtid, config_.server_id);
    if (!open_result) {
      SetLastError(open_result.error().message());

      // Check if binlog purged (non-recoverable)
      if (open_result.error().message().find("purged") != std::string::npos) {
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "binlog_purged")
            .Field("gtid", GetCurrentGTID())
            .Field("error", GetLastError())
            .Error();
        should_stop_.store(true, std::memory_order_release);
        break;
      }

      mygram::utils::LogBinlogError("stream_open_failed", GetCurrentGTID(), GetLastError(), reconnect_attempt + 1);
      int rc = reconnect_with_backoff("stream_open_failed", false);
      if (rc == -1) {
        should_stop_.store(true, std::memory_order_release);
        break;
      }
      continue;
    }

    // Reset no-data counter on successful stream open (reconnection)
    no_data_log_count_.store(0);

    // Log stream opened - use Debug level for idle timeout reconnects to avoid noisy logs
    if (idle_timeout_reconnect) {
      mygram::utils::StructuredLog().Event("binlog_stream_opened").Field("gtid", GetCurrentGTID()).Debug();
      idle_timeout_reconnect = false;
    } else {
      mygram::utils::StructuredLog().Event("binlog_stream_opened").Field("gtid", GetCurrentGTID()).Info();
    }
    reconnect_attempt = 0;

    // Read binlog events
    int event_count = 0;
    bool connection_lost = false;

    while (!should_stop_ && !connection_lost) {
      if (processing_failure_reconnect_requested_.exchange(false, std::memory_order_acq_rel)) {
        mygram::utils::StructuredLog()
            .Event("binlog_processing_failure_reconnect")
            .Field("gtid", GetCurrentGTID())
            .Warn();
        connection_lost = true;
        binlog_stream_->Close(*binlog_connection_);
        break;
      }

      auto fetch = binlog_stream_->Fetch(*binlog_connection_);

      // Log first fetch result for debugging
      if (event_count == 0) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "first_binlog_fetch_result")
            .Field("status", static_cast<int64_t>(fetch.status))
            .Field("length", static_cast<uint64_t>(fetch.event_length))
            .Debug();
      }

      // Check should_stop_ immediately after blocking call to avoid use-after-free
      if (should_stop_) {
        mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "stop_requested_exiting").Debug();
        break;
      }
      if (processing_failure_reconnect_requested_.load(std::memory_order_acquire)) {
        connection_lost = true;
        binlog_stream_->Close(*binlog_connection_);
        break;
      }

      switch (fetch.status) {
        case BinlogFetchResult::Status::kNoData: {
          int current_no_data = no_data_log_count_.fetch_add(1) + 1;
          if (current_no_data % kLogSampleInterval == 1) {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "no_data_received")
                .Field("count", static_cast<int64_t>(current_no_data))
                .Debug();
          }
          std::this_thread::sleep_for(std::chrono::milliseconds(kNoDataPollIntervalMs));
          continue;
        }

        case BinlogFetchResult::Status::kConnectionLost: {
          SetLastError(fetch.error_message);
          // Read timeout - full reconnect (do NOT close stream, handle may be invalid)
          mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "read_timeout_full_reconnect").Debug();
          auto reconnect_result = binlog_connection_->Reconnect(true /* silent */);
          if (!reconnect_result) {
            mygram::utils::LogBinlogError("reconnect_failed", GetCurrentGTID(), reconnect_result.error().message(), 1);
            std::this_thread::sleep_for(std::chrono::milliseconds(config_.reconnect_delay_ms));
          } else if (!ValidateConnection()) {
            mygram::utils::StructuredLog()
                .Event("binlog_error")
                .Field("type", "connection_validation_failed")
                .Field("context", "after_idle_timeout_reconnect")
                .Field("error", GetLastError())
                .Error();
            should_stop_.store(true, std::memory_order_release);
          } else {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "connection_validated_after_idle_timeout_reconnect")
                .Debug();
          }
          idle_timeout_reconnect = true;
          // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - Documents intent; break exits to outer loop
          connection_lost = true;
          break;
        }

        case BinlogFetchResult::Status::kServerGoneAway: {
          SetLastError(fetch.error_message);
          mygram::utils::StructuredLog()
              .Event("binlog_connection_lost")
              .Field("error", GetLastError())
              .Field("gtid", GetCurrentGTID())
              .Warn();
          // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - Documents intent; break exits to outer loop
          connection_lost = true;
          binlog_stream_->Close(*binlog_connection_);
          int rc = reconnect_with_backoff("server_gone_away", false);
          if (rc == -1) {
            should_stop_.store(true, std::memory_order_release);
          } else if (rc == 1) {
            mygram::utils::StructuredLog().Event("binlog_connection_restored").Info();
          }
          break;
        }

        case BinlogFetchResult::Status::kBinlogPurged: {
          SetLastError(fetch.error_message);
          mygram::utils::StructuredLog()
              .Event("binlog_error")
              .Field("type", "binlog_purged")
              .Field("gtid", GetCurrentGTID())
              .Field("error", GetLastError())
              .Field("message",
                     "Binlog position no longer available on server. "
                     "Manual intervention required: run SYNC command to establish new position.")
              .Error();
          should_stop_.store(true, std::memory_order_release);
          break;
        }

        case BinlogFetchResult::Status::kError: {
          SetLastError(fetch.error_message);
          mygram::utils::StructuredLog()
              .Event("binlog_error")
              .Field("type", "fetch_non_recoverable_error")
              .Field("gtid", GetCurrentGTID())
              .Field("error", GetLastError())
              .Field("errno", static_cast<int64_t>(fetch.error_code))
              .Error();
          should_stop_.store(true, std::memory_order_release);
          break;
        }

        case BinlogFetchResult::Status::kOK:
          break;  // Process event below
      }

      // If not kOK, the switch already handled it
      if (fetch.status != BinlogFetchResult::Status::kOK) {
        break;  // Exit inner loop (reconnect or stop)
      }

      const unsigned char* event_buffer = fetch.event_data;
      const size_t event_length = fetch.event_length;

      event_count++;
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "received_binlog_event")
          .Field("event_num", static_cast<int64_t>(event_count))
          .Field("size", static_cast<uint64_t>(event_length))
          .Debug();

      // Verify CRC32 checksum for data integrity.
      if (event_length >= mygram::constants::kBinlogEventHeaderLen + mygram::constants::kBinlogChecksumSize) {
        const size_t data_length = event_length - mygram::constants::kBinlogChecksumSize;
        uint32_t computed_crc = mygram::utils::ComputeCRC32(event_buffer, data_length);
        uint32_t stored_crc = 0;
        std::memcpy(&stored_crc, event_buffer + data_length, sizeof(stored_crc));
        if (computed_crc != stored_crc) {
          crc_errors_++;
          mygram::utils::StructuredLog()
              .Event("binlog_error")
              .Field("type", "crc32_checksum_mismatch")
              .Field("computed_crc", static_cast<uint64_t>(computed_crc))
              .Field("stored_crc", static_cast<uint64_t>(stored_crc))
              .Field("event_length", static_cast<uint64_t>(event_length))
              .Field("gtid", reader_last_gtid)
              .Error();
          continue;
        }
      }

      MySQLBinlogEventType event_type = MySQLBinlogEventType::UNKNOWN_EVENT;

      // Check for GTID events first (need to update current_gtid)
      if (event_length >= mygram::constants::kBinlogEventHeaderLen) {
        event_type = static_cast<MySQLBinlogEventType>(event_buffer[4]);

        if (event_type == MySQLBinlogEventType::GTID_LOG_EVENT) {
          auto gtid_opt = BinlogEventParser::ExtractGTID(event_buffer, event_length);
          if (gtid_opt) {
            reader_last_gtid = gtid_opt.value();
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "reader_gtid_received")
                .Field("gtid", reader_last_gtid)
                .Debug();
          }
          continue;
        }

        if (event_type == MySQLBinlogEventType::GTID_TAGGED_LOG_EVENT) {
          auto gtid_opt = BinlogEventParser::ExtractTaggedGTID(event_buffer, event_length);
          if (gtid_opt) {
            reader_last_gtid = gtid_opt.value();
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "reader_tagged_gtid_received")
                .Field("gtid", reader_last_gtid)
                .Debug();
          } else {
            mygram::utils::StructuredLog()
                .Event("binlog_warning")
                .Field("type", "gtid_tagged_parse_fallback")
                .Field("message", "Failed to parse GTID_TAGGED_LOG_EVENT, keeping previous GTID")
                .Warn();
          }
          continue;
        }

        // MariaDB GTID event (type 162): extract domain-server-seq GTID
        if (event_type == MySQLBinlogEventType::MARIADB_GTID_EVENT) {
          auto gtid_opt = MariaDBEventParser::ExtractGTID(event_buffer, event_length);
          if (gtid_opt) {
            reader_last_gtid = gtid_opt.value();
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "reader_mariadb_gtid_received")
                .Field("gtid", reader_last_gtid)
                .Debug();
          }
          continue;
        }

        // Skip other MariaDB-specific events that don't need processing
        if (event_type == MySQLBinlogEventType::MARIADB_GTID_LIST_EVENT ||
            event_type == MySQLBinlogEventType::MARIADB_ANNOTATE_ROWS_EVENT ||
            event_type == MySQLBinlogEventType::MARIADB_BINLOG_CHECKPOINT_EVENT ||
            event_type == MySQLBinlogEventType::MARIADB_START_ENCRYPTION_EVENT) {
          continue;
        }

        if (event_type == MySQLBinlogEventType::TABLE_MAP_EVENT) {
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "table_map_event_detected")
              .Field("event_num", static_cast<int64_t>(event_count))
              .Debug();
          auto metadata_opt = BinlogEventParser::ParseTableMapEvent(event_buffer, event_length);
          if (!metadata_opt) {
            mygram::utils::StructuredLog()
                .Event("binlog_error")
                .Field("type", "table_map_parse_failed")
                .Field("event_num", static_cast<int64_t>(event_count))
                .Error();
          } else {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "table_map_parsed")
                .Field("database", metadata_opt->database_name)
                .Field("table", metadata_opt->table_name)
                .Field("table_id", metadata_opt->table_id)
                .Debug();

            // Table contexts are keyed by the database-qualified identity
            // (e.g. "testdb.articles"). Match on the qualified key first and
            // fall back to the bare table name for empty-database configs.
            const bool is_monitored_table =
                table_contexts_.find(config::QualifiedTableName(metadata_opt->database_name,
                                                                metadata_opt->table_name)) != table_contexts_.end() ||
                table_contexts_.find(metadata_opt->table_name) != table_contexts_.end();

            if (is_monitored_table) {
              if (!FetchColumnNames(metadata_opt.value())) {
                mygram::utils::StructuredLog()
                    .Event("binlog_warning")
                    .Field("type", "column_fetch_failed")
                    .Field("database", metadata_opt->database_name)
                    .Field("table", metadata_opt->table_name)
                    .Field("gtid", GetCurrentGTID())
                    .Message("Failed to fetch column names, using col_N placeholders")
                    .Warn();
              }
            } else {
              mygram::utils::StructuredLog()
                  .Event("binlog_debug")
                  .Field("action", "skipping_non_monitored_table")
                  .Field("database", metadata_opt->database_name)
                  .Field("table", metadata_opt->table_name)
                  .Debug();
            }

            auto add_result = table_metadata_cache_.AddOrUpdate(metadata_opt->table_id, metadata_opt.value());

            if (add_result == TableMetadataCache::AddResult::kSchemaChanged) {
              {
                std::lock_guard<std::mutex> lock(column_names_cache_mutex_);
                std::string cache_key = metadata_opt->database_name + "." + metadata_opt->table_name;
                column_names_cache_.erase(cache_key);
              }
              if (is_monitored_table) {
                FetchColumnNames(metadata_opt.value());
              }
              mygram::utils::StructuredLog()
                  .Event("binlog_schema_change")
                  .Field("database", metadata_opt->database_name)
                  .Field("table", metadata_opt->table_name)
                  .Field("table_id", metadata_opt->table_id)
                  .Field("gtid", GetCurrentGTID())
                  .Message("Schema change detected, column name cache invalidated")
                  .Warn();
            }

            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action",
                       add_result == TableMetadataCache::AddResult::kAdded ? "cached_table_map" : "updated_table_map")
                .Field("database", metadata_opt->database_name)
                .Field("table", metadata_opt->table_name)
                .Field("table_id", metadata_opt->table_id)
                .Debug();
          }
          continue;
        }

        // Detect unsupported event types that cause silent data loss at runtime
        if (event_type == MySQLBinlogEventType::TRANSACTION_PAYLOAD_EVENT) {
          SetLastError(
              "Received TRANSACTION_PAYLOAD_EVENT: binlog_transaction_compression was enabled "
              "on the server after initial validation. Compressed events cannot be decoded. "
              "Disable compression with: SET GLOBAL binlog_transaction_compression=OFF");
          mygram::utils::StructuredLog()
              .Event("binlog_fatal_error")
              .Field("type", "unsupported_runtime_event")
              .Field("event_type", "TRANSACTION_PAYLOAD_EVENT")
              .Error();
          should_stop_.store(true, std::memory_order_release);
          break;
        }

        if (event_type == MySQLBinlogEventType::PARTIAL_UPDATE_ROWS_EVENT) {
          SetLastError(
              "Received PARTIAL_UPDATE_ROWS_EVENT: binlog_row_value_options=PARTIAL_JSON was enabled "
              "on the server after initial validation. Partial JSON updates cannot be decoded. "
              "Disable with: SET GLOBAL binlog_row_value_options=''");
          mygram::utils::StructuredLog()
              .Event("binlog_fatal_error")
              .Field("type", "unsupported_runtime_event")
              .Field("event_type", "PARTIAL_UPDATE_ROWS_EVENT")
              .Error();
          should_stop_.store(true, std::memory_order_release);
          break;
        }
      }

      // Parse the binlog event using BinlogEventParser
      auto events =
          BinlogEventParser::ParseBinlogEvent(event_buffer, event_length, reader_last_gtid, table_metadata_cache_,
                                              table_contexts_, nullptr, true, mysql_config_.datetime_timezone);

      if (!events.empty()) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "parsed_events")
            .Field("count", static_cast<int64_t>(events.size()))
            .Debug();

        for (auto& event : events) {
          if (spdlog::should_log(spdlog::level::debug)) {
            if (event.type == BinlogEventType::INSERT || event.type == BinlogEventType::UPDATE ||
                event.type == BinlogEventType::DELETE) {
              const char* event_type_str = "UNKNOWN";
              if (event.type == BinlogEventType::INSERT) {
                event_type_str = "INSERT";
              } else if (event.type == BinlogEventType::UPDATE) {
                event_type_str = "UPDATE";
              } else if (event.type == BinlogEventType::DELETE) {
                event_type_str = "DELETE";
              }
              mygram::utils::StructuredLog()
                  .Event("binlog_debug")
                  .Field("action", "binlog_data_event")
                  .Field("event_type", event_type_str)
                  .Field("table", event.table_name)
                  .Field("pk", event.primary_key)
                  .Debug();
            }
          }

          PushEvent(std::make_unique<BinlogEvent>(std::move(event)));
        }
      } else if (IsMonitoredRowsEventParseFailure(event_type, event_buffer, event_length)) {
        SetLastError("Failed to parse monitored ROWS_EVENT from binlog; reconnecting from last processed GTID");
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "rows_event_parse_failed")
            .Field("event_type", GetEventTypeName(event_type))
            .Field("reader_gtid", reader_last_gtid)
            .Field("current_gtid", GetCurrentGTID())
            .Error();
        processing_failure_reconnect_requested_.store(true, std::memory_order_release);
        connection_lost = true;
        binlog_stream_->Close(*binlog_connection_);
        break;
      } else {
        mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "event_skipped").Debug();
      }
    }

    // Close binlog stream
    if (binlog_connection_ && binlog_connection_->IsConnected()) {
      binlog_stream_->Close(*binlog_connection_);
    }

    if (should_stop_) {
      break;
    }
  }

  // Wake the worker thread in case it is blocked in PopEvent() waiting on
  // queue_cv_. Without this notification the worker would hang indefinitely
  // when the reader exits due to a fatal error (should_stop_ = true) because
  // no more events will ever be pushed and the predicate would never flip.
  queue_cv_.notify_all();

  mygram::utils::StructuredLog().Event("binlog_reader_thread_stopped").Info();
}

void BinlogReader::WorkerThreadFunc() {
  mygram::utils::StructuredLog().Event("binlog_worker_thread_started").Info();

  // Process events until PopEvent returns nullptr
  // PopEvent returns nullptr only when: should_stop_ is true AND queue is empty
  // This ensures all pending events are processed during graceful shutdown
  while (true) {
    auto event = PopEvent();
    if (!event) {
      // Exit only when shutdown requested AND queue is empty
      break;
    }

    if (!ProcessQueuedEvent(*event)) {
      mygram::utils::StructuredLog()
          .Event("binlog_error")
          .Field("type", "event_processing_failed")
          .Field("table", event->table_name)
          .Field("primary_key", event->primary_key)
          .Field("gtid", event->gtid)
          .Error();
      {
        std::scoped_lock lock(queue_mutex_);
        while (!event_queue_.empty()) {
          event_queue_.pop();
        }
      }
      queue_full_cv_.notify_all();
      processing_failure_reconnect_requested_.store(true, std::memory_order_release);
      // GTID is not updated on failure. The reader is forced to reconnect
      // from the last processed GTID, and queued later events are discarded
      // so they cannot permanently skip the failed event.
    }
  }

  mygram::utils::StructuredLog().Event("binlog_worker_thread_stopped").Info();
}

bool BinlogReader::ProcessQueuedEvent(const BinlogEvent& event) {
  if (event.type == BinlogEventType::COMMIT) {
    std::string commit_gtid = event.gtid.empty() ? pending_commit_gtid_ : event.gtid;
    if (!commit_gtid.empty()) {
      UpdateCurrentGTID(commit_gtid);
      pending_commit_gtid_.clear();
    }
    return true;
  }

  if (!ProcessEvent(event)) {
    return false;
  }

  processed_events_++;

  if (event.type == BinlogEventType::DDL) {
    if (!event.gtid.empty()) {
      UpdateCurrentGTID(event.gtid);
    }
    return true;
  }

  if (!event.gtid.empty()) {
    pending_commit_gtid_ = event.gtid;
  }
  return true;
}

bool BinlogReader::IsMonitoredRowsEventParseFailure(MySQLBinlogEventType event_type, const unsigned char* buffer,
                                                    unsigned long length) const {
  if (!IsRowsEventType(event_type)) {
    return false;
  }
  if (buffer == nullptr || length < mygram::constants::kBinlogEventHeaderLen + 6) {
    return true;
  }

  const uint64_t table_id = ExtractRowsEventTableId(buffer);
  const TableMetadata* metadata = table_metadata_cache_.Get(table_id);
  if (metadata == nullptr) {
    return false;
  }
  // Table contexts are keyed by the database-qualified identity
  // (e.g. "testdb.articles"). Match on the qualified key first and fall back to
  // the bare table name for empty-database configs.
  auto table_iter = table_contexts_.find(config::QualifiedTableName(metadata->database_name, metadata->table_name));
  if (table_iter == table_contexts_.end()) {
    table_iter = table_contexts_.find(metadata->table_name);
  }
  if (table_iter == table_contexts_.end() || table_iter->second == nullptr) {
    return false;
  }
  const config::TableConfig& table_config = table_iter->second->config;
  return table_config.database.empty() || metadata->database_name == table_config.database;
}

void BinlogReader::PushEvent(std::unique_ptr<BinlogEvent> event) {
  std::unique_lock<std::mutex> lock(queue_mutex_);

  // Wait if queue is full
  queue_full_cv_.wait(lock, [this] { return should_stop_ || event_queue_.size() < config_.queue_size; });

  if (should_stop_ || processing_failure_reconnect_requested_.load(std::memory_order_acquire)) {
    return;
  }

  event_queue_.push(std::move(event));
  queue_cv_.notify_one();
}

std::unique_ptr<BinlogEvent> BinlogReader::PopEvent() {
  std::unique_lock<std::mutex> lock(queue_mutex_);

  // Wait if queue is empty
  queue_cv_.wait(lock, [this] { return should_stop_ || !event_queue_.empty(); });

  if (should_stop_ && event_queue_.empty()) {
    return nullptr;
  }

  auto event = std::move(event_queue_.front());
  event_queue_.pop();

  // Notify reader thread that queue has space
  queue_full_cv_.notify_one();

  return event;
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

#endif  // USE_MYSQL
