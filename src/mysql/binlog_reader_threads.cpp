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
#include "mysql/gtid_encoder.h"
#include "server/tcp_server.h"  // For TableContext definition
#include "utils/constants.h"
#include "utils/structured_log.h"

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

namespace mygramdb::mysql {

/// Heartbeat period in nanoseconds (3 seconds) to keep binlog connection alive
static constexpr uint64_t kHeartbeatPeriodNs = 3000000000;

/// Log every Nth no-data occurrence to avoid spam
static constexpr int kLogSampleInterval = 100;

/// Size of the OK byte prefix in MySQL binlog protocol buffer
static constexpr size_t kBinlogOKByteSize = 1;

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
    // Disable binlog checksums for this connection
    // We don't verify checksums yet, so ask the server to send events without them
    if (mysql_query(binlog_connection_->GetHandle(), "SET @source_binlog_checksum='NONE'") != 0) {
      SetLastError("Failed to disable binlog checksum: " + binlog_connection_->GetLastError());

      // On first attempt (reconnect_attempt == 0), this is likely a read timeout recovery
      // Use debug logging to avoid noisy error logs during normal idle reconnects
      if (reconnect_attempt == 0) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "checksum_query_failed_idle_reconnect")
            .Field("error", GetLastError())
            .Debug();
      } else {
        mygram::utils::LogBinlogError("checksum_disable_failed", GetCurrentGTID(), GetLastError(), reconnect_attempt);
      }

      // Retry connection after delay with exponential backoff
      // Use silent mode on first attempt since this is likely a normal idle timeout recovery
      bool silent = (reconnect_attempt == 0);
      int rc = reconnect_with_backoff("checksum_disable_failed", silent);
      if (rc == -1) {
        should_stop_ = true;
        break;
      }
      // rc == 0 (reconnect failed) or rc == 1 (success): retry from top of loop
      continue;
    }
    mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "checksums_disabled").Debug();

    // Configure heartbeat to keep connection alive during idle periods.
    // The heartbeat period must be shorter than read_timeout (5s) to avoid
    // spurious TCP disconnects. Without heartbeat, each read_timeout expiry
    // causes a full reconnect cycle.
    std::string heartbeat_query = "SET @master_heartbeat_period = " + std::to_string(kHeartbeatPeriodNs);
    if (mysql_query(binlog_connection_->GetHandle(), heartbeat_query.c_str()) != 0) {
      // Non-fatal: heartbeat is optional, log and continue
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "heartbeat_config_failed")
          .Field("error", binlog_connection_->GetLastError())
          .Debug();
    }

    // Initialize MYSQL_RPL structure for binlog reading
    MYSQL_RPL rpl{};
    rpl.file_name_length = 0;  // 0 means start from current position
    rpl.file_name = nullptr;
    rpl.start_position = 4;             // Skip magic number at start of binlog
    rpl.server_id = config_.server_id;  // Use configured server ID for replica
    rpl.flags = MYSQL_RPL_GTID;         // Use GTID mode (allow heartbeat events)

    // Always use current_gtid_ (last processed by worker thread) as the
    // authoritative source for reconnection. Never use executed_gtid_set_
    // (from @@GLOBAL.gtid_executed) which may include events committed on
    // the server but not yet delivered to MygramDB, causing data loss.
    std::string current_gtid;
    {
      std::scoped_lock lock(gtid_mutex_);
      current_gtid = ConvertSingleGtidToRange(current_gtid_);
    }

    // Encode GTID set to binary format if we have one
    // Use local variable to avoid race condition - gtid_encoded_data_ must persist
    // during mysql_binlog_open() call which accesses it via callback
    std::vector<uint8_t> local_gtid_encoded;
    if (!current_gtid.empty()) {
      // Encode GTID set using our encoder
      auto encode_result = mygramdb::mysql::GtidEncoder::Encode(current_gtid);
      if (!encode_result) {
        SetLastError("Failed to encode GTID set: " + encode_result.error().message());
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "gtid_encode_failed")
            .Field("gtid", current_gtid)
            .Field("error", GetLastError())
            .Error();
        should_stop_ = true;
        break;
      }
      local_gtid_encoded = std::move(*encode_result);

      // Also update member variable under lock for consistency with other readers
      {
        std::lock_guard<std::mutex> lock(gtid_mutex_);
        gtid_encoded_data_ = local_gtid_encoded;
      }

      // Use callback approach: MySQL will call our callback to encode the GTID into the packet
      rpl.gtid_set_encoded_size = local_gtid_encoded.size();
      rpl.gtid_set_arg = &local_gtid_encoded;                // Pass pointer to local data
      rpl.fix_gtid_set = &BinlogReader::FixGtidSetCallback;  // Static callback function

      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "using_gtid_set")
          .Field("gtid", current_gtid)
          .Field("encoded_bytes", static_cast<uint64_t>(local_gtid_encoded.size()))
          .Debug();
    } else {
      // Empty GTID set: receive all events from current binlog position
      rpl.gtid_set_encoded_size = 0;
      rpl.gtid_set_arg = nullptr;
      rpl.fix_gtid_set = nullptr;
      mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "using_empty_gtid_set").Debug();
    }

    // Open binlog stream (local_gtid_encoded must be alive during this call)
    if (mysql_binlog_open(binlog_connection_->GetHandle(), &rpl) != 0) {
      unsigned int err_no = mysql_errno(binlog_connection_->GetHandle());
      SetLastError("Failed to open binlog stream: " + binlog_connection_->GetLastError());

      // Check for binlog purged error (errno 1236) - non-recoverable, stop immediately
      if (err_no == kMySQLErrBinlogPurged) {
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "binlog_purged")
            .Field("gtid", GetCurrentGTID())
            .Field("error", GetLastError())
            .Field("errno", static_cast<int64_t>(err_no))
            .Field("message",
                   "Binlog position no longer available on server. "
                   "GTID position has been purged. "
                   "Manual intervention required: run SYNC command to establish new position.")
            .Error();
        should_stop_ = true;
        break;
      }

      // Other errors - retry with reconnection and exponential backoff
      mygram::utils::LogBinlogError("stream_open_failed", GetCurrentGTID(), GetLastError(), reconnect_attempt + 1);
      int rc = reconnect_with_backoff("stream_open_failed", false /* not silent */);
      if (rc == -1) {
        should_stop_ = true;
        break;
      }
      // rc == 0 (reconnect failed) or rc == 1 (success): retry from top of loop
      continue;
    }

    // Log stream opened - use Debug level for idle timeout reconnects to avoid noisy logs
    if (idle_timeout_reconnect) {
      mygram::utils::StructuredLog().Event("binlog_stream_opened").Field("gtid", GetCurrentGTID()).Debug();
      idle_timeout_reconnect = false;  // Reset the flag
    } else {
      mygram::utils::StructuredLog().Event("binlog_stream_opened").Field("gtid", GetCurrentGTID()).Info();
    }
    reconnect_attempt = 0;  // Reset reconnect counter on success

    // Read binlog events
    int event_count = 0;
    bool connection_lost = false;

    while (!should_stop_ && !connection_lost) {
      // Fetch next binlog event
      mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "calling_binlog_fetch").Debug();
      int result = mysql_binlog_fetch(binlog_connection_->GetHandle(), &rpl);

      // Log first fetch result for debugging
      if (event_count == 0) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "first_binlog_fetch_result")
            .Field("result", static_cast<int64_t>(result))
            .Field("size", static_cast<uint64_t>(rpl.size))
            .Debug();
      }

      // Check should_stop_ immediately after blocking call to avoid use-after-free
      // (Stop() may have closed the connection while we were blocked)
      if (should_stop_) {
        mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "stop_requested_exiting").Debug();
        break;
      }

      if (result != 0) {
        // Check stop request FIRST before accessing potentially-closed handle
        if (should_stop_) {
          mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "read_timeout_stop_requested").Debug();
          break;
        }

        unsigned int err_no = mysql_errno(binlog_connection_->GetHandle());
        const char* err_str = mysql_error(binlog_connection_->GetHandle());
        SetLastError("Failed to fetch binlog event: " + std::string(err_str) + " (errno: " + std::to_string(err_no) +
                     ")");

        // Check if this is likely a read timeout (errno 2013 during idle)
        // With heartbeat configured, timeouts indicate actual connection issues.
        // The TCP connection is broken by the timeout, so we need a full reconnect
        if (err_no == kMySQLErrServerLost) {
          // Full reconnect (closes handle, reinitializes, and connects)
          // This is expected behavior during idle periods, so use silent mode
          // Note: Do NOT call mysql_binlog_close() before Reconnect() because
          // the broken TCP connection can leave the handle in an invalid state
          mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "read_timeout_full_reconnect").Debug();
          auto reconnect_result = binlog_connection_->Reconnect(true /* silent */);
          if (!reconnect_result) {
            // Failed to reconnect - log and retry with backoff
            mygram::utils::LogBinlogError("reconnect_failed", GetCurrentGTID(), reconnect_result.error().message(), 1);
            std::this_thread::sleep_for(std::chrono::milliseconds(config_.reconnect_delay_ms));
          }
          idle_timeout_reconnect = true;  // Mark this as an idle timeout reconnect
          // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - Documents intent; break exits to outer loop
          connection_lost = true;
          break;  // Exit inner loop to retry from outer loop
        }

        // Check if this is a real connection loss (server gone away)
        if (err_no == kMySQLErrGoneAway) {  // Server gone away - actual connection loss
          mygram::utils::StructuredLog()
              .Event("binlog_connection_lost")
              .Field("error", GetLastError())
              .Field("fetch_result", static_cast<int64_t>(result))
              .Field("gtid", GetCurrentGTID())
              .Warn();
          // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - Documents intent; break exits to outer loop
          connection_lost = true;

          // Close current binlog stream before reconnecting
          mysql_binlog_close(binlog_connection_->GetHandle(), &rpl);

          int rc = reconnect_with_backoff("server_gone_away", false /* not silent */);
          if (rc == -1) {
            should_stop_ = true;
          } else if (rc == 1) {
            mygram::utils::StructuredLog().Event("binlog_connection_restored").Info();
          }
          break;  // Exit inner loop to retry from outer loop
        }

        // Check for binlog purged error (errno 1236) - special handling required
        // This error means the GTID position we're requesting is no longer available in binlogs
        // Retrying with the same GTID will never succeed - must stop and wait for manual intervention (SYNC)
        if (err_no == kMySQLErrBinlogPurged) {
          mygram::utils::StructuredLog()
              .Event("binlog_error")
              .Field("type", "binlog_purged")
              .Field("gtid", GetCurrentGTID())
              .Field("error", GetLastError())
              .Field("errno", static_cast<int64_t>(err_no))
              .Field("message",
                     "Binlog position no longer available on server. "
                     "GTID position has been purged. "
                     "Manual intervention required: run SYNC command to establish new position.")
              .Error();
          should_stop_ = true;
          break;
        }

        // Other non-recoverable errors - log and stop
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "fetch_non_recoverable_error")
            .Field("gtid", GetCurrentGTID())
            .Field("error", GetLastError())
            .Field("result_code", static_cast<int64_t>(result))
            .Field("errno", static_cast<int64_t>(err_no))
            .Error();
        should_stop_ = true;
        break;
      }

      // Check if we have data
      if (rpl.size == 0 || rpl.buffer == nullptr) {
        // No data available (EOF or keepalive)
        // Log at debug level to diagnose why no events are being received
        int current_no_data = no_data_log_count_.fetch_add(1) + 1;
        if (current_no_data % kLogSampleInterval == 1) {  // Log every Nth occurrence to avoid spam
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "no_data_received")
              .Field("count", static_cast<int64_t>(current_no_data))
              .Field("size", static_cast<uint64_t>(rpl.size))
              .Debug();
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        continue;
      }

      event_count++;
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "received_binlog_event")
          .Field("event_num", static_cast<int64_t>(event_count))
          .Field("size", static_cast<uint64_t>(rpl.size))
          .Debug();

      // MySQL binlog buffer format (from mysql-8.4.7/sql-common/client.cc:mysql_binlog_fetch):
      // rpl.buffer[0] = OK byte (0x00) from network protocol
      // rpl.buffer[1] = Start of actual binlog event data
      // Event type is at rpl.buffer[1 + EVENT_TYPE_OFFSET] where EVENT_TYPE_OFFSET=4
      // (see mysql-8.4.7/libs/mysql/binlog/event/binlog_event.h:431)
      //
      // We skip the OK byte here and pass rpl.buffer+1 to parsers, so parsers can use
      // standard LOG_EVENT_HEADER_LEN (19 bytes) offsets directly.
      const unsigned char* event_buffer = rpl.buffer + kBinlogOKByteSize;
      const unsigned long event_length = rpl.size - kBinlogOKByteSize;

      // Check for GTID events first (need to update current_gtid)
      if (event_length >= mygram::constants::kBinlogEventHeaderLen) {
        auto event_type = static_cast<MySQLBinlogEventType>(event_buffer[4]);

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
          continue;  // Skip to next event
        }

        if (event_type == MySQLBinlogEventType::GTID_TAGGED_LOG_EVENT) {
          // MySQL 8.4+ tagged GTID (UUID:TAG:GNO)
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
          continue;  // Skip to next event
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

            // Check if this table is monitored before fetching column names
            // This avoids permission errors for tables we don't have SELECT access to
            bool is_monitored_table = false;
            if (multi_table_mode_) {
              is_monitored_table = table_contexts_.find(metadata_opt->table_name) != table_contexts_.end();
            } else {
              is_monitored_table = (metadata_opt->table_name == table_config_.name);
            }

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
              // Invalidate column name cache on schema change to avoid stale names
              {
                std::lock_guard<std::mutex> lock(column_names_cache_mutex_);
                std::string cache_key = metadata_opt->database_name + "." + metadata_opt->table_name;
                column_names_cache_.erase(cache_key);
              }
              // Re-fetch column names with fresh data
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
          continue;  // Skip to next event
        }

        // Detect unsupported event types that cause silent data loss at runtime.
        // These are checked at connection time, but settings can be changed dynamically.
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
          should_stop_ = true;
          break;  // Exit read loop
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
          should_stop_ = true;
          break;  // Exit read loop
        }
      }

      // Parse the binlog event using BinlogEventParser
      // Returns vector of events (empty if none, multiple for batch operations)
      auto events = BinlogEventParser::ParseBinlogEvent(
          event_buffer, event_length, reader_last_gtid, table_metadata_cache_, table_contexts_,
          multi_table_mode_ ? nullptr : &table_config_, multi_table_mode_, mysql_config_.datetime_timezone);

      if (!events.empty()) {
        mygram::utils::StructuredLog()
            .Event("binlog_debug")
            .Field("action", "parsed_events")
            .Field("count", static_cast<int64_t>(events.size()))
            .Debug();

        // Process ALL events in the batch (multi-row support)
        for (auto& event : events) {
          // Log data events at debug level (can be high volume in production)
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
      } else {
        mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "event_skipped").Debug();
      }
    }

    // Close binlog stream if still connected
    if (binlog_connection_ && binlog_connection_->IsConnected()) {
      mysql_binlog_close(binlog_connection_->GetHandle(), &rpl);
    }

    // Only exit on explicit stop request
    if (should_stop_) {
      break;
    }
    // If connection_lost is false (normal idle), continue loop to wait for events
    // If connection_lost is true (connection failure), continue loop for reconnect
  }

  mygram::utils::StructuredLog().Event("binlog_reader_thread_stopped").Info();

  // Note: Do NOT set running_ = false here. The running_ flag is managed
  // exclusively by Stop() to prevent a race where Stop() cannot join threads
  // because it sees running_ == false and takes the early return path (#6).
  // Stop() will always join threads and set running_ = false afterward.
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

    // Only update GTID and counter on successful processing
    if (ProcessEvent(*event)) {
      processed_events_++;
      UpdateCurrentGTID(event->gtid);
    } else {
      mygram::utils::StructuredLog()
          .Event("binlog_error")
          .Field("type", "event_processing_failed")
          .Field("table", event->table_name)
          .Field("primary_key", event->primary_key)
          .Field("gtid", event->gtid)
          .Error();
      // GTID is not updated on failure so event can be retried on reconnect
    }
  }

  mygram::utils::StructuredLog().Event("binlog_worker_thread_stopped").Info();
}

void BinlogReader::PushEvent(std::unique_ptr<BinlogEvent> event) {
  std::unique_lock<std::mutex> lock(queue_mutex_);

  // Wait if queue is full
  queue_full_cv_.wait(lock, [this] { return should_stop_ || event_queue_.size() < config_.queue_size; });

  if (should_stop_) {
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
