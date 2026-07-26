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
#include "mysql/mariadb_gtid.h"
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
    case MySQLBinlogEventType::PARTIAL_UPDATE_ROWS_EVENT:
    case MySQLBinlogEventType::MARIADB_WRITE_ROWS_COMPRESSED_EVENT:
    case MySQLBinlogEventType::MARIADB_UPDATE_ROWS_COMPRESSED_EVENT:
    case MySQLBinlogEventType::MARIADB_DELETE_ROWS_COMPRESSED_EVENT:
    case MySQLBinlogEventType::MARIADB_WRITE_ROWS_COMPRESSED_EVENT_V1:
    case MySQLBinlogEventType::MARIADB_UPDATE_ROWS_COMPRESSED_EVENT_V1:
    case MySQLBinlogEventType::MARIADB_DELETE_ROWS_COMPRESSED_EVENT_V1:
      return true;
    default:
      return false;
  }
}

mygram::utils::Expected<bool, mygram::utils::Error> BinlogReader::ShouldSuppressTableReplay(
    const std::string& table_name, const std::string& event_gtid) const {
  auto table_iter = table_contexts_.find(table_name);
  if (table_iter == table_contexts_.end()) {
    const auto separator = table_name.rfind('.');
    if (separator != std::string::npos) {
      table_iter = table_contexts_.find(table_name.substr(separator + 1));
    }
  }
  if (table_iter == table_contexts_.end() || table_iter->second == nullptr || event_gtid.empty()) {
    return false;
  }

  const auto& watermark_state = table_iter->second->replay_watermark;
  if (watermark_state == nullptr) {
    return false;
  }

  std::string snapshot_gtid;
  {
    std::lock_guard<std::mutex> lock(watermark_state->mutex);
    snapshot_gtid = watermark_state->snapshot_gtid;
  }
  if (snapshot_gtid.empty()) {
    return false;
  }

  return GtidEncoder::PositionCoversAuto(event_gtid, snapshot_gtid);
}

uint64_t ExtractRowsEventTableId(const unsigned char* buffer) {
  uint64_t table_id = 0;
  const unsigned char* post_header = buffer + mygram::constants::kBinlogEventHeaderLen;
  for (int i = 0; i < 6; ++i) {
    table_id |= static_cast<uint64_t>(post_header[i]) << (i * 8);
  }
  return table_id;
}

int64_t BinlogReader::ReconnectBackoffDelayMs(int reconnect_attempt) const {
  const auto bounded_attempt = std::clamp(reconnect_attempt, 1, 10);
  const auto base_delay = std::max(config_.reconnect_delay_ms, 0);
  return static_cast<int64_t>(base_delay) * bounded_attempt;
}

bool BinlogReader::WaitForReconnectBackoff(int reconnect_attempt) {
  const auto delay = std::chrono::milliseconds(ReconnectBackoffDelayMs(reconnect_attempt));
  std::unique_lock<std::mutex> lock(queue_mutex_);
  return !queue_cv_.wait_for(lock, delay, [this] { return should_stop_.load(std::memory_order_acquire); });
}

bool BinlogReader::RejectUnsupportedRuntimeEvent(MySQLBinlogEventType event_type) {
  std::string remediation;
  if (event_type == MySQLBinlogEventType::TRANSACTION_PAYLOAD_EVENT) {
    remediation =
        "Received TRANSACTION_PAYLOAD_EVENT: binlog_transaction_compression was enabled "
        "on the server after initial validation. Compressed events cannot be decoded. "
        "Disable compression with: SET GLOBAL binlog_transaction_compression=OFF";
  } else if (event_type == MySQLBinlogEventType::PARTIAL_UPDATE_ROWS_EVENT) {
    remediation =
        "Received PARTIAL_UPDATE_ROWS_EVENT: binlog_row_value_options=PARTIAL_JSON was enabled "
        "on the server after initial validation. Partial JSON updates cannot be decoded. "
        "Disable with: SET GLOBAL binlog_row_value_options=''";
  } else if (event_type == MySQLBinlogEventType::XA_PREPARE_LOG_EVENT) {
    remediation =
        "Received XA_PREPARE_LOG_EVENT. XA transactions are unsupported because prepared rows cannot be "
        "published before a later XA COMMIT or discarded on XA ROLLBACK.";
  } else if (IsUnsupportedMariaDBCompressedEvent(event_type)) {
    remediation =
        "Received a MariaDB compressed binlog event while log_bin_compress is enabled. "
        "Compressed events cannot be decoded; disable log_bin_compress and run SYNC.";
  } else {
    return false;
  }

  SetLastError(remediation);
  mygram::utils::StructuredLog()
      .Event("binlog_fatal_error")
      .Field("type", "unsupported_runtime_event")
      .Field("event_type", GetEventTypeName(event_type))
      .Error();
  // Fail closed before a following XID can advance current_gtid_.
  should_stop_.store(true, std::memory_order_release);
  return true;
}

void BinlogReader::MarkThreadExited() {
  uint8_t active = active_threads_.load(std::memory_order_acquire);
  while (active != 0 && !active_threads_.compare_exchange_weak(active, static_cast<uint8_t>(active - 1),
                                                               std::memory_order_acq_rel, std::memory_order_acquire)) {
  }
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
  // This is the position attached to the transaction currently being read,
  // not the accumulated applied set used to open the stream. It intentionally
  // starts empty so an event received before a GTID event can never publish
  // the saved multi-source position as if it were one completed transaction.
  position_state_.ResetReceived();
  // QUERY_EVENT is the commit boundary for standalone transactions, while
  // row transactions start with BEGIN and finish at XID/COMMIT. Track that
  // distinction locally so ignored/non-target standalone statements still
  // advance the worker-owned applied GTID without publishing a received-but-
  // unapplied position.
  replay_suppressed_table_ids_.clear();

  // Main reconnection loop (infinite retries)
  int reconnect_attempt = 0;
  bool idle_timeout_reconnect = false;  // Track if reconnecting due to idle timeout
  bool validate_before_stream_reopen = false;
  bool initial_stream_open_pending = true;
  auto publish_initial_stream_state = [this](StreamStartupState state) {
    {
      std::lock_guard<std::mutex> lock(stream_startup_mutex_);
      if (stream_startup_state_ != StreamStartupState::kPending) {
        return;
      }
      stream_startup_state_ = state;
    }
    stream_startup_cv_.notify_all();
  };

  // Helper lambda: wait with cancellable backoff, reconnect, validate.
  // Returns: 1 = success, 0 = reconnect failed (retry), -1 = should stop
  auto reconnect_with_backoff = [this, &reconnect_attempt](const std::string& reason, bool silent) -> int {
    reconnect_attempt = std::min(reconnect_attempt + 1, 10);
    const int64_t delay_ms = ReconnectBackoffDelayMs(reconnect_attempt);
    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "retry_connection")
        .Field("reason", reason)
        .Field("delay_ms", static_cast<int64_t>(delay_ms))
        .Field("attempt", static_cast<int64_t>(reconnect_attempt))
        .Debug();
    if (!WaitForReconnectBackoff(reconnect_attempt)) {
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
    return 1;  // Success
  };

  while (!should_stop_) {
    if (validate_before_stream_reopen) {
      validate_before_stream_reopen = false;
      if (!ValidateConnection()) {
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "processing_failure_recovery_validation_failed")
            .Field("error", GetLastError())
            .Error();
        should_stop_.store(true, std::memory_order_release);
        break;
      }
    }

    // Setup session (CRC32 checksum, heartbeat, etc.) via protocol-specific stream
    auto setup_result = binlog_stream_->SetupSession(*binlog_connection_);
    if (!setup_result) {
      SetLastError(setup_result.error());

      if (initial_stream_open_pending) {
        publish_initial_stream_state(StreamStartupState::kFailed);
        should_stop_.store(true, std::memory_order_release);
        break;
      }

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
      SetLastError(open_result.error());

      if (initial_stream_open_pending) {
        publish_initial_stream_state(StreamStartupState::kFailed);
        should_stop_.store(true, std::memory_order_release);
        break;
      }

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

    if (initial_stream_open_pending) {
      initial_stream_open_pending = false;
      publish_initial_stream_state(StreamStartupState::kOpened);
    }

    // A reopened stream starts from the worker-owned applied position. Never
    // carry an in-flight received GTID or transaction boundary across it.
    position_state_.ResetReceived();

    // Reset no-data counter on successful stream open (reconnection)
    no_data_log_count_.store(0);

    // Log stream opened - use Debug level for idle timeout reconnects to avoid noisy logs
    if (idle_timeout_reconnect) {
      mygram::utils::StructuredLog().Event("binlog_stream_opened").Field("gtid", GetCurrentGTID()).Debug();
      idle_timeout_reconnect = false;
    } else {
      mygram::utils::StructuredLog().Event("binlog_stream_opened").Field("gtid", GetCurrentGTID()).Info();
    }
    // Read binlog events
    int event_count = 0;
    bool connection_lost = false;
    bool connection_was_reestablished = false;
    auto request_processing_failure_reconnect = [&]() {
      processing_failure_reconnect_requested_.store(true, std::memory_order_release);
      validate_before_stream_reopen = true;
      connection_lost = true;
      if (binlog_stream_ != nullptr && binlog_connection_ != nullptr && binlog_connection_->IsConnected()) {
        binlog_stream_->Close(*binlog_connection_);
      }
    };

    while (!should_stop_ && !connection_lost) {
      if (processing_failure_reconnect_requested_.exchange(false, std::memory_order_acq_rel)) {
        mygram::utils::StructuredLog()
            .Event("binlog_processing_failure_reconnect")
            .Field("gtid", GetCurrentGTID())
            .Warn();
        validate_before_stream_reopen = true;
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
          mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "connection_lost_reconnect").Debug();
          const int rc = reconnect_with_backoff("connection_lost", true);
          if (rc == -1) {
            should_stop_.store(true, std::memory_order_release);
          } else if (rc == 1) {
            connection_was_reestablished = true;
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
            connection_was_reestablished = true;
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
          // Only a successfully fetched stream event proves the connection is
          // stable. Opening a stream that is immediately lost must continue
          // escalating the reconnect backoff.
          reconnect_attempt = 0;
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
          SetLastError("CRC32 checksum mismatch in binlog event; reconnecting from last processed GTID");
          mygram::utils::StructuredLog()
              .Event("binlog_error")
              .Field("type", "crc32_checksum_mismatch")
              .Field("computed_crc", static_cast<uint64_t>(computed_crc))
              .Field("stored_crc", static_cast<uint64_t>(stored_crc))
              .Field("event_length", static_cast<uint64_t>(event_length))
              .Field("gtid", position_state_.received_gtid())
              .Error();
          request_processing_failure_reconnect();
          break;
        }
      }

      MySQLBinlogEventType event_type = MySQLBinlogEventType::UNKNOWN_EVENT;

      // Check for GTID events first (need to update current_gtid)
      if (event_length >= mygram::constants::kBinlogEventHeaderLen) {
        event_type = static_cast<MySQLBinlogEventType>(event_buffer[4]);

        if (event_type == MySQLBinlogEventType::GTID_LOG_EVENT) {
          auto gtid_opt = BinlogEventParser::ExtractGTID(event_buffer, event_length);
          if (gtid_opt) {
            position_state_.ObserveReceivedGTID(*gtid_opt, false, false);
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "reader_gtid_received")
                .Field("gtid", position_state_.received_gtid())
                .Debug();
          }
          continue;
        }

        if (event_type == MySQLBinlogEventType::GTID_TAGGED_LOG_EVENT) {
          const auto tagged_gtid = BinlogEventParser::ExtractTaggedGTID(event_buffer, event_length);
          SetLastError(
              "Received GTID_TAGGED_LOG_EVENT. Tagged GTIDs are not supported because reconnect cannot encode "
              "UUID:TAG:GNO positions safely." +
              (tagged_gtid.has_value() ? " Received position: " + *tagged_gtid : " The event payload was malformed."));
          mygram::utils::StructuredLog log;
          log.Event("binlog_fatal_error")
              .Field("type", "unsupported_runtime_event")
              .Field("event_type", "GTID_TAGGED_LOG_EVENT");
          if (tagged_gtid.has_value()) {
            log.Field("gtid", *tagged_gtid);
          }
          log.Error();
          should_stop_.store(true, std::memory_order_release);
          break;
        }

        // MariaDB GTID event (type 162): extract domain-server-seq GTID
        if (event_type == MySQLBinlogEventType::MARIADB_GTID_EVENT) {
          const auto flags = MariaDBEventParser::ExtractGTIDFlags(event_buffer, event_length);
          if (flags.has_value() && ((*flags & MariaDBEventParser::kXaFlagMask) != 0U)) {
            SetLastError(
                "Received a MariaDB GTID_EVENT for an XA transaction. XA transactions are unsupported because "
                "prepared rows cannot be published before a later XA COMMIT or discarded on XA ROLLBACK.");
            mygram::utils::StructuredLog()
                .Event("binlog_fatal_error")
                .Field("type", "unsupported_xa_transaction")
                .Field("event_type", "MARIADB_GTID_EVENT")
                .Field("flags", static_cast<uint64_t>(*flags))
                .Error();
            should_stop_.store(true, std::memory_order_release);
            break;
          }
          auto gtid_opt = MariaDBEventParser::ExtractGTID(event_buffer, event_length);
          if (gtid_opt) {
            const bool transaction_open = flags.has_value() && BinlogEventParser::IsMariaDBGtidTransactionOpen(*flags);
            const bool standalone = flags.has_value() && ((*flags & MariaDBEventParser::kStandaloneFlag) != 0U);
            position_state_.ObserveReceivedGTID(*gtid_opt, transaction_open, standalone);
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "reader_mariadb_gtid_received")
                .Field("gtid", position_state_.received_gtid())
                .Debug();
          }
          continue;
        }

        if (event_type == MySQLBinlogEventType::MARIADB_GTID_LIST_EVENT) {
          const auto positions = MariaDBEventParser::ParseGTIDList(event_buffer, event_length);
          if (!positions.has_value()) {
            SetLastError("Malformed MariaDB GTID_LIST_EVENT; reconnecting from last processed GTID");
            request_processing_failure_reconnect();
            break;
          }
          mygram::utils::StructuredLog()
              .Event("binlog_debug")
              .Field("action", "mariadb_gtid_list_received")
              .Field("domain_count", static_cast<uint64_t>(positions->size()))
              .Debug();
          continue;
        }

        if (event_type == MySQLBinlogEventType::MARIADB_ANNOTATE_ROWS_EVENT) {
          const auto annotation = MariaDBEventParser::ExtractAnnotateRows(event_buffer, event_length);
          if (annotation.has_value()) {
            mygram::utils::StructuredLog()
                .Event("binlog_debug")
                .Field("action", "mariadb_rows_annotation")
                .Field("query", *annotation)
                .Debug();
          }
          continue;
        }

        // Skip other MariaDB-specific events that don't need processing.
        if (event_type == MySQLBinlogEventType::MARIADB_BINLOG_CHECKPOINT_EVENT ||
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
            SetLastError("Failed to parse TABLE_MAP_EVENT from binlog; reconnecting from last processed GTID");
            mygram::utils::StructuredLog()
                .Event("binlog_error")
                .Field("type", "table_map_parse_failed")
                .Field("event_num", static_cast<int64_t>(event_count))
                .Error();
            request_processing_failure_reconnect();
            break;
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

            const std::string qualified_table =
                config::QualifiedTableName(metadata_opt->database_name, metadata_opt->table_name);
            auto suppress_replay = ShouldSuppressTableReplay(qualified_table, position_state_.received_gtid());
            if (!suppress_replay) {
              SetLastError("Cannot evaluate per-table SYNC replay watermark: " + suppress_replay.error().message());
              mygram::utils::StructuredLog()
                  .Event("binlog_fatal_error")
                  .Field("type", "invalid_table_replay_watermark")
                  .Field("table", qualified_table)
                  .Field("gtid", position_state_.received_gtid())
                  .FieldError(suppress_replay.error())
                  .Error();
              should_stop_.store(true, std::memory_order_release);
              break;
            }
            if (*suppress_replay) {
              replay_suppressed_table_ids_.insert(metadata_opt->table_id);
              mygram::utils::StructuredLog()
                  .Event("binlog_replay_suppressed")
                  .Field("table", qualified_table)
                  .Field("gtid", position_state_.received_gtid())
                  .Field("reason", "covered_by_sync_snapshot")
                  .Debug();
              continue;
            }
            replay_suppressed_table_ids_.erase(metadata_opt->table_id);

            if (is_monitored_table) {
              if (!FetchColumnNames(metadata_opt.value())) {
                SetLastError(
                    "Failed to fetch column names for monitored TABLE_MAP_EVENT; reconnecting from last "
                    "processed GTID");
                mygram::utils::StructuredLog()
                    .Event("binlog_error")
                    .Field("type", "column_fetch_failed")
                    .Field("database", metadata_opt->database_name)
                    .Field("table", metadata_opt->table_name)
                    .Field("gtid", GetCurrentGTID())
                    .Error();
                request_processing_failure_reconnect();
                break;
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
              InvalidateColumnNamesForSchemaChange(metadata_opt.value());
              if (is_monitored_table) {
                if (!FetchColumnNames(metadata_opt.value())) {
                  SetLastError(
                      "Failed to refresh column names after monitored schema change; reconnecting from last "
                      "processed GTID");
                  request_processing_failure_reconnect();
                  break;
                }
                table_metadata_cache_.AddOrUpdate(metadata_opt->table_id, metadata_opt.value());
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

        // Reject unsupported wire formats before replay suppression. A SYNC
        // watermark may justify skipping a decoded row, but it must never turn
        // an event format we cannot decode into an accepted transaction.
        if (RejectUnsupportedRuntimeEvent(event_type)) {
          break;
        }

        // A preceding TABLE_MAP established that this target-table row event
        // is already included in its isolated SYNC snapshot. Consume the
        // transaction's eventual COMMIT for GTID progress, but do not parse or
        // apply the row image. Non-target tables in the same replay interval
        // continue through the normal path.
        if (IsRowsEventType(event_type) && event_length >= mygram::constants::kBinlogEventHeaderLen + 6 &&
            replay_suppressed_table_ids_.find(ExtractRowsEventTableId(event_buffer)) !=
                replay_suppressed_table_ids_.end()) {
          continue;
        }
      }

      auto query_boundary = BinlogEventParser::QueryTransactionBoundary::kNone;
      const bool query_was_inside_transaction = position_state_.reader_transaction_open();
      bool query_parsed = false;
      std::string query_text;
      if (event_type == MySQLBinlogEventType::QUERY_EVENT) {
        auto query = BinlogEventParser::ExtractQueryString(event_buffer, event_length);
        if (!query.has_value()) {
          SetLastError("Failed to parse QUERY_EVENT while ROW binlog format is required; refusing to advance GTID");
          mygram::utils::StructuredLog()
              .Event("binlog_fatal_error")
              .Field("type", "malformed_query_event")
              .Field("gtid", position_state_.received_gtid())
              .Error();
          should_stop_.store(true, std::memory_order_release);
          break;
        }
        query_parsed = true;
        query_text = std::move(*query);
        query_boundary = BinlogEventParser::ClassifyQueryTransactionBoundary(query_text);
        if (query_boundary == BinlogEventParser::QueryTransactionBoundary::kUnsupportedXa) {
          SetLastError(
              "Received an XA transaction statement. XA transactions are unsupported because prepared rows "
              "cannot be published before a later XA COMMIT or discarded on XA ROLLBACK.");
          mygram::utils::StructuredLog()
              .Event("binlog_fatal_error")
              .Field("type", "unsupported_xa_transaction")
              .Field("query", query_text)
              .Error();
          should_stop_.store(true, std::memory_order_release);
          break;
        }
        if (query_boundary == BinlogEventParser::QueryTransactionBoundary::kBegin) {
          position_state_.BeginReaderTransaction();
        }
      }

      // Parse the binlog event using BinlogEventParser
      auto events = BinlogEventParser::ParseBinlogEvent(event_buffer, event_length, position_state_.received_gtid(),
                                                        table_metadata_cache_, table_contexts_, nullptr, true,
                                                        mysql_config_.datetime_timezone);

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
        if (event_type == MySQLBinlogEventType::QUERY_EVENT &&
            query_boundary == BinlogEventParser::QueryTransactionBoundary::kEnd) {
          position_state_.EndReaderTransaction();
        }
      } else if (IsMonitoredRowsEventParseFailure(event_type, event_buffer, event_length)) {
        SetLastError("Failed to parse monitored ROWS_EVENT from binlog; reconnecting from last processed GTID");
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "rows_event_parse_failed")
            .Field("event_type", GetEventTypeName(event_type))
            .Field("reader_gtid", position_state_.received_gtid())
            .Field("current_gtid", GetCurrentGTID())
            .Error();
        request_processing_failure_reconnect();
        break;
      } else {
        // A standalone QUERY_EVENT has no following XID. Even when the query
        // does not affect a configured table (server initialization DDL,
        // grants, etc.), enqueue its GTID behind all previously parsed work so
        // the applied position remains a complete, ordered server GTID set.
        // Never do this for BEGIN or for statements inside an explicit
        // transaction; those advance only at COMMIT/XID.
        const bool standalone_query_completed =
            event_type == MySQLBinlogEventType::QUERY_EVENT && query_parsed &&
            !position_state_.received_gtid().empty() &&
            (query_boundary == BinlogEventParser::QueryTransactionBoundary::kEnd ||
             (query_boundary == BinlogEventParser::QueryTransactionBoundary::kNone && !query_was_inside_transaction &&
              BinlogEventParser::IsSafeIgnoredQuery(query_text)));
        if (standalone_query_completed) {
          auto progress = std::make_unique<BinlogEvent>();
          progress->type = BinlogEventType::COMMIT;
          progress->gtid = position_state_.received_gtid();
          PushEvent(std::move(progress));
        } else if (event_type == MySQLBinlogEventType::QUERY_EVENT && query_parsed &&
                   query_boundary == BinlogEventParser::QueryTransactionBoundary::kNone &&
                   !BinlogEventParser::IsSafeIgnoredQuery(query_text)) {
          SetLastError(
              "Received an unrecognized QUERY_EVENT while ROW binlog format is required; refusing to "
              "advance GTID without applying possible statement-based data changes");
          mygram::utils::StructuredLog()
              .Event("binlog_fatal_error")
              .Field("type", "unsafe_query_event")
              .Field("query", query_text)
              .Field("gtid", position_state_.received_gtid())
              .Error();
          should_stop_.store(true, std::memory_order_release);
          break;
        }
        if (event_type == MySQLBinlogEventType::QUERY_EVENT &&
            query_boundary == BinlogEventParser::QueryTransactionBoundary::kEnd) {
          position_state_.EndReaderTransaction();
        }
        mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "event_skipped").Debug();
      }

      if (event_type == MySQLBinlogEventType::XID_EVENT) {
        position_state_.EndReaderTransaction();
      }
      if (position_state_.mariadb_standalone_group_open() &&
          BinlogEventParser::IsRowsStatementEnd(event_buffer, static_cast<unsigned long>(event_length))) {
        auto progress = std::make_unique<BinlogEvent>();
        progress->type = BinlogEventType::COMMIT;
        progress->gtid = position_state_.received_gtid();
        PushEvent(std::move(progress));
        position_state_.CloseMariaDBStandaloneGroup();
      } else if (position_state_.mariadb_standalone_group_open() && event_type == MySQLBinlogEventType::QUERY_EVENT) {
        // Standalone QUERY_EVENT completion is handled above. Clear the group
        // marker so a later row event cannot publish the query's GTID.
        position_state_.CloseMariaDBStandaloneGroup();
      }
    }

    // Close binlog stream
    if (ShouldCloseStreamAfterReadLoop(connection_was_reestablished) && binlog_connection_ &&
        binlog_connection_->IsConnected()) {
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

  MarkThreadExited();
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
      const bool schema_incompatible = schema_incompatible_.load(std::memory_order_acquire);
      {
        std::scoped_lock lock(queue_mutex_);
        while (!event_queue_.empty()) {
          event_queue_.pop();
        }
        if (schema_incompatible) {
          // A schema mismatch is persistent. Reconnecting would replay the
          // same DDL forever, so stop and expose not-ready until SYNC/config
          // action.
          should_stop_.store(true, std::memory_order_release);
        } else {
          // GTID is not updated on a retryable failure. Publish the reconnect
          // request while holding queue_mutex_ so PushEvent cannot enqueue a
          // later commit between queue draining and failure publication.
          processing_failure_reconnect_requested_.store(true, std::memory_order_release);
        }
      }
      {
        std::scoped_lock lock(gtid_mutex_);
        position_state_.ResetApplied();
      }
      std::function<void()> after_failure_published_hook;
      {
        std::lock_guard<std::mutex> lock(processing_failure_hook_mutex_);
        after_failure_published_hook = after_processing_failure_published_hook_for_test_;
      }
      if (after_failure_published_hook) {
        after_failure_published_hook();
      }
      queue_full_cv_.notify_all();
      if (schema_incompatible) {
        queue_cv_.notify_all();
      }
    }
  }

  MarkThreadExited();
  mygram::utils::StructuredLog().Event("binlog_worker_thread_stopped").Info();
}

bool BinlogReader::ProcessQueuedEvent(const BinlogEvent& event) {
  if (event.type == BinlogEventType::COMMIT) {
    mygram::utils::Expected<std::string, mygram::utils::Error> advanced = mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError, "GTID commit was not attempted"));
    {
      std::scoped_lock lock(gtid_mutex_);
      if (!position_state_.CommitGTIDMatchesPending(event.gtid)) {
        SetLastError("COMMIT GTID does not match the applied transaction; refusing to advance replication position");
        return false;
      }
      const std::string& commit_gtid = position_state_.ResolveCommitGTID(event.gtid);
      if (commit_gtid.empty()) {
        return true;
      }
      advanced = UpdateCurrentGTIDLocked(commit_gtid);
      if (advanced) {
        position_state_.ClearPendingAppliedGTID();
      }
    }
    if (!advanced) {
      SetLastError(advanced.error());
      return false;
    }
    ClearReachedReplayWatermarks(*advanced);
    return true;
  }

  auto suppress_replay = ShouldSuppressTableReplay(event.table_name, event.gtid);
  if (!suppress_replay) {
    SetLastError("Cannot evaluate per-table SYNC replay watermark: " + suppress_replay.error().message());
    mygram::utils::StructuredLog()
        .Event("binlog_fatal_error")
        .Field("type", "invalid_table_replay_watermark")
        .Field("table", event.table_name)
        .Field("gtid", event.gtid)
        .FieldError(suppress_replay.error())
        .Error();
    schema_incompatible_.store(true, std::memory_order_release);
    return false;
  }
  if (*suppress_replay) {
    processed_events_++;
    if (event.type == BinlogEventType::DDL) {
      auto advanced = UpdateCurrentGTID(event.gtid);
      if (!advanced) {
        SetLastError(advanced.error());
        return false;
      }
    } else if (!event.gtid.empty()) {
      std::scoped_lock lock(gtid_mutex_);
      position_state_.RecordAppliedMutation(event.gtid);
    }
    return true;
  }

  if (event.type == BinlogEventType::DDL) {
    const DDLSchemaCheck schema_check = ValidateSchemaAfterDDL(event);
    if (schema_check != DDLSchemaCheck::kCompatible) {
      return false;
    }
  }

  if (!ProcessEvent(event)) {
    return false;
  }

  processed_events_++;

  if (event.type == BinlogEventType::DDL) {
    if (!event.gtid.empty()) {
      auto advanced = UpdateCurrentGTID(event.gtid);
      if (!advanced) {
        SetLastError(advanced.error());
        return false;
      }
    }
    return true;
  }

  if (!event.gtid.empty()) {
    std::scoped_lock lock(gtid_mutex_);
    position_state_.RecordAppliedMutation(event.gtid);
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
