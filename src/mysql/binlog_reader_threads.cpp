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

#include "mysql/binlog_checksum.h"
#include "mysql/binlog_event_disposition.h"
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

/// Stop after this many retryable processing failures replay the same applied position.
static constexpr int kMaxConsecutiveProcessingFailuresAtSameGtid = 3;

bool HasConfiguredIgnoredDdlPrefix(std::string_view query, const std::vector<std::string>& prefixes) {
  const auto first = query.find_first_not_of(" \t\r\n");
  if (first == std::string_view::npos)
    return false;
  const std::string_view normalized = query.substr(first);
  for (const auto& prefix : prefixes) {
    const auto prefix_first = prefix.find_first_not_of(" \t\r\n");
    const auto prefix_last = prefix.find_last_not_of(" \t\r\n");
    if (prefix_first == std::string::npos)
      continue;
    const std::string_view candidate(prefix.data() + prefix_first, prefix_last - prefix_first + 1);
    if (normalized.size() < candidate.size())
      continue;
    bool equal = true;
    for (size_t i = 0; i < candidate.size(); ++i) {
      if (std::toupper(static_cast<unsigned char>(normalized[i])) !=
          std::toupper(static_cast<unsigned char>(candidate[i]))) {
        equal = false;
        break;
      }
    }
    if (equal)
      return true;
  }
  return false;
}

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

bool BinlogReader::ShouldStopForRepeatedProcessingFailure(const std::string& applied_gtid,
                                                          std::string& last_failure_gtid, int& consecutive_failures) {
  if (applied_gtid == last_failure_gtid) {
    ++consecutive_failures;
  } else {
    last_failure_gtid = applied_gtid;
    consecutive_failures = 1;
  }
  return consecutive_failures >= kMaxConsecutiveProcessingFailuresAtSameGtid;
}

bool BinlogReader::ShouldStopForProcessingFailure(ProcessingFailureKind kind, const std::string& applied_gtid,
                                                  std::string& last_failure_gtid, int& consecutive_failures) {
  if (kind != ProcessingFailureKind::kDeterministic) {
    return false;
  }
  return ShouldStopForRepeatedProcessingFailure(applied_gtid, last_failure_gtid, consecutive_failures);
}

BinlogReader::ProcessingFailureKind BinlogReader::ClassifyProcessingFailure(mygram::utils::ErrorCode code) {
  return IsRetryableSchemaValidationError(code) ? ProcessingFailureKind::kTransientTransport
                                                : ProcessingFailureKind::kDeterministic;
}

void BinlogReader::PublishProcessingFailure(ProcessingFailureKind kind) {
  auto current = processing_failure_reconnect_requested_.load(std::memory_order_acquire);
  while (static_cast<uint8_t>(current) < static_cast<uint8_t>(kind) &&
         !processing_failure_reconnect_requested_.compare_exchange_weak(current, kind, std::memory_order_release,
                                                                        std::memory_order_acquire)) {
  }
}

void BinlogReader::ResetProcessingBackoffAfterProgress(const std::string& applied_gtid, std::string& last_recovery_gtid,
                                                       int& reconnect_attempt) {
  if (applied_gtid != last_recovery_gtid) {
    last_recovery_gtid = applied_gtid;
    reconnect_attempt = 0;
  }
}

void BinlogReader::FailClosedOnUnreplayableEvent(mygram::utils::ErrorCode code, const std::string& message,
                                                 std::string_view failure_type, std::string_view event_type_name) {
  SetLastError(mygram::utils::MakeError(code, message));
  mygram::utils::StructuredLog log;
  log.Event("binlog_fatal_error").Field("type", std::string(failure_type));
  if (!event_type_name.empty()) {
    log.Field("event_type", std::string(event_type_name));
  }
  log.Field("gtid", position_state_.received_gtid()).Field("error", message).Error();
  // Fail closed before a following commit event can advance current_gtid_.
  should_stop_.store(true, std::memory_order_release);
}

void BinlogReader::FailClosedOnUnevaluableReplayWatermark(const std::string& table_name, const std::string& gtid,
                                                          const mygram::utils::Error& error) {
  // The callee's code identifies the root condition (a GTID that cannot be
  // parsed or compared). Replacing it with a generic binlog error, or latching
  // schema_incompatible_, would send the operator after a schema mismatch that
  // does not exist.
  const std::string message = "Cannot evaluate per-table SYNC replay watermark: " + error.message();
  SetLastError(mygram::utils::MakeError(error.code(), message, table_name));
  mygram::utils::StructuredLog()
      .Event("binlog_fatal_error")
      .Field("type", "invalid_table_replay_watermark")
      .Field("table", table_name)
      .Field("gtid", gtid)
      .FieldError(error)
      .Error();
  should_stop_.store(true, std::memory_order_release);
}

bool BinlogReader::RejectUnsupportedRuntimeEvent(MySQLBinlogEventType event_type) {
  if (ClassifyBinlogEventDisposition(event_type) != BinlogEventDisposition::kFailClosed) {
    return false;
  }
  FailClosedOnUnreplayableEvent(mygram::utils::ErrorCode::kMySQLUndecodableBinlogEvent,
                                UnreplayableEventRemediation(event_type), "unreplayable_binlog_event",
                                GetEventTypeName(event_type));
  return true;
}

void BinlogReader::RejectTaggedGtidEvent(const std::optional<std::string>& tagged_gtid) {
  FailClosedOnUnreplayableEvent(
      mygram::utils::ErrorCode::kMySQLUndecodableBinlogEvent,
      "Received GTID_TAGGED_LOG_EVENT. Tagged GTIDs are not supported because reconnect cannot encode "
      "UUID:TAG:GNO positions safely." +
          (tagged_gtid.has_value() ? " Received position: " + *tagged_gtid : " The event payload was malformed") +
          kUnreplayableEventRecovery,
      "unsupported_runtime_event", "GTID_TAGGED_LOG_EVENT");
}

void BinlogReader::RejectUnsupportedXaTransaction(std::string_view source_event, const std::string& statement) {
  // A transaction marked XA is rejected at whichever event reveals it: the
  // MariaDB GTID flags, the XA START statement, or XA_PREPARE_LOG_EVENT. All of
  // them survive a reconnect, so all of them publish the same code.
  if (!statement.empty()) {
    mygram::utils::StructuredLog()
        .Event("binlog_debug")
        .Field("action", "unsupported_xa_transaction")
        .Field("query", statement)
        .Debug();
  }
  FailClosedOnUnreplayableEvent(
      mygram::utils::ErrorCode::kMySQLUndecodableBinlogEvent,
      std::string("Received an XA transaction. XA transactions are unsupported because prepared rows cannot be "
                  "published before a later XA COMMIT or discarded on XA ROLLBACK") +
          kUnreplayableEventRecovery,
      "unsupported_xa_transaction", source_event);
}

void BinlogReader::RejectUnsafeStatementEvent(const std::string& statement) {
  // Statement-based DML is replayed unchanged after any reconnect, so a generic
  // binlog code would turn every SYNC restart into the same stop.
  mygram::utils::StructuredLog()
      .Event("binlog_debug")
      .Field("action", "unsafe_query_event")
      .Field("query", statement)
      .Debug();
  FailClosedOnUnreplayableEvent(
      mygram::utils::ErrorCode::kMySQLUndecodableBinlogEvent,
      std::string("Received an unrecognized QUERY_EVENT while ROW binlog format is required; refusing to advance GTID "
                  "without applying possible statement-based data changes") +
          kUnreplayableEventRecovery,
      "unsafe_query_event", "QUERY_EVENT");
}

void BinlogReader::MarkThreadExited() {
  uint8_t active = active_threads_.load(std::memory_order_acquire);
  while (active != 0 && !active_threads_.compare_exchange_weak(active, static_cast<uint8_t>(active - 1),
                                                               std::memory_order_acq_rel, std::memory_order_acquire)) {
  }
}

void BinlogReader::ReaderThreadFunc() {
  mygram::utils::StructuredLog().Event("binlog_reader_thread_started").Info();

  try {
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
    bool initial_stream_open_pending = true;
    std::string last_processing_failure_gtid;
    std::string last_processing_recovery_gtid;
    int consecutive_processing_failures = 0;
    int processing_reconnect_attempt = 0;
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
    auto reconnect_with_backoff = [this](const std::string& reason, bool silent, int& attempt) -> int {
      attempt = std::min(attempt + 1, 10);
      const int64_t delay_ms = ReconnectBackoffDelayMs(attempt);
      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "retry_connection")
          .Field("reason", reason)
          .Field("delay_ms", static_cast<int64_t>(delay_ms))
          .Field("attempt", static_cast<int64_t>(attempt))
          .Debug();
      if (!WaitForReconnectBackoff(attempt)) {
        mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "stop_requested_during_retry").Debug();
        return -1;
      }

      auto result = binlog_connection_->Reconnect(silent);
      if (!result) {
        mygram::utils::LogBinlogError("reconnect_failed", GetCurrentGTID(), result.error().message(), attempt);
        return 0;  // Retry in next iteration
      }

      // Metadata queries participate in the same recovery attempt. Reconnect
      // this dedicated handle here, under its serialization lock, instead of
      // letting individual metadata helpers retry without backoff.
      {
        std::lock_guard<std::mutex> metadata_lock(metadata_connection_mutex_);
        if (metadata_connection_ == nullptr) {
          SetLastError(mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLDisconnected,
                                                "Metadata connection is unavailable during recovery"));
          return -1;
        }
        auto metadata_result = metadata_connection_->Reconnect(true);
        if (!metadata_result) {
          SetLastError(metadata_result.error());
          mygram::utils::LogBinlogError("metadata_reconnect_failed", GetCurrentGTID(),
                                        metadata_result.error().message(), attempt);
          return 0;
        }
      }

      // Validate connection after reconnection (detect failover, invalid servers)
      if (!ValidateConnection()) {
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "connection_validation_failed")
            .Field("context", "after_reconnect")
            .Field("error", GetLastError())
            .Error();
        if (IsRetryableSchemaValidationError(GetLastErrorObject().code())) {
          return 0;  // Retry transient validation failure with backoff.
        }
        return -1;  // Semantic validation failure: stop replication.
      }

      mygram::utils::StructuredLog()
          .Event("binlog_debug")
          .Field("action", "connection_validated_after_reconnect")
          .Debug();
      return 1;  // Success
    };

    while (!should_stop_) {
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
        int rc = reconnect_with_backoff("session_setup_failed", silent, reconnect_attempt);
        if (rc == -1) {
          should_stop_.store(true, std::memory_order_release);
          break;
        }
        continue;
      }

      // Use current_gtid_ (last applied by the worker thread) as the
      // authoritative source for reconnection. The server's executed set may
      // include events not yet delivered to MygramDB and would skip data.
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
        int rc = reconnect_with_backoff("stream_open_failed", false, reconnect_attempt);
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
      ProcessingFailureKind processing_failure = ProcessingFailureKind::kNone;
      auto request_processing_failure_reconnect = [&](ProcessingFailureKind kind) {
        PublishProcessingFailure(kind);
        if (static_cast<uint8_t>(kind) > static_cast<uint8_t>(processing_failure)) {
          processing_failure = kind;
        }
        connection_lost = true;
        if (binlog_stream_ != nullptr && binlog_connection_ != nullptr && binlog_connection_->IsConnected()) {
          binlog_stream_->Close(*binlog_connection_);
        }
      };

      while (!should_stop_ && !connection_lost) {
        const auto requested =
            processing_failure_reconnect_requested_.exchange(ProcessingFailureKind::kNone, std::memory_order_acq_rel);
        if (requested != ProcessingFailureKind::kNone) {
          mygram::utils::StructuredLog()
              .Event("binlog_processing_failure_reconnect")
              .Field("gtid", GetCurrentGTID())
              .Warn();
          processing_failure = requested;
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
        const auto requested_after_fetch =
            processing_failure_reconnect_requested_.exchange(ProcessingFailureKind::kNone, std::memory_order_acq_rel);
        if (requested_after_fetch != ProcessingFailureKind::kNone) {
          if (static_cast<uint8_t>(requested_after_fetch) > static_cast<uint8_t>(processing_failure)) {
            processing_failure = requested_after_fetch;
          }
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
            SetLastError(mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLDisconnected, fetch.error_message));
            mygram::utils::StructuredLog().Event("binlog_debug").Field("action", "connection_lost_reconnect").Debug();
            const int rc = reconnect_with_backoff("connection_lost", true, reconnect_attempt);
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
            SetLastError(mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLDisconnected, fetch.error_message));
            mygram::utils::StructuredLog()
                .Event("binlog_connection_lost")
                .Field("error", GetLastError())
                .Field("gtid", GetCurrentGTID())
                .Warn();
            // NOLINTNEXTLINE(clang-analyzer-deadcode.DeadStores) - Documents intent; break exits to outer loop
            connection_lost = true;
            binlog_stream_->Close(*binlog_connection_);
            int rc = reconnect_with_backoff("server_gone_away", false, reconnect_attempt);
            if (rc == -1) {
              should_stop_.store(true, std::memory_order_release);
            } else if (rc == 1) {
              connection_was_reestablished = true;
              mygram::utils::StructuredLog().Event("binlog_connection_restored").Info();
            }
            break;
          }

          case BinlogFetchResult::Status::kBinlogPurged: {
            SetLastError(mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError, fetch.error_message));
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
            SetLastError(mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError, fetch.error_message));
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
          auto checksum_result = VerifyBinlogEventChecksum(event_buffer, event_length);
          if (!checksum_result) {
            crc_errors_++;
            SetLastError(checksum_result.error());
            mygram::utils::StructuredLog()
                .Event("binlog_error")
                .Field("type", "crc32_checksum_mismatch")
                .Field("event_length", static_cast<uint64_t>(event_length))
                .Field("gtid", position_state_.received_gtid())
                .FieldError(checksum_result.error())
                .Error();
            request_processing_failure_reconnect(ProcessingFailureKind::kDeterministic);
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
            RejectTaggedGtidEvent(BinlogEventParser::ExtractTaggedGTID(event_buffer, event_length));
            break;
          }

          // MariaDB GTID event (type 162): extract domain-server-seq GTID
          if (event_type == MySQLBinlogEventType::MARIADB_GTID_EVENT) {
            const auto flags = MariaDBEventParser::ExtractGTIDFlags(event_buffer, event_length);
            if (flags.has_value() && ((*flags & MariaDBEventParser::kXaFlagMask) != 0U)) {
              RejectUnsupportedXaTransaction("MARIADB_GTID_EVENT", {});
              break;
            }
            auto gtid_opt = MariaDBEventParser::ExtractGTID(event_buffer, event_length);
            if (gtid_opt) {
              const bool transaction_open =
                  flags.has_value() && BinlogEventParser::IsMariaDBGtidTransactionOpen(*flags);
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
              // A malformed frame is not proof that the event cannot be
              // decoded: a replay from the last processed GTID can deliver it
              // intact, so this must not publish the undecodable-event code.
              SetLastError(mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError,
                                                    "Malformed MariaDB GTID_LIST_EVENT; reconnecting from last "
                                                    "processed GTID"));
              request_processing_failure_reconnect(ProcessingFailureKind::kDeterministic);
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
              // Reconnecting replays this event, so it keeps the generic binlog
              // code rather than the undecodable-event code that makes SYNC
              // restart past the position.
              SetLastError(mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError,
                                                    "Failed to parse TABLE_MAP_EVENT from binlog; reconnecting from "
                                                    "last processed GTID"));
              mygram::utils::StructuredLog()
                  .Event("binlog_error")
                  .Field("type", "table_map_parse_failed")
                  .Field("event_num", static_cast<int64_t>(event_count))
                  .Error();
              request_processing_failure_reconnect(ProcessingFailureKind::kDeterministic);
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
                FailClosedOnUnevaluableReplayWatermark(qualified_table, position_state_.received_gtid(),
                                                       suppress_replay.error());
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
                ProcessingFailureKind failure_kind = ProcessingFailureKind::kDeterministic;
                if (!FetchColumnNames(metadata_opt.value(), &failure_kind)) {
                  mygram::utils::StructuredLog()
                      .Event("binlog_error")
                      .Field("type", "column_fetch_failed")
                      .Field("database", metadata_opt->database_name)
                      .Field("table", metadata_opt->table_name)
                      .Field("gtid", GetCurrentGTID())
                      .Error();
                  request_processing_failure_reconnect(failure_kind);
                  break;
                }
              } else {
                mygram::utils::StructuredLog()
                    .Event("binlog_debug")
                    .Field("action", "skipping_non_monitored_table")
                    .Field("database", metadata_opt->database_name)
                    .Field("table", metadata_opt->table_name)
                    .Debug();
                // Row events for an unmonitored table are intentionally
                // ignored when their table id is absent from this cache. Do
                // not retain metadata for arbitrary server tables.
                continue;
              }

              auto add_result = table_metadata_cache_.AddOrUpdate(metadata_opt->table_id, metadata_opt.value());

              if (add_result == TableMetadataCache::AddResult::kSchemaChanged) {
                InvalidateColumnNamesForSchemaChange(metadata_opt.value());
                if (is_monitored_table) {
                  ProcessingFailureKind failure_kind = ProcessingFailureKind::kDeterministic;
                  if (!FetchColumnNames(metadata_opt.value(), &failure_kind)) {
                    request_processing_failure_reconnect(failure_kind);
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

          // Everything the reader did not decode above is skipped only when it
          // is one of the informational types enumerated with a reason why
          // skipping cannot lose a row or a position. Nothing reaches the
          // parser by default.
          if (ClassifyBinlogEventDisposition(event_type) == BinlogEventDisposition::kDataNeutral) {
            continue;
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
            // The statement text is unreadable in this frame, not proven
            // undecodable: keep the generic binlog code so a replay is still
            // the first recovery an operator tries.
            SetLastError(mygram::utils::MakeError(
                mygram::utils::ErrorCode::kMySQLBinlogError,
                "Failed to parse QUERY_EVENT while ROW binlog format is required; refusing to advance GTID"));
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
            // Real XA traffic stops here, at XA START, rather than at the
            // XA_PREPARE_LOG_EVENT, so this exit is the one SYNC has to
            // recognize as un-replayable.
            RejectUnsupportedXaTransaction("QUERY_EVENT", query_text);
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
          SetLastError(mygram::utils::MakeError(
              mygram::utils::ErrorCode::kMySQLBinlogError,
              "Failed to parse monitored ROWS_EVENT from binlog; reconnecting from last processed GTID"));
          mygram::utils::StructuredLog()
              .Event("binlog_error")
              .Field("type", "rows_event_parse_failed")
              .Field("event_type", GetEventTypeName(event_type))
              .Field("reader_gtid", position_state_.received_gtid())
              .Field("current_gtid", GetCurrentGTID())
              .Error();
          request_processing_failure_reconnect(ProcessingFailureKind::kDeterministic);
          break;
        } else {
          // A standalone QUERY_EVENT has no following XID. Even when the query
          // does not affect a configured table (server initialization DDL,
          // grants, etc.), enqueue its GTID behind all previously parsed work so
          // the applied position remains a complete, ordered server GTID set.
          // Never do this for BEGIN or for statements inside an explicit
          // transaction; those advance only at COMMIT/XID.
          bool drops_configured_database = false;
          if (event_type == MySQLBinlogEventType::QUERY_EVENT && query_parsed) {
            for (const auto& [table_name, table_context] : table_contexts_) {
              static_cast<void>(table_name);
              if (table_context != nullptr &&
                  BinlogEventParser::IsDatabaseAffectingDDL(query_text, table_context->config.database)) {
                drops_configured_database = true;
                break;
              }
            }
          }
          const bool safe_ignored_query =
              query_parsed && !drops_configured_database &&
              (BinlogEventParser::IsSafeIgnoredQuery(query_text) ||
               HasConfiguredIgnoredDdlPrefix(query_text, mysql_config_.ignored_ddl_prefixes));
          const bool standalone_query_completed =
              event_type == MySQLBinlogEventType::QUERY_EVENT && query_parsed &&
              !position_state_.received_gtid().empty() &&
              (query_boundary == BinlogEventParser::QueryTransactionBoundary::kEnd ||
               (query_boundary == BinlogEventParser::QueryTransactionBoundary::kNone && !query_was_inside_transaction &&
                safe_ignored_query));
          if (standalone_query_completed) {
            auto progress = std::make_unique<BinlogEvent>();
            progress->type = BinlogEventType::COMMIT;
            progress->gtid = position_state_.received_gtid();
            PushEvent(std::move(progress));
          } else if (event_type == MySQLBinlogEventType::QUERY_EVENT && query_parsed &&
                     query_boundary == BinlogEventParser::QueryTransactionBoundary::kNone && !safe_ignored_query) {
            RejectUnsafeStatementEvent(query_text);
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

      if (processing_failure != ProcessingFailureKind::kNone) {
        // The request can originate in this reader thread (malformed wire
        // event) or in the worker. Consume its publication before reopening so
        // this recovery is counted exactly once; a concurrent later failure can
        // publish a new request after this exchange.
        const auto published =
            processing_failure_reconnect_requested_.exchange(ProcessingFailureKind::kNone, std::memory_order_acq_rel);
        if (static_cast<uint8_t>(published) > static_cast<uint8_t>(processing_failure)) {
          processing_failure = published;
        }
        const std::string applied_gtid = GetCurrentGTID();
        ResetProcessingBackoffAfterProgress(applied_gtid, last_processing_recovery_gtid, processing_reconnect_attempt);
        if (ShouldStopForProcessingFailure(processing_failure, applied_gtid, last_processing_failure_gtid,
                                           consecutive_processing_failures)) {
          SetLastError(mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError,
                                                "Repeatedly failed to process the binlog event at applied GTID " +
                                                    applied_gtid +
                                                    "; stopping replication to avoid an infinite replay loop"));
          mygram::utils::StructuredLog()
              .Event("binlog_fatal_error")
              .Field("type", "repeated_processing_failure")
              .Field("gtid", applied_gtid)
              .Field("attempt", static_cast<int64_t>(consecutive_processing_failures))
              .Error();
          should_stop_.store(true, std::memory_order_release);
          break;
        }

        // A processing failure can leave the transport healthy, but replaying
        // from the worker-owned applied position still requires a full reconnect
        // and its cancellable backoff. reconnect_with_backoff() validates the
        // newly established connection before the stream can reopen.
        const int rc = reconnect_with_backoff("event_processing_failed", false, processing_reconnect_attempt);
        if (rc == -1) {
          should_stop_.store(true, std::memory_order_release);
          break;
        }
        continue;
      }
    }

  } catch (const std::exception& error) {
    SetLastError(mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError,
                                          std::string("Unhandled exception in binlog reader thread: ") + error.what()));
    mygram::utils::StructuredLog()
        .Event("binlog_fatal_error")
        .Field("type", "reader_thread_exception")
        .Field("error", error.what())
        .Error();
    should_stop_.store(true, std::memory_order_release);
  } catch (...) {
    SetLastError(mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError,
                                          "Unhandled non-standard exception in binlog reader thread"));
    mygram::utils::StructuredLog().Event("binlog_fatal_error").Field("type", "reader_thread_unknown_exception").Error();
    should_stop_.store(true, std::memory_order_release);
  }

  // Wake the worker thread in case it is blocked in PopEvent() waiting on
  // queue_cv_. Without this notification the worker would hang indefinitely
  // when the reader exits due to a fatal error (should_stop_ = true) because
  // no more events will ever be pushed and the predicate would never flip.
  queue_cv_.notify_all();
  queue_full_cv_.notify_all();

  MarkThreadExited();
  mygram::utils::StructuredLog().Event("binlog_reader_thread_stopped").Info();
}

void BinlogReader::WorkerThreadFunc() {
  mygram::utils::StructuredLog().Event("binlog_worker_thread_started").Info();

  try {
    // Process events until PopEvent returns nullptr
    // PopEvent returns nullptr only when: should_stop_ is true AND queue is empty
    // This ensures all pending events are processed during graceful shutdown
    while (true) {
      auto event = PopEvent();
      if (!event) {
        // Exit only when shutdown requested AND queue is empty
        break;
      }

      ProcessingFailureKind failure_kind = ProcessingFailureKind::kDeterministic;
      if (!ProcessQueuedEvent(*event, &failure_kind)) {
        mygram::utils::StructuredLog()
            .Event("binlog_error")
            .Field("type", "event_processing_failed")
            .Field("table", event->table_name)
            .Field("primary_key", event->primary_key)
            .Field("gtid", event->gtid)
            .Error();
        // A schema mismatch is persistent, and so is any condition that already
        // failed closed while processing the event. Reconnecting would replay
        // it forever, so stop and expose not-ready until SYNC/config action
        // instead of publishing a retry.
        const bool stop_without_retry =
            schema_incompatible_.load(std::memory_order_acquire) || should_stop_.load(std::memory_order_acquire);
        {
          std::scoped_lock lock(queue_mutex_);
          while (!event_queue_.empty()) {
            event_queue_.pop();
          }
          if (stop_without_retry) {
            should_stop_.store(true, std::memory_order_release);
          } else {
            // GTID is not updated on a retryable failure. Publish the reconnect
            // request while holding queue_mutex_ so PushEvent cannot enqueue a
            // later commit between queue draining and failure publication.
            PublishProcessingFailure(failure_kind);
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
        if (stop_without_retry) {
          queue_cv_.notify_all();
        }
      }
    }
  } catch (const std::exception& error) {
    SetLastError(mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError,
                                          std::string("Unhandled exception in binlog worker thread: ") + error.what()));
    mygram::utils::StructuredLog()
        .Event("binlog_fatal_error")
        .Field("type", "worker_thread_exception")
        .Field("error", error.what())
        .Error();
    should_stop_.store(true, std::memory_order_release);
  } catch (...) {
    SetLastError(mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLBinlogError,
                                          "Unhandled non-standard exception in binlog worker thread"));
    mygram::utils::StructuredLog().Event("binlog_fatal_error").Field("type", "worker_thread_unknown_exception").Error();
    should_stop_.store(true, std::memory_order_release);
  }

  queue_cv_.notify_all();
  queue_full_cv_.notify_all();
  MarkThreadExited();
  mygram::utils::StructuredLog().Event("binlog_worker_thread_stopped").Info();
}

bool BinlogReader::ProcessQueuedEvent(const BinlogEvent& event, ProcessingFailureKind* failure_kind) {
  if (failure_kind != nullptr) {
    *failure_kind = ProcessingFailureKind::kDeterministic;
  }
  if (event.type == BinlogEventType::COMMIT) {
    mygram::utils::Expected<std::string, mygram::utils::Error> advanced = mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError, "GTID commit was not attempted"));
    {
      std::scoped_lock lock(gtid_mutex_);
      if (!position_state_.CommitGTIDMatchesPending(event.gtid)) {
        SetLastError(mygram::utils::MakeError(
            mygram::utils::ErrorCode::kMySQLBinlogError,
            "COMMIT GTID does not match the applied transaction; refusing to advance replication position"));
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
    return ClearReachedReplayWatermarks(*advanced);
  }

  auto suppress_replay = ShouldSuppressTableReplay(event.table_name, event.gtid);
  if (!suppress_replay) {
    FailClosedOnUnevaluableReplayWatermark(event.table_name, event.gtid, suppress_replay.error());
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
      if (failure_kind != nullptr && schema_check == DDLSchemaCheck::kRetryableFailure) {
        *failure_kind = ProcessingFailureKind::kTransientTransport;
      }
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

  if (should_stop_ ||
      processing_failure_reconnect_requested_.load(std::memory_order_acquire) != ProcessingFailureKind::kNone) {
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
