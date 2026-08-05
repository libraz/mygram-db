/**
 * @file replication_pause_guard.cpp
 * @brief Shared replication pause/drain/restore lifecycle implementation.
 */

#include "server/replication_pause_guard.h"

#include <utility>

#include "mysql/binlog_reader_interface.h"
#include "utils/structured_log.h"

namespace mygramdb::server::replication_pause {

Guard::Guard(mysql::IBinlogReader* reader, Counter& counter, std::atomic<bool>& paused_flag,
             std::atomic<bool>* shutdown_flag, std::string operation)
    : reader_(reader),
      counter_(counter),
      paused_flag_(paused_flag),
      shutdown_flag_(shutdown_flag),
      operation_(std::move(operation)),
      scope_(counter) {}

Guard::~Guard() {
  auto restored = Restore();
  if (!restored) {
    mygram::utils::StructuredLog()
        .Event("replication_restart_failed")
        .Field("operation", operation_)
        .FieldError(restored.error())
        .Error();
  }
}

PauseResult Guard::Pause() {
  if (pause_called_) {
    return result_;
  }
  pause_called_ = true;

  if (reader_ == nullptr) {
    return result_;
  }

  // A later operation must join an existing pause even though the first
  // pauser has already made IsRunning() false. Otherwise the first operation
  // could restart replication while the later operation is still dumping.
  if (!reader_->IsRunning() && !counter_.IsPaused()) {
    result_.drained_gtid = reader_->GetCurrentGTID();
    return result_;
  }

  result_.first_pauser = scope_.Acquire();
  result_.engaged = true;

  if (!result_.first_pauser) {
    result_.drained_gtid = counter_.WaitForDrainedGTID();
    return result_;
  }

  // A manual REPLICATION STOP can win after the initial IsRunning() check.
  // Do not restart a reader this operation did not stop.
  if (!reader_->IsRunning()) {
    result_.drained_gtid = reader_->GetCurrentGTID();
    counter_.PublishDrainedGTID(result_.drained_gtid);
    scope_.Release();
    result_.engaged = false;
    result_.first_pauser = false;
    return result_;
  }

  paused_flag_.store(true, std::memory_order_release);
  reader_->Stop();
  result_.drained_gtid = reader_->GetCurrentGTID();
  counter_.PublishDrainedGTID(result_.drained_gtid);
  return result_;
}

mygram::utils::Expected<void, mygram::utils::Error> Guard::Restore() {
  if (!scope_.held()) {
    return {};
  }

  const bool last_releaser = scope_.Release();
  if (!last_releaser) {
    mygram::utils::StructuredLog()
        .Event("replication_pause_released")
        .Field("operation", operation_)
        .Field("last_releaser", false)
        .Info();
    return {};
  }

  paused_flag_.store(false, std::memory_order_release);
  if (reader_ == nullptr) {
    return {};
  }
  if (shutdown_flag_ != nullptr && shutdown_flag_->load(std::memory_order_acquire)) {
    mygram::utils::StructuredLog()
        .Event("replication_restart_skipped")
        .Field("operation", operation_)
        .Field("reason", "server_shutting_down")
        .Info();
    return {};
  }

  auto started = reader_->Start();
  if (!started) {
    return mygram::utils::MakeUnexpected(started.error());
  }
  mygram::utils::StructuredLog().Event("replication_resumed_after_dump").Field("operation", operation_).Info();
  return {};
}

}  // namespace mygramdb::server::replication_pause
