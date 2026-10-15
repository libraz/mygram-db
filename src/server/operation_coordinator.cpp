#include "server/operation_coordinator.h"

#include <utility>

namespace mygramdb::server {

void OperationCoordinator::Token::Release() {
  if (owner_ == nullptr) {
    return;
  }
  owner_->Release(generation_);
  owner_ = nullptr;
  generation_ = 0;
}

std::optional<OperationCoordinator::Token> OperationCoordinator::TryAcquire(LongOperation type, std::string detail) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (shutdown_blocked_ || active_.has_value()) {
    return std::nullopt;
  }
  const uint64_t generation = next_generation_++;
  active_ = ActiveOperation{type, std::move(detail)};
  active_generation_ = generation;
  return Token(this, generation);
}

std::optional<OperationCoordinator::ActiveOperation> OperationCoordinator::GetActive() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return active_;
}

std::string OperationCoordinator::DescribeActive() const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (shutdown_blocked_) {
    return "server shutdown";
  }
  if (!active_.has_value()) {
    return "another long-running operation";
  }
  std::string description = Name(active_->type);
  if (!active_->detail.empty()) {
    description += " (" + active_->detail + ")";
  }
  return description;
}

void OperationCoordinator::BlockNewOperationsForShutdown(std::atomic<bool>* shutdown_flag) {
  std::lock_guard<std::mutex> lock(mutex_);
  shutdown_blocked_ = true;
  if (shutdown_flag != nullptr) {
    shutdown_flag->store(true, std::memory_order_release);
  }
}

bool OperationCoordinator::ReopenForStartup(std::atomic<bool>* shutdown_flag) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (active_.has_value()) {
    return false;
  }
  shutdown_blocked_ = false;
  if (shutdown_flag != nullptr) {
    shutdown_flag->store(false, std::memory_order_release);
  }
  return true;
}

const char* OperationCoordinator::Name(LongOperation type) {
  switch (type) {
    case LongOperation::kSync:
      return "SYNC";
    case LongOperation::kDumpSave:
      return "DUMP SAVE";
    case LongOperation::kDumpLoad:
      return "DUMP LOAD";
    case LongOperation::kOptimize:
      return "OPTIMIZE";
    case LongOperation::kAutoSnapshot:
      return "auto snapshot";
    case LongOperation::kMysqlReconnect:
      return "MySQL reconnect";
    case LongOperation::kReplicationStart:
      return "REPLICATION START";
    case LongOperation::kReplicationStop:
      return "REPLICATION STOP";
  }
  return "unknown operation";
}

void OperationCoordinator::Release(uint64_t generation) {
  bool became_idle = false;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (active_.has_value() && active_generation_ == generation) {
      active_.reset();
      active_generation_ = 0;
      became_idle = true;
    }
  }
  if (became_idle) {
    idle_cv_.notify_all();
  }
}

bool OperationCoordinator::WaitForIdle(std::chrono::milliseconds timeout) {
  std::unique_lock<std::mutex> lock(mutex_);
  return idle_cv_.wait_for(lock, timeout, [this] { return !active_.has_value(); });
}

void OperationCoordinator::WaitForIdle() {
  std::unique_lock<std::mutex> lock(mutex_);
  idle_cv_.wait(lock, [this] { return !active_.has_value(); });
}

}  // namespace mygramdb::server
