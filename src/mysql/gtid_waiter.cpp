/**
 * @file gtid_waiter.cpp
 * @brief Bounded wait for an applied GTID position.
 */

#include "mysql/gtid_waiter.h"

#include <algorithm>
#include <functional>
#include <string>
#include <thread>
#include <utility>

#include "mysql/gtid_encoder.h"

namespace mygramdb::mysql {

mygram::utils::Expected<void, mygram::utils::Error> WaitForAppliedPosition(
    const IBinlogReader& reader, std::string_view target_gtid, std::chrono::milliseconds timeout,
    std::chrono::milliseconds poll_interval, GtidWaitCancellation cancellation_requested) {
  GtidWaitRuntime runtime{
      []() { return GtidWaitRuntime::Clock::now(); },
      [](std::chrono::milliseconds duration) { std::this_thread::sleep_for(duration); },
  };
  return WaitForAppliedPosition(reader, target_gtid, timeout, poll_interval, std::move(cancellation_requested),
                                runtime);
}

mygram::utils::Expected<void, mygram::utils::Error> WaitForAppliedPosition(const IBinlogReader& reader,
                                                                           std::string_view target_gtid,
                                                                           std::chrono::milliseconds timeout,
                                                                           std::chrono::milliseconds poll_interval,
                                                                           GtidWaitCancellation cancellation_requested,
                                                                           const GtidWaitRuntime& runtime) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (target_gtid.empty()) {
    return {};
  }
  if (!runtime.now || !runtime.sleep_for) {
    return MakeUnexpected(MakeError(ErrorCode::kInternalError, "GTID waiter runtime is not configured"));
  }
  timeout = std::max(timeout, std::chrono::milliseconds::zero());
  poll_interval = std::max(poll_interval, std::chrono::milliseconds(1));
  auto last_progress_at = runtime.now();
  std::string last_position;
  bool have_last_position = false;

  while (true) {
    const std::string current = reader.GetCurrentGTID();
    auto covers = GtidEncoder::PositionCoversAuto(target_gtid, current);
    if (!covers) {
      return MakeUnexpected(covers.error());
    }
    if (*covers) {
      return {};
    }
    if (!reader.IsRunning()) {
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLBinlogError, "Binlog reader stopped before reaching required GTID " +
                                                      std::string(target_gtid) + " (current: " + current + ")"));
    }
    if (cancellation_requested && cancellation_requested()) {
      return MakeUnexpected(
          MakeError(ErrorCode::kCancelled, "Cancelled while waiting for binlog reader to reach required GTID " +
                                               std::string(target_gtid) + " (current: " + current + ")"));
    }

    const auto now = runtime.now();
    if (!have_last_position) {
      last_position = current;
      have_last_position = true;
    } else {
      auto advances = GtidEncoder::PositionCoversAuto(last_position, current);
      if (!advances) {
        return MakeUnexpected(advances.error());
      }
      if (*advances) {
        auto unchanged = GtidEncoder::PositionCoversAuto(current, last_position);
        if (!unchanged) {
          return MakeUnexpected(unchanged.error());
        }
        if (!*unchanged) {
          last_position = current;
          last_progress_at = now;
        }
      }
    }

    if (now - last_progress_at >= timeout) {
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLBinlogError,
                    "Timed out after no GTID progress while waiting for binlog reader to reach required GTID " +
                        std::string(target_gtid) + " (current: " + current + ")"));
    }
    runtime.sleep_for(poll_interval);
  }
}

}  // namespace mygramdb::mysql
