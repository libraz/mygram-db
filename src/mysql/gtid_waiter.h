/**
 * @file gtid_waiter.h
 * @brief Bounded wait for a binlog reader to apply a required GTID position.
 */

#pragma once

#include <chrono>
#include <functional>
#include <string_view>

#include "mysql/binlog_reader_interface.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql {

using GtidWaitCancellation = std::function<bool()>;

/**
 * Injectable runtime used by deterministic waiter tests.
 */
struct GtidWaitRuntime {
  using Clock = std::chrono::steady_clock;

  std::function<Clock::time_point()> now;
  std::function<void(std::chrono::milliseconds)> sleep_for;
};

/**
 * Wait until reader.GetCurrentGTID() covers target_gtid.
 *
 * Empty targets are already satisfied. A stopped reader, malformed position,
 * cancellation, or a lack of monotonic GTID progress for timeout fails closed
 * so callers do not publish readiness for a snapshot whose post-snapshot
 * replication gap is still unapplied.
 */
mygram::utils::Expected<void, mygram::utils::Error> WaitForAppliedPosition(
    const IBinlogReader& reader, std::string_view target_gtid, std::chrono::milliseconds timeout,
    std::chrono::milliseconds poll_interval = std::chrono::milliseconds(10),
    GtidWaitCancellation cancellation_requested = {});

/**
 * Wait using an injected monotonic clock and sleeper.
 *
 * This overload keeps production callers ergonomic while allowing timeout and
 * cancellation behavior to be tested without wall-clock delays.
 */
mygram::utils::Expected<void, mygram::utils::Error> WaitForAppliedPosition(const IBinlogReader& reader,
                                                                           std::string_view target_gtid,
                                                                           std::chrono::milliseconds timeout,
                                                                           std::chrono::milliseconds poll_interval,
                                                                           GtidWaitCancellation cancellation_requested,
                                                                           const GtidWaitRuntime& runtime);

}  // namespace mygramdb::mysql
