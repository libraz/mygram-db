/**
 * @file gtid_waiter.h
 * @brief Bounded wait for a binlog reader to apply a required GTID position.
 */

#pragma once

#include <chrono>
#include <string_view>

#include "mysql/binlog_reader_interface.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql {

/**
 * Wait until reader.GetCurrentGTID() covers target_gtid.
 *
 * Empty targets are already satisfied. A stopped reader, malformed position,
 * or deadline expiry fails closed so callers do not publish readiness for a
 * snapshot whose post-snapshot replication gap is still unapplied.
 */
mygram::utils::Expected<void, mygram::utils::Error> WaitForAppliedPosition(
    const IBinlogReader& reader, std::string_view target_gtid, std::chrono::milliseconds timeout,
    std::chrono::milliseconds poll_interval = std::chrono::milliseconds(10));

}  // namespace mygramdb::mysql
