/**
 * @file replication_pause_guard.h
 * @brief Shared replication pause/drain/restore lifecycle for dump operations.
 */

#pragma once

#include <atomic>
#include <string>

#include "server/replication_pause_counter.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql {
class IBinlogReader;
}

namespace mygramdb::server::replication_pause {

struct PauseResult {
  std::string drained_gtid;
  bool engaged = false;
  bool first_pauser = false;
};

/**
 * Coordinates the complete replication pause lifecycle used by DUMP SAVE,
 * DUMP LOAD, and scheduled snapshots.
 *
 * Pause() synchronously stops the first reader, publishes its drained GTID,
 * and makes later callers wait for that same position. Restore() releases the
 * shared counter and only the last releaser restarts the reader. Destruction
 * performs the same restore, so early returns and exceptions cannot leak a
 * paused reader or the observable pause flag.
 */
class Guard {
 public:
  Guard(mysql::IBinlogReader* reader, Counter& counter, std::atomic<bool>& paused_flag,
        std::atomic<bool>* shutdown_flag, std::string operation);
  ~Guard();

  Guard(const Guard&) = delete;
  Guard& operator=(const Guard&) = delete;
  Guard(Guard&&) = delete;
  Guard& operator=(Guard&&) = delete;

  PauseResult Pause();
  mygram::utils::Expected<void, mygram::utils::Error> Restore();

  [[nodiscard]] bool engaged() const noexcept { return scope_.held(); }
  [[nodiscard]] const PauseResult& result() const noexcept { return result_; }

 private:
  mysql::IBinlogReader* reader_;
  Counter& counter_;
  std::atomic<bool>& paused_flag_;
  std::atomic<bool>* shutdown_flag_;
  std::string operation_;
  Scope scope_;
  PauseResult result_;
  bool pause_called_ = false;
};

}  // namespace mygramdb::server::replication_pause
