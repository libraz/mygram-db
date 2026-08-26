/**
 * @file readiness.h
 * @brief The single readiness classification every health surface renders.
 */

#pragma once

#include <cstdint>
#include <string>

namespace mygramdb::mysql {
class IBinlogReader;
}  // namespace mygramdb::mysql

namespace mygramdb::server {

/**
 * @brief How binlog replication is currently classified.
 *
 * The distinction between an intentional stop and an outage is the point of
 * this enum. A reader stopped so a dump can be written, or so a table can be
 * resynchronised, is doing what it was told; a reader stopped for any other
 * reason is a fault. Reporting both as "not running" makes a routine SYNC
 * indistinguishable from a lost connection.
 */
enum class ReplicationAvailability : uint8_t {
  kDisabled,       ///< No binlog reader is wired; replication is not configured.
  kRunning,        ///< The reader is streaming events.
  kStarting,       ///< Start() has reserved the lifecycle; the stream is not open yet.
  kPausedForDump,  ///< Stopped on purpose while a dump or snapshot is written.
  kPausedForSync,  ///< Stopped on purpose while a table is resynchronised.
  kStopped,        ///< Not running, with no error recorded.
  kFailed,         ///< Not running, with an error recorded.
};

/// Wire value the health surfaces report for an availability state.
const char* ToString(ReplicationAvailability availability);

/// True for the states a correctly operating server passes through.
bool IsReplicationAvailable(ReplicationAvailability availability);

/**
 * @brief Everything the readiness verdict is computed from.
 *
 * Each surface collects these from whichever handles it owns — the HTTP server
 * from its member pointers, the TCP handlers from their HandlerContext — and
 * hands them over unclassified.
 */
struct ReadinessInputs {
  const mysql::IBinlogReader* binlog_reader = nullptr;
  bool data_initialized = true;
  bool loading = false;
  bool sync_in_progress = false;
  bool replication_paused_for_dump = false;
};

/**
 * @brief The classification, computed once and rendered by each surface.
 *
 * The inputs are echoed back so a renderer never has to re-read the state it
 * passed in, which is how the surfaces drifted apart in the first place.
 */
struct ReadinessVerdict {
  bool ready = true;
  ReplicationAvailability replication = ReplicationAvailability::kDisabled;
  bool data_initialized = true;
  bool loading = false;
  bool sync_in_progress = false;
  bool replication_paused_for_dump = false;
  /// Operator-facing explanation of `ready == false`; empty when ready.
  std::string reason;

  bool replication_available() const { return IsReplicationAvailable(replication); }
};

/**
 * @brief Classify replication availability and overall readiness.
 *
 * The verdict is the only place either question is answered. A surface that
 * needs a different presentation renders this differently; it does not decide
 * differently.
 */
ReadinessVerdict EvaluateReadiness(const ReadinessInputs& inputs);

/**
 * @brief Describe a replication fault without repeating MySQL's own text.
 *
 * The reader stores mysql_error() verbatim, and that text can name the
 * replication account, the address MySQL resolved the client to, and whether a
 * password was sent. Surfaces reachable without administrative credentials
 * report the error code's own description instead; the verbatim message stays
 * on GET /replication/status and in the structured log.
 */
std::string ReplicationErrorSummary(const mysql::IBinlogReader& reader);

}  // namespace mygramdb::server
