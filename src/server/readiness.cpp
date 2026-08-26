/**
 * @file readiness.cpp
 * @brief Readiness classification shared by the HTTP and TCP surfaces.
 */

#include "server/readiness.h"

#include "mysql/binlog_reader_interface.h"
#include "utils/error.h"

namespace mygramdb::server {

namespace {

ReplicationAvailability ClassifyReplication(const ReadinessInputs& inputs) {
#ifdef USE_MYSQL
  if (inputs.binlog_reader == nullptr) {
    return ReplicationAvailability::kDisabled;
  }
  if (inputs.binlog_reader->IsRunning()) {
    return ReplicationAvailability::kRunning;
  }
  if (inputs.binlog_reader->IsStarting()) {
    return ReplicationAvailability::kStarting;
  }
  // A dump holds the reader down through `replication_paused_for_dump`, while a
  // SYNC calls Stop() through replication_pause::Scope and never raises that
  // flag. Both are deliberate, so both are classified as a pause rather than as
  // the outage the flag alone would suggest.
  if (inputs.replication_paused_for_dump) {
    return ReplicationAvailability::kPausedForDump;
  }
  if (inputs.sync_in_progress) {
    return ReplicationAvailability::kPausedForSync;
  }
  return inputs.binlog_reader->GetReplicationState() == mysql::ReplicationState::kFailed
             ? ReplicationAvailability::kFailed
             : ReplicationAvailability::kStopped;
#else
  // Without the MySQL client there is no reader to classify, so readiness
  // cannot depend on replication.
  (void)inputs;
  return ReplicationAvailability::kDisabled;
#endif
}

std::string DescribeNotReady(const ReadinessInputs& inputs) {
  if (!inputs.data_initialized) {
    return "Initial data has not been loaded";
  }
  if (inputs.loading) {
    return "Server is loading";
  }
  if (inputs.binlog_reader != nullptr && inputs.binlog_reader->HasSchemaIncompatibleError()) {
    return "Replication stopped due to an incompatible schema";
  }
  if (inputs.binlog_reader != nullptr && !inputs.binlog_reader->GetLastError().empty()) {
    return ReplicationErrorSummary(*inputs.binlog_reader);
  }
  if (inputs.sync_in_progress) {
    return "SYNC is in progress";
  }
  return "Replication is not running";
}

}  // namespace

const char* ToString(ReplicationAvailability availability) {
  switch (availability) {
    case ReplicationAvailability::kDisabled:
      return "disabled";
    case ReplicationAvailability::kRunning:
      return "connected";
    case ReplicationAvailability::kStarting:
      return "starting";
    case ReplicationAvailability::kPausedForDump:
      return "paused_for_dump";
    case ReplicationAvailability::kPausedForSync:
      return "paused_for_sync";
    case ReplicationAvailability::kStopped:
      return "disconnected";
    case ReplicationAvailability::kFailed:
      return "failed";
  }
  return "disconnected";
}

bool IsReplicationAvailable(ReplicationAvailability availability) {
  return availability != ReplicationAvailability::kStopped && availability != ReplicationAvailability::kFailed;
}

ReadinessVerdict EvaluateReadiness(const ReadinessInputs& inputs) {
  ReadinessVerdict verdict;
  verdict.replication = ClassifyReplication(inputs);
  verdict.data_initialized = inputs.data_initialized;
  verdict.loading = inputs.loading;
  verdict.sync_in_progress = inputs.sync_in_progress;
  verdict.replication_paused_for_dump = inputs.replication_paused_for_dump;
  // A SYNC keeps the server out of rotation even though it leaves replication
  // available: the table it is rebuilding cannot answer queries yet.
  verdict.ready = inputs.data_initialized && !inputs.loading && !inputs.sync_in_progress &&
                  IsReplicationAvailable(verdict.replication);
  if (!verdict.ready) {
    verdict.reason = DescribeNotReady(inputs);
  }
  return verdict;
}

std::string ReplicationErrorSummary(const mysql::IBinlogReader& reader) {
  if (reader.GetLastError().empty()) {
    return {};
  }
  return mygram::utils::ErrorCodeToString(reader.GetLastErrorCode());
}

}  // namespace mygramdb::server
