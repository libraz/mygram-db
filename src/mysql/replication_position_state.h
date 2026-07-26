/**
 * @file replication_position_state.h
 * @brief GTID lifecycle state shared by the reader and apply stages
 */

#pragma once

#include <string>
#include <utility>

namespace mygramdb::mysql {

/**
 * Tracks the two distinct positions in the replication pipeline:
 *
 * - received_gtid: the transaction currently being decoded by the reader;
 * - pending_applied_gtid: rows already applied by the worker, awaiting COMMIT.
 *
 * A received position is never exposed as committed. The worker can advance
 * only by taking a commit GTID after all mutations ahead of it were applied.
 * Reader fields are reader-thread-owned; applied fields are protected by the
 * BinlogReader GTID mutex.
 */
class ReplicationPositionState {
 public:
  void ResetReceived() {
    received_gtid_.clear();
    reader_transaction_open_ = false;
    mariadb_standalone_group_open_ = false;
  }

  void ObserveReceivedGTID(std::string gtid, bool transaction_open, bool mariadb_standalone) {
    received_gtid_ = std::move(gtid);
    reader_transaction_open_ = transaction_open;
    mariadb_standalone_group_open_ = mariadb_standalone;
  }

  [[nodiscard]] const std::string& received_gtid() const { return received_gtid_; }
  [[nodiscard]] bool reader_transaction_open() const { return reader_transaction_open_; }
  [[nodiscard]] bool mariadb_standalone_group_open() const { return mariadb_standalone_group_open_; }

  void BeginReaderTransaction() { reader_transaction_open_ = true; }
  void EndReaderTransaction() { reader_transaction_open_ = false; }
  void CloseMariaDBStandaloneGroup() { mariadb_standalone_group_open_ = false; }

  void RecordAppliedMutation(std::string gtid) { pending_applied_gtid_ = std::move(gtid); }

  [[nodiscard]] std::string TakeCommitGTID(const std::string& event_gtid) {
    std::string commit_gtid = event_gtid.empty() ? pending_applied_gtid_ : event_gtid;
    pending_applied_gtid_.clear();
    return commit_gtid;
  }

  [[nodiscard]] bool CommitGTIDMatchesPending(const std::string& event_gtid) const {
    return event_gtid.empty() || pending_applied_gtid_.empty() || event_gtid == pending_applied_gtid_;
  }

  [[nodiscard]] const std::string& ResolveCommitGTID(const std::string& event_gtid) const {
    return event_gtid.empty() ? pending_applied_gtid_ : event_gtid;
  }

  void ClearPendingAppliedGTID() { pending_applied_gtid_.clear(); }

  void ResetApplied() { pending_applied_gtid_.clear(); }

 private:
  std::string received_gtid_;
  std::string pending_applied_gtid_;
  bool reader_transaction_open_ = false;
  bool mariadb_standalone_group_open_ = false;
};

}  // namespace mygramdb::mysql
