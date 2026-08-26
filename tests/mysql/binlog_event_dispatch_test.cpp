/**
 * @file binlog_event_dispatch_test.cpp
 * @brief Fail-closed dispatch of binlog event types
 *
 * The reader may decode an event, skip it as data-neutral, or stop. These tests
 * pin which types belong to which branch and assert that every stop publishes
 * the code SYNC needs to decide the position cannot be replayed.
 */

#include <array>
#include <optional>
#include <set>
#include <string>

#include "binlog_test_fixtures.h"

#ifdef USE_MYSQL

#include "mysql/binlog_event_disposition.h"

using namespace binlog_test;
using mygram::utils::ErrorCode;

namespace {

/** Every value of MySQLBinlogEventType, as it appears in the event header byte. */
constexpr std::array<uint8_t, 49> kEnumeratedEventTypeBytes = {
    0,  1,  2,  3,  4,  5,  7,  9,  11, 13, 14, 15,  16,  17,  18,  19,  23,  24,  25,  26,  27,  28,  29,  30, 31,
    32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 255};

/** Types the reader is allowed to skip, each justified where it is classified. */
const std::set<MySQLBinlogEventType>& DataNeutralTypes() {
  static const std::set<MySQLBinlogEventType> types = {
      MySQLBinlogEventType::STOP_EVENT,
      MySQLBinlogEventType::ROTATE_EVENT,
      MySQLBinlogEventType::FORMAT_DESCRIPTION_EVENT,
      MySQLBinlogEventType::HEARTBEAT_LOG_EVENT,
      MySQLBinlogEventType::HEARTBEAT_LOG_EVENT_V2,
      MySQLBinlogEventType::INTVAR_EVENT,
      MySQLBinlogEventType::RAND_EVENT,
      MySQLBinlogEventType::USER_VAR_EVENT,
      MySQLBinlogEventType::BEGIN_LOAD_QUERY_EVENT,
      MySQLBinlogEventType::APPEND_BLOCK_EVENT,
      MySQLBinlogEventType::DELETE_FILE_EVENT,
      MySQLBinlogEventType::IGNORABLE_LOG_EVENT,
      MySQLBinlogEventType::ROWS_QUERY_LOG_EVENT,
      MySQLBinlogEventType::PREVIOUS_GTIDS_LOG_EVENT,
      MySQLBinlogEventType::TRANSACTION_CONTEXT_EVENT,
      MySQLBinlogEventType::VIEW_CHANGE_EVENT,
      MySQLBinlogEventType::MARIADB_ANNOTATE_ROWS_EVENT,
      MySQLBinlogEventType::MARIADB_BINLOG_CHECKPOINT_EVENT,
      MySQLBinlogEventType::MARIADB_START_ENCRYPTION_EVENT,
  };
  return types;
}

/** Types whose payload the reader or the parser turns into replicated state. */
const std::set<MySQLBinlogEventType>& DecodedTypes() {
  static const std::set<MySQLBinlogEventType> types = {
      MySQLBinlogEventType::QUERY_EVENT,
      MySQLBinlogEventType::XID_EVENT,
      MySQLBinlogEventType::TABLE_MAP_EVENT,
      MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1,
      MySQLBinlogEventType::OBSOLETE_UPDATE_ROWS_EVENT_V1,
      MySQLBinlogEventType::OBSOLETE_DELETE_ROWS_EVENT_V1,
      MySQLBinlogEventType::WRITE_ROWS_EVENT,
      MySQLBinlogEventType::UPDATE_ROWS_EVENT,
      MySQLBinlogEventType::DELETE_ROWS_EVENT,
      MySQLBinlogEventType::GTID_LOG_EVENT,
      MySQLBinlogEventType::MARIADB_GTID_EVENT,
      MySQLBinlogEventType::MARIADB_GTID_LIST_EVENT,
  };
  return types;
}

}  // namespace

/**
 * @brief A type is skipped only if it is on the enumerated data-neutral list.
 *
 * Widening the list is what silently drops an event, so it is pinned here: a
 * type added to it has to be added to this test too, which is where the reason
 * it is harmless gets reviewed.
 */
TEST(BinlogEventDispositionTest, OnlyEnumeratedInformationalTypesAreSkipped) {
  for (const uint8_t type_byte : kEnumeratedEventTypeBytes) {
    const auto type = static_cast<MySQLBinlogEventType>(type_byte);
    SCOPED_TRACE(std::string(GetEventTypeName(type)) + " (" + std::to_string(type_byte) + ")");
    const auto disposition = ClassifyBinlogEventDisposition(type);

    if (DataNeutralTypes().count(type) != 0) {
      EXPECT_EQ(disposition, BinlogEventDisposition::kDataNeutral);
    } else if (DecodedTypes().count(type) != 0) {
      EXPECT_EQ(disposition, BinlogEventDisposition::kDecodeAndApply);
    } else {
      EXPECT_EQ(disposition, BinlogEventDisposition::kFailClosed);
    }
  }
}

/**
 * @brief A header byte that matches no known type stops replication.
 */
TEST(BinlogEventDispositionTest, WireBytesOutsideTheEnumerationFailClosed) {
  const std::set<uint8_t> enumerated(kEnumeratedEventTypeBytes.begin(), kEnumeratedEventTypeBytes.end());
  for (int candidate = 0; candidate <= 255; ++candidate) {
    const auto type_byte = static_cast<uint8_t>(candidate);
    if (enumerated.count(type_byte) != 0) {
      continue;
    }
    SCOPED_TRACE("event type byte " + std::to_string(candidate));
    EXPECT_EQ(ClassifyBinlogEventDisposition(static_cast<MySQLBinlogEventType>(type_byte)),
              BinlogEventDisposition::kFailClosed);
  }
}

/**
 * @brief Every fail-closed event stops with the code SYNC reads.
 *
 * A generic binlog error makes SYNC restart from the drained position and hit
 * the same event again, so the reason is lost and the recovery never completes.
 */
TEST_F(BinlogReaderFixture, EveryFailClosedEventTypeStopsWithTheUndecodableCode) {
  for (const uint8_t type_byte : kEnumeratedEventTypeBytes) {
    const auto type = static_cast<MySQLBinlogEventType>(type_byte);
    if (ClassifyBinlogEventDisposition(type) != BinlogEventDisposition::kFailClosed) {
      EXPECT_FALSE(reader_->RejectUnsupportedRuntimeEvent(type))
          << GetEventTypeName(type) << " is not classified fail-closed";
      continue;
    }
    SCOPED_TRACE(std::string(GetEventTypeName(type)) + " (" + std::to_string(type_byte) + ")");

    reader_->should_stop_.store(false, std::memory_order_release);
    reader_->SetCurrentGTID("uuid:17");
    ASSERT_TRUE(reader_->RejectUnsupportedRuntimeEvent(type));

    EXPECT_TRUE(reader_->should_stop_.load(std::memory_order_acquire));
    EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:17");
    EXPECT_EQ(reader_->GetLastErrorCode(), ErrorCode::kMySQLUndecodableBinlogEvent);
    EXPECT_EQ(reader_->GetReplicationState(), ReplicationState::kFailed);
    const std::string last_error = reader_->GetLastError();
    EXPECT_NE(last_error.find("stays in the binlog"), std::string::npos) << last_error;
    EXPECT_NE(last_error.find("SYNC for every replicated table"), std::string::npos) << last_error;
    EXPECT_FALSE(reader_->HasSchemaIncompatibleError());
  }
  reader_->should_stop_.store(false, std::memory_order_release);
}

/**
 * @brief An anonymous transaction stops before its rows are attributed elsewhere.
 *
 * Its rows carry no GTID of their own. Applying them under the previous
 * transaction's GTID records a position that does not describe what was applied
 * and makes auto-positioning redeliver them on every reconnect.
 */
TEST_F(BinlogReaderFixture, AnonymousGtidEventStopsWithoutAdvancingAnyPosition) {
  reader_->SetCurrentGTID("uuid:17");
  const int64_t applied_time_before = reader_->GetLastAppliedUnixTime();
  reader_->position_state_.ObserveReceivedGTID("uuid:17", false, false);
  reader_->should_stop_.store(false, std::memory_order_release);

  ASSERT_TRUE(reader_->RejectUnsupportedRuntimeEvent(MySQLBinlogEventType::ANONYMOUS_GTID_LOG_EVENT));

  EXPECT_TRUE(reader_->should_stop_.load(std::memory_order_acquire));
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:17") << "an anonymous transaction has no position to record";
  EXPECT_EQ(reader_->GetLastAppliedUnixTime(), applied_time_before)
      << "the lag gauge must not report progress for a transaction that was not applied";
  EXPECT_EQ(reader_->GetLastErrorCode(), ErrorCode::kMySQLUndecodableBinlogEvent);
  EXPECT_EQ(reader_->GetReplicationState(), ReplicationState::kFailed);

  const std::string last_error = reader_->GetLastError();
  EXPECT_NE(last_error.find("gtid_mode"), std::string::npos)
      << "the remediation has to name the setting that produced the event: " << last_error;
  EXPECT_EQ(reader_->GetQueueSize(), 0U);
  reader_->should_stop_.store(false, std::memory_order_release);
}

/**
 * @brief A source-declared binlog gap stops instead of committing across it.
 *
 * The missing interval was never written, so no restart and no reconnect can
 * re-read it. Advancing past it loses the interval permanently while the health
 * surface still reports a running replica.
 */
TEST_F(BinlogReaderFixture, IncidentEventStopsAndNamesTheOnlyRecovery) {
  reader_->SetCurrentGTID("uuid:17");
  reader_->should_stop_.store(false, std::memory_order_release);

  ASSERT_TRUE(reader_->RejectUnsupportedRuntimeEvent(MySQLBinlogEventType::INCIDENT_EVENT));

  EXPECT_TRUE(reader_->should_stop_.load(std::memory_order_acquire));
  EXPECT_EQ(reader_->GetCurrentGTID(), "uuid:17") << "the position must not advance past the declared gap";
  EXPECT_EQ(reader_->GetLastErrorCode(), ErrorCode::kMySQLUndecodableBinlogEvent);
  EXPECT_EQ(reader_->GetReplicationState(), ReplicationState::kFailed);

  const std::string last_error = reader_->GetLastError();
  EXPECT_NE(last_error.find("lost events"), std::string::npos) << last_error;
  EXPECT_NE(last_error.find("SYNC for every replicated table"), std::string::npos)
      << "an operator cannot recover without being told a full SYNC is the only way past it: " << last_error;
  reader_->should_stop_.store(false, std::memory_order_release);
}

/**
 * @brief The stop paths that a reconnect would replay publish the same code.
 *
 * Each of these conditions is encoded in an event that is still there after the
 * stream is reopened. Publishing a generic binlog error sends SYNC back to the
 * drained position, where it stops on the same event again.
 */
TEST_F(BinlogReaderFixture, StopsOnEventsThatSurviveAReconnectPublishTheUndecodableCode) {
  const auto expect_undecodable = [this](const char* what) {
    EXPECT_TRUE(reader_->should_stop_.load(std::memory_order_acquire)) << what;
    EXPECT_EQ(reader_->GetLastErrorCode(), ErrorCode::kMySQLUndecodableBinlogEvent) << what;
    EXPECT_NE(reader_->GetLastError().find("SYNC for every replicated table"), std::string::npos) << what;
    reader_->should_stop_.store(false, std::memory_order_release);
  };

  reader_->RejectTaggedGtidEvent(std::string("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:tag:9"));
  expect_undecodable("GTID_TAGGED_LOG_EVENT");
  EXPECT_NE(reader_->GetLastError().find("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa:tag:9"), std::string::npos);

  reader_->RejectTaggedGtidEvent(std::nullopt);
  expect_undecodable("malformed GTID_TAGGED_LOG_EVENT");

  reader_->RejectUnsupportedXaTransaction("MARIADB_GTID_EVENT", {});
  expect_undecodable("MariaDB GTID event flagged XA");
  EXPECT_NE(reader_->GetLastError().find("XA"), std::string::npos);

  // Real XA traffic reaches this exit, at XA START, before XA_PREPARE_LOG_EVENT.
  reader_->RejectUnsupportedXaTransaction("QUERY_EVENT", "XA START 'x1'");
  expect_undecodable("XA START statement");
  EXPECT_NE(reader_->GetLastError().find("XA"), std::string::npos);

  reader_->RejectUnsafeStatementEvent("INSERT INTO t VALUES (1)");
  expect_undecodable("statement-based DML");
  EXPECT_NE(reader_->GetLastError().find("unrecognized QUERY_EVENT"), std::string::npos);
}

/**
 * @brief A reason a replay would get past must not claim the event is undecodable.
 *
 * These stops and retries resume from the last processed GTID. Publishing the
 * undecodable code would make SYNC skip a position that was only transiently
 * unreadable, dropping whatever it contained.
 */
TEST_F(BinlogReaderFixture, TransientAndReplayableFailuresDoNotPublishTheUndecodableCode) {
  for (const uint8_t type_byte : kEnumeratedEventTypeBytes) {
    const auto type = static_cast<MySQLBinlogEventType>(type_byte);
    if (ClassifyBinlogEventDisposition(type) == BinlogEventDisposition::kFailClosed) {
      continue;
    }
    SCOPED_TRACE(std::string(GetEventTypeName(type)) + " (" + std::to_string(type_byte) + ")");
    EXPECT_FALSE(reader_->RejectUnsupportedRuntimeEvent(type));
    EXPECT_NE(reader_->GetLastErrorCode(), ErrorCode::kMySQLUndecodableBinlogEvent);
  }
}

#endif  // USE_MYSQL
