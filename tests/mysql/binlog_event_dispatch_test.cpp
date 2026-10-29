/**
 * @file binlog_event_dispatch_test.cpp
 * @brief Fail-closed dispatch of binlog event types
 *
 * The reader may decode an event, skip it as data-neutral, or stop. These tests
 * pin which types belong to which branch and assert that every stop publishes
 * the code SYNC needs to decide the position cannot be replayed.
 */

#include <array>
#include <set>
#include <string>
#include <vector>

#include "binlog_event_builder.h"
#include "binlog_test_fixtures.h"

#ifdef USE_MYSQL

#include "mysql/binlog_event_parser.h"
#include "utils/constants.h"
#include "utils/crc32.h"

using namespace binlog_test;
using mygram::utils::ErrorCode;

namespace {

/** Every value of mybinlog::EventType, as it appears in the event header byte. */
constexpr std::array<uint8_t, 50> kEnumeratedEventTypeBytes = {
    0,  1,  2,  3,  4,  5,  7,  9,  11, 13, 14, 15,  16,  17,  18,  19,  23,  24,  25,  26,  27,  28,  29,  30,  31,
    32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 255};

/** Types the reader is allowed to skip, each justified where it is classified. */
const std::set<mybinlog::EventType>& DataNeutralTypes() {
  static const std::set<mybinlog::EventType> types = {
      mybinlog::EventType::kStopEvent,
      mybinlog::EventType::kRotateEvent,
      mybinlog::EventType::kFormatDescriptionEvent,
      mybinlog::EventType::kHeartbeatLogEvent,
      mybinlog::EventType::kHeartbeatLogEventV2,
      mybinlog::EventType::kIntvarEvent,
      mybinlog::EventType::kRandEvent,
      mybinlog::EventType::kUserVarEvent,
      mybinlog::EventType::kBeginLoadQueryEvent,
      mybinlog::EventType::kAppendBlockEvent,
      mybinlog::EventType::kDeleteFileEvent,
      mybinlog::EventType::kIgnorableLogEvent,
      mybinlog::EventType::kRowsQueryLogEvent,
      mybinlog::EventType::kPreviousGtidsEvent,
      mybinlog::EventType::kTransactionContextEvent,
      mybinlog::EventType::kViewChangeEvent,
      mybinlog::EventType::kMariaDBAnnotateRowsEvent,
      mybinlog::EventType::kMariaDBBinlogCheckpointEvent,
      mybinlog::EventType::kMariaDBStartEncryptionEvent,
  };
  return types;
}

/** Types whose payload the reader or the parser turns into replicated state. */
const std::set<mybinlog::EventType>& DecodedTypes() {
  static const std::set<mybinlog::EventType> types = {
      mybinlog::EventType::kQueryEvent,           mybinlog::EventType::kXidEvent,
      mybinlog::EventType::kTableMapEvent,        mybinlog::EventType::kWriteRowsEventV1,
      mybinlog::EventType::kUpdateRowsEventV1,    mybinlog::EventType::kDeleteRowsEventV1,
      mybinlog::EventType::kWriteRowsEvent,       mybinlog::EventType::kUpdateRowsEvent,
      mybinlog::EventType::kDeleteRowsEvent,      mybinlog::EventType::kGtidLogEvent,
      mybinlog::EventType::kGtidTaggedLogEvent,   mybinlog::EventType::kMariaDBGtidEvent,
      mybinlog::EventType::kMariaDBGtidListEvent,
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
    const auto type = static_cast<mybinlog::EventType>(type_byte);
    SCOPED_TRACE(std::string(mybinlog::EventTypeName(type)) + " (" + std::to_string(type_byte) + ")");
    const auto disposition = ClassifyReaderEvent(type_byte);

    if (DataNeutralTypes().count(type) != 0) {
      EXPECT_EQ(disposition, ReaderEventDisposition::kDataNeutral);
    } else if (DecodedTypes().count(type) != 0) {
      EXPECT_EQ(disposition, ReaderEventDisposition::kDecodeAndApply);
    } else {
      EXPECT_EQ(disposition, ReaderEventDisposition::kFailClosed);
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
    EXPECT_EQ(ClassifyReaderEvent(type_byte), ReaderEventDisposition::kFailClosed);
  }
}

/**
 * @brief Only an unrecognized type carrying the source's ignorable flag is skippable.
 */
TEST(BinlogEventDispositionTest, UnrecognizedTypeIsSkippedOnlyWhenFlaggedIgnorable) {
  EXPECT_EQ(ClassifyReaderEvent(200, 0x0080), ReaderEventDisposition::kDataNeutral);
  EXPECT_EQ(ClassifyReaderEvent(200, 0x0081), ReaderEventDisposition::kDataNeutral);
  EXPECT_EQ(ClassifyReaderEvent(200, 0x0001), ReaderEventDisposition::kFailClosed);
  // A recognized type is never reclassified by the flag.
  EXPECT_EQ(ClassifyReaderEvent(static_cast<uint8_t>(mybinlog::EventType::kTransactionPayloadEvent), 0x0080),
            ReaderEventDisposition::kFailClosed);
}

/**
 * @brief A stop on an unrecognized byte names the byte, so two different ones never read the same.
 */
TEST_F(BinlogReaderFixture, UnknownEventTypeStopReportsTheTypeByte) {
  reader_->should_stop_.store(false, std::memory_order_release);
  ASSERT_TRUE(reader_->RejectUnsupportedRuntimeEvent(static_cast<mybinlog::EventType>(200)));

  EXPECT_EQ(reader_->GetLastErrorCode(), ErrorCode::kMySQLUndecodableBinlogEvent);
  EXPECT_NE(reader_->GetLastError().find("type 200"), std::string::npos) << reader_->GetLastError();
  EXPECT_EQ(DescribeEventType(200), "event type 200");
  EXPECT_EQ(DescribeEventType(static_cast<uint8_t>(mybinlog::EventType::kUnknown)), "UNKNOWN_EVENT");
  EXPECT_EQ(DescribeEventType(static_cast<uint8_t>(mybinlog::EventType::kTableMapEvent)), "TABLE_MAP_EVENT");
  reader_->should_stop_.store(false, std::memory_order_release);
}

/**
 * @brief Every fail-closed event stops with the code SYNC reads.
 *
 * A generic binlog error makes SYNC restart from the drained position and hit
 * the same event again, so the reason is lost and the recovery never completes.
 */
TEST_F(BinlogReaderFixture, EveryFailClosedEventTypeStopsWithTheUndecodableCode) {
  for (const uint8_t type_byte : kEnumeratedEventTypeBytes) {
    const auto type = static_cast<mybinlog::EventType>(type_byte);
    if (ClassifyReaderEvent(type_byte) != ReaderEventDisposition::kFailClosed) {
      EXPECT_FALSE(reader_->RejectUnsupportedRuntimeEvent(type))
          << mybinlog::EventTypeName(type) << " is not classified fail-closed";
      continue;
    }
    SCOPED_TRACE(std::string(mybinlog::EventTypeName(type)) + " (" + std::to_string(type_byte) + ")");

    reader_->should_stop_.store(false, std::memory_order_release);
    reader_->SetCurrentGTID("ffffffff-0000-0000-0000-000000000001:17");
    ASSERT_TRUE(reader_->RejectUnsupportedRuntimeEvent(type));

    EXPECT_TRUE(reader_->should_stop_.load(std::memory_order_acquire));
    EXPECT_EQ(reader_->GetCurrentGTID(), "ffffffff-0000-0000-0000-000000000001:1-17");
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
  reader_->SetCurrentGTID("ffffffff-0000-0000-0000-000000000001:17");
  const int64_t applied_time_before = reader_->GetLastAppliedUnixTime();
  reader_->position_state_.ObserveReceivedGTID("ffffffff-0000-0000-0000-000000000001:17", false, false);
  reader_->should_stop_.store(false, std::memory_order_release);

  ASSERT_TRUE(reader_->RejectUnsupportedRuntimeEvent(mybinlog::EventType::kAnonymousGtidLogEvent));

  EXPECT_TRUE(reader_->should_stop_.load(std::memory_order_acquire));
  EXPECT_EQ(reader_->GetCurrentGTID(), "ffffffff-0000-0000-0000-000000000001:1-17")
      << "an anonymous transaction has no position to record";
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
  reader_->SetCurrentGTID("ffffffff-0000-0000-0000-000000000001:17");
  reader_->should_stop_.store(false, std::memory_order_release);

  ASSERT_TRUE(reader_->RejectUnsupportedRuntimeEvent(mybinlog::EventType::kIncidentEvent));

  EXPECT_TRUE(reader_->should_stop_.load(std::memory_order_acquire));
  EXPECT_EQ(reader_->GetCurrentGTID(), "ffffffff-0000-0000-0000-000000000001:1-17")
      << "the position must not advance past the declared gap";
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
    const auto type = static_cast<mybinlog::EventType>(type_byte);
    if (ClassifyReaderEvent(type_byte) == ReaderEventDisposition::kFailClosed) {
      continue;
    }
    SCOPED_TRACE(std::string(mybinlog::EventTypeName(type)) + " (" + std::to_string(type_byte) + ")");
    EXPECT_FALSE(reader_->RejectUnsupportedRuntimeEvent(type));
    EXPECT_NE(reader_->GetLastErrorCode(), ErrorCode::kMySQLUndecodableBinlogEvent);
  }
}

/**
 * @brief Every GTID-carrying event that fails to decode reconnects instead of being skipped.
 *
 * Skipping one leaves the received position at the previous transaction, so
 * the rows that follow would be attributed to it.
 */
TEST_F(BinlogReaderFixture, MalformedPositionEventsReconnectFromTheLastProcessedGtid) {
  using test::BinlogEventBuilder;
  const std::array<uint8_t, 16> uuid = {0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa,
                                        0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa, 0xaa};
  const std::vector<std::pair<const char*, std::vector<uint8_t>>> events = {
      {"GTID_LOG_EVENT", BinlogEventBuilder::BuildGtidEvent(uuid, 7)},
      {"GTID_TAGGED_LOG_EVENT", BinlogEventBuilder::BuildGtidTaggedEvent(uuid, 7, "tag")},
      {"MariaDB GTID_EVENT", BinlogEventBuilder::BuildMariaDBGtidEvent(0, 1, 7)},
      {"MariaDB GTID_LIST_EVENT", BinlogEventBuilder::BuildMariaDBGtidListEvent({{0, 1, 7}})},
  };

  for (const auto& [name, intact] : events) {
    SCOPED_TRACE(name);
    const auto type = static_cast<mybinlog::EventType>(intact[4]);
    EXPECT_EQ(reader_->ObservePositionEvent(type, intact.data(), intact.size()),
              BinlogReader::PositionEventAction::kContinue);
    const std::string received_before = reader_->position_state_.received_gtid();

    auto truncated = intact;
    truncated.resize(mygram::constants::kBinlogEventHeaderLen + 2);
    BinlogEventBuilder::FixEventSize(truncated);
    EXPECT_EQ(reader_->ObservePositionEvent(type, truncated.data(), truncated.size()),
              BinlogReader::PositionEventAction::kReconnect);
    EXPECT_EQ(reader_->GetLastErrorCode(), ErrorCode::kMySQLBinlogError) << reader_->GetLastError();
    EXPECT_NE(reader_->GetLastError().find(std::string("Malformed ") + name), std::string::npos)
        << reader_->GetLastError();
    EXPECT_EQ(reader_->position_state_.received_gtid(), received_before);
  }

  const auto query = BinlogEventBuilder::BuildQueryEvent("db", "BEGIN");
  EXPECT_EQ(reader_->ObservePositionEvent(mybinlog::EventType::kQueryEvent, query.data(), query.size()),
            BinlogReader::PositionEventAction::kNotPositionEvent);
}

/**
 * @brief A frame too short to hold a header and checksum reconnects instead of being skipped.
 *
 * Such a frame cannot be classified, so letting it through either drops it as
 * an unknown event or dispatches bytes no checksum has covered.
 */
TEST_F(BinlogReaderFixture, FramesShorterThanHeaderPlusChecksumFailVerification) {
  const std::vector<unsigned char> ok_byte_only(1, 0x00);
  const std::vector<unsigned char> twenty_bytes(20, 0x00);
  const uint64_t errors_before = reader_->crc_errors_.load();

  const auto verify = [this](const std::vector<unsigned char>& frame, size_t length) {
    const unsigned char* data = frame.data();
    size_t frame_length = length;
    return reader_->VerifyFetchedFrame(data, frame_length);
  };
  EXPECT_FALSE(verify(ok_byte_only, 0));
  EXPECT_FALSE(verify(twenty_bytes, twenty_bytes.size()));
  EXPECT_EQ(reader_->crc_errors_.load(), errors_before + 2);
  EXPECT_NE(reader_->GetLastErrorCode(), ErrorCode::kMySQLUndecodableBinlogEvent)
      << "a replay can deliver the frame intact, so it must not claim the event is undecodable";

  auto intact = test::BinlogEventBuilder::BuildQueryEvent("db", "BEGIN");
  const uint32_t crc = mygram::utils::ComputeCRC32(intact.data(), intact.size() - 4);
  for (size_t i = 0; i < 4; ++i) {
    intact[intact.size() - 4 + i] = static_cast<uint8_t>((crc >> (8 * i)) & 0xFFU);
  }
  EXPECT_TRUE(verify(std::vector<unsigned char>(intact.begin(), intact.end()), intact.size()));
  EXPECT_EQ(reader_->crc_errors_.load(), errors_before + 2);
}

#endif  // USE_MYSQL
