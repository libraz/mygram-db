/**
 * @file binlog_replay_watermark_test.cpp
 * @brief Recovery class of a SYNC replay fence that cannot be evaluated
 *
 * The reader path, the worker path and the fence-clearing path can all observe
 * one unevaluable fence. These tests pin them to a single recovery class, so an
 * operator is not told the schema is incompatible when the fence is simply a
 * GTID nothing can parse.
 */

#include <string>

#include "binlog_test_fixtures.h"

#ifdef USE_MYSQL

#include "mysql/gtid_encoder.h"

using namespace binlog_test;
using mygram::utils::ErrorCode;

namespace {

/** A fence value that neither GTID dialect can parse or compare. */
constexpr const char* kUnparsableWatermark = "not-a-gtid-set";

}  // namespace

/**
 * @brief The worker path stops without claiming a schema is incompatible.
 *
 * Latching the schema flag reports "Replication stopped due to an incompatible
 * schema" for a malformed GTID, which sends an operator after a mismatch that
 * does not exist and refuses a restart that would have worked.
 */
TEST_F(BinlogReaderFixture, WorkerStopsOnUnevaluableReplayWatermarkWithoutSchemaIncompatibility) {
  reader_->legacy_table_context_.replay_watermark->snapshot_gtid = kUnparsableWatermark;
  reader_->should_stop_.store(false, std::memory_order_release);

  const BinlogEvent event = MakeEvent(BinlogEventType::INSERT, "5", 1);
  EXPECT_FALSE(reader_->ProcessQueuedEvent(event));

  EXPECT_FALSE(reader_->HasSchemaIncompatibleError())
      << "an unparsable replay fence is not a schema condition: " << reader_->GetLastError();
  EXPECT_EQ(reader_->GetLastErrorCode(), ErrorCode::kMySQLInvalidGTID)
      << "the cause has to survive as its own code: " << reader_->GetLastError();
  EXPECT_NE(reader_->GetLastError().find("Cannot evaluate per-table SYNC replay watermark"), std::string::npos)
      << reader_->GetLastError();
  EXPECT_TRUE(reader_->should_stop_.load(std::memory_order_acquire))
      << "replaying the event would evaluate the same fence again";
}

/**
 * @brief Clearing a fence that cannot be compared fails closed instead of silently.
 *
 * A fence left in place keeps suppressing rows for its table, so swallowing the
 * comparison error drops replicated data with no error anywhere.
 */
TEST_F(BinlogReaderFixture, ClearingAnUnevaluableReplayWatermarkFailsClosed) {
  reader_->legacy_table_context_.replay_watermark->snapshot_gtid = kUnparsableWatermark;
  reader_->SetCurrentGTID("uuid:10");
  reader_->should_stop_.store(false, std::memory_order_release);

  const auto advanced = reader_->UpdateCurrentGTID("uuid:11");

  ASSERT_FALSE(advanced) << "an unevaluable fence was cleared silently";
  EXPECT_EQ(advanced.error().code(), ErrorCode::kMySQLInvalidGTID);
  EXPECT_EQ(reader_->GetLastErrorCode(), ErrorCode::kMySQLInvalidGTID);
  EXPECT_FALSE(reader_->HasSchemaIncompatibleError());
  EXPECT_TRUE(reader_->should_stop_.load(std::memory_order_acquire));
}

/**
 * @brief All three observers of one unevaluable fence report the same thing.
 *
 * The reader observes it through TABLE_MAP, the worker through a queued row or
 * DDL event, and the fence-clearing path through a commit. One root condition
 * has to produce one code, one message and one restartability.
 */
TEST_F(BinlogReaderFixture, EveryPathObservingOneUnevaluableFenceSharesOneRecoveryClass) {
  struct Observation {
    ErrorCode code;
    std::string message;
    bool schema_incompatible;
    bool stopped;
  };

  const auto observe = [this](const auto& trigger) {
    reader_->SetLastError(mygram::utils::Error{});
    reader_->schema_incompatible_.store(false, std::memory_order_release);
    reader_->should_stop_.store(false, std::memory_order_release);
    reader_->legacy_table_context_.replay_watermark->snapshot_gtid = kUnparsableWatermark;
    trigger();
    return Observation{reader_->GetLastErrorCode(), reader_->GetLastError(), reader_->HasSchemaIncompatibleError(),
                       reader_->should_stop_.load(std::memory_order_acquire)};
  };

  // The reader path: a TABLE_MAP for a table whose fence cannot be compared.
  const Observation reader_path = observe([this] {
    const auto fence = GtidEncoder::PositionCoversAuto("uuid:5", kUnparsableWatermark);
    ASSERT_FALSE(fence);
    reader_->FailClosedOnUnevaluableReplayWatermark(table_config_.name, "uuid:5", fence.error());
  });

  // The worker path: a queued row event for the same table.
  const Observation worker_path = observe([this] {
    const BinlogEvent event = MakeEvent(BinlogEventType::INSERT, "5", 1);
    EXPECT_FALSE(reader_->ProcessQueuedEvent(event));
  });

  // The fence-clearing path: a commit that advances the applied position.
  const Observation clearing_path = observe([this] {
    reader_->SetCurrentGTID("uuid:10");
    EXPECT_FALSE(reader_->UpdateCurrentGTID("uuid:11"));
  });

  EXPECT_EQ(reader_path.code, worker_path.code);
  EXPECT_EQ(reader_path.code, clearing_path.code);
  EXPECT_EQ(reader_path.message, worker_path.message);
  EXPECT_EQ(reader_path.message, clearing_path.message);
  EXPECT_FALSE(reader_path.schema_incompatible);
  EXPECT_FALSE(worker_path.schema_incompatible);
  EXPECT_FALSE(clearing_path.schema_incompatible);
  EXPECT_TRUE(reader_path.stopped);
  EXPECT_TRUE(worker_path.stopped);
  EXPECT_TRUE(clearing_path.stopped);
}

#endif  // USE_MYSQL
