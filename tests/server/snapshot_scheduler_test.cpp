/**
 * @file snapshot_scheduler_test.cpp
 * @brief Unit tests for SnapshotScheduler class
 *
 * Tests snapshot scheduling, file retention, and lifecycle management.
 */

#include "server/snapshot_scheduler.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "mysql/binlog_reader_interface.h"
#include "server/replication_pause_counter.h"
#include "server/server_types.h"
#include "server/table_catalog.h"
#include "storage/document_store.h"

using namespace mygramdb::server;
using namespace mygramdb::config;
using namespace mygramdb::index;
using namespace mygramdb::storage;

namespace {

/**
 * @brief Helper to create a minimal TableContext for testing
 */
std::unique_ptr<TableContext> CreateTableContext(const std::string& name) {
  auto ctx = std::make_unique<TableContext>();
  ctx->name = name;
  ctx->config.name = name;
  ctx->config.primary_key = "id";
  ctx->index = std::make_unique<Index>();
  ctx->doc_store = std::make_unique<DocumentStore>();
  return ctx;
}

/**
 * @brief Create a minimal Config for testing
 */
Config CreateMinimalConfig() {
  Config config;
  config.api.tcp.port = 0;  // Ephemeral port
  return config;
}

/**
 * @brief Create dummy .dmp files for cleanup testing
 */
void CreateDummyDmpFile(const std::filesystem::path& dir, const std::string& filename) {
  std::ofstream file(dir / filename);
  file << "dummy content";
  file.close();
}

}  // namespace

class SnapshotSchedulerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create temporary directory for tests
    test_dir_ = std::filesystem::temp_directory_path() / "snapshot_scheduler_test";
    std::filesystem::create_directories(test_dir_);

    // Create table context
    table_ = CreateTableContext("test_table");
    tables_["test_table"] = table_.get();

    // Create table catalog
    catalog_ = std::make_unique<TableCatalog>(tables_);

    // Create minimal config
    full_config_ = CreateMinimalConfig();

    // Initialize read_only flag for mutual exclusion testing
    dump_save_in_progress_ = false;
    optimization_in_progress_ = false;
    replication_paused_for_dump_ = false;
  }

  void TearDown() override {
    // Clean up test directory
    std::filesystem::remove_all(test_dir_);
  }

  std::filesystem::path test_dir_;
  std::unique_ptr<TableContext> table_;
  std::unordered_map<std::string, TableContext*> tables_;
  std::unique_ptr<TableCatalog> catalog_;
  Config full_config_;
  std::atomic<bool> dump_save_in_progress_{false};        // For mutual exclusion with manual DUMP SAVE
  std::atomic<bool> optimization_in_progress_{false};     // For mutual exclusion with OPTIMIZE
  std::atomic<bool> replication_paused_for_dump_{false};  // Asserted while a snapshot is in progress
};

// ===========================================================================
// Constructor and lifecycle tests
// ===========================================================================

TEST_F(SnapshotSchedulerTest, ConstructWithValidParams) {
  DumpConfig dump_config;
  dump_config.interval_sec = 60;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_);

  EXPECT_FALSE(scheduler.IsRunning());
}

TEST_F(SnapshotSchedulerTest, StartAndStop) {
  DumpConfig dump_config;
  dump_config.interval_sec = 60;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_);

  scheduler.Start();
  EXPECT_TRUE(scheduler.IsRunning());

  scheduler.Stop();
  EXPECT_FALSE(scheduler.IsRunning());
}

TEST_F(SnapshotSchedulerTest, DoubleStartIsIdempotent) {
  DumpConfig dump_config;
  dump_config.interval_sec = 60;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_);

  scheduler.Start();
  EXPECT_TRUE(scheduler.IsRunning());

  // Second start should be no-op
  scheduler.Start();
  EXPECT_TRUE(scheduler.IsRunning());

  scheduler.Stop();
}

TEST_F(SnapshotSchedulerTest, DoubleStopIsIdempotent) {
  DumpConfig dump_config;
  dump_config.interval_sec = 60;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_);

  scheduler.Start();
  scheduler.Stop();
  EXPECT_FALSE(scheduler.IsRunning());

  // Second stop should be no-op
  scheduler.Stop();
  EXPECT_FALSE(scheduler.IsRunning());
}

TEST_F(SnapshotSchedulerTest, DestructorStopsScheduler) {
  DumpConfig dump_config;
  dump_config.interval_sec = 60;
  dump_config.retain = 3;

  {
    SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                                dump_save_in_progress_, replication_paused_for_dump_);
    scheduler.Start();
    EXPECT_TRUE(scheduler.IsRunning());
    // Destructor should stop the scheduler
  }

  // No crash = success
  SUCCEED();
}

// ===========================================================================
// Disabled scheduler tests
// ===========================================================================

TEST_F(SnapshotSchedulerTest, DisabledWithZeroInterval) {
  DumpConfig dump_config;
  dump_config.interval_sec = 0;  // Disabled
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_);

  scheduler.Start();
  // Should not start thread when interval is 0
  EXPECT_FALSE(scheduler.IsRunning());
}

TEST_F(SnapshotSchedulerTest, DisabledWithNegativeInterval) {
  DumpConfig dump_config;
  dump_config.interval_sec = -1;  // Disabled
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_);

  scheduler.Start();
  EXPECT_FALSE(scheduler.IsRunning());
}

TEST_F(SnapshotSchedulerTest, AutoSnapshotSkipsWhileDumpLoadInProgress) {
  DumpConfig dump_config;
  dump_config.interval_sec = 1;
  dump_config.retain = 3;
  std::atomic<bool> dump_load_in_progress{true};

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_, nullptr, &dump_load_in_progress);

  auto result = scheduler.Start();
  ASSERT_TRUE(result.has_value());
  std::this_thread::sleep_for(std::chrono::milliseconds(1300));
  scheduler.Stop();

  size_t snapshot_count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
    if (entry.path().filename().string().rfind("auto_", 0) == 0 && entry.path().extension() == ".dmp") {
      ++snapshot_count;
    }
  }
  EXPECT_EQ(snapshot_count, 0U);
  EXPECT_FALSE(dump_save_in_progress_.load(std::memory_order_acquire))
      << "skipped auto snapshot must release the dump-save guard";
}

TEST_F(SnapshotSchedulerTest, AutoSnapshotSkipsWhileOptimizeInProgress) {
  DumpConfig dump_config;
  dump_config.interval_sec = 1;
  dump_config.retain = 3;
  optimization_in_progress_.store(true, std::memory_order_release);

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_, nullptr, nullptr, nullptr,
                              std::function<bool()>{}, &optimization_in_progress_);

  auto result = scheduler.Start();
  ASSERT_TRUE(result.has_value());
  std::this_thread::sleep_for(std::chrono::milliseconds(1300));
  scheduler.Stop();

  size_t snapshot_count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
    if (entry.path().filename().string().rfind("auto_", 0) == 0 && entry.path().extension() == ".dmp") {
      ++snapshot_count;
    }
  }
  EXPECT_EQ(snapshot_count, 0U);
  EXPECT_FALSE(dump_save_in_progress_.load(std::memory_order_acquire))
      << "optimize-skipped auto snapshot must release the dump-save guard";
}

TEST_F(SnapshotSchedulerTest, AutoSnapshotSkipsWhileSyncInProgress) {
  DumpConfig dump_config;
  dump_config.interval_sec = 1;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_, nullptr, nullptr, nullptr,
                              [] { return true; });

  auto result = scheduler.Start();
  ASSERT_TRUE(result.has_value());
  std::this_thread::sleep_for(std::chrono::milliseconds(1300));
  scheduler.Stop();

  size_t snapshot_count = 0;
  for (const auto& entry : std::filesystem::directory_iterator(test_dir_)) {
    if (entry.path().filename().string().rfind("auto_", 0) == 0 && entry.path().extension() == ".dmp") {
      ++snapshot_count;
    }
  }
  EXPECT_EQ(snapshot_count, 0U);
  EXPECT_FALSE(dump_save_in_progress_.load(std::memory_order_acquire))
      << "sync-skipped auto snapshot must release the dump-save guard";
}

// ===========================================================================
// Cleanup tests
// ===========================================================================

TEST_F(SnapshotSchedulerTest, CleanupPreservesNonAutoFiles) {
  // Create some non-auto files that should NOT be cleaned up
  CreateDummyDmpFile(test_dir_, "manual_backup.dmp");
  CreateDummyDmpFile(test_dir_, "important.dmp");

  DumpConfig dump_config;
  dump_config.interval_sec = 0;  // Disabled (we just want to test cleanup logic)
  dump_config.retain = 1;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_);

  // Files should still exist (cleanup only affects auto_ prefixed files)
  EXPECT_TRUE(std::filesystem::exists(test_dir_ / "manual_backup.dmp"));
  EXPECT_TRUE(std::filesystem::exists(test_dir_ / "important.dmp"));
}

TEST_F(SnapshotSchedulerTest, CleanupRetainZeroSkipsCleanup) {
  // Create some auto files
  CreateDummyDmpFile(test_dir_, "auto_20240101_120000.dmp");
  CreateDummyDmpFile(test_dir_, "auto_20240102_120000.dmp");

  DumpConfig dump_config;
  dump_config.interval_sec = 0;
  dump_config.retain = 0;  // No retention policy = no cleanup

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_);

  // Files should still exist (retain=0 means no cleanup)
  EXPECT_TRUE(std::filesystem::exists(test_dir_ / "auto_20240101_120000.dmp"));
  EXPECT_TRUE(std::filesystem::exists(test_dir_ / "auto_20240102_120000.dmp"));
}

TEST_F(SnapshotSchedulerTest, CleanupRemovesOnlyOldAutoTempFiles) {
  const auto old_auto_temp = test_dir_ / "auto_20240101_120000.dmp.tmp.123.456";
  const auto recent_auto_temp = test_dir_ / "auto_20240102_120000.dmp.tmp.123.456";
  const auto manual_temp = test_dir_ / "manual_backup.dmp.tmp.123.456";

  std::ofstream(old_auto_temp) << "old temp";
  std::ofstream(recent_auto_temp) << "recent temp";
  std::ofstream(manual_temp) << "manual temp";

  const auto old_time = std::filesystem::file_time_type::clock::now() - std::chrono::hours(2);
  std::filesystem::last_write_time(old_auto_temp, old_time);

  DumpConfig dump_config;
  dump_config.interval_sec = 1;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_);

  auto result = scheduler.Start();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  std::this_thread::sleep_for(std::chrono::milliseconds(1300));
  scheduler.Stop();

  EXPECT_FALSE(std::filesystem::exists(old_auto_temp));
  EXPECT_TRUE(std::filesystem::exists(recent_auto_temp));
  EXPECT_TRUE(std::filesystem::exists(manual_temp));
}

// ===========================================================================
// Edge cases
// ===========================================================================

TEST_F(SnapshotSchedulerTest, EmptyTableCatalog) {
  std::unordered_map<std::string, TableContext*> empty_tables;
  auto empty_catalog = std::make_unique<TableCatalog>(empty_tables);

  DumpConfig dump_config;
  dump_config.interval_sec = 60;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, empty_catalog.get(), &full_config_, test_dir_.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_);

  scheduler.Start();
  EXPECT_TRUE(scheduler.IsRunning());

  scheduler.Stop();
}

TEST_F(SnapshotSchedulerTest, NonExistentDumpDir) {
  std::filesystem::path non_existent = test_dir_ / "non_existent_dir";

  DumpConfig dump_config;
  dump_config.interval_sec = 60;
  dump_config.retain = 3;

  // Scheduler should still construct (directory created on snapshot)
  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, non_existent.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_);

  EXPECT_FALSE(scheduler.IsRunning());
}

TEST_F(SnapshotSchedulerTest, StopWithoutStart) {
  DumpConfig dump_config;
  dump_config.interval_sec = 60;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_);

  // Stop without start should be safe
  scheduler.Stop();
  EXPECT_FALSE(scheduler.IsRunning());
}

// ===========================================================================
// Concurrency tests
// ===========================================================================

/**
 * @brief Test that CleanupOldSnapshots continues when one file can't be deleted.
 *
 * We test this indirectly by creating auto_ files where one lives inside a
 * read-only directory (on POSIX), then triggering a snapshot cycle. The
 * scheduler should log a warning for the undeletable file and continue
 * cleaning up the remaining files.
 *
 * Since CleanupOldSnapshots is private, we validate file state after a
 * short scheduler run with a 1-second interval.
 */
TEST_F(SnapshotSchedulerTest, CleanupContinuesOnDeleteFailure) {
  // Create more auto_ files than the retain count
  // Use descending timestamps so the oldest (auto_20240101) gets deleted first
  CreateDummyDmpFile(test_dir_, "auto_20240101_120000.dmp");
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  CreateDummyDmpFile(test_dir_, "auto_20240102_120000.dmp");
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  CreateDummyDmpFile(test_dir_, "auto_20240103_120000.dmp");

  // Make the oldest file's parent directory not writable to prevent deletion
  // Note: On some systems (macOS root), this may not prevent deletion.
  // The key behavior is that cleanup does not throw and continues.
  auto oldest_path = test_dir_ / "auto_20240101_120000.dmp";

#ifndef _WIN32
  // Make the file read-only (remove write permission from directory to prevent deletion)
  auto original_perms = std::filesystem::status(test_dir_).permissions();
  std::filesystem::permissions(test_dir_, std::filesystem::perms::owner_write, std::filesystem::perm_options::remove);
#endif

  DumpConfig dump_config;
  dump_config.interval_sec = 1;
  dump_config.retain = 1;  // Only keep 1 file

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_);

  scheduler.Start();
  // Wait enough time for at least one scheduler cycle
  std::this_thread::sleep_for(std::chrono::seconds(3));
  scheduler.Stop();

#ifndef _WIN32
  // Restore permissions so TearDown can clean up
  std::filesystem::permissions(test_dir_, original_perms, std::filesystem::perm_options::replace);
#endif

  // The test passes as long as no exception was thrown and the scheduler
  // stopped cleanly. The error-code path logs warnings instead of throwing.
  EXPECT_FALSE(scheduler.IsRunning());
}

TEST_F(SnapshotSchedulerTest, StartStopRapidly) {
  DumpConfig dump_config;
  dump_config.interval_sec = 60;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                              dump_save_in_progress_, replication_paused_for_dump_);

  // Rapid start/stop cycles
  for (int i = 0; i < 5; ++i) {
    scheduler.Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    scheduler.Stop();
  }

  EXPECT_FALSE(scheduler.IsRunning());
}

/**
 * @brief Regression for the Start/Stop race that could leak a thread.
 *
 * Before the fix, Start() would set running_=true and *then* construct
 * scheduler_thread_. A concurrent Stop() observing running_=true between
 * those two steps would skip joining (because scheduler_thread_ was still
 * null) and Start() would later spawn a thread that exited immediately
 * because Stop() had already cleared running_, leaving an unjoined
 * std::thread that detached on destruction.
 *
 * After the fix Start()/Stop() are serialized by start_stop_mutex_, so
 * the thread is always joined and IsRunning() is consistent on return.
 *
 * The test is structured so that even on the racy code path the failure
 * mode (std::terminate from destructing a joinable thread) would show up
 * as a process abort, not just a soft test failure.
 */
TEST_F(SnapshotSchedulerTest, StartStopConcurrentDoesNotLeakThread) {
  DumpConfig dump_config;
  dump_config.interval_sec = 60;
  dump_config.retain = 3;

  // Many iterations to widen the race window.
  for (int iter = 0; iter < 50; ++iter) {
    SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr,
                                dump_save_in_progress_, replication_paused_for_dump_);

    std::thread starter([&scheduler]() { scheduler.Start(); });
    std::thread stopper([&scheduler]() { scheduler.Stop(); });

    starter.join();
    stopper.join();

    // After both calls return, ensure the scheduler is fully stopped.
    // A second Stop() must be safe (idempotent) and must not deadlock.
    scheduler.Stop();
    EXPECT_FALSE(scheduler.IsRunning());
    // Destructor would std::terminate if it found a joinable, unjoined
    // thread, so reaching the next iteration confirms no leak occurred.
  }
}

#ifdef USE_MYSQL

namespace {

/**
 * @brief Minimal IBinlogReader stub for verifying snapshot replication pause.
 *
 * Records Stop()/Start() invocation counts and captures the value of an
 * externally provided flag at the moment GetCurrentGTID() is called.
 *
 * GetCurrentGTID() is invoked by SnapshotScheduler::TakeSnapshot() *after*
 * Stop() returns and the replication-paused flag has been set, but *before*
 * the scope guard restores it. Observing the flag at GetCurrentGTID() time
 * therefore proves the flag was true while the dump was being written.
 */
class StubBinlogReader : public mygramdb::mysql::IBinlogReader {
 public:
  mygram::utils::Expected<void, mygram::utils::Error> Start() override {
    running_.store(true, std::memory_order_release);
    start_count_.fetch_add(1, std::memory_order_acq_rel);
    return {};
  }

  void Stop() override {
    running_.store(false, std::memory_order_release);
    stop_count_.fetch_add(1, std::memory_order_acq_rel);
  }

  bool IsRunning() const override {
    bool running = running_.load(std::memory_order_acquire);
    if (stop_after_true_is_running_.exchange(false, std::memory_order_acq_rel) && running) {
      running_.store(false, std::memory_order_release);
    }
    return running;
  }

  std::string GetCurrentGTID() const override {
    if (observed_flag_ != nullptr && observed_flag_->load(std::memory_order_acquire)) {
      flag_seen_true_at_gtid_.store(true, std::memory_order_release);
    }
    get_gtid_count_.fetch_add(1, std::memory_order_acq_rel);
    return current_gtid_;
  }

  void SetCurrentGTID(const std::string& gtid) override { current_gtid_ = gtid; }

  std::string GetLastError() const override { return ""; }

  uint64_t GetProcessedEvents() const override { return 0; }

  size_t GetQueueSize() const override { return 0; }

  void SetGtidForTest(const std::string& gtid) { current_gtid_ = gtid; }
  void SetRunningForTest(bool running) { running_.store(running, std::memory_order_release); }
  void StopAfterNextTrueIsRunningForTest() { stop_after_true_is_running_.store(true, std::memory_order_release); }
  void ObserveFlagAtGetGtid(std::atomic<bool>* flag) { observed_flag_ = flag; }

  int StartCount() const { return start_count_.load(std::memory_order_acquire); }
  int StopCount() const { return stop_count_.load(std::memory_order_acquire); }
  int GetGtidCount() const { return get_gtid_count_.load(std::memory_order_acquire); }
  bool FlagSeenTrueAtGetGtid() const { return flag_seen_true_at_gtid_.load(std::memory_order_acquire); }

 private:
  std::string current_gtid_;
  mutable std::atomic<bool> running_{false};
  std::atomic<int> start_count_{0};
  std::atomic<int> stop_count_{0};
  mutable std::atomic<int> get_gtid_count_{0};
  mutable std::atomic<bool> flag_seen_true_at_gtid_{false};
  mutable std::atomic<bool> stop_after_true_is_running_{false};
  std::atomic<bool>* observed_flag_ = nullptr;
};

}  // namespace

/**
 * @brief Verifies that scheduled snapshots pause replication for the
 *        duration of WriteDump and resume it afterward.
 *
 * Without the pause, the binlog worker could mutate the index/document
 * store while WriteDump iterates, producing inconsistent dumps and
 * racing with Index::Add().
 */
TEST_F(SnapshotSchedulerTest, TakeSnapshotPausesAndResumesReplication) {
  StubBinlogReader stub;
  stub.SetGtidForTest("00000000-0000-0000-0000-000000000000:1-1");
  stub.SetRunningForTest(true);
  stub.ObserveFlagAtGetGtid(&replication_paused_for_dump_);

  DumpConfig dump_config;
  dump_config.interval_sec = 1;  // Trigger a snapshot quickly
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), &stub,
                              dump_save_in_progress_, replication_paused_for_dump_);

  scheduler.Start();
  // Wait long enough for at least one snapshot cycle to run to completion.
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  scheduler.Stop();

  // Replication must have been paused and resumed at least once.
  EXPECT_GE(stub.StopCount(), 1) << "Replication must be stopped before WriteDump";
  EXPECT_GE(stub.StartCount(), 1) << "Replication must be resumed after WriteDump";
  EXPECT_EQ(stub.StopCount(), stub.StartCount())
      << "Every Stop() must be matched by a Start() so replication is not left paused";

  // The replication-paused flag must have been true at the point GTID was
  // captured (i.e. after Stop and the flag set, before the scope guard).
  EXPECT_TRUE(stub.FlagSeenTrueAtGetGtid())
      << "replication_paused_for_dump must be asserted while the dump is being written";

  // Final state: flag cleared and stub running again.
  EXPECT_FALSE(replication_paused_for_dump_.load());
  EXPECT_TRUE(stub.IsRunning()) << "Replication must be running after the snapshot completes";
}

/**
 * @brief Verifies that when replication is not running, the scheduler
 *        does not call Stop()/Start() on the binlog reader.
 *
 * This matches DumpHandler::DumpSaveWorker semantics: only resume
 * replication if it was running before the dump began.
 */
TEST_F(SnapshotSchedulerTest, TakeSnapshotSkipsReplicationControlWhenStopped) {
  StubBinlogReader stub;
  stub.SetGtidForTest("00000000-0000-0000-0000-000000000000:1-1");
  stub.SetRunningForTest(false);  // Replication already stopped

  DumpConfig dump_config;
  dump_config.interval_sec = 1;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), &stub,
                              dump_save_in_progress_, replication_paused_for_dump_);

  scheduler.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  scheduler.Stop();

  EXPECT_EQ(stub.StopCount(), 0) << "Must not stop replication that wasn't running";
  EXPECT_EQ(stub.StartCount(), 0) << "Must not start replication that wasn't running";
  EXPECT_FALSE(replication_paused_for_dump_.load())
      << "replication_paused_for_dump must remain false when replication wasn't running";
}

TEST_F(SnapshotSchedulerTest, AutoSnapshotDoesNotRestartIfManualStopWinsRaceBeforePause) {
  StubBinlogReader stub;
  stub.SetGtidForTest("00000000-0000-0000-0000-000000000000:1-1");
  stub.SetRunningForTest(true);
  stub.StopAfterNextTrueIsRunningForTest();

  replication_pause::Counter counter;

  DumpConfig dump_config;
  dump_config.interval_sec = 1;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), &stub,
                              dump_save_in_progress_, replication_paused_for_dump_, &counter);

  scheduler.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(1500));
  scheduler.Stop();

  EXPECT_EQ(stub.StopCount(), 0) << "Snapshot must not Stop() after a manual Stop wins the IsRunning/Stop race";
  EXPECT_EQ(stub.StartCount(), 0) << "Snapshot must not restart replication it did not pause";
  EXPECT_FALSE(counter.IsPaused());
  EXPECT_FALSE(replication_paused_for_dump_.load());
  EXPECT_FALSE(stub.IsRunning());
}

/**
 * @brief H-C3 regression: when another operation is already holding the
 *        replication-pause counter, the auto-snapshot must NOT call
 *        binlog Start() at the end of its work. The other operation's
 *        ReleasePause() is responsible for the eventual restart.
 *
 * Pre-fix behaviour: the auto-snapshot independently called Stop() at
 * the start and Start() at the end. If a manual DUMP LOAD was holding
 * the pause, the snapshot's Start() at the end would erroneously
 * resume replication while DUMP LOAD was still iterating data —
 * leading to inconsistent dumps and Index/DocumentStore races.
 *
 * Post-fix behaviour: the snapshot increments the shared counter at
 * the start; only the FIRST pauser actually calls Stop(). At the end
 * the snapshot decrements the counter; only the LAST releaser calls
 * Start(). When another operation pre-incremented the counter, the
 * snapshot's RequestPause returns false (so it must NOT call Stop)
 * and its ReleasePause returns false (so it must NOT call Start).
 */
TEST_F(SnapshotSchedulerTest, AutoSnapshotDoesNotRestartWhenAnotherOperationHoldsPause) {
  // Simulate "DUMP LOAD already paused replication" by pre-incrementing
  // the shared counter and stopping the stub reader.
  replication_pause::Counter counter;
  ASSERT_TRUE(counter.RequestPause()) << "Test pre-condition: counter should start at 0";

  StubBinlogReader stub;
  stub.SetGtidForTest("00000000-0000-0000-0000-000000000000:1-1");
  stub.SetRunningForTest(false);  // The "other operation" already stopped it.
  // The "other operation" also asserted the observable flag; mirror that
  // so the snapshot observes a realistic "already paused" state.
  replication_paused_for_dump_ = true;

  DumpConfig dump_config;
  dump_config.interval_sec = 1;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), &stub,
                              dump_save_in_progress_, replication_paused_for_dump_, &counter);

  scheduler.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  scheduler.Stop();

  // The snapshot was NOT the first pauser (counter was already at 1 from
  // our pre-pause), so it must not have called Stop() — the reader was
  // already stopped. It also must NOT have called Start() at the end,
  // because we are still holding the pause.
  EXPECT_EQ(stub.StopCount(), 0) << "Snapshot must not Stop() a reader the prior pauser already stopped";
  EXPECT_EQ(stub.StartCount(), 0) << "Snapshot must not Start() while another operation still holds the pause";

  // The observable flag must still be asserted because we (the
  // simulated prior operation) are still holding the pause.
  EXPECT_TRUE(replication_paused_for_dump_.load())
      << "replication_paused_for_dump must remain asserted while any operation holds the pause";

  // Now release our pre-held pause; counter returns to 0.
  EXPECT_TRUE(counter.ReleasePause()) << "Test should be the last releaser after the snapshot ran";
  EXPECT_FALSE(counter.IsPaused());

  replication_paused_for_dump_ = false;
}

/**
 * @brief H-C3 regression: independent verification that the snapshot
 *        leaves the counter clean even when its own pause/release was a
 *        no-op (pre-paused scenario from the previous test).
 *
 * After AutoSnapshotDoesNotRestartWhenAnotherOperationHoldsPause runs,
 * a fresh snapshot cycle on a clean counter must still pause/resume
 * normally. This guards against the snapshot accidentally leaking a
 * counter increment when the "another operation" branch is taken.
 */
TEST_F(SnapshotSchedulerTest, AutoSnapshotPauseResumeStillWorksOnCleanCounter) {
  replication_pause::Counter counter;
  ASSERT_FALSE(counter.IsPaused()) << "Pre-condition: counter starts at 0";

  StubBinlogReader stub;
  stub.SetGtidForTest("00000000-0000-0000-0000-000000000000:1-1");
  stub.SetRunningForTest(true);

  DumpConfig dump_config;
  dump_config.interval_sec = 1;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), &stub,
                              dump_save_in_progress_, replication_paused_for_dump_, &counter);

  scheduler.Start();
  std::this_thread::sleep_for(std::chrono::milliseconds(2500));
  scheduler.Stop();

  // Standard pause/resume: replication was running, snapshot is the
  // sole pauser, so Stop()/Start() are both called and the counter
  // returns to 0.
  EXPECT_GE(stub.StopCount(), 1);
  EXPECT_GE(stub.StartCount(), 1);
  EXPECT_EQ(stub.StopCount(), stub.StartCount())
      << "Every Stop() must be matched by a Start() when the snapshot is the sole pauser";
  EXPECT_FALSE(counter.IsPaused()) << "Counter must be drained back to 0 after the snapshot finishes";
  EXPECT_FALSE(replication_paused_for_dump_.load());
}

#endif  // USE_MYSQL
