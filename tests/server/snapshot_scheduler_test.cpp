/**
 * @file snapshot_scheduler_test.cpp
 * @brief Unit tests for SnapshotScheduler class
 *
 * Tests snapshot scheduling, file retention, and lifecycle management.
 */

#include "server/snapshot_scheduler.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>

#include "config/config.h"
#include "index/index.h"
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
};

// ===========================================================================
// Constructor and lifecycle tests
// ===========================================================================

TEST_F(SnapshotSchedulerTest, ConstructWithValidParams) {
  DumpConfig dump_config;
  dump_config.interval_sec = 60;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr);

  EXPECT_FALSE(scheduler.IsRunning());
}

TEST_F(SnapshotSchedulerTest, StartAndStop) {
  DumpConfig dump_config;
  dump_config.interval_sec = 60;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr);

  scheduler.Start();
  EXPECT_TRUE(scheduler.IsRunning());

  scheduler.Stop();
  EXPECT_FALSE(scheduler.IsRunning());
}

TEST_F(SnapshotSchedulerTest, DoubleStartIsIdempotent) {
  DumpConfig dump_config;
  dump_config.interval_sec = 60;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr);

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

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr);

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
    SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr);
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

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr);

  scheduler.Start();
  // Should not start thread when interval is 0
  EXPECT_FALSE(scheduler.IsRunning());
}

TEST_F(SnapshotSchedulerTest, DisabledWithNegativeInterval) {
  DumpConfig dump_config;
  dump_config.interval_sec = -1;  // Disabled
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr);

  scheduler.Start();
  EXPECT_FALSE(scheduler.IsRunning());
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

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr);

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

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr);

  // Files should still exist (retain=0 means no cleanup)
  EXPECT_TRUE(std::filesystem::exists(test_dir_ / "auto_20240101_120000.dmp"));
  EXPECT_TRUE(std::filesystem::exists(test_dir_ / "auto_20240102_120000.dmp"));
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

  SnapshotScheduler scheduler(dump_config, empty_catalog.get(), &full_config_, test_dir_.string(), nullptr);

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
  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, non_existent.string(), nullptr);

  EXPECT_FALSE(scheduler.IsRunning());
}

TEST_F(SnapshotSchedulerTest, StopWithoutStart) {
  DumpConfig dump_config;
  dump_config.interval_sec = 60;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr);

  // Stop without start should be safe
  scheduler.Stop();
  EXPECT_FALSE(scheduler.IsRunning());
}

// ===========================================================================
// Concurrency tests
// ===========================================================================

TEST_F(SnapshotSchedulerTest, StartStopRapidly) {
  DumpConfig dump_config;
  dump_config.interval_sec = 60;
  dump_config.retain = 3;

  SnapshotScheduler scheduler(dump_config, catalog_.get(), &full_config_, test_dir_.string(), nullptr);

  // Rapid start/stop cycles
  for (int i = 0; i < 5; ++i) {
    scheduler.Start();
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    scheduler.Stop();
  }

  EXPECT_FALSE(scheduler.IsRunning());
}
