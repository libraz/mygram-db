/**
 * @file server_lifecycle_manager_test.cpp
 * @brief Unit tests for ServerLifecycleManager
 */

#include "server/server_lifecycle_manager.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <optional>
#include <utility>

#include "config/config.h"
#include "index/index.h"
#include "storage/document_store.h"
#include "utils/atomic_file_writer.h"

#ifdef USE_MYSQL
#include "server/sync_operation_manager.h"
#endif

using namespace mygramdb::server;
using namespace mygramdb;

namespace {

using TrackedDumpTempPaths = std::pair<std::filesystem::path, std::filesystem::path>;

std::optional<TrackedDumpTempPaths> CreateAgedDeadTrackedTemp(const std::filesystem::path& directory,
                                                              const std::string& final_name) {
  std::filesystem::create_directories(directory);
  int path_pipe[2] = {-1, -1};
  if (::pipe(path_pipe) != 0) {
    ADD_FAILURE() << "failed to create temp-path pipe";
    return std::nullopt;
  }

  const pid_t child_pid = ::fork();
  if (child_pid < 0) {
    ::close(path_pipe[0]);
    ::close(path_pipe[1]);
    ADD_FAILURE() << "failed to fork dump-temp owner process";
    return std::nullopt;
  }
  if (child_pid == 0) {
    ::close(path_pipe[0]);
    mygramdb::utils::AtomicFileWriter writer((directory / final_name).string(), true);
    const int fd = ::open(writer.GetTempPath().c_str(), O_CREAT | O_EXCL | O_WRONLY, 0600);
    if (fd < 0) {
      _exit(2);
    }
    const auto tracked = writer.TrackDumpTempFile(fd);
    ::close(fd);
    if (!tracked) {
      _exit(3);
    }
    const std::string temp_path = writer.GetTempPath();
    if (::write(path_pipe[1], temp_path.c_str(), temp_path.size() + 1) != static_cast<ssize_t>(temp_path.size() + 1)) {
      _exit(4);
    }
    ::close(path_pipe[1]);
    _exit(0);  // Leave the marker and temp file as a real dead-owner pair.
  }

  ::close(path_pipe[1]);
  std::array<char, 4096> temp_path_buffer{};
  size_t bytes_read = 0;
  while (bytes_read < temp_path_buffer.size() && std::memchr(temp_path_buffer.data(), '\0', bytes_read) == nullptr) {
    const ssize_t count =
        ::read(path_pipe[0], temp_path_buffer.data() + bytes_read, temp_path_buffer.size() - bytes_read);
    if (count <= 0) {
      break;
    }
    bytes_read += static_cast<size_t>(count);
  }
  ::close(path_pipe[0]);

  int child_status = 0;
  if (::waitpid(child_pid, &child_status, 0) != child_pid || !WIFEXITED(child_status) ||
      WEXITSTATUS(child_status) != 0 || bytes_read == 0 ||
      std::memchr(temp_path_buffer.data(), '\0', bytes_read) == nullptr) {
    ADD_FAILURE() << "dump-temp owner process did not leave a tracked temp file";
    return std::nullopt;
  }

  const std::filesystem::path temp_path(temp_path_buffer.data());
  std::filesystem::path marker_path;
  for (const auto& entry : std::filesystem::directory_iterator(directory)) {
    const auto basename = entry.path().filename().string();
    if (basename.compare(0, mygramdb::utils::kDumpTempOwnerMarkerPrefix.size(),
                         mygramdb::utils::kDumpTempOwnerMarkerPrefix) == 0) {
      marker_path = entry.path();
      break;
    }
  }
  if (marker_path.empty()) {
    ADD_FAILURE() << "TrackDumpTempFile did not create an owner marker";
    return std::nullopt;
  }

  const auto old_time = std::filesystem::file_time_type::clock::now() - std::chrono::hours(2);
  std::filesystem::last_write_time(temp_path, old_time);
  std::filesystem::last_write_time(marker_path, old_time);
  return TrackedDumpTempPaths{temp_path, marker_path};
}

}  // namespace

/**
 * @brief Test fixture for ServerLifecycleManager tests
 */
class ServerLifecycleManagerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create minimal table context
    auto index = std::make_unique<index::Index>(1);
    auto doc_store = std::make_unique<storage::DocumentStore>();

    table_context_.name = "test_table";
    table_context_.config.ngram_size = 1;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);

    table_contexts_["test_table"] = &table_context_;

    // Configure server (port 0 = let OS assign)
    server_config_.port = 0;
    server_config_.host = "127.0.0.1";
    server_config_.worker_threads = 2;

    // Configure full config with optional components disabled by default
    full_config_.cache.enabled = false;
    full_config_.dump.interval_sec = 0;  // Disable scheduler

    const auto* test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    const auto dump_path =
        std::filesystem::temp_directory_path() /
        ("server_lifecycle_manager_" + std::string(test_info->name()) + "_" + std::to_string(getpid()));
    std::filesystem::remove_all(dump_path);
    std::filesystem::create_directories(dump_path);
    dump_dir_ = dump_path.string();

#ifdef USE_MYSQL
    // Create SyncOperationManager (required for SyncHandler)
    sync_manager_ = std::make_unique<SyncOperationManager>(table_contexts_, &full_config_, nullptr);
#endif
  }

  void TearDown() override {
    // Cleanup handled by unique_ptr destructors; keep this fixture's dump
    // directory private so parallel lifecycle cases cannot sweep each other's
    // files.
    std::filesystem::remove_all(dump_dir_);
  }

  // Helper: Create lifecycle manager with current config
  std::unique_ptr<ServerLifecycleManager> CreateManager(bool null_full_config = false) {
    const config::Config* full_config = null_full_config ? nullptr : &full_config_;
    auto result = ServerLifecycleManager::Create(
        server_config_, table_contexts_, dump_dir_, full_config, stats_, dump_load_in_progress_, dump_save_in_progress_,
        optimization_in_progress_, replication_paused_for_dump_, replication_pause_counter_,
        nullptr  // binlog_reader
#ifdef USE_MYSQL
        ,
        sync_manager_.get()
#endif
    );
    EXPECT_TRUE(result) << "Create failed: " << result.error().to_string();
    return std::move(*result);
  }

  // Test data
  ServerConfig server_config_;
  config::Config full_config_;
  std::string dump_dir_;
  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;

  // Server state (owned by TcpServer in production)
  ServerStats stats_;
  std::atomic<bool> dump_load_in_progress_{false};
  std::atomic<bool> dump_save_in_progress_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::atomic<bool> replication_paused_for_dump_{false};
  replication_pause::Counter replication_pause_counter_;

#ifdef USE_MYSQL
  std::unique_ptr<SyncOperationManager> sync_manager_;
#endif
};

// ===== P0 Tests (Must-Have) =====

/**
 * @test Initialize_Success_AllRequiredComponentsCreated
 * @brief Verify complete initialization creates all required components
 */
TEST_F(ServerLifecycleManagerTest, Initialize_Success_AllRequiredComponentsCreated) {
  auto manager = CreateManager();

  auto result = manager->Initialize();
  ASSERT_TRUE(result) << "Initialize failed: " << result.error().to_string();

  auto& components = *result;

  // Required components
  EXPECT_NE(components.thread_pool, nullptr);
  EXPECT_NE(components.table_catalog, nullptr);
  EXPECT_NE(components.handler_context, nullptr);
  EXPECT_NE(components.dispatcher, nullptr);
  EXPECT_NE(components.acceptor, nullptr);

  // Command handlers
  EXPECT_NE(components.search_handler, nullptr);
  EXPECT_NE(components.document_handler, nullptr);
  EXPECT_NE(components.dump_handler, nullptr);
  EXPECT_NE(components.admin_handler, nullptr);
  EXPECT_NE(components.replication_handler, nullptr);
  EXPECT_NE(components.debug_handler, nullptr);
  EXPECT_NE(components.cache_handler, nullptr);

#ifdef USE_MYSQL
  EXPECT_NE(components.sync_handler, nullptr);
#endif

  // CacheManager is always constructed when full_config is present; disabled
  // config keeps it inactive while still wiring runtime SET support.
  ASSERT_NE(components.cache_manager, nullptr);
  EXPECT_FALSE(components.cache_manager->IsEnabled());
  EXPECT_EQ(components.scheduler, nullptr);

  // Cleanup: Stop acceptor before destroying
  components.acceptor->Stop();
}

/**
 * @test Initialize_Success_WithoutOptionalComponents
 * @brief Test initialization with cache and scheduler disabled
 */
TEST_F(ServerLifecycleManagerTest, Initialize_Success_WithoutOptionalComponents) {
  full_config_.cache.enabled = false;
  full_config_.dump.interval_sec = 0;

  auto manager = CreateManager();
  auto result = manager->Initialize();

  ASSERT_TRUE(result) << "Initialize failed: " << result.error().to_string();

  // Optional components should be null
  ASSERT_NE(result->cache_manager, nullptr);
  EXPECT_FALSE(result->cache_manager->IsEnabled());
  EXPECT_EQ(result->scheduler, nullptr);

  // Required components should exist
  EXPECT_NE(result->thread_pool, nullptr);
  EXPECT_NE(result->dispatcher, nullptr);

  // Cleanup
  result->acceptor->Stop();
}

TEST_F(ServerLifecycleManagerTest, ExplicitZeroThreadPoolQueueRemainsUnbounded) {
  server_config_.thread_pool_queue_size = 0;
  auto manager = CreateManager();
  auto result = manager->Initialize();
  ASSERT_TRUE(result) << result.error().to_string();
  ASSERT_NE(result->thread_pool, nullptr);
  EXPECT_EQ(result->thread_pool->GetMaxQueueSize(), 0U);
  result->acceptor->Stop();
}

/**
 * @test Initialize_FailsOnInvalidPort
 * @brief Test that initialization fails when port binding fails
 */
TEST_F(ServerLifecycleManagerTest, Initialize_FailsOnInvalidPort) {
  // First, bind to an ephemeral port
  int test_sock = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(test_sock, 0);

  struct sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = inet_addr("127.0.0.1");
  addr.sin_port = 0;  // Let OS assign

  ASSERT_EQ(bind(test_sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)), 0);  // NOLINT
  ASSERT_EQ(listen(test_sock, 5), 0);

  // Get the assigned port
  socklen_t len = sizeof(addr);
  ASSERT_EQ(getsockname(test_sock, reinterpret_cast<struct sockaddr*>(&addr), &len), 0);  // NOLINT
  uint16_t bound_port = ntohs(addr.sin_port);

  // Try to initialize with the same port (should fail)
  server_config_.port = bound_port;
  auto manager = CreateManager();
  auto result = manager->Initialize();

  // Should fail at InitAcceptor
  EXPECT_FALSE(result);

  // Error should be related to binding
  auto error_msg = result.error().to_string();
  EXPECT_TRUE(error_msg.find("bind") != std::string::npos || error_msg.find("Bind") != std::string::npos ||
              error_msg.find("use") != std::string::npos);

  close(test_sock);
}

/**
 * @test Initialize_HandlerContextHasCorrectDependencies
 * @brief Verify HandlerContext receives all required dependencies
 */
TEST_F(ServerLifecycleManagerTest, Initialize_HandlerContextHasCorrectDependencies) {
  auto manager = CreateManager();
  auto result = manager->Initialize();

  ASSERT_TRUE(result);
  auto& handler_context = result->handler_context;

  // Verify non-null pointers
  EXPECT_NE(handler_context->table_catalog, nullptr);
  EXPECT_EQ(&handler_context->stats, &stats_);
  EXPECT_EQ(handler_context->full_config, &full_config_);

  // With cache disabled, cache_manager remains wired but inactive.
  ASSERT_NE(handler_context->cache_manager, nullptr);
  EXPECT_FALSE(handler_context->cache_manager->IsEnabled());

  // Verify atomic references point to our test data
  EXPECT_EQ(&handler_context->dump_load_in_progress, &dump_load_in_progress_);
  EXPECT_EQ(&handler_context->dump_save_in_progress, &dump_save_in_progress_);
  EXPECT_EQ(&handler_context->optimization_in_progress, &optimization_in_progress_);
  EXPECT_EQ(&handler_context->replication_paused_for_dump, &replication_paused_for_dump_);
  EXPECT_EQ(handler_context->replication_pause_counter, &replication_pause_counter_);

  // Cleanup
  result->acceptor->Stop();
}

/**
 * @test Initialize_PartialFailure_NoLeaks
 * @brief Verify no resource leaks on partial initialization failure
 * @note This test should be run with AddressSanitizer to detect leaks
 *
 * This test verifies that if initialization fails partway through,
 * all already-initialized components are properly cleaned up.
 * We use an invalid bind address to force a failure at InitAcceptor.
 */
TEST_F(ServerLifecycleManagerTest, Initialize_PartialFailure_NoLeaks) {
  // Use invalid bind address to cause failure at InitAcceptor
  // This will fail after ThreadPool, TableCatalog, HandlerContext, Handlers, and Dispatcher are created
  server_config_.host = "999.999.999.999";  // Invalid IP address

  auto manager = CreateManager();
  auto result = manager->Initialize();

  // Should fail at InitAcceptor
  EXPECT_FALSE(result);

  // The unique_ptrs should have cleaned up all partially initialized components
  // If running with AddressSanitizer and no leaks are detected, test passes
}

// ===== P1 Tests (Should-Have) =====

/**
 * @test Initialize_Success_WithCacheEnabled
 * @brief Test initialization with cache manager enabled
 */
TEST_F(ServerLifecycleManagerTest, Initialize_Success_WithCacheEnabled) {
  full_config_.cache.enabled = true;
  full_config_.cache.max_memory_bytes = 1024 * 1024;  // 1MB

  auto manager = CreateManager();
  auto result = manager->Initialize();

  ASSERT_TRUE(result) << "Initialize failed: " << result.error().to_string();
  EXPECT_NE(result->cache_manager, nullptr);

  // Verify cache is also in handler context
  EXPECT_EQ(result->handler_context->cache_manager, result->cache_manager.get());

  // Cleanup
  result->acceptor->Stop();
}

/**
 * @test Initialize_Success_WithSchedulerEnabled
 * @brief Test initialization with snapshot scheduler enabled
 *
 * Contract enforced by InitScheduler():
 *   - When dump.interval_sec > 0 and TableCatalog is non-null, the scheduler
 *     is constructed but not started. TcpServer starts it only after the
 *     pre-accept wiring callback. SnapshotScheduler's constructor does not
 *     accept a null catalog (the precondition is checked in
 *     ServerLifecycleManager::InitScheduler).
 */
TEST_F(ServerLifecycleManagerTest, Initialize_Success_WithSchedulerEnabled) {
  full_config_.dump.interval_sec = 60;
  full_config_.dump.dir = dump_dir_;

  auto manager = CreateManager();
  auto result = manager->Initialize();

  ASSERT_TRUE(result) << "Initialize failed: " << result.error().to_string();
  ASSERT_NE(result->scheduler, nullptr);
  EXPECT_FALSE(result->scheduler->IsRunning());

  // Cleanup: the lifecycle manager owns only a constructed scheduler here.
  result->acceptor->Stop();
}

/**
 * @test Initialize_SchedulerDisabledWhenIntervalZero
 * @brief Verify scheduler stays nullptr when interval_sec is 0 (disabled).
 *
 * This complements Initialize_Success_WithSchedulerEnabled and documents the
 * "scheduler is optional" branch in InitScheduler. The TableCatalog null-check
 * branch in InitScheduler is unreachable from Initialize() (TableCatalog is
 * always created non-null by InitTableCatalog), but exists as a defensive
 * guard against future refactors that might bypass that step.
 */
TEST_F(ServerLifecycleManagerTest, Initialize_SchedulerDisabledWhenIntervalZero) {
  full_config_.dump.interval_sec = 0;

  auto manager = CreateManager();
  auto result = manager->Initialize();

  ASSERT_TRUE(result) << "Initialize failed: " << result.error().to_string();
  EXPECT_EQ(result->scheduler, nullptr);

  // Cleanup
  result->acceptor->Stop();
}

TEST_F(ServerLifecycleManagerTest, LifecycleSweepsDeadMarkedTempWithSchedulerDisabledAndPreservesManualTmp) {
  full_config_.dump.interval_sec = 0;
  const auto nested_dir = std::filesystem::path(dump_dir_) / "nested";
  const auto stale_temp = CreateAgedDeadTrackedTemp(nested_dir, "unfinished.dmp");
  ASSERT_TRUE(stale_temp.has_value());
  const auto& [temp_path, marker_path] = *stale_temp;

  const auto completed_manual_tmp = std::filesystem::path(dump_dir_) / "finished-manual-dump.tmp";
  {
    std::ofstream file(completed_manual_tmp);
    file << "completed manual dump";
  }

  {
    auto manager = CreateManager();
    auto result = manager->Initialize();
    ASSERT_TRUE(result) << result.error().to_string();
    EXPECT_EQ(result->scheduler, nullptr);
    EXPECT_FALSE(std::filesystem::exists(temp_path));
    EXPECT_FALSE(std::filesystem::exists(marker_path));
    result->acceptor->Stop();
  }
  EXPECT_TRUE(std::filesystem::exists(completed_manual_tmp));
}

TEST_F(ServerLifecycleManagerTest, LifecycleSweepRunsWithoutFullConfig) {
  const auto stale_temp = CreateAgedDeadTrackedTemp(std::filesystem::path(dump_dir_) / "nested", "unfinished.dmp");
  ASSERT_TRUE(stale_temp.has_value());

  auto manager = CreateManager(/*null_full_config=*/true);
  auto result = manager->Initialize();
  ASSERT_TRUE(result) << result.error().to_string();
  EXPECT_EQ(result->scheduler, nullptr);
  EXPECT_FALSE(std::filesystem::exists(stale_temp->first));
  EXPECT_FALSE(std::filesystem::exists(stale_temp->second));
  result->acceptor->Stop();
}

#ifdef USE_MYSQL
/**
 * @test Initialize_SyncHandlerReceivesSyncManager
 * @brief Verify SyncHandler receives correct SyncOperationManager pointer
 */
TEST_F(ServerLifecycleManagerTest, Initialize_SyncHandlerReceivesSyncManager) {
  auto manager = CreateManager();
  auto result = manager->Initialize();

  ASSERT_TRUE(result);
  EXPECT_NE(result->sync_handler, nullptr);

  // The fact that initialization succeeded means SyncHandler was constructed correctly
  // with the sync_manager pointer (verified by compilation and no crashes)

  // Cleanup
  result->acceptor->Stop();
}

/**
 * @test Initialize_DispatcherRegistersAllSyncQueryTypes
 * @brief SYNC, SYNC_STATUS, and SYNC_STOP must all be wired into the
 *        RequestDispatcher.
 *
 * The completeness check inside InitDispatcher fails fast on missing
 * registrations; this test pins the contract for the SYNC family explicitly.
 */
TEST_F(ServerLifecycleManagerTest, Initialize_DispatcherRegistersAllSyncQueryTypes) {
  auto manager = CreateManager();
  auto result = manager->Initialize();
  ASSERT_TRUE(result) << "Initialize failed: " << result.error().to_string();

  ASSERT_NE(result->dispatcher, nullptr);
  EXPECT_TRUE(result->dispatcher->HasHandler(query::QueryType::SYNC));
  EXPECT_TRUE(result->dispatcher->HasHandler(query::QueryType::SYNC_STATUS));
  EXPECT_TRUE(result->dispatcher->HasHandler(query::QueryType::SYNC_STOP))
      << "SYNC_STOP must be registered in RequestDispatcher";

  // Cleanup
  result->acceptor->Stop();
}
#endif

/**
 * @test Constructor_NullFullConfig_SkipsOptionalComponents
 * @brief Test that null full_config properly skips optional components
 */
TEST_F(ServerLifecycleManagerTest, Constructor_NullFullConfig_SkipsOptionalComponents) {
  auto manager_result =
      ServerLifecycleManager::Create(server_config_, table_contexts_, dump_dir_,
                                     nullptr,  // full_config = nullptr
                                     stats_, dump_load_in_progress_, dump_save_in_progress_, optimization_in_progress_,
                                     replication_paused_for_dump_, replication_pause_counter_,
                                     nullptr  // binlog_reader
#ifdef USE_MYSQL
                                     ,
                                     sync_manager_.get()
#endif
      );
  ASSERT_TRUE(manager_result) << "Create failed: " << manager_result.error().to_string();
  auto manager = std::move(*manager_result);

  auto result = manager->Initialize();
  ASSERT_TRUE(result) << "Initialize failed: " << result.error().to_string();

  // Optional components should be null
  EXPECT_EQ(result->cache_manager, nullptr);
  EXPECT_EQ(result->scheduler, nullptr);

  // Required components should exist
  EXPECT_NE(result->thread_pool, nullptr);
  EXPECT_NE(result->dispatcher, nullptr);

  // Cleanup
  result->acceptor->Stop();
}

/**
 * @test Initialize_StopsAtFirstError
 * @brief Verify that initialization stops at first error and doesn't continue
 */
TEST_F(ServerLifecycleManagerTest, Initialize_StopsAtFirstError) {
  // Use invalid bind address to cause failure at InitAcceptor
  server_config_.host = "999.999.999.999";  // Invalid IP address

  auto manager = CreateManager();
  auto result = manager->Initialize();

  // Should fail
  EXPECT_FALSE(result);

  // Error message should indicate the problem
  auto error_msg = result.error().to_string();
  EXPECT_FALSE(error_msg.empty());
}

// ===== P2 Tests (Nice-to-Have) =====

/**
 * @test Create_ValidArgs_ReturnsValidObject
 * @brief Test that Create() with valid arguments returns a valid manager
 */
TEST_F(ServerLifecycleManagerTest, Create_ValidArgs_ReturnsValidObject) {
  auto result = ServerLifecycleManager::Create(
      server_config_, table_contexts_, dump_dir_, &full_config_, stats_, dump_load_in_progress_, dump_save_in_progress_,
      optimization_in_progress_, replication_paused_for_dump_, replication_pause_counter_,
      nullptr  // binlog_reader
#ifdef USE_MYSQL
      ,
      sync_manager_.get()
#endif
  );

  ASSERT_TRUE(result) << "Create failed: " << result.error().to_string();
  EXPECT_NE(*result, nullptr);
}

#ifdef USE_MYSQL
/**
 * @test Create_NullSyncManager_ReturnsError
 * @brief Test that Create() with null sync_manager returns an error (not throws)
 */
TEST_F(ServerLifecycleManagerTest, Create_NullSyncManager_ReturnsError) {
  auto result = ServerLifecycleManager::Create(
      server_config_, table_contexts_, dump_dir_, &full_config_, stats_, dump_load_in_progress_, dump_save_in_progress_,
      optimization_in_progress_, replication_paused_for_dump_, replication_pause_counter_,
      nullptr,  // binlog_reader
      nullptr   // sync_manager = null
  );

  ASSERT_FALSE(result);
  auto error_msg = result.error().to_string();
  EXPECT_TRUE(error_msg.find("sync_manager") != std::string::npos)
      << "Error should mention sync_manager, got: " << error_msg;
}
#endif

/**
 * @test Initialize_EmptyTableContexts_Succeeds
 * @brief Test initialization with no tables configured
 */
TEST_F(ServerLifecycleManagerTest, Initialize_EmptyTableContexts_Succeeds) {
  table_contexts_.clear();

  auto manager = CreateManager();
  auto result = manager->Initialize();

  // Should succeed even with no tables (handlers can operate with empty table list)
  ASSERT_TRUE(result) << "Initialize failed: " << result.error().to_string();

  // Cleanup
  result->acceptor->Stop();
}

/**
 * @test Initialize_WorkerThreadsZero_UsesDefault
 * @brief Test that worker_threads = 0 uses default CPU count
 */
TEST_F(ServerLifecycleManagerTest, Initialize_WorkerThreadsZero_UsesDefault) {
  server_config_.worker_threads = 0;  // Should use CPU count

  auto manager = CreateManager();
  auto result = manager->Initialize();

  ASSERT_TRUE(result) << "Initialize failed: " << result.error().to_string();
  EXPECT_NE(result->thread_pool, nullptr);

  // ThreadPool should have created worker threads (implementation detail, can't directly test count)
  // But initialization should succeed

  // Cleanup
  result->acceptor->Stop();
}
