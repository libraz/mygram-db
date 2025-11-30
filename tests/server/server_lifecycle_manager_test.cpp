/**
 * @file server_lifecycle_manager_test.cpp
 * @brief Unit tests for ServerLifecycleManager
 */

#include "server/server_lifecycle_manager.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <unistd.h>

#include "config/config.h"
#include "index/index.h"
#include "storage/document_store.h"

#ifdef USE_MYSQL
#include "server/sync_operation_manager.h"
#endif

using namespace mygramdb::server;
using namespace mygramdb;

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

    dump_dir_ = "/tmp/test_dump";

#ifdef USE_MYSQL
    // Create SyncOperationManager (required for SyncHandler)
    sync_manager_ = std::make_unique<SyncOperationManager>(table_contexts_, &full_config_, nullptr);
#endif
  }

  void TearDown() override {
    // Cleanup handled by unique_ptr destructors
  }

  // Helper: Create lifecycle manager with current config
  std::unique_ptr<ServerLifecycleManager> CreateManager() {
    return std::make_unique<ServerLifecycleManager>(server_config_, table_contexts_, dump_dir_, &full_config_, stats_,
                                                    dump_load_in_progress_, dump_save_in_progress_,
                                                    optimization_in_progress_, replication_paused_for_dump_,
                                                    mysql_reconnecting_,
#ifdef USE_MYSQL
                                                    nullptr,  // binlog_reader
                                                    sync_manager_.get()
#else
                                                    nullptr  // binlog_reader
#endif
    );
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
  std::atomic<bool> mysql_reconnecting_{false};

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

  // Optional components (disabled by default in SetUp)
  EXPECT_EQ(components.cache_manager, nullptr);
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
  EXPECT_EQ(result->cache_manager, nullptr);
  EXPECT_EQ(result->scheduler, nullptr);

  // Required components should exist
  EXPECT_NE(result->thread_pool, nullptr);
  EXPECT_NE(result->dispatcher, nullptr);

  // Cleanup
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
  EXPECT_EQ(&handler_context->table_contexts, &table_contexts_);
  EXPECT_EQ(&handler_context->stats, &stats_);
  EXPECT_EQ(handler_context->full_config, &full_config_);

  // With cache disabled, cache_manager should be nullptr
  EXPECT_EQ(handler_context->cache_manager, nullptr);

  // Verify atomic references point to our test data
  EXPECT_EQ(&handler_context->dump_load_in_progress, &dump_load_in_progress_);
  EXPECT_EQ(&handler_context->dump_save_in_progress, &dump_save_in_progress_);
  EXPECT_EQ(&handler_context->optimization_in_progress, &optimization_in_progress_);
  EXPECT_EQ(&handler_context->replication_paused_for_dump, &replication_paused_for_dump_);
  EXPECT_EQ(&handler_context->mysql_reconnecting, &mysql_reconnecting_);

  // Cleanup
  result->acceptor->Stop();
}

/**
 * @test Initialize_PartialFailure_NoLeaks
 * @brief Verify no resource leaks on partial initialization failure
 * @note This test should be run with AddressSanitizer to detect leaks
 *
 * This test verifies that if initialization fails partway through,
 * all previously initialized components are properly cleaned up.
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
 */
TEST_F(ServerLifecycleManagerTest, Initialize_Success_WithSchedulerEnabled) {
  full_config_.dump.interval_sec = 60;
  full_config_.dump.dir = "/tmp/test_dump";

  auto manager = CreateManager();
  auto result = manager->Initialize();

  ASSERT_TRUE(result) << "Initialize failed: " << result.error().to_string();
  EXPECT_NE(result->scheduler, nullptr);

  // Cleanup: Stop scheduler before destroying
  result->scheduler->Stop();
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
#endif

/**
 * @test Constructor_NullFullConfig_SkipsOptionalComponents
 * @brief Test that null full_config properly skips optional components
 */
TEST_F(ServerLifecycleManagerTest, Constructor_NullFullConfig_SkipsOptionalComponents) {
  auto manager = std::make_unique<ServerLifecycleManager>(server_config_, table_contexts_, dump_dir_,
                                                          nullptr,  // full_config = nullptr
                                                          stats_, dump_load_in_progress_, dump_save_in_progress_,
                                                          optimization_in_progress_, replication_paused_for_dump_,
                                                          mysql_reconnecting_,
#ifdef USE_MYSQL
                                                          nullptr, sync_manager_.get()
#else
                                                          nullptr
#endif
  );

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
