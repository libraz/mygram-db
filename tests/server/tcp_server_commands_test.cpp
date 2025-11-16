/**
 * @file tcp_server_commands_test.cpp
 * @brief Unit tests for TCP server - Command handling
 */

#include <arpa/inet.h>
#include <fcntl.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <sstream>
#include <thread>

#include "config/config.h"
#include "server/tcp_server.h"

using namespace mygramdb::server;
using namespace mygramdb;

/**
 * @brief Test fixture for TCP server tests
 */
class TcpServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    SkipIfSocketCreationBlocked();

    // Create index and doc_store as unique_ptrs
    auto index = std::make_unique<index::Index>(1);
    auto doc_store = std::make_unique<storage::DocumentStore>();

    // Store raw pointers in table context
    table_context_.name = "test";
    table_context_.config.ngram_size = 1;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);

    // Keep raw pointers for test access
    index_ = table_context_.index.get();
    doc_store_ = table_context_.doc_store.get();

    table_contexts_["test"] = &table_context_;

    config_.port = 0;  // Let OS assign port
    config_.host = "127.0.0.1";
    config_.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost

    server_ = std::make_unique<TcpServer>(config_, table_contexts_);
  }

  void TearDown() override {
    if (server_ && server_->IsRunning()) {
      server_->Stop();
    }
  }

  int CreateClientSocket(uint16_t port);

  // Helper to send request and receive response
  std::string SendRequest(int sock, const std::string& request) {
    std::string msg = request + "\r\n";
    ssize_t sent = send(sock, msg.c_str(), msg.length(), 0);
    if (sent < 0) {
      return "";
    }

    char buffer[4096];
    ssize_t received = recv(sock, buffer, sizeof(buffer) - 1, 0);
    if (received <= 0) {
      return "";
    }

    buffer[received] = '\0';
    std::string response(buffer);

    // Remove trailing \r\n
    if (response.size() >= 2 && response[response.size() - 2] == '\r' && response[response.size() - 1] == '\n') {
      response = response.substr(0, response.size() - 2);
    }

    return response;
  }

  ServerConfig config_;
  index::Index* index_;                // Raw pointer to table_context_.index
  storage::DocumentStore* doc_store_;  // Raw pointer to table_context_.doc_store
  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<TcpServer> server_;

  static void SkipIfSocketCreationBlocked();
  void StartServerOrSkip();
};

void TcpServerTest::SkipIfSocketCreationBlocked() {
  static bool checked = false;
  static bool skip_due_to_permissions = false;
  static std::string permission_error;

  if (!checked) {
    checked = true;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd >= 0) {
      close(fd);
    } else {
      if (errno == EPERM || errno == EACCES) {
        skip_due_to_permissions = true;
        permission_error = std::strerror(errno);
      }
    }
  }

  if (skip_due_to_permissions) {
    GTEST_SKIP() << "Skipping TcpServerTest: unable to create AF_INET socket (" << permission_error
                 << "). WSL/OS is blocking TCP sockets; enable networking to run this test.";
  }
}

int TcpServerTest::CreateClientSocket(uint16_t port) {
  constexpr int kConnectTimeoutSec = 5;
  constexpr int kSocketIoTimeoutSec = 5;

  int sock = socket(AF_INET, SOCK_STREAM, 0);
  if (sock < 0) {
    return -1;
  }

  // Set socket non-blocking to implement custom connect timeout
  int flags = fcntl(sock, F_GETFL, 0);
  if (flags >= 0) {
    fcntl(sock, F_SETFL, flags | O_NONBLOCK);
  }

  struct sockaddr_in server_addr {};
  server_addr.sin_family = AF_INET;
  server_addr.sin_port = htons(port);
  inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

  int result = connect(sock, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr));
  if (result < 0) {
    if (errno != EINPROGRESS) {
      close(sock);
      return -1;
    }

    fd_set write_fds;
    FD_ZERO(&write_fds);
    FD_SET(sock, &write_fds);

    struct timeval timeout {};
    timeout.tv_sec = kConnectTimeoutSec;

    int ready = select(sock + 1, nullptr, &write_fds, nullptr, &timeout);
    if (ready <= 0) {
      close(sock);
      errno = (ready == 0) ? ETIMEDOUT : errno;
      return -1;
    }

    int so_error = 0;
    socklen_t len = sizeof(so_error);
    if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &so_error, &len) < 0 || so_error != 0) {
      if (so_error != 0) {
        errno = so_error;
      }
      close(sock);
      return -1;
    }
  }

  // Restore blocking mode if we changed it
  if (flags >= 0) {
    fcntl(sock, F_SETFL, flags);
  }

  // Apply send/receive timeouts to avoid hangs on recv()
  struct timeval io_timeout {};
  io_timeout.tv_sec = kSocketIoTimeoutSec;
  setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &io_timeout, sizeof(io_timeout));
  setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &io_timeout, sizeof(io_timeout));

  return sock;
}

void TcpServerTest::StartServerOrSkip() {
  if (server_->Start()) {
    return;
  }

  const std::string& error = server_->GetLastError();
  if (error.find("Operation not permitted") != std::string::npos ||
      error.find("Permission denied") != std::string::npos) {
    GTEST_SKIP() << "Skipping TcpServerTest: " << error << ". This environment does not allow creating TCP sockets.";
  }

  FAIL() << "Failed to start TCP server: " << (error.empty() ? "unknown error" : error);
}

/**
 * @brief Test INFO command
 */
TEST_F(TcpServerTest, InfoCommand) {
  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send INFO command
  std::string response = SendRequest(sock, "INFO");

  // Should return OK INFO with server statistics (Redis-style)
  EXPECT_TRUE(response.find("OK INFO") == 0);

  // Server section
  EXPECT_TRUE(response.find("# Server") != std::string::npos);
  EXPECT_TRUE(response.find("version:") != std::string::npos);
  EXPECT_TRUE(response.find("uptime_seconds:") != std::string::npos);

  // Stats section
  EXPECT_TRUE(response.find("# Stats") != std::string::npos);
  EXPECT_TRUE(response.find("total_commands_processed:") != std::string::npos);
  EXPECT_TRUE(response.find("total_requests:") != std::string::npos);

  // Commandstats section
  EXPECT_TRUE(response.find("# Commandstats") != std::string::npos);

  // Memory section
  EXPECT_TRUE(response.find("# Memory") != std::string::npos);
  EXPECT_TRUE(response.find("used_memory_bytes:") != std::string::npos);
  EXPECT_TRUE(response.find("used_memory_human:") != std::string::npos);

  // System memory information
  EXPECT_TRUE(response.find("total_system_memory:") != std::string::npos);
  EXPECT_TRUE(response.find("available_system_memory:") != std::string::npos);
  EXPECT_TRUE(response.find("system_memory_usage_ratio:") != std::string::npos);

  // Process memory information
  EXPECT_TRUE(response.find("process_rss:") != std::string::npos);
  EXPECT_TRUE(response.find("process_rss_peak:") != std::string::npos);

  // Memory health status
  EXPECT_TRUE(response.find("memory_health:") != std::string::npos);

  // Index section
  EXPECT_TRUE(response.find("# Index") != std::string::npos);
  EXPECT_TRUE(response.find("total_documents:") != std::string::npos);
  EXPECT_TRUE(response.find("total_terms:") != std::string::npos);
  EXPECT_TRUE(response.find("delta_encoded_lists:") != std::string::npos);
  EXPECT_TRUE(response.find("roaring_bitmap_lists:") != std::string::npos);

  // Clients section
  EXPECT_TRUE(response.find("# Clients") != std::string::npos);
  EXPECT_TRUE(response.find("connected_clients:") != std::string::npos);

  // Cache section (should show cache disabled when no cache manager)
  EXPECT_TRUE(response.find("# Cache") != std::string::npos);
  EXPECT_TRUE(response.find("cache_enabled: 0") != std::string::npos);

  EXPECT_TRUE(response.find("END") != std::string::npos);

  close(sock);
}

/**
 * @brief Test INFO command with table names
 */
TEST_F(TcpServerTest, InfoCommandWithTables) {
  // Create additional table contexts
  auto index2 = std::make_unique<index::Index>(1);
  auto doc_store2 = std::make_unique<storage::DocumentStore>();
  TableContext table_context2;
  table_context2.name = "users";
  table_context2.config.ngram_size = 1;
  table_context2.index = std::move(index2);
  table_context2.doc_store = std::move(doc_store2);

  auto index3 = std::make_unique<index::Index>(1);
  auto doc_store3 = std::make_unique<storage::DocumentStore>();
  TableContext table_context3;
  table_context3.name = "comments";
  table_context3.config.ngram_size = 1;
  table_context3.index = std::move(index3);
  table_context3.doc_store = std::move(doc_store3);

  // Add to table contexts
  std::unordered_map<std::string, TableContext*> multi_table_contexts;
  multi_table_contexts["test"] = &table_context_;
  multi_table_contexts["users"] = &table_context2;
  multi_table_contexts["comments"] = &table_context3;

  // Create a config with table information
  config::Config full_config;
  config::TableConfig table1;
  table1.name = "test";
  config::TableConfig table2;
  table2.name = "users";
  config::TableConfig table3;
  table3.name = "comments";
  full_config.tables.push_back(table1);
  full_config.tables.push_back(table2);
  full_config.tables.push_back(table3);

  // Create server with config
  auto server_with_config = std::make_unique<TcpServer>(config_, multi_table_contexts, "./snapshots", &full_config);

  ASSERT_TRUE(server_with_config->Start());
  uint16_t port = server_with_config->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send INFO command
  std::string response = SendRequest(sock, "INFO");

  // Should return OK INFO
  EXPECT_TRUE(response.find("OK INFO") == 0);

  // Should contain Tables section
  EXPECT_TRUE(response.find("# Tables") != std::string::npos);

  // Should contain all table names (order not guaranteed with unordered_map)
  EXPECT_TRUE(response.find("tables: ") != std::string::npos);
  EXPECT_TRUE(response.find("test") != std::string::npos);
  EXPECT_TRUE(response.find("users") != std::string::npos);
  EXPECT_TRUE(response.find("comments") != std::string::npos);

  close(sock);
  server_with_config->Stop();
}

/**
 * @brief Test INFO command without tables (null config)
 */
TEST_F(TcpServerTest, InfoCommandWithoutTables) {
  // Server created in SetUp has nullptr for config
  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send INFO command
  std::string response = SendRequest(sock, "INFO");

  // Should return OK INFO
  EXPECT_TRUE(response.find("OK INFO") == 0);

  // Should contain Tables section (even if empty)
  EXPECT_TRUE(response.find("# Tables") != std::string::npos);

  // Should not crash when full_config_ is nullptr
  // The tables line should be omitted when config is null

  close(sock);
}

/**
 * @brief Test INFO command with single table
 */
TEST_F(TcpServerTest, InfoCommandWithSingleTable) {
  // Create a config with single table
  config::Config full_config;
  config::TableConfig table;
  table.name = "products";
  full_config.tables.push_back(table);

  // Create server with config
  auto server_with_config = std::make_unique<TcpServer>(config_, table_contexts_, "./snapshots", &full_config);

  ASSERT_TRUE(server_with_config->Start());
  uint16_t port = server_with_config->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send INFO command
  std::string response = SendRequest(sock, "INFO");

  // Should return OK INFO
  EXPECT_TRUE(response.find("OK INFO") == 0);

  // Should contain table name from table_contexts (actual loaded tables)
  EXPECT_TRUE(response.find("tables: ") != std::string::npos);
  EXPECT_TRUE(response.find("test") != std::string::npos);

  close(sock);
  server_with_config->Stop();
}

/**
 * @brief Test INFO command includes replication statistics
 */
TEST_F(TcpServerTest, InfoCommandReplicationStatistics) {
  StartServerOrSkip();

  // Get server statistics and increment some replication counters
  ServerStats* stats = server_->GetMutableStats();
  ASSERT_NE(stats, nullptr);

  stats->IncrementReplInsertApplied();
  stats->IncrementReplInsertApplied();
  stats->IncrementReplInsertSkipped();

  stats->IncrementReplUpdateAdded();
  stats->IncrementReplUpdateRemoved();
  stats->IncrementReplUpdateModified();
  stats->IncrementReplUpdateSkipped();

  stats->IncrementReplDeleteApplied();
  stats->IncrementReplDeleteSkipped();

  stats->IncrementReplDdlExecuted();
  stats->IncrementReplEventsSkippedOtherTables();

  int port = server_->GetPort();
  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send INFO command
  std::string response = SendRequest(sock, "INFO");

  // Should return OK INFO
  EXPECT_TRUE(response.find("OK INFO") == 0);

  // Check if replication statistics are included
  EXPECT_TRUE(response.find("replication_inserts_applied: 2") != std::string::npos);
  EXPECT_TRUE(response.find("replication_inserts_skipped: 1") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_applied: 3") != std::string::npos);  // Added + Removed + Modified
  EXPECT_TRUE(response.find("replication_updates_added: 1") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_removed: 1") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_modified: 1") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_skipped: 1") != std::string::npos);
  EXPECT_TRUE(response.find("replication_deletes_applied: 1") != std::string::npos);
  EXPECT_TRUE(response.find("replication_deletes_skipped: 1") != std::string::npos);
  EXPECT_TRUE(response.find("replication_ddl_executed: 1") != std::string::npos);
  EXPECT_TRUE(response.find("replication_events_skipped_other_tables: 1") != std::string::npos);

  close(sock);
}

/**
 * @brief Test INFO command replication statistics initially zero
 */
TEST_F(TcpServerTest, InfoCommandReplicationStatisticsInitiallyZero) {
  StartServerOrSkip();

  int port = server_->GetPort();
  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send INFO command
  std::string response = SendRequest(sock, "INFO");

  // Should return OK INFO
  EXPECT_TRUE(response.find("OK INFO") == 0);

  // All replication statistics should be 0 initially
  EXPECT_TRUE(response.find("replication_inserts_applied: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_inserts_skipped: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_applied: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_added: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_removed: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_modified: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_updates_skipped: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_deletes_applied: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_deletes_skipped: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_ddl_executed: 0") != std::string::npos);
  EXPECT_TRUE(response.find("replication_events_skipped_other_tables: 0") != std::string::npos);

  close(sock);
}

/**
 * @brief Test SAVE command
 */
TEST_F(TcpServerTest, DebugOn) {
  StartServerOrSkip();
  uint16_t port = server_->GetPort();
  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send DEBUG ON command
  std::string response = SendRequest(sock, "DEBUG ON");

  // Should return OK DEBUG_ON
  EXPECT_EQ(response, "OK DEBUG_ON");

  close(sock);
}

/**
 * @brief Test DEBUG OFF command
 */
TEST_F(TcpServerTest, DebugOff) {
  StartServerOrSkip();
  uint16_t port = server_->GetPort();
  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send DEBUG OFF command
  std::string response = SendRequest(sock, "DEBUG OFF");

  // Should return OK DEBUG_OFF
  EXPECT_EQ(response, "OK DEBUG_OFF");

  close(sock);
}

/**
 * @brief Test DEBUG mode with SEARCH command
 */
TEST_F(TcpServerTest, DebugModeWithSearch) {
  // Add test documents
  auto doc_id1 = doc_store_->AddDocument("100", {});
  auto doc_id2 = doc_store_->AddDocument("200", {});
  index_->AddDocument(doc_id1, "hello world");
  index_->AddDocument(doc_id2, "test data");

  StartServerOrSkip();
  uint16_t port = server_->GetPort();
  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Enable debug mode
  std::string debug_on = SendRequest(sock, "DEBUG ON");
  EXPECT_EQ(debug_on, "OK DEBUG_ON");

  // Search with debug mode enabled
  std::string response = SendRequest(sock, "SEARCH test hello LIMIT 10");

  // Should contain results
  EXPECT_TRUE(response.find("OK RESULTS") == 0);

  // Should contain debug information (multi-line format)
  EXPECT_TRUE(response.find("# DEBUG") != std::string::npos);
  EXPECT_TRUE(response.find("query_time:") != std::string::npos);
  EXPECT_TRUE(response.find("index_time:") != std::string::npos);
  EXPECT_TRUE(response.find("terms:") != std::string::npos);
  EXPECT_TRUE(response.find("ngrams:") != std::string::npos);
  EXPECT_TRUE(response.find("candidates:") != std::string::npos);
  EXPECT_TRUE(response.find("final:") != std::string::npos);

  // Disable debug mode
  std::string debug_off = SendRequest(sock, "DEBUG OFF");
  EXPECT_EQ(debug_off, "OK DEBUG_OFF");

  // Search without debug mode
  std::string response2 = SendRequest(sock, "SEARCH test hello LIMIT 10");

  // Should contain results but NO debug info
  EXPECT_TRUE(response2.find("OK RESULTS") == 0);
  EXPECT_TRUE(response2.find("DEBUG") == std::string::npos);

  close(sock);
}

/**
 * @brief Test DEBUG mode is per-connection
 */
TEST_F(TcpServerTest, DebugModePerConnection) {
  // Add test document
  auto doc_id = doc_store_->AddDocument("100", {});
  index_->AddDocument(doc_id, "hello world");

  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  // Connection 1: Enable debug
  int sock1 = CreateClientSocket(port);
  ASSERT_GE(sock1, 0);
  std::string debug_on = SendRequest(sock1, "DEBUG ON");
  EXPECT_EQ(debug_on, "OK DEBUG_ON");

  // Connection 2: Debug should be off by default
  int sock2 = CreateClientSocket(port);
  ASSERT_GE(sock2, 0);

  // Search from connection 1 (debug enabled)
  std::string response1 = SendRequest(sock1, "SEARCH test hello LIMIT 10");
  EXPECT_TRUE(response1.find("DEBUG") != std::string::npos);

  // Search from connection 2 (debug disabled)
  std::string response2 = SendRequest(sock2, "SEARCH test hello LIMIT 10");
  EXPECT_TRUE(response2.find("DEBUG") == std::string::npos);

  close(sock1);
  close(sock2);
}

/**
 * @brief Test debug output shows (default) marker for implicit parameters
 */
TEST_F(TcpServerTest, DebugModeDefaultParameterMarkers) {
  // Add test documents
  auto doc_id1 = doc_store_->AddDocument("100", {});
  index_->AddDocument(doc_id1, "hello world");
  auto doc_id2 = doc_store_->AddDocument("101", {});
  index_->AddDocument(doc_id2, "hello universe");

  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Enable debug mode
  std::string debug_on = SendRequest(sock, "DEBUG ON");
  EXPECT_EQ(debug_on, "OK DEBUG_ON");

  // Test 1: Search without explicit LIMIT, OFFSET, or SORT
  // Should show all as (default)
  std::string response1 = SendRequest(sock, "SEARCH test hello");
  EXPECT_TRUE(response1.find("OK RESULTS") == 0);
  EXPECT_TRUE(response1.find("# DEBUG") != std::string::npos);
  EXPECT_TRUE(response1.find("sort: id DESC (default)") != std::string::npos)
      << "Should show default SORT with (default) marker";
  EXPECT_TRUE(response1.find("limit: 100 (default)") != std::string::npos)
      << "Should show default LIMIT with (default) marker";
  // OFFSET should not be shown when it's 0
  EXPECT_TRUE(response1.find("offset:") == std::string::npos) << "OFFSET should not be shown when 0";

  // Test 2: Search with explicit LIMIT
  // LIMIT should NOT have (default), but SORT should
  std::string response2 = SendRequest(sock, "SEARCH test hello LIMIT 50");
  EXPECT_TRUE(response2.find("OK RESULTS") == 0);
  EXPECT_TRUE(response2.find("sort: id DESC (default)") != std::string::npos)
      << "SORT should still have (default) marker";
  EXPECT_TRUE(response2.find("limit: 50\r\n") != std::string::npos)
      << "Explicit LIMIT should NOT have (default) marker";
  EXPECT_TRUE(response2.find("limit: 50 (default)") == std::string::npos)
      << "Explicit LIMIT should NOT have (default) marker";

  // Test 3: Search with explicit SORT
  // SORT should NOT have (default), but LIMIT should
  std::string response3 = SendRequest(sock, "SEARCH test hello SORT id ASC");
  EXPECT_TRUE(response3.find("OK RESULTS") == 0);
  EXPECT_TRUE(response3.find("sort: id ASC\r\n") != std::string::npos)
      << "Explicit SORT should NOT have (default) marker";
  EXPECT_TRUE(response3.find("sort: id ASC (default)") == std::string::npos)
      << "Explicit SORT should NOT have (default) marker";
  EXPECT_TRUE(response3.find("limit: 100 (default)") != std::string::npos)
      << "Default LIMIT should have (default) marker";

  // Test 4: Search with explicit OFFSET
  // OFFSET should NOT have (default) when explicitly set
  std::string response4 = SendRequest(sock, "SEARCH test hello OFFSET 10");
  EXPECT_TRUE(response4.find("OK RESULTS") == 0);
  EXPECT_TRUE(response4.find("offset: 10\r\n") != std::string::npos)
      << "Explicit OFFSET should NOT have (default) marker";
  EXPECT_TRUE(response4.find("offset: 10 (default)") == std::string::npos)
      << "Explicit OFFSET should NOT have (default) marker";

  // Test 5: Search with all explicit parameters
  // Nothing should have (default)
  std::string response5 = SendRequest(sock, "SEARCH test hello SORT id DESC LIMIT 25 OFFSET 5");
  EXPECT_TRUE(response5.find("OK RESULTS") == 0);
  EXPECT_TRUE(response5.find("sort: id DESC\r\n") != std::string::npos);
  EXPECT_TRUE(response5.find("(default)") == std::string::npos)
      << "No parameters should have (default) when all are explicit";
  EXPECT_TRUE(response5.find("limit: 25\r\n") != std::string::npos);
  EXPECT_TRUE(response5.find("offset: 5\r\n") != std::string::npos);

  close(sock);
}

/**
 * @brief Test COUNT on empty index
 */
TEST_F(TcpServerTest, CountEmpty) {
  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "COUNT test test");
  EXPECT_EQ(response, "OK COUNT 0");

  close(sock);
}

/**
 * @brief Test COUNT with documents
 */
TEST_F(TcpServerTest, CountWithDocuments) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id1), "hello world");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id2), "hello there");

  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "COUNT test hello");
  EXPECT_EQ(response, "OK COUNT 2");

  close(sock);
}

/**
 * @brief Test COUNT with AND operator
 */
TEST_F(TcpServerTest, CountWithAnd) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id1), "abc xyz");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id2), "abc def");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id3), "xyz def");

  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Count documents containing both 'a' AND 'd'
  std::string response = SendRequest(sock, "COUNT test a AND d");
  EXPECT_EQ(response, "OK COUNT 1");

  close(sock);
}

/**
 * @brief Test that COUNT and SEARCH return consistent total results
 */
TEST_F(TcpServerTest, CountSearchConsistency) {
  // Insert test documents
  for (int i = 1; i <= 100; ++i) {
    auto doc_id = doc_store_->AddDocument(std::to_string(i), {});
    index_->AddDocument(static_cast<index::DocId>(doc_id), "test document");
  }

  StartServerOrSkip();
  uint16_t port = server_->GetPort();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Get COUNT
  std::string count_response = SendRequest(sock, "COUNT test test");
  EXPECT_TRUE(count_response.find("OK COUNT 100") == 0) << "COUNT should return 100";

  // Get SEARCH total (with small LIMIT)
  std::string search_response = SendRequest(sock, "SEARCH test test LIMIT 10");
  EXPECT_TRUE(search_response.find("OK RESULTS 100") == 0) << "SEARCH total_results should match COUNT (100)";

  // Get SEARCH total (with large LIMIT)
  std::string search_response2 = SendRequest(sock, "SEARCH test test LIMIT 90");
  EXPECT_TRUE(search_response2.find("OK RESULTS 100") == 0)
      << "SEARCH total_results should be consistent regardless of LIMIT";

  close(sock);
}
