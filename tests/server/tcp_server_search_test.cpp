/**
 * @file tcp_server_search_test.cpp
 * @brief Unit tests for TCP server - Search operations
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
  auto result = server_->Start();
  if (result) {
    return;
  }

  const std::string error = result.error().to_string();
  if (error.find("Operation not permitted") != std::string::npos ||
      error.find("Permission denied") != std::string::npos) {
    GTEST_SKIP() << "Skipping TcpServerTest: " << error << ". This environment does not allow creating TCP sockets.";
  }

  FAIL() << "Failed to start TCP server: " << (error.empty() ? "unknown error" : error);
}

/**
 * @brief Test SEARCH on empty index
 */
TEST_F(TcpServerTest, SearchEmpty) {
  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "SEARCH test test");
  EXPECT_EQ(response, "OK RESULTS 0");

  close(sock);
}

/**
 * @brief Test SEARCH with documents
 */
TEST_F(TcpServerTest, SearchWithDocuments) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id1), "hello world");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id2), "hello there");

  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "SEARCH test hello");
  // Default SORT: PRIMARY KEY DESC (descending order: 2, 1)
  EXPECT_EQ(response, "OK RESULTS 2 2 1");

  close(sock);
}

/**
 * @brief Test SEARCH with LIMIT
 */
TEST_F(TcpServerTest, SearchWithLimit) {
  // Add 5 documents
  for (int i = 1; i <= 5; i++) {
    auto doc_id = doc_store_->AddDocument(std::to_string(i), {});
    index_->AddDocument(static_cast<index::DocId>(*doc_id), "test");
  }

  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "SEARCH test test LIMIT 3");
  // Default SORT: PRIMARY KEY DESC (descending order: 5, 4, 3)
  EXPECT_EQ(response, "OK RESULTS 5 5 4 3");

  close(sock);
}

/**
 * @brief Test SEARCH with OFFSET
 */
TEST_F(TcpServerTest, SearchWithOffset) {
  // Add 5 documents
  for (int i = 1; i <= 5; i++) {
    auto doc_id = doc_store_->AddDocument(std::to_string(i), {});
    index_->AddDocument(static_cast<index::DocId>(*doc_id), "test");
  }

  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "SEARCH test test OFFSET 2");
  // Default SORT: PRIMARY KEY DESC (descending order: 5, 4, 3, 2, 1)
  // OFFSET 2 skips first 2 results (5, 4), returns: 3, 2, 1
  EXPECT_EQ(response, "OK RESULTS 5 3 2 1");

  close(sock);
}

/**
 * @brief Test SEARCH with NOT
 */
TEST_F(TcpServerTest, SearchWithNot) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id1), "abc xyz");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id2), "abc def");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id3), "ghi jkl");

  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Search for documents containing 'a' but not containing 'x'
  // Should match doc_id2 only (has 'a' from "abc" but not 'x')
  std::string response = SendRequest(sock, "SEARCH test a NOT x");
  EXPECT_EQ(response, "OK RESULTS 1 2");

  close(sock);
}

/**
 * @brief Test SEARCH with AND operator
 */
TEST_F(TcpServerTest, SearchWithAnd) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id1), "abc xyz");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id2), "abc def");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id3), "xyz def");

  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Search for documents containing both 'a' AND 'd'
  // Should match doc_id2 only (has both 'a' and 'd')
  std::string response = SendRequest(sock, "SEARCH test a AND d");
  EXPECT_EQ(response, "OK RESULTS 1 2");

  close(sock);
}

/**
 * @brief Test SEARCH with multiple AND operators
 */
TEST_F(TcpServerTest, SearchWithMultipleAnds) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id1), "abc xyz pqr");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id2), "abc def");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id3), "abc xyz");

  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Search for documents containing 'a' AND 'x' AND 'p'
  // Should match doc_id1 only
  std::string response = SendRequest(sock, "SEARCH test a AND x AND p");
  EXPECT_EQ(response, "OK RESULTS 1 1");

  close(sock);
}

/**
 * @brief Test SEARCH with AND and NOT combined
 */
TEST_F(TcpServerTest, SearchWithAndAndNot) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id1), "abc xyz old");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id2), "abc xyz new");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id3), "abc def");

  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Search for documents containing 'a' AND 'x' but NOT 'o'
  // Should match doc_id2 only
  std::string response = SendRequest(sock, "SEARCH test a AND x NOT o");
  EXPECT_EQ(response, "OK RESULTS 1 2");

  close(sock);
}

/**
 * @brief Test SEARCH with quoted strings
 */
TEST_F(TcpServerTest, SearchWithQuotedString) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id1), "hello world");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id2), "hello");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(*doc_id3), "world");

  StartServerOrSkip();
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Search for exact phrase "hello world"
  std::string response = SendRequest(sock, "SEARCH test \"hello world\"");
  EXPECT_EQ(response, "OK RESULTS 1 1");

  close(sock);
}

/**
 * @brief Test that queries are blocked during LOAD
 */
TEST_F(TcpServerTest, HybridNgramSearchWithKanjiNgramSize) {
  // Set up index with hybrid n-gram configuration
  // ngram_size = 2 (for ASCII, hiragana, katakana)
  // kanji_ngram_size = 1 (for kanji)
  table_context_.config.ngram_size = 2;
  table_context_.config.kanji_ngram_size = 1;

  // Recreate index with hybrid configuration
  table_context_.index = std::make_unique<index::Index>(2, 1);
  index_ = table_context_.index.get();

  // Add test documents with Japanese text
  // Document 1: Contains kanji "東"
  doc_store_->AddDocument("1", {});
  index_->AddDocument(1, "東京タワー");  // Tokyo Tower (all kanji)

  // Document 2: Contains kanji "料"
  doc_store_->AddDocument("2", {});
  index_->AddDocument(2, "日本料理");  // Japanese cuisine (all kanji)

  // Document 3: Contains hiragana "ひまわり"
  doc_store_->AddDocument("3", {});
  index_->AddDocument(3, "ひまわり畑");  // sunflower field (hiragana + kanji)

  // Document 4: Contains same kanji as doc 1
  doc_store_->AddDocument("4", {});
  index_->AddDocument(4, "東北地方");  // Tohoku region (kanji only)

  // Start server
  StartServerOrSkip();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  uint16_t port = server_->GetPort();
  ASSERT_GT(port, 0);

  // Test 1: Search for single kanji "東" (should use unigram)
  int sock1 = CreateClientSocket(port);
  ASSERT_GE(sock1, 0);

  std::string response1 = SendRequest(sock1, "SEARCH test 東");
  EXPECT_TRUE(response1.find("OK") == 0) << "Response: " << response1;

  // Should find documents 1 and 4 (both contain "東")
  // Parse IDs from response: "OK RESULTS <count> <id1> <id2> ..."
  std::istringstream iss1(response1);
  std::string ok, results;
  int count;
  iss1 >> ok >> results >> count;  // Skip "OK RESULTS <count>"
  std::vector<int> ids1;
  int id;
  while (iss1 >> id)
    ids1.push_back(id);

  EXPECT_TRUE(std::find(ids1.begin(), ids1.end(), 1) != ids1.end()) << "Doc 1 not found";
  EXPECT_TRUE(std::find(ids1.begin(), ids1.end(), 4) != ids1.end()) << "Doc 4 not found";
  EXPECT_TRUE(std::find(ids1.begin(), ids1.end(), 2) == ids1.end()) << "Doc 2 should not match";
  EXPECT_TRUE(std::find(ids1.begin(), ids1.end(), 3) == ids1.end()) << "Doc 3 should not match";

  close(sock1);

  // Test 2: Search for single kanji "料" (should use unigram)
  int sock2 = CreateClientSocket(port);
  ASSERT_GE(sock2, 0);

  std::string response2 = SendRequest(sock2, "SEARCH test 料");
  EXPECT_TRUE(response2.find("OK") == 0) << "Response: " << response2;

  // Should find only document 2
  // Parse IDs from response: "OK RESULTS <count> <id1> <id2> ..."
  std::istringstream iss2(response2);
  std::string ok2, results2;
  int count2;
  iss2 >> ok2 >> results2 >> count2;  // Skip "OK RESULTS <count>"
  std::vector<int> ids2;
  int id2;
  while (iss2 >> id2)
    ids2.push_back(id2);

  EXPECT_TRUE(std::find(ids2.begin(), ids2.end(), 2) != ids2.end()) << "Doc 2 not found";
  EXPECT_TRUE(std::find(ids2.begin(), ids2.end(), 1) == ids2.end()) << "Doc 1 should not match";
  EXPECT_TRUE(std::find(ids2.begin(), ids2.end(), 3) == ids2.end()) << "Doc 3 should not match";
  EXPECT_TRUE(std::find(ids2.begin(), ids2.end(), 4) == ids2.end()) << "Doc 4 should not match";

  close(sock2);

  // Test 3: Search for hiragana "ひまわり" (should use bigram)
  int sock3 = CreateClientSocket(port);
  ASSERT_GE(sock3, 0);

  std::string response3 = SendRequest(sock3, "SEARCH test ひまわり");
  EXPECT_TRUE(response3.find("OK") == 0) << "Response: " << response3;

  // Should find only document 3
  // Parse IDs from response: "OK RESULTS <count> <id1> <id2> ..."
  std::istringstream iss3(response3);
  std::string ok3, results3;
  int count3;
  iss3 >> ok3 >> results3 >> count3;  // Skip "OK RESULTS <count>"
  std::vector<int> ids3;
  int id3;
  while (iss3 >> id3)
    ids3.push_back(id3);

  EXPECT_TRUE(std::find(ids3.begin(), ids3.end(), 3) != ids3.end()) << "Doc 3 not found";
  EXPECT_TRUE(std::find(ids3.begin(), ids3.end(), 1) == ids3.end()) << "Doc 1 should not match";
  EXPECT_TRUE(std::find(ids3.begin(), ids3.end(), 2) == ids3.end()) << "Doc 2 should not match";
  EXPECT_TRUE(std::find(ids3.begin(), ids3.end(), 4) == ids3.end()) << "Doc 4 should not match";

  close(sock3);

  // Test 4: Search for mixed text "東京" (both kanji, should use unigrams)
  int sock4 = CreateClientSocket(port);
  ASSERT_GE(sock4, 0);

  std::string response4 = SendRequest(sock4, "SEARCH test 東京");
  EXPECT_TRUE(response4.find("OK") == 0) << "Response: " << response4;

  // Should find only document 1 (contains both "東" and "京")
  // Parse IDs from response: "OK RESULTS <count> <id1> <id2> ..."
  std::istringstream iss4(response4);
  std::string ok4, results4;
  int count4;
  iss4 >> ok4 >> results4 >> count4;  // Skip "OK RESULTS <count>"
  std::vector<int> ids4;
  int id4;
  while (iss4 >> id4)
    ids4.push_back(id4);

  EXPECT_TRUE(std::find(ids4.begin(), ids4.end(), 1) != ids4.end()) << "Doc 1 not found";
  EXPECT_TRUE(std::find(ids4.begin(), ids4.end(), 2) == ids4.end()) << "Doc 2 should not match";
  EXPECT_TRUE(std::find(ids4.begin(), ids4.end(), 3) == ids4.end()) << "Doc 3 should not match";
  EXPECT_TRUE(std::find(ids4.begin(), ids4.end(), 4) == ids4.end()) << "Doc 4 should not match";

  close(sock4);
}

/**
 * @brief Test optimization strategy selection based on result set size and LIMIT
 *
 * This test verifies that the server correctly chooses between:
 * - GetTopN optimization (for large result sets with small LIMIT)
 * - reuse-fetch optimization (for small result sets or high LIMIT ratio)
 */
TEST_F(TcpServerTest, OptimizationStrategySelection) {
  // Prepare test data: insert documents with varying characteristics
  // Small result set: 10 documents with "small"
  // Large result set: 1000 documents with "large"

  for (int i = 1; i <= 10; ++i) {
    auto doc_id = doc_store_->AddDocument(std::to_string(i), {});
    index_->AddDocument(static_cast<index::DocId>(*doc_id), "small unique text");
  }

  for (int i = 11; i <= 1010; ++i) {
    auto doc_id = doc_store_->AddDocument(std::to_string(i), {});
    index_->AddDocument(static_cast<index::DocId>(*doc_id), "large dataset text");
  }

  StartServerOrSkip();
  uint16_t port = server_->GetPort();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Enable debug mode to see optimization strategy
  std::string debug_response = SendRequest(sock, "DEBUG ON");
  EXPECT_EQ(debug_response, "OK DEBUG_ON");

  // Test 1: Small result set (10 docs) with small LIMIT (2 docs = 20%)
  // Expected: GetTopN optimization (ratio 20% < 50%)
  std::string response1 = SendRequest(sock, "SEARCH test small LIMIT 2");
  EXPECT_TRUE(response1.find("OK RESULTS 10") == 0) << "Should return total of 10 matching documents";
  EXPECT_TRUE(response1.find("optimization: Index GetTopN") != std::string::npos ||
              response1.find("optimization: reuse-fetch") != std::string::npos)
      << "Should use GetTopN or reuse-fetch optimization";

  // Test 2: Small result set (10 docs) with high LIMIT (9 docs = 90%)
  // Expected: reuse-fetch optimization (ratio 90% > 50%)
  std::string response2 = SendRequest(sock, "SEARCH test small LIMIT 9");
  EXPECT_TRUE(response2.find("OK RESULTS 10") == 0)
      << "Should return total of 10 matching documents. Response: " << response2;
  EXPECT_TRUE(response2.find("optimization: reuse-fetch") != std::string::npos)
      << "Should use reuse-fetch optimization for high LIMIT ratio (90% > 50%). Response: " << response2;

  // Test 3: Large result set (1000 docs) with small LIMIT (10 docs = 1%)
  // Expected: GetTopN optimization (ratio 1% < 50%)
  std::string response3 = SendRequest(sock, "SEARCH test large LIMIT 10");
  EXPECT_TRUE(response3.find("OK RESULTS 1000") == 0) << "Should return total of 1000 matching documents";
  EXPECT_TRUE(response3.find("optimization: Index GetTopN") != std::string::npos)
      << "Should use GetTopN optimization for low LIMIT ratio (1% < 50%)";

  // Test 4: Large result set (1000 docs) with high LIMIT (600 docs = 60%)
  // Expected: reuse-fetch optimization (ratio 60% > 50%)
  std::string response4 = SendRequest(sock, "SEARCH test large LIMIT 600");
  EXPECT_TRUE(response4.find("OK RESULTS 1000") == 0) << "Should return total of 1000 matching documents";
  EXPECT_TRUE(response4.find("optimization: reuse-fetch") != std::string::npos)
      << "Should use reuse-fetch optimization for high LIMIT ratio (60% > 50%)";

  // Test 5: Verify total_results accuracy with optimization
  // Even when using GetTopN, total_results should be accurate (not limited)
  std::string response5 = SendRequest(sock, "SEARCH test large LIMIT 5");
  EXPECT_TRUE(response5.find("OK RESULTS 1000") == 0) << "Total results should be 1000 (accurate count), not 5 (LIMIT)";

  // Count number of IDs in response (should be 5, not 1000)
  size_t id_count = 0;
  size_t pos = response5.find("OK RESULTS 1000");
  if (pos != std::string::npos) {
    std::string ids_part = response5.substr(pos + 16);  // Skip "OK RESULTS 1000 "
    std::istringstream iss(ids_part);
    std::string id;
    while (iss >> id && id_count < 100) {  // Limit check to avoid infinite loop
      if (id.find("\r") != std::string::npos || id.find("#") != std::string::npos) {
        break;  // Stop at debug section
      }
      id_count++;
    }
  }
  EXPECT_EQ(id_count, 5) << "Should return exactly 5 document IDs (LIMIT applied)";

  close(sock);
}
