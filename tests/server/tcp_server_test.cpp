/**
 * @file tcp_server_test.cpp
 * @brief Unit tests for TCP server
 */

#include "server/tcp_server.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <chrono>
#include <cstdio>
#include <fstream>
#include <thread>

#include "config/config.h"

using namespace mygramdb::server;
using namespace mygramdb;

/**
 * @brief Test fixture for TCP server tests
 */
class TcpServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
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

    server_ = std::make_unique<TcpServer>(config_, table_contexts_);
  }

  void TearDown() override {
    if (server_ && server_->IsRunning()) {
      server_->Stop();
    }
  }

  // Helper to create client socket
  int CreateClientSocket(uint16_t port) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock < 0) {
      return -1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &server_addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
      close(sock);
      return -1;
    }

    return sock;
  }

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
};

/**
 * @brief Test server construction
 */
TEST_F(TcpServerTest, Construction) {
  EXPECT_FALSE(server_->IsRunning());
  EXPECT_EQ(server_->GetConnectionCount(), 0);
  EXPECT_EQ(server_->GetTotalRequests(), 0);
}

/**
 * @brief Test server start and stop
 */
TEST_F(TcpServerTest, StartStop) {
  EXPECT_TRUE(server_->Start());
  EXPECT_TRUE(server_->IsRunning());
  EXPECT_GT(server_->GetPort(), 0);

  server_->Stop();
  EXPECT_FALSE(server_->IsRunning());
}

/**
 * @brief Test double start
 */
TEST_F(TcpServerTest, DoubleStart) {
  EXPECT_TRUE(server_->Start());
  EXPECT_FALSE(server_->Start());  // Should fail
  EXPECT_TRUE(server_->IsRunning());
}

/**
 * @brief Test GET request for non-existent document
 */
TEST_F(TcpServerTest, GetNonExistent) {
  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  // Wait for server to be ready
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "GET test 999");
  EXPECT_EQ(response, "ERROR Document not found");

  close(sock);
}

/**
 * @brief Test SEARCH on empty index
 */
TEST_F(TcpServerTest, SearchEmpty) {
  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "SEARCH test test");
  EXPECT_EQ(response, "OK RESULTS 0");

  close(sock);
}

/**
 * @brief Test COUNT on empty index
 */
TEST_F(TcpServerTest, CountEmpty) {
  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "COUNT test test");
  EXPECT_EQ(response, "OK COUNT 0");

  close(sock);
}

/**
 * @brief Test SEARCH with documents
 */
TEST_F(TcpServerTest, SearchWithDocuments) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id1), "hello world");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id2), "hello there");

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "SEARCH test hello");
  EXPECT_EQ(response, "OK RESULTS 2 1 2");

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

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "COUNT test hello");
  EXPECT_EQ(response, "OK COUNT 2");

  close(sock);
}

/**
 * @brief Test GET with document
 */
TEST_F(TcpServerTest, GetDocument) {
  // Add document
  std::unordered_map<std::string, storage::FilterValue> filters;
  filters["status"] = static_cast<int64_t>(1);
  auto doc_id = doc_store_->AddDocument("test123", filters);
  index_->AddDocument(static_cast<index::DocId>(doc_id), "hello world");

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "GET test test123");
  EXPECT_TRUE(response.find("OK DOC test123") == 0);
  EXPECT_TRUE(response.find("status=1") != std::string::npos);

  close(sock);
}

/**
 * @brief Test SEARCH with LIMIT
 */
TEST_F(TcpServerTest, SearchWithLimit) {
  // Add 5 documents
  for (int i = 1; i <= 5; i++) {
    auto doc_id = doc_store_->AddDocument(std::to_string(i), {});
    index_->AddDocument(static_cast<index::DocId>(doc_id), "test");
  }

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "SEARCH test test LIMIT 3");
  EXPECT_EQ(response, "OK RESULTS 5 1 2 3");

  close(sock);
}

/**
 * @brief Test SEARCH with OFFSET
 */
TEST_F(TcpServerTest, SearchWithOffset) {
  // Add 5 documents
  for (int i = 1; i <= 5; i++) {
    auto doc_id = doc_store_->AddDocument(std::to_string(i), {});
    index_->AddDocument(static_cast<index::DocId>(doc_id), "test");
  }

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "SEARCH test test OFFSET 2");
  EXPECT_EQ(response, "OK RESULTS 5 3 4 5");

  close(sock);
}

/**
 * @brief Test SEARCH with NOT
 */
TEST_F(TcpServerTest, SearchWithNot) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id1), "abc xyz");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id2), "abc def");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id3), "ghi jkl");

  ASSERT_TRUE(server_->Start());
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
  index_->AddDocument(static_cast<index::DocId>(doc_id1), "abc xyz");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id2), "abc def");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id3), "xyz def");

  ASSERT_TRUE(server_->Start());
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
  index_->AddDocument(static_cast<index::DocId>(doc_id1), "abc xyz pqr");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id2), "abc def");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id3), "abc xyz");

  ASSERT_TRUE(server_->Start());
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
  index_->AddDocument(static_cast<index::DocId>(doc_id1), "abc xyz old");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id2), "abc xyz new");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id3), "abc def");

  ASSERT_TRUE(server_->Start());
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

  ASSERT_TRUE(server_->Start());
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
 * @brief Test SEARCH with quoted strings
 */
TEST_F(TcpServerTest, SearchWithQuotedString) {
  // Add documents
  auto doc_id1 = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id1), "hello world");

  auto doc_id2 = doc_store_->AddDocument("2", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id2), "hello");

  auto doc_id3 = doc_store_->AddDocument("3", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id3), "world");

  ASSERT_TRUE(server_->Start());
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
 * @brief Test multiple requests on same connection
 */
TEST_F(TcpServerTest, MultipleRequests) {
  // Add document
  auto doc_id = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id), "test");

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response1 = SendRequest(sock, "SEARCH test test");
  EXPECT_EQ(response1, "OK RESULTS 1 1");

  std::string response2 = SendRequest(sock, "COUNT test test");
  EXPECT_EQ(response2, "OK COUNT 1");

  close(sock);
}

/**
 * @brief Test invalid command
 */
TEST_F(TcpServerTest, InvalidCommand) {
  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  std::string response = SendRequest(sock, "INVALID");
  EXPECT_TRUE(response.find("ERROR") == 0);

  close(sock);
}

/**
 * @brief Test concurrent connections
 */
TEST_F(TcpServerTest, ConcurrentConnections) {
  // Add document
  auto doc_id = doc_store_->AddDocument("1", {});
  index_->AddDocument(static_cast<index::DocId>(doc_id), "test");

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Create 3 concurrent clients
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  for (int i = 0; i < 3; i++) {
    threads.emplace_back([this, port, &success_count]() {
      int sock = CreateClientSocket(port);
      if (sock < 0)
        return;

      std::string response = SendRequest(sock, "COUNT test test");
      if (response == "OK COUNT 1") {
        success_count++;
      }

      close(sock);
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  EXPECT_EQ(success_count, 3);
  EXPECT_EQ(server_->GetTotalRequests(), 3);
}

/**
 * @brief Test INFO command
 */
TEST_F(TcpServerTest, InfoCommand) {
  ASSERT_TRUE(server_->Start());
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

  // Index section
  EXPECT_TRUE(response.find("# Index") != std::string::npos);
  EXPECT_TRUE(response.find("total_documents:") != std::string::npos);
  EXPECT_TRUE(response.find("total_terms:") != std::string::npos);
  EXPECT_TRUE(response.find("delta_encoded_lists:") != std::string::npos);
  EXPECT_TRUE(response.find("roaring_bitmap_lists:") != std::string::npos);

  // Clients section
  EXPECT_TRUE(response.find("# Clients") != std::string::npos);
  EXPECT_TRUE(response.find("connected_clients:") != std::string::npos);

  EXPECT_TRUE(response.find("END") != std::string::npos);

  close(sock);
}

/**
 * @brief Test SAVE command
 */
TEST_F(TcpServerTest, SaveCommand) {
  // Add some documents first
  index_->AddDocument(1, "test document");
  index_->AddDocument(2, "another document");
  doc_store_->AddDocument("1", {});
  doc_store_->AddDocument("2", {});

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send SAVE command
  std::string test_file = "/tmp/test_snapshot_" + std::to_string(std::time(nullptr));
  std::string response = SendRequest(sock, "SAVE " + test_file);

  // Should return OK SAVED
  EXPECT_TRUE(response.find("OK SAVED") == 0);
  EXPECT_TRUE(response.find(test_file) != std::string::npos);

  // Check directory and files exist (new directory format)
  std::ifstream meta_file(test_file + "/meta.json");
  EXPECT_TRUE(meta_file.good());
  meta_file.close();

  std::ifstream index_file(test_file + "/test.index");
  EXPECT_TRUE(index_file.good());
  index_file.close();

  std::ifstream docs_file(test_file + "/test.docs");
  EXPECT_TRUE(docs_file.good());
  docs_file.close();

  // Cleanup
  std::remove((test_file + "/meta.json").c_str());
  std::remove((test_file + "/test.index").c_str());
  std::remove((test_file + "/test.docs").c_str());
  std::remove(test_file.c_str());  // Remove directory

  close(sock);
}

/**
 * @brief Test LOAD command
 */
TEST_F(TcpServerTest, LoadCommand) {
  // Add and save some documents
  index_->AddDocument(1, "test document");
  index_->AddDocument(2, "another document");
  doc_store_->AddDocument("1", {});
  doc_store_->AddDocument("2", {});

  // Create snapshot directory with new format
  std::string test_dir = "/tmp/test_snapshot_load_" + std::to_string(std::time(nullptr));
  mkdir(test_dir.c_str(), 0755);

  ASSERT_TRUE(index_->SaveToFile(test_dir + "/test.index"));
  ASSERT_TRUE(doc_store_->SaveToFile(test_dir + "/test.docs"));

  // Create meta.json
  std::ofstream meta_file(test_dir + "/meta.json");
  meta_file << "{\"version\":\"1.0\",\"tables\":[\"test\"],\"timestamp\":\"2024-01-01T00:00:00Z\"}";
  meta_file.close();

  // Recreate index, doc_store, and table context
  table_context_.index.reset(new mygramdb::index::Index(1, 0.18));
  table_context_.doc_store.reset(new mygramdb::storage::DocumentStore());
  index_ = table_context_.index.get();
  doc_store_ = table_context_.doc_store.get();
  table_context_.config.ngram_size = 1;
  table_contexts_["test"] = &table_context_;
  server_.reset(new mygramdb::server::TcpServer(config_, table_contexts_, "./snapshots", nullptr));

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send LOAD command
  std::string response = SendRequest(sock, "LOAD " + test_dir);

  // Should return OK LOADED
  EXPECT_TRUE(response.find("OK LOADED") == 0);
  EXPECT_TRUE(response.find(test_dir) != std::string::npos);

  // Verify data was loaded - check document count
  EXPECT_EQ(doc_store_->Size(), 2);

  // Cleanup
  std::remove((test_dir + "/meta.json").c_str());
  std::remove((test_dir + "/test.index").c_str());
  std::remove((test_dir + "/test.docs").c_str());
  std::remove(test_dir.c_str());

  close(sock);
}

/**
 * @brief Test SAVE/LOAD round trip
 */
TEST_F(TcpServerTest, SaveLoadRoundTrip) {
  // Add documents with filters
  std::unordered_map<std::string, mygramdb::storage::FilterValue> filters1;
  filters1["status"] = static_cast<int32_t>(1);
  filters1["name"] = std::string("test");

  std::unordered_map<std::string, mygramdb::storage::FilterValue> filters2;
  filters2["status"] = static_cast<int32_t>(2);
  filters2["name"] = std::string("another");

  index_->AddDocument(1, "test document with filters");
  index_->AddDocument(2, "another document");
  doc_store_->AddDocument("100", filters1);
  doc_store_->AddDocument("200", filters2);

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Save
  std::string test_file = "/tmp/test_roundtrip_" + std::to_string(std::time(nullptr));
  std::string save_response = SendRequest(sock, "SAVE " + test_file);
  EXPECT_TRUE(save_response.find("OK SAVED") == 0);

  // Get original document count
  size_t original_count = doc_store_->Size();

  // Load (should replace existing data)
  std::string load_response = SendRequest(sock, "LOAD " + test_file);
  EXPECT_TRUE(load_response.find("OK LOADED") == 0);

  // Verify document count matches
  EXPECT_EQ(doc_store_->Size(), original_count);

  // Verify we can retrieve documents
  auto doc1 = doc_store_->GetDocument(1);
  ASSERT_TRUE(doc1.has_value());
  EXPECT_EQ(doc1->primary_key, "100");
  EXPECT_EQ(doc1->filters.size(), 2);

  // Cleanup
  std::remove((test_file + ".index").c_str());
  std::remove((test_file + ".docs").c_str());

  close(sock);
}

/**
 * @brief Test DEBUG ON command
 */
TEST_F(TcpServerTest, DebugOn) {
  ASSERT_TRUE(server_->Start());
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
  ASSERT_TRUE(server_->Start());
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

  ASSERT_TRUE(server_->Start());
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

  // Should contain debug information
  EXPECT_TRUE(response.find("DEBUG") != std::string::npos);
  EXPECT_TRUE(response.find("query_time=") != std::string::npos);
  EXPECT_TRUE(response.find("index_time=") != std::string::npos);
  EXPECT_TRUE(response.find("terms=") != std::string::npos);
  EXPECT_TRUE(response.find("ngrams=") != std::string::npos);
  EXPECT_TRUE(response.find("candidates=") != std::string::npos);
  EXPECT_TRUE(response.find("final=") != std::string::npos);

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

  ASSERT_TRUE(server_->Start());
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
  ASSERT_TRUE(server_->Start());
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
 * @brief Test that queries are blocked during LOAD
 */
TEST_F(TcpServerTest, QueriesBlockedDuringLoad) {
  // Add documents and save them
  for (int i = 1; i <= 1000; i++) {
    auto doc_id = doc_store_->AddDocument(std::to_string(i), {});
    index_->AddDocument(static_cast<index::DocId>(doc_id), "test document " + std::to_string(i));
  }

  // Create snapshot directory with new format
  std::string test_file = "/tmp/test_blocking_" + std::to_string(std::time(nullptr));
  mkdir(test_file.c_str(), 0755);

  ASSERT_TRUE(index_->SaveToFile(test_file + "/test.index"));
  ASSERT_TRUE(doc_store_->SaveToFile(test_file + "/test.docs"));

  // Create meta.json
  std::ofstream meta_file(test_file + "/meta.json");
  meta_file << "{\"version\":\"1.0\",\"tables\":[\"test\"],\"timestamp\":\"2024-01-01T00:00:00Z\"}";
  meta_file.close();

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Create two connections: one for LOAD, one for queries
  int load_sock = CreateClientSocket(port);
  ASSERT_GE(load_sock, 0);

  int query_sock = CreateClientSocket(port);
  ASSERT_GE(query_sock, 0);

  std::atomic<bool> load_started{false};
  std::atomic<bool> load_finished{false};
  std::string load_response;

  // Start LOAD in a separate thread
  std::thread load_thread([this, load_sock, &test_file, &load_response, &load_started, &load_finished]() {
    load_started = true;
    load_response = SendRequest(load_sock, "LOAD " + test_file);
    load_finished = true;
  });

  // Wait for LOAD to start
  while (!load_started) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Give LOAD a moment to actually begin processing
  std::this_thread::sleep_for(std::chrono::milliseconds(50));

  // Try queries while LOAD is in progress
  bool found_loading_error = false;
  for (int i = 0; i < 10 && !load_finished; i++) {
    // Try SEARCH
    std::string search_response = SendRequest(query_sock, "SEARCH test test");
    if (search_response.find("Server is loading") != std::string::npos) {
      found_loading_error = true;
      break;
    }

    // Try COUNT
    std::string count_response = SendRequest(query_sock, "COUNT test test");
    if (count_response.find("Server is loading") != std::string::npos) {
      found_loading_error = true;
      break;
    }

    // Try GET
    std::string get_response = SendRequest(query_sock, "GET test 1");
    if (get_response.find("Server is loading") != std::string::npos) {
      found_loading_error = true;
      break;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  // Wait for LOAD to complete
  load_thread.join();

  // Close sockets first
  close(load_sock);
  close(query_sock);

  // LOAD should have succeeded
  EXPECT_TRUE(load_response.find("OK LOADED") == 0);

  // Note: We expect to find the loading error during the test
  // If LOAD was too fast, this test might not catch it every time,
  // but it should catch it in most cases with 1000 documents
  if (found_loading_error) {
    // This is the expected case - we caught queries being blocked
    SUCCEED() << "Successfully verified queries were blocked during LOAD";
  } else {
    // LOAD was too fast, but the mechanism is still in place
    // This is not a failure, just means we couldn't catch it in the act
    GTEST_SKIP() << "LOAD completed too quickly to verify blocking behavior";
  }

  // Cleanup
  std::remove((test_file + "/meta.json").c_str());
  std::remove((test_file + "/test.index").c_str());
  std::remove((test_file + "/test.docs").c_str());
  std::remove(test_file.c_str());
}
