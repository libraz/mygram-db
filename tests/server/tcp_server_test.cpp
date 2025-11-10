/**
 * @file tcp_server_test.cpp
 * @brief Unit tests for TCP server
 */

#include "server/tcp_server.h"
#include <gtest/gtest.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <thread>
#include <chrono>
#include <fstream>
#include <cstdio>

using namespace mygramdb::server;
using namespace mygramdb;

/**
 * @brief Test fixture for TCP server tests
 */
class TcpServerTest : public ::testing::Test {
 protected:
  void SetUp() override {
    index_ = std::make_unique<index::Index>(1);
    doc_store_ = std::make_unique<storage::DocumentStore>();

    config_.port = 0;  // Let OS assign port
    config_.host = "127.0.0.1";

    server_ = std::make_unique<TcpServer>(config_, *index_, *doc_store_);
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
    if (response.size() >= 2 &&
        response[response.size()-2] == '\r' &&
        response[response.size()-1] == '\n') {
      response = response.substr(0, response.size() - 2);
    }

    return response;
  }

  ServerConfig config_;
  std::unique_ptr<index::Index> index_;
  std::unique_ptr<storage::DocumentStore> doc_store_;
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
      if (sock < 0) return;

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

  // Should return OK INFO with server statistics
  EXPECT_TRUE(response.find("OK INFO") == 0);
  EXPECT_TRUE(response.find("version:") != std::string::npos);
  EXPECT_TRUE(response.find("uptime_seconds:") != std::string::npos);
  EXPECT_TRUE(response.find("total_requests:") != std::string::npos);
  EXPECT_TRUE(response.find("active_connections:") != std::string::npos);
  EXPECT_TRUE(response.find("total_documents:") != std::string::npos);
  EXPECT_TRUE(response.find("ngram_mode:") != std::string::npos ||
              response.find("ngram_size:") != std::string::npos);
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

  // Check files exist
  std::ifstream index_file(test_file + ".index");
  EXPECT_TRUE(index_file.good());
  index_file.close();

  std::ifstream docs_file(test_file + ".docs");
  EXPECT_TRUE(docs_file.good());
  docs_file.close();

  // Cleanup
  std::remove((test_file + ".index").c_str());
  std::remove((test_file + ".docs").c_str());

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

  std::string test_file = "/tmp/test_snapshot_load_" + std::to_string(std::time(nullptr));
  ASSERT_TRUE(index_->SaveToFile(test_file + ".index"));
  ASSERT_TRUE(doc_store_->SaveToFile(test_file + ".docs"));

  // Recreate index and doc_store
  index_.reset(new mygramdb::index::Index(1, 0.18));
  doc_store_.reset(new mygramdb::storage::DocumentStore());
  server_.reset(new mygramdb::server::TcpServer(config_, *index_, *doc_store_, 1, "./snapshots", nullptr));

  ASSERT_TRUE(server_->Start());
  uint16_t port = server_->GetPort();

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  int sock = CreateClientSocket(port);
  ASSERT_GE(sock, 0);

  // Send LOAD command
  std::string response = SendRequest(sock, "LOAD " + test_file);

  // Should return OK LOADED
  EXPECT_TRUE(response.find("OK LOADED") == 0);
  EXPECT_TRUE(response.find(test_file) != std::string::npos);

  // Verify data was loaded - check document count
  EXPECT_EQ(doc_store_->Size(), 2);

  // Cleanup
  std::remove((test_file + ".index").c_str());
  std::remove((test_file + ".docs").c_str());

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
