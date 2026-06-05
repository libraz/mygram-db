/**
 * @file end_to_end_test.cpp
 * @brief End-to-end integration tests for complete workflows
 */

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <fstream>
#include <thread>

#include "config/config.h"
#include "server/tcp_server.h"

using namespace mygramdb::server;
using namespace mygramdb;

/**
 * @brief Helper class for TCP client connections
 */
class TcpClient {
 public:
  TcpClient(const std::string& host, uint16_t port) {
    sock_ = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_ < 0) {
      throw std::runtime_error("Failed to create socket");
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr);

    if (connect(sock_, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
      close(sock_);
      throw std::runtime_error("Failed to connect");
    }
  }

  ~TcpClient() {
    if (sock_ >= 0) {
      close(sock_);
    }
  }

  std::string SendCommand(const std::string& command) {
    std::string request = command + "\r\n";
    send(sock_, request.c_str(), request.length(), 0);

    char buffer[8192];
    ssize_t received = recv(sock_, buffer, sizeof(buffer) - 1, 0);
    if (received <= 0) {
      return "";
    }
    buffer[received] = '\0';
    return std::string(buffer);
  }

 private:
  int sock_ = -1;
};

/**
 * @brief Test fixture for end-to-end tests
 */
class EndToEndTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create table
    auto index = std::make_unique<mygramdb::index::Index>(3, 2);
    auto doc_store = std::make_unique<mygramdb::storage::DocumentStore>();

    table_context_.name = "posts";
    table_context_.config.ngram_size = 3;
    table_context_.config.kanji_ngram_size = 2;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);

    table_contexts_["posts"] = &table_context_;

    // Create server
    config_.port = 0;  // Random port
    config_.host = "127.0.0.1";
    config_.allow_cidrs = {"127.0.0.1/32"};

    server_ = std::make_unique<TcpServer>(config_, table_contexts_, "./snapshots", nullptr);
    ASSERT_TRUE(server_->Start());

    port_ = server_->GetPort();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void TearDown() override {
    server_->Stop();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  ServerConfig config_;
  std::unique_ptr<TcpServer> server_;
  uint16_t port_ = 0;
};

/**
 * @brief Test complete workflow: Add documents → Index → Search → Retrieve
 */
TEST_F(EndToEndTest, CompleteWorkflowAddIndexSearch) {
  TcpClient client("127.0.0.1", port_);

  // Step 1: Add documents directly to the store and index
  auto doc_id1 = table_context_.doc_store->AddDocument("pk1", {{"status", static_cast<int64_t>(1)}});
  table_context_.index->AddDocument(*doc_id1, "hello world");

  auto doc_id2 = table_context_.doc_store->AddDocument("pk2", {{"status", static_cast<int64_t>(2)}});
  table_context_.index->AddDocument(*doc_id2, "hello universe");

  auto doc_id3 = table_context_.doc_store->AddDocument("pk3", {{"status", static_cast<int64_t>(1)}});
  table_context_.index->AddDocument(*doc_id3, "goodbye world");

  // Step 2: Search for "hello"
  std::string response = client.SendCommand("SEARCH posts hello");
  EXPECT_TRUE(response.find("OK RESULTS 2") == 0);
  EXPECT_TRUE(response.find("pk1") != std::string::npos);
  EXPECT_TRUE(response.find("pk2") != std::string::npos);

  // Step 3: Search for "world"
  response = client.SendCommand("SEARCH posts world");
  EXPECT_TRUE(response.find("OK RESULTS 2") == 0);
  EXPECT_TRUE(response.find("pk1") != std::string::npos);
  EXPECT_TRUE(response.find("pk3") != std::string::npos);

  // Step 4: Count query
  response = client.SendCommand("COUNT posts hello");
  EXPECT_TRUE(response.find("OK COUNT 2") == 0);

  // Step 5: Get document by primary key
  response = client.SendCommand("GET posts pk1");
  EXPECT_TRUE(response.find("OK DOC pk1") == 0);
  EXPECT_TRUE(response.find("status=1") != std::string::npos);
}

/**
 * @brief Test workflow with filters and sorting
 */
TEST_F(EndToEndTest, WorkflowWithFiltersAndSorting) {
  TcpClient client("127.0.0.1", port_);

  // Add documents with different filter values
  auto doc_id1 = table_context_.doc_store->AddDocument("pk1", {{"priority", static_cast<int64_t>(1)}});
  table_context_.index->AddDocument(*doc_id1, "task one");

  auto doc_id2 = table_context_.doc_store->AddDocument("pk2", {{"priority", static_cast<int64_t>(3)}});
  table_context_.index->AddDocument(*doc_id2, "task two");

  auto doc_id3 = table_context_.doc_store->AddDocument("pk3", {{"priority", static_cast<int64_t>(2)}});
  table_context_.index->AddDocument(*doc_id3, "task three");

  // Search all tasks
  std::string response = client.SendCommand("SEARCH posts task");
  EXPECT_TRUE(response.find("OK RESULTS 3") == 0);

  // Search with LIMIT
  response = client.SendCommand("SEARCH posts task LIMIT 2");
  EXPECT_TRUE(response.find("OK RESULTS 3") == 0);  // Total is still 3

  // Search with LIMIT and OFFSET
  response = client.SendCommand("SEARCH posts task LIMIT 1 OFFSET 1");
  EXPECT_TRUE(response.find("OK RESULTS 3") == 0);
}

/**
 * @brief Test workflow with AND/NOT operators
 */
TEST_F(EndToEndTest, WorkflowWithLogicalOperators) {
  TcpClient client("127.0.0.1", port_);

  // Add documents
  auto doc_id1 = table_context_.doc_store->AddDocument("pk1", {});
  table_context_.index->AddDocument(*doc_id1, "machine learning tutorial");

  auto doc_id2 = table_context_.doc_store->AddDocument("pk2", {});
  table_context_.index->AddDocument(*doc_id2, "machine learning advanced");

  auto doc_id3 = table_context_.doc_store->AddDocument("pk3", {});
  table_context_.index->AddDocument(*doc_id3, "deep learning tutorial");

  // Search with AND
  std::string response = client.SendCommand("SEARCH posts machine AND learning");
  EXPECT_TRUE(response.find("OK RESULTS 2") == 0);

  // Search with NOT
  response = client.SendCommand("SEARCH posts learning NOT machine");
  EXPECT_TRUE(response.find("OK RESULTS 1") == 0);
  EXPECT_TRUE(response.find("pk3") != std::string::npos);

  // Search with complex expression
  response = client.SendCommand("SEARCH posts learning AND tutorial");
  EXPECT_TRUE(response.find("OK RESULTS 2") == 0);
}

/**
 * @brief Test workflow with INFO command
 */
TEST_F(EndToEndTest, WorkflowWithInfoCommand) {
  TcpClient client("127.0.0.1", port_);

  // Add some documents
  auto doc_id1 = table_context_.doc_store->AddDocument("pk1", {});
  table_context_.index->AddDocument(*doc_id1, "test document");

  // Get server info
  std::string response = client.SendCommand("INFO");
  EXPECT_TRUE(response.find("OK") == 0);
  // INFO response should contain server statistics
  EXPECT_TRUE(response.find("total_documents") != std::string::npos ||
              response.find("Statistics") != std::string::npos ||
              response.length() > 100);  // Should have substantial content
}

/**
 * @brief Test workflow with DEBUG mode
 */
TEST_F(EndToEndTest, WorkflowWithDebugMode) {
  TcpClient client("127.0.0.1", port_);

  // Add documents
  auto doc_id1 = table_context_.doc_store->AddDocument("pk1", {});
  table_context_.index->AddDocument(*doc_id1, "debug test");

  // Enable debug mode
  std::string response = client.SendCommand("DEBUG ON");
  EXPECT_TRUE(response.find("OK") == 0);

  // Search should include debug info
  response = client.SendCommand("SEARCH posts debug");
  EXPECT_TRUE(response.find("OK RESULTS") == 0);
  EXPECT_TRUE(response.find("DEBUG") != std::string::npos || response.find("query_time") != std::string::npos);

  // Disable debug mode
  response = client.SendCommand("DEBUG OFF");
  EXPECT_TRUE(response.find("OK") == 0);

  // Search should not include debug info
  response = client.SendCommand("SEARCH posts debug");
  EXPECT_TRUE(response.find("OK RESULTS") == 0);
}

/**
 * @brief Test error handling in complete workflow
 */
TEST_F(EndToEndTest, WorkflowErrorHandling) {
  TcpClient client("127.0.0.1", port_);

  // Test 1: Invalid table name
  std::string response = client.SendCommand("SEARCH nonexistent hello");
  EXPECT_TRUE(response.find("ERROR") == 0);

  // Test 2: Invalid command
  response = client.SendCommand("INVALID_CMD");
  EXPECT_TRUE(response.find("ERROR") == 0);

  // Test 3: Malformed query
  response = client.SendCommand("SEARCH");
  EXPECT_TRUE(response.find("ERROR") == 0);

  // Test 4: GET non-existent document
  response = client.SendCommand("GET posts nonexistent_pk");
  EXPECT_TRUE(response.find("ERROR") == 0);
}

/**
 * @brief Test concurrent client connections
 */
TEST_F(EndToEndTest, ConcurrentClients) {
  // Add documents
  auto doc_id1 = table_context_.doc_store->AddDocument("pk1", {});
  table_context_.index->AddDocument(*doc_id1, "concurrent test");

  const int num_clients = 10;
  std::vector<std::thread> threads;
  std::atomic<int> success_count{0};

  for (int i = 0; i < num_clients; ++i) {
    threads.emplace_back([this, &success_count]() {
      try {
        TcpClient client("127.0.0.1", port_);

        // Each client sends 10 requests
        for (int j = 0; j < 10; ++j) {
          std::string response = client.SendCommand("SEARCH posts test");
          if (response.find("OK RESULTS") == 0) {
            success_count++;
          }
        }
      } catch (const std::exception& e) {
        // Connection failure
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  // Most requests should succeed
  EXPECT_GT(success_count.load(), num_clients * 10 * 0.9);
}

/**
 * @brief Test Japanese text workflow
 */
TEST_F(EndToEndTest, WorkflowWithJapaneseText) {
  TcpClient client("127.0.0.1", port_);

  // Add Japanese documents
  auto doc_id1 = table_context_.doc_store->AddDocument("jp1", {});
  table_context_.index->AddDocument(*doc_id1, "機械学習のチュートリアル");

  auto doc_id2 = table_context_.doc_store->AddDocument("jp2", {});
  table_context_.index->AddDocument(*doc_id2, "深層学習の応用");

  // Search in Japanese
  std::string response = client.SendCommand("SEARCH posts 学習");
  EXPECT_TRUE(response.find("OK RESULTS 2") == 0);
  EXPECT_TRUE(response.find("jp1") != std::string::npos);
  EXPECT_TRUE(response.find("jp2") != std::string::npos);

  // Search specific term
  response = client.SendCommand("SEARCH posts 機械");
  EXPECT_TRUE(response.find("OK RESULTS 1") == 0);
  EXPECT_TRUE(response.find("jp1") != std::string::npos);
}

/**
 * @brief Test emoji workflow
 */
TEST_F(EndToEndTest, WorkflowWithEmoji) {
  TcpClient client("127.0.0.1", port_);

  // Add documents with emoji
  auto doc_id1 = table_context_.doc_store->AddDocument("emoji1", {});
  table_context_.index->AddDocument(*doc_id1, "Great tutorial 😀");

  auto doc_id2 = table_context_.doc_store->AddDocument("emoji2", {});
  table_context_.index->AddDocument(*doc_id2, "Amazing post 🎉");

  // Search for regular text
  std::string response = client.SendCommand("SEARCH posts tutorial");
  EXPECT_TRUE(response.find("OK RESULTS 1") == 0);
  EXPECT_TRUE(response.find("emoji1") != std::string::npos);
}

/**
 * @brief Test FUZZY search workflow
 */
TEST_F(EndToEndTest, WorkflowWithFuzzySearch) {
  TcpClient client("127.0.0.1", port_);

  // Add documents
  auto doc_id1 = table_context_.doc_store->AddDocument("f1", {});
  table_context_.index->AddDocument(*doc_id1, "hello world");
  table_context_.doc_store->SetNormalizedText(*doc_id1, "hello world");

  auto doc_id2 = table_context_.doc_store->AddDocument("f2", {});
  table_context_.index->AddDocument(*doc_id2, "help world");
  table_context_.doc_store->SetNormalizedText(*doc_id2, "help world");

  auto doc_id3 = table_context_.doc_store->AddDocument("f3", {});
  table_context_.index->AddDocument(*doc_id3, "goodbye earth");
  table_context_.doc_store->SetNormalizedText(*doc_id3, "goodbye earth");

  // Exact search for "hello" — should match only doc with "hello"
  std::string response = client.SendCommand("SEARCH posts hello");
  EXPECT_TRUE(response.find("OK RESULTS") == 0);
  EXPECT_TRUE(response.find("f1") != std::string::npos);

  // Fuzzy search for "helo" (distance 1 from "hello" and "help")
  response = client.SendCommand("SEARCH posts helo FUZZY");
  EXPECT_TRUE(response.find("OK RESULTS") == 0);
  // Should find at least f1 (hello→helo is distance 1)
  EXPECT_TRUE(response.find("f1") != std::string::npos);

  // Fuzzy search for "wrld" (distance 1 from "world")
  response = client.SendCommand("SEARCH posts wrld FUZZY");
  EXPECT_TRUE(response.find("OK RESULTS") == 0);

  //  regression: Short term in multi-term fuzzy should work correctly
  // "hello" AND "x" FUZZY — "x" has no ngrams, should return empty (not silently skip)
  response = client.SendCommand("SEARCH posts hello AND x FUZZY");
  // "x" generates no trigrams, so it should yield 0 results
  EXPECT_TRUE(response.find("OK RESULTS 0") == 0);

  // FUZZY with distance 2
  response = client.SendCommand("SEARCH posts hllo FUZZY 2");
  EXPECT_TRUE(response.find("OK RESULTS") == 0);
}

/**
 * @brief Test FACET command workflow
 */
TEST_F(EndToEndTest, WorkflowWithFacet) {
  TcpClient client("127.0.0.1", port_);

  // Add documents with category filter
  auto doc_id1 = table_context_.doc_store->AddDocument("c1", {{"category", std::string("news")}});
  table_context_.index->AddDocument(*doc_id1, "breaking news today");

  auto doc_id2 = table_context_.doc_store->AddDocument("c2", {{"category", std::string("news")}});
  table_context_.index->AddDocument(*doc_id2, "latest news update");

  auto doc_id3 = table_context_.doc_store->AddDocument("c3", {{"category", std::string("sports")}});
  table_context_.index->AddDocument(*doc_id3, "sports news live");

  auto doc_id4 = table_context_.doc_store->AddDocument("c4", {{"category", std::string("tech")}});
  table_context_.index->AddDocument(*doc_id4, "tech review article");

  // Facet all documents by category
  std::string response = client.SendCommand("FACET posts category");
  EXPECT_TRUE(response.find("OK FACET") == 0);
  EXPECT_TRUE(response.find("news") != std::string::npos);
  EXPECT_TRUE(response.find("sports") != std::string::npos);
  EXPECT_TRUE(response.find("tech") != std::string::npos);

  // Facet with search text — only "news" documents
  response = client.SendCommand("FACET posts category news");
  EXPECT_TRUE(response.find("OK FACET") == 0);
  EXPECT_TRUE(response.find("news") != std::string::npos);

  // Facet with LIMIT
  response = client.SendCommand("FACET posts category LIMIT 1");
  EXPECT_TRUE(response.find("OK FACET 1") == 0);
  // "news" has most docs (3 including "sports news"), should be first
  EXPECT_TRUE(response.find("news") != std::string::npos);

  // Facet on non-existent column — should return empty
  response = client.SendCommand("FACET posts nonexistent");
  EXPECT_TRUE(response.find("OK FACET 0") == 0);

  // Facet on non-existent table — should return error
  response = client.SendCommand("FACET nonexistent category");
  EXPECT_TRUE(response.find("ERROR") == 0);
}

/**
 * @brief Test FACET with NOT filter ( regression)
 */
TEST_F(EndToEndTest, FacetWithNotFilter) {
  TcpClient client("127.0.0.1", port_);

  // Add documents with status filter
  // NOT operates on search text n-grams, so "archived" must NOT appear in the text
  auto doc_id1 = table_context_.doc_store->AddDocument("n1", {{"status", std::string("active")}});
  table_context_.index->AddDocument(*doc_id1, "active document one");

  auto doc_id2 = table_context_.doc_store->AddDocument("n2", {{"status", std::string("active")}});
  table_context_.index->AddDocument(*doc_id2, "active document two");

  auto doc_id3 = table_context_.doc_store->AddDocument("n3", {{"status", std::string("deleted")}});
  table_context_.index->AddDocument(*doc_id3, "removed old entry");

  // Search "document" NOT "removed" — should find n1, n2 (contain "document"), not n3
  std::string response = client.SendCommand("FACET posts status document NOT removed");
  EXPECT_TRUE(response.find("OK FACET") == 0);
  // n1 and n2 match "document" and don't match NOT "removed", so "active" should appear
  EXPECT_TRUE(response.find("active") != std::string::npos);
  // n3 matches NOT "removed", so "deleted" should be excluded
  EXPECT_EQ(response.find("deleted"), std::string::npos);
}

/**
 * @brief Test FACET with FILTER clause
 */
TEST_F(EndToEndTest, FacetWithFilter) {
  TcpClient client("127.0.0.1", port_);

  // Add documents with two filter columns
  auto doc_id1 = table_context_.doc_store->AddDocument(
      "ff1", {{"category", std::string("news")}, {"priority", static_cast<int64_t>(1)}});
  table_context_.index->AddDocument(*doc_id1, "important news");

  auto doc_id2 = table_context_.doc_store->AddDocument(
      "ff2", {{"category", std::string("news")}, {"priority", static_cast<int64_t>(2)}});
  table_context_.index->AddDocument(*doc_id2, "regular news");

  auto doc_id3 = table_context_.doc_store->AddDocument(
      "ff3", {{"category", std::string("sports")}, {"priority", static_cast<int64_t>(1)}});
  table_context_.index->AddDocument(*doc_id3, "important sports");

  // Facet category with filter on priority=1
  std::string response = client.SendCommand("FACET posts category FILTER priority = 1");
  EXPECT_TRUE(response.find("OK FACET") == 0);
  // Both "news" and "sports" have priority=1 docs
  EXPECT_TRUE(response.find("news") != std::string::npos);
  EXPECT_TRUE(response.find("sports") != std::string::npos);
}
