/**
 * @file mygramclient_test.cpp
 * @brief Unit tests for MygramDB client library
 */

#include "client/mygramclient.h"
#include "server/tcp_server.h"
#include "index/index.h"
#include "storage/document_store.h"
#include "utils/string_utils.h"

#include <gtest/gtest.h>
#include <thread>
#include <chrono>

using namespace mygramdb::client;
using namespace mygramdb;

/**
 * @brief Test fixture for MygramClient tests
 */
class MygramClientTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create server components
    auto index = std::make_unique<index::Index>(1);
    auto doc_store = std::make_unique<storage::DocumentStore>();

    // Create table context
    table_context_.name = "test";
    table_context_.config.ngram_size = 1;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);

    // Keep raw pointers for test access
    index_ = table_context_.index.get();
    doc_store_ = table_context_.doc_store.get();

    table_contexts_["test"] = &table_context_;

    // Start server on random port
    server::ServerConfig server_config;
    server_config.port = 0;  // Let OS assign port
    server_config.host = "127.0.0.1";

    server_ = std::make_unique<server::TcpServer>(server_config, table_contexts_);
    ASSERT_TRUE(server_->Start());

    // Wait for server to be ready
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    // Create client
    ClientConfig client_config;
    client_config.host = "127.0.0.1";
    client_config.port = server_->GetPort();
    client_config.timeout_ms = 5000;

    client_ = std::make_unique<MygramClient>(client_config);
  }

  void TearDown() override {
    client_.reset();
    if (server_ && server_->IsRunning()) {
      server_->Stop();
    }
    server_.reset();
  }

  // Helper: Add test documents
  void AddTestDocuments() {
    using storage::DocumentStore;
    using storage::FilterValue;

    // Create filter maps
    std::unordered_map<std::string, FilterValue> filters_active;
    filters_active["status"] = FilterValue("active");

    std::unordered_map<std::string, FilterValue> filters_inactive;
    filters_inactive["status"] = FilterValue("inactive");

    // Add documents with filters
    // Note: Normalize text before adding to index (server does this on search)
    doc_store_->AddDocument("1", filters_active);
    std::string text1 = utils::NormalizeText("Hello world example", true, "keep", true);
    index_->AddDocument(1, text1);

    doc_store_->AddDocument("2", filters_active);
    std::string text2 = utils::NormalizeText("Hello programming", true, "keep", true);
    index_->AddDocument(2, text2);

    doc_store_->AddDocument("3", filters_inactive);
    std::string text3 = utils::NormalizeText("World news today", true, "keep", true);
    index_->AddDocument(3, text3);
  }

  index::Index* index_;  // Raw pointer to table_context_.index
  storage::DocumentStore* doc_store_;  // Raw pointer to table_context_.doc_store
  server::TableContext table_context_;
  std::unordered_map<std::string, server::TableContext*> table_contexts_;
  std::unique_ptr<server::TcpServer> server_;
  std::unique_ptr<MygramClient> client_;
};

/**
 * @brief Test client construction
 */
TEST_F(MygramClientTest, Construction) {
  EXPECT_FALSE(client_->IsConnected());
}

/**
 * @brief Test connection
 */
TEST_F(MygramClientTest, Connect) {
  auto err = client_->Connect();
  EXPECT_FALSE(err.has_value()) << "Connection error: " << err.value_or("");
  EXPECT_TRUE(client_->IsConnected());
}

/**
 * @brief Test disconnect
 */
TEST_F(MygramClientTest, Disconnect) {
  ASSERT_FALSE(client_->Connect().has_value());
  EXPECT_TRUE(client_->IsConnected());

  client_->Disconnect();
  EXPECT_FALSE(client_->IsConnected());
}

/**
 * @brief Test basic search
 */
TEST_F(MygramClientTest, BasicSearch) {
  AddTestDocuments();

  ASSERT_FALSE(client_->Connect().has_value());

  auto result = client_->Search("test", "hello", 100);

  ASSERT_TRUE(std::holds_alternative<SearchResponse>(result))
      << "Search error: " << std::get<Error>(result).message;

  auto resp = std::get<SearchResponse>(result);
  EXPECT_EQ(resp.total_count, 2);  // Documents 1 and 2 contain "hello"
  EXPECT_EQ(resp.results.size(), 2);
}

/**
 * @brief Test search with limit
 */
TEST_F(MygramClientTest, SearchWithLimit) {
  AddTestDocuments();

  ASSERT_FALSE(client_->Connect().has_value());

  auto result = client_->Search("test", "hello", 1);

  ASSERT_TRUE(std::holds_alternative<SearchResponse>(result));

  auto resp = std::get<SearchResponse>(result);
  EXPECT_EQ(resp.total_count, 2);  // Total 2 matches
  EXPECT_EQ(resp.results.size(), 1);  // But only 1 returned due to LIMIT
}

/**
 * @brief Test search with AND terms
 */
TEST_F(MygramClientTest, SearchWithAndTerms) {
  AddTestDocuments();

  ASSERT_FALSE(client_->Connect().has_value());

  std::vector<std::string> and_terms = {"world"};
  auto result = client_->Search("test", "hello", 100, 0, and_terms);

  ASSERT_TRUE(std::holds_alternative<SearchResponse>(result));

  auto resp = std::get<SearchResponse>(result);
  EXPECT_EQ(resp.total_count, 1);  // Only document 1 has both "hello" and "world"
}

/**
 * @brief Test search with NOT terms
 * Note: With unigram (n=1), NOT filtering works at character level,
 * so we need to use distinctive characters that don't overlap.
 */
TEST_F(MygramClientTest, SearchWithNotTerms) {
  AddTestDocuments();

  ASSERT_FALSE(client_->Connect().has_value());

  // Search for "w" (in "world" and "news"), but NOT "x" (only in "example")
  // This should return doc 3 ("World news today") but not doc 1 ("Hello world example")
  std::vector<std::string> not_terms = {"x"};
  auto result = client_->Search("test", "w", 100, 0, {}, not_terms);

  ASSERT_TRUE(std::holds_alternative<SearchResponse>(result))
      << "Search error: " << std::get<Error>(result).message;

  auto resp = std::get<SearchResponse>(result);
  EXPECT_EQ(resp.total_count, 1);  // Only document 3 (doc 1 has "x" in "example")
  EXPECT_EQ(resp.results.size(), 1);
  if (!resp.results.empty()) {
    EXPECT_EQ(resp.results[0].primary_key, "3");
  }
}

/**
 * @brief Test search with filters
 */
TEST_F(MygramClientTest, SearchWithFilters) {
  AddTestDocuments();

  ASSERT_FALSE(client_->Connect().has_value());

  std::vector<std::pair<std::string, std::string>> filters = {{"status", "active"}};
  auto result = client_->Search("test", "hello", 100, 0, {}, {}, filters);

  ASSERT_TRUE(std::holds_alternative<SearchResponse>(result))
      << "Search error: " << std::get<Error>(result).message;

  auto resp = std::get<SearchResponse>(result);
  EXPECT_EQ(resp.total_count, 2);  // Both docs 1 and 2 are active
}

/**
 * @brief Test count query
 */
TEST_F(MygramClientTest, Count) {
  AddTestDocuments();

  ASSERT_FALSE(client_->Connect().has_value());

  auto result = client_->Count("test", "hello");

  ASSERT_TRUE(std::holds_alternative<CountResponse>(result))
      << "Count error: " << std::get<Error>(result).message;

  auto resp = std::get<CountResponse>(result);
  EXPECT_EQ(resp.count, 2);
}

/**
 * @brief Test count with filters
 */
TEST_F(MygramClientTest, CountWithFilters) {
  AddTestDocuments();

  ASSERT_FALSE(client_->Connect().has_value());

  std::vector<std::pair<std::string, std::string>> filters = {{"status", "active"}};
  auto result = client_->Count("test", "world", {}, {}, filters);

  ASSERT_TRUE(std::holds_alternative<CountResponse>(result))
      << "Count error: " << std::get<Error>(result).message;

  auto resp = std::get<CountResponse>(result);
  EXPECT_EQ(resp.count, 1);  // Only doc 1 has "world" and is active
}

/**
 * @brief Test GET command
 */
TEST_F(MygramClientTest, GetDocument) {
  AddTestDocuments();

  ASSERT_FALSE(client_->Connect().has_value());

  auto result = client_->Get("test", "1");

  ASSERT_TRUE(std::holds_alternative<Document>(result))
      << "Get error: " << std::get<Error>(result).message;

  auto doc = std::get<Document>(result);
  EXPECT_EQ(doc.primary_key, "1");

  // Check filter field
  bool found_status = false;
  for (const auto& [key, value] : doc.fields) {
    if (key == "status" && value == "active") {
      found_status = true;
      break;
    }
  }
  EXPECT_TRUE(found_status);
}

/**
 * @brief Test INFO command
 */
TEST_F(MygramClientTest, Info) {
  AddTestDocuments();

  ASSERT_FALSE(client_->Connect().has_value());

  auto result = client_->Info();

  ASSERT_TRUE(std::holds_alternative<ServerInfo>(result))
      << "Info error: " << std::get<Error>(result).message;

  auto info = std::get<ServerInfo>(result);
  EXPECT_FALSE(info.version.empty());
  EXPECT_EQ(info.doc_count, 3);
}

/**
 * @brief Test CONFIG command
 */
TEST_F(MygramClientTest, GetConfig) {
  ASSERT_FALSE(client_->Connect().has_value());

  auto result = client_->GetConfig();

  ASSERT_TRUE(std::holds_alternative<std::string>(result))
      << "GetConfig error: " << std::get<Error>(result).message;

  auto config = std::get<std::string>(result);
  EXPECT_FALSE(config.empty());
}

/**
 * @brief Test debug mode
 */
TEST_F(MygramClientTest, DebugMode) {
  AddTestDocuments();

  ASSERT_FALSE(client_->Connect().has_value());

  // Enable debug mode
  auto err = client_->EnableDebug();
  EXPECT_FALSE(err.has_value()) << "Debug enable error: " << err.value_or("");

  // Perform search - should include debug info
  auto result = client_->Search("test", "hello", 100);

  ASSERT_TRUE(std::holds_alternative<SearchResponse>(result));

  auto resp = std::get<SearchResponse>(result);
  EXPECT_TRUE(resp.debug.has_value());

  // Disable debug mode
  err = client_->DisableDebug();
  EXPECT_FALSE(err.has_value()) << "Debug disable error: " << err.value_or("");
}

/**
 * @brief Test error handling - invalid table
 */
TEST_F(MygramClientTest, ErrorHandling_InvalidTable) {
  AddTestDocuments();

  ASSERT_FALSE(client_->Connect().has_value());

  // Search with invalid table name should return error
  auto result = client_->Search("nonexistent_table", "hello", 100);

  ASSERT_TRUE(std::holds_alternative<Error>(result))
      << "Expected error for invalid table";

  auto err = std::get<Error>(result);
  EXPECT_TRUE(err.message.find("Table not found") != std::string::npos)
      << "Error message: " << err.message;
}

/**
 * @brief Test error handling - not connected
 */
TEST_F(MygramClientTest, ErrorHandling_NotConnected) {
  // Don't connect

  auto result = client_->Search("test", "hello", 100);

  ASSERT_TRUE(std::holds_alternative<Error>(result));
  EXPECT_EQ(std::get<Error>(result).message, "Not connected");
}

/**
 * @brief Test raw SendCommand
 */
TEST_F(MygramClientTest, SendCommand) {
  AddTestDocuments();

  ASSERT_FALSE(client_->Connect().has_value());

  auto result = client_->SendCommand("COUNT test hello");

  ASSERT_TRUE(std::holds_alternative<std::string>(result))
      << "SendCommand error: " << std::get<Error>(result).message;

  auto response = std::get<std::string>(result);
  EXPECT_TRUE(response.find("OK COUNT 2") != std::string::npos);
}

/**
 * @brief Test move semantics
 */
TEST_F(MygramClientTest, MoveSemantics) {
  ASSERT_FALSE(client_->Connect().has_value());
  EXPECT_TRUE(client_->IsConnected());

  // Move construct
  MygramClient moved_client(std::move(*client_));
  EXPECT_TRUE(moved_client.IsConnected());

  // Original should be in valid but unspecified state
  // Just verify we can safely destroy it
}

/**
 * @brief Test 4-byte emoji in search queries
 */
TEST_F(MygramClientTest, EmojiInSearch) {
  // Add documents with emojis
  doc_store_->AddDocument("1", {});
  std::string text1 = utils::NormalizeText("Hello😀World", true, "keep", true);
  index_->AddDocument(1, text1);

  doc_store_->AddDocument("2", {});
  std::string text2 = utils::NormalizeText("😀🎉👍", true, "keep", true);
  index_->AddDocument(2, text2);

  doc_store_->AddDocument("3", {});
  std::string text3 = utils::NormalizeText("Tutorial😀学習", true, "keep", true);
  index_->AddDocument(3, text3);

  ASSERT_FALSE(client_->Connect().has_value());

  // Search for emoji
  auto result = client_->Search("test", "😀", 100);

  ASSERT_TRUE(std::holds_alternative<SearchResponse>(result))
      << "Search error: " << std::get<Error>(result).message;

  auto resp = std::get<SearchResponse>(result);
  EXPECT_EQ(resp.total_count, 3);  // All 3 documents contain 😀
  EXPECT_EQ(resp.results.size(), 3);
}

/**
 * @brief Test multiple emojis in search
 */
TEST_F(MygramClientTest, MultipleEmojisInSearch) {
  // Add documents
  doc_store_->AddDocument("1", {});
  std::string text1 = utils::NormalizeText("😀🎉", true, "keep", true);
  index_->AddDocument(1, text1);

  doc_store_->AddDocument("2", {});
  std::string text2 = utils::NormalizeText("😀👍", true, "keep", true);
  index_->AddDocument(2, text2);

  ASSERT_FALSE(client_->Connect().has_value());

  // Search for specific emoji
  auto result = client_->Search("test", "🎉", 100);

  ASSERT_TRUE(std::holds_alternative<SearchResponse>(result))
      << "Search error: " << std::get<Error>(result).message;

  auto resp = std::get<SearchResponse>(result);
  EXPECT_EQ(resp.total_count, 1);  // Only doc 1 has 🎉
  EXPECT_EQ(resp.results.size(), 1);
  if (!resp.results.empty()) {
    EXPECT_EQ(resp.results[0].primary_key, "1");
  }
}

/**
 * @brief Test emoji with AND search
 */
TEST_F(MygramClientTest, EmojiWithAndSearch) {
  // Add documents
  doc_store_->AddDocument("1", {});
  std::string text1 = utils::NormalizeText("😀ABC", true, "keep", true);
  index_->AddDocument(1, text1);

  doc_store_->AddDocument("2", {});
  std::string text2 = utils::NormalizeText("😀XYZ", true, "keep", true);
  index_->AddDocument(2, text2);

  ASSERT_FALSE(client_->Connect().has_value());

  // Search for emoji AND 'A'
  std::vector<std::string> and_terms = {"A"};
  auto result = client_->Search("test", "😀", 100, 0, and_terms);

  ASSERT_TRUE(std::holds_alternative<SearchResponse>(result))
      << "Search error: " << std::get<Error>(result).message;

  auto resp = std::get<SearchResponse>(result);
  EXPECT_EQ(resp.total_count, 1);  // Only doc 1 has both 😀 and A
  EXPECT_EQ(resp.results.size(), 1);
  if (!resp.results.empty()) {
    EXPECT_EQ(resp.results[0].primary_key, "1");
  }
}
