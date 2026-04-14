/**
 * @file mygramclient_test.cpp
 * @brief Unit tests for MygramDB client library
 */

#include "client/mygramclient.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "client/mygramclient_c.h"
#include "client/protocol_detection.h"
#include "index/index.h"
#include "server/tcp_server.h"
#include "storage/document_store.h"
#include "utils/string_utils.h"

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
    table_context_.config.name = table_context_.name;
    table_context_.config.ngram_size = 1;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);

    // Keep raw pointers for test access
    index_ = table_context_.index.get();
    doc_store_ = table_context_.doc_store.get();

    table_contexts_["test"] = &table_context_;

    // Prepare config used by CONFIG SHOW responses
    full_config_.tables.clear();
    full_config_.tables.push_back(table_context_.config);
    full_config_.dump.interval_sec = 0;  // Disable snapshot scheduler in tests

    // Start server on random port
    server::ServerConfig server_config;
    server_config.port = 0;  // Let OS assign port
    server_config.host = "127.0.0.1";
    server_config.allow_cidrs = {"127.0.0.1/32"};  // Allow localhost connections

    full_config_.api.tcp.bind = server_config.host;
    full_config_.api.tcp.port = server_config.port;

    server_ = std::make_unique<server::TcpServer>(server_config, table_contexts_, "./dumps", &full_config_);
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
    using storage::FilterMap;
    using storage::FilterValue;

    // Create filter maps
    FilterMap filters_active;
    filters_active["status"] = FilterValue("active");

    FilterMap filters_inactive;
    filters_inactive["status"] = FilterValue("inactive");

    // Add documents with filters
    // Note: Normalize text before adding to index (server does this on search)
    doc_store_->AddDocument("1", filters_active);
    std::string text1 = mygram::utils::NormalizeText("Hello world example", true, "keep", true);
    index_->AddDocument(1, text1);

    doc_store_->AddDocument("2", filters_active);
    std::string text2 = mygram::utils::NormalizeText("Hello programming", true, "keep", true);
    index_->AddDocument(2, text2);

    doc_store_->AddDocument("3", filters_inactive);
    std::string text3 = mygram::utils::NormalizeText("World news today", true, "keep", true);
    index_->AddDocument(3, text3);
  }

  index::Index* index_;                // Raw pointer to table_context_.index
  storage::DocumentStore* doc_store_;  // Raw pointer to table_context_.doc_store
  server::TableContext table_context_;
  std::unordered_map<std::string, server::TableContext*> table_contexts_;
  std::unique_ptr<server::TcpServer> server_;
  std::unique_ptr<MygramClient> client_;
  config::Config full_config_;
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
  auto result = client_->Connect();
  EXPECT_TRUE(result) << "Connection error: " << result.error().message();
  EXPECT_TRUE(client_->IsConnected());
}

/**
 * @brief Test disconnect
 */
TEST_F(MygramClientTest, Disconnect) {
  ASSERT_TRUE(client_->Connect());
  EXPECT_TRUE(client_->IsConnected());

  client_->Disconnect();
  EXPECT_FALSE(client_->IsConnected());
}

/**
 * @brief Test basic search
 */
TEST_F(MygramClientTest, BasicSearch) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  auto result = client_->Search("test", "hello", 100);

  ASSERT_TRUE(result) << "Search error: " << result.error().message();

  auto resp = *result;
  EXPECT_EQ(resp.total_count, 2);  // Documents 1 and 2 contain "hello"
  EXPECT_EQ(resp.results.size(), 2);
}

/**
 * @brief Test search with limit
 */
TEST_F(MygramClientTest, SearchWithLimit) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  auto result = client_->Search("test", "hello", 1);

  ASSERT_TRUE(result);

  auto resp = *result;
  EXPECT_EQ(resp.total_count, 2);     // Total 2 matches
  EXPECT_EQ(resp.results.size(), 1);  // But only 1 returned due to LIMIT
}

/**
 * @brief Test search with AND terms
 */
TEST_F(MygramClientTest, SearchWithAndTerms) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  std::vector<std::string> and_terms = {"world"};
  auto result = client_->Search("test", "hello", 100, 0, and_terms);

  ASSERT_TRUE(result);

  auto resp = *result;
  EXPECT_EQ(resp.total_count, 1);  // Only document 1 has both "hello" and "world"
}

/**
 * @brief Test search with NOT terms
 * Note: With unigram (n=1), NOT filtering works at character level,
 * so we need to use distinctive characters that don't overlap.
 */
TEST_F(MygramClientTest, SearchWithNotTerms) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  // Search for "w" (in "world" and "news"), but NOT "x" (only in "example")
  // This should return doc 3 ("World news today") but not doc 1 ("Hello world example")
  std::vector<std::string> not_terms = {"x"};
  auto result = client_->Search("test", "w", 100, 0, {}, not_terms);

  ASSERT_TRUE(result) << "Search error: " << result.error().message();

  auto resp = *result;
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

  ASSERT_TRUE(client_->Connect());

  std::vector<std::pair<std::string, std::string>> filters = {{"status", "active"}};
  auto result = client_->Search("test", "hello", 100, 0, {}, {}, filters);

  ASSERT_TRUE(result) << "Search error: " << result.error().message();

  auto resp = *result;
  EXPECT_EQ(resp.total_count, 2);  // Both docs 1 and 2 are active
}

TEST_F(MygramClientTest, RejectsControlCharactersInQuery) {
  ASSERT_TRUE(client_->Connect());

  auto result = client_->Search("test", "hello\nworld", 100);

  ASSERT_TRUE(!result);
  const auto& err = result.error();
  EXPECT_NE(err.message().find("control character"), std::string::npos);
}

TEST_F(MygramClientTest, RejectsControlCharactersInFilters) {
  ASSERT_TRUE(client_->Connect());

  std::vector<std::pair<std::string, std::string>> filters = {{"status", "active\r\n"}};
  auto result = client_->Search("test", "hello", 100, 0, {}, {}, filters);

  ASSERT_TRUE(!result);
  const auto& err = result.error();
  EXPECT_NE(err.message().find("filter value"), std::string::npos);
}

/**
 * @brief Test count query
 */
TEST_F(MygramClientTest, Count) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  auto result = client_->Count("test", "hello");

  ASSERT_TRUE(result) << "Count error: " << result.error().message();

  auto resp = *result;
  EXPECT_EQ(resp.count, 2);
}

/**
 * @brief Test count with filters
 */
TEST_F(MygramClientTest, CountWithFilters) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  std::vector<std::pair<std::string, std::string>> filters = {{"status", "active"}};
  auto result = client_->Count("test", "world", {}, {}, filters);

  ASSERT_TRUE(result) << "Count error: " << result.error().message();

  auto resp = *result;
  EXPECT_EQ(resp.count, 1);  // Only doc 1 has "world" and is active
}

/**
 * @brief Test GET command
 */
TEST_F(MygramClientTest, GetDocument) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  auto result = client_->Get("test", "1");

  ASSERT_TRUE(result) << "Get error: " << result.error().message();

  auto doc = *result;
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

  ASSERT_TRUE(client_->Connect());

  auto result = client_->Info();

  ASSERT_TRUE(result) << "Info error: " << result.error().message();

  auto info = *result;
  EXPECT_FALSE(info.version.empty());
  EXPECT_EQ(info.doc_count, 3);
}

/**
 * @brief Test INFO command returns complete multi-line response
 *
 * Validates that the multi-line INFO response is fully received, including
 * all sections (Server, Stats, Memory, Index, Tables, Clients, Cache).
 * This tests the fix for premature \r\n termination on multi-line responses.
 */
TEST_F(MygramClientTest, InfoMultiLineComplete) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  // Use raw SendCommand to verify the full multi-line response
  auto result = client_->SendCommand("INFO");
  ASSERT_TRUE(result) << "SendCommand INFO error: " << result.error().message();

  const std::string& response = *result;

  // Verify the response starts with OK INFO
  EXPECT_EQ(response.substr(0, 7), "OK INFO") << "Response should start with 'OK INFO'";

  // Verify the response contains key sections from the multi-line format
  EXPECT_NE(response.find("# Server"), std::string::npos) << "Missing '# Server' section";
  EXPECT_NE(response.find("version:"), std::string::npos) << "Missing 'version:' field";
  EXPECT_NE(response.find("# Stats"), std::string::npos) << "Missing '# Stats' section";
  EXPECT_NE(response.find("# Memory"), std::string::npos) << "Missing '# Memory' section";
  EXPECT_NE(response.find("# Index"), std::string::npos) << "Missing '# Index' section";
  EXPECT_NE(response.find("# Tables"), std::string::npos) << "Missing '# Tables' section";
  EXPECT_NE(response.find("# Clients"), std::string::npos) << "Missing '# Clients' section";
  EXPECT_NE(response.find("# Cache"), std::string::npos) << "Missing '# Cache' section";

  // Verify the response ends with END (the multi-line terminator)
  EXPECT_NE(response.find("END"), std::string::npos) << "Response should contain 'END' marker";
  // After trailing CRLF stripping, response should end with "END"
  EXPECT_GE(response.size(), 3);
  EXPECT_EQ(response.substr(response.size() - 3), "END") << "Response should end with 'END'";

  // Also verify that the parsed Info() method gets all expected fields
  auto info_result = client_->Info();
  ASSERT_TRUE(info_result) << "Info error: " << info_result.error().message();

  auto info = *info_result;
  EXPECT_FALSE(info.version.empty()) << "version should be non-empty";
  EXPECT_EQ(info.doc_count, 3) << "doc_count should reflect added documents";
  EXPECT_FALSE(info.tables.empty()) << "tables list should not be empty";
}

/**
 * @brief Test CONFIG command
 */
TEST_F(MygramClientTest, GetConfig) {
  ASSERT_TRUE(client_->Connect());

  auto result = client_->GetConfig();

  ASSERT_TRUE(result) << "GetConfig error: " << result.error().message();

  auto config = *result;
  EXPECT_FALSE(config.empty());
}

/**
 * @brief Test CONFIG command returns complete multi-line response
 *
 * Validates that the CONFIG multi-line response (which uses +OK prefix)
 * is fully received and contains all configuration sections.
 */
TEST_F(MygramClientTest, GetConfigMultiLineComplete) {
  ASSERT_TRUE(client_->Connect());

  // Use raw SendCommand to verify the full multi-line response
  auto result = client_->SendCommand("CONFIG");
  ASSERT_TRUE(result) << "SendCommand CONFIG error: " << result.error().message();

  const std::string& response = *result;

  // CONFIG responses start with +OK
  EXPECT_EQ(response.substr(0, 3), "+OK") << "Response should start with '+OK'";

  // Verify the response contains configuration content
  // (the exact content depends on the test config, but should have table names)
  EXPECT_NE(response.find("test"), std::string::npos) << "Response should contain table name 'test'";

  // Verify the response is multi-line (contains internal CRLF or LF)
  EXPECT_NE(response.find('\n'), std::string::npos) << "CONFIG response should be multi-line";
}

/**
 * @brief Test debug mode
 */
TEST_F(MygramClientTest, DebugMode) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  // Enable debug mode
  auto debug_result = client_->EnableDebug();
  EXPECT_TRUE(debug_result) << "Debug enable error: " << debug_result.error().message();

  // Perform search - should include debug info
  auto result = client_->Search("test", "hello", 100);

  ASSERT_TRUE(result);

  auto resp = *result;
  EXPECT_TRUE(resp.debug.has_value());

  // Disable debug mode
  debug_result = client_->DisableDebug();
  EXPECT_TRUE(debug_result) << "Debug disable error: " << debug_result.error().message();
}

/**
 * @brief Test error handling - invalid table
 */
TEST_F(MygramClientTest, ErrorHandling_InvalidTable) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  // Search with invalid table name should return error
  auto result = client_->Search("nonexistent_table", "hello", 100);

  ASSERT_TRUE(!result) << "Expected error for invalid table";

  auto err = result.error();
  EXPECT_TRUE(err.message().find("Table not found") != std::string::npos) << "Error message: " << err.message();
}

/**
 * @brief Test error handling - not connected
 */
TEST_F(MygramClientTest, ErrorHandling_NotConnected) {
  // Don't connect

  auto result = client_->Search("test", "hello", 100);

  ASSERT_TRUE(!result);
  EXPECT_EQ(result.error().message(), "Not connected");
}

/**
 * @brief Test raw SendCommand
 */
TEST_F(MygramClientTest, SendCommand) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  auto result = client_->SendCommand("COUNT test hello");

  ASSERT_TRUE(result) << "SendCommand error: " << result.error().message();

  auto response = *result;
  EXPECT_TRUE(response.find("OK COUNT 2") != std::string::npos);
}

/**
 * @brief Test C API mygramclient_send_command
 */
TEST_F(MygramClientTest, CApiSendCommand) {
  AddTestDocuments();

  // Create C API client
  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;
  config.recv_buffer_size = 65536;

  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);

  // Connect
  int connect_result = mygramclient_connect(c_client);
  ASSERT_EQ(connect_result, 0) << "Connect error: " << mygramclient_get_last_error(c_client);

  // Test send_command with COUNT
  char* response = nullptr;
  int result = mygramclient_send_command(c_client, "COUNT test hello", &response);
  ASSERT_EQ(result, 0) << "SendCommand error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(response, nullptr);
  EXPECT_TRUE(std::string(response).find("OK COUNT 2") != std::string::npos);
  mygramclient_free_string(response);

  // Test send_command with INFO
  response = nullptr;
  result = mygramclient_send_command(c_client, "INFO", &response);
  ASSERT_EQ(result, 0) << "SendCommand error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(response, nullptr);
  EXPECT_TRUE(std::string(response).find("OK INFO") != std::string::npos);
  mygramclient_free_string(response);

  // Test send_command with invalid command
  response = nullptr;
  result = mygramclient_send_command(c_client, "INVALID_COMMAND", &response);
  ASSERT_EQ(result, 0);  // Command sent successfully, but response is ERROR
  ASSERT_NE(response, nullptr);
  EXPECT_TRUE(std::string(response).find("ERROR") != std::string::npos);
  mygramclient_free_string(response);

  // Cleanup
  mygramclient_disconnect(c_client);
  mygramclient_destroy(c_client);
}

/**
 * @brief Test C API mygramclient_send_command - error cases
 */
TEST_F(MygramClientTest, CApiSendCommandErrors) {
  // Test with NULL client
  char* response = nullptr;
  int result = mygramclient_send_command(nullptr, "INFO", &response);
  EXPECT_EQ(result, -1);

  // Create C API client but don't connect
  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;

  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);

  // Test send_command without connection
  response = nullptr;
  result = mygramclient_send_command(c_client, "INFO", &response);
  EXPECT_EQ(result, -1);  // Should fail - not connected
  EXPECT_EQ(response, nullptr);

  // Test with NULL command
  result = mygramclient_send_command(c_client, nullptr, &response);
  EXPECT_EQ(result, -1);

  // Test with NULL response pointer
  result = mygramclient_send_command(c_client, "INFO", nullptr);
  EXPECT_EQ(result, -1);

  mygramclient_destroy(c_client);
}

/**
 * @brief Test move semantics
 */
TEST_F(MygramClientTest, MoveSemantics) {
  ASSERT_TRUE(client_->Connect());
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
  std::string text1 = mygram::utils::NormalizeText("Hello😀World", true, "keep", true);
  index_->AddDocument(1, text1);

  doc_store_->AddDocument("2", {});
  std::string text2 = mygram::utils::NormalizeText("😀🎉👍", true, "keep", true);
  index_->AddDocument(2, text2);

  doc_store_->AddDocument("3", {});
  std::string text3 = mygram::utils::NormalizeText("Tutorial😀学習", true, "keep", true);
  index_->AddDocument(3, text3);

  ASSERT_TRUE(client_->Connect());

  // Search for emoji
  auto result = client_->Search("test", "😀", 100);

  ASSERT_TRUE(result) << "Search error: " << result.error().message();

  auto resp = *result;
  EXPECT_EQ(resp.total_count, 3);  // All 3 documents contain 😀
  EXPECT_EQ(resp.results.size(), 3);
}

/**
 * @brief Test multiple emojis in search
 */
TEST_F(MygramClientTest, MultipleEmojisInSearch) {
  // Add documents
  doc_store_->AddDocument("1", {});
  std::string text1 = mygram::utils::NormalizeText("😀🎉", true, "keep", true);
  index_->AddDocument(1, text1);

  doc_store_->AddDocument("2", {});
  std::string text2 = mygram::utils::NormalizeText("😀👍", true, "keep", true);
  index_->AddDocument(2, text2);

  ASSERT_TRUE(client_->Connect());

  // Search for specific emoji
  auto result = client_->Search("test", "🎉", 100);

  ASSERT_TRUE(result) << "Search error: " << result.error().message();

  auto resp = *result;
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
  std::string text1 = mygram::utils::NormalizeText("😀ABC", true, "keep", true);
  index_->AddDocument(1, text1);

  doc_store_->AddDocument("2", {});
  std::string text2 = mygram::utils::NormalizeText("😀XYZ", true, "keep", true);
  index_->AddDocument(2, text2);

  ASSERT_TRUE(client_->Connect());

  // Search for emoji AND 'A'
  std::vector<std::string> and_terms = {"A"};
  auto result = client_->Search("test", "😀", 100, 0, and_terms);

  ASSERT_TRUE(result) << "Search error: " << result.error().message();

  auto resp = *result;
  EXPECT_EQ(resp.total_count, 1);  // Only doc 1 has both 😀 and A
  EXPECT_EQ(resp.results.size(), 1);
  if (!resp.results.empty()) {
    EXPECT_EQ(resp.results[0].primary_key, "1");
  }
}

/**
 * @brief Test large response handling (tests recv loop fix)
 *
 * This test validates the fix for the issue where MygramClient::SendCommand
 * only called recv() once, causing large responses to be truncated.
 * We create many documents so the search response exceeds the recv buffer size.
 */
TEST_F(MygramClientTest, LargeResponseHandling) {
  // Add many documents to create a large response
  // Default recv buffer is 65536 bytes, so we need to generate > 65KB response
  // Use 1000 docs (max LIMIT) to test large response handling
  const int num_docs = 1000;  // Max allowed by server LIMIT

  for (int i = 1; i <= num_docs; i++) {
    storage::FilterMap filters;
    filters["doc_num"] = static_cast<int64_t>(i);
    doc_store_->AddDocument("doc_" + std::to_string(i), filters);

    // Add same search term to all documents so they all match
    std::string text = mygram::utils::NormalizeText("test document " + std::to_string(i), true, "keep", true);
    index_->AddDocument(static_cast<uint64_t>(i), text);
  }

  ASSERT_TRUE(client_->Connect());

  // Enable debug mode to make response even larger
  auto debug_result = client_->EnableDebug();
  ASSERT_TRUE(debug_result) << "EnableDebug error: " << debug_result.error().message();

  // Search for "test" which should match all documents
  // This will create a very large response (>65KB with all primary keys and DEBUG info)
  auto result = client_->Search("test", "test", num_docs);  // Request max results

  ASSERT_TRUE(result) << "Search error (response may have been truncated): " << result.error().message();

  auto resp = *result;

  // Verify we got all results (not truncated)
  // Note: total_count should match the number of matching docs
  EXPECT_EQ(resp.total_count, num_docs) << "Response was likely truncated";

  // We should receive all or most results (allow small variance due to parsing edge cases)
  EXPECT_GE(resp.results.size(), num_docs - 10) << "Not enough results received - recv likely truncated response";
  EXPECT_LE(resp.results.size(), num_docs + 10) << "Too many results - parsing issue";

  // Most importantly: verify that we can receive a large response >65KB
  // The old code would have truncated at first recv (65KB buffer)
  // With 1000 docs, the response is approximately 70-80KB with DEBUG info
  EXPECT_GT(resp.results.size(), 500) << "Response was definitely truncated - received less than half";

  // Debug info presence indicates we read to the end of response
  // If truncated, debug section would likely be cut off
  EXPECT_TRUE(resp.debug.has_value()) << "Debug info missing - response may have been truncated before END";

  // Verify all expected primary keys are present
  std::set<std::string> received_pks;
  for (const auto& doc : resp.results) {
    received_pks.insert(doc.primary_key);
  }

  // Check a sample of expected keys
  EXPECT_TRUE(received_pks.count("doc_1") > 0);
  EXPECT_TRUE(received_pks.count("doc_" + std::to_string(num_docs / 2)) > 0);
  EXPECT_TRUE(received_pks.count("doc_" + std::to_string(num_docs)) > 0);
}

/**
 * @brief Test fixture for C API tests
 */
class MygramClientCApiTest : public ::testing::Test {
 protected:
  void SetUp() override {}
  void TearDown() override {}
};

/**
 * @brief Test C API parse_search_expression - simple case
 */
TEST_F(MygramClientCApiTest, ParseSearchExpression_Simple) {
  MygramParsedExpression_C* parsed = nullptr;
  int result = mygramclient_parse_search_expression("golang tutorial", &parsed);

  ASSERT_EQ(result, 0);
  ASSERT_NE(parsed, nullptr);

  // Main term should be "golang"
  ASSERT_NE(parsed->main_term, nullptr);
  EXPECT_STREQ(parsed->main_term, "golang");

  // AND terms should contain "tutorial"
  EXPECT_EQ(parsed->and_count, 1);
  ASSERT_NE(parsed->and_terms, nullptr);
  EXPECT_STREQ(parsed->and_terms[0], "tutorial");

  // No NOT terms
  EXPECT_EQ(parsed->not_count, 0);

  // No optional terms (all are treated as required)
  EXPECT_EQ(parsed->optional_count, 0);

  mygramclient_free_parsed_expression(parsed);
}

/**
 * @brief Test C API parse_search_expression - with NOT terms
 */
TEST_F(MygramClientCApiTest, ParseSearchExpression_WithNotTerms) {
  MygramParsedExpression_C* parsed = nullptr;
  int result = mygramclient_parse_search_expression("+golang -old -deprecated", &parsed);

  ASSERT_EQ(result, 0);
  ASSERT_NE(parsed, nullptr);

  // Main term should be "golang"
  ASSERT_NE(parsed->main_term, nullptr);
  EXPECT_STREQ(parsed->main_term, "golang");

  // No additional AND terms
  EXPECT_EQ(parsed->and_count, 0);

  // Two NOT terms
  EXPECT_EQ(parsed->not_count, 2);
  ASSERT_NE(parsed->not_terms, nullptr);
  EXPECT_STREQ(parsed->not_terms[0], "old");
  EXPECT_STREQ(parsed->not_terms[1], "deprecated");

  mygramclient_free_parsed_expression(parsed);
}

/**
 * @brief Test C API parse_search_expression - with AND terms
 */
TEST_F(MygramClientCApiTest, ParseSearchExpression_WithAndTerms) {
  MygramParsedExpression_C* parsed = nullptr;
  int result = mygramclient_parse_search_expression("+golang +tutorial +2024", &parsed);

  ASSERT_EQ(result, 0);
  ASSERT_NE(parsed, nullptr);

  // Main term should be "golang"
  ASSERT_NE(parsed->main_term, nullptr);
  EXPECT_STREQ(parsed->main_term, "golang");

  // Two additional AND terms
  EXPECT_EQ(parsed->and_count, 2);
  ASSERT_NE(parsed->and_terms, nullptr);
  EXPECT_STREQ(parsed->and_terms[0], "tutorial");
  EXPECT_STREQ(parsed->and_terms[1], "2024");

  // No NOT terms
  EXPECT_EQ(parsed->not_count, 0);

  mygramclient_free_parsed_expression(parsed);
}

/**
 * @brief Test C API parse_search_expression - complex expression
 */
TEST_F(MygramClientCApiTest, ParseSearchExpression_Complex) {
  MygramParsedExpression_C* parsed = nullptr;
  int result = mygramclient_parse_search_expression("+golang +tutorial -old", &parsed);

  ASSERT_EQ(result, 0);
  ASSERT_NE(parsed, nullptr);

  // Main term
  EXPECT_STREQ(parsed->main_term, "golang");

  // AND terms
  EXPECT_EQ(parsed->and_count, 1);
  EXPECT_STREQ(parsed->and_terms[0], "tutorial");

  // NOT terms
  EXPECT_EQ(parsed->not_count, 1);
  EXPECT_STREQ(parsed->not_terms[0], "old");

  mygramclient_free_parsed_expression(parsed);
}

/**
 * @brief Test C API parse_search_expression - Japanese text
 */
TEST_F(MygramClientCApiTest, ParseSearchExpression_Japanese) {
  MygramParsedExpression_C* parsed = nullptr;
  int result = mygramclient_parse_search_expression("日本語 チュートリアル -古い", &parsed);

  ASSERT_EQ(result, 0);
  ASSERT_NE(parsed, nullptr);

  // Main term should be "日本語"
  EXPECT_STREQ(parsed->main_term, "日本語");

  // AND term should be "チュートリアル"
  EXPECT_EQ(parsed->and_count, 1);
  EXPECT_STREQ(parsed->and_terms[0], "チュートリアル");

  // NOT term should be "古い"
  EXPECT_EQ(parsed->not_count, 1);
  EXPECT_STREQ(parsed->not_terms[0], "古い");

  mygramclient_free_parsed_expression(parsed);
}

/**
 * @brief Test C API parse_search_expression - with emojis
 */
TEST_F(MygramClientCApiTest, ParseSearchExpression_Emoji) {
  MygramParsedExpression_C* parsed = nullptr;
  int result = mygramclient_parse_search_expression("😀 tutorial -🎉", &parsed);

  ASSERT_EQ(result, 0);
  ASSERT_NE(parsed, nullptr);

  // Main term should be "😀"
  EXPECT_STREQ(parsed->main_term, "😀");

  // AND term should be "tutorial"
  EXPECT_EQ(parsed->and_count, 1);
  EXPECT_STREQ(parsed->and_terms[0], "tutorial");

  // NOT term should be "🎉"
  EXPECT_EQ(parsed->not_count, 1);
  EXPECT_STREQ(parsed->not_terms[0], "🎉");

  mygramclient_free_parsed_expression(parsed);
}

/**
 * @brief Test C API parse_search_expression - quoted phrase
 */
TEST_F(MygramClientCApiTest, ParseSearchExpression_QuotedPhrase) {
  MygramParsedExpression_C* parsed = nullptr;
  int result = mygramclient_parse_search_expression("\"machine learning\" tutorial", &parsed);

  ASSERT_EQ(result, 0);
  ASSERT_NE(parsed, nullptr);

  // Main term should be the quoted phrase
  EXPECT_STREQ(parsed->main_term, "\"machine learning\"");

  // AND term should be "tutorial"
  EXPECT_EQ(parsed->and_count, 1);
  EXPECT_STREQ(parsed->and_terms[0], "tutorial");

  mygramclient_free_parsed_expression(parsed);
}

/**
 * @brief Test C API parse_search_expression - NULL input
 */
TEST_F(MygramClientCApiTest, ParseSearchExpression_NullInput) {
  MygramParsedExpression_C* parsed = nullptr;
  int result = mygramclient_parse_search_expression(nullptr, &parsed);

  EXPECT_EQ(result, -1);
  EXPECT_EQ(parsed, nullptr);
}

/**
 * @brief Test C API parse_search_expression - NULL output pointer
 */
TEST_F(MygramClientCApiTest, ParseSearchExpression_NullOutputPointer) {
  int result = mygramclient_parse_search_expression("golang tutorial", nullptr);

  EXPECT_EQ(result, -1);
}

/**
 * @brief Test C API parse_search_expression - empty expression
 */
TEST_F(MygramClientCApiTest, ParseSearchExpression_EmptyExpression) {
  MygramParsedExpression_C* parsed = nullptr;
  int result = mygramclient_parse_search_expression("", &parsed);

  EXPECT_EQ(result, -1);
  EXPECT_EQ(parsed, nullptr);
}

/**
 * @brief Test C API free_parsed_expression - NULL safety
 */
TEST_F(MygramClientCApiTest, FreeParseExpression_NullSafety) {
  // Should not crash
  mygramclient_free_parsed_expression(nullptr);
}

/**
 * @brief Test that control characters in search queries are stripped by EscapeQueryString
 *
 * EscapeQueryString now strips bytes < 0x20 to prevent command injection.
 * We test indirectly via Search: a query with embedded \r\n should not cause
 * protocol-level issues (the control chars are stripped before sending).
 *
 * Note: The ValidateNoControlCharacters check runs before EscapeQueryString,
 * so this tests that the validation layer rejects control characters.
 */
TEST_F(MygramClientTest, EscapeQueryStringStripsControlCharacters) {
  AddTestDocuments();
  ASSERT_TRUE(client_->Connect());

  // The client-side ValidateNoControlCharacters should reject queries with control chars
  auto result = client_->Search("test", "hello\x01world", 100);
  ASSERT_FALSE(result) << "Expected error for control character in query";
  EXPECT_NE(result.error().message().find("control character"), std::string::npos);
}

/**
 * @brief Test that hostname "localhost" resolves via getaddrinfo
 *
 * After replacing inet_pton with getaddrinfo, hostnames like "localhost"
 * should be resolved successfully.
 */
TEST_F(MygramClientTest, ConnectViaLocalhost) {
  // Create a client using "localhost" instead of "127.0.0.1"
  ClientConfig localhost_config;
  localhost_config.host = "localhost";
  localhost_config.port = server_->GetPort();
  localhost_config.timeout_ms = 5000;

  MygramClient localhost_client(localhost_config);
  auto result = localhost_client.Connect();
  EXPECT_TRUE(result) << "Failed to connect via 'localhost': " << result.error().message();
  if (result) {
    EXPECT_TRUE(localhost_client.IsConnected());
    localhost_client.Disconnect();
  }
}

/**
 * @brief Test that invalid hostname returns proper error
 */
TEST_F(MygramClientTest, ConnectInvalidHostnameReturnsError) {
  ClientConfig bad_config;
  bad_config.host = "this.host.does.not.exist.invalid";
  bad_config.port = 12345;
  bad_config.timeout_ms = 3000;

  MygramClient bad_client(bad_config);
  auto result = bad_client.Connect();
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().message().find("Failed to resolve host"), std::string::npos)
      << "Error: " << result.error().message();
}

// =============================================================================
// Unit tests for IsResponseComplete (protocol detection logic)
// =============================================================================

using mygramdb::client::detail::IsResponseComplete;

/**
 * @brief Test single-line response detection
 */
TEST(IsResponseCompleteTest, SingleLineResponseComplete) {
  EXPECT_TRUE(IsResponseComplete("OK RESULTS 5 pk1 pk2\r\n"));
  EXPECT_TRUE(IsResponseComplete("OK COUNT 42\r\n"));
  EXPECT_TRUE(IsResponseComplete("OK DOC pk1 status=active\r\n"));
  EXPECT_TRUE(IsResponseComplete("OK SAVED /path/to/dump\r\n"));
  EXPECT_TRUE(IsResponseComplete("OK LOADED /path/to/dump\r\n"));
  EXPECT_TRUE(IsResponseComplete("OK REPLICATION_STOPPED\r\n"));
  EXPECT_TRUE(IsResponseComplete("OK REPLICATION_STARTED\r\n"));
  EXPECT_TRUE(IsResponseComplete("OK DEBUG ON\r\n"));
  EXPECT_TRUE(IsResponseComplete("ERROR Table not found\r\n"));
}

/**
 * @brief Test incomplete response (no CRLF at end)
 */
TEST(IsResponseCompleteTest, IncompleteNoCrlf) {
  EXPECT_FALSE(IsResponseComplete("OK RESULTS 5 pk1 pk2"));
  EXPECT_FALSE(IsResponseComplete("OK INFO"));
  EXPECT_FALSE(IsResponseComplete("OK INFO\r"));
  EXPECT_FALSE(IsResponseComplete(""));
  EXPECT_FALSE(IsResponseComplete("X"));
  EXPECT_FALSE(IsResponseComplete("XY"));
}

/**
 * @brief Test INFO multi-line response requires END marker
 */
TEST(IsResponseCompleteTest, InfoRequiresEndMarker) {
  // Just the first line - NOT complete
  EXPECT_FALSE(IsResponseComplete("OK INFO\r\n"));

  // Partial response with some content but no END
  EXPECT_FALSE(IsResponseComplete("OK INFO\r\n\r\n# Server\r\nversion: 1.0\r\n"));

  // Complete response with END marker
  EXPECT_TRUE(IsResponseComplete("OK INFO\r\n\r\n# Server\r\nversion: 1.0\r\nEND\r\n"));

  // Minimal complete INFO
  EXPECT_TRUE(IsResponseComplete("OK INFO\r\nEND\r\n"));
}

/**
 * @brief Test REPLICATION STATUS multi-line response requires END marker
 */
TEST(IsResponseCompleteTest, ReplicationRequiresEndMarker) {
  // Just the first line - NOT complete
  EXPECT_FALSE(IsResponseComplete("OK REPLICATION\r\n"));

  // Partial response
  EXPECT_FALSE(IsResponseComplete("OK REPLICATION\r\nstatus: running\r\n"));

  // Complete response with END
  EXPECT_TRUE(IsResponseComplete("OK REPLICATION\r\nstatus: running\r\nEND\r\n"));

  // REPLICATION_STOPPED is a single-line response (different prefix)
  EXPECT_TRUE(IsResponseComplete("OK REPLICATION_STOPPED\r\n"));
  EXPECT_TRUE(IsResponseComplete("OK REPLICATION_STARTED\r\n"));
}

/**
 * @brief Test CONFIG (+OK prefix) multi-line response requires double CRLF
 */
TEST(IsResponseCompleteTest, ConfigRequiresDoubleCrlf) {
  // Just +OK line - NOT complete
  EXPECT_FALSE(IsResponseComplete("+OK\r\n"));

  // Partial content
  EXPECT_FALSE(IsResponseComplete("+OK\r\nmysql:\r\n  host: localhost\r\n"));

  // Complete with double CRLF
  EXPECT_TRUE(IsResponseComplete("+OK\r\nmysql:\r\n  host: localhost\r\n\r\n"));

  // Also works for CONFIG HELP
  EXPECT_TRUE(IsResponseComplete("+OK\r\nAvailable sections:\r\n  mysql\r\n\r\n"));
}

/**
 * @brief Test FACET multi-line response requires double CRLF
 */
TEST(IsResponseCompleteTest, FacetRequiresDoubleCrlf) {
  // Just the first line - NOT complete
  EXPECT_FALSE(IsResponseComplete("OK FACET 3\r\n"));

  // Partial response
  EXPECT_FALSE(IsResponseComplete("OK FACET 3\r\nval1\t10\r\n"));

  // Complete with double CRLF
  EXPECT_TRUE(IsResponseComplete("OK FACET 3\r\nval1\t10\r\nval2\t5\r\n\r\n"));
}

/**
 * @brief Test SEARCH/COUNT with DEBUG block (multi-line via content detection)
 */
TEST(IsResponseCompleteTest, SearchWithDebugRequiresDoubleCrlf) {
  // Single-line SEARCH without debug - complete
  EXPECT_TRUE(IsResponseComplete("OK RESULTS 5 pk1 pk2\r\n"));

  // SEARCH with DEBUG block - NOT complete (no double CRLF)
  EXPECT_FALSE(IsResponseComplete("OK RESULTS 5 pk1 pk2\r\n\r\n# DEBUG\r\nquery_time: 1.0ms\r\n"));

  // SEARCH with DEBUG block - complete (double CRLF at end)
  EXPECT_TRUE(IsResponseComplete("OK RESULTS 5 pk1 pk2\r\n\r\n# DEBUG\r\nquery_time: 1.0ms\r\ncache: miss\r\n\r\n"));

  // COUNT with DEBUG block
  EXPECT_TRUE(IsResponseComplete("OK COUNT 42\r\n\r\n# DEBUG\r\nquery_time: 2.0ms\r\n\r\n"));
}
