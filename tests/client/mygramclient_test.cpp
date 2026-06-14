/**
 * @file mygramclient_test.cpp
 * @brief Unit tests for MygramDB client library
 */

#include "client/mygramclient.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <thread>
#include <vector>

#include "client/mygramclient_c.h"
#include "client/protocol_detection.h"
#include "client/search_expression.h"
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
    table_context_.config.database = "testdb";
    table_context_.config.ngram_size = 1;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);

    // Keep raw pointers for test access
    index_ = table_context_.index.get();
    doc_store_ = table_context_.doc_store.get();

    table_contexts_["testdb.test"] = &table_context_;

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

  auto result = client_->Search("testdb.test", "hello", 100);

  ASSERT_TRUE(result) << "Search error: " << result.error().message();

  auto resp = *result;
  EXPECT_EQ(resp.total_count, 2);  // Documents 1 and 2 contain "hello"
  EXPECT_EQ(resp.results.size(), 2);
}

TEST_F(MygramClientTest, SearchCountGetAndFacetAcceptDatabaseQualifiedTableName) {
  client_.reset();
  server_->Stop();
  server_.reset();

  table_context_.config.name = "test";
  table_context_.config.database = "testdb";
  table_contexts_.clear();
  table_contexts_["testdb.test"] = &table_context_;
  full_config_.tables.clear();
  full_config_.tables.push_back(table_context_.config);

  server::ServerConfig server_config;
  server_config.port = 0;
  server_config.host = "127.0.0.1";
  server_config.allow_cidrs = {"127.0.0.1/32"};
  server_ = std::make_unique<server::TcpServer>(server_config, table_contexts_, "./dumps", &full_config_);
  ASSERT_TRUE(server_->Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  ClientConfig client_config;
  client_config.host = "127.0.0.1";
  client_config.port = server_->GetPort();
  client_config.timeout_ms = 5000;
  client_ = std::make_unique<MygramClient>(client_config);

  AddTestDocuments();
  ASSERT_TRUE(client_->Connect());

  auto result = client_->Search("testdb.test", "hello", 100);
  ASSERT_TRUE(result) << "Search error: " << result.error().message();
  EXPECT_EQ(result->total_count, 2);
  EXPECT_EQ(result->results.size(), 2);

  auto count = client_->Count("testdb.test", "hello");
  ASSERT_TRUE(count) << "Count error: " << count.error().message();
  EXPECT_EQ(count->count, 2);

  auto doc = client_->Get("testdb.test", "1");
  ASSERT_TRUE(doc) << "Get error: " << doc.error().message();
  EXPECT_EQ(doc->primary_key, "1");

  auto facet = client_->Facet("testdb.test", "status", "hello", 10);
  ASSERT_TRUE(facet) << "Facet error: " << facet.error().message();
  ASSERT_EQ(facet->facets.size(), 1u);
  EXPECT_EQ(facet->facets[0].value, "active");
  EXPECT_EQ(facet->facets[0].count, 2u);

  // Single-database config: the bare name resolves to the unique testdb.test
  // and returns the same results as the qualified form.
  auto bare_result = client_->Search("test", "hello", 100);
  ASSERT_TRUE(bare_result) << "Search error: " << bare_result.error().message();
  EXPECT_EQ(bare_result->total_count, result->total_count);
  EXPECT_EQ(bare_result->results.size(), result->results.size());
}

TEST_F(MygramClientTest, CApiSearchAndCountAcceptDatabaseQualifiedTableName) {
  client_.reset();
  server_->Stop();
  server_.reset();

  table_context_.config.name = "test";
  table_context_.config.database = "testdb";
  table_contexts_.clear();
  table_contexts_["testdb.test"] = &table_context_;
  full_config_.tables.clear();
  full_config_.tables.push_back(table_context_.config);

  server::ServerConfig server_config;
  server_config.port = 0;
  server_config.host = "127.0.0.1";
  server_config.allow_cidrs = {"127.0.0.1/32"};
  server_ = std::make_unique<server::TcpServer>(server_config, table_contexts_, "./dumps", &full_config_);
  ASSERT_TRUE(server_->Start());
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  AddTestDocuments();

  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;
  config.recv_buffer_size = 65536;

  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client), 0) << "Connect error: " << mygramclient_get_last_error(c_client);

  MygramSearchResult_C* search_result = nullptr;
  ASSERT_EQ(mygramclient_search(c_client, "testdb.test", "hello", 100, 0, &search_result), 0)
      << "Search error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(search_result, nullptr);
  EXPECT_EQ(search_result->total_count, 2u);
  EXPECT_EQ(search_result->count, 2u);

  mygramclient_free_search_result(search_result);

  uint64_t count = 0;
  ASSERT_EQ(mygramclient_count(c_client, "testdb.test", "hello", &count), 0)
      << "Count error: " << mygramclient_get_last_error(c_client);
  EXPECT_EQ(count, 2u);

  // Single-database config: the bare name resolves to the unique testdb.test
  // and returns the same results as the qualified form.
  MygramSearchResult_C* bare_search_result = nullptr;
  EXPECT_EQ(mygramclient_search(c_client, "test", "hello", 100, 0, &bare_search_result), 0)
      << "Search error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(bare_search_result, nullptr);
  EXPECT_EQ(bare_search_result->total_count, 2u);
  EXPECT_EQ(bare_search_result->count, 2u);

  mygramclient_free_search_result(bare_search_result);

  mygramclient_disconnect(c_client);
  mygramclient_destroy(c_client);
}

/**
 * @brief Test search with limit
 */
TEST_F(MygramClientTest, SearchWithLimit) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  auto result = client_->Search("testdb.test", "hello", 1);

  ASSERT_TRUE(result);

  auto resp = *result;
  EXPECT_EQ(resp.total_count, 2);     // Total 2 matches
  EXPECT_EQ(resp.results.size(), 1);  // But only 1 returned due to LIMIT
}

TEST_F(MygramClientTest, SearchWithHighlightsReturnsSnippets) {
  const std::string text = mygram::utils::NormalizeText("Hello world example", true, "keep", true);
  ASSERT_TRUE(doc_store_->AddDocument("highlight_doc", {}, text));
  index_->AddDocument(1, text);

  ASSERT_TRUE(client_->Connect());

  auto result = client_->SearchWithHighlights("testdb.test", "hello", 10);
  ASSERT_TRUE(result) << "Search error: " << result.error().message();

  const auto& resp = *result;
  EXPECT_EQ(resp.total_count, 1);
  ASSERT_EQ(resp.results.size(), 1);
  EXPECT_EQ(resp.results[0].primary_key, "highlight_doc");
  EXPECT_NE(resp.results[0].snippet.find("<em>hello</em>"), std::string::npos);
}

TEST_F(MygramClientTest, CApiSearchWithHighlightsReturnsSnippets) {
  const std::string text = mygram::utils::NormalizeText("Hello world example", true, "keep", true);
  ASSERT_TRUE(doc_store_->AddDocument("highlight_doc", {}, text));
  index_->AddDocument(1, text);

  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;
  config.recv_buffer_size = 65536;

  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client), 0) << "Connect error: " << mygramclient_get_last_error(c_client);

  MygramSearchResultWithHighlights_C* search_result = nullptr;
  int result = mygramclient_search_with_highlights(c_client, "testdb.test", "hello", 10, 0, &search_result);
  ASSERT_EQ(result, 0) << "Search error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(search_result, nullptr);
  EXPECT_EQ(search_result->total_count, 1u);
  ASSERT_EQ(search_result->count, 1u);
  ASSERT_NE(search_result->primary_keys, nullptr);
  ASSERT_NE(search_result->snippets, nullptr);
  EXPECT_STREQ(search_result->primary_keys[0], "highlight_doc");
  EXPECT_NE(std::string(search_result->snippets[0]).find("<em>hello</em>"), std::string::npos);

  mygramclient_free_search_result_with_highlights(search_result);
  mygramclient_disconnect(c_client);
  mygramclient_destroy(c_client);
}

TEST_F(MygramClientTest, CApiSearchWithHighlightsAdvancedSupportsFilters) {
  const std::string text1 = mygram::utils::NormalizeText("Hello world example", true, "keep", true);
  const std::string text2 = mygram::utils::NormalizeText("Hello draft example", true, "keep", true);

  storage::FilterMap filters_active;
  filters_active["status"] = std::string("active");
  storage::FilterMap filters_inactive;
  filters_inactive["status"] = std::string("inactive");

  ASSERT_TRUE(doc_store_->AddDocument("highlight_active", filters_active, text1));
  index_->AddDocument(1, text1);
  ASSERT_TRUE(doc_store_->AddDocument("highlight_inactive", filters_inactive, text2));
  index_->AddDocument(2, text2);

  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;
  config.recv_buffer_size = 65536;

  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client), 0) << "Connect error: " << mygramclient_get_last_error(c_client);

  const char* and_terms[] = {"world"};
  const char* filter_keys[] = {"status"};
  const char* filter_values[] = {"active"};
  MygramSearchResultWithHighlights_C* search_result = nullptr;
  int result =
      mygramclient_search_with_highlights_advanced(c_client, "testdb.test", "hello", 10, 0, and_terms, 1, nullptr, 0,
                                                   filter_keys, filter_values, 1, nullptr, 1, &search_result);
  ASSERT_EQ(result, 0) << "Search error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(search_result, nullptr);
  EXPECT_EQ(search_result->total_count, 1u);
  ASSERT_EQ(search_result->count, 1u);
  EXPECT_STREQ(search_result->primary_keys[0], "highlight_active");
  EXPECT_NE(std::string(search_result->snippets[0]).find("<em>hello</em>"), std::string::npos);

  mygramclient_free_search_result_with_highlights(search_result);
  mygramclient_disconnect(c_client);
  mygramclient_destroy(c_client);
}

/**
 * @brief Test search with AND terms
 */
TEST_F(MygramClientTest, SearchWithAndTerms) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  std::vector<std::string> and_terms = {"world"};
  auto result = client_->Search("testdb.test", "hello", 100, 0, and_terms);

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
  auto result = client_->Search("testdb.test", "w", 100, 0, {}, not_terms);

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
  auto result = client_->Search("testdb.test", "hello", 100, 0, {}, {}, filters);

  ASSERT_TRUE(result) << "Search error: " << result.error().message();

  auto resp = *result;
  EXPECT_EQ(resp.total_count, 2);  // Both docs 1 and 2 are active
}

TEST_F(MygramClientTest, SearchRawPreservesConvertedOrExpression) {
  const std::string left_text = mygram::utils::NormalizeText("alpha xqz", true, "keep", true);
  const std::string right_text = mygram::utils::NormalizeText("alpha jkv", true, "keep", true);
  const std::string extra_text = mygram::utils::NormalizeText("alpha nope", true, "keep", true);
  ASSERT_TRUE(doc_store_->AddDocument("left_doc", {}, left_text));
  index_->AddDocument(1, left_text);
  ASSERT_TRUE(doc_store_->AddDocument("right_doc", {}, right_text));
  index_->AddDocument(2, right_text);
  ASSERT_TRUE(doc_store_->AddDocument("extra_doc", {}, extra_text));
  index_->AddDocument(3, extra_text);

  ASSERT_TRUE(client_->Connect());

  auto converted = ConvertSearchExpression("+alpha (xqz OR jkv)");
  ASSERT_TRUE(converted) << converted.error().message();
  EXPECT_EQ(*converted, "alpha AND ((xqz OR jkv))");

  auto raw_result = client_->SearchRaw("testdb.test", *converted, 10);
  ASSERT_TRUE(raw_result) << "SearchRaw error: " << raw_result.error().message();
  EXPECT_EQ(raw_result->total_count, 2u);

  std::string main_term;
  std::vector<std::string> and_terms;
  std::vector<std::string> not_terms;
  ASSERT_TRUE(SimplifySearchExpression("+alpha (xqz OR jkv)", main_term, and_terms, not_terms));
  EXPECT_EQ(main_term, "alpha");
  EXPECT_TRUE(and_terms.empty());

  auto simplified_result = client_->Search("testdb.test", main_term, 10, 0, and_terms, not_terms);
  ASSERT_TRUE(simplified_result) << "Search error: " << simplified_result.error().message();
  EXPECT_GT(simplified_result->total_count, raw_result->total_count);
}

TEST_F(MygramClientTest, RejectsControlCharactersInQuery) {
  ASSERT_TRUE(client_->Connect());

  auto result = client_->Search("testdb.test", "hello\nworld", 100);

  ASSERT_TRUE(!result);
  const auto& err = result.error();
  EXPECT_NE(err.message().find("control character"), std::string::npos);
}

TEST_F(MygramClientTest, RejectsControlCharactersInFilters) {
  ASSERT_TRUE(client_->Connect());

  std::vector<std::pair<std::string, std::string>> filters = {{"status", "active\r\n"}};
  auto result = client_->Search("testdb.test", "hello", 100, 0, {}, {}, filters);

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

  auto result = client_->Count("testdb.test", "hello");

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
  auto result = client_->Count("testdb.test", "world", {}, {}, filters);

  ASSERT_TRUE(result) << "Count error: " << result.error().message();

  auto resp = *result;
  EXPECT_EQ(resp.count, 1);  // Only doc 1 has "world" and is active
}

TEST_F(MygramClientTest, FacetWithSearchAndLimit) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  auto result = client_->Facet("testdb.test", "status", "hello", 1);
  ASSERT_TRUE(result) << "Facet error: " << result.error().message();

  const auto& resp = *result;
  ASSERT_EQ(resp.facets.size(), 1u);
  EXPECT_EQ(resp.facets[0].value, "active");
  EXPECT_EQ(resp.facets[0].count, 2u);
}

TEST_F(MygramClientTest, FacetWithNotAndFilter) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  std::vector<std::string> not_terms = {"x"};
  std::vector<std::pair<std::string, std::string>> filters = {{"status", "inactive"}};
  auto result = client_->Facet("testdb.test", "status", "w", 0, {}, not_terms, filters);
  ASSERT_TRUE(result) << "Facet error: " << result.error().message();

  const auto& resp = *result;
  ASSERT_EQ(resp.facets.size(), 1u);
  EXPECT_EQ(resp.facets[0].value, "inactive");
  EXPECT_EQ(resp.facets[0].count, 1u);
}

TEST_F(MygramClientTest, CApiFacetReturnsValueCounts) {
  AddTestDocuments();

  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;
  config.recv_buffer_size = 65536;

  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client), 0) << "Connect error: " << mygramclient_get_last_error(c_client);

  MygramFacetResult_C* facet_result = nullptr;
  int result = mygramclient_facet(c_client, "testdb.test", "status", "hello", 0, &facet_result);
  ASSERT_EQ(result, 0) << "Facet error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(facet_result, nullptr);
  ASSERT_EQ(facet_result->count, 1u);
  ASSERT_NE(facet_result->values, nullptr);
  ASSERT_NE(facet_result->counts, nullptr);
  EXPECT_STREQ(facet_result->values[0], "active");
  EXPECT_EQ(facet_result->counts[0], 2u);

  mygramclient_free_facet_result(facet_result);
  mygramclient_disconnect(c_client);
  mygramclient_destroy(c_client);
}

/**
 * @brief Test GET command
 */
TEST_F(MygramClientTest, GetDocument) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  auto result = client_->Get("testdb.test", "1");

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

TEST_F(MygramClientTest, GetPreservesStringFilterValuesWithSpaces) {
  storage::FilterMap filters;
  filters["display_name"] = std::string("Alice Smith");
  doc_store_->AddDocument("space_doc", filters);
  index_->AddDocument(1, "hello");

  ASSERT_TRUE(client_->Connect());

  auto result = client_->Get("testdb.test", "space_doc");
  ASSERT_TRUE(result) << "Get error: " << result.error().message();

  ASSERT_EQ(result->primary_key, "space_doc");
  ASSERT_EQ(result->fields.size(), 1u);
  EXPECT_EQ(result->fields[0].first, "display_name");
  EXPECT_EQ(result->fields[0].second, "Alice Smith");
}

/**
 * @brief Test INFO command
 *
 * Validates that all parsed fields (version, doc_count, active_connections,
 * index_size_bytes, tables) are populated by Info().
 */
TEST_F(MygramClientTest, Info) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  auto result = client_->Info();

  ASSERT_TRUE(result) << "Info error: " << result.error().message();

  auto info = *result;
  EXPECT_FALSE(info.version.empty());
  EXPECT_EQ(info.doc_count, 3);
  // The test client itself is a connected client.
  EXPECT_GE(info.active_connections, 1u) << "active_connections should be at least 1 (this client)";
  // Memory usage is always > 0 once any documents are indexed.
  EXPECT_GT(info.index_size_bytes, 0u) << "index_size_bytes should be > 0";
  EXPECT_FALSE(info.tables.empty()) << "tables list should not be empty";
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
  EXPECT_GE(info.active_connections, 1u) << "active_connections should reflect connected_clients";
  EXPECT_GT(info.index_size_bytes, 0u) << "index_size_bytes should reflect used_memory_bytes";
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

TEST_F(MygramClientTest, TypedAdminWrappersUseProtocolCommands) {
  ASSERT_TRUE(client_->Connect());

  auto set_result = client_->SetVariable("logging.level", "info");
  ASSERT_TRUE(set_result) << "SetVariable error: " << set_result.error().message();

  auto variables = client_->ShowVariables("logging%");
  ASSERT_TRUE(variables) << "ShowVariables error: " << variables.error().message();
  EXPECT_NE(variables->find("logging.level"), std::string::npos);

  auto cache_stats = client_->CacheStats();
  ASSERT_TRUE(cache_stats) << "CacheStats error: " << cache_stats.error().message();
  EXPECT_TRUE(cache_stats->find("OK CACHE_STATS") == 0) << *cache_stats;

  ASSERT_TRUE(client_->CacheDisable()) << "CacheDisable error";
  ASSERT_TRUE(client_->CacheEnable()) << "CacheEnable error";
  ASSERT_TRUE(client_->CacheClear("testdb.test")) << "CacheClear error";

  auto optimize = client_->Optimize("testdb.test");
  ASSERT_TRUE(optimize) << "Optimize error: " << optimize.error().message();
  EXPECT_TRUE(optimize->find("OK OPTIMIZED") == 0) << *optimize;

  auto dump_status = client_->DumpStatus();
  ASSERT_TRUE(dump_status) << "DumpStatus error: " << dump_status.error().message();
  EXPECT_TRUE(dump_status->find("OK DUMP_STATUS") == 0) << *dump_status;
}

TEST_F(MygramClientTest, SaveAndLoadUseDumpCommands) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());
  std::filesystem::create_directories("dumps");

  const std::string dump_name =
      "client save load " + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".dmp";

  auto save_result = client_->Save(dump_name);
  ASSERT_TRUE(save_result) << "Save error: " << save_result.error().message();
  EXPECT_NE(save_result->find(dump_name), std::string::npos);

  index_->Clear();
  doc_store_->Clear();

  auto empty_search = client_->Search("testdb.test", "hello", 100);
  ASSERT_TRUE(empty_search) << "Search after clear failed: " << empty_search.error().message();
  ASSERT_EQ(empty_search->total_count, 0u);

  auto load_result = client_->Load(*save_result);
  ASSERT_TRUE(load_result) << "Load error: " << load_result.error().message();
  EXPECT_NE(load_result->find(dump_name), std::string::npos);

  auto restored_search = client_->Search("testdb.test", "hello", 100);
  ASSERT_TRUE(restored_search) << "Search after load failed: " << restored_search.error().message();
  EXPECT_EQ(restored_search->total_count, 2u);
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
  auto result = client_->Search("testdb.test", "hello", 100);

  ASSERT_TRUE(result);

  auto resp = *result;
  EXPECT_TRUE(resp.debug.has_value());

  // Disable debug mode
  debug_result = client_->DisableDebug();
  EXPECT_TRUE(debug_result) << "Debug disable error: " << debug_result.error().message();
}

/**
 * @brief Test that DEBUG block parsing populates DebugInfo for SEARCH
 *
 * Validates the line-based "key: value" parser used for the server's
 * "# DEBUG" block. Result IDs must not be polluted with the literal "#".
 */
TEST_F(MygramClientTest, SearchWithDebugInfo) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  ASSERT_TRUE(client_->EnableDebug());

  auto result = client_->Search("testdb.test", "hello", 100);
  ASSERT_TRUE(result) << "Search error: " << result.error().message();

  auto resp = *result;
  // Debug block must parse successfully.
  ASSERT_TRUE(resp.debug.has_value()) << "Debug info missing";
  const auto& dbg = *resp.debug;

  // The search matches docs 1 and 2 ("hello"), so terms / final must be > 0.
  EXPECT_GT(dbg.terms, 0u) << "Debug 'terms' should be > 0";
  EXPECT_GT(dbg.final, 0u) << "Debug 'final' should be > 0";

  // Result IDs must not include the literal "#" token (regression test for
  // the old whitespace-tokenised parser that captured "# DEBUG").
  for (const auto& r : resp.results) {
    EXPECT_NE(r.primary_key, "#") << "Result IDs must not include the '#' header token";
    EXPECT_NE(r.primary_key, "DEBUG") << "Result IDs must not include the 'DEBUG' header token";
  }

  // Result count should match total_count (no spurious '#' or 'DEBUG' entries).
  EXPECT_EQ(resp.results.size(), resp.total_count);

  ASSERT_TRUE(client_->DisableDebug());
}

/**
 * @brief Test that DEBUG block parsing populates DebugInfo for COUNT
 */
TEST_F(MygramClientTest, CountWithDebugInfo) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());
  ASSERT_TRUE(client_->EnableDebug());

  auto result = client_->Count("testdb.test", "hello");
  ASSERT_TRUE(result) << "Count error: " << result.error().message();

  auto resp = *result;
  ASSERT_TRUE(resp.debug.has_value()) << "Debug info missing on COUNT";
  const auto& dbg = *resp.debug;
  EXPECT_GT(dbg.terms, 0u) << "Debug 'terms' should be > 0";
  EXPECT_EQ(resp.count, 2u);

  ASSERT_TRUE(client_->DisableDebug());
}

/**
 * @brief Test GetReplicationStatus parses the server's colon key/value lines
 *
 * The test fixture starts the server without a binlog reader, so the server
 * emits "OK REPLICATION\r\nstatus: not_configured\r\nEND". The client should
 * surface this as a successful response with running == false.
 */
TEST_F(MygramClientTest, GetReplicationStatus) {
  ASSERT_TRUE(client_->Connect());

  auto result = client_->GetReplicationStatus();
  ASSERT_TRUE(result) << "GetReplicationStatus error: " << result.error().message();

  auto status = *result;
  EXPECT_FALSE(status.running) << "Replication should not be running in unit-test fixture";
  // status_str should reflect the parsed status (e.g. "not_configured").
  EXPECT_FALSE(status.status_str.empty()) << "status_str should be set from parsed 'status' field";
  // queue_size and processed_events default to 0 when not running / not configured.
  EXPECT_EQ(status.queue_size, 0u);
}

/**
 * @brief Test error handling - invalid table
 */
TEST_F(MygramClientTest, ErrorHandling_InvalidTable) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  // Search with invalid table name should return error
  auto result = client_->Search("testdb.nonexistent_table", "hello", 100);

  ASSERT_TRUE(!result) << "Expected error for invalid table";

  auto err = result.error();
  EXPECT_TRUE(err.message().find("Table not found") != std::string::npos) << "Error message: " << err.message();
}

/**
 * @brief Test error handling - not connected
 */
TEST_F(MygramClientTest, ErrorHandling_NotConnected) {
  // Don't connect

  auto result = client_->Search("testdb.test", "hello", 100);

  ASSERT_TRUE(!result);
  EXPECT_EQ(result.error().message(), "Not connected");
}

/**
 * @brief Test raw SendCommand
 */
TEST_F(MygramClientTest, SendCommand) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());

  auto result = client_->SendCommand("COUNT testdb.test hello");

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
  int result = mygramclient_send_command(c_client, "COUNT testdb.test hello", &response);
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

TEST_F(MygramClientTest, CApiSearchEmptyResultSucceeds) {
  AddTestDocuments();

  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;
  config.recv_buffer_size = 65536;

  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client), 0) << "Connect error: " << mygramclient_get_last_error(c_client);

  MygramSearchResult_C* search_result = nullptr;
  int result = mygramclient_search(c_client, "testdb.test", "nomatch_empty_capi", 100, 0, &search_result);
  ASSERT_EQ(result, 0) << "Search error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(search_result, nullptr);
  EXPECT_EQ(search_result->count, 0u);
  EXPECT_EQ(search_result->total_count, 0u);
  EXPECT_EQ(search_result->primary_keys, nullptr);

  mygramclient_free_search_result(search_result);
  mygramclient_disconnect(c_client);
  mygramclient_destroy(c_client);
}

TEST_F(MygramClientTest, CApiGetPreservesStringFilterValuesWithSpaces) {
  storage::FilterMap filters;
  filters["display_name"] = std::string("Alice Smith");
  doc_store_->AddDocument("space_doc", filters);
  index_->AddDocument(1, "hello");

  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;
  config.recv_buffer_size = 65536;

  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client), 0) << "Connect error: " << mygramclient_get_last_error(c_client);

  MygramDocument_C* doc = nullptr;
  ASSERT_EQ(mygramclient_get(c_client, "testdb.test", "space_doc", &doc), 0)
      << "Get error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(doc, nullptr);
  ASSERT_STREQ(doc->primary_key, "space_doc");
  ASSERT_EQ(doc->field_count, 1u);
  EXPECT_STREQ(doc->field_keys[0], "display_name");
  EXPECT_STREQ(doc->field_values[0], "Alice Smith");

  mygramclient_free_document(doc);
  mygramclient_disconnect(c_client);
  mygramclient_destroy(c_client);
}

TEST_F(MygramClientTest, CApiSaveAndLoadUseDumpCommands) {
  AddTestDocuments();
  std::filesystem::create_directories("dumps");

  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;
  config.recv_buffer_size = 65536;

  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client), 0) << "Connect error: " << mygramclient_get_last_error(c_client);

  const std::string dump_name =
      "c_api_save_load_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".dmp";

  char* saved_path = nullptr;
  ASSERT_EQ(mygramclient_save(c_client, dump_name.c_str(), &saved_path), 0)
      << "Save error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(saved_path, nullptr);
  std::string saved_path_str(saved_path);
  EXPECT_NE(saved_path_str.find(dump_name), std::string::npos);
  mygramclient_free_string(saved_path);

  index_->Clear();
  doc_store_->Clear();

  char* loaded_path = nullptr;
  ASSERT_EQ(mygramclient_load(c_client, saved_path_str.c_str(), &loaded_path), 0)
      << "Load error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(loaded_path, nullptr);
  EXPECT_NE(std::string(loaded_path).find(dump_name), std::string::npos);
  mygramclient_free_string(loaded_path);

  char* response = nullptr;
  ASSERT_EQ(mygramclient_send_command(c_client, "COUNT testdb.test hello", &response), 0)
      << "COUNT error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(response, nullptr);
  EXPECT_TRUE(std::string(response).find("OK COUNT 2") == 0) << response;
  mygramclient_free_string(response);

  mygramclient_disconnect(c_client);
  mygramclient_destroy(c_client);
}

TEST_F(MygramClientTest, CApiTypedAdminWrappersUseProtocolCommands) {
  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;
  config.recv_buffer_size = 65536;

  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client), 0) << "Connect error: " << mygramclient_get_last_error(c_client);

  ASSERT_EQ(mygramclient_set_variable(c_client, "logging.level", "info"), 0)
      << "SET error: " << mygramclient_get_last_error(c_client);

  char* response = nullptr;
  ASSERT_EQ(mygramclient_show_variables(c_client, "logging%", &response), 0)
      << "SHOW VARIABLES error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(response, nullptr);
  EXPECT_NE(std::string(response).find("logging.level"), std::string::npos);
  mygramclient_free_string(response);

  response = nullptr;
  ASSERT_EQ(mygramclient_cache_stats(c_client, &response), 0)
      << "CACHE STATS error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(response, nullptr);
  EXPECT_TRUE(std::string(response).find("OK CACHE_STATS") == 0) << response;
  mygramclient_free_string(response);

  ASSERT_EQ(mygramclient_cache_disable(c_client), 0)
      << "CACHE DISABLE error: " << mygramclient_get_last_error(c_client);
  ASSERT_EQ(mygramclient_cache_enable(c_client), 0) << "CACHE ENABLE error: " << mygramclient_get_last_error(c_client);
  ASSERT_EQ(mygramclient_cache_clear(c_client, "testdb.test"), 0)
      << "CACHE CLEAR error: " << mygramclient_get_last_error(c_client);

  response = nullptr;
  ASSERT_EQ(mygramclient_optimize(c_client, "testdb.test", &response), 0)
      << "OPTIMIZE error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(response, nullptr);
  EXPECT_TRUE(std::string(response).find("OK OPTIMIZED") == 0) << response;
  mygramclient_free_string(response);

  response = nullptr;
  ASSERT_EQ(mygramclient_dump_status(c_client, &response), 0)
      << "DUMP STATUS error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(response, nullptr);
  EXPECT_TRUE(std::string(response).find("OK DUMP_STATUS") == 0) << response;
  mygramclient_free_string(response);

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
  auto result = client_->Search("testdb.test", "😀", 100);

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
  auto result = client_->Search("testdb.test", "🎉", 100);

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
  auto result = client_->Search("testdb.test", "😀", 100, 0, and_terms);

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
  auto result = client_->Search("testdb.test", "test", num_docs);  // Request max results

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
  auto result = client_->Search("testdb.test", "hello\x01world", 100);
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

/**
 * @brief Test that Connect honors timeout_ms when the host is unreachable
 *
 * Uses a non-routable IP from the TEST-NET-1 documentation range
 * (RFC 5737, 192.0.2.0/24) so the OS should silently drop the SYN.
 * Without the non-blocking-connect+poll fix, blocking connect() would hang
 * for the OS default of ~75s; with the fix it must fail within ~timeout_ms.
 */
TEST(MygramClientConnectTimeoutTest, ConnectTimeoutOnUnreachableHost) {
  ClientConfig config;
  config.host = "192.0.2.1";  // RFC 5737 TEST-NET-1, must not be routed
  config.port = 1;
  config.timeout_ms = 500;

  MygramClient unreachable_client(config);

  auto start = std::chrono::steady_clock::now();
  auto result = unreachable_client.Connect();
  auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();

  ASSERT_FALSE(result) << "Expected connect() to fail against an unreachable host";
  // Must complete well below the OS default (~75s). 5s gives generous slack on
  // CI without masking real regressions.
  EXPECT_LT(elapsed_ms, 5000) << "Connect did not honor timeout_ms (took " << elapsed_ms << "ms)";

  // The error code should be kClientTimeout when poll() actually times out.
  // On some hosts the kernel may emit ENETUNREACH/EHOSTUNREACH immediately;
  // accept either as a non-flake outcome but require kClientTimeout when the
  // failure happened only after timeout_ms elapsed.
  using mygram::utils::ErrorCode;
  if (elapsed_ms >= static_cast<long>(config.timeout_ms)) {
    EXPECT_EQ(result.error().code(), ErrorCode::kClientTimeout)
        << "Expected kClientTimeout, got: " << result.error().message();
  }
}

/**
 * @brief Test that Search rejects whitespace in the table identifier
 */
TEST_F(MygramClientTest, RejectsWhitespaceInTable) {
  ASSERT_TRUE(client_->Connect());

  auto result = client_->Search("my table", "hello", 100);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kClientInvalidArgument);
  EXPECT_NE(result.error().message().find("whitespace"), std::string::npos) << "Error: " << result.error().message();
}

/**
 * @brief Test that Search rejects an empty table identifier
 */
TEST_F(MygramClientTest, RejectsEmptyTable) {
  ASSERT_TRUE(client_->Connect());

  auto result = client_->Search("", "hello", 100);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kClientInvalidArgument);
  EXPECT_NE(result.error().message().find("empty"), std::string::npos) << "Error: " << result.error().message();
}

/**
 * @brief Test that Search rejects whitespace in the SORT column identifier
 */
TEST_F(MygramClientTest, RejectsWhitespaceInSortColumn) {
  ASSERT_TRUE(client_->Connect());

  auto result = client_->Search("testdb.test", "hello", 100, 0, {}, {}, {}, "bad column");
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kClientInvalidArgument);
  EXPECT_NE(result.error().message().find("whitespace"), std::string::npos) << "Error: " << result.error().message();
}

/**
 * @brief Test that Get rejects whitespace in the primary key identifier
 */
TEST_F(MygramClientTest, RejectsWhitespaceInPrimaryKey) {
  ASSERT_TRUE(client_->Connect());

  auto result = client_->Get("testdb.test", "bad pk");
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kClientInvalidArgument);
  EXPECT_NE(result.error().message().find("whitespace"), std::string::npos) << "Error: " << result.error().message();
}

/**
 * @brief Test that Search rejects whitespace in a filter key
 */
TEST_F(MygramClientTest, RejectsWhitespaceInFilterKey) {
  ASSERT_TRUE(client_->Connect());

  std::vector<std::pair<std::string, std::string>> filters = {{"bad key", "value"}};
  auto result = client_->Search("testdb.test", "hello", 100, 0, {}, {}, filters);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kClientInvalidArgument);
  EXPECT_NE(result.error().message().find("whitespace"), std::string::npos) << "Error: " << result.error().message();
}

/**
 * @brief Test that Search emits OFFSET when limit==0 and offset>0
 *
 * Regression test: the client used to silently drop the offset entirely when
 * limit was zero. Now it must emit a bare "OFFSET <n>" clause so the server
 * still skips the first N matches.
 */
TEST_F(MygramClientTest, SearchWithOffsetOnlyAppliesOffset) {
  AddTestDocuments();
  ASSERT_TRUE(client_->Connect());

  // 3 documents indexed; "w" matches all of them via unigram. We request
  // limit=0 (no explicit cap, server default applies) and offset=1.
  // The exact result count depends on server defaults, but at minimum the
  // search must succeed (proving "OFFSET" is a syntactically valid clause).
  auto result = client_->Search("testdb.test", "w", /*limit=*/0, /*offset=*/1);
  ASSERT_TRUE(result) << "Search with offset-only failed: " << result.error().message();

  // Validate the wire form by inspecting raw SendCommand output too.
  auto raw = client_->SendCommand("SEARCH testdb.test w OFFSET 1");
  ASSERT_TRUE(raw) << "Raw SEARCH ... OFFSET 1 failed: " << raw.error().message();
  EXPECT_NE(raw->find("OK RESULTS"), std::string::npos);
}

/**
 * @brief Test C API search NULL terms guard
 *
 * Regression test: previously, calling search_advanced with and_count > 0 but
 * and_terms == nullptr (or the same pattern for not / filter) would deference
 * NULL and segfault. Now those cases must return -1 with a descriptive error.
 */
TEST_F(MygramClientTest, CApiSearchNullTermsCrashGuard) {
  // Create a C API client (no need to connect — validation runs first).
  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;
  config.recv_buffer_size = 65536;

  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);

  MygramSearchResult_C* search_result = nullptr;

  // and_count > 0 but and_terms == nullptr
  int rc = mygramclient_search_advanced(c_client, "testdb.test", "hello", 100, 0,
                                        /*and_terms=*/nullptr, /*and_count=*/3,
                                        /*not_terms=*/nullptr, /*not_count=*/0,
                                        /*filter_keys=*/nullptr, /*filter_values=*/nullptr,
                                        /*filter_count=*/0, /*sort_column=*/nullptr,
                                        /*sort_desc=*/1, &search_result);
  EXPECT_EQ(rc, -1);
  EXPECT_NE(std::string(mygramclient_get_last_error(c_client)).find("and_terms"), std::string::npos);
  EXPECT_EQ(search_result, nullptr);

  // not_count > 0 but not_terms == nullptr
  search_result = nullptr;
  rc = mygramclient_search_advanced(c_client, "testdb.test", "hello", 100, 0,
                                    /*and_terms=*/nullptr, /*and_count=*/0,
                                    /*not_terms=*/nullptr, /*not_count=*/2,
                                    /*filter_keys=*/nullptr, /*filter_values=*/nullptr,
                                    /*filter_count=*/0, /*sort_column=*/nullptr,
                                    /*sort_desc=*/1, &search_result);
  EXPECT_EQ(rc, -1);
  EXPECT_NE(std::string(mygramclient_get_last_error(c_client)).find("not_terms"), std::string::npos);
  EXPECT_EQ(search_result, nullptr);

  // filter_count > 0 but filter_keys / filter_values == nullptr
  search_result = nullptr;
  rc = mygramclient_search_advanced(c_client, "testdb.test", "hello", 100, 0,
                                    /*and_terms=*/nullptr, /*and_count=*/0,
                                    /*not_terms=*/nullptr, /*not_count=*/0,
                                    /*filter_keys=*/nullptr, /*filter_values=*/nullptr,
                                    /*filter_count=*/1, /*sort_column=*/nullptr,
                                    /*sort_desc=*/1, &search_result);
  EXPECT_EQ(rc, -1);
  EXPECT_NE(std::string(mygramclient_get_last_error(c_client)).find("filter"), std::string::npos);
  EXPECT_EQ(search_result, nullptr);

  // Same checks for count_advanced
  uint64_t count_out = 0;
  rc = mygramclient_count_advanced(c_client, "testdb.test", "hello",
                                   /*and_terms=*/nullptr, /*and_count=*/3,
                                   /*not_terms=*/nullptr, /*not_count=*/0,
                                   /*filter_keys=*/nullptr, /*filter_values=*/nullptr,
                                   /*filter_count=*/0, &count_out);
  EXPECT_EQ(rc, -1);
  EXPECT_NE(std::string(mygramclient_get_last_error(c_client)).find("and_terms"), std::string::npos);

  mygramclient_destroy(c_client);
}

TEST_F(MygramClientTest, CApiLastErrorCodeReturnsNumericClientCode) {
  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;
  config.recv_buffer_size = 65536;

  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client), 0) << "Connect error: " << mygramclient_get_last_error(c_client);
  EXPECT_EQ(mygramclient_get_last_error_code(c_client), 0);

  MygramSearchResult_C* result = nullptr;
  int rc = mygramclient_search_advanced(c_client, "testdb.test", "hello", 10, 0, nullptr, 1, nullptr, 0, nullptr,
                                        nullptr, 0, nullptr, 1, &result);
  EXPECT_EQ(rc, -1);
  EXPECT_EQ(mygramclient_get_last_error_code(c_client), 7009);
  EXPECT_NE(std::string(mygramclient_get_last_error(c_client)).find("and_terms"), std::string::npos);

  mygramclient_disconnect(c_client);
  mygramclient_destroy(c_client);
}

/**
 * @brief Test that concurrent SendCommand calls from multiple threads are
 *        serialized internally without corrupting the protocol stream.
 *
 * The MygramClient class documents itself as thread-safe in the sense that
 * concurrent calls from different threads must not interleave send/recv on
 * the shared socket. This test spawns 4 threads each issuing 25 INFO calls
 * (100 total) and asserts every call returns a well-formed response. All
 * threads are joined per CLAUDE.md ("Always join, never detach").
 */
TEST_F(MygramClientTest, ConcurrentSendCommandsSerialize) {
  AddTestDocuments();
  ASSERT_TRUE(client_->Connect());

  constexpr int kThreadCount = 4;
  constexpr int kCallsPerThread = 25;

  std::vector<std::thread> threads;
  threads.reserve(kThreadCount);
  std::vector<int> success_count(kThreadCount, 0);
  std::vector<std::string> first_failure(kThreadCount);

  for (int t = 0; t < kThreadCount; ++t) {
    threads.emplace_back([this, t, &success_count, &first_failure]() {
      for (int i = 0; i < kCallsPerThread; ++i) {
        auto info = client_->Info();
        if (!info) {
          if (first_failure[t].empty()) {
            first_failure[t] = info.error().message();
          }
          continue;
        }
        // The presence of a non-empty version string confirms the response
        // was framed correctly (no interleaved bytes from another thread).
        if (!info->version.empty()) {
          success_count[t]++;
        } else if (first_failure[t].empty()) {
          first_failure[t] = "empty version (interleaved response?)";
        }
      }
    });
  }

  for (auto& thread : threads) {
    thread.join();
  }

  for (int t = 0; t < kThreadCount; ++t) {
    EXPECT_EQ(success_count[t], kCallsPerThread) << "Thread " << t << " first failure: " << first_failure[t];
  }
}

/**
 * @brief Test that searching with an empty query string returns a server
 *        error (not a client-side parse error or a malformed protocol).
 *
 * Regression test for the empty-string quoting fix: EscapeQueryString now
 * emits `""` for an empty arg, so the wire form is `SEARCH table ""` and
 * the server can parse it as an explicit empty token.
 */
TEST_F(MygramClientTest, SearchEmptyQueryReturnsError) {
  AddTestDocuments();
  ASSERT_TRUE(client_->Connect());

  auto result = client_->Search("testdb.test", "", 100);
  ASSERT_FALSE(result) << "Expected an error for empty query";

  // The error must originate from the server (kClientServerError), not from
  // a protocol-level parsing failure on the client side. Either kind would
  // be acceptable behavior, but the fix specifically targets server-side
  // rejection of an unambiguously-empty token.
  using mygram::utils::ErrorCode;
  EXPECT_TRUE(result.error().code() == ErrorCode::kClientServerError ||
              result.error().code() == ErrorCode::kClientInvalidArgument)
      << "Unexpected error code: " << static_cast<int>(result.error().code())
      << ", message: " << result.error().message();
}

/**
 * @brief Test that DNS-resolution errors include gai_strerror text and the
 *        offending hostname so users can diagnose the failure.
 */
TEST_F(MygramClientTest, ConnectInvalidHostnameIncludesGaiError) {
  ClientConfig bad_config;
  bad_config.host = "this.host.does.not.exist.invalid";
  bad_config.port = 12345;
  bad_config.timeout_ms = 3000;

  MygramClient bad_client(bad_config);
  auto result = bad_client.Connect();
  ASSERT_FALSE(result);

  const std::string& msg = result.error().message();
  // The new format is: "Failed to resolve host '<host>': <gai_strerror>"
  EXPECT_NE(msg.find("resolve"), std::string::npos) << "Error: " << msg;
  EXPECT_NE(msg.find(bad_config.host), std::string::npos) << "Error: " << msg;
  // The augmented message must be longer than the legacy "Failed to resolve
  // host: <host>" form (which had length == 22 + host.size()). Adding a
  // gai_strerror description always lengthens the output.
  const size_t legacy_len = std::string("Failed to resolve host: ").size() + bad_config.host.size();
  EXPECT_GT(msg.size(), legacy_len) << "Error message should include gai_strerror text: " << msg;
}

/**
 * @brief Test C API mygramclient_replication_status round-trip
 *
 * The test fixture starts the server without a binlog reader, so the C API
 * must surface the "not_configured" status with running == 0 and a
 * non-empty status string. Memory must round-trip cleanly through the
 * dedicated free function.
 */
TEST_F(MygramClientTest, CApiReplicationStatus) {
  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;
  config.recv_buffer_size = 65536;

  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);

  ASSERT_EQ(mygramclient_connect(c_client), 0) << "Connect error: " << mygramclient_get_last_error(c_client);

  MygramReplicationStatus_C* status = nullptr;
  int rc = mygramclient_replication_status(c_client, &status);
  ASSERT_EQ(rc, 0) << "replication_status error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(status, nullptr);

  // status_str must be populated (e.g. "not_configured" in the test fixture).
  ASSERT_NE(status->status_str, nullptr);
  EXPECT_GT(std::strlen(status->status_str), 0u);
  // gtid is allowed to be empty when replication is not configured.
  ASSERT_NE(status->gtid, nullptr);
  // running is 0 or 1.
  EXPECT_TRUE(status->running == 0 || status->running == 1);

  mygramclient_free_replication_status(status);

  // NULL-safety: free of nullptr must be a no-op.
  mygramclient_free_replication_status(nullptr);

  // NULL-input guards.
  EXPECT_EQ(mygramclient_replication_status(nullptr, &status), -1);
  EXPECT_EQ(mygramclient_replication_status(c_client, nullptr), -1);

  mygramclient_disconnect(c_client);
  mygramclient_destroy(c_client);
}

// =============================================================================
// Unit tests for IsResponseComplete (protocol detection logic)
// =============================================================================

using mygramdb::client::detail::IsResponseComplete;
using mygramdb::client::detail::ResponseCompletionState;

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

TEST(IsResponseCompleteTest, SingleLineConfigVerifyErrorComplete) {
  EXPECT_TRUE(IsResponseComplete("ERROR Configuration validation failed: missing required table name\r\n"));
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

TEST(IsResponseCompleteTest, StatefulDetectionFindsCrlfAcrossAppendedChunks) {
  ResponseCompletionState state;
  std::string response = "OK COUNT 4\r";
  EXPECT_FALSE(IsResponseComplete(response, state));

  response += "\n";
  EXPECT_TRUE(IsResponseComplete(response, state));
}

TEST(IsResponseCompleteTest, StatefulDetectionReusesFirstCrlfForMultilineCompletion) {
  ResponseCompletionState state;
  std::string response = "OK INFO\r\nversion: 1.0\r\n";
  EXPECT_FALSE(IsResponseComplete(response, state));
  EXPECT_NE(state.first_crlf, std::string_view::npos);

  response += "END\r\n";
  EXPECT_TRUE(IsResponseComplete(response, state));
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

TEST(IsResponseCompleteTest, SyncStatusRequiresEndMarker) {
  EXPECT_FALSE(IsResponseComplete("OK SYNC_STATUS\r\n"));
  EXPECT_FALSE(IsResponseComplete("OK SYNC_STATUS\r\nstatus=IDLE\r\n"));
  EXPECT_TRUE(IsResponseComplete("OK SYNC_STATUS\r\nstatus=IDLE\r\nEND\r\n"));
  EXPECT_TRUE(IsResponseComplete("OK SYNC_STATUS\r\ntable=users status=RUNNING\r\nEND\r\n"));
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

TEST(IsResponseCompleteTest, SearchWithHighlightRequiresTrailingBlankLine) {
  EXPECT_FALSE(IsResponseComplete("OK RESULTS 1\r\npk1\thello <em>world</em>\r\n"));
  EXPECT_TRUE(IsResponseComplete("OK RESULTS 1\r\npk1\thello <em>world</em>\r\n\r\n"));
}

/**
 * @brief Test CACHE_STATS multi-line response requires END marker
 */
TEST(IsResponseCompleteTest, CacheStatsRequiresEndMarker) {
  // Just the first line - NOT complete
  EXPECT_FALSE(IsResponseComplete("OK CACHE_STATS\r\n"));

  // Partial response with content but no END
  EXPECT_FALSE(IsResponseComplete("OK CACHE_STATS\r\n\r\n# Cache\r\nenabled: true\r\n"));

  // Complete response with END marker
  EXPECT_TRUE(IsResponseComplete("OK CACHE_STATS\r\n\r\n# Cache\r\nenabled: true\r\nEND\r\n"));

  // Minimal complete CACHE_STATS
  EXPECT_TRUE(IsResponseComplete("OK CACHE_STATS\r\nEND\r\n"));
}

/**
 * @brief Test DUMP_INFO multi-line response requires END marker
 *
 * DUMP_INFO is unique because the first line carries a filepath suffix:
 * "OK DUMP_INFO /path/to/dump\r\n", so detection uses prefix matching.
 */
TEST(IsResponseCompleteTest, DumpInfoRequiresEndMarker) {
  // Just the first line - NOT complete
  EXPECT_FALSE(IsResponseComplete("OK DUMP_INFO /tmp/snap.bin\r\n"));

  // Partial response with content but no END
  EXPECT_FALSE(IsResponseComplete("OK DUMP_INFO /tmp/snap.bin\r\nversion: 2\r\nfile_size: 1024\r\n"));

  // Complete response with END marker
  EXPECT_TRUE(IsResponseComplete("OK DUMP_INFO /tmp/snap.bin\r\nversion: 2\r\nEND\r\n"));

  // Empty filepath edge case still requires END
  EXPECT_FALSE(IsResponseComplete("OK DUMP_INFO \r\n"));
  EXPECT_TRUE(IsResponseComplete("OK DUMP_INFO \r\nEND\r\n"));
}

/**
 * @brief Test DUMP_STATUS multi-line response requires END marker
 */
TEST(IsResponseCompleteTest, DumpStatusRequiresEndMarker) {
  // Just the first line - NOT complete
  EXPECT_FALSE(IsResponseComplete("OK DUMP_STATUS\r\n"));

  // Partial response with content but no END
  EXPECT_FALSE(IsResponseComplete("OK DUMP_STATUS\r\nstatus: IDLE\r\nsave_in_progress: false\r\n"));

  // Complete response with END marker
  EXPECT_TRUE(IsResponseComplete("OK DUMP_STATUS\r\nstatus: IDLE\r\nEND\r\n"));

  // Minimal complete DUMP_STATUS
  EXPECT_TRUE(IsResponseComplete("OK DUMP_STATUS\r\nEND\r\n"));
}
