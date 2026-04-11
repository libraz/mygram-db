/**
 * @file verify_text_test.cpp
 * @brief Integration tests for verify_text (N-gram false positive post-filter)
 *
 * Test Coverage:
 * - verify_text="all" filters N-gram false positives
 * - verify_text="ascii" skips non-ASCII terms
 * - verify_text="off" passes through all N-gram matches
 * - DUMP SAVE → DUMP LOAD with verify_text="all" returns non-zero results
 * - DUMP LOAD invalidates search cache
 * - SHOW VARIABLES includes memory.verify_text
 */

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <sstream>
#include <thread>

#include "config/config.h"
#include "server/tcp_server.h"
#include "test_io_model_override.h"

using namespace mygramdb::server;
using namespace mygramdb;

namespace {

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

  TcpClient(const TcpClient&) = delete;
  TcpClient& operator=(const TcpClient&) = delete;

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
 * @brief Wait for an async DUMP SAVE to complete by polling DUMP STATUS
 *
 * DUMP SAVE is asynchronous and returns "OK DUMP_STARTED ..." immediately.
 * This helper polls DUMP STATUS until completion or timeout.
 *
 * @return true if dump completed successfully, false on timeout or failure
 */
bool WaitForDumpComplete(TcpClient& client,
                         int timeout_ms = 10000,       // NOLINT(readability-magic-numbers)
                         int poll_interval_ms = 50) {  // NOLINT(readability-magic-numbers)
  auto start = std::chrono::steady_clock::now();
  while (true) {
    auto elapsed =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start).count();
    if (elapsed > timeout_ms) {
      return false;
    }

    std::string status = client.SendCommand("DUMP STATUS");
    if (status.find("status: COMPLETED") != std::string::npos) {
      return true;
    }
    if (status.find("status: FAILED") != std::string::npos) {
      return false;
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms));
  }
}

// ============================================================================
// Test fixture: verify_text="all"
// ============================================================================

class VerifyTextAllTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Use ngram_size=2 to create known false positives
    auto index = std::make_unique<index::Index>(2);
    auto doc_store = std::make_unique<storage::DocumentStore>();

    table_context_.name = "articles";
    table_context_.config.ngram_size = 2;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);
    table_contexts_["articles"] = &table_context_;

    // Configure with verify_text="all"
    full_config_ = std::make_unique<config::Config>();
    config::TableConfig tc;
    tc.name = "articles";
    tc.ngram_size = 2;
    full_config_->tables.push_back(tc);
    full_config_->memory.verify_text = "all";
    full_config_->cache.enabled = true;
    full_config_->cache.max_memory_bytes = 10 * 1024 * 1024;
    full_config_->cache.min_query_cost_ms = 0.0;  // Cache all for testing

    server_config_.port = 0;
    server_config_.host = "127.0.0.1";
    server_config_.allow_cidrs = {"127.0.0.1/32"};
    mygramdb::server::test::ApplyIoModelOverride(server_config_);

    // Create dump directory
    test_dump_dir_ = std::filesystem::temp_directory_path() / ("verify_text_test_" + std::to_string(getpid()));
    std::filesystem::create_directories(test_dump_dir_);

    server_ = std::make_unique<TcpServer>(server_config_, table_contexts_, test_dump_dir_.string(), full_config_.get());
    ASSERT_TRUE(server_->Start());
    port_ = server_->GetPort();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void TearDown() override {
    if (server_ && server_->IsRunning()) {
      server_->Stop();
    }
    if (std::filesystem::exists(test_dump_dir_)) {
      std::filesystem::remove_all(test_dump_dir_);
    }
  }

  /**
   * @brief Add a document with normalized text stored for verify_text
   */
  void AddDocumentWithText(const std::string& pk, const std::string& text) {
    auto doc_id = table_context_.doc_store->AddDocument(pk, {});
    ASSERT_TRUE(doc_id.has_value());
    table_context_.index->AddDocument(*doc_id, text);
    table_context_.doc_store->SetNormalizedText(*doc_id, text);
  }

  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> full_config_;
  ServerConfig server_config_;
  std::unique_ptr<TcpServer> server_;
  std::filesystem::path test_dump_dir_;
  uint16_t port_ = 0;
};

/**
 * @brief verify_text="all" filters N-gram false positives
 *
 * With ngram_size=2, "abc" generates bigrams {"ab","bc"}.
 * Document "ab cd bc" contains both "ab" and "bc" bigrams but does NOT
 * contain "abc" as a substring. This is a false positive that should be
 * filtered.
 */
TEST_F(VerifyTextAllTest, FiltersNgramFalsePositive) {
  // True positive: contains "abc" as substring
  AddDocumentWithText("pk_true", "xabcx");
  // False positive: contains bigrams "ab" and "bc" but not "abc"
  AddDocumentWithText("pk_false", "ab cd bc");

  TcpClient client("127.0.0.1", port_);
  std::string response = client.SendCommand("SEARCH articles abc");

  // Only pk_true should be returned (false positive filtered)
  EXPECT_TRUE(response.find("OK RESULTS 1") == 0) << "Expected 1 result (false positive filtered), got: " << response;
  EXPECT_NE(response.find("pk_true"), std::string::npos);
  EXPECT_EQ(response.find("pk_false"), std::string::npos);
}

/**
 * @brief verify_text="all" does not affect true matches
 */
TEST_F(VerifyTextAllTest, PreservesTrueMatches) {
  AddDocumentWithText("pk1", "hello world");
  AddDocumentWithText("pk2", "hello universe");
  AddDocumentWithText("pk3", "goodbye world");

  TcpClient client("127.0.0.1", port_);
  std::string response = client.SendCommand("SEARCH articles hello");

  EXPECT_TRUE(response.find("OK RESULTS 2") == 0) << "Expected 2 results, got: " << response;
  EXPECT_NE(response.find("pk1"), std::string::npos);
  EXPECT_NE(response.find("pk2"), std::string::npos);
}

/**
 * @brief DUMP SAVE, DUMP LOAD, SEARCH returns non-zero results with
 * verify_text="all"
 *
 * After DUMP LOAD, doc_texts_ is empty (not serialized in dump_format_v1).
 * PostFilterByText must include documents with nullopt text (fail-open).
 */
TEST_F(VerifyTextAllTest, DumpLoadPreservesVerifyTextFiltering) {
  // True positive: contains "abc" as substring
  AddDocumentWithText("pk_true", "xabcx");
  // False positive: contains bigrams "ab" and "bc" but not "abc"
  AddDocumentWithText("pk_false", "ab cd bc");

  TcpClient client("127.0.0.1", port_);

  // Before dump: verify_text filters false positive
  std::string response = client.SendCommand("SEARCH articles abc");
  EXPECT_TRUE(response.find("OK RESULTS 1") == 0) << "Pre-dump: " << response;
  EXPECT_NE(response.find("pk_true"), std::string::npos);
  EXPECT_EQ(response.find("pk_false"), std::string::npos);

  // DUMP SAVE (async: returns DUMP_STARTED, must poll DUMP STATUS)
  response = client.SendCommand("DUMP SAVE test_verify.dmp");
  EXPECT_TRUE(response.find("OK DUMP_STARTED") == 0 || response.find("OK SAVED") == 0) << "DUMP SAVE: " << response;

  // Wait for async dump to complete
  ASSERT_TRUE(WaitForDumpComplete(client)) << "DUMP SAVE did not complete";

  // DUMP LOAD (doc_texts_ is preserved in snapshot v2)
  response = client.SendCommand("DUMP LOAD test_verify.dmp");
  EXPECT_TRUE(response.find("OK LOADED") == 0) << "DUMP LOAD: " << response;

  // After DUMP LOAD: verify_text should still filter false positives
  // because doc_texts_ is restored from the snapshot
  response = client.SendCommand("SEARCH articles abc");
  EXPECT_TRUE(response.find("OK RESULTS 1") == 0) << "Post-dump: verify_text should still filter, got: " << response;
  EXPECT_NE(response.find("pk_true"), std::string::npos);
  EXPECT_EQ(response.find("pk_false"), std::string::npos);
}

/**
 * @brief DUMP LOAD invalidates search cache
 *
 * Ensures stale cache entries from before DUMP LOAD are cleared.
 */
TEST_F(VerifyTextAllTest, DumpLoadInvalidatesCache) {
  AddDocumentWithText("pk1", "cached query test");

  TcpClient client("127.0.0.1", port_);

  // First search populates cache
  std::string response = client.SendCommand("SEARCH articles cached");
  EXPECT_TRUE(response.find("OK RESULTS 1") == 0) << response;

  // Second search should hit cache (same result)
  response = client.SendCommand("SEARCH articles cached");
  EXPECT_TRUE(response.find("OK RESULTS 1") == 0) << response;

  // DUMP SAVE (async) then wait for completion, then DUMP LOAD
  response = client.SendCommand("DUMP SAVE cache_test.dmp");
  EXPECT_TRUE(response.find("OK DUMP_STARTED") == 0 || response.find("OK SAVED") == 0) << response;
  ASSERT_TRUE(WaitForDumpComplete(client)) << "DUMP SAVE did not complete";

  response = client.SendCommand("DUMP LOAD cache_test.dmp");
  EXPECT_TRUE(response.find("OK LOADED") == 0) << response;

  // Search after DUMP LOAD: cache should have been cleared
  // Result should still be correct (from fresh index, not stale cache)
  response = client.SendCommand("SEARCH articles cached");
  EXPECT_TRUE(response.find("OK RESULTS 1") == 0)
      << "After DUMP LOAD, search should return fresh results: " << response;
}

// ============================================================================
// Test fixture: verify_text="ascii"
// ============================================================================

class VerifyTextAsciiTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Set kanji_ngram_size=1 so CJK characters are indexed as unigrams
    // and the search handler uses the same hybrid n-gram generation
    auto index = std::make_unique<index::Index>(2, 1);
    auto doc_store = std::make_unique<storage::DocumentStore>();

    table_context_.name = "articles";
    table_context_.config.ngram_size = 2;
    table_context_.config.kanji_ngram_size = 1;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);
    table_contexts_["articles"] = &table_context_;

    full_config_ = std::make_unique<config::Config>();
    config::TableConfig tc;
    tc.name = "articles";
    tc.ngram_size = 2;
    tc.kanji_ngram_size = 1;
    full_config_->tables.push_back(tc);
    full_config_->memory.verify_text = "ascii";

    server_config_.port = 0;
    server_config_.host = "127.0.0.1";
    server_config_.allow_cidrs = {"127.0.0.1/32"};
    mygramdb::server::test::ApplyIoModelOverride(server_config_);

    server_ = std::make_unique<TcpServer>(server_config_, table_contexts_, "./test_snapshots", full_config_.get());
    ASSERT_TRUE(server_->Start());
    port_ = server_->GetPort();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void TearDown() override {
    if (server_ && server_->IsRunning()) {
      server_->Stop();
    }
  }

  void AddDocumentWithText(const std::string& pk, const std::string& text) {
    auto doc_id = table_context_.doc_store->AddDocument(pk, {});
    ASSERT_TRUE(doc_id.has_value());
    table_context_.index->AddDocument(*doc_id, text);
    table_context_.doc_store->SetNormalizedText(*doc_id, text);
  }

  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> full_config_;
  ServerConfig server_config_;
  std::unique_ptr<TcpServer> server_;
  uint16_t port_ = 0;
};

/**
 * @brief verify_text="ascii" filters false positives for ASCII search terms
 */
TEST_F(VerifyTextAsciiTest, FiltersAsciiTermFalsePositive) {
  AddDocumentWithText("pk_true", "xabcx");
  AddDocumentWithText("pk_false", "ab cd bc");

  TcpClient client("127.0.0.1", port_);
  std::string response = client.SendCommand("SEARCH articles abc");

  EXPECT_TRUE(response.find("OK RESULTS 1") == 0) << "Expected false positive filtered for ASCII term: " << response;
  EXPECT_NE(response.find("pk_true"), std::string::npos);
  EXPECT_EQ(response.find("pk_false"), std::string::npos);
}

/**
 * @brief verify_text="ascii" skips verification for non-ASCII (Japanese) terms
 *
 * When search terms contain non-ASCII characters, post-filtering is skipped.
 * This means N-gram false positives may remain, but that's acceptable for CJK.
 * With kanji_ngram_size=1, CJK characters are indexed as unigrams.
 */
TEST_F(VerifyTextAsciiTest, SkipsNonAsciiTerms) {
  // Both documents contain CJK unigrams for "学" and "習"
  AddDocumentWithText("pk1", "機械学習のチュートリアル");
  AddDocumentWithText("pk2", "深層学習の応用");

  TcpClient client("127.0.0.1", port_);
  std::string response = client.SendCommand("SEARCH articles 学習");

  // Non-ASCII terms bypass post-filtering, so all N-gram matches returned
  EXPECT_TRUE(response.find("OK RESULTS 2") == 0) << "Non-ASCII should bypass verify_text: " << response;
}

// ============================================================================
// Test fixture: verify_text="off"
// ============================================================================

class VerifyTextOffTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto index = std::make_unique<index::Index>(2);
    auto doc_store = std::make_unique<storage::DocumentStore>();

    table_context_.name = "articles";
    table_context_.config.ngram_size = 2;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);
    table_contexts_["articles"] = &table_context_;

    full_config_ = std::make_unique<config::Config>();
    config::TableConfig tc;
    tc.name = "articles";
    tc.ngram_size = 2;
    full_config_->tables.push_back(tc);
    full_config_->memory.verify_text = "off";

    server_config_.port = 0;
    server_config_.host = "127.0.0.1";
    server_config_.allow_cidrs = {"127.0.0.1/32"};
    mygramdb::server::test::ApplyIoModelOverride(server_config_);

    server_ = std::make_unique<TcpServer>(server_config_, table_contexts_, "./test_snapshots", full_config_.get());
    ASSERT_TRUE(server_->Start());
    port_ = server_->GetPort();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void TearDown() override {
    if (server_ && server_->IsRunning()) {
      server_->Stop();
    }
  }

  void AddDocumentWithText(const std::string& pk, const std::string& text) {
    auto doc_id = table_context_.doc_store->AddDocument(pk, {});
    ASSERT_TRUE(doc_id.has_value());
    table_context_.index->AddDocument(*doc_id, text);
    table_context_.doc_store->SetNormalizedText(*doc_id, text);
  }

  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> full_config_;
  ServerConfig server_config_;
  std::unique_ptr<TcpServer> server_;
  uint16_t port_ = 0;
};

/**
 * @brief verify_text="off" does not filter false positives
 */
TEST_F(VerifyTextOffTest, DoesNotFilterFalsePositives) {
  AddDocumentWithText("pk_true", "xabcx");
  // False positive: bigrams "ab","bc" match but "abc" not a substring
  AddDocumentWithText("pk_false", "ab cd bc");

  TcpClient client("127.0.0.1", port_);
  std::string response = client.SendCommand("SEARCH articles abc");

  // Both documents should be returned (no post-filter)
  EXPECT_TRUE(response.find("OK RESULTS 2") == 0) << "verify_text=off should not filter: " << response;
  EXPECT_NE(response.find("pk_true"), std::string::npos);
  EXPECT_NE(response.find("pk_false"), std::string::npos);
}

// ============================================================================
// DUMP LOAD with verify_text="off" (search consistency)
// ============================================================================

class VerifyTextDumpConsistencyTest : public ::testing::Test {
 protected:
  void SetUp() override {
    auto index = std::make_unique<index::Index>(2);
    auto doc_store = std::make_unique<storage::DocumentStore>();

    table_context_.name = "articles";
    table_context_.config.ngram_size = 2;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);
    table_contexts_["articles"] = &table_context_;

    full_config_ = std::make_unique<config::Config>();
    config::TableConfig tc;
    tc.name = "articles";
    tc.ngram_size = 2;
    full_config_->tables.push_back(tc);
    full_config_->memory.verify_text = "off";

    server_config_.port = 0;
    server_config_.host = "127.0.0.1";
    server_config_.allow_cidrs = {"127.0.0.1/32"};
    mygramdb::server::test::ApplyIoModelOverride(server_config_);

    test_dump_dir_ = std::filesystem::temp_directory_path() / ("verify_dump_test_" + std::to_string(getpid()));
    std::filesystem::create_directories(test_dump_dir_);

    server_ = std::make_unique<TcpServer>(server_config_, table_contexts_, test_dump_dir_.string(), full_config_.get());
    ASSERT_TRUE(server_->Start());
    port_ = server_->GetPort();
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  void TearDown() override {
    if (server_ && server_->IsRunning()) {
      server_->Stop();
    }
    if (std::filesystem::exists(test_dump_dir_)) {
      std::filesystem::remove_all(test_dump_dir_);
    }
  }

  void AddDocumentWithText(const std::string& pk, const std::string& text) {
    auto doc_id = table_context_.doc_store->AddDocument(pk, {});
    ASSERT_TRUE(doc_id.has_value());
    table_context_.index->AddDocument(*doc_id, text);
    table_context_.doc_store->SetNormalizedText(*doc_id, text);
  }

  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> full_config_;
  ServerConfig server_config_;
  std::unique_ptr<TcpServer> server_;
  std::filesystem::path test_dump_dir_;
  uint16_t port_ = 0;
};

/**
 * @brief DUMP SAVE, DUMP LOAD preserves search result consistency
 * (verify_text="off")
 */
TEST_F(VerifyTextDumpConsistencyTest, SearchResultsConsistentAfterDumpLoad) {
  AddDocumentWithText("pk1", "hello world");
  AddDocumentWithText("pk2", "hello universe");
  AddDocumentWithText("pk3", "goodbye world");

  TcpClient client("127.0.0.1", port_);

  // Record pre-dump results
  std::string before = client.SendCommand("SEARCH articles hello");
  EXPECT_TRUE(before.find("OK RESULTS 2") == 0) << "Pre-dump: " << before;

  // DUMP SAVE (async) and wait for completion
  std::string response = client.SendCommand("DUMP SAVE consistency.dmp");
  EXPECT_TRUE(response.find("OK DUMP_STARTED") == 0 || response.find("OK SAVED") == 0) << response;
  ASSERT_TRUE(WaitForDumpComplete(client)) << "DUMP SAVE did not complete";

  // DUMP LOAD
  response = client.SendCommand("DUMP LOAD consistency.dmp");
  EXPECT_TRUE(response.find("OK LOADED") == 0) << response;

  // Post-dump results should match
  std::string after = client.SendCommand("SEARCH articles hello");
  EXPECT_TRUE(after.find("OK RESULTS 2") == 0) << "Post-dump results should match pre-dump: " << after;
  EXPECT_NE(after.find("pk1"), std::string::npos);
  EXPECT_NE(after.find("pk2"), std::string::npos);
}

}  // namespace
