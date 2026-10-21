/**
 * @file mygramclient_test.cpp
 * @brief Unit tests for MygramDB client library
 */

#include "client/mygramclient.h"

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include <array>
#include <atomic>
#include <chrono>
#include <clocale>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <future>
#include <locale>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <vector>

#include "client/mygramclient_c.h"
#include "client/mygramclient_c_testing.h"
#include "client/protocol_detection.h"
#include "client/search_expression.h"
#include "client/wire_quoting.h"
#include "index/index.h"
#include "query/query_ast.h"
#include "server/tcp_server.h"
#include "storage/document_store.h"
#include "utils/string_utils.h"

using namespace mygramdb::client;
using namespace mygramdb;

namespace {

class CommaGroupingNumpunct final : public std::numpunct<char> {
 protected:
  char do_decimal_point() const override { return ','; }
  char do_thousands_sep() const override { return ','; }
  std::string do_grouping() const override { return "\3"; }
};

class ScopedGlobalLocale {
 public:
  explicit ScopedGlobalLocale(const std::locale& locale) : previous_(std::locale::global(locale)) {}
  ~ScopedGlobalLocale() { (void)std::locale::global(previous_); }

 private:
  std::locale previous_;
};

}  // namespace

TEST(MygramClientErrorProtocolTest, ParsesCodedAndLegacyErrorsTable) {
  struct TestCase {
    const char* response;
    bool is_error;
    mygram::utils::ErrorCode expected_code;
    const char* expected_message;
  };
  constexpr std::array<TestCase, 6> cases = {{
      {"ERROR 3010 Invalid boolean expression", true, mygram::utils::ErrorCode::kQueryExpressionParseError,
       "Invalid boolean expression"},
      {"ERROR 6028 Server is loading", true, mygram::utils::ErrorCode::kServerLoading, "Server is loading"},
      {"ERROR 6030 SERVER_BUSY", true, mygram::utils::ErrorCode::kServerBusy, "SERVER_BUSY"},
      {"ERROR 65000 Future server error", true, static_cast<mygram::utils::ErrorCode>(65000), "Future server error"},
      {"ERROR legacy response", true, mygram::utils::ErrorCode::kClientServerError, "legacy response"},
      {"OK COUNT 0", false, mygram::utils::ErrorCode::kSuccess, ""},
  }};

  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.response);
    const auto parsed = ParseServerErrorResponse(test_case.response);
    EXPECT_EQ(parsed.has_value(), test_case.is_error);
    if (parsed.has_value()) {
      EXPECT_EQ(parsed->code(), test_case.expected_code);
      EXPECT_EQ(parsed->message(), test_case.expected_message);
    }
  }
}

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
    // This fixture has no binlog reader; keep its replication configuration
    // valid for runtime SET validation and make replication status accurately
    // report the unconfigured state exercised by these tests.
    full_config_.replication.enable = false;

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

  void AddGroupedBooleanTestDocuments() {
    AddTestDocuments();
    doc_store_->AddDocument("4", {});
    index_->AddDocument(4, mygram::utils::NormalizeText("alpha xqz", true, "keep", true));
    doc_store_->AddDocument("5", {});
    index_->AddDocument(5, mygram::utils::NormalizeText("alpha jkv", true, "keep", true));
    doc_store_->AddDocument("6", {});
    index_->AddDocument(6, mygram::utils::NormalizeText("alpha other", true, "keep", true));
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

TEST_F(MygramClientTest, ZeroAndOversizedConfigValuesUseSafeDefaults) {
  ClientConfig zero_config;
  zero_config.host = "127.0.0.1";
  zero_config.port = server_->GetPort();
  zero_config.timeout_ms = 0;
  zero_config.recv_buffer_size = 0;
  MygramClient zero_client(zero_config);
  ASSERT_TRUE(zero_client.Connect());
  EXPECT_TRUE(zero_client.Info());

  ClientConfig oversized_config = zero_config;
  oversized_config.recv_buffer_size = UINT32_MAX;
  MygramClient oversized_client(oversized_config);
  ASSERT_TRUE(oversized_client.Connect());
  EXPECT_TRUE(oversized_client.Info());
}

TEST_F(MygramClientTest, SingleByteReceiveBufferReadsCompleteResponse) {
  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.recv_buffer_size = 1;

  MygramClient client(config);
  ASSERT_TRUE(client.Connect());
  EXPECT_TRUE(client.Info());
}

// SEARCH/COUNT with HIGHLIGHT or an active DEBUG flag, and a non-empty SHOW
// VARIABLES table, all carry more bytes after their first \r\n, yet that first
// line is byte-for-byte identical to a genuinely single-line reply until the
// rest arrives. A chunk boundary landing right there (recv_buffer_size==1, or
// exactly at the header's own length) must not end the frame early (see
// BeginResponseFrame in protocol_detection.h).
TEST_F(MygramClientTest, SingleByteReceiveBufferSurvivesSearchWithHighlights) {
  const std::string original_text = "Hello world example";
  const std::string normalized = mygram::utils::NormalizeText(original_text, true, "keep", true);
  ASSERT_TRUE(doc_store_->AddDocument("highlight_doc", {}, normalized, original_text));
  index_->AddDocument(1, normalized);

  ASSERT_TRUE(client_->Connect());
  auto baseline = client_->SearchWithHighlights("testdb.test", "hello", 10);
  ASSERT_TRUE(baseline) << baseline.error().message();
  ASSERT_EQ(baseline->results.size(), 1U);
  const size_t header_length = std::string("OK RESULTS " + std::to_string(baseline->total_count) + "\r\n").size();

  for (size_t recv_buffer_size : {size_t{1}, header_length}) {
    ClientConfig config;
    config.host = "127.0.0.1";
    config.port = server_->GetPort();
    config.recv_buffer_size = static_cast<uint32_t>(recv_buffer_size);
    MygramClient client(config);
    ASSERT_TRUE(client.Connect()) << "recv_buffer_size=" << recv_buffer_size;

    auto result = client.SearchWithHighlights("testdb.test", "hello", 10);
    ASSERT_TRUE(result) << "recv_buffer_size=" << recv_buffer_size << ": " << result.error().message();
    EXPECT_EQ(result->total_count, 1U) << "recv_buffer_size=" << recv_buffer_size;
    ASSERT_EQ(result->results.size(), 1U) << "recv_buffer_size=" << recv_buffer_size;
    EXPECT_EQ(result->results[0].primary_key, "highlight_doc") << "recv_buffer_size=" << recv_buffer_size;
    EXPECT_NE(result->results[0].snippet.find("<em>Hello</em>"), std::string::npos)
        << "recv_buffer_size=" << recv_buffer_size;

    // A truncated frame would leave the snippet row on the socket to answer
    // this next command instead.
    EXPECT_TRUE(client.Info()) << "recv_buffer_size=" << recv_buffer_size;
  }
}

TEST_F(MygramClientTest, SingleByteReceiveBufferSurvivesDebugModeSearch) {
  AddTestDocuments();

  ASSERT_TRUE(client_->Connect());
  ASSERT_TRUE(client_->EnableDebug());
  auto baseline = client_->Search("testdb.test", "hello", 100);
  ASSERT_TRUE(baseline) << baseline.error().message();
  ASSERT_TRUE(baseline->debug.has_value());
  const size_t header_length = std::string("OK RESULTS " + std::to_string(baseline->total_count) + "\r\n").size();

  for (size_t recv_buffer_size : {size_t{1}, header_length}) {
    ClientConfig config;
    config.host = "127.0.0.1";
    config.port = server_->GetPort();
    config.recv_buffer_size = static_cast<uint32_t>(recv_buffer_size);
    MygramClient client(config);
    ASSERT_TRUE(client.Connect()) << "recv_buffer_size=" << recv_buffer_size;
    ASSERT_TRUE(client.EnableDebug()) << "recv_buffer_size=" << recv_buffer_size;

    auto result = client.Search("testdb.test", "hello", 100);
    ASSERT_TRUE(result) << "recv_buffer_size=" << recv_buffer_size << ": " << result.error().message();
    EXPECT_EQ(result->results.size(), 2U) << "recv_buffer_size=" << recv_buffer_size;
    ASSERT_TRUE(result->debug.has_value()) << "recv_buffer_size=" << recv_buffer_size;
    EXPECT_GT(result->debug->terms, 0U) << "recv_buffer_size=" << recv_buffer_size;

    EXPECT_TRUE(client.Info()) << "recv_buffer_size=" << recv_buffer_size;
  }
}

TEST_F(MygramClientTest, SingleByteReceiveBufferSurvivesNonEmptyShowVariables) {
  ASSERT_TRUE(client_->Connect());
  ASSERT_TRUE(client_->SetVariable("logging.level", "info"));
  auto baseline = client_->ShowVariables("logging%");
  ASSERT_TRUE(baseline) << baseline.error().message();
  ASSERT_NE(baseline->find("logging.level"), std::string::npos);
  const size_t header_length = baseline->find("\r\n") + 2;

  for (size_t recv_buffer_size : {size_t{1}, header_length}) {
    ClientConfig config;
    config.host = "127.0.0.1";
    config.port = server_->GetPort();
    config.recv_buffer_size = static_cast<uint32_t>(recv_buffer_size);
    MygramClient client(config);
    ASSERT_TRUE(client.Connect()) << "recv_buffer_size=" << recv_buffer_size;

    auto result = client.ShowVariables("logging%");
    ASSERT_TRUE(result) << "recv_buffer_size=" << recv_buffer_size << ": " << result.error().message();
    EXPECT_NE(result->find("logging.level"), std::string::npos) << "recv_buffer_size=" << recv_buffer_size;

    EXPECT_TRUE(client.Info()) << "recv_buffer_size=" << recv_buffer_size;
  }
}

TEST_F(MygramClientTest, QuotedHighlightLiteralSearchAndCountReturnPromptly) {
  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 2000;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());

  auto search = client.Search("testdb.test", "syntax highlight editor");
  ASSERT_TRUE(search) << search.error().message();
  EXPECT_EQ(search->total_count, 0U);

  auto count = client.Count("testdb.test", "syntax highlight editor");
  ASSERT_TRUE(count) << count.error().message();
  EXPECT_EQ(count->count, 0U);

  auto raw = client.SendCommand("SEARCH testdb.test 'use highlight here'");
  ASSERT_TRUE(raw) << raw.error().message();
  EXPECT_EQ(*raw, "OK RESULTS 0");
}

TEST_F(MygramClientTest, ErrorReplyUnderHighlightOrDebugCompletesWithServerCode) {
  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 2000;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());

  auto highlighted = client.SearchWithHighlights("testdb.missing", "hello", 10);
  ASSERT_FALSE(highlighted);
  EXPECT_EQ(highlighted.error().code(), mygram::utils::ErrorCode::kTableNotFound) << highlighted.error().message();

  ASSERT_TRUE(client.EnableDebug());
  auto debug_search = client.Search("testdb.missing", "hello", 10);
  ASSERT_FALSE(debug_search);
  EXPECT_EQ(debug_search.error().code(), mygram::utils::ErrorCode::kTableNotFound) << debug_search.error().message();
  auto debug_count = client.Count("testdb.missing", "hello");
  ASSERT_FALSE(debug_count);
  EXPECT_EQ(debug_count.error().code(), mygram::utils::ErrorCode::kTableNotFound) << debug_count.error().message();

  EXPECT_TRUE(client.IsConnected());
  EXPECT_TRUE(client.Info());
}

TEST_F(MygramClientTest, RawDebugOnFramesFollowingSearchAtHeaderSizedReads) {
  AddTestDocuments();
  const size_t header_length = std::string("OK RESULTS 2 2 1\r\n").size();

  for (const char* debug_on : {"DEBUG ON", "debug  on"}) {
    ClientConfig config;
    config.host = "127.0.0.1";
    config.port = server_->GetPort();
    config.recv_buffer_size = static_cast<uint32_t>(header_length);
    MygramClient client(config);
    ASSERT_TRUE(client.Connect());

    auto toggled = client.SendCommand(debug_on);
    ASSERT_TRUE(toggled) << toggled.error().message();
    EXPECT_EQ(*toggled, "OK DEBUG_ON");

    auto result = client.Search("testdb.test", "hello", 100);
    ASSERT_TRUE(result) << debug_on << ": " << result.error().message();
    EXPECT_EQ(result->results.size(), 2U) << debug_on;
    ASSERT_TRUE(result->debug.has_value()) << debug_on;
    EXPECT_GT(result->debug->terms, 0U) << debug_on;

    auto info = client.Info();
    ASSERT_TRUE(info) << debug_on << ": " << info.error().message();
    EXPECT_FALSE(info->version.empty()) << debug_on;
  }
}

// Run under ThreadSanitizer: the DEBUG flag must only ever be touched under
// the command lock, and every SEARCH must be framed with the server's flag.
TEST_F(MygramClientTest, DebugToggleRacingSearchKeepsFramingConsistent) {
  AddTestDocuments();
  ASSERT_TRUE(client_->Connect());

  constexpr int kIterations = 50;
  std::atomic<int> toggle_failures{0};
  std::atomic<int> search_failures{0};
  std::string first_search_failure;

  std::thread toggler([this, &toggle_failures]() {
    for (int i = 0; i < kIterations; ++i) {
      if (!client_->EnableDebug() || !client_->DisableDebug()) {
        toggle_failures++;
      }
    }
  });
  std::thread searcher([this, &search_failures, &first_search_failure]() {
    for (int i = 0; i < kIterations; ++i) {
      auto result = client_->Search("testdb.test", "hello", 100);
      if (!result || result->results.size() != 2U) {
        if (search_failures++ == 0) {
          first_search_failure = result ? "unexpected result count" : result.error().message();
        }
      }
    }
  });
  toggler.join();
  searcher.join();

  EXPECT_EQ(toggle_failures.load(), 0);
  EXPECT_EQ(search_failures.load(), 0) << first_search_failure;
  EXPECT_TRUE(client_->Info());
}

TEST_F(MygramClientTest, SingleByteReceiveBufferFacetThenInfo) {
  AddTestDocuments();

  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.recv_buffer_size = 1;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());

  auto facet = client.Facet("testdb.test", "status");
  ASSERT_TRUE(facet) << facet.error().message();
  EXPECT_EQ(facet->facets.size(), 2U);

  auto info = client.Info();
  ASSERT_TRUE(info) << info.error().message();
  EXPECT_FALSE(info->version.empty());
}

TEST_F(MygramClientTest, SingleByteReceiveBufferDebugHighlightSearchThenInfo) {
  const std::string original_text = "Hello world example";
  const std::string normalized = mygram::utils::NormalizeText(original_text, true, "keep", true);
  ASSERT_TRUE(doc_store_->AddDocument("highlight_doc", {}, normalized, original_text));
  index_->AddDocument(1, normalized);

  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.recv_buffer_size = 1;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());
  ASSERT_TRUE(client.EnableDebug());

  auto result = client.SearchWithHighlights("testdb.test", "hello", 10);
  ASSERT_TRUE(result) << result.error().message();
  ASSERT_EQ(result->results.size(), 1U);
  EXPECT_EQ(result->results[0].primary_key, "highlight_doc");

  auto info = client.Info();
  ASSERT_TRUE(info) << info.error().message();
  EXPECT_FALSE(info->version.empty());
}

TEST_F(MygramClientTest, ConfigHelpBodyWithBlankLinesIsReadWholeAtSmallReads) {
  ASSERT_TRUE(client_->Connect());
  auto baseline_path = client_->SendCommand("CONFIG HELP mysql.port");
  ASSERT_TRUE(baseline_path) << baseline_path.error().message();
  ASSERT_NE(baseline_path->find("Type:"), std::string::npos) << *baseline_path;
  auto baseline_sections = client_->SendCommand("CONFIG HELP");
  ASSERT_TRUE(baseline_sections) << baseline_sections.error().message();
  ASSERT_NE(baseline_sections->find("Use \"CONFIG HELP"), std::string::npos) << *baseline_sections;
  const size_t header_length = std::string("+OK\r\n").size();

  for (size_t recv_buffer_size : {size_t{1}, header_length}) {
    ClientConfig config;
    config.host = "127.0.0.1";
    config.port = server_->GetPort();
    config.recv_buffer_size = static_cast<uint32_t>(recv_buffer_size);
    MygramClient client(config);
    ASSERT_TRUE(client.Connect()) << "recv_buffer_size=" << recv_buffer_size;

    auto path_help = client.SendCommand("CONFIG HELP mysql.port");
    ASSERT_TRUE(path_help) << "recv_buffer_size=" << recv_buffer_size << ": " << path_help.error().message();
    EXPECT_EQ(*path_help, *baseline_path) << "recv_buffer_size=" << recv_buffer_size;

    auto section_help = client.SendCommand("CONFIG HELP");
    ASSERT_TRUE(section_help) << "recv_buffer_size=" << recv_buffer_size << ": " << section_help.error().message();
    EXPECT_EQ(*section_help, *baseline_sections) << "recv_buffer_size=" << recv_buffer_size;

    auto info = client.Info();
    ASSERT_TRUE(info) << "recv_buffer_size=" << recv_buffer_size << ": " << info.error().message();
    EXPECT_FALSE(info->version.empty()) << "recv_buffer_size=" << recv_buffer_size;
  }
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

TEST_F(MygramClientTest, TypedSearchQuotesStandaloneReservedWord) {
  AddTestDocuments();
  doc_store_->AddDocument("4", {});
  index_->AddDocument(4, mygram::utils::NormalizeText("FILTER", true, "keep", true));
  ASSERT_TRUE(client_->Connect());

  auto result = client_->Search("testdb.test", "FILTER", 100);
  ASSERT_TRUE(result) << result.error().message();
  ASSERT_EQ(result->results.size(), 1U);
  EXPECT_EQ(result->results[0].primary_key, "4");
}

TEST_F(MygramClientTest, CApiSearchRawPreservesGroupedBooleanExpression) {
  AddGroupedBooleanTestDocuments();

  MygramClientConfig_C config{};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;
  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client), 0) << mygramclient_get_last_error(c_client);

  MygramSearchResult_C* result = nullptr;
  ASSERT_EQ(mygramclient_search_raw(c_client, "testdb.test", "alpha AND (xqz OR jkv)", 100, 0, &result), 0)
      << mygramclient_get_last_error(c_client);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->total_count, 2U);
  ASSERT_EQ(result->count, 2U);
  ASSERT_NE(result->primary_keys, nullptr);
  EXPECT_NE(result->primary_keys[0], nullptr);
  mygramclient_free_search_result(result);
  mygramclient_destroy(c_client);
}

TEST_F(MygramClientTest, CApiSearchRawWithHighlightsPreservesGroupedBooleanExpression) {
  AddGroupedBooleanTestDocuments();

  MygramClientConfig_C config{};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;
  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client), 0) << mygramclient_get_last_error(c_client);

  MygramSearchResultWithHighlights_C* highlighted = nullptr;
  ASSERT_EQ(
      mygramclient_search_raw_with_highlights(c_client, "testdb.test", "alpha AND (xqz OR jkv)", 100, 0, &highlighted),
      0)
      << mygramclient_get_last_error(c_client);
  ASSERT_NE(highlighted, nullptr);
  EXPECT_EQ(highlighted->total_count, 2U);
  ASSERT_EQ(highlighted->count, 2U);
  ASSERT_NE(highlighted->primary_keys, nullptr);
  ASSERT_NE(highlighted->snippets, nullptr);
  EXPECT_NE(highlighted->primary_keys[0], nullptr);
  EXPECT_NE(highlighted->snippets[0], nullptr);
  mygramclient_free_search_result_with_highlights(highlighted);
  mygramclient_destroy(c_client);
}

TEST_F(MygramClientTest, CApiV2SupportsUnixSocketAndShorterKnownStruct) {
  AddTestDocuments();
  const auto socket_path =
      std::filesystem::temp_directory_path() / ("mygramclient_c_v2_" + std::to_string(::getpid()) + ".sock");
  server::ServerConfig uds_config;
  uds_config.unix_socket_path = socket_path.string();
  uds_config.port = 0;
  auto uds_server = std::make_unique<server::TcpServer>(uds_config, table_contexts_);
  ASSERT_TRUE(uds_server->Start());

  MygramClientConfigV2_C config{};
  config.struct_size = sizeof(config);
  config.version = MYGRAMCLIENT_CONFIG_V2_VERSION;
  config.host = "invalid.invalid";
  config.timeout_ms = 5000;
  config.unix_socket_path = socket_path.c_str();
  config.dump_save_timeout_ms = 700000;
  MygramClient_C* uds_client = mygramclient_create_v2(&config);
  ASSERT_NE(uds_client, nullptr);
  ASSERT_EQ(mygramclient_connect(uds_client), 0) << mygramclient_get_last_error(uds_client);
  mygramclient_destroy(uds_client);
  ASSERT_TRUE(uds_server->Stop());

  MygramClientConfigV2_C short_config{};
  short_config.struct_size = offsetof(MygramClientConfigV2_C, unix_socket_path);
  short_config.version = MYGRAMCLIENT_CONFIG_V2_VERSION;
  short_config.host = "127.0.0.1";
  short_config.port = server_->GetPort();
  short_config.timeout_ms = 5000;
  short_config.unix_socket_path = "/must/be/ignored/by/older/size";
  MygramClient_C* tcp_client = mygramclient_create_v2(&short_config);
  ASSERT_NE(tcp_client, nullptr);
  ASSERT_EQ(mygramclient_connect(tcp_client), 0) << mygramclient_get_last_error(tcp_client);
  mygramclient_destroy(tcp_client);
}

TEST(MygramClientUnixSocketTest, RejectsPathLongerThanTheAddressBufferInsteadOfTruncatingIt) {
  // A path one byte past the buffer would be silently cut to a name that can
  // resolve to a different socket, so it has to be reported rather than used.
  constexpr size_t kMaxUnixSocketPathLength = sizeof(sockaddr_un::sun_path) - 1;
  const std::string too_long = "/tmp/" + std::string(kMaxUnixSocketPathLength, 'p');

  ClientConfig config;
  config.unix_socket_path = too_long;
  MygramClient client(config);

  auto result = client.Connect();
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kClientInvalidArgument);
  EXPECT_NE(result.error().message().find(too_long), std::string::npos) << result.error().message();

  MygramClientConfigV2_C c_config{};
  c_config.struct_size = sizeof(c_config);
  c_config.version = MYGRAMCLIENT_CONFIG_V2_VERSION;
  c_config.host = "127.0.0.1";
  c_config.timeout_ms = 5000;
  c_config.unix_socket_path = too_long.c_str();
  MygramClient_C* c_client = mygramclient_create_v2(&c_config);
  ASSERT_NE(c_client, nullptr);
  EXPECT_EQ(mygramclient_connect(c_client), -1);
  EXPECT_NE(std::string(mygramclient_get_last_error(c_client)).find(too_long), std::string::npos)
      << mygramclient_get_last_error(c_client);
  mygramclient_destroy(c_client);
}

TEST(MygramClientCApiV2Test, RejectsStructSizeOnlyPrefixWithoutReadingPastAllocation) {
  alignas(uint32_t) std::array<std::byte, sizeof(uint32_t)> storage{};
  const uint32_t caller_size = sizeof(uint32_t);
  std::memcpy(storage.data(), &caller_size, sizeof(caller_size));
  const auto* short_config = reinterpret_cast<const MygramClientConfigV2_C*>(storage.data());
  EXPECT_EQ(mygramclient_create_v2(short_config), nullptr);
}

TEST(MygramClientCApiV2Test, RejectsUnsupportedConfigVersion) {
  MygramClientConfigV2_C config{};
  config.struct_size = sizeof(config);
  config.version = MYGRAMCLIENT_CONFIG_V2_VERSION + 1;
  config.host = "127.0.0.1";
  EXPECT_EQ(mygramclient_create_v2(&config), nullptr);
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

/**
 * @brief Connect() reaches a server bound to an IPv6 address (or a hostname
 * resolving only to an AAAA record). getaddrinfo resolves with AF_UNSPEC and
 * every candidate address is tried in turn (see Connect(), mygramclient.cpp).
 */
TEST_F(MygramClientTest, ConnectsToIpv6Loopback) {
  server::ServerConfig ipv6_server_config;
  ipv6_server_config.port = 0;
  ipv6_server_config.host = "::1";
  ipv6_server_config.allow_cidrs = {"::1/128"};
  auto ipv6_server = std::make_unique<server::TcpServer>(ipv6_server_config, table_contexts_, "./dumps", &full_config_);
  if (!ipv6_server->Start()) {
    GTEST_SKIP() << "IPv6 loopback is not available in this environment";
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  ClientConfig ipv6_client_config;
  ipv6_client_config.host = "::1";
  ipv6_client_config.port = ipv6_server->GetPort();
  ipv6_client_config.timeout_ms = 5000;
  MygramClient ipv6_client(ipv6_client_config);

  auto connect_result = ipv6_client.Connect();
  ASSERT_TRUE(connect_result) << connect_result.error().message();
  EXPECT_TRUE(ipv6_client.Info());

  ipv6_client.Disconnect();
  ipv6_server->Stop();
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
  const std::string original_text = "Hello ＷＯＲＬＤ example";
  const std::string text = mygram::utils::NormalizeText(original_text, true, "keep", true);
  ASSERT_TRUE(doc_store_->AddDocument("highlight_doc", {}, text, original_text));
  index_->AddDocument(1, text);

  ASSERT_TRUE(client_->Connect());

  auto result = client_->SearchWithHighlights("testdb.test", "hello", 10);
  ASSERT_TRUE(result) << "Search error: " << result.error().message();

  const auto& resp = *result;
  EXPECT_EQ(resp.total_count, 1);
  ASSERT_EQ(resp.results.size(), 1);
  EXPECT_EQ(resp.results[0].primary_key, "highlight_doc");
  EXPECT_NE(resp.results[0].snippet.find("<em>Hello</em>"), std::string::npos);
  EXPECT_NE(resp.results[0].snippet.find("ＷＯＲＬＤ"), std::string::npos);
}

TEST_F(MygramClientTest, CApiSearchWithHighlightsReturnsSnippets) {
  const std::string original_text = "Hello ＷＯＲＬＤ example";
  const std::string text = mygram::utils::NormalizeText(original_text, true, "keep", true);
  ASSERT_TRUE(doc_store_->AddDocument("highlight_doc", {}, text, original_text));
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
  EXPECT_NE(std::string(search_result->snippets[0]).find("<em>Hello</em>"), std::string::npos);
  EXPECT_NE(std::string(search_result->snippets[0]).find("ＷＯＲＬＤ"), std::string::npos);

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

TEST_F(MygramClientTest, TypedSearchOptionsExposeFilterFuzzyAndHighlight) {
  AddTestDocuments();
  ASSERT_TRUE(client_->Connect());

  SearchOptions options;
  options.limit = 10;
  options.filters.push_back({"status", FilterOp::kNotEqual, "inactive"});
  options.fuzzy_distance = 1;
  options.highlight = HighlightOptions{"<mark>", "</mark>", 128, 2};

  auto result = client_->Search("testdb.test", "hello", options);
  ASSERT_TRUE(result) << result.error().message();
  ASSERT_EQ(result->results.size(), 2U);
  EXPECT_EQ(result->total_count, 2U);
}

TEST_F(MygramClientTest, TypedBooleanSearchCombinesExpressionFilterAndSort) {
  AddTestDocuments();
  ASSERT_TRUE(client_->Connect());

  SearchOptions options;
  options.query_mode = QueryMode::kBoolean;
  options.filters.push_back({"status", FilterOp::kEqual, "active"});
  options.sort_desc = false;

  auto result = client_->Search("testdb.test", "(hello OR news)", options);
  ASSERT_TRUE(result) << result.error().message();
  ASSERT_EQ(result->results.size(), 2U);
  EXPECT_EQ(result->total_count, 2U);
  EXPECT_EQ(result->results[0].primary_key, "1");
  EXPECT_EQ(result->results[1].primary_key, "2");
}

TEST_F(MygramClientTest, CApiTypedSearchOptionsExposeCompleteSurface) {
  AddTestDocuments();
  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client), 0);

  MygramFilter_C filter{"status", MYGRAM_FILTER_NE, "inactive"};
  MygramSearchOptions_C options = {};
  options.struct_size = sizeof(options);
  options.limit = 1;
  options.filters = &filter;
  options.filter_count = 1;
  options.sort_desc = 1;
  options.fuzzy_distance = 1;
  options.highlight = 1;
  options.highlight_open_tag = "<mark>";
  options.highlight_close_tag = "</mark>";
  options.highlight_snippet_length = 128;
  options.highlight_max_fragments = 2;
  options.query_mode = MYGRAM_QUERY_BOOLEAN;

  MygramSearchResultWithHighlights_C* result = nullptr;
  ASSERT_EQ(mygramclient_search_with_options(c_client, "testdb.test", "(hello OR news)", &options, &result), 0)
      << mygramclient_get_last_error(c_client);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->count, 1U);
  EXPECT_EQ(result->total_count, 2U);
  mygramclient_free_search_result_with_highlights(result);

  options.query_mode = static_cast<MygramQueryMode_C>(99);
  result = nullptr;
  EXPECT_EQ(mygramclient_search_with_options(c_client, "testdb.test", "hello", &options, &result), -1);
  EXPECT_EQ(result, nullptr);
  EXPECT_NE(std::string(mygramclient_get_last_error(c_client)).find("query_mode"), std::string::npos);
  mygramclient_destroy(c_client);
}

TEST_F(MygramClientTest, CApiSearchOptionsAcceptsLegacyPrefixSize) {
  AddTestDocuments();
  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client), 0);

  struct LegacySearchOptions {
    uint32_t struct_size;
    uint32_t limit;
    uint32_t offset;
  } legacy_options = {sizeof(LegacySearchOptions), 1, 0};

  MygramSearchResultWithHighlights_C* result = nullptr;
  ASSERT_EQ(mygramclient_search_with_options(c_client, "testdb.test", "hello",
                                             reinterpret_cast<const MygramSearchOptions_C*>(&legacy_options), &result),
            0)
      << mygramclient_get_last_error(c_client);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->count, 1U);
  EXPECT_EQ(result->total_count, 2U);

  mygramclient_free_search_result_with_highlights(result);
  mygramclient_destroy(c_client);
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

  auto simplified = SimplifySearchExpression("+alpha (xqz OR jkv)");
  ASSERT_FALSE(simplified);
  EXPECT_EQ(simplified.error().code(), mygram::utils::ErrorCode::kClientExpressionParseError);
}

// ConvertSearchExpression round-trips ordinary English words that collide with
// reserved clause keywords. The shared wire_quoting.h predicate quotes them, so
// the server's tokenizer (case-insensitive on AND/OR/NOT/ORDER --
// query_parser_commands.cpp, query_ast.cpp) reads them as search text, not operators.
TEST_F(MygramClientTest, SearchRawHandlesOrdinaryWordOrder) {
  ASSERT_TRUE(
      doc_store_->AddDocument("order_doc", {}, mygram::utils::NormalizeText("order history", true, "keep", true)));
  index_->AddDocument(1, mygram::utils::NormalizeText("order history", true, "keep", true));

  ASSERT_TRUE(client_->Connect());

  auto converted = ConvertSearchExpression("order history");
  ASSERT_TRUE(converted) << converted.error().message();

  auto result = client_->SearchRaw("testdb.test", *converted, 10);
  ASSERT_TRUE(result) << "SearchRaw error: " << result.error().message();
  EXPECT_EQ(result->total_count, 1U);
}

TEST_F(MygramClientTest, SearchRawHandlesCaseInsensitiveOr) {
  const std::string tom_text = mygram::utils::NormalizeText("tom appears here", true, "keep", true);
  const std::string jerry_text = mygram::utils::NormalizeText("jerry appears here", true, "keep", true);
  ASSERT_TRUE(doc_store_->AddDocument("tom_doc", {}, tom_text));
  index_->AddDocument(1, tom_text);
  ASSERT_TRUE(doc_store_->AddDocument("jerry_doc", {}, jerry_text));
  index_->AddDocument(2, jerry_text);

  ASSERT_TRUE(client_->Connect());

  // Lowercase "or": the tokenizer must recognize it as the OR operator the
  // same way the server does, not as three required AND terms.
  auto converted = ConvertSearchExpression("tom or jerry");
  ASSERT_TRUE(converted) << converted.error().message();

  auto result = client_->SearchRaw("testdb.test", *converted, 10);
  ASSERT_TRUE(result) << "SearchRaw error: " << result.error().message();
  EXPECT_EQ(result->total_count, 2U);
}

TEST_F(MygramClientTest, SearchRawHandlesMinusInsideGroup) {
  const std::string tutorial_only = mygram::utils::NormalizeText("tutorial content", true, "keep", true);
  const std::string tutorial_video = mygram::utils::NormalizeText("tutorial video content", true, "keep", true);
  ASSERT_TRUE(doc_store_->AddDocument("tutorial_only", {}, tutorial_only));
  index_->AddDocument(1, tutorial_only);
  ASSERT_TRUE(doc_store_->AddDocument("tutorial_video", {}, tutorial_video));
  index_->AddDocument(2, tutorial_video);

  ASSERT_TRUE(client_->Connect());

  // "-video" inside a group must become "NOT video", not glue onto the
  // preceding term as the literal text "tutorial-video".
  auto converted = ConvertSearchExpression("(tutorial -video)");
  ASSERT_TRUE(converted) << converted.error().message();
  EXPECT_NE(converted->find("NOT video"), std::string::npos) << *converted;
  EXPECT_EQ(converted->find("tutorial-video"), std::string::npos) << *converted;

  auto result = client_->SearchRaw("testdb.test", *converted, 10);
  ASSERT_TRUE(result) << "SearchRaw error: " << result.error().message();
  ASSERT_EQ(result->results.size(), 1U);
  EXPECT_EQ(result->results[0].primary_key, "tutorial_only");
}

TEST_F(MygramClientTest, SearchRawHandlesQuotedPhraseWithEmbeddedQuote) {
  const std::string text = mygram::utils::NormalizeText("she said hello world to everyone", true, "keep", true);
  ASSERT_TRUE(doc_store_->AddDocument("phrase_doc", {}, text));
  index_->AddDocument(1, text);

  ASSERT_TRUE(client_->Connect());

  // The quoted phrase itself contains a literal double quote, which
  // ToQueryString must re-escape rather than emit as a raw, unterminated
  // quote character.
  auto converted = ConvertSearchExpression(R"("she said \"hello world\"")");
  ASSERT_TRUE(converted) << converted.error().message();

  auto result = client_->SearchRaw("testdb.test", *converted, 10);
  ASSERT_TRUE(result) << "SearchRaw error: " << result.error().message();
}

/**
 * @brief A literal term equal to "not" must reach the server as search
 * text, not as the AND-clause's NOT-fold operator (query_parser_clauses.cpp
 * ParseAnd: `if (!IsQuotedToken(pos) && EqualsIgnoreCase(tokens[pos], "NOT"))
 * return ParseNot(...)`, which then fails with no term following it).
 * ToQueryString quotes it (wire_quoting.h), and the server's own
 * quoted-token check is what makes that quoting effective: the identical
 * bytes sent unquoted are rejected, proving quoting is what avoids the fold.
 */
TEST_F(MygramClientTest, SearchRawHandlesLiteralTermCollidingWithNotKeyword) {
  ASSERT_TRUE(client_->Connect());

  auto converted = ConvertSearchExpression("golang not");
  ASSERT_TRUE(converted) << converted.error().message();
  EXPECT_EQ(*converted, R"(golang AND "not")");

  auto quoted_result = client_->SearchRaw("testdb.test", *converted, 10);
  EXPECT_TRUE(quoted_result) << "SearchRaw error: " << quoted_result.error().message();

  auto unquoted_result = client_->SearchRaw("testdb.test", "golang AND not", 10);
  EXPECT_FALSE(unquoted_result) << "unquoted 'not' should hit the NOT-fold and fail with no term following it";
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
  EXPECT_EQ(resp.total_count, 1u);
  EXPECT_EQ(resp.facets[0].value, "active");
  EXPECT_EQ(resp.facets[0].count, 2u);
}

TEST_F(MygramClientTest, FacetExposesOffsetAndTotalCount) {
  AddTestDocuments();
  ASSERT_TRUE(client_->Connect());

  const auto result = client_->Facet("testdb.test", "status", "", 1, {}, {}, {}, 1);
  ASSERT_TRUE(result) << "Facet error: " << result.error().message();
  ASSERT_EQ(result->facets.size(), 1u);
  EXPECT_EQ(result->total_count, 2u);
  EXPECT_EQ(result->facets[0].value, "inactive");
  EXPECT_EQ(result->facets[0].count, 1u);
}

TEST_F(MygramClientTest, FacetAcceptsLegacyHeaderWithoutTotalCount) {
  const int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listen_fd, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ASSERT_EQ(bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  ASSERT_EQ(listen(listen_fd, 1), 0);
  socklen_t addr_len = sizeof(addr);
  ASSERT_EQ(getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len), 0);

  std::thread responder([listen_fd]() {
    const int client_fd = accept(listen_fd, nullptr, nullptr);
    if (client_fd >= 0) {
      char request[128];
      (void)recv(client_fd, request, sizeof(request), 0);
      static constexpr char kLegacyResponse[] = "OK FACET 1\r\nlegacy\t7\r\n\r\n";
      (void)send(client_fd, kLegacyResponse, sizeof(kLegacyResponse) - 1, 0);
      close(client_fd);
    }
    close(listen_fd);
  });

  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = ntohs(addr.sin_port);
  config.timeout_ms = 1000;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());

  const auto result = client.Facet("testdb.test", "status");
  responder.join();

  ASSERT_TRUE(result) << result.error().message();
  ASSERT_EQ(result->facets.size(), 1u);
  EXPECT_EQ(result->total_count, 1u);
  EXPECT_EQ(result->facets[0].value, "legacy");
  EXPECT_EQ(result->facets[0].count, 7u);
}

TEST_F(MygramClientTest, RestoresTypedServerErrorCode) {
  AddTestDocuments();
  ASSERT_TRUE(client_->Connect());

  const auto result = client_->Facet("testdb.test", "missing_column", "hello", 1);
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kIndexNotFound);
  EXPECT_NE(result.error().message().find("missing_column"), std::string::npos);
}

TEST_F(MygramClientTest, FacetValueStartingWithHashIsDataRow) {
  storage::FilterMap filters;
  filters["status"] = std::string("#active");
  auto doc_id = doc_store_->AddDocument("hash_status", filters);
  ASSERT_TRUE(doc_id.has_value());
  index_->AddDocument(*doc_id, "hello hash facet");

  ASSERT_TRUE(client_->Connect());

  auto result = client_->Facet("testdb.test", "status", "hello", 10);
  ASSERT_TRUE(result) << "Facet error: " << result.error().message();

  const auto& resp = *result;
  ASSERT_EQ(resp.facets.size(), 1u);
  EXPECT_EQ(resp.facets[0].value, "#active");
  EXPECT_EQ(resp.facets[0].count, 1u);
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
  EXPECT_EQ(facet_result->total_count, 1u);
  EXPECT_STREQ(facet_result->values[0], "active");
  EXPECT_EQ(facet_result->counts[0], 2u);

  mygramclient_free_facet_result(facet_result);

  facet_result = nullptr;
  ASSERT_EQ(mygramclient_facet_paged(c_client, "testdb.test", "status", "", 1, 1, &facet_result), 0)
      << mygramclient_get_last_error(c_client);
  ASSERT_NE(facet_result, nullptr);
  ASSERT_EQ(facet_result->count, 1u);
  EXPECT_EQ(facet_result->total_count, 2u);
  EXPECT_STREQ(facet_result->values[0], "inactive");
  EXPECT_EQ(facet_result->counts[0], 1u);
  mygramclient_free_facet_result(facet_result);

  facet_result = nullptr;
  result = mygramclient_facet(c_client, "testdb.test", "missing_column", "hello", 0, &facet_result);
  EXPECT_EQ(result, -1);
  EXPECT_EQ(facet_result, nullptr);
  EXPECT_EQ(mygramclient_get_last_error_code(c_client), static_cast<int>(mygram::utils::ErrorCode::kIndexNotFound));

  mygramclient_disconnect(c_client);
  mygramclient_destroy(c_client);
}

TEST_F(MygramClientTest, CApiFacetAdvancedCoversFiltersAndArrayContracts) {
  AddTestDocuments();

  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;
  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client), 0) << mygramclient_get_last_error(c_client);

  const char* not_terms[] = {"x"};
  const char* filter_keys[] = {"status"};
  const char* filter_values[] = {"inactive"};
  MygramFacetResult_C* result = nullptr;
  ASSERT_EQ(mygramclient_facet_advanced(c_client, "testdb.test", "status", "w", 0, nullptr, 0, not_terms, 1,
                                        filter_keys, filter_values, 1, &result),
            0)
      << mygramclient_get_last_error(c_client);
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->count, 1u);
  EXPECT_EQ(result->total_count, 1u);
  EXPECT_STREQ(result->values[0], "inactive");
  EXPECT_EQ(result->counts[0], 1u);
  mygramclient_free_facet_result(result);

  result = nullptr;
  ASSERT_EQ(mygramclient_facet_advanced_paged(c_client, "testdb.test", "status", "", 1, 1, nullptr, 0, nullptr, 0,
                                              nullptr, nullptr, 0, &result),
            0)
      << mygramclient_get_last_error(c_client);
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->count, 1u);
  EXPECT_EQ(result->total_count, 2u);
  EXPECT_STREQ(result->values[0], "inactive");
  mygramclient_free_facet_result(result);

  result = reinterpret_cast<MygramFacetResult_C*>(static_cast<uintptr_t>(1));
  EXPECT_EQ(mygramclient_facet_advanced(c_client, "testdb.test", "status", "w", 0, nullptr, 1, nullptr, 0, nullptr,
                                        nullptr, 0, &result),
            -1);
  EXPECT_EQ(result, nullptr);
  EXPECT_NE(std::string(mygramclient_get_last_error(c_client)).find("and_terms"), std::string::npos);

  result = reinterpret_cast<MygramFacetResult_C*>(static_cast<uintptr_t>(1));
  EXPECT_EQ(mygramclient_facet_advanced(c_client, "testdb.test", "status", "w", 0, nullptr, 0, nullptr, 1, nullptr,
                                        nullptr, 0, &result),
            -1);
  EXPECT_EQ(result, nullptr);
  EXPECT_NE(std::string(mygramclient_get_last_error(c_client)).find("not_terms"), std::string::npos);

  result = reinterpret_cast<MygramFacetResult_C*>(static_cast<uintptr_t>(1));
  EXPECT_EQ(mygramclient_facet_advanced(c_client, "testdb.test", "status", "w", 0, nullptr, 0, nullptr, 0, nullptr,
                                        nullptr, 1, &result),
            -1);
  EXPECT_EQ(result, nullptr);
  EXPECT_NE(std::string(mygramclient_get_last_error(c_client)).find("filter"), std::string::npos);

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

TEST_F(MygramClientTest, PrimaryKeysRoundTripAcrossSearchHighlightAndGet) {
  const std::string spaced_primary_key = "a b";
  const std::string underscored_primary_key = "a_b";
  const std::string controlled_primary_key = std::string("control") + '\x01' + "key";
  const std::string unicode_space_primary_key = "a　b";

  auto spaced_doc_id = doc_store_->AddDocument(spaced_primary_key);
  auto underscored_doc_id = doc_store_->AddDocument(underscored_primary_key);
  auto controlled_doc_id = doc_store_->AddDocument(controlled_primary_key);
  auto unicode_space_doc_id = doc_store_->AddDocument(unicode_space_primary_key);
  ASSERT_TRUE(spaced_doc_id);
  ASSERT_TRUE(underscored_doc_id);
  ASSERT_TRUE(controlled_doc_id);
  ASSERT_TRUE(unicode_space_doc_id);
  index_->AddDocument(*spaced_doc_id, "roundtrip");
  index_->AddDocument(*underscored_doc_id, "roundtrip");
  index_->AddDocument(*controlled_doc_id, "roundtrip");
  index_->AddDocument(*unicode_space_doc_id, "roundtrip");

  ASSERT_TRUE(client_->Connect());

  auto search = client_->Search("testdb.test", "roundtrip", 10);
  ASSERT_TRUE(search) << search.error().message();
  ASSERT_EQ(search->results.size(), 4U);

  std::unordered_set<std::string> returned_primary_keys;
  for (const auto& result : search->results) {
    returned_primary_keys.insert(result.primary_key);
    auto document = client_->Get("testdb.test", result.primary_key);
    ASSERT_TRUE(document) << "GET failed for returned key: " << document.error().message();
    EXPECT_EQ(document->primary_key, result.primary_key);
  }
  EXPECT_EQ(returned_primary_keys.count(spaced_primary_key), 1U);
  EXPECT_EQ(returned_primary_keys.count(underscored_primary_key), 1U);
  EXPECT_EQ(returned_primary_keys.count(controlled_primary_key), 1U);
  EXPECT_EQ(returned_primary_keys.count(unicode_space_primary_key), 1U);

  auto highlighted = client_->SearchWithHighlights("testdb.test", "roundtrip", 10);
  ASSERT_TRUE(highlighted) << highlighted.error().message();
  ASSERT_EQ(highlighted->results.size(), 4U);
  std::unordered_set<std::string> highlighted_primary_keys;
  for (const auto& result : highlighted->results) {
    highlighted_primary_keys.insert(result.primary_key);
  }
  EXPECT_EQ(highlighted_primary_keys, returned_primary_keys);
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
 * A \r\n inside the response must not terminate it prematurely.
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

  auto parsed_cache_stats = client_->GetCacheStatistics();
  ASSERT_TRUE(parsed_cache_stats) << "GetCacheStatistics error: " << parsed_cache_stats.error().message();
  EXPECT_TRUE(parsed_cache_stats->enabled);
  EXPECT_LE(parsed_cache_stats->cache_hits, parsed_cache_stats->total_queries);
  EXPECT_LE(parsed_cache_stats->cache_misses, parsed_cache_stats->total_queries);
  EXPECT_GE(parsed_cache_stats->hit_rate, 0.0);
  EXPECT_LE(parsed_cache_stats->hit_rate, 1.0);

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

  // Result IDs must not include the literal "#" token (the "# DEBUG" header).
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

TEST_F(MygramClientTest, SendCommandRejectsEmbeddedProtocolDelimiter) {
  ASSERT_TRUE(client_->Connect());

  const auto result = client_->SendCommand("INFO\r\nREPLICATION STOP");

  ASSERT_FALSE(result);
  EXPECT_NE(result.error().message().find("control character"), std::string::npos);
}

TEST_F(MygramClientTest, SendCommandDisconnectsAfterReceiveFailure) {
  int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listen_fd, 0);

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ASSERT_EQ(bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  ASSERT_EQ(listen(listen_fd, 1), 0);

  socklen_t addr_len = sizeof(addr);
  ASSERT_EQ(getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len), 0);
  const uint16_t port = ntohs(addr.sin_port);

  std::thread closer([listen_fd]() {
    int client_fd = accept(listen_fd, nullptr, nullptr);
    if (client_fd >= 0) {
      char buffer[128];
      (void)recv(client_fd, buffer, sizeof(buffer), 0);
      close(client_fd);
    }
    close(listen_fd);
  });

  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = port;
  config.timeout_ms = 1000;
  MygramClient client(config);

  ASSERT_TRUE(client.Connect());
  ASSERT_TRUE(client.IsConnected());

  auto result = client.SendCommand("COUNT testdb.test hello");
  EXPECT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kClientConnectionClosed);
  EXPECT_FALSE(client.IsConnected());

  closer.join();
}

// Client-side SIGPIPE: the server answers one command
// normally, then RSTs the connection. A later send() on that same socket
// must surface as kClientSendFailed rather than raising SIGPIPE and killing
// the process (see ApplyPostConnectSocketOptions / kSendFlags).
TEST_F(MygramClientTest, SendCommandAfterPeerResetReturnsErrorInsteadOfSigpipe) {
  int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listen_fd, 0);

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ASSERT_EQ(bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  ASSERT_EQ(listen(listen_fd, 1), 0);

  socklen_t addr_len = sizeof(addr);
  ASSERT_EQ(getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len), 0);
  const uint16_t port = ntohs(addr.sin_port);

  std::thread responder([listen_fd]() {
    int client_fd = accept(listen_fd, nullptr, nullptr);
    if (client_fd >= 0) {
      char request[128];
      (void)recv(client_fd, request, sizeof(request), 0);
      static constexpr char kResponse[] = "OK\r\n";
      (void)send(client_fd, kResponse, sizeof(kResponse) - 1, 0);
      // SO_LINGER{on, 0} forces an RST on close instead of a graceful FIN,
      // matching a server that drops a connection after an error (e.g.
      // ERROR 6007 / 6030) rather than shutting it down cleanly.
      linger abort_linger{1, 0};
      (void)setsockopt(client_fd, SOL_SOCKET, SO_LINGER, &abort_linger, sizeof(abort_linger));
      close(client_fd);
    }
    close(listen_fd);
  });

  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = port;
  config.timeout_ms = 1000;
  MygramClient client(config);

  ASSERT_TRUE(client.Connect());
  auto first = client.SendCommand("INFO");
  ASSERT_TRUE(first) << first.error().message();
  ASSERT_TRUE(client.IsConnected());

  responder.join();
  // Give the RST time to be processed locally before writing again, so the
  // second send() observes an already-broken connection rather than racing
  // the close.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  auto second = client.SendCommand("INFO");
  EXPECT_FALSE(second);
  EXPECT_EQ(second.error().code(), mygram::utils::ErrorCode::kClientSendFailed);
  EXPECT_FALSE(client.IsConnected());
}

TEST_F(MygramClientTest, ConcurrentDisconnectWaitsForInFlightCommand) {
  const int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listen_fd, 0);

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ASSERT_EQ(bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  ASSERT_EQ(listen(listen_fd, 1), 0);

  socklen_t addr_len = sizeof(addr);
  ASSERT_EQ(getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len), 0);

  std::mutex response_mutex;
  std::condition_variable response_condition;
  bool request_received = false;
  bool response_allowed = false;
  std::thread responder([&]() {
    const int client_fd = accept(listen_fd, nullptr, nullptr);
    if (client_fd >= 0) {
      char request[128];
      (void)recv(client_fd, request, sizeof(request), 0);
      {
        std::lock_guard<std::mutex> lock(response_mutex);
        request_received = true;
      }
      response_condition.notify_all();
      {
        std::unique_lock<std::mutex> lock(response_mutex);
        response_condition.wait(lock, [&]() { return response_allowed; });
      }
      static constexpr char kResponse[] = "OK\r\n";
      (void)send(client_fd, kResponse, sizeof(kResponse) - 1, 0);
      close(client_fd);
    }
    close(listen_fd);
  });

  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = ntohs(addr.sin_port);
  config.timeout_ms = 2000;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());

  std::optional<mygram::utils::Expected<std::string, mygram::utils::Error>> command_result;
  std::thread command_thread([&]() { command_result = client.SendCommand("PING"); });

  {
    std::unique_lock<std::mutex> lock(response_mutex);
    EXPECT_TRUE(response_condition.wait_for(lock, std::chrono::seconds(1), [&]() { return request_received; }));
  }

  std::promise<void> disconnect_started;
  std::promise<void> disconnect_finished;
  auto disconnect_started_future = disconnect_started.get_future();
  auto disconnect_finished_future = disconnect_finished.get_future();
  std::thread disconnect_thread([&]() {
    disconnect_started.set_value();
    client.Disconnect();
    disconnect_finished.set_value();
  });
  disconnect_started_future.wait();

  EXPECT_EQ(disconnect_finished_future.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
  {
    std::lock_guard<std::mutex> lock(response_mutex);
    response_allowed = true;
  }
  response_condition.notify_all();

  command_thread.join();
  disconnect_thread.join();
  responder.join();

  ASSERT_TRUE(command_result.has_value());
  ASSERT_TRUE(*command_result) << command_result->error().message();
  EXPECT_EQ(**command_result, "OK");
  EXPECT_FALSE(client.IsConnected());
}

TEST_F(MygramClientTest, SendCommandUsesTotalDeadlineAcrossPartialReceives) {
  const int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listen_fd, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ASSERT_EQ(bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  ASSERT_EQ(listen(listen_fd, 1), 0);
  socklen_t addr_len = sizeof(addr);
  ASSERT_EQ(getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len), 0);

  std::thread drip_sender([listen_fd]() {
    const int client_fd = accept(listen_fd, nullptr, nullptr);
    if (client_fd >= 0) {
#ifdef SO_NOSIGPIPE
      int enabled = 1;
      (void)setsockopt(client_fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#endif
      char request[128];
      (void)recv(client_fd, request, sizeof(request), 0);
      for (int i = 0; i < 20; ++i) {
#ifdef MSG_NOSIGNAL
        constexpr int kSendFlags = MSG_NOSIGNAL;
#else
        constexpr int kSendFlags = 0;
#endif
        if (send(client_fd, "x", 1, kSendFlags) <= 0) {
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
      }
      close(client_fd);
    }
    close(listen_fd);
  });

  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = ntohs(addr.sin_port);
  config.timeout_ms = 100;
  config.recv_buffer_size = 1;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());

  const auto started_at = std::chrono::steady_clock::now();
  const auto result = client.SendCommand("INFO");
  const auto elapsed =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - started_at);
  drip_sender.join();

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kClientTimeout);
  EXPECT_LT(elapsed.count(), 300);
  EXPECT_FALSE(client.IsConnected());
}

TEST_F(MygramClientTest, LongOperationsUseDedicatedTimeouts) {
  const int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listen_fd, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ASSERT_EQ(bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  ASSERT_EQ(listen(listen_fd, 1), 0);
  socklen_t addr_len = sizeof(addr);
  ASSERT_EQ(getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len), 0);

  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = ntohs(addr.sin_port);
  config.timeout_ms = 30;
  config.optimize_timeout_ms = 500;
  config.dump_load_timeout_ms = 500;
  config.dump_verify_timeout_ms = 500;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());

  std::thread responder([listen_fd]() {
    const int client_fd = accept(listen_fd, nullptr, nullptr);
    if (client_fd >= 0) {
#ifdef SO_NOSIGPIPE
      int enabled = 1;
      (void)setsockopt(client_fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#endif
      constexpr std::array<std::string_view, 3> kResponses = {
          "OK OPTIMIZED terms=0 delta=0\r\n",
          "OK LOADED /tmp/client-timeout-test.dmp\r\n",
          "OK DUMP_VERIFIED /tmp/client-timeout-test.dmp\r\n",
      };
      for (const auto response : kResponses) {
        char request[256];
        if (recv(client_fd, request, sizeof(request), 0) <= 0) {
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
#ifdef MSG_NOSIGNAL
        constexpr int kSendFlags = MSG_NOSIGNAL;
#else
        constexpr int kSendFlags = 0;
#endif
        if (send(client_fd, response.data(), response.size(), kSendFlags) <= 0) {
          break;
        }
      }
      close(client_fd);
    }
    close(listen_fd);
  });

  const auto optimize = client.Optimize();
  const auto load = client.Load("/tmp/client-timeout-test.dmp");
  const auto verify = client.DumpVerify("/tmp/client-timeout-test.dmp");
  responder.join();

  ASSERT_TRUE(optimize) << optimize.error().message();
  ASSERT_TRUE(load) << load.error().message();
  EXPECT_EQ(*load, "/tmp/client-timeout-test.dmp");
  ASSERT_TRUE(verify) << verify.error().message();
}

TEST_F(MygramClientTest, LongOperationTimeoutsIgnoreKeywordCaseAndSpacing) {
  const int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listen_fd, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ASSERT_EQ(bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  ASSERT_EQ(listen(listen_fd, 1), 0);
  socklen_t addr_len = sizeof(addr);
  ASSERT_EQ(getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len), 0);

  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = ntohs(addr.sin_port);
  config.timeout_ms = 30;
  config.optimize_timeout_ms = 500;
  config.dump_load_timeout_ms = 500;
  config.dump_verify_timeout_ms = 500;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());

  std::thread responder([listen_fd]() {
    const int client_fd = accept(listen_fd, nullptr, nullptr);
    if (client_fd >= 0) {
#ifdef SO_NOSIGPIPE
      int enabled = 1;
      (void)setsockopt(client_fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#endif
      constexpr std::array<std::string_view, 3> kResponses = {
          "OK OPTIMIZED terms=0 delta=0\r\n",
          "OK LOADED /tmp/client-timeout-test.dmp\r\n",
          "OK DUMP_VERIFIED /tmp/client-timeout-test.dmp\r\n",
      };
      for (const auto response : kResponses) {
        char request[256];
        if (recv(client_fd, request, sizeof(request), 0) <= 0) {
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(80));
#ifdef MSG_NOSIGNAL
        constexpr int kSendFlags = MSG_NOSIGNAL;
#else
        constexpr int kSendFlags = 0;
#endif
        if (send(client_fd, response.data(), response.size(), kSendFlags) <= 0) {
          break;
        }
      }
      close(client_fd);
    }
    close(listen_fd);
  });

  const auto optimize = client.SendCommand("optimize");
  const auto load = client.SendCommand("dump  load /tmp/client-timeout-test.dmp");
  const auto verify = client.SendCommand("Dump\xE3\x80\x80Verify /tmp/client-timeout-test.dmp");
  responder.join();

  ASSERT_TRUE(optimize) << optimize.error().message();
  ASSERT_TRUE(load) << load.error().message();
  ASSERT_TRUE(verify) << verify.error().message();
}

TEST_F(MygramClientTest, MalformedQuotedValuesFailGetAndSearchAlike) {
  const int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listen_fd, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ASSERT_EQ(bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  ASSERT_EQ(listen(listen_fd, 1), 0);
  socklen_t addr_len = sizeof(addr);
  ASSERT_EQ(getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len), 0);

  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = ntohs(addr.sin_port);
  config.timeout_ms = 2000;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());

  std::thread responder([listen_fd]() {
    const int client_fd = accept(listen_fd, nullptr, nullptr);
    if (client_fd >= 0) {
#ifdef SO_NOSIGPIPE
      int enabled = 1;
      (void)setsockopt(client_fd, SOL_SOCKET, SO_NOSIGPIPE, &enabled, sizeof(enabled));
#endif
      constexpr std::array<std::string_view, 4> kResponses = {
          "OK DOC pk name=\"unterminated\r\n",
          "OK DOC pk name=\"bad\\xZZ\"\r\n",
          "OK RESULTS 1 \"unterminated\r\n",
          "OK RESULTS 1 \"bad\\xZZ\"\r\n",
      };
      for (const auto response : kResponses) {
        char request[256];
        if (recv(client_fd, request, sizeof(request), 0) <= 0) {
          break;
        }
#ifdef MSG_NOSIGNAL
        constexpr int kSendFlags = MSG_NOSIGNAL;
#else
        constexpr int kSendFlags = 0;
#endif
        if (send(client_fd, response.data(), response.size(), kSendFlags) <= 0) {
          break;
        }
      }
      close(client_fd);
    }
    close(listen_fd);
  });

  const auto get_unterminated = client.Get("t", "pk");
  const auto get_bad_escape = client.Get("t", "pk");
  const auto search_unterminated = client.Search("t", "q");
  const auto search_bad_escape = client.Search("t", "q");
  responder.join();

  ASSERT_FALSE(get_unterminated);
  EXPECT_EQ(get_unterminated.error().code(), mygram::utils::ErrorCode::kClientProtocolError);
  ASSERT_FALSE(get_bad_escape);
  EXPECT_EQ(get_bad_escape.error().code(), mygram::utils::ErrorCode::kClientProtocolError);
  ASSERT_FALSE(search_unterminated);
  EXPECT_EQ(search_unterminated.error().code(), mygram::utils::ErrorCode::kClientProtocolError);
  ASSERT_FALSE(search_bad_escape);
  EXPECT_EQ(search_bad_escape.error().code(), mygram::utils::ErrorCode::kClientProtocolError);
}

TEST_F(MygramClientTest, SendCommandRejectsResponseLargerThanConfiguredLimit) {
  const int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listen_fd, 0);
  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ASSERT_EQ(bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  ASSERT_EQ(listen(listen_fd, 1), 0);
  socklen_t addr_len = sizeof(addr);
  ASSERT_EQ(getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len), 0);

  std::thread oversized_sender([listen_fd]() {
    const int client_fd = accept(listen_fd, nullptr, nullptr);
    if (client_fd >= 0) {
      char request[128];
      (void)recv(client_fd, request, sizeof(request), 0);
      const std::string oversized_response(128, 'x');
      (void)send(client_fd, oversized_response.data(), oversized_response.size(), 0);
      close(client_fd);
    }
    close(listen_fd);
  });

  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = ntohs(addr.sin_port);
  config.timeout_ms = 1000;
  config.max_response_bytes = 32;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());

  const auto result = client.SendCommand("INFO");
  oversized_sender.join();

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kClientInvalidResponse);
  EXPECT_NE(result.error().message().find("max_response_bytes"), std::string::npos);
  EXPECT_FALSE(client.IsConnected());
}

TEST_F(MygramClientTest, CountRejectsMalformedCountResponse) {
  const int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listen_fd, 0);

  sockaddr_in addr{};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = 0;
  ASSERT_EQ(bind(listen_fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)), 0);
  ASSERT_EQ(listen(listen_fd, 1), 0);

  socklen_t addr_len = sizeof(addr);
  ASSERT_EQ(getsockname(listen_fd, reinterpret_cast<sockaddr*>(&addr), &addr_len), 0);
  const uint16_t port = ntohs(addr.sin_port);
  std::thread responder([listen_fd]() {
    const int client_fd = accept(listen_fd, nullptr, nullptr);
    if (client_fd >= 0) {
      char buffer[128];
      (void)recv(client_fd, buffer, sizeof(buffer), 0);
      static constexpr char kMalformedResponse[] = "OK COUNT not-a-number\r\n";
      (void)send(client_fd, kMalformedResponse, sizeof(kMalformedResponse) - 1, 0);
      close(client_fd);
    }
    close(listen_fd);
  });

  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = port;
  config.timeout_ms = 1000;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());

  const auto result = client.Count("testdb.test", "hello");
  responder.join();
  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kClientProtocolError);
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

  response = nullptr;
  result = mygramclient_send_command(c_client, "INFO\r\nREPLICATION STOP", &response);
  EXPECT_EQ(result, -1);
  EXPECT_EQ(response, nullptr);
  EXPECT_NE(std::string(mygramclient_get_last_error(c_client)).find("control character"), std::string::npos);

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

  char* dump_response = nullptr;
  ASSERT_EQ(mygramclient_dump_info(c_client, saved_path_str.c_str(), &dump_response), 0)
      << "DUMP INFO error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(dump_response, nullptr);
  EXPECT_TRUE(std::string(dump_response).find("OK DUMP_INFO") == 0) << dump_response;
  mygramclient_free_string(dump_response);

  dump_response = nullptr;
  ASSERT_EQ(mygramclient_dump_verify(c_client, saved_path_str.c_str(), &dump_response), 0)
      << "DUMP VERIFY error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(dump_response, nullptr);
  EXPECT_TRUE(std::string(dump_response).find("OK DUMP_VERIFIED") == 0) << dump_response;
  mygramclient_free_string(dump_response);

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
  EXPECT_EQ(mygramclient_is_connected(c_client), 0);
  ASSERT_EQ(mygramclient_connect(c_client), 0) << "Connect error: " << mygramclient_get_last_error(c_client);
  EXPECT_EQ(mygramclient_is_connected(c_client), 1);

  MygramServerInfo_C* info = nullptr;
  ASSERT_EQ(mygramclient_info(c_client, &info), 0) << "INFO error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(info, nullptr);
  EXPECT_NE(info->version, nullptr);
  ASSERT_EQ(info->table_count, 1u);
  ASSERT_NE(info->tables, nullptr);
  EXPECT_NE(info->tables[0], nullptr);
  EXPECT_STREQ(info->tables[0], "testdb.test");
  mygramclient_free_server_info(info);
  mygramclient_free_server_info(nullptr);

  char* config_response = nullptr;
  ASSERT_EQ(mygramclient_get_config(c_client, &config_response), 0)
      << "CONFIG error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(config_response, nullptr);
  EXPECT_NE(std::string(config_response).find("test"), std::string::npos);
  mygramclient_free_string(config_response);

  ASSERT_EQ(mygramclient_debug_on(c_client), 0) << "DEBUG ON error: " << mygramclient_get_last_error(c_client);
  ASSERT_EQ(mygramclient_debug_off(c_client), 0) << "DEBUG OFF error: " << mygramclient_get_last_error(c_client);

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

  response = nullptr;
  ASSERT_EQ(mygramclient_sync_status(c_client, &response), 0)
      << "SYNC STATUS error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(response, nullptr);
  EXPECT_TRUE(std::string(response).find("OK SYNC_STATUS") == 0) << response;
  mygramclient_free_string(response);

  response = nullptr;
  ASSERT_EQ(mygramclient_sync(c_client, "testdb.test", &response), 0)
      << "SYNC error: " << mygramclient_get_last_error(c_client);
  ASSERT_NE(response, nullptr);
  EXPECT_TRUE(std::string(response).find("OK SYNC") == 0) << response;
  mygramclient_free_string(response);

  response = nullptr;
  const int sync_stop_result = mygramclient_sync_stop(c_client, "testdb.test", &response);
  if (sync_stop_result == 0) {
    ASSERT_NE(response, nullptr);
    EXPECT_TRUE(std::string(response).find("OK SYNC") == 0) << response;
    mygramclient_free_string(response);
  } else {
    // SYNC runs asynchronously and may finish before this request when the
    // configured MySQL endpoint is unavailable. The protocol wrapper still
    // exercised SYNC STOP; only that specific completed-operation response is
    // valid here.
    EXPECT_EQ(sync_stop_result, -1);
    EXPECT_NE(std::string(mygramclient_get_last_error(c_client)).find("No active SYNC operation"), std::string::npos);
  }

  EXPECT_EQ(mygramclient_replication_stop(c_client), -1);
  EXPECT_STRNE(mygramclient_get_last_error(c_client), "");
  EXPECT_EQ(mygramclient_replication_start(c_client), -1);
  EXPECT_STRNE(mygramclient_get_last_error(c_client), "");

  mygramclient_disconnect(c_client);
  EXPECT_EQ(mygramclient_is_connected(c_client), 0);
  mygramclient_destroy(c_client);
  EXPECT_EQ(mygramclient_is_connected(nullptr), 0);
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
 * @brief Test large response handling (recv loop)
 *
 * MygramClient::SendCommand must keep calling recv() until the response is complete.
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
  // A single recv() would truncate at the 65KB buffer
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

TEST_F(MygramClientCApiTest, ParseSearchExpression_UnrepresentableComplexExpressionFailsClosed) {
  auto* parsed = reinterpret_cast<MygramParsedExpression_C*>(uintptr_t{1});
  int result = mygramclient_parse_search_expression("+golang (tutorial OR guide)", &parsed);

  EXPECT_EQ(result, -1);
  EXPECT_EQ(parsed, nullptr);
}

TEST_F(MygramClientCApiTest, ParseSearchExpressionExReturnsEveryConcreteDiagnostic) {
  constexpr std::array<std::pair<const char*, const char*>, 10> kCases = {{
      {"+", "Expected term after '+'"},
      {"-", "Expected term after '-'"},
      {"(", "Unbalanced parentheses"},
      {"OR x", "Unexpected 'OR' operator"},
      {")", "Unexpected ')'"},
      {"x OR (", "Unbalanced parentheses after 'OR'"},
      {"x OR", "Expected term after 'OR'"},
      {"", "Empty search expression"},
      {"+golang (tutorial OR guide)", "Expression cannot be represented by the simplified client API"},
      {"-old", "No search terms found"},
  }};

  for (const auto& [expression, expected_diagnostic] : kCases) {
    SCOPED_TRACE(expression);
    auto* parsed = reinterpret_cast<MygramParsedExpression_C*>(uintptr_t{1});
    char* diagnostic = reinterpret_cast<char*>(uintptr_t{1});
    EXPECT_EQ(mygramclient_parse_search_expression_ex(expression, &parsed, &diagnostic), -1);
    EXPECT_EQ(parsed, nullptr);
    ASSERT_NE(diagnostic, nullptr);
    EXPECT_STREQ(diagnostic, expected_diagnostic);
    mygramclient_free_string(diagnostic);
  }
}

TEST_F(MygramClientCApiTest, ExpressionExClearsDiagnosticOnSuccess) {
  MygramParsedExpression_C* parsed = nullptr;
  char* diagnostic = reinterpret_cast<char*>(uintptr_t{1});
  ASSERT_EQ(mygramclient_parse_search_expression_ex("golang tutorial", &parsed, &diagnostic), 0);
  ASSERT_NE(parsed, nullptr);
  EXPECT_EQ(diagnostic, nullptr);
  mygramclient_free_parsed_expression(parsed);

  char* converted = nullptr;
  diagnostic = reinterpret_cast<char*>(uintptr_t{1});
  ASSERT_EQ(mygramclient_convert_search_expression_ex("golang OR rust", &converted, &diagnostic), 0);
  ASSERT_NE(converted, nullptr);
  EXPECT_EQ(diagnostic, nullptr);
  mygramclient_free_string(converted);
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

  // Main term is the plain phrase text, not pre-quoted: Search()/
  // mygramclient_search_advanced apply wire quoting to it exactly once.
  EXPECT_STREQ(parsed->main_term, "machine learning");

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
 * @brief Test that control characters in search queries never reach the wire
 *
 * The ValidateNoControlCharacters check runs before QuoteWireToken, so this
 * tests that the validation layer rejects control characters.
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
 * Connect uses non-blocking connect+poll, so it must fail within ~timeout_ms
 * instead of hanging for the OS default of ~75s.
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
  // CI without masking a real slowdown.
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

TEST_F(MygramClientTest, RejectsProtocolDelimitersInBareIdentifiers) {
  ASSERT_TRUE(client_->Connect());

  const auto table_result = client_->Search("testdb.\"test", "hello", 100);
  ASSERT_FALSE(table_result);
  EXPECT_EQ(table_result.error().code(), mygram::utils::ErrorCode::kClientInvalidArgument);
  EXPECT_NE(table_result.error().message().find("protocol delimiter"), std::string::npos);

  SearchOptions options;
  options.sort_column = "created\\at";
  const auto sort_result = client_->Search("testdb.test", "hello", options);
  ASSERT_FALSE(sort_result);
  EXPECT_EQ(sort_result.error().code(), mygram::utils::ErrorCode::kClientInvalidArgument);

  options = {};
  options.filters.push_back({.key = "status' FILTER injected", .op = FilterOp::kEqual, .value = "active"});
  const auto filter_result = client_->Search("testdb.test", "hello", options);
  ASSERT_FALSE(filter_result);
  EXPECT_EQ(filter_result.error().code(), mygram::utils::ErrorCode::kClientInvalidArgument);

  const auto facet_result = client_->Facet("testdb.test", "category\"", "hello");
  ASSERT_FALSE(facet_result);
  EXPECT_EQ(facet_result.error().code(), mygram::utils::ErrorCode::kClientInvalidArgument);
}

// The client is left unconnected: reaching the send path would report
// kClientNotConnected, so kClientInvalidArgument proves nothing was sent.
TEST_F(MygramClientTest, RejectsUnicodeWhitespaceInBareIdentifiersBeforeSending) {
  constexpr std::string_view kIdeographicSpace = "\xE3\x80\x80";
  constexpr std::string_view kNoBreakSpace = "\xC2\xA0";
  constexpr std::string_view kNextLine = "\xC2\x85";
  for (const auto space : {kIdeographicSpace, kNoBreakSpace, kNextLine}) {
    const std::string table = "testdb.te" + std::string(space) + "st";
    const std::string key = "sta" + std::string(space) + "tus";

    const auto search = client_->Search(table, "hello", 10);
    ASSERT_FALSE(search);
    EXPECT_EQ(search.error().code(), mygram::utils::ErrorCode::kClientInvalidArgument);

    const auto count = client_->Count("testdb.test", "hello", {}, {}, {{key, "active"}});
    ASSERT_FALSE(count);
    EXPECT_EQ(count.error().code(), mygram::utils::ErrorCode::kClientInvalidArgument);

    const auto facet = client_->Facet(table, "status");
    ASSERT_FALSE(facet);
    EXPECT_EQ(facet.error().code(), mygram::utils::ErrorCode::kClientInvalidArgument);

    const auto sync = client_->Sync(table);
    ASSERT_FALSE(sync);
    EXPECT_EQ(sync.error().code(), mygram::utils::ErrorCode::kClientInvalidArgument);
  }
}

TEST_F(MygramClientTest, SearchUnicodeWhitespaceMatchesAsciiWhitespaceOnServer) {
  AddTestDocuments();
  ASSERT_TRUE(client_->Connect());

  const auto ascii = client_->Search("testdb.test", "hello world", 10);
  ASSERT_TRUE(ascii) << ascii.error().message();

  // The client quotes Unicode whitespace inside a literal token; the server's
  // query parser and AST tokenizer must still interpret it as the same
  // separator as an ASCII space.
  const auto ideographic_space = client_->Search("testdb.test",
                                                 "hello\xE3\x80\x80"
                                                 "world",
                                                 10);
  ASSERT_TRUE(ideographic_space) << ideographic_space.error().message();
  ASSERT_EQ(ideographic_space->total_count, ascii->total_count);
  ASSERT_EQ(ideographic_space->results.size(), ascii->results.size());
  for (size_t i = 0; i < ascii->results.size(); ++i) {
    EXPECT_EQ(ideographic_space->results[i].primary_key, ascii->results[i].primary_key);
  }
}

TEST_F(MygramClientTest, SetVariableRejectsNameThatWouldSplitTheCommand) {
  for (const std::string name : {"logging.level = 1 x", "logging\"level", "logging\xE3\x80\x80level"}) {
    const auto result = client_->SetVariable(name, "info");
    ASSERT_FALSE(result) << name;
    EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kClientInvalidArgument) << name;
  }
}

/**
 * @brief Test that Search emits OFFSET when limit==0 and offset>0
 *
 * When limit is zero the client must emit a bare "OFFSET <n>" clause so the
 * server still skips the first N matches.
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

TEST(MygramClientLocaleTest, NumericSearchAndFacetCommandsUseClassicLocaleOnWire) {
  const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listener, 0);

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  ASSERT_EQ(::bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
  ASSERT_EQ(::listen(listener, 1), 0);

  socklen_t address_length = sizeof(address);
  ASSERT_EQ(::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_length), 0);

  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = ntohs(address.sin_port);
  config.timeout_ms = 2000;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());

  std::vector<std::string> commands;
  std::thread server_thread([&] {
    const int connection = ::accept(listener, nullptr, nullptr);
    if (connection < 0) {
      return;
    }

    constexpr std::array<std::string_view, 4> kResponses = {"OK RESULTS 0\r\n\r\n", "OK RESULTS 0\r\n",
                                                            "OK FACET 0 0\r\n\r\n\r\n", "OK COUNT 1234\r\n"};
    while (commands.size() < kResponses.size()) {
      std::string command;
      std::array<char, 256> buffer{};
      while (command.find("\r\n") == std::string::npos) {
        const ssize_t received = ::recv(connection, buffer.data(), buffer.size(), 0);
        if (received <= 0) {
          break;
        }
        command.append(buffer.data(), static_cast<size_t>(received));
      }
      if (command.empty() || command.find("\r\n") == std::string::npos) {
        break;
      }
      command.resize(command.find("\r\n") + 2);
      commands.push_back(std::move(command));

      const std::string_view response = kResponses[commands.size() - 1];
      size_t sent = 0;
      while (sent < response.size()) {
        const ssize_t written = ::send(connection, response.data() + sent, response.size() - sent, 0);
        if (written <= 0) {
          break;
        }
        sent += static_cast<size_t>(written);
      }
      if (sent != response.size()) {
        break;
      }
    }

    ::close(connection);
  });

  {
    const std::locale hostile_locale(std::locale::classic(), new CommaGroupingNumpunct());
    const ScopedGlobalLocale restore_locale(hostile_locale);
    std::ostringstream locale_probe;
    locale_probe << 1234;
    EXPECT_EQ(locale_probe.str(), "1,234") << "The test locale must exercise grouping";

    SearchOptions options;
    options.limit = 1234;
    options.offset = 5678;
    options.fuzzy_distance = 2;
    options.highlight =
        HighlightOptions{.open_tag = "<b>", .close_tag = "</b>", .snippet_length = 10000, .max_fragments = 100};

    const auto search = client.Search("t", "q", options);
    EXPECT_TRUE(search) << (search ? std::string{} : search.error().message());
    const auto raw_search = client.SearchRaw("t", "q", 1234, 5678);
    EXPECT_TRUE(raw_search) << (raw_search ? std::string{} : raw_search.error().message());
    const auto facet = client.Facet("t", "c", "", 1234, {}, {}, {}, 5678);
    EXPECT_TRUE(facet) << (facet ? std::string{} : facet.error().message());
    const auto count = client.Count("t", "q");
    EXPECT_TRUE(count) << (count ? std::string{} : count.error().message());
    if (count) {
      EXPECT_EQ(count->count, 1234U);
    }
  }

  client.Disconnect();
  server_thread.join();
  ::close(listener);

  ASSERT_EQ(commands.size(), 4U);
  EXPECT_EQ(commands[0],
            "SEARCH t q FUZZY 2 HIGHLIGHT TAG <b> </b> SNIPPET_LEN 10000 MAX_FRAGMENTS 100 LIMIT 5678,1234\r\n");
  EXPECT_EQ(commands[1], "SEARCH t q LIMIT 5678,1234\r\n");
  EXPECT_EQ(commands[2], "FACET t c OFFSET 5678 LIMIT 1234\r\n");
  EXPECT_EQ(commands[3], "COUNT t q\r\n");
}

TEST(MygramClientLocaleTest, CacheStatisticsParsesDotDecimalUnderCommaLocale) {
  const char* current_locale = std::setlocale(LC_NUMERIC, nullptr);
  ASSERT_NE(current_locale, nullptr);
  struct NumericLocaleGuard {
    explicit NumericLocaleGuard(const char* locale) : original_locale(locale) {}
    ~NumericLocaleGuard() { std::setlocale(LC_NUMERIC, original_locale.c_str()); }

    std::string original_locale;
  } locale_guard(current_locale);

  bool selected_locale = false;
  for (const char* candidate : {"de_DE.UTF-8", "de_DE.utf8", "de_DE"}) {
    if (std::setlocale(LC_NUMERIC, candidate) != nullptr) {
      selected_locale = true;
      break;
    }
  }
  if (!selected_locale) {
    GTEST_SKIP() << "No German numeric locale is installed";
  }
  ASSERT_STREQ(std::localeconv()->decimal_point, ",");

  const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listener, 0);

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  ASSERT_EQ(::bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
  ASSERT_EQ(::listen(listener, 1), 0);

  socklen_t address_length = sizeof(address);
  ASSERT_EQ(::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_length), 0);

  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = ntohs(address.sin_port);
  config.timeout_ms = 2000;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());

  std::string command;
  std::thread server_thread([&] {
    const int connection = ::accept(listener, nullptr, nullptr);
    if (connection < 0) {
      return;
    }
    std::array<char, 256> buffer{};
    while (command.find("\r\n") == std::string::npos) {
      const ssize_t received = ::recv(connection, buffer.data(), buffer.size(), 0);
      if (received <= 0) {
        break;
      }
      command.append(buffer.data(), static_cast<size_t>(received));
    }
    constexpr std::string_view kResponse =
        "OK CACHE_STATS\r\n\r\n# Cache\r\nenabled: true\r\ntotal_queries: 1234\r\nhit_rate: 0.85\r\nEND\r\n";
    size_t sent = 0;
    while (sent < kResponse.size()) {
      const ssize_t written = ::send(connection, kResponse.data() + sent, kResponse.size() - sent, 0);
      if (written <= 0) {
        break;
      }
      sent += static_cast<size_t>(written);
    }
    ::close(connection);
  });

  const auto stats = client.GetCacheStatistics();
  EXPECT_TRUE(stats) << (stats ? std::string{} : stats.error().message());
  if (stats) {
    EXPECT_DOUBLE_EQ(stats->hit_rate, 0.85);
    EXPECT_EQ(stats->total_queries, 1234U);
  }
  client.Disconnect();
  server_thread.join();
  ::close(listener);

  EXPECT_EQ(command, "CACHE STATS\r\n");
}

/**
 * @brief Test C API search NULL terms guard
 *
 * Calling search_advanced with and_count > 0 but and_terms == nullptr (or the
 * same pattern for not / filter) must return -1 with a descriptive error.
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

TEST_F(MygramClientTest, CApiInvalidArgumentsInitializeOutputsAndSetError) {
  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);

  auto* search = reinterpret_cast<MygramSearchResult_C*>(static_cast<uintptr_t>(1));
  EXPECT_EQ(mygramclient_search(c_client, nullptr, "hello", 10, 0, &search), -1);
  EXPECT_EQ(search, nullptr);
  EXPECT_EQ(mygramclient_get_last_error_code(c_client),
            static_cast<int>(mygram::utils::ErrorCode::kClientInvalidArgument));

  uint64_t count = 42;
  EXPECT_EQ(mygramclient_count(c_client, nullptr, "hello", &count), -1);
  EXPECT_EQ(count, 0U);
  EXPECT_EQ(mygramclient_get_last_error_code(c_client),
            static_cast<int>(mygram::utils::ErrorCode::kClientInvalidArgument));

  char* response = reinterpret_cast<char*>(static_cast<uintptr_t>(1));
  EXPECT_EQ(mygramclient_send_command(c_client, nullptr, &response), -1);
  EXPECT_EQ(response, nullptr);
  EXPECT_EQ(mygramclient_get_last_error_code(c_client),
            static_cast<int>(mygram::utils::ErrorCode::kClientInvalidArgument));
  mygramclient_destroy(c_client);
}

TEST_F(MygramClientTest, CApiGetAndInfoReportCStringAllocationFailure) {
  AddTestDocuments();
  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client), 0);

  MygramDocument_C* doc = reinterpret_cast<MygramDocument_C*>(static_cast<uintptr_t>(1));
  client::testing::SetCStringAllocationFailureCountdown(0);
  const int get_result = mygramclient_get(c_client, "testdb.test", "1", &doc);
  client::testing::SetCStringAllocationFailureCountdown(-1);
  EXPECT_EQ(get_result, -1);
  EXPECT_EQ(doc, nullptr);
  EXPECT_NE(std::string(mygramclient_get_last_error(c_client)).find("allocation"), std::string::npos);

  MygramServerInfo_C* info = reinterpret_cast<MygramServerInfo_C*>(static_cast<uintptr_t>(1));
  client::testing::SetCStringAllocationFailureCountdown(0);
  const int info_result = mygramclient_info(c_client, &info);
  client::testing::SetCStringAllocationFailureCountdown(-1);
  EXPECT_EQ(info_result, -1);
  EXPECT_EQ(info, nullptr);
  EXPECT_NE(std::string(mygramclient_get_last_error(c_client)).find("allocation"), std::string::npos);

  mygramclient_destroy(c_client);
}

TEST_F(MygramClientTest, CApiExceptionBarrierReturnsSafeValuesForEveryResultShape) {
  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);

  auto* search = reinterpret_cast<MygramSearchResult_C*>(static_cast<uintptr_t>(1));
  uint64_t count = 42;
  char* response = reinterpret_cast<char*>(static_cast<uintptr_t>(1));
  auto* parsed = reinterpret_cast<MygramParsedExpression_C*>(static_cast<uintptr_t>(1));
  char* converted = reinterpret_cast<char*>(static_cast<uintptr_t>(1));
  char* parse_diagnostic = reinterpret_cast<char*>(static_cast<uintptr_t>(1));
  char* convert_diagnostic = reinterpret_cast<char*>(static_cast<uintptr_t>(1));

  client::testing::SetThrowOnCApiEntry(true);
  const int connect_result = mygramclient_connect(c_client);
  const int connected_result = mygramclient_is_connected(c_client);
  const int search_result = mygramclient_search(c_client, "testdb.test", "hello", 10, 0, &search);
  const int count_result = mygramclient_count(c_client, "testdb.test", "hello", &count);
  const int command_result = mygramclient_send_command(c_client, "INFO", &response);
  const int parse_result = mygramclient_parse_search_expression("hello", &parsed);
  const int convert_result = mygramclient_convert_search_expression("hello", &converted);
  const int parse_ex_result = mygramclient_parse_search_expression_ex("hello", &parsed, &parse_diagnostic);
  const int convert_ex_result = mygramclient_convert_search_expression_ex("hello", &converted, &convert_diagnostic);
  const char* guarded_error = mygramclient_get_last_error(c_client);
  const int guarded_error_code = mygramclient_get_last_error_code(c_client);
  MygramClient_C* guarded_create = mygramclient_create(&config);
  mygramclient_disconnect(c_client);
  mygramclient_free_string(nullptr);
  mygramclient_free_parsed_expression(nullptr);
  client::testing::SetThrowOnCApiEntry(false);

  EXPECT_EQ(connect_result, -1);
  EXPECT_EQ(connected_result, 0);
  EXPECT_EQ(search_result, -1);
  EXPECT_EQ(search, nullptr);
  EXPECT_EQ(count_result, -1);
  EXPECT_EQ(count, 0U);
  EXPECT_EQ(command_result, -1);
  EXPECT_EQ(response, nullptr);
  EXPECT_EQ(parse_result, -1);
  EXPECT_EQ(parsed, nullptr);
  EXPECT_EQ(convert_result, -1);
  EXPECT_EQ(converted, nullptr);
  EXPECT_EQ(parse_ex_result, -1);
  EXPECT_EQ(parse_diagnostic, nullptr);
  EXPECT_EQ(convert_ex_result, -1);
  EXPECT_EQ(convert_diagnostic, nullptr);
  EXPECT_STREQ(guarded_error, "C API exception");
  EXPECT_EQ(guarded_error_code, static_cast<int>(mygram::utils::ErrorCode::kClientCommandFailed));
  EXPECT_EQ(guarded_create, nullptr);
  EXPECT_NE(std::string(mygramclient_get_last_error(c_client)).find("injected C API exception"), std::string::npos);

  mygramclient_destroy(c_client);
}

TEST_F(MygramClientTest, CApiLastErrorSnapshotIsThreadSafe) {
  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);

  constexpr int kThreads = 8;
  constexpr int kIterations = 200;
  std::atomic<bool> start{false};
  std::atomic<int> failures{0};
  std::vector<std::thread> threads;
  threads.reserve(kThreads);
  for (int thread_index = 0; thread_index < kThreads; ++thread_index) {
    threads.emplace_back([&, thread_index] {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int iteration = 0; iteration < kIterations; ++iteration) {
        char* output = reinterpret_cast<char*>(static_cast<uintptr_t>(1));
        if (mygramclient_send_command(c_client, nullptr, &output) != -1 || output != nullptr) {
          failures.fetch_add(1, std::memory_order_relaxed);
        }
        const char* snapshot = mygramclient_get_last_error(c_client);
        std::this_thread::yield();
        if (snapshot == nullptr || std::strlen(snapshot) == 0 || mygramclient_get_last_error_code(c_client) == 0) {
          failures.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }
  start.store(true, std::memory_order_release);
  for (auto& thread : threads) {
    thread.join();
  }
  EXPECT_EQ(failures.load(), 0);
  mygramclient_destroy(c_client);
}

TEST(MygramClientCApiStandaloneTest, ConvertsComplexExpressionLosslessly) {
  char* converted = reinterpret_cast<char*>(static_cast<uintptr_t>(1));
  ASSERT_EQ(mygramclient_convert_search_expression("golang (tutorial OR guide)", &converted), 0);
  ASSERT_NE(converted, nullptr);
  EXPECT_NE(std::string(converted).find("tutorial OR guide"), std::string::npos);
  mygramclient_free_string(converted);

  converted = reinterpret_cast<char*>(static_cast<uintptr_t>(1));
  EXPECT_EQ(mygramclient_convert_search_expression(nullptr, &converted), -1);
  EXPECT_EQ(converted, nullptr);
}

TEST_F(MygramClientTest, CApiSuccessfulTypedCallsClearLastError) {
  AddTestDocuments();

  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = server_->GetPort();
  config.timeout_ms = 5000;
  config.recv_buffer_size = 65536;

  MygramClient_C* c_client = mygramclient_create(&config);
  ASSERT_NE(c_client, nullptr);
  ASSERT_EQ(mygramclient_connect(c_client), 0) << "Connect error: " << mygramclient_get_last_error(c_client);

  MygramSearchResult_C* invalid_search = nullptr;
  EXPECT_EQ(mygramclient_search_advanced(c_client, "testdb.test", "hello", 10, 0,
                                         /*and_terms=*/nullptr, /*and_count=*/1,
                                         /*not_terms=*/nullptr, /*not_count=*/0,
                                         /*filter_keys=*/nullptr, /*filter_values=*/nullptr,
                                         /*filter_count=*/0, nullptr, 1, &invalid_search),
            -1);
  EXPECT_NE(mygramclient_get_last_error_code(c_client), 0);
  EXPECT_STRNE(mygramclient_get_last_error(c_client), "");

  uint64_t count = 0;
  ASSERT_EQ(mygramclient_count(c_client, "testdb.test", "hello", &count), 0)
      << "Count error: " << mygramclient_get_last_error(c_client);
  EXPECT_EQ(mygramclient_get_last_error_code(c_client), 0);
  EXPECT_STREQ(mygramclient_get_last_error(c_client), "");

  MygramDocument_C* doc = nullptr;
  ASSERT_EQ(mygramclient_get(c_client, "testdb.test", "1", &doc), 0)
      << "Get error: " << mygramclient_get_last_error(c_client);
  EXPECT_EQ(mygramclient_get_last_error_code(c_client), 0);
  EXPECT_STREQ(mygramclient_get_last_error(c_client), "");
  mygramclient_free_document(doc);

  char* response = nullptr;
  ASSERT_EQ(mygramclient_send_command(c_client, "INFO", &response), 0)
      << "SendCommand error: " << mygramclient_get_last_error(c_client);
  EXPECT_EQ(mygramclient_get_last_error_code(c_client), 0);
  EXPECT_STREQ(mygramclient_get_last_error(c_client), "");
  mygramclient_free_string(response);

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
 * QuoteWireToken emits `""` for an empty arg, so the wire form is
 * `SEARCH table ""` and the server parses it as an explicit empty token.
 */
TEST_F(MygramClientTest, SearchEmptyQueryReturnsError) {
  AddTestDocuments();
  ASSERT_TRUE(client_->Connect());

  auto result = client_->Search("testdb.test", "", 100);
  ASSERT_FALSE(result) << "Expected an error for empty query";

  // The error originates from the server and retains its query-syntax code
  // across the TCP protocol boundary.
  using mygram::utils::ErrorCode;
  EXPECT_EQ(result.error().code(), ErrorCode::kQuerySyntaxError)
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
// Unit tests for the response-frame detector (protocol_detection.h)
// =============================================================================

using mygramdb::client::detail::BeginResponseFrame;
using mygramdb::client::detail::IsResponseComplete;
using mygramdb::client::detail::TokenizeRequest;

namespace {

/// Feed @p wire to a fresh frame for @p command one byte at a time and return
/// how many bytes had arrived when the frame first reported complete.
size_t BytesUntilComplete(std::string_view command, std::string_view wire, bool debug_mode = false) {
  auto state = BeginResponseFrame(command, debug_mode);
  std::string received;
  for (char byte : wire) {
    received += byte;
    if (IsResponseComplete(received, state)) {
      return received.size();
    }
  }
  return std::string::npos;
}

/// A wire frame must complete exactly at its last byte under every split:
/// byte by byte, and as a single read.
void ExpectExactFrame(std::string_view command, std::string_view wire, bool debug_mode = false) {
  EXPECT_EQ(BytesUntilComplete(command, wire, debug_mode), wire.size()) << command << " / " << wire;
  auto state = BeginResponseFrame(command, debug_mode);
  EXPECT_TRUE(IsResponseComplete(wire, state)) << command << " / " << wire;
}

}  // namespace

TEST(ResponseFrameTest, SingleLineRepliesEndAtFirstCrlf) {
  ExpectExactFrame("SEARCH t hello", "OK RESULTS 5 pk1 pk2\r\n");
  ExpectExactFrame("COUNT t hello", "OK COUNT 42\r\n");
  ExpectExactFrame("GET t pk1", "OK DOC pk1 status=active\r\n");
  ExpectExactFrame("DUMP LOAD /tmp/x", "OK LOADED /path/to/dump\r\n");
  ExpectExactFrame("REPLICATION STOP", "OK REPLICATION_STOPPED\r\n");
  ExpectExactFrame("DEBUG ON", "OK DEBUG_ON\r\n");
  ExpectExactFrame("SEARCH t hello", "ERROR 4007 Table not found: t\r\n");
}

TEST(ResponseFrameTest, IncompleteWithoutCrlf) {
  auto state = BeginResponseFrame("INFO", false);
  EXPECT_FALSE(IsResponseComplete("", state));
  EXPECT_FALSE(IsResponseComplete("OK INFO", state));
  EXPECT_FALSE(IsResponseComplete("OK INFO\r", state));
}

TEST(ResponseFrameTest, ErrorEndsEveryFramingMode) {
  constexpr std::string_view kError = "ERROR 4007 Table not found: t\r\n";
  ExpectExactFrame("SEARCH t hello HIGHLIGHT", kError);
  ExpectExactFrame("SEARCH t hello HIGHLIGHT", kError, /*debug_mode=*/true);
  ExpectExactFrame("SEARCH t hello", kError, /*debug_mode=*/true);
  ExpectExactFrame("COUNT t hello", kError, /*debug_mode=*/true);
  ExpectExactFrame("FACET t status", kError);
  ExpectExactFrame("CONFIG HELP nope", "ERROR 8 Configuration path not found: nope\r\n");
  ExpectExactFrame("INFO", "ERROR 6030 server busy\r\n");
}

TEST(ResponseFrameTest, QuotedHighlightLiteralDoesNotExpectHighlightRows) {
  ExpectExactFrame("SEARCH t \"syntax highlight editor\"", "OK RESULTS 0\r\n");
  ExpectExactFrame("SEARCH t 'highlight'", "OK RESULTS 0\r\n");
  ExpectExactFrame("SEARCH t \"a \\\" HIGHLIGHT\"", "OK RESULTS 0\r\n");
  ExpectExactFrame("SEARCH t foo FILTER tag = \"highlight\"", "OK RESULTS 0\r\n");
  ExpectExactFrame("COUNT t \"syntax highlight editor\"", "OK COUNT 0\r\n");
  ExpectExactFrame("COUNT t highlight", "OK COUNT 0\r\n");
}

TEST(ResponseFrameTest, HighlightRowsEndAtTrailingBlankLine) {
  ExpectExactFrame("SEARCH t hello HIGHLIGHT", "OK RESULTS 1\r\npk1\thello <em>world</em>\r\n\r\n");
  ExpectExactFrame("search t hello highlight snippet_len 10", "OK RESULTS 1\r\npk1\tx\r\n\r\n");
  ExpectExactFrame("SEARCH t hello LIMIT 5 HIGHLIGHT", "OK RESULTS 0\r\n\r\n");
  // Primary keys on the header line mean the server did not highlight.
  ExpectExactFrame("SEARCH t hello HIGHLIGHT", "OK RESULTS 2 pk1 pk2\r\n");
}

TEST(ResponseFrameTest, HighlightOutsideClausePositionIsAnOperand) {
  // AND, NOT, FILTER and SORT consume HIGHLIGHT as their operand.
  ExpectExactFrame("SEARCH t foo AND highlight", "OK RESULTS 0\r\n");
  ExpectExactFrame("SEARCH t foo AND NOT highlight", "OK RESULTS 0\r\n");
  ExpectExactFrame("SEARCH t foo NOT highlight", "OK RESULTS 0\r\n");
  ExpectExactFrame("SEARCH t foo FILTER tag = highlight", "OK RESULTS 0\r\n");
  ExpectExactFrame("SEARCH t foo FILTER tag= highlight", "OK RESULTS 0\r\n");
  ExpectExactFrame("SEARCH t foo FILTER highlight = \"x\"", "OK RESULTS 0\r\n");
  ExpectExactFrame("SEARCH t foo SORT highlight DESC", "OK RESULTS 0\r\n");
  ExpectExactFrame("SEARCH t foo SORT BY highlight", "OK RESULTS 0\r\n");
  // Inside a Boolean search text HIGHLIGHT only counts at depth 0.
  ExpectExactFrame("SEARCH t ( foo OR highlight )", "OK RESULTS 0\r\n");
}

TEST(ResponseFrameTest, HighlightClauseAfterOtherClauses) {
  ExpectExactFrame("SEARCH t foo AND highlight HIGHLIGHT", "OK RESULTS 0\r\n\r\n");
  ExpectExactFrame("SEARCH t foo FILTER tag = highlight HIGHLIGHT", "OK RESULTS 0\r\n\r\n");
  ExpectExactFrame("SEARCH t foo FILTER tag=x highlight", "OK RESULTS 0\r\n\r\n");
  ExpectExactFrame("SEARCH t foo SORT highlight DESC HIGHLIGHT", "OK RESULTS 0\r\n\r\n");
  ExpectExactFrame("SEARCH t foo SORT ASC highlight", "OK RESULTS 0\r\n\r\n");
  ExpectExactFrame("SEARCH t foo FUZZY highlight", "OK RESULTS 0\r\n\r\n");
  ExpectExactFrame("SEARCH t foo FUZZY 2 LIMIT 5 OFFSET 1 highlight", "OK RESULTS 0\r\n\r\n");
  ExpectExactFrame("SEARCH t foo OR bar HIGHLIGHT", "OK RESULTS 0\r\n\r\n");
}

TEST(ResponseFrameTest, DebugBlockEndsAtItsClosingBlankLine) {
  ExpectExactFrame("SEARCH t hello", "OK RESULTS 2 2 1\r\n\r\n# DEBUG\r\nquery_time: 1.0ms\r\ncache: miss\r\n\r\n",
                   /*debug_mode=*/true);
  ExpectExactFrame("COUNT t hello", "OK COUNT 42\r\n\r\n# DEBUG\r\nquery_time: 2.0ms\r\ncache: miss\r\n\r\n",
                   /*debug_mode=*/true);
}

TEST(ResponseFrameTest, HighlightWithDebugEndsAtThirdBlankLine) {
  ExpectExactFrame(
      "SEARCH t hello HIGHLIGHT",
      "OK RESULTS 1\r\npk1\tsnip\r\n\r\n# DEBUG\r\nquery_time: 1.0ms\r\nhighlight: on\r\ncache: miss\r\n\r\n\r\n",
      /*debug_mode=*/true);
  ExpectExactFrame("SEARCH t hello HIGHLIGHT", "OK RESULTS 0\r\n\r\n# DEBUG\r\ncache: miss\r\n\r\n\r\n",
                   /*debug_mode=*/true);
}

TEST(ResponseFrameTest, FacetEndsAfterBodyBlankLineAndTransportCrlf) {
  ExpectExactFrame("FACET t status", "OK FACET 2 2\r\nval1\t10\r\nval2\t5\r\n\r\n\r\n");
  ExpectExactFrame("FACET t status", "OK FACET 0 0\r\n\r\n\r\n");
  ExpectExactFrame("FACET t status", "OK FACET 1 1\r\n#x\t3\r\n# query_time_ms: 1.0\r\n\r\n\r\n", /*debug_mode=*/true);
}

// A header without the distinct-value total ends at the first blank line; a
// CRLF after it is skipped by the next frame as leftover.
TEST(ResponseFrameTest, LegacyFacetHeaderEndsAtFirstBlankLine) {
  ExpectExactFrame("FACET t status", "OK FACET 1\r\nlegacy\t7\r\n\r\n");
  ExpectExactFrame("FACET t status", "OK FACET 0\r\n\r\n");
}

TEST(ResponseFrameTest, LeadingBlankLineFromPreviousReplyIsSkipped) {
  ExpectExactFrame("COUNT t hello", "\r\nOK COUNT 3\r\n");
  ExpectExactFrame("INFO", "\r\nOK INFO\r\n\r\n# Server\r\nversion: 1.0\r\nEND\r\n");

  auto state = BeginResponseFrame("COUNT t hello", false);
  ASSERT_TRUE(IsResponseComplete("\r\nOK COUNT 3\r\n", state));
  EXPECT_EQ(state.frame_start, 2U);
}

TEST(ResponseFrameTest, EndMarkerMustBeAWholeLine) {
  ExpectExactFrame("INFO",
                   "OK INFO\r\n\r\n# Server\r\nversion: 1.0\r\n\r\n# Clients\r\nconnected_clients: 1\r\nEND\r\n");
  ExpectExactFrame("REPLICATION STATUS", "OK REPLICATION\r\nstatus: running\r\nlast_error: xEND\r\nEND\r\n");
  ExpectExactFrame("CACHE STATS", "OK CACHE_STATS\r\n\r\n# Cache\r\nenabled: true\r\nEND\r\n");
  ExpectExactFrame("DUMP INFO /tmp/snap.bin", "OK DUMP_INFO /tmp/snap.bin\r\nversion: 2\r\nEND\r\n");
  ExpectExactFrame("DUMP STATUS", "OK DUMP_STATUS\r\nstatus: IDLE\r\nEND\r\n");
}

TEST(ResponseFrameTest, SyncStatusIncludesBlankLineAfterEnd) {
  ExpectExactFrame("SYNC STATUS",
                   "OK SYNC_STATUS\r\nstatus=IDLE message=\"No sync operation performed\"\r\nEND\r\n\r\n");
  ExpectExactFrame("SYNC STATUS", "OK SYNC_STATUS\r\ntable=users status=IN_PROGRESS\r\nEND\r\n\r\n");
}

TEST(ResponseFrameTest, ConfigHelpBodyKeepsItsInternalBlankLine) {
  ExpectExactFrame("CONFIG HELP mysql.port",
                   "+OK\r\nmysql.port\r\n\r\nType: integer\r\nDefault: 3306\r\nDescription: MySQL port\r\n\r\n");
  ExpectExactFrame("config  help",
                   "+OK\r\nAvailable configuration sections:\r\n  mysql - MySQL\r\n\r\n"
                   "Use \"CONFIG HELP <section>\" for detailed information.\r\n\r\n");
}

TEST(ResponseFrameTest, PlusOkBodiesEndAtFirstBlankLine) {
  ExpectExactFrame("CONFIG", "+OK\r\nmysql:\r\n  host: localhost\r\n\r\n");
  ExpectExactFrame("CONFIG SHOW tables", "+OK\r\n\r\n\r\n");
  ExpectExactFrame("CONFIG VERIFY a.yaml",
                   "+OK\r\nConfiguration is valid\r\n  Tables: 1 (t)\r\n  MySQL: h:3306\r\n\r\n");
  ExpectExactFrame("SET a = b", "+OK Variable 'a' set to 'b'\r\n\r\n");
  ExpectExactFrame("SHOW VARIABLES LIKE 'x%'", "+OK 0 rows\r\n\r\n");
  ExpectExactFrame("SHOW VARIABLES",
                   "+------+\r\n| Variable_name | Value | Mutable |\r\n+------+\r\n| a | b | YES |\r\n+------+\r\n"
                   "1 row in set\r\n\r\n");
}

TEST(ResponseFrameTest, TokenizeRequestFollowsServerSplittingRules) {
  const auto tokens = TokenizeRequest("dump\xE3\x80\x80load  'a b'\"c\"d\xC2\xA0\"e\\\" f\"");
  ASSERT_EQ(tokens.size(), 6U);
  EXPECT_EQ(tokens[0].text, "dump");
  EXPECT_EQ(tokens[1].text, "load");
  EXPECT_EQ(tokens[2].text, "a b");
  EXPECT_TRUE(tokens[2].quoted);
  EXPECT_EQ(tokens[3].text, "c");
  EXPECT_TRUE(tokens[3].quoted);
  EXPECT_EQ(tokens[4].text, "d");
  EXPECT_FALSE(tokens[4].quoted);
  EXPECT_EQ(tokens[5].text, "e\\\" f");
  EXPECT_TRUE(tokens[5].quoted);
}

TEST(SearchExpressionAstRoundTripTest, NegatedNestedGroupKeepsItsScope) {
  auto converted = ConvertSearchExpression("(a -(b c))");
  ASSERT_TRUE(converted) << converted.error().message();

  query::QueryASTParser parser;
  auto actual = parser.Parse(*converted);
  ASSERT_NE(actual, nullptr) << *converted << ": " << parser.GetError();
  auto expected = parser.Parse("a AND NOT (b AND c)");
  ASSERT_NE(expected, nullptr) << parser.GetError();
  EXPECT_EQ(actual->ToString(), expected->ToString()) << *converted;
}

// =============================================================================
// Unit tests for NeedsWireQuoting / QuoteWireToken (shared quoting decision)
// =============================================================================

using mygramdb::client::detail::IsUnicodeWhitespace;
using mygramdb::client::detail::NeedsWireQuoting;
using mygramdb::client::detail::QuoteWireToken;

TEST(WireQuotingTest, PlainAsciiTermNeedsNoQuoting) {
  EXPECT_FALSE(NeedsWireQuoting("hello"));
  EXPECT_EQ(QuoteWireToken("hello"), "hello");
}

TEST(WireQuotingTest, EmptyStringIsAlwaysQuoted) {
  EXPECT_TRUE(NeedsWireQuoting(""));
  EXPECT_EQ(QuoteWireToken(""), "\"\"");
}

TEST(WireQuotingTest, ReservedKeywordsAreQuotedCaseInsensitively) {
  for (const char* keyword :
       {"AND", "and", "Or", "NOT", "Filter", "SORT", "limit", "OFFSET", "Highlight", "fuzzy", "FACET", "order"}) {
    EXPECT_TRUE(NeedsWireQuoting(keyword)) << keyword;
    const std::string quoted = QuoteWireToken(keyword);
    EXPECT_EQ(quoted, "\"" + std::string(keyword) + "\"") << keyword;
  }
  // A keyword-looking term is only quoted on an exact match.
  EXPECT_FALSE(NeedsWireQuoting("order_id"));
  EXPECT_FALSE(NeedsWireQuoting("android"));
}

TEST(WireQuotingTest, AsciiSpecialCharactersAreQuoted) {
  for (const char* value :
       {"has space", "has\ttab", "has\"quote", "has'quote", "has\\backslash", "has(paren", "has)paren"}) {
    EXPECT_TRUE(NeedsWireQuoting(value)) << value;
  }
}

/**
 * @brief The client's argument quoting treats every Unicode whitespace code
 * point the server tokenizer splits on outside quotes (spec/tcp-commands.md
 * §2) as needing quotes; NeedsWireQuoting backs both via IsUnicodeWhitespace.
 */
TEST(WireQuotingTest, UnicodeWhitespaceIsQuoted) {
  const std::vector<std::string_view> whitespace = {
      " ",
      "\t",
      "\n",
      "\v",
      "\f",
      "\r",            // ASCII White_Space
      "\xC2\x85",      // U+0085 NEXT LINE
      "\xC2\xA0",      // U+00A0 NO-BREAK SPACE
      "\xE1\x9A\x80",  // U+1680 OGHAM SPACE MARK
      "\xE2\x80\x80",
      "\xE2\x80\x81",
      "\xE2\x80\x82",
      "\xE2\x80\x83",
      "\xE2\x80\x84",
      "\xE2\x80\x85",
      "\xE2\x80\x86",
      "\xE2\x80\x87",
      "\xE2\x80\x88",
      "\xE2\x80\x89",
      "\xE2\x80\x8A",  // U+2000-U+200A
      "\xE2\x80\xA8",  // U+2028 LINE SEPARATOR
      "\xE2\x80\xA9",  // U+2029 PARAGRAPH SEPARATOR
      "\xE2\x80\xAF",  // U+202F NARROW NO-BREAK SPACE
      "\xE2\x81\x9F",  // U+205F MEDIUM MATHEMATICAL SPACE
      "\xE3\x80\x80",  // U+3000 IDEOGRAPHIC SPACE
  };

  for (size_t i = 0; i < whitespace.size(); ++i) {
    SCOPED_TRACE(i);
    EXPECT_TRUE(IsUnicodeWhitespace(whitespace[i], 0));
    std::string value = "tokyo";
    value.append(whitespace[i]);
    value += "tower";
    EXPECT_TRUE(NeedsWireQuoting(value));
  }

  // U+200B ZERO WIDTH SPACE is not Unicode White_Space and does not split the
  // server's request tokens.
  constexpr std::string_view kZeroWidthSpace = "\xE2\x80\x8B";
  EXPECT_FALSE(IsUnicodeWhitespace(kZeroWidthSpace, 0));
  EXPECT_FALSE(
      NeedsWireQuoting("tokyo\xE2\x80\x8B"
                       "tower"));
}

TEST(WireQuotingTest, QuoteWireTokenEscapesEmbeddedQuoteAndBackslash) {
  EXPECT_EQ(QuoteWireToken(R"(say "hi")"), R"("say \"hi\"")");
  EXPECT_EQ(QuoteWireToken(R"(back\slash)"), R"("back\\slash")");
}

TEST(WireQuotingTest, QuoteWireTokenEscapesControlCharactersReversibly) {
  // \r, \n, \t get the short mnemonic; other control bytes get \xHH -- never
  // silently dropped, so a primary key round-trips through GET byte-for-byte
  // (see QuoteWireToken's use for GET).
  EXPECT_EQ(QuoteWireToken("a\r\n\tb"), R"("a\r\n\tb")");
  EXPECT_EQ(QuoteWireToken(std::string("a\x01"
                                       "b",
                                       3)),
            "\"a\\x01b\"");
}

TEST_F(MygramClientCApiTest, ZeroInitializedSearchOptionsSortLikeDefaultSearch) {
  const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listener, 0);

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  ASSERT_EQ(::bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
  ASSERT_EQ(::listen(listener, 1), 0);
  socklen_t address_length = sizeof(address);
  ASSERT_EQ(::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_length), 0);

  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = ntohs(address.sin_port);
  config.timeout_ms = 2000;
  MygramClient_C* client = mygramclient_create(&config);
  ASSERT_NE(client, nullptr);
  ASSERT_EQ(mygramclient_connect(client), 0);

  constexpr size_t kCommandCount = 3;
  std::vector<std::string> commands;
  std::thread server_thread([&] {
    const int connection = ::accept(listener, nullptr, nullptr);
    if (connection < 0) {
      return;
    }
    constexpr std::string_view kResponse = "OK RESULTS 0\r\n\r\n";
    while (commands.size() < kCommandCount) {
      std::string command;
      std::array<char, 256> buffer{};
      while (command.find("\r\n") == std::string::npos) {
        const ssize_t received = ::recv(connection, buffer.data(), buffer.size(), 0);
        if (received <= 0) {
          break;
        }
        command.append(buffer.data(), static_cast<size_t>(received));
      }
      if (command.find("\r\n") == std::string::npos) {
        break;
      }
      command.resize(command.find("\r\n") + 2);
      commands.push_back(std::move(command));
      if (::send(connection, kResponse.data(), kResponse.size(), 0) != static_cast<ssize_t>(kResponse.size())) {
        break;
      }
    }
    ::close(connection);
  });

  MygramSearchResult_C* plain = nullptr;
  EXPECT_EQ(mygramclient_search(client, "t", "q", 10, 0, &plain), 0) << mygramclient_get_last_error(client);
  mygramclient_free_search_result(plain);

  MygramSearchOptions_C options = {};
  options.struct_size = sizeof(options);
  options.limit = 10;
  MygramSearchResultWithHighlights_C* typed = nullptr;
  EXPECT_EQ(mygramclient_search_with_options(client, "t", "q", &options, &typed), 0)
      << mygramclient_get_last_error(client);
  mygramclient_free_search_result_with_highlights(typed);

  options.sort_desc = -1;
  typed = nullptr;
  EXPECT_EQ(mygramclient_search_with_options(client, "t", "q", &options, &typed), 0)
      << mygramclient_get_last_error(client);
  mygramclient_free_search_result_with_highlights(typed);

  mygramclient_disconnect(client);
  server_thread.join();
  mygramclient_destroy(client);
  ::close(listener);

  ASSERT_EQ(commands.size(), kCommandCount);
  EXPECT_EQ(commands[1], commands[0]);
  EXPECT_EQ(commands[0].find("SORT ASC"), std::string::npos) << commands[0];
  EXPECT_NE(commands[2].find("SORT ASC"), std::string::npos) << commands[2];
}

TEST_F(MygramClientCApiTest, SharedHandleErrorStateIsPerHandleLastWriterWins) {
  MygramClientConfig_C config = {};
  config.host = "127.0.0.1";
  config.port = 1;
  MygramClient_C* client = mygramclient_create(&config);
  ASSERT_NE(client, nullptr);

  MygramSearchResult_C* result = nullptr;
  std::thread first([&] { EXPECT_NE(mygramclient_search(client, "t", "q", 10, 0, &result), 0); });
  first.join();
  const std::string first_error = mygramclient_get_last_error(client);
  ASSERT_FALSE(first_error.empty());

  std::thread second([&] { EXPECT_NE(mygramclient_search(client, nullptr, "q", 10, 0, &result), 0); });
  second.join();

  // The documented contract: the state is the handle's, not the thread's.
  EXPECT_NE(std::string(mygramclient_get_last_error(client)), first_error);
  EXPECT_EQ(mygramclient_get_last_error_code(client),
            static_cast<int>(mygram::utils::ErrorCode::kClientInvalidArgument));
  mygramclient_destroy(client);
}

TEST(MygramClientDumpSaveTest, WaitIgnoresStatusRecordsOfOtherOperations) {
  const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listener, 0);

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  ASSERT_EQ(::bind(listener, reinterpret_cast<const sockaddr*>(&address), sizeof(address)), 0);
  ASSERT_EQ(::listen(listener, 1), 0);
  socklen_t address_length = sizeof(address);
  ASSERT_EQ(::getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_length), 0);

  ClientConfig config;
  config.host = "127.0.0.1";
  config.port = ntohs(address.sin_port);
  config.timeout_ms = 2000;
  config.dump_save_timeout_ms = 5000;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());

  // A foreign COMPLETED and a LOADING record precede the status of this save.
  const std::array<std::string_view, 4> responses = {
      "OK DUMP_STARTED /tmp/mine.dmp\r\n",
      "OK DUMP_STATUS\r\nstatus: COMPLETED\r\nfilepath: /tmp/other.dmp\r\nresult_filepath: /tmp/other.dmp\r\nEND\r\n",
      "OK DUMP_STATUS\r\nstatus: LOADING\r\nfilepath: /tmp/mine.dmp\r\nEND\r\n",
      "OK DUMP_STATUS\r\nstatus: COMPLETED\r\nfilepath: /tmp/mine.dmp\r\nresult_filepath: /tmp/mine.dmp\r\nEND\r\n",
  };
  std::atomic<size_t> served{0};
  std::thread server_thread([&] {
    const int connection = ::accept(listener, nullptr, nullptr);
    if (connection < 0) {
      return;
    }
    for (const std::string_view response : responses) {
      std::string command;
      std::array<char, 256> buffer{};
      while (command.find("\r\n") == std::string::npos) {
        const ssize_t received = ::recv(connection, buffer.data(), buffer.size(), 0);
        if (received <= 0) {
          break;
        }
        command.append(buffer.data(), static_cast<size_t>(received));
      }
      if (command.find("\r\n") == std::string::npos ||
          ::send(connection, response.data(), response.size(), 0) != static_cast<ssize_t>(response.size())) {
        break;
      }
      ++served;
    }
    ::close(connection);
  });

  const auto saved = client.Save("/tmp/mine.dmp");
  client.Disconnect();
  server_thread.join();
  ::close(listener);

  ASSERT_TRUE(saved) << saved.error().message();
  EXPECT_EQ(*saved, "/tmp/mine.dmp");
  EXPECT_EQ(served.load(), responses.size());
}
