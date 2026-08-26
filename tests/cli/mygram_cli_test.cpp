/**
 * @file mygram_cli_test.cpp
 * @brief Tests for mygram-cli helpers, response parsing, and configuration.
 *
 * The CLI source is monolithic (single file with an anonymous namespace and
 * main()). To exercise the internal helpers we include the source file
 * directly with main() renamed to avoid linker conflicts. Formatting and
 * exit-status helpers are exercised through MygramClient's public API.
 */

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <array>
#include <atomic>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

// Rename main() to avoid clashing with gtest's main.
#define main cli_main
#include "cli/mygram-cli.cpp"  // NOLINT(bugprone-suspicious-include)
#undef main

namespace {

/**
 * @brief RAII helper to capture stdout into a string.
 */
class StdoutCapture {
 public:
  StdoutCapture() { old_buf_ = std::cout.rdbuf(capture_.rdbuf()); }
  ~StdoutCapture() { std::cout.rdbuf(old_buf_); }

  StdoutCapture(const StdoutCapture&) = delete;
  StdoutCapture& operator=(const StdoutCapture&) = delete;
  StdoutCapture(StdoutCapture&&) = delete;
  StdoutCapture& operator=(StdoutCapture&&) = delete;

  [[nodiscard]] std::string GetOutput() const { return capture_.str(); }

 private:
  std::ostringstream capture_;
  std::streambuf* old_buf_{nullptr};
};

class StderrCapture {
 public:
  StderrCapture() { old_buf_ = std::cerr.rdbuf(capture_.rdbuf()); }
  ~StderrCapture() { std::cerr.rdbuf(old_buf_); }

  StderrCapture(const StderrCapture&) = delete;
  StderrCapture& operator=(const StderrCapture&) = delete;
  StderrCapture(StderrCapture&&) = delete;
  StderrCapture& operator=(StderrCapture&&) = delete;

  [[nodiscard]] std::string GetOutput() const { return capture_.str(); }

 private:
  std::ostringstream capture_;
  std::streambuf* old_buf_{nullptr};
};

class StdinRedirect {
 public:
  explicit StdinRedirect(std::string input) : input_(std::move(input)) { old_buf_ = std::cin.rdbuf(input_.rdbuf()); }
  ~StdinRedirect() { std::cin.rdbuf(old_buf_); }

  StdinRedirect(const StdinRedirect&) = delete;
  StdinRedirect& operator=(const StdinRedirect&) = delete;
  StdinRedirect(StdinRedirect&&) = delete;
  StdinRedirect& operator=(StdinRedirect&&) = delete;

 private:
  std::istringstream input_;
  std::streambuf* old_buf_{nullptr};
};

// =============================================================================
// Config defaults
// =============================================================================

class CliConfigTest : public ::testing::Test {};

TEST_F(CliConfigTest, DefaultValues) {
  Config config;
  EXPECT_EQ(config.host, "127.0.0.1");
  EXPECT_EQ(config.port, 11016);
  EXPECT_TRUE(config.interactive);
  EXPECT_FALSE(config.wait_ready);
  EXPECT_EQ(config.retry_count, 0);
  EXPECT_EQ(config.retry_interval, 3);
  EXPECT_FALSE(config.retry_count_explicit);
  EXPECT_EQ(config.timeout_ms, kInteractiveTimeoutMs);
  EXPECT_EQ(config.connect_timeout_ms, kInteractiveTimeoutMs);
  EXPECT_EQ(config.dump_save_timeout_ms, kLongOperationTimeoutMs);
  EXPECT_EQ(config.dump_load_timeout_ms, kLongOperationTimeoutMs);
  EXPECT_EQ(config.dump_verify_timeout_ms, kLongOperationTimeoutMs);
  EXPECT_EQ(config.optimize_timeout_ms, kLongOperationTimeoutMs);
  EXPECT_TRUE(config.socket_path.empty());
}

TEST_F(CliConfigTest, SocketPathOverride) {
  Config config;
  config.socket_path = "/tmp/mygramdb.sock";
  EXPECT_EQ(config.socket_path, "/tmp/mygramdb.sock");
}

TEST_F(CliConfigTest, WaitReadyRetrySetsMaxRetries) {
  Config config;
  config.wait_ready = true;
  config.retry_count = kMaxWaitReadyRetries;
  EXPECT_TRUE(config.wait_ready);
  EXPECT_EQ(config.retry_count, 100);
}

TEST_F(CliConfigTest, InteractiveBannerAndPromptUseStderr) {
  Config config;
  MygramClient client(config);
  StdinRedirect input("exit\n");
  StdoutCapture stdout_capture;
  StderrCapture stderr_capture;

  client.RunInteractive();

  EXPECT_TRUE(stdout_capture.GetOutput().empty());
  const std::string diagnostics = stderr_capture.GetOutput();
  EXPECT_NE(diagnostics.find("mygram-cli 127.0.0.1:11016"), std::string::npos);
  EXPECT_NE(diagnostics.find("127.0.0.1:11016>"), std::string::npos);
  EXPECT_NE(diagnostics.find("Bye!"), std::string::npos);
}

// =============================================================================
// Constants
// =============================================================================

class CliConstantsTest : public ::testing::Test {};

TEST_F(CliConstantsTest, BufferSizeIs64KB) {
  EXPECT_EQ(kReceiveBufferSize, 65536U);
}

TEST_F(CliConstantsTest, PrefixLengthsMatchProtocol) {
  // Sanity: each prefix offset corresponds to the documented response prefix.
  EXPECT_EQ(kErrorPrefixLength, std::string("ERROR ").length());
  EXPECT_EQ(kOkSavedPrefixLength, std::string("OK SAVED ").length());
  EXPECT_EQ(kOkLoadedPrefixLength, std::string("OK LOADED ").length());
}

TEST_F(CliConstantsTest, MaxWaitReadyRetries) {
  EXPECT_EQ(kMaxWaitReadyRetries, 100);
}

TEST_F(CliConstantsTest, WaitReadyRetryableResponses) {
  struct TestCase {
    const char* response;
    bool expected;
  };
  constexpr std::array<TestCase, 11> cases = {{
      {"ERROR 6028 Server is loading, please try again later", true},
      {"ERROR 6029 Replication is not running", true},
      {"ERROR 4000 Server is loading", false},
      {"ERROR 7005 Server-defined error must not look like a local timeout", false},
      {"ERROR Server is loading, please try again later", true},
      {"(error) status not_ready", true},
      {"ERROR Replication is not running", true},
      {"(error) SERVER_DISCONNECTED: Server closed", true},
      {"(error) SERVER_TIMEOUT: Server did not respond", true},
      {"OK INFO\r\nstatus: ready\r\nEND", false},
      {"OK RESULTS 1 SERVER_DISCONNECTED SERVER_TIMEOUT NOT_READY", false},
  }};
  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.response);
    EXPECT_EQ(MygramClient::IsWaitReadyRetryableResponse(test_case.response), test_case.expected);
  }
}

TEST_F(CliConstantsTest, ConnectionFailureClassificationUsesErrorCodes) {
  using mygram::utils::ErrorCode;
  constexpr std::array<std::pair<ErrorCode, bool>, 8> cases = {{
      {ErrorCode::kClientNotConnected, true},
      {ErrorCode::kClientConnectionFailed, true},
      {ErrorCode::kClientSendFailed, true},
      {ErrorCode::kClientReceiveFailed, true},
      {ErrorCode::kClientTimeout, true},
      {ErrorCode::kClientConnectionClosed, true},
      {ErrorCode::kClientCommandFailed, false},
      {ErrorCode::kIndexNotFound, false},
  }};
  for (const auto& [code, expected] : cases) {
    SCOPED_TRACE(static_cast<int>(code));
    EXPECT_EQ(MygramClient::IsConnectionFailureCode(code), expected);
  }
}

TEST_F(CliConstantsTest, WaitReadyReconnectsAfterDroppedConnection) {
  int listener = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listener, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  ASSERT_EQ(bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  ASSERT_EQ(listen(listener, 2), 0);
  socklen_t address_size = sizeof(address);
  ASSERT_EQ(getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_size), 0);

  std::atomic<int> served{0};
  std::thread server([&] {
    for (int connection_index = 0; connection_index < 2; ++connection_index) {
      int connection = accept(listener, nullptr, nullptr);
      if (connection < 0) {
        return;
      }
      std::array<char, 256> request{};
      (void)recv(connection, request.data(), request.size(), 0);
      if (connection_index == 1) {
        constexpr std::string_view response = "OK INFO\r\nstatus: ready\r\nEND\r\n";
        (void)send(connection, response.data(), response.size(), 0);
      }
      close(connection);
      served.fetch_add(1);
    }
  });

  Config config;
  config.host = "127.0.0.1";
  config.port = ntohs(address.sin_port);
  config.wait_ready = true;
  config.retry_count = 1;
  config.retry_interval = 0;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());
  EXPECT_EQ(client.RunSingleCommand("INFO"), 0);

  server.join();
  close(listener);
  EXPECT_EQ(served.load(), 2);
}

TEST_F(CliConstantsTest, WaitReadyRetriesNameResolutionFailures) {
  Config config;
  config.host = "invalid.invalid";
  config.wait_ready = true;
  config.retry_count = 2;
  config.retry_interval = 0;
  MygramClient client(config);
  StderrCapture stderr_capture;

  EXPECT_FALSE(client.Connect());

  const std::string diagnostics = stderr_capture.GetOutput();
  size_t retry_messages = 0;
  size_t offset = 0;
  constexpr std::string_view kRetryMessage = "Retrying in";
  while ((offset = diagnostics.find(kRetryMessage, offset)) != std::string::npos) {
    ++retry_messages;
    offset += kRetryMessage.size();
  }
  EXPECT_EQ(retry_messages, 2U);
  EXPECT_NE(diagnostics.find("Hostname resolution failed"), std::string::npos);
}

// =============================================================================
// String helpers
// =============================================================================

class CliStringHelperTest : public ::testing::Test {};

TEST_F(CliStringHelperTest, ToUpperBasic) {
  EXPECT_EQ(ToUpper("search"), "SEARCH");
  EXPECT_EQ(ToUpper("Search"), "SEARCH");
  EXPECT_EQ(ToUpper("SEARCH"), "SEARCH");
  EXPECT_EQ(ToUpper(""), "");
}

TEST_F(CliStringHelperTest, TrimWhitespace) {
  EXPECT_EQ(TrimAsciiWhitespace("  hello  "), "hello");
  EXPECT_EQ(TrimAsciiWhitespace("\t\nhello\r\n"), "hello");
  EXPECT_EQ(TrimAsciiWhitespace("hello"), "hello");
  EXPECT_EQ(TrimAsciiWhitespace("   "), "");
  EXPECT_EQ(TrimAsciiWhitespace(""), "");
  EXPECT_EQ(TrimAsciiWhitespace("hello world"), "hello world");
}

TEST_F(CliStringHelperTest, NormalizeCrlfReplacesAllOccurrences) {
  EXPECT_EQ(NormalizeCrlf("a\r\nb\r\nc"), "a\nb\nc");
  EXPECT_EQ(NormalizeCrlf("\r\n"), "\n");
  EXPECT_EQ(NormalizeCrlf(""), "");
  EXPECT_EQ(NormalizeCrlf("no crlf here"), "no crlf here");
  // Lone \r or \n untouched
  EXPECT_EQ(NormalizeCrlf("a\rb\nc"), "a\rb\nc");
}

TEST_F(CliStringHelperTest, StripTrailingEndMarker) {
  EXPECT_EQ(StripTrailingEndMarker("foo\nEND"), "foo");
  EXPECT_EQ(StripTrailingEndMarker("foo\nEND\n"), "foo");
  EXPECT_EQ(StripTrailingEndMarker("foo\nEND\r\n"), "foo");
  EXPECT_EQ(StripTrailingEndMarker("END"), "");
  // Don't strip "END" mid-line / not at line start
  EXPECT_EQ(StripTrailingEndMarker("FRIEND"), "FRIEND");
  EXPECT_EQ(StripTrailingEndMarker("FRIEND\n"), "FRIEND");
  // Empty / no-op
  EXPECT_EQ(StripTrailingEndMarker(""), "");
  EXPECT_EQ(StripTrailingEndMarker("no marker"), "no marker");
}

TEST_F(CliStringHelperTest, QuoteArgIfNeeded) {
  EXPECT_EQ(QuoteArgIfNeeded("plain"), "plain");
  EXPECT_EQ(QuoteArgIfNeeded("with space"), "\"with space\"");
  EXPECT_EQ(QuoteArgIfNeeded("with\"quote"), "\"with\\\"quote\"");
  EXPECT_EQ(QuoteArgIfNeeded("with\\backslash"), "\"with\\\\backslash\"");
  EXPECT_EQ(QuoteArgIfNeeded(""), "\"\"");
  // Control characters get stripped (consistent with client lib's
  // EscapeQueryString — prevents protocol injection via embedded \r\n).
  EXPECT_EQ(QuoteArgIfNeeded("a\x01"
                             "b"),
            "\"ab\"");
  EXPECT_EQ(QuoteArgIfNeeded("with\ttab"), "\"withtab\"");
  EXPECT_EQ(QuoteArgIfNeeded("with\nnewline"), "\"withnewline\"");
}

TEST_F(CliStringHelperTest, JoinArgsForCommand) {
  EXPECT_EQ(JoinArgsForCommand({"SEARCH", "articles", "hello"}), "SEARCH articles hello");
  EXPECT_EQ(JoinArgsForCommand({"SEARCH", "articles", "hello world"}), "SEARCH articles hello world");
  EXPECT_EQ(JoinArgsForCommand({"SEARCH", "articles", "(golang OR python)", "AND", "tutorial"}),
            "SEARCH articles (golang OR python) AND tutorial");
  EXPECT_EQ(JoinArgsForCommand({"SEARCH", "articles", "\"hello world\""}), "SEARCH articles \"hello world\"");
  EXPECT_EQ(JoinArgsForCommand({"INFO\r\nSHUTDOWN"}), "INFOSHUTDOWN");
  EXPECT_EQ(JoinArgsForCommand({}), "");
  EXPECT_EQ(JoinArgsForCommand({"INFO"}), "INFO");
}

TEST_F(CliStringHelperTest, ParsesDumpSaveFilepath) {
  ASSERT_TRUE(ParseDumpSaveFilepath("DUMP SAVE").has_value());
  EXPECT_TRUE(ParseDumpSaveFilepath("DUMP SAVE")->empty());
  EXPECT_EQ(ParseDumpSaveFilepath("dump save /tmp/a b.dmp"), "/tmp/a b.dmp");
  EXPECT_EQ(ParseDumpSaveFilepath(R"(DUMP SAVE "/tmp/a b.dmp")"), "/tmp/a b.dmp");
  EXPECT_EQ(ParseDumpSaveFilepath(R"(DUMP SAVE '/tmp/a b.dmp')"), "/tmp/a b.dmp");
  EXPECT_FALSE(ParseDumpSaveFilepath("DUMP STATUS").has_value());
  EXPECT_FALSE(ParseDumpSaveFilepath("DUMP SAVER /tmp/a.dmp").has_value());
}

TEST_F(CliStringHelperTest, HelpMatchesImplementedRuntimeSyntax) {
  StdoutCapture capture;
  MygramClient::PrintHelp();

  std::string output = capture.GetOutput();
  EXPECT_NE(output.find("SET <variable> = <value>"), std::string::npos);
  EXPECT_NE(output.find("SHOW VARIABLES [LIKE <pattern>]"), std::string::npos);
  EXPECT_NE(output.find("SEARCH <db.table> <text>"), std::string::npos);
  EXPECT_NE(output.find("COUNT <db.table> <text>"), std::string::npos);
  EXPECT_NE(output.find("GET <db.table> <primary_key>"), std::string::npos);
  EXPECT_NE(output.find("SYNC <db.table>|STOP [db.table]|STATUS"), std::string::npos);
  EXPECT_NE(output.find("FACET <db.table> <column> [text]"), std::string::npos);
  EXPECT_EQ(output.find("SYNC START"), std::string::npos);
  EXPECT_EQ(output.find("FACET <db.table> <column> [WHERE"), std::string::npos);
}

TEST_F(CliStringHelperTest, HelpListsEverySearchClauseTheServerAccepts) {
  StdoutCapture capture;
  MygramClient::PrintHelp();

  // Discovering the protocol through the CLI must not make a clause the parser
  // accepts look like an HTTP-only feature.
  const std::string output = capture.GetOutput();
  for (const auto* clause : {"AND", "OR", "NOT", "FILTER", "SORT", "LIMIT", "OFFSET", "HIGHLIGHT", "FUZZY"}) {
    EXPECT_NE(output.find(clause), std::string::npos) << "SEARCH clause missing from help: " << clause;
  }
}

// =============================================================================
// Port parsing (closes the gap where the old code silently truncated)
// =============================================================================

class CliPortParsingTest : public ::testing::Test {};

TEST_F(CliPortParsingTest, ParsesValidPorts) {
  EXPECT_TRUE(ParsePort("1").ok);
  EXPECT_EQ(ParsePort("1").port, 1);
  EXPECT_TRUE(ParsePort("11016").ok);
  EXPECT_EQ(ParsePort("11016").port, 11016);
  EXPECT_TRUE(ParsePort("65535").ok);
  EXPECT_EQ(ParsePort("65535").port, 65535);
}

TEST_F(CliPortParsingTest, RejectsOutOfRange) {
  EXPECT_FALSE(ParsePort("0").ok);
  EXPECT_FALSE(ParsePort("-1").ok);
  EXPECT_FALSE(ParsePort("65536").ok);
  EXPECT_FALSE(ParsePort("70000").ok);
  EXPECT_FALSE(ParsePort("99999").ok);
}

TEST_F(CliPortParsingTest, RejectsNonNumeric) {
  EXPECT_FALSE(ParsePort("").ok);
  EXPECT_FALSE(ParsePort("abc").ok);
  EXPECT_FALSE(ParsePort("80abc").ok);
  EXPECT_FALSE(ParsePort("80 ").ok);
}

TEST_F(CliPortParsingTest, ErrorMessageNonEmpty) {
  EXPECT_FALSE(ParsePort("foo").error.empty());
  EXPECT_FALSE(ParsePort("70000").error.empty());
}

// =============================================================================
// PrintResponse — SEARCH responses
// =============================================================================

class CliPrintResponseTest : public ::testing::Test {};

TEST_F(CliPrintResponseTest, SearchResponseWithResults) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK RESULTS 42 101 102 103");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("42 results"), std::string::npos);
  EXPECT_NE(output.find("showing 3"), std::string::npos);
  EXPECT_NE(output.find("1) 101"), std::string::npos);
  EXPECT_NE(output.find("2) 102"), std::string::npos);
  EXPECT_NE(output.find("3) 103"), std::string::npos);
}

TEST_F(CliPrintResponseTest, SearchResponseZeroResults) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK RESULTS 0");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("0 results"), std::string::npos);
  EXPECT_EQ(output.find("showing"), std::string::npos);
}

TEST_F(CliPrintResponseTest, SearchResponseSingleResult) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK RESULTS 1 42");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("1 results"), std::string::npos);
  EXPECT_NE(output.find("showing 1"), std::string::npos);
  EXPECT_NE(output.find("1) 42"), std::string::npos);
}

TEST_F(CliPrintResponseTest, SearchResponseWithDebugInfo) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK RESULTS 5 1 2 3 4 5\r\n\r\n# DEBUG\r\ntime: 1ms");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("5 results"), std::string::npos);
  EXPECT_NE(output.find("# DEBUG"), std::string::npos);
  EXPECT_NE(output.find("time: 1ms"), std::string::npos);
  // Debug should be normalized to LF, not contain literal "\r\n"
  EXPECT_EQ(output.find("\\r\\n"), std::string::npos);
}

TEST_F(CliPrintResponseTest, SearchResponseLargeCount) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK RESULTS 1000000 1 2 3 4 5 6 7 8 9 10");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("1000000 results"), std::string::npos);
  EXPECT_NE(output.find("showing 10"), std::string::npos);
}

TEST_F(CliPrintResponseTest, SearchResponseWithHighlights) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK RESULTS 2\r\npk1\thello <em>world</em>\r\npk2\tsecond snippet");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("2 results"), std::string::npos);
  EXPECT_NE(output.find("showing 2"), std::string::npos);
  EXPECT_NE(output.find("1) pk1"), std::string::npos);
  EXPECT_NE(output.find("hello <em>world</em>"), std::string::npos);
  EXPECT_NE(output.find("2) pk2"), std::string::npos);
  EXPECT_NE(output.find("second snippet"), std::string::npos);
  EXPECT_EQ(output.find('\r'), std::string::npos);
}

// =============================================================================
// PrintResponse — COUNT responses
// =============================================================================

TEST_F(CliPrintResponseTest, CountResponse) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK COUNT 256");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("(integer) 256"), std::string::npos);
}

TEST_F(CliPrintResponseTest, CountResponseZero) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK COUNT 0");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("(integer) 0"), std::string::npos);
}

TEST_F(CliPrintResponseTest, CountResponseWithDebugInfo) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK COUNT 100\r\n\r\n# DEBUG\r\nindex_scan: 2ms");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("(integer) 100"), std::string::npos);
  EXPECT_NE(output.find("# DEBUG"), std::string::npos);
  EXPECT_NE(output.find("index_scan: 2ms"), std::string::npos);
}

// =============================================================================
// PrintResponse — GET responses
// =============================================================================

TEST_F(CliPrintResponseTest, DocResponseStripsOkPrefix) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK DOC 42 title=hello author=world");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("DOC 42 title=hello author=world"), std::string::npos);
  EXPECT_EQ(output.find("OK "), std::string::npos);
}

TEST_F(CliPrintResponseTest, DocResponseUnescapesQuotedStringFilterValues) {
  StdoutCapture capture;
  MygramClient::PrintResponse(R"(OK DOC 42 display_name="Alice Smith" label="a \"quoted\" path\\name")");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find(R"(DOC 42 display_name=Alice Smith label=a "quoted" path\name)"), std::string::npos);
  EXPECT_EQ(output.find(R"(display_name="Alice Smith")"), std::string::npos);
  EXPECT_EQ(output.find(R"(\")"), std::string::npos);
}

TEST_F(CliPrintResponseTest, DocResponseUnescapesServerEscapeSequences) {
  StdoutCapture capture;
  MygramClient::PrintResponse(R"(OK DOC 42 text="line1\nline2" tab="a\tb" carriage="a\rb" hex="A\x21B")");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("text=line1\nline2"), std::string::npos);
  EXPECT_NE(output.find("tab=a\tb"), std::string::npos);
  EXPECT_NE(output.find("carriage=a\rb"), std::string::npos);
  EXPECT_NE(output.find("hex=A!B"), std::string::npos);
  EXPECT_EQ(output.find(R"(\x21)"), std::string::npos);
}

// =============================================================================
// PrintResponse — INFO / REPLICATION (multi-line, CRLF normalization,
// trailing-END stripping)
// =============================================================================

TEST_F(CliPrintResponseTest, InfoResponseNormalizesAndStripsEnd) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK INFO\r\n\r\n# Server\r\nversion: 1.0\r\ntables: articles\r\nEND");
  std::string output = capture.GetOutput();

  // No CRLF leaks into the visible output
  EXPECT_EQ(output.find('\r'), std::string::npos);
  // Section header / fields preserved
  EXPECT_NE(output.find("# Server"), std::string::npos);
  EXPECT_NE(output.find("version: 1.0"), std::string::npos);
  EXPECT_NE(output.find("tables: articles"), std::string::npos);
  // Trailing END marker stripped
  EXPECT_EQ(output.find("END"), std::string::npos);
}

TEST_F(CliPrintResponseTest, ReplicationStatusResponseNormalizesCrlf) {
  // This is the case that was completely broken in the old CLI: it searched
  // for literal "\\r\\n" / replaced with literal "\\n" — neither matched.
  StdoutCapture capture;
  MygramClient::PrintResponse("OK REPLICATION\r\nstatus: running\r\ncurrent_gtid: abc-def\r\nEND");
  std::string output = capture.GetOutput();

  // No raw CR characters in output, no literal "\r\n", no "\\n"
  EXPECT_EQ(output.find('\r'), std::string::npos);
  EXPECT_EQ(output.find("\\r\\n"), std::string::npos);
  EXPECT_EQ(output.find("\\n"), std::string::npos);
  // Real fields visible on their own lines
  EXPECT_NE(output.find("status: running"), std::string::npos);
  EXPECT_NE(output.find("current_gtid: abc-def"), std::string::npos);
  // Trailing END marker stripped
  EXPECT_EQ(output.find("END"), std::string::npos);
}

TEST_F(CliPrintResponseTest, ReplicationStoppedShortForm) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK REPLICATION_STOPPED");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("Replication stopped successfully"), std::string::npos);
}

TEST_F(CliPrintResponseTest, ReplicationStartedShortForm) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK REPLICATION_STARTED");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("Replication started successfully"), std::string::npos);
}

// =============================================================================
// PrintResponse — CONFIG / FACET / CACHE / SYNC / DUMP / OPTIMIZED / +OK
// (previously fell through to "Unknown response" passthrough)
// =============================================================================

// CONFIG SHOW emits "+OK\r\n<body>\r\n\r\n" (handled by the +OK branch); the
// server never emits a literal "OK CONFIG" prefix, so there is no
// dedicated PrintResponse case to test here.

TEST_F(CliPrintResponseTest, CacheStatsResponseNormalizesAndStripsEnd) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK CACHE_STATS\r\n\r\n# Cache\r\nenabled: true\r\ntotal_queries: 100\r\nEND");
  std::string output = capture.GetOutput();

  EXPECT_EQ(output.find('\r'), std::string::npos);
  EXPECT_NE(output.find("# Cache"), std::string::npos);
  EXPECT_NE(output.find("enabled: true"), std::string::npos);
  EXPECT_NE(output.find("total_queries: 100"), std::string::npos);
  EXPECT_EQ(output.find("END"), std::string::npos);
}

TEST_F(CliPrintResponseTest, CacheClearedShortForm) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK CACHE_CLEARED");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("CACHE_CLEARED"), std::string::npos);
  EXPECT_EQ(output.find("OK CACHE_CLEARED"), std::string::npos);  // "OK " stripped
}

TEST_F(CliPrintResponseTest, CacheEnabledShortForm) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK CACHE_ENABLED");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("CACHE_ENABLED"), std::string::npos);
}

TEST_F(CliPrintResponseTest, CacheDisabledShortForm) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK CACHE_DISABLED");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("CACHE_DISABLED"), std::string::npos);
}

TEST_F(CliPrintResponseTest, FacetResponseShowsValueLines) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK FACET 2\r\nhello\t10\r\nworld\t5\r\n");
  std::string output = capture.GetOutput();

  EXPECT_EQ(output.find('\r'), std::string::npos);
  // First line: "FACET 2" (with "OK " stripped)
  EXPECT_NE(output.find("FACET 2"), std::string::npos);
  EXPECT_NE(output.find("hello\t10"), std::string::npos);
  EXPECT_NE(output.find("world\t5"), std::string::npos);
}

TEST_F(CliPrintResponseTest, SyncStartedShortForm) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK SYNC STARTED table=foo");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("SYNC STARTED"), std::string::npos);
  EXPECT_NE(output.find("table=foo"), std::string::npos);
}

TEST_F(CliPrintResponseTest, SyncStatusMultiLineStripsStatusAndEnd) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK SYNC_STATUS\r\nstatus=IDLE\r\nactive_jobs=0\r\nEND");
  std::string output = capture.GetOutput();

  EXPECT_EQ(output.find('\r'), std::string::npos);
  EXPECT_EQ(output.find("OK SYNC_STATUS"), std::string::npos);
  EXPECT_EQ(output.find("END"), std::string::npos);
  EXPECT_NE(output.find("status=IDLE"), std::string::npos);
  EXPECT_NE(output.find("active_jobs=0"), std::string::npos);
}

TEST_F(CliPrintResponseTest, DumpStartedShortForm) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK DUMP_STARTED /tmp/dump.bin");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("DUMP_STARTED /tmp/dump.bin"), std::string::npos);
}

TEST_F(CliPrintResponseTest, DumpInfoMultiLine) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK DUMP_INFO /tmp/dump.bin\r\nversion: 2\r\ntables: 5\r\nfile_size: 12345\r\nEND");
  std::string output = capture.GetOutput();

  EXPECT_EQ(output.find('\r'), std::string::npos);
  EXPECT_NE(output.find("DUMP_INFO /tmp/dump.bin"), std::string::npos);
  EXPECT_NE(output.find("version: 2"), std::string::npos);
  EXPECT_NE(output.find("file_size: 12345"), std::string::npos);
  EXPECT_EQ(output.find("END"), std::string::npos);
}

TEST_F(CliPrintResponseTest, OptimizedShortForm) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK OPTIMIZED terms=100 delta=5");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("OPTIMIZED terms=100"), std::string::npos);
  EXPECT_EQ(output.find("OK OPTIMIZED"), std::string::npos);  // "OK " stripped
}

TEST_F(CliPrintResponseTest, PlusOkSimpleOk) {
  StdoutCapture capture;
  MygramClient::PrintResponse("+OK Variable 'foo' set to 'bar'");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("Variable 'foo' set to 'bar'"), std::string::npos);
  EXPECT_EQ(output.find("+OK"), std::string::npos);
}

TEST_F(CliPrintResponseTest, PlusOkBareOk) {
  StdoutCapture capture;
  MygramClient::PrintResponse("+OK");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("OK"), std::string::npos);
  EXPECT_EQ(output.find("+OK"), std::string::npos);
}

TEST_F(CliPrintResponseTest, PlusOkWithCrlfBody) {
  StdoutCapture capture;
  MygramClient::PrintResponse("+OK\r\nrow1\r\nrow2");
  std::string output = capture.GetOutput();

  EXPECT_EQ(output.find('\r'), std::string::npos);
  EXPECT_NE(output.find("row1"), std::string::npos);
  EXPECT_NE(output.find("row2"), std::string::npos);
}

// =============================================================================
// PrintResponse — DEBUG / SAVE / LOAD
// =============================================================================

TEST_F(CliPrintResponseTest, DebugOnResponse) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK DEBUG_ON");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("Debug mode enabled"), std::string::npos);
}

TEST_F(CliPrintResponseTest, DebugOffResponse) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK DEBUG_OFF");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("Debug mode disabled"), std::string::npos);
}

TEST_F(CliPrintResponseTest, SaveResponse) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK SAVED /data/snapshot.bin");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("Snapshot saved to: /data/snapshot.bin"), std::string::npos);
}

TEST_F(CliPrintResponseTest, LoadResponse) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK LOADED /data/snapshot.bin");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("Snapshot loaded from: /data/snapshot.bin"), std::string::npos);
}

// =============================================================================
// PrintResponse — ERROR / fallback
// =============================================================================

TEST_F(CliPrintResponseTest, ErrorResponse) {
  StdoutCapture stdout_capture;
  StderrCapture stderr_capture;
  MygramClient::PrintResponse("ERROR Unknown command");

  EXPECT_TRUE(stdout_capture.GetOutput().empty());
  EXPECT_NE(stderr_capture.GetOutput().find("(error) Unknown command"), std::string::npos);
}

TEST_F(CliPrintResponseTest, ErrorResponseWithCode) {
  StdoutCapture stdout_capture;
  StderrCapture stderr_capture;
  MygramClient::PrintResponse("ERROR 3001 Invalid query syntax");

  EXPECT_TRUE(stdout_capture.GetOutput().empty());
  EXPECT_NE(stderr_capture.GetOutput().find("(error) 3001 Invalid query syntax"), std::string::npos);
}

TEST_F(CliPrintResponseTest, ClientErrorGoesToStderr) {
  StdoutCapture stdout_capture;
  StderrCapture stderr_capture;
  MygramClient::PrintResponse("(error) SERVER_TIMEOUT: request timed out");

  EXPECT_TRUE(stdout_capture.GetOutput().empty());
  EXPECT_NE(stderr_capture.GetOutput().find("SERVER_TIMEOUT"), std::string::npos);
}

TEST_F(CliPrintResponseTest, UnknownResponseFallbackNormalizesCrlf) {
  StdoutCapture capture;
  MygramClient::PrintResponse("SOMETHING UNEXPECTED\r\nWITH MULTIPLE LINES");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("SOMETHING UNEXPECTED"), std::string::npos);
  EXPECT_NE(output.find("WITH MULTIPLE LINES"), std::string::npos);
  EXPECT_EQ(output.find('\r'), std::string::npos);
}

TEST_F(CliPrintResponseTest, EmptyResponseDoesNotCrash) {
  StdoutCapture capture;
  MygramClient::PrintResponse("");
  std::string output = capture.GetOutput();

  EXPECT_EQ(output, "\n");
}

// =============================================================================
// Single-command exit status
// =============================================================================

class CliSingleCommandExitStatusTest : public ::testing::Test {};

TEST_F(CliSingleCommandExitStatusTest, SuccessResponsesExitZero) {
  EXPECT_EQ(MygramClient::ExitCodeForSingleCommandResponse("OK COUNT 1"), 0);
  EXPECT_EQ(MygramClient::ExitCodeForSingleCommandResponse("+OK Variable 'foo' set to 'bar'"), 0);
}

TEST_F(CliSingleCommandExitStatusTest, ServerErrorsExitNonZero) {
  EXPECT_EQ(MygramClient::ExitCodeForSingleCommandResponse("ERROR Unknown table"), 1);
  EXPECT_EQ(MygramClient::ExitCodeForSingleCommandResponse("ERROR SERVER_BUSY Server is too busy"), 1);
}

TEST_F(CliSingleCommandExitStatusTest, ClientErrorsExitNonZero) {
  EXPECT_EQ(MygramClient::ExitCodeForSingleCommandResponse("(error) Not connected"), 1);
  EXPECT_EQ(MygramClient::ExitCodeForSingleCommandResponse("(error) SERVER_DISCONNECTED: Server closed"), 1);
  EXPECT_EQ(MygramClient::ExitCodeForSingleCommandResponse("(error) SERVER_TIMEOUT: Server did not respond"), 1);
}

TEST_F(CliSingleCommandExitStatusTest, PayloadContainingDisconnectWordsStillExitsZero) {
  EXPECT_EQ(MygramClient::ExitCodeForSingleCommandResponse("OK DOC 1 note=\"SERVER_DISCONNECTED\""), 0);
  EXPECT_EQ(MygramClient::ExitCodeForSingleCommandResponse("OK RESULTS 1 SERVER_TIMEOUT"), 0);
}

TEST_F(CliSingleCommandExitStatusTest, DumpSaveWaitsForCompletion) {
  const int listener = socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(listener, 0);
  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  ASSERT_EQ(bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)), 0);
  ASSERT_EQ(listen(listener, 1), 0);
  socklen_t address_size = sizeof(address);
  ASSERT_EQ(getsockname(listener, reinterpret_cast<sockaddr*>(&address), &address_size), 0);

  Config config;
  config.host = "127.0.0.1";
  config.port = ntohs(address.sin_port);
  config.timeout_ms = 1000;
  MygramClient client(config);
  ASSERT_TRUE(client.Connect());

  std::vector<std::string> requests;
  std::thread server([&]() {
    const int connection = accept(listener, nullptr, nullptr);
    if (connection >= 0) {
      timeval receive_timeout{};
      receive_timeout.tv_sec = 1;
      (void)setsockopt(connection, SOL_SOCKET, SO_RCVTIMEO, &receive_timeout, sizeof(receive_timeout));
      constexpr std::array<std::string_view, 2> responses = {
          "OK DUMP_STARTED /tmp/cli-dump.dmp\r\n",
          "OK DUMP_STATUS\r\nstatus: COMPLETED\r\nresult_filepath: /tmp/cli-dump.dmp\r\nEND\r\n",
      };
      for (const auto response : responses) {
        std::array<char, 256> request{};
        const auto received = recv(connection, request.data(), request.size(), 0);
        if (received <= 0) {
          break;
        }
        requests.emplace_back(request.data(), static_cast<size_t>(received));
        if (send(connection, response.data(), response.size(), 0) <= 0) {
          break;
        }
      }
      close(connection);
    }
    close(listener);
  });

  StdoutCapture capture;
  const int exit_code = client.RunSingleCommand("DUMP SAVE /tmp/cli-dump.dmp");
  server.join();

  EXPECT_EQ(exit_code, 0);
  EXPECT_NE(capture.GetOutput().find("Snapshot saved to: /tmp/cli-dump.dmp"), std::string::npos);
  ASSERT_EQ(requests.size(), 2U);
  EXPECT_EQ(requests[0], "DUMP SAVE /tmp/cli-dump.dmp\r\n");
  EXPECT_EQ(requests[1], "DUMP STATUS\r\n");
}

// =============================================================================
// Argument parsing simulation
// =============================================================================

class CliArgumentParsingTest : public ::testing::Test {};

TEST_F(CliArgumentParsingTest, NoArgsDefaultsToInteractive) {
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
  char arg0[] = "mygram-cli";
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
  char* argv[] = {arg0};

  ParseResult result = ParseArguments(1, argv);
  EXPECT_FALSE(result.exit_now);
  EXPECT_TRUE(result.config.interactive);
  EXPECT_EQ(result.config.host, "127.0.0.1");
  EXPECT_EQ(result.config.port, 11016);
  EXPECT_TRUE(result.command_args.empty());
}

TEST_F(CliArgumentParsingTest, HostAndPortFlags) {
  char arg0[] = "mygram-cli";
  char arg1[] = "-h";
  char arg2[] = "example.com";
  char arg3[] = "-p";
  char arg4[] = "12345";
  char* argv[] = {arg0, arg1, arg2, arg3, arg4};

  ParseResult result = ParseArguments(5, argv);
  EXPECT_FALSE(result.exit_now);
  EXPECT_EQ(result.config.host, "example.com");
  EXPECT_EQ(result.config.port, 12345);
  EXPECT_TRUE(result.config.interactive);
}

TEST_F(CliArgumentParsingTest, RejectsOutOfRangePort) {
  char arg0[] = "mygram-cli";
  char arg1[] = "-p";
  char arg2[] = "70000";
  char* argv[] = {arg0, arg1, arg2};

  ParseResult result = ParseArguments(3, argv);
  EXPECT_TRUE(result.exit_now);
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliArgumentParsingTest, RejectsNonNumericPort) {
  char arg0[] = "mygram-cli";
  char arg1[] = "-p";
  char arg2[] = "abc";
  char* argv[] = {arg0, arg1, arg2};

  ParseResult result = ParseArguments(3, argv);
  EXPECT_TRUE(result.exit_now);
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliArgumentParsingTest, RejectsZeroPort) {
  char arg0[] = "mygram-cli";
  char arg1[] = "-p";
  char arg2[] = "0";
  char* argv[] = {arg0, arg1, arg2};

  ParseResult result = ParseArguments(3, argv);
  EXPECT_TRUE(result.exit_now);
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliArgumentParsingTest, RejectsNegativePort) {
  char arg0[] = "mygram-cli";
  char arg1[] = "-p";
  char arg2[] = "-5";
  char* argv[] = {arg0, arg1, arg2};

  ParseResult result = ParseArguments(3, argv);
  EXPECT_TRUE(result.exit_now);
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliArgumentParsingTest, RejectsTrailingGarbageInPort) {
  char arg0[] = "mygram-cli";
  char arg1[] = "-p";
  char arg2[] = "80abc";
  char* argv[] = {arg0, arg1, arg2};

  ParseResult result = ParseArguments(3, argv);
  EXPECT_TRUE(result.exit_now);
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliArgumentParsingTest, MissingPortArgument) {
  char arg0[] = "mygram-cli";
  char arg1[] = "-p";
  char* argv[] = {arg0, arg1};

  ParseResult result = ParseArguments(2, argv);
  EXPECT_TRUE(result.exit_now);
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliArgumentParsingTest, MissingHostArgument) {
  char arg0[] = "mygram-cli";
  char arg1[] = "-h";
  char* argv[] = {arg0, arg1};

  ParseResult result = ParseArguments(2, argv);
  EXPECT_TRUE(result.exit_now);
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliArgumentParsingTest, SocketPathFlag) {
  char arg0[] = "mygram-cli";
  char arg1[] = "-s";
  char arg2[] = "/tmp/mygramdb.sock";
  char* argv[] = {arg0, arg1, arg2};

  ParseResult result = ParseArguments(3, argv);
  EXPECT_FALSE(result.exit_now);
  EXPECT_EQ(result.config.socket_path, "/tmp/mygramdb.sock");
  EXPECT_TRUE(result.config.interactive);
}

TEST_F(CliArgumentParsingTest, RetryFlagAccepted) {
  char arg0[] = "mygram-cli";
  char arg1[] = "--retry";
  char arg2[] = "5";
  char* argv[] = {arg0, arg1, arg2};

  ParseResult result = ParseArguments(3, argv);
  EXPECT_FALSE(result.exit_now);
  EXPECT_EQ(result.config.retry_count, 5);
}

TEST_F(CliArgumentParsingTest, TimeoutFlagsConfigureRequestAndConnectionDeadlines) {
  char arg0[] = "mygram-cli";
  char arg1[] = "--timeout";
  char arg2[] = "750";
  char arg3[] = "--connect-timeout";
  char arg4[] = "125";
  char* argv[] = {arg0, arg1, arg2, arg3, arg4};

  ParseResult result = ParseArguments(5, argv);
  EXPECT_FALSE(result.exit_now);
  EXPECT_EQ(result.config.timeout_ms, 750U);
  EXPECT_EQ(result.config.connect_timeout_ms, 125U);
  EXPECT_EQ(result.config.dump_save_timeout_ms, 750U);
  EXPECT_EQ(result.config.dump_load_timeout_ms, 750U);
  EXPECT_EQ(result.config.dump_verify_timeout_ms, 750U);
  EXPECT_EQ(result.config.optimize_timeout_ms, 750U);
}

TEST_F(CliArgumentParsingTest, TimeoutFlagsRejectMissingZeroAndInvalidValues) {
  char arg0[] = "mygram-cli";
  char timeout[] = "--timeout";
  char connect_timeout[] = "--connect-timeout";
  char zero[] = "0";
  char invalid[] = "invalid";

  char* missing_timeout[] = {arg0, timeout};
  EXPECT_TRUE(ParseArguments(2, missing_timeout).exit_now);
  char* zero_timeout[] = {arg0, timeout, zero};
  EXPECT_TRUE(ParseArguments(3, zero_timeout).exit_now);
  char* missing_connect_timeout[] = {arg0, connect_timeout};
  EXPECT_TRUE(ParseArguments(2, missing_connect_timeout).exit_now);
  char* invalid_connect_timeout[] = {arg0, connect_timeout, invalid};
  EXPECT_TRUE(ParseArguments(3, invalid_connect_timeout).exit_now);
}

TEST_F(CliArgumentParsingTest, RetryFlagRejectsNegative) {
  char arg0[] = "mygram-cli";
  char arg1[] = "--retry";
  char arg2[] = "-1";
  char* argv[] = {arg0, arg1, arg2};

  ParseResult result = ParseArguments(3, argv);
  EXPECT_TRUE(result.exit_now);
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliArgumentParsingTest, WaitReadyFlag) {
  char arg0[] = "mygram-cli";
  char arg1[] = "--wait-ready";
  char* argv[] = {arg0, arg1};

  ParseResult result = ParseArguments(2, argv);
  EXPECT_FALSE(result.exit_now);
  EXPECT_TRUE(result.config.wait_ready);
  EXPECT_EQ(result.config.retry_count, kMaxWaitReadyRetries);
}

TEST_F(CliArgumentParsingTest, WaitReadyRespectsExplicitRetryIndependentOfArgumentOrder) {
  char arg0[] = "mygram-cli";
  char wait_ready[] = "--wait-ready";
  char retry[] = "--retry";
  char retry_count[] = "5";

  char* wait_then_retry[] = {arg0, wait_ready, retry, retry_count};
  ParseResult result1 = ParseArguments(4, wait_then_retry);
  EXPECT_FALSE(result1.exit_now);
  EXPECT_TRUE(result1.config.wait_ready);
  EXPECT_EQ(result1.config.retry_count, 5);
  EXPECT_TRUE(result1.config.retry_count_explicit);

  char* retry_then_wait[] = {arg0, retry, retry_count, wait_ready};
  ParseResult result2 = ParseArguments(4, retry_then_wait);
  EXPECT_FALSE(result2.exit_now);
  EXPECT_TRUE(result2.config.wait_ready);
  EXPECT_EQ(result2.config.retry_count, 5);
  EXPECT_TRUE(result2.config.retry_count_explicit);
}

TEST_F(CliArgumentParsingTest, RetryIntervalFlagAcceptsZeroAndRejectsInvalidValues) {
  char arg0[] = "mygram-cli";
  char retry_interval[] = "--retry-interval";
  char zero[] = "0";
  char negative[] = "-1";

  char* valid[] = {arg0, retry_interval, zero};
  ParseResult valid_result = ParseArguments(3, valid);
  EXPECT_FALSE(valid_result.exit_now);
  EXPECT_EQ(valid_result.config.retry_interval, 0);

  char* missing[] = {arg0, retry_interval};
  EXPECT_TRUE(ParseArguments(2, missing).exit_now);
  char* invalid[] = {arg0, retry_interval, negative};
  EXPECT_TRUE(ParseArguments(3, invalid).exit_now);
}

TEST_F(CliArgumentParsingTest, SingleCommandModeSetsInteractiveFalse) {
  char arg0[] = "mygram-cli";
  char arg1[] = "SEARCH";
  char arg2[] = "articles";
  char arg3[] = "hello";
  char* argv[] = {arg0, arg1, arg2, arg3};

  ParseResult result = ParseArguments(4, argv);
  EXPECT_FALSE(result.exit_now);
  EXPECT_FALSE(result.config.interactive);
  ASSERT_EQ(result.command_args.size(), 3U);
  EXPECT_EQ(result.command_args[0], "SEARCH");
  EXPECT_EQ(result.command_args[1], "articles");
  EXPECT_EQ(result.command_args[2], "hello");
}

TEST_F(CliArgumentParsingTest, HelpFlagExits) {
  char arg0[] = "mygram-cli";
  char arg1[] = "--help";
  char* argv[] = {arg0, arg1};

  StdoutCapture capture;
  ParseResult result = ParseArguments(2, argv);
  EXPECT_TRUE(result.exit_now);
  EXPECT_EQ(result.exit_code, 0);
  // Help text should mention key options
  std::string output = capture.GetOutput();
  EXPECT_NE(output.find("-h HOST"), std::string::npos);
  EXPECT_NE(output.find("-p PORT"), std::string::npos);
  EXPECT_NE(output.find("--retry"), std::string::npos);
  EXPECT_NE(output.find("--retry-interval"), std::string::npos);
  EXPECT_NE(output.find("--wait-ready"), std::string::npos);
  EXPECT_NE(output.find("--timeout"), std::string::npos);
  EXPECT_NE(output.find("--connect-timeout"), std::string::npos);
  EXPECT_NE(output.find("SEARCH app.articles hello"), std::string::npos);
}

TEST_F(CliArgumentParsingTest, VersionFlagExits) {
  char arg0[] = "mygram-cli";
  char arg1[] = "--version";
  char* argv[] = {arg0, arg1};

  StdoutCapture capture;
  ParseResult result = ParseArguments(2, argv);
  EXPECT_TRUE(result.exit_now);
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_NE(capture.GetOutput().find("mygram-cli"), std::string::npos);
}

// =============================================================================
// Single-command-mode argument joining preserves boolean expression text.
// =============================================================================

class CliJoinArgsTest : public ::testing::Test {};

TEST_F(CliJoinArgsTest, PreservesSimpleArgs) {
  EXPECT_EQ(JoinArgsForCommand({"SEARCH", "articles", "hello", "world"}), "SEARCH articles hello world");
}

TEST_F(CliJoinArgsTest, PreservesSpaceContainingExpressionArgs) {
  EXPECT_EQ(JoinArgsForCommand({"SEARCH", "articles", "hello world"}), "SEARCH articles hello world");
}

TEST_F(CliJoinArgsTest, PreservesEmbeddedQuotes) {
  EXPECT_EQ(JoinArgsForCommand({"SEARCH", "articles", "say \"hi\""}), "SEARCH articles say \"hi\"");
}

}  // namespace
