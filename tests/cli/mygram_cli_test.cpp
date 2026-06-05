/**
 * @file mygram_cli_test.cpp
 * @brief Tests for mygram-cli helpers, response parsing, and configuration.
 *
 * The CLI source is monolithic (single file with an anonymous namespace and
 * main()). To exercise the internal helpers we include the source file
 * directly with main() renamed to avoid linker conflicts. We also #define
 * private public so we can call MygramClient::PrintResponse, which is a
 * private static.
 */

#include <gtest/gtest.h>

#include <sstream>
#include <string>
#include <vector>

// Rename main() to avoid clashing with gtest's main, and unprivate
// MygramClient::PrintResponse for direct test access.
#define main cli_main
#define private public         // NOLINT(cppcoreguidelines-macro-usage)
#include "cli/mygram-cli.cpp"  // NOLINT(bugprone-suspicious-include)
#undef private
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

// =============================================================================
// Config defaults
// =============================================================================

class CliConfigTest : public ::testing::Test {};

TEST_F(CliConfigTest, DefaultValues) {
  Config config;
  EXPECT_EQ(config.host, "127.0.0.1");
  EXPECT_EQ(config.port, 11016);
  EXPECT_TRUE(config.interactive);
  EXPECT_EQ(config.retry_count, 0);
  EXPECT_EQ(config.retry_interval, 3);
  EXPECT_TRUE(config.socket_path.empty());
}

TEST_F(CliConfigTest, SocketPathOverride) {
  Config config;
  config.socket_path = "/tmp/mygramdb.sock";
  EXPECT_EQ(config.socket_path, "/tmp/mygramdb.sock");
}

TEST_F(CliConfigTest, WaitReadyRetrySetsMaxRetries) {
  Config config;
  config.retry_count = kMaxWaitReadyRetries;
  EXPECT_EQ(config.retry_count, 100);
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
  EXPECT_EQ(JoinArgsForCommand({"SEARCH", "articles", "hello world"}), "SEARCH articles \"hello world\"");
  EXPECT_EQ(JoinArgsForCommand({}), "");
  EXPECT_EQ(JoinArgsForCommand({"INFO"}), "INFO");
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
  MygramClient::PrintResponse("OK SYNC STARTED table=foo job_id=1");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("SYNC STARTED"), std::string::npos);
  EXPECT_NE(output.find("table=foo"), std::string::npos);
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
  StdoutCapture capture;
  MygramClient::PrintResponse("ERROR Unknown command");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("(error) Unknown command"), std::string::npos);
}

TEST_F(CliPrintResponseTest, ErrorResponseWithCode) {
  StdoutCapture capture;
  MygramClient::PrintResponse("ERROR 3001 Invalid query syntax");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("(error) 3001 Invalid query syntax"), std::string::npos);
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
  EXPECT_EQ(result.config.retry_count, kMaxWaitReadyRetries);
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
  EXPECT_NE(output.find("--wait-ready"), std::string::npos);
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
// Single-command-mode argument joining (preserves spaces via QuoteArgIfNeeded)
// =============================================================================

class CliJoinArgsTest : public ::testing::Test {};

TEST_F(CliJoinArgsTest, PreservesSimpleArgs) {
  EXPECT_EQ(JoinArgsForCommand({"SEARCH", "articles", "hello", "world"}), "SEARCH articles hello world");
}

TEST_F(CliJoinArgsTest, QuotesSpaceContainingArgs) {
  // The old CLI just space-joined argv: "hello world" -> 2 tokens on the wire.
  // With proper quoting, it stays a single token.
  EXPECT_EQ(JoinArgsForCommand({"SEARCH", "articles", "hello world"}), "SEARCH articles \"hello world\"");
}

TEST_F(CliJoinArgsTest, EscapesEmbeddedQuotes) {
  EXPECT_EQ(JoinArgsForCommand({"SEARCH", "articles", "say \"hi\""}), "SEARCH articles \"say \\\"hi\\\"\"");
}

}  // namespace
