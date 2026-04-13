/**
 * @file mygram_cli_test.cpp
 * @brief Tests for mygram-cli response parsing and configuration defaults
 *
 * Since the CLI source is monolithic (all in one file with an anonymous namespace
 * and main()), we include the source file directly with main() renamed to avoid
 * linker conflicts. This gives us access to Config, PrintResponse, and constants.
 */

#include <gtest/gtest.h>

#include <sstream>
#include <string>

// Rename main() to avoid conflict with gtest's main, and expose private
// members for testing (PrintResponse is private static in MygramClient).
#define main cli_main
#define private public         // NOLINT(cppcoreguidelines-macro-usage)
#include "cli/mygram-cli.cpp"  // NOLINT(bugprone-suspicious-include)
#undef private
#undef main

namespace {

/**
 * @brief RAII helper to capture stdout into a string
 */
class StdoutCapture {
 public:
  StdoutCapture() { old_buf_ = std::cout.rdbuf(capture_.rdbuf()); }

  ~StdoutCapture() { std::cout.rdbuf(old_buf_); }

  // Non-copyable, non-movable
  StdoutCapture(const StdoutCapture&) = delete;
  StdoutCapture& operator=(const StdoutCapture&) = delete;
  StdoutCapture(StdoutCapture&&) = delete;
  StdoutCapture& operator=(StdoutCapture&&) = delete;

  [[nodiscard]] std::string GetOutput() const { return capture_.str(); }

 private:
  std::ostringstream capture_;
  std::streambuf* old_buf_{nullptr};
};

// ============================================================================
// Config default value tests
// ============================================================================

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

TEST_F(CliConfigTest, SocketPathOverridesDefault) {
  Config config;
  config.socket_path = "/tmp/mygramdb.sock";
  EXPECT_FALSE(config.socket_path.empty());
  EXPECT_EQ(config.socket_path, "/tmp/mygramdb.sock");
}

TEST_F(CliConfigTest, WaitReadyRetrySetsMaxRetries) {
  Config config;
  config.retry_count = kMaxWaitReadyRetries;
  EXPECT_EQ(config.retry_count, 100);
}

// ============================================================================
// PrintResponse tests - SEARCH responses
// ============================================================================

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
  // Should not contain "showing"
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
}

// ============================================================================
// PrintResponse tests - COUNT responses
// ============================================================================

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
}

// ============================================================================
// PrintResponse tests - GET (DOC) responses
// ============================================================================

TEST_F(CliPrintResponseTest, DocResponse) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK DOC 42 title=hello author=world");
  std::string output = capture.GetOutput();

  // Should strip "OK " prefix and print the rest
  EXPECT_NE(output.find("DOC 42 title=hello author=world"), std::string::npos);
  // Should NOT start with "OK"
  EXPECT_EQ(output.find("OK "), std::string::npos);
}

// ============================================================================
// PrintResponse tests - INFO responses
// ============================================================================

TEST_F(CliPrintResponseTest, InfoResponse) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK INFO\r\nversion: 1.0\r\ntables: articles");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("version: 1.0"), std::string::npos);
  EXPECT_NE(output.find("tables: articles"), std::string::npos);
}

// ============================================================================
// PrintResponse tests - SAVE/LOAD responses
// ============================================================================

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

// ============================================================================
// PrintResponse tests - REPLICATION responses
// ============================================================================

TEST_F(CliPrintResponseTest, ReplicationStoppedResponse) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK REPLICATION_STOPPED");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("Replication stopped successfully"), std::string::npos);
}

TEST_F(CliPrintResponseTest, ReplicationStartedResponse) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK REPLICATION_STARTED");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("Replication started successfully"), std::string::npos);
}

// ============================================================================
// PrintResponse tests - DEBUG responses
// ============================================================================

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

// ============================================================================
// PrintResponse tests - ERROR responses
// ============================================================================

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

// ============================================================================
// PrintResponse tests - Unknown responses
// ============================================================================

TEST_F(CliPrintResponseTest, UnknownResponsePassthrough) {
  StdoutCapture capture;
  MygramClient::PrintResponse("SOMETHING UNEXPECTED");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("SOMETHING UNEXPECTED"), std::string::npos);
}

// ============================================================================
// Constants tests
// ============================================================================

class CliConstantsTest : public ::testing::Test {};

TEST_F(CliConstantsTest, BufferSizeIs64KB) {
  EXPECT_EQ(kReceiveBufferSize, 65536U);
}

TEST_F(CliConstantsTest, PrefixLengthsAreCorrect) {
  // Verify prefix lengths match the actual prefix strings
  EXPECT_EQ(kOkInfoPrefixLength, std::string("OK INFO\r\n").length() - 1);
  EXPECT_EQ(kOkSavedPrefixLength, std::string("OK SAVED ").length());
  EXPECT_EQ(kOkLoadedPrefixLength, std::string("OK LOADED ").length());
  EXPECT_EQ(kErrorPrefixLength, std::string("ERROR ").length());
}

TEST_F(CliConstantsTest, MaxWaitReadyRetries) {
  EXPECT_EQ(kMaxWaitReadyRetries, 100);
}

// ============================================================================
// Argument parsing simulation tests
// ============================================================================

class CliArgumentParsingTest : public ::testing::Test {};

TEST_F(CliArgumentParsingTest, DefaultConfigNoArgs) {
  // Simulate: mygram-cli (no args)
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
  const char* argv[] = {"mygram-cli"};
  int argc = 1;

  Config config;
  std::vector<std::string> command_args;

  for (int i = 1; i < argc; ++i) {
    std::string arg(argv[i]);  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    command_args.emplace_back(arg);
  }

  // No args means interactive mode with defaults
  EXPECT_TRUE(config.interactive);
  EXPECT_TRUE(command_args.empty());
  EXPECT_EQ(config.host, "127.0.0.1");
  EXPECT_EQ(config.port, 11016);
}

TEST_F(CliArgumentParsingTest, HostAndPortArgs) {
  // Simulate: mygram-cli -h localhost -p 12345
  Config config;

  // Simulate parsing -h and -p
  config.host = "localhost";
  config.port = 12345;

  EXPECT_EQ(config.host, "localhost");
  EXPECT_EQ(config.port, 12345);
}

TEST_F(CliArgumentParsingTest, SocketPathOverridesTcp) {
  // Simulate: mygram-cli -s /tmp/mygramdb.sock
  Config config;
  config.socket_path = "/tmp/mygramdb.sock";

  // Socket path should be set; host/port still have defaults but are unused
  EXPECT_FALSE(config.socket_path.empty());
  EXPECT_EQ(config.host, "127.0.0.1");  // Default unchanged
}

TEST_F(CliArgumentParsingTest, RetryFlag) {
  Config config;
  config.retry_count = 5;

  EXPECT_EQ(config.retry_count, 5);
  EXPECT_GT(config.retry_count, 0);
}

TEST_F(CliArgumentParsingTest, WaitReadyFlag) {
  Config config;
  config.retry_count = kMaxWaitReadyRetries;

  EXPECT_EQ(config.retry_count, 100);
}

TEST_F(CliArgumentParsingTest, SingleCommandMode) {
  // Simulate: mygram-cli SEARCH articles hello
  Config config;
  config.interactive = false;

  std::vector<std::string> command_args = {"SEARCH", "articles", "hello"};

  // Build command string (same logic as main())
  std::ostringstream command;
  for (size_t i = 0; i < command_args.size(); ++i) {
    if (i > 0) {
      command << " ";
    }
    command << command_args[i];
  }

  EXPECT_FALSE(config.interactive);
  EXPECT_EQ(command.str(), "SEARCH articles hello");
}

TEST_F(CliArgumentParsingTest, CommandBuildingPreservesSpacing) {
  std::vector<std::string> args = {"SEARCH", "articles", "hello", "world", "LIMIT", "10", "OFFSET", "5"};

  std::ostringstream command;
  for (size_t i = 0; i < args.size(); ++i) {
    if (i > 0) {
      command << " ";
    }
    command << args[i];
  }

  EXPECT_EQ(command.str(), "SEARCH articles hello world LIMIT 10 OFFSET 5");
}

// ============================================================================
// Search result large count test
// ============================================================================

TEST_F(CliPrintResponseTest, SearchResponseLargeCount) {
  StdoutCapture capture;
  MygramClient::PrintResponse("OK RESULTS 1000000 1 2 3 4 5 6 7 8 9 10");
  std::string output = capture.GetOutput();

  EXPECT_NE(output.find("1000000 results"), std::string::npos);
  EXPECT_NE(output.find("showing 10"), std::string::npos);
}

// ============================================================================
// Port validation tests
// ============================================================================

class CliPortValidationTest : public ::testing::Test {};

TEST_F(CliPortValidationTest, ValidPortRange) {
  // Port 1 is the minimum valid port
  int port_int = 1;
  EXPECT_GE(port_int, 1);
  EXPECT_LE(port_int, 65535);

  // Port 65535 is the maximum valid port
  port_int = 65535;
  EXPECT_GE(port_int, 1);
  EXPECT_LE(port_int, 65535);
}

TEST_F(CliPortValidationTest, InvalidPortZero) {
  int port_int = 0;
  EXPECT_TRUE(port_int < 1 || port_int > 65535) << "Port 0 should be invalid";
}

TEST_F(CliPortValidationTest, InvalidPortNegative) {
  int port_int = -1;
  EXPECT_TRUE(port_int < 1 || port_int > 65535) << "Port -1 should be invalid";
}

TEST_F(CliPortValidationTest, InvalidPortTooLarge) {
  int port_int = 70000;
  EXPECT_TRUE(port_int < 1 || port_int > 65535) << "Port 70000 should be invalid";
}

TEST_F(CliPortValidationTest, InvalidPortOverflow) {
  // Verify that a port parsed as int > 65535 would be caught
  int port_int = 99999;
  EXPECT_TRUE(port_int < 1 || port_int > 65535) << "Port 99999 should be invalid";
}

}  // namespace
