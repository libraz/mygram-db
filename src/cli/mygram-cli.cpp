/**
 * @file mygram-cli.cpp
 * @brief Command-line client for MygramDB (redis-cli style)
 *
 * This file is a thin CLI wrapper around mygramdb::client::MygramClient.
 * The library handles socket I/O, hostname resolution, response framing,
 * and timeouts; the CLI adds:
 *   - Argument parsing for -h/-p/-s/--retry/--wait-ready/--help/--version
 *   - Connect-with-retry semantics and helpful error hints
 *   - Pretty-printing of protocol responses
 *   - Optional readline-based tab completion
 */

#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <csignal>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "client/mygramclient.h"
#include "config/config.h"
#include "server/protocol_constants.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/numeric_parse.h"
#include "utils/string_utils.h"
#include "version.h"

#ifdef HAVE_READLINE
#include <readline/history.h>
#include <readline/readline.h>
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define USE_READLINE 1
#endif

namespace {

namespace lib = mygramdb::client;
namespace proto = mygramdb::server::protocol;

// Re-exported protocol constants (also referenced from tests)
[[maybe_unused]] constexpr size_t kReceiveBufferSize = proto::kDefaultClientRecvBufferSize;
constexpr size_t kErrorPrefixLength = proto::kErrorPrefixLen;
constexpr size_t kOkSavedPrefixLength = proto::kOkSavedPrefixLen;
constexpr size_t kOkLoadedPrefixLength = proto::kOkLoadedPrefixLen;

// CLI behavior constants
constexpr int kMaxWaitReadyRetries = 100;  // ~5 minutes at 3s interval
constexpr int kExitSuccess = 0;
constexpr int kExitFailure = 1;
constexpr uint16_t kDefaultPort = static_cast<uint16_t>(mygramdb::config::defaults::kTcpPort);
constexpr uint32_t kInteractiveTimeoutMs = 30000;  // 30 seconds for interactive sessions
constexpr int kMinTcpPort = 1;
constexpr int kMaxTcpPort = 65535;
constexpr unsigned char kAsciiPrintableMin = 0x20;  // First printable ASCII codepoint

// =============================================================================
// String helpers
// =============================================================================

using mygram::utils::ToUpper;
using mygram::utils::TrimAsciiWhitespace;

/**
 * @brief Helper: case-sensitive prefix check.
 */
bool StartsWith(const std::string& str, std::string_view prefix) {
  return str.size() >= prefix.size() && str.compare(0, prefix.size(), prefix) == 0;
}

/**
 * @brief Replace all CRLF (\r\n) sequences with LF (\n).
 *
 * Many multi-line server responses (INFO, REPLICATION STATUS, CONFIG, etc.)
 * use CRLF line endings on the wire. Convert to plain LF for terminal output.
 */
std::string NormalizeCrlf(std::string str) {
  size_t pos = 0;
  while ((pos = str.find("\r\n", pos)) != std::string::npos) {
    str.replace(pos, 2, "\n");
    pos += 1;
  }
  return str;
}

/**
 * @brief Strip a trailing "END" line marker (with optional surrounding LF) from
 *        a response body. Multi-line responses such as INFO, REPLICATION,
 *        CACHE_STATS, DUMP_INFO emit a trailing "END" sentinel.
 */
std::string StripTrailingEndMarker(std::string str) {
  // Trim trailing newlines first
  while (!str.empty() && (str.back() == '\n' || str.back() == '\r')) {
    str.pop_back();
  }
  // Remove trailing "END"
  constexpr const char* kEnd = "END";
  constexpr size_t kEndLen = 3;
  if (str.size() >= kEndLen && str.compare(str.size() - kEndLen, kEndLen, kEnd) == 0) {
    // Check that "END" is at start-of-line (preceded by '\n' or buffer start)
    bool at_line_start = (str.size() == kEndLen) || str[str.size() - kEndLen - 1] == '\n';
    if (at_line_start) {
      str.erase(str.size() - kEndLen);
      // Trim trailing newlines that preceded END
      while (!str.empty() && (str.back() == '\n' || str.back() == '\r')) {
        str.pop_back();
      }
    }
  }
  return str;
}

std::string DecodeGetDocBodyForDisplay(std::string_view body) {
  std::string result;
  result.reserve(body.size());

  for (size_t i = 0; i < body.size(); ++i) {
    result += body[i];
    if (body[i] != '=' || i + 1 >= body.size() || body[i + 1] != '"') {
      continue;
    }

    ++i;  // Skip opening quote.
    while (++i < body.size()) {
      if (body[i] == '\\' && i + 1 < body.size()) {
        result += body[++i];
        continue;
      }
      if (body[i] == '"') {
        break;
      }
      result += body[i];
    }
  }

  return result;
}

/**
 * @brief Quote an argument for the wire protocol if it contains whitespace
 *        or special characters that would otherwise be split into multiple
 *        tokens. Backslash and double-quote are escaped within the quotes.
 */
std::string QuoteArgIfNeeded(const std::string& arg) {
  bool needs_quotes = arg.empty();
  for (char character : arg) {
    if (character == ' ' || character == '\t' || character == '"' || character == '\\' || character == '\'') {
      needs_quotes = true;
      break;
    }
    // Refuse control characters silently (caller already validates input)
    if (static_cast<unsigned char>(character) < kAsciiPrintableMin) {
      needs_quotes = true;
      break;
    }
  }
  if (!needs_quotes) {
    return arg;
  }
  std::string result;
  result.reserve(arg.size() + 2);
  result += '"';
  for (char character : arg) {
    if (static_cast<unsigned char>(character) < kAsciiPrintableMin) {
      // Drop control chars to prevent protocol injection
      continue;
    }
    if (character == '"' || character == '\\') {
      result += '\\';
    }
    result += character;
  }
  result += '"';
  return result;
}

std::string JoinArgsForCommand(const std::vector<std::string>& args) {
  std::string result;
  for (size_t i = 0; i < args.size(); ++i) {
    if (i > 0) {
      result += ' ';
    }
    result += QuoteArgIfNeeded(args[i]);
  }
  return result;
}

// =============================================================================
// Tab completion (readline)
// =============================================================================

#ifdef USE_READLINE

// NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-avoid-non-const-global-variables)
const char* command_list[] = {"SEARCH", "COUNT", "GET",  "INFO", "CONFIG",   "FACET", "SET",  "SHOW", "REPLICATION",
                              "DEBUG",  "CACHE", "DUMP", "SYNC", "OPTIMIZE", "quit",  "exit", "help", nullptr};

char* CommandGenerator(const char* text, int state) {
  static int list_index;
  static int len;
  const char* name = nullptr;

  if (state == 0) {
    list_index = 0;
    len = static_cast<int>(strlen(text));
  }

  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
  while ((name = command_list[list_index++]) != nullptr) {
    if (strncasecmp(name, text, len) == 0) {
      // NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
      return strdup(name);
    }
  }
  return nullptr;
}

std::vector<std::string> ParseTokens(const std::string& line) {
  std::vector<std::string> tokens;
  std::istringstream iss(line);
  std::string token;
  while (iss >> token) {
    tokens.push_back(token);
  }
  return tokens;
}

char* KeywordGenerator(const std::vector<std::string>& keywords, const char* text, int state) {
  static size_t list_index;
  static int len;

  if (state == 0) {
    list_index = 0;
    len = static_cast<int>(strlen(text));
  }

  while (list_index < keywords.size()) {
    const std::string& name = keywords[list_index++];
    if (len == 0 || strncasecmp(name.c_str(), text, len) == 0) {
      // NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
      return strdup(name.c_str());
    }
  }
  return nullptr;
}

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::vector<std::string> current_keywords;

// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
std::vector<std::string> available_tables;

char* KeywordGeneratorWrapper(const char* text, int state) {
  return KeywordGenerator(current_keywords, text, state);
}

bool TokenListContains(const std::vector<std::string>& tokens, const std::string& upper_keyword) {
  return std::any_of(tokens.begin(), tokens.end(),
                     [&upper_keyword](const std::string& token) { return ToUpper(token) == upper_keyword; });
}

std::string PreviousTokenUpper(const std::vector<std::string>& tokens) {
  if (!tokens.empty()) {
    return ToUpper(tokens.back());
  }
  return "";
}

char** CommandCompletion(const char* text, int start, int /* end */) {
  rl_attempted_completion_over = 1;

  std::string line(rl_line_buffer, start);
  std::vector<std::string> tokens = ParseTokens(line);

  // Beginning of the line: complete command names
  if (tokens.empty()) {
    return rl_completion_matches(text, CommandGenerator);
  }

  std::string command = ToUpper(tokens[0]);
  size_t token_count = tokens.size();

  // Helper: when no useful suggestions exist, return nullptr so readline
  // falls back to "no completion" (rather than auto-inserting a placeholder).

  if (command == "SEARCH") {
    std::string prev = PreviousTokenUpper(tokens);
    if (prev == "ORDER" || prev == "SORT") {
      current_keywords = {"BY", "ASC", "DESC"};
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    if (prev == "BY" && (TokenListContains(tokens, "ORDER") || TokenListContains(tokens, "SORT"))) {
      current_keywords = {"ASC", "DESC"};
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    if (token_count == 1) {
      if (!available_tables.empty()) {
        current_keywords = available_tables;
        return rl_completion_matches(text, KeywordGeneratorWrapper);
      }
      return nullptr;
    }
    if (token_count == 2) {
      // No useful suggestion for free-form search text
      return nullptr;
    }
    current_keywords = {"AND", "OR", "NOT", "FILTER", "SORT", "LIMIT", "OFFSET"};
    return rl_completion_matches(text, KeywordGeneratorWrapper);
  }

  if (command == "COUNT") {
    if (token_count == 1) {
      if (!available_tables.empty()) {
        current_keywords = available_tables;
        return rl_completion_matches(text, KeywordGeneratorWrapper);
      }
      return nullptr;
    }
    if (token_count == 2) {
      return nullptr;
    }
    current_keywords = {"AND", "OR", "NOT", "FILTER"};
    return rl_completion_matches(text, KeywordGeneratorWrapper);
  }

  if (command == "GET" || command == "FACET") {
    if (token_count == 1) {
      if (!available_tables.empty()) {
        current_keywords = available_tables;
        return rl_completion_matches(text, KeywordGeneratorWrapper);
      }
    }
    return nullptr;
  }

  if (command == "SHOW") {
    if (token_count == 1) {
      current_keywords = {"VARIABLES"};
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    if (token_count == 2 && tokens[1] == "VARIABLES") {
      current_keywords = {"LIKE"};
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    return nullptr;
  }

  if (command == "REPLICATION") {
    if (token_count == 1) {
      current_keywords = {"STATUS", "STOP", "START"};
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    return nullptr;
  }

  if (command == "DEBUG") {
    if (token_count == 1) {
      // DEBUG accepts ON/OFF subcommands. OPTIMIZE is a separate top-level
      // command (handled by query_parser) and was incorrectly listed here.
      current_keywords = {"ON", "OFF"};
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    return nullptr;
  }

  if (command == "CACHE") {
    if (token_count == 1) {
      current_keywords = {"STATS", "CLEAR", "ENABLE", "DISABLE"};
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    return nullptr;
  }

  if (command == "DUMP") {
    if (token_count == 1) {
      current_keywords = {"SAVE", "LOAD", "VERIFY", "INFO", "STATUS"};
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    if (token_count == 2 &&
        (tokens[1] == "SAVE" || tokens[1] == "LOAD" || tokens[1] == "VERIFY" || tokens[1] == "INFO")) {
      // Defer to filename completion
      rl_attempted_completion_over = 0;
      return nullptr;
    }
    return nullptr;
  }

  if (command == "SYNC") {
    if (token_count == 1) {
      current_keywords = {"STOP", "STATUS"};
      if (!available_tables.empty()) {
        current_keywords.insert(current_keywords.end(), available_tables.begin(), available_tables.end());
      }
      return rl_completion_matches(text, KeywordGeneratorWrapper);
    }
    return nullptr;
  }

  return nullptr;
}
#endif

// =============================================================================
// Configuration
// =============================================================================

struct Config {
  std::string host = "127.0.0.1";
  uint16_t port = kDefaultPort;
  bool interactive = true;
  bool wait_ready = false;
  int retry_count = 0;     // Number of retries (0 = no retry)
  int retry_interval = 3;  // Seconds between retries
  std::string socket_path;
};

// =============================================================================
// Port parsing
// =============================================================================

struct ParsedPort {
  uint16_t port = 0;
  bool ok = false;
  std::string error;
};

ParsedPort ParsePort(const std::string& str) {
  ParsedPort result;
  auto parsed = mygram::utils::ParseNumeric<int32_t>(str);
  if (!parsed.has_value()) {
    result.error = "Invalid port number: '" + str + "'";
    return result;
  }
  if (*parsed < kMinTcpPort || *parsed > kMaxTcpPort) {
    result.error = "Port out of range [1, 65535]: " + str;
    return result;
  }
  result.port = static_cast<uint16_t>(*parsed);
  result.ok = true;
  return result;
}

// =============================================================================
// CLI client wrapper
// =============================================================================

class MygramClient {
 public:
  // NOLINTNEXTLINE(google-explicit-constructor) -- intentional single-arg ctor
  explicit MygramClient(Config config) : config_(std::move(config)) {
    lib::ClientConfig lib_cfg;
    lib_cfg.host = config_.host;
    lib_cfg.port = config_.port;
    lib_cfg.unix_socket_path = config_.socket_path;
    lib_cfg.timeout_ms = kInteractiveTimeoutMs;
    lib_cfg.recv_buffer_size = proto::kDefaultClientRecvBufferSize;
    client_ = std::make_unique<lib::MygramClient>(std::move(lib_cfg));
  }

  ~MygramClient() = default;

  MygramClient(const MygramClient&) = delete;
  MygramClient& operator=(const MygramClient&) = delete;
  MygramClient(MygramClient&&) = default;
  MygramClient& operator=(MygramClient&&) = default;

  bool Connect() {
    int max_attempts = 1 + config_.retry_count;

    for (int attempt = 0; attempt < max_attempts; ++attempt) {
      if (attempt > 0) {
        std::cerr << "\nRetrying in " << config_.retry_interval << " seconds... (attempt " << (attempt + 1) << "/"
                  << max_attempts << ")\n";
        sleep(static_cast<unsigned int>(config_.retry_interval));
      }

      auto result = client_->Connect();
      if (result) {
        if (attempt > 0) {
          std::cerr << "\nConnected successfully after " << attempt << " retry(ies)!\n\n";
        }
        return true;
      }

      // Connection failed: print error and decide whether to retry.
      // The library already prefixes its own messages with "Connection
      // failed: " or "Failed to resolve host: ", so just emit the message
      // verbatim instead of double-prefixing.
      const std::string& message = result.error().message();
      bool refused = message.find("Connection refused") != std::string::npos ||
                     message.find("No such file or directory") != std::string::npos;

      std::cerr << message << '\n';
      PrintConnectionHints(message);

      if (!refused) {
        return false;  // Not retriable
      }
      // Otherwise loop and retry
    }

    return false;
  }

  void Disconnect() {
    if (client_) {
      client_->Disconnect();
    }
  }

  [[nodiscard]] bool IsConnected() const { return client_ && client_->IsConnected(); }

#ifdef USE_READLINE
  /**
   * @brief Fetch table names from server INFO response into available_tables.
   */
  void FetchTableNames() const {
    if (!IsConnected()) {
      return;
    }
    auto info = client_->Info();
    if (!info) {
      return;
    }
    available_tables = info->tables;
  }
#endif

  /**
   * @brief Send a raw command and return the response (or formatted error).
   *
   * Errors are returned with the legacy "(error) ..." string prefix so that
   * PrintResponse() can format them like other responses. Connection-loss
   * errors include the SERVER_DISCONNECTED / SERVER_TIMEOUT keyword so the
   * interactive loop can detect them.
   */
  [[nodiscard]] std::string SendCommand(const std::string& command) const {
    if (!IsConnected()) {
      return "(error) Not connected";
    }

    auto result = client_->SendCommand(command);
    if (result) {
      return *result;
    }

    using mygram::utils::ErrorCode;
    const auto& err = result.error();
    switch (err.code()) {
      case ErrorCode::kClientNotConnected:
        return "(error) Not connected";
      case ErrorCode::kClientConnectionClosed:
        return "(error) SERVER_DISCONNECTED: Server closed the connection. This usually means:\n"
               "  1. Server was shut down gracefully\n"
               "  2. Server crashed or encountered a fatal error\n"
               "  3. Server restarted and dropped all connections\n"
               "\nTry reconnecting to check if the server is still running.";
      case ErrorCode::kClientCommandFailed: {
        const std::string& message = err.message();
        if (message.find("Broken pipe") != std::string::npos || message.find("Connection reset") != std::string::npos) {
          return "(error) SERVER_DISCONNECTED: Connection lost while sending command. The server may "
                 "have crashed or been shut down.";
        }
        if (message.find("Resource temporarily unavailable") != std::string::npos ||
            message.find("Operation timed out") != std::string::npos) {
          return "(error) SERVER_TIMEOUT: Server did not respond in time. It may be under heavy load or "
                 "frozen.";
        }
        return "(error) " + message;
      }
      default:
        return "(error) " + err.message();
    }
  }

  void RunInteractive() const {
    if (!config_.socket_path.empty()) {
      std::cout << "mygram-cli " << config_.socket_path << '\n';
    } else {
      std::cout << "mygram-cli " << config_.host << ":" << config_.port << '\n';
    }
    std::cout << "Type 'quit' or 'exit' to exit, 'help' for help\n";
#ifdef USE_READLINE
    std::cout << "Use TAB for context-aware command completion\n";
#endif
    std::cout << '\n';

#ifdef USE_READLINE
    FetchTableNames();
    rl_attempted_completion_function = CommandCompletion;
#endif

    while (true) {
      std::string line;

#ifdef USE_READLINE
      std::string prompt = config_.socket_path.empty() ? config_.host + ":" + std::to_string(config_.port) + "> "
                                                       : config_.socket_path + "> ";
      // NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
      char* raw_input = readline(prompt.c_str());
      if (raw_input == nullptr) {
        // EOF (Ctrl-D)
        std::cout << '\n';
        break;
      }
      // NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory)
      std::unique_ptr<char, decltype(&free)> input(raw_input, &free);
      line = TrimAsciiWhitespace(input.get());

      // Add only non-empty, non-trivial commands to history
      if (!line.empty() && !IsTrivialCommand(line)) {
        add_history(line.c_str());
      }
#else
      std::cout << config_.host << ":" << config_.port << "> ";
      std::cout.flush();
      if (!std::getline(std::cin, line)) {
        break;
      }
      line = TrimAsciiWhitespace(line);
#endif

      if (line.empty()) {
        continue;
      }

      if (line == "quit" || line == "exit") {
        std::cout << "Bye!" << '\n';
        break;
      }

      if (line == "help") {
        PrintHelp();
        continue;
      }

      std::string response = SendCommand(line);

      bool disconnected = response.find("SERVER_DISCONNECTED") != std::string::npos ||
                          response.find("SERVER_TIMEOUT") != std::string::npos;
      PrintResponse(response);
      if (disconnected) {
        std::cout << "\nConnection to server lost. Exiting...\n";
        break;
      }
    }
  }

  [[nodiscard]] int RunSingleCommand(const std::string& command) const {
    std::string response;
    const int max_attempts = config_.wait_ready ? (1 + config_.retry_count) : 1;
    for (int attempt = 0; attempt < max_attempts; ++attempt) {
      if (attempt > 0) {
        std::cerr << "Server is not ready; retrying in " << config_.retry_interval << " seconds... (attempt "
                  << (attempt + 1) << "/" << max_attempts << ")\n";
        sleep(static_cast<unsigned int>(config_.retry_interval));
      }

      response = SendCommand(command);
      if (!IsWaitReadyRetryableResponse(response)) {
        break;
      }
    }
    PrintResponse(response);
    return ExitCodeForSingleCommandResponse(response);
  }

  /**
   * @brief Pretty-print a server response. Public-static for testability.
   */
  static void PrintResponse(const std::string& response) {
    if (response.empty()) {
      std::cout << '\n';
      return;
    }

    // Order matters: check more specific prefixes before more general ones.
    if (StartsWith(response, proto::kOkResultsPrefix)) {
      PrintSearchOrCountResponse(response, /*is_search=*/true);
      return;
    }
    if (StartsWith(response, proto::kOkCountPrefix)) {
      PrintSearchOrCountResponse(response, /*is_search=*/false);
      return;
    }
    if (StartsWith(response, proto::kOkDebugOn)) {
      std::cout << "Debug mode enabled\n";
      return;
    }
    if (StartsWith(response, proto::kOkDebugOff)) {
      std::cout << "Debug mode disabled\n";
      return;
    }
    if (StartsWith(response, proto::kOkOptimizedPrefix)) {
      std::cout << response.substr(proto::kOkPrefixLen) << '\n';
      return;
    }
    if (StartsWith(response, proto::kOkDocPrefix)) {
      std::cout << DecodeGetDocBodyForDisplay(response.substr(proto::kOkPrefixLen)) << '\n';
      return;
    }
    if (StartsWith(response, proto::kOkSavedPrefix)) {
      std::cout << "Snapshot saved to: " << response.substr(kOkSavedPrefixLength) << '\n';
      return;
    }
    if (StartsWith(response, proto::kOkLoadedPrefix)) {
      std::cout << "Snapshot loaded from: " << response.substr(kOkLoadedPrefixLength) << '\n';
      return;
    }
    if (StartsWith(response, proto::kOkReplicationStopped)) {
      std::cout << "Replication stopped successfully\n";
      return;
    }
    if (StartsWith(response, proto::kOkReplicationStarted)) {
      std::cout << "Replication started successfully\n";
      return;
    }
    if (StartsWith(response, proto::kOkInfoPrefix) || StartsWith(response, proto::kOkReplicationPrefix) ||
        StartsWith(response, proto::kOkCacheStatsPrefix) || StartsWith(response, proto::kOkDumpStatusPrefix) ||
        StartsWith(response, proto::kOkSyncStatusPrefix)) {
      // Multi-line responses with potential trailing END marker.
      // Strip leading status line + normalize CRLF + strip END.
      // Note: CONFIG SHOW emits "+OK\r\n..." (handled below), not "OK CONFIG".
      std::cout << FormatMultiLineBody(response) << '\n';
      return;
    }
    if (StartsWith(response, proto::kOkDumpInfoPrefix) || StartsWith(response, proto::kOkDumpStartedPrefix) ||
        StartsWith(response, proto::kOkDumpVerifiedPrefix)) {
      PrintHeaderAndBody(response);
      return;
    }
    if (StartsWith(response, proto::kOkFacetPrefix)) {
      // OK FACET <num>\r\n<value>\t<count>\r\n...
      PrintHeaderAndBody(response);
      return;
    }
    if (StartsWith(response, proto::kOkCacheCleared) || StartsWith(response, proto::kOkCacheEnabled) ||
        StartsWith(response, proto::kOkCacheDisabled) || StartsWith(response, proto::kOkSyncPrefix)) {
      // Single-line status responses; strip "OK "
      std::cout << response.substr(proto::kOkPrefixLen) << '\n';
      return;
    }
    if (StartsWith(response, proto::kPlusOkPrefix)) {
      // Admin / variable responses: "+OK\r\n<body>" or "+OK <message>"
      std::string body = response.substr(proto::kPlusOkPrefixLen);
      if (!body.empty() && (body.front() == '\r' || body.front() == '\n')) {
        body = NormalizeCrlf(std::move(body));
        // Trim leading newlines after normalization
        while (!body.empty() && body.front() == '\n') {
          body.erase(0, 1);
        }
      } else if (!body.empty() && body.front() == ' ') {
        body.erase(0, 1);
      }
      // Trim trailing whitespace/newlines
      while (!body.empty() && (body.back() == '\n' || body.back() == '\r' || body.back() == ' ')) {
        body.pop_back();
      }
      if (body.empty()) {
        std::cout << "OK\n";
      } else {
        std::cout << body << '\n';
      }
      return;
    }
    if (StartsWith(response, proto::kErrorPrefix)) {
      std::cout << "(error) " << response.substr(kErrorPrefixLength) << '\n';
      return;
    }

    // Fallback: print as-is, but normalize CRLF so multi-line responses we
    // forgot to handle still display readably.
    std::cout << NormalizeCrlf(response) << '\n';
  }

  [[nodiscard]] static int ExitCodeForSingleCommandResponse(std::string_view response) {
    const bool server_error = response.size() >= proto::kErrorPrefix.size() &&
                              response.compare(0, proto::kErrorPrefix.size(), proto::kErrorPrefix) == 0;
    const bool client_error = response.rfind("(error)", 0) == 0;
    const bool disconnected = response.find("SERVER_DISCONNECTED") != std::string_view::npos ||
                              response.find("SERVER_TIMEOUT") != std::string_view::npos;
    const bool failure = server_error || client_error || disconnected;
    return failure ? kExitFailure : kExitSuccess;
  }

  [[nodiscard]] static bool IsWaitReadyRetryableResponse(std::string_view response) {
    std::string upper_response = ToUpper(std::string(response));
    const bool error_response =
        StartsWith(upper_response, proto::kErrorPrefix) || upper_response.rfind("(ERROR)", 0) == 0;
    if (!error_response) {
      return false;
    }
    return upper_response.find("SERVER IS LOADING") != std::string::npos ||
           upper_response.find("NOT_READY") != std::string::npos ||
           upper_response.find("NOT READY") != std::string::npos ||
           upper_response.find("REPLICATION IS NOT RUNNING") != std::string::npos ||
           upper_response.find("REPLICATION NOT RUNNING") != std::string::npos;
  }

 private:
  /**
   * @brief Detect commands that should not be added to readline history
   *        (no point storing 'help', 'quit', empty lines, etc.).
   */
  static bool IsTrivialCommand(const std::string& line) { return line == "help" || line == "quit" || line == "exit"; }

  /**
   * @brief Print a response of the form
   *        "OK <verb> <maybe-args>\r\n<key>: <value>\r\n...END" by emitting
   *        the first line with "OK " stripped, then the (CRLF-normalized,
   *        END-stripped) body if there is one.
   */
  static void PrintHeaderAndBody(const std::string& response) {
    constexpr size_t kOkPrefixLen = 3;  // strlen("OK ")
    auto first_lf = response.find('\n');
    std::string first_line = (first_lf == std::string::npos) ? response : response.substr(0, first_lf);
    while (!first_line.empty() && first_line.back() == '\r') {
      first_line.pop_back();
    }
    std::cout << first_line.substr(kOkPrefixLen) << '\n';
    if (first_lf != std::string::npos) {
      std::string body = response.substr(first_lf + 1);
      body = StripTrailingEndMarker(NormalizeCrlf(std::move(body)));
      if (!body.empty()) {
        std::cout << body << '\n';
      }
    }
  }

  static void PrintConnectionHints(const std::string& message) {
    if (message.find("Connection refused") != std::string::npos) {
      std::cerr << "\nPossible reasons:\n"
                << "  1. MygramDB server is not running\n"
                << "  2. Server is still initializing (building initial index from MySQL)\n"
                << "  3. Wrong port (check config.yaml - default is " << kDefaultPort << ")\n"
                << "\nTo check server status:\n"
                << "  ps aux | grep mygramdb\n"
                << "  lsof -i -P | grep LISTEN | grep " << kDefaultPort << "\n"
                << "\nFor large datasets, initial index build may take 10-30 minutes.\n"
                << "Server will start accepting connections after initialization completes.\n";
    } else if (message.find("Operation timed out") != std::string::npos ||
               message.find("timed out") != std::string::npos) {
      std::cerr << "\nServer is not responding. Check if the server is running and network is accessible.\n";
    } else if (message.find("Network is unreachable") != std::string::npos ||
               message.find("No route to host") != std::string::npos) {
      std::cerr << "\nNetwork is unreachable. Check hostname and network connectivity.\n";
    } else if (message.find("Failed to resolve host") != std::string::npos) {
      std::cerr << "\nHostname resolution failed. Check the host name and DNS configuration.\n";
    } else if (message.find("No such file or directory") != std::string::npos) {
      std::cerr << "\nUnix socket not found. Verify the server is running and the path is correct.\n";
    }
  }

  static void PrintHelp() {
    std::cout << "Available commands:\n"
              << "  SEARCH <db.table> <text> [(AND|OR|NOT) <term>...] [FILTER <col=val>...]\n"
              << "         [SORT [BY] <col>|ASC|DESC] [LIMIT <n>] [OFFSET <n>]\n"
              << "  COUNT <db.table> <text> [(AND|OR|NOT) <term>...] [FILTER <col=val>...]\n"
              << "  GET <db.table> <primary_key>\n"
              << "  INFO              - Show server statistics\n"
              << "  CONFIG            - Show current configuration\n"
              << "  SET <variable> = <value> [, ...]         - Change runtime variables\n"
              << "  SHOW VARIABLES [LIKE <pattern>]         - Show runtime variables\n"
              << "  FACET <db.table> <column> [text] [FILTER <col=val>...] [LIMIT <n>]\n"
              << "                    - Compute facet counts\n"
              << "  REPLICATION STATUS|STOP|START\n"
              << "  DEBUG ON|OFF      - Toggle per-connection debug output\n"
              << "  OPTIMIZE [db.table]  - Compact posting lists for one or all tables\n"
              << "  CACHE STATS|CLEAR|ENABLE|DISABLE\n"
              << "  DUMP SAVE|LOAD|VERIFY|INFO|STATUS\n"
              << "  SYNC <db.table>|STOP [db.table]|STATUS\n"
              << '\n'
              << "Query syntax examples:\n"
              << "  SEARCH app.threads golang                          # Simple search\n"
              << "  SEARCH app.threads (golang OR python) AND tutorial # Boolean query\n"
              << "  SEARCH app.threads golang SORT DESC LIMIT 10       # With sorting\n"
              << "  SEARCH app.threads golang SORT BY created_at ASC   # Sort by column\n"
              << '\n'
              << "Other commands:\n"
              << "  quit/exit - Exit the client\n"
              << "  help - Show this help\n";
  }

  /**
   * @brief Format the body of a multi-line response by stripping the leading
   *        status line (up to the first newline), normalizing CRLF→LF, and
   *        removing the trailing "END" marker (if any).
   */
  static std::string FormatMultiLineBody(const std::string& response) {
    auto first_lf = response.find('\n');
    if (first_lf == std::string::npos) {
      return NormalizeCrlf(response);
    }
    std::string body = response.substr(first_lf + 1);
    body = NormalizeCrlf(std::move(body));
    // Strip leading blank lines (server emits "OK INFO\r\n\r\n")
    while (!body.empty() && body.front() == '\n') {
      body.erase(0, 1);
    }
    body = StripTrailingEndMarker(std::move(body));
    return body;
  }

  static void PrintSearchOrCountResponse(const std::string& response, bool is_search) {
    // The response may contain a DEBUG block separated by "\r\n\r\n".
    size_t debug_separator = response.find("\r\n\r\n");
    std::string main_response = (debug_separator != std::string::npos) ? response.substr(0, debug_separator) : response;
    std::string debug_section = (debug_separator != std::string::npos) ? response.substr(debug_separator + 4) : "";

    std::istringstream iss(main_response);
    std::string status;
    std::string keyword;
    iss >> status >> keyword;

    if (is_search) {
      uint64_t count = 0;
      iss >> count;

      std::vector<std::string> ids;
      std::vector<std::string> snippets;

      if (main_response.find('\n') == std::string::npos) {
        std::string token;
        while (iss >> token) {
          ids.push_back(token);
        }
      } else {
        size_t first_line_end = main_response.find('\n');
        std::istringstream rows(main_response.substr(first_line_end + 1));
        std::string line;
        while (std::getline(rows, line)) {
          if (!line.empty() && line.back() == '\r') {
            line.pop_back();
          }
          if (line.empty()) {
            continue;
          }

          size_t tab_pos = line.find('\t');
          if (tab_pos == std::string::npos) {
            ids.push_back(line);
            snippets.emplace_back();
          } else {
            ids.push_back(line.substr(0, tab_pos));
            snippets.push_back(line.substr(tab_pos + 1));
          }
        }
      }

      std::cout << '(' << count << " results";
      if (!ids.empty()) {
        std::cout << ", showing " << ids.size() << ')';
      } else {
        std::cout << ')';
      }
      std::cout << '\n';

      for (size_t i = 0; i < ids.size(); ++i) {
        std::cout << (i + 1) << ") " << ids[i] << '\n';
        if (i < snippets.size() && !snippets[i].empty()) {
          std::cout << "   " << NormalizeCrlf(snippets[i]) << '\n';
        }
      }
    } else {
      uint64_t count = 0;
      iss >> count;
      std::cout << "(integer) " << count << '\n';
    }

    if (!debug_section.empty()) {
      std::cout << '\n' << NormalizeCrlf(debug_section);
      if (debug_section.back() != '\n' && debug_section.back() != '\r') {
        std::cout << '\n';
      }
    }
  }

  Config config_;
  std::unique_ptr<lib::MygramClient> client_;
};

// =============================================================================
// Argument parsing
// =============================================================================

void PrintUsage(const char* program_name) {
  std::cout << "Usage: " << program_name << " [OPTIONS] [COMMAND]\n"
            << '\n'
            << "Options:\n"
            << "  -h HOST         Server hostname (default: 127.0.0.1)\n"
            << "  -p PORT         Server port (default: " << kDefaultPort << ")\n"
            << "  -s SOCKET_PATH  Unix domain socket path (overrides -h/-p)\n"
            << "  --retry N       Retry connection N times if refused (default: 0)\n"
            << "  --wait-ready    Keep retrying until server is ready (max " << kMaxWaitReadyRetries << " attempts)\n"
            << "  --version       Show client version and exit\n"
            << "  --help          Show this help\n"
            << '\n'
            << "Examples:\n"
            << "  " << program_name << "                          # Interactive mode\n"
            << "  " << program_name << " -h localhost -p " << kDefaultPort
            << "    # Connect to specific server (hostnames supported)\n"
            << "  " << program_name << " --retry 5 INFO           # Retry connection 5 times if refused\n"
            << "  " << program_name << " --wait-ready INFO        # Wait until server is ready\n"
            << "  " << program_name << " SEARCH app.articles hello # Execute single command\n"
            << "  " << program_name << " -s /tmp/mygramdb.sock INFO # Connect via Unix socket\n";
}

void PrintVersion() {
  std::cout << "mygram-cli " << ::mygramdb::Version::FullString() << '\n';
}

struct ParseResult {
  Config config;
  std::vector<std::string> command_args;
  bool exit_now = false;
  int exit_code = 0;
};

// NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays) -- standard main()-style signature
ParseResult ParseArguments(int argc, char* argv[]) {
  ParseResult result;
  for (int i = 1; i < argc; ++i) {
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    std::string arg(argv[i]);

    if (arg == "--help") {
      // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
      PrintUsage(argv[0]);
      result.exit_now = true;
      return result;
    }
    if (arg == "--version" || arg == "-V") {
      PrintVersion();
      result.exit_now = true;
      return result;
    }
    if (arg == "-h") {
      if (i + 1 >= argc) {
        std::cerr << "Error: -h requires an argument\n";
        result.exit_now = true;
        result.exit_code = 1;
        return result;
      }
      // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
      result.config.host = argv[++i];
    } else if (arg == "-p") {
      if (i + 1 >= argc) {
        std::cerr << "Error: -p requires an argument\n";
        result.exit_now = true;
        result.exit_code = 1;
        return result;
      }
      // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
      auto port_result = ParsePort(argv[++i]);
      if (!port_result.ok) {
        std::cerr << "Error: " << port_result.error << '\n';
        result.exit_now = true;
        result.exit_code = 1;
        return result;
      }
      result.config.port = port_result.port;
    } else if (arg == "-s") {
      if (i + 1 >= argc) {
        std::cerr << "Error: -s requires an argument\n";
        result.exit_now = true;
        result.exit_code = 1;
        return result;
      }
      // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
      result.config.socket_path = argv[++i];
    } else if (arg == "--retry") {
      if (i + 1 >= argc) {
        std::cerr << "Error: --retry requires an argument\n";
        result.exit_now = true;
        result.exit_code = 1;
        return result;
      }
      // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
      auto count = mygram::utils::ParseNumeric<int32_t>(argv[++i]);
      if (!count.has_value() || *count < 0) {
        std::cerr << "Error: --retry value must be a non-negative integer\n";
        result.exit_now = true;
        result.exit_code = 1;
        return result;
      }
      result.config.retry_count = *count;
    } else if (arg == "--wait-ready") {
      result.config.wait_ready = true;
    } else {
      // First non-option arg starts the command
      for (int j = i; j < argc; ++j) {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        result.command_args.emplace_back(argv[j]);
      }
      result.config.interactive = false;
      break;
    }
  }
  if (result.config.wait_ready && result.config.retry_count < kMaxWaitReadyRetries) {
    result.config.retry_count = kMaxWaitReadyRetries;
  }
  return result;
}

}  // namespace

int main(int argc, char* argv[]) {
  // Ignore SIGPIPE so a broken connection raises an errno (EPIPE) instead of
  // silently killing the process during send().
  signal(SIGPIPE, SIG_IGN);

  ParseResult parsed = ParseArguments(argc, argv);
  if (parsed.exit_now) {
    return parsed.exit_code;
  }

  MygramClient client(parsed.config);
  if (!client.Connect()) {
    return 1;
  }

  if (parsed.config.interactive) {
    client.RunInteractive();
  } else {
    return client.RunSingleCommand(JoinArgsForCommand(parsed.command_args));
  }
  return 0;
}
