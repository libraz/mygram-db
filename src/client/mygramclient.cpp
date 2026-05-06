/**
 * @file mygramclient.cpp
 * @brief Implementation of MygramDB client library
 */

#include "client/mygramclient.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/un.h>
#include <unistd.h>

#include <cctype>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <limits>
#include <mutex>
#include <sstream>
#include <thread>
#include <utility>

#include "client/protocol_detection.h"
#include "server/protocol_constants.h"
#include "utils/constants.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/numeric_parse.h"

using namespace mygram::utils;

namespace mygramdb::client {

// Protocol constants alias - 1:1 reuse of shared protocol header.
namespace proto = mygramdb::server::protocol;

namespace {

constexpr int64_t kMillisecondsPerSecond = mygram::constants::kMillisecondsPerSecond;
constexpr int64_t kMicrosecondsPerMillisecond = mygram::constants::kMicrosecondsPerMillisecond;

/**
 * @brief Check if response is an error and return appropriate Expected
 */
Expected<void, Error> CheckErrorResponse(const std::string& response) {
  if (response.find("ERROR") == 0) {
    return MakeUnexpected(MakeError(ErrorCode::kClientServerError, response.substr(proto::kErrorPrefixLen)));
  }
  return {};
}

/**
 * @brief Trim ASCII whitespace (space, tab, CR, LF) from both ends
 */
std::string TrimAsciiWhitespace(std::string_view str) {
  size_t start = str.find_first_not_of(" \t\r\n");
  if (start == std::string_view::npos) {
    return std::string();
  }
  size_t end = str.find_last_not_of(" \t\r\n");
  return std::string(str.substr(start, end - start + 1));
}

/**
 * @brief Parse "key: value" formatted lines (Redis-style) into pairs
 *
 * Splits the input on "\r\n" (or "\n"), skips empty lines and comment lines
 * starting with '#', and returns each remaining line as a (key, value) pair.
 * Both key and value are whitespace-trimmed. Lines without ':' are ignored.
 *
 * @param str Raw response body (may contain a leading status line)
 * @return Vector of trimmed key/value pairs in source order
 */
std::vector<std::pair<std::string, std::string>> ParseColonKeyValueLines(const std::string& str) {
  std::vector<std::pair<std::string, std::string>> pairs;
  size_t pos = 0;
  while (pos < str.size()) {
    size_t end = str.find('\n', pos);
    std::string_view line;
    if (end == std::string::npos) {
      line = std::string_view(str).substr(pos);
      pos = str.size();
    } else {
      line = std::string_view(str).substr(pos, end - pos);
      pos = end + 1;
    }
    // Strip optional trailing '\r'
    if (!line.empty() && line.back() == '\r') {
      line.remove_suffix(1);
    }
    if (line.empty() || line.front() == '#') {
      continue;
    }
    size_t colon = line.find(':');
    if (colon == std::string_view::npos) {
      continue;
    }
    std::string key = TrimAsciiWhitespace(line.substr(0, colon));
    std::string value = TrimAsciiWhitespace(line.substr(colon + 1));
    if (key.empty()) {
      continue;
    }
    pairs.emplace_back(std::move(key), std::move(value));
  }
  return pairs;
}

/**
 * @brief Strip a trailing "ms" suffix from a numeric time string
 *
 * The server emits time-valued debug fields as "1.234ms"; this helper
 * removes the suffix so the remainder can be parsed by ParseNumeric<double>.
 */
std::string StripMillisecondSuffix(const std::string& value) {
  constexpr std::string_view kMs = "ms";
  if (value.size() >= kMs.size() && value.compare(value.size() - kMs.size(), kMs.size(), kMs) == 0) {
    return TrimAsciiWhitespace(std::string_view(value).substr(0, value.size() - kMs.size()));
  }
  return value;
}

/**
 * @brief Parse the server's "# DEBUG" block (line-based, key: value)
 *
 * The block is delimited from the main response by a blank line ("\r\n\r\n")
 * and starts with a "# DEBUG" header. Subsequent lines have the form
 * "key: value\r\n", with time fields suffixed by "ms". Unknown keys are
 * ignored for forward compatibility.
 *
 * @param debug_block The raw debug section text (may include the "# DEBUG"
 *                    header line and trailing CRLFs)
 * @return Populated DebugInfo, or std::nullopt if no recognised fields parsed
 */
std::optional<DebugInfo> ParseDebugInfo(const std::string& debug_block) {
  auto pairs = ParseColonKeyValueLines(debug_block);
  if (pairs.empty()) {
    return std::nullopt;
  }

  DebugInfo info;
  for (const auto& [key, value] : pairs) {
    if (key == "query_time") {
      info.query_time_ms = mygram::utils::ParseNumeric<double>(StripMillisecondSuffix(value)).value_or(0.0);
    } else if (key == "index_time") {
      info.index_time_ms = mygram::utils::ParseNumeric<double>(StripMillisecondSuffix(value)).value_or(0.0);
    } else if (key == "filter_time") {
      info.filter_time_ms = mygram::utils::ParseNumeric<double>(StripMillisecondSuffix(value)).value_or(0.0);
    } else if (key == "terms") {
      info.terms = mygram::utils::ParseNumeric<uint32_t>(value).value_or(0);
    } else if (key == "ngrams") {
      info.ngrams = mygram::utils::ParseNumeric<uint32_t>(value).value_or(0);
    } else if (key == "candidates") {
      info.candidates = mygram::utils::ParseNumeric<uint64_t>(value).value_or(0);
    } else if (key == "after_intersection") {
      info.after_intersection = mygram::utils::ParseNumeric<uint64_t>(value).value_or(0);
    } else if (key == "after_not") {
      info.after_not = mygram::utils::ParseNumeric<uint64_t>(value).value_or(0);
    } else if (key == "after_filters") {
      info.after_filters = mygram::utils::ParseNumeric<uint64_t>(value).value_or(0);
    } else if (key == "final") {
      info.final = mygram::utils::ParseNumeric<uint64_t>(value).value_or(0);
    } else if (key == "optimization") {
      info.optimization = value;
    }
    // Unknown keys (e.g. sort, limit, offset, cache_*, highlight) are ignored.
  }

  return info;
}

/**
 * @brief Split the raw response into (main, debug) sections
 *
 * The server appends an optional "# DEBUG" block to SEARCH/COUNT responses,
 * separated by a blank line ("\r\n\r\n"). Returns the main portion and an
 * optional debug portion (without the leading separator). If no separator
 * exists, debug is empty and main equals the input.
 */
std::pair<std::string, std::string> SplitDebugBlock(const std::string& response) {
  size_t pos = response.find("\r\n\r\n");
  if (pos == std::string::npos) {
    return {response, std::string()};
  }
  return {response.substr(0, pos), response.substr(pos + 4)};
}

/**
 * @brief Parse key=value pairs from a whitespace-tokenised string
 *
 * Used for response fragments where the server emits "key=value" tokens
 * separated by whitespace (e.g. GET document filter fields).
 */
std::vector<std::pair<std::string, std::string>> ParseKeyValuePairs(const std::string& str) {
  std::vector<std::pair<std::string, std::string>> pairs;
  std::istringstream iss(str);
  std::string token;

  while (iss >> token) {
    size_t pos = token.find('=');
    if (pos != std::string::npos) {
      std::string key = token.substr(0, pos);
      std::string value = token.substr(pos + 1);
      pairs.emplace_back(std::move(key), std::move(value));
    }
  }

  return pairs;
}

/**
 * @brief Validate that a string does not contain ASCII control characters
 */
std::optional<std::string> ValidateNoControlCharacters(const std::string& value, const char* field_name) {
  for (unsigned char character : value) {
    if (std::iscntrl(character) != 0) {
      std::ostringstream oss;
      oss << "Input for " << field_name << " contains control character 0x" << std::uppercase << std::hex
          << std::setw(2) << std::setfill('0') << static_cast<int>(character) << ", which is not allowed";
      return oss.str();
    }
  }

  return std::nullopt;
}

/**
 * @brief Escape special characters in query strings
 *
 * Empty strings are emitted as the explicit `""` token to keep the wire
 * form unambiguous: an unquoted empty arg would collapse into surrounding
 * whitespace and produce a malformed command (e.g. `SEARCH table  AND foo`).
 */
std::string EscapeQueryString(const std::string& str) {
  // Empty strings must be quoted so the server sees an explicit empty token.
  if (str.empty()) {
    return "\"\"";
  }

  // Check if string needs quoting (contains spaces or special chars)
  bool needs_quotes = false;
  for (char character : str) {
    if (character == ' ' || character == '\t' || character == '\n' || character == '\r' || character == '"' ||
        character == '\'') {
      needs_quotes = true;
      break;
    }
  }

  if (!needs_quotes) {
    return str;
  }

  // Use double quotes and escape internal quotes
  std::string result = "\"";
  for (char character : str) {
    if (character == '"' || character == '\\') {
      result += '\\';
      result += character;
    } else if (static_cast<unsigned char>(character) < 0x20) {
      // Skip control characters to prevent command injection
      continue;
    } else {
      result += character;
    }
  }
  result += '"';
  return result;
}

/**
 * @brief Validate that an identifier-like value is non-empty and has no whitespace/control chars
 *
 * Used for unquoted identifiers (table names, primary keys, sort columns, filter keys)
 * that are sent on the wire without quoting. Embedded whitespace would break the protocol
 * by splitting the value into multiple tokens.
 *
 * @param value Identifier value
 * @param field_name Human-readable field name for error messages
 * @return nullopt on success, or error message string on failure
 */
std::optional<std::string> ValidateIdentifier(const std::string& value, const char* field_name) {
  if (value.empty()) {
    std::ostringstream oss;
    oss << "Input for " << field_name << " is empty";
    return oss.str();
  }
  for (unsigned char character : value) {
    if (std::iscntrl(character) != 0) {
      std::ostringstream oss;
      oss << "Input for " << field_name << " contains control character 0x" << std::uppercase << std::hex
          << std::setw(2) << std::setfill('0') << static_cast<int>(character) << ", which is not allowed";
      return oss.str();
    }
    if (std::isspace(character) != 0) {
      std::ostringstream oss;
      oss << "Input for " << field_name << " contains whitespace, which is not allowed in identifiers";
      return oss.str();
    }
  }

  return std::nullopt;
}

/**
 * @brief Validate common search inputs for control characters
 *
 * Checks table (identifier), query (free-form), and/not terms, and filter keys
 * (identifier) / filter values (free-form).
 *
 * @return nullopt on success, or error message string on failure
 */
std::optional<std::string> ValidateSearchInputs(const std::string& table, const std::string& query,
                                                const std::vector<std::string>& and_terms,
                                                const std::vector<std::string>& not_terms,
                                                const std::vector<std::pair<std::string, std::string>>& filters) {
  if (auto err = ValidateIdentifier(table, "table name")) {
    return err;
  }
  if (auto err = ValidateNoControlCharacters(query, "search query")) {
    return err;
  }
  for (const auto& term : and_terms) {
    if (auto err = ValidateNoControlCharacters(term, "AND term")) {
      return err;
    }
  }
  for (const auto& term : not_terms) {
    if (auto err = ValidateNoControlCharacters(term, "NOT term")) {
      return err;
    }
  }
  for (const auto& [key, value] : filters) {
    if (auto err = ValidateIdentifier(key, "filter key")) {
      return err;
    }
    if (auto err = ValidateNoControlCharacters(value, "filter value")) {
      return err;
    }
  }
  return std::nullopt;
}

/**
 * @brief Apply SO_RCVTIMEO and SO_SNDTIMEO to a socket
 *
 * These timeouts govern subsequent send()/recv() operations only; they do
 * not affect connect(). Failures are intentionally swallowed: the client
 * library is deliberately self-contained and does not link spdlog or any
 * other structured logger (see src/client/CMakeLists.txt — the client lib
 * has no link dependencies so it can be embedded by FFI bindings without
 * pulling in server-side libraries). A setsockopt() failure here just
 * means the socket runs with kernel-default timeouts, which is acceptable
 * (the bounded poll() in ConnectWithTimeout already prevents an unbounded
 * connect-time hang).
 *
 * @param sock Socket file descriptor
 * @param timeout_ms Timeout in milliseconds
 */
void ApplySocketTimeouts(int sock, uint32_t timeout_ms) {
  struct timeval timeout_val = {};
  timeout_val.tv_sec = static_cast<decltype(timeout_val.tv_sec)>(timeout_ms / kMillisecondsPerSecond);
  timeout_val.tv_usec =
      static_cast<decltype(timeout_val.tv_usec)>((timeout_ms % kMillisecondsPerSecond) * kMicrosecondsPerMillisecond);
  // Intentionally silent on failure (see function-level comment).
  (void)setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &timeout_val, sizeof(timeout_val));
  (void)setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeout_val, sizeof(timeout_val));
}

/**
 * @brief Connect a socket with a bounded timeout
 *
 * Performs a non-blocking connect() and waits at most @p timeout_ms for the
 * connection to complete (or fail). Restores the socket to blocking mode on
 * success so subsequent send/recv calls observe SO_RCVTIMEO/SO_SNDTIMEO.
 *
 * @param sock       Connected socket descriptor (must be open)
 * @param addr       Destination address
 * @param addrlen    Length of @p addr
 * @param timeout_ms Maximum time to wait for the connect to complete (ms)
 * @return Empty on success; error with kClientTimeout or kClientConnectionFailed on failure.
 */
Expected<void, Error> ConnectWithTimeout(int sock, const sockaddr* addr, socklen_t addrlen, uint32_t timeout_ms) {
  // Mark the socket non-blocking for the connect() call.
  int original_flags = fcntl(sock, F_GETFL, 0);
  if (original_flags < 0) {
    return MakeUnexpected(
        MakeError(ErrorCode::kClientConnectionFailed, std::string("fcntl(F_GETFL) failed: ") + strerror(errno)));
  }
  if (fcntl(sock, F_SETFL, original_flags | O_NONBLOCK) < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kClientConnectionFailed,
                                    std::string("fcntl(F_SETFL O_NONBLOCK) failed: ") + strerror(errno)));
  }

  int connect_result = connect(sock, addr, addrlen);
  if (connect_result == 0) {
    // Connected immediately - restore blocking mode and return.
    if (fcntl(sock, F_SETFL, original_flags) < 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientConnectionFailed,
                                      std::string("fcntl(F_SETFL restore) failed: ") + strerror(errno)));
    }
    return {};
  }

  // Any non-success return that isn't EINPROGRESS / EWOULDBLOCK is a hard failure.
  if (errno != EINPROGRESS && errno != EWOULDBLOCK) {
    return MakeUnexpected(
        MakeError(ErrorCode::kClientConnectionFailed, std::string("Connection failed: ") + strerror(errno)));
  }

  // Wait for socket to become writable (or timeout).
  struct pollfd pfd {};
  pfd.fd = sock;
  pfd.events = POLLOUT;

  // poll() takes int milliseconds; clamp to int max to avoid overflow.
  int poll_timeout = (timeout_ms > static_cast<uint32_t>(std::numeric_limits<int>::max()))
                         ? std::numeric_limits<int>::max()
                         : static_cast<int>(timeout_ms);

  int poll_result = 0;
  while (true) {
    poll_result = poll(&pfd, 1, poll_timeout);
    if (poll_result >= 0 || errno != EINTR) {
      break;
    }
    // EINTR: retry. We do not adjust the remaining timeout for simplicity;
    // a short signal storm on a long-running connect is acceptable.
  }

  if (poll_result == 0) {
    return MakeUnexpected(
        MakeError(ErrorCode::kClientTimeout, "Connection timed out after " + std::to_string(timeout_ms) + " ms"));
  }
  if (poll_result < 0) {
    return MakeUnexpected(
        MakeError(ErrorCode::kClientConnectionFailed, std::string("poll() failed: ") + strerror(errno)));
  }

  // Check whether the connect() actually succeeded.
  int so_error = 0;
  socklen_t so_error_len = sizeof(so_error);
  if (getsockopt(sock, SOL_SOCKET, SO_ERROR, &so_error, &so_error_len) < 0) {
    return MakeUnexpected(
        MakeError(ErrorCode::kClientConnectionFailed, std::string("getsockopt(SO_ERROR) failed: ") + strerror(errno)));
  }
  if (so_error != 0) {
    return MakeUnexpected(
        MakeError(ErrorCode::kClientConnectionFailed, std::string("Connection failed: ") + strerror(so_error)));
  }

  // Restore blocking mode so subsequent send/recv use SO_RCVTIMEO/SO_SNDTIMEO.
  if (fcntl(sock, F_SETFL, original_flags) < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kClientConnectionFailed,
                                    std::string("fcntl(F_SETFL restore) failed: ") + strerror(errno)));
  }

  return {};
}

}  // namespace

/**
 * @brief PIMPL implementation class
 */
class MygramClient::Impl {
 public:
  explicit Impl(ClientConfig config) : config_(std::move(config)) {}

  ~Impl() { Disconnect(); }

  // Non-copyable, non-movable.
  // (std::mutex is neither copyable nor movable; the public MygramClient
  // wraps Impl in a unique_ptr to provide move semantics.)
  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;
  Impl(Impl&&) = delete;
  Impl& operator=(Impl&&) = delete;

  Expected<void, Error> Connect() {
    if (sock_ >= 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientAlreadyConnected, "Already connected"));
    }

    if (!config_.unix_socket_path.empty()) {
      sock_ = socket(AF_UNIX, SOCK_STREAM, 0);
      if (sock_ < 0) {
        return MakeUnexpected(MakeError(ErrorCode::kClientConnectionFailed,
                                        std::string("Failed to create unix socket: ") + strerror(errno)));
      }

      struct sockaddr_un server_addr {};
      server_addr.sun_family = AF_UNIX;
      std::strncpy(server_addr.sun_path, config_.unix_socket_path.c_str(), sizeof(server_addr.sun_path) - 1);

      // Bounded-timeout connect (non-blocking + poll).
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for socket API
      auto connect_result = ConnectWithTimeout(sock_, reinterpret_cast<const sockaddr*>(&server_addr),
                                               sizeof(server_addr), config_.timeout_ms);
      if (!connect_result) {
        close(sock_);
        sock_ = -1;
        // Preserve the original error code (kClientTimeout vs kClientConnectionFailed)
        // but augment the message to indicate the unix socket path.
        const auto& orig = connect_result.error();
        return MakeUnexpected(MakeError(orig.code(), std::string("Unix socket ") + orig.message()));
      }

      // Apply send/recv timeouts now that we are connected.
      ApplySocketTimeouts(sock_, config_.timeout_ms);

      return {};
    }

    sock_ = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_ < 0) {
      return MakeUnexpected(
          MakeError(ErrorCode::kClientConnectionFailed, std::string("Failed to create socket: ") + strerror(errno)));
    }

    struct sockaddr_in server_addr = {};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(config_.port);

    {
      struct addrinfo hints {
      }, *addr_result = nullptr;
      hints.ai_family = AF_INET;
      hints.ai_socktype = SOCK_STREAM;
      int gai_err = getaddrinfo(config_.host.c_str(), nullptr, &hints, &addr_result);
      if (gai_err != 0 || addr_result == nullptr) {
        close(sock_);
        sock_ = -1;
        // Surface the gai_strerror text so callers see "Name or service not
        // known" / "Temporary failure in name resolution" / etc. rather than
        // a generic message.
        const char* gai_msg = (gai_err != 0) ? gai_strerror(gai_err) : "no addresses returned";
        return MakeUnexpected(MakeError(
            ErrorCode::kClientConnectionFailed,
            "Failed to resolve host '" + config_.host + "': " + (gai_msg != nullptr ? gai_msg : "unknown error")));
      }
      // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for socket API
      server_addr.sin_addr = reinterpret_cast<struct sockaddr_in*>(addr_result->ai_addr)->sin_addr;
      freeaddrinfo(addr_result);
    }

    // Bounded-timeout connect (non-blocking + poll).
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for socket API
    auto connect_result = ConnectWithTimeout(sock_, reinterpret_cast<const sockaddr*>(&server_addr),
                                             sizeof(server_addr), config_.timeout_ms);
    if (!connect_result) {
      close(sock_);
      sock_ = -1;
      return MakeUnexpected(connect_result.error());
    }

    // Apply send/recv timeouts now that we are connected.
    ApplySocketTimeouts(sock_, config_.timeout_ms);

    return {};
  }

  void Disconnect() {
    if (sock_ >= 0) {
      close(sock_);
      sock_ = -1;
    }
  }

  [[nodiscard]] bool IsConnected() const { return sock_ >= 0; }

  /**
   * @brief Send a command and validate that the response begins with @p expected_prefix.
   *
   * Wraps the common pattern: SendCommand -> CheckErrorResponse -> prefix check.
   * If @p expected_prefix is empty, the prefix check is skipped and the raw
   * response is returned (useful for void-returning commands that just need
   * error detection).
   */
  Expected<std::string, Error> SendAndExpectPrefix(const std::string& cmd, std::string_view expected_prefix) const {
    auto result = SendCommand(cmd);
    if (!result) {
      return MakeUnexpected(result.error());
    }
    std::string response = *result;
    auto err = CheckErrorResponse(response);
    if (!err) {
      return MakeUnexpected(err.error());
    }
    if (!expected_prefix.empty() && response.find(expected_prefix) != 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientProtocolError, "Unexpected response format"));
    }
    return response;
  }

  Expected<std::string, Error> SendCommand(const std::string& command) const {
    // Serialize concurrent SendCommand() calls so multiple threads do not
    // interleave send()/recv() byte streams on the same socket. The lock
    // is held across both send() and the full recv() loop, so each command
    // observes a complete request/response transaction.
    std::lock_guard<std::mutex> lock(command_mutex_);

    if (!IsConnected()) {
      return MakeUnexpected(MakeError(ErrorCode::kClientNotConnected, "Not connected"));
    }

    // Send command with \r\n terminator
    std::string msg = command + "\r\n";
    size_t total_sent = 0;
    while (total_sent < msg.size()) {
      ssize_t sent = send(sock_, msg.c_str() + total_sent, msg.size() - total_sent, 0);
      if (sent < 0) {
        return MakeUnexpected(
            MakeError(ErrorCode::kClientCommandFailed, std::string("Failed to send command: ") + strerror(errno)));
      }
      total_sent += static_cast<size_t>(sent);
    }

    // Receive response (loop until complete response is received)
    std::string response;
    std::vector<char> buffer(config_.recv_buffer_size);

    while (true) {
      ssize_t received = recv(sock_, buffer.data(), buffer.size() - 1, 0);
      if (received <= 0) {
        if (received == 0) {
          return MakeUnexpected(MakeError(ErrorCode::kClientConnectionClosed, "Connection closed by server"));
        }
        return MakeUnexpected(
            MakeError(ErrorCode::kClientCommandFailed, std::string("Failed to receive response: ") + strerror(errno)));
      }

      response.append(buffer.data(), static_cast<size_t>(received));

      // Check if the accumulated response is complete
      if (detail::IsResponseComplete(response)) {
        break;
      }
    }

    // Remove trailing \r\n (strip all trailing CR/LF characters)
    while (!response.empty() && (response.back() == '\n' || response.back() == '\r')) {
      response.pop_back();
    }

    return response;
  }

  Expected<SearchResponse, Error> Search(const std::string& table, const std::string& query, uint32_t limit,
                                         uint32_t offset, const std::vector<std::string>& and_terms,
                                         const std::vector<std::string>& not_terms,
                                         const std::vector<std::pair<std::string, std::string>>& filters,
                                         const std::string& sort_column, bool sort_desc) const {
    if (auto err = ValidateSearchInputs(table, query, and_terms, not_terms, filters)) {
      return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
    }
    if (!sort_column.empty()) {
      // sort_column is an identifier sent unquoted on the wire; reject whitespace.
      if (auto err = ValidateIdentifier(sort_column, "sort column")) {
        return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
      }
    }

    // Build command
    std::ostringstream cmd;
    cmd << "SEARCH " << table << " " << EscapeQueryString(query);

    for (const auto& term : and_terms) {
      cmd << " AND " << EscapeQueryString(term);
    }

    for (const auto& term : not_terms) {
      cmd << " NOT " << EscapeQueryString(term);
    }

    for (const auto& [key, value] : filters) {
      cmd << " FILTER " << key << " = " << EscapeQueryString(value);
    }

    // SORT clause (replaces ORDER BY)
    if (!sort_column.empty()) {
      cmd << " SORT " << sort_column << (sort_desc ? " DESC" : " ASC");
    } else if (!sort_desc) {
      // Only add SORT ASC if explicitly requesting ascending order for primary key
      cmd << " SORT ASC";
    }
    // Default is SORT DESC (primary key descending), so no need to add it explicitly

    // LIMIT / OFFSET clauses.
    // - When both are set, prefer the atomic MySQL-style "LIMIT offset,count" form.
    // - When only LIMIT is set, emit "LIMIT <n>".
    // - When only OFFSET is set, emit "OFFSET <n>" so the server still skips the
    //   first <n> results (the previous behaviour silently dropped the offset).
    if (limit > 0 && offset > 0) {
      cmd << " LIMIT " << offset << "," << limit;
    } else if (limit > 0) {
      cmd << " LIMIT " << limit;
    } else if (offset > 0) {
      cmd << " OFFSET " << offset;
    }

    // Parse response: "OK RESULTS <total_count> [<id1> <id2> ...]"
    // optionally followed by "\r\n\r\n# DEBUG\r\n<key: value>...".
    auto result = SendAndExpectPrefix(cmd.str(), "OK RESULTS");
    if (!result) {
      return MakeUnexpected(result.error());
    }
    const std::string& response = *result;

    auto [main_part, debug_part] = SplitDebugBlock(response);

    std::istringstream iss(main_part);
    std::string status;
    std::string results_str;
    uint64_t total_count = 0;
    iss >> status >> results_str >> total_count;

    SearchResponse resp;
    resp.total_count = total_count;

    // Remaining whitespace-separated tokens on the main line are primary keys.
    std::string token;
    while (iss >> token) {
      resp.results.emplace_back(token);
    }

    if (!debug_part.empty()) {
      resp.debug = ParseDebugInfo(debug_part);
    }

    return resp;
  }

  Expected<CountResponse, Error> Count(const std::string& table, const std::string& query,
                                       const std::vector<std::string>& and_terms,
                                       const std::vector<std::string>& not_terms,
                                       const std::vector<std::pair<std::string, std::string>>& filters) const {
    if (auto err = ValidateSearchInputs(table, query, and_terms, not_terms, filters)) {
      return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
    }

    // Build command
    std::ostringstream cmd;
    cmd << "COUNT " << table << " " << EscapeQueryString(query);

    for (const auto& term : and_terms) {
      cmd << " AND " << EscapeQueryString(term);
    }

    for (const auto& term : not_terms) {
      cmd << " NOT " << EscapeQueryString(term);
    }

    for (const auto& [key, value] : filters) {
      cmd << " FILTER " << key << " = " << EscapeQueryString(value);
    }

    // Parse response: "OK COUNT <n>" optionally followed by
    // "\r\n\r\n# DEBUG\r\n<key: value>...".
    auto result = SendAndExpectPrefix(cmd.str(), "OK COUNT");
    if (!result) {
      return MakeUnexpected(result.error());
    }
    const std::string& response = *result;

    auto [main_part, debug_part] = SplitDebugBlock(response);

    std::istringstream iss(main_part);
    std::string status;
    std::string count_str;
    uint64_t count = 0;
    iss >> status >> count_str >> count;

    CountResponse resp;
    resp.count = count;

    if (!debug_part.empty()) {
      resp.debug = ParseDebugInfo(debug_part);
    }

    return resp;
  }

  Expected<Document, Error> Get(const std::string& table, const std::string& primary_key) const {
    // table and primary_key are identifiers sent unquoted on the wire; reject
    // whitespace, control characters and empty values.
    if (auto err = ValidateIdentifier(table, "table name")) {
      return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
    }
    if (auto err = ValidateIdentifier(primary_key, "primary key")) {
      return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
    }

    std::ostringstream cmd;
    cmd << "GET " << table << " " << primary_key;

    // Parse response: OK DOC <primary_key> [<key=value>...]
    auto result = SendAndExpectPrefix(cmd.str(), "OK DOC");
    if (!result) {
      return MakeUnexpected(result.error());
    }
    const std::string& response = *result;

    std::istringstream iss(response);
    std::string status;
    std::string doc_str;
    std::string doc_pk;
    iss >> status >> doc_str >> doc_pk;

    Document doc(doc_pk);

    // Parse remaining key=value pairs
    std::string rest;
    std::getline(iss, rest);
    doc.fields = ParseKeyValuePairs(rest);

    return doc;
  }

  Expected<ServerInfo, Error> Info() const {
    auto result = SendAndExpectPrefix("INFO", "OK INFO");
    if (!result) {
      return MakeUnexpected(result.error());
    }
    const std::string& response = *result;

    // Parse Redis-style INFO response (multi-line key: value format)
    ServerInfo info;
    std::istringstream iss(response);
    std::string line;

    // Skip first line "OK INFO"
    std::getline(iss, line);

    while (std::getline(iss, line)) {
      // Skip empty lines and section headers (lines starting with #)
      if (line.empty() || line[0] == '#' || line[0] == '\r') {
        continue;
      }

      // Parse "key: value" format
      size_t colon_pos = line.find(':');
      if (colon_pos != std::string::npos) {
        std::string key = line.substr(0, colon_pos);
        std::string value = line.substr(colon_pos + 1);

        // Trim leading/trailing whitespace from value
        size_t start = value.find_first_not_of(" \t\r\n");
        size_t end = value.find_last_not_of(" \t\r\n");
        if (start != std::string::npos) {
          value = value.substr(start, end - start + 1);
        }

        if (key == "version") {
          info.version = value;
        } else if (key == "uptime_seconds") {
          info.uptime_seconds = mygram::utils::ParseNumeric<uint64_t>(value).value_or(0);
        } else if (key == "total_requests") {
          info.total_requests = mygram::utils::ParseNumeric<uint64_t>(value).value_or(0);
        } else if (key == "connected_clients") {
          // Server emits "connected_clients" (not "active_connections").
          info.active_connections = mygram::utils::ParseNumeric<uint64_t>(value).value_or(0);
        } else if (key == "used_memory_bytes") {
          // Server emits "used_memory_bytes" as the total numeric memory usage.
          // Map it onto ServerInfo::index_size_bytes (preserves API; the field
          // represents total resident memory bytes, not strictly index-only).
          info.index_size_bytes = mygram::utils::ParseNumeric<uint64_t>(value).value_or(0);
        } else if (key == "doc_count" || key == "total_documents") {
          info.doc_count = mygram::utils::ParseNumeric<uint64_t>(value).value_or(0);
        } else if (key == "tables") {
          // Parse comma-separated table names
          std::istringstream table_iss(value);
          std::string table;
          while (std::getline(table_iss, table, ',')) {
            if (!table.empty()) {
              info.tables.push_back(table);
            }
          }
        }
      }
    }

    return info;
  }

  Expected<std::string, Error> GetConfig() const {
    // CONFIG response has no fixed prefix; just check for ERROR and return raw.
    return SendAndExpectPrefix("CONFIG", std::string_view{});
  }

  Expected<std::string, Error> Save(const std::string& filepath) const {
    if (!filepath.empty()) {
      if (auto err = ValidateNoControlCharacters(filepath, "filepath")) {
        return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
      }
    }

    std::string cmd = filepath.empty() ? "SAVE" : "SAVE " + filepath;

    // Parse: OK SAVED <filepath>
    auto result = SendAndExpectPrefix(cmd, "OK SAVED");
    if (!result) {
      return MakeUnexpected(result.error());
    }
    return result->substr(proto::kOkSavedPrefixLen);  // Return filepath after "OK SAVED "
  }

  Expected<std::string, Error> Load(const std::string& filepath) const {
    if (auto err = ValidateNoControlCharacters(filepath, "filepath")) {
      return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
    }

    // Parse: OK LOADED <filepath>
    auto result = SendAndExpectPrefix("LOAD " + filepath, "OK LOADED");
    if (!result) {
      return MakeUnexpected(result.error());
    }
    return result->substr(proto::kOkLoadedPrefixLen);  // Return filepath after "OK LOADED "
  }

  Expected<ReplicationStatus, Error> GetReplicationStatus() const {
    auto result = SendAndExpectPrefix("REPLICATION STATUS", "OK REPLICATION");
    if (!result) {
      return MakeUnexpected(result.error());
    }
    const std::string& response = *result;

    // Server emits Redis-style "key: value\r\n" lines after the status line.
    ReplicationStatus status;
    status.status_str = response;

    auto pairs = ParseColonKeyValueLines(response);
    for (const auto& [key, value] : pairs) {
      if (key == "status") {
        status.status_str = value;
        status.running = (value == "running");
      } else if (key == "current_gtid") {
        status.gtid = value;
      } else if (key == "processed_events") {
        status.processed_events = mygram::utils::ParseNumeric<uint64_t>(value).value_or(0);
      } else if (key == "queue_size") {
        status.queue_size = mygram::utils::ParseNumeric<uint64_t>(value).value_or(0);
      }
    }

    return status;
  }

  Expected<void, Error> StopReplication() const {
    auto result = SendAndExpectPrefix("REPLICATION STOP", std::string_view{});
    if (!result) {
      return MakeUnexpected(result.error());
    }
    return {};
  }

  Expected<void, Error> StartReplication() const {
    auto result = SendAndExpectPrefix("REPLICATION START", std::string_view{});
    if (!result) {
      return MakeUnexpected(result.error());
    }
    return {};
  }

  Expected<void, Error> EnableDebug() const {
    auto result = SendAndExpectPrefix("DEBUG ON", std::string_view{});
    if (!result) {
      return MakeUnexpected(result.error());
    }
    return {};
  }

  Expected<void, Error> DisableDebug() const {
    auto result = SendAndExpectPrefix("DEBUG OFF", std::string_view{});
    if (!result) {
      return MakeUnexpected(result.error());
    }
    return {};
  }

 private:
  ClientConfig config_;
  int sock_{-1};
  // Serializes SendCommand() calls so concurrent threads do not interleave
  // send/recv on the same socket. Connect()/Disconnect() are intentionally
  // not protected by this mutex; concurrent Disconnect() during an in-flight
  // SendCommand() is a logic error from the caller (documented in the
  // public MygramClient class) but cannot deadlock because Disconnect()
  // never acquires this mutex.
  mutable std::mutex command_mutex_;
};

// MygramClient public interface implementation

MygramClient::MygramClient(ClientConfig config) : impl_(std::make_unique<Impl>(std::move(config))) {}

MygramClient::~MygramClient() = default;

MygramClient::MygramClient(MygramClient&&) noexcept = default;
MygramClient& MygramClient::operator=(MygramClient&&) noexcept = default;

mygram::utils::Expected<void, mygram::utils::Error> MygramClient::Connect() {
  return impl_->Connect();
}

void MygramClient::Disconnect() {
  impl_->Disconnect();
}

bool MygramClient::IsConnected() const {
  return impl_->IsConnected();
}

mygram::utils::Expected<SearchResponse, mygram::utils::Error> MygramClient::Search(
    const std::string& table, const std::string& query, uint32_t limit, uint32_t offset,
    const std::vector<std::string>& and_terms, const std::vector<std::string>& not_terms,
    const std::vector<std::pair<std::string, std::string>>& filters, const std::string& sort_column,
    bool sort_desc) const {
  return impl_->Search(table, query, limit, offset, and_terms, not_terms, filters, sort_column, sort_desc);
}

mygram::utils::Expected<CountResponse, mygram::utils::Error> MygramClient::Count(
    const std::string& table, const std::string& query, const std::vector<std::string>& and_terms,
    const std::vector<std::string>& not_terms, const std::vector<std::pair<std::string, std::string>>& filters) const {
  return impl_->Count(table, query, and_terms, not_terms, filters);
}

mygram::utils::Expected<Document, mygram::utils::Error> MygramClient::Get(const std::string& table,
                                                                          const std::string& primary_key) const {
  return impl_->Get(table, primary_key);
}

mygram::utils::Expected<ServerInfo, mygram::utils::Error> MygramClient::Info() const {
  return impl_->Info();
}

mygram::utils::Expected<std::string, mygram::utils::Error> MygramClient::GetConfig() const {
  return impl_->GetConfig();
}

mygram::utils::Expected<std::string, mygram::utils::Error> MygramClient::Save(const std::string& filepath) const {
  return impl_->Save(filepath);
}

mygram::utils::Expected<std::string, mygram::utils::Error> MygramClient::Load(const std::string& filepath) const {
  return impl_->Load(filepath);
}

mygram::utils::Expected<ReplicationStatus, mygram::utils::Error> MygramClient::GetReplicationStatus() const {
  return impl_->GetReplicationStatus();
}

mygram::utils::Expected<void, mygram::utils::Error> MygramClient::StopReplication() const {
  return impl_->StopReplication();
}

mygram::utils::Expected<void, mygram::utils::Error> MygramClient::StartReplication() const {
  return impl_->StartReplication();
}

mygram::utils::Expected<void, mygram::utils::Error> MygramClient::EnableDebug() const {
  return impl_->EnableDebug();
}

mygram::utils::Expected<void, mygram::utils::Error> MygramClient::DisableDebug() const {
  return impl_->DisableDebug();
}

mygram::utils::Expected<std::string, mygram::utils::Error> MygramClient::SendCommand(const std::string& command) const {
  return impl_->SendCommand(command);
}

}  // namespace mygramdb::client
