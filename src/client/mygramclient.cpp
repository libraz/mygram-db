/**
 * @file mygramclient.cpp
 * @brief Implementation of MygramDB client library
 */

#include "client/mygramclient.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include <cctype>
#include <chrono>
#include <cstring>
#include <iomanip>
#include <sstream>
#include <thread>
#include <utility>

#include "utils/error.h"
#include "utils/expected.h"

using namespace mygram::utils;

namespace mygramdb::client {

namespace {

// Protocol constants
constexpr size_t kErrorPrefixLen = 6;    // Length of "ERROR "
constexpr size_t kSavedPrefixLen = 9;    // Length of "SNAPSHOT "
constexpr size_t kLoadedPrefixLen = 10;  // Length of "SNAPSHOT: "
constexpr int kMillisecondsPerSecond = 1000;
constexpr int kMicrosecondsPerMillisecond = 1000;

/**
 * @brief Parse key=value pairs from string
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
 * @brief Extract debug info from response tokens
 */
std::optional<DebugInfo> ParseDebugInfo(const std::vector<std::string>& tokens, size_t start_index) {
  if (start_index >= tokens.size() || tokens[start_index] != "DEBUG") {
    return std::nullopt;
  }

  DebugInfo info;
  for (size_t i = start_index + 1; i < tokens.size(); ++i) {
    const auto& token = tokens[i];
    size_t pos = token.find('=');
    if (pos == std::string::npos) {
      continue;
    }

    std::string key = token.substr(0, pos);
    std::string value = token.substr(pos + 1);

    if (key == "query_time") {
      info.query_time_ms = std::stod(value);
    } else if (key == "index_time") {
      info.index_time_ms = std::stod(value);
    } else if (key == "filter_time") {
      info.filter_time_ms = std::stod(value);
    } else if (key == "terms") {
      info.terms = static_cast<uint32_t>(std::stoul(value));
    } else if (key == "ngrams") {
      info.ngrams = static_cast<uint32_t>(std::stoul(value));
    } else if (key == "candidates") {
      info.candidates = std::stoull(value);
    } else if (key == "after_intersection") {
      info.after_intersection = std::stoull(value);
    } else if (key == "after_not") {
      info.after_not = std::stoull(value);
    } else if (key == "after_filters") {
      info.after_filters = std::stoull(value);
    } else if (key == "final") {
      info.final = std::stoull(value);
    } else if (key == "optimization") {
      info.optimization = value;
    }
  }

  return info;
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
 */
std::string EscapeQueryString(const std::string& str) {
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
    }
    result += character;
  }
  result += '"';
  return result;
}

}  // namespace

/**
 * @brief PIMPL implementation class
 */
class MygramClient::Impl {
 public:
  explicit Impl(ClientConfig config) : config_(std::move(config)) {}

  ~Impl() { Disconnect(); }

  // Non-copyable, movable
  Impl(const Impl&) = delete;
  Impl& operator=(const Impl&) = delete;
  Impl(Impl&&) = default;
  Impl& operator=(Impl&&) = default;

  Expected<void, Error> Connect() {
    if (sock_ >= 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientAlreadyConnected, "Already connected"));
    }

    sock_ = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_ < 0) {
      return MakeUnexpected(
          MakeError(ErrorCode::kClientConnectionFailed, std::string("Failed to create socket: ") + strerror(errno)));
    }

    // Set socket timeout
    struct timeval timeout_val = {};
    timeout_val.tv_sec = static_cast<decltype(timeout_val.tv_sec)>(config_.timeout_ms / kMillisecondsPerSecond);
    timeout_val.tv_usec = static_cast<decltype(timeout_val.tv_usec)>((config_.timeout_ms % kMillisecondsPerSecond) *
                                                                     kMicrosecondsPerMillisecond);
    setsockopt(sock_, SOL_SOCKET, SO_RCVTIMEO, &timeout_val, sizeof(timeout_val));
    setsockopt(sock_, SOL_SOCKET, SO_SNDTIMEO, &timeout_val, sizeof(timeout_val));

    struct sockaddr_in server_addr = {};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(config_.port);

    if (inet_pton(AF_INET, config_.host.c_str(), &server_addr.sin_addr) <= 0) {
      close(sock_);
      sock_ = -1;
      return MakeUnexpected(MakeError(ErrorCode::kClientConnectionFailed, "Invalid address: " + config_.host));
    }

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for socket API
    if (connect(sock_, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
      std::string error_msg = std::string("Connection failed: ") + strerror(errno);
      close(sock_);
      sock_ = -1;
      return MakeUnexpected(MakeError(ErrorCode::kClientConnectionFailed, error_msg));
    }

    return {};
  }

  void Disconnect() {
    if (sock_ >= 0) {
      close(sock_);
      sock_ = -1;
    }
  }

  [[nodiscard]] bool IsConnected() const { return sock_ >= 0; }

  Expected<std::string, Error> SendCommand(const std::string& command) const {
    if (!IsConnected()) {
      return MakeUnexpected(MakeError(ErrorCode::kClientNotConnected, "Not connected"));
    }

    // Send command with \r\n terminator
    std::string msg = command + "\r\n";
    ssize_t sent = send(sock_, msg.c_str(), msg.length(), 0);
    if (sent < 0) {
      return MakeUnexpected(
          MakeError(ErrorCode::kClientCommandFailed, std::string("Failed to send command: ") + strerror(errno)));
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

      buffer[received] = '\0';
      response.append(buffer.data(), received);

      // Check if response is complete by looking for \r\n terminator
      // All protocol responses end with \r\n
      if (response.size() >= 2 && response[response.size() - 2] == '\r' && response[response.size() - 1] == '\n') {
        // Response is complete
        break;
      }

      // If we received less than buffer size, server has no more data (for now)
      // But response doesn't end with \r\n yet, so keep reading
      // This handles the case where server sends data in chunks
      if (static_cast<size_t>(received) < buffer.size() - 1) {
        // Small delay to allow more data to arrive
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
      }
    }

    // Remove trailing \r\n
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
    if (auto err = ValidateNoControlCharacters(table, "table name")) {
      return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
    }
    if (auto err = ValidateNoControlCharacters(query, "search query")) {
      return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
    }
    for (const auto& term : and_terms) {
      if (auto err = ValidateNoControlCharacters(term, "AND term")) {
        return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
      }
    }
    for (const auto& term : not_terms) {
      if (auto err = ValidateNoControlCharacters(term, "NOT term")) {
        return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
      }
    }
    for (const auto& [key, value] : filters) {
      if (auto err = ValidateNoControlCharacters(key, "filter key")) {
        return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
      }
      if (auto err = ValidateNoControlCharacters(value, "filter value")) {
        return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
      }
    }
    if (!sort_column.empty()) {
      if (auto err = ValidateNoControlCharacters(sort_column, "sort column")) {
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

    // LIMIT clause - support MySQL-style offset,count format when both are specified
    if (limit > 0 && offset > 0) {
      cmd << " LIMIT " << offset << "," << limit;
    } else if (limit > 0) {
      cmd << " LIMIT " << limit;
    }

    // OFFSET clause - only needed if LIMIT didn't use offset,count format
    // (This is redundant if we used LIMIT offset,count above, but kept for clarity)
    // Note: The LIMIT offset,count format above already handles offset, so we skip this
    // if (offset > 0 && limit == 0) {
    //   cmd << " OFFSET " << offset;
    // }

    auto result = SendCommand(cmd.str());
    if (!result) {
      return MakeUnexpected(result.error());
    }

    std::string response = *result;
    if (response.find("ERROR") == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientServerError, response.substr(kErrorPrefixLen)));
    }

    // Parse response: OK RESULTS <total_count> [<id1> <id2> ...] [DEBUG ...]
    if (response.find("OK RESULTS") != 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientProtocolError, "Unexpected response format"));
    }

    std::istringstream iss(response);
    std::string status;
    std::string results_str;
    uint64_t total_count = 0;
    iss >> status >> results_str >> total_count;

    SearchResponse resp;
    resp.total_count = total_count;

    std::vector<std::string> tokens;
    std::string token;
    while (iss >> token) {
      tokens.push_back(token);
    }

    // Find DEBUG marker if present
    size_t debug_index = tokens.size();
    for (size_t i = 0; i < tokens.size(); ++i) {
      if (tokens[i] == "DEBUG") {
        debug_index = i;
        break;
      }
    }

    // Extract result IDs (before DEBUG)
    for (size_t i = 0; i < debug_index; ++i) {
      resp.results.emplace_back(tokens[i]);
    }

    // Parse debug info if present
    if (debug_index < tokens.size()) {
      resp.debug = ParseDebugInfo(tokens, debug_index);
    }

    return resp;
  }

  Expected<CountResponse, Error> Count(const std::string& table, const std::string& query,
                                       const std::vector<std::string>& and_terms,
                                       const std::vector<std::string>& not_terms,
                                       const std::vector<std::pair<std::string, std::string>>& filters) const {
    if (auto err = ValidateNoControlCharacters(table, "table name")) {
      return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
    }
    if (auto err = ValidateNoControlCharacters(query, "search query")) {
      return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
    }
    for (const auto& term : and_terms) {
      if (auto err = ValidateNoControlCharacters(term, "AND term")) {
        return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
      }
    }
    for (const auto& term : not_terms) {
      if (auto err = ValidateNoControlCharacters(term, "NOT term")) {
        return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
      }
    }
    for (const auto& [key, value] : filters) {
      if (auto err = ValidateNoControlCharacters(key, "filter key")) {
        return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
      }
      if (auto err = ValidateNoControlCharacters(value, "filter value")) {
        return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
      }
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

    auto result = SendCommand(cmd.str());
    if (!result) {
      return MakeUnexpected(result.error());
    }

    std::string response = *result;
    if (response.find("ERROR") == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientServerError, response.substr(kErrorPrefixLen)));
    }

    // Parse response: OK COUNT <n> [DEBUG ...]
    if (response.find("OK COUNT") != 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientProtocolError, "Unexpected response format"));
    }

    std::istringstream iss(response);
    std::string status;
    std::string count_str;
    uint64_t count = 0;
    iss >> status >> count_str >> count;

    CountResponse resp;
    resp.count = count;

    // Check for debug info
    std::vector<std::string> tokens;
    std::string token;
    while (iss >> token) {
      tokens.push_back(token);
    }

    if (!tokens.empty() && tokens[0] == "DEBUG") {
      resp.debug = ParseDebugInfo(tokens, 0);
    }

    return resp;
  }

  Expected<Document, Error> Get(const std::string& table, const std::string& primary_key) const {
    if (auto err = ValidateNoControlCharacters(table, "table name")) {
      return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
    }
    if (auto err = ValidateNoControlCharacters(primary_key, "primary key")) {
      return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
    }

    std::ostringstream cmd;
    cmd << "GET " << table << " " << primary_key;

    auto result = SendCommand(cmd.str());
    if (!result) {
      return MakeUnexpected(result.error());
    }

    std::string response = *result;
    if (response.find("ERROR") == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientServerError, response.substr(kErrorPrefixLen)));
    }

    // Parse response: OK DOC <primary_key> [<key=value>...]
    if (response.find("OK DOC") != 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientProtocolError, "Unexpected response format"));
    }

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
    auto result = SendCommand("INFO");
    if (!result) {
      return MakeUnexpected(result.error());
    }

    std::string response = *result;
    if (response.find("ERROR") == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientServerError, response.substr(kErrorPrefixLen)));
    }

    if (response.find("OK INFO") != 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientProtocolError, "Unexpected response format"));
    }

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
          info.uptime_seconds = std::stoull(value);
        } else if (key == "total_requests") {
          info.total_requests = std::stoull(value);
        } else if (key == "active_connections") {
          info.active_connections = std::stoull(value);
        } else if (key == "index_size_bytes") {
          info.index_size_bytes = std::stoull(value);
        } else if (key == "doc_count" || key == "total_documents") {
          info.doc_count = std::stoull(value);
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
    auto result = SendCommand("CONFIG");
    if (!result) {
      return MakeUnexpected(result.error());
    }

    std::string response = *result;
    if (response.find("ERROR") == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientServerError, response.substr(kErrorPrefixLen)));
    }

    // Return raw config response (already formatted)
    return response;
  }

  Expected<std::string, Error> Save(const std::string& filepath) const {
    if (!filepath.empty()) {
      if (auto err = ValidateNoControlCharacters(filepath, "filepath")) {
        return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
      }
    }

    std::string cmd = filepath.empty() ? "SAVE" : "SAVE " + filepath;

    auto result = SendCommand(cmd);
    if (!result) {
      return MakeUnexpected(result.error());
    }

    std::string response = *result;
    if (response.find("ERROR") == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientServerError, response.substr(kErrorPrefixLen)));
    }

    // Parse: OK SAVED <filepath>
    if (response.find("OK SAVED") != 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientProtocolError, "Unexpected response format"));
    }

    return response.substr(kSavedPrefixLen);  // Return filepath after "OK SAVED "
  }

  Expected<std::string, Error> Load(const std::string& filepath) const {
    if (auto err = ValidateNoControlCharacters(filepath, "filepath")) {
      return MakeUnexpected(MakeError(ErrorCode::kClientInvalidArgument, *err));
    }

    auto result = SendCommand("LOAD " + filepath);
    if (!result) {
      return MakeUnexpected(result.error());
    }

    std::string response = *result;
    if (response.find("ERROR") == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientServerError, response.substr(kErrorPrefixLen)));
    }

    // Parse: OK LOADED <filepath>
    if (response.find("OK LOADED") != 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientProtocolError, "Unexpected response format"));
    }

    return response.substr(kLoadedPrefixLen);  // Return filepath after "OK LOADED "
  }

  Expected<ReplicationStatus, Error> GetReplicationStatus() const {
    auto result = SendCommand("REPLICATION STATUS");
    if (!result) {
      return MakeUnexpected(result.error());
    }

    std::string response = *result;
    if (response.find("ERROR") == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientServerError, response.substr(kErrorPrefixLen)));
    }

    if (response.find("OK REPLICATION") != 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientProtocolError, "Unexpected response format"));
    }

    ReplicationStatus status;
    status.status_str = response;

    auto pairs = ParseKeyValuePairs(response);
    for (const auto& [key, value] : pairs) {
      if (key == "status") {
        status.running = (value == "running");
      } else if (key == "gtid") {
        status.gtid = value;
      }
    }

    return status;
  }

  Expected<void, Error> StopReplication() const {
    auto result = SendCommand("REPLICATION STOP");
    if (!result) {
      return MakeUnexpected(result.error());
    }

    std::string response = *result;
    if (response.find("ERROR") == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientServerError, response.substr(kErrorPrefixLen)));
    }

    return {};
  }

  Expected<void, Error> StartReplication() const {
    auto result = SendCommand("REPLICATION START");
    if (!result) {
      return MakeUnexpected(result.error());
    }

    std::string response = *result;
    if (response.find("ERROR") == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientServerError, response.substr(kErrorPrefixLen)));
    }

    return {};
  }

  Expected<void, Error> EnableDebug() const {
    auto result = SendCommand("DEBUG ON");
    if (!result) {
      return MakeUnexpected(result.error());
    }

    std::string response = *result;
    if (response.find("ERROR") == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientServerError, response.substr(kErrorPrefixLen)));
    }

    return {};
  }

  Expected<void, Error> DisableDebug() const {
    auto result = SendCommand("DEBUG OFF");
    if (!result) {
      return MakeUnexpected(result.error());
    }

    std::string response = *result;
    if (response.find("ERROR") == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kClientServerError, response.substr(kErrorPrefixLen)));
    }

    return {};
  }

 private:
  ClientConfig config_;
  int sock_{-1};
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
