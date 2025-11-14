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

#include <cstring>
#include <sstream>
#include <utility>

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

  std::optional<std::string> Connect() {
    if (sock_ >= 0) {
      return "Already connected";
    }

    sock_ = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_ < 0) {
      last_error_ = std::string("Failed to create socket: ") + strerror(errno);
      return last_error_;
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
      last_error_ = "Invalid address: " + config_.host;
      close(sock_);
      sock_ = -1;
      return last_error_;
    }

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for socket API
    if (connect(sock_, reinterpret_cast<struct sockaddr*>(&server_addr), sizeof(server_addr)) < 0) {
      last_error_ = std::string("Connection failed: ") + strerror(errno);
      close(sock_);
      sock_ = -1;
      return last_error_;
    }

    return std::nullopt;
  }

  void Disconnect() {
    if (sock_ >= 0) {
      close(sock_);
      sock_ = -1;
    }
  }

  [[nodiscard]] bool IsConnected() const { return sock_ >= 0; }

  std::variant<std::string, Error> SendCommand(const std::string& command) {
    if (!IsConnected()) {
      last_error_ = "Not connected";

      return Error(last_error_);
    }

    // Send command with \r\n terminator
    std::string msg = command + "\r\n";
    ssize_t sent = send(sock_, msg.c_str(), msg.length(), 0);
    if (sent < 0) {
      last_error_ = std::string("Failed to send command: ") + strerror(errno);

      return Error(last_error_);
    }

    // Receive response
    std::vector<char> buffer(config_.recv_buffer_size);
    ssize_t received = recv(sock_, buffer.data(), buffer.size() - 1, 0);
    if (received <= 0) {
      if (received == 0) {
        last_error_ = "Connection closed by server";
      } else {
        last_error_ = std::string("Failed to receive response: ") + strerror(errno);
      }

      return Error(last_error_);
    }

    buffer[received] = '\0';
    std::string response(buffer.data(), received);

    // Remove trailing \r\n
    while (!response.empty() && (response.back() == '\n' || response.back() == '\r')) {
      response.pop_back();
    }

    return response;
  }

  std::variant<SearchResponse, Error> Search(const std::string& table, const std::string& query, uint32_t limit,
                                             uint32_t offset, const std::vector<std::string>& and_terms,
                                             const std::vector<std::string>& not_terms,
                                             const std::vector<std::pair<std::string, std::string>>& filters,
                                             const std::string& sort_column, bool sort_desc) {
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
    if (auto* err = std::get_if<Error>(&result)) {
      return *err;
    }

    std::string response = std::get<std::string>(result);
    if (response.find("ERROR") == 0) {
      return Error(response.substr(kErrorPrefixLen));  // Remove "ERROR "
    }

    // Parse response: OK RESULTS <total_count> [<id1> <id2> ...] [DEBUG ...]
    if (response.find("OK RESULTS") != 0) {
      return Error("Unexpected response format");
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

  std::variant<CountResponse, Error> Count(const std::string& table, const std::string& query,
                                           const std::vector<std::string>& and_terms,
                                           const std::vector<std::string>& not_terms,
                                           const std::vector<std::pair<std::string, std::string>>& filters) {
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
    if (auto* err = std::get_if<Error>(&result)) {
      return *err;
    }

    std::string response = std::get<std::string>(result);
    if (response.find("ERROR") == 0) {
      return Error(response.substr(kErrorPrefixLen));  // Remove "ERROR "
    }

    // Parse response: OK COUNT <n> [DEBUG ...]
    if (response.find("OK COUNT") != 0) {
      return Error("Unexpected response format");
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

  std::variant<Document, Error> Get(const std::string& table, const std::string& primary_key) {
    std::ostringstream cmd;
    cmd << "GET " << table << " " << primary_key;

    auto result = SendCommand(cmd.str());
    if (auto* err = std::get_if<Error>(&result)) {
      return *err;
    }

    std::string response = std::get<std::string>(result);
    if (response.find("ERROR") == 0) {
      return Error(response.substr(kErrorPrefixLen));  // Remove "ERROR "
    }

    // Parse response: OK DOC <primary_key> [<key=value>...]
    if (response.find("OK DOC") != 0) {
      return Error("Unexpected response format");
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

  std::variant<ServerInfo, Error> Info() {
    auto result = SendCommand("INFO");
    if (auto* err = std::get_if<Error>(&result)) {
      return *err;
    }

    std::string response = std::get<std::string>(result);
    if (response.find("ERROR") == 0) {
      return Error(response.substr(kErrorPrefixLen));
    }

    if (response.find("OK INFO") != 0) {
      return Error("Unexpected response format");
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

  std::variant<std::string, Error> GetConfig() {
    auto result = SendCommand("CONFIG");
    if (auto* err = std::get_if<Error>(&result)) {
      return *err;
    }

    std::string response = std::get<std::string>(result);
    if (response.find("ERROR") == 0) {
      return response.substr(kErrorPrefixLen);
    }

    // Return raw config response (already formatted)
    return response;
  }

  std::variant<std::string, Error> Save(const std::string& filepath) {
    std::string cmd = filepath.empty() ? "SAVE" : "SAVE " + filepath;

    auto result = SendCommand(cmd);
    if (auto* err = std::get_if<Error>(&result)) {
      return *err;
    }

    std::string response = std::get<std::string>(result);
    if (response.find("ERROR") == 0) {
      return response.substr(kErrorPrefixLen);
    }

    // Parse: OK SAVED <filepath>
    if (response.find("OK SAVED") != 0) {
      return Error("Unexpected response format");
    }

    return response.substr(kSavedPrefixLen);  // Return filepath after "OK SAVED "
  }

  std::variant<std::string, Error> Load(const std::string& filepath) {
    auto result = SendCommand("LOAD " + filepath);
    if (auto* err = std::get_if<Error>(&result)) {
      return *err;
    }

    std::string response = std::get<std::string>(result);
    if (response.find("ERROR") == 0) {
      return response.substr(kErrorPrefixLen);
    }

    // Parse: OK LOADED <filepath>
    if (response.find("OK LOADED") != 0) {
      return Error("Unexpected response format");
    }

    return response.substr(kLoadedPrefixLen);  // Return filepath after "OK LOADED "
  }

  std::variant<ReplicationStatus, Error> GetReplicationStatus() {
    auto result = SendCommand("REPLICATION STATUS");
    if (auto* err = std::get_if<Error>(&result)) {
      return *err;
    }

    std::string response = std::get<std::string>(result);
    if (response.find("ERROR") == 0) {
      return Error(response.substr(kErrorPrefixLen));
    }

    if (response.find("OK REPLICATION") != 0) {
      return Error("Unexpected response format");
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

  std::optional<std::string> StopReplication() {
    auto result = SendCommand("REPLICATION STOP");
    if (auto* err = std::get_if<Error>(&result)) {
      return *err;
    }

    std::string response = std::get<std::string>(result);
    if (response.find("ERROR") == 0) {
      return response.substr(kErrorPrefixLen);
    }

    return std::nullopt;
  }

  std::optional<std::string> StartReplication() {
    auto result = SendCommand("REPLICATION START");
    if (auto* err = std::get_if<Error>(&result)) {
      return *err;
    }

    std::string response = std::get<std::string>(result);
    if (response.find("ERROR") == 0) {
      return response.substr(kErrorPrefixLen);
    }

    return std::nullopt;
  }

  std::optional<std::string> EnableDebug() {
    auto result = SendCommand("DEBUG ON");
    if (auto* err = std::get_if<Error>(&result)) {
      return *err;
    }

    std::string response = std::get<std::string>(result);
    if (response.find("ERROR") == 0) {
      return response.substr(kErrorPrefixLen);
    }

    return std::nullopt;
  }

  std::optional<std::string> DisableDebug() {
    auto result = SendCommand("DEBUG OFF");
    if (auto* err = std::get_if<Error>(&result)) {
      return *err;
    }

    std::string response = std::get<std::string>(result);
    if (response.find("ERROR") == 0) {
      return response.substr(kErrorPrefixLen);
    }

    return std::nullopt;
  }

  [[nodiscard]] const std::string& GetLastError() const { return last_error_; }

 private:
  ClientConfig config_;
  int sock_{-1};
  std::string last_error_;
};

// MygramClient public interface implementation

MygramClient::MygramClient(ClientConfig config) : impl_(std::make_unique<Impl>(std::move(config))) {}

MygramClient::~MygramClient() = default;

MygramClient::MygramClient(MygramClient&&) noexcept = default;
MygramClient& MygramClient::operator=(MygramClient&&) noexcept = default;

std::optional<std::string> MygramClient::Connect() {
  return impl_->Connect();
}

void MygramClient::Disconnect() {
  impl_->Disconnect();
}

bool MygramClient::IsConnected() const {
  return impl_->IsConnected();
}

std::variant<SearchResponse, Error> MygramClient::Search(
    const std::string& table, const std::string& query, uint32_t limit, uint32_t offset,
    const std::vector<std::string>& and_terms, const std::vector<std::string>& not_terms,
    const std::vector<std::pair<std::string, std::string>>& filters, const std::string& sort_column, bool sort_desc) {
  return impl_->Search(table, query, limit, offset, and_terms, not_terms, filters, sort_column, sort_desc);
}

std::variant<CountResponse, Error> MygramClient::Count(
    const std::string& table, const std::string& query, const std::vector<std::string>& and_terms,
    const std::vector<std::string>& not_terms, const std::vector<std::pair<std::string, std::string>>& filters) {
  return impl_->Count(table, query, and_terms, not_terms, filters);
}

std::variant<Document, Error> MygramClient::Get(const std::string& table, const std::string& primary_key) {
  return impl_->Get(table, primary_key);
}

std::variant<ServerInfo, Error> MygramClient::Info() {
  return impl_->Info();
}

std::variant<std::string, Error> MygramClient::GetConfig() {
  return impl_->GetConfig();
}

std::variant<std::string, Error> MygramClient::Save(const std::string& filepath) {
  return impl_->Save(filepath);
}

std::variant<std::string, Error> MygramClient::Load(const std::string& filepath) {
  return impl_->Load(filepath);
}

std::variant<ReplicationStatus, Error> MygramClient::GetReplicationStatus() {
  return impl_->GetReplicationStatus();
}

std::optional<std::string> MygramClient::StopReplication() {
  return impl_->StopReplication();
}

std::optional<std::string> MygramClient::StartReplication() {
  return impl_->StartReplication();
}

std::optional<std::string> MygramClient::EnableDebug() {
  return impl_->EnableDebug();
}

std::optional<std::string> MygramClient::DisableDebug() {
  return impl_->DisableDebug();
}

std::variant<std::string, Error> MygramClient::SendCommand(const std::string& command) {
  return impl_->SendCommand(command);
}

const std::string& MygramClient::GetLastError() const {
  return impl_->GetLastError();
}

}  // namespace mygramdb::client
