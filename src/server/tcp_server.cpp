/**
 * @file tcp_server.cpp
 * @brief TCP server implementation
 */

#include "server/tcp_server.h"
#include "utils/string_utils.h"
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <cstring>
#include <sstream>

namespace mygramdb {
namespace server {

TcpServer::TcpServer(const ServerConfig& config,
                     index::Index& index,
                     storage::DocumentStore& doc_store)
    : config_(config), index_(index), doc_store_(doc_store) {}

TcpServer::~TcpServer() {
  Stop();
}

bool TcpServer::Start() {
  if (running_) {
    last_error_ = "Server already running";
    return false;
  }

  // Create socket
  server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
  if (server_fd_ < 0) {
    last_error_ = "Failed to create socket: " + std::string(strerror(errno));
    spdlog::error(last_error_);
    return false;
  }

  // Set socket options
  if (!SetSocketOptions(server_fd_)) {
    close(server_fd_);
    server_fd_ = -1;
    return false;
  }

  // Bind
  struct sockaddr_in address;
  std::memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(config_.port);

  if (bind(server_fd_, (struct sockaddr*)&address, sizeof(address)) < 0) {
    last_error_ = "Failed to bind to port " + std::to_string(config_.port) +
                  ": " + std::string(strerror(errno));
    spdlog::error(last_error_);
    close(server_fd_);
    server_fd_ = -1;
    return false;
  }

  // Get actual port if port 0 was specified
  if (config_.port == 0) {
    socklen_t addr_len = sizeof(address);
    if (getsockname(server_fd_, (struct sockaddr*)&address, &addr_len) == 0) {
      actual_port_ = ntohs(address.sin_port);
    }
  } else {
    actual_port_ = config_.port;
  }

  // Listen
  if (listen(server_fd_, config_.max_connections) < 0) {
    last_error_ = "Failed to listen: " + std::string(strerror(errno));
    spdlog::error(last_error_);
    close(server_fd_);
    server_fd_ = -1;
    return false;
  }

  should_stop_ = false;
  running_ = true;

  // Start accept thread
  accept_thread_ = std::make_unique<std::thread>(
      &TcpServer::AcceptThreadFunc, this);

  spdlog::info("TCP server started on {}:{}", config_.host, actual_port_);
  return true;
}

void TcpServer::Stop() {
  if (!running_) {
    return;
  }

  spdlog::info("Stopping TCP server...");
  should_stop_ = true;

  // Close server socket to unblock accept()
  if (server_fd_ >= 0) {
    shutdown(server_fd_, SHUT_RDWR);
    close(server_fd_);
    server_fd_ = -1;
  }

  // Wait for accept thread
  if (accept_thread_ && accept_thread_->joinable()) {
    accept_thread_->join();
  }

  // Close all active connections
  {
    std::lock_guard<std::mutex> lock(connections_mutex_);
    for (int fd : active_connections_) {
      shutdown(fd, SHUT_RDWR);
      close(fd);
    }
    active_connections_.clear();
  }

  // Wait for worker threads
  for (auto& thread : worker_threads_) {
    if (thread && thread->joinable()) {
      thread->join();
    }
  }
  worker_threads_.clear();

  running_ = false;
  spdlog::info("TCP server stopped. Handled {} total requests",
               total_requests_.load());
}

size_t TcpServer::GetConnectionCount() const {
  std::lock_guard<std::mutex> lock(connections_mutex_);
  return active_connections_.size();
}

void TcpServer::AcceptThreadFunc() {
  spdlog::info("Accept thread started");

  while (!should_stop_) {
    struct sockaddr_in client_addr;
    socklen_t client_len = sizeof(client_addr);

    int client_fd = accept(server_fd_, (struct sockaddr*)&client_addr,
                          &client_len);

    if (client_fd < 0) {
      if (should_stop_) {
        break;
      }
      spdlog::warn("Accept failed: {}", strerror(errno));
      continue;
    }

    // Add to active connections
    {
      std::lock_guard<std::mutex> lock(connections_mutex_);
      active_connections_.push_back(client_fd);
    }

    // Handle client in new thread
    auto thread = std::make_unique<std::thread>(
        &TcpServer::HandleClient, this, client_fd);
    worker_threads_.push_back(std::move(thread));

    spdlog::debug("Accepted connection from {}:{}",
                 inet_ntoa(client_addr.sin_addr),
                 ntohs(client_addr.sin_port));
  }

  spdlog::info("Accept thread stopped");
}

void TcpServer::HandleClient(int client_fd) {
  std::vector<char> buffer(config_.recv_buffer_size);
  std::string accumulated;

  while (!should_stop_) {
    ssize_t n = recv(client_fd, buffer.data(), buffer.size() - 1, 0);

    if (n <= 0) {
      if (n < 0) {
        spdlog::debug("recv error: {}", strerror(errno));
      }
      break;
    }

    buffer[n] = '\0';
    accumulated += buffer.data();

    // Process complete requests (ending with \r\n)
    size_t pos;
    while ((pos = accumulated.find("\r\n")) != std::string::npos) {
      std::string request = accumulated.substr(0, pos);
      accumulated = accumulated.substr(pos + 2);

      if (request.empty()) {
        continue;
      }

      // Process request
      std::string response = ProcessRequest(request);
      total_requests_++;

      // Send response
      response += "\r\n";
      ssize_t sent = send(client_fd, response.c_str(), response.length(), 0);
      if (sent < 0) {
        spdlog::debug("send error: {}", strerror(errno));
        break;
      }
    }
  }

  RemoveConnection(client_fd);
  close(client_fd);
}

std::string TcpServer::ProcessRequest(const std::string& request) {
  spdlog::debug("Processing request: {}", request);

  // Parse query
  auto query = query_parser_.Parse(request);

  if (!query.IsValid()) {
    return FormatError(query_parser_.GetError());
  }

  try {
    switch (query.type) {
      case query::QueryType::SEARCH: {
        // Generate n-grams from search text
        std::string normalized = utils::NormalizeText(query.search_text,
                                                       true, "keep", true);
        auto terms = utils::GenerateNgrams(normalized, 1);

        // Search index
        auto results = index_.SearchAnd(terms);

        // Apply NOT filter if present
        if (!query.not_terms.empty()) {
          // Generate NOT term n-grams
          std::vector<std::string> not_ngrams;
          for (const auto& not_term : query.not_terms) {
            std::string norm_not = utils::NormalizeText(not_term, true, "keep", true);
            auto ngrams = utils::GenerateNgrams(norm_not, 1);
            not_ngrams.insert(not_ngrams.end(), ngrams.begin(), ngrams.end());
          }

          results = index_.SearchNot(results, not_ngrams);
        }

        // TODO: Apply filter conditions

        return FormatSearchResponse(results, query.limit, query.offset);
      }

      case query::QueryType::COUNT: {
        // Generate n-grams
        std::string normalized = utils::NormalizeText(query.search_text,
                                                       true, "keep", true);
        auto terms = utils::GenerateNgrams(normalized, 1);

        // Count
        auto results = index_.SearchAnd(terms);

        // Apply NOT filter if present
        if (!query.not_terms.empty()) {
          std::vector<std::string> not_ngrams;
          for (const auto& not_term : query.not_terms) {
            std::string norm_not = utils::NormalizeText(not_term, true, "keep", true);
            auto ngrams = utils::GenerateNgrams(norm_not, 1);
            not_ngrams.insert(not_ngrams.end(), ngrams.begin(), ngrams.end());
          }

          results = index_.SearchNot(results, not_ngrams);
        }

        return FormatCountResponse(results.size());
      }

      case query::QueryType::GET: {
        auto doc_id_opt = doc_store_.GetDocId(query.primary_key);
        if (!doc_id_opt) {
          return FormatError("Document not found");
        }

        auto doc = doc_store_.GetDocument(doc_id_opt.value());
        return FormatGetResponse(doc);
      }

      default:
        return FormatError("Unknown query type");
    }
  } catch (const std::exception& e) {
    return FormatError(std::string("Exception: ") + e.what());
  }
}

std::string TcpServer::FormatSearchResponse(
    const std::vector<index::DocId>& results,
    uint32_t limit, uint32_t offset) {
  std::ostringstream oss;
  oss << "OK RESULTS " << results.size();

  // Apply offset and limit
  size_t start = std::min(static_cast<size_t>(offset), results.size());
  size_t end = std::min(start + limit, results.size());

  for (size_t i = start; i < end; ++i) {
    auto pk_opt = doc_store_.GetPrimaryKey(static_cast<storage::DocId>(results[i]));
    if (pk_opt) {
      oss << " " << pk_opt.value();
    }
  }

  return oss.str();
}

std::string TcpServer::FormatCountResponse(uint64_t count) {
  return "OK COUNT " + std::to_string(count);
}

std::string TcpServer::FormatGetResponse(
    const std::optional<storage::Document>& doc) {
  if (!doc) {
    return FormatError("Document not found");
  }

  std::ostringstream oss;
  oss << "OK DOC " << doc->primary_key;

  // Add filters
  for (const auto& [name, value] : doc->filters) {
    oss << " " << name << "=";
    if (std::holds_alternative<int64_t>(value)) {
      oss << std::get<int64_t>(value);
    } else if (std::holds_alternative<std::string>(value)) {
      oss << std::get<std::string>(value);
    }
  }

  return oss.str();
}

std::string TcpServer::FormatError(const std::string& message) {
  return "ERROR " + message;
}

bool TcpServer::SetSocketOptions(int fd) {
  // Reuse address
  int opt = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
    last_error_ = "Failed to set SO_REUSEADDR: " + std::string(strerror(errno));
    spdlog::error(last_error_);
    return false;
  }

  // Set receive buffer size
  int recv_buf = config_.recv_buffer_size;
  if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &recv_buf, sizeof(recv_buf)) < 0) {
    spdlog::warn("Failed to set SO_RCVBUF: {}", strerror(errno));
  }

  // Set send buffer size
  int send_buf = config_.send_buffer_size;
  if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &send_buf, sizeof(send_buf)) < 0) {
    spdlog::warn("Failed to set SO_SNDBUF: {}", strerror(errno));
  }

  return true;
}

void TcpServer::RemoveConnection(int fd) {
  std::lock_guard<std::mutex> lock(connections_mutex_);
  active_connections_.erase(
      std::remove(active_connections_.begin(), active_connections_.end(), fd),
      active_connections_.end());
}

}  // namespace server
}  // namespace mygramdb
