/**
 * @file tcp_server.cpp
 * @brief TCP server implementation
 */

#include "server/tcp_server.h"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <limits>
#include <sstream>
#include <utility>

#include "utils/string_utils.h"
#include "utils/network_utils.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#endif

namespace mygramdb {
namespace server {

TcpServer::TcpServer(ServerConfig config, index::Index& index, storage::DocumentStore& doc_store,
                     int ngram_size, std::string snapshot_dir, const config::Config* full_config,
#ifdef USE_MYSQL
                     mysql::BinlogReader* binlog_reader
#else
                     void* binlog_reader
#endif
                     )
    : config_(std::move(config)),
      index_(index),
      doc_store_(doc_store),
      ngram_size_(ngram_size),
      snapshot_dir_(std::move(snapshot_dir)),
      full_config_(full_config),
      binlog_reader_(binlog_reader) {
  // Create thread pool
  thread_pool_ = std::make_unique<ThreadPool>(
      config_.worker_threads > 0 ? config_.worker_threads : 0,
      1000  // Queue size for backpressure
  );
}

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
  struct sockaddr_in address{};
  std::memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(config_.port);

  if (bind(server_fd_, reinterpret_cast<struct sockaddr*>(&address), sizeof(address)) < 0) {
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
    if (getsockname(server_fd_, reinterpret_cast<struct sockaddr*>(&address), &addr_len) == 0) {
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

  // Shutdown thread pool (completes pending tasks)
  if (thread_pool_) {
    thread_pool_->Shutdown();
  }

  // Close all active connections
  {
    std::scoped_lock lock(connections_mutex_);
    for (int file_descriptor : connection_fds_) {
      shutdown(file_descriptor, SHUT_RDWR);
      close(file_descriptor);
    }
    connection_fds_.clear();
  }

  running_ = false;
  spdlog::info("TCP server stopped. Handled {} total requests",
               stats_.GetTotalRequests());
}

void TcpServer::AcceptThreadFunc() {
  spdlog::info("Accept thread started");

  while (!should_stop_) {
    struct sockaddr_in client_addr{};
    socklen_t client_len = sizeof(client_addr);

    int client_fd = accept(server_fd_, reinterpret_cast<struct sockaddr*>(&client_addr),
                          &client_len);

    if (client_fd < 0) {
      if (should_stop_) {
        break;
      }
      spdlog::warn("Accept failed: {}", strerror(errno));
      continue;
    }

    // Get client IP address
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
    char client_ip_str[INET_ADDRSTRLEN];
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip_str, INET_ADDRSTRLEN);

    // Check CIDR allow list
    // NOLINTNEXTLINE(readability-implicit-bool-conversion)
    if (full_config_ && !full_config_->network.allow_cidrs.empty()) {
      // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
      if (!utils::IsIPAllowed(client_ip_str, full_config_->network.allow_cidrs)) {
        spdlog::warn("Connection from {} rejected (not in allow_cidrs)", client_ip_str);
        shutdown(client_fd, SHUT_RDWR);
        close(client_fd);
        continue;
      }
    }

    // Check connection limit
    size_t current_connections = stats_.GetActiveConnections();
    if (current_connections >= static_cast<size_t>(config_.max_connections)) {
      spdlog::warn("Connection limit reached ({}), rejecting new connection",
                   config_.max_connections);
      shutdown(client_fd, SHUT_RDWR);
      close(client_fd);
      continue;
    }

    // Increment active connection count and total connection counter
    stats_.IncrementConnections();
    stats_.IncrementTotalConnections();

    // Add to connection set
    {
      std::scoped_lock lock(connections_mutex_);
      connection_fds_.insert(client_fd);
    }

    // Submit to thread pool
    bool submitted = thread_pool_->Submit([this, client_fd]() {
      HandleClient(client_fd);
    });

    if (!submitted) {
      spdlog::warn("Thread pool queue full, rejecting connection");
      RemoveConnection(client_fd);
      shutdown(client_fd, SHUT_RDWR);
      close(client_fd);
      stats_.DecrementConnections();
      continue;
    }

    spdlog::debug("Accepted connection from {}:{} (active: {})",
                 inet_ntoa(client_addr.sin_addr),
                 ntohs(client_addr.sin_port),
                 stats_.GetActiveConnections());
  }

  spdlog::info("Accept thread stopped");
}

void TcpServer::HandleClient(int client_fd) {
  std::vector<char> buffer(config_.recv_buffer_size);
  std::string accumulated;

  while (!should_stop_) {
    ssize_t bytes_received = recv(client_fd, buffer.data(), buffer.size() - 1, 0);

    if (bytes_received <= 0) {
      if (bytes_received < 0) {
        spdlog::debug("recv error: {}", strerror(errno));
      }
      break;
    }

    buffer[bytes_received] = '\0';
    accumulated += buffer.data();

    // Process complete requests (ending with \r\n)
    size_t pos = 0;
    while ((pos = accumulated.find("\r\n")) != std::string::npos) {
      std::string request = accumulated.substr(0, pos);
      accumulated = accumulated.substr(pos + 2);

      if (request.empty()) {
        continue;
      }

      // Process request
      std::string response = ProcessRequest(request);
      stats_.IncrementRequests();

      // Send response
      response += "\r\n";
      ssize_t sent = send(client_fd, response.c_str(), response.length(), 0);
      if (sent < 0) {
        spdlog::debug("send error: {}", strerror(errno));
        break;
      }
    }
  }

  // Clean up connection
  RemoveConnection(client_fd);
  close(client_fd);
  stats_.DecrementConnections();

  spdlog::debug("Connection closed (active: {})", stats_.GetActiveConnections());
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity)
std::string TcpServer::ProcessRequest(const std::string& request) {
  spdlog::debug("Processing request: {}", request);

  // Parse query
  auto query = query_parser_.Parse(request);

  if (!query.IsValid()) {
    return FormatError(query_parser_.GetError());
  }

  // Increment command statistics
  stats_.IncrementCommand(query.type);

  try {
    switch (query.type) {
      case query::QueryType::SEARCH: {
        // Collect all search terms (main + AND terms)
        std::vector<std::string> all_search_terms;
        all_search_terms.push_back(query.search_text);
        all_search_terms.insert(all_search_terms.end(),
                               query.and_terms.begin(),
                               query.and_terms.end());

        // Generate n-grams for each term and estimate result sizes
        struct TermInfo {
          std::vector<std::string> ngrams;
          size_t estimated_size;
        };
        std::vector<TermInfo> term_infos;
        term_infos.reserve(all_search_terms.size());

        for (const auto& search_term : all_search_terms) {
          std::string normalized = utils::NormalizeText(search_term, true, "keep", true);
          std::vector<std::string> ngrams;
          if (ngram_size_ == 0) {
            ngrams = utils::GenerateHybridNgrams(normalized);
          } else {
            ngrams = utils::GenerateNgrams(normalized, ngram_size_);
          }

          // Estimate result size by checking the smallest posting list
          size_t min_size = std::numeric_limits<size_t>::max();
          for (const auto& ngram : ngrams) {
            const auto* posting = index_.GetPostingList(ngram);
            if (posting != nullptr) {
              min_size = std::min(min_size, static_cast<size_t>(posting->Size()));
            } else {
              min_size = 0;
              break;
            }
          }

          term_infos.push_back({std::move(ngrams), min_size});
        }

        // Sort terms by estimated size (smallest first for faster intersection)
        std::sort(term_infos.begin(), term_infos.end(),
                 [](const TermInfo& lhs, const TermInfo& rhs) {
                   return lhs.estimated_size < rhs.estimated_size;
                 });

        // If any term has zero results, return empty immediately
        if (!term_infos.empty() && term_infos[0].estimated_size == 0) {
          return FormatSearchResponse({}, query.limit, query.offset);
        }

        // Process most selective term first
        auto results = index_.SearchAnd(term_infos[0].ngrams);

        // Intersect with remaining terms
        for (size_t i = 1; i < term_infos.size() && !results.empty(); ++i) {
          auto and_results = index_.SearchAnd(term_infos[i].ngrams);
          std::vector<storage::DocId> intersection;
          intersection.reserve(std::min(results.size(), and_results.size()));
          std::set_intersection(results.begin(), results.end(),
                              and_results.begin(), and_results.end(),
                              std::back_inserter(intersection));
          results = std::move(intersection);
        }

        // Apply NOT filter if present
        if (!query.not_terms.empty()) {
          // Generate NOT term n-grams
          std::vector<std::string> not_ngrams;
          for (const auto& not_term : query.not_terms) {
            std::string norm_not = utils::NormalizeText(not_term, true, "keep", true);
            std::vector<std::string> ngrams;
            if (ngram_size_ == 0) {
              ngrams = utils::GenerateHybridNgrams(norm_not);
            } else {
              ngrams = utils::GenerateNgrams(norm_not, ngram_size_);
            }
            not_ngrams.insert(not_ngrams.end(), ngrams.begin(), ngrams.end());
          }

          results = index_.SearchNot(results, not_ngrams);
        }

        // Apply filter conditions
        if (!query.filters.empty()) {
          std::vector<storage::DocId> filtered_results;
          filtered_results.reserve(results.size());

          for (const auto& doc_id : results) {
            bool matches_all_filters = true;

            for (const auto& filter_cond : query.filters) {
              auto stored_value = doc_store_.GetFilterValue(doc_id, filter_cond.column);

              // For now, only support equality operator
              if (filter_cond.op == query::FilterOp::EQ) {
                // Convert filter value string to appropriate type and compare
                bool matches = stored_value &&
                              std::visit([&](const auto& val) {
                                using T = std::decay_t<decltype(val)>;
                                if constexpr (std::is_same_v<T, std::string>) {
                                  return val == filter_cond.value;
                                } else {
                                  return std::to_string(val) == filter_cond.value;
                                }
                              }, stored_value.value());

                if (!matches) {
                  matches_all_filters = false;
                  break;
                }
              }
            }

            if (matches_all_filters) {
              filtered_results.push_back(doc_id);
            }
          }

          results = filtered_results;
        }

        return FormatSearchResponse(results, query.limit, query.offset);
      }

      case query::QueryType::COUNT: {
        // Collect all search terms (main + AND terms)
        std::vector<std::string> all_search_terms;
        all_search_terms.push_back(query.search_text);
        all_search_terms.insert(all_search_terms.end(),
                               query.and_terms.begin(),
                               query.and_terms.end());

        // Generate n-grams for each term and estimate result sizes
        struct TermInfo {
          std::vector<std::string> ngrams;
          size_t estimated_size;
        };
        std::vector<TermInfo> term_infos;
        term_infos.reserve(all_search_terms.size());

        for (const auto& search_term : all_search_terms) {
          std::string normalized = utils::NormalizeText(search_term, true, "keep", true);
          std::vector<std::string> ngrams;
          if (ngram_size_ == 0) {
            ngrams = utils::GenerateHybridNgrams(normalized);
          } else {
            ngrams = utils::GenerateNgrams(normalized, ngram_size_);
          }

          // Estimate result size by checking the smallest posting list
          size_t min_size = std::numeric_limits<size_t>::max();
          for (const auto& ngram : ngrams) {
            const auto* posting = index_.GetPostingList(ngram);
            if (posting != nullptr) {
              min_size = std::min(min_size, static_cast<size_t>(posting->Size()));
            } else {
              min_size = 0;
              break;
            }
          }

          term_infos.push_back({std::move(ngrams), min_size});
        }

        // Sort terms by estimated size (smallest first for faster intersection)
        std::sort(term_infos.begin(), term_infos.end(),
                 [](const TermInfo& lhs, const TermInfo& rhs) {
                   return lhs.estimated_size < rhs.estimated_size;
                 });

        // If any term has zero results, return 0 immediately
        if (!term_infos.empty() && term_infos[0].estimated_size == 0) {
          return FormatCountResponse(0);
        }

        // Process most selective term first
        auto results = index_.SearchAnd(term_infos[0].ngrams);

        // Intersect with remaining terms
        for (size_t i = 1; i < term_infos.size() && !results.empty(); ++i) {
          auto and_results = index_.SearchAnd(term_infos[i].ngrams);
          std::vector<storage::DocId> intersection;
          intersection.reserve(std::min(results.size(), and_results.size()));
          std::set_intersection(results.begin(), results.end(),
                              and_results.begin(), and_results.end(),
                              std::back_inserter(intersection));
          results = std::move(intersection);
        }

        // Apply NOT filter if present
        if (!query.not_terms.empty()) {
          std::vector<std::string> not_ngrams;
          for (const auto& not_term : query.not_terms) {
            std::string norm_not = utils::NormalizeText(not_term, true, "keep", true);
            std::vector<std::string> ngrams;
            if (ngram_size_ == 0) {
              ngrams = utils::GenerateHybridNgrams(norm_not);
            } else {
              ngrams = utils::GenerateNgrams(norm_not, ngram_size_);
            }
            not_ngrams.insert(not_ngrams.end(), ngrams.begin(), ngrams.end());
          }

          results = index_.SearchNot(results, not_ngrams);
        }

        // Apply filter conditions
        if (!query.filters.empty()) {
          std::vector<storage::DocId> filtered_results;
          filtered_results.reserve(results.size());

          for (const auto& doc_id : results) {
            bool matches_all_filters = true;

            for (const auto& filter_cond : query.filters) {
              auto stored_value = doc_store_.GetFilterValue(doc_id, filter_cond.column);

              // For now, only support equality operator
              if (filter_cond.op == query::FilterOp::EQ) {
                // Convert filter value string to appropriate type and compare
                bool matches = stored_value &&
                              std::visit([&](const auto& val) {
                                using T = std::decay_t<decltype(val)>;
                                if constexpr (std::is_same_v<T, std::string>) {
                                  return val == filter_cond.value;
                                } else {
                                  return std::to_string(val) == filter_cond.value;
                                }
                              }, stored_value.value());

                if (!matches) {
                  matches_all_filters = false;
                  break;
                }
              }
            }

            if (matches_all_filters) {
              filtered_results.push_back(doc_id);
            }
          }

          results = filtered_results;
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

      case query::QueryType::INFO: {
        return FormatInfoResponse();
      }

      case query::QueryType::SAVE: {
        // Determine filepath
        std::string filepath;
        if (!query.filepath.empty()) {
          filepath = query.filepath;
          // If relative path, prepend snapshot_dir
          if (filepath[0] != '/') {
            filepath = snapshot_dir_ + "/" + filepath;
          }
        } else {
          // Generate default filepath with timestamp
          auto now = std::time(nullptr);
          std::array<char, 64> buf{};
          std::strftime(buf.data(), buf.size(), "snapshot_%Y%m%d_%H%M%S", std::localtime(&now));
          filepath = snapshot_dir_ + "/" + std::string(buf.data());
        }

        // Get current GTID before stopping replication
        std::string current_gtid;
#ifdef USE_MYSQL
        bool replication_was_running = false;
        if ((binlog_reader_ != nullptr) && binlog_reader_->IsRunning()) {
          current_gtid = binlog_reader_->GetCurrentGTID();
          spdlog::info("Stopping binlog replication for snapshot save at GTID: {}", current_gtid);
          binlog_reader_->Stop();
          replication_was_running = true;
        }
#endif

        // Set read-only mode
        read_only_ = true;

        // Save index and document store with GTID
        std::string index_path = filepath + ".index";
        std::string doc_path = filepath + ".docs";

        bool success = index_.SaveToFile(index_path) &&
                      doc_store_.SaveToFile(doc_path, current_gtid);

        // Clear read-only mode
        read_only_ = false;

        // Restart binlog replication if it was running
#ifdef USE_MYSQL
        if (replication_was_running && (binlog_reader_ != nullptr)) {
          spdlog::info("Restarting binlog replication after snapshot save");
          if (!binlog_reader_->Start()) {
            spdlog::warn("Failed to restart binlog replication: {}",
                        binlog_reader_->GetLastError());
          }
        }
#endif

        if (success) {
          return FormatSaveResponse(filepath);
        }
        return FormatError("Failed to save snapshot");
      }

      case query::QueryType::LOAD: {
        // Determine filepath
        std::string filepath;
        if (!query.filepath.empty()) {
          filepath = query.filepath;
          // If relative path, prepend snapshot_dir
          if (filepath[0] != '/') {
            filepath = snapshot_dir_ + "/" + filepath;
          }
        } else {
          return FormatError("LOAD requires a filepath");
        }

        // Stop binlog replication if running
        bool replication_was_running = false;
#ifdef USE_MYSQL
        if ((binlog_reader_ != nullptr) && binlog_reader_->IsRunning()) {
          spdlog::info("Stopping binlog replication for snapshot load");
          binlog_reader_->Stop();
          replication_was_running = true;
        }
#endif

        // Set read-only mode
        read_only_ = true;

        // Load index and document store with GTID
        std::string index_path = filepath + ".index";
        std::string doc_path = filepath + ".docs";
        std::string loaded_gtid;

        bool success = index_.LoadFromFile(index_path) &&
                      doc_store_.LoadFromFile(doc_path, &loaded_gtid);

        // Clear read-only mode
        read_only_ = false;

        // Restore GTID if snapshot had one
#ifdef USE_MYSQL
        if (success && !loaded_gtid.empty() && (binlog_reader_ != nullptr)) {
          binlog_reader_->SetCurrentGTID(loaded_gtid);
          spdlog::info("Restored replication GTID from snapshot: {}", loaded_gtid);
        }

        // Restart binlog replication if it was running
        if (replication_was_running && (binlog_reader_ != nullptr)) {
          spdlog::info("Restarting binlog replication after snapshot load");
          if (!binlog_reader_->Start()) {
            spdlog::warn("Failed to restart binlog replication: {}",
                        binlog_reader_->GetLastError());
          }
        }
#endif

        if (success) {
          return FormatLoadResponse(filepath);
        }
        return FormatError("Failed to load snapshot");
      }

      case query::QueryType::REPLICATION_STATUS: {
        return FormatReplicationStatusResponse();
      }

      case query::QueryType::REPLICATION_STOP: {
#ifdef USE_MYSQL
        if (binlog_reader_ != nullptr) {
          if (binlog_reader_->IsRunning()) {
            spdlog::info("Stopping binlog replication by user request");
            binlog_reader_->Stop();
            return FormatReplicationStopResponse();
          }
          return FormatError("Replication is not running");

        }
        return FormatError("Replication is not configured");

#else
        return FormatError("MySQL support not compiled");
#endif
      }

      case query::QueryType::REPLICATION_START: {
#ifdef USE_MYSQL
        if (binlog_reader_ != nullptr) {
          if (!binlog_reader_->IsRunning()) {
            spdlog::info("Starting binlog replication by user request");
            if (binlog_reader_->Start()) {
              return FormatReplicationStartResponse();
            }
            return FormatError("Failed to start replication: " + binlog_reader_->GetLastError());

          }
          return FormatError("Replication is already running");

        }
        return FormatError("Replication is not configured");
#else
        return FormatError("MySQL support not compiled");
#endif
      }

      case query::QueryType::CONFIG: {
        return FormatConfigResponse();
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

std::string TcpServer::FormatInfoResponse() {
  std::ostringstream oss;
  oss << "OK INFO\r\n\r\n";

  // Server information
  oss << "# Server\r\n";
  oss << "version: MygramDB 1.0.0\r\n";
  oss << "uptime_seconds: " << stats_.GetUptimeSeconds() << "\r\n";
  oss << "\r\n";

  // Stats - Command counters
  oss << "# Stats\r\n";
  oss << "total_commands_processed: " << stats_.GetTotalCommands() << "\r\n";
  oss << "total_connections_received: " << stats_.GetStatistics().total_connections_received << "\r\n";
  oss << "total_requests: " << stats_.GetTotalRequests() << "\r\n";
  oss << "\r\n";

  // Command counters
  oss << "# Commandstats\r\n";
  auto cmd_stats = stats_.GetStatistics();
  if (cmd_stats.cmd_search > 0) {
    oss << "cmd_search: " << cmd_stats.cmd_search << "\r\n";
  }
  if (cmd_stats.cmd_count > 0) {
    oss << "cmd_count: " << cmd_stats.cmd_count << "\r\n";
  }
  if (cmd_stats.cmd_get > 0) {
    oss << "cmd_get: " << cmd_stats.cmd_get << "\r\n";
  }
  if (cmd_stats.cmd_info > 0) {
    oss << "cmd_info: " << cmd_stats.cmd_info << "\r\n";
  }
  if (cmd_stats.cmd_save > 0) {
    oss << "cmd_save: " << cmd_stats.cmd_save << "\r\n";
  }
  if (cmd_stats.cmd_load > 0) {
    oss << "cmd_load: " << cmd_stats.cmd_load << "\r\n";
  }
  if (cmd_stats.cmd_replication_status > 0) {
    oss << "cmd_replication_status: " << cmd_stats.cmd_replication_status << "\r\n";
  }
  if (cmd_stats.cmd_replication_stop > 0) {
    oss << "cmd_replication_stop: " << cmd_stats.cmd_replication_stop << "\r\n";
  }
  if (cmd_stats.cmd_replication_start > 0) {
    oss << "cmd_replication_start: " << cmd_stats.cmd_replication_start << "\r\n";
  }
  if (cmd_stats.cmd_config > 0) {
    oss << "cmd_config: " << cmd_stats.cmd_config << "\r\n";
  }
  oss << "\r\n";

  // Memory statistics
  oss << "# Memory\r\n";
  size_t index_memory = index_.MemoryUsage();
  size_t doc_memory = doc_store_.MemoryUsage();
  size_t total_memory = index_memory + doc_memory;

  // Update memory stats in real-time
  const_cast<ServerStats&>(stats_).UpdateMemoryUsage(total_memory);

  oss << "used_memory_bytes: " << total_memory << "\r\n";
  oss << "used_memory_human: " << utils::FormatBytes(total_memory) << "\r\n";
  oss << "used_memory_peak_bytes: " << stats_.GetPeakMemoryUsage() << "\r\n";
  oss << "used_memory_peak_human: " << utils::FormatBytes(stats_.GetPeakMemoryUsage()) << "\r\n";
  oss << "used_memory_index: " << utils::FormatBytes(index_memory) << "\r\n";
  oss << "used_memory_documents: " << utils::FormatBytes(doc_memory) << "\r\n";

  // Memory fragmentation estimate
  if (total_memory > 0) {
    size_t peak = stats_.GetPeakMemoryUsage();
    double fragmentation = peak > 0 ? static_cast<double>(peak) / total_memory : 1.0;
    oss << "memory_fragmentation_ratio: " << std::fixed << std::setprecision(2)
        << fragmentation << "\r\n";
  }
  oss << "\r\n";

  // Index statistics
  oss << "# Index\r\n";
  auto index_stats = index_.GetStatistics();
  oss << "total_documents: " << doc_store_.Size() << "\r\n";
  oss << "total_terms: " << index_stats.total_terms << "\r\n";
  oss << "total_postings: " << index_stats.total_postings << "\r\n";
  if (index_stats.total_terms > 0) {
    double avg_postings = static_cast<double>(index_stats.total_postings) / index_stats.total_terms;
    oss << "avg_postings_per_term: " << std::fixed << std::setprecision(2)
        << avg_postings << "\r\n";
  }
  oss << "delta_encoded_lists: " << index_stats.delta_encoded_lists << "\r\n";
  oss << "roaring_bitmap_lists: " << index_stats.roaring_bitmap_lists << "\r\n";

  // N-gram configuration
  if (ngram_size_ == 0) {
    oss << "ngram_mode: hybrid (kanji=1, others=2)\r\n";
  } else {
    oss << "ngram_size: " << ngram_size_ << "\r\n";
  }
  oss << "\r\n";

  // Clients
  oss << "# Clients\r\n";
  oss << "connected_clients: " << stats_.GetActiveConnections() << "\r\n";
  oss << "\r\n";

  // Replication information
#ifdef USE_MYSQL
  if (binlog_reader_ != nullptr) {
    oss << "# Replication\r\n";
    oss << "replication_status: " << (binlog_reader_->IsRunning() ? "running" : "stopped") << "\r\n";
    oss << "replication_gtid: " << binlog_reader_->GetCurrentGTID() << "\r\n";
    oss << "replication_events: " << binlog_reader_->GetProcessedEvents() << "\r\n";
    oss << "\r\n";
  }
#endif

  oss << "END";
  return oss.str();
}

std::string TcpServer::FormatSaveResponse(const std::string& filepath) {
  return "OK SAVED " + filepath;
}

std::string TcpServer::FormatLoadResponse(const std::string& filepath) {
  return "OK LOADED " + filepath;
}

std::string TcpServer::FormatReplicationStatusResponse() {
#ifdef USE_MYSQL
  std::ostringstream oss;
  oss << "OK REPLICATION\r\n";

  if (binlog_reader_ != nullptr) {
    bool is_running = binlog_reader_->IsRunning();
    oss << "status: " << (is_running ? "running" : "stopped") << "\r\n";
    oss << "current_gtid: " << binlog_reader_->GetCurrentGTID() << "\r\n";
    oss << "processed_events: " << binlog_reader_->GetProcessedEvents() << "\r\n";

    if (is_running) {
      oss << "queue_size: " << binlog_reader_->GetQueueSize() << "\r\n";
    }
  } else {
    oss << "status: not_configured\r\n";
  }

  oss << "END";
  return oss.str();
#else
  return FormatError("MySQL support not compiled");
#endif
}

std::string TcpServer::FormatReplicationStopResponse() {
  return "OK REPLICATION_STOPPED";
}

std::string TcpServer::FormatReplicationStartResponse() {
  return "OK REPLICATION_STARTED";
}

std::string TcpServer::FormatConfigResponse() {
  std::ostringstream oss;
  oss << "OK CONFIG\n";

  if (full_config_ == nullptr) {
    oss << "  [Configuration not available]\n";
    return oss.str();
  }

  // MySQL configuration
  oss << "  mysql:\n";
  oss << "    host: " << full_config_->mysql.host << "\n";
  oss << "    port: " << full_config_->mysql.port << "\n";
  oss << "    user: " << full_config_->mysql.user << "\n";
  oss << "    database: " << full_config_->mysql.database << "\n";
  oss << "    use_gtid: " << (full_config_->mysql.use_gtid ? "true" : "false") << "\n";

  // Tables configuration
  oss << "  tables: " << full_config_->tables.size() << "\n";
  for (const auto& table : full_config_->tables) {
    oss << "    - name: " << table.name << "\n";
    oss << "      primary_key: " << table.primary_key << "\n";
    oss << "      ngram_size: " << table.ngram_size << "\n";
    oss << "      filters: " << table.filters.size() << "\n";
  }

  // API configuration
  oss << "  api:\n";
  oss << "    tcp.bind: " << full_config_->api.tcp.bind << "\n";
  oss << "    tcp.port: " << full_config_->api.tcp.port << "\n";

  // Replication configuration
  oss << "  replication:\n";
  oss << "    enable: " << (full_config_->replication.enable ? "true" : "false") << "\n";
  oss << "    server_id: " << full_config_->replication.server_id << "\n";
  oss << "    start_from: " << full_config_->replication.start_from << "\n";
  oss << "    state_file: " << full_config_->replication.state_file << "\n";

  // Memory configuration
  oss << "  memory:\n";
  oss << "    hard_limit_mb: " << full_config_->memory.hard_limit_mb << "\n";
  oss << "    soft_target_mb: " << full_config_->memory.soft_target_mb << "\n";
  oss << "    roaring_threshold: " << full_config_->memory.roaring_threshold << "\n";

  // Snapshot configuration
  oss << "  snapshot:\n";
  oss << "    dir: " << full_config_->snapshot.dir << "\n";

  // Logging configuration
  oss << "  logging:\n";
  oss << "    level: " << full_config_->logging.level << "\n";

  // Runtime status
  oss << "  runtime:\n";
  oss << "    connections: " << GetConnectionCount() << "\n";
  oss << "    max_connections: " << config_.max_connections << "\n";
  oss << "    read_only: " << (read_only_ ? "true" : "false") << "\n";
  oss << "    uptime: " << stats_.GetUptimeSeconds() << "s\n";

  return oss.str();
}

std::string TcpServer::FormatError(const std::string& message) {
  return "ERROR " + message;
}

bool TcpServer::SetSocketOptions(int socket_fd) {
  // Reuse address
  int opt = 1;
  if (setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
    last_error_ = "Failed to set SO_REUSEADDR: " + std::string(strerror(errno));
    spdlog::error(last_error_);
    return false;
  }

  // Set receive buffer size
  int recv_buf = config_.recv_buffer_size;
  if (setsockopt(socket_fd, SOL_SOCKET, SO_RCVBUF, &recv_buf, sizeof(recv_buf)) < 0) {
    spdlog::warn("Failed to set SO_RCVBUF: {}", strerror(errno));
  }

  // Set send buffer size
  int send_buf = config_.send_buffer_size;
  if (setsockopt(socket_fd, SOL_SOCKET, SO_SNDBUF, &send_buf, sizeof(send_buf)) < 0) {
    spdlog::warn("Failed to set SO_SNDBUF: {}", strerror(errno));
  }

  return true;
}

void TcpServer::RemoveConnection(int socket_fd) {
  std::scoped_lock lock(connections_mutex_);
  connection_fds_.erase(socket_fd);
}

}  // namespace server
}  // namespace mygramdb
