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
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <utility>

#include "query/result_sorter.h"
#include "utils/network_utils.h"
#include "utils/string_utils.h"
#include "version.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#endif

namespace mygramdb::server {

namespace {
// Thread pool queue size for backpressure
constexpr size_t kThreadPoolQueueSize = 1000;

// Buffer size for IP address formatting
constexpr size_t kIpAddressBufferSize = 64;

// Length of "gtid=\"" prefix in meta content
constexpr size_t kGtidPrefixLength = 7;

/**
 * @brief Helper to safely cast sockaddr_in* to sockaddr* for socket API
 *
 * POSIX socket API requires sockaddr* but we use sockaddr_in for IPv4.
 * This helper centralizes the required reinterpret_cast to a single location.
 *
 * Why reinterpret_cast is necessary here:
 * - POSIX socket functions (bind, accept, getsockname) require struct sockaddr*
 * - We use struct sockaddr_in for IPv4, which is binary-compatible
 * - This is the standard pattern in all POSIX socket programming
 * - The cast is safe as both types share the same memory layout for the address family
 *
 * @param addr Pointer to sockaddr_in structure
 * @return Pointer to sockaddr (same memory location, different type)
 */
inline struct sockaddr* ToSockaddr(struct sockaddr_in* addr) {
  // Suppressing clang-tidy warning for POSIX socket API compatibility
  // This reinterpret_cast is required and safe for socket address structures
  return reinterpret_cast<struct sockaddr*>(addr);  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
}

}  // namespace

TcpServer::TcpServer(ServerConfig config, std::unordered_map<std::string, TableContext*> table_contexts,
                     std::string snapshot_dir, const config::Config* full_config,
#ifdef USE_MYSQL
                     mysql::BinlogReader* binlog_reader
#else
                     void* binlog_reader
#endif
                     )
    : config_(std::move(config)),
      table_contexts_(std::move(table_contexts)),
      snapshot_dir_(std::move(snapshot_dir)),
      full_config_(full_config),
      binlog_reader_(binlog_reader) {
  // Create thread pool
  thread_pool_ =
      std::make_unique<ThreadPool>(config_.worker_threads > 0 ? config_.worker_threads : 0, kThreadPoolQueueSize);
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
  struct sockaddr_in address = {};
  std::memset(&address, 0, sizeof(address));
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = INADDR_ANY;
  address.sin_port = htons(config_.port);

  if (bind(server_fd_, ToSockaddr(&address), sizeof(address)) < 0) {
    last_error_ = "Failed to bind to port " + std::to_string(config_.port) + ": " + std::string(strerror(errno));
    spdlog::error(last_error_);
    close(server_fd_);
    server_fd_ = -1;
    return false;
  }

  // Get actual port if port 0 was specified
  if (config_.port == 0) {
    socklen_t addr_len = sizeof(address);
    if (getsockname(server_fd_, ToSockaddr(&address), &addr_len) == 0) {
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
  accept_thread_ = std::make_unique<std::thread>(&TcpServer::AcceptThreadFunc, this);

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
  spdlog::info("TCP server stopped. Handled {} total requests", stats_.GetTotalRequests());
}

void TcpServer::AcceptThreadFunc() {
  spdlog::info("Accept thread started");

  while (!should_stop_) {
    struct sockaddr_in client_addr = {};
    socklen_t client_len = sizeof(client_addr);

    int client_fd = accept(server_fd_, ToSockaddr(&client_addr), &client_len);

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
      spdlog::warn("Connection limit reached ({}), rejecting new connection", config_.max_connections);
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
    bool submitted = thread_pool_->Submit([this, client_fd]() { HandleClient(client_fd); });

    if (!submitted) {
      spdlog::warn("Thread pool queue full, rejecting connection");
      RemoveConnection(client_fd);
      shutdown(client_fd, SHUT_RDWR);
      close(client_fd);
      stats_.DecrementConnections();
      continue;
    }

    spdlog::debug("Accepted connection from {}:{} (active: {})", inet_ntoa(client_addr.sin_addr),
                  ntohs(client_addr.sin_port), stats_.GetActiveConnections());
  }

  spdlog::info("Accept thread stopped");
}

void TcpServer::HandleClient(int client_fd) {
  std::vector<char> buffer(config_.recv_buffer_size);
  std::string accumulated;

  // Initialize connection context
  ConnectionContext ctx;
  ctx.client_fd = client_fd;
  ctx.debug_mode = false;
  {
    std::scoped_lock<std::mutex> lock(contexts_mutex_);
    connection_contexts_[client_fd] = ctx;
  }

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

      // Get connection context
      {
        std::scoped_lock<std::mutex> lock(contexts_mutex_);
        ctx = connection_contexts_[client_fd];
      }

      // Process request
      std::string response = ProcessRequest(request, ctx);
      stats_.IncrementRequests();

      // Update connection context (in case debug mode changed)
      {
        std::scoped_lock<std::mutex> lock(contexts_mutex_);
        connection_contexts_[client_fd] = ctx;
      }

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
  {
    std::scoped_lock<std::mutex> lock(contexts_mutex_);
    connection_contexts_.erase(client_fd);
  }
  close(client_fd);
  stats_.DecrementConnections();

  spdlog::debug("Connection closed (active: {})", stats_.GetActiveConnections());
}

std::string TcpServer::ProcessRequest(const std::string& request, ConnectionContext& ctx) {
  spdlog::debug("Processing request: {}", request);

  // Start timing for debug mode
  auto start_time = std::chrono::high_resolution_clock::now();
  query::DebugInfo debug_info;

  // Parse query
  auto query = query_parser_.Parse(request);

  if (!query.IsValid()) {
    return FormatError(query_parser_.GetError());
  }

  // Increment command statistics
  stats_.IncrementCommand(query.type);

  // Lookup table from query
  index::Index* current_index = nullptr;
  storage::DocumentStore* current_doc_store = nullptr;
  int current_ngram_size = 0;
  int current_kanji_ngram_size = 0;

  // For queries that require a table, validate and fetch context
  if (!query.table.empty()) {
    auto table_iter = table_contexts_.find(query.table);
    if (table_iter == table_contexts_.end()) {
      return FormatError("Table not found: " + query.table);
    }
    current_index = table_iter->second->index.get();
    current_doc_store = table_iter->second->doc_store.get();
    current_ngram_size = table_iter->second->config.ngram_size;
    current_kanji_ngram_size = table_iter->second->config.kanji_ngram_size;
  }

  try {
    switch (query.type) {
      case query::QueryType::SEARCH: {
        // Check if server is loading
        if (loading_) {
          return FormatError("Server is loading, please try again later");
        }

        // Verify index is available
        if (current_index == nullptr) {
          return FormatError("Index not available");
        }

        // Start index search timing
        auto index_start = std::chrono::high_resolution_clock::now();

        // Collect all search terms (main + AND terms)
        std::vector<std::string> all_search_terms;
        all_search_terms.push_back(query.search_text);
        all_search_terms.insert(all_search_terms.end(), query.and_terms.begin(), query.and_terms.end());

        // Collect debug info for search terms
        if (ctx.debug_mode) {
          debug_info.search_terms = all_search_terms;
        }

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
          // Always use hybrid n-grams if kanji_ngram_size is configured
          if (current_kanji_ngram_size > 0) {
            ngrams = utils::GenerateHybridNgrams(normalized, current_ngram_size, current_kanji_ngram_size);
          } else if (current_ngram_size == 0) {
            ngrams = utils::GenerateHybridNgrams(normalized);
          } else {
            ngrams = utils::GenerateNgrams(normalized, current_ngram_size);
          }

          // Estimate result size by checking the smallest posting list
          size_t min_size = std::numeric_limits<size_t>::max();
          for (const auto& ngram : ngrams) {
            const auto* posting = current_index->GetPostingList(ngram);
            if (posting != nullptr) {
              min_size = std::min(min_size, static_cast<size_t>(posting->Size()));
            } else {
              min_size = 0;
              break;
            }
          }

          // Collect debug info for n-grams and posting list sizes
          if (ctx.debug_mode) {
            for (const auto& ngram : ngrams) {
              debug_info.ngrams_used.push_back(ngram);
            }
            debug_info.posting_list_sizes.push_back(min_size);
          }

          term_infos.push_back({std::move(ngrams), min_size});
        }

        // Sort terms by estimated size (smallest first for faster intersection)
        std::sort(term_infos.begin(), term_infos.end(),
                  [](const TermInfo& lhs, const TermInfo& rhs) { return lhs.estimated_size < rhs.estimated_size; });

        // If any term has zero results, return empty immediately
        if (!term_infos.empty() && term_infos[0].estimated_size == 0) {
          if (ctx.debug_mode) {
            debug_info.optimization_used = "early-exit (empty posting list)";
            debug_info.final_results = 0;
            auto end_time = std::chrono::high_resolution_clock::now();
            debug_info.query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
            debug_info.index_time_ms = std::chrono::duration<double, std::milli>(end_time - index_start).count();
            return FormatSearchResponse({}, 0, current_doc_store, &debug_info);
          }
          return FormatSearchResponse({}, 0, current_doc_store);
        }

        // Process most selective term first
        auto results = current_index->SearchAnd(term_infos[0].ngrams);
        if (ctx.debug_mode) {
          debug_info.total_candidates = results.size();
          debug_info.after_intersection = results.size();
          debug_info.optimization_used = "size-based term ordering";
        }

        // Intersect with remaining terms
        for (size_t i = 1; i < term_infos.size() && !results.empty(); ++i) {
          auto and_results = current_index->SearchAnd(term_infos[i].ngrams);
          std::vector<storage::DocId> intersection;
          intersection.reserve(std::min(results.size(), and_results.size()));
          std::set_intersection(results.begin(), results.end(), and_results.begin(), and_results.end(),
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
            if (current_ngram_size == 0) {
              ngrams = utils::GenerateHybridNgrams(norm_not);
            } else {
              ngrams = utils::GenerateNgrams(norm_not, current_ngram_size);
            }
            not_ngrams.insert(not_ngrams.end(), ngrams.begin(), ngrams.end());
          }

          results = current_index->SearchNot(results, not_ngrams);
          if (ctx.debug_mode) {
            debug_info.after_not = results.size();
          }
        } else if (ctx.debug_mode) {
          debug_info.after_not = results.size();
        }

        // Apply filter conditions
        auto filter_start = std::chrono::high_resolution_clock::now();
        if (!query.filters.empty()) {
          std::vector<storage::DocId> filtered_results;
          filtered_results.reserve(results.size());

          for (const auto& doc_id : results) {
            bool matches_all_filters = true;

            for (const auto& filter_cond : query.filters) {
              auto stored_value = current_doc_store->GetFilterValue(doc_id, filter_cond.column);

              // For now, only support equality operator
              if (filter_cond.op == query::FilterOp::EQ) {
                // Convert filter value string to appropriate type and compare
                bool matches = stored_value && std::visit(
                                                   [&](const auto& val) {
                                                     using T = std::decay_t<decltype(val)>;
                                                     if constexpr (std::is_same_v<T, std::monostate>) {
                                                       // NULL value never matches
                                                       return false;
                                                     } else if constexpr (std::is_same_v<T, std::string>) {
                                                       return val == filter_cond.value;
                                                     } else {
                                                       return std::to_string(val) == filter_cond.value;
                                                     }
                                                   },
                                                   stored_value.value());

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
          if (ctx.debug_mode) {
            auto filter_end = std::chrono::high_resolution_clock::now();
            debug_info.filter_time_ms = std::chrono::duration<double, std::milli>(filter_end - filter_start).count();
            debug_info.after_filters = results.size();
          }
        } else if (ctx.debug_mode) {
          debug_info.after_filters = results.size();
        }

        // Sort and paginate results using ResultSorter
        // This uses in-place sorting with partial_sort optimization for memory efficiency
        size_t total_results = results.size();
        auto sorted_results = query::ResultSorter::SortAndPaginate(results, *current_doc_store, query);

        // Calculate final debug info
        if (ctx.debug_mode) {
          auto end_time = std::chrono::high_resolution_clock::now();
          auto index_end = std::chrono::high_resolution_clock::now();
          debug_info.query_time_ms = std::chrono::duration<double, std::milli>(end_time - start_time).count();
          debug_info.index_time_ms = std::chrono::duration<double, std::milli>(index_end - index_start).count();
          debug_info.final_results = sorted_results.size();
          return FormatSearchResponse(sorted_results, total_results, current_doc_store, &debug_info);
        }

        return FormatSearchResponse(sorted_results, total_results, current_doc_store);
      }

      case query::QueryType::COUNT: {
        // Check if server is loading
        if (loading_) {
          return FormatError("Server is loading, please try again later");
        }

        // Verify index is available
        if (current_index == nullptr) {
          return FormatError("Index not available");
        }

        // Collect all search terms (main + AND terms)
        std::vector<std::string> all_search_terms;
        all_search_terms.push_back(query.search_text);
        all_search_terms.insert(all_search_terms.end(), query.and_terms.begin(), query.and_terms.end());

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
          // Always use hybrid n-grams if kanji_ngram_size is configured
          if (current_kanji_ngram_size > 0) {
            ngrams = utils::GenerateHybridNgrams(normalized, current_ngram_size, current_kanji_ngram_size);
          } else if (current_ngram_size == 0) {
            ngrams = utils::GenerateHybridNgrams(normalized);
          } else {
            ngrams = utils::GenerateNgrams(normalized, current_ngram_size);
          }

          // Estimate result size by checking the smallest posting list
          size_t min_size = std::numeric_limits<size_t>::max();
          for (const auto& ngram : ngrams) {
            const auto* posting = current_index->GetPostingList(ngram);
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
                  [](const TermInfo& lhs, const TermInfo& rhs) { return lhs.estimated_size < rhs.estimated_size; });

        // If any term has zero results, return 0 immediately
        if (!term_infos.empty() && term_infos[0].estimated_size == 0) {
          return FormatCountResponse(0);
        }

        // Process most selective term first
        auto results = current_index->SearchAnd(term_infos[0].ngrams);

        // Intersect with remaining terms
        for (size_t i = 1; i < term_infos.size() && !results.empty(); ++i) {
          auto and_results = current_index->SearchAnd(term_infos[i].ngrams);
          std::vector<storage::DocId> intersection;
          intersection.reserve(std::min(results.size(), and_results.size()));
          std::set_intersection(results.begin(), results.end(), and_results.begin(), and_results.end(),
                                std::back_inserter(intersection));
          results = std::move(intersection);
        }

        // Apply NOT filter if present
        if (!query.not_terms.empty()) {
          std::vector<std::string> not_ngrams;
          for (const auto& not_term : query.not_terms) {
            std::string norm_not = utils::NormalizeText(not_term, true, "keep", true);
            std::vector<std::string> ngrams;
            if (current_ngram_size == 0) {
              ngrams = utils::GenerateHybridNgrams(norm_not);
            } else {
              ngrams = utils::GenerateNgrams(norm_not, current_ngram_size);
            }
            not_ngrams.insert(not_ngrams.end(), ngrams.begin(), ngrams.end());
          }

          results = current_index->SearchNot(results, not_ngrams);
        }

        // Apply filter conditions
        if (!query.filters.empty()) {
          std::vector<storage::DocId> filtered_results;
          filtered_results.reserve(results.size());

          for (const auto& doc_id : results) {
            bool matches_all_filters = true;

            for (const auto& filter_cond : query.filters) {
              auto stored_value = current_doc_store->GetFilterValue(doc_id, filter_cond.column);

              // For now, only support equality operator
              if (filter_cond.op == query::FilterOp::EQ) {
                // Convert filter value string to appropriate type and compare
                bool matches = stored_value && std::visit(
                                                   [&](const auto& val) {
                                                     using T = std::decay_t<decltype(val)>;
                                                     if constexpr (std::is_same_v<T, std::monostate>) {
                                                       // NULL value never matches
                                                       return false;
                                                     } else if constexpr (std::is_same_v<T, std::string>) {
                                                       return val == filter_cond.value;
                                                     } else {
                                                       return std::to_string(val) == filter_cond.value;
                                                     }
                                                   },
                                                   stored_value.value());

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
        // Check if server is loading
        if (loading_) {
          return FormatError("Server is loading, please try again later");
        }

        // Verify doc_store is available
        if (current_doc_store == nullptr) {
          return FormatError("Document store not available");
        }

        auto doc_id_opt = current_doc_store->GetDocId(query.primary_key);
        if (!doc_id_opt) {
          return FormatError("Document not found");
        }

        auto doc = current_doc_store->GetDocument(doc_id_opt.value());
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
          std::array<char, kIpAddressBufferSize> buf{};
          std::strftime(buf.data(), buf.size(), "snapshot_%Y%m%d_%H%M%S", std::localtime(&now));
          filepath = snapshot_dir_ + "/" + std::string(buf.data());
        }

        // Get current GTID from shared binlog reader
        // TODO: Need access to shared binlog_reader from main
        std::string current_gtid;
        spdlog::info("GTID capture in SAVE not yet implemented (need shared binlog_reader reference)");

        // Set read-only mode
        read_only_ = true;

        // Save all tables to directory
        bool success = false;

        // Create directory
        if (system(("mkdir -p \"" + filepath + "\"").c_str()) != 0) {
          read_only_ = false;
          return FormatError("Failed to create snapshot directory");
        }

        // Save each table
        success = true;
        for (const auto& [table_name, ctx] : table_contexts_) {
          std::string table_index_path = filepath;
          table_index_path += "/";
          table_index_path += table_name;
          table_index_path += ".index";
          std::string table_doc_path = filepath;
          table_doc_path += "/";
          table_doc_path += table_name;
          table_doc_path += ".docs";

          if (!ctx->index->SaveToFile(table_index_path) || !ctx->doc_store->SaveToFile(table_doc_path, "")) {
            spdlog::error("Failed to save table '{}'", table_name);
            success = false;
            break;
          }
          spdlog::info("Saved table '{}' to snapshot", table_name);
        }

        // Save metadata
        if (success) {
          std::string meta_path = filepath + "/meta.json";
          std::ofstream meta_file(meta_path);
          if (meta_file) {
            // NOLINTNEXTLINE(modernize-raw-string-literal)
            meta_file << "{\n";
            meta_file << "  \"version\": \"1.0\",\n";
            // NOLINTNEXTLINE(modernize-raw-string-literal)
            meta_file << "  \"gtid\": \"" << current_gtid << "\",\n";
            meta_file << "  \"tables\": [";
            bool first = true;
            for (const auto& [table_name, ctx] : table_contexts_) {
              if (!first) {
                meta_file << ", ";
              }
              meta_file << "\"" << table_name << "\"";
              first = false;
            }
            meta_file << "]\n";
            meta_file << "}\n";
            meta_file.close();
            spdlog::info("Saved snapshot metadata with {} table(s)", table_contexts_.size());
          } else {
            spdlog::error("Failed to write metadata file");
            success = false;
          }
        }

        // Clear read-only mode
        read_only_ = false;

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

        // TODO: Stop shared binlog replication if running (need binlog_reader reference)

        // Set loading mode (blocks queries)
        loading_ = true;

        // Load from directory
        // Check if directory exists
        struct stat file_stat = {};
        if (stat(filepath.c_str(), &file_stat) != 0 || !S_ISDIR(file_stat.st_mode)) {
          loading_ = false;
          return FormatError("Snapshot directory not found: " + filepath);
        }

        // Read metadata
        std::string meta_path = filepath + "/meta.json";
        std::ifstream meta_file(meta_path);
        if (!meta_file) {
          loading_ = false;
          return FormatError("Snapshot metadata file not found");
        }

        // Simple JSON parsing (extract GTID)
        std::string meta_content((std::istreambuf_iterator<char>(meta_file)), std::istreambuf_iterator<char>());
        meta_file.close();

        std::string loaded_gtid;
        size_t gtid_pos = meta_content.find("\"gtid\":");
        if (gtid_pos != std::string::npos) {
          // Skip past "gtid":" to find the opening quote of the value
          size_t quote_start = meta_content.find('\"', gtid_pos + kGtidPrefixLength);
          size_t quote_end = meta_content.find('\"', quote_start + 1);
          if (quote_start != std::string::npos && quote_end != std::string::npos) {
            loaded_gtid = meta_content.substr(quote_start + 1, quote_end - quote_start - 1);
          }
        }

        // Load each table
        bool success = true;
        for (const auto& [table_name, ctx] : table_contexts_) {
          std::string table_index_path = filepath;
          table_index_path += "/";
          table_index_path += table_name;
          table_index_path += ".index";
          std::string table_doc_path = filepath;
          table_doc_path += "/";
          table_doc_path += table_name;
          table_doc_path += ".docs";

          // Check if snapshot files exist for this table
          if (stat(table_index_path.c_str(), &file_stat) != 0) {
            spdlog::warn("Snapshot for table '{}' not found, skipping (table may be new)", table_name);
            continue;
          }

          if (!ctx->index->LoadFromFile(table_index_path) || !ctx->doc_store->LoadFromFile(table_doc_path, nullptr)) {
            spdlog::error("Failed to load table '{}'", table_name);
            success = false;
            break;
          }
          spdlog::info("Loaded table '{}' from snapshot", table_name);
        }

        // Clear loading mode
        loading_ = false;

        // TODO: Restore GTID to shared binlog_reader
        if (success && !loaded_gtid.empty()) {
          spdlog::info("Found GTID in snapshot: {} (TODO: apply to shared binlog_reader)", loaded_gtid);
        }

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
          // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
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
          // NOLINTNEXTLINE(clang-analyzer-core.CallAndMessage)
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

      case query::QueryType::OPTIMIZE: {
        // Verify index is available
        if (current_index == nullptr) {
          return FormatError("Index not available");
        }

        // Check if optimization is already running
        if (current_index->IsOptimizing()) {
          return FormatError("Optimization already in progress");
        }

        spdlog::info("Starting index optimization by user request");
        uint64_t total_docs = current_doc_store->Size();

        // Run optimization (this will block, but it's intentional for now)
        bool started = current_index->OptimizeInBatches(total_docs);

        if (started) {
          auto stats = current_index->GetStatistics();
          std::ostringstream oss;
          oss << "OK OPTIMIZED terms=" << stats.total_terms << " delta=" << stats.delta_encoded_lists
              << " roaring=" << stats.roaring_bitmap_lists;
          return oss.str();
        }
        return FormatError("Failed to start optimization");
      }

      case query::QueryType::DEBUG_ON: {
        ctx.debug_mode = true;
        spdlog::debug("Debug mode enabled for connection {}", ctx.client_fd);
        return "OK DEBUG_ON";
      }

      case query::QueryType::DEBUG_OFF: {
        ctx.debug_mode = false;
        spdlog::debug("Debug mode disabled for connection {}", ctx.client_fd);
        return "OK DEBUG_OFF";
      }

      default:
        return FormatError("Unknown query type");
    }
  } catch (const std::exception& e) {
    return FormatError(std::string("Exception: ") + e.what());
  }
}

// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
std::string TcpServer::FormatSearchResponse(const std::vector<index::DocId>& results, size_t total_results,
                                            storage::DocumentStore* doc_store, const query::DebugInfo* debug_info) {
  std::ostringstream oss;
  oss << "OK RESULTS " << total_results;

  // Results are already sorted and paginated by ResultSorter::SortAndPaginate
  // Just format them directly (no offset/limit needed)
  for (const auto& doc_id : results) {
    auto pk_opt = doc_store->GetPrimaryKey(static_cast<storage::DocId>(doc_id));
    if (pk_opt) {
      oss << " " << pk_opt.value();
    }
  }

  // Add debug information if provided
  if (debug_info != nullptr) {
    oss << " DEBUG";
    oss << " query_time=" << std::fixed << std::setprecision(3) << debug_info->query_time_ms << "ms";
    oss << " index_time=" << debug_info->index_time_ms << "ms";
    if (debug_info->filter_time_ms > 0.0) {
      oss << " filter_time=" << debug_info->filter_time_ms << "ms";
    }
    oss << " terms=" << debug_info->search_terms.size();
    oss << " ngrams=" << debug_info->ngrams_used.size();
    oss << " candidates=" << debug_info->total_candidates;
    oss << " after_intersection=" << debug_info->after_intersection;
    if (debug_info->after_not > 0) {
      oss << " after_not=" << debug_info->after_not;
    }
    if (debug_info->after_filters > 0) {
      oss << " after_filters=" << debug_info->after_filters;
    }
    oss << " final=" << debug_info->final_results;
    if (!debug_info->optimization_used.empty()) {
      oss << " optimization=\"" << debug_info->optimization_used << "\"";
    }
  }

  return oss.str();
}

std::string TcpServer::FormatCountResponse(uint64_t count, const query::DebugInfo* debug_info) {
  std::ostringstream oss;
  oss << "OK COUNT " << count;

  // Add debug information if provided
  if (debug_info != nullptr) {
    oss << " DEBUG";
    oss << " query_time=" << std::fixed << std::setprecision(3) << debug_info->query_time_ms << "ms";
    oss << " index_time=" << debug_info->index_time_ms << "ms";
    oss << " terms=" << debug_info->search_terms.size();
    oss << " ngrams=" << debug_info->ngrams_used.size();
  }

  return oss.str();
}

std::string TcpServer::FormatGetResponse(const std::optional<storage::Document>& doc) {
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
  oss << "version: " << mygramdb::Version::FullString() << "\r\n";
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

  // Aggregate memory and index statistics across all tables
  size_t total_index_memory = 0;
  size_t total_doc_memory = 0;
  size_t total_documents = 0;
  size_t total_terms = 0;
  size_t total_postings = 0;
  size_t total_delta_encoded = 0;
  size_t total_roaring_bitmap = 0;
  bool any_optimizing = false;

  for (const auto& [table_name, ctx] : table_contexts_) {
    total_index_memory += ctx->index->MemoryUsage();
    total_doc_memory += ctx->doc_store->MemoryUsage();
    total_documents += ctx->doc_store->Size();
    auto idx_stats = ctx->index->GetStatistics();
    total_terms += idx_stats.total_terms;
    total_postings += idx_stats.total_postings;
    total_delta_encoded += idx_stats.delta_encoded_lists;
    total_roaring_bitmap += idx_stats.roaring_bitmap_lists;
    if (ctx->index->IsOptimizing()) {
      any_optimizing = true;
    }
  }

  size_t total_memory = total_index_memory + total_doc_memory;

  // Memory statistics
  oss << "# Memory\r\n";

  // Update memory stats in real-time
  stats_.UpdateMemoryUsage(total_memory);

  oss << "used_memory_bytes: " << total_memory << "\r\n";
  oss << "used_memory_human: " << utils::FormatBytes(total_memory) << "\r\n";
  oss << "used_memory_peak_bytes: " << stats_.GetPeakMemoryUsage() << "\r\n";
  oss << "used_memory_peak_human: " << utils::FormatBytes(stats_.GetPeakMemoryUsage()) << "\r\n";
  oss << "used_memory_index: " << utils::FormatBytes(total_index_memory) << "\r\n";
  oss << "used_memory_documents: " << utils::FormatBytes(total_doc_memory) << "\r\n";

  // Memory fragmentation estimate
  if (total_memory > 0) {
    size_t peak = stats_.GetPeakMemoryUsage();
    double fragmentation = peak > 0 ? static_cast<double>(peak) / static_cast<double>(total_memory) : 1.0;
    oss << "memory_fragmentation_ratio: " << std::fixed << std::setprecision(2) << fragmentation << "\r\n";
  }
  oss << "\r\n";

  // Index statistics (aggregated)
  oss << "# Index\r\n";
  oss << "total_documents: " << total_documents << "\r\n";
  oss << "total_terms: " << total_terms << "\r\n";
  oss << "total_postings: " << total_postings << "\r\n";
  if (total_terms > 0) {
    double avg_postings = static_cast<double>(total_postings) / static_cast<double>(total_terms);
    oss << "avg_postings_per_term: " << std::fixed << std::setprecision(2) << avg_postings << "\r\n";
  }
  oss << "delta_encoded_lists: " << total_delta_encoded << "\r\n";
  oss << "roaring_bitmap_lists: " << total_roaring_bitmap << "\r\n";

  // Optimization status
  if (any_optimizing) {
    oss << "optimization_status: in_progress\r\n";
  } else {
    oss << "optimization_status: idle\r\n";
  }
  oss << "\r\n";

  // Tables
  oss << "# Tables\r\n";
  oss << "tables: ";
  size_t idx = 0;
  for (const auto& [name, unused_context] : table_contexts_) {
    (void)unused_context;  // Mark as intentionally unused
    if (idx++ > 0) {
      oss << ",";
    }
    oss << name;
  }
  oss << "\r\n\r\n";

  // Clients
  oss << "# Clients\r\n";
  oss << "connected_clients: " << stats_.GetActiveConnections() << "\r\n";
  oss << "\r\n";

  // Replication information
#ifdef USE_MYSQL
  oss << "# Replication\r\n";
  if (binlog_reader_ != nullptr) {
    oss << "replication_status: " << (binlog_reader_->IsRunning() ? "running" : "stopped") << "\r\n";
    oss << "replication_gtid: " << binlog_reader_->GetCurrentGTID() << "\r\n";
    oss << "replication_events: " << binlog_reader_->GetProcessedEvents() << "\r\n";
  } else {
    oss << "replication_status: disabled\r\n";
  }

  // Event statistics (always shown, even when binlog_reader is null)
  oss << "replication_inserts_applied: " << stats_.GetReplInsertsApplied() << "\r\n";
  oss << "replication_inserts_skipped: " << stats_.GetReplInsertsSkipped() << "\r\n";
  oss << "replication_updates_applied: " << stats_.GetReplUpdatesApplied() << "\r\n";
  oss << "replication_updates_added: " << stats_.GetReplUpdatesAdded() << "\r\n";
  oss << "replication_updates_removed: " << stats_.GetReplUpdatesRemoved() << "\r\n";
  oss << "replication_updates_modified: " << stats_.GetReplUpdatesModified() << "\r\n";
  oss << "replication_updates_skipped: " << stats_.GetReplUpdatesSkipped() << "\r\n";
  oss << "replication_deletes_applied: " << stats_.GetReplDeletesApplied() << "\r\n";
  oss << "replication_deletes_skipped: " << stats_.GetReplDeletesSkipped() << "\r\n";
  oss << "replication_ddl_executed: " << stats_.GetReplDdlExecuted() << "\r\n";
  oss << "replication_events_skipped_other_tables: " << stats_.GetReplEventsSkippedOtherTables() << "\r\n";
  oss << "\r\n";
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

}  // namespace mygramdb::server
