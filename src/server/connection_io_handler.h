/**
 * @file connection_io_handler.h
 * @brief Handles network I/O for client connections
 */

#pragma once

#include <atomic>
#include <functional>
#include <string>
#include <vector>

// Forward declare to avoid circular dependency
namespace mygramdb::server {
struct ConnectionContext;
}

namespace mygramdb::server {

// Constants for I/O configuration defaults
inline constexpr size_t kDefaultIORecvBufferSize = 4096;  // Separate from server_types.h to avoid conflicts
inline constexpr size_t kDefaultMaxQueryLength = 1024 * 1024;  // 1MB
inline constexpr int kDefaultRecvTimeoutSec = 60;

/**
 * @brief Configuration for connection I/O handling
 */
struct IOConfig {
  size_t recv_buffer_size = kDefaultIORecvBufferSize;
  size_t max_query_length = kDefaultMaxQueryLength;
  int recv_timeout_sec = kDefaultRecvTimeoutSec;
};

/**
 * @brief Callback for processing complete requests
 * @param request The request string (without \r\n)
 * @param ctx Connection context (may be modified)
 * @return Response string (without \r\n)
 */
using RequestProcessor = std::function<std::string(const std::string&, ConnectionContext&)>;

/**
 * @brief Handles network I/O for a single client connection
 *
 * Responsibilities:
 * - Read data from socket with buffering
 * - Parse protocol messages (delimiter: \r\n)
 * - Enforce size limits
 * - Write responses to socket
 * - Handle I/O errors gracefully
 */
class ConnectionIOHandler {
 public:
  /**
   * @brief Construct I/O handler
   * @param config I/O configuration
   * @param processor Callback to process complete requests
   * @param shutdown_flag Reference to shutdown signal
   */
  ConnectionIOHandler(const IOConfig& config, RequestProcessor processor, const std::atomic<bool>& shutdown_flag);

  ~ConnectionIOHandler() = default;

  // Non-copyable and non-movable
  ConnectionIOHandler(const ConnectionIOHandler&) = delete;
  ConnectionIOHandler& operator=(const ConnectionIOHandler&) = delete;
  ConnectionIOHandler(ConnectionIOHandler&&) = delete;
  ConnectionIOHandler& operator=(ConnectionIOHandler&&) = delete;

  /**
   * @brief Handle connection I/O loop
   * @param client_fd Client socket file descriptor
   * @param ctx Connection context
   *
   * Runs until:
   * - Client disconnects
   * - I/O error occurs
   * - Shutdown signal received
   */
  void HandleConnection(int client_fd, ConnectionContext& ctx);

 private:
  const IOConfig config_;
  RequestProcessor processor_;
  const std::atomic<bool>& shutdown_flag_;

  /**
   * @brief Process accumulated buffer and extract complete requests
   * @param accumulated Current buffer
   * @param client_fd Socket for sending responses
   * @param ctx Connection context
   * @return True if connection should continue
   */
  bool ProcessBuffer(std::string& accumulated, int client_fd, ConnectionContext& ctx);

  /**
   * @brief Send response to client
   * @param client_fd Socket file descriptor
   * @param response Response string (will add \r\n)
   * @return True if send succeeded
   *
   * Note: Kept as member function (not static) for consistency with other handlers
   * and potential future extensions (e.g., response buffering, metrics)
   */
  // NOLINTNEXTLINE(readability-convert-member-functions-to-static)
  bool SendResponse(int client_fd, const std::string& response);
};

}  // namespace mygramdb::server
