/**
 * @file connection_io_handler.cpp
 * @brief Network I/O handler implementation
 */

#include "server/connection_io_handler.h"

#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

#include "server/server_types.h"

namespace mygramdb::server {

ConnectionIOHandler::ConnectionIOHandler(const IOConfig& config, RequestProcessor processor,
                                         const std::atomic<bool>& shutdown_flag)
    : config_(config), processor_(std::move(processor)), shutdown_flag_(shutdown_flag) {}

void ConnectionIOHandler::HandleConnection(int client_fd, ConnectionContext& ctx) {
  // Set receive timeout on the socket if configured
  if (config_.recv_timeout_sec > 0) {
    struct timeval timeout {};  // Zero-initialized to avoid uninitialized warning
    timeout.tv_sec = config_.recv_timeout_sec;
    timeout.tv_usec = 0;
    if (setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
      spdlog::warn("Failed to set SO_RCVTIMEO on fd {}: {}", client_fd, strerror(errno));
      // Continue anyway - timeout is not critical for functionality
    }
  }

  std::vector<char> buffer(config_.recv_buffer_size);
  std::string accumulated;
  const size_t max_accumulated = config_.max_query_length * 10;

  while (!shutdown_flag_) {
    ssize_t bytes = recv(client_fd, buffer.data(), buffer.size() - 1, 0);

    if (bytes <= 0) {
      if (bytes < 0) {
        // With SO_RCVTIMEO set, timeout will trigger EAGAIN/EWOULDBLOCK
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          spdlog::debug("recv timeout on fd {}, closing connection", client_fd);
          break;  // Timeout - close connection
        }
        spdlog::debug("recv error on fd {}: {}", client_fd, strerror(errno));
      }
      break;
    }

    buffer[bytes] = '\0';

    // Check buffer size limit
    if (accumulated.size() + bytes > max_accumulated) {
      spdlog::warn("Accumulated buffer exceeded limit on fd {}, closing", client_fd);
      SendResponse(client_fd, "ERROR Request too large (no newline detected)");
      break;
    }

    accumulated += buffer.data();

    // Process complete requests
    if (!ProcessBuffer(accumulated, client_fd, ctx)) {
      break;
    }
  }
}

bool ConnectionIOHandler::ProcessBuffer(std::string& accumulated, int client_fd, ConnectionContext& ctx) {
  size_t pos = 0;

  while ((pos = accumulated.find("\r\n")) != std::string::npos) {
    std::string request = accumulated.substr(0, pos);
    accumulated = accumulated.substr(pos + 2);

    if (request.empty()) {
      continue;
    }

    // Process request
    std::string response = processor_(request, ctx);

    // Send response
    if (!SendResponse(client_fd, response)) {
      return false;
    }
  }

  return true;
}

// Kept as member function for consistency and potential future extensions
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
bool ConnectionIOHandler::SendResponse(int client_fd, const std::string& response) {
  std::string full_response = response + "\r\n";
  size_t total_sent = 0;
  size_t to_send = full_response.length();

  // Handle partial sends
  while (total_sent < to_send) {
    // Use MSG_NOSIGNAL to prevent SIGPIPE when client closes connection unexpectedly
    // Pointer arithmetic needed for partial send resumption with POSIX send()
    ssize_t sent = send(client_fd, full_response.c_str() + total_sent,  // NOLINT(cppcoreguidelines-pro-bounds-pointer-arithmetic)
                        to_send - total_sent, MSG_NOSIGNAL);

    if (sent < 0) {
      if (errno == EINTR) {
        continue;  // Interrupted, retry
      }
      // EPIPE is expected when client closes connection
      if (errno != EPIPE) {
        spdlog::debug("send error on fd {}: {}", client_fd, strerror(errno));
      }
      return false;
    }

    if (sent == 0) {
      spdlog::debug("send returned 0 on fd {}", client_fd);
      return false;
    }

    total_sent += sent;
  }

  return true;
}

}  // namespace mygramdb::server
