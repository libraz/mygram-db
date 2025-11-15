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
  std::vector<char> buffer(config_.recv_buffer_size);
  std::string accumulated;
  const size_t max_accumulated = config_.max_query_length * 10;

  while (!shutdown_flag_) {
    ssize_t bytes = recv(client_fd, buffer.data(), buffer.size() - 1, 0);

    if (bytes <= 0) {
      if (bytes < 0) {
        // Timeout is normal (EAGAIN/EWOULDBLOCK)
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          continue;
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
    // Pointer arithmetic needed for partial send resumption with POSIX send()
    // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
    ssize_t sent = send(client_fd, full_response.c_str() + total_sent, to_send - total_sent, 0);

    if (sent < 0) {
      if (errno == EINTR) {
        continue;  // Interrupted, retry
      }
      spdlog::debug("send error on fd {}: {}", client_fd, strerror(errno));
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
