/**
 * @file connection_io_handler.cpp
 * @brief Network I/O handler implementation
 */

#include "server/connection_io_handler.h"

#include <spdlog/spdlog.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>

#include "server/server_types.h"
#include "utils/structured_log.h"

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
      mygram::utils::StructuredLog()
          .Event("server_warning")
          .Field("operation", "setsockopt")
          .Field("option", "SO_RCVTIMEO")
          .Field("fd", static_cast<uint64_t>(client_fd))
          .Field("error", strerror(errno))
          .Warn();
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
          mygram::utils::StructuredLog()
              .Event("connection_recv_timeout")
              .Field("fd", static_cast<uint64_t>(client_fd))
              .Debug();
          break;  // Timeout - close connection
        }
        mygram::utils::StructuredLog()
            .Event("connection_recv_error")
            .Field("fd", static_cast<uint64_t>(client_fd))
            .Field("error", strerror(errno))
            .Debug();
      }
      break;
    }

    buffer[bytes] = '\0';

    // Check buffer size limit
    if (accumulated.size() + bytes > max_accumulated) {
      mygram::utils::StructuredLog()
          .Event("server_warning")
          .Field("type", "request_too_large")
          .Field("fd", static_cast<uint64_t>(client_fd))
          .Field("size", static_cast<uint64_t>(accumulated.size() + bytes))
          .Field("limit", static_cast<uint64_t>(max_accumulated))
          .Warn();
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
  // Optimized: Use indices instead of substr() to avoid string copies
  size_t start = 0;
  size_t pos = 0;

  while ((pos = accumulated.find("\r\n", start)) != std::string::npos) {
    // Create string_view for zero-copy parsing (convert to string only when needed)
    size_t len = pos - start;
    if (len == 0) {
      start = pos + 2;
      continue;
    }

    // Extract request - single allocation here is unavoidable as processor needs string
    std::string request = accumulated.substr(start, len);
    start = pos + 2;

    // Process request
    std::string response = processor_(request, ctx);

    // Send response
    if (!SendResponse(client_fd, response)) {
      // Cleanup: remove processed portion before returning
      if (start > 0) {
        accumulated.erase(0, start);
      }
      return false;
    }
  }

  // Remove all processed data in single operation (instead of per-request copies)
  if (start > 0) {
    accumulated.erase(0, start);
  }

  return true;
}

// Kept as member function for consistency and potential future extensions
// NOLINTNEXTLINE(readability-convert-member-functions-to-static)
bool ConnectionIOHandler::SendResponse(int client_fd, const std::string& response) {
  // Zero-copy send using writev (scatter-gather I/O)
  // Avoids creating a copy of response just to append "\r\n"
  static constexpr std::array<char, 3> kCRLF = {'\r', '\n', '\0'};

  // const_cast required: iovec::iov_base is void*, but writev only reads data
  std::array<struct iovec, 2> iov = {
      {{const_cast<char*>(response.data()), response.size()},  // NOLINT(cppcoreguidelines-pro-type-const-cast)
       {const_cast<char*>(kCRLF.data()), 2}}};                 // NOLINT(cppcoreguidelines-pro-type-const-cast)

  size_t total_to_send = response.size() + 2;
  size_t total_sent = 0;
  size_t current_iov = 0;

  while (total_sent < total_to_send && current_iov < 2) {
    ssize_t sent = writev(client_fd, &iov.at(current_iov), static_cast<int>(2 - current_iov));

    if (sent < 0) {
      if (errno == EINTR) {
        continue;  // Interrupted, retry
      }
      // EPIPE is expected when client closes connection
      if (errno != EPIPE) {
        mygram::utils::StructuredLog()
            .Event("connection_writev_error")
            .Field("fd", static_cast<uint64_t>(client_fd))
            .Field("error", strerror(errno))
            .Debug();
      }
      return false;
    }

    if (sent == 0) {
      mygram::utils::StructuredLog()
          .Event("connection_writev_zero")
          .Field("fd", static_cast<uint64_t>(client_fd))
          .Debug();
      return false;
    }

    total_sent += sent;

    // Adjust iov for partial writes
    size_t remaining = sent;
    while (remaining > 0 && current_iov < 2) {
      if (remaining >= iov.at(current_iov).iov_len) {
        remaining -= iov.at(current_iov).iov_len;
        iov.at(current_iov).iov_len = 0;
        current_iov++;
      } else {
        // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
        iov.at(current_iov).iov_base = static_cast<char*>(iov.at(current_iov).iov_base) + remaining;
        iov.at(current_iov).iov_len -= remaining;
        remaining = 0;
      }
    }
  }

  return true;
}

}  // namespace mygramdb::server
