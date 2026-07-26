/**
 * @file network_test_utils.h
 * @brief Socket helpers for tests that need an isolated loopback listener.
 */

#pragma once

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdint>

namespace mygramdb::testing {

inline uint16_t FindAvailableLoopbackPort() {
  const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    return 0;
  }

  sockaddr_in address{};
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  address.sin_port = 0;
  if (::bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
    ::close(fd);
    return 0;
  }

  socklen_t length = sizeof(address);
  if (::getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
    ::close(fd);
    return 0;
  }
  ::close(fd);
  return ntohs(address.sin_port);
}

}  // namespace mygramdb::testing
