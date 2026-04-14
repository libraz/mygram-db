/**
 * @file tcp_server_test_helpers.h
 * @brief Shared test helpers for TCP server tests
 */

#pragma once

#include <gtest/gtest.h>
#include <sys/socket.h>

#include <cerrno>
#include <cstring>
#include <string>

namespace mygramdb::test {

/**
 * @brief Skip the test if the OS blocks AF_INET socket creation.
 *
 * Call this at the beginning of SetUp() in TCP server test fixtures.
 * Uses static locals so the actual socket probe runs only once per process.
 */
inline void SkipIfSocketCreationBlocked() {
  static bool checked = false;
  static bool skip_due_to_permissions = false;
  static std::string permission_error;

  if (!checked) {
    checked = true;
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd >= 0) {
      close(fd);
    } else {
      if (errno == EPERM || errno == EACCES) {
        skip_due_to_permissions = true;
        permission_error = std::strerror(errno);
      }
    }
  }

  if (skip_due_to_permissions) {
    GTEST_SKIP() << "Skipping TcpServerTest: unable to create AF_INET socket (" << permission_error
                 << "). WSL/OS is blocking TCP sockets; enable networking to run this test.";
  }
}

}  // namespace mygramdb::test
