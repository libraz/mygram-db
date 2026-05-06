/**
 * @file socket_utils_test.cpp
 * @brief Unit tests for server::socket_utils::TrySetSockOpt helpers.
 *
 * The helpers themselves are thin wrappers around setsockopt(), but they are
 * the single chokepoint for setsockopt-failure logging in the server. These
 * tests guard:
 *   - the success path returns true and applies the value,
 *   - failure on a closed / invalid fd returns false (never aborts),
 *   - the int convenience overload forwards correctly to the pointer overload.
 *
 * We deliberately avoid asserting on the structured log payload here; the log
 * shape is exercised indirectly by the connection_acceptor integration tests
 * and changes to log fields should not require re-touching unit tests for the
 * tiny helper.
 */
#include "server/socket_utils.h"

#include <gtest/gtest.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

// RAII for a freshly created TCP socket that closes itself on scope exit so
// per-test fd leaks cannot accumulate even if an assertion aborts the body.
class ScopedSocket {
 public:
  ScopedSocket() : fd_(::socket(AF_INET, SOCK_STREAM, 0)) {}
  ScopedSocket(const ScopedSocket&) = delete;
  ScopedSocket& operator=(const ScopedSocket&) = delete;
  ~ScopedSocket() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }
  int fd() const { return fd_; }
  void close() {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

 private:
  int fd_;
};

}  // namespace

TEST(SocketUtilsTrySetSockOpt, IntOverloadSucceedsOnValidFd) {
  ScopedSocket sock;
  ASSERT_GE(sock.fd(), 0) << "socket() must succeed in CI";

  EXPECT_TRUE(mygramdb::server::socket_utils::TrySetSockOpt(sock.fd(), SOL_SOCKET, SO_REUSEADDR, 1, "SO_REUSEADDR"));

  // Verify the kernel actually retained the option we set, otherwise the
  // helper succeeded only because it silently dropped the call.
  int actual = 0;
  socklen_t len = sizeof(actual);
  ASSERT_EQ(0, ::getsockopt(sock.fd(), SOL_SOCKET, SO_REUSEADDR, &actual, &len));
  EXPECT_NE(0, actual);
}

TEST(SocketUtilsTrySetSockOpt, PointerOverloadSucceedsOnValidFd) {
  ScopedSocket sock;
  ASSERT_GE(sock.fd(), 0);

  int rcvbuf = 64 * 1024;
  EXPECT_TRUE(mygramdb::server::socket_utils::TrySetSockOpt(sock.fd(), SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf),
                                                            "SO_RCVBUF"));
}

TEST(SocketUtilsTrySetSockOpt, ReturnsFalseOnClosedFd) {
  ScopedSocket sock;
  ASSERT_GE(sock.fd(), 0);
  const int fd = sock.fd();
  sock.close();  // setsockopt() on a closed fd must fail with EBADF

  EXPECT_FALSE(mygramdb::server::socket_utils::TrySetSockOpt(fd, SOL_SOCKET, SO_REUSEADDR, 1, "SO_REUSEADDR"));
}

TEST(SocketUtilsTrySetSockOpt, ReturnsFalseOnInvalidFd) {
  // -1 is the universally invalid fd; setsockopt() must reject it without
  // crashing the helper or silently logging success.
  EXPECT_FALSE(mygramdb::server::socket_utils::TrySetSockOpt(-1, SOL_SOCKET, SO_REUSEADDR, 1, "SO_REUSEADDR"));
}

TEST(SocketUtilsTrySetSockOpt, ReturnsFalseOnUnsupportedOption) {
  // TCP_NODELAY is invalid on a Unix domain socket and the kernel must
  // refuse it. The helper should propagate the failure rather than swallow
  // it, otherwise per-client tunings could regress without operators noticing.
  int fd = ::socket(AF_UNIX, SOCK_STREAM, 0);
  ASSERT_GE(fd, 0);

  const bool ok = mygramdb::server::socket_utils::TrySetSockOpt(fd, IPPROTO_TCP, TCP_NODELAY, 1, "TCP_NODELAY_on_UDS");
  ::close(fd);

  EXPECT_FALSE(ok);
}
