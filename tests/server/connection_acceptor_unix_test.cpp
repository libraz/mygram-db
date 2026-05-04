/**
 * @file connection_acceptor_unix_test.cpp
 * @brief Tests for Unix domain socket support in ConnectionAcceptor
 */

#include <gtest/gtest.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <filesystem>
#include <thread>

#include "server/connection_acceptor.h"
#include "server/server_types.h"

namespace mygramdb::server {

class ConnectionAcceptorUnixTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Generate unique socket path for each test
    socket_path_ = "/tmp/mygramdb_test_" + std::to_string(getpid()) + "_" +
                   std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".sock";
    // Ensure clean state
    unlink(socket_path_.c_str());
  }

  void TearDown() override { unlink(socket_path_.c_str()); }

  int ConnectToUnixSocket(const std::string& path) {
    int sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
      return -1;
    }

    struct sockaddr_un addr {};
    addr.sun_family = AF_UNIX;
    std::strncpy(addr.sun_path, path.c_str(), sizeof(addr.sun_path) - 1);

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    if (connect(sock, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)) < 0) {
      close(sock);
      return -1;
    }

    return sock;
  }

  std::string socket_path_;
};

TEST_F(ConnectionAcceptorUnixTest, StartAndStopUnixSocket) {
  ServerConfig config;
  config.unix_socket_path = socket_path_;
  config.max_connections = 10;

  ConnectionAcceptor acceptor(config);

  std::atomic<int> connections{0};
  acceptor.SetReactorHandler([&connections](int fd) {
    connections++;
    close(fd);
    return true;
  });

  auto result = acceptor.Start();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_TRUE(acceptor.StartAccepting().has_value());
  EXPECT_TRUE(acceptor.IsRunning());
  EXPECT_TRUE(acceptor.IsUnixSocket());
  EXPECT_EQ(acceptor.GetPort(), 0);

  // Socket file should exist
  EXPECT_TRUE(std::filesystem::exists(socket_path_));

  acceptor.Stop();
  EXPECT_FALSE(acceptor.IsRunning());

  // Socket file should be removed after stop
  EXPECT_FALSE(std::filesystem::exists(socket_path_));
}

TEST_F(ConnectionAcceptorUnixTest, AcceptsUnixConnection) {
  ServerConfig config;
  config.unix_socket_path = socket_path_;
  config.max_connections = 10;

  ConnectionAcceptor acceptor(config);

  std::atomic<int> connections{0};
  acceptor.SetReactorHandler([&connections](int fd) {
    connections++;
    close(fd);
    return true;
  });

  auto result = acceptor.Start();
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(acceptor.StartAccepting().has_value());

  // Connect via UDS
  int client_fd = ConnectToUnixSocket(socket_path_);
  ASSERT_GE(client_fd, 0) << "Failed to connect: " << strerror(errno);
  close(client_fd);

  // Wait for handler to run
  std::this_thread::sleep_for(std::chrono::milliseconds(200));
  EXPECT_GE(connections.load(), 1);

  acceptor.Stop();
}

TEST_F(ConnectionAcceptorUnixTest, StaleSocketCleanup) {
  // Create a stale socket file (not listening)
  int stale_fd = socket(AF_UNIX, SOCK_STREAM, 0);
  ASSERT_GE(stale_fd, 0);

  struct sockaddr_un addr {};
  addr.sun_family = AF_UNIX;
  std::strncpy(addr.sun_path, socket_path_.c_str(), sizeof(addr.sun_path) - 1);

  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  ASSERT_EQ(bind(stale_fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr)), 0);
  close(stale_fd);  // Close without listen — creates stale file

  EXPECT_TRUE(std::filesystem::exists(socket_path_));

  // New acceptor should clean up the stale file and start successfully
  ServerConfig config;
  config.unix_socket_path = socket_path_;
  config.max_connections = 10;

  ConnectionAcceptor acceptor(config);
  acceptor.SetReactorHandler([](int fd) {
    close(fd);
    return true;
  });

  auto result = acceptor.Start();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_TRUE(acceptor.StartAccepting().has_value());

  acceptor.Stop();
}

TEST_F(ConnectionAcceptorUnixTest, PathTooLongError) {
  ServerConfig config;
  // Create a path that exceeds sockaddr_un::sun_path limit (typically 104 or 108 bytes)
  config.unix_socket_path = "/tmp/" + std::string(200, 'x') + ".sock";
  config.max_connections = 10;

  ConnectionAcceptor acceptor(config);
  acceptor.SetReactorHandler([](int fd) {
    close(fd);
    return true;
  });

  auto result = acceptor.Start();
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kNetworkUnixSocketPathTooLong);
}

TEST_F(ConnectionAcceptorUnixTest, SocketFileRemovedOnStop) {
  ServerConfig config;
  config.unix_socket_path = socket_path_;
  config.max_connections = 10;

  ConnectionAcceptor acceptor(config);
  acceptor.SetReactorHandler([](int fd) {
    close(fd);
    return true;
  });

  auto result = acceptor.Start();
  ASSERT_TRUE(result.has_value());
  ASSERT_TRUE(acceptor.StartAccepting().has_value());
  EXPECT_TRUE(std::filesystem::exists(socket_path_));

  acceptor.Stop();
  EXPECT_FALSE(std::filesystem::exists(socket_path_));
}

TEST_F(ConnectionAcceptorUnixTest, SocketPermissionsAreRestricted) {
  ServerConfig config;
  config.unix_socket_path = socket_path_;
  config.max_connections = 10;

  ConnectionAcceptor acceptor(config);
  acceptor.SetReactorHandler([](int fd) {
    close(fd);
    return true;
  });

  auto result = acceptor.Start();
  ASSERT_TRUE(result.has_value()) << result.error().to_string();
  ASSERT_TRUE(acceptor.StartAccepting().has_value());

  // Verify socket file permissions are 0770
  struct stat st {};
  ASSERT_EQ(stat(socket_path_.c_str(), &st), 0) << strerror(errno);
  mode_t perms = st.st_mode & 0777;
  EXPECT_EQ(perms, 0770) << "Socket permissions should be 0770, got: " << std::oct << perms;

  acceptor.Stop();
}

}  // namespace mygramdb::server
