/**
 * @file http_server_test.cpp
 * @brief Tests for HttpServer startup race condition fix (promise/future pattern)
 */

#include "server/http_server.h"

#include <gtest/gtest.h>
#include <httplib.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <nlohmann/json.hpp>

#include "config/config.h"
#include "index/index.h"
#include "server/tcp_server.h"  // For TableContext definition
#include "storage/document_store.h"

namespace mygramdb {
namespace server {

class HttpServerStartupTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Create minimal table context
    auto index = std::make_unique<index::Index>(1);
    auto doc_store = std::make_unique<storage::DocumentStore>();
    auto doc_id = doc_store->AddDocument("doc_1", {});
    index->AddDocument(*doc_id, "test document");

    table_context_.name = "test";
    table_context_.config.ngram_size = 1;
    table_context_.index = std::move(index);
    table_context_.doc_store = std::move(doc_store);
    table_contexts_["test"] = &table_context_;

    config_ = std::make_unique<config::Config>();
  }

  HttpServerConfig MakeConfig(int port) {
    HttpServerConfig cfg;
    cfg.bind = "127.0.0.1";
    cfg.port = port;
    cfg.allow_cidrs = {"127.0.0.1/32"};
    return cfg;
  }

  TableContext table_context_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
};

/**
 * @brief Verify that starting on a valid port succeeds without sleep-based polling
 */
TEST_F(HttpServerStartupTest, StartOnValidPortSucceeds) {
  auto cfg = MakeConfig(18090);
  HttpServer server(cfg, table_contexts_, config_.get());

  auto result = server.Start();
  ASSERT_TRUE(result.has_value()) << "Start() should succeed on an available port";
  EXPECT_TRUE(server.IsRunning());

  // Verify the server actually accepts connections
  httplib::Client client("http://127.0.0.1:18090");
  auto res = client.Get("/health");
  ASSERT_TRUE(res);
  EXPECT_EQ(res->status, 200);

  server.Stop();
  EXPECT_FALSE(server.IsRunning());
}

/**
 * @brief Verify that starting on a port already in use returns an error (no crash or hang)
 *
 * Uses a raw socket with SO_REUSEADDR disabled to exclusively occupy the port,
 * preventing httplib's SO_REUSEADDR from allowing a second bind.
 */
TEST_F(HttpServerStartupTest, StartOnOccupiedPortReturnsError) {
  // Occupy the port with a raw socket (without SO_REUSEADDR)
  int sock_fd = ::socket(AF_INET, SOCK_STREAM, 0);
  ASSERT_GE(sock_fd, 0) << "Failed to create socket";

  // Explicitly disable SO_REUSEADDR so httplib cannot share the port
  int opt_off = 0;
  ::setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &opt_off, sizeof(opt_off));
#ifdef SO_REUSEPORT
  ::setsockopt(sock_fd, SOL_SOCKET, SO_REUSEPORT, &opt_off, sizeof(opt_off));
#endif

  struct sockaddr_in addr {};
  addr.sin_family = AF_INET;
  addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  addr.sin_port = htons(18091);

  int bind_result = ::bind(sock_fd, reinterpret_cast<struct sockaddr*>(&addr), sizeof(addr));
  ASSERT_EQ(bind_result, 0) << "Failed to bind raw socket to port 18091";
  ASSERT_EQ(::listen(sock_fd, 1), 0) << "Failed to listen on raw socket";

  // Now attempt to start HttpServer on the same port
  auto cfg = MakeConfig(18091);
  HttpServer server(cfg, table_contexts_, config_.get());
  auto result = server.Start();

  // Should fail with an error, not crash or hang
  ASSERT_FALSE(result.has_value()) << "Server should fail to bind to occupied port";
  EXPECT_FALSE(server.IsRunning());

  // Verify error message mentions the bind failure
  const auto& error = result.error();
  EXPECT_TRUE(error.message().find("Failed to bind") != std::string::npos)
      << "Error should mention bind failure, got: " << error.message();

  ::close(sock_fd);
}

/**
 * @brief Verify that double-start returns an appropriate error
 */
TEST_F(HttpServerStartupTest, DoubleStartReturnsError) {
  auto cfg = MakeConfig(18092);
  HttpServer server(cfg, table_contexts_, config_.get());

  auto result1 = server.Start();
  ASSERT_TRUE(result1.has_value());

  auto result2 = server.Start();
  ASSERT_FALSE(result2.has_value()) << "Double-start should return an error";

  server.Stop();
}

}  // namespace server
}  // namespace mygramdb
