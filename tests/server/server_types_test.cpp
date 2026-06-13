/**
 * @file server_types_test.cpp
 * @brief Tests for server type definitions (ServerConfig::FromConfig, etc.)
 */

#include "server/server_types.h"

#include <gtest/gtest.h>

#include "config/config.h"

namespace mygramdb::server {

/**
 * @brief Test ServerConfig::FromConfig copies all TCP settings correctly
 */
TEST(ServerConfigTest, FromConfigCopiesTcpSettings) {
  config::Config cfg;
  cfg.api.tcp.bind = "0.0.0.0";
  cfg.api.tcp.port = 12345;
  cfg.api.tcp.max_connections = 500;
  cfg.api.tcp.worker_threads = 8;
  cfg.api.tcp.recv_timeout_sec = 30;
  cfg.api.tcp.thread_pool_queue_size = 2000;
  cfg.api.tcp.max_write_queue_bytes = 32LL * 1024 * 1024;

  auto sc = ServerConfig::FromConfig(cfg);

  EXPECT_EQ(sc.host, "0.0.0.0");
  EXPECT_EQ(sc.port, 12345);
  EXPECT_EQ(sc.max_connections, 500);
  EXPECT_EQ(sc.worker_threads, 8);
  EXPECT_EQ(sc.recv_timeout_sec, 30);
  EXPECT_EQ(sc.thread_pool_queue_size, 2000);
  EXPECT_EQ(sc.max_write_queue_bytes, 32LL * 1024 * 1024);
}

/**
 * @brief Test ServerConfig::FromConfig copies keepalive settings
 */
TEST(ServerConfigTest, FromConfigCopiesKeepaliveSettings) {
  config::Config cfg;
  cfg.api.tcp.keepalive.enabled = false;
  cfg.api.tcp.keepalive.idle_sec = 120;
  cfg.api.tcp.keepalive.interval_sec = 30;
  cfg.api.tcp.keepalive.probe_count = 5;

  auto sc = ServerConfig::FromConfig(cfg);

  EXPECT_EQ(sc.keepalive.enabled, false);
  EXPECT_EQ(sc.keepalive.idle_sec, 120);
  EXPECT_EQ(sc.keepalive.interval_sec, 30);
  EXPECT_EQ(sc.keepalive.probe_count, 5);
}

/**
 * @brief Test ServerConfig::FromConfig copies API-level settings
 */
TEST(ServerConfigTest, FromConfigCopiesApiSettings) {
  config::Config cfg;
  cfg.api.default_limit = 50;
  cfg.api.max_query_length = 256;

  auto sc = ServerConfig::FromConfig(cfg);

  EXPECT_EQ(sc.default_limit, 50);
  EXPECT_EQ(sc.max_query_length, 256);
}

/**
 * @brief Test ServerConfig::FromConfig copies network ACLs
 */
TEST(ServerConfigTest, FromConfigCopiesNetworkAcls) {
  config::Config cfg;
  cfg.network.allow_cidrs = {"10.0.0.0/8", "192.168.1.0/24"};

  auto sc = ServerConfig::FromConfig(cfg);

  ASSERT_EQ(sc.allow_cidrs.size(), 2);
  EXPECT_EQ(sc.allow_cidrs[0], "10.0.0.0/8");
  EXPECT_EQ(sc.allow_cidrs[1], "192.168.1.0/24");
}

/**
 * @brief Test ServerConfig::FromConfig copies unix socket path
 */
TEST(ServerConfigTest, FromConfigCopiesUnixSocketPath) {
  config::Config cfg;
  cfg.api.unix_socket.path = "/var/run/mygramdb.sock";

  auto sc = ServerConfig::FromConfig(cfg);

  EXPECT_EQ(sc.unix_socket_path, "/var/run/mygramdb.sock");
}

/**
 * @brief Test ServerConfig::FromConfig with default config values
 */
TEST(ServerConfigTest, FromConfigWithDefaults) {
  config::Config cfg;  // All defaults

  auto sc = ServerConfig::FromConfig(cfg);

  EXPECT_EQ(sc.host, "127.0.0.1");
  EXPECT_EQ(sc.port, config::defaults::kTcpPort);
  EXPECT_EQ(sc.default_limit, config::defaults::kDefaultLimit);
  EXPECT_EQ(sc.max_query_length, config::defaults::kDefaultQueryLengthLimit);
  EXPECT_TRUE(sc.keepalive.enabled);
  EXPECT_TRUE(sc.allow_cidrs.empty());
  EXPECT_TRUE(sc.unix_socket_path.empty());
}

/**
 * @brief Test ServerConfig::FromConfig copies empty unix socket path
 */
TEST(ServerConfigTest, FromConfigEmptyUnixSocketMeansTcpMode) {
  config::Config cfg;
  cfg.api.unix_socket.path = "";

  auto sc = ServerConfig::FromConfig(cfg);

  EXPECT_TRUE(sc.unix_socket_path.empty());
}

TEST(BM25StatsTest, RemoveDocumentSaturatesDocCountAndTotalLength) {
  BM25Stats stats;

  stats.AddDocument(10);
  stats.RemoveDocument(30);
  stats.RemoveDocument(1);

  EXPECT_EQ(stats.total_doc_length.load(std::memory_order_relaxed), 0u);
  EXPECT_EQ(stats.doc_count.load(std::memory_order_relaxed), 0u);
  EXPECT_EQ(stats.avg_doc_length(), 0.0);
}

TEST(BM25StatsTest, RemoveDocumentSaturatesPartialUnderflowOnly) {
  BM25Stats stats;

  stats.AddDocument(10);
  stats.AddDocument(20);
  stats.RemoveDocument(15);
  stats.RemoveDocument(30);

  EXPECT_EQ(stats.total_doc_length.load(std::memory_order_relaxed), 0u);
  EXPECT_EQ(stats.doc_count.load(std::memory_order_relaxed), 0u);
  EXPECT_EQ(stats.avg_doc_length(), 0.0);
}

}  // namespace mygramdb::server
