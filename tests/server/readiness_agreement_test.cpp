/**
 * @file readiness_agreement_test.cpp
 * @brief The readiness views must classify the same state the same way.
 *
 * `GET /health/ready`, `GET /health/detail` and the TCP `INFO` command all
 * answer "is this server available?" from the same handles: the binlog reader,
 * the sync manager, the dump pause flag and the initial-data checker. These
 * tests drive all three real code paths over one shared set of handles and
 * compare the verdicts they publish, so a surface that grows its own predicate
 * fails here rather than in production.
 *
 * The probe-accounting cases cover the other half of the same contract: an
 * orchestrator probe must reach a verdict at all, and must be accounted the
 * same way whether it is served or rejected.
 */

#include <gtest/gtest.h>
#include <httplib.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "cache/cache_manager.h"
#include "config/config.h"
#include "index/index.h"
#include "mysql/binlog_reader_interface.h"
#include "query/query_parser.h"
#include "server/handlers/admin_handler.h"
#include "server/http_server.h"
#include "server/server_stats.h"
#include "server/server_types.h"
#include "server/table_catalog.h"
#include "storage/document_store.h"
#include "support/network_test_utils.h"
#ifdef USE_MYSQL
#include "server/sync_operation_manager.h"
#endif

using json = nlohmann::json;

namespace mygramdb::server {
namespace {

constexpr const char* kTableName = "app.articles";

/// Reader whose lifecycle state the test drives directly.
class ScriptedBinlogReader final : public mysql::IBinlogReader {
 public:
  mygram::utils::Expected<void, mygram::utils::Error> Start() override {
    running_.store(true, std::memory_order_release);
    return {};
  }
  void Stop() override { running_.store(false, std::memory_order_release); }
  bool IsRunning() const override { return running_.load(std::memory_order_acquire); }
  bool IsStarting() const override { return starting_.load(std::memory_order_acquire); }
  std::string GetCurrentGTID() const override { return "8e3f4b0a:1-9"; }
  void SetCurrentGTID(const std::string&) override {}
  std::string GetLastError() const override { return {}; }
  uint64_t GetProcessedEvents() const override { return 11; }
  size_t GetQueueSize() const override { return 0; }

  void SetRunning(bool running) { running_.store(running, std::memory_order_release); }
  void SetStarting(bool starting) { starting_.store(starting, std::memory_order_release); }

 private:
  std::atomic<bool> running_{true};
  std::atomic<bool> starting_{false};
};

/// One observation of the three surfaces, taken back to back.
struct ObservedVerdicts {
  bool http_ready = false;
  bool http_detail_healthy = false;
  bool tcp_info_ready = false;
  std::string binlog_component_status;
};

}  // namespace

class ReadinessAgreementTest : public ::testing::Test {
 protected:
  void SetUp() override {
    spdlog::set_level(spdlog::level::off);

    table_ctx_.name = kTableName;
    table_ctx_.index = std::make_unique<index::Index>(2);
    table_ctx_.doc_store = std::make_unique<storage::DocumentStore>();
    table_contexts_[kTableName] = &table_ctx_;
    table_catalog_ = std::make_unique<TableCatalog>(table_contexts_);

    config_ = std::make_unique<config::Config>();
    stats_ = std::make_unique<ServerStats>();

    config::CacheConfig cache_config;
    cache_config.enabled = false;
    cache_manager_ = std::make_unique<cache::CacheManager>(cache_config, cache::NgramConfigMap{});

#ifdef USE_MYSQL
    sync_manager_ =
        std::make_unique<SyncOperationManager>(table_contexts_, config_.get(), &binlog_reader_, nullptr, nullptr);
#endif

    port_ = mygramdb::testing::FindAvailableLoopbackPort();
    ASSERT_GT(port_, 0);

    HttpServerConfig http_config;
    http_config.bind = "127.0.0.1";
    http_config.port = port_;
    http_config.read_timeout_sec = 5;
    http_config.write_timeout_sec = 5;
    http_config.allow_cidrs = {"127.0.0.1/32"};

    server_ = std::make_unique<HttpServer>(
        http_config, table_contexts_, config_.get(), &binlog_reader_, cache_manager_.get(), &loading_, stats_.get(),
        nullptr, &replication_paused_for_dump_, SyncManager(), std::function<bool(const std::string&)>{},
        std::function<bool()>{}, [this]() { return data_initialized_.load(std::memory_order_acquire); });
    auto started = server_->Start();
    if (!started) {
      GTEST_SKIP() << "HTTP server bind unavailable: " << started.error().to_string();
    }

    handler_ctx_ = std::make_unique<HandlerContext>(HandlerContext{
        .table_catalog = table_catalog_.get(),
        .stats = *stats_,
        .full_config = config_.get(),
        .dump_dir = "/tmp",
        .dump_load_in_progress = loading_,
        .dump_save_in_progress = dump_save_in_progress_,
        .optimization_in_progress = optimization_in_progress_,
        .replication_paused_for_dump = replication_paused_for_dump_,
        .mysql_reconnecting = mysql_reconnecting_,
        .binlog_reader = &binlog_reader_,
#ifdef USE_MYSQL
        .sync_manager = SyncManager(),
#endif
        .cache_manager = cache_manager_.get(),
        .initial_data_ready_checker = [this]() { return data_initialized_.load(std::memory_order_acquire); },
    });
    admin_handler_ = std::make_unique<AdminHandler>(*handler_ctx_);

    WaitForServer();
  }

  void TearDown() override {
    SetSyncing(false);
    if (server_) {
      server_->Stop();
    }
  }

  SyncOperationManager* SyncManager() {
#ifdef USE_MYSQL
    return sync_manager_.get();
#else
    return nullptr;
#endif
  }

  /// Put the shared sync manager into (or out of) the state both surfaces read.
  void SetSyncing(bool syncing) {
#ifdef USE_MYSQL
    if (syncing) {
      sync_manager_->MarkSyncingTableForTest(kTableName);
    } else {
      sync_manager_->ClearSyncingTableForTest(kTableName);
    }
#else
    (void)syncing;
#endif
  }

  bool SyncSupported() const {
#ifdef USE_MYSQL
    return true;
#else
    return false;
#endif
  }

  httplib::Client Client() const {
    httplib::Client client("127.0.0.1", port_);
    client.set_connection_timeout(5);
    client.set_read_timeout(5, 0);
    return client;
  }

  void WaitForServer() {
    auto client = Client();
    for (int attempt = 0; attempt < 50; ++attempt) {
      if (client.Get("/health/live")) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    FAIL() << "HTTP server did not answer within timeout";
  }

  ObservedVerdicts Observe() {
    ObservedVerdicts observed;
    auto client = Client();

    auto ready = client.Get("/health/ready");
    EXPECT_TRUE(ready) << "/health/ready did not answer";
    if (ready) {
      observed.http_ready = json::parse(ready->body).value("status", "") == "ready";
    }

    auto detail = client.Get("/health/detail");
    EXPECT_TRUE(detail) << "/health/detail did not answer";
    if (detail) {
      const auto body = json::parse(detail->body);
      observed.http_detail_healthy = body.value("status", "") == "healthy";
      const auto& components = body["components"];
      if (components.contains("binlog")) {
        observed.binlog_component_status = components["binlog"].value("status", "");
      }
    }

    query::Query info_query;
    info_query.type = query::QueryType::INFO;
    const std::string info = admin_handler_->Handle(info_query, conn_ctx_);
    observed.tcp_info_ready = info.find("readiness: ready\r\n") != std::string::npos;

    return observed;
  }

  /// Assert that all three surfaces reached the same verdict.
  void ExpectAgreement(const std::string& state) {
    const ObservedVerdicts observed = Observe();
    EXPECT_EQ(observed.http_ready, observed.tcp_info_ready)
        << state << ": /health/ready and TCP INFO disagree (ready=" << observed.http_ready
        << ", info=" << observed.tcp_info_ready << ")";
    EXPECT_EQ(observed.http_ready, observed.http_detail_healthy)
        << state << ": /health/ready and /health/detail disagree (ready=" << observed.http_ready
        << ", detail_healthy=" << observed.http_detail_healthy << ")";
  }

  TableContext table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<TableCatalog> table_catalog_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<ServerStats> stats_;
  std::unique_ptr<cache::CacheManager> cache_manager_;
  ScriptedBinlogReader binlog_reader_;
#ifdef USE_MYSQL
  std::unique_ptr<SyncOperationManager> sync_manager_;
#endif
  std::unique_ptr<HttpServer> server_;
  std::unique_ptr<HandlerContext> handler_ctx_;
  std::unique_ptr<AdminHandler> admin_handler_;
  ConnectionContext conn_ctx_;

  std::atomic<bool> loading_{false};
  std::atomic<bool> dump_save_in_progress_{false};
  std::atomic<bool> optimization_in_progress_{false};
  std::atomic<bool> replication_paused_for_dump_{false};
  std::atomic<bool> mysql_reconnecting_{false};
  std::atomic<bool> data_initialized_{true};
  int port_ = 0;
};

TEST_F(ReadinessAgreementTest, EveryViewAgreesAcrossTheStatesTheyAllRead) {
  ExpectAgreement("replication running");

  loading_.store(true, std::memory_order_release);
  ExpectAgreement("dump load in progress");
  loading_.store(false, std::memory_order_release);

  data_initialized_.store(false, std::memory_order_release);
  ExpectAgreement("initial data not loaded");
  data_initialized_.store(true, std::memory_order_release);

  binlog_reader_.SetRunning(false);
  ExpectAgreement("replication stopped");

  binlog_reader_.SetStarting(true);
  ExpectAgreement("replication starting");
  binlog_reader_.SetStarting(false);

  replication_paused_for_dump_.store(true, std::memory_order_release);
  ExpectAgreement("replication paused for dump");
  replication_paused_for_dump_.store(false, std::memory_order_release);

  binlog_reader_.SetRunning(true);

  if (SyncSupported()) {
    SetSyncing(true);
    ExpectAgreement("sync in progress");

    // A SYNC stops the reader through replication_pause::Scope, which never
    // raises the dump pause flag. The views must still agree.
    binlog_reader_.SetRunning(false);
    ExpectAgreement("sync in progress with the reader stopped");
    binlog_reader_.SetRunning(true);
    SetSyncing(false);
  }
}

#ifdef USE_MYSQL

TEST_F(ReadinessAgreementTest, ASyncPauseIsDistinguishableFromADisconnection) {
  binlog_reader_.SetRunning(false);
  const ObservedVerdicts disconnected = Observe();
  EXPECT_EQ(disconnected.binlog_component_status, "disconnected");

  SetSyncing(true);
  const ObservedVerdicts syncing = Observe();
  EXPECT_NE(syncing.binlog_component_status, "disconnected")
      << "a SYNC-induced pause is reported as a replication outage";
  EXPECT_EQ(syncing.binlog_component_status, "paused_for_sync");
  SetSyncing(false);
}

TEST_F(ReadinessAgreementTest, DetailNamesTheReasonItIsDegraded) {
  data_initialized_.store(false, std::memory_order_release);

  auto client = Client();
  auto detail = client.Get("/health/detail");
  ASSERT_TRUE(detail);
  const auto detail_body = json::parse(detail->body);
  ASSERT_EQ(detail_body.value("status", ""), "degraded") << detail->body;
  ASSERT_TRUE(detail_body.contains("reason")) << detail->body;

  auto ready = client.Get("/health/ready");
  ASSERT_TRUE(ready);
  EXPECT_EQ(detail_body["reason"], json::parse(ready->body)["reason"]);

  data_initialized_.store(true, std::memory_order_release);
}

#endif  // USE_MYSQL

/**
 * @brief Probe accounting under an exhausted rate-limit bucket.
 *
 * A one-token bucket refilling at one token per second is exhausted by a single
 * request, so every assertion below holds without a sleep or a wall-clock
 * dependency.
 */
class ProbeAccountingTest : public ::testing::Test {
 protected:
  void SetUp() override {
    spdlog::set_level(spdlog::level::off);

    table_ctx_.name = kTableName;
    table_ctx_.index = std::make_unique<index::Index>(2);
    table_ctx_.doc_store = std::make_unique<storage::DocumentStore>();
    table_contexts_[kTableName] = &table_ctx_;

    config_ = std::make_unique<config::Config>();
    config_->api.rate_limiting.enable = true;
    config_->api.rate_limiting.capacity = 1;
    config_->api.rate_limiting.refill_rate = 1;
    config_->api.rate_limiting.max_clients = 8;

    port_ = mygramdb::testing::FindAvailableLoopbackPort();
    ASSERT_GT(port_, 0);

    HttpServerConfig http_config;
    http_config.bind = "127.0.0.1";
    http_config.port = port_;
    http_config.read_timeout_sec = 5;
    http_config.write_timeout_sec = 5;
    http_config.allow_cidrs = {"127.0.0.1/32"};

    server_ = std::make_unique<HttpServer>(http_config, table_contexts_, config_.get());
    auto started = server_->Start();
    if (!started) {
      GTEST_SKIP() << "HTTP server bind unavailable: " << started.error().to_string();
    }

    auto client = Client();
    for (int attempt = 0; attempt < 50; ++attempt) {
      if (client.Get("/health/live")) {
        return;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    FAIL() << "HTTP server did not answer within timeout";
  }

  void TearDown() override {
    if (server_) {
      server_->Stop();
    }
  }

  httplib::Client Client() const {
    httplib::Client client("127.0.0.1", port_);
    client.set_connection_timeout(5);
    client.set_read_timeout(5, 0);
    return client;
  }

  TableContext table_ctx_;
  std::unordered_map<std::string, TableContext*> table_contexts_;
  std::unique_ptr<config::Config> config_;
  std::unique_ptr<HttpServer> server_;
  int port_ = 0;
};

TEST_F(ProbeAccountingTest, OrchestratorProbesAreNeverRateLimited) {
  auto client = Client();

  // Drain the bucket through a route that is rate limited, so the probes below
  // are answered only if they are exempt rather than because tokens remain.
  client.Get("/metrics");
  client.Get("/metrics");

  for (const auto* path : {"/health", "/health/live", "/health/ready"}) {
    for (int attempt = 0; attempt < 5; ++attempt) {
      auto response = client.Get(path);
      ASSERT_TRUE(response) << path;
      EXPECT_NE(response->status, 429) << path << " was rate limited on attempt " << attempt;
    }
  }
}

TEST_F(ProbeAccountingTest, ProbesAreExcludedFromTotalRequestsOnEveryOutcome) {
  auto client = Client();

  // Exhaust the bucket first so the probes that follow exercise both the served
  // and the rejected path of whatever accounting rule is in force.
  client.Get("/metrics");
  client.Get("/metrics");

  const uint64_t before = server_->GetTotalRequests();
  for (int attempt = 0; attempt < 5; ++attempt) {
    client.Get("/health");
    client.Get("/health/live");
    client.Get("/health/ready");
    client.Get("/health/detail");
  }
  const uint64_t after = server_->GetTotalRequests();

  EXPECT_EQ(after, before) << "health probes moved total_requests by " << (after - before)
                           << "; the counter must not depend on whether a probe was served";
}

}  // namespace mygramdb::server
