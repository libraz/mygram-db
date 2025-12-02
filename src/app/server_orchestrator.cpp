/**
 * @file server_orchestrator.cpp
 * @brief Server component lifecycle orchestration implementation
 */

#include "app/server_orchestrator.h"

#include <spdlog/spdlog.h>

#include "app/mysql_reconnection_handler.h"
#include "app/signal_manager.h"
#include "config/runtime_variable_manager.h"
#include "loader/initial_loader.h"
#include "server/http_server.h"
#include "server/tcp_server.h"
#include "utils/structured_log.h"

namespace mygramdb::app {

namespace {
constexpr uint64_t kProgressLogInterval = 10000;  // Log progress every N rows
constexpr size_t kGtidPrefixLength = 5;           // "gtid="
constexpr int kMillisecondsPerSecond = 1000;      // Milliseconds to seconds conversion
}  // namespace

mygram::utils::Expected<std::unique_ptr<ServerOrchestrator>, mygram::utils::Error> ServerOrchestrator::Create(
    Dependencies deps) {
  auto orchestrator = std::unique_ptr<ServerOrchestrator>(new ServerOrchestrator(deps));
  return orchestrator;
}

ServerOrchestrator::ServerOrchestrator(Dependencies deps) : deps_(deps) {}

ServerOrchestrator::~ServerOrchestrator() {
  if (started_) {
    Stop();
  }
}

mygram::utils::Expected<void, mygram::utils::Error> ServerOrchestrator::Initialize() {
  if (initialized_) {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError, "ServerOrchestrator already initialized"));
  }

  // Step 1: Initialize tables
  auto tables_result = InitializeTables();
  if (!tables_result) {
    return mygram::utils::MakeUnexpected(tables_result.error());
  }

#ifdef USE_MYSQL
  // Step 2: Initialize MySQL connection
  auto mysql_result = InitializeMySQL();
  if (!mysql_result) {
    return mygram::utils::MakeUnexpected(mysql_result.error());
  }

  // Step 3: Build snapshots (if enabled)
  auto snapshot_result = BuildSnapshots();
  if (!snapshot_result) {
    return mygram::utils::MakeUnexpected(snapshot_result.error());
  }

  // Step 4: Initialize BinlogReader
  auto binlog_result = InitializeBinlogReader();
  if (!binlog_result) {
    return mygram::utils::MakeUnexpected(binlog_result.error());
  }
#endif

  // Step 5: Initialize servers
  auto servers_result = InitializeServers();
  if (!servers_result) {
    return mygram::utils::MakeUnexpected(servers_result.error());
  }

  initialized_ = true;
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ServerOrchestrator::Start() {
  if (!initialized_) {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError, "Cannot start: not initialized"));
  }

  if (started_) {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError, "Already started"));
  }

#ifdef USE_MYSQL
  // Start BinlogReader (if GTID available and replication enabled)
  if (binlog_reader_ && !snapshot_gtid_.empty()) {
    auto start_result = binlog_reader_->Start();
    if (!start_result) {
      return mygram::utils::MakeUnexpected(start_result.error());
    }
    mygram::utils::StructuredLog().Event("binlog_replication_started").Field("gtid", snapshot_gtid_).Info();
  }
#endif

  // Start TCP server
  auto tcp_start = tcp_server_->Start();
  if (!tcp_start) {
    return mygram::utils::MakeUnexpected(tcp_start.error());
  }

  // Start HTTP server (if enabled)
  if (http_server_) {
    auto http_start = http_server_->Start();
    if (!http_start) {
      // Cleanup: stop TCP server
      tcp_server_->Stop();
      return mygram::utils::MakeUnexpected(http_start.error());
    }
    mygram::utils::StructuredLog()
        .Event("http_server_started")
        .Field("bind", deps_.config.api.http.bind)
        .Field("port", static_cast<uint64_t>(deps_.config.api.http.port))
        .Info();
  }

  started_ = true;
  mygram::utils::StructuredLog()
      .Event("server_ready")
      .Field("tables", static_cast<uint64_t>(table_contexts_.size()))
      .Field("tcp_port", static_cast<uint64_t>(deps_.config.api.tcp.port))
      .Info();
  return {};
}

void ServerOrchestrator::Stop() {
  mygram::utils::StructuredLog().Event("server_debug").Field("action", "stopping_components").Debug();

  // Stop HTTP server first (depends on TCP server's cache manager)
  if (http_server_ && http_server_->IsRunning()) {
    mygram::utils::StructuredLog().Event("server_debug").Field("action", "stopping_http_server").Debug();
    http_server_->Stop();
  }

  // Stop TCP server
  if (tcp_server_) {
    tcp_server_->Stop();
  }

#ifdef USE_MYSQL
  // Stop BinlogReader
  if (binlog_reader_ && binlog_reader_->IsRunning()) {
    mygram::utils::StructuredLog().Event("server_debug").Field("action", "stopping_binlog_reader").Debug();
    binlog_reader_->Stop();
  }

  // Close MySQL connection
  if (mysql_connection_) {
    mysql_connection_->Close();
  }
#endif

  // Table contexts will be automatically destroyed
  started_ = false;
  mygram::utils::StructuredLog().Event("server_debug").Field("action", "all_components_stopped").Debug();
}

bool ServerOrchestrator::IsRunning() const {
  return started_;
}

mygram::utils::Expected<void, mygram::utils::Error> ServerOrchestrator::InitializeTables() {
  mygram::utils::StructuredLog()
      .Event("server_debug")
      .Field("action", "initializing_tables")
      .Field("count", static_cast<uint64_t>(deps_.config.tables.size()))
      .Debug();

  for (const auto& table_config : deps_.config.tables) {
    mygram::utils::StructuredLog()
        .Event("server_debug")
        .Field("action", "initializing_table")
        .Field("table", table_config.name)
        .Debug();

    auto ctx = std::make_unique<server::TableContext>();
    ctx->name = table_config.name;
    ctx->config = table_config;

    // Create index and document store for this table
    ctx->index = std::make_unique<index::Index>(table_config.ngram_size, table_config.kanji_ngram_size);
    ctx->doc_store = std::make_unique<storage::DocumentStore>();

    table_contexts_[table_config.name] = std::move(ctx);
    mygram::utils::StructuredLog()
        .Event("server_debug")
        .Field("action", "table_initialized")
        .Field("table", table_config.name)
        .Debug();
  }

  mygram::utils::StructuredLog()
      .Event("server_debug")
      .Field("action", "all_tables_initialized")
      .Field("count", static_cast<uint64_t>(table_contexts_.size()))
      .Debug();
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ServerOrchestrator::InitializeMySQL() {
#ifdef USE_MYSQL
  mygram::utils::StructuredLog().Event("server_debug").Field("action", "initializing_mysql").Debug();

  mysql::Connection::Config mysql_config;
  mysql_config.host = deps_.config.mysql.host;
  mysql_config.port = deps_.config.mysql.port;
  mysql_config.user = deps_.config.mysql.user;
  mysql_config.password = deps_.config.mysql.password;
  mysql_config.database = deps_.config.mysql.database;
  mysql_config.connect_timeout = deps_.config.mysql.connect_timeout_ms / kMillisecondsPerSecond;
  mysql_config.read_timeout = deps_.config.mysql.read_timeout_ms / kMillisecondsPerSecond;
  mysql_config.write_timeout = deps_.config.mysql.write_timeout_ms / kMillisecondsPerSecond;
  mysql_config.session_timeout_sec = deps_.config.mysql.session_timeout_sec;
  mysql_config.ssl_enable = deps_.config.mysql.ssl_enable;
  mysql_config.ssl_ca = deps_.config.mysql.ssl_ca;
  mysql_config.ssl_cert = deps_.config.mysql.ssl_cert;
  mysql_config.ssl_key = deps_.config.mysql.ssl_key;
  mysql_config.ssl_verify_server_cert = deps_.config.mysql.ssl_verify_server_cert;

  mysql_connection_ = std::make_unique<mysql::Connection>(mysql_config);

  if (!mysql_connection_->Connect("snapshot builder")) {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLConnectionFailed,
                                 "Failed to connect to MySQL: " + mysql_connection_->GetLastError()));
  }

  mygram::utils::StructuredLog().Event("server_debug").Field("action", "mysql_connected").Debug();
#endif
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ServerOrchestrator::BuildSnapshots() {
#ifdef USE_MYSQL
  if (!deps_.config.replication.auto_initial_snapshot) {
    mygram::utils::StructuredLog()
        .Event("server_debug")
        .Field("action", "skip_auto_snapshot")
        .Field("reason", "auto_initial_snapshot=false")
        .Debug();
    mygram::utils::StructuredLog().Event("server_debug").Field("action", "manual_sync_required").Debug();
    return {};
  }

  for (const auto& table_config : deps_.config.tables) {
    auto& ctx = table_contexts_[table_config.name];

    mygram::utils::StructuredLog()
        .Event("snapshot_building")
        .Field("table", table_config.name)
        .Field("message", "This may take a while for large tables")
        .Info();

    loader::InitialLoader initial_loader(*mysql_connection_, *ctx->index, *ctx->doc_store, table_config,
                                         deps_.config.mysql, deps_.config.build);

    auto load_result = initial_loader.Load([this, &table_config, &initial_loader](const auto& progress) {
      // Check cancellation flag in progress callback
      if (SignalManager::IsShutdownRequested()) {
        mygram::utils::StructuredLog().Event("initial_load_cancellation_requested").Info();
        initial_loader.Cancel();
      }

      if (progress.processed_rows % kProgressLogInterval == 0) {
        mygram::utils::StructuredLog()
            .Event("server_debug")
            .Field("action", "initial_load_progress")
            .Field("table", table_config.name)
            .Field("rows", progress.processed_rows)
            .Field("rows_per_sec", progress.rows_per_second)
            .Debug();
      }
    });

    // Check if shutdown was requested
    if (SignalManager::IsShutdownRequested()) {
      mygram::utils::StructuredLog()
          .Event("initial_load_cancelled")
          .Field("table", table_config.name)
          .Field("reason", "shutdown_signal")
          .Warn();
      return mygram::utils::MakeUnexpected(
          mygram::utils::MakeError(mygram::utils::ErrorCode::kCancelled, "Initial load cancelled"));
    }

    if (!load_result) {
      return mygram::utils::MakeUnexpected(load_result.error());
    }

    mygram::utils::StructuredLog()
        .Event("initial_load_completed")
        .Field("table", table_config.name)
        .Field("documents", initial_loader.GetProcessedRows())
        .Info();

    // Capture GTID from first table's initial load
    if (snapshot_gtid_.empty() && deps_.config.replication.enable) {
      snapshot_gtid_ = initial_loader.GetStartGTID();
      if (!snapshot_gtid_.empty()) {
        mygram::utils::StructuredLog().Event("snapshot_gtid_captured").Field("gtid", snapshot_gtid_).Info();
      }
    }
  }
#endif
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ServerOrchestrator::InitializeBinlogReader() {
#ifdef USE_MYSQL
  if (!deps_.config.replication.enable) {
    mygram::utils::StructuredLog().Event("binlog_replication_disabled").Info();
    return {};
  }

  if (table_contexts_.empty()) {
    mygram::utils::StructuredLog().Event("binlog_reader_skipped").Field("reason", "no_tables_configured").Warn();
    return {};
  }

  std::string start_gtid;
  const std::string& start_from = deps_.config.replication.start_from;

  if (start_from == "snapshot") {
    // Use GTID captured during snapshot build
    start_gtid = snapshot_gtid_;
    if (start_gtid.empty()) {
      mygram::utils::StructuredLog()
          .Event("snapshot_gtid_unavailable")
          .Field("warning", "replication may miss changes")
          .Warn();
    } else {
      mygram::utils::StructuredLog()
          .Event("server_debug")
          .Field("action", "replication_from_snapshot_gtid")
          .Field("gtid", start_gtid)
          .Debug();
    }
  } else if (start_from == "latest") {
    // Get current GTID from MySQL
    auto latest_gtid = mysql_connection_->GetLatestGTID();
    if (latest_gtid) {
      start_gtid = latest_gtid.value();
      mygram::utils::StructuredLog()
          .Event("server_debug")
          .Field("action", "replication_from_latest_gtid")
          .Field("gtid", start_gtid)
          .Debug();
    } else {
      mygram::utils::StructuredLog().Event("latest_gtid_failed").Field("fallback", "starting from empty").Warn();
      start_gtid = "";
    }
  } else if (start_from.find("gtid=") == 0) {
    // Extract GTID from "gtid=<UUID:txn>" format
    start_gtid = start_from.substr(kGtidPrefixLength);
    mygram::utils::StructuredLog()
        .Event("server_debug")
        .Field("action", "replication_from_specified_gtid")
        .Field("gtid", start_gtid)
        .Debug();
  }

  // Prepare table_contexts map for BinlogReader
  std::unordered_map<std::string, server::TableContext*> table_contexts_ptrs;
  for (auto& [name, ctx] : table_contexts_) {
    table_contexts_ptrs[name] = ctx.get();
  }

  // Create binlog reader
  mysql::BinlogReader::Config binlog_config;
  binlog_config.start_gtid = start_gtid;
  binlog_config.queue_size = deps_.config.replication.queue_size;
  binlog_config.server_id = deps_.config.replication.server_id;

  binlog_reader_ =
      std::make_unique<mysql::BinlogReader>(*mysql_connection_, table_contexts_ptrs, deps_.config.mysql, binlog_config);

  // Save GTID for Start() method
  snapshot_gtid_ = start_gtid;

  if (start_gtid.empty()) {
    mygram::utils::StructuredLog().Event("server_debug").Field("action", "binlog_initialized_waiting_gtid").Debug();
  } else {
    mygram::utils::StructuredLog().Event("server_debug").Field("action", "binlog_initialized").Debug();
  }
#endif
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ServerOrchestrator::InitializeServers() {
  // Validate network ACL configuration
  if (deps_.config.network.allow_cidrs.empty()) {
    mygram::utils::StructuredLog()
        .Event("network_acl_empty")
        .Field("action", "all connections will be DENIED by default")
        .Field("hint", "Configure network.allow_cidrs to allow specific IP ranges")
        .Warn();
  }

  // Prepare table_contexts map for servers
  std::unordered_map<std::string, server::TableContext*> table_contexts_ptrs;
  for (auto& [name, ctx] : table_contexts_) {
    table_contexts_ptrs[name] = ctx.get();
  }

  // Initialize TCP server
  server::ServerConfig server_config;
  server_config.host = deps_.config.api.tcp.bind;
  server_config.port = deps_.config.api.tcp.port;
  server_config.max_connections = deps_.config.api.tcp.max_connections;
  server_config.default_limit = deps_.config.api.default_limit;
  server_config.max_query_length = deps_.config.api.max_query_length;
  server_config.allow_cidrs = deps_.config.network.allow_cidrs;

#ifdef USE_MYSQL
  tcp_server_ = std::make_unique<server::TcpServer>(server_config, table_contexts_ptrs, deps_.dump_dir, &deps_.config,
                                                    binlog_reader_.get());

  // Set server statistics for binlog reader
  if (binlog_reader_) {
    binlog_reader_->SetServerStats(tcp_server_->GetMutableStats());
  }
#else
  tcp_server_ =
      std::make_unique<server::TcpServer>(server_config, table_contexts_ptrs, deps_.dump_dir, &deps_.config, nullptr);
#endif

  mygram::utils::StructuredLog().Event("server_debug").Field("action", "tcp_server_initialized").Debug();

#ifdef USE_MYSQL
  // Setup MySQL reconnection callback for RuntimeVariableManager
  if (mysql_connection_ && binlog_reader_) {
    auto* variable_manager = tcp_server_->GetVariableManager();
    if (variable_manager != nullptr) {
      // Create reconnection handler (use shared_ptr so it can be captured in std::function)
      // Pass the mysql_reconnecting flag to block manual REPLICATION START during reconnection
      auto reconnection_handler = std::make_shared<MysqlReconnectionHandler>(
          mysql_connection_.get(), binlog_reader_.get(), tcp_server_->GetMysqlReconnectingFlag());

      // Set callback that captures the reconnection handler
      variable_manager->SetMysqlReconnectCallback([handler = reconnection_handler](const std::string& host, int port) {
        return handler->Reconnect(host, port);
      });

      mygram::utils::StructuredLog().Event("mysql_reconnection_callback_registered").Info();
    }
  }
#endif

  // Setup rate limiter callback for RuntimeVariableManager
  {
    auto* variable_manager = tcp_server_->GetVariableManager();
    auto* rate_limiter = tcp_server_->GetRateLimiter();
    if (variable_manager != nullptr && rate_limiter != nullptr) {
      variable_manager->SetRateLimiterCallback([rate_limiter](bool enabled, size_t capacity, size_t refill_rate) {
        // Note: enabled parameter is currently not used by RateLimiter::UpdateParameters
        // Future enhancement: Add enable/disable functionality to RateLimiter
        (void)enabled;  // Suppress unused parameter warning
        rate_limiter->UpdateParameters(capacity, refill_rate);
      });
      mygram::utils::StructuredLog().Event("rate_limiter_callback_registered").Info();
    }
  }

  // Initialize HTTP server (if enabled)
  if (deps_.config.api.http.enable) {
    server::HttpServerConfig http_config;
    http_config.bind = deps_.config.api.http.bind;
    http_config.port = deps_.config.api.http.port;
    http_config.enable_cors = deps_.config.api.http.enable_cors;
    http_config.cors_allow_origin = deps_.config.api.http.cors_allow_origin;
    http_config.allow_cidrs = deps_.config.network.allow_cidrs;

#ifdef USE_MYSQL
    http_server_ = std::make_unique<server::HttpServer>(
        http_config, table_contexts_ptrs, &deps_.config, binlog_reader_.get(), tcp_server_->GetCacheManager(),
        tcp_server_->GetDumpLoadInProgressFlag(), tcp_server_->GetMutableStats());
#else
    http_server_ = std::make_unique<server::HttpServer>(
        http_config, table_contexts_ptrs, &deps_.config, nullptr, tcp_server_->GetCacheManager(),
        tcp_server_->GetDumpLoadInProgressFlag(), tcp_server_->GetMutableStats());
#endif

    mygram::utils::StructuredLog()
        .Event("http_server_initialized")
        .Field("bind", http_config.bind)
        .Field("port", static_cast<uint64_t>(http_config.port))
        .Info();
  }

  return {};
}

}  // namespace mygramdb::app
