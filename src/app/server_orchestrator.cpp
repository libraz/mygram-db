/**
 * @file server_orchestrator.cpp
 * @brief Server component lifecycle orchestration implementation
 */

#include "app/server_orchestrator.h"

#include <spdlog/spdlog.h>

#include "app/signal_manager.h"
#include "server/http_server.h"
#include "server/tcp_server.h"
#include "storage/snapshot_builder.h"

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
    spdlog::info("Binlog replication started from GTID: {}", snapshot_gtid_);
  }
#endif

  // Start TCP server
  auto tcp_start = tcp_server_->Start();
  if (!tcp_start) {
    return mygram::utils::MakeUnexpected(tcp_start.error());
  }
  spdlog::info("TCP server started on {}:{}", deps_.config.api.tcp.bind, deps_.config.api.tcp.port);

  // Start HTTP server (if enabled)
  if (http_server_) {
    auto http_start = http_server_->Start();
    if (!http_start) {
      // Cleanup: stop TCP server
      tcp_server_->Stop();
      return mygram::utils::MakeUnexpected(http_start.error());
    }
    spdlog::info("HTTP server started on {}:{}", deps_.config.api.http.bind, deps_.config.api.http.port);
  }

  started_ = true;
  spdlog::info("MygramDB is ready to serve requests");
  return {};
}

void ServerOrchestrator::Stop() {
  spdlog::info("Stopping server components...");

  // Stop HTTP server first (depends on TCP server's cache manager)
  if (http_server_ && http_server_->IsRunning()) {
    spdlog::info("Stopping HTTP server");
    http_server_->Stop();
  }

  // Stop TCP server
  if (tcp_server_) {
    spdlog::info("Stopping TCP server");
    tcp_server_->Stop();
  }

#ifdef USE_MYSQL
  // Stop BinlogReader
  if (binlog_reader_ && binlog_reader_->IsRunning()) {
    spdlog::info("Stopping binlog reader");
    binlog_reader_->Stop();
  }

  // Close MySQL connection
  if (mysql_connection_) {
    mysql_connection_->Close();
  }
#endif

  // Table contexts will be automatically destroyed
  started_ = false;
  spdlog::info("All server components stopped");
}

bool ServerOrchestrator::IsRunning() const {
  return started_;
}

mygram::utils::Expected<void, mygram::utils::Error> ServerOrchestrator::ReloadConfig(const config::Config& new_config) {
#ifdef USE_MYSQL
  // Check if MySQL connection settings changed
  bool mysql_changed =
      (new_config.mysql.host != deps_.config.mysql.host) || (new_config.mysql.port != deps_.config.mysql.port) ||
      (new_config.mysql.user != deps_.config.mysql.user) || (new_config.mysql.password != deps_.config.mysql.password);

  if (mysql_changed) {
    auto handle_result = HandleMySQLConfigChange(new_config);
    if (!handle_result) {
      return mygram::utils::MakeUnexpected(handle_result.error());
    }
  }
#endif

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ServerOrchestrator::InitializeTables() {
  spdlog::info("Initializing {} table(s)...", deps_.config.tables.size());

  for (const auto& table_config : deps_.config.tables) {
    spdlog::info("Initializing table: {}", table_config.name);

    auto ctx = std::make_unique<server::TableContext>();
    ctx->name = table_config.name;
    ctx->config = table_config;

    // Create index and document store for this table
    ctx->index = std::make_unique<index::Index>(table_config.ngram_size, table_config.kanji_ngram_size);
    ctx->doc_store = std::make_unique<storage::DocumentStore>();

    table_contexts_[table_config.name] = std::move(ctx);
    spdlog::info("Table initialized successfully: {}", table_config.name);
  }

  spdlog::info("All {} table(s) initialized", table_contexts_.size());
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ServerOrchestrator::InitializeMySQL() {
#ifdef USE_MYSQL
  spdlog::info("Initializing MySQL connection...");

  mysql::Connection::Config mysql_config;
  mysql_config.host = deps_.config.mysql.host;
  mysql_config.port = deps_.config.mysql.port;
  mysql_config.user = deps_.config.mysql.user;
  mysql_config.password = deps_.config.mysql.password;
  mysql_config.database = deps_.config.mysql.database;
  mysql_config.connect_timeout = deps_.config.mysql.connect_timeout_ms / kMillisecondsPerSecond;
  mysql_config.read_timeout = deps_.config.mysql.read_timeout_ms / kMillisecondsPerSecond;
  mysql_config.write_timeout = deps_.config.mysql.write_timeout_ms / kMillisecondsPerSecond;
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

  spdlog::info("MySQL connection established");
#endif
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ServerOrchestrator::BuildSnapshots() {
#ifdef USE_MYSQL
  if (!deps_.config.replication.auto_initial_snapshot) {
    spdlog::info("Skipping automatic snapshot build (auto_initial_snapshot=false)");
    spdlog::info("Use SYNC command to manually trigger snapshot synchronization");
    return {};
  }

  for (const auto& table_config : deps_.config.tables) {
    auto& ctx = table_contexts_[table_config.name];

    spdlog::info("Building snapshot from table: {}", table_config.name);
    spdlog::info("This may take a while for large tables. Please wait...");

    storage::SnapshotBuilder snapshot_builder(*mysql_connection_, *ctx->index, *ctx->doc_store, table_config,
                                              deps_.config.build);

    auto snapshot_result = snapshot_builder.Build([this, &table_config, &snapshot_builder](const auto& progress) {
      // Check cancellation flag in progress callback
      if (SignalManager::IsShutdownRequested()) {
        spdlog::info("Snapshot cancellation requested during build");
        snapshot_builder.Cancel();
      }

      if (progress.processed_rows % kProgressLogInterval == 0) {
        spdlog::debug("table: {} - Progress: {} rows processed ({:.0f} rows/s)", table_config.name,
                      progress.processed_rows, progress.rows_per_second);
      }
    });

    // Check if shutdown was requested
    if (SignalManager::IsShutdownRequested()) {
      spdlog::warn("Snapshot build cancelled by shutdown signal for table: {}", table_config.name);
      return mygram::utils::MakeUnexpected(
          mygram::utils::MakeError(mygram::utils::ErrorCode::kCancelled, "Snapshot build cancelled"));
    }

    if (!snapshot_result) {
      return mygram::utils::MakeUnexpected(snapshot_result.error());
    }

    spdlog::info("Snapshot build completed - table: {}, documents: {}", table_config.name,
                 snapshot_builder.GetProcessedRows());

    // Capture GTID from first table's snapshot
    if (snapshot_gtid_.empty() && deps_.config.replication.enable) {
      snapshot_gtid_ = snapshot_builder.GetSnapshotGTID();
      if (!snapshot_gtid_.empty()) {
        spdlog::info("Captured snapshot GTID for replication: {}", snapshot_gtid_);
      }
    }
  }
#endif
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ServerOrchestrator::InitializeBinlogReader() {
#ifdef USE_MYSQL
  if (!deps_.config.replication.enable) {
    spdlog::info("Binlog replication disabled");
    return {};
  }

  if (table_contexts_.empty()) {
    spdlog::warn("No tables configured, skipping binlog reader initialization");
    return {};
  }

  std::string start_gtid;
  const std::string& start_from = deps_.config.replication.start_from;

  if (start_from == "snapshot") {
    // Use GTID captured during snapshot build
    start_gtid = snapshot_gtid_;
    if (start_gtid.empty()) {
      spdlog::warn("Snapshot GTID not available, replication may miss changes");
    } else {
      spdlog::info("Replication will start from snapshot GTID: {}", start_gtid);
    }
  } else if (start_from == "latest") {
    // Get current GTID from MySQL
    auto latest_gtid = mysql_connection_->GetLatestGTID();
    if (latest_gtid) {
      start_gtid = latest_gtid.value();
      spdlog::info("Replication will start from latest GTID: {}", start_gtid);
    } else {
      spdlog::warn("Failed to get latest GTID, starting from empty");
      start_gtid = "";
    }
  } else if (start_from.find("gtid=") == 0) {
    // Extract GTID from "gtid=<UUID:txn>" format
    start_gtid = start_from.substr(kGtidPrefixLength);
    spdlog::info("Replication will start from specified GTID: {}", start_gtid);
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

  binlog_reader_ = std::make_unique<mysql::BinlogReader>(*mysql_connection_, table_contexts_ptrs, binlog_config);

  // Save GTID for Start() method
  snapshot_gtid_ = start_gtid;

  if (start_gtid.empty()) {
    spdlog::info("Binlog replication initialized but not started (waiting for GTID)");
  } else {
    spdlog::info("Binlog replication initialized (will start on Start() call)");
  }
#endif
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ServerOrchestrator::InitializeServers() {
  // Validate network ACL configuration
  if (deps_.config.network.allow_cidrs.empty()) {
    spdlog::warn(
        "Network ACL is empty - all connections will be DENIED by default. "
        "Configure 'network.allow_cidrs' to allow specific IP ranges, "
        "or use ['0.0.0.0/0'] to allow all (NOT RECOMMENDED for production).");
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

  spdlog::info("TCP server initialized");

  // Initialize HTTP server (if enabled)
  if (deps_.config.api.http.enable) {
    server::HttpServerConfig http_config;
    http_config.bind = deps_.config.api.http.bind;
    http_config.port = deps_.config.api.http.port;
    http_config.enable_cors = deps_.config.api.http.enable_cors;
    http_config.cors_allow_origin = deps_.config.api.http.cors_allow_origin;
    http_config.allow_cidrs = deps_.config.network.allow_cidrs;

#ifdef USE_MYSQL
    http_server_ = std::make_unique<server::HttpServer>(http_config, table_contexts_ptrs, &deps_.config,
                                                        binlog_reader_.get(), tcp_server_->GetCacheManager(),
                                                        tcp_server_->GetLoadingFlag(), tcp_server_->GetMutableStats());
#else
    http_server_ = std::make_unique<server::HttpServer>(http_config, table_contexts_ptrs, &deps_.config, nullptr,
                                                        tcp_server_->GetCacheManager(), tcp_server_->GetLoadingFlag(),
                                                        tcp_server_->GetMutableStats());
#endif

    spdlog::info("HTTP server initialized");
  }

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> ServerOrchestrator::HandleMySQLConfigChange(
    const config::Config& new_config) {
#ifdef USE_MYSQL
  spdlog::info("MySQL connection settings changed, reconnecting...");

  // Stop binlog reader if running
  bool was_running = false;
  if (binlog_reader_ && binlog_reader_->IsRunning()) {
    was_running = true;
    spdlog::info("Stopping binlog reader for reconnection");
    binlog_reader_->Stop();
  }

  // Close and recreate connection
  mysql_connection_->Close();

  mysql::Connection::Config mysql_config{
      .host = new_config.mysql.host,
      .port = static_cast<uint16_t>(new_config.mysql.port),
      .user = new_config.mysql.user,
      .password = new_config.mysql.password,
      .database = new_config.mysql.database,
      .connect_timeout = static_cast<uint32_t>(new_config.mysql.connect_timeout_ms / kMillisecondsPerSecond),
      .read_timeout = static_cast<uint32_t>(new_config.mysql.read_timeout_ms / kMillisecondsPerSecond),
      .write_timeout = static_cast<uint32_t>(new_config.mysql.write_timeout_ms / kMillisecondsPerSecond),
      .ssl_enable = new_config.mysql.ssl_enable,
      .ssl_ca = new_config.mysql.ssl_ca,
      .ssl_cert = new_config.mysql.ssl_cert,
      .ssl_key = new_config.mysql.ssl_key,
      .ssl_verify_server_cert = new_config.mysql.ssl_verify_server_cert,
  };

  mysql_connection_ = std::make_unique<mysql::Connection>(mysql_config);

  auto connect_result = mysql_connection_->Connect("config-reload");
  if (!connect_result) {
    spdlog::error("Failed to reconnect to MySQL after config reload: {}", connect_result.error().to_string());
    spdlog::warn("Binlog replication will remain stopped until manual restart");
    return mygram::utils::MakeUnexpected(connect_result.error());
  }

  spdlog::info("Successfully reconnected to MySQL: {}@{}:{}", new_config.mysql.user, new_config.mysql.host,
               new_config.mysql.port);

  // Restart binlog reader if it was running
  if (was_running && binlog_reader_) {
    auto start_result = binlog_reader_->Start();
    if (!start_result) {
      spdlog::error("Failed to restart binlog reader: {}", start_result.error().to_string());
      return mygram::utils::MakeUnexpected(start_result.error());
    }
    spdlog::info("Binlog reader restarted successfully");
  }
#endif

  return {};
}

}  // namespace mygramdb::app
