/**
 * @file main.cpp
 * @brief Entry point for MygramDB
 */

#include "config/config.h"
#include "index/index.h"
#include "server/http_server.h"
#include "server/tcp_server.h"
#include "storage/document_store.h"
#include "storage/snapshot_builder.h"
#include "utils/daemon_utils.h"
#include "version.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#include "mysql/connection.h"
#endif

#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <csignal>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <thread>
#include <unordered_map>

#ifndef _WIN32
#include <unistd.h>  // for getuid(), geteuid()
#endif

namespace {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
volatile std::sig_atomic_t g_shutdown_requested = 0;
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
volatile std::sig_atomic_t g_cancel_snapshot_requested = 0;

constexpr uint64_t kProgressLogInterval = 10000;  // Log progress every N rows
constexpr size_t kGtidPrefixLength = 5;           // "gtid="
constexpr size_t kDefaultMaxConnections = 1000;   // Default max TCP connections
constexpr int kShutdownCheckIntervalMs = 100;     // Shutdown check interval (ms)
constexpr int kMillisecondsPerSecond = 1000;      // Milliseconds to seconds conversion

/**
 * @brief Signal handler for graceful shutdown
 * @param signal Signal number
 *
 * This handler is async-signal-safe: it only sets atomic flags and performs
 * no other operations (no mutex locks, no heap allocations, no function calls).
 */
void SignalHandler(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    g_shutdown_requested = 1;
    g_cancel_snapshot_requested = 1;  // Set flag for main loop to process
  }
}

}  // namespace

// Use TableContext from tcp_server.h
using TableContext = mygramdb::server::TableContext;

/**
 * @brief Main entry point
 * @param argc Argument count
 * @param argv Argument values
 * @return Exit code
 */
int main(int argc, char* argv[]) {
  // Check if running as root (security check)
#ifndef _WIN32
  if (getuid() == 0 || geteuid() == 0) {
    std::cerr << "ERROR: Running MygramDB as root is not allowed for security reasons.\n";
    std::cerr << "Please run as a non-privileged user.\n";
    std::cerr << "\n";
    std::cerr << "Recommended approaches:\n";
    std::cerr << "  - systemd: Use User= and Group= directives in service file\n";
    std::cerr << "  - Docker: Use USER directive in Dockerfile (already configured)\n";
    std::cerr << "  - Manual: Run as a dedicated user (e.g., 'sudo -u mygramdb mygramdb -c config.yaml')\n";
    return 1;
  }
#endif

  // Setup signal handlers
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  // Setup logging
  spdlog::set_level(spdlog::level::info);
  spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");

  // Parse command line arguments
  // NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  bool config_test_mode = false;
  bool daemon_mode = false;
  const char* config_path = nullptr;
  const char* schema_path = nullptr;

  // Handle help and version flags first
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "-h" || arg == "--help") {
      std::cout << "Usage: " << argv[0] << " [OPTIONS] <config.yaml|config.json>\n";
      std::cout << "       " << argv[0] << " -c <config.yaml|config.json> [OPTIONS]\n";
      std::cout << "\n";
      std::cout << "Options:\n";
      std::cout << "  -c, --config <file>            Configuration file path\n";
      std::cout << "  -d, --daemon                   Run as daemon (background process)\n";
      std::cout << "  -t, --config-test              Test configuration file and exit\n";
      std::cout << "  -s, --schema <schema.json>     Use custom JSON Schema (optional)\n";
      std::cout << "  -h, --help                     Show this help message\n";
      std::cout << "  -v, --version                  Show version information\n";
      std::cout << "\n";
      std::cout << "Configuration file format (auto-detected):\n";
      std::cout << "  - YAML (.yaml, .yml) - validated against built-in schema\n";
      std::cout << "  - JSON (.json)       - validated against built-in schema\n";
      std::cout << "\n";
      std::cout << "Note: All configurations are validated automatically using the built-in\n";
      std::cout << "      JSON Schema. Use --schema only to override with a custom schema.\n";
      return 0;
    }
    if (arg == "-v" || arg == "--version") {
      std::cout << mygramdb::Version::FullString() << "\n";
      return 0;
    }
  }

  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " [OPTIONS] <config.yaml|config.json>\n";
    std::cerr << "Try '" << argv[0] << " --help' for more information.\n";
    return 1;
  }

  // Parse arguments
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "-c" || arg == "--config") {
      if (i + 1 >= argc) {
        std::cerr << "Error: --config requires an argument\n";
        return 1;
      }
      config_path = argv[++i];
    } else if (arg == "-d" || arg == "--daemon") {
      daemon_mode = true;
    } else if (arg == "-t" || arg == "--config-test") {
      config_test_mode = true;
    } else if (arg == "-s" || arg == "--schema") {
      if (i + 1 >= argc) {
        std::cerr << "Error: --schema requires an argument\n";
        return 1;
      }
      schema_path = argv[++i];
    } else if (config_path == nullptr && arg[0] != '-') {
      // Positional argument for config file (backward compatibility)
      config_path = argv[i];
    } else {
      std::cerr << "Error: Unexpected argument: " << argv[i] << "\n";
      return 1;
    }
  }
  // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)

  if (config_path == nullptr) {
    std::cerr << "Error: Configuration file path required\n";
    return 1;
  }

  if (!config_test_mode) {
    spdlog::info("{} starting...", mygramdb::Version::FullString());
  }

  spdlog::info("Loading configuration from: {}", config_path);
  if (schema_path != nullptr) {
    spdlog::info("Using custom JSON Schema: {}", schema_path);
  } else {
    spdlog::debug("Using built-in JSON Schema for validation");
  }

  // Load configuration
  auto config_result = mygramdb::config::LoadConfig(config_path, schema_path != nullptr ? schema_path : "");
  if (!config_result) {
    spdlog::error("Failed to load configuration: {}", config_result.error().to_string());
    return 1;
  }
  mygramdb::config::Config config = *config_result;

  // Apply logging configuration
  if (config.logging.level == "debug") {
    spdlog::set_level(spdlog::level::debug);
  } else if (config.logging.level == "info") {
    spdlog::set_level(spdlog::level::info);
  } else if (config.logging.level == "warn") {
    spdlog::set_level(spdlog::level::warn);
  } else if (config.logging.level == "error") {
    spdlog::set_level(spdlog::level::err);
  }

  // Configure log output (file or stdout)
  if (!config.logging.file.empty()) {
    try {
      // Ensure log directory exists
      std::filesystem::path log_path(config.logging.file);
      std::filesystem::path log_dir = log_path.parent_path();
      if (!log_dir.empty() && !std::filesystem::exists(log_dir)) {
        std::filesystem::create_directories(log_dir);
      }

      // Create file logger
      auto file_logger = spdlog::basic_logger_mt("mygramdb", config.logging.file);
      spdlog::set_default_logger(file_logger);
      spdlog::info("Logging to file: {}", config.logging.file);
    } catch (const spdlog::spdlog_ex& ex) {
      std::cerr << "Log file initialization failed: " << ex.what() << "\n";
      return 1;
    }
  }

  spdlog::info("Configuration loaded successfully from {}", config_path);

  if (config.tables.empty()) {
    spdlog::error("No tables configured");
    return 1;
  }

  // Daemonize if requested (must be done after argument parsing but before opening files/sockets)
  if (daemon_mode) {
    spdlog::info("Daemonizing process...");
    if (!mygramdb::utils::Daemonize()) {
      spdlog::error("Failed to daemonize process");
      return 1;
    }
    // Note: After daemonization, stdout/stderr are redirected to /dev/null
    // All output must go through spdlog to be visible (configure file logging if needed)
  }

  // Config test mode: validate and exit
  if (config_test_mode) {
    std::cout << "Configuration file syntax is OK\n";
    std::cout << "Configuration details:\n";
    std::cout << "  MySQL: " << config.mysql.user << "@" << config.mysql.host << ":" << config.mysql.port << "\n";
    std::cout << "  Tables: " << config.tables.size() << "\n";
    for (const auto& table : config.tables) {
      std::cout << "    - " << table.name << " (primary_key: " << table.primary_key
                << ", ngram_size: " << table.ngram_size << ")\n";
    }
    std::cout << "  API TCP: " << config.api.tcp.bind << ":" << config.api.tcp.port << "\n";
    std::cout << "  Replication: " << (config.replication.enable ? "enabled" : "disabled") << "\n";
    std::cout << "  Logging level: " << config.logging.level << "\n";
    return 0;
  }

  // Verify dump directory permissions
  try {
    std::filesystem::path dump_dir(config.dump.dir);

    // Create directory if it doesn't exist
    if (!std::filesystem::exists(dump_dir)) {
      spdlog::info("Creating dump directory: {}", config.dump.dir);
      std::filesystem::create_directories(dump_dir);
    }

    // Check if directory is writable by attempting to create a test file
    std::filesystem::path test_file = dump_dir / ".write_test";
    std::ofstream test_stream(test_file);
    if (!test_stream.is_open()) {
      spdlog::error("Dump directory is not writable: {}", config.dump.dir);
      spdlog::error("Please check directory permissions");
      return 1;
    }
    test_stream.close();
    std::filesystem::remove(test_file);

    spdlog::info("Dump directory verified: {}", config.dump.dir);
  } catch (const std::exception& e) {
    spdlog::error("Failed to verify dump directory: {}", e.what());
    return 1;
  }

  // Initialize table contexts for all configured tables
  spdlog::info("Initializing {} table(s)...", config.tables.size());
  std::unordered_map<std::string, std::unique_ptr<TableContext>> table_contexts;

#ifdef USE_MYSQL
  // Initialize MySQL connection
  mygramdb::mysql::Connection::Config mysql_config;
  mysql_config.host = config.mysql.host;
  mysql_config.port = config.mysql.port;
  mysql_config.user = config.mysql.user;
  mysql_config.password = config.mysql.password;
  mysql_config.database = config.mysql.database;
  mysql_config.connect_timeout = config.mysql.connect_timeout_ms / kMillisecondsPerSecond;  // ms to sec
  mysql_config.read_timeout = config.mysql.read_timeout_ms / kMillisecondsPerSecond;        // ms to sec
  mysql_config.write_timeout = config.mysql.write_timeout_ms / kMillisecondsPerSecond;      // ms to sec
  mysql_config.ssl_enable = config.mysql.ssl_enable;
  mysql_config.ssl_ca = config.mysql.ssl_ca;
  mysql_config.ssl_cert = config.mysql.ssl_cert;
  mysql_config.ssl_key = config.mysql.ssl_key;
  mysql_config.ssl_verify_server_cert = config.mysql.ssl_verify_server_cert;

  auto mysql_conn = std::make_unique<mygramdb::mysql::Connection>(mysql_config);

  if (!mysql_conn->Connect("snapshot builder")) {
    spdlog::error("Failed to connect to MySQL: {}", mysql_conn->GetLastError());
    return 1;
  }
#else
  spdlog::warn("MySQL support not compiled, running without replication");
#endif

  // Build snapshots for all tables and collect GTID
  std::string snapshot_gtid;  // Single GTID for all tables

  // Process each configured table
  for (const auto& table_config : config.tables) {
    spdlog::info("Initializing table: {}", table_config.name);

    auto ctx = std::make_unique<TableContext>();
    ctx->name = table_config.name;
    ctx->config = table_config;

    // Create index and document store for this table
    ctx->index = std::make_unique<mygramdb::index::Index>(table_config.ngram_size, table_config.kanji_ngram_size);
    ctx->doc_store = std::make_unique<mygramdb::storage::DocumentStore>();

#ifdef USE_MYSQL
    // Build snapshot for this table (if auto_initial_snapshot is enabled)
    if (config.replication.auto_initial_snapshot) {
      spdlog::info("Building snapshot from table: {}", table_config.name);
      spdlog::info("This may take a while for large tables. Please wait...");
      mygramdb::storage::SnapshotBuilder snapshot_builder(*mysql_conn, *ctx->index, *ctx->doc_store, table_config,
                                                          config.build);

      auto snapshot_result = snapshot_builder.Build([&table_config, &snapshot_builder](const auto& progress) {
        // Check cancellation flag in progress callback (safe: main thread context)
        if (g_cancel_snapshot_requested != 0) {
          spdlog::info("Snapshot cancellation requested during build");
          snapshot_builder.Cancel();
        }

        if (progress.processed_rows % kProgressLogInterval == 0) {
          spdlog::debug("table: {} - Progress: {} rows processed ({:.0f} rows/s)", table_config.name,
                        progress.processed_rows, progress.rows_per_second);
        }
      });

      // Check if shutdown was requested
      if (g_shutdown_requested != 0) {
        spdlog::warn("Snapshot build cancelled by shutdown signal for table: {}", table_config.name);
        return 1;
      }

      if (!snapshot_result) {
        spdlog::error("Failed to build snapshot for table: {} - {}", table_config.name,
                      snapshot_result.error().message());
        return 1;
      }

      spdlog::info("Snapshot build completed - table: {}, documents: {}", table_config.name,
                   snapshot_builder.GetProcessedRows());

      // Capture GTID from first table's snapshot
      if (snapshot_gtid.empty() && config.replication.enable) {
        snapshot_gtid = snapshot_builder.GetSnapshotGTID();
        if (!snapshot_gtid.empty()) {
          spdlog::info("Captured snapshot GTID for replication: {}", snapshot_gtid);
        }
      }
    } else {
      spdlog::info("Skipping automatic snapshot build for table: {} (auto_initial_snapshot=false)", table_config.name);
      spdlog::info("Use SYNC command to manually trigger snapshot synchronization");
    }
#endif

    // Store table context
    table_contexts[table_config.name] = std::move(ctx);
    spdlog::info("Table initialized successfully: {}", table_config.name);
  }

  spdlog::info("All {} table(s) initialized", table_contexts.size());

#ifdef USE_MYSQL
  // Create single shared BinlogReader for all tables
  std::unique_ptr<mygramdb::mysql::BinlogReader> binlog_reader;

  if (config.replication.enable && !table_contexts.empty()) {
    std::string start_gtid;
    const std::string& start_from = config.replication.start_from;

    if (start_from == "snapshot") {
      // Use GTID captured during snapshot build
      start_gtid = snapshot_gtid;
      if (start_gtid.empty()) {
        spdlog::warn("Snapshot GTID not available, replication may miss changes");
      } else {
        spdlog::info("Replication will start from snapshot GTID: {}", start_gtid);
      }
    } else if (start_from == "latest") {
      // Get current GTID from MySQL
      auto latest_gtid = mysql_conn->GetLatestGTID();
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
    std::unordered_map<std::string, TableContext*> table_contexts_ptrs;
    for (auto& [name, ctx] : table_contexts) {
      table_contexts_ptrs[name] = ctx.get();
    }

    // Create single binlog reader for all tables
    mygramdb::mysql::BinlogReader::Config binlog_config;
    binlog_config.start_gtid = start_gtid;
    binlog_config.queue_size = config.replication.queue_size;

    binlog_reader = std::make_unique<mygramdb::mysql::BinlogReader>(*mysql_conn, table_contexts_ptrs, binlog_config);

    // Only start if we have a GTID
    if (!start_gtid.empty()) {
      auto start_result = binlog_reader->Start();
      if (!start_result) {
        spdlog::error("Failed to start binlog reader: {}", start_result.error().to_string());
        return 1;
      }
      spdlog::info("Binlog replication started from GTID: {}", start_gtid);
    } else {
      spdlog::info("Binlog replication initialized but not started (waiting for GTID)");
    }
  }
#endif

  // Prepare table_contexts map for servers (already prepared above for BinlogReader)
  // Reuse the same map for TCP server
  std::unordered_map<std::string, TableContext*> table_contexts_ptrs;
  for (auto& [name, ctx] : table_contexts) {
    table_contexts_ptrs[name] = ctx.get();
  }

  // Validate network ACL configuration
  if (config.network.allow_cidrs.empty()) {
    spdlog::warn(
        "Network ACL is empty - all connections will be DENIED by default. "
        "Configure 'network.allow_cidrs' to allow specific IP ranges, "
        "or use ['0.0.0.0/0'] to allow all (NOT RECOMMENDED for production).");
  }

  // Start TCP server
  mygramdb::server::ServerConfig server_config;
  server_config.host = config.api.tcp.bind;
  server_config.port = config.api.tcp.port;
  server_config.max_connections = kDefaultMaxConnections;
  server_config.default_limit = config.api.default_limit;
  server_config.max_query_length = config.api.max_query_length;
  server_config.allow_cidrs = config.network.allow_cidrs;

#ifdef USE_MYSQL
  mygramdb::server::TcpServer tcp_server(server_config, table_contexts_ptrs, config.dump.dir, &config,
                                         binlog_reader.get());

  // Set server statistics for binlog reader
  if (binlog_reader) {
    binlog_reader->SetServerStats(tcp_server.GetMutableStats());
  }
#else
  mygramdb::server::TcpServer tcp_server(server_config, table_contexts_ptrs, config.dump.dir, &config, nullptr);
#endif

  auto tcp_start_result = tcp_server.Start();
  if (!tcp_start_result) {
    spdlog::error("Failed to start TCP server: {}", tcp_start_result.error().to_string());
    return 1;
  }

  spdlog::info("TCP server started on {}:{}", server_config.host, server_config.port);

  // Start HTTP server (if enabled)
  std::unique_ptr<mygramdb::server::HttpServer> http_server;
  if (config.api.http.enable) {
    mygramdb::server::HttpServerConfig http_config;
    http_config.bind = config.api.http.bind;
    http_config.port = config.api.http.port;
    http_config.enable_cors = config.api.http.enable_cors;
    http_config.cors_allow_origin = config.api.http.cors_allow_origin;
    http_config.allow_cidrs = config.network.allow_cidrs;

#ifdef USE_MYSQL
    http_server = std::make_unique<mygramdb::server::HttpServer>(
        http_config, table_contexts_ptrs, &config, binlog_reader.get(), tcp_server.GetCacheManager(),
        tcp_server.GetLoadingFlag(), tcp_server.GetMutableStats());
#else
    http_server = std::make_unique<mygramdb::server::HttpServer>(
        http_config, table_contexts_ptrs, &config, nullptr, tcp_server.GetCacheManager(), tcp_server.GetLoadingFlag(),
        tcp_server.GetMutableStats());
#endif

    auto http_start_result = http_server->Start();
    if (!http_start_result) {
      spdlog::error("Failed to start HTTP server: {}", http_start_result.error().to_string());
      tcp_server.Stop();
      return 1;
    }

    spdlog::info("HTTP server started on {}:{}", http_config.bind, http_config.port);
  }

  spdlog::info("MygramDB is ready to serve requests");

  // Wait for shutdown signal
  while (g_shutdown_requested == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(kShutdownCheckIntervalMs));
  }

  spdlog::info("Shutdown requested, cleaning up...");

  // Cleanup in reverse order of initialization
  // 1. Stop HTTP server first (depends on table_contexts and binlog_reader)
  if (http_server && http_server->IsRunning()) {
    spdlog::info("Stopping HTTP server");
    http_server->Stop();
  }

  // 2. Stop TCP server (depends on table_contexts and binlog_reader)
  spdlog::info("Stopping TCP server");
  tcp_server.Stop();

#ifdef USE_MYSQL
  // 3. Stop and destroy binlog reader (depends on mysql connection and table_contexts)
  if (binlog_reader) {
    if (binlog_reader->IsRunning()) {
      spdlog::info("Stopping binlog reader");
      binlog_reader->Stop();
    }
    // Explicitly destroy binlog_reader before other resources
    binlog_reader.reset();
  }

  // 4. Close MySQL connection
  if (mysql_conn) {
    mysql_conn->Close();
  }
#endif

  // 5. HTTP server will be automatically destroyed
  http_server.reset();

  // 6. Table contexts will be automatically destroyed (index, doc_store)
  // 7. Config will be automatically destroyed

  spdlog::info("MygramDB stopped");
  return 0;
}
