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
#include "version.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#include "mysql/connection.h"
#include "storage/gtid_state.h"
#endif

#include <spdlog/spdlog.h>

#include <csignal>
#include <iostream>
#include <memory>
#include <thread>
#include <unordered_map>

namespace {
// NOLINTNEXTLINE(cppcoreguidelines-avoid-non-const-global-variables)
volatile std::sig_atomic_t g_shutdown_requested = 0;

/**
 * @brief Signal handler for graceful shutdown
 * @param signal Signal number
 */
void SignalHandler(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    g_shutdown_requested = 1;
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
// NOLINTNEXTLINE(readability-function-cognitive-complexity)
int main(int argc, char* argv[]) {
  // Setup signal handlers
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  // Setup logging
  spdlog::set_level(spdlog::level::info);
  spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");

  // Parse command line arguments
  bool config_test_mode = false;
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
    } else if (arg == "-v" || arg == "--version") {
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
  mygramdb::config::Config config;
  try {
    config = mygramdb::config::LoadConfig(config_path, schema_path != nullptr ? schema_path : "");
  } catch (const std::exception& e) {
    spdlog::error("Failed to load configuration: {}", e.what());
    return 1;
  }

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
  spdlog::info("Configuration loaded successfully from {}", config_path);

  if (config.tables.empty()) {
    spdlog::error("No tables configured");
    return 1;
  }

  // Config test mode: validate and exit
  if (config_test_mode) {
    std::cout << "Configuration file syntax is OK\n";
    std::cout << "Configuration details:\n";
    std::cout << "  MySQL: " << config.mysql.user << "@" << config.mysql.host << ":"
              << config.mysql.port << "\n";
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

  auto mysql_conn = std::make_unique<mygramdb::mysql::Connection>(mysql_config);

  if (!mysql_conn->Connect()) {
    spdlog::error("Failed to connect to MySQL: {}", mysql_conn->GetLastError());
    return 1;
  }

  spdlog::info("Connected to MySQL {}:{}/{}", config.mysql.host, config.mysql.port,
               config.mysql.database);
#else
  spdlog::warn("MySQL support not compiled, running without replication");
#endif

  // Build snapshots for all tables and collect GTID
  std::string snapshot_gtid;  // Single GTID for all tables

  // Process each configured table
  for (const auto& table_config : config.tables) {
    spdlog::info("Initializing table: '{}'", table_config.name);

    auto ctx = std::make_unique<TableContext>();
    ctx->name = table_config.name;
    ctx->config = table_config;

    // Create index and document store for this table
    ctx->index = std::make_unique<mygramdb::index::Index>(table_config.ngram_size,
                                                          table_config.kanji_ngram_size);
    ctx->doc_store = std::make_unique<mygramdb::storage::DocumentStore>();

#ifdef USE_MYSQL
    // Build snapshot for this table
    spdlog::info("Building snapshot from table '{}'...", table_config.name);
    mygramdb::storage::SnapshotBuilder snapshot_builder(*mysql_conn, *ctx->index, *ctx->doc_store,
                                                        table_config, config.build);

    bool snapshot_success = snapshot_builder.Build([&table_config](const auto& progress) {
      if (progress.processed_rows % 10000 == 0) {
        spdlog::info("[{}] Processed {} rows ({:.0f} rows/s)", table_config.name,
                     progress.processed_rows, progress.rows_per_second);
      }
    });

    if (!snapshot_success) {
      spdlog::error("Failed to build snapshot for table '{}': {}", table_config.name,
                    snapshot_builder.GetLastError());
      return 1;
    }

    spdlog::info("Snapshot build completed for table '{}': {} documents indexed", table_config.name,
                 snapshot_builder.GetProcessedRows());

    // Capture GTID from first table's snapshot
    if (snapshot_gtid.empty() && config.replication.enable) {
      snapshot_gtid = snapshot_builder.GetSnapshotGTID();
      if (!snapshot_gtid.empty()) {
        spdlog::info("Captured snapshot GTID for replication: {}", snapshot_gtid);
      }
    }
#endif

    // Store table context
    table_contexts[table_config.name] = std::move(ctx);
    spdlog::info("Table '{}' initialized successfully", table_config.name);
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
      start_gtid = start_from.substr(5);
      spdlog::info("Replication will start from specified GTID: {}", start_gtid);
    } else if (start_from == "state_file") {
      // Read GTID from state file (single file for all tables)
      mygramdb::storage::GTIDStateFile gtid_state(config.replication.state_file);
      auto saved_gtid = gtid_state.Read();
      if (saved_gtid) {
        start_gtid = saved_gtid.value();
        spdlog::info("Replication will start from state file GTID: {}", start_gtid);
      } else {
        spdlog::warn("Failed to read GTID from state file, starting from empty");
        start_gtid = "";
      }
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
    binlog_config.state_file_path = config.replication.state_file;

    binlog_reader = std::make_unique<mygramdb::mysql::BinlogReader>(
        *mysql_conn, table_contexts_ptrs, binlog_config);

    // Only start if we have a GTID
    if (!start_gtid.empty()) {
      if (!binlog_reader->Start()) {
        spdlog::error("Failed to start binlog reader");
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

  // Start TCP server
  mygramdb::server::ServerConfig server_config;
  server_config.host = config.api.tcp.bind;
  server_config.port = config.api.tcp.port;
  server_config.max_connections = 1000;  // Default value

#ifdef USE_MYSQL
  mygramdb::server::TcpServer tcp_server(server_config, table_contexts_ptrs, config.snapshot.dir,
                                         &config, binlog_reader.get());
#else
  mygramdb::server::TcpServer tcp_server(server_config, table_contexts_ptrs, config.snapshot.dir,
                                         &config, nullptr);
#endif

  if (!tcp_server.Start()) {
    spdlog::error("Failed to start TCP server: {}", tcp_server.GetLastError());
    return 1;
  }

  spdlog::info("TCP server started on {}:{}", server_config.host, server_config.port);

  // Start HTTP server (if enabled)
  std::unique_ptr<mygramdb::server::HttpServer> http_server;
  if (config.api.http.enable) {
    mygramdb::server::HttpServerConfig http_config;
    http_config.bind = config.api.http.bind;
    http_config.port = config.api.http.port;

#ifdef USE_MYSQL
    http_server = std::make_unique<mygramdb::server::HttpServer>(http_config, table_contexts_ptrs,
                                                                 &config, binlog_reader.get());
#else
    http_server = std::make_unique<mygramdb::server::HttpServer>(http_config, table_contexts_ptrs,
                                                                 &config, nullptr);
#endif

    if (!http_server->Start()) {
      spdlog::error("Failed to start HTTP server: {}", http_server->GetLastError());
      tcp_server.Stop();
      return 1;
    }

    spdlog::info("HTTP server started on {}:{}", http_config.bind, http_config.port);
  }

  spdlog::info("MygramDB is ready to serve requests");

  // Wait for shutdown signal
  while (g_shutdown_requested == 0) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  spdlog::info("Shutdown requested, cleaning up...");

  // Cleanup
#ifdef USE_MYSQL
  // Stop shared binlog reader
  if (binlog_reader && binlog_reader->IsRunning()) {
    spdlog::info("Stopping binlog reader");
    binlog_reader->Stop();
  }
#endif

  if (http_server && http_server->IsRunning()) {
    http_server->Stop();
  }

  tcp_server.Stop();

  spdlog::info("MygramDB stopped");
  return 0;
}
