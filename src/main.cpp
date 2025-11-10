/**
 * @file main.cpp
 * @brief Entry point for MygramDB
 */

#include "config/config.h"
#include "index/index.h"
#include "storage/document_store.h"
#include "storage/snapshot_builder.h"
#include "server/tcp_server.h"

#ifdef USE_MYSQL
#include "mysql/connection.h"
#include "mysql/binlog_reader.h"
#include "storage/gtid_state.h"
#endif

#include <csignal>
#include <iostream>
#include <memory>
#include <thread>
#include <spdlog/spdlog.h>

namespace {
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

/**
 * @brief Main entry point
 * @param argc Argument count
 * @param argv Argument values
 * @return Exit code
 */
int main(int argc, char* argv[]) {
  // Setup signal handlers
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  // Setup logging
  spdlog::set_level(spdlog::level::info);
  spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");

  spdlog::info("MygramDB v1.0.0 starting...");

  if (argc < 2) {
    spdlog::error("Usage: {} <config.yaml>", argv[0]);
    return 1;
  }

  const char* config_path = argv[1];
  spdlog::info("Loading configuration from: {}", config_path);

  // Load configuration
  mygramdb::config::Config config;
  try {
    config = mygramdb::config::LoadConfig(config_path);
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

  // For now, only support one table
  const auto& table_config = config.tables[0];
  spdlog::info("Configured table: {}", table_config.name);

  // Create index and document store
  auto index = std::make_unique<mygramdb::index::Index>(table_config.ngram_size);
  auto doc_store = std::make_unique<mygramdb::storage::DocumentStore>();

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

  // Build snapshot
  spdlog::info("Building snapshot from table '{}'...", table_config.name);
  mygramdb::storage::SnapshotBuilder snapshot_builder(
      *mysql_conn, *index, *doc_store, table_config);

  bool snapshot_success = snapshot_builder.Build([](const auto& progress) {
    if (progress.processed_rows % 10000 == 0) {
      spdlog::info("Processed {} rows ({:.0f} rows/s)",
                   progress.processed_rows, progress.rows_per_second);
    }
  });

  if (!snapshot_success) {
    spdlog::error("Failed to build snapshot: {}", snapshot_builder.GetLastError());
    return 1;
  }

  spdlog::info("Snapshot build completed: {} documents indexed",
               snapshot_builder.GetProcessedRows());

  // Determine starting GTID based on replication.start_from configuration
  std::unique_ptr<mygramdb::mysql::BinlogReader> binlog_reader;
  std::string start_gtid;

  if (config.replication.enable) {
    const std::string& start_from = config.replication.start_from;

    if (start_from == "snapshot") {
      // Use GTID captured during snapshot build
      start_gtid = snapshot_builder.GetSnapshotGTID();
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
      // Read GTID from state file
      mygramdb::storage::GTIDStateFile gtid_state("./gtid_state.txt");
      auto saved_gtid = gtid_state.Read();
      if (saved_gtid) {
        start_gtid = saved_gtid.value();
        spdlog::info("Replication will start from state file GTID: {}", start_gtid);
      } else {
        spdlog::warn("Failed to read GTID from state file, starting from empty");
        start_gtid = "";
      }
    }

    // Create binlog reader
    mygramdb::mysql::BinlogReader::Config binlog_config;
    binlog_config.start_gtid = start_gtid;
    binlog_config.queue_size = config.replication.queue_size;

    binlog_reader = std::make_unique<mygramdb::mysql::BinlogReader>(
        *mysql_conn, *index, *doc_store, table_config, binlog_config);

    // Only start if we have a GTID (not for snapshot mode during initial build)
    if (!start_gtid.empty()) {
      if (!binlog_reader->Start()) {
        spdlog::error("Failed to start binlog reader");
        return 1;
      }
      spdlog::info("Binlog replication started from GTID: {}", start_gtid);
    } else {
      spdlog::info("Binlog replication initialized but not started (waiting for snapshot GTID)");
    }
  }
#else
  spdlog::warn("MySQL support not compiled, running without replication");
#endif

  // Start TCP server
  mygramdb::server::ServerConfig server_config;
  server_config.host = config.api.tcp.bind;
  server_config.port = config.api.tcp.port;
  server_config.max_connections = 1000;  // Default value

  mygramdb::server::TcpServer tcp_server(server_config, *index, *doc_store,
                                         table_config.ngram_size,
                                         config.snapshot.dir,
#ifdef USE_MYSQL
                                         binlog_reader.get()
#else
                                         nullptr
#endif
                                         );

  if (!tcp_server.Start()) {
    spdlog::error("Failed to start TCP server: {}", tcp_server.GetLastError());
    return 1;
  }

  spdlog::info("TCP server started on {}:{}", server_config.host, server_config.port);
  spdlog::info("MygramDB is ready to serve requests");

  // Wait for shutdown signal
  while (!g_shutdown_requested) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  spdlog::info("Shutdown requested, cleaning up...");

  // Cleanup
#ifdef USE_MYSQL
  if (binlog_reader && binlog_reader->IsRunning()) {
    binlog_reader->Stop();
  }
#endif

  tcp_server.Stop();

  spdlog::info("MygramDB stopped");
  return 0;
}
