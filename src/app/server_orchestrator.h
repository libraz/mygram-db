/**
 * @file server_orchestrator.h
 * @brief Server component lifecycle orchestration
 */

#ifndef MYGRAMDB_APP_SERVER_ORCHESTRATOR_H_
#define MYGRAMDB_APP_SERVER_ORCHESTRATOR_H_

#include <memory>
#include <string>
#include <unordered_map>

#include "config/config.h"
#include "server/server_types.h"
#include "utils/error.h"
#include "utils/expected.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#include "mysql/connection.h"
#endif

namespace mygramdb {

namespace server {
class TcpServer;
class HttpServer;
}  // namespace server

namespace app {

// Import Expected from utils namespace
using mygram::utils::Error;
using mygram::utils::Expected;

class SignalManager;

/**
 * @brief Orchestrates server component lifecycle
 *
 * Responsibilities:
 * - Initialize table contexts (Index, DocumentStore)
 * - Build snapshots (if auto_initial_snapshot enabled)
 * - Initialize MySQL connection and BinlogReader
 * - Start TCP/HTTP servers
 * - Handle config changes affecting MySQL connection
 *
 * Design Pattern: Facade + Lifecycle Manager
 * - Owns all server components (table contexts, MySQL, servers)
 * - Manages initialization and shutdown order
 * - Provides runtime variable management through RuntimeVariableManager
 *
 * Dependency Order:
 * 1. Table contexts (no dependencies)
 * 2. MySQL connection (independent)
 * 3. Snapshots (requires MySQL + table contexts)
 * 4. BinlogReader (requires MySQL + table contexts)
 * 5. TCP server (requires all above)
 * 6. HTTP server (requires TCP server's cache manager)
 */
class ServerOrchestrator {
 public:
  /**
   * @brief Dependencies required by ServerOrchestrator
   *
   * Uses reference members for dependency injection (non-owning).
   * Lifetime managed by Application class.
   */
  struct Dependencies {
    // NOLINTBEGIN(cppcoreguidelines-avoid-const-or-ref-data-members) - Dependency injection pattern
    const config::Config& config;
    SignalManager& signal_manager;  ///< For cancellation checks during snapshot build
    const std::string& dump_dir;
    // NOLINTEND(cppcoreguidelines-avoid-const-or-ref-data-members)
  };

  /**
   * @brief Create orchestrator with dependencies
   * @param deps Dependencies (non-owning references)
   * @return Expected with orchestrator instance or error
   */
  static Expected<std::unique_ptr<ServerOrchestrator>, mygram::utils::Error> Create(Dependencies deps);

  ~ServerOrchestrator();

  // Non-copyable, non-movable
  ServerOrchestrator(const ServerOrchestrator&) = delete;
  ServerOrchestrator& operator=(const ServerOrchestrator&) = delete;
  ServerOrchestrator(ServerOrchestrator&&) = delete;
  ServerOrchestrator& operator=(ServerOrchestrator&&) = delete;

  /**
   * @brief Initialize all components (tables, MySQL, servers)
   * @return Expected with void or error
   *
   * Initialization order:
   * 1. Table contexts (Index, DocumentStore)
   * 2. MySQL connection
   * 3. Snapshot building (if enabled)
   * 4. BinlogReader initialization
   * 5. TCP server initialization
   * 6. HTTP server initialization (if enabled)
   *
   * Note: This method does NOT start servers (use Start() for that)
   */
  Expected<void, mygram::utils::Error> Initialize();

  /**
   * @brief Start all server components
   * @return Expected with void or error
   *
   * This method starts:
   * - BinlogReader (if GTID available)
   * - TCP server
   * - HTTP server (if enabled)
   */
  Expected<void, mygram::utils::Error> Start();

  /**
   * @brief Stop all server components (reverse order)
   *
   * Shutdown order:
   * 1. HTTP server (if running)
   * 2. TCP server
   * 3. BinlogReader (if running)
   * 4. MySQL connection
   * 5. Table contexts (Index, DocumentStore)
   */
  void Stop();

  /**
   * @brief Check if servers are running
   */
  bool IsRunning() const;

 private:
  ServerOrchestrator(Dependencies deps);

  // Initialization steps
  Expected<void, mygram::utils::Error> InitializeTables();
  Expected<void, mygram::utils::Error> InitializeMySQL();
  Expected<void, mygram::utils::Error> BuildSnapshots();
  Expected<void, mygram::utils::Error> InitializeBinlogReader();
  Expected<void, mygram::utils::Error> InitializeServers();

  // Dependencies (non-owning)
  Dependencies deps_;

  // Owned components
  std::unordered_map<std::string, std::unique_ptr<server::TableContext>> table_contexts_;
#ifdef USE_MYSQL
  std::unique_ptr<mysql::Connection> mysql_connection_;
  std::unique_ptr<mysql::BinlogReader> binlog_reader_;
#endif
  std::unique_ptr<server::TcpServer> tcp_server_;
  std::unique_ptr<server::HttpServer> http_server_;

  // State
  std::string snapshot_gtid_;  ///< Captured during snapshot build
  bool initialized_{false};
  bool started_{false};
};

}  // namespace app
}  // namespace mygramdb

#endif  // MYGRAMDB_APP_SERVER_ORCHESTRATOR_H_
