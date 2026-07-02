/**
 * @file server_orchestrator.cpp
 * @brief Server component lifecycle orchestration implementation
 */

#include "app/server_orchestrator.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <unordered_map>

#include "app/mysql_reconnection_handler.h"
#include "app/signal_manager.h"
#include "cache/cache_manager.h"
#include "config/runtime_variable_manager.h"
#include "loader/initial_loader.h"
#include "mysql/connection_validator.h"
#include "query/synonym_dictionary.h"
#include "server/http_server.h"
#include "server/request_dispatcher.h"
#include "server/tcp_server.h"
#include "utils/constants.h"
#include "utils/error.h"
#include "utils/expected.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::app {

namespace {
constexpr uint64_t kProgressLogInterval = 10000;  // Log progress every N rows
constexpr int kMillisecondsPerSecond = static_cast<int>(mygram::constants::kMillisecondsPerSecond);
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

bool ShouldStartBinlogReaderOnServerStart(const mysql::IBinlogReader* binlog_reader, std::string_view start_gtid) {
  (void)start_gtid;
  return binlog_reader != nullptr;
}

std::vector<mysql::ConnectionValidator::RequiredTable> CollectRequiredTables(
    const std::unordered_map<std::string, std::unique_ptr<server::TableContext>>& table_contexts) {
  std::vector<mysql::ConnectionValidator::RequiredTable> required_tables;
  required_tables.reserve(table_contexts.size());
  for (const auto& [name, ctx] : table_contexts) {
    if (ctx != nullptr && !ctx->config.name.empty()) {
      required_tables.push_back({ctx->config.database, ctx->config.name});
    } else {
      required_tables.push_back({"", name});
    }
  }
  return required_tables;
}

Expected<std::string, mygram::utils::Error> ResolveReplicationStartGtid(std::string_view start_from,
                                                                        std::string_view snapshot_gtid,
                                                                        const LatestGtidProvider& latest_provider) {
  if (start_from == "snapshot") {
    return std::string(snapshot_gtid);
  }

  if (start_from == "latest") {
    if (!latest_provider) {
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLQueryFailed,
                                                                    "latest GTID provider is not configured"));
    }
    return latest_provider();
  }

  constexpr std::string_view kGtidPrefix = "gtid=";
  if (start_from.substr(0, kGtidPrefix.size()) == kGtidPrefix) {
    return std::string(start_from.substr(kGtidPrefix.size()));
  }

  return std::string{};
}

bool ShouldStoreNormalizedTexts(const config::Config& config, const config::TableConfig& table_config) {
  (void)config;
  (void)table_config;
  // HIGHLIGHT is a query-level feature with no startup-time disable switch, so
  // regular server initialization must retain normalized text independently of
  // memory.verify_text. Defensive runtime guards still cover tests/manual
  // setups that explicitly disable storage via DocumentStore::SetStoreTexts().
  return true;
}

bool RequiresMysqlConnectionForStartup(const config::Config& config) {
  return config.replication.enable || config.replication.auto_initial_snapshot;
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
  bool binlog_started = false;

  // Start BinlogReader when replication is configured. An empty GTID is a
  // valid start position for a fresh MySQL instance; gating on non-empty GTID
  // leaves replication permanently stopped and readiness stuck at 503.
  if (ShouldStartBinlogReaderOnServerStart(binlog_reader_.get(), snapshot_gtid_)) {
    auto start_result = binlog_reader_->Start();
    if (!start_result) {
      return mygram::utils::MakeUnexpected(start_result.error());
    }
    binlog_started = true;
    mygram::utils::StructuredLog().Event("binlog_replication_started").Field("gtid", snapshot_gtid_).Info();
  }
#endif

  // Start TCP server
  auto tcp_start = tcp_server_->Start();
  if (!tcp_start) {
#ifdef USE_MYSQL
    if (binlog_started && binlog_reader_ && binlog_reader_->IsRunning()) {
      binlog_reader_->Stop();
    }
#endif
    return mygram::utils::MakeUnexpected(tcp_start.error());
  }

  RegisterRuntimeCallbacks();

  // Start HTTP server (if enabled)
  if (http_server_) {
    auto http_start = http_server_->Start();
    if (!http_start) {
      // Cleanup in reverse start order. Start() must be transactional: callers
      // either get a fully-running orchestrator or no background workers left
      // behind after a partial startup failure.
      tcp_server_->Stop();
#ifdef USE_MYSQL
      if (binlog_started && binlog_reader_ && binlog_reader_->IsRunning()) {
        binlog_reader_->Stop();
      }
#endif
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
    ctx->index = std::make_unique<index::Index>(
        table_config.ngram_size, table_config.kanji_ngram_size, deps_.config.memory.roaring_threshold,
        table_config.cross_boundary_ngrams, deps_.config.memory.normalize.nfkc, deps_.config.memory.normalize.width,
        deps_.config.memory.normalize.lower);
    ctx->doc_store = std::make_unique<storage::DocumentStore>();

    // Retain normalized text for text-dependent query features; the decision is
    // intentionally decoupled from memory.verify_text, which only controls
    // post-filtering.
    if (!ShouldStoreNormalizedTexts(deps_.config, table_config)) {
      ctx->doc_store->SetStoreTexts(false);
    }

    // Load synonym dictionary if configured
    if (table_config.synonyms.enable && !table_config.synonyms.file.empty()) {
      ctx->synonym_dict = std::make_unique<query::SynonymDictionary>();
      auto normalizer = [&ctx](std::string_view text) { return ctx->index->NormalizeText(text); };
      auto load_result = ctx->synonym_dict->LoadFromFile(table_config.synonyms.file, normalizer);
      if (!load_result) {
        mygram::utils::StructuredLog()
            .Event("synonym_load_error")
            .Field("table", table_config.name)
            .Field("file", table_config.synonyms.file)
            .Field("error", load_result.error().message())
            .Error();
      } else {
        mygram::utils::StructuredLog()
            .Event("synonym_dictionary_loaded")
            .Field("table", table_config.name)
            .Field("groups", static_cast<uint64_t>(ctx->synonym_dict->GroupCount()))
            .Field("terms", static_cast<uint64_t>(ctx->synonym_dict->TermCount()))
            .Info();

        // Diagnostic: warn about terms that can't produce any n-grams given
        // the configured ngram_size/kanji_ngram_size. Such terms are silently
        // unreachable at search time, which is surprising for operators.
        const int effective_kanji_size =
            table_config.kanji_ngram_size > 0 ? table_config.kanji_ngram_size : table_config.ngram_size;
        ctx->synonym_dict->ForEachTerm([&](const std::string& term) {
          auto ngrams = mygram::utils::GenerateQueryNgrams(term, table_config.ngram_size, effective_kanji_size,
                                                           table_config.cross_boundary_ngrams);
          if (ngrams.empty()) {
            mygram::utils::StructuredLog()
                .Event("synonym_variant_unreachable")
                .Field("table", table_config.name)
                .Field("term", term)
                .Field("ngram_size", static_cast<int64_t>(table_config.ngram_size))
                .Field("kanji_ngram_size", static_cast<int64_t>(effective_kanji_size))
                .Field("reason", "term_too_short_for_configured_ngram_sizes")
                .Warn();
          }
        });
      }
    }

    table_contexts_[config::QualifiedTableName(table_config)] = std::move(ctx);
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
  if (!RequiresMysqlConnectionForStartup(deps_.config)) {
    mygram::utils::StructuredLog()
        .Event("server_debug")
        .Field("action", "mysql_connection_skipped")
        .Field("reason", "replication_and_auto_snapshot_disabled")
        .Debug();
    return {};
  }

  mygram::utils::StructuredLog().Event("server_debug").Field("action", "initializing_mysql").Debug();

  mysql::Connection::Config mysql_config;
  mysql_config.host = deps_.config.mysql.host;
  mysql_config.port = deps_.config.mysql.port;
  mysql_config.user = deps_.config.mysql.user;
  mysql_config.password = deps_.config.mysql.password;
  mysql_config.database = deps_.config.mysql.database;
  // Use ceiling division to avoid truncating sub-second timeouts to zero
  auto ceil_div_ms = [](int ms) -> int {
    if (ms <= 0)
      return 0;
    return (ms + kMillisecondsPerSecond - 1) / kMillisecondsPerSecond;
  };
  mysql_config.connect_timeout = ceil_div_ms(deps_.config.mysql.connect_timeout_ms);
  mysql_config.read_timeout = ceil_div_ms(deps_.config.mysql.read_timeout_ms);
  mysql_config.write_timeout = ceil_div_ms(deps_.config.mysql.write_timeout_ms);
  mysql_config.session_timeout_sec = deps_.config.mysql.session_timeout_sec;
  mysql_config.ssl_enable = deps_.config.mysql.ssl_enable;
  mysql_config.ssl_ca = deps_.config.mysql.ssl_ca;
  mysql_config.ssl_cert = deps_.config.mysql.ssl_cert;
  mysql_config.ssl_key = deps_.config.mysql.ssl_key;
  mysql_config.ssl_verify_server_cert = deps_.config.mysql.ssl_verify_server_cert;

  mysql_connection_ = std::make_unique<mysql::Connection>(mysql_config);

  auto connect_result = mysql_connection_->Connect("snapshot builder");
  if (!connect_result) {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLConnectionFailed,
                                 "Failed to connect to MySQL: " + connect_result.error().message()));
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

  const bool use_shared_snapshot = deps_.config.replication.enable && deps_.config.tables.size() > 1;
  std::string shared_snapshot_gtid;
  if (use_shared_snapshot) {
    auto start_txn_result = mysql_connection_->ExecuteUpdate("START TRANSACTION WITH CONSISTENT SNAPSHOT");
    if (!start_txn_result) {
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
          mygram::utils::ErrorCode::kStorageSnapshotBuildFailed,
          "Failed to start shared consistent snapshot: " + start_txn_result.error().message()));
    }

    auto gtid_result = mysql_connection_->GetExecutedGTID();
    if (gtid_result) {
      shared_snapshot_gtid = *gtid_result;
      shared_snapshot_gtid.erase(std::remove_if(shared_snapshot_gtid.begin(), shared_snapshot_gtid.end(),
                                                [](unsigned char chr) { return std::isspace(chr); }),
                                 shared_snapshot_gtid.end());
    }
    if (shared_snapshot_gtid.empty()) {
      auto rollback_result = mysql_connection_->ExecuteUpdate("ROLLBACK");
      if (!rollback_result) {
        mygram::utils::StructuredLog()
            .Event("loader_warning")
            .Field("operation", "rollback_shared_snapshot")
            .Field("error", rollback_result.error().message())
            .Warn();
      }
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
          mygram::utils::ErrorCode::kStorageSnapshotBuildFailed,
          "GTID is empty - cannot start multi-table replication from undefined snapshot position"));
    }

    snapshot_gtid_ = shared_snapshot_gtid;
    mygram::utils::StructuredLog()
        .Event("shared_snapshot_gtid_captured")
        .Field("gtid", shared_snapshot_gtid)
        .Field("tables", static_cast<uint64_t>(deps_.config.tables.size()))
        .Info();
  }

  auto rollback_shared_snapshot = [this]() {
    auto rollback_result = mysql_connection_->ExecuteUpdate("ROLLBACK");
    if (!rollback_result) {
      mygram::utils::StructuredLog()
          .Event("loader_warning")
          .Field("operation", "rollback_shared_snapshot")
          .Field("error", rollback_result.error().message())
          .Warn();
    }
  };

  for (const auto& table_config : deps_.config.tables) {
    const auto table_key = config::QualifiedTableName(table_config);
    auto ctx_iter = table_contexts_.find(table_key);
    if (ctx_iter == table_contexts_.end() || ctx_iter->second == nullptr) {
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
          mygram::utils::ErrorCode::kStorageSnapshotBuildFailed, "Table context not found: " + table_key));
    }
    auto& ctx = ctx_iter->second;

    mygram::utils::StructuredLog()
        .Event("snapshot_building")
        .Field("table", table_key)
        .Field("message", "This may take a while for large tables")
        .Info();

    loader::InitialLoader initial_loader(*mysql_connection_, *ctx->index, *ctx->doc_store, table_config,
                                         deps_.config.mysql, deps_.config.build);

    auto progress_callback = [this, &table_config, &initial_loader](const auto& progress) {
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
    };

    auto load_result = use_shared_snapshot
                           ? initial_loader.LoadFromExistingSnapshot(shared_snapshot_gtid, progress_callback)
                           : initial_loader.Load(progress_callback);

    // Check if shutdown was requested
    if (SignalManager::IsShutdownRequested()) {
      if (use_shared_snapshot) {
        rollback_shared_snapshot();
      }
      mygram::utils::StructuredLog()
          .Event("initial_load_cancelled")
          .Field("table", table_config.name)
          .Field("reason", "shutdown_signal")
          .Warn();
      return mygram::utils::MakeUnexpected(
          mygram::utils::MakeError(mygram::utils::ErrorCode::kCancelled, "Initial load cancelled"));
    }

    if (!load_result) {
      if (use_shared_snapshot) {
        rollback_shared_snapshot();
      }
      return mygram::utils::MakeUnexpected(load_result.error());
    }

    mygram::utils::StructuredLog()
        .Event("initial_load_completed")
        .Field("table", table_config.name)
        .Field("documents", initial_loader.GetProcessedRows())
        .Info();

    // Rebuild BM25 corpus statistics from loaded documents
    {
      auto all_doc_ids = ctx->doc_store->GetAllDocIds();
      uint64_t total_length = 0;
      uint64_t doc_count = 0;
      for (auto doc_id : all_doc_ids) {
        auto text_opt = ctx->doc_store->GetNormalizedText(doc_id);
        if (text_opt.has_value() && !text_opt->empty()) {
          total_length += mygram::utils::CountCodePoints(*text_opt);
          ++doc_count;
        }
      }
      ctx->bm25_stats.total_doc_length.store(total_length, std::memory_order_relaxed);
      ctx->bm25_stats.doc_count.store(doc_count, std::memory_order_relaxed);
      mygram::utils::StructuredLog()
          .Event("bm25_stats_initialized")
          .Field("table", table_config.name)
          .Field("doc_count", static_cast<uint64_t>(doc_count))
          .Field("avg_doc_length", ctx->bm25_stats.avg_doc_length())
          .Info();
    }

    // Capture GTID from single-table initial load. Multi-table loads use one
    // shared transaction and set snapshot_gtid_ before loading any table.
    if (!use_shared_snapshot && snapshot_gtid_.empty() && deps_.config.replication.enable) {
      snapshot_gtid_ = initial_loader.GetStartGTID();
      if (!snapshot_gtid_.empty()) {
        mygram::utils::StructuredLog().Event("snapshot_gtid_captured").Field("gtid", snapshot_gtid_).Info();
      }
    }
  }

  if (use_shared_snapshot) {
    auto commit_result = mysql_connection_->ExecuteUpdate("COMMIT");
    if (!commit_result) {
      return mygram::utils::MakeUnexpected(
          mygram::utils::MakeError(mygram::utils::ErrorCode::kStorageSnapshotBuildFailed,
                                   "Failed to commit shared consistent snapshot: " + commit_result.error().message()));
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
  auto start_gtid_result =
      ResolveReplicationStartGtid(start_from, snapshot_gtid_, [this]() { return mysql_connection_->GetLatestGTID(); });
  if (!start_gtid_result) {
    mygram::utils::StructuredLog()
        .Event("latest_gtid_failed")
        .Field("error", start_gtid_result.error().message())
        .Error();
    return mygram::utils::MakeUnexpected(start_gtid_result.error());
  }
  start_gtid = *start_gtid_result;

  if (start_from == "snapshot") {
    // Use GTID captured during snapshot build
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
    mygram::utils::StructuredLog()
        .Event("server_debug")
        .Field("action", "replication_from_latest_gtid")
        .Field("gtid", start_gtid)
        .Debug();
  } else if (start_from.find("gtid=") == 0) {
    // Extract GTID from "gtid=<UUID:txn>" format
    mygram::utils::StructuredLog()
        .Event("server_debug")
        .Field("action", "replication_from_specified_gtid")
        .Field("gtid", start_gtid)
        .Debug();
  }

  // Prepare table_contexts map for BinlogReader
  auto table_contexts_ptrs = BuildTableContextPointerMap();

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
  auto table_contexts_ptrs = BuildTableContextPointerMap();

  // Initialize TCP server
  auto server_config = server::ServerConfig::FromConfig(deps_.config);

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

  // Initialize HTTP server (if enabled)
  if (deps_.config.api.http.enable) {
    auto http_config = server::HttpServerConfig::FromConfig(deps_.config);

#ifdef USE_MYSQL
    http_server_ = std::make_unique<server::HttpServer>(
        http_config, table_contexts_ptrs, &deps_.config, binlog_reader_.get(), tcp_server_->GetCacheManager(),
        tcp_server_->GetDumpLoadInProgressFlag(), tcp_server_->GetMutableStats(), tcp_server_->GetSharedRateLimiter(),
        tcp_server_->GetReplicationPausedForDumpFlag(), tcp_server_->GetSyncManager());
#else
    http_server_ = std::make_unique<server::HttpServer>(
        http_config, table_contexts_ptrs, &deps_.config, nullptr, tcp_server_->GetCacheManager(),
        tcp_server_->GetDumpLoadInProgressFlag(), tcp_server_->GetMutableStats(), tcp_server_->GetSharedRateLimiter(),
        tcp_server_->GetReplicationPausedForDumpFlag());
#endif

    mygram::utils::StructuredLog()
        .Event("http_server_initialized")
        .Field("bind", http_config.bind)
        .Field("port", static_cast<uint64_t>(http_config.port))
        .Info();
  }

  return {};
}

std::unordered_map<std::string, server::TableContext*> ServerOrchestrator::BuildTableContextPointerMap() {
  std::unordered_map<std::string, server::TableContext*> result;
  for (auto& [key, ctx] : table_contexts_) {
    if (ctx == nullptr) {
      continue;
    }
    result[key] = ctx.get();
  }
  return result;
}

void ServerOrchestrator::RegisterRuntimeCallbacks() {
  if (!tcp_server_) {
    return;
  }

  auto* variable_manager = tcp_server_->GetVariableManager();
  if (variable_manager == nullptr) {
    return;
  }

#ifdef USE_MYSQL
  if (mysql_connection_ && binlog_reader_) {
    auto required_tables = CollectRequiredTables(table_contexts_);
    auto reconnection_handler = std::make_shared<MysqlReconnectionHandler>(
        mysql_connection_.get(), binlog_reader_.get(), tcp_server_->GetMysqlReconnectingFlag(),
        std::move(required_tables), tcp_server_->GetDumpSaveInProgressFlag(),
        tcp_server_->GetReplicationPausedForDumpFlag(), tcp_server_->GetReplicationPauseCounter());

    variable_manager->SetMysqlReconnectCallback(
        [handler = reconnection_handler](const std::string& host, int port) { return handler->Reconnect(host, port); });

    mygram::utils::StructuredLog().Event("mysql_reconnection_callback_registered").Info();
  }
#endif

  if (auto* rate_limiter = tcp_server_->GetRateLimiter(); rate_limiter != nullptr) {
    variable_manager->SetRateLimiterCallback([rate_limiter](bool enabled, size_t capacity, size_t refill_rate) {
      rate_limiter->UpdateParameters(capacity, refill_rate);
      rate_limiter->SetEnabled(enabled);
    });
    mygram::utils::StructuredLog().Event("rate_limiter_callback_registered").Info();
  }

  if (auto* cache_manager = tcp_server_->GetCacheManager(); cache_manager != nullptr) {
    variable_manager->SetCacheToggleCallback([cache_manager](bool enabled) -> mygram::utils::Expected<void, Error> {
      if (enabled) {
        if (!cache_manager->Enable()) {
          return mygram::utils::MakeUnexpected(
              mygram::utils::MakeError(mygram::utils::ErrorCode::kInvalidArgument, "Cache cannot be enabled"));
        }
      } else {
        cache_manager->Disable();
      }
      return {};
    });
    mygram::utils::StructuredLog().Event("cache_toggle_callback_registered").Info();
  }

  auto* http_server = http_server_.get();
  variable_manager->AddApiConfigCallback([http_server](int default_limit, int max_query_length) {
    if (http_server != nullptr) {
      http_server->UpdateApiConfig(default_limit, max_query_length);
    }
  });
  mygram::utils::StructuredLog().Event("api_config_callback_registered").Info();
}

}  // namespace mygramdb::app
