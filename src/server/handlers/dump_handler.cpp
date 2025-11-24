/**
 * @file dump_handler.cpp
 * @brief Handler for dump-related commands
 */

#include "server/handlers/dump_handler.h"

#include <spdlog/spdlog.h>
#include <sys/stat.h>

#include <array>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <sstream>

#include "server/sync_operation_manager.h"
#include "storage/dump_format_v1.h"
#include "utils/structured_log.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#endif

namespace mygramdb::server {

namespace {
constexpr int kIpAddressBufferSize = 256;
constexpr size_t kGtidPrefixLength = 7;  // Length of "gtid":"

/**
 * @brief RAII guard for atomic boolean flags
 *
 * Automatically sets flag to true on construction and resets to false on destruction.
 * Exception-safe: ensures flag is always reset even if exceptions are thrown.
 */
class FlagGuard {
 public:
  explicit FlagGuard(std::atomic<bool>& flag) : flag_(flag) { flag_ = true; }
  ~FlagGuard() { flag_ = false; }

  // Non-copyable and non-movable
  FlagGuard(const FlagGuard&) = delete;
  FlagGuard& operator=(const FlagGuard&) = delete;
  FlagGuard(FlagGuard&&) = delete;
  FlagGuard& operator=(FlagGuard&&) = delete;

 private:
  std::atomic<bool>& flag_;
};

}  // namespace

std::string DumpHandler::Handle(const query::Query& query, ConnectionContext& conn_ctx) {
  (void)conn_ctx;  // Unused for dump commands

  switch (query.type) {
    case query::QueryType::DUMP_SAVE:
      return HandleDumpSave(query);
    case query::QueryType::DUMP_LOAD:
      return HandleDumpLoad(query);
    case query::QueryType::DUMP_VERIFY:
      return HandleDumpVerify(query);
    case query::QueryType::DUMP_INFO:
      return HandleDumpInfo(query);
    default:
      return ResponseFormatter::FormatError("Invalid query type for DumpHandler");
  }
}

std::string DumpHandler::HandleDumpSave(const query::Query& query) {
#ifdef USE_MYSQL
  // Check if GTID is set (required for consistent dump)
  std::string current_gtid;
  bool replication_was_running = false;
  if (ctx_.binlog_reader != nullptr) {
    auto* reader = static_cast<mysql::BinlogReader*>(ctx_.binlog_reader);
    current_gtid = reader->GetCurrentGTID();
    if (current_gtid.empty()) {
      return ResponseFormatter::FormatError(
          "Cannot save dump without GTID position. "
          "Please run SYNC command first to establish initial position.");
    }

    // Check if replication is running
    replication_was_running = reader->IsRunning();
  }

  // Block if any table is currently syncing
  if (ctx_.sync_manager != nullptr) {
    std::vector<std::string> syncing_tables;
    if (ctx_.sync_manager->GetSyncingTablesIfAny(syncing_tables)) {
      std::ostringstream oss;
      oss << "Cannot save dump while SYNC is in progress for tables:";
      for (const auto& table : syncing_tables) {
        oss << " " << table;
      }
      return ResponseFormatter::FormatError(oss.str());
    }
  }
#endif

  // Check if DUMP LOAD is in progress (block DUMP SAVE)
  if (ctx_.loading.load()) {
    return ResponseFormatter::FormatError(
        "Cannot save dump while DUMP LOAD is in progress. "
        "Please wait for load to complete.");
  }

  // Check if another DUMP SAVE is in progress (block concurrent saves)
  if (ctx_.read_only.load()) {
    return ResponseFormatter::FormatError(
        "Cannot save dump while another DUMP SAVE is in progress. "
        "Please wait for current save to complete.");
  }

#ifdef USE_MYSQL
  // Stop replication before DUMP SAVE (if running)
  if (replication_was_running && ctx_.binlog_reader != nullptr) {
    auto* reader = static_cast<mysql::BinlogReader*>(ctx_.binlog_reader);
    reader->Stop();
    ctx_.replication_paused_for_dump = true;
    spdlog::info("Stopped replication before DUMP SAVE (will auto-restart after completion)");
    mygram::utils::StructuredLog()
        .Event("replication_paused")
        .Field("operation", "dump_save")
        .Field("reason", "automatic_pause_for_consistency")
        .Info();
  }
#endif

  // Determine filepath
  std::string filepath;
  if (!query.filepath.empty()) {
    filepath = query.filepath;
    if (filepath[0] != '/') {
      filepath = ctx_.dump_dir + "/" + filepath;
    }
    // Canonicalize path and validate it's within dump_dir
    try {
      std::filesystem::path canonical = std::filesystem::weakly_canonical(filepath);
      std::filesystem::path dump_canonical = std::filesystem::weakly_canonical(ctx_.dump_dir);
      auto rel = canonical.lexically_relative(dump_canonical);
      if (rel.empty() || rel.string().substr(0, 2) == "..") {
        return ResponseFormatter::FormatError("Invalid filepath: path traversal detected");
      }
    } catch (const std::exception& e) {
      return ResponseFormatter::FormatError(std::string("Invalid filepath: ") + e.what());
    }
  } else {
    auto now = std::time(nullptr);
    std::tm tm_buf{};
    localtime_r(&now, &tm_buf);  // Thread-safe version of localtime
    std::array<char, kIpAddressBufferSize> buf{};
    std::strftime(buf.data(), buf.size(), "dump_%Y%m%d_%H%M%S.dmp", &tm_buf);
    filepath = ctx_.dump_dir + "/" + std::string(buf.data());
  }

  spdlog::info("Attempting to save dump to: {}", filepath);

  // Check if full_config is available
  if (ctx_.full_config == nullptr) {
    std::string error_msg = "Cannot save dump: server configuration is not available";
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "dump_save")
        .Field("reason", "config_not_available")
        .Error();
    return ResponseFormatter::FormatError(error_msg);
  }

  // Get current GTID
  std::string gtid;
#ifdef USE_MYSQL
  if (ctx_.binlog_reader != nullptr) {
    auto* reader = static_cast<mysql::BinlogReader*>(ctx_.binlog_reader);
    gtid = reader->GetCurrentGTID();
  }
#endif

  // Set read-only mode (RAII guard ensures it's cleared even on exceptions)
  FlagGuard read_only_guard(ctx_.read_only);

  // Convert table_contexts to format expected by dump_v1::WriteDumpV1
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> converted_contexts;
  for (const auto& [table_name, table_ctx] : ctx_.table_contexts) {
    converted_contexts[table_name] = {table_ctx->index.get(), table_ctx->doc_store.get()};
  }

  // Call dump_v1 API
  auto result = storage::dump_v1::WriteDumpV1(filepath, gtid, *ctx_.full_config, converted_contexts);

#ifdef USE_MYSQL
  // Auto-restart replication after DUMP SAVE (regardless of success/failure)
  if (replication_was_running && ctx_.binlog_reader != nullptr) {
    auto* reader = static_cast<mysql::BinlogReader*>(ctx_.binlog_reader);
    ctx_.replication_paused_for_dump = false;

    if (reader->Start()) {
      spdlog::info("Auto-restarted replication after DUMP SAVE");
      mygram::utils::StructuredLog()
          .Event("replication_resumed")
          .Field("operation", "dump_save")
          .Field("reason", "automatic_restart_after_completion")
          .Info();
    } else {
      std::string replication_error = reader->GetLastError();
      mygram::utils::StructuredLog()
          .Event("replication_restart_failed")
          .Field("operation", "dump_save")
          .Field("error", replication_error)
          .Error();
      // Don't fail DUMP SAVE due to replication restart failure
      // User can manually restart replication
    }
  }
#endif

  if (result) {
    spdlog::info("Successfully saved dump to: {}", filepath);
    return ResponseFormatter::FormatSaveResponse(filepath);
  }

  std::string error_msg = "Failed to save dump to " + filepath + ": " + result.error().message();
  mygram::utils::StructuredLog()
      .Event("server_error")
      .Field("operation", "dump_save")
      .Field("filepath", filepath)
      .Field("error", result.error().message())
      .Error();
  return ResponseFormatter::FormatError(error_msg);
}

std::string DumpHandler::HandleDumpLoad(const query::Query& query) {
#ifdef USE_MYSQL
  // Check if replication is running (need to stop it before DUMP LOAD)
  bool replication_was_running = false;
  if (ctx_.binlog_reader != nullptr) {
    auto* reader = static_cast<mysql::BinlogReader*>(ctx_.binlog_reader);
    replication_was_running = reader->IsRunning();
  }

  // Check if any table is currently syncing (block DUMP LOAD)
  if (ctx_.sync_manager != nullptr) {
    std::vector<std::string> syncing_tables;
    if (ctx_.sync_manager->GetSyncingTablesIfAny(syncing_tables)) {
      std::ostringstream oss;
      oss << "Cannot load dump while SYNC is in progress for tables:";
      for (const auto& table : syncing_tables) {
        oss << " " << table;
      }
      return ResponseFormatter::FormatError(oss.str());
    }
  }
#endif

  // Check if OPTIMIZE is in progress (block DUMP LOAD)
  if (ctx_.optimization_in_progress.load()) {
    return ResponseFormatter::FormatError(
        "Cannot load dump while OPTIMIZE is in progress. "
        "Please wait for optimization to complete.");
  }

  // Check if DUMP SAVE is in progress (block DUMP LOAD)
  if (ctx_.read_only.load()) {
    return ResponseFormatter::FormatError(
        "Cannot load dump while DUMP SAVE is in progress. "
        "Please wait for save to complete.");
  }

  // Check if another DUMP LOAD is in progress (block concurrent loads)
  if (ctx_.loading.load()) {
    return ResponseFormatter::FormatError(
        "Cannot load dump while another DUMP LOAD is in progress. "
        "Please wait for current load to complete.");
  }

#ifdef USE_MYSQL
  // Stop replication before DUMP LOAD (if running)
  if (replication_was_running && ctx_.binlog_reader != nullptr) {
    auto* reader = static_cast<mysql::BinlogReader*>(ctx_.binlog_reader);
    reader->Stop();
    ctx_.replication_paused_for_dump = true;
    spdlog::info("Stopped replication before DUMP LOAD (will auto-restart after completion)");
    mygram::utils::StructuredLog()
        .Event("replication_paused")
        .Field("operation", "dump_load")
        .Field("reason", "automatic_pause_for_consistency")
        .Info();
  }
#endif

  std::string filepath;
  if (!query.filepath.empty()) {
    filepath = query.filepath;
    if (filepath[0] != '/') {
      filepath = ctx_.dump_dir + "/" + filepath;
    }
    // Canonicalize path and validate it's within dump_dir
    try {
      std::filesystem::path canonical = std::filesystem::weakly_canonical(filepath);
      std::filesystem::path dump_canonical = std::filesystem::weakly_canonical(ctx_.dump_dir);
      auto rel = canonical.lexically_relative(dump_canonical);
      if (rel.empty() || rel.string().substr(0, 2) == "..") {
        return ResponseFormatter::FormatError("Invalid filepath: path traversal detected");
      }
    } catch (const std::exception& e) {
      return ResponseFormatter::FormatError(std::string("Invalid filepath: ") + e.what());
    }
  } else {
    return ResponseFormatter::FormatError("DUMP LOAD requires a filepath");
  }

  spdlog::info("Attempting to load dump from: {}", filepath);

  // Set loading mode (RAII guard ensures it's cleared even on exceptions)
  FlagGuard loading_guard(ctx_.loading);

  // Convert table_contexts to format expected by dump_v1::ReadDumpV1
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> converted_contexts;
  for (const auto& [table_name, table_ctx] : ctx_.table_contexts) {
    converted_contexts[table_name] = {table_ctx->index.get(), table_ctx->doc_store.get()};
  }

  // Variables to receive loaded data
  std::string gtid;
  config::Config loaded_config;
  storage::dump_format::IntegrityError integrity_error;

  // Call dump_v1 API
  auto result = storage::dump_v1::ReadDumpV1(filepath, gtid, loaded_config, converted_contexts, nullptr, nullptr,
                                             &integrity_error);

#ifdef USE_MYSQL
  // Auto-restart replication after DUMP LOAD (regardless of success/failure)
  if (replication_was_running && ctx_.binlog_reader != nullptr) {
    auto* reader = static_cast<mysql::BinlogReader*>(ctx_.binlog_reader);
    ctx_.replication_paused_for_dump = false;

    // Update GTID from loaded dump (if load was successful and GTID is available)
    if (result && !gtid.empty()) {
      reader->SetCurrentGTID(gtid);
      spdlog::info("Updated replication GTID to loaded position: {}", gtid);
    }

    if (reader->Start()) {
      spdlog::info("Auto-restarted replication after DUMP LOAD");
      mygram::utils::StructuredLog()
          .Event("replication_resumed")
          .Field("operation", "dump_load")
          .Field("reason", "automatic_restart_after_completion")
          .Field("gtid", gtid)
          .Info();
    } else {
      std::string replication_error = reader->GetLastError();
      mygram::utils::StructuredLog()
          .Event("replication_restart_failed")
          .Field("operation", "dump_load")
          .Field("error", replication_error)
          .Error();
      // Don't fail DUMP LOAD due to replication restart failure
      // User can manually restart replication
    }
  }
#endif

  if (result) {
    spdlog::info("Successfully loaded dump from: {} (GTID: {})", filepath, gtid);
    return ResponseFormatter::FormatLoadResponse(filepath);
  }

  std::string error_msg = "Failed to load dump from " + filepath + ": " + result.error().message();
  if (!integrity_error.message.empty()) {
    error_msg += " (" + integrity_error.message + ")";
  }
  mygram::utils::StructuredLog()
      .Event("server_error")
      .Field("operation", "dump_load")
      .Field("filepath", filepath)
      .Field("error", error_msg)
      .Error();
  return ResponseFormatter::FormatError(error_msg);
}

std::string DumpHandler::HandleDumpVerify(const query::Query& query) {
  std::string filepath;
  if (!query.filepath.empty()) {
    filepath = query.filepath;
    if (filepath[0] != '/') {
      filepath = ctx_.dump_dir + "/" + filepath;
    }
    // Canonicalize path and validate it's within dump_dir
    try {
      std::filesystem::path canonical = std::filesystem::weakly_canonical(filepath);
      std::filesystem::path dump_canonical = std::filesystem::weakly_canonical(ctx_.dump_dir);
      auto rel = canonical.lexically_relative(dump_canonical);
      if (rel.empty() || rel.string().substr(0, 2) == "..") {
        return ResponseFormatter::FormatError("Invalid filepath: path traversal detected");
      }
    } catch (const std::exception& e) {
      return ResponseFormatter::FormatError(std::string("Invalid filepath: ") + e.what());
    }
  } else {
    return ResponseFormatter::FormatError("DUMP VERIFY requires a filepath");
  }

  spdlog::info("Verifying dump: {}", filepath);

  storage::dump_format::IntegrityError integrity_error;
  auto result = storage::dump_v1::VerifyDumpIntegrity(filepath, integrity_error);

  if (result) {
    spdlog::info("Dump verification succeeded: {}", filepath);
    return "OK DUMP_VERIFIED " + filepath;
  }

  std::string error_msg = "Dump verification failed for " + filepath + ": " + result.error().message();
  if (!integrity_error.message.empty()) {
    error_msg += " (" + integrity_error.message + ")";
  }
  mygram::utils::StructuredLog()
      .Event("server_error")
      .Field("operation", "dump_verify")
      .Field("filepath", filepath)
      .Field("error", error_msg)
      .Error();
  return ResponseFormatter::FormatError(error_msg);
}

std::string DumpHandler::HandleDumpInfo(const query::Query& query) {
  std::string filepath;
  if (!query.filepath.empty()) {
    filepath = query.filepath;
    if (filepath[0] != '/') {
      filepath = ctx_.dump_dir + "/" + filepath;
    }
    // Canonicalize path and validate it's within dump_dir
    try {
      std::filesystem::path canonical = std::filesystem::weakly_canonical(filepath);
      std::filesystem::path dump_canonical = std::filesystem::weakly_canonical(ctx_.dump_dir);
      auto rel = canonical.lexically_relative(dump_canonical);
      if (rel.empty() || rel.string().substr(0, 2) == "..") {
        return ResponseFormatter::FormatError("Invalid filepath: path traversal detected");
      }
    } catch (const std::exception& e) {
      return ResponseFormatter::FormatError(std::string("Invalid filepath: ") + e.what());
    }
  } else {
    return ResponseFormatter::FormatError("DUMP INFO requires a filepath");
  }

  spdlog::info("Reading dump info: {}", filepath);

  storage::dump_v1::DumpInfo info;
  auto info_result = storage::dump_v1::GetDumpInfo(filepath, info);

  if (!info_result) {
    return ResponseFormatter::FormatError("Failed to read dump info from " + filepath + ": " +
                                          info_result.error().message());
  }

  std::ostringstream result;
  result << "OK DUMP_INFO " << filepath << "\r\n";
  result << "version: " << info.version << "\r\n";
  result << "gtid: " << info.gtid << "\r\n";
  result << "tables: " << info.table_count << "\r\n";
  result << "flags: " << info.flags << "\r\n";
  result << "file_size: " << info.file_size << "\r\n";
  result << "timestamp: " << info.timestamp << "\r\n";
  result << "has_statistics: " << (info.has_statistics ? "true" : "false") << "\r\n";
  result << "END";

  return result.str();
}

}  // namespace mygramdb::server
