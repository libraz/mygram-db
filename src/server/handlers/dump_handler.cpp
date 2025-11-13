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
#include <fstream>
#include <sstream>

#include "storage/dump_format_v1.h"

#ifdef USE_MYSQL
#include "mysql/binlog_reader.h"
#endif

namespace mygramdb::server {

namespace {
constexpr int kIpAddressBufferSize = 256;
constexpr size_t kGtidPrefixLength = 7;  // Length of "gtid":"
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
  // Determine filepath
  std::string filepath;
  if (!query.filepath.empty()) {
    filepath = query.filepath;
    if (filepath[0] != '/') {
      filepath = ctx_.dump_dir + "/" + filepath;
    }
  } else {
    auto now = std::time(nullptr);
    std::array<char, kIpAddressBufferSize> buf{};
    std::strftime(buf.data(), buf.size(), "dump_%Y%m%d_%H%M%S.dmp", std::localtime(&now));
    filepath = ctx_.dump_dir + "/" + std::string(buf.data());
  }

  spdlog::info("Attempting to save dump to: {}", filepath);

  // Get current GTID
  std::string gtid;
#ifdef USE_MYSQL
  if (ctx_.binlog_reader != nullptr) {
    auto* reader = static_cast<mysql::BinlogReader*>(ctx_.binlog_reader);
    gtid = reader->GetCurrentGTID();
  }
#endif

  // Set read-only mode
  ctx_.read_only = true;

  // Convert table_contexts to format expected by dump_v1::WriteDumpV1
  std::unordered_map<std::string, std::pair<index::Index*, storage::DocumentStore*>> converted_contexts;
  for (const auto& [table_name, table_ctx] : ctx_.table_contexts) {
    converted_contexts[table_name] = {table_ctx->index.get(), table_ctx->doc_store.get()};
  }

  // Call dump_v1 API
  bool success = storage::dump_v1::WriteDumpV1(filepath, gtid, *ctx_.full_config, converted_contexts);

  // Clear read-only mode
  ctx_.read_only = false;

  if (success) {
    spdlog::info("Successfully saved dump to: {}", filepath);
    return ResponseFormatter::FormatSaveResponse(filepath);
  }

  std::string error_msg = "Failed to save dump to: " + filepath;
  spdlog::error("{}", error_msg);
  return ResponseFormatter::FormatError(error_msg);
}

std::string DumpHandler::HandleDumpLoad(const query::Query& query) {
  std::string filepath;
  if (!query.filepath.empty()) {
    filepath = query.filepath;
    if (filepath[0] != '/') {
      filepath = ctx_.dump_dir + "/" + filepath;
    }
  } else {
    return ResponseFormatter::FormatError("DUMP LOAD requires a filepath");
  }

  spdlog::info("Attempting to load dump from: {}", filepath);

  // Set loading mode
  ctx_.loading = true;

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
  bool success = storage::dump_v1::ReadDumpV1(filepath, gtid, loaded_config, converted_contexts, nullptr, nullptr,
                                              &integrity_error);

  // Clear loading mode
  ctx_.loading = false;

  if (success) {
    spdlog::info("Successfully loaded dump from: {} (GTID: {})", filepath, gtid);
    return ResponseFormatter::FormatLoadResponse(filepath);
  }

  std::string error_msg = "Failed to load dump from: " + filepath;
  if (!integrity_error.message.empty()) {
    error_msg += " (" + integrity_error.message + ")";
  }
  spdlog::error("{}", error_msg);
  return ResponseFormatter::FormatError(error_msg);
}

std::string DumpHandler::HandleDumpVerify(const query::Query& query) {
  std::string filepath;
  if (!query.filepath.empty()) {
    filepath = query.filepath;
    if (filepath[0] != '/') {
      filepath = ctx_.dump_dir + "/" + filepath;
    }
  } else {
    return ResponseFormatter::FormatError("DUMP VERIFY requires a filepath");
  }

  spdlog::info("Verifying dump: {}", filepath);

  storage::dump_format::IntegrityError integrity_error;
  bool success = storage::dump_v1::VerifyDumpIntegrity(filepath, integrity_error);

  if (success) {
    spdlog::info("Dump verification succeeded: {}", filepath);
    return "OK DUMP_VERIFIED " + filepath;
  }

  std::string error_msg = "Dump verification failed: " + filepath;
  if (!integrity_error.message.empty()) {
    error_msg += " (" + integrity_error.message + ")";
  }
  spdlog::error("{}", error_msg);
  return ResponseFormatter::FormatError(error_msg);
}

std::string DumpHandler::HandleDumpInfo(const query::Query& query) {
  std::string filepath;
  if (!query.filepath.empty()) {
    filepath = query.filepath;
    if (filepath[0] != '/') {
      filepath = ctx_.dump_dir + "/" + filepath;
    }
  } else {
    return ResponseFormatter::FormatError("DUMP INFO requires a filepath");
  }

  spdlog::info("Reading dump info: {}", filepath);

  storage::dump_v1::DumpInfo info;
  bool success = storage::dump_v1::GetDumpInfo(filepath, info);

  if (!success) {
    return ResponseFormatter::FormatError("Failed to read dump info from: " + filepath);
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
