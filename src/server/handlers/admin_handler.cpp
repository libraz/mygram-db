/**
 * @file admin_handler.cpp
 * @brief Handler for administrative commands
 */

#include "server/handlers/admin_handler.h"

#include <fcntl.h>
#include <unistd.h>

#include <filesystem>

#include "config/config_help.h"
#include "server/statistics_service.h"
#include "server/table_catalog.h"
#include "utils/safe_path.h"
#include "utils/structured_log.h"

namespace mygramdb::server {

std::string AdminHandler::Handle(const query::Query& query, ConnectionContext& conn_ctx) {
  (void)conn_ctx;  // Unused for admin commands

  switch (query.type) {
    case query::QueryType::INFO: {
      {
        // INFO is the only AdminHandler command that requires table_catalog.
        // CONFIG HELP/SHOW/VERIFY operate purely on the loaded config and intentionally
        // do not need a catalog (verified by ConfigHandlerTest which constructs handler
        // with table_catalog=nullptr). Keep the null-check scoped to commands that
        // dereference the catalog so config commands remain usable in admin-only contexts.
        if (ctx_.table_catalog == nullptr) {
          return ResponseFormatter::FormatError("Table catalog not initialized");
        }
        const auto& tables = ctx_.table_catalog->GetTables();
        // 1. Aggregate metrics (domain layer, pure function)
        auto metrics = StatisticsService::AggregateMetrics(tables);

        // 2. Update stats (domain layer, explicit side effect)
        StatisticsService::UpdateServerStatistics(ctx_.stats, metrics);

        // 3. Format response (presentation layer, pure function)
        return ResponseFormatter::FormatInfoResponse(metrics, ctx_.stats, tables, ctx_.binlog_reader,
                                                     ctx_.cache_manager);
      }
    }

    case query::QueryType::CONFIG_HELP:
      return HandleConfigHelp(query.filepath);

    case query::QueryType::CONFIG_SHOW:
      return HandleConfigShow(query.filepath);

    case query::QueryType::CONFIG_VERIFY:
      return HandleConfigVerify(query.filepath);

    default:
      return ResponseFormatter::FormatError("Invalid query type for AdminHandler");
  }
}

std::string AdminHandler::HandleConfigHelp(const std::string& path) {
  auto explorer_result = config::ConfigSchemaExplorer::Create();
  if (!explorer_result) {
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "config_help")
        .FieldError(explorer_result.error())
        .Error();
    return ResponseFormatter::FormatError(std::string("CONFIG HELP failed: ") + explorer_result.error().message());
  }
  auto& explorer = *explorer_result;

  if (path.empty()) {
    // Show top-level sections
    auto paths = explorer.ListPaths("");
    std::string result = config::ConfigSchemaExplorer::FormatPathList(paths, "");
    // Ensure result ends with \r\n for multi-line end-of-response detection
    if (result.size() < 2 || result[result.size() - 2] != '\r' || result[result.size() - 1] != '\n') {
      result.append("\r\n");
    }
    return ResponseFormatter::FormatOk() + "\r\n" + result;
  }

  // Show help for specific path
  auto help_info = explorer.GetHelp(path);
  if (!help_info.has_value()) {
    return ResponseFormatter::FormatError("Configuration path not found: " + path);
  }

  std::string result = config::ConfigSchemaExplorer::FormatHelp(help_info.value());
  // Ensure result ends with \r\n for multi-line end-of-response detection
  if (result.size() < 2 || result[result.size() - 2] != '\r' || result[result.size() - 1] != '\n') {
    result.append("\r\n");
  }
  return ResponseFormatter::FormatOk() + "\r\n" + result;
}

std::string AdminHandler::HandleConfigShow(const std::string& path) {
  if (ctx_.full_config == nullptr) {
    mygram::utils::StructuredLog()
        .Event("server_warning")
        .Field("operation", "config_show")
        .Field("reason", "config_not_available")
        .Warn();
    return ResponseFormatter::FormatError("Server configuration is not available");
  }

  auto format_result = config::FormatConfigForDisplay(*ctx_.full_config, path);
  if (!format_result) {
    mygram::utils::StructuredLog()
        .Event("server_error")
        .Field("operation", "config_show")
        .FieldError(format_result.error())
        .Error();
    return ResponseFormatter::FormatError(std::string("CONFIG SHOW failed: ") + format_result.error().message());
  }

  std::string result = std::move(*format_result);
  // Ensure result ends with \r\n so the server's appended \r\n produces
  // the \r\n\r\n end-of-response marker that clients use to detect
  // multi-line response completion.
  if (result.size() < 2 || result[result.size() - 2] != '\r' || result[result.size() - 1] != '\n') {
    result.append("\r\n");
  }
  return ResponseFormatter::FormatOk() + "\r\n" + result;
}

std::string AdminHandler::HandleConfigVerify(const std::string& filepath) {
  if (filepath.empty()) {
    return ResponseFormatter::FormatError("CONFIG VERIFY requires a filepath");
  }

  // Security: restrict to relative paths to avoid reading arbitrary filesystem
  // paths through CONFIG VERIFY. The check is performed before symlink
  // resolution so that the user-visible argument is what we evaluate.
  if (!filepath.empty() && filepath[0] == '/') {
    return ResponseFormatter::FormatError("CONFIG VERIFY: absolute paths not allowed");
  }
  // Reject any traversal sequence (..) explicitly — even though
  // ResolveSafePath would later catch escapes via lexically_relative, the
  // historical error message "path traversal (..) not allowed" is part of the
  // public CLI surface and must be preserved.
  if (filepath.find("..") != std::string::npos) {
    return ResponseFormatter::FormatError("CONFIG VERIFY: path traversal (..) not allowed");
  }

  // Resolve via shared safe-path utility. CONFIG VERIFY uses the current
  // working directory as the base directory (matching the previous
  // behaviour of passing the relative path directly to LoadConfig).
  std::error_code ec;
  std::string base_dir = std::filesystem::current_path(ec).string();
  if (ec) {
    return ResponseFormatter::FormatError(std::string("CONFIG VERIFY: cannot resolve current directory: ") +
                                          ec.message());
  }

  auto resolved = mygram::utils::ResolveSafePath(filepath, base_dir, {".yaml", ".yml"});
  if (!resolved) {
    const auto& err = resolved.error();
    // Surface a more user-friendly error for the most common failure modes.
    if (err.message().find("Disallowed file extension") != std::string::npos) {
      return ResponseFormatter::FormatError("CONFIG VERIFY only accepts .yaml or .yml files");
    }
    if (err.message().find("must be within base directory") != std::string::npos) {
      return ResponseFormatter::FormatError("CONFIG VERIFY: path traversal (..) not allowed");
    }
    return ResponseFormatter::FormatError("CONFIG VERIFY: file not found: " + filepath);
  }
  std::string canonical_path = std::move(*resolved);

  // ResolveSafePath canonicalizes via std::filesystem::canonical which
  // follows symlinks, so a symlinked file resolves to its real target inside
  // base_dir. To preserve the historical "symbolic links are not allowed"
  // semantic and reject any symlink (file or parent directory), compare the
  // resolved canonical path against the path obtained by joining base_dir
  // with the input via lexically_normal — they differ when any component
  // along the way was a symlink.
  std::filesystem::path joined = std::filesystem::path(base_dir) / std::filesystem::path(filepath);
  std::filesystem::path joined_normal = joined.lexically_normal();
  if (std::filesystem::exists(joined_normal, ec) && std::filesystem::path(canonical_path) != joined_normal) {
    return ResponseFormatter::FormatError("CONFIG VERIFY: symbolic links are not allowed");
  }

  // Verify the resolved target is a regular file (not a device, FIFO, etc.).
  if (!std::filesystem::exists(canonical_path, ec)) {
    return ResponseFormatter::FormatError("CONFIG VERIFY: file not found: " + filepath);
  }
  if (!std::filesystem::is_regular_file(canonical_path, ec)) {
    return ResponseFormatter::FormatError("CONFIG VERIFY: not a regular file");
  }

  // TOCTOU mitigation: between the canonical()/exists()/is_regular_file()
  // checks above and LoadConfig()'s file-open below, an attacker with
  // write access to the directory could swap canonical_path for a symlink.
  // Probe the resolved path with O_NOFOLLOW to ensure it is still NOT a
  // symlink at the moment of file-open. O_NOFOLLOW is portable to both
  // Linux and macOS and causes open() to fail with ELOOP if the final
  // path component is a symlink.
  //
  // Residual risk: between this open() and LoadConfig()'s open(), a path
  // *component directory* (not the final symlink) could still be replaced.
  // This is a much narrower window and is acceptable for an operator-facing
  // CONFIG VERIFY command; full mitigation would require LoadConfig to
  // accept an open fd, which is out of scope.
  {
    // POSIX open() is variadic; the vararg warning here is unavoidable.
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
    int probe_fd = ::open(canonical_path.c_str(), O_RDONLY | O_NOFOLLOW | O_CLOEXEC);
    if (probe_fd < 0) {
      // ELOOP indicates the path is now a symlink (raced); other errors
      // (ENOENT, EACCES, ...) also defeat verification, so reject.
      return ResponseFormatter::FormatError("CONFIG VERIFY: symbolic links are not allowed");
    }
    ::close(probe_fd);
  }

  // Try to load and validate the configuration file
  auto config_result = config::LoadConfig(canonical_path);
  if (!config_result) {
    // Logged at WARN: this is a client-input validation failure (user supplied
    // a bad YAML), not a server-side system error, so it should not raise
    // operator alerts. Event renamed from "server_error" to "config_verify_failed"
    // for clearer attribution.
    mygram::utils::StructuredLog()
        .Event("config_verify_failed")
        .Field("operation", "config_verify")
        .Field("filepath", filepath)
        .FieldError(config_result.error())
        .Warn();
    return ResponseFormatter::FormatError(std::string("Configuration validation failed:\r\n  ") +
                                          config_result.error().message());
  }

  config::Config test_config = *config_result;

  // Build summary information
  std::ostringstream summary;
  summary << "Configuration is valid\r\n";
  summary << "  Tables: " << test_config.tables.size();
  if (!test_config.tables.empty()) {
    summary << " (";
    for (size_t i = 0; i < test_config.tables.size(); ++i) {
      if (i > 0) {
        summary << ", ";
      }
      summary << test_config.tables[i].name;
    }
    summary << ")";
  }
  summary << "\r\n";
  // Intentionally omit mysql.user and mysql.password to prevent credential
  // disclosure via TCP. CONFIG VERIFY is accessible to all authenticated clients.
  summary << "  MySQL: " << test_config.mysql.host << ":" << test_config.mysql.port << "\r\n";

  return ResponseFormatter::FormatOk() + "\r\n" + summary.str();
}

}  // namespace mygramdb::server
