/**
 * @file dump_handler.h
 * @brief Handler for dump-related commands (DUMP)
 */

#pragma once

#include "server/handlers/command_handler.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::server {

/**
 * @brief Handler for dump-related commands
 *
 * Handles DUMP commands for dump management.
 */
class DumpHandler : public CommandHandler {
 public:
  explicit DumpHandler(HandlerContext& ctx) : CommandHandler(ctx) {}

  std::string Handle(const query::Query& query, ConnectionContext& conn_ctx) override;

 private:
  /**
   * @brief Handle DUMP_SAVE command
   */
  std::string HandleDumpSave(const query::Query& query);

  /**
   * @brief Handle DUMP_LOAD command
   */
  std::string HandleDumpLoad(const query::Query& query);

  /**
   * @brief Handle DUMP_VERIFY command
   */
  std::string HandleDumpVerify(const query::Query& query);

  /**
   * @brief Handle DUMP_INFO command
   */
  std::string HandleDumpInfo(const query::Query& query);

  /**
   * @brief Handle DUMP_STATUS command
   */
  std::string HandleDumpStatus();

  /**
   * @brief Background worker for async DUMP SAVE
   * @param filepath Path to save dump file
   * @return true if the dump was saved successfully, false otherwise
   */
  bool DumpSaveWorker(const std::string& filepath);

  /**
   * @brief Resolve and validate a dump file path
   *
   * Prepends dump_dir for relative paths and validates the resolved path
   * is within the dump directory (prevents path traversal).
   *
   * @param input Raw filepath from the query
   * @param dump_dir Base dump directory
   * @return Resolved canonical filepath, or error on traversal/invalid path
   */
  static mygram::utils::Expected<std::string, mygram::utils::Error> ResolveDumpPath(const std::string& input,
                                                                                    const std::string& dump_dir);
};

}  // namespace mygramdb::server
