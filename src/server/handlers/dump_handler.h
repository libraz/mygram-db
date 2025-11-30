/**
 * @file dump_handler.h
 * @brief Handler for dump-related commands (DUMP)
 */

#pragma once

#include "server/handlers/command_handler.h"

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
   */
  void DumpSaveWorker(const std::string& filepath);
};

}  // namespace mygramdb::server
