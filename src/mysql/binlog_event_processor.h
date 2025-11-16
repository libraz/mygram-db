/**
 * @file binlog_event_processor.h
 * @brief Binlog event processor for applying changes to index and document store
 */

#pragma once

#ifdef USE_MYSQL

#include <unordered_map>

#include "config/config.h"
#include "index/index.h"
#include "mysql/binlog_reader.h"
#include "storage/document_store.h"

// Forward declaration
namespace mygramdb::server {
class ServerStats;
}

namespace mygramdb::mysql {

/**
 * @brief Binlog event processor
 *
 * Processes parsed binlog events and applies changes to index and document store
 */
class BinlogEventProcessor {
 public:
  /**
   * @brief Process single binlog event
   * @param event Parsed binlog event
   * @param index Index to update
   * @param doc_store Document store to update
   * @param table_config Table configuration
   * @param stats Optional server statistics tracker
   * @return true if successfully processed
   */
  static bool ProcessEvent(const BinlogEvent& event, index::Index& index, storage::DocumentStore& doc_store,
                           const config::TableConfig& table_config, server::ServerStats* stats = nullptr);
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
