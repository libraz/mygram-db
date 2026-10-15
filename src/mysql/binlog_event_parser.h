/**
 * @file binlog_event_parser.h
 * @brief MySQL binlog event parsing utilities
 */

#pragma once

#ifdef USE_MYSQL

#include <functional>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "mysql/binlog_reader.h"
#include "mysql/ddl_classifier.h"
#include "mysql/rows_parser.h"
#include "mysql/table_metadata.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql {

/**
 * @brief Decoded QUERY_EVENT: default database, statement text, and the lexing mode it was logged under
 */
struct QueryEventData {
  std::string database;
  std::string query;
  QueryLexOptions lex_options;  ///< sql_mode bits the statement was logged under that change its quoting
  bool sql_mode_known = true;   ///< false when the status variables could not be walked to Q_SQL_MODE_CODE
};

/**
 * @brief Binlog event parser
 *
 * Parses raw MySQL binlog event buffers into structured BinlogEvent objects
 */
class BinlogEventParser {
 public:
  enum class QueryTransactionBoundary : uint8_t { kNone, kBegin, kEnd, kUnsupportedXa };

  /**
   * @brief Parse binlog event buffer and create BinlogEvent(s)
   * @param buffer Raw binlog event data
   * @param length Length of the buffer
   * @param current_gtid Current GTID for this event
   * @param table_metadata_cache Cache for table metadata
   * @param table_contexts Map of table name to TableContext (multi-table mode)
   * @param table_config Single table config (single-table mode, can be nullptr)
   * @param multi_table_mode Whether operating in multi-table mode
   * @param datetime_timezone Timezone offset for DATETIME interpretation (e.g., "+09:00")
   * @param table_names_case_insensitive Whether the source runs with lower_case_table_names != 0, so
   *        database and table names match ASCII case-insensitively instead of byte-exactly
   * @return Vector of BinlogEvents (empty if no events parsed, multiple for batch operations)
   */
  static std::vector<BinlogEvent> ParseBinlogEvent(
      const unsigned char* buffer, unsigned long length, const std::string& current_gtid,
      const TableMetadataCache& table_metadata_cache,
      const std::unordered_map<std::string, server::TableContext*>& table_contexts,
      const config::TableConfig* table_config, bool multi_table_mode, const std::string& datetime_timezone = "+00:00",
      bool table_names_case_insensitive = false);

  /**
   * @brief Extract GTID from GTID_LOG_EVENT
   * @param buffer Event buffer
   * @param length Buffer length
   * @return GTID string if found
   */
  static std::optional<std::string> ExtractGTID(const unsigned char* buffer, unsigned long length);

  /**
   * @brief Extract tagged GTID from GTID_TAGGED_LOG_EVENT (MySQL 8.4+)
   *
   * The event body is a serialization-framework message: a format version, the
   * message size and the last non-ignorable field id, then fields in order
   * (flags, TSID bytes, GNO, tag, ...) each preceded by its id, with integers
   * as variable-length integers. Returns GTID in "UUID:TAG:GNO" or "UUID:GNO" format.
   *
   * @param buffer Event buffer
   * @param length Buffer length
   * @return GTID string if found
   */
  static std::optional<std::string> ExtractTaggedGTID(const unsigned char* buffer, unsigned long length);

  /**
   * @brief Parse TABLE_MAP_EVENT
   *
   * Reads the optional-metadata block that follows the NULL bitmap: column
   * names, SIGNEDNESS, and ENUM/SET labels (binlog_row_metadata=FULL). Unknown
   * block types are skipped. The result records which of these the event
   * supplied.
   *
   * @param buffer Event buffer (post-header)
   * @param length Buffer length
   * @param wants_row_metadata Called with the table identity before the optional
   *        block is read; returning false skips it (the result then reports no
   *        row metadata). Empty means always read it.
   * @return TableMetadata, kMySQLUndecodableBinlogEvent for an intact frame with a
   *         column type that cannot be sized or optional metadata that does not add up,
   *         or kMySQLBinlogError for a frame that is malformed before that point
   */
  static mygram::utils::Expected<TableMetadata, mygram::utils::Error> ParseTableMapEvent(
      const unsigned char* buffer, unsigned long length,
      const std::function<bool(const TableMetadata&)>& wants_row_metadata = {});

  /**
   * @brief Extract the default database, SQL text and quoting-relevant sql_mode bits from QUERY_EVENT
   *
   * The mode comes from the Q_SQL_MODE_CODE status variable, found by walking
   * the status variables in write order; an unknown variable ends the walk, and
   * a mode not reached by then is reported as unknown (sql_mode_known).
   *
   * @param buffer Event buffer
   * @param length Buffer length
   * @return Query data if successfully extracted
   */
  static std::optional<QueryEventData> ExtractQueryString(const unsigned char* buffer, unsigned long length);

  /**
   * @brief Quoting modes a query must be judged under
   *
   * Its own mode when known; otherwise every ANSI_QUOTES and NO_BACKSLASH_ESCAPES
   * combination, so that no reading of the text can hide a statement.
   */
  static std::vector<QueryLexOptions> LexModes(const QueryEventData& query);

  /// Boundary of a QUERY_EVENT under every mode in LexModes (XA, then BEGIN, then END win).
  static QueryTransactionBoundary ClassifyQueryTransactionBoundary(const QueryEventData& query);

  /// Whether the query is safe to ignore under every mode in LexModes.
  static bool IsSafeIgnoredQuery(const QueryEventData& query, const std::vector<std::string>& ignored_prefixes = {});

  /**
   * @brief Classify QUERY_EVENT transaction boundaries.
   *
   * The reader uses this to distinguish a standalone statement (whose GTID
   * completes at the QUERY_EVENT) from statements inside an explicit
   * transaction (whose GTID completes at XID/COMMIT). A CREATE TABLE ... START
   * TRANSACTION statement, which MySQL logs for CREATE TABLE ... SELECT, opens
   * a transaction that its row events and XID complete.
   *
   * @param query Query text as logged in the binlog
   * @param lex_options The event's sql_mode bits that change quoting
   */
  static QueryTransactionBoundary ClassifyQueryTransactionBoundary(std::string_view query,
                                                                   QueryLexOptions lex_options = {});

  /**
   * @brief Whether a MariaDB GTID event starts a transaction that needs a later COMMIT/XID.
   *
   * MariaDB sets FL_STANDALONE when the GTID event itself represents a
   * statement with no terminating COMMIT event.
   */
  static bool IsMariaDBGtidTransactionOpen(uint8_t flags);

  /**
   * @brief Whether a row event is the final event for its SQL statement.
   *
   * MariaDB standalone non-transactional groups have no XID/COMMIT event, so
   * the reader uses the row-event STMT_END_F flag as their commit boundary.
   */
  static bool IsRowsStatementEnd(const unsigned char* buffer, unsigned long length);

  /**
   * @brief Whether an ignored standalone QUERY_EVENT is proven not to mutate table rows.
   *
   * Statement-based DML must never advance the applied GTID because the row
   * materializer has not applied its effects. Only statements led by a verb
   * known not to change rows (DDL, account and transaction control) are safe,
   * once ParseBinlogEvent has emitted every configured-table and
   * configured-schema DDL. Every statement of a multi-statement event must be
   * safe, and a table DDL whose names cannot be read never is.
   *
   * @param query Query text as logged in the binlog
   * @param ignored_prefixes `mysql.ignored_ddl_prefixes`; a statement beginning
   *        with one of them is safe regardless of its verb
   * @param lex_options The event's sql_mode bits that change quoting
   */
  static bool IsSafeIgnoredQuery(std::string_view query, const std::vector<std::string>& ignored_prefixes = {},
                                 QueryLexOptions lex_options = {});

  /**
   * @brief Check if DDL affects target table
   * @param query SQL query string
   * @param table_name Target table name
   * @param lex_options The event's sql_mode bits that change quoting
   * @param names_case_insensitive Whether the source runs with lower_case_table_names != 0
   * @return true if DDL affects the table
   */
  static bool IsTableAffectingDDL(const std::string& query, const std::string& table_name,
                                  QueryLexOptions lex_options = {}, bool names_case_insensitive = false);
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
