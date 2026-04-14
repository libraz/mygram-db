/**
 * @file mariadb_event_parser.h
 * @brief MariaDB-specific binlog event parsing
 *
 * Parses MariaDB-specific events that differ from MySQL:
 * - MARIADB_GTID_EVENT (162): Extract domain-server-seq GTID
 * - MARIADB_GTID_LIST_EVENT (163): Parse GTID list at binlog start
 * - MARIADB_ANNOTATE_ROWS_EVENT (160): Extract SQL text for debug
 */

#pragma once

#ifdef USE_MYSQL

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "mysql/mariadb_gtid.h"

namespace mygramdb::mysql {

/**
 * @brief MariaDB-specific binlog event parser
 *
 * Handles events with type codes 160-164 that are unique to MariaDB.
 * Row events (TABLE_MAP, WRITE/UPDATE/DELETE_ROWS) use the same binary
 * format as MySQL and are parsed by BinlogEventParser.
 */
class MariaDBEventParser {
 public:
  /**
   * @brief Extract GTID from MARIADB_GTID_EVENT (type 162)
   *
   * Event layout after 19-byte header:
   *   seq_no    (8 bytes, little-endian uint64)
   *   domain_id (4 bytes, little-endian uint32)
   *   flags     (1 byte)
   *
   * The server_id comes from the standard event header (bytes 5-8).
   *
   * @param buffer Raw event data (including 19-byte header)
   * @param length Total event length
   * @return GTID string in "domain-server-seq" format, or nullopt on error
   */
  static std::optional<std::string> ExtractGTID(const unsigned char* buffer, size_t length);

  /**
   * @brief Parse MARIADB_GTID_LIST_EVENT (type 163)
   *
   * Event layout after 19-byte header:
   *   count_and_flags (4 bytes, little-endian) - lower 28 bits = count
   *   entries[count]:
   *     domain_id (4 bytes, little-endian uint32)
   *     server_id (4 bytes, little-endian uint32)
   *     seq_no    (8 bytes, little-endian uint64)
   *
   * @param buffer Raw event data (including 19-byte header)
   * @param length Total event length
   * @return Vector of GTIDs, or nullopt on parse error
   */
  static std::optional<std::vector<MariaDBGTID>> ParseGTIDList(const unsigned char* buffer, size_t length);

  /**
   * @brief Extract SQL text from MARIADB_ANNOTATE_ROWS_EVENT (type 160)
   *
   * The SQL text occupies the event body after the header,
   * excluding the 4-byte CRC32 checksum at the end.
   *
   * @param buffer Raw event data (including 19-byte header)
   * @param length Total event length
   * @return SQL text, or nullopt on error
   */
  static std::optional<std::string> ExtractAnnotateRows(const unsigned char* buffer, size_t length);
};

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
