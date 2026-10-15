/**
 * @file binlog_event_builder.h
 * @brief Test helper: construct MySQL binlog events in exact wire format
 *
 * Built from MySQL Internals Manual (public protocol specification).
 * No MySQL source code is used. Wire protocol formats are not copyrightable.
 */

#pragma once

#include <array>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "mysql/binlog_event_types.h"
#include "utils/crc32.h"

// NOLINTBEGIN(readability-magic-numbers)

namespace mygramdb::mysql::test {

/**
 * @brief Constructs MySQL binlog events in exact wire format for unit testing.
 *
 * All methods are static and inline since this is a header-only test utility.
 * Event formats follow the MySQL Internals Manual (public protocol spec).
 */
class BinlogEventBuilder {
 public:
  /** @brief Append a uint16_t in little-endian format. */
  static inline void AppendLittleEndian16(std::vector<uint8_t>& buf, uint16_t value) {
    buf.push_back(static_cast<uint8_t>(value & 0xFF));
    buf.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
  }

  /** @brief Append a uint32_t in little-endian format. */
  static inline void AppendLittleEndian32(std::vector<uint8_t>& buf, uint32_t value) {
    buf.push_back(static_cast<uint8_t>(value & 0xFF));
    buf.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
    buf.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
    buf.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
  }

  /** @brief Append a uint64_t in little-endian format. */
  static inline void AppendLittleEndian64(std::vector<uint8_t>& buf, uint64_t value) {
    for (int i = 0; i < 8; ++i) {
      buf.push_back(static_cast<uint8_t>((value >> (i * 8)) & 0xFF));
    }
  }

  /** @brief Append a 6-byte little-endian table ID (MySQL binlog format). */
  static inline void AppendTableId(std::vector<uint8_t>& buf, uint64_t table_id) {
    for (int i = 0; i < 6; ++i) {
      buf.push_back(static_cast<uint8_t>((table_id >> (i * 8)) & 0xFF));
    }
  }

  /**
   * @brief Append a MySQL packed integer (length-encoded integer).
   *
   * Format:
   * - value < 251: 1 byte
   * - value < 2^16: 0xFC + 2 bytes LE
   * - value < 2^24: 0xFD + 3 bytes LE
   * - else: 0xFE + 8 bytes LE
   */
  static inline void AppendPackedInt(std::vector<uint8_t>& buf, uint64_t value) {
    if (value < 251) {
      buf.push_back(static_cast<uint8_t>(value));
    } else if (value < (1ULL << 16)) {
      buf.push_back(0xFC);
      AppendLittleEndian16(buf, static_cast<uint16_t>(value));
    } else if (value < (1ULL << 24)) {
      buf.push_back(0xFD);
      buf.push_back(static_cast<uint8_t>(value & 0xFF));
      buf.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
      buf.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
    } else {
      buf.push_back(0xFE);
      AppendLittleEndian64(buf, value);
    }
  }

  /**
   * @brief Append an unsigned variable-length integer of the serialization framework.
   *
   * The byte count is 1 + ceil((bits-1)/7) for 1..63 significant bits and 9 for 64.
   * The first byte holds (byte_count - 1) trailing one bits and the value's low bits
   * above them; the remaining bytes carry the rest little-endian.
   */
  static inline void AppendSerializedVarlen(std::vector<uint8_t>& buf, uint64_t value) {
    int bits = 0;
    for (uint64_t rest = value; rest != 0; rest >>= 1) {
      ++bits;
    }
    const int byte_count = ((bits * 575) >> 12) + 1;
    buf.push_back(static_cast<uint8_t>(((1 << (byte_count - 1)) - 1) | static_cast<uint8_t>(value << byte_count)));
    if (byte_count == 1) {
      return;
    }
    uint64_t rest = value >> (8 - byte_count + ((byte_count + 7) >> 4));
    for (int i = 0; i < byte_count - 1; ++i) {
      buf.push_back(static_cast<uint8_t>((rest >> (i * 8)) & 0xFF));
    }
  }

  /** @brief Append a signed variable-length integer: the sign in the low bit, a negative's complement above it. */
  static inline void AppendSerializedVarlenSigned(std::vector<uint8_t>& buf, int64_t value) {
    const uint64_t sign_mask = value < 0 ? ~uint64_t{0} : 0;
    AppendSerializedVarlen(buf, ((static_cast<uint64_t>(value) ^ sign_mask) << 1) | (sign_mask & 1));
  }

  /**
   * @brief Append a bitmap as raw bytes (least-significant bit first).
   * @param bits Bitmask value
   * @param count Number of bits in the bitmap
   */
  static inline void AppendBitmap(std::vector<uint8_t>& buf, uint64_t bits, size_t count) {
    size_t num_bytes = (count + 7) / 8;
    for (size_t i = 0; i < num_bytes; ++i) {
      buf.push_back(static_cast<uint8_t>((bits >> (i * 8)) & 0xFF));
    }
  }

  /**
   * @brief Append a null bitmap from a vector of booleans.
   * @param nulls Vector where true means the column is NULL
   */
  static inline void AppendNullBitmap(std::vector<uint8_t>& buf, const std::vector<bool>& nulls) {
    size_t num_bytes = (nulls.size() + 7) / 8;
    for (size_t i = 0; i < num_bytes; ++i) {
      uint8_t byte = 0;
      for (size_t bit = 0; bit < 8 && (i * 8 + bit) < nulls.size(); ++bit) {
        if (nulls[i * 8 + bit]) {
          byte |= (1 << bit);
        }
      }
      buf.push_back(byte);
    }
  }

  /**
   * @brief Fix the event_size field in bytes [9-12] of the common header.
   *
   * Sets the event_size to the current buffer size.
   */
  static inline void FixEventSize(std::vector<uint8_t>& buf) {
    auto size = static_cast<uint32_t>(buf.size());
    buf[9] = static_cast<uint8_t>(size & 0xFF);
    buf[10] = static_cast<uint8_t>((size >> 8) & 0xFF);
    buf[11] = static_cast<uint8_t>((size >> 16) & 0xFF);
    buf[12] = static_cast<uint8_t>((size >> 24) & 0xFF);
  }

  /**
   * @brief Fix the event_size field to include the buffer size
   *        (which should already contain a 4-byte checksum placeholder).
   */
  static inline void FixEventSizeWithChecksum(std::vector<uint8_t>& buf) { FixEventSize(buf); }

  /**
   * @brief Compute CRC32 over all bytes except the trailing 4-byte checksum
   *        and write the result into the last 4 bytes (little-endian).
   *
   * Call this after FixEventSizeWithChecksum() to produce a valid event.
   */
  static inline void FixChecksum(std::vector<uint8_t>& buf) {
    if (buf.size() < 4) {
      return;
    }
    size_t data_len = buf.size() - 4;
    uint32_t crc = mygram::utils::ComputeCRC32(buf.data(), data_len);
    std::memcpy(buf.data() + data_len, &crc, sizeof(crc));
  }

  /**
   * @brief Build the 19-byte common event header.
   *
   * Format: [timestamp(4)][event_type(1)][server_id(4)][event_size(4)]
   *         [log_pos(4)][flags(2)]
   *
   * event_size is set to 0; call FixEventSize() after building the full event.
   */
  static inline std::vector<uint8_t> BuildHeader(MySQLBinlogEventType type, uint32_t server_id = 1,
                                                 uint32_t log_pos = 0, uint16_t flags = 0) {
    std::vector<uint8_t> buf;
    buf.reserve(19);

    // timestamp (4 bytes) - use 0
    AppendLittleEndian32(buf, 0);
    // event_type (1 byte)
    buf.push_back(static_cast<uint8_t>(type));
    // server_id (4 bytes)
    AppendLittleEndian32(buf, server_id);
    // event_size (4 bytes) - placeholder, fixed later
    AppendLittleEndian32(buf, 0);
    // log_pos (4 bytes)
    AppendLittleEndian32(buf, log_pos);
    // flags (2 bytes)
    AppendLittleEndian16(buf, flags);

    return buf;
  }

  /**
   * @brief Build a QUERY_EVENT with no status variables.
   *
   * Post-header: [thread_id(4B)][exec_time(4B)][db_len(1B)][error_code(2B)][status_vars_len(2B)]
   * Body: [database][0x00][query]
   * Tail: [CRC32 checksum placeholder(4B)]
   */
  static inline std::vector<uint8_t> BuildQueryEvent(const std::string& database, const std::string& query) {
    auto buf = BuildHeader(MySQLBinlogEventType::QUERY_EVENT);
    AppendLittleEndian32(buf, 1);  // thread_id
    AppendLittleEndian32(buf, 0);  // query_exec_time
    buf.push_back(static_cast<uint8_t>(database.size()));
    AppendLittleEndian16(buf, 0);  // error_code
    AppendLittleEndian16(buf, 0);  // status_vars_len
    buf.insert(buf.end(), database.begin(), database.end());
    buf.push_back(0x00);
    buf.insert(buf.end(), query.begin(), query.end());
    AppendLittleEndian32(buf, 0);
    FixEventSizeWithChecksum(buf);
    return buf;
  }

  /**
   * @brief Build a QUERY_EVENT carrying the given status variable bytes.
   *
   * @param status_vars Encoded status variables, each starting with its code byte
   */
  static inline std::vector<uint8_t> BuildQueryEventWithStatusVars(const std::string& database,
                                                                   const std::string& query,
                                                                   const std::vector<uint8_t>& status_vars) {
    auto buf = BuildHeader(MySQLBinlogEventType::QUERY_EVENT);
    AppendLittleEndian32(buf, 1);  // thread_id
    AppendLittleEndian32(buf, 0);  // query_exec_time
    buf.push_back(static_cast<uint8_t>(database.size()));
    AppendLittleEndian16(buf, 0);  // error_code
    AppendLittleEndian16(buf, static_cast<uint16_t>(status_vars.size()));
    buf.insert(buf.end(), status_vars.begin(), status_vars.end());
    buf.insert(buf.end(), database.begin(), database.end());
    buf.push_back(0x00);
    buf.insert(buf.end(), query.begin(), query.end());
    AppendLittleEndian32(buf, 0);
    FixEventSizeWithChecksum(buf);
    return buf;
  }

  /**
   * @brief Build a V2 WRITE_ROWS_EVENT (type 30).
   *
   * Post-header: [table_id(6B)][flags(2B)][var_header_len(2B)][extra_data]
   * Body: [packed_int column_count][columns_bitmap][row_data]
   * Tail: [CRC32 checksum placeholder(4B)]
   */
  static inline std::vector<uint8_t> BuildWriteRowsV2(uint64_t table_id, uint16_t flags, uint16_t var_header_len,
                                                      const std::vector<uint8_t>& extra_data, uint64_t column_count,
                                                      const std::vector<uint8_t>& columns_bitmap,
                                                      const std::vector<uint8_t>& row_data) {
    auto buf = BuildHeader(MySQLBinlogEventType::WRITE_ROWS_EVENT);

    // Post-header
    AppendTableId(buf, table_id);
    AppendLittleEndian16(buf, flags);
    AppendLittleEndian16(buf, var_header_len);
    buf.insert(buf.end(), extra_data.begin(), extra_data.end());

    // Body
    AppendPackedInt(buf, column_count);
    buf.insert(buf.end(), columns_bitmap.begin(), columns_bitmap.end());
    buf.insert(buf.end(), row_data.begin(), row_data.end());

    // CRC32 checksum placeholder (4 zero bytes)
    AppendLittleEndian32(buf, 0);

    FixEventSizeWithChecksum(buf);
    return buf;
  }

  /**
   * @brief Build a V1 WRITE_ROWS_EVENT (type 23, obsolete).
   *
   * Post-header: [table_id(6B)][flags(2B)] (no var_header_len)
   * Body: [packed_int column_count][columns_bitmap][row_data]
   * Tail: [CRC32 checksum placeholder(4B)]
   */
  static inline std::vector<uint8_t> BuildWriteRowsV1(uint64_t table_id, uint16_t flags, uint64_t column_count,
                                                      const std::vector<uint8_t>& columns_bitmap,
                                                      const std::vector<uint8_t>& row_data) {
    auto buf = BuildHeader(MySQLBinlogEventType::OBSOLETE_WRITE_ROWS_EVENT_V1);

    // Post-header (V1: no var_header_len)
    AppendTableId(buf, table_id);
    AppendLittleEndian16(buf, flags);

    // Body
    AppendPackedInt(buf, column_count);
    buf.insert(buf.end(), columns_bitmap.begin(), columns_bitmap.end());
    buf.insert(buf.end(), row_data.begin(), row_data.end());

    // CRC32 checksum placeholder (4 zero bytes)
    AppendLittleEndian32(buf, 0);

    FixEventSizeWithChecksum(buf);
    return buf;
  }

  /**
   * @brief Build a V2 UPDATE_ROWS_EVENT (type 31).
   *
   * Post-header: [table_id(6B)][flags(2B)][var_header_len(2B)][extra_data]
   * Body: [packed_int column_count][columns_before_bitmap]
   *       [columns_after_bitmap][row_data]
   * Tail: [CRC32 checksum placeholder(4B)]
   */
  static inline std::vector<uint8_t> BuildUpdateRowsV2(uint64_t table_id, uint16_t flags, uint16_t var_header_len,
                                                       const std::vector<uint8_t>& extra_data, uint64_t column_count,
                                                       const std::vector<uint8_t>& columns_before_bitmap,
                                                       const std::vector<uint8_t>& columns_after_bitmap,
                                                       const std::vector<uint8_t>& row_data) {
    auto buf = BuildHeader(MySQLBinlogEventType::UPDATE_ROWS_EVENT);

    // Post-header
    AppendTableId(buf, table_id);
    AppendLittleEndian16(buf, flags);
    AppendLittleEndian16(buf, var_header_len);
    buf.insert(buf.end(), extra_data.begin(), extra_data.end());

    // Body (UPDATE has two bitmaps: before and after)
    AppendPackedInt(buf, column_count);
    buf.insert(buf.end(), columns_before_bitmap.begin(), columns_before_bitmap.end());
    buf.insert(buf.end(), columns_after_bitmap.begin(), columns_after_bitmap.end());
    buf.insert(buf.end(), row_data.begin(), row_data.end());

    // CRC32 checksum placeholder (4 zero bytes)
    AppendLittleEndian32(buf, 0);

    FixEventSizeWithChecksum(buf);
    return buf;
  }

  /**
   * @brief Build a V2 DELETE_ROWS_EVENT (type 32).
   *
   * Post-header: [table_id(6B)][flags(2B)][var_header_len(2B)][extra_data]
   * Body: [packed_int column_count][columns_bitmap][row_data]
   * Tail: [CRC32 checksum placeholder(4B)]
   */
  static inline std::vector<uint8_t> BuildDeleteRowsV2(uint64_t table_id, uint16_t flags, uint16_t var_header_len,
                                                       const std::vector<uint8_t>& extra_data, uint64_t column_count,
                                                       const std::vector<uint8_t>& columns_bitmap,
                                                       const std::vector<uint8_t>& row_data) {
    auto buf = BuildHeader(MySQLBinlogEventType::DELETE_ROWS_EVENT);

    // Post-header
    AppendTableId(buf, table_id);
    AppendLittleEndian16(buf, flags);
    AppendLittleEndian16(buf, var_header_len);
    buf.insert(buf.end(), extra_data.begin(), extra_data.end());

    // Body
    AppendPackedInt(buf, column_count);
    buf.insert(buf.end(), columns_bitmap.begin(), columns_bitmap.end());
    buf.insert(buf.end(), row_data.begin(), row_data.end());

    // CRC32 checksum placeholder (4 zero bytes)
    AppendLittleEndian32(buf, 0);

    FixEventSizeWithChecksum(buf);
    return buf;
  }

  /**
   * @brief Build a GTID_LOG_EVENT (type 33).
   *
   * Post-header: [commit_flag(1B)][UUID(16B)][GNO(8B LE)]
   * Tail: [CRC32 checksum placeholder(4B)]
   */
  static inline std::vector<uint8_t> BuildGtidEvent(const std::array<uint8_t, 16>& uuid, uint64_t gno,
                                                    uint8_t commit_flag = 1) {
    auto buf = BuildHeader(MySQLBinlogEventType::GTID_LOG_EVENT);

    // Post-header
    buf.push_back(commit_flag);
    buf.insert(buf.end(), uuid.begin(), uuid.end());
    AppendLittleEndian64(buf, gno);

    // CRC32 checksum placeholder (4 zero bytes)
    AppendLittleEndian32(buf, 0);

    FixEventSizeWithChecksum(buf);
    return buf;
  }

  /**
   * @brief Build a GTID_TAGGED_LOG_EVENT (type 42, MySQL 8.4+).
   *
   * The body is a serialization-framework message with no commit_flag byte:
   * format version, message size and last non-ignorable field id, then fields
   * each preceded by its id: 0 gtid_flags, 1 UUID (16 varlen bytes), 2 GNO (signed
   * varlen), 3 tag (varlen length + bytes), 4 last_committed, 5 sequence_number,
   * 6 immediate_commit_timestamp, 8 transaction_length, 9 immediate_server_version.
   */
  static inline std::vector<uint8_t> BuildGtidTaggedEvent(const std::array<uint8_t, 16>& uuid, int64_t gno,
                                                          const std::string& tag, uint8_t gtid_flags = 1) {
    auto buf = BuildHeader(MySQLBinlogEventType::GTID_TAGGED_LOG_EVENT);

    std::vector<uint8_t> fields;
    AppendSerializedVarlen(fields, 0);  // field id: gtid_flags
    AppendSerializedVarlen(fields, gtid_flags);
    AppendSerializedVarlen(fields, 1);  // field id: UUID
    for (const uint8_t byte : uuid) {
      AppendSerializedVarlen(fields, byte);
    }
    AppendSerializedVarlen(fields, 2);  // field id: GNO
    AppendSerializedVarlenSigned(fields, gno);
    AppendSerializedVarlen(fields, 3);  // field id: tag
    AppendSerializedVarlen(fields, tag.size());
    fields.insert(fields.end(), tag.begin(), tag.end());
    AppendSerializedVarlen(fields, 4);  // field id: last_committed
    AppendSerializedVarlenSigned(fields, 0);
    AppendSerializedVarlen(fields, 5);  // field id: sequence_number
    AppendSerializedVarlenSigned(fields, 1);
    AppendSerializedVarlen(fields, 6);  // field id: immediate_commit_timestamp
    AppendSerializedVarlen(fields, 1700000000000000ULL);

    AppendSerializedVarlen(buf, 1);  // serialization format version
    AppendSerializedVarlen(buf, fields.size());
    AppendSerializedVarlen(buf, 7);  // last non-ignorable field id
    buf.insert(buf.end(), fields.begin(), fields.end());

    // CRC32 checksum placeholder (4 zero bytes)
    AppendLittleEndian32(buf, 0);

    FixEventSizeWithChecksum(buf);
    return buf;
  }

  // =========================================================================
  // MariaDB-specific event builders
  // =========================================================================

  /**
   * @brief Build a MARIADB_GTID_EVENT (type 162).
   *
   * Post-header: [seq_no(8B LE)][domain_id(4B LE)][flags(1B)]
   * Tail: [CRC32 checksum placeholder(4B)]
   *
   * The server_id is stored in the standard event header.
   */
  static inline std::vector<uint8_t> BuildMariaDBGtidEvent(uint32_t domain_id, uint32_t server_id, uint64_t seq_no,
                                                           uint8_t flags = 0) {
    auto buf = BuildHeader(MySQLBinlogEventType::MARIADB_GTID_EVENT, server_id);

    // Post-header
    AppendLittleEndian64(buf, seq_no);
    AppendLittleEndian32(buf, domain_id);
    buf.push_back(flags);

    // CRC32 checksum placeholder
    AppendLittleEndian32(buf, 0);

    FixEventSizeWithChecksum(buf);
    FixChecksum(buf);
    return buf;
  }

  /**
   * @brief Build a MARIADB_GTID_LIST_EVENT (type 163).
   *
   * Post-header: [count_and_flags(4B LE)]
   * Body: [entries: domain_id(4B) + server_id(4B) + seq_no(8B)] * count
   * Tail: [CRC32 checksum placeholder(4B)]
   */
  struct MariaDBGtidEntry {
    uint32_t domain_id;
    uint32_t server_id;
    uint64_t seq_no;
  };

  static inline std::vector<uint8_t> BuildMariaDBGtidListEvent(const std::vector<MariaDBGtidEntry>& entries,
                                                               uint32_t flags = 0) {
    auto buf = BuildHeader(MySQLBinlogEventType::MARIADB_GTID_LIST_EVENT);

    uint32_t count_and_flags = (flags << 28) | (static_cast<uint32_t>(entries.size()) & 0x0FFFFFFFu);
    AppendLittleEndian32(buf, count_and_flags);

    for (const auto& entry : entries) {
      AppendLittleEndian32(buf, entry.domain_id);
      AppendLittleEndian32(buf, entry.server_id);
      AppendLittleEndian64(buf, entry.seq_no);
    }

    // CRC32 checksum placeholder
    AppendLittleEndian32(buf, 0);

    FixEventSizeWithChecksum(buf);
    FixChecksum(buf);
    return buf;
  }

  /**
   * @brief Build a MARIADB_ANNOTATE_ROWS_EVENT (type 160).
   *
   * Body: SQL text
   * Tail: [CRC32 checksum placeholder(4B)]
   */
  static inline std::vector<uint8_t> BuildMariaDBAnnotateRowsEvent(const std::string& sql) {
    auto buf = BuildHeader(MySQLBinlogEventType::MARIADB_ANNOTATE_ROWS_EVENT);

    buf.insert(buf.end(), sql.begin(), sql.end());

    // CRC32 checksum placeholder
    AppendLittleEndian32(buf, 0);

    FixEventSizeWithChecksum(buf);
    FixChecksum(buf);
    return buf;
  }
};

}  // namespace mygramdb::mysql::test

// NOLINTEND(readability-magic-numbers)
