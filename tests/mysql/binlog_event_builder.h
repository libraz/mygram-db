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
#include <string>
#include <vector>

#include "mysql/binlog_event_types.h"

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
   * Uses the MySQL 8.4 serialization framework format with field_id + type_id
   * pairs. Fields:
   * - Field 1 (COMMIT_GROUP_TICKET): 8 bytes
   * - Field 2 (TSID): UUID(16B) + tag_len(1B) + tag_bytes
   * - Field 3 (SPEC): GNO(8B)
   * - Field 4 (COMMIT_TIMESTAMP): original(7B) + immediate(7B)
   */
  static inline std::vector<uint8_t> BuildGtidTaggedEvent(const std::array<uint8_t, 16>& uuid, uint64_t gno,
                                                          const std::string& tag, uint8_t commit_flag = 1) {
    auto buf = BuildHeader(MySQLBinlogEventType::GTID_TAGGED_LOG_EVENT);

    // commit_flag byte (present before serialized fields)
    buf.push_back(commit_flag);

    // Field 1: COMMIT_GROUP_TICKET (field_id=0, type_id=1 for uint64)
    // Value: 0 (no ticket)
    buf.push_back(0);  // field_id
    buf.push_back(1);  // type_id: uint64
    AppendLittleEndian64(buf, 0);

    // Field 2: TSID (field_id=1, type_id=2 for variable-length)
    // Contains UUID (16B) + tag_length (1B) + tag bytes
    buf.push_back(1);  // field_id
    buf.push_back(2);  // type_id: variable-length
    buf.insert(buf.end(), uuid.begin(), uuid.end());
    buf.push_back(static_cast<uint8_t>(tag.size()));
    if (!tag.empty()) {
      buf.insert(buf.end(), tag.begin(), tag.end());
    }

    // Field 3: SPEC (field_id=2, type_id=1 for uint64)
    // Contains GNO
    buf.push_back(2);  // field_id
    buf.push_back(1);  // type_id: uint64
    AppendLittleEndian64(buf, gno);

    // Field 4: COMMIT_TIMESTAMP (field_id=3, type_id=2 for variable-length)
    // Contains original_commit_timestamp (7B) + immediate_commit_timestamp (7B)
    buf.push_back(3);  // field_id
    buf.push_back(2);  // type_id: variable-length
    // original_commit_timestamp: 7 bytes (0 = unknown)
    for (int i = 0; i < 7; ++i) {
      buf.push_back(0);
    }
    // immediate_commit_timestamp: 7 bytes (0 = unknown)
    for (int i = 0; i < 7; ++i) {
      buf.push_back(0);
    }

    // CRC32 checksum placeholder (4 zero bytes)
    AppendLittleEndian32(buf, 0);

    FixEventSizeWithChecksum(buf);
    return buf;
  }
};

}  // namespace mygramdb::mysql::test

// NOLINTEND(readability-magic-numbers)
