/**
 * @file mariadb_event_parser.cpp
 * @brief MariaDB-specific binlog event parsing implementation
 */

#include "mysql/mariadb_event_parser.h"

#ifdef USE_MYSQL

#include <cstring>

#include "utils/constants.h"

// NOLINTBEGIN(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

namespace mygramdb::mysql {

/// Minimum size of MARIADB_GTID_EVENT post-header: seq_no(8) + domain_id(4) + flags(1)
static constexpr size_t kMariaDBGtidEventMinPostHeader = 13;

/// Size of each entry in GTID_LIST_EVENT: domain_id(4) + server_id(4) + seq_no(8)
static constexpr size_t kGtidListEntrySize = 16;

/// Mask for extracting entry count from GTID_LIST count_and_flags field (lower 28 bits)
static constexpr uint32_t kGtidListCountMask = 0x0FFFFFFFu;

std::optional<std::string> MariaDBEventParser::ExtractGTID(const unsigned char* buffer, size_t length) {
  // Need at least header + seq_no(8) + domain_id(4) + flags(1)
  if (buffer == nullptr || length < mygram::constants::kBinlogEventHeaderLen + kMariaDBGtidEventMinPostHeader) {
    return std::nullopt;
  }

  // Extract server_id from event header (bytes 5-8, little-endian)
  uint32_t server_id = 0;
  std::memcpy(&server_id, buffer + 5, sizeof(server_id));

  // Post-header starts after 19-byte event header
  const unsigned char* post_header = buffer + mygram::constants::kBinlogEventHeaderLen;

  // seq_no: 8 bytes at offset 0 (little-endian uint64)
  uint64_t seq_no = 0;
  std::memcpy(&seq_no, post_header, sizeof(seq_no));

  // domain_id: 4 bytes at offset 8 (little-endian uint32)
  uint32_t domain_id = 0;
  std::memcpy(&domain_id, post_header + 8, sizeof(domain_id));

  // Construct "domain-server-seq" format
  MariaDBGTID gtid;
  gtid.domain_id = domain_id;
  gtid.server_id = server_id;
  gtid.sequence_no = seq_no;
  return gtid.ToString();
}

std::optional<std::vector<MariaDBGTID>> MariaDBEventParser::ParseGTIDList(const unsigned char* buffer, size_t length) {
  // Need at least header + count_and_flags(4)
  if (buffer == nullptr || length < mygram::constants::kBinlogEventHeaderLen + 4) {
    return std::nullopt;
  }

  const unsigned char* post_header = buffer + mygram::constants::kBinlogEventHeaderLen;

  // count_and_flags: 4 bytes (lower 28 bits = count)
  uint32_t count_and_flags = 0;
  std::memcpy(&count_and_flags, post_header, sizeof(count_and_flags));
  uint32_t count = count_and_flags & kGtidListCountMask;

  // Validate we have enough data for all entries
  size_t entries_offset = mygram::constants::kBinlogEventHeaderLen + 4;
  size_t required_size = entries_offset + (static_cast<size_t>(count) * kGtidListEntrySize);
  if (length < required_size) {
    return std::nullopt;
  }

  std::vector<MariaDBGTID> result;
  result.reserve(count);

  const unsigned char* entry_ptr = buffer + entries_offset;
  for (uint32_t i = 0; i < count; ++i) {
    MariaDBGTID gtid;
    std::memcpy(&gtid.domain_id, entry_ptr, sizeof(gtid.domain_id));
    std::memcpy(&gtid.server_id, entry_ptr + 4, sizeof(gtid.server_id));
    std::memcpy(&gtid.sequence_no, entry_ptr + 8, sizeof(gtid.sequence_no));
    result.push_back(gtid);
    entry_ptr += kGtidListEntrySize;
  }

  return result;
}

std::optional<std::string> MariaDBEventParser::ExtractAnnotateRows(const unsigned char* buffer, size_t length) {
  // Need at least header + 1 byte of text + CRC32(4)
  if (buffer == nullptr ||
      length <= mygram::constants::kBinlogEventHeaderLen + mygram::constants::kBinlogChecksumSize) {
    return std::nullopt;
  }

  // SQL text is everything between header and CRC32 checksum
  size_t text_offset = mygram::constants::kBinlogEventHeaderLen;
  size_t text_length = length - text_offset - mygram::constants::kBinlogChecksumSize;

  if (text_length == 0) {
    return std::nullopt;
  }

  return std::string(reinterpret_cast<const char*>(buffer + text_offset), text_length);
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-pro-*,cppcoreguidelines-avoid-*,readability-magic-numbers)

#endif  // USE_MYSQL
