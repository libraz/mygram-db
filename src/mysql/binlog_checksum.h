/**
 * @file binlog_checksum.h
 * @brief MySQL binlog event checksum verification.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include "utils/constants.h"
#include "utils/crc32.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::mysql {

/**
 * Verify the trailing little-endian CRC32 of a complete binlog event.
 *
 * A checksum mismatch is reported with the dedicated
 * `kMySQLBinlogChecksumMismatch` code so replication diagnostics can expose
 * the actual failure class instead of collapsing it into kMySQLBinlogError.
 */
inline mygram::utils::Expected<void, mygram::utils::Error> VerifyBinlogEventChecksum(const unsigned char* event_data,
                                                                                     size_t event_length) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  constexpr size_t kMinimumLength = mygram::constants::kBinlogEventHeaderLen + mygram::constants::kBinlogChecksumSize;
  if (event_data == nullptr || event_length < kMinimumLength) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Binlog event is too short for CRC32 checksum"));
  }

  const size_t data_length = event_length - mygram::constants::kBinlogChecksumSize;
  const uint32_t computed_crc = mygram::utils::ComputeCRC32(event_data, data_length);
  const unsigned char* checksum = event_data + data_length;
  const uint32_t stored_crc = static_cast<uint32_t>(checksum[0]) | (static_cast<uint32_t>(checksum[1]) << 8U) |
                              (static_cast<uint32_t>(checksum[2]) << 16U) | (static_cast<uint32_t>(checksum[3]) << 24U);
  if (computed_crc != stored_crc) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogChecksumMismatch,
                  "CRC32 checksum mismatch in binlog event; reconnecting from last processed GTID",
                  "computed=" + std::to_string(computed_crc) + " stored=" + std::to_string(stored_crc)));
  }
  return {};
}

}  // namespace mygramdb::mysql
