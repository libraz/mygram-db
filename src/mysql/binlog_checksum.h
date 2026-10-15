/**
 * @file binlog_checksum.h
 * @brief MySQL binlog event checksum verification.
 */

#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "mysql/binlog_event_types.h"
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

/// Checksum algorithm a binlog file declares in its FORMAT_DESCRIPTION_EVENT.
enum class BinlogChecksumAlgorithm : uint8_t {
  kOff = 0,
  kCrc32 = 1,
  kUndefined = 255,  ///< Written by a server that predates checksums: no event carries one.
};

/// Offset of the server version string in a FORMAT_DESCRIPTION_EVENT frame (header, then binlog version).
inline constexpr size_t kFormatDescriptionServerVersionOffset = mygram::constants::kBinlogEventHeaderLen + 2;
inline constexpr size_t kFormatDescriptionServerVersionLength = 50;
/// Offset of the post-header length array, the first byte after the fixed FORMAT_DESCRIPTION_EVENT fields.
inline constexpr size_t kFormatDescriptionPostHeaderOffset = kFormatDescriptionServerVersionOffset + 50 + 4 + 1;
/// Offset of the event length field in the common header.
inline constexpr size_t kBinlogEventLengthOffset = 9;

/**
 * Whether a FORMAT_DESCRIPTION_EVENT server version predates binlog checksums:
 * MySQL before 5.6.1, MariaDB before 5.3.0.
 */
inline bool ServerVersionPredatesChecksums(std::string_view version) {
  uint32_t parts[3] = {0, 0, 0};
  size_t pos = 0;
  for (auto& part : parts) {
    while (pos < version.size() && version[pos] >= '0' && version[pos] <= '9') {
      part = part * 10 + static_cast<uint32_t>(version[pos] - '0');
      ++pos;
    }
    if (pos >= version.size() || version[pos] != '.') {
      break;
    }
    ++pos;
  }
  const bool mariadb =
      version.find("MariaDB") != std::string_view::npos || version.find("-maria-") != std::string_view::npos;
  const uint32_t floor[3] = {5, mariadb ? 3U : 6U, mariadb ? 0U : 1U};
  for (size_t i = 0; i < 3; ++i) {
    if (parts[i] != floor[i]) {
      return parts[i] < floor[i];
    }
  }
  return false;
}

/**
 * Read the checksum algorithm a FORMAT_DESCRIPTION_EVENT frame declares.
 *
 * The algorithm byte sits just before the 4 bytes that close the event, and
 * only servers new enough to know checksums write it.
 *
 * @return the declared algorithm, or an error when the frame is too short or the byte is not a known algorithm
 */
inline mygram::utils::Expected<BinlogChecksumAlgorithm, mygram::utils::Error> ReadFormatDescriptionChecksumAlgorithm(
    const unsigned char* event_data, size_t event_length) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (event_data == nullptr ||
      event_length < kFormatDescriptionPostHeaderOffset + mygram::constants::kBinlogChecksumSize + 1) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "FORMAT_DESCRIPTION_EVENT is too short"));
  }
  const std::string_view version(reinterpret_cast<const char*>(event_data) + kFormatDescriptionServerVersionOffset,
                                 kFormatDescriptionServerVersionLength);
  if (ServerVersionPredatesChecksums(version.substr(0, version.find('\0')))) {
    return BinlogChecksumAlgorithm::kUndefined;
  }
  const unsigned char algorithm = event_data[event_length - mygram::constants::kBinlogChecksumSize - 1];
  if (algorithm != static_cast<unsigned char>(BinlogChecksumAlgorithm::kOff) &&
      algorithm != static_cast<unsigned char>(BinlogChecksumAlgorithm::kCrc32)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLBinlogError, "FORMAT_DESCRIPTION_EVENT declares unknown binlog checksum algorithm " +
                                                    std::to_string(static_cast<unsigned>(algorithm))));
  }
  return static_cast<BinlogChecksumAlgorithm>(algorithm);
}

/// A frame that carries the 4-byte checksum trailer every parser expects.
struct ChecksumFramedEvent {
  const unsigned char* data = nullptr;
  size_t length = 0;
};

/**
 * Tracks which checksum algorithm the source applies to the frames it sends.
 *
 * The source sends each binlog file with that file's own algorithm, taken from
 * the file's FORMAT_DESCRIPTION_EVENT. Heartbeats and the artificial ROTATE
 * sent between files use whichever algorithm was last in effect, which is the
 * one requested for the session until the first FORMAT_DESCRIPTION_EVENT. So
 * the algorithm in effect starts as CRC32 on every stream and follows the
 * format descriptions that arrive.
 *
 * The parsers read a 4-byte trailer from every frame. A frame sent without a
 * checksum is therefore presented with a zeroed trailer and a header length
 * that counts it, so that no parser needs to know the algorithm.
 */
class BinlogChecksumTracker {
 public:
  /// Return to the algorithm requested for a newly opened stream.
  void Reset() { algorithm_ = BinlogChecksumAlgorithm::kCrc32; }

  [[nodiscard]] BinlogChecksumAlgorithm algorithm() const { return algorithm_; }

  /**
   * Verify one frame under the algorithm in effect and return it in the trailer-carrying layout.
   *
   * A CRC32 mismatch is an error. The returned pointer is valid until the next call.
   */
  mygram::utils::Expected<ChecksumFramedEvent, mygram::utils::Error> Admit(const unsigned char* event_data,
                                                                           size_t event_length) {
    using mygram::utils::MakeUnexpected;

    if (event_data == nullptr || event_length < mygram::constants::kBinlogEventHeaderLen) {
      return MakeUnexpected(mygram::utils::MakeError(mygram::utils::ErrorCode::kMySQLFieldTruncated,
                                                     "Binlog event is shorter than the event header"));
    }

    if (static_cast<MySQLBinlogEventType>(event_data[4]) == MySQLBinlogEventType::FORMAT_DESCRIPTION_EVENT) {
      auto declared = ReadFormatDescriptionChecksumAlgorithm(event_data, event_length);
      if (!declared) {
        return MakeUnexpected(declared.error());
      }
      algorithm_ = *declared;
      // A format description always reserves the trailer, but the source does
      // not recompute it for a file written without checksums.
      if (algorithm_ == BinlogChecksumAlgorithm::kCrc32) {
        auto verified = VerifyBinlogEventChecksum(event_data, event_length);
        if (!verified) {
          return MakeUnexpected(verified.error());
        }
      }
      if (algorithm_ == BinlogChecksumAlgorithm::kUndefined) {
        return WithTrailer(event_data, event_length);
      }
      return ChecksumFramedEvent{event_data, event_length};
    }

    if (algorithm_ == BinlogChecksumAlgorithm::kCrc32) {
      auto verified = VerifyBinlogEventChecksum(event_data, event_length);
      if (!verified) {
        return MakeUnexpected(verified.error());
      }
      return ChecksumFramedEvent{event_data, event_length};
    }
    return WithTrailer(event_data, event_length);
  }

 private:
  ChecksumFramedEvent WithTrailer(const unsigned char* event_data, size_t event_length) {
    framed_.assign(event_data, event_data + event_length);
    framed_.resize(event_length + mygram::constants::kBinlogChecksumSize, 0);
    const uint32_t declared_length = static_cast<uint32_t>(event_data[kBinlogEventLengthOffset]) |
                                     (static_cast<uint32_t>(event_data[kBinlogEventLengthOffset + 1]) << 8U) |
                                     (static_cast<uint32_t>(event_data[kBinlogEventLengthOffset + 2]) << 16U) |
                                     (static_cast<uint32_t>(event_data[kBinlogEventLengthOffset + 3]) << 24U);
    if (declared_length == event_length) {
      const auto grown = static_cast<uint32_t>(framed_.size());
      for (size_t i = 0; i < 4; ++i) {
        framed_[kBinlogEventLengthOffset + i] = static_cast<unsigned char>((grown >> (8U * i)) & 0xFFU);
      }
    }
    return ChecksumFramedEvent{framed_.data(), framed_.size()};
  }

  BinlogChecksumAlgorithm algorithm_ = BinlogChecksumAlgorithm::kCrc32;
  std::vector<unsigned char> framed_;
};

}  // namespace mygramdb::mysql
