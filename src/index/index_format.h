/**
 * @file index_format.h
 * @brief On-disk format constants for serialized index files
 *
 * A serialized index starts with a 4-byte magic followed by a little-endian
 * uint32 format version. The reader accepts every version listed here; the
 * writer always emits kCurrentFormatVersion.
 */

#pragma once

#include <cstdint>
#include <string_view>

namespace mygramdb::index::format {

/// Magic prefix identifying a serialized index ("MGIX").
inline constexpr std::string_view kMagic = "MGIX";

constexpr uint32_t kFormatVersionV1 = 1;
constexpr uint32_t kFormatVersionV2 = 2;
constexpr uint32_t kFormatVersionV3 = 3;
constexpr uint32_t kFormatVersionV4 = 4;

/// Version the writer emits (tokenizer config plus a CRC32 trailer).
constexpr uint32_t kCurrentFormatVersion = kFormatVersionV4;

/// Oldest version the reader accepts.
constexpr uint32_t kMinSupportedVersion = kFormatVersionV1;

/// Newest version the reader accepts.
constexpr uint32_t kMaxSupportedVersion = kFormatVersionV4;

/// Size of the CRC32 checksum trailer in bytes.
constexpr size_t kCRC32Size = 4;

}  // namespace mygramdb::index::format
