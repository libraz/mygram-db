/**
 * @file dump_format_v1_integrity.cpp
 * @brief CRC32 calculation, integrity verification, and dump info for format V1
 */

#include <zlib.h>

#include <array>
#include <cstring>
#include <fstream>
#include <vector>

#include "storage/dump_format_v1.h"
#include "storage/dump_format_v1_internal.h"
#include "utils/binary_io.h"
#include "utils/structured_log.h"

namespace mygramdb::storage::dump_v1 {

using namespace mygram::utils;
using internal::ReadString;

// ============================================================================
// CRC32 Calculation
// ============================================================================

/**
 * @brief Calculate CRC32 of a file using streaming (chunked) reads
 *
 * This avoids loading the entire file into memory, preventing OOM for large files.
 *
 * @param ifs Input file stream (must be seekable)
 * @param file_size Total file size to read
 * @param crc_offset Position of the CRC field to zero out during calculation
 * @return CRC32 checksum of the file
 */
uint32_t CalculateCRC32Streaming(std::ifstream& ifs, uint64_t file_size, size_t crc_offset) {
  constexpr size_t kChunkSize = 1024 * 1024;  // 1MB chunks
  constexpr size_t kCrcFieldSize = 4;

  ifs.seekg(0, std::ios::beg);

  uint32_t crc = 0;
  std::vector<char> buffer(kChunkSize);
  uint64_t bytes_read = 0;

  while (bytes_read < file_size) {
    size_t to_read = std::min(kChunkSize, static_cast<size_t>(file_size - bytes_read));
    ifs.read(buffer.data(), static_cast<std::streamsize>(to_read));
    auto actually_read = static_cast<size_t>(ifs.gcount());

    if (actually_read == 0) {
      break;  // EOF or error
    }

    // Zero out the CRC field if it falls within this chunk
    if (crc_offset >= bytes_read && crc_offset < bytes_read + actually_read) {
      size_t offset_in_chunk = crc_offset - bytes_read;
      size_t zero_bytes = std::min(kCrcFieldSize, actually_read - offset_in_chunk);
      std::memset(&buffer[offset_in_chunk], 0, zero_bytes);
    }
    // Handle case where CRC field spans chunk boundary
    if (crc_offset + kCrcFieldSize > bytes_read && crc_offset < bytes_read) {
      size_t zero_start = 0;
      size_t zero_end = std::min<size_t>(kCrcFieldSize - (bytes_read - crc_offset), actually_read);
      std::memset(&buffer[zero_start], 0, zero_end);
    }

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for zlib crc32 API
    crc = static_cast<uint32_t>(
        crc32(crc, reinterpret_cast<const Bytef*>(buffer.data()), static_cast<uInt>(actually_read)));  // NOLINT

    bytes_read += actually_read;
  }

  return crc;
}

// ============================================================================
// Snapshot Integrity Verification
// ============================================================================

Expected<void, Error> VerifyDumpIntegrity(const std::string& filepath, dump_format::IntegrityError& integrity_error) {
  try {
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "Failed to open file: " + filepath;
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
    }

    // Read and verify fixed file header
    std::array<char, 4> magic{};
    ifs.read(magic.data(), 4);
    if (!ifs.good() || std::memcmp(magic.data(), dump_format::kMagicNumber.data(), 4) != 0) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "Invalid magic number";
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
    }

    uint32_t version = 0;
    if (!ReadBinary(ifs, version)) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "Failed to read version";
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
    }

    // Version compatibility check
    if (version > dump_format::kMaxSupportedVersion) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "Version " + std::to_string(version) + " is newer than supported version " +
                                std::to_string(dump_format::kMaxSupportedVersion);
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
    }
    if (version < dump_format::kMinSupportedVersion) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "Version " + std::to_string(version) + " is older than minimum supported version " +
                                std::to_string(dump_format::kMinSupportedVersion);
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
    }

    // Read V1 header
    HeaderV1 header;
    if (!ReadHeaderV1(ifs, header)) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "Failed to read V1 header";
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
    }

    // Verify file size if specified
    if (header.total_file_size > 0) {
      ifs.seekg(0, std::ios::end);
      auto actual_size = static_cast<uint64_t>(ifs.tellg());

      if (actual_size != header.total_file_size) {
        integrity_error.type = dump_format::CRCErrorType::FileCRC;
        integrity_error.message = "File size mismatch: expected " + std::to_string(header.total_file_size) +
                                  " bytes, got " + std::to_string(actual_size) + " bytes (file may be truncated)";
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
      }
    }

    // Verify CRC32 if specified
    // Use streaming CRC to avoid loading entire file into memory (prevents OOM for large files)
    if (header.file_crc32 != 0) {
      // Get file size
      ifs.seekg(0, std::ios::end);
      auto file_size = static_cast<uint64_t>(ifs.tellg());

      // CRC field offset: magic + version + header_size + flags + timestamp + total_file_size
      const size_t crc_offset = 4 + 4 + 4 + 4 + 8 + 8;

      uint32_t calculated_crc = CalculateCRC32Streaming(ifs, file_size, crc_offset);

      if (calculated_crc != header.file_crc32) {
        integrity_error.type = dump_format::CRCErrorType::FileCRC;
        integrity_error.message = "CRC32 checksum mismatch";
        StructuredLog()
            .Event("storage_validation_error")
            .Field("type", "crc32_verification_failed")
            .Field("filepath", filepath)
            .Field("expected_crc", static_cast<uint64_t>(header.file_crc32))
            .Field("actual_crc", static_cast<uint64_t>(calculated_crc))
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
      }

      StructuredLog().Event("dump_verification_passed").Field("path", filepath).Field("crc_verified", true).Info();
    } else {
      StructuredLog().Event("dump_verification_passed").Field("path", filepath).Field("crc_verified", false).Info();
    }

    integrity_error.type = dump_format::CRCErrorType::None;
    integrity_error.message = "";
    return {};

  } catch (const std::exception& e) {
    integrity_error.type = dump_format::CRCErrorType::FileCRC;
    integrity_error.message = std::string("Exception during verification: ") + e.what();
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
  }
}

// ============================================================================
// Snapshot File Information
// ============================================================================

Expected<void, Error> GetDumpInfo(const std::string& filepath, DumpInfo& info) {
  try {
    std::ifstream ifs(filepath, std::ios::binary | std::ios::ate);
    if (!ifs) {
      LogStorageError("open_file", filepath, "Failed to open snapshot file");
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Get dump info failed"));
    }

    // Get file size
    info.file_size = static_cast<uint64_t>(ifs.tellg());
    ifs.seekg(0, std::ios::beg);

    // Read and verify magic number
    std::array<char, 4> magic{};
    ifs.read(magic.data(), 4);
    if (!ifs.good() || std::memcmp(magic.data(), dump_format::kMagicNumber.data(), 4) != 0) {
      StructuredLog()
          .Event("storage_validation_error")
          .Field("type", "invalid_magic_number")
          .Field("filepath", filepath)
          .Field("operation", "get_dump_info")
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Get dump info failed"));
    }

    // Read version
    if (!ReadBinary(ifs, info.version)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Get dump info failed"));
    }

    // Version compatibility check
    if (info.version > dump_format::kMaxSupportedVersion) {
      StructuredLog()
          .Event("storage_validation_error")
          .Field("type", "version_too_new")
          .Field("filepath", filepath)
          .Field("operation", "get_dump_info")
          .Field("version", static_cast<uint64_t>(info.version))
          .Field("max_supported", static_cast<uint64_t>(dump_format::kMaxSupportedVersion))
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Get dump info failed"));
    }
    if (info.version < dump_format::kMinSupportedVersion) {
      StructuredLog()
          .Event("storage_validation_error")
          .Field("type", "version_too_old")
          .Field("filepath", filepath)
          .Field("operation", "get_dump_info")
          .Field("version", static_cast<uint64_t>(info.version))
          .Field("min_supported", static_cast<uint64_t>(dump_format::kMinSupportedVersion))
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Get dump info failed"));
    }

    // Read V1 header
    HeaderV1 header;
    if (auto result = ReadHeaderV1(ifs, header); !result) {
      LogStorageError("read_header", filepath, result.error().message());
      return result;
    }

    info.gtid = header.gtid;
    info.flags = header.flags;
    info.timestamp = header.dump_timestamp;
    info.has_statistics = (header.flags & dump_format::flags_v1::kWithStatistics) != 0;

    // Read config section to get table count
    uint32_t config_len = 0;
    if (!ReadBinary(ifs, config_len)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Get dump info failed"));
    }
    if (config_len > 0) {
      ifs.seekg(config_len, std::ios::cur);  // Skip config data
    }

    // Skip statistics section if present
    uint32_t stats_len = 0;
    if (!ReadBinary(ifs, stats_len)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Get dump info failed"));
    }
    if (stats_len > 0) {
      ifs.seekg(stats_len, std::ios::cur);
    }

    // Read table count
    if (!ReadBinary(ifs, info.table_count)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Get dump info failed"));
    }

    return {};

  } catch (const std::exception& e) {
    LogStorageError("get_dump_info_exception", filepath, e.what());
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Get dump info failed"));
  }
}

}  // namespace mygramdb::storage::dump_v1
