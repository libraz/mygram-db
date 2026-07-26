/**
 * @file dump_format_v1_integrity.cpp
 * @brief CRC32 calculation, integrity verification, and dump info for format V1
 */

#include <zlib.h>

#include <array>
#include <cstring>
#include <fstream>
#include <vector>

#include "server/log_field_names.h"
#include "storage/dump_format_internal.h"
#include "storage/dump_format_v1.h"
#include "storage/dump_format_v1_internal.h"
#include "utils/binary_io.h"
#include "utils/structured_log.h"

namespace mygramdb::storage::dump_v1 {

using namespace mygram::utils;
using internal::ReadString;

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
    if (version != static_cast<uint32_t>(dump_format::FormatVersion::V1)) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "V1 integrity API cannot read dump version " + std::to_string(version);
      return MakeUnexpected(MakeError(ErrorCode::kStorageVersionMismatch, integrity_error.message));
    }

    // Read V1 header
    HeaderV1 header;
    if (auto header_result = ReadHeaderV1(ifs, header); !header_result) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "Failed to read V1 header: " + header_result.error().message();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
    }
    if (auto header_result = ValidateHeaderIntegrityFields(header); !header_result) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = header_result.error().message();
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
      const auto crc_offset = static_cast<size_t>(kHeaderFileCRC32Offset);

      uint32_t calculated_crc = dump_internal::CalculateCRC32Streaming(ifs, file_size, crc_offset);

      if (calculated_crc != header.file_crc32) {
        integrity_error.type = dump_format::CRCErrorType::FileCRC;
        integrity_error.message = "CRC32 checksum mismatch";
        StructuredLog()
            .Event("storage_validation_error")
            .Field("type", "crc32_verification_failed")
            .Field(server::log_fields::kFieldFilepath, filepath)
            .Field("expected_crc", static_cast<uint64_t>(header.file_crc32))
            .Field("actual_crc", static_cast<uint64_t>(calculated_crc))
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
      }

      StructuredLog()
          .Event("dump_verification_passed")
          .Field(server::log_fields::kFieldFilepath, filepath)
          .Field("crc_verified", true)
          .Info();
    } else {
      StructuredLog()
          .Event("dump_verification_passed")
          .Field(server::log_fields::kFieldFilepath, filepath)
          .Field("crc_verified", false)
          .Info();
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
          .Field(server::log_fields::kFieldFilepath, filepath)
          .Field(server::log_fields::kFieldOperation, "get_dump_info")
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
          .Field(server::log_fields::kFieldFilepath, filepath)
          .Field(server::log_fields::kFieldOperation, "get_dump_info")
          .Field("version", static_cast<uint64_t>(info.version))
          .Field("max_supported", static_cast<uint64_t>(dump_format::kMaxSupportedVersion))
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Get dump info failed"));
    }
    if (info.version < dump_format::kMinSupportedVersion) {
      StructuredLog()
          .Event("storage_validation_error")
          .Field("type", "version_too_old")
          .Field(server::log_fields::kFieldFilepath, filepath)
          .Field(server::log_fields::kFieldOperation, "get_dump_info")
          .Field("version", static_cast<uint64_t>(info.version))
          .Field("min_supported", static_cast<uint64_t>(dump_format::kMinSupportedVersion))
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Get dump info failed"));
    }
    if (info.version != static_cast<uint32_t>(dump_format::FormatVersion::V1)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageVersionMismatch,
                                      "V1 dump info API cannot read dump version " + std::to_string(info.version)));
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
