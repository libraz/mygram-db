/**
 * @file dump_format_v1.cpp
 * @brief Dump file format Version 1 implementation - header, statistics, and read/write
 */

#include "storage/dump_format_v1.h"

#include <spdlog/spdlog.h>
#include <zlib.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <thread>
#include <vector>

#include "storage/dump_format_v1_internal.h"
#include "utils/atomic_file_writer.h"
#include "utils/binary_io.h"
#include "utils/structured_log.h"

#ifdef _WIN32
#include <io.h>
#define CHMOD _chmod
#else
#include <fcntl.h>
#include <sys/fcntl.h>  // For O_NOFOLLOW on macOS
#include <sys/stat.h>
#include <unistd.h>
#define CHMOD chmod

// Ensure O_NOFOLLOW is defined (standard on POSIX systems)
#ifndef O_NOFOLLOW
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage): Platform compatibility constant for symlink protection
#define O_NOFOLLOW 0x00000100
#endif
#endif

namespace mygramdb::storage::dump_v1 {

using namespace mygram::utils;
using internal::ReadString;
using internal::WriteString;

// Forward declaration for streaming CRC calculation (defined in dump_format_v1_integrity.cpp)
uint32_t CalculateCRC32Streaming(std::ifstream& ifs, uint64_t file_size, size_t crc_offset);

// ============================================================================
// Header V1 Serialization
// ============================================================================

Expected<void, Error> WriteHeaderV1(std::ostream& output_stream, const HeaderV1& header) {
  if (!WriteBinary(output_stream, header.header_size)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write header size"));
  }
  if (!WriteBinary(output_stream, header.flags)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write header flags"));
  }
  if (!WriteBinary(output_stream, header.dump_timestamp)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write dump timestamp"));
  }
  if (!WriteBinary(output_stream, header.total_file_size)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write total file size"));
  }
  if (!WriteBinary(output_stream, header.file_crc32)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write file CRC32"));
  }
  if (!WriteString(output_stream, header.gtid)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write GTID"));
  }
  return {};
}

Expected<void, Error> ReadHeaderV1(std::istream& input_stream, HeaderV1& header) {
  if (!ReadBinary(input_stream, header.header_size)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read header size"));
  }
  if (!ReadBinary(input_stream, header.flags)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read header flags"));
  }
  if (!ReadBinary(input_stream, header.dump_timestamp)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read dump timestamp"));
  }
  if (!ReadBinary(input_stream, header.total_file_size)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read total file size"));
  }
  if (!ReadBinary(input_stream, header.file_crc32)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read file CRC32"));
  }
  if (!ReadString(input_stream, header.gtid, kMaxPathLength)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read GTID"));
  }
  return {};
}

// ============================================================================
// Statistics Serialization
// ============================================================================

Expected<void, Error> SerializeStatistics(std::ostream& output_stream, const DumpStatistics& stats) {
  if (!WriteBinary(output_stream, stats.total_documents)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write total_documents"));
  }
  if (!WriteBinary(output_stream, stats.total_terms)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write total_terms"));
  }
  if (!WriteBinary(output_stream, stats.total_index_bytes)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write total_index_bytes"));
  }
  if (!WriteBinary(output_stream, stats.total_docstore_bytes)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write total_docstore_bytes"));
  }
  if (!WriteBinary(output_stream, stats.dump_time_ms)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write dump_time_ms"));
  }
  return {};
}

Expected<void, Error> DeserializeStatistics(std::istream& input_stream, DumpStatistics& stats) {
  if (!ReadBinary(input_stream, stats.total_documents)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read total_documents"));
  }
  if (!ReadBinary(input_stream, stats.total_terms)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read total_terms"));
  }
  if (!ReadBinary(input_stream, stats.total_index_bytes)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read total_index_bytes"));
  }
  if (!ReadBinary(input_stream, stats.total_docstore_bytes)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read total_docstore_bytes"));
  }
  if (!ReadBinary(input_stream, stats.dump_time_ms)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read dump_time_ms"));
  }
  return {};
}

Expected<void, Error> SerializeTableStatistics(std::ostream& output_stream, const TableStatistics& stats) {
  if (!WriteBinary(output_stream, stats.document_count)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write document_count"));
  }
  if (!WriteBinary(output_stream, stats.term_count)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write term_count"));
  }
  if (!WriteBinary(output_stream, stats.index_bytes)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write index_bytes"));
  }
  if (!WriteBinary(output_stream, stats.docstore_bytes)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write docstore_bytes"));
  }
  if (!WriteBinary(output_stream, stats.next_doc_id)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write next_doc_id"));
  }
  if (!WriteBinary(output_stream, stats.last_update_time)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write last_update_time"));
  }
  return {};
}

Expected<void, Error> DeserializeTableStatistics(std::istream& input_stream, TableStatistics& stats) {
  if (!ReadBinary(input_stream, stats.document_count)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read document_count"));
  }
  if (!ReadBinary(input_stream, stats.term_count)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read term_count"));
  }
  if (!ReadBinary(input_stream, stats.index_bytes)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read index_bytes"));
  }
  if (!ReadBinary(input_stream, stats.docstore_bytes)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read docstore_bytes"));
  }
  if (!ReadBinary(input_stream, stats.next_doc_id)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read next_doc_id"));
  }
  if (!ReadBinary(input_stream, stats.last_update_time)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read last_update_time"));
  }
  return {};
}

// ============================================================================
// Complete Snapshot Read/Write (Version 1)
// ============================================================================

Expected<void, Error> WriteDumpV1(
    const std::string& filepath, const std::string& gtid, const config::Config& config,
    const std::unordered_map<std::string, std::pair<index::Index*, DocumentStore*>>& table_contexts,
    const DumpStatistics* stats, const std::unordered_map<std::string, TableStatistics>* table_stats) {
  // Atomic write strategy (BUG-0077):
  // 1. Write to temporary file with unique suffix to avoid concurrent write collisions
  // 2. fsync the temporary file
  // 3. Atomically rename to final path
  // This ensures the original file is never corrupted during write failures.

  // Generate unique temp filename to avoid collisions with concurrent writes
  AtomicFileWriter writer(filepath, true);  // true = unique PID+random suffix
  const auto& temp_filepath = writer.GetTempPath();

  try {
    // Ensure parent directory exists
    std::filesystem::path file_path(filepath);
    std::filesystem::path parent_dir = file_path.parent_path();

    if (!parent_dir.empty() && !std::filesystem::exists(parent_dir)) {
      std::error_code error_code;
      if (!std::filesystem::create_directories(parent_dir, error_code)) {
        LogStorageError("create_directory", parent_dir.string(), error_code.message());
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }
      StructuredLog().Event("dump_directory_created").Field("path", parent_dir.string()).Info();
    }

#ifndef _WIN32
    // SECURITY: Validate dump directory exists and is not itself a symlink
    // Note: We allow symlinks in parent paths (like /var -> /private/var on macOS)
    // but not in the final directory component
    if (!parent_dir.empty() && std::filesystem::exists(parent_dir)) {
      if (std::filesystem::is_symlink(parent_dir)) {
        StructuredLog()
            .Event("storage_security_error")
            .Field("type", "symlink_directory")
            .Field("filepath", parent_dir.string())
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }
    }

    // SECURITY: Check if final path is a symlink before opening
    std::error_code error_code;
    // SECURITY: Reject symlinks regardless of whether target exists (prevent TOCTOU attacks)
    // Note: is_symlink() uses symlink_status() which checks the link itself, not the target
    if (std::filesystem::is_symlink(filepath, error_code)) {
      StructuredLog().Event("storage_security_error").Field("type", "symlink_file").Field("filepath", filepath).Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    // Remove any existing temp file
    std::filesystem::remove(temp_filepath, error_code);

    // SECURITY: Open temp file with O_NOFOLLOW to prevent symlink attacks (TOCTOU protection)
    // O_CREAT | O_EXCL: Fail if file already exists (atomic creation)
    // O_NOFOLLOW: Fail if the file is a symbolic link
    // S_IRUSR | S_IWUSR: Set permissions to 600 (rw-------)
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): POSIX open() requires varargs for mode
    int file_descriptor = open(temp_filepath.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, S_IRUSR | S_IWUSR);
    if (file_descriptor < 0) {
      LogStorageError("create_temp_file", temp_filepath, std::strerror(errno));
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    // Verify ownership (file must be owned by current process user)
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init): stat struct is filled by fstat()
    struct stat file_stat {};
    if (fstat(file_descriptor, &file_stat) != 0 || file_stat.st_uid != geteuid()) {
      close(file_descriptor);
      StructuredLog()
          .Event("storage_security_error")
          .Field("type", "ownership_verification_failed")
          .Field("filepath", temp_filepath)
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    // Create ofstream from file descriptor
    // Note: We need to close file_descriptor manually or use __gnu_cxx::stdio_filebuf on Linux
    // For simplicity, we'll close file_descriptor and reopen with ofstream since ownership is verified
    close(file_descriptor);

    std::ofstream ofs(temp_filepath, std::ios::binary | std::ios::trunc | std::ios::out);
    if (!ofs) {
      LogStorageError("open_temp_file", temp_filepath, "Failed to open for writing");
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
#else
    // Windows: Use standard file opening (symlink attacks less common on Windows)
    // Remove any existing temp file
    std::error_code error_code;
    std::filesystem::remove(temp_filepath, error_code);

    std::ofstream ofs(temp_filepath, std::ios::binary | std::ios::trunc);
    if (!ofs) {
      LogStorageError("open_temp_file", temp_filepath, "Failed to open for writing");
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
#endif

    // Write fixed file header
    ofs.write(dump_format::kMagicNumber.data(), 4);
    auto version = static_cast<uint32_t>(dump_format::FormatVersion::V1);
    if (!WriteBinary(ofs, version)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    // Prepare Version 1 header
    HeaderV1 header;
    header.header_size = 0;  // Will be calculated
    header.flags = dump_format::flags_v1::kNone;
    if (stats != nullptr) {
      header.flags |= dump_format::flags_v1::kWithStatistics;
    }
    header.dump_timestamp = static_cast<uint64_t>(std::time(nullptr));
    header.gtid = gtid;

    // Write V1 header
    if (auto result = WriteHeaderV1(ofs, header); !result) {
      LogStorageError("write_header", temp_filepath, result.error().message());
      return result;
    }

    // Write config section
    std::ostringstream config_stream;
    if (auto result = SerializeConfig(config_stream, config); !result) {
      LogStorageError("serialize_config", temp_filepath, result.error().message());
      return result;
    }
    std::string config_data = config_stream.str();
    auto config_len = static_cast<uint32_t>(config_data.size());
    if (!WriteBinary(ofs, config_len)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
    ofs.write(config_data.data(), static_cast<std::streamsize>(config_len));

    // Write statistics section (if present)
    if (stats != nullptr) {
      std::ostringstream stats_stream;
      if (auto result = SerializeStatistics(stats_stream, *stats); !result) {
        LogStorageError("serialize_statistics", temp_filepath, result.error().message());
        return result;
      }
      std::string stats_data = stats_stream.str();
      auto stats_len = static_cast<uint32_t>(stats_data.size());
      if (!WriteBinary(ofs, stats_len)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }
      ofs.write(stats_data.data(), static_cast<std::streamsize>(stats_len));
    } else {
      uint32_t stats_len = 0;
      if (!WriteBinary(ofs, stats_len)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }
    }

    // Write table data section
    auto table_count = static_cast<uint32_t>(table_contexts.size());
    if (!WriteBinary(ofs, table_count)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    for (const auto& [table_name, ctx_pair] : table_contexts) {
      index::Index* index = ctx_pair.first;
      DocumentStore* doc_store = ctx_pair.second;

      // Write table name
      if (!WriteString(ofs, table_name)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }

      // Write table statistics (if present)
      if (table_stats != nullptr && table_stats->count(table_name) > 0) {
        std::ostringstream table_stats_stream;
        if (auto result = SerializeTableStatistics(table_stats_stream, table_stats->at(table_name)); !result) {
          StructuredLog()
              .Event("storage_error")
              .Field("operation", "serialize_table_statistics")
              .Field("filepath", temp_filepath)
              .Field("table", table_name)
              .Field("error", result.error().message())
              .Error();
          return result;
        }
        std::string table_stats_data = table_stats_stream.str();
        auto table_stats_len = static_cast<uint32_t>(table_stats_data.size());
        if (!WriteBinary(ofs, table_stats_len)) {
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
        }
        ofs.write(table_stats_data.data(), static_cast<std::streamsize>(table_stats_len));
      } else {
        uint32_t table_stats_len = 0;
        if (!WriteBinary(ofs, table_stats_len)) {
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
        }
      }

      // Save index directly to stringstream
      std::ostringstream index_stream;
      if (!index->SaveToStream(index_stream)) {
        StructuredLog()
            .Event("storage_error")
            .Field("operation", "save_index")
            .Field("filepath", temp_filepath)
            .Field("table", table_name)
            .Field("error", "SaveToStream failed")
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }

      std::string index_data = index_stream.str();
      auto index_len = static_cast<uint64_t>(index_data.size());
      if (!WriteBinary(ofs, index_len)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }
      ofs.write(index_data.data(), static_cast<std::streamsize>(index_len));

      // Save document store directly to stringstream
      std::ostringstream doc_stream;
      if (auto result = doc_store->SaveToStream(doc_stream, ""); !result) {
        StructuredLog()
            .Event("storage_error")
            .Field("operation", "save_documents")
            .Field("filepath", temp_filepath)
            .Field("table", table_name)
            .Field("error", result.error().message())
            .Error();
        return result;
      }

      std::string doc_data = doc_stream.str();
      auto doc_len = static_cast<uint64_t>(doc_data.size());
      if (!WriteBinary(ofs, doc_len)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }
      ofs.write(doc_data.data(), static_cast<std::streamsize>(doc_len));

      StructuredLog().Event("dump_table_saved").Field("table", table_name).Debug();
    }

    ofs.close();
    if (!ofs.good()) {
      LogStorageError("write_dump", temp_filepath, "Stream error during write");
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    // Calculate file size
    std::ifstream ifs_size(temp_filepath, std::ios::binary | std::ios::ate);
    if (!ifs_size) {
      LogStorageError("reopen_file", temp_filepath, "Failed to reopen for size calculation");
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
    uint64_t file_size = static_cast<uint64_t>(ifs_size.tellg());
    ifs_size.close();

    // Update total_file_size in the header
    {
      std::fstream update_stream1(temp_filepath, std::ios::in | std::ios::out | std::ios::binary);
      if (!update_stream1) {
        LogStorageError("open_file", temp_filepath, "Failed to open for header update");
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }

      // Seek to total_file_size position (after magic + version + header_size + flags + timestamp)
      const std::streamoff file_size_offset = 4 + 4 + 4 + 4 + 8;
      update_stream1.seekp(file_size_offset);
      if (!WriteBinary(update_stream1, file_size)) {
        LogStorageError("write_header_field", temp_filepath, "Failed to write total_file_size");
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }
      update_stream1.close();
    }

    // Calculate CRC32 of the file
    std::ifstream ifs(temp_filepath, std::ios::binary);
    if (!ifs) {
      LogStorageError("reopen_file", temp_filepath, "Failed to reopen for CRC calculation");
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    // CRC field is at offset: magic(4) + version(4) + header_size(4) + flags(4) + timestamp(8) + total_file_size(8)
    const size_t crc_offset = 4 + 4 + 4 + 4 + 8 + 8;

    uint32_t calculated_crc = CalculateCRC32Streaming(ifs, file_size, crc_offset);
    ifs.close();

    // Update header with CRC
    {
      std::fstream update_stream2(temp_filepath, std::ios::in | std::ios::out | std::ios::binary);
      if (!update_stream2) {
        LogStorageError("open_file", temp_filepath, "Failed to open for CRC update");
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }

      // Seek to file_crc32 position (right after total_file_size)
      const std::streamoff file_size_offset = 4 + 4 + 4 + 4 + 8;
      const std::streamoff crc_file_offset = file_size_offset + 8;
      update_stream2.seekp(crc_file_offset);
      if (!WriteBinary(update_stream2, calculated_crc)) {
        LogStorageError("write_header_field", temp_filepath, "Failed to write file_crc32");
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }

      update_stream2.close();
      if (!update_stream2.good()) {
        LogStorageError("update_header", temp_filepath, "Stream error during header update");
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }
    }

    // Atomic commit: fsync temp file, rename to final path, fsync directory
    if (auto result = writer.Commit(); !result) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    StructuredLog()
        .Event("dump_saved_atomically")
        .Field("filepath", filepath)
        .Field("crc32", static_cast<uint64_t>(calculated_crc))
        .Field("file_size", file_size)
        .Info();

    return {};

  } catch (const std::exception& e) {
    // writer destructor will clean up temp file
    LogStorageError("write_dump_exception", filepath, e.what());
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
  }
}

Expected<void, Error> ReadDumpV1(
    const std::string& filepath, std::string& gtid, config::Config& config,
    std::unordered_map<std::string, std::pair<index::Index*, DocumentStore*>>& table_contexts, DumpStatistics* stats,
    std::unordered_map<std::string, TableStatistics>* table_stats, dump_format::IntegrityError* integrity_error) {
  try {
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs) {
      LogStorageError("open_file", filepath, "Failed to open for reading");
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
    }

    // Read and verify fixed file header
    std::array<char, 4> magic{};
    ifs.read(magic.data(), 4);
    if (std::memcmp(magic.data(), dump_format::kMagicNumber.data(), 4) != 0) {
      StructuredLog()
          .Event("storage_validation_error")
          .Field("type", "invalid_magic_number")
          .Field("filepath", filepath)
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
    }

    uint32_t version = 0;
    if (!ReadBinary(ifs, version)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
    }

    // Version compatibility check
    if (version > dump_format::kMaxSupportedVersion) {
      StructuredLog()
          .Event("storage_validation_error")
          .Field("type", "version_too_new")
          .Field("filepath", filepath)
          .Field("version", static_cast<uint64_t>(version))
          .Field("max_supported", static_cast<uint64_t>(dump_format::kMaxSupportedVersion))
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
    }
    if (version < dump_format::kMinSupportedVersion) {
      StructuredLog()
          .Event("storage_validation_error")
          .Field("type", "version_too_old")
          .Field("filepath", filepath)
          .Field("version", static_cast<uint64_t>(version))
          .Field("min_supported", static_cast<uint64_t>(dump_format::kMinSupportedVersion))
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
    }

    // Currently only V1 is implemented
    if (version != static_cast<uint32_t>(dump_format::FormatVersion::V1)) {
      StructuredLog()
          .Event("storage_validation_error")
          .Field("type", "version_not_implemented")
          .Field("filepath", filepath)
          .Field("version", static_cast<uint64_t>(version))
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
    }

    // Read V1 header
    HeaderV1 header;
    if (auto result = ReadHeaderV1(ifs, header); !result) {
      LogStorageError("read_header", filepath, result.error().message());
      return result;
    }
    gtid = header.gtid;

    // Verify file size if specified
    if (header.total_file_size > 0) {
      std::streampos saved_pos = ifs.tellg();
      ifs.seekg(0, std::ios::end);
      auto actual_size = static_cast<uint64_t>(ifs.tellg());
      ifs.seekg(saved_pos);  // Restore position

      if (actual_size != header.total_file_size) {
        StructuredLog()
            .Event("storage_validation_error")
            .Field("type", "file_size_mismatch")
            .Field("filepath", filepath)
            .Field("expected_size", header.total_file_size)
            .Field("actual_size", actual_size)
            .Error();
        if (integrity_error != nullptr) {
          integrity_error->type = dump_format::CRCErrorType::FileCRC;
          integrity_error->message = "File size mismatch";
        }
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
      }
    }

    // Verify CRC32 if specified
    // Use streaming CRC to avoid loading entire file into memory (prevents OOM for large files)
    if (header.file_crc32 != 0) {
      // Save current position
      std::streampos current_pos = ifs.tellg();

      // Get file size
      ifs.seekg(0, std::ios::end);
      auto file_size = static_cast<uint64_t>(ifs.tellg());

      // CRC field offset: magic + version + header_size + flags + timestamp + total_file_size
      const size_t crc_offset = 4 + 4 + 4 + 4 + 8 + 8;

      uint32_t calculated_crc = CalculateCRC32Streaming(ifs, file_size, crc_offset);

      if (calculated_crc != header.file_crc32) {
        StructuredLog()
            .Event("storage_validation_error")
            .Field("type", "crc32_mismatch")
            .Field("filepath", filepath)
            .Field("expected_crc", static_cast<uint64_t>(header.file_crc32))
            .Field("actual_crc", static_cast<uint64_t>(calculated_crc))
            .Error();
        if (integrity_error != nullptr) {
          integrity_error->type = dump_format::CRCErrorType::FileCRC;
          integrity_error->message = "CRC32 checksum mismatch";
        }
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
      }

      StructuredLog().Event("dump_crc_verified").Field("crc32", static_cast<uint64_t>(calculated_crc)).Debug();

      // Restore file position
      ifs.seekg(current_pos);
    }

    // Read config section
    uint32_t config_len = 0;
    if (!ReadBinary(ifs, config_len)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
    }
    std::string config_data(config_len, '\0');
    ifs.read(config_data.data(), static_cast<std::streamsize>(config_len));
    std::istringstream config_stream(config_data);
    if (auto result = DeserializeConfig(config_stream, config); !result) {
      LogStorageError("deserialize_config", filepath, result.error().message());
      return result;
    }

    // Read statistics section
    uint32_t stats_len = 0;
    if (!ReadBinary(ifs, stats_len)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
    }
    if (stats_len > 0 && stats != nullptr) {
      std::string stats_data(stats_len, '\0');
      ifs.read(stats_data.data(), static_cast<std::streamsize>(stats_len));
      std::istringstream stats_stream(stats_data);
      if (auto result = DeserializeStatistics(stats_stream, *stats); !result) {
        LogStorageError("deserialize_statistics", filepath, result.error().message());
        return result;
      }
    } else if (stats_len > 0) {
      // Skip statistics if not requested
      ifs.seekg(stats_len, std::ios::cur);
    }

    // Read table data section
    uint32_t table_count = 0;
    if (!ReadBinary(ifs, table_count)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
    }

    for (uint32_t i = 0; i < table_count; ++i) {
      std::string table_name;
      if (!ReadString(ifs, table_name, kMaxIdentifierLength)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
      }

      // Read table statistics
      uint32_t table_stats_len = 0;
      if (!ReadBinary(ifs, table_stats_len)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
      }
      if (table_stats_len > 0 && table_stats != nullptr) {
        std::string table_stats_data(table_stats_len, '\0');
        ifs.read(table_stats_data.data(), static_cast<std::streamsize>(table_stats_len));
        std::istringstream table_stats_stream(table_stats_data);
        TableStatistics table_stat;
        if (auto result = DeserializeTableStatistics(table_stats_stream, table_stat); !result) {
          StructuredLog()
              .Event("storage_error")
              .Field("operation", "deserialize_table_statistics")
              .Field("filepath", filepath)
              .Field("table", table_name)
              .Field("error", result.error().message())
              .Error();
          return result;
        }
        (*table_stats)[table_name] = table_stat;
      } else if (table_stats_len > 0) {
        // Skip table statistics if not requested
        ifs.seekg(table_stats_len, std::ios::cur);
      }

      // Check if table context exists
      if (table_contexts.count(table_name) == 0) {
        StructuredLog()
            .Event("storage_warning")
            .Field("type", "table_not_found")
            .Field("operation", "load_dump")
            .Field("filepath", filepath)
            .Field("table", table_name)
            .Warn();
        // Skip index and document data
        uint64_t index_len = 0;
        if (!ReadBinary(ifs, index_len)) {
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
        }
        ifs.seekg(static_cast<std::streamoff>(index_len), std::ios::cur);

        uint64_t doc_len = 0;
        if (!ReadBinary(ifs, doc_len)) {
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
        }
        ifs.seekg(static_cast<std::streamoff>(doc_len), std::ios::cur);
        continue;
      }

      auto& ctx_pair = table_contexts[table_name];
      index::Index* index = ctx_pair.first;
      DocumentStore* doc_store = ctx_pair.second;

      // Read index data
      uint64_t index_len = 0;
      if (!ReadBinary(ifs, index_len)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
      }

      if (index_len > 0) {
        // Standard mode: load index from dump
        std::string index_data(index_len, '\0');
        ifs.read(index_data.data(), static_cast<std::streamsize>(index_len));

        // Load index directly from stringstream
        std::istringstream index_stream(index_data);
        if (!index->LoadFromStream(index_stream)) {
          StructuredLog()
              .Event("storage_error")
              .Field("operation", "load_index")
              .Field("filepath", filepath)
              .Field("table", table_name)
              .Field("error", "LoadFromStream failed")
              .Error();
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
        }
      } else {
        StructuredLog()
            .Event("storage_validation_error")
            .Field("type", "invalid_index_length")
            .Field("filepath", filepath)
            .Field("table", table_name)
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
      }

      // Read document store data
      uint64_t doc_len = 0;
      if (!ReadBinary(ifs, doc_len)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
      }
      std::string doc_data(doc_len, '\0');
      ifs.read(doc_data.data(), static_cast<std::streamsize>(doc_len));

      // Load document store directly from stringstream
      std::istringstream doc_stream(doc_data);
      if (auto result = doc_store->LoadFromStream(doc_stream, nullptr); !result) {
        StructuredLog()
            .Event("storage_error")
            .Field("operation", "load_documents")
            .Field("filepath", filepath)
            .Field("table", table_name)
            .Field("error", result.error().message())
            .Error();
        return result;
      }

      StructuredLog().Event("dump_table_loaded").Field("table", table_name).Info();
    }

    return {};

  } catch (const std::exception& e) {
    LogStorageError("read_dump_exception", filepath, e.what());
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
  }
}

}  // namespace mygramdb::storage::dump_v1
