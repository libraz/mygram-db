/**
 * @file dump_format_v1.cpp
 * @brief Dump file format Version 1 implementation
 */

#include "storage/dump_format_v1.h"

#include <spdlog/spdlog.h>
#include <zlib.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <limits>
#include <memory>
#include <sstream>
#include <thread>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "server/log_field_names.h"
#include "storage/dump_format_internal.h"
#include "storage/dump_format_v1_internal.h"
#include "storage/dump_load_access.h"
#include "utils/atomic_file_writer.h"
#include "utils/binary_io.h"
#include "utils/fd_guard.h"
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

namespace {

// Use shared WriteBinary/ReadBinary from utils/binary_io.h
using dump_internal::ApplyPendingTableLoads;
using dump_internal::BoundedInputStream;
using dump_internal::LoadPendingDocumentStore;
using dump_internal::LoadPendingIndex;
using dump_internal::PendingTableLoad;
using dump_internal::ValidateDumpTableSet;
using internal::ReadString;
using internal::WriteString;
using mygram::utils::ReadBinary;
using mygram::utils::WriteBinary;

#ifndef _WIN32
template <typename WritePayload>
Expected<void, Error> WriteSizedPayloadToFd(std::ostream& output_stream, int file_descriptor,
                                            WritePayload&& write_payload) {
  output_stream.flush();
  if (!output_stream.good()) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
  }

  off_t length_offset = lseek(file_descriptor, 0, SEEK_CUR);
  if (length_offset < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to locate payload length"));
  }

  uint64_t payload_length = 0;
  if (!WriteBinary(output_stream, payload_length)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
  }
  output_stream.flush();

  off_t payload_start = lseek(file_descriptor, 0, SEEK_CUR);
  if (payload_start < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to locate payload start"));
  }

  if (auto result = write_payload(output_stream); !result) {
    return result;
  }
  output_stream.flush();

  off_t payload_end = lseek(file_descriptor, 0, SEEK_CUR);
  if (payload_end < payload_start) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to locate payload end"));
  }
  payload_length = static_cast<uint64_t>(payload_end - payload_start);
  if (!dump_internal::WriteBinaryAt(file_descriptor, payload_length, length_offset)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write payload length"));
  }
  return {};
}

#endif

Expected<uint64_t, Error> BytesRemaining(std::istream& input_stream, uint64_t file_size) {
  const std::streampos position = input_stream.tellg();
  if (position < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to locate V1 section"));
  }
  const uint64_t offset = static_cast<uint64_t>(position);
  if (offset > file_size) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "V1 section starts past end of dump file"));
  }
  return file_size - offset;
}

Expected<void, Error> ValidateSectionLength(std::istream& input_stream, uint64_t file_size, uint64_t length,
                                            const RestoreLimits& restore_limits, const std::string& section_name,
                                            uint64_t staged_memory_bytes = 0) {
  auto remaining = BytesRemaining(input_stream, file_size);
  if (!remaining) {
    return MakeUnexpected(remaining.error());
  }
  if (length > *remaining) {
    return MakeUnexpected(
        MakeError(ErrorCode::kStorageDumpReadError, section_name + " length exceeds bytes remaining in V1 dump file"));
  }
  if (length > restore_limits.max_section_bytes) {
    return MakeUnexpected(
        MakeError(ErrorCode::kStorageDumpReadError,
                  section_name + " length exceeds configured restore section limit (dump.restore_max_section_mb)"));
  }
  if (staged_memory_bytes > restore_limits.memory_budget_bytes ||
      length > restore_limits.memory_budget_bytes - staged_memory_bytes) {
    return MakeUnexpected(
        MakeError(ErrorCode::kStorageDumpReadError,
                  section_name + " exceeds configured V1 restore memory budget (dump.restore_memory_budget_mb)"));
  }
  return {};
}

}  // namespace

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

uint32_t ExpectedHeaderSizeV1(const HeaderV1& header) {
  return static_cast<uint32_t>(4 + 4 + 8 + 8 + 4 + 4 + header.gtid.size());
}

Expected<void, Error> ValidateHeaderIntegrityFields(const HeaderV1& header) {
  const uint32_t expected_header_size = ExpectedHeaderSizeV1(header);
  // Releases up to and including v1.5.3 emitted this field as a literal zero
  // and never patched it, so zero marks such a release rather than a wrong
  // length. The V1 header layout is fixed, so parsing continues at the offset
  // the layout implies. Any other value still has to be exact.
  if (header.header_size != kUnrecordedHeaderSizeV1 && header.header_size != expected_header_size) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError,
                                    "Invalid V1 header size: expected " + std::to_string(expected_header_size) +
                                        ", got " + std::to_string(header.header_size)));
  }
  if (header.total_file_size == 0) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Invalid V1 header: total_file_size is zero"));
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
  // Atomic write strategy:
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
      StructuredLog()
          .Event("dump_directory_created")
          .Field(server::log_fields::kFieldFilepath, parent_dir.string())
          .Info();
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
    int file_descriptor = open(temp_filepath.c_str(), O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW, S_IRUSR | S_IWUSR);
    if (file_descriptor < 0) {
      LogStorageError("create_temp_file", temp_filepath, std::strerror(errno));
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
    FDGuard fd_guard(file_descriptor);

    // Verify ownership (file must be owned by current process user)
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init): stat struct is filled by fstat()
    struct stat file_stat {};
    if (fstat(file_descriptor, &file_stat) != 0 || file_stat.st_uid != geteuid()) {
      StructuredLog()
          .Event("storage_security_error")
          .Field("type", "ownership_verification_failed")
          .Field("filepath", temp_filepath)
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    // SECURITY: Use the already-open file descriptor directly to prevent TOCTOU attacks.
    // Closing the fd and reopening by path would allow an attacker to replace the file
    // with a symlink between close() and reopen().
    dump_internal::FdStreambuf fd_streambuf(file_descriptor);
    std::ostream ofs(&fd_streambuf);
    if (!ofs) {
      LogStorageError("create_stream", temp_filepath, "Failed to create stream from file descriptor");
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
    // Calculate actual header size: header_size(4) + flags(4) + dump_timestamp(8) +
    // total_file_size(8) + file_crc32(4) + gtid_length(4) + gtid_data(N)
    header.header_size = static_cast<uint32_t>(4 + 4 + 8 + 8 + 4 + 4 + gtid.size());
    header.flags = dump_format::flags_v1::kNone | dump_format::flags_v1::kWithCRC;
    header.flags |= dump_format::flags_v1::kHasCompatibilityMetadata;
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
    if (auto result = SerializeCompatibilityMetadata(config_stream, config); !result) {
      LogStorageError("serialize_compatibility_metadata", temp_filepath, result.error().message());
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

#ifndef _WIN32
      if (auto result = WriteSizedPayloadToFd(ofs, file_descriptor,
                                              [&](std::ostream& stream) -> Expected<void, Error> {
                                                if (auto index_result = index->SaveToStream(stream); !index_result) {
                                                  StructuredLog()
                                                      .Event("storage_error")
                                                      .Field("operation", "save_index")
                                                      .Field("filepath", temp_filepath)
                                                      .Field("table", table_name)
                                                      .Field("error", index_result.error().message())
                                                      .Error();
                                                  return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError,
                                                                                  "Write operation failed"));
                                                }
                                                return {};
                                              });
          !result) {
        return result;
      }
#else
      // Save index to stringstream where fd-based length patching is unavailable.
      std::ostringstream index_stream;
      if (auto index_result = index->SaveToStream(index_stream); !index_result) {
        StructuredLog()
            .Event("storage_error")
            .Field("operation", "save_index")
            .Field("filepath", temp_filepath)
            .Field("table", table_name)
            .Field("error", index_result.error().message())
            .Error();
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }

      std::string index_data = index_stream.str();
      auto index_len = static_cast<uint64_t>(index_data.size());
      if (!WriteBinary(ofs, index_len)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }
      ofs.write(index_data.data(), static_cast<std::streamsize>(index_len));
#endif

#ifndef _WIN32
      if (auto result = WriteSizedPayloadToFd(
              ofs, file_descriptor,
              [&](std::ostream& stream) -> Expected<void, Error> {
                if (auto doc_result = doc_store->SaveToStream(stream, ""); !doc_result) {
                  StructuredLog()
                      .Event("storage_error")
                      .Field("operation", "save_documents")
                      .Field("filepath", temp_filepath)
                      .Field("table", table_name)
                      .Field("error", doc_result.error().message())
                      .Error();
                  return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
                }
                return {};
              });
          !result) {
        return result;
      }
#else
      // Save document store to stringstream where fd-based length patching is unavailable.
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
#endif

      StructuredLog().Event("dump_table_saved").Field("table", table_name).Debug();
    }

    ofs.flush();
    if (!ofs.good()) {
      LogStorageError("write_dump", temp_filepath, "Stream error during write");
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
    // Calculate file size from the verified fd and patch header fields in place.
    // Reopening temp_filepath by name here would reintroduce a TOCTOU window.
    if (fstat(file_descriptor, &file_stat) != 0) {
      LogStorageError("stat_temp_file", temp_filepath, std::strerror(errno));
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
    uint64_t file_size = static_cast<uint64_t>(file_stat.st_size);

    // Update total_file_size in the header
    if (!dump_internal::WriteBinaryAt(file_descriptor, file_size, kHeaderTotalFileSizeOffset)) {
      LogStorageError("write_header_field", temp_filepath, "Failed to write total_file_size");
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    // Calculate CRC32 of the file
    const size_t crc_offset = static_cast<size_t>(kHeaderFileCRC32Offset);
    auto crc_result = dump_internal::CalculateCRC32StreamingFd(file_descriptor, file_size, crc_offset);
    if (!crc_result) {
      LogStorageError("calculate_crc32", temp_filepath, crc_result.error().message());
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
    uint32_t calculated_crc = crc_result.value();

    // Update header with CRC
    if (!dump_internal::WriteBinaryAt(file_descriptor, calculated_crc, kHeaderFileCRC32Offset)) {
      LogStorageError("write_header_field", temp_filepath, "Failed to write file_crc32");
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
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
    std::unordered_map<std::string, TableStatistics>* table_stats, dump_format::IntegrityError* integrity_error,
    const DumpConfigValidationCallback& config_validator, const RestoreLimits& restore_limits) {
  try {
    if (restore_limits.memory_budget_bytes == 0 || restore_limits.max_section_bytes == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "V1 restore limits must be greater than zero"));
    }
    std::string loaded_gtid;
    config::Config loaded_config;
    DumpStatistics loaded_stats;
    std::unordered_map<std::string, TableStatistics> loaded_table_stats;
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs) {
      LogStorageError("open_file", filepath, "Failed to open for reading");
      return MakeUnexpected(
          MakeError(ErrorCode::kStorageDumpReadError, "Failed to open dump file for reading", filepath));
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
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Invalid magic number in dump file", filepath));
    }

    uint32_t version = 0;
    if (!ReadBinary(ifs, version)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read dump file version", filepath));
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
      return MakeUnexpected(MakeError(ErrorCode::kStorageVersionMismatch, "Dump file version too new", filepath));
    }
    if (version < dump_format::kMinSupportedVersion) {
      StructuredLog()
          .Event("storage_validation_error")
          .Field("type", "version_too_old")
          .Field("filepath", filepath)
          .Field("version", static_cast<uint64_t>(version))
          .Field("min_supported", static_cast<uint64_t>(dump_format::kMinSupportedVersion))
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageVersionMismatch, "Dump file version too old", filepath));
    }

    // Currently only V1 is implemented
    if (version != static_cast<uint32_t>(dump_format::FormatVersion::V1)) {
      StructuredLog()
          .Event("storage_validation_error")
          .Field("type", "version_not_implemented")
          .Field("filepath", filepath)
          .Field("version", static_cast<uint64_t>(version))
          .Error();
      return MakeUnexpected(
          MakeError(ErrorCode::kStorageVersionMismatch, "Dump file version not implemented", filepath));
    }

    // Read V1 header
    HeaderV1 header;
    if (auto result = ReadHeaderV1(ifs, header); !result) {
      LogStorageError("read_header", filepath, result.error().message());
      return result;
    }
    if (auto result = ValidateHeaderIntegrityFields(header); !result) {
      LogStorageError("validate_header_v1", filepath, result.error().message());
      if (integrity_error != nullptr) {
        integrity_error->type = dump_format::CRCErrorType::FileCRC;
        integrity_error->message = result.error().message();
      }
      return result;
    }
    loaded_gtid = header.gtid;

    uint64_t actual_file_size = 0;
    {
      std::streampos saved_pos = ifs.tellg();
      ifs.seekg(0, std::ios::end);
      const std::streampos end_pos = ifs.tellg();
      if (end_pos < 0) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to determine V1 dump file size"));
      }
      actual_file_size = static_cast<uint64_t>(end_pos);
      ifs.seekg(saved_pos);  // Restore position

      if (actual_file_size != header.total_file_size) {
        StructuredLog()
            .Event("storage_validation_error")
            .Field("type", "file_size_mismatch")
            .Field("filepath", filepath)
            .Field("expected_size", header.total_file_size)
            .Field("actual_size", actual_file_size)
            .Error();
        if (integrity_error != nullptr) {
          integrity_error->type = dump_format::CRCErrorType::FileCRC;
          integrity_error->message = "File size mismatch";
        }
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
      }
    }

    // V1 dumps always carry a file CRC. Zero is a valid checksum value, not
    // an "absent" sentinel.
    // Use streaming CRC to avoid loading entire file into memory (prevents OOM for large files)
    {
      // Save current position
      std::streampos current_pos = ifs.tellg();

      // Get file size
      ifs.seekg(0, std::ios::end);
      auto file_size = static_cast<uint64_t>(ifs.tellg());

      // CRC field offset defined by kHeaderFileCRC32Offset
      const size_t crc_offset = static_cast<size_t>(kHeaderFileCRC32Offset);

      uint32_t calculated_crc = dump_internal::CalculateCRC32Streaming(ifs, file_size, crc_offset);

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
    if (config_len > kMaxConfigSectionLength) {
      LogStorageError("read_config_section", filepath, "Config section too large: " + std::to_string(config_len));
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Config section too large"));
    }
    if (auto result = ValidateSectionLength(ifs, actual_file_size, config_len, restore_limits, "Config section");
        !result) {
      return result;
    }
    BoundedInputStream config_stream(ifs, config_len);
    if (auto result = DeserializeConfig(config_stream, loaded_config); !result) {
      LogStorageError("deserialize_config", filepath, result.error().message());
      return result;
    }
    if ((header.flags & dump_format::flags_v1::kHasCompatibilityMetadata) != 0U) {
      if (auto result = DeserializeCompatibilityMetadata(config_stream, loaded_config); !result) {
        LogStorageError("deserialize_compatibility_metadata", filepath, result.error().message());
        return result;
      }
    }
    if (config_stream.Remaining() != 0) {
      return MakeUnexpected(
          MakeError(ErrorCode::kStorageDumpReadError, "Config section contains trailing or malformed data"));
    }
    // Read statistics section
    uint32_t stats_len = 0;
    if (!ReadBinary(ifs, stats_len)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
    }
    if (stats_len > kMaxStatsSectionLength) {
      LogStorageError("read_stats_section", filepath, "Statistics section too large: " + std::to_string(stats_len));
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Statistics section exceeds maximum size"));
    }
    if (auto result = ValidateSectionLength(ifs, actual_file_size, stats_len, restore_limits, "Statistics section");
        !result) {
      return result;
    }
    if (stats_len > 0) {
      BoundedInputStream stats_stream(ifs, stats_len);
      if (stats != nullptr) {
        if (auto result = DeserializeStatistics(stats_stream, loaded_stats); !result) {
          LogStorageError("deserialize_statistics", filepath, result.error().message());
          return result;
        }
      } else if (!stats_stream.Drain()) {
        LogStorageError("skip_stats_section", filepath, "Statistics section is truncated");
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
      }
      if (stats_stream.Remaining() != 0) {
        LogStorageError("deserialize_statistics", filepath, "Statistics section contains trailing bytes");
        return MakeUnexpected(
            MakeError(ErrorCode::kStorageDumpReadError, "Statistics section contains trailing or malformed data"));
      }
    }

    // Read table data section
    uint32_t table_count = 0;
    if (!ReadBinary(ifs, table_count)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
    }

    std::unordered_set<std::string> dump_tables;
    std::vector<PendingTableLoad> pending_table_loads;
    uint64_t staged_memory_bytes = 0;

    for (uint32_t i = 0; i < table_count; ++i) {
      std::string table_name;
      if (!ReadString(ifs, table_name, kMaxIdentifierLength)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
      }
      dump_tables.insert(table_name);

      // Read table statistics
      uint32_t table_stats_len = 0;
      if (!ReadBinary(ifs, table_stats_len)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
      }
      if (table_stats_len > kMaxStatsSectionLength) {
        return MakeUnexpected(
            MakeError(ErrorCode::kStorageDumpReadError, "Table statistics section exceeds maximum size"));
      }
      if (auto result = ValidateSectionLength(ifs, actual_file_size, table_stats_len, restore_limits,
                                              "Table statistics section", staged_memory_bytes);
          !result) {
        return result;
      }
      if (table_stats_len > 0) {
        BoundedInputStream table_stats_stream(ifs, table_stats_len);
        if (table_stats != nullptr) {
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
          loaded_table_stats.emplace(table_name, std::move(table_stat));
        } else if (!table_stats_stream.Drain()) {
          LogStorageError("skip_table_stats_section", filepath, "Table statistics section is truncated");
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
        }
        if (table_stats_stream.Remaining() != 0) {
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError,
                                          "Table statistics section contains trailing or malformed data"));
        }
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
        if (auto result = ValidateSectionLength(ifs, actual_file_size, index_len, restore_limits, "Index data",
                                                staged_memory_bytes);
            !result) {
          return result;
        }
        BoundedInputStream index_stream(ifs, index_len);
        if (!index_stream.Drain()) {
          LogStorageError("skip_index_section", filepath, "Index section is truncated");
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
        }

        uint64_t doc_len = 0;
        if (!ReadBinary(ifs, doc_len)) {
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
        }
        if (auto result = ValidateSectionLength(ifs, actual_file_size, doc_len, restore_limits, "Document data",
                                                staged_memory_bytes);
            !result) {
          return result;
        }
        BoundedInputStream doc_stream(ifs, doc_len);
        if (!doc_stream.Drain()) {
          LogStorageError("skip_document_section", filepath, "Document section is truncated");
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
        }
        continue;
      }

      auto& ctx_pair = table_contexts[table_name];
      PendingTableLoad pending;
      pending.table_name = table_name;
      pending.index = ctx_pair.first;
      pending.doc_store = ctx_pair.second;

      // Read index data
      uint64_t index_len = 0;
      if (!ReadBinary(ifs, index_len)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
      }
      if (index_len == 0) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Invalid index length"));
      }
      if (auto result = ValidateSectionLength(ifs, actual_file_size, index_len, restore_limits, "Index data",
                                              staged_memory_bytes);
          !result) {
        return result;
      }
      if (auto result = dump_internal::ValidateRestoreMaterializationBudget(
              staged_memory_bytes, index_len, restore_limits.memory_budget_bytes, "Index data", "V1",
              dump_internal::kIndexMaterializationFactor);
          !result) {
        return result;
      }
      {
        BoundedInputStream index_stream(ifs, index_len);
        if (auto result = LoadPendingIndex(pending, index_stream); !result) {
          return result;
        }
        if (index_stream.Remaining() != 0) {
          return MakeUnexpected(
              MakeError(ErrorCode::kStorageDumpReadError, "Index decoder did not consume its bounded payload"));
        }
      }
      const uint64_t index_memory = pending.loaded_index->MemoryUsage();
      if (staged_memory_bytes > restore_limits.memory_budget_bytes ||
          index_memory > restore_limits.memory_budget_bytes - staged_memory_bytes) {
        return MakeUnexpected(
            MakeError(ErrorCode::kStorageDumpReadError,
                      "Loaded index exceeds configured V1 restore memory budget (dump.restore_memory_budget_mb)"));
      }

      // Read document store data
      uint64_t doc_len = 0;
      if (!ReadBinary(ifs, doc_len)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
      }
      if (doc_len == 0) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Invalid document data length"));
      }
      if (auto result = ValidateSectionLength(ifs, actual_file_size, doc_len, restore_limits, "Document data",
                                              staged_memory_bytes + index_memory);
          !result) {
        return result;
      }
      {
        BoundedInputStream doc_stream(ifs, doc_len);
        dump_internal::DocumentSectionHeader doc_header;
        if (auto result = dump_internal::ReadDocumentSectionHeader(doc_stream, doc_len, doc_header); !result) {
          return result;
        }
        if (auto result = dump_internal::ValidateRestoreDocumentBudget(staged_memory_bytes + index_memory, doc_len,
                                                                       doc_header.document_count,
                                                                       restore_limits.memory_budget_bytes, "V1");
            !result) {
          return result;
        }
        dump_internal::PrefixedInputStream replayed_doc_stream(std::move(doc_header.consumed_prefix), doc_stream);
        if (auto result = LoadPendingDocumentStore(pending, replayed_doc_stream); !result) {
          return result;
        }
        if (doc_stream.Remaining() != 0) {
          return MakeUnexpected(
              MakeError(ErrorCode::kStorageDumpReadError, "DocumentStore decoder did not consume its bounded payload"));
        }
      }
      const uint64_t doc_memory = pending.loaded_doc_store->MemoryUsage();
      if (index_memory > std::numeric_limits<uint64_t>::max() - doc_memory) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Loaded table memory size overflow"));
      }
      const uint64_t table_memory = index_memory + doc_memory;
      if (staged_memory_bytes > restore_limits.memory_budget_bytes ||
          table_memory > restore_limits.memory_budget_bytes - staged_memory_bytes) {
        return MakeUnexpected(
            MakeError(ErrorCode::kStorageDumpReadError,
                      "Loaded tables exceed configured V1 restore memory budget (dump.restore_memory_budget_mb)"));
      }

      staged_memory_bytes += table_memory;
      pending_table_loads.push_back(std::move(pending));
    }

    if (auto result = ValidateDumpTableSet(dump_tables, table_contexts); !result) {
      return result;
    }

    if (config_validator) {
      if (auto result = config_validator(loaded_config, loaded_gtid); !result) {
        return result;
      }
    }

    if (auto result = ApplyPendingTableLoads(pending_table_loads); !result) {
      return result;
    }

    config = std::move(loaded_config);
    if (stats != nullptr) {
      *stats = loaded_stats;
    }
    if (table_stats != nullptr) {
      *table_stats = std::move(loaded_table_stats);
    }
    gtid = std::move(loaded_gtid);
    return {};

  } catch (const std::exception& e) {
    LogStorageError("read_dump_exception", filepath, e.what());
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
  }
}

}  // namespace mygramdb::storage::dump_v1
