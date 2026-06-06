/**
 * @file dump_format_v2.cpp
 * @brief Dump file format Version 2 implementation
 *
 * Section envelope format with per-section CRC32.
 * Reuses V1 config/statistics serialization internally.
 */

#include "storage/dump_format_v2.h"

#include <spdlog/spdlog.h>
#include <zlib.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "storage/dump_format_v1.h"
#include "storage/dump_format_v1_internal.h"
#include "utils/atomic_file_writer.h"
#include "utils/binary_io.h"
#include "utils/fd_guard.h"
#include "utils/memory_utils.h"
#include "utils/structured_log.h"

#ifdef _WIN32
#include <io.h>
#define CHMOD _chmod
#else
#include <fcntl.h>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#define CHMOD chmod

#ifndef O_NOFOLLOW
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage): Platform compatibility constant for symlink protection
#define O_NOFOLLOW 0x00000100
#endif
#endif

namespace mygramdb::storage {

struct DumpLoadAccess {
  struct LoadedTableReplacement {
    std::string table_name;
    index::Index* target_index = nullptr;
    index::Index* loaded_index = nullptr;
    DocumentStore* target_doc_store = nullptr;
    DocumentStore* loaded_doc_store = nullptr;
  };

  static void ReplaceLoadedTables(std::vector<LoadedTableReplacement> replacements) {
    std::sort(replacements.begin(), replacements.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.table_name < rhs.table_name; });

    std::vector<std::unique_lock<std::shared_mutex>> target_locks;
    target_locks.reserve(replacements.size() * 2);
    for (const auto& replacement : replacements) {
      target_locks.emplace_back(replacement.target_index->postings_mutex_);
      target_locks.emplace_back(replacement.target_doc_store->mutex_);
    }

    for (const auto& replacement : replacements) {
      replacement.target_index->term_postings_ = std::move(replacement.loaded_index->term_postings_);
      replacement.target_index->load_generation_.fetch_add(1, std::memory_order_acq_rel);

      replacement.target_doc_store->doc_id_to_pk_ = std::move(replacement.loaded_doc_store->doc_id_to_pk_);
      replacement.target_doc_store->pk_to_doc_id_ = std::move(replacement.loaded_doc_store->pk_to_doc_id_);
      replacement.target_doc_store->doc_filters_ = std::move(replacement.loaded_doc_store->doc_filters_);
      replacement.target_doc_store->doc_texts_ = std::move(replacement.loaded_doc_store->doc_texts_);
      replacement.target_doc_store->filter_index_ = std::move(replacement.loaded_doc_store->filter_index_);
      replacement.target_doc_store->next_doc_id_ = replacement.loaded_doc_store->next_doc_id_;
    }
  }
};

}  // namespace mygramdb::storage

namespace mygramdb::storage::dump_v2 {

using namespace mygram::utils;

// Forward declaration
uint32_t CalculateCRC32Streaming(std::ifstream& ifs, uint64_t file_size, size_t crc_offset);

namespace {

using dump_v1::internal::ReadString;
using dump_v1::internal::WriteString;
using mygram::utils::ReadBinary;
using mygram::utils::WriteBinary;

struct PendingTableLoad {
  std::string table_name;
  index::Index* index = nullptr;
  DocumentStore* doc_store = nullptr;
  std::unique_ptr<index::Index> loaded_index;
  std::unique_ptr<DocumentStore> loaded_doc_store;
};

Expected<void, Error> LoadPendingTableData(PendingTableLoad& pending, const std::string& index_data,
                                           const std::string& doc_data) {
  auto loaded_index = std::make_unique<index::Index>(
      pending.index->GetNgramSize(), pending.index->GetKanjiNgramSize(), index::kDefaultRoaringThreshold,
      pending.index->GetCrossBoundaryNgrams(), pending.index->GetNormalizeNfkc(), pending.index->GetNormalizeWidth(),
      pending.index->GetNormalizeLower());
  std::istringstream index_stream(index_data);
  if (auto index_result = loaded_index->LoadFromStream(index_stream); !index_result) {
    return MakeUnexpected(
        MakeError(ErrorCode::kStorageDumpReadError, "LoadFromStream failed for index", index_result.error().message()));
  }

  auto loaded_doc_store = std::make_unique<DocumentStore>();
  std::istringstream doc_stream(doc_data);
  if (auto result = loaded_doc_store->LoadFromStream(doc_stream, nullptr); !result) {
    return result;
  }

  pending.loaded_index = std::move(loaded_index);
  pending.loaded_doc_store = std::move(loaded_doc_store);
  return {};
}

uint32_t ExpectedHeaderSizeV2(const HeaderV2& header) {
  return static_cast<uint32_t>(4 + 4 + 8 + 8 + 4 + 4 + 4 + header.gtid.size());
}

Expected<void, Error> ValidateHeaderIntegrityFields(const HeaderV2& header) {
  const uint32_t expected_header_size = ExpectedHeaderSizeV2(header);
  if (header.header_size != expected_header_size) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError,
                                    "Invalid V2 header size: expected " + std::to_string(expected_header_size) +
                                        ", got " + std::to_string(header.header_size)));
  }
  if (header.total_file_size == 0) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Invalid V2 header: total_file_size is zero"));
  }
  if (header.section_count == 0) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Invalid V2 header: section_count is zero"));
  }
  if ((header.flags & dump_format::flags_v2::kWithCRC) != 0 && header.file_crc32 == 0) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Invalid V2 header: file_crc32 is zero"));
  }
  return {};
}

Expected<void, Error> ApplyPendingTableLoads(const std::vector<PendingTableLoad>& pending_loads) {
  std::vector<DumpLoadAccess::LoadedTableReplacement> replacements;
  replacements.reserve(pending_loads.size());
  for (const auto& pending : pending_loads) {
    replacements.push_back(DumpLoadAccess::LoadedTableReplacement{pending.table_name, pending.index,
                                                                  pending.loaded_index.get(), pending.doc_store,
                                                                  pending.loaded_doc_store.get()});
  }

  DumpLoadAccess::ReplaceLoadedTables(std::move(replacements));

  for (const auto& pending : pending_loads) {
    StructuredLog().Event("dump_v2_table_loaded").Field("table", pending.table_name).Info();
  }

  return {};
}

Expected<void, Error> ValidateDumpTableSet(
    const std::unordered_set<std::string>& dump_tables,
    const std::unordered_map<std::string, std::pair<index::Index*, DocumentStore*>>& table_contexts) {
  std::vector<std::string> missing_tables;
  std::vector<std::string> unexpected_tables;

  for (const auto& [table_name, unused_context] : table_contexts) {
    if (dump_tables.count(table_name) == 0) {
      missing_tables.push_back(table_name);
    }
  }
  for (const auto& table_name : dump_tables) {
    if (table_contexts.count(table_name) == 0) {
      unexpected_tables.push_back(table_name);
    }
  }

  if (missing_tables.empty() && unexpected_tables.empty()) {
    return {};
  }

  auto join_names = [](const std::vector<std::string>& names) {
    std::ostringstream joined;
    for (size_t i = 0; i < names.size(); ++i) {
      if (i > 0) {
        joined << ",";
      }
      joined << names[i];
    }
    return joined.str();
  };

  StructuredLog()
      .Event("dump_v2_table_set_mismatch")
      .Field("missing_configured_tables", join_names(missing_tables))
      .Field("unexpected_dump_tables", join_names(unexpected_tables))
      .Error();
  return MakeUnexpected(MakeError(
      ErrorCode::kStorageDumpReadError,
      "Dump table set does not match configured tables. Missing configured tables: " + join_names(missing_tables) +
          "; unexpected dump tables: " + join_names(unexpected_tables)));
}

#ifndef _WIN32
class FdStreambuf : public std::streambuf {
 public:
  explicit FdStreambuf(int fd) : fd_(fd) { setp(buffer_, buffer_ + kBufferSize); }
  ~FdStreambuf() override { sync(); }

  FdStreambuf(const FdStreambuf&) = delete;
  FdStreambuf& operator=(const FdStreambuf&) = delete;
  FdStreambuf(FdStreambuf&&) = delete;
  FdStreambuf& operator=(FdStreambuf&&) = delete;

 protected:
  int overflow(int ch) override {
    if (ch != traits_type::eof()) {
      *pptr() = static_cast<char>(ch);
      pbump(1);
    }
    return FlushBuffer() < 0 ? traits_type::eof() : ch;
  }

  int sync() override { return FlushBuffer(); }

 private:
  static constexpr size_t kBufferSize = 8192;

  int FlushBuffer() {
    size_t bytes = static_cast<size_t>(pptr() - pbase());
    if (bytes == 0) {
      return 0;
    }
    const char* ptr = pbase();
    while (bytes > 0) {
      ssize_t written = write(fd_, ptr, bytes);
      if (written < 0) {
        if (errno == EINTR) {
          continue;
        }
        return -1;
      }
      if (written == 0) {
        return -1;
      }
      ptr += written;
      bytes -= static_cast<size_t>(written);
    }
    setp(buffer_, buffer_ + kBufferSize);
    return 0;
  }

  int fd_;
  char buffer_[kBufferSize]{};
};

bool WriteAllAt(int fd, const void* data, size_t size, off_t offset) {
  const auto* current = static_cast<const char*>(data);
  size_t remaining = size;
  off_t current_offset = offset;
  while (remaining > 0) {
    ssize_t written = pwrite(fd, current, remaining, current_offset);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (written == 0) {
      return false;
    }
    current += written;
    remaining -= static_cast<size_t>(written);
    current_offset += written;
  }
  return true;
}

template <typename T>
bool WriteBinaryAt(int fd, T value, off_t offset) {
  if constexpr (std::is_integral_v<T>) {
    value = mygram::utils::ToLittleEndian(value);
  }
  return WriteAllAt(fd, &value, sizeof(value), offset);
}

Expected<uint32_t, Error> CalculateCRC32StreamingFd(int fd, uint64_t file_size, size_t crc_offset) {
  constexpr size_t kChunkSize = 1024 * 1024;
  constexpr size_t kCrcFieldSize = 4;

  uint32_t crc = 0;
  std::vector<char> buffer(kChunkSize);
  uint64_t bytes_read = 0;

  while (bytes_read < file_size) {
    size_t to_read = std::min(kChunkSize, static_cast<size_t>(file_size - bytes_read));
    ssize_t actually_read = pread(fd, buffer.data(), to_read, static_cast<off_t>(bytes_read));
    if (actually_read < 0) {
      if (errno == EINTR) {
        continue;
      }
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to read file for CRC32"));
    }
    if (actually_read == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Unexpected EOF while calculating CRC32"));
    }

    auto chunk_size = static_cast<size_t>(actually_read);
    if (crc_offset >= bytes_read && crc_offset < bytes_read + chunk_size) {
      size_t offset_in_chunk = crc_offset - bytes_read;
      size_t zero_bytes = std::min(kCrcFieldSize, chunk_size - offset_in_chunk);
      std::memset(&buffer[offset_in_chunk], 0, zero_bytes);
    }
    if (crc_offset + kCrcFieldSize > bytes_read && crc_offset < bytes_read) {
      size_t zero_end = std::min<size_t>(kCrcFieldSize - (bytes_read - crc_offset), chunk_size);
      std::memset(buffer.data(), 0, zero_end);
    }

    crc = static_cast<uint32_t>(
        crc32(crc, reinterpret_cast<const Bytef*>(buffer.data()), static_cast<uInt>(chunk_size)));  // NOLINT
    bytes_read += chunk_size;
  }

  return crc;
}

Expected<uint32_t, Error> CalculateCRC32RangeFd(int fd, off_t start_offset, uint64_t length) {
  constexpr size_t kChunkSize = 1024 * 1024;

  uint32_t crc = 0;
  std::vector<char> buffer(kChunkSize);
  uint64_t bytes_read = 0;

  while (bytes_read < length) {
    size_t to_read = std::min(kChunkSize, static_cast<size_t>(length - bytes_read));
    ssize_t actually_read = pread(fd, buffer.data(), to_read, start_offset + static_cast<off_t>(bytes_read));
    if (actually_read < 0) {
      if (errno == EINTR) {
        continue;
      }
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to read section for CRC32"));
    }
    if (actually_read == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Unexpected EOF while calculating CRC32"));
    }

    auto chunk_size = static_cast<size_t>(actually_read);
    crc = static_cast<uint32_t>(
        crc32(crc, reinterpret_cast<const Bytef*>(buffer.data()), static_cast<uInt>(chunk_size)));  // NOLINT
    bytes_read += chunk_size;
  }

  return crc;
}
#endif

/**
 * @brief Compute CRC32 of a data buffer
 */
uint32_t ComputeCRC32(const std::string& data) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast) - Required for zlib crc32 API
  return static_cast<uint32_t>(crc32(0, reinterpret_cast<const Bytef*>(data.data()), static_cast<uInt>(data.size())));
}

class CountingStreambuf : public std::streambuf {
 public:
  explicit CountingStreambuf(std::ostream& destination) : destination_(destination) {}

  [[nodiscard]] uint64_t bytes_written() const { return bytes_written_; }

 protected:
  int overflow(int ch) override {
    if (ch == traits_type::eof()) {
      return traits_type::not_eof(ch);
    }
    char value = static_cast<char>(ch);
    if (!WriteAndTrack(&value, 1)) {
      return traits_type::eof();
    }
    return ch;
  }

  std::streamsize xsputn(const char* data, std::streamsize size) override {
    if (size <= 0) {
      return 0;
    }
    if (!WriteAndTrack(data, static_cast<size_t>(size))) {
      return 0;
    }
    return size;
  }

  int sync() override {
    destination_.flush();
    return destination_.good() ? 0 : -1;
  }

 private:
  bool WriteAndTrack(const char* data, size_t size) {
    destination_.write(data, static_cast<std::streamsize>(size));
    if (!destination_.good()) {
      return false;
    }
    bytes_written_ += size;
    return true;
  }

  std::ostream& destination_;
  uint64_t bytes_written_ = 0;
};

#ifndef _WIN32
Expected<void, Error> WriteStreamingTableSection(std::ostream& output_stream, int file_descriptor,
                                                 const std::string& table_name, index::Index* index,
                                                 DocumentStore* doc_store,
                                                 const std::unordered_map<std::string, TableStatistics>* table_stats,
                                                 const std::string& temp_filepath) {
  output_stream.flush();
  if (!output_stream.good()) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
  }

  off_t section_start = lseek(file_descriptor, 0, SEEK_CUR);
  if (section_start < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to locate section start"));
  }

  auto type_val = static_cast<uint32_t>(dump_format::SectionType::kTableData);
  uint32_t crc_placeholder = 0;
  uint64_t length_placeholder = 0;
  if (!WriteBinary(output_stream, type_val) || !WriteBinary(output_stream, crc_placeholder) ||
      !WriteBinary(output_stream, length_placeholder)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write section envelope"));
  }

  CountingStreambuf counting_streambuf(output_stream);
  std::ostream section_stream(&counting_streambuf);

  if (!dump_v1::internal::WriteString(section_stream, table_name)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
  }

  if (table_stats != nullptr && table_stats->count(table_name) > 0) {
    std::ostringstream ts_stream;
    if (auto result = dump_v1::SerializeTableStatistics(ts_stream, table_stats->at(table_name)); !result) {
      return result;
    }
    std::string ts_data = ts_stream.str();
    auto ts_len = static_cast<uint32_t>(ts_data.size());
    if (!WriteBinary(section_stream, ts_len)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
    section_stream.write(ts_data.data(), static_cast<std::streamsize>(ts_data.size()));
  } else {
    uint32_t ts_len = 0;
    if (!WriteBinary(section_stream, ts_len)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
  }

  auto write_sized_payload = [&](auto write_payload) -> Expected<void, Error> {
    output_stream.flush();
    off_t length_offset = lseek(file_descriptor, 0, SEEK_CUR);
    if (length_offset < 0) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to locate payload length"));
    }
    uint64_t payload_length = 0;
    if (!WriteBinary(section_stream, payload_length)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    output_stream.flush();
    off_t payload_start = lseek(file_descriptor, 0, SEEK_CUR);
    if (payload_start < 0) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to locate payload start"));
    }

    if (auto result = write_payload(section_stream); !result) {
      return result;
    }

    section_stream.flush();
    output_stream.flush();
    off_t payload_end = lseek(file_descriptor, 0, SEEK_CUR);
    if (payload_end < payload_start) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to locate payload end"));
    }
    payload_length = static_cast<uint64_t>(payload_end - payload_start);
    if (!WriteBinaryAt(file_descriptor, payload_length, length_offset)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write payload length"));
    }
    return {};
  };

  if (auto result = write_sized_payload([&](std::ostream& stream) -> Expected<void, Error> {
        if (auto index_result = index->SaveToStream(stream); !index_result) {
          StructuredLog()
              .Event("storage_error")
              .Field("operation", "save_index")
              .Field("filepath", temp_filepath)
              .Field("table", table_name)
              .Field("error", index_result.error().message())
              .Error();
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
        }
        return {};
      });
      !result) {
    return result;
  }

  if (auto result = write_sized_payload([&](std::ostream& stream) -> Expected<void, Error> {
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

  section_stream.flush();
  if (!section_stream.good() || !output_stream.good()) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
  }

  auto section_crc = CalculateCRC32RangeFd(file_descriptor, section_start + dump_format::kSectionEnvelopeSize,
                                           counting_streambuf.bytes_written());
  if (!section_crc) {
    return MakeUnexpected(section_crc.error());
  }
  if (!WriteBinaryAt(file_descriptor, section_crc.value(), section_start + 4)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write section CRC32"));
  }
  if (!WriteBinaryAt(file_descriptor, counting_streambuf.bytes_written(), section_start + 8)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write section length"));
  }
  return {};
}
#endif

}  // namespace

// ============================================================================
// Header V2 Serialization
// ============================================================================

Expected<void, Error> WriteHeaderV2(std::ostream& output_stream, const HeaderV2& header) {
  if (!WriteBinary(output_stream, header.header_size)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write V2 header size"));
  }
  if (!WriteBinary(output_stream, header.flags)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write V2 header flags"));
  }
  if (!WriteBinary(output_stream, header.dump_timestamp)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write V2 dump timestamp"));
  }
  if (!WriteBinary(output_stream, header.total_file_size)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write V2 total file size"));
  }
  if (!WriteBinary(output_stream, header.file_crc32)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write V2 file CRC32"));
  }
  if (!WriteBinary(output_stream, header.section_count)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write V2 section count"));
  }
  if (!WriteString(output_stream, header.gtid)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write V2 GTID"));
  }
  return {};
}

Expected<void, Error> ReadHeaderV2(std::istream& input_stream, HeaderV2& header) {
  if (!ReadBinary(input_stream, header.header_size)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read V2 header size"));
  }
  if (!ReadBinary(input_stream, header.flags)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read V2 header flags"));
  }
  if (!ReadBinary(input_stream, header.dump_timestamp)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read V2 dump timestamp"));
  }
  if (!ReadBinary(input_stream, header.total_file_size)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read V2 total file size"));
  }
  if (!ReadBinary(input_stream, header.file_crc32)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read V2 file CRC32"));
  }
  if (!ReadBinary(input_stream, header.section_count)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read V2 section count"));
  }
  if (!ReadString(input_stream, header.gtid, dump_v1::kMaxPathLength)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read V2 GTID"));
  }
  return {};
}

// ============================================================================
// Section Envelope Read/Write
// ============================================================================

Expected<void, Error> WriteSectionEnvelope(std::ostream& output_stream, dump_format::SectionType type,
                                           const std::string& data) {
  auto type_val = static_cast<uint32_t>(type);
  if (!WriteBinary(output_stream, type_val)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write section type"));
  }

  uint32_t crc = ComputeCRC32(data);
  if (!WriteBinary(output_stream, crc)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write section CRC32"));
  }

  auto length = static_cast<uint64_t>(data.size());
  if (!WriteBinary(output_stream, length)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write section data length"));
  }

  if (!data.empty()) {
    output_stream.write(data.data(), static_cast<std::streamsize>(data.size()));
    if (!output_stream.good()) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write section data"));
    }
  }

  return {};
}

Expected<void, Error> WriteSectionEnvelope(std::ostream& output_stream, dump_format::SectionType type,
                                           std::ostringstream& data_stream) {
  // Delegate to the string overload. std::ostringstream's internal stringbuf
  // is opened in output-only mode, so its get area is not configured for
  // reading. Using .str() is the safe way to extract the written data.
  return WriteSectionEnvelope(output_stream, type, data_stream.str());
}

Expected<void, Error> ReadSectionEnvelope(std::istream& input_stream, dump_format::SectionEnvelope& envelope) {
  uint32_t type_val = 0;
  if (!ReadBinary(input_stream, type_val)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read section type"));
  }
  envelope.type = static_cast<dump_format::SectionType>(type_val);

  if (!ReadBinary(input_stream, envelope.crc32)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read section CRC32"));
  }

  if (!ReadBinary(input_stream, envelope.data_length)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read section data length"));
  }

  // Validate data length against maximum
  if (envelope.data_length > kMaxSectionDataLength) {
    StructuredLog()
        .Event("storage_validation_error")
        .Field("type", "section_data_length_exceeded")
        .Field("section_type", static_cast<uint64_t>(type_val))
        .Field("data_length", static_cast<uint64_t>(envelope.data_length))
        .Field("max_length", static_cast<uint64_t>(kMaxSectionDataLength))
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Section data length exceeds maximum"));
  }

  return {};
}

// ============================================================================
// WriteDumpV2
// ============================================================================

Expected<void, Error> WriteDumpV2(
    const std::string& filepath, const std::string& gtid, const config::Config& config,
    const std::unordered_map<std::string, std::pair<index::Index*, DocumentStore*>>& table_contexts,
    const DumpStatistics* stats, const std::unordered_map<std::string, TableStatistics>* table_stats,
    const DumpTableProgressCallback& table_progress_callback) {
  AtomicFileWriter writer(filepath, true);
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
    }

#ifndef _WIN32
    // SECURITY: Validate dump directory is not a symlink
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

    // SECURITY: Check if final path is a symlink
    std::error_code error_code;
    if (std::filesystem::is_symlink(filepath, error_code)) {
      StructuredLog().Event("storage_security_error").Field("type", "symlink_file").Field("filepath", filepath).Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    // Remove any existing temp file
    std::filesystem::remove(temp_filepath, error_code);

    // SECURITY: Open with O_NOFOLLOW + O_CREAT + O_EXCL
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): POSIX open() requires varargs for mode
    int file_descriptor = open(temp_filepath.c_str(), O_RDWR | O_CREAT | O_EXCL | O_NOFOLLOW, S_IRUSR | S_IWUSR);
    if (file_descriptor < 0) {
      LogStorageError("create_temp_file", temp_filepath, std::strerror(errno));
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
    FDGuard fd_guard(file_descriptor);

    // Verify ownership
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

    FdStreambuf fd_streambuf(file_descriptor);
    std::ostream ofs(&fd_streambuf);
    if (!ofs) {
      LogStorageError("create_stream", temp_filepath, "Failed to create stream from file descriptor");
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
#else
    std::error_code error_code;
    std::filesystem::remove(temp_filepath, error_code);

    std::ofstream ofs(temp_filepath, std::ios::binary | std::ios::trunc);
    if (!ofs) {
      LogStorageError("open_temp_file", temp_filepath, "Failed to open for writing");
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
#endif

    // Track section count
    uint32_t section_count = 0;

    // Write fixed file header
    ofs.write(dump_format::kMagicNumber.data(), 4);
    auto version = static_cast<uint32_t>(dump_format::FormatVersion::V2);
    if (!WriteBinary(ofs, version)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    // Prepare V2 header (placeholders for file_size, CRC, section_count)
    HeaderV2 header;
    // header_size = header_size(4) + flags(4) + dump_timestamp(8) + total_file_size(8) +
    //               file_crc32(4) + section_count(4) + gtid_length(4) + gtid(N)
    //             = 36 + gtid.size()
    header.header_size = static_cast<uint32_t>(4 + 4 + 8 + 8 + 4 + 4 + 4 + gtid.size());
    header.flags = dump_format::flags_v2::kNone | dump_format::flags_v2::kWithCRC;
    if (stats != nullptr) {
      header.flags |= dump_format::flags_v2::kWithStatistics;
    }
    header.dump_timestamp = static_cast<uint64_t>(std::time(nullptr));
    header.gtid = gtid;

    if (auto result = WriteHeaderV2(ofs, header); !result) {
      LogStorageError("write_header_v2", temp_filepath, result.error().message());
      return result;
    }

    // Section 1: Config
    {
      std::ostringstream config_stream;
      if (auto result = dump_v1::SerializeConfig(config_stream, config); !result) {
        LogStorageError("serialize_config", temp_filepath, result.error().message());
        return result;
      }
      if (auto result = WriteSectionEnvelope(ofs, dump_format::SectionType::kConfig, config_stream); !result) {
        LogStorageError("write_config_section", temp_filepath, result.error().message());
        return result;
      }
      ++section_count;
    }

    // Section 2: Statistics (optional)
    if (stats != nullptr) {
      std::ostringstream stats_stream;
      if (auto result = dump_v1::SerializeStatistics(stats_stream, *stats); !result) {
        LogStorageError("serialize_statistics", temp_filepath, result.error().message());
        return result;
      }
      if (auto result = WriteSectionEnvelope(ofs, dump_format::SectionType::kStatistics, stats_stream); !result) {
        LogStorageError("write_stats_section", temp_filepath, result.error().message());
        return result;
      }
      ++section_count;
    }

    // Sections 3..N: TableData (one per table)
    size_t tables_processed = 0;
    for (const auto& [table_name, ctx_pair] : table_contexts) {
      index::Index* index = ctx_pair.first;
      DocumentStore* doc_store = ctx_pair.second;
      if (table_progress_callback) {
        table_progress_callback(table_name, tables_processed);
      }

#ifndef _WIN32
      if (auto result = WriteStreamingTableSection(ofs, file_descriptor, table_name, index, doc_store, table_stats,
                                                   temp_filepath);
          !result) {
        LogStorageError("write_table_section", temp_filepath, result.error().message());
        return result;
      }
#else
      // Serialize table data into a single buffer
      std::ostringstream table_stream;

      // Table name
      if (!dump_v1::internal::WriteString(table_stream, table_name)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }

      // Table statistics (optional)
      if (table_stats != nullptr && table_stats->count(table_name) > 0) {
        std::ostringstream ts_stream;
        if (auto result = dump_v1::SerializeTableStatistics(ts_stream, table_stats->at(table_name)); !result) {
          return result;
        }
        std::string ts_data = ts_stream.str();
        auto ts_len = static_cast<uint32_t>(ts_data.size());
        if (!WriteBinary(table_stream, ts_len)) {
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
        }
        table_stream.write(ts_data.data(), static_cast<std::streamsize>(ts_data.size()));
      } else {
        uint32_t ts_len = 0;
        if (!WriteBinary(table_stream, ts_len)) {
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
        }
      }

      // Index data
      {
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
        if (!WriteBinary(table_stream, index_len)) {
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
        }
        table_stream.write(index_data.data(), static_cast<std::streamsize>(index_data.size()));
      }

      // DocStore data
      {
        std::ostringstream doc_stream;
        if (auto result = doc_store->SaveToStream(doc_stream, ""); !result) {
          StructuredLog()
              .Event("storage_error")
              .Field("operation", "save_documents")
              .Field("filepath", temp_filepath)
              .Field("table", table_name)
              .Error();
          return result;
        }
        std::string doc_data = doc_stream.str();
        auto doc_len = static_cast<uint64_t>(doc_data.size());
        if (!WriteBinary(table_stream, doc_len)) {
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
        }
        table_stream.write(doc_data.data(), static_cast<std::streamsize>(doc_data.size()));
      }

      // Write entire table as one section envelope
      if (auto result = WriteSectionEnvelope(ofs, dump_format::SectionType::kTableData, table_stream); !result) {
        LogStorageError("write_table_section", temp_filepath, result.error().message());
        return result;
      }
#endif
      ++section_count;
      ++tables_processed;

      StructuredLog().Event("dump_v2_table_saved").Field("table", table_name).Debug();
    }

    ofs.flush();
    if (!ofs.good()) {
      LogStorageError("write_dump_v2", temp_filepath, "Stream error during write");
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

#ifndef _WIN32
    // Calculate file size from the verified fd and patch header fields in place.
    // Reopening temp_filepath by name here would reintroduce a TOCTOU window.
    if (fstat(file_descriptor, &file_stat) != 0) {
      LogStorageError("stat_temp_file", temp_filepath, std::strerror(errno));
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
    uint64_t file_size = static_cast<uint64_t>(file_stat.st_size);

    // Update header: total_file_size, section_count
    if (!WriteBinaryAt(file_descriptor, file_size, kV2HeaderTotalFileSizeOffset)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write total_file_size"));
    }
    if (!WriteBinaryAt(file_descriptor, section_count, kV2HeaderSectionCountOffset)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write section_count"));
    }

    // Calculate and update file CRC32
    const auto crc_offset = static_cast<size_t>(kV2HeaderFileCRC32Offset);
    auto crc_result = CalculateCRC32StreamingFd(file_descriptor, file_size, crc_offset);
    if (!crc_result) {
      LogStorageError("calculate_crc32", temp_filepath, crc_result.error().message());
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
    uint32_t calculated_crc = crc_result.value();

    if (!WriteBinaryAt(file_descriptor, calculated_crc, kV2HeaderFileCRC32Offset)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write file CRC32"));
    }
#else
    ofs.close();
    if (!ofs.good()) {
      LogStorageError("write_dump_v2", temp_filepath, "Stream error during write");
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

    {
      std::fstream update_stream(temp_filepath, std::ios::in | std::ios::out | std::ios::binary);
      if (!update_stream) {
        LogStorageError("open_file", temp_filepath, "Failed to open for header update");
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }

      update_stream.seekp(kV2HeaderTotalFileSizeOffset);
      if (!WriteBinary(update_stream, file_size)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write total_file_size"));
      }
      update_stream.seekp(kV2HeaderSectionCountOffset);
      if (!WriteBinary(update_stream, section_count)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write section_count"));
      }
      update_stream.close();
    }

    std::ifstream ifs(temp_filepath, std::ios::binary);
    if (!ifs) {
      LogStorageError("reopen_file", temp_filepath, "Failed to reopen for CRC calculation");
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
    const auto crc_offset = static_cast<size_t>(kV2HeaderFileCRC32Offset);
    uint32_t calculated_crc = CalculateCRC32Streaming(ifs, file_size, crc_offset);
    ifs.close();

    {
      std::fstream update_stream(temp_filepath, std::ios::in | std::ios::out | std::ios::binary);
      if (!update_stream) {
        LogStorageError("open_file", temp_filepath, "Failed to open for CRC update");
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }

      update_stream.seekp(kV2HeaderFileCRC32Offset);
      if (!WriteBinary(update_stream, calculated_crc)) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write file CRC32"));
      }

      update_stream.close();
      if (!update_stream.good()) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Stream error during header update"));
      }
    }
#endif

    // Atomic commit
    if (auto result = writer.Commit(); !result) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    StructuredLog()
        .Event("dump_v2_saved")
        .Field("filepath", filepath)
        .Field("version", static_cast<uint64_t>(2))
        .Field("sections", static_cast<uint64_t>(section_count))
        .Field("crc32", static_cast<uint64_t>(calculated_crc))
        .Field("file_size", file_size)
        .Info();

    return {};

  } catch (const std::exception& e) {
    LogStorageError("write_dump_v2_exception", filepath, e.what());
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
  }
}

// ============================================================================
// ReadDumpV2
// ============================================================================

Expected<void, Error> ReadDumpV2(
    const std::string& filepath, std::string& gtid, config::Config& config,
    std::unordered_map<std::string, std::pair<index::Index*, DocumentStore*>>& table_contexts, DumpStatistics* stats,
    std::unordered_map<std::string, TableStatistics>* table_stats, dump_format::IntegrityError* integrity_error,
    const DumpConfigValidationCallback& config_validator) {
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

    if (version != static_cast<uint32_t>(dump_format::FormatVersion::V2)) {
      StructuredLog()
          .Event("storage_validation_error")
          .Field("type", "version_mismatch")
          .Field("filepath", filepath)
          .Field("expected", static_cast<uint64_t>(2))
          .Field("actual", static_cast<uint64_t>(version))
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Not a V2 dump file"));
    }

    // Read V2 header
    HeaderV2 header;
    if (auto result = ReadHeaderV2(ifs, header); !result) {
      LogStorageError("read_header_v2", filepath, result.error().message());
      return result;
    }
    if (auto result = ValidateHeaderIntegrityFields(header); !result) {
      LogStorageError("validate_header_v2", filepath, result.error().message());
      if (integrity_error != nullptr) {
        integrity_error->type = dump_format::CRCErrorType::FileCRC;
        integrity_error->message = result.error().message();
      }
      return result;
    }
    gtid = header.gtid;

    // Verify file size
    {
      std::streampos saved_pos = ifs.tellg();
      ifs.seekg(0, std::ios::end);
      auto actual_size = static_cast<uint64_t>(ifs.tellg());
      ifs.seekg(saved_pos);

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

    // Verify file-level CRC32
    if (header.file_crc32 != 0) {
      std::streampos current_pos = ifs.tellg();
      ifs.seekg(0, std::ios::end);
      auto file_size = static_cast<uint64_t>(ifs.tellg());

      const auto crc_offset = static_cast<size_t>(kV2HeaderFileCRC32Offset);
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

      ifs.seekg(current_pos);
    }

    // Read sections
    bool config_found = false;
    uint32_t sections_read = 0;
    std::vector<PendingTableLoad> pending_table_loads;
    std::unordered_set<std::string> dump_tables;

    // Maximum allowed size for table data sections
    const uint64_t kMaxTableDataSectionLength = [] {
      auto mem_info = mygram::utils::GetSystemMemoryInfo();
      return mem_info ? mem_info->total_physical_bytes : 64ULL * 1024 * 1024 * 1024;
    }();

    for (uint32_t i = 0; i < header.section_count; ++i) {
      dump_format::SectionEnvelope envelope;
      if (auto result = ReadSectionEnvelope(ifs, envelope); !result) {
        if (ifs.eof()) {
          break;  // Normal EOF
        }
        return result;
      }

      // Read section data
      if (envelope.data_length > kMaxTableDataSectionLength) {
        return MakeUnexpected(
            MakeError(ErrorCode::kStorageDumpReadError, "Section data length exceeds physical memory"));
      }

      std::string section_data(envelope.data_length, '\0');
      if (envelope.data_length > 0) {
        ifs.read(section_data.data(), static_cast<std::streamsize>(envelope.data_length));
        if (!ifs.good()) {
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read section data"));
        }
      }

      // Verify section CRC
      uint32_t actual_crc = ComputeCRC32(section_data);
      if (actual_crc != envelope.crc32) {
        StructuredLog()
            .Event("storage_validation_error")
            .Field("type", "section_crc32_mismatch")
            .Field("filepath", filepath)
            .Field("section_type", static_cast<uint64_t>(static_cast<uint32_t>(envelope.type)))
            .Field("expected_crc", static_cast<uint64_t>(envelope.crc32))
            .Field("actual_crc", static_cast<uint64_t>(actual_crc))
            .Error();
        if (integrity_error != nullptr) {
          integrity_error->type = dump_format::CRCErrorType::SectionCRC;
          integrity_error->message = "Section CRC32 mismatch";
        }
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Section CRC32 verification failed"));
      }

      // Dispatch by section type
      switch (envelope.type) {
        case dump_format::SectionType::kConfig: {
          std::istringstream config_stream(section_data);
          if (auto result = dump_v1::DeserializeConfig(config_stream, config); !result) {
            LogStorageError("deserialize_config_v2", filepath, result.error().message());
            return result;
          }
          config_found = true;
          break;
        }

        case dump_format::SectionType::kStatistics: {
          if (stats != nullptr) {
            std::istringstream stats_stream(section_data);
            if (auto result = dump_v1::DeserializeStatistics(stats_stream, *stats); !result) {
              LogStorageError("deserialize_statistics_v2", filepath, result.error().message());
              return result;
            }
          }
          break;
        }

        case dump_format::SectionType::kTableData: {
          std::istringstream table_stream(section_data);

          // Read table name
          std::string table_name;
          if (!dump_v1::internal::ReadString(table_stream, table_name, kMaxIdentifierLength)) {
            return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read table name"));
          }
          dump_tables.insert(table_name);

          // Read table statistics
          uint32_t table_stats_len = 0;
          if (!ReadBinary(table_stream, table_stats_len)) {
            return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read table stats length"));
          }
          if (table_stats_len > 0 && table_stats != nullptr) {
            std::string ts_data(table_stats_len, '\0');
            table_stream.read(ts_data.data(), static_cast<std::streamsize>(table_stats_len));
            std::istringstream ts_stream(ts_data);
            TableStatistics table_stat;
            if (auto result = dump_v1::DeserializeTableStatistics(ts_stream, table_stat); !result) {
              return result;
            }
            (*table_stats)[table_name] = table_stat;
          } else if (table_stats_len > 0) {
            table_stream.seekg(table_stats_len, std::ios::cur);
          }

          // Check if table context exists
          if (table_contexts.count(table_name) == 0) {
            StructuredLog()
                .Event("storage_warning")
                .Field("type", "table_not_found")
                .Field("operation", "load_dump_v2")
                .Field("table", table_name)
                .Warn();
            // Skip remaining data in this section (already consumed by stringstream)
            break;
          }

          auto& ctx_pair = table_contexts[table_name];
          PendingTableLoad pending;
          pending.table_name = table_name;
          pending.index = ctx_pair.first;
          pending.doc_store = ctx_pair.second;

          // Read index data
          uint64_t index_len = 0;
          if (!ReadBinary(table_stream, index_len)) {
            return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read index length"));
          }
          std::string index_data;
          if (index_len > 0) {
            index_data.resize(index_len);
            table_stream.read(index_data.data(), static_cast<std::streamsize>(index_len));
            if (!table_stream.good()) {
              return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read index data"));
            }
          } else {
            return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Invalid index length"));
          }

          // Read docstore data
          uint64_t doc_len = 0;
          if (!ReadBinary(table_stream, doc_len)) {
            return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read docstore length"));
          }
          std::string doc_data;
          doc_data.resize(doc_len);
          table_stream.read(doc_data.data(), static_cast<std::streamsize>(doc_len));
          if (!table_stream.good()) {
            return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read docstore data"));
          }

          if (auto result = LoadPendingTableData(pending, index_data, doc_data); !result) {
            return result;
          }

          pending_table_loads.push_back(std::move(pending));
          break;
        }

        default:
          // Unknown section type: skip (forward compatibility)
          StructuredLog()
              .Event("dump_v2_unknown_section")
              .Field("section_type", static_cast<uint64_t>(static_cast<uint32_t>(envelope.type)))
              .Field("data_length", envelope.data_length)
              .Warn();
          break;
      }

      ++sections_read;
    }

    // Validate section count matches header
    if (sections_read != header.section_count) {
      StructuredLog()
          .Event("storage_validation_error")
          .Field("type", "section_count_mismatch")
          .Field("filepath", filepath)
          .Field("expected", static_cast<uint64_t>(header.section_count))
          .Field("actual", static_cast<uint64_t>(sections_read))
          .Error();
      return MakeUnexpected(MakeError(
          ErrorCode::kStorageDumpReadError,
          "Expected " + std::to_string(header.section_count) + " sections but found " + std::to_string(sections_read)));
    }

    // Validate required sections
    if (!config_found) {
      StructuredLog()
          .Event("storage_validation_error")
          .Field("type", "missing_config_section")
          .Field("filepath", filepath)
          .Error();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "V2 dump missing required Config section"));
    }

    if (config_validator) {
      if (auto result = config_validator(config); !result) {
        return result;
      }
    }

    if (auto result = ValidateDumpTableSet(dump_tables, table_contexts); !result) {
      return result;
    }

    if (auto result = ApplyPendingTableLoads(pending_table_loads); !result) {
      return result;
    }

    StructuredLog()
        .Event("dump_v2_loaded")
        .Field("filepath", filepath)
        .Field("sections_read", static_cast<uint64_t>(sections_read))
        .Info();

    return {};

  } catch (const std::exception& e) {
    LogStorageError("read_dump_v2_exception", filepath, e.what());
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
  }
}

// ============================================================================
// CRC32 Streaming Calculation
// ============================================================================

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
      break;
    }

    // Zero out the CRC field if it falls within this chunk
    if (crc_offset >= bytes_read && crc_offset < bytes_read + actually_read) {
      size_t offset_in_chunk = crc_offset - bytes_read;
      size_t zero_bytes = std::min(kCrcFieldSize, actually_read - offset_in_chunk);
      std::memset(&buffer[offset_in_chunk], 0, zero_bytes);
    }
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
// Version Dispatch Functions
// ============================================================================

Expected<void, Error> WriteDump(
    const std::string& filepath, const std::string& gtid, const config::Config& config,
    const std::unordered_map<std::string, std::pair<index::Index*, DocumentStore*>>& table_contexts,
    const DumpStatistics* stats, const std::unordered_map<std::string, TableStatistics>* table_stats,
    const DumpTableProgressCallback& table_progress_callback) {
  // Always write in the latest format (V2)
  return WriteDumpV2(filepath, gtid, config, table_contexts, stats, table_stats, table_progress_callback);
}

Expected<void, Error> ReadDump(
    const std::string& filepath, std::string& gtid, config::Config& config,
    std::unordered_map<std::string, std::pair<index::Index*, DocumentStore*>>& table_contexts, DumpStatistics* stats,
    std::unordered_map<std::string, TableStatistics>* table_stats, dump_format::IntegrityError* integrity_error,
    const DumpConfigValidationCallback& config_validator) {
  // Read magic + version to determine format
  std::ifstream ifs(filepath, std::ios::binary);
  if (!ifs) {
    LogStorageError("open_file", filepath, "Failed to open for reading");
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Read operation failed"));
  }

  std::array<char, 4> magic{};
  ifs.read(magic.data(), 4);
  if (!ifs.good() || std::memcmp(magic.data(), dump_format::kMagicNumber.data(), 4) != 0) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Invalid magic number"));
  }

  uint32_t version = 0;
  if (!ReadBinary(ifs, version)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read version"));
  }
  ifs.close();

  if (version < dump_format::kMinSupportedVersion || version > dump_format::kMaxSupportedVersion) {
    StructuredLog()
        .Event("storage_validation_error")
        .Field("type", "unsupported_version")
        .Field("filepath", filepath)
        .Field("version", static_cast<uint64_t>(version))
        .Error();
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Unsupported dump version"));
  }

  if (version == static_cast<uint32_t>(dump_format::FormatVersion::V1)) {
    (void)config_validator;
    return dump_v1::ReadDumpV1(filepath, gtid, config, table_contexts, stats, table_stats, integrity_error);
  }

  return ReadDumpV2(filepath, gtid, config, table_contexts, stats, table_stats, integrity_error, config_validator);
}

Expected<void, Error> VerifyDumpIntegrity(const std::string& filepath, dump_format::IntegrityError& integrity_error) {
  // Read version and dispatch
  std::ifstream ifs(filepath, std::ios::binary);
  if (!ifs) {
    integrity_error.type = dump_format::CRCErrorType::FileCRC;
    integrity_error.message = "Failed to open file: " + filepath;
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
  }

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

  if (version < dump_format::kMinSupportedVersion || version > dump_format::kMaxSupportedVersion) {
    integrity_error.type = dump_format::CRCErrorType::FileCRC;
    integrity_error.message = "Unsupported version: " + std::to_string(version);
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
  }

  ifs.close();

  if (version == static_cast<uint32_t>(dump_format::FormatVersion::V1)) {
    return dump_v1::VerifyDumpIntegrity(filepath, integrity_error);
  }

  // V2 integrity verification
  try {
    std::ifstream ifs2(filepath, std::ios::binary);
    if (!ifs2) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "Failed to reopen file";
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
    }

    // Skip magic + version (already verified)
    ifs2.seekg(dump_format::kFixedHeaderSize);

    HeaderV2 header;
    if (auto result = ReadHeaderV2(ifs2, header); !result) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "Failed to read V2 header: " + result.error().message();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
    }
    if (auto result = ValidateHeaderIntegrityFields(header); !result) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = result.error().message();
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
    }
    const auto section_start = ifs2.tellg();

    // Verify file size
    ifs2.seekg(0, std::ios::end);
    auto actual_size = static_cast<uint64_t>(ifs2.tellg());
    if (actual_size != header.total_file_size) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "File size mismatch: expected " + std::to_string(header.total_file_size) +
                                " bytes, got " + std::to_string(actual_size) + " bytes";
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
    }

    // Verify file-level CRC32
    if (header.file_crc32 != 0) {
      ifs2.seekg(0, std::ios::end);
      auto file_size = static_cast<uint64_t>(ifs2.tellg());
      const auto crc_offset = static_cast<size_t>(kV2HeaderFileCRC32Offset);
      uint32_t calculated_crc = CalculateCRC32Streaming(ifs2, file_size, crc_offset);

      if (calculated_crc != header.file_crc32) {
        integrity_error.type = dump_format::CRCErrorType::FileCRC;
        integrity_error.message = "CRC32 checksum mismatch";
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
      }
    }

    ifs2.clear();
    ifs2.seekg(section_start);
    uint32_t sections_read = 0;
    for (uint32_t i = 0; i < header.section_count; ++i) {
      dump_format::SectionEnvelope envelope;
      if (auto result = ReadSectionEnvelope(ifs2, envelope); !result) {
        integrity_error.type = dump_format::CRCErrorType::SectionCRC;
        integrity_error.message = "Failed to read section envelope " + std::to_string(i + 1) + " of " +
                                  std::to_string(header.section_count) + ": " + result.error().message();
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
      }

      if (envelope.data_length > actual_size ||
          static_cast<uint64_t>(ifs2.tellg()) + envelope.data_length > actual_size) {
        integrity_error.type = dump_format::CRCErrorType::SectionCRC;
        integrity_error.message = "Section data extends beyond end of file";
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
      }

      std::string section_data(envelope.data_length, '\0');
      if (envelope.data_length > 0) {
        ifs2.read(section_data.data(), static_cast<std::streamsize>(envelope.data_length));
        if (!ifs2.good()) {
          integrity_error.type = dump_format::CRCErrorType::SectionCRC;
          integrity_error.message = "Failed to read section data";
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
        }
      }

      uint32_t actual_crc = ComputeCRC32(section_data);
      if (actual_crc != envelope.crc32) {
        integrity_error.type = dump_format::CRCErrorType::SectionCRC;
        integrity_error.message = "Section CRC32 mismatch";
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
      }
      ++sections_read;
    }

    if (sections_read != header.section_count) {
      integrity_error.type = dump_format::CRCErrorType::SectionCRC;
      integrity_error.message = "Section count mismatch: expected " + std::to_string(header.section_count) +
                                " sections, got " + std::to_string(sections_read);
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Integrity verification failed"));
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

Expected<void, Error> GetDumpInfo(const std::string& filepath, DumpV2Info& info) {
  try {
    std::ifstream ifs(filepath, std::ios::binary | std::ios::ate);
    if (!ifs) {
      LogStorageError("open_file", filepath, "Failed to open for dump info");
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Get dump info failed"));
    }

    info.file_size = static_cast<uint64_t>(ifs.tellg());
    ifs.seekg(0, std::ios::beg);

    // Read magic
    std::array<char, 4> magic{};
    ifs.read(magic.data(), 4);
    if (!ifs.good() || std::memcmp(magic.data(), dump_format::kMagicNumber.data(), 4) != 0) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Invalid magic number"));
    }

    // Read version
    if (!ReadBinary(ifs, info.version)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read version"));
    }

    if (info.version < dump_format::kMinSupportedVersion || info.version > dump_format::kMaxSupportedVersion) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Unsupported version"));
    }

    // V1 dispatch
    if (info.version == static_cast<uint32_t>(dump_format::FormatVersion::V1)) {
      dump_v1::DumpInfo v1_info;
      ifs.close();
      if (auto result = dump_v1::GetDumpInfo(filepath, v1_info); !result) {
        return result;
      }
      info.version = v1_info.version;
      info.gtid = v1_info.gtid;
      info.table_count = v1_info.table_count;
      info.flags = v1_info.flags;
      info.file_size = v1_info.file_size;
      info.timestamp = v1_info.timestamp;
      info.has_statistics = v1_info.has_statistics;
      info.section_count = 0;
      return {};
    }

    // V2: Read header
    HeaderV2 header;
    if (auto result = ReadHeaderV2(ifs, header); !result) {
      return result;
    }

    info.gtid = header.gtid;
    info.flags = header.flags;
    info.timestamp = header.dump_timestamp;
    info.section_count = header.section_count;
    info.has_statistics = (header.flags & dump_format::flags_v2::kWithStatistics) != 0;

    // Read section envelopes to catalog types and count tables
    uint32_t table_count = 0;
    for (uint32_t i = 0; i < header.section_count; ++i) {
      dump_format::SectionEnvelope envelope;
      if (auto result = ReadSectionEnvelope(ifs, envelope); !result) {
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError,
                                        "Failed to read section envelope " + std::to_string(i + 1) + " of " +
                                            std::to_string(header.section_count) + ": " + result.error().message()));
      }

      info.section_types.push_back(envelope.type);

      if (envelope.type == dump_format::SectionType::kTableData) {
        ++table_count;
      }

      // Skip section data
      if (envelope.data_length > 0) {
        if (envelope.data_length > info.file_size ||
            static_cast<uint64_t>(ifs.tellg()) + envelope.data_length > info.file_size) {
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Section data extends beyond end of file"));
        }
        ifs.seekg(static_cast<std::streamoff>(envelope.data_length), std::ios::cur);
        if (!ifs.good()) {
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to skip section data"));
        }
      }
    }

    info.table_count = table_count;
    return {};

  } catch (const std::exception& e) {
    LogStorageError("get_dump_info_exception", filepath, e.what());
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Get dump info failed"));
  }
}

}  // namespace mygramdb::storage::dump_v2
