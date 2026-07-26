/**
 * @file dump_format_internal.cpp
 * @brief Shared implementation details for all dump format versions
 */

#include "storage/dump_format_internal.h"

#include <zlib.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <sstream>

#include "storage/dump_load_access.h"
#include "utils/structured_log.h"

#ifndef _WIN32
#include <unistd.h>
#endif

namespace mygramdb::storage::dump_internal {

using mygram::utils::ErrorCode;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;
using mygram::utils::StructuredLog;

#ifndef _WIN32
FdStreambuf::FdStreambuf(int fd) : fd_(fd) {
  setp(buffer_.data(), buffer_.data() + buffer_.size());
}

FdStreambuf::~FdStreambuf() {
  sync();
}

int FdStreambuf::overflow(int ch) {
  // overflow() is entered with pptr() == epptr(). Flush before storing the
  // incoming character so that buffer_[kBufferSize] is never written.
  if (FlushBuffer() < 0) {
    return traits_type::eof();
  }
  if (ch != traits_type::eof()) {
    *pptr() = static_cast<char>(ch);
    pbump(1);
  }
  return traits_type::not_eof(ch);
}

int FdStreambuf::sync() {
  return FlushBuffer();
}

int FdStreambuf::FlushBuffer() {
  size_t bytes = static_cast<size_t>(pptr() - pbase());
  const char* current = pbase();
  while (bytes > 0) {
    const auto written = ::write(fd_, current, bytes);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return -1;
    }
    if (written == 0) {
      return -1;
    }
    current += written;
    bytes -= static_cast<size_t>(written);
  }
  setp(buffer_.data(), buffer_.data() + buffer_.size());
  return 0;
}

bool WriteAllAt(int fd, const void* data, size_t size, off_t offset) {
  const auto* current = static_cast<const char*>(data);
  size_t remaining = size;
  off_t current_offset = offset;
  while (remaining > 0) {
    const auto written = pwrite(fd, current, remaining, current_offset);
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

Expected<uint32_t, Error> CalculateCRC32StreamingFd(int fd, uint64_t file_size, size_t crc_offset) {
  constexpr size_t kChunkSize = 1024 * 1024;
  constexpr size_t kCrcFieldSize = sizeof(uint32_t);
  uint32_t crc = 0;
  std::vector<char> buffer(kChunkSize);
  uint64_t bytes_read = 0;

  while (bytes_read < file_size) {
    const size_t to_read = static_cast<size_t>(std::min<uint64_t>(kChunkSize, file_size - bytes_read));
    const auto actually_read = pread(fd, buffer.data(), to_read, static_cast<off_t>(bytes_read));
    if (actually_read < 0) {
      if (errno == EINTR) {
        continue;
      }
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to read file for CRC32"));
    }
    if (actually_read == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Unexpected EOF while calculating CRC32"));
    }

    const auto chunk_size = static_cast<size_t>(actually_read);
    if (crc_offset >= bytes_read && crc_offset < bytes_read + chunk_size) {
      const size_t offset_in_chunk = crc_offset - bytes_read;
      const size_t zero_bytes = std::min(kCrcFieldSize, chunk_size - offset_in_chunk);
      std::memset(buffer.data() + offset_in_chunk, 0, zero_bytes);
    }
    if (crc_offset + kCrcFieldSize > bytes_read && crc_offset < bytes_read) {
      const size_t zero_bytes = std::min<size_t>(kCrcFieldSize - (bytes_read - crc_offset), chunk_size);
      std::memset(buffer.data(), 0, zero_bytes);
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
    const size_t to_read = static_cast<size_t>(std::min<uint64_t>(kChunkSize, length - bytes_read));
    const auto actually_read = pread(fd, buffer.data(), to_read, start_offset + static_cast<off_t>(bytes_read));
    if (actually_read < 0) {
      if (errno == EINTR) {
        continue;
      }
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to read section for CRC32"));
    }
    if (actually_read == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Unexpected EOF while calculating CRC32"));
    }
    const auto chunk_size = static_cast<size_t>(actually_read);
    crc = static_cast<uint32_t>(
        crc32(crc, reinterpret_cast<const Bytef*>(buffer.data()), static_cast<uInt>(chunk_size)));  // NOLINT
    bytes_read += chunk_size;
  }
  return crc;
}
#endif

uint32_t CalculateCRC32Streaming(std::ifstream& input, uint64_t file_size, size_t crc_offset) {
  constexpr size_t kChunkSize = 1024 * 1024;
  constexpr size_t kCrcFieldSize = sizeof(uint32_t);
  input.clear();
  input.seekg(0, std::ios::beg);

  uint32_t crc = 0;
  std::vector<char> buffer(kChunkSize);
  uint64_t bytes_read = 0;
  while (bytes_read < file_size) {
    const size_t to_read = static_cast<size_t>(std::min<uint64_t>(kChunkSize, file_size - bytes_read));
    input.read(buffer.data(), static_cast<std::streamsize>(to_read));
    const auto actually_read = static_cast<size_t>(input.gcount());
    if (actually_read == 0) {
      break;
    }
    if (crc_offset >= bytes_read && crc_offset < bytes_read + actually_read) {
      const size_t offset_in_chunk = crc_offset - bytes_read;
      const size_t zero_bytes = std::min(kCrcFieldSize, actually_read - offset_in_chunk);
      std::memset(buffer.data() + offset_in_chunk, 0, zero_bytes);
    }
    if (crc_offset + kCrcFieldSize > bytes_read && crc_offset < bytes_read) {
      const size_t zero_bytes = std::min<size_t>(kCrcFieldSize - (bytes_read - crc_offset), actually_read);
      std::memset(buffer.data(), 0, zero_bytes);
    }
    crc = static_cast<uint32_t>(
        crc32(crc, reinterpret_cast<const Bytef*>(buffer.data()), static_cast<uInt>(actually_read)));  // NOLINT
    bytes_read += actually_read;
  }
  return crc;
}

BoundedInputStreambuf::BoundedInputStreambuf(std::istream& source, uint64_t length, bool track_crc)
    : source_(source), source_remaining_(length), buffer_(kBufferSize), track_crc_(track_crc) {
  setg(buffer_.data(), buffer_.data(), buffer_.data());
}

uint64_t BoundedInputStreambuf::Remaining() const {
  return source_remaining_ + static_cast<uint64_t>(egptr() - gptr());
}

BoundedInputStreambuf::int_type BoundedInputStreambuf::underflow() {
  if (gptr() < egptr()) {
    return traits_type::to_int_type(*gptr());
  }
  if (source_remaining_ == 0) {
    return traits_type::eof();
  }
  const size_t requested = static_cast<size_t>(std::min<uint64_t>(buffer_.size(), source_remaining_));
  source_.read(buffer_.data(), static_cast<std::streamsize>(requested));
  const auto count = source_.gcount();
  if (count <= 0) {
    return traits_type::eof();
  }
  if (track_crc_) {
    crc32_ = static_cast<uint32_t>(
        crc32(crc32_, reinterpret_cast<const Bytef*>(buffer_.data()), static_cast<uInt>(count)));  // NOLINT
  }
  source_remaining_ -= static_cast<uint64_t>(count);
  setg(buffer_.data(), buffer_.data(), buffer_.data() + count);
  return traits_type::to_int_type(*gptr());
}

std::streamsize BoundedInputStreambuf::xsgetn(char* destination, std::streamsize count) {
  if (count <= 0) {
    return 0;
  }
  std::streamsize copied = 0;
  const std::streamsize buffered = egptr() - gptr();
  const std::streamsize from_buffer = std::min(count, buffered);
  if (from_buffer > 0) {
    std::memcpy(destination, gptr(), static_cast<size_t>(from_buffer));
    gbump(static_cast<int>(from_buffer));
    copied += from_buffer;
  }
  const uint64_t wanted = std::min<uint64_t>(static_cast<uint64_t>(count - copied), source_remaining_);
  if (wanted > 0) {
    source_.read(destination + copied, static_cast<std::streamsize>(wanted));
    const auto read = source_.gcount();
    if (read > 0) {
      if (track_crc_) {
        crc32_ = static_cast<uint32_t>(
            crc32(crc32_, reinterpret_cast<const Bytef*>(destination + copied), static_cast<uInt>(read)));  // NOLINT
      }
      source_remaining_ -= static_cast<uint64_t>(read);
      copied += read;
    }
  }
  return copied;
}

BoundedInputStream::BoundedInputStream(std::istream& source, uint64_t length, bool track_crc)
    : std::istream(nullptr), buffer_(source, length, track_crc) {
  rdbuf(&buffer_);
}

uint64_t BoundedInputStream::Remaining() const {
  return buffer_.Remaining();
}

bool BoundedInputStream::Drain() {
  std::vector<char> discard(64 * 1024);
  while (Remaining() > 0) {
    const auto chunk = static_cast<std::streamsize>(std::min<uint64_t>(discard.size(), Remaining()));
    read(discard.data(), chunk);
    if (gcount() != chunk) {
      return false;
    }
  }
  return true;
}

Expected<void, Error> LoadPendingIndex(PendingTableLoad& pending, std::istream& index_stream) {
  auto loaded_index = std::make_unique<index::Index>(
      pending.index->GetNgramSize(), pending.index->GetKanjiNgramSize(), pending.index->GetRoaringThreshold(),
      pending.index->GetCrossBoundaryNgrams(), pending.index->GetNormalizeNfkc(), pending.index->GetNormalizeWidth(),
      pending.index->GetNormalizeLower());
  if (auto result = loaded_index->LoadFromStream(index_stream); !result) {
    return MakeUnexpected(
        MakeError(ErrorCode::kStorageDumpReadError, "LoadFromStream failed for index", result.error().message()));
  }
  pending.loaded_index = std::move(loaded_index);
  return {};
}

Expected<void, Error> LoadPendingDocumentStore(PendingTableLoad& pending, std::istream& doc_stream) {
  auto loaded_doc_store = std::make_unique<DocumentStore>();
  if (auto result = loaded_doc_store->LoadFromStream(doc_stream, nullptr); !result) {
    return result;
  }
  pending.loaded_doc_store = std::move(loaded_doc_store);
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
  if (!DumpLoadAccess::ReplaceLoadedTables(std::move(replacements))) {
    return MakeUnexpected(
        MakeError(ErrorCode::kStorageDumpReadError, "Duplicate or invalid table replacement in dump"));
  }
  for (const auto& pending : pending_loads) {
    StructuredLog().Event("dump_table_loaded").Field("table", pending.table_name).Info();
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
  const auto join_names = [](const std::vector<std::string>& names) {
    std::ostringstream output;
    for (size_t i = 0; i < names.size(); ++i) {
      if (i > 0) {
        output << ",";
      }
      output << names[i];
    }
    return output.str();
  };
  const std::string missing = join_names(missing_tables);
  const std::string unexpected = join_names(unexpected_tables);
  return MakeUnexpected(
      MakeError(ErrorCode::kStorageDumpReadError,
                "Dump table set does not match configured tables. Missing configured tables: " + missing +
                    "; unexpected dump tables: " + unexpected + "; missing=" + missing + "; unexpected=" + unexpected));
}

}  // namespace mygramdb::storage::dump_internal
