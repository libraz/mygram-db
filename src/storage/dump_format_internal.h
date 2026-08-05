/**
 * @file dump_format_internal.h
 * @brief Shared implementation details for all dump format versions
 */

#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <istream>
#include <memory>
#include <ostream>
#include <streambuf>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "index/index.h"
#include "storage/document_store.h"
#include "utils/binary_io.h"
#include "utils/error.h"
#include "utils/expected.h"

#ifndef _WIN32
#include <sys/types.h>
#endif

namespace mygramdb::storage::dump_internal {

using mygram::utils::Error;
using mygram::utils::Expected;

#ifndef _WIN32
/**
 * Buffered output over an already-open descriptor. The caller owns the fd.
 */
class FdStreambuf : public std::streambuf {
 public:
  explicit FdStreambuf(int fd);
  ~FdStreambuf() override;

  FdStreambuf(const FdStreambuf&) = delete;
  FdStreambuf& operator=(const FdStreambuf&) = delete;
  FdStreambuf(FdStreambuf&&) = delete;
  FdStreambuf& operator=(FdStreambuf&&) = delete;

 protected:
  int overflow(int ch) override;
  int sync() override;

 private:
  static constexpr size_t kBufferSize = 8192;

  int FlushBuffer();

  int fd_;
  std::array<char, kBufferSize> buffer_{};
};

bool WriteAllAt(int fd, const void* data, size_t size, off_t offset);

template <typename T>
bool WriteBinaryAt(int fd, T value, off_t offset) {
  if constexpr (std::is_integral_v<T>) {
    value = mygram::utils::ToLittleEndian(value);
  }
  return WriteAllAt(fd, &value, sizeof(value), offset);
}

Expected<uint32_t, Error> CalculateCRC32StreamingFd(int fd, uint64_t file_size, size_t crc_offset);
Expected<uint32_t, Error> CalculateCRC32RangeFd(int fd, off_t start_offset, uint64_t length);
#endif

uint32_t CalculateCRC32Streaming(std::ifstream& input, uint64_t file_size, size_t crc_offset);

class BoundedInputStreambuf : public std::streambuf {
 public:
  BoundedInputStreambuf(std::istream& source, uint64_t length, bool track_crc = false);

  [[nodiscard]] uint64_t Remaining() const;
  [[nodiscard]] uint32_t Crc32() const { return crc32_; }

 protected:
  int_type underflow() override;
  std::streamsize xsgetn(char* destination, std::streamsize count) override;

 private:
  static constexpr size_t kBufferSize = 64 * 1024;
  std::istream& source_;
  uint64_t source_remaining_;
  std::vector<char> buffer_;
  bool track_crc_;
  uint32_t crc32_ = 0;
};

class BoundedInputStream : public std::istream {
 public:
  BoundedInputStream(std::istream& source, uint64_t length, bool track_crc = false);

  [[nodiscard]] uint64_t Remaining() const;
  [[nodiscard]] uint32_t Crc32() const { return buffer_.Crc32(); }
  bool Drain();

 private:
  BoundedInputStreambuf buffer_;
};

/**
 * Bound the peak working set before Index::LoadFromStream materializes its
 * length-delimited payload. Serialized bytes may coexist with decoded index
 * containers and their growth slack, so reserve three times the encoded
 * length conservatively.
 */
Expected<void, Error> ValidateRestoreMaterializationBudget(uint64_t staged_memory_bytes, uint64_t encoded_length,
                                                           uint64_t memory_budget_bytes, std::string_view section_name,
                                                           std::string_view format_name);

struct PendingTableLoad {
  std::string table_name;
  index::Index* index = nullptr;
  DocumentStore* doc_store = nullptr;
  std::unique_ptr<index::Index> loaded_index;
  std::unique_ptr<DocumentStore> loaded_doc_store;
};

Expected<void, Error> LoadPendingIndex(PendingTableLoad& pending, std::istream& index_stream);
Expected<void, Error> LoadPendingDocumentStore(PendingTableLoad& pending, std::istream& doc_stream);
Expected<void, Error> ApplyPendingTableLoads(const std::vector<PendingTableLoad>& pending_loads);
Expected<void, Error> ValidateDumpTableSet(
    const std::unordered_set<std::string>& dump_tables,
    const std::unordered_map<std::string, std::pair<index::Index*, DocumentStore*>>& table_contexts);

}  // namespace mygramdb::storage::dump_internal
