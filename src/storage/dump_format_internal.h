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
#include "utils/endian_utils.h"
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
 * Encoded-to-resident expansion of an index payload.
 *
 * An index section is dominated by term bytes and posting-list payload, both of
 * which are written close to their in-memory form; the serialized bytes may
 * still coexist with the decoded containers and their growth slack.
 */
inline constexpr uint64_t kIndexMaterializationFactor = 3;

/**
 * Encoded bytes a document costs no matter how little it carries: its DocID and
 * the length prefixes for its primary key, filter list, normalized text and
 * original text. Subtracting these leaves an upper bound on the byte payload
 * that survives into the store, so the estimate stays a lower bound.
 */
inline constexpr uint64_t kEncodedHeaderBytesPerDocument = 20;

/**
 * Resident bytes a document costs no matter how little it carries.
 *
 * Every restored document occupies a slot in the DocID-to-key map, the
 * key-to-DocID map and the filter map, and each slot holds a std::string or a
 * value vector plus its control byte. This is deliberately below the smallest
 * per-document cost any platform produces, so that scaling it by a document
 * count can only under-state what the decode will allocate.
 * Measured in tests/storage/restore_materialization_test.cpp.
 */
inline constexpr uint64_t kMinimumResidentBytesPerDocument = 80;

/**
 * Refuse a section whose materialized form cannot fit in what the restore
 * budget still has free, before any of it is decoded. @p materialization_factor
 * scales the encoded length to the resident size it expands into.
 */
Expected<void, Error> ValidateRestoreMaterializationBudget(uint64_t staged_memory_bytes, uint64_t encoded_length,
                                                           uint64_t memory_budget_bytes, std::string_view section_name,
                                                           std::string_view format_name,
                                                           uint64_t materialization_factor);

/**
 * Smallest resident size a document-store payload of @p encoded_length bytes
 * holding @p document_count documents can decode into.
 *
 * A document-store section expands by anywhere from under two to over seven
 * times depending on how much of its bytes are primary keys and text rather
 * than per-document framing, so a single factor over the encoded length cannot
 * both refuse a section of minimal documents and admit a text-heavy one. The
 * document count separates the two, and it can be trusted because it is the
 * same count the decoder will loop over: understating it shortens the decode,
 * and overstating it runs the decode out of bytes.
 */
[[nodiscard]] uint64_t EstimateDocumentSectionResidentFloor(uint64_t encoded_length, uint64_t document_count);

/**
 * Header a document-store payload opens with, read before the payload is
 * decoded so that the restore budget can be applied to the document count.
 * @p consumed_prefix holds the bytes taken off the stream to read it, which the
 * decoder still needs to see.
 */
struct DocumentSectionHeader {
  uint64_t document_count = 0;
  std::string consumed_prefix;
};

/**
 * Read the fixed header of a document-store payload from @p input_stream,
 * consuming at most @p encoded_length bytes. Nothing is validated beyond what
 * is needed to locate the document count; the decoder re-checks every field.
 */
Expected<void, Error> ReadDocumentSectionHeader(std::istream& input_stream, uint64_t encoded_length,
                                                DocumentSectionHeader& header);

/**
 * Refuse a document-store section that provably cannot fit in what the restore
 * budget still has free, before any of it is decoded.
 */
Expected<void, Error> ValidateRestoreDocumentBudget(uint64_t staged_memory_bytes, uint64_t encoded_length,
                                                    uint64_t document_count, uint64_t memory_budget_bytes,
                                                    std::string_view format_name);

/**
 * Serves an already-consumed prefix ahead of the rest of a stream, so a decoder
 * can be handed a payload whose header was inspected first. The prefix is
 * copied; the rest is read through by reference and must outlive this stream.
 */
class PrefixedInputStreambuf : public std::streambuf {
 public:
  PrefixedInputStreambuf(std::string prefix, std::istream& rest);

  PrefixedInputStreambuf(const PrefixedInputStreambuf&) = delete;
  PrefixedInputStreambuf& operator=(const PrefixedInputStreambuf&) = delete;
  PrefixedInputStreambuf(PrefixedInputStreambuf&&) = delete;
  PrefixedInputStreambuf& operator=(PrefixedInputStreambuf&&) = delete;
  ~PrefixedInputStreambuf() override = default;

 protected:
  int_type underflow() override;
  std::streamsize xsgetn(char* dest, std::streamsize count) override;

 private:
  static constexpr size_t kPrefixReplayBufferSize = 64 * 1024;

  std::string prefix_;
  size_t prefix_pos_ = 0;
  std::istream& rest_;
  std::vector<char> buffer_;
};

class PrefixedInputStream : public std::istream {
 public:
  PrefixedInputStream(std::string prefix, std::istream& rest);

 private:
  PrefixedInputStreambuf buffer_;
};

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
