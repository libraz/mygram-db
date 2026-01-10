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
#include <sstream>
#include <vector>

#include "utils/endian_utils.h"
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

// Forward declaration for streaming CRC calculation
uint32_t CalculateCRC32Streaming(std::ifstream& ifs, uint64_t file_size, size_t crc_offset);

namespace {

/**
 * @brief Write binary data to stream in little-endian format
 *
 * All multi-byte integers are stored in little-endian format for
 * cross-platform compatibility, as specified in dump_format_v1.h.
 */
template <typename T>
bool WriteBinary(std::ostream& output_stream, const T& value) {
  if constexpr (std::is_same_v<T, double>) {
    double le_value = mygram::utils::ToLittleEndianDouble(value);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    output_stream.write(reinterpret_cast<const char*>(&le_value), sizeof(T));
  } else if constexpr (std::is_integral_v<T>) {
    T le_value = mygram::utils::ToLittleEndian(value);
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    output_stream.write(reinterpret_cast<const char*>(&le_value), sizeof(T));
  } else {
    // For non-integral types (e.g., structs), write as-is
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    output_stream.write(reinterpret_cast<const char*>(&value), sizeof(T));
  }
  return output_stream.good();
}

/**
 * @brief Read binary data from stream in little-endian format
 *
 * All multi-byte integers are stored in little-endian format for
 * cross-platform compatibility, as specified in dump_format_v1.h.
 */
template <typename T>
bool ReadBinary(std::istream& input_stream, T& value) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  input_stream.read(reinterpret_cast<char*>(&value), sizeof(T));
  if (!input_stream.good()) {
    return false;
  }

  if constexpr (std::is_same_v<T, double>) {
    value = mygram::utils::FromLittleEndianDouble(value);
  } else if constexpr (std::is_integral_v<T>) {
    value = mygram::utils::FromLittleEndian(value);
  }
  // For non-integral types (e.g., structs), keep as-is

  return true;
}

/**
 * @brief Write string to stream (length-prefixed)
 */
bool WriteString(std::ostream& output_stream, const std::string& str) {
  auto len = static_cast<uint32_t>(str.size());
  if (!WriteBinary(output_stream, len)) {
    return false;
  }
  if (len > 0) {
    output_stream.write(str.data(), len);
  }
  return output_stream.good();
}

/**
 * @brief Read string from stream (length-prefixed)
 */
bool ReadString(std::istream& input_stream, std::string& str) {
  constexpr uint32_t kMaxStringLength = 256 * 1024 * 1024;  // 256MB limit
  uint32_t len = 0;
  if (!ReadBinary(input_stream, len)) {
    return false;
  }
  if (len > kMaxStringLength) {
    StructuredLog()
        .Event("storage_validation_error")
        .Field("type", "string_length_exceeded")
        .Field("length", static_cast<uint64_t>(len))
        .Field("max_length", static_cast<uint64_t>(kMaxStringLength))
        .Error();
    return false;
  }
  if (len > 0) {
    str.resize(len);
    input_stream.read(str.data(), len);
  } else {
    str.clear();
  }
  return input_stream.good();
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
  if (!ReadString(input_stream, header.gtid)) {
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
// Config Serialization
// ============================================================================

namespace {

/**
 * @brief Serialize FilterConfig to stream
 */
bool SerializeFilterConfig(std::ostream& output_stream, const config::FilterConfig& filter) {
  if (!WriteString(output_stream, filter.name)) {
    return false;
  }
  if (!WriteString(output_stream, filter.type)) {
    return false;
  }
  if (!WriteBinary(output_stream, filter.dict_compress)) {
    return false;
  }
  if (!WriteBinary(output_stream, filter.bitmap_index)) {
    return false;
  }
  if (!WriteString(output_stream, filter.bucket)) {
    return false;
  }
  return true;
}

/**
 * @brief Deserialize FilterConfig from stream
 */
bool DeserializeFilterConfig(std::istream& input_stream, config::FilterConfig& filter) {
  if (!ReadString(input_stream, filter.name)) {
    return false;
  }
  if (!ReadString(input_stream, filter.type)) {
    return false;
  }
  if (!ReadBinary(input_stream, filter.dict_compress)) {
    return false;
  }
  if (!ReadBinary(input_stream, filter.bitmap_index)) {
    return false;
  }
  if (!ReadString(input_stream, filter.bucket)) {
    return false;
  }
  return true;
}

/**
 * @brief Serialize RequiredFilterConfig to stream
 */
bool SerializeRequiredFilterConfig(std::ostream& output_stream, const config::RequiredFilterConfig& filter) {
  if (!WriteString(output_stream, filter.name)) {
    return false;
  }
  if (!WriteString(output_stream, filter.type)) {
    return false;
  }
  if (!WriteString(output_stream, filter.op)) {
    return false;
  }
  if (!WriteString(output_stream, filter.value)) {
    return false;
  }
  if (!WriteBinary(output_stream, filter.bitmap_index)) {
    return false;
  }
  return true;
}

/**
 * @brief Deserialize RequiredFilterConfig from stream
 */
bool DeserializeRequiredFilterConfig(std::istream& input_stream, config::RequiredFilterConfig& filter) {
  if (!ReadString(input_stream, filter.name)) {
    return false;
  }
  if (!ReadString(input_stream, filter.type)) {
    return false;
  }
  if (!ReadString(input_stream, filter.op)) {
    return false;
  }
  if (!ReadString(input_stream, filter.value)) {
    return false;
  }
  if (!ReadBinary(input_stream, filter.bitmap_index)) {
    return false;
  }
  return true;
}

/**
 * @brief Serialize TableConfig to stream
 */
bool SerializeTableConfig(std::ostream& output_stream, const config::TableConfig& table) {
  if (!WriteString(output_stream, table.name)) {
    return false;
  }
  if (!WriteString(output_stream, table.primary_key)) {
    return false;
  }

  // text_source
  if (!WriteString(output_stream, table.text_source.column)) {
    return false;
  }
  auto concat_size = static_cast<uint32_t>(table.text_source.concat.size());
  if (!WriteBinary(output_stream, concat_size)) {
    return false;
  }
  for (const auto& col : table.text_source.concat) {
    if (!WriteString(output_stream, col)) {
      return false;
    }
  }
  if (!WriteString(output_stream, table.text_source.delimiter)) {
    return false;
  }

  // required_filters
  auto req_filter_count = static_cast<uint32_t>(table.required_filters.size());
  if (!WriteBinary(output_stream, req_filter_count)) {
    return false;
  }
  for (const auto& filter : table.required_filters) {
    if (!SerializeRequiredFilterConfig(output_stream, filter)) {
      return false;
    }
  }

  // filters
  auto filter_count = static_cast<uint32_t>(table.filters.size());
  if (!WriteBinary(output_stream, filter_count)) {
    return false;
  }
  for (const auto& filter : table.filters) {
    if (!SerializeFilterConfig(output_stream, filter)) {
      return false;
    }
  }

  // ngram sizes
  if (!WriteBinary(output_stream, table.ngram_size)) {
    return false;
  }
  if (!WriteBinary(output_stream, table.kanji_ngram_size)) {
    return false;
  }

  // posting config
  if (!WriteBinary(output_stream, table.posting.block_size)) {
    return false;
  }
  if (!WriteBinary(output_stream, table.posting.freq_bits)) {
    return false;
  }
  if (!WriteString(output_stream, table.posting.use_roaring)) {
    return false;
  }

  return true;
}

/**
 * @brief Deserialize TableConfig from stream
 */
bool DeserializeTableConfig(std::istream& input_stream, config::TableConfig& table) {
  if (!ReadString(input_stream, table.name)) {
    return false;
  }
  if (!ReadString(input_stream, table.primary_key)) {
    return false;
  }

  // text_source
  constexpr uint32_t kMaxConcatColumns = 1000;
  constexpr uint32_t kMaxFilterCount = 1000;
  if (!ReadString(input_stream, table.text_source.column)) {
    return false;
  }
  uint32_t concat_size = 0;
  if (!ReadBinary(input_stream, concat_size)) {
    return false;
  }
  if (concat_size > kMaxConcatColumns) {
    StructuredLog()
        .Event("storage_validation_error")
        .Field("type", "concat_columns_exceeded")
        .Field("count", static_cast<uint64_t>(concat_size))
        .Field("max_count", static_cast<uint64_t>(kMaxConcatColumns))
        .Error();
    return false;
  }
  table.text_source.concat.resize(concat_size);
  for (uint32_t i = 0; i < concat_size; ++i) {
    if (!ReadString(input_stream, table.text_source.concat[i])) {
      return false;
    }
  }
  if (!ReadString(input_stream, table.text_source.delimiter)) {
    return false;
  }

  // required_filters
  uint32_t req_filter_count = 0;
  if (!ReadBinary(input_stream, req_filter_count)) {
    return false;
  }
  if (req_filter_count > kMaxFilterCount) {
    StructuredLog()
        .Event("storage_validation_error")
        .Field("type", "required_filters_exceeded")
        .Field("count", static_cast<uint64_t>(req_filter_count))
        .Field("max_count", static_cast<uint64_t>(kMaxFilterCount))
        .Error();
    return false;
  }
  table.required_filters.resize(req_filter_count);
  for (uint32_t i = 0; i < req_filter_count; ++i) {
    if (!DeserializeRequiredFilterConfig(input_stream, table.required_filters[i])) {
      return false;
    }
  }

  // filters
  uint32_t filter_count = 0;
  if (!ReadBinary(input_stream, filter_count)) {
    return false;
  }
  if (filter_count > kMaxFilterCount) {
    StructuredLog()
        .Event("storage_validation_error")
        .Field("type", "filters_exceeded")
        .Field("count", static_cast<uint64_t>(filter_count))
        .Field("max_count", static_cast<uint64_t>(kMaxFilterCount))
        .Error();
    return false;
  }
  table.filters.resize(filter_count);
  for (uint32_t i = 0; i < filter_count; ++i) {
    if (!DeserializeFilterConfig(input_stream, table.filters[i])) {
      return false;
    }
  }

  // ngram sizes
  if (!ReadBinary(input_stream, table.ngram_size)) {
    return false;
  }
  if (!ReadBinary(input_stream, table.kanji_ngram_size)) {
    return false;
  }

  // posting config
  if (!ReadBinary(input_stream, table.posting.block_size)) {
    return false;
  }
  if (!ReadBinary(input_stream, table.posting.freq_bits)) {
    return false;
  }
  if (!ReadString(input_stream, table.posting.use_roaring)) {
    return false;
  }

  return true;
}

}  // namespace

Expected<void, Error> SerializeConfig(std::ostream& output_stream, const config::Config& config) {
  // MySQL config (excluding sensitive credentials)
  if (!WriteString(output_stream, config.mysql.host)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write mysql.host"));
  }
  if (!WriteBinary(output_stream, config.mysql.port)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write mysql.port"));
  }
  // Write empty strings for user and password (security: do not persist credentials)
  std::string empty_user;
  std::string empty_password;
  if (!WriteString(output_stream, empty_user)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write mysql.user"));
  }
  if (!WriteString(output_stream, empty_password)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write mysql.password"));
  }
  if (!WriteString(output_stream, config.mysql.database)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write mysql.database"));
  }
  if (!WriteBinary(output_stream, config.mysql.use_gtid)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write mysql.use_gtid"));
  }
  if (!WriteString(output_stream, config.mysql.binlog_format)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write mysql.binlog_format"));
  }
  if (!WriteString(output_stream, config.mysql.binlog_row_image)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write mysql.binlog_row_image"));
  }
  if (!WriteBinary(output_stream, config.mysql.connect_timeout_ms)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write mysql.connect_timeout_ms"));
  }
  if (!WriteBinary(output_stream, config.mysql.read_timeout_ms)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write mysql.read_timeout_ms"));
  }
  if (!WriteBinary(output_stream, config.mysql.write_timeout_ms)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write mysql.write_timeout_ms"));
  }

  // Tables
  auto table_count = static_cast<uint32_t>(config.tables.size());
  if (!WriteBinary(output_stream, table_count)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write table_count"));
  }
  for (const auto& table : config.tables) {
    if (!SerializeTableConfig(output_stream, table)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write table config"));
    }
  }

  // Build config
  if (!WriteString(output_stream, config.build.mode)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write build.mode"));
  }
  if (!WriteBinary(output_stream, config.build.batch_size)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write build.batch_size"));
  }
  if (!WriteBinary(output_stream, config.build.parallelism)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write build.parallelism"));
  }
  if (!WriteBinary(output_stream, config.build.throttle_ms)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write build.throttle_ms"));
  }

  // Replication config
  if (!WriteBinary(output_stream, config.replication.enable)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write replication.enable"));
  }
  if (!WriteBinary(output_stream, config.replication.server_id)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write replication.server_id"));
  }
  if (!WriteString(output_stream, config.replication.start_from)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write replication.start_from"));
  }
  if (!WriteBinary(output_stream, config.replication.queue_size)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write replication.queue_size"));
  }
  if (!WriteBinary(output_stream, config.replication.reconnect_backoff_min_ms)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write replication.reconnect_backoff_min_ms"));
  }
  if (!WriteBinary(output_stream, config.replication.reconnect_backoff_max_ms)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write replication.reconnect_backoff_max_ms"));
  }

  // Memory config
  if (!WriteBinary(output_stream, config.memory.hard_limit_mb)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write memory.hard_limit_mb"));
  }
  if (!WriteBinary(output_stream, config.memory.soft_target_mb)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write memory.soft_target_mb"));
  }
  if (!WriteBinary(output_stream, config.memory.arena_chunk_mb)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write memory.arena_chunk_mb"));
  }
  if (!WriteBinary(output_stream, config.memory.roaring_threshold)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write memory.roaring_threshold"));
  }
  if (!WriteBinary(output_stream, config.memory.minute_epoch)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write memory.minute_epoch"));
  }
  if (!WriteBinary(output_stream, config.memory.normalize.nfkc)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write memory.normalize.nfkc"));
  }
  if (!WriteString(output_stream, config.memory.normalize.width)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write memory.normalize.width"));
  }
  if (!WriteBinary(output_stream, config.memory.normalize.lower)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write memory.normalize.lower"));
  }

  // Snapshot config
  if (!WriteString(output_stream, config.dump.dir)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write dump.dir"));
  }
  if (!WriteBinary(output_stream, config.dump.interval_sec)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write dump.interval_sec"));
  }
  if (!WriteBinary(output_stream, config.dump.retain)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write dump.retain"));
  }

  // API config
  if (!WriteString(output_stream, config.api.tcp.bind)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write api.tcp.bind"));
  }
  if (!WriteBinary(output_stream, config.api.tcp.port)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write api.tcp.port"));
  }
  if (!WriteBinary(output_stream, config.api.http.enable)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write api.http.enable"));
  }
  if (!WriteString(output_stream, config.api.http.bind)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write api.http.bind"));
  }
  if (!WriteBinary(output_stream, config.api.http.port)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write api.http.port"));
  }
  if (!WriteBinary(output_stream, config.api.default_limit)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write api.default_limit"));
  }

  // Network config
  auto cidr_count = static_cast<uint32_t>(config.network.allow_cidrs.size());
  if (!WriteBinary(output_stream, cidr_count)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write cidr_count"));
  }
  for (const auto& cidr : config.network.allow_cidrs) {
    if (!WriteString(output_stream, cidr)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write network CIDR"));
    }
  }

  // Logging config
  if (!WriteString(output_stream, config.logging.level)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write logging.level"));
  }
  if (!WriteString(output_stream, config.logging.format)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write logging.format"));
  }

  // Query limits
  if (!WriteBinary(output_stream, config.api.max_query_length)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Failed to write api.max_query_length"));
  }

  return {};
}

Expected<void, Error> DeserializeConfig(std::istream& input_stream, config::Config& config) {
  constexpr uint32_t kMaxTableCount = 10000;  // Reasonable limit for table count
  // MySQL config
  if (!ReadString(input_stream, config.mysql.host)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.host"));
  }
  if (!ReadBinary(input_stream, config.mysql.port)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.port"));
  }
  // Read user/password fields (will be empty in new dumps, ignored from old dumps)
  std::string unused_user;
  std::string unused_password;
  if (!ReadString(input_stream, unused_user)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.user"));
  }
  if (!ReadString(input_stream, unused_password)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.password"));
  }
  // Note: user/password from dump are intentionally ignored for security.
  // Credentials must be provided via config file at startup.
  if (!ReadString(input_stream, config.mysql.database)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.database"));
  }
  if (!ReadBinary(input_stream, config.mysql.use_gtid)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.use_gtid"));
  }
  if (!ReadString(input_stream, config.mysql.binlog_format)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.binlog_format"));
  }
  if (!ReadString(input_stream, config.mysql.binlog_row_image)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.binlog_row_image"));
  }
  if (!ReadBinary(input_stream, config.mysql.connect_timeout_ms)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.connect_timeout_ms"));
  }
  if (!ReadBinary(input_stream, config.mysql.read_timeout_ms)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.read_timeout_ms"));
  }
  if (!ReadBinary(input_stream, config.mysql.write_timeout_ms)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.write_timeout_ms"));
  }

  // Tables
  uint32_t table_count = 0;
  if (!ReadBinary(input_stream, table_count)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read table_count"));
  }
  if (table_count > kMaxTableCount) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError,
                                    "Table count exceeds maximum allowed: " + std::to_string(table_count)));
  }
  config.tables.resize(table_count);
  for (uint32_t i = 0; i < table_count; ++i) {
    if (!DeserializeTableConfig(input_stream, config.tables[i])) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read table config"));
    }
  }

  // Build config
  if (!ReadString(input_stream, config.build.mode)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read build.mode"));
  }
  if (!ReadBinary(input_stream, config.build.batch_size)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read build.batch_size"));
  }
  if (!ReadBinary(input_stream, config.build.parallelism)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read build.parallelism"));
  }
  if (!ReadBinary(input_stream, config.build.throttle_ms)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read build.throttle_ms"));
  }

  // Replication config
  if (!ReadBinary(input_stream, config.replication.enable)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read replication.enable"));
  }
  if (!ReadBinary(input_stream, config.replication.server_id)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read replication.server_id"));
  }
  if (!ReadString(input_stream, config.replication.start_from)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read replication.start_from"));
  }
  if (!ReadBinary(input_stream, config.replication.queue_size)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read replication.queue_size"));
  }
  if (!ReadBinary(input_stream, config.replication.reconnect_backoff_min_ms)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kStorageDumpReadError, "Failed to read replication.reconnect_backoff_min_ms"));
  }
  if (!ReadBinary(input_stream, config.replication.reconnect_backoff_max_ms)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kStorageDumpReadError, "Failed to read replication.reconnect_backoff_max_ms"));
  }

  // Memory config
  if (!ReadBinary(input_stream, config.memory.hard_limit_mb)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read memory.hard_limit_mb"));
  }
  if (!ReadBinary(input_stream, config.memory.soft_target_mb)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read memory.soft_target_mb"));
  }
  if (!ReadBinary(input_stream, config.memory.arena_chunk_mb)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read memory.arena_chunk_mb"));
  }
  if (!ReadBinary(input_stream, config.memory.roaring_threshold)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read memory.roaring_threshold"));
  }
  if (!ReadBinary(input_stream, config.memory.minute_epoch)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read memory.minute_epoch"));
  }
  if (!ReadBinary(input_stream, config.memory.normalize.nfkc)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read memory.normalize.nfkc"));
  }
  if (!ReadString(input_stream, config.memory.normalize.width)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read memory.normalize.width"));
  }
  if (!ReadBinary(input_stream, config.memory.normalize.lower)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read memory.normalize.lower"));
  }

  // Snapshot config
  if (!ReadString(input_stream, config.dump.dir)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read dump.dir"));
  }
  if (!ReadBinary(input_stream, config.dump.interval_sec)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read dump.interval_sec"));
  }
  if (!ReadBinary(input_stream, config.dump.retain)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read dump.retain"));
  }

  // API config
  if (!ReadString(input_stream, config.api.tcp.bind)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read api.tcp.bind"));
  }
  if (!ReadBinary(input_stream, config.api.tcp.port)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read api.tcp.port"));
  }
  if (!ReadBinary(input_stream, config.api.http.enable)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read api.http.enable"));
  }
  if (!ReadString(input_stream, config.api.http.bind)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read api.http.bind"));
  }
  if (!ReadBinary(input_stream, config.api.http.port)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read api.http.port"));
  }
  if (!ReadBinary(input_stream, config.api.default_limit)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read api.default_limit"));
  }

  // Network config
  constexpr uint32_t kMaxCIDRCount = 10000;
  uint32_t cidr_count = 0;
  if (!ReadBinary(input_stream, cidr_count)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read cidr_count"));
  }
  if (cidr_count > kMaxCIDRCount) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError,
                                    "CIDR count exceeds maximum allowed: " + std::to_string(cidr_count)));
  }
  config.network.allow_cidrs.resize(cidr_count);
  for (uint32_t i = 0; i < cidr_count; ++i) {
    if (!ReadString(input_stream, config.network.allow_cidrs[i])) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read network CIDR"));
    }
  }

  // Logging config
  if (!ReadString(input_stream, config.logging.level)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read logging.level"));
  }
  if (!ReadString(input_stream, config.logging.format)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read logging.format"));
  }

  // Query limits (added in newer dumps; optional for backward compatibility)
  if (!ReadBinary(input_stream, config.api.max_query_length)) {
    if (input_stream.eof()) {
      input_stream.clear();
      config.api.max_query_length = config::defaults::kDefaultQueryLengthLimit;
      return {};
    }
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read api.max_query_length"));
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
  // 1. Write to temporary file (.tmp suffix)
  // 2. fsync the temporary file
  // 3. Atomically rename to final path
  // This ensures the original file is never corrupted during write failures.

  std::string temp_filepath = filepath + ".tmp";

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
    if (std::filesystem::exists(filepath, error_code) && std::filesystem::is_symlink(filepath)) {
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
      std::filesystem::remove(temp_filepath);  // Clean up potentially compromised file
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
      std::filesystem::remove(temp_filepath);
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
      std::filesystem::remove(temp_filepath);
      return result;
    }

    // Write config section
    std::ostringstream config_stream;
    if (auto result = SerializeConfig(config_stream, config); !result) {
      LogStorageError("serialize_config", temp_filepath, result.error().message());
      std::filesystem::remove(temp_filepath);
      return result;
    }
    std::string config_data = config_stream.str();
    auto config_len = static_cast<uint32_t>(config_data.size());
    if (!WriteBinary(ofs, config_len)) {
      std::filesystem::remove(temp_filepath);
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
    ofs.write(config_data.data(), static_cast<std::streamsize>(config_len));

    // Write statistics section (if present)
    if (stats != nullptr) {
      std::ostringstream stats_stream;
      if (auto result = SerializeStatistics(stats_stream, *stats); !result) {
        LogStorageError("serialize_statistics", temp_filepath, result.error().message());
        std::filesystem::remove(temp_filepath);
        return result;
      }
      std::string stats_data = stats_stream.str();
      auto stats_len = static_cast<uint32_t>(stats_data.size());
      if (!WriteBinary(ofs, stats_len)) {
        std::filesystem::remove(temp_filepath);
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }
      ofs.write(stats_data.data(), static_cast<std::streamsize>(stats_len));
    } else {
      uint32_t stats_len = 0;
      if (!WriteBinary(ofs, stats_len)) {
        std::filesystem::remove(temp_filepath);
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }
    }

    // Write table data section
    auto table_count = static_cast<uint32_t>(table_contexts.size());
    if (!WriteBinary(ofs, table_count)) {
      std::filesystem::remove(temp_filepath);
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    for (const auto& [table_name, ctx_pair] : table_contexts) {
      index::Index* index = ctx_pair.first;
      DocumentStore* doc_store = ctx_pair.second;

      // Write table name
      if (!WriteString(ofs, table_name)) {
        std::filesystem::remove(temp_filepath);
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
          std::filesystem::remove(temp_filepath);
          return result;
        }
        std::string table_stats_data = table_stats_stream.str();
        auto table_stats_len = static_cast<uint32_t>(table_stats_data.size());
        if (!WriteBinary(ofs, table_stats_len)) {
          std::filesystem::remove(temp_filepath);
          return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
        }
        ofs.write(table_stats_data.data(), static_cast<std::streamsize>(table_stats_len));
      } else {
        uint32_t table_stats_len = 0;
        if (!WriteBinary(ofs, table_stats_len)) {
          std::filesystem::remove(temp_filepath);
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
        std::filesystem::remove(temp_filepath);
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }

      std::string index_data = index_stream.str();
      auto index_len = static_cast<uint64_t>(index_data.size());
      if (!WriteBinary(ofs, index_len)) {
        std::filesystem::remove(temp_filepath);
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
        std::filesystem::remove(temp_filepath);
        return result;
      }

      std::string doc_data = doc_stream.str();
      auto doc_len = static_cast<uint64_t>(doc_data.size());
      if (!WriteBinary(ofs, doc_len)) {
        std::filesystem::remove(temp_filepath);
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }
      ofs.write(doc_data.data(), static_cast<std::streamsize>(doc_len));

      StructuredLog().Event("dump_table_saved").Field("table", table_name).Debug();
    }

    ofs.close();
    if (!ofs.good()) {
      LogStorageError("write_dump", temp_filepath, "Stream error during write");
      std::filesystem::remove(temp_filepath);
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    // Calculate file size
    std::ifstream ifs_size(temp_filepath, std::ios::binary | std::ios::ate);
    if (!ifs_size) {
      LogStorageError("reopen_file", temp_filepath, "Failed to reopen for size calculation");
      std::filesystem::remove(temp_filepath);
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }
    uint64_t file_size = static_cast<uint64_t>(ifs_size.tellg());
    ifs_size.close();

    // Update total_file_size in the header
    {
      std::fstream update_stream1(temp_filepath, std::ios::in | std::ios::out | std::ios::binary);
      if (!update_stream1) {
        LogStorageError("open_file", temp_filepath, "Failed to open for header update");
        std::filesystem::remove(temp_filepath);
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }

      // Seek to total_file_size position (after magic + version + header_size + flags + timestamp)
      const std::streamoff file_size_offset = 4 + 4 + 4 + 4 + 8;
      update_stream1.seekp(file_size_offset);
      if (!WriteBinary(update_stream1, file_size)) {
        LogStorageError("write_header_field", temp_filepath, "Failed to write total_file_size");
        std::filesystem::remove(temp_filepath);
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }
      update_stream1.close();
    }

    // Calculate CRC32 of the file
    std::ifstream ifs(temp_filepath, std::ios::binary);
    if (!ifs) {
      LogStorageError("reopen_file", temp_filepath, "Failed to reopen for CRC calculation");
      std::filesystem::remove(temp_filepath);
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
        std::filesystem::remove(temp_filepath);
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }

      // Seek to file_crc32 position (right after total_file_size)
      const std::streamoff file_size_offset = 4 + 4 + 4 + 4 + 8;
      const std::streamoff crc_file_offset = file_size_offset + 8;
      update_stream2.seekp(crc_file_offset);
      if (!WriteBinary(update_stream2, calculated_crc)) {
        LogStorageError("write_header_field", temp_filepath, "Failed to write file_crc32");
        std::filesystem::remove(temp_filepath);
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }

      update_stream2.close();
      if (!update_stream2.good()) {
        LogStorageError("update_header", temp_filepath, "Stream error during header update");
        std::filesystem::remove(temp_filepath);
        return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
      }
    }

    // Ensure temp file data is flushed to disk BEFORE rename
    // This is critical for atomicity - ensures data is durable before the rename
#ifndef _WIN32
    int fd = open(temp_filepath.c_str(), O_RDONLY);
    if (fd >= 0) {
      if (fsync(fd) != 0) {
        StructuredLog()
            .Event("storage_warning")
            .Field("operation", "fsync_temp_file")
            .Field("filepath", temp_filepath)
            .Field("errno", static_cast<int64_t>(errno))
            .Warn();
      }
      close(fd);
    }
#endif

    // Atomic rename: temp file -> final path
    // On POSIX systems, rename() is atomic within the same filesystem
    std::error_code rename_error;
    std::filesystem::rename(temp_filepath, filepath, rename_error);
    if (rename_error) {
      LogStorageError("atomic_rename", filepath, rename_error.message());
      std::filesystem::remove(temp_filepath);
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpWriteError, "Write operation failed"));
    }

    // Sync the directory to ensure the rename is durable
#ifndef _WIN32
    int dir_fd = open(parent_dir.empty() ? "." : parent_dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (dir_fd >= 0) {
      if (fsync(dir_fd) != 0) {
        StructuredLog()
            .Event("storage_warning")
            .Field("operation", "fsync_directory")
            .Field("filepath", parent_dir.empty() ? "." : parent_dir.string())
            .Field("errno", static_cast<int64_t>(errno))
            .Warn();
      }
      close(dir_fd);
    }
#endif

    StructuredLog()
        .Event("dump_saved_atomically")
        .Field("filepath", filepath)
        .Field("crc32", static_cast<uint64_t>(calculated_crc))
        .Field("file_size", file_size)
        .Info();

    return {};

  } catch (const std::exception& e) {
    // Clean up temp file on any exception
    std::error_code cleanup_error;
    std::filesystem::remove(temp_filepath, cleanup_error);
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
      if (!ReadString(ifs, table_name)) {
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

// ============================================================================
// CRC32 Calculation
// ============================================================================

uint32_t CalculateCRC32(const void* data, size_t length) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  return static_cast<uint32_t>(crc32(0L, reinterpret_cast<const Bytef*>(data), static_cast<uInt>(length)));
}

uint32_t CalculateCRC32(const std::string& str) {
  return CalculateCRC32(str.data(), str.size());
}

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
      size_t zero_end = std::min(kCrcFieldSize - (bytes_read - crc_offset), actually_read);
      std::memset(&buffer[zero_start], 0, zero_end);
    }

    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
    crc = static_cast<uint32_t>(
        crc32(crc, reinterpret_cast<const Bytef*>(buffer.data()), static_cast<uInt>(actually_read)));

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
