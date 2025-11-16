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

namespace {

/**
 * @brief Write binary data to stream
 */
template <typename T>
bool WriteBinary(std::ostream& output_stream, const T& value) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  output_stream.write(reinterpret_cast<const char*>(&value), sizeof(T));
  return output_stream.good();
}

/**
 * @brief Read binary data from stream
 */
template <typename T>
bool ReadBinary(std::istream& input_stream, T& value) {
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
  input_stream.read(reinterpret_cast<char*>(&value), sizeof(T));
  return input_stream.good();
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
    spdlog::error("String length {} exceeds maximum allowed size {}", len, kMaxStringLength);
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

bool WriteHeaderV1(std::ostream& output_stream, const HeaderV1& header) {
  if (!WriteBinary(output_stream, header.header_size)) {
    return false;
  }
  if (!WriteBinary(output_stream, header.flags)) {
    return false;
  }
  if (!WriteBinary(output_stream, header.dump_timestamp)) {
    return false;
  }
  if (!WriteBinary(output_stream, header.total_file_size)) {
    return false;
  }
  if (!WriteBinary(output_stream, header.file_crc32)) {
    return false;
  }
  if (!WriteString(output_stream, header.gtid)) {
    return false;
  }
  return true;
}

bool ReadHeaderV1(std::istream& input_stream, HeaderV1& header) {
  if (!ReadBinary(input_stream, header.header_size)) {
    return false;
  }
  if (!ReadBinary(input_stream, header.flags)) {
    return false;
  }
  if (!ReadBinary(input_stream, header.dump_timestamp)) {
    return false;
  }
  if (!ReadBinary(input_stream, header.total_file_size)) {
    return false;
  }
  if (!ReadBinary(input_stream, header.file_crc32)) {
    return false;
  }
  if (!ReadString(input_stream, header.gtid)) {
    return false;
  }
  return true;
}

// ============================================================================
// Statistics Serialization
// ============================================================================

bool SerializeStatistics(std::ostream& output_stream, const DumpStatistics& stats) {
  if (!WriteBinary(output_stream, stats.total_documents)) {
    return false;
  }
  if (!WriteBinary(output_stream, stats.total_terms)) {
    return false;
  }
  if (!WriteBinary(output_stream, stats.total_index_bytes)) {
    return false;
  }
  if (!WriteBinary(output_stream, stats.total_docstore_bytes)) {
    return false;
  }
  if (!WriteBinary(output_stream, stats.dump_time_ms)) {
    return false;
  }
  return true;
}

bool DeserializeStatistics(std::istream& input_stream, DumpStatistics& stats) {
  if (!ReadBinary(input_stream, stats.total_documents)) {
    return false;
  }
  if (!ReadBinary(input_stream, stats.total_terms)) {
    return false;
  }
  if (!ReadBinary(input_stream, stats.total_index_bytes)) {
    return false;
  }
  if (!ReadBinary(input_stream, stats.total_docstore_bytes)) {
    return false;
  }
  if (!ReadBinary(input_stream, stats.dump_time_ms)) {
    return false;
  }
  return true;
}

bool SerializeTableStatistics(std::ostream& output_stream, const TableStatistics& stats) {
  if (!WriteBinary(output_stream, stats.document_count)) {
    return false;
  }
  if (!WriteBinary(output_stream, stats.term_count)) {
    return false;
  }
  if (!WriteBinary(output_stream, stats.index_bytes)) {
    return false;
  }
  if (!WriteBinary(output_stream, stats.docstore_bytes)) {
    return false;
  }
  if (!WriteBinary(output_stream, stats.next_doc_id)) {
    return false;
  }
  if (!WriteBinary(output_stream, stats.last_update_time)) {
    return false;
  }
  return true;
}

bool DeserializeTableStatistics(std::istream& input_stream, TableStatistics& stats) {
  if (!ReadBinary(input_stream, stats.document_count)) {
    return false;
  }
  if (!ReadBinary(input_stream, stats.term_count)) {
    return false;
  }
  if (!ReadBinary(input_stream, stats.index_bytes)) {
    return false;
  }
  if (!ReadBinary(input_stream, stats.docstore_bytes)) {
    return false;
  }
  if (!ReadBinary(input_stream, stats.next_doc_id)) {
    return false;
  }
  if (!ReadBinary(input_stream, stats.last_update_time)) {
    return false;
  }
  return true;
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
    spdlog::error("Concat column count {} exceeds maximum allowed {}", concat_size, kMaxConcatColumns);
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
    spdlog::error("Required filter count {} exceeds maximum allowed {}", req_filter_count, kMaxFilterCount);
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
    spdlog::error("Filter count {} exceeds maximum allowed {}", filter_count, kMaxFilterCount);
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

bool SerializeConfig(std::ostream& output_stream, const config::Config& config) {
  // MySQL config (excluding sensitive credentials)
  if (!WriteString(output_stream, config.mysql.host)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.mysql.port)) {
    return false;
  }
  // Write empty strings for user and password (security: do not persist credentials)
  std::string empty_user;
  std::string empty_password;
  if (!WriteString(output_stream, empty_user)) {
    return false;
  }
  if (!WriteString(output_stream, empty_password)) {
    return false;
  }
  if (!WriteString(output_stream, config.mysql.database)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.mysql.use_gtid)) {
    return false;
  }
  if (!WriteString(output_stream, config.mysql.binlog_format)) {
    return false;
  }
  if (!WriteString(output_stream, config.mysql.binlog_row_image)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.mysql.connect_timeout_ms)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.mysql.read_timeout_ms)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.mysql.write_timeout_ms)) {
    return false;
  }

  // Tables
  auto table_count = static_cast<uint32_t>(config.tables.size());
  if (!WriteBinary(output_stream, table_count)) {
    return false;
  }
  for (const auto& table : config.tables) {
    if (!SerializeTableConfig(output_stream, table)) {
      return false;
    }
  }

  // Build config
  if (!WriteString(output_stream, config.build.mode)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.build.batch_size)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.build.parallelism)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.build.throttle_ms)) {
    return false;
  }

  // Replication config
  if (!WriteBinary(output_stream, config.replication.enable)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.replication.server_id)) {
    return false;
  }
  if (!WriteString(output_stream, config.replication.start_from)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.replication.queue_size)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.replication.reconnect_backoff_min_ms)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.replication.reconnect_backoff_max_ms)) {
    return false;
  }

  // Memory config
  if (!WriteBinary(output_stream, config.memory.hard_limit_mb)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.memory.soft_target_mb)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.memory.arena_chunk_mb)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.memory.roaring_threshold)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.memory.minute_epoch)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.memory.normalize.nfkc)) {
    return false;
  }
  if (!WriteString(output_stream, config.memory.normalize.width)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.memory.normalize.lower)) {
    return false;
  }

  // Snapshot config
  if (!WriteString(output_stream, config.dump.dir)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.dump.interval_sec)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.dump.retain)) {
    return false;
  }

  // API config
  if (!WriteString(output_stream, config.api.tcp.bind)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.api.tcp.port)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.api.http.enable)) {
    return false;
  }
  if (!WriteString(output_stream, config.api.http.bind)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.api.http.port)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.api.default_limit)) {
    return false;
  }

  // Network config
  auto cidr_count = static_cast<uint32_t>(config.network.allow_cidrs.size());
  if (!WriteBinary(output_stream, cidr_count)) {
    return false;
  }
  for (const auto& cidr : config.network.allow_cidrs) {
    if (!WriteString(output_stream, cidr)) {
      return false;
    }
  }

  // Logging config
  if (!WriteString(output_stream, config.logging.level)) {
    return false;
  }
  if (!WriteBinary(output_stream, config.logging.json)) {
    return false;
  }

  // Query limits
  if (!WriteBinary(output_stream, config.api.max_query_length)) {
    return false;
  }

  return true;
}

bool DeserializeConfig(std::istream& input_stream, config::Config& config) {
  constexpr uint32_t kMaxTableCount = 10000;  // Reasonable limit for table count
  // MySQL config
  if (!ReadString(input_stream, config.mysql.host)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.mysql.port)) {
    return false;
  }
  // Read user/password fields (will be empty in new dumps, ignored from old dumps)
  std::string unused_user;
  std::string unused_password;
  if (!ReadString(input_stream, unused_user)) {
    return false;
  }
  if (!ReadString(input_stream, unused_password)) {
    return false;
  }
  // Note: user/password from dump are intentionally ignored for security.
  // Credentials must be provided via config file at startup.
  if (!ReadString(input_stream, config.mysql.database)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.mysql.use_gtid)) {
    return false;
  }
  if (!ReadString(input_stream, config.mysql.binlog_format)) {
    return false;
  }
  if (!ReadString(input_stream, config.mysql.binlog_row_image)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.mysql.connect_timeout_ms)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.mysql.read_timeout_ms)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.mysql.write_timeout_ms)) {
    return false;
  }

  // Tables
  uint32_t table_count = 0;
  if (!ReadBinary(input_stream, table_count)) {
    return false;
  }
  if (table_count > kMaxTableCount) {
    spdlog::error("Table count {} exceeds maximum allowed {}", table_count, kMaxTableCount);
    return false;
  }
  config.tables.resize(table_count);
  for (uint32_t i = 0; i < table_count; ++i) {
    if (!DeserializeTableConfig(input_stream, config.tables[i])) {
      return false;
    }
  }

  // Build config
  if (!ReadString(input_stream, config.build.mode)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.build.batch_size)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.build.parallelism)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.build.throttle_ms)) {
    return false;
  }

  // Replication config
  if (!ReadBinary(input_stream, config.replication.enable)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.replication.server_id)) {
    return false;
  }
  if (!ReadString(input_stream, config.replication.start_from)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.replication.queue_size)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.replication.reconnect_backoff_min_ms)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.replication.reconnect_backoff_max_ms)) {
    return false;
  }

  // Memory config
  if (!ReadBinary(input_stream, config.memory.hard_limit_mb)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.memory.soft_target_mb)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.memory.arena_chunk_mb)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.memory.roaring_threshold)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.memory.minute_epoch)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.memory.normalize.nfkc)) {
    return false;
  }
  if (!ReadString(input_stream, config.memory.normalize.width)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.memory.normalize.lower)) {
    return false;
  }

  // Snapshot config
  if (!ReadString(input_stream, config.dump.dir)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.dump.interval_sec)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.dump.retain)) {
    return false;
  }

  // API config
  if (!ReadString(input_stream, config.api.tcp.bind)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.api.tcp.port)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.api.http.enable)) {
    return false;
  }
  if (!ReadString(input_stream, config.api.http.bind)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.api.http.port)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.api.default_limit)) {
    return false;
  }

  // Network config
  constexpr uint32_t kMaxCIDRCount = 10000;
  uint32_t cidr_count = 0;
  if (!ReadBinary(input_stream, cidr_count)) {
    return false;
  }
  if (cidr_count > kMaxCIDRCount) {
    spdlog::error("CIDR count {} exceeds maximum allowed {}", cidr_count, kMaxCIDRCount);
    return false;
  }
  config.network.allow_cidrs.resize(cidr_count);
  for (uint32_t i = 0; i < cidr_count; ++i) {
    if (!ReadString(input_stream, config.network.allow_cidrs[i])) {
      return false;
    }
  }

  // Logging config
  if (!ReadString(input_stream, config.logging.level)) {
    return false;
  }
  if (!ReadBinary(input_stream, config.logging.json)) {
    return false;
  }

  // Query limits (added in newer dumps; optional for backward compatibility)
  if (!ReadBinary(input_stream, config.api.max_query_length)) {
    if (input_stream.eof()) {
      input_stream.clear();
      config.api.max_query_length = config::defaults::kDefaultQueryLengthLimit;
      return true;
    }
    return false;
  }

  return true;
}

// ============================================================================
// Complete Snapshot Read/Write (Version 1)
// ============================================================================

bool WriteDumpV1(const std::string& filepath, const std::string& gtid, const config::Config& config,
                 const std::unordered_map<std::string, std::pair<index::Index*, DocumentStore*>>& table_contexts,
                 const DumpStatistics* stats, const std::unordered_map<std::string, TableStatistics>* table_stats) {
  try {
    // Ensure parent directory exists
    std::filesystem::path file_path(filepath);
    std::filesystem::path parent_dir = file_path.parent_path();

    if (!parent_dir.empty() && !std::filesystem::exists(parent_dir)) {
      std::error_code error_code;
      if (!std::filesystem::create_directories(parent_dir, error_code)) {
        spdlog::error("Failed to create dump directory: {} ({})", parent_dir.string(), error_code.message());
        return false;
      }
      spdlog::info("Created dump directory: {}", parent_dir.string());
    }

#ifndef _WIN32
    // SECURITY: Validate dump directory exists and is not itself a symlink
    // Note: We allow symlinks in parent paths (like /var -> /private/var on macOS)
    // but not in the final directory component
    if (!parent_dir.empty() && std::filesystem::exists(parent_dir)) {
      if (std::filesystem::is_symlink(parent_dir)) {
        spdlog::error("Dump directory is a symlink - not allowed: {}", parent_dir.string());
        return false;
      }
    }

    // SECURITY: Check if file path is a symlink before opening
    std::error_code error_code;
    if (std::filesystem::exists(filepath, error_code) && std::filesystem::is_symlink(filepath)) {
      spdlog::error("Dump file path is a symlink - not allowed: {}", filepath);
      return false;
    }

    // SECURITY: Open file with O_NOFOLLOW to prevent symlink attacks (TOCTOU protection)
    // O_CREAT | O_EXCL: Fail if file already exists (atomic creation)
    // O_NOFOLLOW: Fail if the file is a symbolic link
    // S_IRUSR | S_IWUSR: Set permissions to 600 (rw-------)
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): POSIX open() requires varargs for mode
    int file_descriptor = open(filepath.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, S_IRUSR | S_IWUSR);
    if (file_descriptor < 0) {
      if (errno == EEXIST) {
        // File already exists - check if it's a symlink before removing
        if (std::filesystem::is_symlink(filepath)) {
          spdlog::error("Existing dump file is a symlink - not allowed: {}", filepath);
          return false;
        }
        // File exists and is not a symlink - try to remove and recreate
        spdlog::warn("Dump file already exists, removing: {}", filepath);
        if (std::filesystem::remove(filepath)) {
          // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg): POSIX open() requires varargs for mode
          file_descriptor = open(filepath.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_NOFOLLOW, S_IRUSR | S_IWUSR);
        }
      }
      if (file_descriptor < 0) {
        spdlog::error("Failed to create dump file securely: {} ({})", filepath, std::strerror(errno));
        return false;
      }
    }

    // Verify ownership (file must be owned by current process user)
    // NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init): stat struct is filled by fstat()
    struct stat file_stat {};
    if (fstat(file_descriptor, &file_stat) != 0 || file_stat.st_uid != geteuid()) {
      close(file_descriptor);
      spdlog::error("Dump file ownership verification failed: {}", filepath);
      std::filesystem::remove(filepath);  // Clean up potentially compromised file
      return false;
    }

    // Create ofstream from file descriptor
    // Note: We need to close file_descriptor manually or use __gnu_cxx::stdio_filebuf on Linux
    // For simplicity, we'll close file_descriptor and reopen with ofstream since ownership is verified
    close(file_descriptor);

    std::ofstream ofs(filepath, std::ios::binary | std::ios::trunc | std::ios::out);
    if (!ofs) {
      spdlog::error("Failed to open dump file for writing: {}", filepath);
      return false;
    }
#else
    // Windows: Use standard file opening (symlink attacks less common on Windows)
    std::ofstream ofs(filepath, std::ios::binary | std::ios::trunc);
    if (!ofs) {
      spdlog::error("Failed to open dump file for writing: {}", filepath);
      return false;
    }
#endif

    // Write fixed file header
    ofs.write(dump_format::kMagicNumber.data(), 4);
    auto version = static_cast<uint32_t>(dump_format::FormatVersion::V1);
    if (!WriteBinary(ofs, version)) {
      return false;
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
    if (!WriteHeaderV1(ofs, header)) {
      spdlog::error("Failed to write V1 header");
      return false;
    }

    // Write config section
    std::ostringstream config_stream;
    if (!SerializeConfig(config_stream, config)) {
      spdlog::error("Failed to serialize config");
      return false;
    }
    std::string config_data = config_stream.str();
    auto config_len = static_cast<uint32_t>(config_data.size());
    if (!WriteBinary(ofs, config_len)) {
      return false;
    }
    ofs.write(config_data.data(), static_cast<std::streamsize>(config_len));

    // Write statistics section (if present)
    if (stats != nullptr) {
      std::ostringstream stats_stream;
      if (!SerializeStatistics(stats_stream, *stats)) {
        spdlog::error("Failed to serialize statistics");
        return false;
      }
      std::string stats_data = stats_stream.str();
      auto stats_len = static_cast<uint32_t>(stats_data.size());
      if (!WriteBinary(ofs, stats_len)) {
        return false;
      }
      ofs.write(stats_data.data(), static_cast<std::streamsize>(stats_len));
    } else {
      uint32_t stats_len = 0;
      if (!WriteBinary(ofs, stats_len)) {
        return false;
      }
    }

    // Write table data section
    auto table_count = static_cast<uint32_t>(table_contexts.size());
    if (!WriteBinary(ofs, table_count)) {
      return false;
    }

    for (const auto& [table_name, ctx_pair] : table_contexts) {
      index::Index* index = ctx_pair.first;
      DocumentStore* doc_store = ctx_pair.second;

      // Write table name
      if (!WriteString(ofs, table_name)) {
        return false;
      }

      // Write table statistics (if present)
      if (table_stats != nullptr && table_stats->count(table_name) > 0) {
        std::ostringstream table_stats_stream;
        if (!SerializeTableStatistics(table_stats_stream, table_stats->at(table_name))) {
          spdlog::error("Failed to serialize table statistics for: {}", table_name);
          return false;
        }
        std::string table_stats_data = table_stats_stream.str();
        auto table_stats_len = static_cast<uint32_t>(table_stats_data.size());
        if (!WriteBinary(ofs, table_stats_len)) {
          return false;
        }
        ofs.write(table_stats_data.data(), static_cast<std::streamsize>(table_stats_len));
      } else {
        uint32_t table_stats_len = 0;
        if (!WriteBinary(ofs, table_stats_len)) {
          return false;
        }
      }

      // Save index directly to stringstream
      std::ostringstream index_stream;
      if (!index->SaveToStream(index_stream)) {
        spdlog::error("Failed to save index for table: {}", table_name);
        return false;
      }

      std::string index_data = index_stream.str();
      auto index_len = static_cast<uint64_t>(index_data.size());
      if (!WriteBinary(ofs, index_len)) {
        return false;
      }
      ofs.write(index_data.data(), static_cast<std::streamsize>(index_len));

      // Save document store directly to stringstream
      std::ostringstream doc_stream;
      if (!doc_store->SaveToStream(doc_stream, "")) {
        spdlog::error("Failed to save documents for table: {}", table_name);
        return false;
      }

      std::string doc_data = doc_stream.str();
      auto doc_len = static_cast<uint64_t>(doc_data.size());
      if (!WriteBinary(ofs, doc_len)) {
        return false;
      }
      ofs.write(doc_data.data(), static_cast<std::streamsize>(doc_len));

      spdlog::info("Saved table to dump: {}", table_name);
    }

    ofs.close();
    if (!ofs.good()) {
      spdlog::error("Error occurred while writing dump file");
      return false;
    }

    // Calculate file size
    std::ifstream ifs_size(filepath, std::ios::binary | std::ios::ate);
    if (!ifs_size) {
      spdlog::error("Failed to reopen dump file for size calculation");
      return false;
    }
    uint64_t file_size = static_cast<uint64_t>(ifs_size.tellg());
    ifs_size.close();

    // First, update total_file_size in the header (but keep CRC as 0)
    {
      std::fstream update_stream1(filepath, std::ios::in | std::ios::out | std::ios::binary);
      if (!update_stream1) {
        spdlog::error("Failed to open snapshot file for header update");
        return false;
      }

      // Seek to total_file_size position (after magic + version + header_size + flags + timestamp)
      const std::streamoff file_size_offset = 4 + 4 + 4 + 4 + 8;
      update_stream1.seekp(file_size_offset);
      if (!WriteBinary(update_stream1, file_size)) {
        spdlog::error("Failed to write total_file_size to header");
        return false;
      }
      update_stream1.close();
    }

    // Now read the file WITH total_file_size set to calculate CRC
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs) {
      spdlog::error("Failed to reopen dump file for CRC calculation");
      return false;
    }

    std::vector<char> file_data(file_size);
    ifs.read(file_data.data(), static_cast<std::streamsize>(file_size));
    ifs.close();

    // Calculate CRC32 of entire file (excluding the CRC field itself by zeroing it)
    // CRC field is at offset: magic(4) + version(4) + header_size(4) + flags(4) + timestamp(8) + total_file_size(8) =
    // 32 bytes
    const size_t crc_offset = 4 + 4 + 4 + 4 + 8 + 8;  // Position of file_crc32 in file
    // Zero out the CRC field before calculation (CRC excludes itself)
    if (file_size > crc_offset + 4) {
      std::memset(&file_data[crc_offset], 0, 4);
    }

    uint32_t calculated_crc = CalculateCRC32(file_data.data(), file_size);

    // Update header with CRC
    {
      std::fstream update_stream2(filepath, std::ios::in | std::ios::out | std::ios::binary);
      if (!update_stream2) {
        spdlog::error("Failed to open snapshot file for CRC update");
        return false;
      }

      // Seek to file_crc32 position (right after total_file_size)
      const std::streamoff file_size_offset = 4 + 4 + 4 + 4 + 8;
      const std::streamoff crc_file_offset = file_size_offset + 8;
      update_stream2.seekp(crc_file_offset);
      if (!WriteBinary(update_stream2, calculated_crc)) {
        spdlog::error("Failed to write file_crc32 to header");
        return false;
      }

      update_stream2.close();
      if (!update_stream2.good()) {
        spdlog::error("Error occurred while updating dump file header");
        return false;
      }
    }

    spdlog::debug("Snapshot CRC32: 0x{:08x}, Size: {} bytes", calculated_crc, file_size);

    return true;

  } catch (const std::exception& e) {
    spdlog::error("Exception while writing dump: {}", e.what());
    return false;
  }
}

bool ReadDumpV1(const std::string& filepath, std::string& gtid, config::Config& config,
                std::unordered_map<std::string, std::pair<index::Index*, DocumentStore*>>& table_contexts,
                DumpStatistics* stats, std::unordered_map<std::string, TableStatistics>* table_stats,
                dump_format::IntegrityError* integrity_error) {
  try {
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs) {
      spdlog::error("Failed to open dump file for reading: {}", filepath);
      return false;
    }

    // Read and verify fixed file header
    std::array<char, 4> magic{};
    ifs.read(magic.data(), 4);
    if (std::memcmp(magic.data(), dump_format::kMagicNumber.data(), 4) != 0) {
      spdlog::error("Invalid dump file: magic number mismatch");
      return false;
    }

    uint32_t version = 0;
    if (!ReadBinary(ifs, version)) {
      return false;
    }

    // Version compatibility check
    if (version > dump_format::kMaxSupportedVersion) {
      spdlog::error("Snapshot file version {} is newer than supported version {}. Please upgrade MygramDB.", version,
                    dump_format::kMaxSupportedVersion);
      return false;
    }
    if (version < dump_format::kMinSupportedVersion) {
      spdlog::error("Snapshot file version {} is older than minimum supported version {}.", version,
                    dump_format::kMinSupportedVersion);
      return false;
    }

    // Currently only V1 is implemented
    if (version != static_cast<uint32_t>(dump_format::FormatVersion::V1)) {
      spdlog::error("Snapshot format version {} is not yet implemented.", version);
      return false;
    }

    // Read V1 header
    HeaderV1 header;
    if (!ReadHeaderV1(ifs, header)) {
      spdlog::error("Failed to read V1 header");
      return false;
    }
    gtid = header.gtid;

    // Verify file size if specified
    if (header.total_file_size > 0) {
      std::streampos saved_pos = ifs.tellg();
      ifs.seekg(0, std::ios::end);
      auto actual_size = static_cast<uint64_t>(ifs.tellg());
      ifs.seekg(saved_pos);  // Restore position

      if (actual_size != header.total_file_size) {
        spdlog::error("File size mismatch: expected {} bytes, got {} bytes (file may be truncated or corrupted)",
                      header.total_file_size, actual_size);
        if (integrity_error != nullptr) {
          integrity_error->type = dump_format::CRCErrorType::FileCRC;
          integrity_error->message = "File size mismatch";
        }
        return false;
      }
    }

    // Verify CRC32 if specified
    if (header.file_crc32 != 0) {
      // Save current position
      std::streampos current_pos = ifs.tellg();

      // Read entire file for CRC verification
      ifs.seekg(0, std::ios::end);
      auto file_size = static_cast<uint64_t>(ifs.tellg());
      ifs.seekg(0, std::ios::beg);

      std::vector<char> file_data(file_size);
      ifs.read(file_data.data(), static_cast<std::streamsize>(file_size));

      // Zero out the CRC field before calculation (CRC excludes itself)
      const size_t crc_offset = 4 + 4 + 4 + 4 + 8 + 8;  // magic + version + header_size + flags + timestamp +
                                                        // total_file_size
      if (file_size > crc_offset + 4) {
        std::memset(&file_data[crc_offset], 0, 4);
      }

      uint32_t calculated_crc = CalculateCRC32(file_data.data(), file_size);

      if (calculated_crc != header.file_crc32) {
        spdlog::error("CRC32 mismatch: expected 0x{:08x}, got 0x{:08x} (file may be corrupted)", header.file_crc32,
                      calculated_crc);
        if (integrity_error != nullptr) {
          integrity_error->type = dump_format::CRCErrorType::FileCRC;
          integrity_error->message = "CRC32 checksum mismatch";
        }
        return false;
      }

      spdlog::debug("Snapshot CRC32 verified: 0x{:08x}", calculated_crc);

      // Restore file position
      ifs.seekg(current_pos);
    }

    // Read config section
    uint32_t config_len = 0;
    if (!ReadBinary(ifs, config_len)) {
      return false;
    }
    std::string config_data(config_len, '\0');
    ifs.read(config_data.data(), static_cast<std::streamsize>(config_len));
    std::istringstream config_stream(config_data);
    if (!DeserializeConfig(config_stream, config)) {
      spdlog::error("Failed to deserialize config");
      return false;
    }

    // Read statistics section
    uint32_t stats_len = 0;
    if (!ReadBinary(ifs, stats_len)) {
      return false;
    }
    if (stats_len > 0 && stats != nullptr) {
      std::string stats_data(stats_len, '\0');
      ifs.read(stats_data.data(), static_cast<std::streamsize>(stats_len));
      std::istringstream stats_stream(stats_data);
      if (!DeserializeStatistics(stats_stream, *stats)) {
        spdlog::error("Failed to deserialize statistics");
        return false;
      }
    } else if (stats_len > 0) {
      // Skip statistics if not requested
      ifs.seekg(stats_len, std::ios::cur);
    }

    // Read table data section
    uint32_t table_count = 0;
    if (!ReadBinary(ifs, table_count)) {
      return false;
    }

    for (uint32_t i = 0; i < table_count; ++i) {
      std::string table_name;
      if (!ReadString(ifs, table_name)) {
        return false;
      }

      // Read table statistics
      uint32_t table_stats_len = 0;
      if (!ReadBinary(ifs, table_stats_len)) {
        return false;
      }
      if (table_stats_len > 0 && table_stats != nullptr) {
        std::string table_stats_data(table_stats_len, '\0');
        ifs.read(table_stats_data.data(), static_cast<std::streamsize>(table_stats_len));
        std::istringstream table_stats_stream(table_stats_data);
        TableStatistics table_stat;
        if (!DeserializeTableStatistics(table_stats_stream, table_stat)) {
          spdlog::error("Failed to deserialize table statistics for: {}", table_name);
          return false;
        }
        (*table_stats)[table_name] = table_stat;
      } else if (table_stats_len > 0) {
        // Skip table statistics if not requested
        ifs.seekg(table_stats_len, std::ios::cur);
      }

      // Check if table context exists
      if (table_contexts.count(table_name) == 0) {
        spdlog::warn("Table not found in context, skipping: {}", table_name);
        // Skip index and document data
        uint64_t index_len = 0;
        if (!ReadBinary(ifs, index_len)) {
          return false;
        }
        ifs.seekg(static_cast<std::streamoff>(index_len), std::ios::cur);

        uint64_t doc_len = 0;
        if (!ReadBinary(ifs, doc_len)) {
          return false;
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
        return false;
      }

      if (index_len > 0) {
        // Standard mode: load index from dump
        std::string index_data(index_len, '\0');
        ifs.read(index_data.data(), static_cast<std::streamsize>(index_len));

        // Load index directly from stringstream
        std::istringstream index_stream(index_data);
        if (!index->LoadFromStream(index_stream)) {
          spdlog::error("Failed to load index for table: {}", table_name);
          return false;
        }
      } else {
        spdlog::error("Invalid dump: index_len=0 (corrupted or unsupported format)");
        return false;
      }

      // Read document store data
      uint64_t doc_len = 0;
      if (!ReadBinary(ifs, doc_len)) {
        return false;
      }
      std::string doc_data(doc_len, '\0');
      ifs.read(doc_data.data(), static_cast<std::streamsize>(doc_len));

      // Load document store directly from stringstream
      std::istringstream doc_stream(doc_data);
      if (!doc_store->LoadFromStream(doc_stream, nullptr)) {
        spdlog::error("Failed to load documents for table: {}", table_name);
        return false;
      }

      spdlog::info("Loaded table from dump: {}", table_name);
    }

    return true;

  } catch (const std::exception& e) {
    spdlog::error("Exception while reading dump: {}", e.what());
    return false;
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

// ============================================================================
// Snapshot Integrity Verification
// ============================================================================

bool VerifyDumpIntegrity(const std::string& filepath, dump_format::IntegrityError& integrity_error) {
  try {
    std::ifstream ifs(filepath, std::ios::binary);
    if (!ifs) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "Failed to open file: " + filepath;
      return false;
    }

    // Read and verify fixed file header
    std::array<char, 4> magic{};
    ifs.read(magic.data(), 4);
    if (!ifs.good() || std::memcmp(magic.data(), dump_format::kMagicNumber.data(), 4) != 0) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "Invalid magic number";
      return false;
    }

    uint32_t version = 0;
    if (!ReadBinary(ifs, version)) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "Failed to read version";
      return false;
    }

    // Version compatibility check
    if (version > dump_format::kMaxSupportedVersion) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "Version " + std::to_string(version) + " is newer than supported version " +
                                std::to_string(dump_format::kMaxSupportedVersion);
      return false;
    }
    if (version < dump_format::kMinSupportedVersion) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "Version " + std::to_string(version) + " is older than minimum supported version " +
                                std::to_string(dump_format::kMinSupportedVersion);
      return false;
    }

    // Read V1 header
    HeaderV1 header;
    if (!ReadHeaderV1(ifs, header)) {
      integrity_error.type = dump_format::CRCErrorType::FileCRC;
      integrity_error.message = "Failed to read V1 header";
      return false;
    }

    // Verify file size if specified
    if (header.total_file_size > 0) {
      ifs.seekg(0, std::ios::end);
      auto actual_size = static_cast<uint64_t>(ifs.tellg());

      if (actual_size != header.total_file_size) {
        integrity_error.type = dump_format::CRCErrorType::FileCRC;
        integrity_error.message = "File size mismatch: expected " + std::to_string(header.total_file_size) +
                                  " bytes, got " + std::to_string(actual_size) + " bytes (file may be truncated)";
        return false;
      }
    }

    // Verify CRC32 if specified
    if (header.file_crc32 != 0) {
      // Read entire file for CRC verification
      ifs.seekg(0, std::ios::end);
      auto file_size = static_cast<uint64_t>(ifs.tellg());
      ifs.seekg(0, std::ios::beg);

      std::vector<char> file_data(file_size);
      ifs.read(file_data.data(), static_cast<std::streamsize>(file_size));

      // Zero out the CRC field before calculation (CRC excludes itself)
      const size_t crc_offset = 4 + 4 + 4 + 4 + 8 + 8;  // magic + version + header_size + flags + timestamp +
                                                        // total_file_size
      if (file_size > crc_offset + 4) {
        std::memset(&file_data[crc_offset], 0, 4);
      }

      uint32_t calculated_crc = CalculateCRC32(file_data.data(), file_size);

      if (calculated_crc != header.file_crc32) {
        integrity_error.type = dump_format::CRCErrorType::FileCRC;
        integrity_error.message = "CRC32 checksum mismatch";
        spdlog::error("CRC32 mismatch: expected 0x{:08x}, got 0x{:08x}", header.file_crc32, calculated_crc);
        return false;
      }

      spdlog::info("Snapshot file verification passed (CRC verified): {}", filepath);
    } else {
      spdlog::info("Snapshot file verification passed (basic check, no CRC): {}", filepath);
    }

    integrity_error.type = dump_format::CRCErrorType::None;
    integrity_error.message = "";
    return true;

  } catch (const std::exception& e) {
    integrity_error.type = dump_format::CRCErrorType::FileCRC;
    integrity_error.message = std::string("Exception during verification: ") + e.what();
    return false;
  }
}

// ============================================================================
// Snapshot File Information
// ============================================================================

bool GetDumpInfo(const std::string& filepath, DumpInfo& info) {
  try {
    std::ifstream ifs(filepath, std::ios::binary | std::ios::ate);
    if (!ifs) {
      spdlog::error("Failed to open snapshot file: {}", filepath);
      return false;
    }

    // Get file size
    info.file_size = static_cast<uint64_t>(ifs.tellg());
    ifs.seekg(0, std::ios::beg);

    // Read and verify magic number
    std::array<char, 4> magic{};
    ifs.read(magic.data(), 4);
    if (!ifs.good() || std::memcmp(magic.data(), dump_format::kMagicNumber.data(), 4) != 0) {
      spdlog::error("Invalid dump file: magic number mismatch");
      return false;
    }

    // Read version
    if (!ReadBinary(ifs, info.version)) {
      return false;
    }

    // Version compatibility check
    if (info.version > dump_format::kMaxSupportedVersion) {
      spdlog::error("Snapshot file version {} is newer than supported version {}", info.version,
                    dump_format::kMaxSupportedVersion);
      return false;
    }
    if (info.version < dump_format::kMinSupportedVersion) {
      spdlog::error("Snapshot file version {} is older than minimum supported version {}", info.version,
                    dump_format::kMinSupportedVersion);
      return false;
    }

    // Read V1 header
    HeaderV1 header;
    if (!ReadHeaderV1(ifs, header)) {
      spdlog::error("Failed to read V1 header");
      return false;
    }

    info.gtid = header.gtid;
    info.flags = header.flags;
    info.timestamp = header.dump_timestamp;
    info.has_statistics = (header.flags & dump_format::flags_v1::kWithStatistics) != 0;

    // Read config section to get table count
    uint32_t config_len = 0;
    if (!ReadBinary(ifs, config_len)) {
      return false;
    }
    if (config_len > 0) {
      ifs.seekg(config_len, std::ios::cur);  // Skip config data
    }

    // Skip statistics section if present
    uint32_t stats_len = 0;
    if (!ReadBinary(ifs, stats_len)) {
      return false;
    }
    if (stats_len > 0) {
      ifs.seekg(stats_len, std::ios::cur);
    }

    // Read table count
    if (!ReadBinary(ifs, info.table_count)) {
      return false;
    }

    return true;

  } catch (const std::exception& e) {
    spdlog::error("Exception while reading dump info: {}", e.what());
    return false;
  }
}

}  // namespace mygramdb::storage::dump_v1
