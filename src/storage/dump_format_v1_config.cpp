/**
 * @file dump_format_v1_config.cpp
 * @brief Config serialization/deserialization for dump format V1
 */

#include <string>

#include "storage/dump_format_v1.h"
#include "storage/dump_format_v1_internal.h"
#include "utils/binary_io.h"
#include "utils/structured_log.h"

namespace mygramdb::storage::dump_v1 {

using namespace mygram::utils;
using internal::ReadString;
using internal::WriteString;

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
  if (!ReadString(input_stream, filter.name, kMaxIdentifierLength)) {
    return false;
  }
  if (!ReadString(input_stream, filter.type, kMaxIdentifierLength)) {
    return false;
  }
  if (!ReadBinary(input_stream, filter.dict_compress)) {
    return false;
  }
  if (!ReadBinary(input_stream, filter.bitmap_index)) {
    return false;
  }
  if (!ReadString(input_stream, filter.bucket, kMaxIdentifierLength)) {
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
  if (!ReadString(input_stream, filter.name, kMaxIdentifierLength)) {
    return false;
  }
  if (!ReadString(input_stream, filter.type, kMaxIdentifierLength)) {
    return false;
  }
  if (!ReadString(input_stream, filter.op, kMaxIdentifierLength)) {
    return false;
  }
  if (!ReadString(input_stream, filter.value, kMaxConfigValueLength)) {
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

  // cross_boundary_ngrams
  uint8_t cross_boundary = table.cross_boundary_ngrams ? 1 : 0;
  if (!WriteBinary(output_stream, cross_boundary)) {
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
  if (!ReadString(input_stream, table.name, kMaxIdentifierLength)) {
    return false;
  }
  if (!ReadString(input_stream, table.primary_key, kMaxIdentifierLength)) {
    return false;
  }

  // text_source
  constexpr uint32_t kMaxConcatColumns = 1000;
  constexpr uint32_t kMaxFilterCount = 1000;
  if (!ReadString(input_stream, table.text_source.column, kMaxIdentifierLength)) {
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
    if (!ReadString(input_stream, table.text_source.concat[i], kMaxIdentifierLength)) {
      return false;
    }
  }
  if (!ReadString(input_stream, table.text_source.delimiter, kMaxIdentifierLength)) {
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

  // cross_boundary_ngrams
  uint8_t cross_boundary = 1;
  if (!ReadBinary(input_stream, cross_boundary)) {
    return false;
  }
  table.cross_boundary_ngrams = (cross_boundary != 0);

  // posting config
  if (!ReadBinary(input_stream, table.posting.block_size)) {
    return false;
  }
  if (!ReadBinary(input_stream, table.posting.freq_bits)) {
    return false;
  }
  if (!ReadString(input_stream, table.posting.use_roaring, kMaxIdentifierLength)) {
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
  if (!ReadString(input_stream, config.mysql.host, kMaxConfigValueLength)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.host"));
  }
  if (!ReadBinary(input_stream, config.mysql.port)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.port"));
  }
  // Read user/password fields (will be empty in new dumps, ignored from old dumps)
  std::string unused_user;
  std::string unused_password;
  if (!ReadString(input_stream, unused_user, kMaxConfigValueLength)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.user"));
  }
  if (!ReadString(input_stream, unused_password, kMaxConfigValueLength)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.password"));
  }
  // Note: user/password from dump are intentionally ignored for security.
  // Credentials must be provided via config file at startup.
  if (!ReadString(input_stream, config.mysql.database, kMaxIdentifierLength)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.database"));
  }
  if (!ReadBinary(input_stream, config.mysql.use_gtid)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.use_gtid"));
  }
  if (!ReadString(input_stream, config.mysql.binlog_format, kMaxIdentifierLength)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read mysql.binlog_format"));
  }
  if (!ReadString(input_stream, config.mysql.binlog_row_image, kMaxIdentifierLength)) {
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
  if (!ReadString(input_stream, config.build.mode, kMaxIdentifierLength)) {
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
  if (!ReadString(input_stream, config.replication.start_from, kMaxPathLength)) {
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
  if (!ReadString(input_stream, config.memory.normalize.width, kMaxIdentifierLength)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read memory.normalize.width"));
  }
  if (!ReadBinary(input_stream, config.memory.normalize.lower)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read memory.normalize.lower"));
  }

  // Snapshot config
  if (!ReadString(input_stream, config.dump.dir, kMaxPathLength)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read dump.dir"));
  }
  if (!ReadBinary(input_stream, config.dump.interval_sec)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read dump.interval_sec"));
  }
  if (!ReadBinary(input_stream, config.dump.retain)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read dump.retain"));
  }

  // API config
  if (!ReadString(input_stream, config.api.tcp.bind, kMaxConfigValueLength)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read api.tcp.bind"));
  }
  if (!ReadBinary(input_stream, config.api.tcp.port)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read api.tcp.port"));
  }
  if (!ReadBinary(input_stream, config.api.http.enable)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read api.http.enable"));
  }
  if (!ReadString(input_stream, config.api.http.bind, kMaxConfigValueLength)) {
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
    if (!ReadString(input_stream, config.network.allow_cidrs[i], kMaxConfigValueLength)) {
      return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read network CIDR"));
    }
  }

  // Logging config
  if (!ReadString(input_stream, config.logging.level, kMaxIdentifierLength)) {
    return MakeUnexpected(MakeError(ErrorCode::kStorageDumpReadError, "Failed to read logging.level"));
  }
  if (!ReadString(input_stream, config.logging.format, kMaxIdentifierLength)) {
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

}  // namespace mygramdb::storage::dump_v1
