/**
 * @file dump_format_v2.h
 * @brief Dump file format Version 2 serialization/deserialization
 *
 * Version 2 uses a section envelope pattern where each data section is wrapped
 * in a self-describing envelope (SectionType + CRC32 + data_length). This enables:
 *
 * - Forward compatibility: unknown section types are safely skipped
 * - Per-section integrity: each section has its own CRC32
 * - Extensibility: new section types (BM25, synonyms) can be added without
 *   breaking existing readers
 *
 * File Structure:
 * ┌─────────────────────────────────────────────────────────────┐
 * │ Fixed File Header (8 bytes)                                 │
 * │   - Magic: "MGDB" (4 bytes)                                 │
 * │   - Format Version: 2 (4 bytes)                             │
 * ├─────────────────────────────────────────────────────────────┤
 * │ Version 2 Header (variable length)                          │
 * │   - Header Size                                             │
 * │   - Flags                                                   │
 * │   - Dump Timestamp                                          │
 * │   - Total File Size                                         │
 * │   - File CRC32                                              │
 * │   - Section Count                                           │
 * │   - GTID (replication position)                            │
 * ├─────────────────────────────────────────────────────────────┤
 * │ Section 1: Config (envelope + data)                         │
 * │   - SectionType: kConfig (4 bytes)                          │
 * │   - CRC32 (4 bytes)                                         │
 * │   - Data Length (8 bytes)                                    │
 * │   - Config Data (N bytes)                                   │
 * ├─────────────────────────────────────────────────────────────┤
 * │ Section 2: Statistics (envelope + data, optional)           │
 * ├─────────────────────────────────────────────────────────────┤
 * │ Section 3..N: TableData (envelope + data, one per table)   │
 * │   - Table Name (length-prefixed)                            │
 * │   - Table Statistics (optional)                             │
 * │   - Index Data (length + data)                              │
 * │   - DocStore Data (length + data)                           │
 * └─────────────────────────────────────────────────────────────┘
 *
 * All multi-byte integers are stored in little-endian format.
 * CRC32 checksums use zlib implementation (polynomial: 0xEDB88320).
 *
 * Snapshot consistency contract:
 * WriteDumpV2 serializes each table's Index and DocumentStore as separate
 * component snapshots. Callers that require a cross-component, cross-table
 * point-in-time dump must quiesce writers before calling this function. The
 * server dump paths satisfy this by pausing replication and blocking DUMP LOAD;
 * direct callers must provide equivalent exclusion.
 */

#pragma once

#include <cstdint>
#include <functional>
#include <iosfwd>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "storage/document_store.h"
#include "storage/dump_format.h"
#include "storage/dump_format_v1.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::storage::dump_v2 {

using mygram::utils::Error;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

using DumpTableProgressCallback = std::function<void(const std::string& table_name, size_t tables_processed)>;
using DumpConfigValidationCallback = std::function<Expected<void, Error>(const config::Config& loaded_config)>;

// Reuse string limits from V1
using dump_v1::kMaxConfigSectionLength;
using dump_v1::kMaxConfigValueLength;
using dump_v1::kMaxGeneralStringLength;
using dump_v1::kMaxIdentifierLength;
using dump_v1::kMaxPathLength;
using dump_v1::kMaxStatsSectionLength;
using dump_v1::kMaxTextContentLength;

/// Maximum allowed section data length (4 GB - prevents OOM from malicious files)
constexpr uint64_t kMaxSectionDataLength = 4ULL * 1024 * 1024 * 1024;

/**
 * @brief Version 2 dump header
 *
 * Layout:
 * | Offset | Size | Field           | Description                        |
 * |--------|------|-----------------|------------------------------------|
 * | 0      | 4    | header_size     | Size of V2 header in bytes         |
 * | 4      | 4    | flags           | Feature flags (see flags_v2)       |
 * | 8      | 8    | dump_timestamp  | Unix timestamp (seconds)           |
 * | 16     | 8    | total_file_size | Expected file size (bytes)         |
 * | 24     | 4    | file_crc32      | CRC32 of entire file               |
 * | 28     | 4    | section_count   | Number of sections in the file     |
 * | 32     | 4    | gtid_length     | Length of GTID string              |
 * | 36     | N    | gtid            | GTID string (UTF-8)                |
 */
struct HeaderV2 {
  uint32_t header_size = 0;      // Size of this header in bytes
  uint32_t flags = 0;            // Flags (see dump_format::flags_v2)
  uint64_t dump_timestamp = 0;   // Unix timestamp when dump was created
  uint64_t total_file_size = 0;  // Expected total file size (for truncation detection)
  uint32_t file_crc32 = 0;       // CRC32 of entire file (excluding this field itself)
  uint32_t section_count = 0;    // Number of sections in the file
  std::string gtid;              // Replication GTID at dump time
};

/// @name Header field offsets for V2 format (relative to start of file)
/// Layout: magic(4) + version(4) + header_size(4) + flags(4) + timestamp(8) = 24
/// @{
constexpr std::streamoff kV2HeaderTotalFileSizeOffset = 24;
/// total_file_size(8) follows, so CRC32 is at offset 32
constexpr std::streamoff kV2HeaderFileCRC32Offset = 32;
/// file_crc32(4) follows, so section_count is at offset 36
constexpr std::streamoff kV2HeaderSectionCountOffset = 36;
/// @}

/**
 * @brief Write Version 2 dump header
 */
Expected<void, Error> WriteHeaderV2(std::ostream& output_stream, const HeaderV2& header);

/**
 * @brief Read Version 2 dump header
 */
Expected<void, Error> ReadHeaderV2(std::istream& input_stream, HeaderV2& header);

/**
 * @brief Write a section envelope + data to stream
 *
 * Computes CRC32 of data, then writes: type(4) + crc32(4) + data_length(8) + data(N)
 *
 * @param output_stream Output stream
 * @param type Section type
 * @param data Section data bytes
 * @return Expected<void, Error>
 */
Expected<void, Error> WriteSectionEnvelope(std::ostream& output_stream, dump_format::SectionType type,
                                           const std::string& data);

/**
 * @brief Write a section envelope from an ostringstream
 *
 * Convenience overload that extracts the data via .str() and delegates to
 * the string overload. This creates a copy of the buffer contents.
 *
 * @param output_stream Output stream
 * @param type Section type
 * @param data_stream Stream containing section data
 * @return Expected<void, Error>
 */
Expected<void, Error> WriteSectionEnvelope(std::ostream& output_stream, dump_format::SectionType type,
                                           std::ostringstream& data_stream);

/**
 * @brief Read a section envelope from stream (does not read the data)
 *
 * Reads: type(4) + crc32(4) + data_length(8)
 * Stream position is left at the start of the section data.
 *
 * @param input_stream Input stream
 * @param envelope Output envelope
 * @return Expected<void, Error>
 */
Expected<void, Error> ReadSectionEnvelope(std::istream& input_stream, dump_format::SectionEnvelope& envelope);

/**
 * @brief Write complete dump to file (Version 2 format)
 *
 * @pre Writers that can mutate any Index or DocumentStore in table_contexts are
 *      paused for the duration of the call when a point-in-time dump is needed.
 *      The function does not acquire a global multi-component snapshot lock.
 *
 * @param filepath Output file path
 * @param gtid Current GTID for replication resume
 * @param config Full configuration to serialize
 * @param table_contexts Map of table name to (Index*, DocumentStore*) pairs
 * @param stats Optional dump-level statistics
 * @param table_stats Optional per-table statistics map
 * @return Expected<void, Error>
 */
Expected<void, Error> WriteDumpV2(
    const std::string& filepath, const std::string& gtid, const config::Config& config,
    const std::unordered_map<std::string, std::pair<index::Index*, DocumentStore*>>& table_contexts,
    const DumpStatistics* stats = nullptr,
    const std::unordered_map<std::string, TableStatistics>* table_stats = nullptr,
    const DumpTableProgressCallback& table_progress_callback = {});

/**
 * @brief Read complete dump from file (Version 2 format)
 *
 * Reads section envelopes, dispatches known types, skips unknown types.
 * Verifies per-section CRC32 and file-level CRC32.
 *
 * @param filepath Input file path
 * @param gtid Output GTID for replication resume
 * @param config Output configuration loaded from dump
 * @param table_contexts Map of table name to (Index*, DocumentStore*) pairs (pre-allocated)
 * @param stats Optional output for dump-level statistics
 * @param table_stats Optional output for per-table statistics map
 * @param integrity_error Optional output for detailed integrity error information
 * @return Expected<void, Error>
 */
Expected<void, Error> ReadDumpV2(
    const std::string& filepath, std::string& gtid, config::Config& config,
    std::unordered_map<std::string, std::pair<index::Index*, DocumentStore*>>& table_contexts,
    DumpStatistics* stats = nullptr, std::unordered_map<std::string, TableStatistics>* table_stats = nullptr,
    dump_format::IntegrityError* integrity_error = nullptr, const DumpConfigValidationCallback& config_validator = {});

// ============================================================================
// Version dispatch functions
// ============================================================================

/**
 * @brief Write dump using current format version (V2)
 *
 * Always writes in the latest format (V2).
 */
Expected<void, Error> WriteDump(
    const std::string& filepath, const std::string& gtid, const config::Config& config,
    const std::unordered_map<std::string, std::pair<index::Index*, DocumentStore*>>& table_contexts,
    const DumpStatistics* stats = nullptr,
    const std::unordered_map<std::string, TableStatistics>* table_stats = nullptr,
    const DumpTableProgressCallback& table_progress_callback = {});

/**
 * @brief Read dump from file (auto-detects V1 or V2 format)
 *
 * Reads the fixed header to determine the format version, then dispatches
 * to the appropriate reader.
 */
Expected<void, Error> ReadDump(
    const std::string& filepath, std::string& gtid, config::Config& config,
    std::unordered_map<std::string, std::pair<index::Index*, DocumentStore*>>& table_contexts,
    DumpStatistics* stats = nullptr, std::unordered_map<std::string, TableStatistics>* table_stats = nullptr,
    dump_format::IntegrityError* integrity_error = nullptr, const DumpConfigValidationCallback& config_validator = {});

/**
 * @brief Verify dump file integrity (auto-detects V1 or V2 format)
 */
Expected<void, Error> VerifyDumpIntegrity(const std::string& filepath, dump_format::IntegrityError& integrity_error);

/**
 * @brief Dump file metadata for V2 format
 */
struct DumpV2Info {
  uint32_t version = 0;
  std::string gtid;
  uint32_t table_count = 0;
  uint32_t flags = 0;
  uint64_t file_size = 0;
  uint64_t timestamp = 0;
  uint32_t section_count = 0;
  bool has_statistics = false;
  std::vector<dump_format::SectionType> section_types;  // Section types present in the file
};

/**
 * @brief Read dump file metadata (auto-detects V1 or V2 format)
 *
 * For V1 files, populates a subset of fields. For V2 files, also reports
 * section types present.
 */
Expected<void, Error> GetDumpInfo(const std::string& filepath, DumpV2Info& info);

}  // namespace mygramdb::storage::dump_v2
