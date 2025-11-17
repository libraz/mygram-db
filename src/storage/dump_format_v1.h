/**
 * @file dump_format_v1.h
 * @brief Dump file format Version 1 serialization/deserialization
 *
 * This file defines the Version 1 dump format for MygramDB. Dumps are binary
 * files (.dmp) that contain the complete database state including configuration,
 * indexes, document stores, and replication position (GTID).
 *
 * File Structure:
 * ┌─────────────────────────────────────────────────────────────┐
 * │ Fixed File Header (8 bytes)                                 │
 * │   - Magic: "MGDB" (4 bytes)                                 │
 * │   - Format Version: 1 (4 bytes)                             │
 * ├─────────────────────────────────────────────────────────────┤
 * │ Version 1 Header (variable length)                          │
 * │   - Header Size                                             │
 * │   - Flags (kWithStatistics, kWithCRC)                       │
 * │   - Dump Timestamp                                          │
 * │   - Total File Size (for truncation detection)             │
 * │   - File CRC32 (entire file checksum)                      │
 * │   - GTID (replication position)                            │
 * ├─────────────────────────────────────────────────────────────┤
 * │ Config Section                                              │
 * │   - Length (4 bytes)                                        │
 * │   - CRC32 (4 bytes)                                         │
 * │   - Serialized Configuration                               │
 * ├─────────────────────────────────────────────────────────────┤
 * │ Statistics Section (optional, if kWithStatistics)          │
 * │   - Length (4 bytes)                                        │
 * │   - CRC32 (4 bytes)                                         │
 * │   - Dump Statistics                                         │
 * ├─────────────────────────────────────────────────────────────┤
 * │ Table Data Section                                          │
 * │   - Table Count (4 bytes)                                  │
 * │   ┌───────────────────────────────────────────────────────┐│
 * │   │ For each table:                                       ││
 * │   │   - Table Name (length-prefixed string)               ││
 * │   │   - Table Statistics (optional, if kWithStatistics)  ││
 * │   │   - Index Data (length + CRC32 + data)               ││
 * │   │   - DocStore Data (length + CRC32 + data)            ││
 * │   └───────────────────────────────────────────────────────┘│
 * └─────────────────────────────────────────────────────────────┘
 *
 * All multi-byte integers are stored in little-endian format.
 * All strings are UTF-8 encoded with length-prefix (uint32_t).
 * CRC32 checksums use zlib implementation (polynomial: 0xEDB88320).
 */

#pragma once

#include <iosfwd>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "storage/document_store.h"
#include "storage/dump_format.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::storage::dump_v1 {

using mygram::utils::Error;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

/**
 * @brief Version 1 dump header
 *
 * This header contains metadata about the dump file and integrity
 * verification information. It follows the fixed 8-byte file header.
 *
 * Layout:
 * | Offset | Size | Field          | Description                        |
 * |--------|------|----------------|------------------------------------|
 * | 0      | 4    | header_size    | Size of V1 header in bytes         |
 * | 4      | 4    | flags          | Feature flags (see flags_v1)       |
 * | 8      | 8    | dump_timestamp | Unix timestamp (seconds)           |
 * | 16     | 8    | total_file_size| Expected file size (bytes)         |
 * | 24     | 4    | file_crc32     | CRC32 of entire file               |
 * | 28     | 4    | gtid_length    | Length of GTID string              |
 * | 32     | N    | gtid           | GTID string (UTF-8)                |
 */
struct HeaderV1 {
  uint32_t header_size = 0;      // Size of this header in bytes
  uint32_t flags = 0;            // Flags (see dump_format::flags_v1)
  uint64_t dump_timestamp = 0;   // Unix timestamp when dump was created
  uint64_t total_file_size = 0;  // Expected total file size (for truncation detection)
  uint32_t file_crc32 = 0;       // CRC32 of entire file (excluding this field itself)
  std::string gtid;              // Replication GTID at dump time
};

/**
 * @brief Serialize Config to output stream
 * @param output_stream Output stream
 * @param config Configuration to serialize
 * @return Expected<void, Error> Success or error with details
 */
Expected<void, Error> SerializeConfig(std::ostream& output_stream, const config::Config& config);

/**
 * @brief Deserialize Config from input stream
 * @param input_stream Input stream
 * @param config Configuration to deserialize into
 * @return Expected<void, Error> Success or error with details
 */
Expected<void, Error> DeserializeConfig(std::istream& input_stream, config::Config& config);

/**
 * @brief Serialize DumpStatistics to output stream
 * @param output_stream Output stream
 * @param stats Statistics to serialize
 * @return Expected<void, Error> Success or error with details
 */
Expected<void, Error> SerializeStatistics(std::ostream& output_stream, const DumpStatistics& stats);

/**
 * @brief Deserialize DumpStatistics from input stream
 * @param input_stream Input stream
 * @param stats Statistics to deserialize into
 * @return Expected<void, Error> Success or error with details
 */
Expected<void, Error> DeserializeStatistics(std::istream& input_stream, DumpStatistics& stats);

/**
 * @brief Serialize TableStatistics to output stream
 * @param output_stream Output stream
 * @param stats Table statistics to serialize
 * @return Expected<void, Error> Success or error with details
 */
Expected<void, Error> SerializeTableStatistics(std::ostream& output_stream, const TableStatistics& stats);

/**
 * @brief Deserialize TableStatistics from input stream
 * @param input_stream Input stream
 * @param stats Table statistics to deserialize into
 * @return Expected<void, Error> Success or error with details
 */
Expected<void, Error> DeserializeTableStatistics(std::istream& input_stream, TableStatistics& stats);

/**
 * @brief Write Version 1 dump header
 * @param output_stream Output stream
 * @param header Header to write
 * @return Expected<void, Error> Success or error with details
 */
Expected<void, Error> WriteHeaderV1(std::ostream& output_stream, const HeaderV1& header);

/**
 * @brief Read Version 1 dump header
 * @param input_stream Input stream
 * @param header Header to read into
 * @return Expected<void, Error> Success or error with details
 */
Expected<void, Error> ReadHeaderV1(std::istream& input_stream, HeaderV1& header);

/**
 * @brief Write complete dump to file (Version 1 format)
 *
 * Creates a dump file containing the complete database state. The write process
 * is atomic - data is first written to a temporary file, then renamed on success.
 *
 * The dump includes:
 * - Fixed file header (magic number "MGDB" + version)
 * - V1 header (metadata, flags, GTID, CRC32)
 * - Configuration section
 * - Statistics section (if stats provided)
 * - Table data (index + document store for each table)
 *
 * CRC32 checksums are calculated for:
 * - Entire file (file_crc32 in header)
 * - Config section
 * - Statistics section
 * - Each table's index data
 * - Each table's document store data
 *
 * Write process:
 * 1. Write fixed header (magic + version)
 * 2. Write V1 header (with placeholder CRC32 and file size)
 * 3. Write config section
 * 4. Write statistics section (if provided)
 * 5. Write table data sections
 * 6. Calculate file CRC32 and update header
 * 7. Atomic rename from temp file to final path
 *
 * @param filepath Output file path (e.g., "/var/lib/mygramdb/dumps/mygramdb.dmp")
 * @param gtid Current GTID for replication resume
 * @param config Full configuration to serialize
 * @param table_contexts Map of table name to (Index*, DocumentStore*) pairs
 * @param stats Optional dump-level statistics (total documents, terms, bytes)
 * @param table_stats Optional per-table statistics map
 * @return Expected<void, Error> Success or error with details (context: filepath)
 *
 * @note Writes to temporary file first (.tmp suffix), then atomic rename
 * @note All data is written in little-endian format
 * @note CRC32 uses zlib implementation (polynomial: 0xEDB88320)
 */
Expected<void, Error> WriteDumpV1(
    const std::string& filepath, const std::string& gtid, const config::Config& config,
    const std::unordered_map<std::string, std::pair<index::Index*, DocumentStore*>>& table_contexts,
    const DumpStatistics* stats = nullptr,
    const std::unordered_map<std::string, TableStatistics>* table_stats = nullptr);

/**
 * @brief Read complete dump from file (Version 1 format)
 *
 * Loads a dump file and restores the complete database state. All data is
 * validated using CRC32 checksums to ensure integrity.
 *
 * Load process:
 * 1. Read and validate fixed header (magic + version)
 * 2. Validate file size against header.total_file_size
 * 3. Calculate and verify file-level CRC32
 * 4. Read and deserialize config section (verify CRC32)
 * 5. Read statistics section if present (verify CRC32)
 * 6. For each table:
 *    - Read table name
 *    - Read table statistics if present (verify CRC32)
 *    - Load index data (verify CRC32)
 *    - Load document store data (verify CRC32)
 * 7. Populate table_contexts with loaded data
 * 8. Return GTID for replication resume
 *
 * Error handling:
 * - Version mismatch: Returns false if version is unsupported
 * - File truncation: Detected via file size check
 * - CRC mismatch: Detected at file and section levels
 * - All errors are logged via spdlog and optionally returned in integrity_error
 *
 * @param filepath Input file path (e.g., "/var/lib/mygramdb/dumps/mygramdb.dmp")
 * @param gtid Output GTID for replication resume
 * @param config Output configuration loaded from dump
 * @param table_contexts Map of table name to (Index*, DocumentStore*) pairs.
 *                       MUST be pre-allocated with empty Index/DocumentStore objects.
 *                       Data will be loaded into these objects.
 * @param stats Optional output for dump-level statistics
 * @param table_stats Optional output for per-table statistics map
 * @param integrity_error Optional output for detailed integrity error information
 * @return Expected<void, Error> Success or error with details (context: filepath, section)
 *
 * @note table_contexts MUST contain pre-allocated Index and DocumentStore objects
 * @note All loaded data replaces existing data in the provided objects
 * @note CRC32 verification is always performed
 * @note Little-endian format expected for all multi-byte integers
 */
Expected<void, Error> ReadDumpV1(
    const std::string& filepath, std::string& gtid, config::Config& config,
    std::unordered_map<std::string, std::pair<index::Index*, DocumentStore*>>& table_contexts,
    DumpStatistics* stats = nullptr, std::unordered_map<std::string, TableStatistics>* table_stats = nullptr,
    dump_format::IntegrityError* integrity_error = nullptr);

/**
 * @brief Verify dump file integrity without loading
 *
 * Validates a dump file's integrity without loading the actual data into memory.
 * This is much faster than a full load and useful for validating backups.
 *
 * Verification checks:
 * 1. File exists and is readable
 * 2. Magic number is correct ("MGDB")
 * 3. Version is supported
 * 4. File size matches header.total_file_size
 * 5. File-level CRC32 matches calculated checksum
 *
 * This function does NOT verify:
 * - Individual section CRC32s (config, stats, index, docstore)
 * - Data deserialization correctness
 * - Configuration validity
 *
 * @param filepath Dump file path to verify
 * @param integrity_error Output for detailed error information if verification fails
 * @return Expected<void, Error> Success or error with details (context: filepath)
 *
 * @note This is a fast check suitable for cron jobs and health checks
 * @note For full validation, use ReadDumpV1() which verifies all sections
 */
Expected<void, Error> VerifyDumpIntegrity(const std::string& filepath, dump_format::IntegrityError& integrity_error);

/**
 * @brief Calculate CRC32 checksum for data
 * @param data Data pointer
 * @param length Data length in bytes
 * @return CRC32 checksum
 */
uint32_t CalculateCRC32(const void* data, size_t length);

/**
 * @brief Calculate CRC32 checksum for string
 * @param str String data
 * @return CRC32 checksum
 */
uint32_t CalculateCRC32(const std::string& str);

/**
 * @brief Rebuild index from document store (for compact mode)
 * @param doc_store Document store containing all documents
 * @param index Index to rebuild
 * @param table_config Table configuration for n-gram settings
 * @return Expected<void, Error> Success or error with details
 */
Expected<void, Error> RebuildIndexFromDocStore(const DocumentStore& doc_store, index::Index& index,
                                               const config::TableConfig& table_config);

/**
 * @brief Dump file metadata information
 *
 * Lightweight metadata structure returned by GetDumpInfo().
 * Contains summary information about a dump file without loading
 * the actual data.
 */
struct DumpInfo {
  uint32_t version = 0;         // Format version (1 for V1)
  std::string gtid;             // Replication GTID at dump time
  uint32_t table_count = 0;     // Number of tables in dump
  uint32_t flags = 0;           // Feature flags (see dump_format::flags_v1)
  uint64_t file_size = 0;       // Total file size in bytes
  uint64_t timestamp = 0;       // Unix timestamp when dump was created
  bool has_statistics = false;  // True if dump contains statistics sections
};

/**
 * @brief Read dump file metadata without loading data
 *
 * Quickly reads dump metadata without loading indexes or document stores.
 * Useful for displaying dump information to users (DUMP INFO command).
 *
 * Information extracted:
 * - Format version
 * - GTID (replication position)
 * - Table count (from config section)
 * - Feature flags (statistics, CRC, etc.)
 * - File size
 * - Creation timestamp
 *
 * This function:
 * - Does NOT load index or document store data
 * - Does NOT verify CRC checksums
 * - Only reads headers and config section
 * - Is very fast (< 1ms for typical files)
 *
 * @param filepath Dump file path
 * @param info Output structure for dump metadata
 * @return Expected<void, Error> Success or error with details (context: filepath)
 *
 * @note This does not validate file integrity - use VerifyDumpIntegrity() for that
 * @note Suitable for listing/browsing dump files
 */
Expected<void, Error> GetDumpInfo(const std::string& filepath, DumpInfo& info);

}  // namespace mygramdb::storage::dump_v1
