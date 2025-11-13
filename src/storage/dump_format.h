/**
 * @file dump_format.h
 * @brief Binary format definitions for dump files (.dmp)
 *
 * This file defines constants and data structures for MygramDB dump files.
 * Dumps are binary files that contain the complete database state including
 * configuration, indexes, document stores, and replication position (GTID).
 *
 * File Format Overview:
 * Every dump file starts with an 8-byte fixed header:
 *   - 4 bytes: Magic number "MGDB" (0x4D474442)
 *   - 4 bytes: Format version (uint32_t, little-endian)
 *
 * The fixed header is followed by version-specific data.
 * See dump_format_v1.h for Version 1 format details.
 */

#pragma once

#include <array>
#include <cstdint>
#include <string>

namespace mygramdb::storage {

/**
 * @brief Dump file format constants
 */
namespace dump_format {

// Magic number for dump files ("MGDB" in ASCII)
// Used to quickly identify MygramDB dump files
constexpr std::array<char, 4> kMagicNumber = {'M', 'G', 'D', 'B'};

// Current format version (version we write)
// Increment when introducing breaking changes to the format
constexpr uint32_t kCurrentVersion = 1;

// Maximum supported version (versions we can read)
// Must be >= kCurrentVersion, can support newer versions for forward compatibility
constexpr uint32_t kMaxSupportedVersion = 1;

// Minimum supported version (oldest version we can read)
// Must be <= kCurrentVersion, set to 1 to support all versions since initial release
constexpr uint32_t kMinSupportedVersion = 1;

// Fixed file header size (magic + version)
// This header is present in all dump versions
constexpr size_t kFixedHeaderSize = 8;

/**
 * @brief Format version enum for type safety
 */
// NOLINTNEXTLINE(performance-enum-size) - Must match file format uint32_t
enum class FormatVersion : uint32_t {
  V1 = 1,  // Initial version
  // Future versions can be added here
  // V2 = 2,
  // V3 = 3,
};

/**
 * @brief Flags for future extensions (Version 1)
 *
 * These flags are stored in the V1 header and indicate which features
 * are enabled for a particular dump file. Multiple flags can be
 * combined using bitwise OR.
 *
 * Current flags:
 * - kWithStatistics: Dump includes performance statistics
 * - kWithCRC: Dump includes CRC32 checksums (always set in V1)
 *
 * Reserved flags for future use:
 * - kCompressed: Data compression (not yet implemented)
 * - kEncrypted: Data encryption (not yet implemented)
 * - kIncremental: Incremental dump (not yet implemented)
 */
namespace flags_v1 {
constexpr uint32_t kNone = 0x00000000;            // No flags set
constexpr uint32_t kCompressed = 0x00000001;      // Data is compressed (reserved for future)
constexpr uint32_t kEncrypted = 0x00000002;       // Data is encrypted (reserved for future)
constexpr uint32_t kIncremental = 0x00000004;     // Incremental dump (reserved for future)
constexpr uint32_t kWithStatistics = 0x00000008;  // Contains statistics sections
constexpr uint32_t kWithCRC = 0x00000010;         // Contains CRC checksums (always set in V1)
}  // namespace flags_v1

/**
 * @brief CRC error types
 *
 * Classifies the type of CRC mismatch detected during dump verification.
 * This helps identify which part of the dump file is corrupted.
 */
enum class CRCErrorType : std::uint8_t {
  None = 0,           // No error detected
  FileCRC = 1,        // File-level CRC mismatch (entire file corrupted)
  ConfigCRC = 2,      // Config section CRC mismatch
  StatsCRC = 3,       // Statistics section CRC mismatch
  TableStatsCRC = 4,  // Table statistics CRC mismatch (table-specific)
  IndexCRC = 5,       // Index data CRC mismatch (table-specific)
  DocStoreCRC = 6,    // DocStore data CRC mismatch (table-specific)
};

/**
 * @brief File integrity error information
 *
 * Contains detailed information about integrity check failures.
 * Returned by VerifyDumpIntegrity() and ReadDumpV1().
 *
 * Fields:
 * - type: Type of CRC error (None if no error)
 * - message: Human-readable error description
 * - table_name: Table name (for table-specific errors), empty otherwise
 */
struct IntegrityError {
  CRCErrorType type = CRCErrorType::None;  // Type of error detected
  std::string message;                     // Human-readable error message
  std::string table_name;                  // Table name (for table-specific errors)

  /**
   * @brief Check if an error occurred
   * @return true if type != None
   */
  [[nodiscard]] bool HasError() const { return type != CRCErrorType::None; }
};

}  // namespace dump_format

/**
 * @brief Dump statistics (stored in dump file)
 *
 * Aggregate statistics across all tables in the dump.
 * Only included when kWithStatistics flag is set.
 *
 * Use cases:
 * - Monitoring dump growth over time
 * - Capacity planning
 * - Performance analysis
 * - Backup validation
 */
struct DumpStatistics {
  uint64_t total_documents = 0;       // Total documents across all tables
  uint64_t total_terms = 0;           // Total unique terms across all tables
  uint64_t total_index_bytes = 0;     // Total index memory usage (bytes)
  uint64_t total_docstore_bytes = 0;  // Total document store memory usage (bytes)
  uint64_t dump_time_ms = 0;          // Time taken to create dump (milliseconds)
};

/**
 * @brief Per-table statistics (stored in dump file)
 *
 * Statistics for a single table. Only included when kWithStatistics flag is set.
 *
 * Fields:
 * - document_count: Number of documents in the table
 * - term_count: Number of unique N-gram terms in the index
 * - index_bytes: Memory used by the index (bytes)
 * - docstore_bytes: Memory used by the document store (bytes)
 * - next_doc_id: Next document ID that will be assigned (auto-increment)
 * - last_update_time: Unix timestamp of last INSERT/UPDATE/DELETE
 */
struct TableStatistics {
  uint64_t document_count = 0;    // Number of documents in table
  uint64_t term_count = 0;        // Number of unique N-gram terms
  uint64_t index_bytes = 0;       // Index memory usage (bytes)
  uint64_t docstore_bytes = 0;    // Document store memory usage (bytes)
  uint32_t next_doc_id = 0;       // Next document ID to be assigned
  uint64_t last_update_time = 0;  // Last update timestamp (Unix time, seconds)
};

}  // namespace mygramdb::storage
