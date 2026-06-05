/**
 * @file constants.h
 * @brief Common constants used across the codebase
 *
 * This header consolidates commonly used constants to avoid duplication
 * and ensure consistency (REF-0008).
 */

#pragma once

#include <cstddef>
#include <cstdint>

#include "utils/namespace_compat.h"

namespace mygram::constants {

// ============================================================================
// Byte unit constants
// ============================================================================

/// Bytes per kilobyte (1024)
constexpr size_t kBytesPerKilobyte = 1024;

/// Bytes per megabyte (1024 * 1024)
constexpr size_t kBytesPerMegabyte = kBytesPerKilobyte * 1024;

/// Bytes per gigabyte (1024 * 1024 * 1024)
constexpr size_t kBytesPerGigabyte = kBytesPerMegabyte * 1024;

/// Bytes per kilobyte as double (for floating-point calculations)
constexpr double kBytesPerKilobyteDouble = 1024.0;

/// Bytes per megabyte as double (for floating-point calculations)
constexpr double kBytesPerMegabyteDouble = kBytesPerKilobyteDouble * 1024.0;

/// Bytes per gigabyte as double (for floating-point calculations)
constexpr double kBytesPerGigabyteDouble = kBytesPerMegabyteDouble * 1024.0;

// ============================================================================
// Time constants
// ============================================================================

/// Milliseconds per second
constexpr int64_t kMillisecondsPerSecond = 1000;

/// Microseconds per millisecond
constexpr int64_t kMicrosecondsPerMillisecond = 1000;

/// Microseconds per second
constexpr int64_t kMicrosecondsPerSecond = 1000000;

/// Nanoseconds per second
constexpr int64_t kNanosecondsPerSecond = 1000000000;

/// Seconds per minute
constexpr int kSecondsPerMinute = 60;

/// Seconds per hour
constexpr int kSecondsPerHour = 3600;

/// Seconds per day
constexpr int kSecondsPerDay = 86400;

// ============================================================================
// MySQL binlog constants
// ============================================================================

/// Standard MySQL binlog event header length (LOG_EVENT_HEADER_LEN)
constexpr size_t kBinlogEventHeaderLen = 19;

/// CRC32 checksum length appended to each binlog event (BINLOG_CHECKSUM_LEN)
constexpr size_t kBinlogChecksumSize = 4;

/// Maximum number of columns in a MySQL table
constexpr uint64_t kMySQLMaxColumns = 4096;

/// Estimated average bytes per binlog row (for reserve heuristics)
constexpr size_t kEstimatedBytesPerBinlogRow = 100;

/// Maximum number of rows to pre-reserve in binlog parsing
constexpr size_t kMaxPreReserveRows = 10000;

// ============================================================================
// GTID constants
// ============================================================================

/// Length of the "gtid=" prefix in start_from configuration
constexpr size_t kGtidPrefixLength = 5;

// ============================================================================
// Character encoding constants
// ============================================================================

/// First byte value outside the ASCII range (0x80 = 128)
constexpr unsigned char kFirstNonAsciiByte = 0x80;

// ============================================================================
// Floating-point comparison constants
// ============================================================================

/// Absolute epsilon for filter value floating-point comparisons.
/// Appropriate for user-facing numbers (prices, ratings, etc.).
constexpr double kFilterValueEpsilon = 1e-9;

}  // namespace mygram::constants
