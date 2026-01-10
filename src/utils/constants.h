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

}  // namespace mygram::constants
