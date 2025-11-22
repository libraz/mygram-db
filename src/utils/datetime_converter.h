/**
 * @file datetime_converter.h
 * @brief DateTime string to epoch seconds converter with timezone support
 */

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::utils {

using mygram::utils::Error;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

/**
 * @brief Timezone offset value object
 *
 * Represents a timezone offset in hours and minutes from UTC.
 * Thread-safe and immutable after construction.
 */
class TimezoneOffset {
 public:
  /**
   * @brief Parse timezone offset string
   * @param offset_str String in format "+HH:MM" or "-HH:MM" (e.g., "+09:00", "-05:30")
   * @return TimezoneOffset if valid, Error otherwise
   */
  static Expected<TimezoneOffset, Error> Parse(std::string_view offset_str);

  /**
   * @brief Create UTC timezone offset
   */
  static TimezoneOffset UTC() { return TimezoneOffset(0); }

  /**
   * @brief Get offset in seconds from UTC
   */
  int32_t GetOffsetSeconds() const { return offset_seconds_; }

  /**
   * @brief Get string representation (e.g., "+09:00")
   */
  std::string ToString() const;

 private:
  explicit TimezoneOffset(int32_t offset_seconds) : offset_seconds_(offset_seconds) {}

  int32_t offset_seconds_;
};

/**
 * @brief DateTime and Time processor with timezone support
 *
 * Thread-safe processor for converting between MySQL datetime/time strings
 * and Unix epoch seconds. Configured with a timezone offset for DATETIME interpretation.
 */
class DateTimeProcessor {
 public:
  /**
   * @brief Construct processor with timezone offset
   * @param timezone Timezone offset for DATETIME interpretation
   */
  explicit DateTimeProcessor(TimezoneOffset timezone) : timezone_(timezone) {}

  /**
   * @brief Convert DATETIME string to epoch seconds
   * @param datetime_str String in format "YYYY-MM-DD HH:MM:SS[.ffffff]"
   * @return Unix epoch seconds (UTC), or Error if invalid
   */
  Expected<uint64_t, Error> DateTimeToEpoch(std::string_view datetime_str) const;

  /**
   * @brief Convert TIMESTAMP string (already in epoch) to uint64_t
   * @param timestamp_str Numeric string representing epoch seconds
   * @return Unix epoch seconds, or Error if invalid
   */
  static Expected<uint64_t, Error> TimestampToEpoch(std::string_view timestamp_str);

  /**
   * @brief Convert TIME string to seconds since midnight
   * @param time_str String in format "HH:MM:SS[.ffffff]" or "HHH:MM:SS"
   * @return Seconds since midnight (0-86399 for normal time, can be negative or >86400 for time ranges)
   */
  static Expected<int64_t, Error> TimeToSeconds(std::string_view time_str);

  /**
   * @brief Parse datetime/timestamp value (auto-detect format)
   * @param value_str Either epoch seconds (numeric) or ISO8601 datetime string
   * @return Unix epoch seconds (UTC), or Error if invalid
   */
  Expected<uint64_t, Error> ParseDateTimeValue(std::string_view value_str) const;

  /**
   * @brief Get configured timezone
   */
  const TimezoneOffset& GetTimezone() const { return timezone_; }

 private:
  TimezoneOffset timezone_;
};

// Legacy functions for backward compatibility (will be deprecated)

/**
 * @brief Parse timezone offset string to seconds
 *
 * @param timezone_str Timezone offset string (e.g., "+09:00", "-05:30", "+00:00")
 * @return Timezone offset in seconds, or std::nullopt if invalid format
 *
 * Examples:
 * - "+09:00" → 32400 (9 * 3600)
 * - "-05:30" → -19800 (-(5 * 3600 + 30 * 60))
 * - "+00:00" → 0
 */
std::optional<int32_t> ParseTimezoneOffset(std::string_view timezone_str);

/**
 * @brief Check if string is a numeric string (epoch seconds)
 *
 * @param str String to check
 * @return true if string contains only digits
 */
bool IsNumericString(std::string_view str);

/**
 * @brief Convert ISO8601-style datetime string to epoch seconds
 *
 * Supported formats:
 * - "YYYY-MM-DD HH:MM:SS" (e.g., "2024-11-22 10:00:00")
 * - "YYYY-MM-DDTHH:MM:SS" (e.g., "2024-11-22T10:00:00")
 * - "YYYY-MM-DD HH:MM:SS.ffffff" (with microseconds)
 *
 * @param datetime_str DateTime string in ISO8601 format
 * @param timezone_offset_sec Timezone offset in seconds (0 for UTC)
 * @return Unix epoch seconds (UTC), or std::nullopt if invalid format
 *
 * Example:
 * - "2024-11-22 10:00:00" with timezone_offset_sec=32400 (+09:00)
 *   → 1732240800 (2024-11-22 01:00:00 UTC)
 */
std::optional<uint64_t> ConvertToEpoch(std::string_view datetime_str, int32_t timezone_offset_sec);

/**
 * @brief Parse datetime value (either epoch seconds or ISO8601 string)
 *
 * This is a convenience function that:
 * 1. If the string is numeric, treats it as epoch seconds
 * 2. If the string is ISO8601 format, converts it using the timezone offset
 *
 * @param value_str DateTime value (either "1732240800" or "2024-11-22 10:00:00")
 * @param timezone_str Timezone offset string (e.g., "+09:00")
 * @return Unix epoch seconds (UTC), or std::nullopt if invalid
 */
std::optional<uint64_t> ParseDatetimeValue(std::string_view value_str, std::string_view timezone_str);

}  // namespace mygramdb::utils
