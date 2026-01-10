/**
 * @file datetime_converter.cpp
 * @brief DateTime string to epoch seconds converter implementation
 */

#include "utils/datetime_converter.h"

#include <algorithm>
#include <ctime>
#include <iomanip>
#include <regex>
#include <sstream>

namespace mygramdb::utils {

using mygram::utils::ErrorCode;

// ============================================================================
// Constants for datetime parsing
// ============================================================================

// Timezone offset format constants
constexpr size_t kTimezoneOffsetLength = 6;  // Format: "+HH:MM" or "-HH:MM"
constexpr size_t kMinuteFirstDigitPos = 4;   // Position of minute's first digit in "+HH:MM"
constexpr size_t kMinuteSecondDigitPos = 5;  // Position of minute's second digit in "+HH:MM"
constexpr int kDecimalBase = 10;             // Decimal base for numeric conversions
constexpr int kMaxHour = 23;                 // Maximum hour value (0-23)
constexpr int kMaxMinute = 59;               // Maximum minute value (0-59)
constexpr int kMinutesFirstDigitMax = 5;     // First digit of minutes (0-5)

// Time conversion constants
constexpr int kSecondsPerHour = 3600;  // 3600 seconds per hour
constexpr int kSecondsPerMinute = 60;  // 60 seconds per minute

// MySQL TIME type constants
constexpr int kMaxMySQLTimeHours = 838;  // MySQL TIME allows -838:59:59 to 838:59:59

// ISO8601 datetime string parsing constants (for "YYYY-MM-DD HH:MM:SS")
constexpr size_t kDateTimeMinLength = 19;     // Minimum length: "YYYY-MM-DD HH:MM:SS"
constexpr size_t kYearStartPos = 0;           // Year start position
constexpr size_t kYearEndPos = 4;             // Year end position (exclusive)
constexpr size_t kFirstDashPos = 4;           // First dash position
constexpr size_t kMonthStartPos = 5;          // Month start position
constexpr size_t kMonthEndPos = 7;            // Month end position (exclusive)
constexpr size_t kSecondDashPos = 7;          // Second dash position
constexpr size_t kDayStartPos = 8;            // Day start position
constexpr size_t kDayEndPos = 10;             // Day end position (exclusive)
constexpr size_t kDateTimeSeparatorPos = 10;  // Space or 'T' separator position
constexpr size_t kHourStartPos = 11;          // Hour start position
constexpr size_t kHourEndPos = 13;            // Hour end position (exclusive)
constexpr size_t kFirstColonPos = 13;         // First colon position
constexpr size_t kMinuteStartPos = 14;        // Minute start position
constexpr size_t kMinuteEndPos = 16;          // Minute end position (exclusive)
constexpr size_t kSecondColonPos = 16;        // Second colon position
constexpr size_t kSecondStartPos = 17;        // Second start position
constexpr size_t kSecondEndPos = 19;          // Second end position (exclusive)

// Date/time validation constants
constexpr int kMinMonth = 1;    // Minimum month value
constexpr int kMaxMonth = 12;   // Maximum month value
constexpr int kMinDay = 1;      // Minimum day value
constexpr int kMaxDay = 31;     // Maximum day value
constexpr int kMaxSecond = 59;  // Maximum second value (same as kMaxMinute)

// std::tm epoch constants
constexpr int kTmEpochYear = 1900;  // std::tm year offset (years since 1900)

// Leap year calculation constants
constexpr int kLeapYearDivisor4 = 4;       // Divisible by 4
constexpr int kLeapYearDivisor100 = 100;   // Not divisible by 100 (unless 400)
constexpr int kLeapYearDivisor400 = 400;   // Divisible by 400
constexpr int kFebruaryMonth = 2;          // February month number
constexpr int kFebruaryLeapDays = 29;      // Days in February in leap year

// ============================================================================
// Calendar validation helpers
// ============================================================================

// Check if a year is a leap year
inline bool IsLeapYear(int year) {
  return (year % kLeapYearDivisor4 == 0 && year % kLeapYearDivisor100 != 0) || (year % kLeapYearDivisor400 == 0);
}

// Get the number of days in a month for a given year
inline int DaysInMonth(int year, int month) {
  // Days in each month (1-indexed, index 0 unused)
  // NOLINTBEGIN(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  static constexpr int kDaysInMonth[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  // NOLINTEND(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)

  if (month < kMinMonth || month > kMaxMonth) {
    return 0;
  }

  if (month == kFebruaryMonth && IsLeapYear(year)) {
    return kFebruaryLeapDays;
  }

  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-constant-array-index)
  return kDaysInMonth[month];
}

// Validate if a date is a valid calendar date
inline bool IsValidCalendarDate(int year, int month, int day) {
  if (month < kMinMonth || month > kMaxMonth || day < kMinDay) {
    return false;
  }
  return day <= DaysInMonth(year, month);
}

// ============================================================================
// TimezoneOffset implementation
// ============================================================================

Expected<TimezoneOffset, Error> TimezoneOffset::Parse(std::string_view offset_str) {
  // Expected format: [+-]HH:MM (e.g., "+09:00", "-05:30")
  if (offset_str.size() != kTimezoneOffsetLength) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid timezone offset format (expected +HH:MM)"));
  }

  char sign = offset_str[0];
  if (sign != '+' && sign != '-') {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Timezone offset must start with + or -"));
  }

  // Parse hours
  if (offset_str[1] < '0' || offset_str[1] > '2' || offset_str[2] < '0' || offset_str[2] > '9') {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid hours in timezone offset"));
  }
  int hours = (offset_str[1] - '0') * kDecimalBase + (offset_str[2] - '0');
  if (hours > kMaxHour) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Hours must be 0-23"));
  }

  // Check colon separator
  if (offset_str[3] != ':') {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Missing colon separator in timezone offset"));
  }

  // Parse minutes
  if (offset_str[kMinuteFirstDigitPos] < '0' ||
      offset_str[kMinuteFirstDigitPos] > static_cast<char>('0' + kMinutesFirstDigitMax) ||
      offset_str[kMinuteSecondDigitPos] < '0' || offset_str[kMinuteSecondDigitPos] > '9') {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid minutes in timezone offset"));
  }
  int minutes = (offset_str[kMinuteFirstDigitPos] - '0') * kDecimalBase + (offset_str[kMinuteSecondDigitPos] - '0');
  if (minutes > kMaxMinute) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Minutes must be 0-59"));
  }

  int32_t offset_seconds = hours * kSecondsPerHour + minutes * kSecondsPerMinute;
  if (sign == '-') {
    offset_seconds = -offset_seconds;
  }

  return TimezoneOffset(offset_seconds);
}

std::string TimezoneOffset::ToString() const {
  std::ostringstream oss;
  int32_t abs_offset = std::abs(offset_seconds_);
  int hours = abs_offset / kSecondsPerHour;
  int minutes = (abs_offset % kSecondsPerHour) / kSecondsPerMinute;

  oss << (offset_seconds_ >= 0 ? '+' : '-') << std::setfill('0') << std::setw(2) << hours << ':' << std::setw(2)
      << minutes;
  return oss.str();
}

// ============================================================================
// DateTimeProcessor implementation
// ============================================================================

Expected<uint64_t, Error> DateTimeProcessor::DateTimeToEpoch(std::string_view datetime_str) const {
  // Use the legacy ConvertToEpoch function
  auto result = ConvertToEpoch(datetime_str, timezone_.GetOffsetSeconds());
  if (!result) {
    return MakeUnexpected(
        MakeError(ErrorCode::kInvalidArgument, std::string("Invalid datetime format: ") + std::string(datetime_str)));
  }
  return *result;
}

Expected<uint64_t, Error> DateTimeProcessor::TimestampToEpoch(std::string_view timestamp_str) {
  try {
    size_t pos = 0;
    uint64_t epoch = std::stoull(std::string(timestamp_str), &pos);
    if (pos != timestamp_str.length()) {
      return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid timestamp: trailing characters"));
    }
    return epoch;
  } catch (const std::exception& e) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, std::string("Invalid timestamp: ") + e.what()));
  }
}

Expected<int64_t, Error> DateTimeProcessor::TimeToSeconds(std::string_view time_str) {
  // MySQL TIME format: "HH:MM:SS" or "HHH:MM:SS" (can be -838:59:59 to 838:59:59)
  // Also supports fractional seconds: "HH:MM:SS.ffffff"

  if (time_str.empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Empty time string"));
  }

  // Check for negative sign
  bool is_negative = false;
  size_t pos = 0;
  if (time_str[0] == '-') {
    is_negative = true;
    pos = 1;
  }

  // Find hour separator
  size_t hour_end = time_str.find(':', pos);
  if (hour_end == std::string_view::npos) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Missing hour separator"));
  }

  // Parse hours (can be 0-838)
  int hours = 0;
  for (size_t i = pos; i < hour_end; ++i) {
    if (time_str[i] < '0' || time_str[i] > '9') {
      return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid hour digit"));
    }
    hours = hours * kDecimalBase + (time_str[i] - '0');
  }
  if (hours > kMaxMySQLTimeHours) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Hours must be 0-838"));
  }

  // Find minute separator
  pos = hour_end + 1;
  size_t minute_end = time_str.find(':', pos);
  if (minute_end == std::string_view::npos || minute_end != pos + 2) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid minute format"));
  }

  // Parse minutes
  int minutes = 0;
  for (size_t i = pos; i < minute_end; ++i) {
    if (time_str[i] < '0' || time_str[i] > '9') {
      return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid minute digit"));
    }
    minutes = minutes * kDecimalBase + (time_str[i] - '0');
  }
  if (minutes > kMaxMinute) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Minutes must be 0-59"));
  }

  // Parse seconds
  pos = minute_end + 1;
  size_t second_end = time_str.find('.', pos);
  if (second_end == std::string_view::npos) {
    second_end = time_str.length();
  }
  if (second_end != pos + 2) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid second format"));
  }

  int seconds = 0;
  for (size_t i = pos; i < second_end; ++i) {
    if (time_str[i] < '0' || time_str[i] > '9') {
      return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid second digit"));
    }
    seconds = seconds * kDecimalBase + (time_str[i] - '0');
  }
  if (seconds > kMaxMinute) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Seconds must be 0-59"));
  }

  // Calculate total seconds
  int64_t total_seconds = static_cast<int64_t>(hours) * kSecondsPerHour + minutes * kSecondsPerMinute + seconds;
  if (is_negative) {
    total_seconds = -total_seconds;
  }

  return total_seconds;
}

Expected<uint64_t, Error> DateTimeProcessor::ParseDateTimeValue(std::string_view value_str) const {
  // If numeric, treat as epoch seconds
  if (IsNumericString(value_str)) {
    return TimestampToEpoch(value_str);
  }

  // Otherwise treat as datetime string
  return DateTimeToEpoch(value_str);
}

// ============================================================================
// Legacy functions (for backward compatibility)
// ============================================================================

std::optional<int32_t> ParseTimezoneOffset(std::string_view timezone_str) {
  // Delegate to TimezoneOffset::Parse() to avoid code duplication
  auto result = TimezoneOffset::Parse(timezone_str);
  if (!result) {
    return std::nullopt;
  }
  return result->GetOffsetSeconds();
}

bool IsNumericString(std::string_view str) {
  if (str.empty()) {
    return false;
  }
  return std::all_of(str.begin(), str.end(), [](char character) { return character >= '0' && character <= '9'; });
}

std::optional<uint64_t> ConvertToEpoch(std::string_view datetime_str, int32_t timezone_offset_sec) {
  // Expected formats:
  // - "YYYY-MM-DD HH:MM:SS"
  // - "YYYY-MM-DDTHH:MM:SS"
  // - "YYYY-MM-DD HH:MM:SS.ffffff" (microseconds ignored)

  if (datetime_str.size() < kDateTimeMinLength) {
    return std::nullopt;
  }

  // Parse components
  int year = 0;
  int month = 0;
  int day = 0;
  int hour = 0;
  int minute = 0;
  int second = 0;

  // Simple manual parsing (avoid regex for performance)
  // Format: "YYYY-MM-DD HH:MM:SS" or "YYYY-MM-DDTHH:MM:SS"
  //          0123456789012345678

  // Parse year (position 0-3)
  for (size_t i = kYearStartPos; i < kYearEndPos; ++i) {
    if (datetime_str[i] < '0' || datetime_str[i] > '9') {
      return std::nullopt;
    }
    year = year * kDecimalBase + (datetime_str[i] - '0');
  }

  // Check separator (position 4)
  if (datetime_str[kFirstDashPos] != '-') {
    return std::nullopt;
  }

  // Parse month (position 5-6)
  for (size_t i = kMonthStartPos; i < kMonthEndPos; ++i) {
    if (datetime_str[i] < '0' || datetime_str[i] > '9') {
      return std::nullopt;
    }
    month = month * kDecimalBase + (datetime_str[i] - '0');
  }

  // Check separator (position 7)
  if (datetime_str[kSecondDashPos] != '-') {
    return std::nullopt;
  }

  // Parse day (position 8-9)
  for (size_t i = kDayStartPos; i < kDayEndPos; ++i) {
    if (datetime_str[i] < '0' || datetime_str[i] > '9') {
      return std::nullopt;
    }
    day = day * kDecimalBase + (datetime_str[i] - '0');
  }

  // Check separator (position 10): space or 'T'
  if (datetime_str[kDateTimeSeparatorPos] != ' ' && datetime_str[kDateTimeSeparatorPos] != 'T') {
    return std::nullopt;
  }

  // Parse hour (position 11-12)
  for (size_t i = kHourStartPos; i < kHourEndPos; ++i) {
    if (datetime_str[i] < '0' || datetime_str[i] > '9') {
      return std::nullopt;
    }
    hour = hour * kDecimalBase + (datetime_str[i] - '0');
  }

  // Check separator (position 13)
  if (datetime_str[kFirstColonPos] != ':') {
    return std::nullopt;
  }

  // Parse minute (position 14-15)
  for (size_t i = kMinuteStartPos; i < kMinuteEndPos; ++i) {
    if (datetime_str[i] < '0' || datetime_str[i] > '9') {
      return std::nullopt;
    }
    minute = minute * kDecimalBase + (datetime_str[i] - '0');
  }

  // Check separator (position 16)
  if (datetime_str[kSecondColonPos] != ':') {
    return std::nullopt;
  }

  // Parse second (position 17-18)
  for (size_t i = kSecondStartPos; i < kSecondEndPos; ++i) {
    if (datetime_str[i] < '0' || datetime_str[i] > '9') {
      return std::nullopt;
    }
    second = second * kDecimalBase + (datetime_str[i] - '0');
  }

  // Validate basic ranges
  if (month < kMinMonth || month > kMaxMonth || day < kMinDay || day > kMaxDay || hour < 0 || hour > kMaxHour ||
      minute < 0 || minute > kMaxMinute || second < 0 || second > kMaxSecond) {
    return std::nullopt;
  }

  // Validate calendar correctness (e.g., Feb 30 is invalid)
  if (!IsValidCalendarDate(year, month, day)) {
    return std::nullopt;
  }

  // Convert to epoch seconds using standard library
  // Note: std::tm uses month as 0-11, year as years since 1900
  std::tm tm_struct = {};
  tm_struct.tm_year = year - kTmEpochYear;
  tm_struct.tm_mon = month - 1;
  tm_struct.tm_mday = day;
  tm_struct.tm_hour = hour;
  tm_struct.tm_min = minute;
  tm_struct.tm_sec = second;
  tm_struct.tm_isdst = -1;  // Let mktime determine DST (though we're using UTC)

  // timegm() is not portable, so we use a workaround:
  // 1. Save current TZ
  // 2. Set TZ=UTC
  // 3. Call mktime()
  // 4. Restore TZ
  //
  // However, for thread-safety, we use a different approach:
  // mktime assumes local time, so we apply timezone offset manually

  // Use mktime to get local time interpretation
  std::time_t local_time = std::mktime(&tm_struct);
  if (local_time == -1) {
    return std::nullopt;
  }

  // mktime treats tm_struct as local time, but we want to treat it as the specified timezone
  // We need to reverse the local timezone offset and apply the specified offset
  //
  // Actually, we want to treat the datetime as being in the specified timezone,
  // then convert to UTC. So:
  // 1. datetime_str is in timezone with offset timezone_offset_sec
  // 2. To get UTC, subtract the offset
  //
  // But mktime interprets tm_struct as local time. We need a different approach.

  // Thread-safe approach: Use timegm if available, otherwise calculate manually
#if defined(__GLIBC__) || defined(__APPLE__)
  // timegm is available on Linux and macOS
  std::time_t utc_time = timegm(&tm_struct);
  if (utc_time == -1) {
    return std::nullopt;
  }
#else
  // Fallback: Calculate epoch manually (simplified, assumes Gregorian calendar)
  // This is less accurate but portable
  // Days since epoch calculation
  // Note: Uses IsLeapYear() function defined at the top of this file
  constexpr int days_in_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

  int days_since_epoch = 0;
  // Count years
  for (int y = 1970; y < year; ++y) {
    days_since_epoch += IsLeapYear(y) ? 366 : 365;
  }
  // Count months
  for (int m = 1; m < month; ++m) {
    days_since_epoch += days_in_month[m - 1];
    if (m == 2 && IsLeapYear(year)) {
      days_since_epoch += 1;  // Leap year February
    }
  }
  // Add days
  days_since_epoch += day - 1;

  std::time_t utc_time = static_cast<std::time_t>(days_since_epoch) * 86400 + hour * 3600 + minute * 60 + second;
#endif

  // Now we have the datetime interpreted as UTC
  // Apply the timezone offset: datetime is in timezone_offset_sec timezone,
  // so to get UTC, we subtract the offset
  int64_t epoch_seconds = static_cast<int64_t>(utc_time) - static_cast<int64_t>(timezone_offset_sec);

  if (epoch_seconds < 0) {
    return std::nullopt;  // Dates before 1970-01-01 are not supported
  }

  return static_cast<uint64_t>(epoch_seconds);
}

std::optional<uint64_t> ParseDatetimeValue(std::string_view value_str, std::string_view timezone_str) {
  // If numeric, treat as epoch seconds
  if (IsNumericString(value_str)) {
    try {
      return std::stoull(std::string(value_str));
    } catch (const std::invalid_argument&) {
      return std::nullopt;
    } catch (const std::out_of_range&) {
      return std::nullopt;
    }
  }

  // Parse timezone offset
  auto offset_opt = ParseTimezoneOffset(timezone_str);
  if (!offset_opt) {
    return std::nullopt;
  }

  // Convert ISO8601 string to epoch
  return ConvertToEpoch(value_str, *offset_opt);
}

}  // namespace mygramdb::utils
