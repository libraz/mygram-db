/**
 * @file datetime_converter_test.cpp
 * @brief Unit tests for datetime/timestamp/time conversion utilities
 */

#include "utils/datetime_converter.h"

#include <gtest/gtest.h>

namespace mygramdb::utils {

// ============================================================================
// TimezoneOffset Tests
// ============================================================================

TEST(TimezoneOffsetTest, ParseValidOffsets) {
  // Positive offsets
  auto offset1 = TimezoneOffset::Parse("+00:00");
  ASSERT_TRUE(offset1.has_value());
  EXPECT_EQ(offset1->GetOffsetSeconds(), 0);
  EXPECT_EQ(offset1->ToString(), "+00:00");

  auto offset2 = TimezoneOffset::Parse("+09:00");
  ASSERT_TRUE(offset2.has_value());
  EXPECT_EQ(offset2->GetOffsetSeconds(), 9 * 3600);
  EXPECT_EQ(offset2->ToString(), "+09:00");

  auto offset3 = TimezoneOffset::Parse("+05:30");
  ASSERT_TRUE(offset3.has_value());
  EXPECT_EQ(offset3->GetOffsetSeconds(), 5 * 3600 + 30 * 60);
  EXPECT_EQ(offset3->ToString(), "+05:30");

  // Negative offsets
  auto offset4 = TimezoneOffset::Parse("-05:00");
  ASSERT_TRUE(offset4.has_value());
  EXPECT_EQ(offset4->GetOffsetSeconds(), -5 * 3600);
  EXPECT_EQ(offset4->ToString(), "-05:00");

  auto offset5 = TimezoneOffset::Parse("-08:30");
  ASSERT_TRUE(offset5.has_value());
  EXPECT_EQ(offset5->GetOffsetSeconds(), -(8 * 3600 + 30 * 60));
  EXPECT_EQ(offset5->ToString(), "-08:30");
}

TEST(TimezoneOffsetTest, ParseInvalidOffsets) {
  // Invalid format
  EXPECT_FALSE(TimezoneOffset::Parse("").has_value());
  EXPECT_FALSE(TimezoneOffset::Parse("09:00").has_value());    // Missing sign
  EXPECT_FALSE(TimezoneOffset::Parse("+9:00").has_value());    // Single digit hour
  EXPECT_FALSE(TimezoneOffset::Parse("+09:0").has_value());    // Single digit minute
  EXPECT_FALSE(TimezoneOffset::Parse("+0900").has_value());    // Missing colon
  EXPECT_FALSE(TimezoneOffset::Parse("+ 09:00").has_value());  // Space after sign

  // Out of range
  EXPECT_FALSE(TimezoneOffset::Parse("+24:00").has_value());  // Hour >= 24
  EXPECT_FALSE(TimezoneOffset::Parse("+09:60").has_value());  // Minute >= 60
}

// ============================================================================
// DateTimeProcessor::TimeToSeconds Tests
// ============================================================================

TEST(DateTimeProcessorTest, TimeToSecondsValid) {
  DateTimeProcessor processor(TimezoneOffset::Parse("+00:00").value());

  // Normal time values
  auto result1 = processor.TimeToSeconds("00:00:00");
  ASSERT_TRUE(result1.has_value());
  EXPECT_EQ(*result1, 0);

  auto result2 = processor.TimeToSeconds("10:30:00");
  ASSERT_TRUE(result2.has_value());
  EXPECT_EQ(*result2, 10 * 3600 + 30 * 60);

  auto result3 = processor.TimeToSeconds("23:59:59");
  ASSERT_TRUE(result3.has_value());
  EXPECT_EQ(*result3, 23 * 3600 + 59 * 60 + 59);

  // Negative time values
  auto result4 = processor.TimeToSeconds("-10:30:00");
  ASSERT_TRUE(result4.has_value());
  EXPECT_EQ(*result4, -(10 * 3600 + 30 * 60));

  // Edge cases: MySQL TIME range
  auto result5 = processor.TimeToSeconds("838:59:59");
  ASSERT_TRUE(result5.has_value());
  EXPECT_EQ(*result5, 838 * 3600 + 59 * 60 + 59);  // 3020399

  auto result6 = processor.TimeToSeconds("-838:59:59");
  ASSERT_TRUE(result6.has_value());
  EXPECT_EQ(*result6, -(838 * 3600 + 59 * 60 + 59));  // -3020399
}

TEST(DateTimeProcessorTest, TimeToSecondsInvalid) {
  DateTimeProcessor processor(TimezoneOffset::Parse("+00:00").value());

  // Invalid format
  EXPECT_FALSE(processor.TimeToSeconds("").has_value());
  EXPECT_FALSE(processor.TimeToSeconds("10:30").has_value());        // Missing seconds
  EXPECT_FALSE(processor.TimeToSeconds("10:30:00:00").has_value());  // Too many parts
  EXPECT_FALSE(processor.TimeToSeconds("abc:30:00").has_value());    // Non-numeric hour
  EXPECT_FALSE(processor.TimeToSeconds("10:abc:00").has_value());    // Non-numeric minute
  EXPECT_FALSE(processor.TimeToSeconds("10:30:abc").has_value());    // Non-numeric second

  // Out of range (minute/second)
  EXPECT_FALSE(processor.TimeToSeconds("10:60:00").has_value());  // Minute >= 60
  EXPECT_FALSE(processor.TimeToSeconds("10:30:60").has_value());  // Second >= 60

  // Out of MySQL TIME range
  EXPECT_FALSE(processor.TimeToSeconds("839:00:00").has_value());   // Hour > 838
  EXPECT_FALSE(processor.TimeToSeconds("-839:00:00").has_value());  // Hour < -838
}

TEST(DateTimeProcessorTest, TimeToSecondsEdgeCases) {
  DateTimeProcessor processor(TimezoneOffset::Parse("+00:00").value());

  // Hours exactly at boundary
  auto result1 = processor.TimeToSeconds("838:00:00");
  ASSERT_TRUE(result1.has_value());
  EXPECT_EQ(*result1, 838 * 3600);

  auto result2 = processor.TimeToSeconds("-838:00:00");
  ASSERT_TRUE(result2.has_value());
  EXPECT_EQ(*result2, -838 * 3600);

  // Just beyond boundary should fail
  EXPECT_FALSE(processor.TimeToSeconds("839:00:00").has_value());
  EXPECT_FALSE(processor.TimeToSeconds("-839:00:00").has_value());
  EXPECT_FALSE(processor.TimeToSeconds("838:60:00").has_value());
  EXPECT_FALSE(processor.TimeToSeconds("838:59:60").has_value());
}

// ============================================================================
// DateTimeProcessor::DateTimeToEpoch Tests
// ============================================================================

TEST(DateTimeProcessorTest, DateTimeToEpochUTC) {
  DateTimeProcessor processor(TimezoneOffset::Parse("+00:00").value());

  // Known datetime: 2024-01-01 00:00:00 UTC = 1704067200
  auto result1 = processor.DateTimeToEpoch("2024-01-01 00:00:00");
  ASSERT_TRUE(result1.has_value());
  EXPECT_EQ(*result1, 1704067200);

  // Known datetime: 2024-11-22 10:00:00 UTC = 1732269600
  auto result2 = processor.DateTimeToEpoch("2024-11-22 10:00:00");
  ASSERT_TRUE(result2.has_value());
  EXPECT_EQ(*result2, 1732269600);
}

TEST(DateTimeProcessorTest, DateTimeToEpochWithTimezone) {
  // JST (UTC+09:00)
  DateTimeProcessor processor_jst(TimezoneOffset::Parse("+09:00").value());

  // 2024-01-01 00:00:00 JST = 2023-12-31 15:00:00 UTC = 1704034800
  auto result1 = processor_jst.DateTimeToEpoch("2024-01-01 00:00:00");
  ASSERT_TRUE(result1.has_value());
  EXPECT_EQ(*result1, 1704034800);

  // EST (UTC-05:00)
  DateTimeProcessor processor_est(TimezoneOffset::Parse("-05:00").value());

  // 2024-01-01 00:00:00 EST = 2024-01-01 05:00:00 UTC = 1704085200
  auto result2 = processor_est.DateTimeToEpoch("2024-01-01 00:00:00");
  ASSERT_TRUE(result2.has_value());
  EXPECT_EQ(*result2, 1704085200);
}

TEST(DateTimeProcessorTest, DateTimeToEpochInvalid) {
  DateTimeProcessor processor(TimezoneOffset::Parse("+00:00").value());

  // Invalid format
  EXPECT_FALSE(processor.DateTimeToEpoch("").has_value());
  EXPECT_FALSE(processor.DateTimeToEpoch("2024-01-01").has_value());           // Missing time (less than 19 chars)
  EXPECT_FALSE(processor.DateTimeToEpoch("2024/01/01 00:00:00").has_value());  // Wrong separator

  // Invalid date/time (basic range checks only)
  EXPECT_FALSE(processor.DateTimeToEpoch("2024-13-01 00:00:00").has_value());  // Month > 12
  EXPECT_FALSE(processor.DateTimeToEpoch("2024-01-32 00:00:00").has_value());  // Day > 31
  EXPECT_FALSE(processor.DateTimeToEpoch("2024-01-01 24:00:00").has_value());  // Hour >= 24
  EXPECT_FALSE(processor.DateTimeToEpoch("2024-01-01 10:60:00").has_value());  // Minute >= 60
  EXPECT_FALSE(processor.DateTimeToEpoch("2024-01-01 10:30:60").has_value());  // Second >= 60
}

TEST(DateTimeProcessorTest, DateTimeToEpochInvalidCalendarDates) {
  DateTimeProcessor processor(TimezoneOffset::Parse("+00:00").value());

  // Invalid calendar dates (day out of range for given month)
  EXPECT_FALSE(processor.DateTimeToEpoch("2024-02-30 00:00:00").has_value());  // Feb 30 doesn't exist
  EXPECT_FALSE(processor.DateTimeToEpoch("2024-02-31 00:00:00").has_value());  // Feb 31 doesn't exist
  EXPECT_FALSE(processor.DateTimeToEpoch("2023-02-29 00:00:00").has_value());  // Not a leap year (2023)
  EXPECT_FALSE(processor.DateTimeToEpoch("2024-04-31 00:00:00").has_value());  // April has 30 days
  EXPECT_FALSE(processor.DateTimeToEpoch("2024-06-31 00:00:00").has_value());  // June has 30 days
  EXPECT_FALSE(processor.DateTimeToEpoch("2024-09-31 00:00:00").has_value());  // September has 30 days
  EXPECT_FALSE(processor.DateTimeToEpoch("2024-11-31 00:00:00").has_value());  // November has 30 days

  // Valid edge cases (leap year)
  EXPECT_TRUE(processor.DateTimeToEpoch("2024-02-29 00:00:00").has_value());  // 2024 is a leap year
  EXPECT_TRUE(processor.DateTimeToEpoch("2000-02-29 00:00:00").has_value());  // 2000 is a leap year (div by 400)

  // Non-leap years divisible by 100
  EXPECT_FALSE(processor.DateTimeToEpoch("1900-02-29 00:00:00").has_value());  // 1900 is not a leap year
  EXPECT_FALSE(processor.DateTimeToEpoch("2100-02-29 00:00:00").has_value());  // 2100 is not a leap year

  // Valid dates at month boundaries
  EXPECT_TRUE(processor.DateTimeToEpoch("2024-01-31 00:00:00").has_value());  // Jan 31 is valid
  EXPECT_TRUE(processor.DateTimeToEpoch("2024-03-31 00:00:00").has_value());  // Mar 31 is valid
  EXPECT_TRUE(processor.DateTimeToEpoch("2024-04-30 00:00:00").has_value());  // Apr 30 is valid
  EXPECT_TRUE(processor.DateTimeToEpoch("2024-02-28 00:00:00").has_value());  // Feb 28 always valid
}

// ============================================================================
// DateTimeProcessor::ParseDateTimeValue Tests
// ============================================================================

TEST(DateTimeProcessorTest, ParseDateTimeValueNumeric) {
  DateTimeProcessor processor(TimezoneOffset::Parse("+00:00").value());

  // Pure numeric values are treated as epoch seconds
  auto result1 = processor.ParseDateTimeValue("1704067200");
  ASSERT_TRUE(result1.has_value());
  EXPECT_EQ(*result1, 1704067200);

  auto result2 = processor.ParseDateTimeValue("0");
  ASSERT_TRUE(result2.has_value());
  EXPECT_EQ(*result2, 0);
}

TEST(DateTimeProcessorTest, ParseDateTimeValueISO8601) {
  DateTimeProcessor processor(TimezoneOffset::Parse("+00:00").value());

  // ISO8601 format should be parsed as datetime
  auto result1 = processor.ParseDateTimeValue("2024-01-01 00:00:00");
  ASSERT_TRUE(result1.has_value());
  EXPECT_EQ(*result1, 1704067200);

  // Date-only format is not supported (requires full datetime with time component)
  auto result2 = processor.ParseDateTimeValue("2024-01-01");
  EXPECT_FALSE(result2.has_value());
}

TEST(DateTimeProcessorTest, ParseDateTimeValueWithTimezone) {
  DateTimeProcessor processor_jst(TimezoneOffset::Parse("+09:00").value());

  // Numeric values are not affected by timezone (already UTC epoch)
  auto result1 = processor_jst.ParseDateTimeValue("1704067200");
  ASSERT_TRUE(result1.has_value());
  EXPECT_EQ(*result1, 1704067200);

  // ISO8601 format is interpreted with timezone
  auto result2 = processor_jst.ParseDateTimeValue("2024-01-01 00:00:00");
  ASSERT_TRUE(result2.has_value());
  EXPECT_EQ(*result2, 1704034800);  // JST - 9 hours
}

// ============================================================================
// Integration Tests
// ============================================================================

TEST(DateTimeProcessorTest, RoundTripConversion) {
  DateTimeProcessor processor(TimezoneOffset::Parse("+00:00").value());

  // Test that parsing and formatting are consistent
  const std::string datetime_str = "2024-11-22 10:00:00";
  auto epoch = processor.DateTimeToEpoch(datetime_str);
  ASSERT_TRUE(epoch.has_value());

  // Re-parsing the epoch as string should give the same value
  auto epoch_str = std::to_string(*epoch);
  auto reparsed = processor.ParseDateTimeValue(epoch_str);
  ASSERT_TRUE(reparsed.has_value());
  EXPECT_EQ(*reparsed, *epoch);
}

TEST(DateTimeProcessorTest, CompareTimeValues) {
  DateTimeProcessor processor(TimezoneOffset::Parse("+00:00").value());

  auto time1 = processor.TimeToSeconds("10:30:00");
  auto time2 = processor.TimeToSeconds("15:45:30");
  auto time3 = processor.TimeToSeconds("-05:00:00");

  ASSERT_TRUE(time1.has_value());
  ASSERT_TRUE(time2.has_value());
  ASSERT_TRUE(time3.has_value());

  EXPECT_LT(*time1, *time2);  // 10:30:00 < 15:45:30
  EXPECT_GT(*time1, *time3);  // 10:30:00 > -05:00:00
  EXPECT_LT(*time3, 0);       // Negative time
}

}  // namespace mygramdb::utils
