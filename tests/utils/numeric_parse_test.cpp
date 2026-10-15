/**
 * @file numeric_parse_test.cpp
 * @brief Unit tests for ParseNumeric utility
 */

#include "utils/numeric_parse.h"

#include <gtest/gtest.h>

#include <clocale>
#include <cstdint>
#include <limits>
#include <string>

using namespace mygramdb::utils;

// --- Integer types ---

TEST(ParseNumericTest, Int8Valid) {
  auto result = ParseNumeric<int8_t>("42");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 42);
}

TEST(ParseNumericTest, Int8Negative) {
  auto result = ParseNumeric<int8_t>("-128");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, -128);
}

TEST(ParseNumericTest, Int8Overflow) {
  auto result = ParseNumeric<int8_t>("200");
  EXPECT_FALSE(result.has_value());
}

TEST(ParseNumericTest, Uint8Valid) {
  auto result = ParseNumeric<uint8_t>("255");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 255);
}

TEST(ParseNumericTest, Uint8Negative) {
  auto result = ParseNumeric<uint8_t>("-1");
  EXPECT_FALSE(result.has_value());
}

TEST(ParseNumericTest, Int16Valid) {
  auto result = ParseNumeric<int16_t>("32767");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 32767);
}

TEST(ParseNumericTest, Int32Valid) {
  auto result = ParseNumeric<int32_t>("2147483647");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 2147483647);
}

TEST(ParseNumericTest, Uint32Valid) {
  auto result = ParseNumeric<uint32_t>("4294967295");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 4294967295U);
}

TEST(ParseNumericTest, Int64Valid) {
  auto result = ParseNumeric<int64_t>("9223372036854775807");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, std::numeric_limits<int64_t>::max());
}

TEST(ParseNumericTest, Uint64Valid) {
  auto result = ParseNumeric<uint64_t>("18446744073709551615");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, std::numeric_limits<uint64_t>::max());
}

// --- Failure cases ---

TEST(ParseNumericTest, EmptyString) {
  EXPECT_FALSE(ParseNumeric<int32_t>("").has_value());
}

TEST(ParseNumericTest, NonNumeric) {
  EXPECT_FALSE(ParseNumeric<int32_t>("abc").has_value());
}

TEST(ParseNumericTest, TrailingChars) {
  EXPECT_FALSE(ParseNumeric<int32_t>("42abc").has_value());
}

TEST(ParseNumericTest, LeadingWhitespace) {
  // from_chars does not skip whitespace
  EXPECT_FALSE(ParseNumeric<int32_t>(" 42").has_value());
}

// --- Double ---

TEST(ParseNumericTest, DoubleValid) {
  auto result = ParseNumeric<double>("3.14");
  ASSERT_TRUE(result.has_value());
  EXPECT_DOUBLE_EQ(*result, 3.14);
}

TEST(ParseNumericTest, DoubleNegative) {
  auto result = ParseNumeric<double>("-1.5");
  ASSERT_TRUE(result.has_value());
  EXPECT_DOUBLE_EQ(*result, -1.5);
}

TEST(ParseNumericTest, DoubleInteger) {
  auto result = ParseNumeric<double>("42");
  ASSERT_TRUE(result.has_value());
  EXPECT_DOUBLE_EQ(*result, 42.0);
}

TEST(ParseNumericTest, DoubleScientific) {
  auto result = ParseNumeric<double>("1.5e2");
  ASSERT_TRUE(result.has_value());
  EXPECT_DOUBLE_EQ(*result, 150.0);
}

TEST(ParseNumericTest, DoubleInvalid) {
  EXPECT_FALSE(ParseNumeric<double>("not_a_number").has_value());
}

TEST(ParseNumericTest, DoubleTrailingChars) {
  EXPECT_FALSE(ParseNumeric<double>("3.14abc").has_value());
}

TEST(ParseNumericTest, DoubleEmpty) {
  EXPECT_FALSE(ParseNumeric<double>("").has_value());
}

TEST(ParseNumericTest, DoubleRejectsWhitespace) {
  EXPECT_FALSE(ParseNumeric<double>(" 3.14").has_value());
  EXPECT_FALSE(ParseNumeric<double>("3.14 ").has_value());
  EXPECT_FALSE(ParseNumeric<double>("3. 14").has_value());
}

TEST(ParseNumericTest, DoubleRejectsLeadingPlus) {
  EXPECT_FALSE(ParseNumeric<double>("+3.14").has_value());
}

TEST(ParseNumericTest, DoubleRejectsNonFiniteValues) {
  EXPECT_FALSE(ParseNumeric<double>("inf").has_value());
  EXPECT_FALSE(ParseNumeric<double>("-inf").has_value());
  EXPECT_FALSE(ParseNumeric<double>("nan").has_value());
}

TEST(ParseNumericTest, FloatValidAndRejectsTargetTypeOverflow) {
  auto result = ParseNumeric<float>("1.25e1");
  ASSERT_TRUE(result.has_value());
  EXPECT_FLOAT_EQ(*result, 12.5F);

  EXPECT_FALSE(ParseNumeric<float>("3.5e38").has_value());
}

TEST(ParseNumericTest, LongDoublePreservesTheSupportedDecimalParsingContract) {
  auto result = ParseNumeric<long double>("17.25");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(*result, 17.25L);

  EXPECT_FALSE(ParseNumeric<long double>("17.25tail").has_value());
}

TEST(ParseNumericTest, DoubleRejectsOverflow) {
  EXPECT_FALSE(ParseNumeric<double>("1e309").has_value());
}

TEST(ParseNumericTest, FloatingPointUsesDecimalGeneralFormat) {
  EXPECT_FALSE(ParseNumeric<double>("0x1p2").has_value());
}

TEST(ParseNumericTest, FloatingPointParsingIsIndependentOfNumericLocale) {
  const char* current_locale = std::setlocale(LC_NUMERIC, nullptr);
  ASSERT_NE(current_locale, nullptr);
  struct NumericLocaleGuard {
    explicit NumericLocaleGuard(const std::string& locale) : original_locale(locale) {}
    ~NumericLocaleGuard() { std::setlocale(LC_NUMERIC, original_locale.c_str()); }

    std::string original_locale;
  } locale_guard(current_locale);

  bool selected_locale = false;
  for (const char* candidate : {"de_DE.UTF-8", "de_DE.utf8", "de_DE"}) {
    if (std::setlocale(LC_NUMERIC, candidate) != nullptr) {
      selected_locale = true;
      break;
    }
  }
  if (!selected_locale) {
    GTEST_SKIP() << "No German numeric locale is installed";
  }

  auto result = ParseNumeric<double>("3.14");
  EXPECT_TRUE(result.has_value());
  if (result) {
    EXPECT_DOUBLE_EQ(*result, 3.14);
  }
}
