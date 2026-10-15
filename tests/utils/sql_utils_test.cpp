/**
 * @file sql_utils_test.cpp
 * @brief Unit tests for SQL parsing utility functions
 */

#include "utils/sql_utils.h"

#include <gtest/gtest.h>

#include <clocale>
#include <cstdint>
#include <limits>
#include <string>

using namespace mygramdb::utils;

// --- Identifier quoting tests ---

TEST(QuoteSQLIdentifierTest, QuotesOrdinaryIdentifier) {
  auto result = QuoteSQLIdentifier("articles");

  ASSERT_TRUE(result);
  EXPECT_EQ(*result, "`articles`");
}

TEST(QuoteSQLIdentifierTest, DoublesEmbeddedBackticks) {
  auto result = QuoteSQLIdentifier("column` FROM mysql.user; --");

  ASSERT_TRUE(result);
  EXPECT_EQ(*result, "`column`` FROM mysql.user; --`");
}

TEST(QuoteSQLIdentifierTest, AllowsNonNulQuotedIdentifierCharacters) {
  auto result = QuoteSQLIdentifier("résumé column-$");

  ASSERT_TRUE(result);
  EXPECT_EQ(*result, "`résumé column-$`");
}

TEST(QuoteSQLIdentifierTest, RejectsEmptyIdentifier) {
  auto result = QuoteSQLIdentifier("");

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), ErrorCode::kInvalidArgument);
}

TEST(QuoteSQLIdentifierTest, RejectsEmbeddedNul) {
  const std::string identifier("safe\0injected", 13);
  auto result = QuoteSQLIdentifier(identifier);

  ASSERT_FALSE(result);
  EXPECT_EQ(result.error().code(), ErrorCode::kInvalidArgument);
}

TEST(QuoteQualifiedSQLIdentifierTest, QuotesBothComponents) {
  auto result = QuoteQualifiedSQLIdentifier("archive`db", "article`table");

  ASSERT_TRUE(result);
  EXPECT_EQ(*result, "`archive``db`.`article``table`");
}

TEST(QuoteQualifiedSQLIdentifierTest, EmptyDatabaseFallsBackToBareIdentifier) {
  auto result = QuoteQualifiedSQLIdentifier("", "articles");

  ASSERT_TRUE(result);
  EXPECT_EQ(*result, "`articles`");
}

TEST(QuoteQualifiedSQLIdentifierTest, RejectsInvalidComponent) {
  const std::string database("db\0suffix", 9);

  EXPECT_FALSE(QuoteQualifiedSQLIdentifier(database, "articles"));
  EXPECT_FALSE(QuoteQualifiedSQLIdentifier("database", ""));
}

TEST(EncodeMySQLStringLiteralTest, EncodesEmptyAndAsciiValues) {
  EXPECT_EQ(EncodeMySQLStringLiteral(""), "_utf8mb4 X''");
  EXPECT_EQ(EncodeMySQLStringLiteral("O'Reilly"), "_utf8mb4 X'4F275265696C6C79'");
}

TEST(EncodeMySQLStringLiteralTest, IsIndependentOfBackslashEscapingMode) {
  const std::string value = "line\\n'quoted'\nnext";
  const std::string encoded = EncodeMySQLStringLiteral(value);

  EXPECT_EQ(encoded, "_utf8mb4 X'6C696E655C6E2771756F746564270A6E657874'");
  EXPECT_EQ(encoded.find('\\'), std::string::npos);
  EXPECT_EQ(encoded.find("quoted"), std::string::npos);
}

TEST(EncodeMySQLStringLiteralTest, EncodesUtf8BytesWithoutLoss) {
  EXPECT_EQ(EncodeMySQLStringLiteral("日本語"), "_utf8mb4 X'E697A5E69CACE8AA9E'");
}

TEST(FormatMySQLDateTimeLiteralTest, EnforcesMySQLYearBoundsBeforeCalendarConversion) {
  constexpr int64_t kFirstSupportedLocalEpoch = -30610224000;
  constexpr int64_t kLastSupportedLocalEpoch = 253402300799;

  EXPECT_EQ(FormatMySQLDateTimeLiteral(kFirstSupportedLocalEpoch, 0), "1000-01-01 00:00:00");
  EXPECT_EQ(FormatMySQLDateTimeLiteral(kLastSupportedLocalEpoch, 0), "9999-12-31 23:59:59");
  EXPECT_FALSE(FormatMySQLDateTimeLiteral(kFirstSupportedLocalEpoch - 1, 0).has_value());
  EXPECT_FALSE(FormatMySQLDateTimeLiteral(kLastSupportedLocalEpoch + 1, 0).has_value());

  EXPECT_FALSE(FormatMySQLDateTimeLiteral(std::numeric_limits<int64_t>::min(), 0).has_value());
  EXPECT_FALSE(FormatMySQLDateTimeLiteral(std::numeric_limits<int64_t>::max(), 0).has_value());
  EXPECT_FALSE(FormatMySQLDateTimeLiteral(std::numeric_limits<int64_t>::max(), 9 * 60 * 60).has_value());
  EXPECT_FALSE(FormatMySQLDateTimeLiteral(std::numeric_limits<int64_t>::min(), INT32_MIN).has_value());
  EXPECT_FALSE(FormatMySQLDateTimeLiteral(std::numeric_limits<int64_t>::max(), INT32_MAX).has_value());
  EXPECT_EQ(FormatMySQLDateTimeLiteral(0, INT32_MIN), "1901-12-13 20:45:52");
  EXPECT_EQ(FormatMySQLDateTimeLiteral(0, INT32_MAX), "2038-01-19 03:14:07");
}

TEST(FormatMySQLDateTimeLiteralTest, AppliesOffsetAtTheSupportedYearBoundaries) {
  constexpr int64_t kFirstSupportedLocalEpoch = -30610224000;
  constexpr int64_t kLastSupportedLocalEpoch = 253402300799;
  constexpr int32_t kOneHour = 3600;

  EXPECT_EQ(FormatMySQLDateTimeLiteral(kFirstSupportedLocalEpoch - kOneHour, kOneHour), "1000-01-01 00:00:00");
  EXPECT_EQ(FormatMySQLDateTimeLiteral(kFirstSupportedLocalEpoch + kOneHour, -kOneHour), "1000-01-01 00:00:00");
  EXPECT_EQ(FormatMySQLDateTimeLiteral(kLastSupportedLocalEpoch - kOneHour, kOneHour), "9999-12-31 23:59:59");
  EXPECT_EQ(FormatMySQLDateTimeLiteral(kLastSupportedLocalEpoch + kOneHour, -kOneHour), "9999-12-31 23:59:59");

  EXPECT_FALSE(FormatMySQLDateTimeLiteral(kFirstSupportedLocalEpoch - kOneHour - 1, kOneHour).has_value());
  EXPECT_FALSE(FormatMySQLDateTimeLiteral(kLastSupportedLocalEpoch + kOneHour + 1, -kOneHour).has_value());
}

TEST(FormatMySQLDoubleLiteralTest, IsIndependentOfNumericLocale) {
  const char* current_locale = std::setlocale(LC_NUMERIC, nullptr);
  ASSERT_NE(current_locale, nullptr);
  struct NumericLocaleGuard {
    explicit NumericLocaleGuard(const char* locale) : original_locale(locale) {}
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

  EXPECT_EQ(FormatMySQLDoubleLiteral(1.1), "1.10000000000000009e+00");
}
