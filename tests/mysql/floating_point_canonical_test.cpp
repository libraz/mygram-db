/**
 * @file floating_point_canonical_test.cpp
 * @brief FLOAT and DOUBLE canonical text must not depend on the process locale
 */

#include <gtest/gtest.h>

#ifdef USE_MYSQL

#include <locale>
#include <string>

#include "mysql/value_canonicalizer.h"

using namespace mygramdb::mysql;

namespace {

struct CommaDecimal : std::numpunct<char> {
  char do_decimal_point() const override { return ','; }
};

/// Installs a comma-decimal global locale for the test body and restores the previous one.
class ScopedCommaDecimalLocale {
 public:
  ScopedCommaDecimalLocale() : previous_(std::locale::global(std::locale(std::locale::classic(), new CommaDecimal))) {}
  ~ScopedCommaDecimalLocale() { std::locale::global(previous_); }
  ScopedCommaDecimalLocale(const ScopedCommaDecimalLocale&) = delete;
  ScopedCommaDecimalLocale& operator=(const ScopedCommaDecimalLocale&) = delete;

 private:
  std::locale previous_;
};

}  // namespace

TEST(FloatingPointCanonicalTest, SnapshotAndBinlogTextAreIdenticalUnderACommaDecimalLocale) {
  const std::string expected_double = FormatFloatingPoint(0.1);
  const std::string expected_float = FormatFloatingPoint(0.1F);

  ScopedCommaDecimalLocale comma_locale;

  EXPECT_EQ(FormatFloatingPoint(0.1), expected_double);
  EXPECT_EQ(FormatFloatingPoint(0.1F), expected_float);
  EXPECT_EQ(CanonicalizeColumnValue("0.1", CanonicalValueKind::kDouble), expected_double);
  EXPECT_EQ(CanonicalizeColumnValue("0.1", CanonicalValueKind::kFloat), FormatFloatingPoint(static_cast<float>(0.1)));
}

#endif  // USE_MYSQL
