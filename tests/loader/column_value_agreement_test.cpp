/**
 * @file column_value_agreement_test.cpp
 * @brief The initial snapshot and the binlog row image must decode to one string
 *
 * A configured column is read twice: as text from the initial-load result set
 * and as a binary field from a row event. The primary key produced by the two
 * paths is what identifies a document, so a type whose two renderings differ by
 * one character turns one MySQL row into two documents, and an UPDATE reaches
 * neither. Text sources have the same requirement one level down: the indexed
 * text has to be the text replication would produce for the same row.
 *
 * Each case below carries the value twice - as the bytes MySQL's text protocol
 * returns and as the bytes a row event carries - and asserts that the two
 * ingest paths reduce them to the same string. The text-protocol renderings are
 * facts only a server can settle and were measured on MySQL 8.4.11.
 */

#ifdef USE_MYSQL

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <set>
#include <string>
#include <vector>

#include "mysql/column_type_support.h"
#include "mysql/rows_parser_internal.h"
#include "mysql/table_metadata.h"
#include "mysql/value_canonicalizer.h"

namespace mygramdb::mysql {
namespace {

using internal::DecodeFieldValue;

// ===========================================================================
// Row image encoders
//
// Each mirrors the layout its type is written in, so a case is described by the
// value rather than by a byte string nobody can check.
// ===========================================================================

using Bytes = std::vector<uint8_t>;

Bytes LittleEndian(uint64_t value, size_t width) {
  Bytes bytes(width);
  for (size_t index = 0; index < width; ++index) {
    bytes[index] = static_cast<uint8_t>((value >> (index * 8)) & 0xFF);
  }
  return bytes;
}

Bytes BigEndian(uint64_t value, size_t width) {
  Bytes bytes(width);
  for (size_t index = 0; index < width; ++index) {
    bytes[width - 1 - index] = static_cast<uint8_t>((value >> (index * 8)) & 0xFF);
  }
  return bytes;
}

void Append(Bytes& target, const Bytes& tail) {
  target.insert(target.end(), tail.begin(), tail.end());
}

/// Length-prefixed payload, as VARCHAR, CHAR, TEXT and BLOB all carry it.
Bytes LengthPrefixed(const std::string& value, size_t prefix_width) {
  Bytes bytes = LittleEndian(value.size(), prefix_width);
  bytes.insert(bytes.end(), value.begin(), value.end());
  return bytes;
}

Bytes EncodeFloat(float value) {
  Bytes bytes(sizeof(float));
  std::memcpy(bytes.data(), &value, sizeof(float));
  return bytes;
}

Bytes EncodeDouble(double value) {
  Bytes bytes(sizeof(double));
  std::memcpy(bytes.data(), &value, sizeof(double));
  return bytes;
}

/// Packed 3-byte DATE: year in the high 14 bits, then month and day.
Bytes EncodeDate(unsigned year, unsigned month, unsigned day) {
  return LittleEndian((static_cast<uint64_t>(year) << 9) | (month << 5) | day, 3);
}

/// Scale microseconds down to the digits a given fractional precision stores.
uint64_t FractionalDigits(uint32_t microseconds, uint8_t precision) {
  static constexpr uint32_t kMultipliers[] = {0, 10000, 10000, 100, 100, 1, 1};
  return precision == 0 ? 0 : microseconds / kMultipliers[precision];
}

size_t FractionalWidth(uint8_t precision) {
  return (precision + 1) / 2;
}

Bytes EncodeDatetime2(unsigned year, unsigned month, unsigned day, unsigned hour, unsigned minute, unsigned second,
                      uint32_t microseconds, uint8_t precision) {
  const uint64_t year_month = (static_cast<uint64_t>(year) * 13) + month;
  const uint64_t packed = (((year_month << 5) | day) << 17) | (hour << 12) | (minute << 6) | second;
  constexpr uint64_t kDatetimeIntOfs = 0x8000000000ULL;
  Bytes bytes = BigEndian(packed + kDatetimeIntOfs, 5);
  if (precision > 0) {
    Append(bytes, BigEndian(FractionalDigits(microseconds, precision), FractionalWidth(precision)));
  }
  return bytes;
}

Bytes EncodeTimestamp2(uint32_t epoch_seconds, uint32_t microseconds, uint8_t precision) {
  Bytes bytes = BigEndian(epoch_seconds, 4);
  if (precision > 0) {
    Append(bytes, BigEndian(FractionalDigits(microseconds, precision), FractionalWidth(precision)));
  }
  return bytes;
}

/**
 * @brief Pack a TIME value the way the server writes it.
 *
 * The whole seconds and the fractional part are stored as one signed number, so
 * a negative time with a fraction borrows from the seconds: the stored seconds
 * are one lower and the stored fraction is its complement. Precisions of five
 * and six digits drop the split and store the number outright.
 */
Bytes EncodeTime2(bool negative, unsigned hour, unsigned minute, unsigned second, uint32_t microseconds,
                  uint8_t precision) {
  constexpr int64_t kPackedFractionBase = int64_t{1} << 24;
  const int64_t clock = (static_cast<int64_t>(hour) << 12) | (minute << 6) | second;
  int64_t packed = (clock * kPackedFractionBase) + microseconds;
  if (negative) {
    packed = -packed;
  }

  if (precision >= 5) {
    constexpr int64_t kTimefOfs = 0x800000000000LL;
    return BigEndian(static_cast<uint64_t>(packed + kTimefOfs), 6);
  }

  constexpr int64_t kTimefIntOfs = 0x800000;
  int64_t whole = negative ? -clock : clock;
  int64_t fractional = static_cast<int64_t>(FractionalDigits(microseconds, precision));
  if (negative && fractional != 0) {
    whole -= 1;
    fractional = (int64_t{1} << (8 * static_cast<int64_t>(FractionalWidth(precision)))) - fractional;
  }
  Bytes bytes = BigEndian(static_cast<uint64_t>(whole + kTimefIntOfs) & 0xFFFFFF, 3);
  if (precision > 0) {
    Append(bytes, BigEndian(static_cast<uint64_t>(fractional), FractionalWidth(precision)));
  }
  return bytes;
}

/**
 * @brief Pack a DECIMAL whose integral and fractional halves each fit one group.
 *
 * Digits are stored in groups of nine, most significant group first, in the
 * fewest bytes that hold the group. The sign lives in the top bit of the first
 * byte: a negative value is the positive encoding with every bit inverted.
 */
Bytes EncodeDecimal(bool negative, uint32_t integral, uint32_t fractional, uint8_t precision, uint8_t scale) {
  static constexpr size_t kDigitsToBytes[] = {0, 1, 1, 2, 2, 3, 3, 4, 4, 4};
  const uint8_t integral_digits = precision - scale;
  Bytes bytes = BigEndian(integral, kDigitsToBytes[integral_digits]);
  if (scale > 0) {
    Append(bytes, BigEndian(fractional, kDigitsToBytes[scale]));
  }
  for (auto& byte : bytes) {
    byte = negative ? static_cast<uint8_t>(~byte) : byte;
  }
  bytes[0] = static_cast<uint8_t>(bytes[0] ^ 0x80);
  return bytes;
}

// ===========================================================================
// Cases
// ===========================================================================

/// One stored value, as each ingest path receives it.
struct AgreementCase {
  std::string label;
  ColumnType snapshot_type;   ///< Code the client library reports for the column
  std::string snapshot_text;  ///< Bytes the result set returns, measured on the server
  ColumnType binlog_type;     ///< Code a row event carries for the same column
  uint16_t metadata;
  Bytes row_image;
  bool is_unsigned = false;
  std::vector<std::string> labels;
};

void AppendIntegerCases(std::vector<AgreementCase>& cases) {
  struct Width {
    const char* label;
    ColumnType type;
    size_t bytes;
    int64_t low;
    int64_t high;
    uint64_t unsigned_high;
  };
  const std::vector<Width> widths = {
      {"tinyint", ColumnType::TINY, 1, -128, 127, 255},
      {"smallint", ColumnType::SHORT, 2, -32768, 32767, 65535},
      {"mediumint", ColumnType::INT24, 3, -8388608, 8388607, 16777215},
      {"int", ColumnType::LONG, 4, -2147483648LL, 2147483647LL, 4294967295ULL},
      {"bigint", ColumnType::LONGLONG, 8, INT64_MIN, INT64_MAX, UINT64_MAX},
  };

  for (const auto& width : widths) {
    for (const int64_t value : {width.low, int64_t{-1}, int64_t{0}, int64_t{1}, width.high}) {
      cases.push_back({std::string(width.label) + " " + std::to_string(value),
                       width.type,
                       std::to_string(value),
                       width.type,
                       0,
                       LittleEndian(static_cast<uint64_t>(value), width.bytes),
                       false,
                       {}});
    }
    for (const uint64_t value : {uint64_t{0}, uint64_t{1}, width.unsigned_high}) {
      cases.push_back({std::string(width.label) + " unsigned " + std::to_string(value),
                       width.type,
                       std::to_string(value),
                       width.type,
                       0,
                       LittleEndian(value, width.bytes),
                       true,
                       {}});
    }
  }
}

/**
 * Measured: a DOUBLE column returns the shortest decimal that reads back as
 * the stored value, in whichever notation fits.
 */
struct DoubleRendering {
  double value;
  const char* rendered;
};

constexpr DoubleRendering kDoubleRenderings[] = {
    {0.0, "0"},
    {-0.0, "-0"},
    {0.1, "0.1"},
    {-1.5, "-1.5"},
    {0.001, "0.001"},
    {0.1 + 0.2, "0.30000000000000004"},
    {1.0 / 3.0, "0.3333333333333333"},
    {1.23e10, "12300000000"},
    {5e-300, "5e-300"},
    {1.7976931348623157e308, "1.7976931348623157e308"},
};

void AppendApproximateNumericCases(std::vector<AgreementCase>& cases) {
  for (const auto& rendering : kDoubleRenderings) {
    cases.push_back({std::string("double ") + rendering.rendered,
                     ColumnType::DOUBLE,
                     rendering.rendered,
                     ColumnType::DOUBLE,
                     0,
                     EncodeDouble(rendering.value),
                     false,
                     {}});
  }
}

void AppendDecimalCases(std::vector<AgreementCase>& cases) {
  // Measured: a DECIMAL column returns every declared fractional digit, and
  // drops the leading zeros an INSERT wrote.
  cases.push_back({"decimal 12.30",
                   ColumnType::NEWDECIMAL,
                   "12.30",
                   ColumnType::NEWDECIMAL,
                   (10 << 8) | 2,
                   EncodeDecimal(false, 12, 30, 10, 2),
                   false,
                   {}});
  cases.push_back({"decimal -12.30",
                   ColumnType::NEWDECIMAL,
                   "-12.30",
                   ColumnType::NEWDECIMAL,
                   (10 << 8) | 2,
                   EncodeDecimal(true, 12, 30, 10, 2),
                   false,
                   {}});
  cases.push_back({"decimal 0.00",
                   ColumnType::NEWDECIMAL,
                   "0.00",
                   ColumnType::NEWDECIMAL,
                   (10 << 8) | 2,
                   EncodeDecimal(false, 0, 0, 10, 2),
                   false,
                   {}});
  cases.push_back({"decimal -0.0500",
                   ColumnType::NEWDECIMAL,
                   "-0.0500",
                   ColumnType::NEWDECIMAL,
                   (10 << 8) | 4,
                   EncodeDecimal(true, 0, 500, 10, 4),
                   false,
                   {}});
  cases.push_back({"decimal 0",
                   ColumnType::NEWDECIMAL,
                   "0",
                   ColumnType::NEWDECIMAL,
                   (6 << 8) | 0,
                   EncodeDecimal(false, 0, 0, 6, 0),
                   false,
                   {}});
}

void AppendTemporalCases(std::vector<AgreementCase>& cases) {
  // Measured: DATE returns the calendar date, including the all-zero date a
  // permissive sql_mode allows.
  cases.push_back({"date", ColumnType::DATE, "2024-01-01", ColumnType::DATE, 0, EncodeDate(2024, 1, 1), false, {}});
  cases.push_back({"zero date", ColumnType::DATE, "0000-00-00", ColumnType::DATE, 0, EncodeDate(0, 0, 0), false, {}});

  // Measured: DATETIME(n) returns exactly n fractional digits, and none at all
  // for DATETIME.
  cases.push_back({"datetime",
                   ColumnType::DATETIME,
                   "2024-01-01 12:00:00",
                   ColumnType::DATETIME2,
                   0,
                   EncodeDatetime2(2024, 1, 1, 12, 0, 0, 0, 0),
                   false,
                   {}});
  cases.push_back({"datetime(3)",
                   ColumnType::DATETIME,
                   "2024-01-01 12:00:00.500",
                   ColumnType::DATETIME2,
                   3,
                   EncodeDatetime2(2024, 1, 1, 12, 0, 0, 500000, 3),
                   false,
                   {}});
  cases.push_back({"datetime(6)",
                   ColumnType::DATETIME,
                   "2024-01-01 00:00:00.000001",
                   ColumnType::DATETIME2,
                   6,
                   EncodeDatetime2(2024, 1, 1, 0, 0, 0, 1, 6),
                   false,
                   {}});

  // Measured with the session time zone pinned to UTC, which is what the load
  // connection sets: TIMESTAMP returns the UTC wall clock of the stored epoch.
  cases.push_back({"timestamp",
                   ColumnType::TIMESTAMP,
                   "2024-01-01 12:00:00",
                   ColumnType::TIMESTAMP2,
                   0,
                   EncodeTimestamp2(1704110400, 0, 0),
                   false,
                   {}});
  cases.push_back({"timestamp(3)",
                   ColumnType::TIMESTAMP,
                   "2024-01-01 00:00:00.500",
                   ColumnType::TIMESTAMP2,
                   3,
                   EncodeTimestamp2(1704067200, 500000, 3),
                   false,
                   {}});

  // Measured: TIME(n) returns n fractional digits, keeps the sign, and lets the
  // hour run past a day.
  cases.push_back(
      {"time", ColumnType::TIME, "12:00:00", ColumnType::TIME2, 0, EncodeTime2(false, 12, 0, 0, 0, 0), false, {}});
  cases.push_back({"time(1) zero",
                   ColumnType::TIME,
                   "00:00:00.0",
                   ColumnType::TIME2,
                   1,
                   EncodeTime2(false, 0, 0, 0, 0, 1),
                   false,
                   {}});
  cases.push_back({"time(3)",
                   ColumnType::TIME,
                   "12:00:00.500",
                   ColumnType::TIME2,
                   3,
                   EncodeTime2(false, 12, 0, 0, 500000, 3),
                   false,
                   {}});
  cases.push_back({"negative time(3)",
                   ColumnType::TIME,
                   "-10:20:30.500",
                   ColumnType::TIME2,
                   3,
                   EncodeTime2(true, 10, 20, 30, 500000, 3),
                   false,
                   {}});
  cases.push_back({"negative time",
                   ColumnType::TIME,
                   "-10:20:30",
                   ColumnType::TIME2,
                   0,
                   EncodeTime2(true, 10, 20, 30, 0, 0),
                   false,
                   {}});
  cases.push_back({"time(6) past a day",
                   ColumnType::TIME,
                   "100:00:00.000001",
                   ColumnType::TIME2,
                   6,
                   EncodeTime2(false, 100, 0, 0, 1, 6),
                   false,
                   {}});
  cases.push_back({"negative time(6)",
                   ColumnType::TIME,
                   "-838:59:59.999999",
                   ColumnType::TIME2,
                   6,
                   EncodeTime2(true, 838, 59, 59, 999999, 6),
                   false,
                   {}});

  // Measured: YEAR returns four digits, and the zero year as four zeros.
  cases.push_back({"year", ColumnType::YEAR, "2024", ColumnType::YEAR, 0, {0x7C}, false, {}});
  cases.push_back({"zero year", ColumnType::YEAR, "0000", ColumnType::YEAR, 0, {0x00}, false, {}});
}

void AppendBitCases(std::vector<AgreementCase>& cases) {
  // Measured: a BIT column returns its raw storage bytes, most significant
  // first, and not a printed number.
  cases.push_back(
      {"bit(1) set", ColumnType::BIT, std::string("\x01", 1), ColumnType::BIT, (0 << 8) | 1, {0x01}, false, {}});
  cases.push_back(
      {"bit(12)", ColumnType::BIT, std::string("\x0A\xAA", 2), ColumnType::BIT, (1 << 8) | 4, {0x0A, 0xAA}, false, {}});
  cases.push_back({"bit(64)",
                   ColumnType::BIT,
                   std::string(8, '\xFF'),
                   ColumnType::BIT,
                   (8 << 8) | 0,
                   {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
                   false,
                   {}});
  cases.push_back(
      {"bit(8) zero", ColumnType::BIT, std::string("\x00", 1), ColumnType::BIT, (1 << 8) | 0, {0x00}, false, {}});
}

void AppendStringCases(std::vector<AgreementCase>& cases) {
  // CHAR loses trailing spaces on both paths, so the padded case keeps its
  // spaces where every type stores them.
  const std::vector<std::string> values = {"", "published", "  padded", "\xE6\x97\xA5\xE6\x9C\xAC\xE8\xAA\x9E",
                                           std::string("with\0nul", 8)};
  for (const auto& value : values) {
    // VARCHAR: the client reports VAR_STRING, a row event carries VARCHAR.
    cases.push_back({"varchar '" + value + "'",
                     ColumnType::VAR_STRING,
                     value,
                     ColumnType::VARCHAR,
                     300,
                     LengthPrefixed(value, 2),
                     false,
                     {}});
    cases.push_back({"short varchar '" + value + "'",
                     ColumnType::VAR_STRING,
                     value,
                     ColumnType::VARCHAR,
                     64,
                     LengthPrefixed(value, 1),
                     false,
                     {}});
    // CHAR, whose declared byte length is folded into the metadata high byte.
    cases.push_back({"char '" + value + "'",
                     ColumnType::STRING,
                     value,
                     ColumnType::STRING,
                     static_cast<uint16_t>((0xFE << 8) | 40),
                     LengthPrefixed(value, 1),
                     false,
                     {}});
    // TEXT and its siblings, whose metadata is the width of the length prefix.
    cases.push_back({"tinytext '" + value + "'",
                     ColumnType::TINY_BLOB,
                     value,
                     ColumnType::TINY_BLOB,
                     1,
                     LengthPrefixed(value, 1),
                     false,
                     {}});
    cases.push_back(
        {"text '" + value + "'", ColumnType::BLOB, value, ColumnType::BLOB, 2, LengthPrefixed(value, 2), false, {}});
    cases.push_back({"mediumtext '" + value + "'",
                     ColumnType::MEDIUM_BLOB,
                     value,
                     ColumnType::MEDIUM_BLOB,
                     3,
                     LengthPrefixed(value, 3),
                     false,
                     {}});
    cases.push_back({"longtext '" + value + "'",
                     ColumnType::LONG_BLOB,
                     value,
                     ColumnType::LONG_BLOB,
                     4,
                     LengthPrefixed(value, 4),
                     false,
                     {}});
  }

  // Measured: an ENUM column returns the member label and a SET column returns
  // the selected labels joined by commas, in declaration order. The client
  // reports both as CHAR.
  const std::vector<std::string> enum_labels = {"draft", "published"};
  cases.push_back({"enum first", ColumnType::STRING, "draft", ColumnType::ENUM, 1, {0x01}, false, enum_labels});
  cases.push_back({"enum second", ColumnType::STRING, "published", ColumnType::ENUM, 1, {0x02}, false, enum_labels});
  cases.push_back({"enum empty", ColumnType::STRING, "", ColumnType::ENUM, 1, {0x00}, false, enum_labels});

  const std::vector<std::string> set_labels = {"a", "b", "c"};
  cases.push_back({"set none", ColumnType::STRING, "", ColumnType::SET, 1, {0x00}, false, set_labels});
  cases.push_back({"set two", ColumnType::STRING, "a,b", ColumnType::SET, 1, {0x03}, false, set_labels});
  cases.push_back({"set all", ColumnType::STRING, "a,b,c", ColumnType::SET, 1, {0x07}, false, set_labels});
}

std::vector<AgreementCase> GenerateCases() {
  std::vector<AgreementCase> cases;
  AppendIntegerCases(cases);
  AppendApproximateNumericCases(cases);
  AppendDecimalCases(cases);
  AppendTemporalCases(cases);
  AppendBitCases(cases);
  AppendStringCases(cases);
  return cases;
}

/// What the initial-load path publishes for a result-set value.
std::string SnapshotValue(const AgreementCase& generated) {
  return CanonicalizeColumnValue(generated.snapshot_text,
                                 DescribeColumnType(generated.snapshot_type).snapshot_normalization);
}

/// What the binlog path publishes for the same stored value.
std::string BinlogValue(const AgreementCase& generated) {
  const auto* data = generated.row_image.data();
  auto decoded = DecodeFieldValue(static_cast<uint8_t>(generated.binlog_type), data, generated.metadata, false,
                                  data + generated.row_image.size(), generated.is_unsigned, &generated.labels);
  return decoded ? *decoded : "<decode failed: " + decoded.error().message() + ">";
}

// ===========================================================================
// Tests
// ===========================================================================

/**
 * @brief Both ingest paths reduce the same stored value to the same string.
 *
 * This is what lets a primary key from the snapshot and a primary key from a
 * row event address one document, and what makes replicated text comparable to
 * indexed text.
 */
TEST(ColumnValueAgreementTest, EveryAcceptedTypeDecodesIdenticallyOnBothPaths) {
  const auto cases = GenerateCases();
  ASSERT_GT(cases.size(), 100U) << "the generated space collapsed";

  for (const auto& generated : cases) {
    EXPECT_EQ(SnapshotValue(generated), BinlogValue(generated)) << generated.label;
  }
}

/**
 * @brief A NULL column is the empty string on both paths.
 */
TEST(ColumnValueAgreementTest, NullIsTheEmptyStringOnBothPaths) {
  for (const auto& generated : GenerateCases()) {
    const auto* data = generated.row_image.data();
    auto decoded = DecodeFieldValue(static_cast<uint8_t>(generated.binlog_type), data, generated.metadata, true,
                                    data + generated.row_image.size(), generated.is_unsigned, &generated.labels);
    ASSERT_TRUE(decoded) << generated.label << ": " << decoded.error().message();
    EXPECT_EQ(*decoded, "") << generated.label;
  }
}

/**
 * @brief No type is called usable without a value proving the two paths agree.
 *
 * The table decides which types a configuration may name. A type accepted there
 * and absent here would be accepted on the strength of nothing.
 */
TEST(ColumnValueAgreementTest, EveryAcceptedTypeIsCoveredByAValue) {
  std::set<ColumnType> covered;
  for (const auto& generated : GenerateCases()) {
    covered.insert(generated.snapshot_type);
    covered.insert(generated.binlog_type);
  }

  for (int code = 0; code <= 255; ++code) {
    const auto type = static_cast<ColumnType>(code);
    if (DescribeColumnType(type).acceptance != ColumnAcceptance::kAccepted) {
      continue;
    }
    EXPECT_NE(covered.find(type), covered.end())
        << "column type " << code << " is accepted with no value showing the two paths agree";
  }
}

/**
 * @brief The fractional padding is what makes a sub-second temporal key agree.
 *
 * The row decoder always writes six fractional digits while the result set
 * writes the declared precision, so a TIME(3) key reads as two different
 * documents unless the snapshot side is padded.
 */
TEST(ColumnValueAgreementTest, SubSecondTemporalTextIsPaddedToTheDecodedWidth) {
  for (const auto snapshot_type : {ColumnType::TIME, ColumnType::DATETIME, ColumnType::TIMESTAMP}) {
    const auto kind = DescribeColumnType(snapshot_type).snapshot_normalization;
    EXPECT_EQ(kind, CanonicalValueKind::kTemporal) << static_cast<int>(snapshot_type);
  }
  EXPECT_EQ(CanonicalizeColumnValue("12:00:00.500", CanonicalValueKind::kTemporal), "12:00:00.500000");
  EXPECT_EQ(CanonicalizeColumnValue("12:00:00", CanonicalValueKind::kTemporal), "12:00:00");
}

/**
 * @brief A FLOAT column is refused because the result set arrives short of digits.
 *
 * Measured on MySQL 8.4.11: a FLOAT column holding 16777216 renders as
 * `16777200`, and 1/3 renders as `0.333333`. Six significant digits are fewer
 * than a float needs to be recovered, so the digits the row image carries are
 * not in the snapshot text to be normalized back.
 */
TEST(ColumnValueAgreementTest, FloatColumnsCannotBeReconciledAndAreRefused) {
  EXPECT_EQ(DescribeColumnType(ColumnType::FLOAT).acceptance, ColumnAcceptance::kRejectedPathsDisagree);

  const Bytes image = EncodeFloat(16777216.0F);
  auto decoded = DecodeFieldValue(static_cast<uint8_t>(ColumnType::FLOAT), image.data(), 0, false,
                                  image.data() + image.size(), false, nullptr);
  ASSERT_TRUE(decoded) << decoded.error().message();
  EXPECT_EQ(*decoded, "16777216");
  EXPECT_NE(CanonicalizeColumnValue("16777200", CanonicalValueKind::kFloat), *decoded)
      << "the six digits a result set carries would have to name the stored float for these to agree";
}

/**
 * @brief A BIT key is the number both paths read out of the same bytes.
 *
 * The result set returns the raw storage bytes, which are not text: sanitizing
 * them replaces every byte outside ASCII, so two different BIT values can even
 * collapse onto one key.
 */
TEST(ColumnValueAgreementTest, BitColumnsAreReadAsANumberOnBothPaths) {
  EXPECT_EQ(DescribeColumnType(ColumnType::BIT).snapshot_normalization, CanonicalValueKind::kBitset);
  EXPECT_EQ(CanonicalizeColumnValue(std::string("\x0A\xAA", 2), CanonicalValueKind::kBitset), "2730");
  EXPECT_NE(CanonicalizeColumnValue(std::string("\x0A\xAA", 2), CanonicalValueKind::kBitset),
            CanonicalizeColumnValue(std::string("\x0A\xAB", 2), CanonicalValueKind::kText));
}

}  // namespace
}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
