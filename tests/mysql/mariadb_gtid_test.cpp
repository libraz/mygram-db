/**
 * @file mariadb_gtid_test.cpp
 * @brief Unit tests for MariaDB GTID parsing and representation
 */

#include "mysql/mariadb_gtid.h"

#include <gtest/gtest.h>

#include "utils/error.h"

using namespace mygramdb::mysql;

// ===========================================================================
// Parse: valid single GTID
// ===========================================================================

TEST(MariaDBGTIDTest, ParseBasicGtid) {
  auto result = MariaDBGTID::Parse("0-1-42");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->domain_id, 0u);
  EXPECT_EQ(result->server_id, 1u);
  EXPECT_EQ(result->sequence_no, 42u);
}

TEST(MariaDBGTIDTest, ParseLargeValues) {
  auto result = MariaDBGTID::Parse("100-4294967295-18446744073709551615");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->domain_id, 100u);
  EXPECT_EQ(result->server_id, 4294967295u);  // uint32_t max
  EXPECT_EQ(result->sequence_no, 18446744073709551615ULL);  // uint64_t max
}

TEST(MariaDBGTIDTest, ParseZeroValues) {
  auto result = MariaDBGTID::Parse("0-0-0");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->domain_id, 0u);
  EXPECT_EQ(result->server_id, 0u);
  EXPECT_EQ(result->sequence_no, 0u);
}

TEST(MariaDBGTIDTest, ParseWithWhitespace) {
  auto result = MariaDBGTID::Parse("  0-1-42  ");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->domain_id, 0u);
  EXPECT_EQ(result->server_id, 1u);
  EXPECT_EQ(result->sequence_no, 42u);
}

TEST(MariaDBGTIDTest, ParseWithNewlines) {
  auto result = MariaDBGTID::Parse("\n0-1-42\n");
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result->domain_id, 0u);
  EXPECT_EQ(result->server_id, 1u);
  EXPECT_EQ(result->sequence_no, 42u);
}

// ===========================================================================
// Parse: error cases
// ===========================================================================

TEST(MariaDBGTIDTest, ParseEmptyString) {
  auto result = MariaDBGTID::Parse("");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMariaDBInvalidGTID);
}

TEST(MariaDBGTIDTest, ParseMissingDash) {
  auto result = MariaDBGTID::Parse("0-1");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMariaDBInvalidGTID);
}

TEST(MariaDBGTIDTest, ParseExtraDash) {
  auto result = MariaDBGTID::Parse("0-1-2-3");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMariaDBInvalidGTID);
}

TEST(MariaDBGTIDTest, ParseNonNumericDomain) {
  auto result = MariaDBGTID::Parse("abc-1-42");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMariaDBInvalidGTID);
}

TEST(MariaDBGTIDTest, ParseNonNumericServer) {
  auto result = MariaDBGTID::Parse("0-abc-42");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMariaDBInvalidGTID);
}

TEST(MariaDBGTIDTest, ParseNonNumericSequence) {
  auto result = MariaDBGTID::Parse("0-1-abc");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMariaDBInvalidGTID);
}

TEST(MariaDBGTIDTest, ParseMySQLGtidFormat) {
  // MySQL GTID should fail (has 4 dashes, hex characters)
  auto result = MariaDBGTID::Parse("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMariaDBInvalidGTID);
}

TEST(MariaDBGTIDTest, ParseNegativeNumber) {
  // Negative numbers should fail (contains non-digit '-' after first segment)
  auto result = MariaDBGTID::Parse("0--1-42");
  ASSERT_FALSE(result.has_value());
}

TEST(MariaDBGTIDTest, ParseDomainIdOverflow) {
  // uint32_t max + 1 = 4294967296
  auto result = MariaDBGTID::Parse("4294967296-1-42");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMariaDBInvalidGTID);
}

// ===========================================================================
// ParseSet: valid sets
// ===========================================================================

TEST(MariaDBGTIDTest, ParseSetSingleGtid) {
  auto result = MariaDBGTID::ParseSet("0-1-42");
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->size(), 1u);
  EXPECT_EQ((*result)[0].domain_id, 0u);
  EXPECT_EQ((*result)[0].server_id, 1u);
  EXPECT_EQ((*result)[0].sequence_no, 42u);
}

TEST(MariaDBGTIDTest, ParseSetMultipleGtids) {
  auto result = MariaDBGTID::ParseSet("0-1-42,1-2-100");
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->size(), 2u);
  EXPECT_EQ((*result)[0].domain_id, 0u);
  EXPECT_EQ((*result)[0].sequence_no, 42u);
  EXPECT_EQ((*result)[1].domain_id, 1u);
  EXPECT_EQ((*result)[1].sequence_no, 100u);
}

TEST(MariaDBGTIDTest, ParseSetThreeGtids) {
  auto result = MariaDBGTID::ParseSet("0-1-42,1-2-100,2-3-200");
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->size(), 3u);
}

TEST(MariaDBGTIDTest, ParseSetEmptyString) {
  auto result = MariaDBGTID::ParseSet("");
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->empty());
}

TEST(MariaDBGTIDTest, ParseSetWithWhitespace) {
  auto result = MariaDBGTID::ParseSet("  0-1-42 , 1-2-100  ");
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->size(), 2u);
  EXPECT_EQ((*result)[0].sequence_no, 42u);
  EXPECT_EQ((*result)[1].sequence_no, 100u);
}

TEST(MariaDBGTIDTest, ParseSetWithNewlines) {
  auto result = MariaDBGTID::ParseSet("0-1-42,\n1-2-100");
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->size(), 2u);
}

TEST(MariaDBGTIDTest, ParseSetTrailingComma) {
  // Trailing comma results in empty entry, which is skipped
  auto result = MariaDBGTID::ParseSet("0-1-42,");
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->size(), 1u);
}

// ===========================================================================
// ParseSet: error cases
// ===========================================================================

TEST(MariaDBGTIDTest, ParseSetInvalidEntry) {
  auto result = MariaDBGTID::ParseSet("0-1-42,invalid");
  ASSERT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), mygram::utils::ErrorCode::kMariaDBInvalidGTID);
}

// ===========================================================================
// ToString
// ===========================================================================

TEST(MariaDBGTIDTest, ToStringBasic) {
  MariaDBGTID gtid{0, 1, 42};
  EXPECT_EQ(gtid.ToString(), "0-1-42");
}

TEST(MariaDBGTIDTest, ToStringLargeValues) {
  MariaDBGTID gtid{100, 4294967295u, 18446744073709551615ULL};
  EXPECT_EQ(gtid.ToString(), "100-4294967295-18446744073709551615");
}

TEST(MariaDBGTIDTest, ToStringZeros) {
  MariaDBGTID gtid{0, 0, 0};
  EXPECT_EQ(gtid.ToString(), "0-0-0");
}

// ===========================================================================
// SetToString
// ===========================================================================

TEST(MariaDBGTIDTest, SetToStringEmpty) {
  EXPECT_EQ(MariaDBGTID::SetToString({}), "");
}

TEST(MariaDBGTIDTest, SetToStringSingle) {
  std::vector<MariaDBGTID> gtids = {{0, 1, 42}};
  EXPECT_EQ(MariaDBGTID::SetToString(gtids), "0-1-42");
}

TEST(MariaDBGTIDTest, SetToStringMultiple) {
  std::vector<MariaDBGTID> gtids = {{0, 1, 42}, {1, 2, 100}};
  EXPECT_EQ(MariaDBGTID::SetToString(gtids), "0-1-42,1-2-100");
}

// ===========================================================================
// Roundtrip: Parse -> ToString
// ===========================================================================

TEST(MariaDBGTIDTest, RoundtripSingle) {
  std::string original = "0-1-42";
  auto parsed = MariaDBGTID::Parse(original);
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(parsed->ToString(), original);
}

TEST(MariaDBGTIDTest, RoundtripSet) {
  std::string original = "0-1-42,1-2-100";
  auto parsed = MariaDBGTID::ParseSet(original);
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(MariaDBGTID::SetToString(*parsed), original);
}

// ===========================================================================
// IsMariaDBGtidFormat
// ===========================================================================

TEST(MariaDBGTIDTest, IsMariaDBFormatValid) {
  EXPECT_TRUE(MariaDBGTID::IsMariaDBGtidFormat("0-1-42"));
  EXPECT_TRUE(MariaDBGTID::IsMariaDBGtidFormat("100-200-300"));
  EXPECT_TRUE(MariaDBGTID::IsMariaDBGtidFormat("0-0-0"));
}

TEST(MariaDBGTIDTest, IsMariaDBFormatWithWhitespace) {
  EXPECT_TRUE(MariaDBGTID::IsMariaDBGtidFormat("  0-1-42  "));
}

TEST(MariaDBGTIDTest, IsMariaDBFormatMySQLGtid) {
  // MySQL GTID has 4 dashes and hex characters
  EXPECT_FALSE(MariaDBGTID::IsMariaDBGtidFormat("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-77"));
}

TEST(MariaDBGTIDTest, IsMariaDBFormatEmpty) {
  EXPECT_FALSE(MariaDBGTID::IsMariaDBGtidFormat(""));
}

TEST(MariaDBGTIDTest, IsMariaDBFormatOneDash) {
  EXPECT_FALSE(MariaDBGTID::IsMariaDBGtidFormat("0-1"));
}

TEST(MariaDBGTIDTest, IsMariaDBFormatThreeDashes) {
  EXPECT_FALSE(MariaDBGTID::IsMariaDBGtidFormat("0-1-2-3"));
}

TEST(MariaDBGTIDTest, IsMariaDBFormatNonNumeric) {
  EXPECT_FALSE(MariaDBGTID::IsMariaDBGtidFormat("abc-1-42"));
  EXPECT_FALSE(MariaDBGTID::IsMariaDBGtidFormat("0-abc-42"));
  EXPECT_FALSE(MariaDBGTID::IsMariaDBGtidFormat("0-1-abc"));
}

// ===========================================================================
// Equality operators
// ===========================================================================

TEST(MariaDBGTIDTest, EqualityOperator) {
  MariaDBGTID a{0, 1, 42};
  MariaDBGTID b{0, 1, 42};
  MariaDBGTID c{0, 1, 43};

  EXPECT_EQ(a, b);
  EXPECT_NE(a, c);
}

TEST(MariaDBGTIDTest, EqualityDifferentDomain) {
  MariaDBGTID a{0, 1, 42};
  MariaDBGTID b{1, 1, 42};
  EXPECT_NE(a, b);
}

TEST(MariaDBGTIDTest, EqualityDifferentServer) {
  MariaDBGTID a{0, 1, 42};
  MariaDBGTID b{0, 2, 42};
  EXPECT_NE(a, b);
}

// ===========================================================================
// ServerFlavor detection (tests for server_flavor.h)
// ===========================================================================

#include "mysql/server_flavor.h"

TEST(ServerFlavorTest, DetectMySQL) {
  EXPECT_EQ(DetectServerFlavor("8.4.7"), ServerFlavor::kMySQL);
  EXPECT_EQ(DetectServerFlavor("9.0.1"), ServerFlavor::kMySQL);
  EXPECT_EQ(DetectServerFlavor("5.7.44"), ServerFlavor::kMySQL);
}

TEST(ServerFlavorTest, DetectMariaDB) {
  EXPECT_EQ(DetectServerFlavor("10.11.6-MariaDB"), ServerFlavor::kMariaDB);
  EXPECT_EQ(DetectServerFlavor("11.4.0-MariaDB-1:11.4.0+maria~ubu2404"), ServerFlavor::kMariaDB);
  EXPECT_EQ(DetectServerFlavor("10.6.12-MariaDB-log"), ServerFlavor::kMariaDB);
}

TEST(ServerFlavorTest, DetectMariaDBLowercase) {
  // Robustness: handle unlikely lowercase
  EXPECT_EQ(DetectServerFlavor("10.11.6-mariadb"), ServerFlavor::kMariaDB);
}

TEST(ServerFlavorTest, DetectEmpty) {
  EXPECT_EQ(DetectServerFlavor(""), ServerFlavor::kMySQL);
}

TEST(ServerFlavorTest, GetFlavorName) {
  EXPECT_STREQ(GetServerFlavorName(ServerFlavor::kMySQL), "MySQL");
  EXPECT_STREQ(GetServerFlavorName(ServerFlavor::kMariaDB), "MariaDB");
}

// ===========================================================================
// BinlogEventTypes: MariaDB events (tests for binlog_event_types.h)
// ===========================================================================

#include "mysql/binlog_event_types.h"

TEST(BinlogEventTypesTest, MariaDBEventValues) {
  EXPECT_EQ(static_cast<uint8_t>(MySQLBinlogEventType::MARIADB_ANNOTATE_ROWS_EVENT), 160);
  EXPECT_EQ(static_cast<uint8_t>(MySQLBinlogEventType::MARIADB_BINLOG_CHECKPOINT_EVENT), 161);
  EXPECT_EQ(static_cast<uint8_t>(MySQLBinlogEventType::MARIADB_GTID_EVENT), 162);
  EXPECT_EQ(static_cast<uint8_t>(MySQLBinlogEventType::MARIADB_GTID_LIST_EVENT), 163);
  EXPECT_EQ(static_cast<uint8_t>(MySQLBinlogEventType::MARIADB_START_ENCRYPTION_EVENT), 164);
}

TEST(BinlogEventTypesTest, MariaDBEventNames) {
  EXPECT_STREQ(GetEventTypeName(MySQLBinlogEventType::MARIADB_ANNOTATE_ROWS_EVENT), "MARIADB_ANNOTATE_ROWS_EVENT");
  EXPECT_STREQ(GetEventTypeName(MySQLBinlogEventType::MARIADB_GTID_EVENT), "MARIADB_GTID_EVENT");
  EXPECT_STREQ(GetEventTypeName(MySQLBinlogEventType::MARIADB_GTID_LIST_EVENT), "MARIADB_GTID_LIST_EVENT");
}
