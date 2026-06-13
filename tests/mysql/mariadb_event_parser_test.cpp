/**
 * @file mariadb_event_parser_test.cpp
 * @brief Tests for MariaDB-specific binlog event parsing
 */

#include "mysql/mariadb_event_parser.h"

#include <gtest/gtest.h>

#include <vector>

#include "mysql/mariadb_gtid.h"
#include "utils/constants.h"

namespace mygramdb::mysql {
namespace {

void WriteU32At(std::vector<unsigned char>& buf, size_t offset, uint32_t val);

/// Build a minimal binlog event header
/// @param type Event type code
/// @param server_id Server ID (stored at bytes 5-8)
/// @param event_length Total event length including header
/// @return 19-byte header
std::vector<unsigned char> MakeEventHeader(uint8_t type, uint32_t server_id, uint32_t event_length) {
  std::vector<unsigned char> header(mygram::constants::kBinlogEventHeaderLen, 0);
  // timestamp (4 bytes) = 0
  header[4] = type;  // type_code
  WriteU32At(header, 5, server_id);
  WriteU32At(header, 9, event_length);
  // next_position (4 bytes) = 0
  // flags (2 bytes) = 0
  return header;
}

void WriteU32At(std::vector<unsigned char>& buf, size_t offset, uint32_t val) {
  ASSERT_LE(offset + 4, buf.size());
  buf[offset] = static_cast<unsigned char>(val & 0xFFu);
  buf[offset + 1] = static_cast<unsigned char>((val >> 8) & 0xFFu);
  buf[offset + 2] = static_cast<unsigned char>((val >> 16) & 0xFFu);
  buf[offset + 3] = static_cast<unsigned char>((val >> 24) & 0xFFu);
}

/// Append little-endian uint32
void AppendU32(std::vector<unsigned char>& buf, uint32_t val) {
  buf.push_back(static_cast<unsigned char>(val & 0xFFu));
  buf.push_back(static_cast<unsigned char>((val >> 8) & 0xFFu));
  buf.push_back(static_cast<unsigned char>((val >> 16) & 0xFFu));
  buf.push_back(static_cast<unsigned char>((val >> 24) & 0xFFu));
}

/// Append little-endian uint64
void AppendU64(std::vector<unsigned char>& buf, uint64_t val) {
  for (size_t i = 0; i < 8; ++i) {
    buf.push_back(static_cast<unsigned char>((val >> (i * 8)) & 0xFFu));
  }
}

/// Append CRC32 placeholder (4 zero bytes)
void AppendCRC32Placeholder(std::vector<unsigned char>& buf) {
  buf.push_back(0);
  buf.push_back(0);
  buf.push_back(0);
  buf.push_back(0);
}

// =============================================================================
// ExtractGTID tests
// =============================================================================

class MariaDBEventParserGTIDTest : public ::testing::Test {
 protected:
  /// Build a MariaDB GTID event (type 162)
  /// Layout: header(19) + seq_no(8) + domain_id(4) + flags(1) + CRC32(4)
  std::vector<unsigned char> BuildGTIDEvent(uint32_t domain_id, uint32_t server_id, uint64_t seq_no,
                                            uint8_t flags = 0) {
    uint32_t total_len = mygram::constants::kBinlogEventHeaderLen + 8 + 4 + 1 + mygram::constants::kBinlogChecksumSize;
    auto buf = MakeEventHeader(162, server_id, total_len);
    AppendU64(buf, seq_no);       // seq_no at offset 19
    AppendU32(buf, domain_id);    // domain_id at offset 27
    buf.push_back(flags);         // flags at offset 31
    AppendCRC32Placeholder(buf);  // CRC32
    return buf;
  }
};

TEST_F(MariaDBEventParserGTIDTest, BasicGTID) {
  auto event = BuildGTIDEvent(0, 1, 42);
  auto result = MariaDBEventParser::ExtractGTID(event.data(), event.size());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), "0-1-42");
}

TEST_F(MariaDBEventParserGTIDTest, LargeValues) {
  auto event = BuildGTIDEvent(100, 200, 999999999);
  auto result = MariaDBEventParser::ExtractGTID(event.data(), event.size());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), "100-200-999999999");
}

TEST_F(MariaDBEventParserGTIDTest, ReadsExplicitLittleEndianBytes) {
  auto event = BuildGTIDEvent(0x0A0B0C0Du, 0x01020304u, 0x0102030405060708ULL);

  EXPECT_EQ(event[5], 0x04);
  EXPECT_EQ(event[6], 0x03);
  EXPECT_EQ(event[7], 0x02);
  EXPECT_EQ(event[8], 0x01);
  EXPECT_EQ(event[mygram::constants::kBinlogEventHeaderLen], 0x08);
  EXPECT_EQ(event[mygram::constants::kBinlogEventHeaderLen + 7], 0x01);
  EXPECT_EQ(event[mygram::constants::kBinlogEventHeaderLen + 8], 0x0D);

  auto result = MariaDBEventParser::ExtractGTID(event.data(), event.size());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), "168496141-16909060-72623859790382856");
}

TEST_F(MariaDBEventParserGTIDTest, MaxDomainAndServer) {
  auto event = BuildGTIDEvent(UINT32_MAX, UINT32_MAX, UINT64_MAX);
  auto result = MariaDBEventParser::ExtractGTID(event.data(), event.size());
  ASSERT_TRUE(result.has_value());

  // Verify round-trip through MariaDBGTID::Parse
  auto parsed = MariaDBGTID::Parse(result.value());
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(parsed->domain_id, UINT32_MAX);
  EXPECT_EQ(parsed->server_id, UINT32_MAX);
  EXPECT_EQ(parsed->sequence_no, UINT64_MAX);
}

TEST_F(MariaDBEventParserGTIDTest, ZeroValues) {
  auto event = BuildGTIDEvent(0, 0, 0);
  auto result = MariaDBEventParser::ExtractGTID(event.data(), event.size());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), "0-0-0");
}

TEST_F(MariaDBEventParserGTIDTest, WithFlags) {
  auto event = BuildGTIDEvent(1, 2, 100, 0x01);
  auto result = MariaDBEventParser::ExtractGTID(event.data(), event.size());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), "1-2-100");
}

TEST_F(MariaDBEventParserGTIDTest, NullBuffer) {
  auto result = MariaDBEventParser::ExtractGTID(nullptr, 100);
  EXPECT_FALSE(result.has_value());
}

TEST_F(MariaDBEventParserGTIDTest, TooShort) {
  // Header only, no post-header data
  auto header = MakeEventHeader(162, 1, 19);
  auto result = MariaDBEventParser::ExtractGTID(header.data(), header.size());
  EXPECT_FALSE(result.has_value());
}

TEST_F(MariaDBEventParserGTIDTest, ExactMinimumSize) {
  // header(19) + seq_no(8) + domain_id(4) + flags(1) = 32 bytes (no CRC32)
  auto event = BuildGTIDEvent(5, 10, 50);
  // Remove CRC32 to test exact minimum
  size_t min_size = mygram::constants::kBinlogEventHeaderLen + 13;  // 32 bytes
  auto result = MariaDBEventParser::ExtractGTID(event.data(), min_size);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), "5-10-50");
}

// =============================================================================
// ParseGTIDList tests
// =============================================================================

class MariaDBEventParserGTIDListTest : public ::testing::Test {
 protected:
  /// Build a GTID_LIST event (type 163)
  /// Layout: header(19) + count_and_flags(4) + entries(16 each) + CRC32(4)
  std::vector<unsigned char> BuildGTIDListEvent(const std::vector<MariaDBGTID>& gtids, uint32_t flags = 0) {
    uint32_t count = static_cast<uint32_t>(gtids.size());
    uint32_t count_and_flags = (flags << 28) | (count & 0x0FFFFFFFu);
    uint32_t total_len =
        mygram::constants::kBinlogEventHeaderLen + 4 + (count * 16) + mygram::constants::kBinlogChecksumSize;

    auto buf = MakeEventHeader(163, 1, total_len);
    AppendU32(buf, count_and_flags);

    for (const auto& gtid : gtids) {
      AppendU32(buf, gtid.domain_id);
      AppendU32(buf, gtid.server_id);
      AppendU64(buf, gtid.sequence_no);
    }

    AppendCRC32Placeholder(buf);
    return buf;
  }
};

TEST_F(MariaDBEventParserGTIDListTest, EmptyList) {
  auto event = BuildGTIDListEvent({});
  auto result = MariaDBEventParser::ParseGTIDList(event.data(), event.size());
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(result->empty());
}

TEST_F(MariaDBEventParserGTIDListTest, SingleEntry) {
  std::vector<MariaDBGTID> gtids = {{0, 1, 42}};
  auto event = BuildGTIDListEvent(gtids);
  auto result = MariaDBEventParser::ParseGTIDList(event.data(), event.size());
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->size(), 1u);
  EXPECT_EQ((*result)[0], (MariaDBGTID{0, 1, 42}));
}

TEST_F(MariaDBEventParserGTIDListTest, MultipleEntries) {
  std::vector<MariaDBGTID> gtids = {{0, 1, 42}, {1, 2, 100}, {3, 5, 999}};
  auto event = BuildGTIDListEvent(gtids);
  auto result = MariaDBEventParser::ParseGTIDList(event.data(), event.size());
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->size(), 3u);
  EXPECT_EQ((*result)[0], (MariaDBGTID{0, 1, 42}));
  EXPECT_EQ((*result)[1], (MariaDBGTID{1, 2, 100}));
  EXPECT_EQ((*result)[2], (MariaDBGTID{3, 5, 999}));
}

TEST_F(MariaDBEventParserGTIDListTest, WithFlags) {
  // Upper 4 bits of count_and_flags are flags, should not affect count
  std::vector<MariaDBGTID> gtids = {{0, 1, 42}};
  auto event = BuildGTIDListEvent(gtids, 0x5);  // flags = 5
  auto result = MariaDBEventParser::ParseGTIDList(event.data(), event.size());
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->size(), 1u);
  EXPECT_EQ((*result)[0], (MariaDBGTID{0, 1, 42}));
}

TEST_F(MariaDBEventParserGTIDListTest, NullBuffer) {
  auto result = MariaDBEventParser::ParseGTIDList(nullptr, 100);
  EXPECT_FALSE(result.has_value());
}

TEST_F(MariaDBEventParserGTIDListTest, TooShortForHeader) {
  auto header = MakeEventHeader(163, 1, 20);
  auto result = MariaDBEventParser::ParseGTIDList(header.data(), header.size());
  EXPECT_FALSE(result.has_value());
}

TEST_F(MariaDBEventParserGTIDListTest, TruncatedEntries) {
  // Claim 2 entries but only provide data for 1
  std::vector<MariaDBGTID> gtids = {{0, 1, 42}};
  auto event = BuildGTIDListEvent(gtids);
  // Overwrite count to 2
  uint32_t fake_count = 2;
  WriteU32At(event, mygram::constants::kBinlogEventHeaderLen, fake_count);
  auto result = MariaDBEventParser::ParseGTIDList(event.data(), event.size());
  EXPECT_FALSE(result.has_value());
}

TEST_F(MariaDBEventParserGTIDListTest, LargeValues) {
  std::vector<MariaDBGTID> gtids = {{UINT32_MAX, UINT32_MAX, UINT64_MAX}};
  auto event = BuildGTIDListEvent(gtids);
  auto result = MariaDBEventParser::ParseGTIDList(event.data(), event.size());
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->size(), 1u);
  EXPECT_EQ((*result)[0].domain_id, UINT32_MAX);
  EXPECT_EQ((*result)[0].server_id, UINT32_MAX);
  EXPECT_EQ((*result)[0].sequence_no, UINT64_MAX);
}

// =============================================================================
// ExtractAnnotateRows tests
// =============================================================================

class MariaDBEventParserAnnotateTest : public ::testing::Test {
 protected:
  /// Build an ANNOTATE_ROWS event (type 160)
  /// Layout: header(19) + sql_text + CRC32(4)
  std::vector<unsigned char> BuildAnnotateRowsEvent(const std::string& sql) {
    uint32_t total_len = mygram::constants::kBinlogEventHeaderLen + sql.size() + mygram::constants::kBinlogChecksumSize;
    auto buf = MakeEventHeader(160, 1, total_len);
    buf.insert(buf.end(), sql.begin(), sql.end());
    AppendCRC32Placeholder(buf);
    return buf;
  }
};

TEST_F(MariaDBEventParserAnnotateTest, SimpleQuery) {
  auto event = BuildAnnotateRowsEvent("INSERT INTO t1 VALUES (1, 'hello')");
  auto result = MariaDBEventParser::ExtractAnnotateRows(event.data(), event.size());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), "INSERT INTO t1 VALUES (1, 'hello')");
}

TEST_F(MariaDBEventParserAnnotateTest, ComplexQuery) {
  std::string sql = "UPDATE articles SET content = 'new text', updated_at = NOW() WHERE id = 42";
  auto event = BuildAnnotateRowsEvent(sql);
  auto result = MariaDBEventParser::ExtractAnnotateRows(event.data(), event.size());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), sql);
}

TEST_F(MariaDBEventParserAnnotateTest, UnicodeQuery) {
  std::string sql = u8"INSERT INTO t1 VALUES (1, '日本語テスト')";
  auto event = BuildAnnotateRowsEvent(sql);
  auto result = MariaDBEventParser::ExtractAnnotateRows(event.data(), event.size());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(result.value(), sql);
}

TEST_F(MariaDBEventParserAnnotateTest, NullBuffer) {
  auto result = MariaDBEventParser::ExtractAnnotateRows(nullptr, 100);
  EXPECT_FALSE(result.has_value());
}

TEST_F(MariaDBEventParserAnnotateTest, EmptyText) {
  // header(19) + CRC32(4) = 23 bytes, no text
  auto buf = MakeEventHeader(160, 1, 23);
  AppendCRC32Placeholder(buf);
  auto result = MariaDBEventParser::ExtractAnnotateRows(buf.data(), buf.size());
  EXPECT_FALSE(result.has_value());
}

TEST_F(MariaDBEventParserAnnotateTest, TooShortForHeaderAndChecksum) {
  // Just the header, not even room for CRC32
  auto header = MakeEventHeader(160, 1, 19);
  auto result = MariaDBEventParser::ExtractAnnotateRows(header.data(), header.size());
  EXPECT_FALSE(result.has_value());
}

// =============================================================================
// Integration: GTID round-trip (event -> parse -> MariaDBGTID -> string)
// =============================================================================

TEST(MariaDBEventParserIntegrationTest, GTIDRoundTrip) {
  // Build a GTID event with known values
  uint32_t total_len = mygram::constants::kBinlogEventHeaderLen + 13 + mygram::constants::kBinlogChecksumSize;
  auto header = MakeEventHeader(162, 42, total_len);

  uint64_t seq_no = 12345;
  uint32_t domain_id = 7;
  uint8_t flags = 0;
  AppendU64(header, seq_no);
  AppendU32(header, domain_id);
  header.push_back(flags);
  AppendCRC32Placeholder(header);

  // Extract GTID from event
  auto gtid_str = MariaDBEventParser::ExtractGTID(header.data(), header.size());
  ASSERT_TRUE(gtid_str.has_value());
  EXPECT_EQ(gtid_str.value(), "7-42-12345");

  // Parse GTID string back
  auto parsed = MariaDBGTID::Parse(gtid_str.value());
  ASSERT_TRUE(parsed.has_value());
  EXPECT_EQ(parsed->domain_id, 7u);
  EXPECT_EQ(parsed->server_id, 42u);
  EXPECT_EQ(parsed->sequence_no, 12345u);

  // ToString and verify
  EXPECT_EQ(parsed->ToString(), "7-42-12345");
}

TEST(MariaDBEventParserIntegrationTest, GTIDListToSet) {
  std::vector<MariaDBGTID> gtids = {{0, 1, 42}, {1, 2, 100}};

  // Build GTID_LIST event
  uint32_t count = 2;
  uint32_t total_len =
      mygram::constants::kBinlogEventHeaderLen + 4 + (count * 16) + mygram::constants::kBinlogChecksumSize;
  auto buf = MakeEventHeader(163, 1, total_len);
  AppendU32(buf, count);
  for (const auto& gtid : gtids) {
    AppendU32(buf, gtid.domain_id);
    AppendU32(buf, gtid.server_id);
    AppendU64(buf, gtid.sequence_no);
  }
  AppendCRC32Placeholder(buf);

  // Parse
  auto result = MariaDBEventParser::ParseGTIDList(buf.data(), buf.size());
  ASSERT_TRUE(result.has_value());
  ASSERT_EQ(result->size(), 2u);

  // Convert to set string
  std::string set_str = MariaDBGTID::SetToString(*result);
  EXPECT_EQ(set_str, "0-1-42,1-2-100");
}

}  // namespace
}  // namespace mygramdb::mysql
