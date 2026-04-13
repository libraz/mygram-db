/**
 * @file dump_format_v1_test.cpp
 * @brief Unit tests for dump format V1 header serialization
 */

#include <gtest/gtest.h>

#include <sstream>

#include "storage/dump_format_v1.h"

using namespace mygramdb::storage::dump_v1;

/**
 * @brief Test that header_size is non-zero after write
 */
TEST(DumpFormatV1Test, HeaderSizeNonZeroAfterWrite) {
  // Create a header with a GTID
  HeaderV1 header;
  header.header_size = 0;
  header.flags = 0;
  header.dump_timestamp = 1234567890;
  header.total_file_size = 0;
  header.file_crc32 = 0;
  header.gtid = "3E11FA47-71CA-11E1-9E33-C80AA9429562:42";

  // Calculate expected header size:
  // header_size(4) + flags(4) + dump_timestamp(8) + total_file_size(8) +
  // file_crc32(4) + gtid_length_prefix(4) + gtid_data(N)
  uint32_t expected_size = static_cast<uint32_t>(4 + 4 + 8 + 8 + 4 + 4 + header.gtid.size());
  header.header_size = expected_size;

  // Write header to stream
  std::ostringstream oss;
  auto write_result = WriteHeaderV1(oss, header);
  ASSERT_TRUE(write_result.has_value()) << "WriteHeaderV1 failed: " << write_result.error().message();

  // Read header back
  std::istringstream iss(oss.str());
  HeaderV1 read_header;
  auto read_result = ReadHeaderV1(iss, read_header);
  ASSERT_TRUE(read_result.has_value()) << "ReadHeaderV1 failed: " << read_result.error().message();

  // Verify header_size is non-zero and matches expected value
  EXPECT_GT(read_header.header_size, 0u) << "header_size should be non-zero after write";
  EXPECT_EQ(read_header.header_size, expected_size) << "header_size should match calculated value";

  // Verify other fields round-trip correctly
  EXPECT_EQ(read_header.flags, header.flags);
  EXPECT_EQ(read_header.dump_timestamp, header.dump_timestamp);
  EXPECT_EQ(read_header.gtid, header.gtid);
}

/**
 * @brief Test header_size with empty GTID
 */
TEST(DumpFormatV1Test, HeaderSizeWithEmptyGtid) {
  HeaderV1 header;
  header.gtid = "";
  // Expected: 4 + 4 + 8 + 8 + 4 + 4 + 0 = 32
  uint32_t expected_size = 32;
  header.header_size = expected_size;

  std::ostringstream oss;
  auto write_result = WriteHeaderV1(oss, header);
  ASSERT_TRUE(write_result.has_value());

  std::istringstream iss(oss.str());
  HeaderV1 read_header;
  auto read_result = ReadHeaderV1(iss, read_header);
  ASSERT_TRUE(read_result.has_value());

  EXPECT_EQ(read_header.header_size, expected_size);
}
