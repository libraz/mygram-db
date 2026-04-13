/**
 * @file binary_io_test.cpp
 * @brief Unit tests for binary I/O utilities
 */

#include "utils/binary_io.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <sstream>
#include <string>

using namespace mygram::utils;

TEST(BinaryIOTest, WriteThenReadUint32) {
  std::stringstream ss;
  uint32_t value = 0x12345678;
  ASSERT_TRUE(WriteBinary(ss, value));

  uint32_t result = 0;
  ASSERT_TRUE(ReadBinary(ss, result));
  EXPECT_EQ(result, value);
}

TEST(BinaryIOTest, WriteThenReadUint64) {
  std::stringstream ss;
  uint64_t value = 0x123456789ABCDEF0ULL;
  ASSERT_TRUE(WriteBinary(ss, value));

  uint64_t result = 0;
  ASSERT_TRUE(ReadBinary(ss, result));
  EXPECT_EQ(result, value);
}

TEST(BinaryIOTest, WriteThenReadString) {
  std::stringstream ss;
  std::string value = "hello, world!";
  ASSERT_TRUE(WriteString(ss, value));

  std::string result;
  ASSERT_TRUE(ReadString(ss, result));
  EXPECT_EQ(result, value);
}

TEST(BinaryIOTest, WriteThenReadEmptyString) {
  std::stringstream ss;
  std::string value;
  ASSERT_TRUE(WriteString(ss, value));

  std::string result = "non-empty";
  ASSERT_TRUE(ReadString(ss, result));
  EXPECT_TRUE(result.empty());
}

TEST(BinaryIOTest, ReadFromEmptyStream) {
  std::stringstream ss;
  uint32_t value = 0;
  EXPECT_FALSE(ReadBinary(ss, value));

  std::string str;
  EXPECT_FALSE(ReadString(ss, str));
}

TEST(BinaryIOTest, RoundTripMultipleValues) {
  std::stringstream ss;

  uint32_t u32 = 42;
  uint64_t u64 = 9999999999ULL;
  std::string str = "test_string";
  uint16_t u16 = 1234;

  ASSERT_TRUE(WriteBinary(ss, u32));
  ASSERT_TRUE(WriteBinary(ss, u64));
  ASSERT_TRUE(WriteString(ss, str));
  ASSERT_TRUE(WriteBinary(ss, u16));

  uint32_t r_u32 = 0;
  uint64_t r_u64 = 0;
  std::string r_str;
  uint16_t r_u16 = 0;

  ASSERT_TRUE(ReadBinary(ss, r_u32));
  ASSERT_TRUE(ReadBinary(ss, r_u64));
  ASSERT_TRUE(ReadString(ss, r_str));
  ASSERT_TRUE(ReadBinary(ss, r_u16));

  EXPECT_EQ(r_u32, u32);
  EXPECT_EQ(r_u64, u64);
  EXPECT_EQ(r_str, str);
  EXPECT_EQ(r_u16, u16);
}

TEST(BinaryIOTest, ReadStringOversizedLengthReturnsFalse) {
  std::stringstream ss;
  // Write a length larger than 64 MB (the sanity limit)
  uint32_t oversized_len = 128 * 1024 * 1024;  // 128 MB
  ASSERT_TRUE(WriteBinary(ss, oversized_len));

  std::string result;
  EXPECT_FALSE(ReadString(ss, result));
}

TEST(BinaryIOTest, WriteThenReadDouble) {
  std::stringstream ss;
  double value = 3.14159265358979;
  ASSERT_TRUE(WriteBinary(ss, value));

  double result = 0.0;
  ASSERT_TRUE(ReadBinary(ss, result));
  EXPECT_DOUBLE_EQ(result, value);
}
