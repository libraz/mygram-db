/**
 * @file hash_utils_test.cpp
 * @brief Unit tests for transparent hash and equality utilities
 */

#include "utils/hash_utils.h"

#include <gtest/gtest.h>

#include <string>
#include <string_view>

using namespace mygramdb::utils;

TEST(TransparentStringHashTest, SameContentSameHash) {
  TransparentStringHash hasher;
  std::string str = "hello";
  std::string_view sv = "hello";
  const char* cstr = "hello";

  size_t hash_str = hasher(str);
  size_t hash_sv = hasher(sv);
  size_t hash_cstr = hasher(cstr);

  EXPECT_EQ(hash_str, hash_sv);
  EXPECT_EQ(hash_str, hash_cstr);
}

TEST(TransparentStringHashTest, DifferentStringsDifferentHash) {
  TransparentStringHash hasher;
  size_t hash1 = hasher(std::string("abc"));
  size_t hash2 = hasher(std::string("def"));
  // Technically hashes could collide, but these short strings should not
  EXPECT_NE(hash1, hash2);
}

TEST(TransparentStringHashTest, EmptyStringsMatch) {
  TransparentStringHash hasher;
  EXPECT_EQ(hasher(std::string("")), hasher(std::string_view("")));
  EXPECT_EQ(hasher(std::string("")), hasher(""));
}

TEST(TransparentStringEqualTest, EqualCrossType) {
  TransparentStringEqual eq;
  std::string str = "test";
  std::string_view sv = "test";

  EXPECT_TRUE(eq(str, sv));
  EXPECT_TRUE(eq(sv, str));
  EXPECT_TRUE(eq(str, "test"));
  EXPECT_TRUE(eq("test", sv));
}

TEST(TransparentStringEqualTest, NotEqual) {
  TransparentStringEqual eq;
  EXPECT_FALSE(eq("abc", "def"));
  EXPECT_FALSE(eq(std::string("abc"), std::string_view("def")));
}

TEST(TransparentStringEqualTest, EmptyStringsEqual) {
  TransparentStringEqual eq;
  EXPECT_TRUE(eq(std::string(""), std::string_view("")));
}
