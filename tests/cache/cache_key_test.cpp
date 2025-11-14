/**
 * @file cache_key_test.cpp
 * @brief Unit tests for CacheKey
 */

#include "cache/cache_key.h"

#include <gtest/gtest.h>

#include <set>
#include <string>

namespace mygramdb::cache {

/**
 * @brief Test CacheKey generation
 */
TEST(CacheKeyTest, Generate) {
  auto key1 = CacheKeyGenerator::Generate("test query");
  auto key2 = CacheKeyGenerator::Generate("test query");
  auto key3 = CacheKeyGenerator::Generate("different query");

  // Same input produces same key
  EXPECT_EQ(key1.hash_high, key2.hash_high);
  EXPECT_EQ(key1.hash_low, key2.hash_low);

  // Different input produces different key
  EXPECT_NE(key1.hash_high, key3.hash_high);
}

/**
 * @brief Test CacheKey comparison operator
 */
TEST(CacheKeyTest, ComparisonOperator) {
  auto key1 = CacheKeyGenerator::Generate("aaa");
  auto key2 = CacheKeyGenerator::Generate("bbb");
  auto key3 = CacheKeyGenerator::Generate("aaa");

  // Same keys are equal
  EXPECT_FALSE(key1 < key3);
  EXPECT_FALSE(key3 < key1);

  // Keys can be ordered
  bool ordered = (key1 < key2) || (key2 < key1);
  EXPECT_TRUE(ordered);
}

/**
 * @brief Test CacheKey can be used in std::set
 */
TEST(CacheKeyTest, StdSetUsage) {
  std::set<CacheKey> keys;

  auto key1 = CacheKeyGenerator::Generate("query1");
  auto key2 = CacheKeyGenerator::Generate("query2");
  auto key3 = CacheKeyGenerator::Generate("query1");

  keys.insert(key1);
  keys.insert(key2);
  keys.insert(key3);

  // Only 2 unique keys (query1 and query2)
  EXPECT_EQ(2, keys.size());
}

/**
 * @brief Test CacheKey ToString
 */
TEST(CacheKeyTest, ToString) {
  auto key = CacheKeyGenerator::Generate("test");
  std::string str = key.ToString();

  // Should be 32 hex characters (128 bits = 16 bytes = 32 hex chars)
  EXPECT_EQ(32, str.length());

  // All characters should be hex
  for (char c : str) {
    EXPECT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
  }
}

}  // namespace mygramdb::cache
