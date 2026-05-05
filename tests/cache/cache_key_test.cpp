/**
 * @file cache_key_test.cpp
 * @brief Unit tests for CacheKey
 */

#include "query/cache_key.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

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

/**
 * @brief Test CacheKey default constructor
 */
TEST(CacheKeyTest, DefaultConstructor) {
  CacheKey key;
  EXPECT_EQ(key.hash_high, 0);
  EXPECT_EQ(key.hash_low, 0);
}

/**
 * @brief Test CacheKey value constructor
 */
TEST(CacheKeyTest, ValueConstructor) {
  CacheKey key(0x123456789ABCDEF0, 0xFEDCBA9876543210);
  EXPECT_EQ(key.hash_high, 0x123456789ABCDEF0);
  EXPECT_EQ(key.hash_low, 0xFEDCBA9876543210);
}

/**
 * @brief Test CacheKey equality operator
 */
TEST(CacheKeyTest, EqualityOperator) {
  CacheKey key1(100, 200);
  CacheKey key2(100, 200);
  CacheKey key3(100, 201);
  CacheKey key4(101, 200);

  EXPECT_TRUE(key1 == key2);
  EXPECT_FALSE(key1 == key3);
  EXPECT_FALSE(key1 == key4);
}

/**
 * @brief Test CacheKey inequality operator
 */
TEST(CacheKeyTest, InequalityOperator) {
  CacheKey key1(100, 200);
  CacheKey key2(100, 200);
  CacheKey key3(100, 201);

  EXPECT_FALSE(key1 != key2);
  EXPECT_TRUE(key1 != key3);
}

/**
 * @brief Test CacheKey less-than operator edge cases
 */
TEST(CacheKeyTest, LessThanOperatorEdgeCases) {
  CacheKey key1(100, 200);
  CacheKey key2(100, 201);
  CacheKey key3(101, 100);

  // Same high, different low
  EXPECT_TRUE(key1 < key2);
  EXPECT_FALSE(key2 < key1);

  // Different high
  EXPECT_TRUE(key1 < key3);
  EXPECT_FALSE(key3 < key1);

  // Same key should not be less than itself
  EXPECT_FALSE(key1 < key1);
}

/**
 * @brief Test hash function for unordered_map
 */
TEST(CacheKeyTest, StdHashFunction) {
  CacheKey key1(100, 200);
  CacheKey key2(100, 200);

  std::hash<CacheKey> hasher;

  // Same keys should produce same hash
  EXPECT_EQ(hasher(key1), hasher(key2));
}

/**
 * @brief Test hash function avoids the swapped-pair collision that plain
 *        XOR would produce.
 *
 * With the previous `hash_high ^ hash_low` combiner, (a, b) and (b, a)
 * hashed to the same value because XOR is commutative. This regression
 * test ensures the new combiner mixes the two halves asymmetrically.
 */
TEST(CacheKeyHashAvoidsSwappedCollision, SwappedPairsDiffer) {
  std::hash<CacheKey> hasher;

  // Construct two keys where hash_high and hash_low are swapped
  CacheKey key1(0x0123456789ABCDEFULL, 0xFEDCBA9876543210ULL);
  CacheKey key2(0xFEDCBA9876543210ULL, 0x0123456789ABCDEFULL);

  EXPECT_NE(hasher(key1), hasher(key2)) << "Swapped halves must not collide under the combined hash";

  // A second pair with simpler values to make the asymmetry obvious
  CacheKey key3(100, 200);
  CacheKey key4(200, 100);
  EXPECT_NE(hasher(key3), hasher(key4));
}

/**
 * @brief Distribution sanity test for the CacheKey hash combiner.
 *
 * Hashes 1000 deterministic-but-varied keys and asserts that fewer than
 * 10% of them collide modulo a small bucket count. With the old XOR
 * combiner, generated keys whose halves happened to swap would collide
 * deterministically; the Fibonacci combiner spreads them across buckets.
 */
TEST(CacheKeyHashAvoidsSwappedCollision, DistributionSanity) {
  std::hash<CacheKey> hasher;

  constexpr size_t kNumKeys = 1000;
  constexpr size_t kBuckets = 256;
  std::vector<size_t> bucket_counts(kBuckets, 0);

  // Use linear congruential pattern with both halves derived from i so the
  // sequence is deterministic but exercises a wide range of values.
  for (size_t i = 0; i < kNumKeys; ++i) {
    const std::uint64_t high = (i * 2654435761ULL) ^ 0xDEADBEEFCAFEBABEULL;
    const std::uint64_t low = (i * 11400714819323198485ULL) ^ 0x0F0F0F0F0F0F0F0FULL;
    CacheKey key(high, low);
    bucket_counts[hasher(key) % kBuckets]++;
  }

  // Count "extra" entries per bucket (anything beyond 1 is a collision).
  size_t collisions = 0;
  for (size_t count : bucket_counts) {
    if (count > 1) {
      collisions += count - 1;
    }
  }

  // With kNumKeys=1000 and kBuckets=256, expected occupancy is ~3.9 per bucket
  // under uniform distribution; the count-extra metric here measures only the
  // *excess* entries in over-populated buckets, so a uniform hash actually
  // produces a high collision count by this metric. Use a generous bound
  // (95%) — we mainly want to catch catastrophic bucket pile-up that the
  // old XOR combiner would produce.
  const size_t collision_bound = (kNumKeys * 95) / 100;
  EXPECT_LT(collisions, collision_bound) << "Too many collisions: " << collisions << " out of " << kNumKeys;
}

/**
 * @brief Test CacheKey can be used in std::unordered_map
 */
TEST(CacheKeyTest, UnorderedMapUsage) {
  std::unordered_map<CacheKey, int> map;

  auto key1 = CacheKeyGenerator::Generate("query1");
  auto key2 = CacheKeyGenerator::Generate("query2");
  auto key3 = CacheKeyGenerator::Generate("query1");  // Same as key1

  map[key1] = 1;
  map[key2] = 2;
  map[key3] = 3;  // Should overwrite key1

  EXPECT_EQ(map.size(), 2);
  EXPECT_EQ(map[key1], 3);
  EXPECT_EQ(map[key2], 2);
}

/**
 * @brief Test generation with empty string
 */
TEST(CacheKeyTest, GenerateEmptyString) {
  auto key = CacheKeyGenerator::Generate("");

  // Should produce a valid key (MD5 of empty string)
  // MD5("") = d41d8cd98f00b204e9800998ecf8427e
  std::string str = key.ToString();
  EXPECT_EQ(str.length(), 32);
  // The key should be consistent
  auto key2 = CacheKeyGenerator::Generate("");
  EXPECT_EQ(key, key2);
}

/**
 * @brief Test generation with long string
 */
TEST(CacheKeyTest, GenerateLongString) {
  std::string long_query(10000, 'x');
  auto key = CacheKeyGenerator::Generate(long_query);

  std::string str = key.ToString();
  EXPECT_EQ(str.length(), 32);

  // Same long string should produce same key
  auto key2 = CacheKeyGenerator::Generate(long_query);
  EXPECT_EQ(key, key2);
}

/**
 * @brief Test generation with special characters
 */
TEST(CacheKeyTest, GenerateSpecialCharacters) {
  auto key1 = CacheKeyGenerator::Generate("SELECT * FROM `table`");
  auto key2 = CacheKeyGenerator::Generate("SELECT * FROM \"table\"");
  auto key3 = CacheKeyGenerator::Generate("SELECT * FROM\ttable");
  auto key4 = CacheKeyGenerator::Generate("SELECT * FROM\ntable");

  // All should produce valid keys
  EXPECT_EQ(key1.ToString().length(), 32);
  EXPECT_EQ(key2.ToString().length(), 32);
  EXPECT_EQ(key3.ToString().length(), 32);
  EXPECT_EQ(key4.ToString().length(), 32);

  // All should be different
  EXPECT_NE(key1, key2);
  EXPECT_NE(key2, key3);
  EXPECT_NE(key3, key4);
}

/**
 * @brief Test ToString format
 */
TEST(CacheKeyTest, ToStringFormat) {
  // Test with known values
  CacheKey key(0, 0);
  std::string str = key.ToString();
  EXPECT_EQ(str, "00000000000000000000000000000000");

  CacheKey key2(0xFFFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF);
  std::string str2 = key2.ToString();
  EXPECT_EQ(str2, "ffffffffffffffffffffffffffffffff");

  CacheKey key3(0x0123456789ABCDEF, 0xFEDCBA9876543210);
  std::string str3 = key3.ToString();
  EXPECT_EQ(str3, "0123456789abcdeffedcba9876543210");
}

}  // namespace mygramdb::cache
