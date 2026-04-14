/**
 * @file edit_distance_test.cpp
 * @brief Unit tests for edit distance utility functions
 */

#include "utils/edit_distance.h"

#include <gtest/gtest.h>

using namespace mygram::utils;

class EditDistanceTest : public ::testing::Test {};

// --- LevenshteinDistance tests ---

TEST_F(EditDistanceTest, IdenticalStrings) {
  EXPECT_EQ(LevenshteinDistance("hello", "hello", 5), 0);
}

TEST_F(EditDistanceTest, SingleSubstitution) {
  EXPECT_EQ(LevenshteinDistance("hello", "hallo", 5), 1);
}

TEST_F(EditDistanceTest, SingleInsertion) {
  EXPECT_EQ(LevenshteinDistance("hello", "helloo", 5), 1);
}

TEST_F(EditDistanceTest, SingleDeletion) {
  EXPECT_EQ(LevenshteinDistance("hello", "hell", 5), 1);
}

TEST_F(EditDistanceTest, Transposition) {
  // Levenshtein treats transposition as 2 operations (not Damerau-Levenshtein)
  EXPECT_EQ(LevenshteinDistance("ab", "ba", 5), 2);
}

TEST_F(EditDistanceTest, MaxDistanceCutoff) {
  // "hello" vs "world" has distance > 1, so returns max_distance+1 = 2
  EXPECT_EQ(LevenshteinDistance("hello", "world", 1), 2);
}

TEST_F(EditDistanceTest, EmptyFirstString) {
  EXPECT_EQ(LevenshteinDistance("", "hello", 10), 5);
}

TEST_F(EditDistanceTest, EmptySecondString) {
  EXPECT_EQ(LevenshteinDistance("hello", "", 10), 5);
}

TEST_F(EditDistanceTest, BothEmpty) {
  EXPECT_EQ(LevenshteinDistance("", "", 5), 0);
}

TEST_F(EditDistanceTest, UTF8Strings) {
  // "café" vs "cafe" — one codepoint difference (é vs e)
  EXPECT_EQ(LevenshteinDistance("café", "cafe", 5), 1);
}

TEST_F(EditDistanceTest, JapaneseStrings) {
  // "東京都" vs "東京市" — one character difference (都 vs 市)
  EXPECT_EQ(LevenshteinDistance("東京都", "東京市", 5), 1);
}

TEST_F(EditDistanceTest, LengthDifferenceExceedsMax) {
  // "hi" (2 chars) vs "hello world" (11 chars), difference = 9 > max_distance=2
  // Returns max_distance+1 = 3
  EXPECT_EQ(LevenshteinDistance("hi", "hello world", 2), 3);
}

TEST_F(EditDistanceTest, MaxDistanceZero) {
  // Identical strings: distance is 0, within max_distance=0
  EXPECT_EQ(LevenshteinDistance("hello", "hello", 0), 0);
  // One substitution: distance is 1, exceeds max_distance=0, returns 0+1=1
  EXPECT_EQ(LevenshteinDistance("hello", "hallo", 0), 1);
}

TEST_F(EditDistanceTest, MultipleEdits) {
  // Classic example: kitten -> sitting requires 3 edits
  // k->s, e->i, +g
  EXPECT_EQ(LevenshteinDistance("kitten", "sitting", 10), 3);
}

// --- ContainsFuzzyMatch tests ---

TEST_F(EditDistanceTest, ExactMatch) {
  EXPECT_TRUE(ContainsFuzzyMatch("hello world", "hello", 0));
}

TEST_F(EditDistanceTest, FuzzyMatchDistance1) {
  EXPECT_TRUE(ContainsFuzzyMatch("hello world", "hallo", 1));
}

TEST_F(EditDistanceTest, NoMatchExceedingDistance) {
  EXPECT_FALSE(ContainsFuzzyMatch("hello world", "xxxxx", 1));
}

TEST_F(EditDistanceTest, MultipleWords) {
  // "quikc" is within distance 2 of "quick" (transposition = 2 in Levenshtein)
  EXPECT_TRUE(ContainsFuzzyMatch("the quick brown fox", "quikc", 2));
}

TEST_F(EditDistanceTest, EmptyText) {
  EXPECT_FALSE(ContainsFuzzyMatch("", "hello", 1));
}

TEST_F(EditDistanceTest, EmptyTerm) {
  // Empty term returns true immediately (implementation detail: early return)
  EXPECT_TRUE(ContainsFuzzyMatch("hello world", "", 1));
  EXPECT_TRUE(ContainsFuzzyMatch("a", "", 1));
}

TEST_F(EditDistanceTest, SingleWordText) {
  // "restrant" vs "restaurant": missing "au" = distance 2
  EXPECT_TRUE(ContainsFuzzyMatch("restaurant", "restrant", 2));
}

TEST_F(EditDistanceTest, UTF8Words) {
  // "東京都" vs "東京市": distance 1
  EXPECT_TRUE(ContainsFuzzyMatch("東京都 大阪府", "東京市", 1));
}

TEST_F(EditDistanceTest, NoWordsCloseEnough) {
  EXPECT_FALSE(ContainsFuzzyMatch("alpha beta gamma", "zzzzz", 2));
}

TEST_F(EditDistanceTest, ExactWordAmongMany) {
  EXPECT_TRUE(ContainsFuzzyMatch("a b c hello d e", "hello", 0));
}

// --- PERF-1: Stack allocation - verify correctness for various lengths ---

TEST_F(EditDistanceTest, LongAsciiStrings) {
  // Strings longer than typical but still ASCII
  std::string a(100, 'a');
  std::string b(100, 'a');
  b[50] = 'b';  // Single substitution
  EXPECT_EQ(LevenshteinDistance(a, b, 5), 1);
}

// --- EDGE-1: Whitespace splitting - tabs and newlines ---

TEST_F(EditDistanceTest, TabSeparatedWords) {
  EXPECT_TRUE(ContainsFuzzyMatch("hello\tworld", "hello", 0));
  EXPECT_TRUE(ContainsFuzzyMatch("hello\tworld", "world", 0));
}

TEST_F(EditDistanceTest, NewlineSeparatedWords) {
  EXPECT_TRUE(ContainsFuzzyMatch("hello\nworld", "hello", 0));
  EXPECT_TRUE(ContainsFuzzyMatch("hello\r\nworld", "world", 0));
}

TEST_F(EditDistanceTest, MixedWhitespace) {
  EXPECT_TRUE(ContainsFuzzyMatch("one\ttwo\nthree four", "three", 0));
  EXPECT_TRUE(ContainsFuzzyMatch("one\ttwo\nthree four", "threa", 1));  // fuzzy: 1 edit from "three"
}

// --- kStackDpLimit boundary tests (kStackDpLimit = 256 in edit_distance.cpp) ---

TEST_F(EditDistanceTest, AtStackDpLimitBoundary) {
  // 255 chars: a_len+1 == 256 == kStackDpLimit, uses stack path
  std::string a255(255, 'a');
  std::string b255(255, 'a');
  b255[127] = 'b';
  EXPECT_EQ(LevenshteinDistance(a255, b255, 5), 1u);
}

TEST_F(EditDistanceTest, ExactlyAtStackDpLimit) {
  // 256 chars: a_len+1 == 257 > kStackDpLimit, uses heap path
  std::string a256(256, 'a');
  std::string b256(256, 'a');
  b256[0] = 'b';
  b256[255] = 'b';
  EXPECT_EQ(LevenshteinDistance(a256, b256, 5), 2u);
}

TEST_F(EditDistanceTest, AboveStackDpLimit) {
  // 257 chars: clearly above kStackDpLimit, must use heap allocation
  std::string a257(257, 'a');
  std::string b257(257, 'a');
  b257[128] = 'b';
  EXPECT_EQ(LevenshteinDistance(a257, b257, 5), 1u);
}

TEST_F(EditDistanceTest, SameLengthHighDistance) {
  // Same length, all different — tests row_min > max_distance early termination
  // Returns max_distance+1 when distance exceeds threshold
  EXPECT_EQ(LevenshteinDistance("aaaaa", "bbbbb", 1), 2u);
}

TEST_F(EditDistanceTest, SingleCharacterTermFuzzyMatch) {
  EXPECT_TRUE(ContainsFuzzyMatch("a b c", "b", 0));
  EXPECT_TRUE(ContainsFuzzyMatch("a b c", "x", 1));  // "a", "b", or "c" within distance 1 of "x"
}
