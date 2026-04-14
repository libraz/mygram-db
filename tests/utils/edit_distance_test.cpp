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
