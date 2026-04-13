/**
 * @file comparison_utils_test.cpp
 * @brief Unit tests for comparison utility functions
 */

#include "utils/comparison_utils.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <limits>
#include <string>

using namespace mygram::utils;

// ---------------------------------------------------------------------------
// CompareValues<int64_t> tests
// ---------------------------------------------------------------------------

TEST(CompareValuesInt64Test, EqualOperator) {
  EXPECT_TRUE(CompareValues<int64_t>(42, 42, "="));
  EXPECT_FALSE(CompareValues<int64_t>(42, 43, "="));
}

TEST(CompareValuesInt64Test, NotEqualOperator) {
  EXPECT_TRUE(CompareValues<int64_t>(42, 43, "!="));
  EXPECT_FALSE(CompareValues<int64_t>(42, 42, "!="));
}

TEST(CompareValuesInt64Test, LessThanOperator) {
  EXPECT_TRUE(CompareValues<int64_t>(1, 2, "<"));
  EXPECT_FALSE(CompareValues<int64_t>(2, 2, "<"));
  EXPECT_FALSE(CompareValues<int64_t>(3, 2, "<"));
}

TEST(CompareValuesInt64Test, GreaterThanOperator) {
  EXPECT_TRUE(CompareValues<int64_t>(3, 2, ">"));
  EXPECT_FALSE(CompareValues<int64_t>(2, 2, ">"));
  EXPECT_FALSE(CompareValues<int64_t>(1, 2, ">"));
}

TEST(CompareValuesInt64Test, LessOrEqualOperator) {
  EXPECT_TRUE(CompareValues<int64_t>(1, 2, "<="));
  EXPECT_TRUE(CompareValues<int64_t>(2, 2, "<="));
  EXPECT_FALSE(CompareValues<int64_t>(3, 2, "<="));
}

TEST(CompareValuesInt64Test, GreaterOrEqualOperator) {
  EXPECT_TRUE(CompareValues<int64_t>(3, 2, ">="));
  EXPECT_TRUE(CompareValues<int64_t>(2, 2, ">="));
  EXPECT_FALSE(CompareValues<int64_t>(1, 2, ">="));
}

TEST(CompareValuesInt64Test, UnknownOperatorReturnsFalse) {
  EXPECT_FALSE(CompareValues<int64_t>(1, 1, "=="));
  EXPECT_FALSE(CompareValues<int64_t>(1, 1, "<>"));
  EXPECT_FALSE(CompareValues<int64_t>(1, 1, ""));
  EXPECT_FALSE(CompareValues<int64_t>(1, 1, "LIKE"));
}

TEST(CompareValuesInt64Test, BoundaryValues) {
  constexpr int64_t kMin = std::numeric_limits<int64_t>::min();
  constexpr int64_t kMax = std::numeric_limits<int64_t>::max();

  EXPECT_TRUE(CompareValues<int64_t>(kMin, kMin, "="));
  EXPECT_TRUE(CompareValues<int64_t>(kMax, kMax, "="));
  EXPECT_TRUE(CompareValues<int64_t>(kMin, kMax, "<"));
  EXPECT_TRUE(CompareValues<int64_t>(kMax, kMin, ">"));
  EXPECT_TRUE(CompareValues<int64_t>(kMin, kMax, "!="));
  EXPECT_TRUE(CompareValues<int64_t>(kMin, kMin, "<="));
  EXPECT_TRUE(CompareValues<int64_t>(kMax, kMax, ">="));
}

TEST(CompareValuesInt64Test, EqualValuesAllOperators) {
  constexpr int64_t kVal = 100;
  EXPECT_TRUE(CompareValues<int64_t>(kVal, kVal, "="));
  EXPECT_FALSE(CompareValues<int64_t>(kVal, kVal, "!="));
  EXPECT_FALSE(CompareValues<int64_t>(kVal, kVal, "<"));
  EXPECT_FALSE(CompareValues<int64_t>(kVal, kVal, ">"));
  EXPECT_TRUE(CompareValues<int64_t>(kVal, kVal, "<="));
  EXPECT_TRUE(CompareValues<int64_t>(kVal, kVal, ">="));
}

// ---------------------------------------------------------------------------
// CompareValues<std::string> tests
// ---------------------------------------------------------------------------

TEST(CompareValuesStringTest, EqualOperator) {
  EXPECT_TRUE(CompareValues<std::string>("abc", "abc", "="));
  EXPECT_FALSE(CompareValues<std::string>("abc", "def", "="));
}

TEST(CompareValuesStringTest, NotEqualOperator) {
  EXPECT_TRUE(CompareValues<std::string>("abc", "def", "!="));
  EXPECT_FALSE(CompareValues<std::string>("abc", "abc", "!="));
}

TEST(CompareValuesStringTest, LessThanOperator) {
  EXPECT_TRUE(CompareValues<std::string>("abc", "def", "<"));
  EXPECT_FALSE(CompareValues<std::string>("def", "abc", "<"));
  EXPECT_FALSE(CompareValues<std::string>("abc", "abc", "<"));
}

TEST(CompareValuesStringTest, GreaterThanOperator) {
  EXPECT_TRUE(CompareValues<std::string>("def", "abc", ">"));
  EXPECT_FALSE(CompareValues<std::string>("abc", "def", ">"));
  EXPECT_FALSE(CompareValues<std::string>("abc", "abc", ">"));
}

TEST(CompareValuesStringTest, LessOrEqualOperator) {
  EXPECT_TRUE(CompareValues<std::string>("abc", "def", "<="));
  EXPECT_TRUE(CompareValues<std::string>("abc", "abc", "<="));
  EXPECT_FALSE(CompareValues<std::string>("def", "abc", "<="));
}

TEST(CompareValuesStringTest, GreaterOrEqualOperator) {
  EXPECT_TRUE(CompareValues<std::string>("def", "abc", ">="));
  EXPECT_TRUE(CompareValues<std::string>("abc", "abc", ">="));
  EXPECT_FALSE(CompareValues<std::string>("abc", "def", ">="));
}

TEST(CompareValuesStringTest, UnknownOperatorReturnsFalse) {
  EXPECT_FALSE(CompareValues<std::string>("abc", "abc", "=="));
  EXPECT_FALSE(CompareValues<std::string>("abc", "abc", ""));
}

TEST(CompareValuesStringTest, EmptyStrings) {
  EXPECT_TRUE(CompareValues<std::string>("", "", "="));
  EXPECT_FALSE(CompareValues<std::string>("", "", "!="));
  EXPECT_TRUE(CompareValues<std::string>("", "a", "<"));
  EXPECT_TRUE(CompareValues<std::string>("a", "", ">"));
}

// ---------------------------------------------------------------------------
// CompareDoubleValues tests
// ---------------------------------------------------------------------------

TEST(CompareDoubleValuesTest, EqualityWithinEpsilon) {
  // Values differ by less than the default epsilon (1e-9)
  EXPECT_TRUE(CompareDoubleValues(1.0, 1.0 + 1e-10, "="));
  EXPECT_TRUE(CompareDoubleValues(1.0, 1.0 - 1e-10, "="));
  EXPECT_TRUE(CompareDoubleValues(0.0, 0.0, "="));
}

TEST(CompareDoubleValuesTest, EqualityOutsideEpsilon) {
  // Values differ by more than the default epsilon (1e-9)
  EXPECT_FALSE(CompareDoubleValues(1.0, 1.0 + 1e-8, "="));
  EXPECT_FALSE(CompareDoubleValues(1.0, 2.0, "="));
}

TEST(CompareDoubleValuesTest, InequalityWithinEpsilon) {
  // Values within epsilon should NOT be considered not-equal
  EXPECT_FALSE(CompareDoubleValues(1.0, 1.0 + 1e-10, "!="));
  EXPECT_FALSE(CompareDoubleValues(5.0, 5.0, "!="));
}

TEST(CompareDoubleValuesTest, InequalityOutsideEpsilon) {
  EXPECT_TRUE(CompareDoubleValues(1.0, 1.0 + 1e-8, "!="));
  EXPECT_TRUE(CompareDoubleValues(1.0, 2.0, "!="));
}

TEST(CompareDoubleValuesTest, LessThanOperator) {
  EXPECT_TRUE(CompareDoubleValues(1.0, 2.0, "<"));
  EXPECT_FALSE(CompareDoubleValues(2.0, 1.0, "<"));
  EXPECT_FALSE(CompareDoubleValues(1.0, 1.0, "<"));
}

TEST(CompareDoubleValuesTest, GreaterThanOperator) {
  EXPECT_TRUE(CompareDoubleValues(2.0, 1.0, ">"));
  EXPECT_FALSE(CompareDoubleValues(1.0, 2.0, ">"));
  EXPECT_FALSE(CompareDoubleValues(1.0, 1.0, ">"));
}

TEST(CompareDoubleValuesTest, LessOrEqualOperator) {
  EXPECT_TRUE(CompareDoubleValues(1.0, 2.0, "<="));
  EXPECT_TRUE(CompareDoubleValues(1.0, 1.0, "<="));
  EXPECT_FALSE(CompareDoubleValues(2.0, 1.0, "<="));
}

TEST(CompareDoubleValuesTest, GreaterOrEqualOperator) {
  EXPECT_TRUE(CompareDoubleValues(2.0, 1.0, ">="));
  EXPECT_TRUE(CompareDoubleValues(1.0, 1.0, ">="));
  EXPECT_FALSE(CompareDoubleValues(1.0, 2.0, ">="));
}

TEST(CompareDoubleValuesTest, VerySmallValuesNearZero) {
  EXPECT_TRUE(CompareDoubleValues(0.0, 1e-10, "="));
  EXPECT_FALSE(CompareDoubleValues(0.0, 1e-8, "="));
  EXPECT_TRUE(CompareDoubleValues(1e-15, 2e-15, "<"));
  EXPECT_TRUE(CompareDoubleValues(-1e-15, 1e-15, "<"));
}

TEST(CompareDoubleValuesTest, CustomEpsilon) {
  // With a larger epsilon, values further apart are considered equal
  EXPECT_TRUE(CompareDoubleValues(1.0, 1.001, "=", 0.01));
  EXPECT_FALSE(CompareDoubleValues(1.0, 1.1, "=", 0.01));
}

TEST(CompareDoubleValuesTest, UnknownOperatorReturnsFalse) {
  EXPECT_FALSE(CompareDoubleValues(1.0, 1.0, "=="));
  EXPECT_FALSE(CompareDoubleValues(1.0, 1.0, "<>"));
  EXPECT_FALSE(CompareDoubleValues(1.0, 1.0, ""));
}
