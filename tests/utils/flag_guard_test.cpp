/**
 * @file flag_guard_test.cpp
 * @brief Tests for AtomicFlagGuard and AtomicFlagResetGuard
 */

#include "utils/flag_guard.h"

#include <gtest/gtest.h>

#include <atomic>
#include <stdexcept>

namespace mygram::utils {
namespace {

// --- AtomicFlagGuard tests ---

TEST(AtomicFlagGuardTest, SetsFlagOnConstruction) {
  std::atomic<bool> flag{false};
  {
    AtomicFlagGuard guard(flag);
    EXPECT_TRUE(flag.load());
  }
}

TEST(AtomicFlagGuardTest, ResetsFlagOnDestruction) {
  std::atomic<bool> flag{false};
  {
    AtomicFlagGuard guard(flag);
    EXPECT_TRUE(flag.load());
  }
  EXPECT_FALSE(flag.load());
}

TEST(AtomicFlagGuardTest, ResetsFlagEvenWhenExceptionThrown) {
  std::atomic<bool> flag{false};
  try {
    AtomicFlagGuard guard(flag);
    EXPECT_TRUE(flag.load());
    throw std::runtime_error("test exception");
  } catch (const std::runtime_error&) {
    // Expected
  }
  EXPECT_FALSE(flag.load());
}

TEST(AtomicFlagGuardTest, WorksWithNestedScopes) {
  std::atomic<bool> flag{false};
  {
    AtomicFlagGuard outer(flag);
    EXPECT_TRUE(flag.load());
  }
  EXPECT_FALSE(flag.load());
  {
    AtomicFlagGuard inner(flag);
    EXPECT_TRUE(flag.load());
  }
  EXPECT_FALSE(flag.load());
}

// --- AtomicFlagResetGuard tests ---

TEST(AtomicFlagResetGuardTest, DoesNotSetFlagOnConstruction) {
  std::atomic<bool> flag{false};
  {
    AtomicFlagResetGuard guard(flag);
    EXPECT_FALSE(flag.load());
  }
}

TEST(AtomicFlagResetGuardTest, ResetsFlagOnDestruction) {
  std::atomic<bool> flag{true};
  {
    AtomicFlagResetGuard guard(flag);
    EXPECT_TRUE(flag.load());
  }
  EXPECT_FALSE(flag.load());
}

TEST(AtomicFlagResetGuardTest, ResetsFlagEvenWhenExceptionThrown) {
  std::atomic<bool> flag{true};
  try {
    AtomicFlagResetGuard guard(flag);
    EXPECT_TRUE(flag.load());
    throw std::runtime_error("test exception");
  } catch (const std::runtime_error&) {
    // Expected
  }
  EXPECT_FALSE(flag.load());
}

TEST(AtomicFlagResetGuardTest, TypicalCompareExchangePattern) {
  std::atomic<bool> flag{false};
  bool expected = false;
  ASSERT_TRUE(flag.compare_exchange_strong(expected, true));
  {
    AtomicFlagResetGuard guard(flag);
    EXPECT_TRUE(flag.load());
  }
  EXPECT_FALSE(flag.load());
}

}  // namespace
}  // namespace mygram::utils
