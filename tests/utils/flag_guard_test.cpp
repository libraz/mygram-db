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

// --- OperationGuard tests (Phase 4 H-D1) ---

TEST(OperationGuardTest, TryAcquireOnFalseFlagReturnsEngagedAndSetsFlag) {
  std::atomic<bool> flag{false};
  auto guard = OperationGuard::TryAcquire(flag);
  EXPECT_TRUE(guard.engaged());
  EXPECT_TRUE(flag.load()) << "TryAcquire must atomically set the flag on success";
}

TEST(OperationGuardTest, TryAcquireOnTrueFlagReturnsDisengaged) {
  std::atomic<bool> flag{true};
  auto guard = OperationGuard::TryAcquire(flag);
  EXPECT_FALSE(guard.engaged()) << "concurrent acquire must report disengaged";
  EXPECT_TRUE(flag.load()) << "flag must remain unchanged on failure";
}

TEST(OperationGuardTest, EngagedGuardResetsFlagOnScopeExit) {
  std::atomic<bool> flag{false};
  {
    auto guard = OperationGuard::TryAcquire(flag);
    ASSERT_TRUE(guard.engaged());
  }
  EXPECT_FALSE(flag.load()) << "destructor must reset the flag";
}

TEST(OperationGuardTest, DisengagedGuardDoesNotTouchFlag) {
  std::atomic<bool> flag{true};
  {
    auto guard = OperationGuard::TryAcquire(flag);
    ASSERT_FALSE(guard.engaged());
  }
  EXPECT_TRUE(flag.load()) << "disengaged guard must not reset a flag it does not own";
}

TEST(OperationGuardTest, ExplicitReleaseClearsFlagAndDisengages) {
  std::atomic<bool> flag{false};
  auto guard = OperationGuard::TryAcquire(flag);
  ASSERT_TRUE(guard.engaged());

  guard.Release();
  EXPECT_FALSE(flag.load()) << "explicit Release() must reset the flag";
  EXPECT_FALSE(guard.engaged()) << "guard becomes disengaged after Release()";
}

TEST(OperationGuardTest, DoubleReleaseIsNoOp) {
  std::atomic<bool> flag{false};
  auto guard = OperationGuard::TryAcquire(flag);
  ASSERT_TRUE(guard.engaged());

  guard.Release();
  flag.store(true);  // Simulate the flag being legitimately re-acquired by another caller.
  guard.Release();   // Must NOT touch the flag — guard is already disengaged.
  EXPECT_TRUE(flag.load()) << "second Release() must not touch a flag that the guard no longer owns";
}

TEST(OperationGuardTest, MoveConstructionTransfersOwnership) {
  std::atomic<bool> flag{false};
  auto first = OperationGuard::TryAcquire(flag);
  ASSERT_TRUE(first.engaged());

  {
    OperationGuard second(std::move(first));
    EXPECT_TRUE(second.engaged());
    EXPECT_FALSE(first.engaged())  // NOLINT(bugprone-use-after-move) — testing post-move state
        << "source must report disengaged so its dtor does not double-release";
    EXPECT_TRUE(flag.load());
  }

  EXPECT_FALSE(flag.load()) << "second's dtor must release; first's must be a no-op";
}

TEST(OperationGuardTest, FailedAcquireDoesNotResetOnDestruction) {
  // Regression guard: a disengaged guard must NEVER reset the flag, even if
  // some unrelated code resets the flag during the guard's lifetime. This
  // protects the "another DUMP is in progress" branch in handlers from
  // ever clobbering the legitimate holder's state.
  std::atomic<bool> flag{true};
  {
    auto guard = OperationGuard::TryAcquire(flag);
    ASSERT_FALSE(guard.engaged());
    EXPECT_TRUE(flag.load());
  }
  EXPECT_TRUE(flag.load());
}

TEST(OperationGuardTest, DefaultConstructedGuardIsDisengaged) {
  // Useful as a placeholder for code that conditionally acquires later.
  OperationGuard guard;
  EXPECT_FALSE(guard.engaged());
  // dtor must be a no-op — nothing to verify beyond "no crash".
}

TEST(OperationGuardTest, DismissDisengagesWithoutClearingFlag) {
  // Regression for the DUMP SAVE async path: when ownership of the held
  // flag is being transferred to a worker thread (which has its own RAII
  // reset at the end of its work), the outer guard must Dismiss(),
  // NOT Release(). Release() would clear the flag immediately and let a
  // concurrent client slip through compare_exchange before the worker
  // observed the held state.
  std::atomic<bool> flag{false};
  {
    auto guard = OperationGuard::TryAcquire(flag);
    ASSERT_TRUE(guard.engaged());
    ASSERT_TRUE(flag.load());

    guard.Dismiss();
    EXPECT_FALSE(guard.engaged()) << "Dismiss() must disengage the guard";
    EXPECT_TRUE(flag.load()) << "Dismiss() must NOT clear the flag (transfers ownership elsewhere)";
  }
  EXPECT_TRUE(flag.load()) << "destructor of a Dismiss()'d guard must not touch the flag";
}

TEST(OperationGuardTest, DismissAfterReleaseIsNoOp) {
  std::atomic<bool> flag{false};
  auto guard = OperationGuard::TryAcquire(flag);
  ASSERT_TRUE(guard.engaged());

  guard.Release();
  ASSERT_FALSE(flag.load());

  guard.Dismiss();  // Already disengaged; must not touch the flag.
  EXPECT_FALSE(flag.load());
}

}  // namespace
}  // namespace mygram::utils
