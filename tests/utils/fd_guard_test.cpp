/**
 * @file fd_guard_test.cpp
 * @brief Unit tests for FDGuard and ScopeGuard RAII utilities
 */

#include "utils/fd_guard.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <functional>
#include <utility>

using namespace mygramdb::utils;

// Helper type alias for ScopeGuard tests
using ScopeGuardFunc = ScopeGuard<std::function<void()>>;

// --- FDGuard Tests ---

TEST(FDGuardTest, ClosesOnDestruction) {
  int fds[2];
  ASSERT_EQ(pipe(fds), 0);

  // Guard should close the read end on destruction
  { FDGuard guard(fds[0]); }

  // Writing to the pipe should still work (write end is open)
  // But reading from fds[0] should fail because it was closed
  EXPECT_EQ(write(fds[0], "x", 1), -1);

  close(fds[1]);
}

TEST(FDGuardTest, ReleasePreventClose) {
  int fds[2];
  ASSERT_EQ(pipe(fds), 0);

  {
    FDGuard guard(fds[0]);
    guard.Release();
  }

  // fds[0] should still be valid after Release
  char buf = 0;
  // Write something and read it back to prove fds[0] is still open
  ASSERT_EQ(write(fds[1], "a", 1), 1);
  EXPECT_EQ(read(fds[0], &buf, 1), 1);
  EXPECT_EQ(buf, 'a');

  close(fds[0]);
  close(fds[1]);
}

TEST(FDGuardTest, MoveConstruct) {
  int fds[2];
  ASSERT_EQ(pipe(fds), 0);

  FDGuard guard1(fds[0]);
  FDGuard guard2(std::move(guard1));

  EXPECT_EQ(guard1.Get(), -1);
  EXPECT_EQ(guard2.Get(), fds[0]);

  close(fds[1]);
  // guard2 destructor will close fds[0]
}

TEST(FDGuardTest, MoveAssign) {
  int fds1[2];
  int fds2[2];
  ASSERT_EQ(pipe(fds1), 0);
  ASSERT_EQ(pipe(fds2), 0);

  FDGuard guard1(fds1[0]);
  FDGuard guard2(fds2[0]);

  // Move-assign guard1 into guard2; guard2's old FD (fds2[0]) should be closed
  guard2 = std::move(guard1);

  EXPECT_EQ(guard1.Get(), -1);
  EXPECT_EQ(guard2.Get(), fds1[0]);

  // fds2[0] should have been closed by the move assignment
  EXPECT_EQ(write(fds2[0], "x", 1), -1);

  close(fds1[1]);
  close(fds2[1]);
  // guard2 destructor will close fds1[0]
}

TEST(FDGuardTest, InvalidFDIsSafe) {
  // Constructing with -1 and letting it destruct should not crash
  FDGuard guard(-1);
  EXPECT_EQ(guard.Get(), -1);
}

// --- ScopeGuard Tests ---

TEST(ScopeGuardTest, ExecutesOnDestruction) {
  bool executed = false;
  {
    ScopeGuardFunc guard([&executed]() { executed = true; });
  }
  EXPECT_TRUE(executed);
}

TEST(ScopeGuardTest, ReleasePreventExecution) {
  bool executed = false;
  {
    ScopeGuardFunc guard([&executed]() { executed = true; });
    guard.Release();
  }
  EXPECT_FALSE(executed);
}

TEST(ScopeGuardTest, MoveSemantics) {
  int count = 0;
  {
    ScopeGuardFunc guard1([&count]() { count++; });
    ScopeGuardFunc guard2(std::move(guard1));
    // guard1 is released by the move, guard2 owns the cleanup
  }
  // Cleanup should have run exactly once
  EXPECT_EQ(count, 1);
}
