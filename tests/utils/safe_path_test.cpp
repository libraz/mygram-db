/**
 * @file safe_path_test.cpp
 * @brief Unit tests for ResolveSafePath
 */

#include "utils/safe_path.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <string>

namespace {

class SafePathTest : public ::testing::Test {
 protected:
  void SetUp() override {
    base_dir_ = std::filesystem::temp_directory_path() / ("safe_path_test_" + std::to_string(getpid()) + "_" +
                                                          std::to_string(reinterpret_cast<uintptr_t>(this)));
    std::filesystem::create_directories(base_dir_);

    // Pre-canonicalize so equality comparisons in the tests are stable on
    // platforms (macOS) where /tmp itself is a symlink.
    base_dir_ = std::filesystem::canonical(base_dir_);
  }

  void TearDown() override {
    std::error_code ec;
    std::filesystem::remove_all(base_dir_, ec);
  }

  // Create an empty regular file at the given relative path within base_dir_.
  std::filesystem::path TouchFile(const std::string& rel) const {
    auto path = base_dir_ / rel;
    std::filesystem::create_directories(path.parent_path());
    std::ofstream(path).close();
    return path;
  }

  std::filesystem::path base_dir_;
};

// Existing relative path within base_dir resolves correctly.
TEST_F(SafePathTest, ResolvesRelativePathInsideBaseDir) {
  TouchFile("snapshot.dat");
  auto result = mygram::utils::ResolveSafePath("snapshot.dat", base_dir_.string());
  ASSERT_TRUE(result) << result.error().message();
  EXPECT_EQ(*result, (base_dir_ / "snapshot.dat").string());
}

// A non-existing relative path is still resolved (weakly_canonical).
TEST_F(SafePathTest, ResolvesNonExistentRelativePath) {
  auto result = mygram::utils::ResolveSafePath("new_dump.dat", base_dir_.string());
  ASSERT_TRUE(result) << result.error().message();
  EXPECT_EQ(*result, (base_dir_ / "new_dump.dat").string());
}

// Absolute path inside base_dir is allowed.
TEST_F(SafePathTest, AcceptsAbsolutePathInsideBaseDir) {
  TouchFile("inside.dat");
  auto target = (base_dir_ / "inside.dat").string();
  auto result = mygram::utils::ResolveSafePath(target, base_dir_.string());
  ASSERT_TRUE(result) << result.error().message();
  EXPECT_EQ(*result, target);
}

// `..` traversal that escapes base_dir is rejected.
TEST_F(SafePathTest, RejectsParentTraversal) {
  auto result = mygram::utils::ResolveSafePath("../escape.dat", base_dir_.string());
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().message().find("must be within base directory"), std::string::npos);
}

// Absolute path outside base_dir is rejected.
TEST_F(SafePathTest, RejectsAbsolutePathOutsideBaseDir) {
  auto result = mygram::utils::ResolveSafePath("/etc/passwd", base_dir_.string());
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().message().find("must be within base directory"), std::string::npos);
}

// Empty input is rejected.
TEST_F(SafePathTest, RejectsEmptyInput) {
  auto result = mygram::utils::ResolveSafePath("", base_dir_.string());
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().message().find("Empty filepath"), std::string::npos);
}

// Empty base_dir is rejected.
TEST_F(SafePathTest, RejectsEmptyBaseDir) {
  auto result = mygram::utils::ResolveSafePath("foo.dat", "");
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().message().find("Empty base directory"), std::string::npos);
}

// Allowed extension passes.
TEST_F(SafePathTest, AllowsMatchingExtension) {
  TouchFile("config.yaml");
  auto result = mygram::utils::ResolveSafePath("config.yaml", base_dir_.string(), {".yaml", ".yml"});
  ASSERT_TRUE(result) << result.error().message();
}

// Disallowed extension is rejected.
TEST_F(SafePathTest, RejectsDisallowedExtension) {
  TouchFile("config.txt");
  auto result = mygram::utils::ResolveSafePath("config.txt", base_dir_.string(), {".yaml", ".yml"});
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().message().find("Disallowed file extension"), std::string::npos);
}

// File without extension is rejected when allowed_extensions is set.
TEST_F(SafePathTest, RejectsMissingExtensionWhenRequired) {
  auto result = mygram::utils::ResolveSafePath("config", base_dir_.string(), {".yaml", ".yml"});
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().message().find("Disallowed file extension"), std::string::npos);
}

// Extension match is case-insensitive.
TEST_F(SafePathTest, ExtensionMatchIsCaseInsensitive) {
  TouchFile("config.YAML");
  auto result = mygram::utils::ResolveSafePath("config.YAML", base_dir_.string(), {".yaml", ".yml"});
  ASSERT_TRUE(result) << result.error().message();
}

// Symlink whose target lies outside base_dir must be rejected (canonical()
// follows symlinks; lexically_relative then catches the escape).
TEST_F(SafePathTest, RejectsSymlinkPointingOutsideBaseDir) {
  // Create a target outside base_dir
  auto outside_dir = std::filesystem::temp_directory_path() / ("safe_path_outside_" + std::to_string(getpid()) + "_" +
                                                               std::to_string(reinterpret_cast<uintptr_t>(this)));
  std::filesystem::create_directories(outside_dir);
  outside_dir = std::filesystem::canonical(outside_dir);
  auto outside_target = outside_dir / "secret.dat";
  std::ofstream(outside_target).close();

  // Symlink inside base_dir pointing to the outside target
  auto link = base_dir_ / "link.dat";
  std::error_code ec;
  std::filesystem::create_symlink(outside_target, link, ec);
  if (ec) {
    GTEST_SKIP() << "Cannot create symlinks on this filesystem: " << ec.message();
  }

  auto result = mygram::utils::ResolveSafePath("link.dat", base_dir_.string());
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().message().find("must be within base directory"), std::string::npos);

  std::filesystem::remove_all(outside_dir, ec);
}

// Symlink whose target lies inside base_dir is accepted: canonical() returns
// the real path, and lexically_relative confirms it's within base_dir.
TEST_F(SafePathTest, AcceptsSymlinkResolvingInsideBaseDir) {
  auto target = TouchFile("real.dat");
  auto link = base_dir_ / "alias.dat";
  std::error_code ec;
  std::filesystem::create_symlink(target, link, ec);
  if (ec) {
    GTEST_SKIP() << "Cannot create symlinks on this filesystem: " << ec.message();
  }

  auto result = mygram::utils::ResolveSafePath("alias.dat", base_dir_.string());
  ASSERT_TRUE(result) << result.error().message();
  // canonical() resolves the link to its real target inside base_dir.
  EXPECT_EQ(*result, target.string());
}

// base_dir that does not exist surfaces a filesystem error.
TEST_F(SafePathTest, RejectsNonExistentBaseDir) {
  auto missing = base_dir_ / "definitely_not_present";
  auto result = mygram::utils::ResolveSafePath("foo.dat", missing.string());
  ASSERT_FALSE(result);
  EXPECT_NE(result.error().message().find("Invalid filepath"), std::string::npos);
}

}  // namespace
