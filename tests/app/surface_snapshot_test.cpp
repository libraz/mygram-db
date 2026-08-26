/**
 * @file surface_snapshot_test.cpp
 * @brief Pins the rendered external surface against the checked-in golden file
 */

#include <gtest/gtest.h>

#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "app/surface_descriptor.h"

namespace {

/// Absolute path to the golden, injected by CMake so the test does not depend
/// on the working directory ctest happens to use.
constexpr const char* kSpecDir = MYGRAMDB_SPEC_DIR;
constexpr const char* kGoldenName = "surface.snapshot.txt";

std::string GoldenPath() {
  return std::string(kSpecDir) + "/" + kGoldenName;
}

std::vector<std::string> SplitLines(const std::string& text) {
  std::vector<std::string> lines;
  std::istringstream stream(text);
  std::string line;
  while (std::getline(stream, line)) {
    lines.push_back(line);
  }
  return lines;
}

std::string RegenerationHint() {
  return "A difference here means the server's static external surface changed. Decide whether that change is\n"
         "intended before regenerating the golden: an unintended difference is a behavioural regression, and an\n"
         "intended one needs the golden updated in the same change.\n"
         "Regenerate with: make surface-snapshot";
}

TEST(SurfaceSnapshotTest, MatchesGoldenFile) {
  std::ifstream golden(GoldenPath(), std::ios::binary);
  ASSERT_TRUE(golden.is_open()) << "Golden file not found at " << GoldenPath() << "\n" << RegenerationHint();

  std::ostringstream buffer;
  buffer << golden.rdbuf();
  const std::string expected = buffer.str();
  const std::string actual = mygramdb::app::RenderSurfaceSnapshot();

  if (expected == actual) {
    SUCCEED();
    return;
  }

  const std::vector<std::string> expected_lines = SplitLines(expected);
  const std::vector<std::string> actual_lines = SplitLines(actual);

  size_t index = 0;
  while (index < expected_lines.size() && index < actual_lines.size() && expected_lines[index] == actual_lines[index]) {
    ++index;
  }

  const std::string expected_line = index < expected_lines.size() ? expected_lines[index] : "<end of file>";
  const std::string actual_line = index < actual_lines.size() ? actual_lines[index] : "<end of file>";

  FAIL() << "Rendered surface differs from " << GoldenPath() << "\n"
         << "First difference at line " << (index + 1) << ":\n"
         << "  expected: " << expected_line << "\n"
         << "  actual:   " << actual_line << "\n"
         << "Golden has " << expected_lines.size() << " lines, rendering has " << actual_lines.size() << ".\n"
         << RegenerationHint();
}

TEST(SurfaceSnapshotTest, RenderingIsDeterministic) {
  EXPECT_EQ(mygramdb::app::RenderSurfaceSnapshot(), mygramdb::app::RenderSurfaceSnapshot());
}

}  // namespace
