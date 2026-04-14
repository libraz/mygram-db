/**
 * @file index_serialization_test.cpp
 * @brief Regression tests for Index::LoadFromStream validation
 *
 * Tests that malformed index data is rejected with proper error codes
 * instead of causing excessive memory allocation or crashes.
 */

#include <gtest/gtest.h>

#include <cstring>
#include <sstream>
#include <string>
#include <vector>

#include "index/index.h"
#include "utils/endian_utils.h"
#include "utils/error.h"

using namespace mygramdb::index;
using mygram::utils::ErrorCode;

namespace {

/**
 * @brief Helper to build a raw V1 index stream with configurable term_len
 *
 * V1 format: [magic "MGIX"][version=1][ngram_size][term_count][term entries...]
 * Each term entry: [term_len][term_bytes][posting_size][posting_data]
 */
std::string BuildV1IndexWithTermLen(uint32_t term_len) {
  std::vector<uint8_t> data;

  auto append = [&](const void* ptr, size_t size) {
    const auto* bytes = static_cast<const uint8_t*>(ptr);
    data.insert(data.end(), bytes, bytes + size);
  };

  // Magic "MGIX"
  data.push_back('M');
  data.push_back('G');
  data.push_back('I');
  data.push_back('X');

  // Version = 1 (V1, no CRC32)
  uint32_t version = mygram::utils::ToLittleEndian(static_cast<uint32_t>(1));
  append(&version, sizeof(version));

  // ngram_size = 2
  uint32_t ngram = mygram::utils::ToLittleEndian(static_cast<uint32_t>(2));
  append(&ngram, sizeof(ngram));

  // term_count = 1
  uint64_t term_count = mygram::utils::ToLittleEndian(static_cast<uint64_t>(1));
  append(&term_count, sizeof(term_count));

  // term_len (the value under test)
  uint32_t tl = mygram::utils::ToLittleEndian(term_len);
  append(&tl, sizeof(tl));

  // Pad with enough zeros to prevent "truncated data" from triggering before
  // the term_len validation. We need term_len bytes for the term string,
  // plus 8 bytes for posting_size, plus some posting data.
  size_t padding = static_cast<size_t>(term_len) + 128;
  // Cap padding to avoid allocating too much memory in the test
  if (padding > 200000) {
    padding = 200000;
  }
  data.resize(data.size() + padding, 0);

  return std::string(data.begin(), data.end());
}

}  // namespace

/**
 * @brief Regression test: LoadFromStream rejects excessively large term_len
 *
 * A malformed index file with term_len > 10000 should return kStorageCorrupted
 * instead of attempting a multi-GB allocation.
 */
TEST(IndexSerializationTest, LoadFromStreamRejectsExcessiveTermLength) {
  // Build a V1 index stream with term_len=100000 (> kMaxTermLength=10000)
  std::string data = BuildV1IndexWithTermLen(100000);
  std::istringstream iss(data);

  Index index(2);
  auto result = index.LoadFromStream(iss);
  EXPECT_FALSE(result.has_value());
  // Should fail with kStorageCorrupted, not crash with OOM
  EXPECT_EQ(result.error().code(), ErrorCode::kStorageCorrupted);
}

/**
 * @brief Verify LoadFromStream accepts a valid term_len within bounds
 */
TEST(IndexSerializationTest, LoadFromStreamAcceptsValidTermLength) {
  // Build a V1 index stream with term_len=5 (well within limits)
  // This will still fail because the posting data is all zeros,
  // but it should NOT fail due to term_len validation.
  std::string data = BuildV1IndexWithTermLen(5);
  std::istringstream iss(data);

  Index index(2);
  auto result = index.LoadFromStream(iss);

  // The result may succeed or fail for other reasons (invalid posting data),
  // but if it fails, it should NOT be due to excessive term length.
  if (!result.has_value()) {
    EXPECT_EQ(result.error().message().find("exceeds maximum"), std::string::npos)
        << "Small term_len should not trigger the excessive length guard. "
        << "Actual error: " << result.error().message();
  }
}

/**
 * @brief Boundary test: term_len exactly at limit (10000) should be accepted
 */
TEST(IndexSerializationTest, LoadFromStreamTermLengthAtBoundary) {
  // term_len=10000 is exactly kMaxTermLength — should be accepted
  std::string data = BuildV1IndexWithTermLen(10000);
  std::istringstream iss(data);

  Index index(2);
  auto result = index.LoadFromStream(iss);

  // May fail for other reasons (malformed posting data), but not term length
  if (!result.has_value()) {
    EXPECT_EQ(result.error().message().find("exceeds maximum"), std::string::npos)
        << "term_len=10000 should not trigger the excessive length guard";
  }
}

/**
 * @brief Boundary test: term_len=10001 should be rejected
 */
TEST(IndexSerializationTest, LoadFromStreamTermLengthJustOverBoundary) {
  std::string data = BuildV1IndexWithTermLen(10001);
  std::istringstream iss(data);

  Index index(2);
  auto result = index.LoadFromStream(iss);
  EXPECT_FALSE(result.has_value());
  EXPECT_EQ(result.error().code(), ErrorCode::kStorageCorrupted);
}
