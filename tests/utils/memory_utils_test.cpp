/**
 * @file memory_utils_test.cpp
 * @brief Unit tests for memory utilities
 */

#include "utils/memory_utils.h"

#include <gtest/gtest.h>

using namespace mygramdb::utils;

/**
 * @brief Test getting system memory info
 */
TEST(MemoryUtilsTest, GetSystemMemoryInfo) {
  auto info = GetSystemMemoryInfo();
  ASSERT_TRUE(info.has_value());

  // Verify reasonable values
  EXPECT_GT(info->total_physical_bytes, 0);
  EXPECT_LE(info->available_physical_bytes, info->total_physical_bytes);

  // Total physical memory should be at least 1GB on modern systems
  EXPECT_GE(info->total_physical_bytes, 1024ULL * 1024 * 1024);
}

/**
 * @brief Test getting process memory info
 */
TEST(MemoryUtilsTest, GetProcessMemoryInfo) {
  auto info = GetProcessMemoryInfo();
  ASSERT_TRUE(info.has_value());

  // Verify reasonable values
  EXPECT_GT(info->rss_bytes, 0);
  EXPECT_GT(info->virtual_bytes, 0);
  EXPECT_GE(info->peak_rss_bytes, info->rss_bytes);

  // RSS should be less than total virtual memory
  EXPECT_LE(info->rss_bytes, info->virtual_bytes);
}

/**
 * @brief Test memory availability check
 */
TEST(MemoryUtilsTest, CheckMemoryAvailability) {
  // Small allocation should always succeed
  EXPECT_TRUE(CheckMemoryAvailability(1024));         // 1KB
  EXPECT_TRUE(CheckMemoryAvailability(1024 * 1024));  // 1MB

  // Extremely large allocation should fail
  uint64_t huge_size = 1000ULL * 1024 * 1024 * 1024;  // 1000 GB
  EXPECT_FALSE(CheckMemoryAvailability(huge_size));
}

/**
 * @brief Test memory health status
 */
TEST(MemoryUtilsTest, GetMemoryHealthStatus) {
  auto status = GetMemoryHealthStatus();

  // Status should not be UNKNOWN on supported platforms
  EXPECT_NE(status, MemoryHealthStatus::UNKNOWN);

  // On development machine, should typically be HEALTHY or WARNING
  EXPECT_TRUE(status == MemoryHealthStatus::HEALTHY || status == MemoryHealthStatus::WARNING ||
              status == MemoryHealthStatus::CRITICAL);
}

/**
 * @brief Test memory health status to string conversion
 */
TEST(MemoryUtilsTest, MemoryHealthStatusToString) {
  EXPECT_EQ(MemoryHealthStatusToString(MemoryHealthStatus::HEALTHY), "HEALTHY");
  EXPECT_EQ(MemoryHealthStatusToString(MemoryHealthStatus::WARNING), "WARNING");
  EXPECT_EQ(MemoryHealthStatusToString(MemoryHealthStatus::CRITICAL), "CRITICAL");
  EXPECT_EQ(MemoryHealthStatusToString(MemoryHealthStatus::UNKNOWN), "UNKNOWN");
}

/**
 * @brief Test bytes formatting
 */
TEST(MemoryUtilsTest, FormatBytes) {
  // Note: FormatBytes uses setprecision(2), but values < 1KB don't have decimal places
  EXPECT_EQ(FormatBytes(0), "0 B");
  EXPECT_EQ(FormatBytes(512), "512 B");
  EXPECT_EQ(FormatBytes(1024), "1.00 KB");
  EXPECT_EQ(FormatBytes(1536), "1.50 KB");
  EXPECT_EQ(FormatBytes(1024 * 1024), "1.00 MB");
  EXPECT_EQ(FormatBytes(1024ULL * 1024 * 1024), "1.00 GB");
  EXPECT_EQ(FormatBytes(2560ULL * 1024 * 1024), "2.50 GB");
}

/**
 * @brief Test optimization memory estimation
 */
TEST(MemoryUtilsTest, EstimateOptimizationMemory) {
  uint64_t index_size = 100 * 1024 * 1024;  // 100 MB
  size_t batch_size = 1000;

  uint64_t estimated = EstimateOptimizationMemory(index_size, batch_size);

  // Estimated memory should be greater than original index
  EXPECT_GT(estimated, index_size);

  // But not excessively large (should be < 2x original for typical batch sizes)
  EXPECT_LT(estimated, index_size * 2);

  // Zero inputs should return zero
  EXPECT_EQ(EstimateOptimizationMemory(0, 1000), 0);
  EXPECT_EQ(EstimateOptimizationMemory(100000, 0), 0);
}

/**
 * @brief Test memory info consistency
 */
TEST(MemoryUtilsTest, MemoryInfoConsistency) {
  auto sys_info = GetSystemMemoryInfo();
  auto proc_info = GetProcessMemoryInfo();

  ASSERT_TRUE(sys_info.has_value());
  ASSERT_TRUE(proc_info.has_value());

  // Process RSS should not exceed total system memory
  EXPECT_LE(proc_info->rss_bytes, sys_info->total_physical_bytes);

  // Process virtual memory should be greater than or equal to RSS
  EXPECT_GE(proc_info->virtual_bytes, proc_info->rss_bytes);
}
