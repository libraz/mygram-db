/**
 * @file memory_utils.h
 * @brief Memory health check and monitoring utilities
 */

#ifndef MYGRAMDB_UTILS_MEMORY_UTILS_H_
#define MYGRAMDB_UTILS_MEMORY_UTILS_H_

#include <cstdint>
#include <optional>
#include <string>

namespace mygramdb::utils {

/**
 * @brief System memory information
 */
struct SystemMemoryInfo {
  uint64_t total_physical_bytes;      ///< Total physical RAM
  uint64_t available_physical_bytes;  ///< Available physical RAM
  uint64_t total_swap_bytes;          ///< Total swap space
  uint64_t available_swap_bytes;      ///< Available swap space
};

/**
 * @brief Process memory usage information
 */
struct ProcessMemoryInfo {
  uint64_t rss_bytes;       ///< Resident Set Size (physical memory used)
  uint64_t virtual_bytes;   ///< Virtual memory size
  uint64_t peak_rss_bytes;  ///< Peak RSS (high water mark)
};

/**
 * @brief Memory health status
 */
enum class MemoryHealthStatus : std::uint8_t {
  HEALTHY,   ///< Sufficient memory available (>20%)
  WARNING,   ///< Memory running low (10-20%)
  CRITICAL,  ///< Memory critically low (<10%)
  UNKNOWN    ///< Unable to determine status
};

/**
 * @brief Get system memory information
 *
 * @return System memory info, or std::nullopt on error
 */
std::optional<SystemMemoryInfo> GetSystemMemoryInfo();

/**
 * @brief Get current process memory usage
 *
 * @return Process memory info, or std::nullopt on error
 */
std::optional<ProcessMemoryInfo> GetProcessMemoryInfo();

// Default safety margin for memory availability checks (10%)
inline constexpr double kDefaultMemorySafetyMargin = 0.1;

/**
 * @brief Check if specified amount of memory is likely available
 *
 * This function estimates whether the system can accommodate an additional
 * memory allocation of the specified size without causing OOM conditions.
 *
 * @param required_bytes Number of bytes to check for availability
 * @param safety_margin_ratio Safety margin ratio (default: 0.1 = 10%)
 * @return true if memory is likely available, false otherwise
 */
bool CheckMemoryAvailability(uint64_t required_bytes, double safety_margin_ratio = kDefaultMemorySafetyMargin);

/**
 * @brief Get current memory health status
 *
 * @return Memory health status enum
 */
MemoryHealthStatus GetMemoryHealthStatus();

/**
 * @brief Get human-readable string for memory health status
 *
 * @param status Memory health status
 * @return String representation ("HEALTHY", "WARNING", "CRITICAL", "UNKNOWN")
 */
std::string MemoryHealthStatusToString(MemoryHealthStatus status);

/**
 * @brief Estimate memory required for index optimization
 *
 * This estimates the peak memory usage during OptimizeInBatches operation.
 * The actual memory used depends on:
 * - Number of terms in the index
 * - Average posting list size
 * - Batch size parameter
 *
 * @param index_memory_usage Current index memory usage (bytes)
 * @param batch_size Batch size for optimization
 * @return Estimated peak memory requirement (bytes)
 */
uint64_t EstimateOptimizationMemory(uint64_t index_memory_usage, size_t batch_size);

}  // namespace mygramdb::utils

#endif  // MYGRAMDB_UTILS_MEMORY_UTILS_H_
