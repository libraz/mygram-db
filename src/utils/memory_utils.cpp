/**
 * @file memory_utils.cpp
 * @brief Memory health check and monitoring utilities implementation
 */

#include "utils/memory_utils.h"

#include <spdlog/spdlog.h>

#include <array>
#include <cmath>
#include <cstdio>
#include <fstream>
#include <iomanip>
#include <sstream>

#ifdef __APPLE__
#include <mach/mach.h>
#include <mach/mach_host.h>
#include <mach/vm_statistics.h>
#include <sys/sysctl.h>
#include <sys/types.h>
#elif __linux__
#include <sys/resource.h>
#include <sys/sysinfo.h>
#include <unistd.h>
#endif

namespace mygramdb::utils {

namespace {

// Safety thresholds for memory health status
constexpr double kHealthyThreshold = 0.2;  // 20% available = healthy
constexpr double kWarningThreshold = 0.1;  // 10% available = warning
// Below 10% = critical

// Bytes conversion constants
constexpr uint64_t kBytesPerKB = 1024;
constexpr uint64_t kBytesPerMB = 1024 * 1024;
constexpr uint64_t kBytesPerGB = 1024 * 1024 * 1024;

}  // namespace

std::optional<SystemMemoryInfo> GetSystemMemoryInfo() {
  SystemMemoryInfo info{};

#ifdef __APPLE__
  // Get total physical memory
  // C-style array required by macOS sysctl() API
  int mib[2] = {CTL_HW, HW_MEMSIZE};  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
  uint64_t physical_memory = 0;
  size_t length = sizeof(physical_memory);
  // Array decay required by macOS sysctl() system call
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
  if (sysctl(mib, 2, &physical_memory, &length, nullptr, 0) != 0) {
    spdlog::error("Failed to get total physical memory (sysctl)");
    return std::nullopt;
  }
  info.total_physical_bytes = physical_memory;

  // Get VM statistics for available memory
  mach_port_t host_port = mach_host_self();
  vm_size_t page_size = 0;
  host_page_size(host_port, &page_size);

  vm_statistics64_data_t vm_stats{};
  mach_msg_type_number_t count = HOST_VM_INFO64_COUNT;
  // kern_return_t is standard Mach API naming; reinterpret_cast required by Mach API
  // NOLINTNEXTLINE(readability-identifier-length)
  kern_return_t kern_ret = host_statistics64(host_port, HOST_VM_INFO64,
                                             // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
                                             reinterpret_cast<host_info64_t>(&vm_stats), &count);

  if (kern_ret != KERN_SUCCESS) {
    spdlog::error("Failed to get VM statistics");
    return std::nullopt;
  }

  // Available = free + inactive pages
  uint64_t free_pages = vm_stats.free_count;
  uint64_t inactive_pages = vm_stats.inactive_count;
  info.available_physical_bytes = (free_pages + inactive_pages) * page_size;

  // macOS swap info (from swapusage sysctl)
  struct xsw_usage swap_info {};
  size_t swap_size = sizeof(swap_info);
  // C-style array required by macOS sysctl() API
  int swap_mib[2] = {CTL_VM, VM_SWAPUSAGE};  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
  // Array decay required by macOS sysctl() system call
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-array-to-pointer-decay)
  if (sysctl(swap_mib, 2, &swap_info, &swap_size, nullptr, 0) == 0) {
    info.total_swap_bytes = swap_info.xsu_total;
    info.available_swap_bytes = swap_info.xsu_avail;
  } else {
    info.total_swap_bytes = 0;
    info.available_swap_bytes = 0;
  }

#elif __linux__
  // Read /proc/meminfo for detailed memory information
  std::ifstream meminfo("/proc/meminfo");
  if (!meminfo) {
    spdlog::error("Failed to open /proc/meminfo");
    return std::nullopt;
  }

  std::string line;
  while (std::getline(meminfo, line)) {
    std::istringstream iss(line);
    std::string key;
    uint64_t value = 0;
    std::string unit;

    iss >> key >> value >> unit;

    // Convert kB to bytes
    value *= kBytesPerKB;

    if (key == "MemTotal:") {
      info.total_physical_bytes = value;
    } else if (key == "MemAvailable:") {
      info.available_physical_bytes = value;
    } else if (key == "SwapTotal:") {
      info.total_swap_bytes = value;
    } else if (key == "SwapFree:") {
      info.available_swap_bytes = value;
    }
  }

  // Validate we got the essential information
  if (info.total_physical_bytes == 0) {
    spdlog::error("Failed to parse total physical memory from /proc/meminfo");
    return std::nullopt;
  }

#else
  spdlog::error("Unsupported platform for memory info");
  return std::nullopt;
#endif

  return info;
}

std::optional<ProcessMemoryInfo> GetProcessMemoryInfo() {
  ProcessMemoryInfo info{};

#ifdef __APPLE__
  // Get task info for current process
  struct task_basic_info_64 task_basic_info {};
  mach_msg_type_number_t count = TASK_BASIC_INFO_64_COUNT;
  // kern_return_t is standard Mach API naming; reinterpret_cast required by Mach task_info() API
  // NOLINTNEXTLINE(readability-identifier-length,cppcoreguidelines-pro-type-reinterpret-cast)
  kern_return_t kern_ret =
      task_info(mach_task_self(), TASK_BASIC_INFO_64,
                reinterpret_cast<task_info_t>(&task_basic_info),  // NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
                &count);

  if (kern_ret != KERN_SUCCESS) {
    spdlog::error("Failed to get task info");
    return std::nullopt;
  }

  info.rss_bytes = task_basic_info.resident_size;
  info.virtual_bytes = task_basic_info.virtual_size;

  // Peak RSS from rusage
  struct rusage usage {};
  if (getrusage(RUSAGE_SELF, &usage) == 0) {
    // ru_maxrss is in bytes on macOS
    info.peak_rss_bytes = static_cast<uint64_t>(usage.ru_maxrss);
  } else {
    info.peak_rss_bytes = info.rss_bytes;
  }

#elif __linux__
  // Read /proc/self/status for memory info
  std::ifstream status("/proc/self/status");
  if (!status) {
    spdlog::error("Failed to open /proc/self/status");
    return std::nullopt;
  }

  std::string line;
  while (std::getline(status, line)) {
    std::istringstream iss(line);
    std::string key;
    uint64_t value = 0;
    std::string unit;

    iss >> key >> value >> unit;

    // Convert kB to bytes
    value *= kBytesPerKB;

    if (key == "VmRSS:") {
      info.rss_bytes = value;
    } else if (key == "VmSize:") {
      info.virtual_bytes = value;
    } else if (key == "VmHWM:") {
      info.peak_rss_bytes = value;
    }
  }

  // Validate we got essential information
  if (info.rss_bytes == 0) {
    spdlog::error("Failed to parse RSS from /proc/self/status");
    return std::nullopt;
  }

#else
  spdlog::error("Unsupported platform for process memory info");
  return std::nullopt;
#endif

  return info;
}

bool CheckMemoryAvailability(uint64_t required_bytes, double safety_margin_ratio) {
  auto system_info = GetSystemMemoryInfo();
  if (!system_info) {
    spdlog::warn("Unable to check memory availability, allowing operation");
    return true;  // Fail-open: allow operation if we can't check
  }

  // Calculate required bytes with safety margin
  auto required_with_margin = static_cast<uint64_t>(static_cast<double>(required_bytes) * (1.0 + safety_margin_ratio));

  // Check if available physical memory is sufficient
  if (system_info->available_physical_bytes < required_with_margin) {
    spdlog::warn("Insufficient memory: required={} ({} with margin), available={}", FormatBytes(required_bytes),
                 FormatBytes(required_with_margin), FormatBytes(system_info->available_physical_bytes));
    return false;
  }

  return true;
}

MemoryHealthStatus GetMemoryHealthStatus() {
  auto system_info = GetSystemMemoryInfo();
  if (!system_info) {
    return MemoryHealthStatus::UNKNOWN;
  }

  // Calculate available memory ratio
  double available_ratio = static_cast<double>(system_info->available_physical_bytes) /
                           static_cast<double>(system_info->total_physical_bytes);

  if (available_ratio >= kHealthyThreshold) {
    return MemoryHealthStatus::HEALTHY;
  }
  if (available_ratio >= kWarningThreshold) {
    return MemoryHealthStatus::WARNING;
  }
  return MemoryHealthStatus::CRITICAL;
}

std::string MemoryHealthStatusToString(MemoryHealthStatus status) {
  switch (status) {
    case MemoryHealthStatus::HEALTHY:
      return "HEALTHY";
    case MemoryHealthStatus::WARNING:
      return "WARNING";
    case MemoryHealthStatus::CRITICAL:
      return "CRITICAL";
    case MemoryHealthStatus::UNKNOWN:
      return "UNKNOWN";
  }
  return "UNKNOWN";
}

std::string FormatBytes(uint64_t bytes) {
  std::ostringstream oss;
  oss << std::fixed << std::setprecision(2);

  if (bytes >= kBytesPerGB) {
    oss << static_cast<double>(bytes) / static_cast<double>(kBytesPerGB) << " GB";
  } else if (bytes >= kBytesPerMB) {
    oss << static_cast<double>(bytes) / static_cast<double>(kBytesPerMB) << " MB";
  } else if (bytes >= kBytesPerKB) {
    oss << static_cast<double>(bytes) / static_cast<double>(kBytesPerKB) << " KB";
  } else {
    oss << bytes << " B";
  }

  return oss.str();
}

uint64_t EstimateOptimizationMemory(uint64_t index_memory_usage, size_t batch_size) {
  // Optimization creates clones of posting lists in batches
  // Peak memory usage occurs when:
  // 1. Original index is fully loaded
  // 2. One batch worth of cloned posting lists is being created
  // 3. Temporary data structures for batch processing
  //
  // Memory breakdown:
  // - Original index: index_memory_usage
  // - Cloned batch (worst case): (batch_size / total_terms) * index_memory_usage
  // - Temporary overhead: ~10% of batch memory
  //
  // Conservative estimate: assume average term size
  // Typical batch represents ~1-5% of total index for default batch size (1000)

  if (batch_size == 0 || index_memory_usage == 0) {
    return 0;
  }

  // Estimate batch represents 5% of index (conservative)
  constexpr double kBatchRatio = 0.05;
  auto batch_memory = static_cast<uint64_t>(static_cast<double>(index_memory_usage) * kBatchRatio);

  // Add 10% overhead for temporary structures
  constexpr double kOverheadRatio = 0.10;
  auto overhead = static_cast<uint64_t>(static_cast<double>(batch_memory) * kOverheadRatio);

  // Total peak = original + batch + overhead
  return index_memory_usage + batch_memory + overhead;
}

}  // namespace mygramdb::utils
