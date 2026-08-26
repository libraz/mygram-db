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
#include <limits>
#include <memory>
#include <sstream>

#include "utils/string_utils.h"
#include "utils/structured_log.h"

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
    mygram::utils::StructuredLog()
        .Event("memory_error")
        .Field("type", "sysctl_failed")
        .Field("operation", "get_total_physical_memory")
        .Field("platform", "macos")
        .Error();
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
    mygram::utils::StructuredLog()
        .Event("memory_error")
        .Field("type", "vm_statistics_failed")
        .Field("operation", "host_statistics64")
        .Field("platform", "macos")
        .Error();
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
  constexpr uint64_t kBytesPerKB = 1024ULL;
  std::ifstream meminfo("/proc/meminfo");
  if (!meminfo) {
    mygram::utils::StructuredLog()
        .Event("memory_error")
        .Field("type", "file_open_failed")
        .Field("file", "/proc/meminfo")
        .Field("platform", "linux")
        .Error();
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
    mygram::utils::StructuredLog()
        .Event("memory_error")
        .Field("type", "parse_failed")
        .Field("field", "total_physical_memory")
        .Field("file", "/proc/meminfo")
        .Field("platform", "linux")
        .Error();
    return std::nullopt;
  }

#else
  mygram::utils::StructuredLog()
      .Event("memory_error")
      .Field("type", "unsupported_platform")
      .Field("operation", "get_system_memory_info")
      .Error();
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
    mygram::utils::StructuredLog()
        .Event("memory_error")
        .Field("type", "task_info_failed")
        .Field("operation", "task_info")
        .Field("platform", "macos")
        .Error();
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
  constexpr uint64_t kBytesPerKB = 1024ULL;
  std::ifstream status("/proc/self/status");
  if (!status) {
    mygram::utils::StructuredLog()
        .Event("memory_error")
        .Field("type", "file_open_failed")
        .Field("file", "/proc/self/status")
        .Field("platform", "linux")
        .Error();
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
    mygram::utils::StructuredLog()
        .Event("memory_error")
        .Field("type", "parse_failed")
        .Field("field", "rss")
        .Field("file", "/proc/self/status")
        .Field("platform", "linux")
        .Error();
    return std::nullopt;
  }

#else
  mygram::utils::StructuredLog()
      .Event("memory_error")
      .Field("type", "unsupported_platform")
      .Field("operation", "get_process_memory_info")
      .Error();
  return std::nullopt;
#endif

  return info;
}

bool CheckMemoryAvailability(uint64_t required_bytes, double safety_margin_ratio) {
  auto system_info = GetSystemMemoryInfo();
  if (!system_info) {
    mygram::utils::StructuredLog().Event("memory_check_unavailable").Field("action", "allowing operation").Warn();
    return true;  // Fail-open: allow operation if we can't check
  }

  // Calculate required bytes with safety margin
  auto required_with_margin = static_cast<uint64_t>(static_cast<double>(required_bytes) * (1.0 + safety_margin_ratio));

  // Check if available physical memory is sufficient
  if (system_info->available_physical_bytes < required_with_margin) {
    mygram::utils::StructuredLog()
        .Event("memory_insufficient")
        .Field("required", FormatBytes(required_bytes))
        .Field("required_with_margin", FormatBytes(required_with_margin))
        .Field("available", FormatBytes(system_info->available_physical_bytes))
        .Warn();
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

uint64_t EstimateOptimizationMemory(uint64_t index_memory_usage, size_t batch_size, uint64_t term_count) {
  if (batch_size == 0 || index_memory_usage == 0) {
    return 0;
  }

  // Optimization rebuilds every posting list into a fresh allocation and
  // releases the original once the replacement is published. Batching bounds
  // how many replacements are live at one time, but it does not bound the
  // footprint: a rebuilt list rarely fits the hole its original left, so a
  // first pass grows the resident set by close to a second copy of the index
  // whatever batch size it runs with. The rebuild is therefore charged in full.
  const uint64_t rebuild_memory = index_memory_usage;

  // Temporary structures around the rebuild.
  constexpr double kOverheadRatio = 0.10;
  auto overhead = static_cast<uint64_t>(static_cast<double>(rebuild_memory) * kOverheadRatio);

  // The term-name snapshot taken before the batch loop is proportional to the
  // term count, not to posting-list memory: one offset plus the term bytes for
  // every distinct term in the index.
  constexpr uint64_t kAverageTermBytes = 8;
  constexpr uint64_t kBytesPerSnapshottedTerm = sizeof(size_t) + kAverageTermBytes;
  const uint64_t term_snapshot_memory = term_count > std::numeric_limits<uint64_t>::max() / kBytesPerSnapshottedTerm
                                            ? std::numeric_limits<uint64_t>::max()
                                            : term_count * kBytesPerSnapshottedTerm;

  // Each batch also holds, per term, the captured version plus the snapshot and
  // optimized posting-list handles.
  constexpr uint64_t kBytesPerBatchedTerm = sizeof(uint64_t) + (2 * sizeof(std::shared_ptr<void>));
  const uint64_t batch_bookkeeping_memory = static_cast<uint64_t>(batch_size) * kBytesPerBatchedTerm;

  // Total peak = the index that is already resident + the rebuild + the
  // structures that carry it. Saturate rather than wrap: an estimate that
  // overflows would read as a small number and admit the very run it must
  // refuse.
  uint64_t total = index_memory_usage;
  for (const uint64_t component : {rebuild_memory, overhead, term_snapshot_memory, batch_bookkeeping_memory}) {
    if (total > std::numeric_limits<uint64_t>::max() - component) {
      return std::numeric_limits<uint64_t>::max();
    }
    total += component;
  }
  return total;
}

}  // namespace mygramdb::utils
