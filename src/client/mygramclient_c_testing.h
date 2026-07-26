#pragma once

#include <atomic>
#include <cstdint>
#include <stdexcept>

namespace mygramdb::client::testing {

inline std::atomic<int64_t> c_string_allocations_before_failure{-1};

inline void SetCStringAllocationFailureCountdown(int64_t successful_allocations) {
  c_string_allocations_before_failure.store(successful_allocations);
}

inline bool ShouldFailCStringAllocation() {
  int64_t remaining = c_string_allocations_before_failure.load();
  while (remaining >= 0) {
    if (remaining == 0) {
      return true;
    }
    if (c_string_allocations_before_failure.compare_exchange_weak(remaining, remaining - 1)) {
      break;
    }
  }
  return false;
}

inline std::atomic<bool> throw_on_c_api_entry{false};

inline void SetThrowOnCApiEntry(bool enabled) {
  throw_on_c_api_entry.store(enabled);
}

inline void ThrowOnCApiEntryIfRequested() {
  if (throw_on_c_api_entry.load()) {
    throw std::runtime_error("injected C API exception");
  }
}

}  // namespace mygramdb::client::testing
