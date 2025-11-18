/**
 * @file fd_guard.h
 * @brief RAII guard for file descriptors to prevent leaks
 */

#pragma once

#include <unistd.h>

namespace mygramdb::utils {

/**
 * @brief RAII guard for file descriptors
 *
 * Automatically closes a file descriptor when the guard goes out of scope,
 * unless explicitly released. This prevents FD leaks in exception scenarios.
 *
 * Usage:
 * @code
 * void HandleConnection(int fd) {
 *   FDGuard guard{fd};
 *   // ... operations that might throw ...
 *   guard.Release();  // Success - ownership transferred elsewhere
 * }
 * @endcode
 */
class FDGuard {
 public:
  /**
   * @brief Construct a guard for the given file descriptor
   * @param file_descriptor File descriptor to guard (-1 for invalid)
   */
  explicit FDGuard(int file_descriptor = -1) : fd_(file_descriptor) {}

  /**
   * @brief Destructor - closes FD if not released
   */
  ~FDGuard() {
    if (fd_ >= 0 && !released_) {
      close(fd_);
    }
  }

  // Disable copy
  FDGuard(const FDGuard&) = delete;
  FDGuard& operator=(const FDGuard&) = delete;

  // Enable move
  FDGuard(FDGuard&& other) noexcept : fd_(other.fd_), released_(other.released_) {
    other.fd_ = -1;
    other.released_ = true;
  }

  FDGuard& operator=(FDGuard&& other) noexcept {
    if (this != &other) {
      // Close current FD if owned
      if (fd_ >= 0 && !released_) {
        close(fd_);
      }
      fd_ = other.fd_;
      released_ = other.released_;
      other.fd_ = -1;
      other.released_ = true;
    }
    return *this;
  }

  /**
   * @brief Release ownership of the FD (won't be closed on destruction)
   */
  void Release() { released_ = true; }

  /**
   * @brief Get the file descriptor
   */
  [[nodiscard]] int Get() const { return fd_; }

 private:
  int fd_ = -1;
  bool released_ = false;
};

/**
 * @brief RAII guard for generic cleanup actions
 *
 * Executes a cleanup function when the guard goes out of scope,
 * unless explicitly released. Useful for ensuring cleanup happens
 * even in exception scenarios.
 *
 * Usage:
 * @code
 * void SomeFunction() {
 *   stats.Increment();
 *   ScopeGuard guard([&stats]() { stats.Decrement(); });
 *   // ... operations that might throw ...
 *   guard.Release();  // Success - don't decrement
 * }
 * @endcode
 */
template <typename CleanupFunc>
class ScopeGuard {
 public:
  /**
   * @brief Construct a guard with a cleanup function
   * @param cleanup Function to call on destruction
   */
  explicit ScopeGuard(CleanupFunc cleanup) : cleanup_(std::move(cleanup)) {}

  /**
   * @brief Destructor - calls cleanup function if not released
   */
  ~ScopeGuard() {
    if (!released_) {
      cleanup_();
    }
  }

  // Disable copy
  ScopeGuard(const ScopeGuard&) = delete;
  ScopeGuard& operator=(const ScopeGuard&) = delete;

  // Enable move
  ScopeGuard(ScopeGuard&& other) noexcept : cleanup_(std::move(other.cleanup_)), released_(other.released_) {
    other.released_ = true;
  }

  ScopeGuard& operator=(ScopeGuard&& other) noexcept {
    if (this != &other) {
      if (!released_) {
        cleanup_();
      }
      cleanup_ = std::move(other.cleanup_);
      released_ = other.released_;
      other.released_ = true;
    }
    return *this;
  }

  /**
   * @brief Release the guard (cleanup won't be called)
   */
  void Release() { released_ = true; }

 private:
  CleanupFunc cleanup_;
  bool released_ = false;
};

}  // namespace mygramdb::utils
