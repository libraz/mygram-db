/**
 * @file operation_coordinator.h
 * @brief Atomic admission control for long-running stateful operations
 */

#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>

namespace mygramdb::server {

enum class LongOperation : uint8_t {
  kSync,
  kDumpSave,
  kDumpLoad,
  kOptimize,
  kAutoSnapshot,
  kMysqlReconnect,
};

class OperationCoordinator {
 public:
  class Token {
   public:
    Token() = default;
    ~Token() { Release(); }

    Token(const Token&) = delete;
    Token& operator=(const Token&) = delete;

    Token(Token&& other) noexcept { MoveFrom(other); }
    Token& operator=(Token&& other) noexcept {
      if (this != &other) {
        Release();
        MoveFrom(other);
      }
      return *this;
    }

    [[nodiscard]] bool engaged() const { return owner_ != nullptr; }
    void Release();

   private:
    friend class OperationCoordinator;
    Token(OperationCoordinator* owner, uint64_t generation) : owner_(owner), generation_(generation) {}

    void MoveFrom(Token& other) noexcept {
      owner_ = other.owner_;
      generation_ = other.generation_;
      other.owner_ = nullptr;
      other.generation_ = 0;
    }

    OperationCoordinator* owner_ = nullptr;
    uint64_t generation_ = 0;
  };

  struct ActiveOperation {
    LongOperation type;
    std::string detail;
  };

  /**
   * All listed operations are intentionally mutually exclusive. They either
   * pause replication, serialize/replace live stores, or rebuild index state;
   * allowing any pair to overlap would expose a partial snapshot or race two
   * writers. Read-only status/verify commands do not enter this coordinator.
   */
  [[nodiscard]] std::optional<Token> TryAcquire(LongOperation type, std::string detail = {});

  [[nodiscard]] std::optional<ActiveOperation> GetActive() const;
  [[nodiscard]] std::string DescribeActive() const;

  /** Prevent all new long-running work while allowing an existing token to drain. */
  void BlockNewOperationsForShutdown();

  static const char* Name(LongOperation type);

 private:
  void Release(uint64_t generation);

  mutable std::mutex mutex_;
  std::optional<ActiveOperation> active_;
  uint64_t active_generation_ = 0;
  uint64_t next_generation_ = 1;
  bool shutdown_blocked_ = false;
};

}  // namespace mygramdb::server
