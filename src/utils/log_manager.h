/**
 * @file log_manager.h
 * @brief Sole owner of the process default spdlog logger.
 *
 * spdlog's free logging functions read the default logger through an
 * unsynchronized raw pointer, so replacing it while other threads log is a
 * data race that can free the logger under them. LogManager therefore
 * installs its file logger as the default once, and later reopens rotate the
 * file underneath that same logger by swapping the child sink of a
 * distribution sink under the sink mutex. Reopen and level changes share one
 * mutex so neither can undo the other.
 */

#pragma once

#include <spdlog/sinks/dist_sink.h>
#include <spdlog/spdlog.h>

#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "utils/error.h"
#include "utils/expected.h"
#include "utils/namespace_compat.h"

namespace mygramdb::utils {

/**
 * @brief Process-wide owner of the default logger and its level.
 *
 * Thread Safety: every member function is thread-safe. The default logger is
 * replaced only when it is not already the managed file logger, which in the
 * server happens once at startup before any other thread is running.
 */
class LogManager {
 public:
  /** @brief Return the process-wide instance. */
  static LogManager& Instance();

  /**
   * @brief Route the default logger to a size-rotated file at @p path.
   *
   * The first call installs the managed logger as the default, keeping the
   * current level. Later calls reopen @p path in place: the logger object
   * stays the same and no message is dropped.
   *
   * @param path Log file path (its directory must exist)
   * @return Expected with void, or kIOError if the file cannot be opened
   */
  Expected<void, Error> OpenFile(const std::string& path);

  /**
   * @brief Set the level of the default logger and every registered logger.
   * @param level New level
   */
  void SetLevel(spdlog::level::level_enum level);

  /**
   * @brief Parse a configuration level name (debug/info/warn/error).
   * @param name Level name
   * @return Level, or std::nullopt for an unknown name
   */
  static std::optional<spdlog::level::level_enum> ParseLevel(const std::string& name);

  LogManager(const LogManager&) = delete;
  LogManager& operator=(const LogManager&) = delete;
  LogManager(LogManager&&) = delete;
  LogManager& operator=(LogManager&&) = delete;

 private:
  LogManager() = default;
  ~LogManager() = default;

  std::mutex mutex_;
  std::shared_ptr<spdlog::sinks::dist_sink_mt> file_sink_;
  std::shared_ptr<spdlog::logger> logger_;
};

}  // namespace mygramdb::utils
