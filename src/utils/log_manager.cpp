/**
 * @file log_manager.cpp
 * @brief LogManager implementation
 */

#include "utils/log_manager.h"

#include <spdlog/sinks/rotating_file_sink.h>

#include <utility>
#include <vector>

namespace mygramdb::utils {

namespace {

constexpr size_t kMaxLogFileBytes = 100U * 1024U * 1024U;
constexpr size_t kMaxRotatedLogFiles = 5;
constexpr const char* kLoggerName = "mygramdb";

}  // namespace

LogManager& LogManager::Instance() {
  static LogManager instance;
  return instance;
}

Expected<void, Error> LogManager::OpenFile(const std::string& path) {
  std::lock_guard<std::mutex> lock(mutex_);

  std::shared_ptr<spdlog::sinks::sink> file_sink;
  try {
    // rotate_on_open=false preserves the existing audit trail across startup
    // and SIGUSR1 reopen while bounding the total log volume.
    file_sink =
        std::make_shared<spdlog::sinks::rotating_file_sink_mt>(path, kMaxLogFileBytes, kMaxRotatedLogFiles, false);
  } catch (const spdlog::spdlog_ex& ex) {
    return MakeUnexpected(MakeError(ErrorCode::kIOError, ex.what()));
  }

  if (logger_ != nullptr && spdlog::default_logger_raw() == logger_.get()) {
    // The old file closes when the dist sink drops it under the sink mutex.
    file_sink_->set_sinks({std::move(file_sink)});
    return {};
  }

  file_sink_ = std::make_shared<spdlog::sinks::dist_sink_mt>(std::vector<spdlog::sink_ptr>{std::move(file_sink)});
  logger_ = std::make_shared<spdlog::logger>(kLoggerName, file_sink_);
  logger_->set_level(spdlog::get_level());
  spdlog::set_default_logger(logger_);
  return {};
}

void LogManager::SetLevel(spdlog::level::level_enum level) {
  std::lock_guard<std::mutex> lock(mutex_);
  spdlog::set_level(level);
}

std::optional<spdlog::level::level_enum> LogManager::ParseLevel(const std::string& name) {
  if (name == "debug") {
    return spdlog::level::debug;
  }
  if (name == "info") {
    return spdlog::level::info;
  }
  if (name == "warn") {
    return spdlog::level::warn;
  }
  if (name == "error") {
    return spdlog::level::err;
  }
  return std::nullopt;
}

}  // namespace mygramdb::utils
