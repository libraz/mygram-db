/**
 * @file runtime_variable_manager.cpp
 * @brief Runtime variable manager implementation
 */

#include "config/runtime_variable_manager.h"

#include <spdlog/spdlog.h>

#include <charconv>
#include <cmath>
#include <optional>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <vector>

#include "cache/cache_manager.h"
#include "config/config_help.h"
#include "utils/constants.h"
#include "utils/structured_log.h"

namespace mygramdb::config {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

namespace {
constexpr int kMaxRuntimeQueryLength = 4096;

std::string JoinStrings(const std::vector<std::string>& values, const std::string& delimiter) {
  std::ostringstream oss;
  for (size_t index = 0; index < values.size(); ++index) {
    if (index > 0) {
      oss << delimiter;
    }
    oss << values[index];
  }
  return oss.str();
}
}  // namespace

// Runtime mutation is an intentionally small allowlist. Every other variable
// comes from ConfigToVariableMap and is therefore known-but-immutable without
// being copied into a second exhaustive registry.
static const std::set<std::string> kMutableVariables = {
    "logging.level",
    "logging.format",
    "api.default_limit",
    "api.max_query_length",
    "api.rate_limiting.enable",
    "api.rate_limiting.capacity",
    "api.rate_limiting.refill_rate",
    "cache.enabled",
    "cache.min_query_cost_ms",
    "cache.ttl_seconds",
};

Expected<std::unique_ptr<RuntimeVariableManager>, Error> RuntimeVariableManager::Create(const Config& initial_config) {
  auto manager = std::unique_ptr<RuntimeVariableManager>(new RuntimeVariableManager());
  manager->base_config_ = initial_config;
  manager->InitializeRuntimeValues();
  return manager;
}

void RuntimeVariableManager::InitializeRuntimeValues() {
  // Initialize only mutable variables
  runtime_values_["logging.level"] = base_config_.logging.level;
  runtime_values_["logging.format"] = base_config_.logging.format;
  runtime_values_["api.default_limit"] = std::to_string(base_config_.api.default_limit);
  runtime_values_["api.max_query_length"] = std::to_string(base_config_.api.max_query_length);
  runtime_values_["api.rate_limiting.enable"] = base_config_.api.rate_limiting.enable ? "true" : "false";
  runtime_values_["api.rate_limiting.capacity"] = std::to_string(base_config_.api.rate_limiting.capacity);
  runtime_values_["api.rate_limiting.refill_rate"] = std::to_string(base_config_.api.rate_limiting.refill_rate);
  runtime_values_["cache.enabled"] = base_config_.cache.enabled ? "true" : "false";
  runtime_values_["cache.min_query_cost_ms"] = nlohmann::json(base_config_.cache.min_query_cost_ms).dump();
  runtime_values_["cache.ttl_seconds"] = std::to_string(base_config_.cache.ttl_seconds);
}

Expected<void, Error> RuntimeVariableManager::SetVariable(const std::string& variable_name, const std::string& value) {
  if (kMutableVariables.find(variable_name) == kMutableVariables.end()) {
    std::shared_lock lock(mutex_);
    if (GetVariableInternal(variable_name).has_value()) {
      return MakeUnexpected(MakeError(ErrorCode::kConfigVariableNotMutable,
                                      "Variable '" + variable_name + "' is immutable (requires restart)"));
    }
    return MakeUnexpected(MakeError(ErrorCode::kConfigUnknownVariable, "Unknown variable: " + variable_name));
  }

  // Every branch below validates first and, once valid, holds mutex_ across
  // both the state update (runtime_values_/base_config_) and the side effect
  // on the backing component. That keeps GetVariable()/SHOW VARIABLES from
  // ever reporting a value that failed validation, and keeps two concurrent
  // SETs from applying their side effects in an order other than the one in
  // which they acquired the lock (which would desync reported vs. effective
  // state, e.g. between rate_limiting.capacity and .refill_rate).
  if (variable_name == "logging.level") {
    std::unique_lock lock(mutex_);
    auto result = ApplyLoggingLevel(value);
    if (!result) {
      return result;
    }
    runtime_values_[variable_name] = value;
    base_config_.logging.level = value;
  } else if (variable_name == "logging.format") {
    std::unique_lock lock(mutex_);
    auto result = ApplyLoggingFormat(value);
    if (!result) {
      return result;
    }
    runtime_values_[variable_name] = value;
    base_config_.logging.format = value;
  } else if (variable_name == "api.default_limit") {
    auto limit = ParseInt(value);
    if (!limit) {
      return MakeUnexpected(limit.error());
    }
    auto result = ApplyApiDefaultLimit(*limit);
    if (!result) {
      return result;
    }
  } else if (variable_name == "api.max_query_length") {
    auto length = ParseInt(value);
    if (!length) {
      return MakeUnexpected(length.error());
    }
    auto result = ApplyApiMaxQueryLength(*length);
    if (!result) {
      return result;
    }
  } else if (variable_name == "api.rate_limiting.enable") {
    auto enabled = ParseBool(value);
    if (!enabled) {
      return MakeUnexpected(enabled.error());
    }
    auto result = ApplyRateLimitingEnable(*enabled);
    if (!result) {
      return result;
    }
  } else if (variable_name == "api.rate_limiting.capacity") {
    auto capacity = ParseInt(value);
    if (!capacity) {
      return MakeUnexpected(capacity.error());
    }
    auto result = ApplyRateLimitingCapacity(*capacity);
    if (!result) {
      return result;
    }
  } else if (variable_name == "api.rate_limiting.refill_rate") {
    auto rate = ParseInt(value);
    if (!rate) {
      return MakeUnexpected(rate.error());
    }
    auto result = ApplyRateLimitingRefillRate(*rate);
    if (!result) {
      return result;
    }
  } else if (variable_name == "cache.enabled") {
    auto enabled = ParseBool(value);
    if (!enabled) {
      return MakeUnexpected(enabled.error());
    }
    auto result = ApplyCacheEnabled(*enabled);
    if (!result) {
      return result;
    }
  } else if (variable_name == "cache.min_query_cost_ms") {
    auto cost = ParseDouble(value);
    if (!cost) {
      return MakeUnexpected(cost.error());
    }
    auto result = ApplyCacheMinQueryCost(*cost);
    if (!result) {
      return result;
    }
  } else if (variable_name == "cache.ttl_seconds") {
    auto ttl = ParseInt(value);
    if (!ttl) {
      return MakeUnexpected(ttl.error());
    }
    auto result = ApplyCacheTtl(*ttl);
    if (!result) {
      return result;
    }
  } else {
    return MakeUnexpected(
        MakeError(ErrorCode::kConfigVariableNotMutable, "Variable not implemented: " + variable_name));
  }

  // Log the change
  mygram::utils::StructuredLog()
      .Event("variable_changed")
      .Field("variable", variable_name)
      .Field("value", value)
      .Info();

  return {};
}

Expected<std::string, Error> RuntimeVariableManager::GetVariable(const std::string& variable_name) const {
  std::shared_lock lock(mutex_);
  auto value = GetVariableInternal(variable_name);
  if (!value.has_value()) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigUnknownVariable, "Unknown variable: " + variable_name));
  }
  return *value;
}

std::map<std::string, VariableInfo> RuntimeVariableManager::GetAllVariables(const std::string& prefix) const {
  std::shared_lock lock(mutex_);
  std::map<std::string, VariableInfo> result;
  auto variables = ConfigToVariableMap(base_config_);
  variables["cache.max_memory_bytes"] = std::to_string(base_config_.cache.max_memory_bytes);
  for (const auto& [name, value] : runtime_values_) {
    variables[name] = value;
  }
  for (const auto& [name, value] : variables) {
    if (prefix.empty() || name.find(prefix) == 0) {
      result[name] = {value, kMutableVariables.find(name) != kMutableVariables.end()};
    }
  }

  return result;
}

Config RuntimeVariableManager::GetCurrentConfig() const {
  std::shared_lock lock(mutex_);
  return base_config_;
}

bool RuntimeVariableManager::IsMutable(const std::string& variable_name) {
  return kMutableVariables.find(variable_name) != kMutableVariables.end();
}

void RuntimeVariableManager::SetCacheToggleCallback(std::function<Expected<void, Error>(bool enabled)> callback) {
  std::unique_lock lock(mutex_);
  cache_toggle_callback_ = std::move(callback);
}

void RuntimeVariableManager::SetCacheManager(cache::CacheManager* cache_manager) {
  std::unique_lock lock(mutex_);
  cache_manager_ = cache_manager;
}

void RuntimeVariableManager::SetRateLimiterCallback(std::function<void(bool, size_t, size_t)> callback) {
  std::unique_lock lock(mutex_);
  rate_limiter_callback_ = std::move(callback);
}

void RuntimeVariableManager::SetApiConfigCallback(std::function<void(int, int)> callback) {
  std::unique_lock lock(mutex_);
  api_config_callbacks_.clear();
  if (callback) {
    api_config_callbacks_.push_back(std::move(callback));
  }
}

void RuntimeVariableManager::AddApiConfigCallback(std::function<void(int, int)> callback) {
  if (!callback) {
    return;
  }
  std::unique_lock lock(mutex_);
  api_config_callbacks_.push_back(std::move(callback));
}

// ========== Apply functions ==========

Expected<void, Error> RuntimeVariableManager::ApplyLoggingLevel(const std::string& value) {
  // Validate and apply logging level
  auto is_valid_level = [&value]() -> bool {
    return value == "debug" || value == "info" || value == "warn" || value == "error";
  };

  if (!is_valid_level()) {
    return MakeUnexpected(
        MakeError(ErrorCode::kConfigInvalidValue, "Invalid logging level (must be debug/info/warn/error): " + value));
  }

  // Apply to spdlog
  if (value == "debug") {
    spdlog::set_level(spdlog::level::debug);
  } else if (value == "info") {
    spdlog::set_level(spdlog::level::info);
  } else if (value == "warn") {
    spdlog::set_level(spdlog::level::warn);
  } else if (value == "error") {
    spdlog::set_level(spdlog::level::err);
  }

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyLoggingFormat(const std::string& value) {
  // Validate format
  if (value != "json" && value != "text") {
    return MakeUnexpected(
        MakeError(ErrorCode::kConfigInvalidValue, "Invalid logging format (must be json/text): " + value));
  }

  // Apply format change to StructuredLog
  // Note: This changes the global format for all subsequent StructuredLog calls.
  // The change is thread-safe (uses atomic operations) but only affects new log messages.
  // spdlog's native format is not affected - this only controls StructuredLog output format.
  mygram::utils::LogFormat format = (value == "json") ? mygram::utils::LogFormat::JSON : mygram::utils::LogFormat::TEXT;
  mygram::utils::StructuredLog::SetFormat(format);

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyApiDefaultLimit(int value) {
  if (value < defaults::kMinLimit || value > defaults::kMaxLimit) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue, "Invalid api.default_limit (must be " +
                                                                        std::to_string(defaults::kMinLimit) + "-" +
                                                                        std::to_string(defaults::kMaxLimit) + ")"));
  }

  // Update and notify while still holding the lock, so a concurrent SET on
  // api.max_query_length cannot apply its callback between this callback's
  // capture of max_query_length and its invocation.
  std::unique_lock lock(mutex_);
  base_config_.api.default_limit = value;
  runtime_values_["api.default_limit"] = std::to_string(value);
  const int max_query_length = base_config_.api.max_query_length;
  for (const auto& callback : api_config_callbacks_) {
    callback(value, max_query_length);
  }
  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyApiMaxQueryLength(int value) {
  if (value < 0 || value > kMaxRuntimeQueryLength) {
    return MakeUnexpected(MakeError(
        ErrorCode::kConfigInvalidValue,
        "api.max_query_length must be between 0 and " + std::to_string(kMaxRuntimeQueryLength) + " (0 = unlimited)"));
  }

  // See ApplyApiDefaultLimit: update and notify under the same lock so the
  // pair of values a callback sees is never a mix of two concurrent SETs.
  std::unique_lock lock(mutex_);
  base_config_.api.max_query_length = value;
  runtime_values_["api.max_query_length"] = std::to_string(value);
  const int default_limit = base_config_.api.default_limit;
  for (const auto& callback : api_config_callbacks_) {
    callback(default_limit, value);
  }
  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyRateLimitingEnable(bool value) {
  // Update config + runtime_values_ and notify the rate limiter under one
  // lock: a concurrent SET on capacity/refill_rate could otherwise apply its
  // own callback between this call's capture and its invocation, leaving the
  // rate limiter holding a stale pair of parameters.
  std::unique_lock lock(mutex_);
  base_config_.api.rate_limiting.enable = value;
  runtime_values_["api.rate_limiting.enable"] = value ? "true" : "false";
  const size_t capacity = static_cast<size_t>(base_config_.api.rate_limiting.capacity);
  const size_t refill_rate = static_cast<size_t>(base_config_.api.rate_limiting.refill_rate);
  if (rate_limiter_callback_) {
    rate_limiter_callback_(value, capacity, refill_rate);
  }

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyRateLimitingCapacity(int value) {
  if (value <= 0 || value > ApiConfig::kMaxRateLimitCapacity) {
    return MakeUnexpected(MakeError(
        ErrorCode::kConfigInvalidValue,
        "api.rate_limiting.capacity must be between 1 and " + std::to_string(ApiConfig::kMaxRateLimitCapacity)));
  }

  // See ApplyRateLimitingEnable: update and notify under one lock.
  std::unique_lock lock(mutex_);
  base_config_.api.rate_limiting.capacity = value;
  runtime_values_["api.rate_limiting.capacity"] = std::to_string(value);
  const bool enabled = base_config_.api.rate_limiting.enable;
  const size_t refill_rate = static_cast<size_t>(base_config_.api.rate_limiting.refill_rate);
  if (rate_limiter_callback_) {
    rate_limiter_callback_(enabled, static_cast<size_t>(value), refill_rate);
  }

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyRateLimitingRefillRate(int value) {
  if (value <= 0 || value > ApiConfig::kMaxRateLimitRefillRate) {
    return MakeUnexpected(MakeError(
        ErrorCode::kConfigInvalidValue,
        "api.rate_limiting.refill_rate must be between 1 and " + std::to_string(ApiConfig::kMaxRateLimitRefillRate)));
  }

  // See ApplyRateLimitingEnable: update and notify under one lock.
  std::unique_lock lock(mutex_);
  base_config_.api.rate_limiting.refill_rate = value;
  runtime_values_["api.rate_limiting.refill_rate"] = std::to_string(value);
  const bool enabled = base_config_.api.rate_limiting.enable;
  const size_t capacity = static_cast<size_t>(base_config_.api.rate_limiting.capacity);
  if (rate_limiter_callback_) {
    rate_limiter_callback_(enabled, capacity, static_cast<size_t>(value));
  }

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyCacheEnabled(bool value) {
  // Update, toggle, and roll back on failure all under one lock: releasing
  // the lock between the state update and the toggle would let a concurrent
  // GetVariable()/SHOW VARIABLES observe cache.enabled=value before the
  // toggle is known to succeed, and let a concurrent SET race the toggle.
  std::unique_lock lock(mutex_);
  base_config_.cache.enabled = value;
  runtime_values_["cache.enabled"] = value ? "true" : "false";

  if (cache_toggle_callback_) {
    auto result = cache_toggle_callback_(value);
    if (!result) {
      base_config_.cache.enabled = !value;
      runtime_values_["cache.enabled"] = !value ? "true" : "false";
      return result;
    }
  } else if (cache_manager_ != nullptr) {
    if (value) {
      if (!cache_manager_->Enable()) {
        base_config_.cache.enabled = false;
        runtime_values_["cache.enabled"] = "false";
        return MakeUnexpected(MakeError(ErrorCode::kCacheDisabled, "Cache cannot be enabled"));
      }
    } else {
      cache_manager_->Disable();
    }
  }

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyCacheMinQueryCost(double value) {
  // ParseDouble already rejects non-finite input before this is reached.
  if (value < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue, "cache.min_query_cost_ms must be >= 0"));
  }

  // Update config + runtime_values_ and apply to CacheManager under one lock
  // (see ApplyCacheEnabled for why: keeps reported and effective in step).
  std::unique_lock lock(mutex_);
  base_config_.cache.min_query_cost_ms = value;
  runtime_values_["cache.min_query_cost_ms"] = nlohmann::json(value).dump();
  if (cache_manager_ != nullptr) {
    cache_manager_->SetMinQueryCost(value);
  }

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyCacheTtl(int value) {
  if (value < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue, "cache.ttl_seconds must be >= 0"));
  }

  // Update config + runtime_values_ and apply to CacheManager under one lock.
  std::unique_lock lock(mutex_);
  base_config_.cache.ttl_seconds = value;
  runtime_values_["cache.ttl_seconds"] = std::to_string(value);
  if (cache_manager_ != nullptr) {
    cache_manager_->SetTtl(value);
  }

  return {};
}

// ========== Internal helpers ==========

std::optional<std::string> RuntimeVariableManager::GetVariableInternal(const std::string& variable_name) const {
  // Check runtime values first (mutable variables)
  auto value_iter = runtime_values_.find(variable_name);
  if (value_iter != runtime_values_.end()) {
    return value_iter->second;
  }

  auto variables = ConfigToVariableMap(base_config_);
  variables["cache.max_memory_bytes"] = std::to_string(base_config_.cache.max_memory_bytes);
  auto canonical = variables.find(variable_name);
  if (canonical != variables.end()) {
    return canonical->second;
  }

  // Check base config for immutable variables
  if (variable_name == "logging.file") {
    return base_config_.logging.file;
  }

  if (variable_name == "mysql.user") {
    return base_config_.mysql.user;
  }
  if (variable_name == "mysql.password") {
    return std::string("***");
  }
  if (variable_name == "mysql.database") {
    return base_config_.mysql.database;
  }
  if (variable_name == "mysql.use_gtid") {
    return base_config_.mysql.use_gtid ? "true" : "false";
  }
  if (variable_name == "mysql.binlog_format") {
    return base_config_.mysql.binlog_format;
  }
  if (variable_name == "mysql.binlog_row_image") {
    return base_config_.mysql.binlog_row_image;
  }
  if (variable_name == "mysql.connect_timeout_ms") {
    return std::to_string(base_config_.mysql.connect_timeout_ms);
  }
  if (variable_name == "mysql.read_timeout_ms") {
    return std::to_string(base_config_.mysql.read_timeout_ms);
  }
  if (variable_name == "mysql.write_timeout_ms") {
    return std::to_string(base_config_.mysql.write_timeout_ms);
  }
  if (variable_name == "mysql.session_timeout_sec") {
    return std::to_string(base_config_.mysql.session_timeout_sec);
  }
  if (variable_name == "mysql.ssl_enable") {
    return base_config_.mysql.ssl_enable ? "true" : "false";
  }
  // mysql.ssl_ca/ssl_cert/ssl_key are already reachable (and, for ssl_key,
  // masked) through the canonical `variables` map above -- ConfigToJson
  // includes them, so a dedicated branch here would never be reached.
  if (variable_name == "mysql.ssl_verify_server_cert") {
    return base_config_.mysql.ssl_verify_server_cert ? "true" : "false";
  }
  if (variable_name == "mysql.datetime_timezone") {
    return base_config_.mysql.datetime_timezone;
  }

  // API immutable variables
  if (variable_name == "api.tcp.bind") {
    return base_config_.api.tcp.bind;
  }
  if (variable_name == "api.tcp.port") {
    return std::to_string(base_config_.api.tcp.port);
  }
  if (variable_name == "api.tcp.max_connections") {
    return std::to_string(base_config_.api.tcp.max_connections);
  }
  if (variable_name == "api.tcp.worker_threads") {
    return std::to_string(base_config_.api.tcp.worker_threads);
  }
  if (variable_name == "api.tcp.recv_timeout_sec") {
    return std::to_string(base_config_.api.tcp.recv_timeout_sec);
  }
  if (variable_name == "api.tcp.idle_timeout_sec") {
    return std::to_string(base_config_.api.tcp.idle_timeout_sec);
  }
  if (variable_name == "api.tcp.reaper_interval_sec") {
    return std::to_string(base_config_.api.tcp.reaper_interval_sec);
  }
  if (variable_name == "api.tcp.thread_pool_queue_size") {
    return std::to_string(base_config_.api.tcp.thread_pool_queue_size);
  }
  if (variable_name == "api.tcp.keepalive.enabled") {
    return base_config_.api.tcp.keepalive.enabled ? "true" : "false";
  }
  if (variable_name == "api.tcp.keepalive.idle_sec") {
    return std::to_string(base_config_.api.tcp.keepalive.idle_sec);
  }
  if (variable_name == "api.tcp.keepalive.interval_sec") {
    return std::to_string(base_config_.api.tcp.keepalive.interval_sec);
  }
  if (variable_name == "api.tcp.keepalive.probe_count") {
    return std::to_string(base_config_.api.tcp.keepalive.probe_count);
  }
  if (variable_name == "api.tcp.max_write_queue_bytes") {
    return std::to_string(base_config_.api.tcp.max_write_queue_bytes);
  }
  if (variable_name == "api.tcp.max_total_buffered_bytes") {
    return std::to_string(base_config_.api.tcp.max_total_buffered_bytes);
  }
  if (variable_name == "api.tcp.max_pending_frames") {
    return std::to_string(base_config_.api.tcp.max_pending_frames);
  }
  if (variable_name == "api.tcp.max_pending_frame_bytes") {
    return std::to_string(base_config_.api.tcp.max_pending_frame_bytes);
  }
  if (variable_name == "api.http.enable") {
    return base_config_.api.http.enable ? "true" : "false";
  }
  if (variable_name == "api.http.bind") {
    return base_config_.api.http.bind;
  }
  if (variable_name == "api.http.port") {
    return std::to_string(base_config_.api.http.port);
  }
  if (variable_name == "api.http.max_connections") {
    return std::to_string(base_config_.api.http.max_connections);
  }
  if (variable_name == "api.http.enable_cors") {
    return base_config_.api.http.enable_cors ? "true" : "false";
  }
  if (variable_name == "api.http.cors_allow_origin") {
    return base_config_.api.http.cors_allow_origin;
  }
  if (variable_name == "api.http.read_timeout_sec") {
    return std::to_string(base_config_.api.http.read_timeout_sec);
  }
  if (variable_name == "api.http.write_timeout_sec") {
    return std::to_string(base_config_.api.http.write_timeout_sec);
  }
  if (variable_name == "api.http.max_body_bytes") {
    return std::to_string(base_config_.api.http.max_body_bytes);
  }
  if (variable_name == "api.unix_socket.path") {
    return base_config_.api.unix_socket.path;
  }
  if (variable_name == "api.rate_limiting.max_clients") {
    return std::to_string(base_config_.api.rate_limiting.max_clients);
  }

  // Cache immutable variables
  if (variable_name == "cache.max_memory_mb") {
    return std::to_string(base_config_.cache.max_memory_bytes / mygram::constants::kBytesPerMegabyte);
  }
  if (variable_name == "cache.max_memory_bytes") {
    return std::to_string(base_config_.cache.max_memory_bytes);
  }
  if (variable_name == "cache.invalidation_strategy") {
    return base_config_.cache.invalidation_strategy;
  }
  if (variable_name == "cache.compression_enabled") {
    return base_config_.cache.compression_enabled ? "true" : "false";
  }
  if (variable_name == "cache.invalidation.batch_size") {
    return std::to_string(base_config_.cache.invalidation.batch_size);
  }
  if (variable_name == "cache.invalidation.max_delay_ms") {
    return std::to_string(base_config_.cache.invalidation.max_delay_ms);
  }
  if (variable_name == "cache.invalidation.max_queue_size") {
    return std::to_string(base_config_.cache.invalidation.max_queue_size);
  }

  // Memory config
  if (variable_name == "memory.hard_limit_mb") {
    return std::to_string(base_config_.memory.hard_limit_mb);
  }
  if (variable_name == "memory.soft_target_mb") {
    return std::to_string(base_config_.memory.soft_target_mb);
  }
  if (variable_name == "memory.arena_chunk_mb") {
    return std::to_string(base_config_.memory.arena_chunk_mb);
  }
  if (variable_name == "memory.roaring_threshold") {
    return std::to_string(base_config_.memory.roaring_threshold);
  }
  if (variable_name == "memory.minute_epoch") {
    return base_config_.memory.minute_epoch ? "true" : "false";
  }
  if (variable_name == "memory.normalize.nfkc") {
    return base_config_.memory.normalize.nfkc ? "true" : "false";
  }
  if (variable_name == "memory.normalize.width") {
    return base_config_.memory.normalize.width;
  }
  if (variable_name == "memory.normalize.lower") {
    return base_config_.memory.normalize.lower ? "true" : "false";
  }
  if (variable_name == "memory.verify_text") {
    return base_config_.memory.verify_text;
  }

  // Replication config
  if (variable_name == "replication.enable") {
    return base_config_.replication.enable ? "true" : "false";
  }
  if (variable_name == "replication.auto_initial_snapshot") {
    return base_config_.replication.auto_initial_snapshot ? "true" : "false";
  }
  if (variable_name == "replication.server_id") {
    return std::to_string(base_config_.replication.server_id);
  }
  if (variable_name == "replication.start_from") {
    return base_config_.replication.start_from;
  }
  if (variable_name == "replication.queue_size") {
    return std::to_string(base_config_.replication.queue_size);
  }
  if (variable_name == "replication.reconnect_backoff_min_ms") {
    return std::to_string(base_config_.replication.reconnect_backoff_min_ms);
  }
  if (variable_name == "replication.reconnect_backoff_max_ms") {
    return std::to_string(base_config_.replication.reconnect_backoff_max_ms);
  }

  // Build config
  if (variable_name == "build.mode") {
    return base_config_.build.mode;
  }
  if (variable_name == "build.batch_size") {
    return std::to_string(base_config_.build.batch_size);
  }
  if (variable_name == "build.parallelism") {
    return std::to_string(base_config_.build.parallelism);
  }
  if (variable_name == "build.throttle_ms") {
    return std::to_string(base_config_.build.throttle_ms);
  }

  // Dump config
  if (variable_name == "dump.dir") {
    return base_config_.dump.dir;
  }
  if (variable_name == "dump.default_filename") {
    return base_config_.dump.default_filename;
  }
  if (variable_name == "dump.load_on_startup") {
    return base_config_.dump.load_on_startup ? "true" : "false";
  }
  if (variable_name == "dump.interval_sec") {
    return std::to_string(base_config_.dump.interval_sec);
  }
  if (variable_name == "dump.retain") {
    return std::to_string(base_config_.dump.retain);
  }
  if (variable_name == "dump.restore_memory_budget_mb") {
    return std::to_string(base_config_.dump.restore_memory_budget_mb);
  }
  if (variable_name == "dump.restore_max_section_mb") {
    return std::to_string(base_config_.dump.restore_max_section_mb);
  }

  // Network config
  if (variable_name == "network.allow_cidrs") {
    return JoinStrings(base_config_.network.allow_cidrs, ",");
  }

  // BM25 config
  if (variable_name == "bm25.enable") {
    return base_config_.bm25.enable ? "true" : "false";
  }
  if (variable_name == "bm25.k1") {
    return std::to_string(base_config_.bm25.k1);
  }
  if (variable_name == "bm25.b") {
    return std::to_string(base_config_.bm25.b);
  }

  return std::nullopt;  // Unknown variable
}

Expected<bool, Error> RuntimeVariableManager::ParseBool(const std::string& value) {
  // Lambda to check if value represents true
  auto is_true_value = [&value]() -> bool {
    return value == "true" || value == "1" || value == "yes" || value == "on";
  };

  // Lambda to check if value represents false
  auto is_false_value = [&value]() -> bool {
    return value == "false" || value == "0" || value == "no" || value == "off";
  };

  if (is_true_value()) {
    return true;
  }
  if (is_false_value()) {
    return false;
  }
  return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue, "Invalid boolean value: " + value));
}

Expected<int, Error> RuntimeVariableManager::ParseInt(const std::string& value) {
  int result = 0;
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), result);
  if (ec != std::errc{}) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue, "Invalid integer value: " + value));
  }
  // Reject trailing non-numeric characters (e.g., "42abc")
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  if (ptr != value.data() + value.size()) {
    return MakeUnexpected(
        MakeError(ErrorCode::kConfigInvalidValue, "Invalid integer value (trailing characters): " + value));
  }
  return result;
}

Expected<double, Error> RuntimeVariableManager::ParseDouble(const std::string& value) {
  double result = 0.0;
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), result);
  if (ec != std::errc{}) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue, "Invalid double value: " + value));
  }
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  if (ptr != value.data() + value.size()) {
    return MakeUnexpected(
        MakeError(ErrorCode::kConfigInvalidValue, "Invalid double value (trailing characters): " + value));
  }
  // from_chars' floating-point grammar accepts "nan"/"inf"/"infinity", and a
  // relational range check downstream (e.g. value >= 0) is always false for
  // NaN, so it would pass validation by never tripping the rejection branch.
  if (!std::isfinite(result)) {
    return MakeUnexpected(MakeError(ErrorCode::kConfigInvalidValue, "Double value must be finite: " + value));
  }
  return result;
}

}  // namespace mygramdb::config
