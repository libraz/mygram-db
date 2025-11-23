/**
 * @file runtime_variable_manager.cpp
 * @brief Runtime variable manager implementation
 */

#include "config/runtime_variable_manager.h"

#include <spdlog/spdlog.h>

#include <charconv>
#include <shared_mutex>

#include "cache/cache_manager.h"
#include "utils/structured_log.h"

namespace mygramdb::config {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

namespace {
constexpr int kMaxPortNumber = 65535;  // Maximum valid TCP/UDP port number
}  // namespace

// Mutable variables (can be changed at runtime)
static const std::map<std::string, bool> kVariableMutability = {
    // Logging
    {"logging.level", true},
    {"logging.format", true},
    {"logging.file", false},  // Immutable (requires file handle reopening)

    // MySQL connection
    {"mysql.host", true},
    {"mysql.port", true},
    {"mysql.user", false},                    // Immutable (authentication)
    {"mysql.password", false},                // Immutable (authentication)
    {"mysql.database", false},                // Immutable (requires reinitialization)
    {"mysql.use_gtid", false},                // Immutable (replication mode)
    {"mysql.binlog_format", false},           // Immutable (validation only)
    {"mysql.binlog_row_image", false},        // Immutable (validation only)
    {"mysql.connect_timeout_ms", false},      // Immutable
    {"mysql.read_timeout_ms", false},         // Immutable
    {"mysql.write_timeout_ms", false},        // Immutable
    {"mysql.session_timeout_sec", false},     // Immutable
    {"mysql.ssl_enable", false},              // Immutable (SSL setup)
    {"mysql.ssl_ca", false},                  // Immutable
    {"mysql.ssl_cert", false},                // Immutable
    {"mysql.ssl_key", false},                 // Immutable
    {"mysql.ssl_verify_server_cert", false},  // Immutable
    {"mysql.datetime_timezone", false},       // Immutable

    // API settings
    {"api.default_limit", true},
    {"api.max_query_length", true},
    {"api.tcp.bind", false},                // Immutable (requires socket rebind)
    {"api.tcp.port", false},                // Immutable
    {"api.tcp.max_connections", false},     // Immutable
    {"api.http.enable", false},             // Immutable
    {"api.http.bind", false},               // Immutable
    {"api.http.port", false},               // Immutable
    {"api.http.enable_cors", false},        // Immutable
    {"api.http.cors_allow_origin", false},  // Immutable

    // Rate limiting
    {"api.rate_limiting.enable", true},
    {"api.rate_limiting.capacity", true},
    {"api.rate_limiting.refill_rate", true},
    {"api.rate_limiting.max_clients", false},  // Immutable (memory allocation)

    // Cache
    {"cache.enabled", true},
    {"cache.min_query_cost_ms", true},
    {"cache.ttl_seconds", true},
    {"cache.max_memory_bytes", false},           // Immutable (memory allocation)
    {"cache.invalidation_strategy", false},      // Immutable (architecture change)
    {"cache.compression_enabled", false},        // Immutable
    {"cache.eviction_batch_size", false},        // Immutable
    {"cache.invalidation.batch_size", false},    // Immutable
    {"cache.invalidation.max_delay_ms", false},  // Immutable

    // Memory (all immutable)
    {"memory.hard_limit_mb", false},
    {"memory.soft_target_mb", false},
    {"memory.arena_chunk_mb", false},
    {"memory.roaring_threshold", false},
    {"memory.minute_epoch", false},
    {"memory.normalize.nfkc", false},
    {"memory.normalize.width", false},
    {"memory.normalize.lower", false},

    // Replication (all immutable)
    {"replication.enable", false},
    {"replication.auto_initial_snapshot", false},
    {"replication.server_id", false},
    {"replication.start_from", false},
    {"replication.queue_size", false},
    {"replication.reconnect_backoff_min_ms", false},
    {"replication.reconnect_backoff_max_ms", false},

    // Build (all immutable)
    {"build.mode", false},
    {"build.batch_size", false},
    {"build.parallelism", false},
    {"build.throttle_ms", false},

    // Dump (all immutable)
    {"dump.dir", false},
    {"dump.default_filename", false},
    {"dump.interval_sec", false},
    {"dump.retain", false},

    // Network (immutable - security critical)
    {"network.allow_cidrs", false},

    // Tables (all immutable - requires index rebuild)
    // Note: table.* variables are not listed here (checked dynamically)
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
  runtime_values_["mysql.host"] = base_config_.mysql.host;
  runtime_values_["mysql.port"] = std::to_string(base_config_.mysql.port);
  runtime_values_["api.default_limit"] = std::to_string(base_config_.api.default_limit);
  runtime_values_["api.max_query_length"] = std::to_string(base_config_.api.max_query_length);
  runtime_values_["api.rate_limiting.enable"] = base_config_.api.rate_limiting.enable ? "true" : "false";
  runtime_values_["api.rate_limiting.capacity"] = std::to_string(base_config_.api.rate_limiting.capacity);
  runtime_values_["api.rate_limiting.refill_rate"] = std::to_string(base_config_.api.rate_limiting.refill_rate);
  runtime_values_["cache.enabled"] = base_config_.cache.enabled ? "true" : "false";
  runtime_values_["cache.min_query_cost_ms"] = std::to_string(base_config_.cache.min_query_cost_ms);
  runtime_values_["cache.ttl_seconds"] = std::to_string(base_config_.cache.ttl_seconds);
}

Expected<void, Error> RuntimeVariableManager::SetVariable(const std::string& variable_name, const std::string& value) {
  // Check if variable exists
  auto var_iter = kVariableMutability.find(variable_name);
  if (var_iter == kVariableMutability.end()) {
    // Check if it's a table variable (always immutable)
    if (variable_name.find("tables[") == 0) {
      return MakeUnexpected(
          MakeError(ErrorCode::kInvalidArgument, "Variable '" + variable_name + "' is immutable (requires restart)"));
    }
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Unknown variable: " + variable_name));
  }

  // Check if variable is mutable
  if (!var_iter->second) {
    return MakeUnexpected(
        MakeError(ErrorCode::kInvalidArgument, "Variable '" + variable_name + "' is immutable (requires restart)"));
  }

  // Apply variable-specific logic
  Expected<void, Error> result;

  if (variable_name == "logging.level") {
    result = ApplyLoggingLevel(value);
  } else if (variable_name == "logging.format") {
    result = ApplyLoggingFormat(value);
  } else if (variable_name == "mysql.host") {
    result = ApplyMysqlHost(value);
  } else if (variable_name == "mysql.port") {
    auto port = ParseInt(value);
    if (!port) {
      return MakeUnexpected(port.error());
    }
    result = ApplyMysqlPort(*port);
  } else if (variable_name == "api.default_limit") {
    auto limit = ParseInt(value);
    if (!limit) {
      return MakeUnexpected(limit.error());
    }
    result = ApplyApiDefaultLimit(*limit);
  } else if (variable_name == "api.max_query_length") {
    auto length = ParseInt(value);
    if (!length) {
      return MakeUnexpected(length.error());
    }
    result = ApplyApiMaxQueryLength(*length);
  } else if (variable_name == "api.rate_limiting.enable") {
    auto enabled = ParseBool(value);
    if (!enabled) {
      return MakeUnexpected(enabled.error());
    }
    result = ApplyRateLimitingEnable(*enabled);
  } else if (variable_name == "api.rate_limiting.capacity") {
    auto capacity = ParseInt(value);
    if (!capacity) {
      return MakeUnexpected(capacity.error());
    }
    result = ApplyRateLimitingCapacity(*capacity);
  } else if (variable_name == "api.rate_limiting.refill_rate") {
    auto rate = ParseInt(value);
    if (!rate) {
      return MakeUnexpected(rate.error());
    }
    result = ApplyRateLimitingRefillRate(*rate);
  } else if (variable_name == "cache.enabled") {
    auto enabled = ParseBool(value);
    if (!enabled) {
      return MakeUnexpected(enabled.error());
    }
    result = ApplyCacheEnabled(*enabled);
  } else if (variable_name == "cache.min_query_cost_ms") {
    auto cost = ParseDouble(value);
    if (!cost) {
      return MakeUnexpected(cost.error());
    }
    result = ApplyCacheMinQueryCost(*cost);
  } else if (variable_name == "cache.ttl_seconds") {
    auto ttl = ParseInt(value);
    if (!ttl) {
      return MakeUnexpected(ttl.error());
    }
    result = ApplyCacheTtl(*ttl);
  } else {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Variable not implemented: " + variable_name));
  }

  if (!result) {
    return result;
  }

  // Update runtime value (with lock)
  {
    std::unique_lock lock(mutex_);
    runtime_values_[variable_name] = value;
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
  std::string value = GetVariableInternal(variable_name);
  if (value.empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Unknown variable: " + variable_name));
  }
  return value;
}

std::map<std::string, VariableInfo> RuntimeVariableManager::GetAllVariables(const std::string& prefix) const {
  std::shared_lock lock(mutex_);
  std::map<std::string, VariableInfo> result;

  // Add all known variables
  for (const auto& [name, is_mutable] : kVariableMutability) {
    if (!prefix.empty() && name.find(prefix) != 0) {
      continue;  // Skip if doesn't match prefix
    }

    std::string value = GetVariableInternal(name);
    if (!value.empty()) {
      result[name] = {value, is_mutable};
    }
  }

  // Add table variables (always immutable)
  for (size_t i = 0; i < base_config_.tables.size(); ++i) {
    const auto& table = base_config_.tables[i];
    std::string table_prefix = "tables[" + std::to_string(i) + "].";

    if (!prefix.empty() && table_prefix.find(prefix) != 0) {
      continue;
    }

    result[table_prefix + "name"] = {table.name, false};
    result[table_prefix + "primary_key"] = {table.primary_key, false};
    result[table_prefix + "ngram_size"] = {std::to_string(table.ngram_size), false};
    // Add more table fields as needed
  }

  return result;
}

bool RuntimeVariableManager::IsMutable(const std::string& variable_name) {
  // Table variables are always immutable
  if (variable_name.find("tables[") == 0) {
    return false;
  }

  auto var_iter = kVariableMutability.find(variable_name);
  return (var_iter != kVariableMutability.end()) && var_iter->second;
}

void RuntimeVariableManager::SetMysqlReconnectCallback(
    std::function<Expected<void, Error>(const std::string& host, int port)> callback) {
  mysql_reconnect_callback_ = std::move(callback);
}

void RuntimeVariableManager::SetCacheToggleCallback(std::function<Expected<void, Error>(bool enabled)> callback) {
  cache_toggle_callback_ = std::move(callback);
}

void RuntimeVariableManager::SetCacheManager(cache::CacheManager* cache_manager) {
  cache_manager_ = cache_manager;
}

void RuntimeVariableManager::SetRateLimiterCallback(std::function<void(size_t, size_t)> callback) {
  rate_limiter_callback_ = std::move(callback);
}

// ========== Apply functions ==========

Expected<void, Error> RuntimeVariableManager::ApplyLoggingLevel(const std::string& value) {
  // Validate and apply logging level
  auto is_valid_level = [&value]() -> bool {
    return value == "debug" || value == "info" || value == "warn" || value == "error";
  };

  if (!is_valid_level()) {
    return MakeUnexpected(
        MakeError(ErrorCode::kInvalidArgument, "Invalid logging level (must be debug/info/warn/error): " + value));
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
        MakeError(ErrorCode::kInvalidArgument, "Invalid logging format (must be json/text): " + value));
  }

  // Apply format change to StructuredLog
  // Note: This changes the global format for all subsequent StructuredLog calls.
  // The change is thread-safe (uses atomic operations) but only affects new log messages.
  // spdlog's native format is not affected - this only controls StructuredLog output format.
  mygram::utils::LogFormat format = (value == "json") ? mygram::utils::LogFormat::JSON : mygram::utils::LogFormat::TEXT;
  mygram::utils::StructuredLog::SetFormat(format);

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyMysqlHost(const std::string& value) {
  if (value.empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "mysql.host cannot be empty"));
  }

  // Trigger reconnection if callback is set
  if (mysql_reconnect_callback_) {
    int current_port = 0;
    {
      std::shared_lock lock(mutex_);
      auto port_str = runtime_values_.at("mysql.port");
      auto port_result = ParseInt(port_str);
      if (!port_result) {
        return MakeUnexpected(port_result.error());
      }
      current_port = *port_result;
    }
    return mysql_reconnect_callback_(value, current_port);
  }

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyMysqlPort(int value) {
  if (value <= 0 || value > kMaxPortNumber) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid port number (must be 1-65535)"));
  }

  // Trigger reconnection if callback is set
  if (mysql_reconnect_callback_) {
    std::string current_host;
    {
      std::shared_lock lock(mutex_);
      current_host = runtime_values_.at("mysql.host");
    }
    return mysql_reconnect_callback_(current_host, value);
  }

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyApiDefaultLimit(int value) {
  if (value < defaults::kMinLimit || value > defaults::kMaxLimit) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid api.default_limit (must be " +
                                                                     std::to_string(defaults::kMinLimit) + "-" +
                                                                     std::to_string(defaults::kMaxLimit) + ")"));
  }

  // Update base config
  base_config_.api.default_limit = value;

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyApiMaxQueryLength(int value) {
  if (value <= 0) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "api.max_query_length must be > 0"));
  }

  // Update base config
  base_config_.api.max_query_length = value;

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyRateLimitingEnable(bool value) {
  // Update base config
  base_config_.api.rate_limiting.enable = value;

  // TODO: Add callback for rate limiter enable/disable

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyRateLimitingCapacity(int value) {
  if (value <= 0) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "api.rate_limiting.capacity must be > 0"));
  }

  // Update base config
  base_config_.api.rate_limiting.capacity = value;

  // Apply to rate limiter if callback is set
  if (rate_limiter_callback_) {
    rate_limiter_callback_(static_cast<size_t>(value), base_config_.api.rate_limiting.refill_rate);
  }

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyRateLimitingRefillRate(int value) {
  if (value <= 0) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "api.rate_limiting.refill_rate must be > 0"));
  }

  // Update base config
  base_config_.api.rate_limiting.refill_rate = value;

  // Apply to rate limiter if callback is set
  if (rate_limiter_callback_) {
    rate_limiter_callback_(base_config_.api.rate_limiting.capacity, static_cast<size_t>(value));
  }

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyCacheEnabled(bool value) {
  // Update base config
  base_config_.cache.enabled = value;

  // Trigger cache toggle callback
  if (cache_toggle_callback_) {
    return cache_toggle_callback_(value);
  }

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyCacheMinQueryCost(double value) {
  if (value < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "cache.min_query_cost_ms must be >= 0"));
  }

  // Update base config
  base_config_.cache.min_query_cost_ms = value;

  // Apply to CacheManager if available
  if (cache_manager_ != nullptr) {
    cache_manager_->SetMinQueryCost(value);
  }

  return {};
}

Expected<void, Error> RuntimeVariableManager::ApplyCacheTtl(int value) {
  if (value < 0) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "cache.ttl_seconds must be >= 0"));
  }

  // Update base config
  base_config_.cache.ttl_seconds = value;

  // Apply to CacheManager if available
  if (cache_manager_ != nullptr) {
    cache_manager_->SetTtl(value);
  }

  return {};
}

// ========== Internal helpers ==========

std::string RuntimeVariableManager::GetVariableInternal(const std::string& variable_name) const {
  // Check runtime values first (mutable variables)
  auto value_iter = runtime_values_.find(variable_name);
  if (value_iter != runtime_values_.end()) {
    return value_iter->second;
  }

  // Check base config for immutable variables
  if (variable_name == "mysql.user") {
    return base_config_.mysql.user;
  }
  if (variable_name == "mysql.password") {
    return base_config_.mysql.password;
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
  if (variable_name == "api.http.enable") {
    return base_config_.api.http.enable ? "true" : "false";
  }
  if (variable_name == "api.http.bind") {
    return base_config_.api.http.bind;
  }
  if (variable_name == "api.http.port") {
    return std::to_string(base_config_.api.http.port);
  }
  if (variable_name == "api.rate_limiting.max_clients") {
    return std::to_string(base_config_.api.rate_limiting.max_clients);
  }

  // Cache immutable variables
  if (variable_name == "cache.max_memory_bytes") {
    return std::to_string(base_config_.cache.max_memory_bytes);
  }
  if (variable_name == "cache.invalidation_strategy") {
    return base_config_.cache.invalidation_strategy;
  }

  // Memory config
  if (variable_name == "memory.hard_limit_mb") {
    return std::to_string(base_config_.memory.hard_limit_mb);
  }
  if (variable_name == "memory.soft_target_mb") {
    return std::to_string(base_config_.memory.soft_target_mb);
  }

  // Add more as needed...

  return "";  // Unknown variable
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
  return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid boolean value: " + value));
}

Expected<int, Error> RuntimeVariableManager::ParseInt(const std::string& value) {
  int result = 0;
  // NOLINTNEXTLINE(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  auto [ptr, ec] = std::from_chars(value.data(), value.data() + value.size(), result);
  if (ec != std::errc{}) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid integer value: " + value));
  }
  return result;
}

Expected<double, Error> RuntimeVariableManager::ParseDouble(const std::string& value) {
  try {
    size_t pos = 0;
    double result = std::stod(value, &pos);
    if (pos != value.size()) {
      return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid double value: " + value));
    }
    return result;
  } catch (const std::exception& e) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "Invalid double value: " + value));
  }
}

}  // namespace mygramdb::config
