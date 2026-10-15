/**
 * @file runtime_variable_manager.cpp
 * @brief Runtime variable manager implementation
 */

#include "config/runtime_variable_manager.h"

#include <spdlog/spdlog.h>

#include <map>
#include <set>
#include <shared_mutex>
#include <type_traits>

#include "cache/cache_manager.h"
#include "config/config_help.h"
#include "config/config_internal.h"
#include "utils/memory_utils.h"
#include "utils/string_utils.h"
#include "utils/structured_log.h"

namespace mygramdb::config {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

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

Expected<std::unique_ptr<RuntimeVariableManager>, Error> RuntimeVariableManager::Create(
    const Config& initial_config, PhysicalMemoryProvider physical_memory_provider) {
  auto manager = std::unique_ptr<RuntimeVariableManager>(new RuntimeVariableManager());
  manager->base_config_ = initial_config;
  manager->physical_memory_provider_ =
      physical_memory_provider ? std::move(physical_memory_provider) : []() -> std::optional<std::uint64_t> {
    const auto memory = mygram::utils::GetSystemMemoryInfo();
    if (!memory.has_value()) {
      return std::nullopt;
    }
    return memory->total_physical_bytes;
  };
  manager->cached_config_variables_ = ConfigToVariableMap(initial_config);
  manager->cached_config_variables_["cache.max_memory_bytes"] = std::to_string(initial_config.cache.max_memory_bytes);
  manager->cached_config_variables_.try_emplace("network.allow_cidrs", "");
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
  auto result = SetVariables({{variable_name, value}});
  if (!result) {
    return MakeUnexpected(result.error().error);
  }
  return {};
}

Expected<void, VariableSetError> RuntimeVariableManager::SetVariables(
    const std::vector<std::pair<std::string, std::string>>& assignments) {
  if (assignments.empty()) {
    return {};
  }

  std::unique_lock lock(mutex_);

  // Build the complete candidate state and effect description before touching
  // any component. If allocation or validation fails, readers and callbacks
  // continue to observe the existing state.
  Config staged_config = base_config_;
  auto staged_runtime_values = runtime_values_;
  std::map<std::string, std::string> final_values;
  bool api_config_changed = false;
  bool rate_limiter_changed = false;
  bool cache_enabled_assigned = false;
  bool cache_min_query_cost_assigned = false;
  bool cache_ttl_assigned = false;

  const auto fail = [](const std::string& name, Error error) {
    return MakeUnexpected(VariableSetError{name, std::move(error)});
  };

  for (const auto& [variable_name, raw_value] : assignments) {
    if (kMutableVariables.find(variable_name) == kMutableVariables.end()) {
      if (GetVariableInternal(variable_name).has_value()) {
        return fail(variable_name, MakeError(ErrorCode::kConfigVariableNotMutable,
                                             "Variable '" + variable_name + "' is immutable (requires restart)"));
      }
      return fail(variable_name, MakeError(ErrorCode::kConfigUnknownVariable, "Unknown variable: " + variable_name));
    }

    auto parsed_value = internal::ValidateConfigValue(variable_name, raw_value);
    if (!parsed_value) {
      return fail(variable_name, parsed_value.error());
    }

    const auto& parsed = *parsed_value;
    std::string canonical_value = parsed.is_string() ? parsed.get<std::string>() : parsed.dump();
    if (variable_name == "logging.level") {
      staged_config.logging.level = parsed.get<std::string>();
    } else if (variable_name == "logging.format") {
      staged_config.logging.format = parsed.get<std::string>();
    } else if (variable_name == "api.default_limit") {
      staged_config.api.default_limit = parsed.get<int>();
      api_config_changed = true;
    } else if (variable_name == "api.max_query_length") {
      staged_config.api.max_query_length = parsed.get<int>();
      api_config_changed = true;
    } else if (variable_name == "api.rate_limiting.enable") {
      staged_config.api.rate_limiting.enable = parsed.get<bool>();
      rate_limiter_changed = true;
    } else if (variable_name == "api.rate_limiting.capacity") {
      staged_config.api.rate_limiting.capacity = parsed.get<int>();
      rate_limiter_changed = true;
    } else if (variable_name == "api.rate_limiting.refill_rate") {
      staged_config.api.rate_limiting.refill_rate = parsed.get<int>();
      rate_limiter_changed = true;
    } else if (variable_name == "cache.enabled") {
      staged_config.cache.enabled = parsed.get<bool>();
      cache_enabled_assigned = true;
    } else if (variable_name == "cache.min_query_cost_ms") {
      staged_config.cache.min_query_cost_ms = parsed.get<double>();
      cache_min_query_cost_assigned = true;
    } else if (variable_name == "cache.ttl_seconds") {
      staged_config.cache.ttl_seconds = parsed.get<int>();
      cache_ttl_assigned = true;
    }

    staged_runtime_values[variable_name] = canonical_value;
    final_values[variable_name] = std::move(canonical_value);
  }

  std::optional<std::uint64_t> physical_memory_bytes;
  if (staged_config.cache.enabled && staged_config.cache.max_memory_bytes > 0 && physical_memory_provider_) {
    physical_memory_bytes = physical_memory_provider_();
  }
  std::string failed_variable;
  auto final_state = internal::ValidateFinalConfigState(staged_config, physical_memory_bytes, &failed_variable);
  if (!final_state) {
    return fail(failed_variable, final_state.error());
  }

  // The cache toggle is the sole fallible component effect. Run it before any
  // commit, callback, cache tuning, or logging so a failed toggle is atomic.
  if (cache_enabled_assigned) {
    if (cache_toggle_callback_) {
      auto toggled = cache_toggle_callback_(staged_config.cache.enabled);
      if (!toggled) {
        return fail("cache.enabled", toggled.error());
      }
    } else if (cache_manager_ != nullptr) {
      if (staged_config.cache.enabled) {
        if (!cache_manager_->Enable()) {
          return fail("cache.enabled", MakeError(ErrorCode::kCacheDisabled, "Cache cannot be enabled"));
        }
      } else {
        cache_manager_->Disable();
      }
    }
  }

  static_assert(std::is_nothrow_swappable_v<Config>);
  std::swap(base_config_, staged_config);
  runtime_values_.swap(staged_runtime_values);

  if (final_values.find("logging.level") != final_values.end()) {
    const auto& level = final_values.at("logging.level");
    if (level == "debug") {
      spdlog::set_level(spdlog::level::debug);
    } else if (level == "info") {
      spdlog::set_level(spdlog::level::info);
    } else if (level == "warn") {
      spdlog::set_level(spdlog::level::warn);
    } else if (level == "error") {
      spdlog::set_level(spdlog::level::err);
    }
  }
  if (final_values.find("logging.format") != final_values.end()) {
    const auto& format = final_values.at("logging.format");
    mygram::utils::StructuredLog::SetFormat(format == "json" ? mygram::utils::LogFormat::JSON
                                                             : mygram::utils::LogFormat::TEXT);
  }

  if (api_config_changed) {
    for (const auto& callback : api_config_callbacks_) {
      callback(base_config_.api.default_limit, base_config_.api.max_query_length);
    }
  }
  if (rate_limiter_changed && rate_limiter_callback_) {
    rate_limiter_callback_(base_config_.api.rate_limiting.enable,
                           static_cast<size_t>(base_config_.api.rate_limiting.capacity),
                           static_cast<size_t>(base_config_.api.rate_limiting.refill_rate));
  }
  if (cache_manager_ != nullptr) {
    if (cache_min_query_cost_assigned) {
      cache_manager_->SetMinQueryCost(base_config_.cache.min_query_cost_ms);
    }
    if (cache_ttl_assigned) {
      cache_manager_->SetTtl(base_config_.cache.ttl_seconds);
    }
  }

  for (const auto& [name, value] : final_values) {
    mygram::utils::StructuredLog().Event("variable_changed").Field("variable", name).Field("value", value).Info();
  }

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
  const std::string normalized_prefix = mygram::utils::ToLower(prefix);
  for (const auto& [name, config_value] : cached_config_variables_) {
    if (!normalized_prefix.empty() &&
        (name.size() < normalized_prefix.size() || name.compare(0, normalized_prefix.size(), normalized_prefix) != 0)) {
      continue;
    }

    const auto runtime_value = runtime_values_.find(name);
    const auto& value = runtime_value == runtime_values_.end() ? config_value : runtime_value->second;
    result[name] = {value, kMutableVariables.find(name) != kMutableVariables.end()};
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

std::optional<std::string> RuntimeVariableManager::GetVariableInternal(const std::string& variable_name) const {
  const auto runtime_value = runtime_values_.find(variable_name);
  if (runtime_value != runtime_values_.end()) {
    return runtime_value->second;
  }

  const auto config_value = cached_config_variables_.find(variable_name);
  if (config_value != cached_config_variables_.end()) {
    return config_value->second;
  }
  return std::nullopt;
}

}  // namespace mygramdb::config
