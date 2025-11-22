/**
 * @file runtime_variable_manager.h
 * @brief Runtime variable manager for MySQL-style SET VARIABLE
 */

#pragma once

#include <functional>
#include <map>
#include <memory>
#include <shared_mutex>
#include <string>
#include <utility>

#include "config/config.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::cache {
class CacheManager;
}  // namespace mygramdb::cache

namespace mygramdb::config {

/**
 * @brief Runtime variable information
 */
struct VariableInfo {
  std::string value;     ///< Current value as string
  bool mutable_{false};  ///< True if variable can be changed at runtime
};

/**
 * @brief Runtime variable manager (MySQL-style SET)
 *
 * Responsibilities:
 * - Store runtime-modifiable configuration variables
 * - Validate variable changes before applying
 * - Apply changes to active components (logging, cache, rate limiter)
 * - Provide SHOW VARIABLES functionality
 *
 * Thread Safety: Thread-safe (uses shared_mutex)
 *
 * Mutable Variables (can be changed at runtime):
 * - logging.level (debug/info/warn/error)
 * - logging.format (json/text)
 * - mysql.host (triggers reconnection)
 * - mysql.port (triggers reconnection)
 * - api.default_limit (5-1000)
 * - api.max_query_length (>0)
 * - api.rate_limiting.enable (true/false)
 * - api.rate_limiting.capacity (>0)
 * - api.rate_limiting.refill_rate (>0)
 * - cache.enabled (true/false)
 * - cache.min_query_cost_ms (>=0)
 * - cache.ttl_seconds (>=0)
 *
 * Immutable Variables (require restart):
 * - mysql.user, mysql.password, mysql.database
 * - mysql.use_gtid, mysql.ssl_*
 * - tables[*].* (index structure)
 * - memory.* (allocator initialization)
 * - build.* (snapshot building)
 * - replication.* (replication thread)
 * - dump.* (dump thread)
 * - api.tcp.*, api.http.* (server sockets)
 * - network.allow_cidrs (security)
 */
class RuntimeVariableManager {
 public:
  /**
   * @brief Create manager from initial config
   * @param initial_config Initial configuration
   * @return Expected with unique_ptr or error
   */
  static mygram::utils::Expected<std::unique_ptr<RuntimeVariableManager>, mygram::utils::Error> Create(
      const Config& initial_config);

  RuntimeVariableManager(const RuntimeVariableManager&) = delete;
  RuntimeVariableManager& operator=(const RuntimeVariableManager&) = delete;
  RuntimeVariableManager(RuntimeVariableManager&&) = delete;
  RuntimeVariableManager& operator=(RuntimeVariableManager&&) = delete;
  ~RuntimeVariableManager() = default;

  /**
   * @brief Set runtime variable (SET command)
   * @param variable_name Dot-separated name (e.g., "logging.level")
   * @param value New value as string
   * @return Expected with void or error
   *
   * Side effects:
   * - Updates internal state
   * - Applies change to relevant components (spdlog, cache, etc.)
   * - Logs the change
   *
   * Examples:
   * - SetVariable("logging.level", "debug")
   * - SetVariable("mysql.host", "192.168.1.20")
   * - SetVariable("cache.enabled", "true")
   */
  mygram::utils::Expected<void, mygram::utils::Error> SetVariable(const std::string& variable_name,
                                                                  const std::string& value);

  /**
   * @brief Get variable value
   * @param variable_name Dot-separated name
   * @return Expected with value string or error
   */
  mygram::utils::Expected<std::string, mygram::utils::Error> GetVariable(const std::string& variable_name) const;

  /**
   * @brief Get all variables with mutability info (SHOW VARIABLES)
   * @param prefix Optional prefix filter (e.g., "logging", "mysql")
   * @return Map of variable_name -> VariableInfo
   */
  std::map<std::string, VariableInfo> GetAllVariables(const std::string& prefix = "") const;

  /**
   * @brief Check if variable is mutable
   * @param variable_name Dot-separated name
   * @return True if variable can be changed at runtime
   */
  static bool IsMutable(const std::string& variable_name);

  /**
   * @brief Set MySQL reconnection callback
   * @param callback Function to call when mysql.host or mysql.port changes
   *
   * The callback should perform the reconnection and return success/error.
   * It will be called from SetVariable() when mysql.host or mysql.port changes.
   */
  void SetMysqlReconnectCallback(
      std::function<mygram::utils::Expected<void, mygram::utils::Error>(const std::string& host, int port)> callback);

  /**
   * @brief Set cache toggle callback
   * @param callback Function to call when cache.enabled changes
   */
  void SetCacheToggleCallback(
      std::function<mygram::utils::Expected<void, mygram::utils::Error>(bool enabled)> callback);

  /**
   * @brief Set cache manager for runtime configuration updates
   * @param cache_manager Pointer to CacheManager (non-owning)
   *
   * This allows RuntimeVariableManager to directly update cache settings
   * like min_query_cost_ms and ttl_seconds.
   */
  void SetCacheManager(cache::CacheManager* cache_manager);

  /**
   * @brief Set rate limiter configuration callback
   * @param callback Function to call when rate limiting parameters change
   *
   * The callback receives capacity and refill_rate values.
   */
  void SetRateLimiterCallback(std::function<void(size_t capacity, size_t refill_rate)> callback);

 private:
  RuntimeVariableManager() = default;

  // Thread-safe storage (readers-writer lock)
  mutable std::shared_mutex mutex_;

  // Current runtime values (only mutable variables)
  std::map<std::string, std::string> runtime_values_;

  // Original config (immutable variables + defaults)
  Config base_config_;

  // Callbacks and component references
  std::function<mygram::utils::Expected<void, mygram::utils::Error>(const std::string& host, int port)>
      mysql_reconnect_callback_;
  std::function<mygram::utils::Expected<void, mygram::utils::Error>(bool enabled)> cache_toggle_callback_;
  std::function<void(size_t capacity, size_t refill_rate)> rate_limiter_callback_;
  cache::CacheManager* cache_manager_ = nullptr;  // Non-owning pointer for cache configuration updates

  /**
   * @brief Apply logging.level change
   */
  static mygram::utils::Expected<void, mygram::utils::Error> ApplyLoggingLevel(const std::string& value);

  /**
   * @brief Apply logging.format change
   */
  static mygram::utils::Expected<void, mygram::utils::Error> ApplyLoggingFormat(const std::string& value);

  /**
   * @brief Apply mysql.host change (triggers reconnection)
   */
  mygram::utils::Expected<void, mygram::utils::Error> ApplyMysqlHost(const std::string& value);

  /**
   * @brief Apply mysql.port change (triggers reconnection)
   */
  mygram::utils::Expected<void, mygram::utils::Error> ApplyMysqlPort(int value);

  /**
   * @brief Apply api.default_limit change
   */
  mygram::utils::Expected<void, mygram::utils::Error> ApplyApiDefaultLimit(int value);

  /**
   * @brief Apply api.max_query_length change
   */
  mygram::utils::Expected<void, mygram::utils::Error> ApplyApiMaxQueryLength(int value);

  /**
   * @brief Apply api.rate_limiting.enable change
   */
  mygram::utils::Expected<void, mygram::utils::Error> ApplyRateLimitingEnable(bool value);

  /**
   * @brief Apply api.rate_limiting.capacity change
   */
  mygram::utils::Expected<void, mygram::utils::Error> ApplyRateLimitingCapacity(int value);

  /**
   * @brief Apply api.rate_limiting.refill_rate change
   */
  mygram::utils::Expected<void, mygram::utils::Error> ApplyRateLimitingRefillRate(int value);

  /**
   * @brief Apply cache.enabled change
   */
  mygram::utils::Expected<void, mygram::utils::Error> ApplyCacheEnabled(bool value);

  /**
   * @brief Apply cache.min_query_cost_ms change
   */
  mygram::utils::Expected<void, mygram::utils::Error> ApplyCacheMinQueryCost(double value);

  /**
   * @brief Apply cache.ttl_seconds change
   */
  mygram::utils::Expected<void, mygram::utils::Error> ApplyCacheTtl(int value);

  /**
   * @brief Get current value for a variable (internal, no lock)
   */
  std::string GetVariableInternal(const std::string& variable_name) const;

  /**
   * @brief Initialize runtime values from config
   */
  void InitializeRuntimeValues();

  /**
   * @brief Parse boolean value
   */
  static mygram::utils::Expected<bool, mygram::utils::Error> ParseBool(const std::string& value);

  /**
   * @brief Parse integer value
   */
  static mygram::utils::Expected<int, mygram::utils::Error> ParseInt(const std::string& value);

  /**
   * @brief Parse double value
   */
  static mygram::utils::Expected<double, mygram::utils::Error> ParseDouble(const std::string& value);
};

}  // namespace mygramdb::config
