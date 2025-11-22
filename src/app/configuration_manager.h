/**
 * @file configuration_manager.h
 * @brief Configuration manager for loading and validating configuration files
 */

#ifndef MYGRAMDB_APP_CONFIGURATION_MANAGER_H_
#define MYGRAMDB_APP_CONFIGURATION_MANAGER_H_

#include <memory>
#include <string>

#include "config/config.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::app {

// Import Expected from utils namespace
using mygram::utils::Error;
using mygram::utils::Expected;

/**
 * @brief Configuration manager
 *
 * Responsibilities:
 * - Load configuration from file (YAML/JSON)
 * - Validate against schema
 * - Apply logging configuration changes
 * - Provide read-only access to configuration
 *
 * Design Pattern: Factory + Facade
 * - Create() validates config before returning instance
 * - Owns the Config object (single source of truth)
 * - Provides read-only access via GetConfig()
 *
 * Note: Runtime configuration changes are handled by RuntimeVariableManager
 */
class ConfigurationManager {
 public:
  /**
   * @brief Create manager and load initial configuration
   * @param config_file Path to configuration file
   * @param schema_file Optional schema file path (empty = use built-in)
   * @return Expected with manager instance or error
   *
   * This factory method:
   * 1. Loads configuration from file
   * 2. Validates against schema
   * 3. Returns manager instance if valid, error otherwise
   */
  static Expected<std::unique_ptr<ConfigurationManager>, mygram::utils::Error> Create(
      const std::string& config_file, const std::string& schema_file = "");

  ~ConfigurationManager() = default;

  // Non-copyable, non-movable (owns configuration state)
  ConfigurationManager(const ConfigurationManager&) = delete;
  ConfigurationManager& operator=(const ConfigurationManager&) = delete;
  ConfigurationManager(ConfigurationManager&&) = delete;
  ConfigurationManager& operator=(ConfigurationManager&&) = delete;

  /**
   * @brief Get current configuration (read-only)
   * @return Const reference to current config
   */
  const config::Config& GetConfig() const { return config_; }

  /**
   * @brief Test mode: Print configuration details and exit
   * @return Exit code (0 = success)
   *
   * This method prints configuration details to stdout:
   * - MySQL connection settings
   * - Table configurations
   * - API endpoints
   * - Replication settings
   * - Logging level
   */
  int PrintConfigTest() const;

  /**
   * @brief Apply logging configuration
   * @return Expected with void or error
   *
   * Side effects:
   * - Sets spdlog log level (debug/info/warn/error)
   * - Configures file or console output
   * - Creates log directory if needed
   *
   * This method should be called:
   * - After initial configuration load
   *
   * Note: Runtime logging level changes are handled by RuntimeVariableManager
   */
  Expected<void, mygram::utils::Error> ApplyLoggingConfig();

  /**
   * @brief Get config file path
   */
  const std::string& GetConfigFilePath() const { return config_file_; }

  /**
   * @brief Get schema file path
   */
  const std::string& GetSchemaFilePath() const { return schema_file_; }

 private:
  ConfigurationManager(std::string config_file, std::string schema_file, config::Config initial_config);

  std::string config_file_;
  std::string schema_file_;
  config::Config config_;
};

}  // namespace mygramdb::app

#endif  // MYGRAMDB_APP_CONFIGURATION_MANAGER_H_
