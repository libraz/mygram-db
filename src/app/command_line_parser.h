/**
 * @file command_line_parser.h
 * @brief Command-line argument parser
 */

#ifndef MYGRAMDB_APP_COMMAND_LINE_PARSER_H_
#define MYGRAMDB_APP_COMMAND_LINE_PARSER_H_

#include <string>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::app {

// Import Expected from utils namespace
using mygram::utils::Error;
using mygram::utils::Expected;

/**
 * @brief Parsed command-line arguments
 */
struct CommandLineArgs {
  std::string config_file;
  std::string schema_file;  ///< Optional JSON Schema file path
  bool daemon_mode = false;
  bool config_test_mode = false;
  bool show_help = false;
  bool show_version = false;
};

/**
 * @brief Command-line argument parser
 *
 * This class provides static methods for parsing command-line arguments
 * following POSIX conventions. It supports both short (-c) and long (--config)
 * option formats, as well as positional arguments for backward compatibility.
 */
class CommandLineParser {
 public:
  /**
   * @brief Parse command line arguments
   * @param argc Argument count
   * @param argv Argument values
   * @return Expected with parsed arguments or error
   *
   * Supported options:
   * - -c, --config <file>: Configuration file path
   * - -d, --daemon: Run as daemon (background process)
   * - -t, --config-test: Test configuration file and exit
   * - -s, --schema <file>: Use custom JSON Schema
   * - -h, --help: Show help message
   * - -v, --version: Show version information
   * - Positional argument: Configuration file path (backward compatibility)
   *
   * @note Help and version flags take precedence (set show_help/show_version flags)
   */
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays) - Standard C/C++ main signature
  static Expected<CommandLineArgs, mygram::utils::Error> Parse(int argc, char* argv[]);

  /**
   * @brief Print help message to stdout
   * @param program_name Program name (argv[0])
   */
  static void PrintHelp(const char* program_name);

  /**
   * @brief Print version information to stdout
   */
  static void PrintVersion();

 private:
  // Private constructor (utility class - all static methods)
  CommandLineParser() = default;
};

}  // namespace mygramdb::app

#endif  // MYGRAMDB_APP_COMMAND_LINE_PARSER_H_
