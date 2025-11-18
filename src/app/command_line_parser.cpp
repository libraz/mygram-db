/**
 * @file command_line_parser.cpp
 * @brief Command-line argument parser implementation
 */

#include "app/command_line_parser.h"

#include <iostream>

#include "version.h"

namespace mygramdb::app {

namespace {

/**
 * @brief Check if argument matches short or long option
 * @param arg Command-line argument
 * @param short_opt Short option (e.g., "-c")
 * @param long_opt Long option (e.g., "--config")
 * @return True if argument matches either option
 */
bool MatchesOption(const std::string& arg, const char* short_opt, const char* long_opt) {
  return arg == short_opt || arg == long_opt;
}

}  // namespace

// NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays) - Standard C/C++ main signature
Expected<CommandLineArgs, mygram::utils::Error> CommandLineParser::Parse(int argc, char* argv[]) {
  // NOLINTBEGIN(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  CommandLineArgs args;

  if (argc < 1) {
    return MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInvalidArgument, "Invalid argument count (argc < 1)"));
  }

  // Handle help and version flags first (early exit)
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "-h" || arg == "--help") {
      args.show_help = true;
      return args;  // Early return - no further parsing needed
    }
    if (arg == "-v" || arg == "--version") {
      args.show_version = true;
      return args;  // Early return - no further parsing needed
    }
  }

  // Require at least one argument (config file or flag)
  if (argc < 2) {
    return MakeUnexpected(mygram::utils::MakeError(mygram::utils::ErrorCode::kInvalidArgument,
                                                   "No arguments provided. Use --help for usage."));
  }

  // Parse all other arguments
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];

    if (MatchesOption(arg, "-c", "--config")) {
      // Config file argument
      if (i + 1 >= argc) {
        return MakeUnexpected(mygram::utils::MakeError(mygram::utils::ErrorCode::kInvalidArgument,
                                                       "--config requires a file path argument"));
      }
      args.config_file = argv[++i];
    } else if (MatchesOption(arg, "-d", "--daemon")) {
      // Daemon mode
      args.daemon_mode = true;
    } else if (MatchesOption(arg, "-t", "--config-test")) {
      // Config test mode
      args.config_test_mode = true;
    } else if (MatchesOption(arg, "-s", "--schema")) {
      // Custom schema file
      if (i + 1 >= argc) {
        return MakeUnexpected(mygram::utils::MakeError(mygram::utils::ErrorCode::kInvalidArgument,
                                                       "--schema requires a file path argument"));
      }
      args.schema_file = argv[++i];
    } else if (arg[0] == '-') {
      // Unknown option
      return MakeUnexpected(
          mygram::utils::MakeError(mygram::utils::ErrorCode::kInvalidArgument, "Unknown option: " + arg));
    } else {
      // Positional argument (backward compatibility: config file without -c flag)
      if (args.config_file.empty()) {
        args.config_file = arg;
      } else {
        return MakeUnexpected(
            mygram::utils::MakeError(mygram::utils::ErrorCode::kInvalidArgument,
                                     "Unexpected positional argument: " + arg + " (config file already specified)"));
      }
    }
  }

  // Validate: config file is required (unless help/version already handled)
  if (args.config_file.empty()) {
    return MakeUnexpected(mygram::utils::MakeError(mygram::utils::ErrorCode::kInvalidArgument,
                                                   "Configuration file path required. Use --help for usage."));
  }

  // NOLINTEND(cppcoreguidelines-pro-bounds-pointer-arithmetic)
  return args;
}

void CommandLineParser::PrintHelp(const char* program_name) {
  std::cout << "Usage: " << program_name << " [OPTIONS] <config.yaml|config.json>\n";
  std::cout << "       " << program_name << " -c <config.yaml|config.json> [OPTIONS]\n";
  std::cout << "\n";
  std::cout << "Options:\n";
  std::cout << "  -c, --config <file>            Configuration file path\n";
  std::cout << "  -d, --daemon                   Run as daemon (background process)\n";
  std::cout << "  -t, --config-test              Test configuration file and exit\n";
  std::cout << "  -s, --schema <schema.json>     Use custom JSON Schema (optional)\n";
  std::cout << "  -h, --help                     Show this help message\n";
  std::cout << "  -v, --version                  Show version information\n";
  std::cout << "\n";
  std::cout << "Configuration file format (auto-detected):\n";
  std::cout << "  - YAML (.yaml, .yml) - validated against built-in schema\n";
  std::cout << "  - JSON (.json)       - validated against built-in schema\n";
  std::cout << "\n";
  std::cout << "Note: All configurations are validated automatically using the built-in\n";
  std::cout << "      JSON Schema. Use --schema only to override with a custom schema.\n";
}

void CommandLineParser::PrintVersion() {
  std::cout << Version::FullString() << "\n";
}

}  // namespace mygramdb::app
