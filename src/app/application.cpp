/**
 * @file application.cpp
 * @brief Main application class implementation
 */

#include "app/application.h"

#include <spdlog/spdlog.h>

#include <filesystem>
#include <fstream>
#include <iostream>

#include "utils/daemon_utils.h"
#include "utils/structured_log.h"
#include "version.h"

#ifndef _WIN32
#include <unistd.h>  // for getuid(), geteuid()
#endif

namespace mygramdb::app {

namespace {
constexpr int kShutdownCheckIntervalMs = 100;  // Shutdown check interval (ms)
}  // namespace

// NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays) - Standard C/C++ main signature
mygram::utils::Expected<std::unique_ptr<Application>, mygram::utils::Error> Application::Create(
    int argc, char* argv[]) {  // NOLINT(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays)
  // Step 1: Parse command-line arguments
  auto args_result = CommandLineParser::Parse(argc, argv);
  if (!args_result) {
    return mygram::utils::MakeUnexpected(args_result.error());
  }

  CommandLineArgs args = std::move(*args_result);

  // Handle help and version early (before loading config)
  if (args.show_help) {
    CommandLineParser::PrintHelp(argv[0]);  // NOLINT
    // Return special "success" application that exits immediately
    auto app = std::unique_ptr<Application>(new Application(std::move(args), nullptr));
    return app;
  }

  if (args.show_version) {
    CommandLineParser::PrintVersion();
    // Return special "success" application that exits immediately
    auto app = std::unique_ptr<Application>(new Application(std::move(args), nullptr));
    return app;
  }

  // Step 2: Load configuration
  auto config_mgr = ConfigurationManager::Create(args.config_file, args.schema_file);
  if (!config_mgr) {
    return mygram::utils::MakeUnexpected(config_mgr.error());
  }

  // Create application instance
  auto app = std::unique_ptr<Application>(new Application(std::move(args), std::move(*config_mgr)));
  return app;
}

Application::Application(CommandLineArgs args, std::unique_ptr<ConfigurationManager> config_mgr)
    : args_(std::move(args)), config_manager_(std::move(config_mgr)) {}

Application::~Application() {
  if (started_) {
    Stop();
  }
}

int Application::Run() {
  // Handle special modes (--help, --version, --config-test)
  int special_exit_code = HandleSpecialModes();
  if (special_exit_code >= 0) {
    return special_exit_code;  // Early exit
  }

  // Log startup message
  spdlog::info("{} starting...", Version::FullString());

  // Check root privilege
  auto root_check = CheckRootPrivilege();
  if (!root_check) {
    mygram::utils::StructuredLog()
        .Event("application_error")
        .Field("type", "root_privilege_check_failed")
        .Field("phase", "startup")
        .Field("error", root_check.error().to_string())
        .Error();
    return 1;
  }

  // Apply logging configuration
  auto logging_result = config_manager_->ApplyLoggingConfig();
  if (!logging_result) {
    mygram::utils::StructuredLog()
        .Event("application_error")
        .Field("type", "logging_config_failed")
        .Field("phase", "startup")
        .Field("error", logging_result.error().to_string())
        .Error();
    return 1;
  }

  // Daemonize if requested (must be done before opening files/sockets)
  auto daemon_result = DaemonizeIfRequested();
  if (!daemon_result) {
    mygram::utils::StructuredLog()
        .Event("application_error")
        .Field("type", "daemonization_failed")
        .Field("phase", "startup")
        .Field("error", daemon_result.error().to_string())
        .Error();
    return 1;
  }

  // Verify dump directory permissions
  auto dump_check = VerifyDumpDirectory();
  if (!dump_check) {
    mygram::utils::StructuredLog()
        .Event("application_error")
        .Field("type", "dump_directory_verification_failed")
        .Field("phase", "startup")
        .Field("error", dump_check.error().to_string())
        .Error();
    return 1;
  }

  // Initialize components
  auto init_result = Initialize();
  if (!init_result) {
    mygram::utils::StructuredLog()
        .Event("application_error")
        .Field("type", "initialization_failed")
        .Field("phase", "startup")
        .Field("error", init_result.error().to_string())
        .Error();
    return 1;
  }

  // Start servers
  auto start_result = Start();
  if (!start_result) {
    mygram::utils::StructuredLog()
        .Event("application_error")
        .Field("type", "server_startup_failed")
        .Field("phase", "startup")
        .Field("error", start_result.error().to_string())
        .Error();
    return 1;
  }

  // Run main loop (blocks until shutdown signal)
  RunMainLoop();

  // Graceful shutdown
  Stop();

  spdlog::info("MygramDB stopped");
  return 0;
}

mygram::utils::Expected<void, mygram::utils::Error> Application::Initialize() {
  if (initialized_) {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError, "Application already initialized"));
  }

  // Setup signal handlers
  auto signal_mgr = SignalManager::Create();
  if (!signal_mgr) {
    return mygram::utils::MakeUnexpected(signal_mgr.error());
  }
  signal_manager_ = std::move(*signal_mgr);

  // Initialize server orchestrator
  ServerOrchestrator::Dependencies deps{
      .config = config_manager_->GetConfig(),
      .signal_manager = *signal_manager_,
      .dump_dir = config_manager_->GetConfig().dump.dir,
  };

  auto orchestrator = ServerOrchestrator::Create(deps);
  if (!orchestrator) {
    return mygram::utils::MakeUnexpected(orchestrator.error());
  }
  server_orchestrator_ = std::move(*orchestrator);

  // Initialize server components (tables, MySQL, servers)
  auto init_result = server_orchestrator_->Initialize();
  if (!init_result) {
    return mygram::utils::MakeUnexpected(init_result.error());
  }

  initialized_ = true;
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> Application::Start() {
  if (!initialized_) {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError, "Cannot start: not initialized"));
  }

  if (started_) {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError, "Already started"));
  }

  // Start servers
  auto start_result = server_orchestrator_->Start();
  if (!start_result) {
    return mygram::utils::MakeUnexpected(start_result.error());
  }

  started_ = true;
  return {};
}

void Application::RunMainLoop() {
  spdlog::debug("Entering main loop...");

  while (!signal_manager_->IsShutdownRequested()) {
    // Check for log rotation signal (SIGUSR1)
    if (signal_manager_->ConsumeLogReopenRequest()) {
      auto reopen_result = config_manager_->ReopenLogFile();
      if (!reopen_result) {
        // Log to stderr as file logging may be broken
        std::cerr << "Failed to reopen log file: " << reopen_result.error().to_string() << '\n';
      }
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(kShutdownCheckIntervalMs));
  }

  spdlog::debug("Shutdown requested, cleaning up...");
}

void Application::Stop() {
  if (!started_) {
    return;  // Nothing to stop
  }

  // Stop server orchestrator (stops all servers in reverse order)
  if (server_orchestrator_) {
    server_orchestrator_->Stop();
  }

  started_ = false;
}

int Application::HandleSpecialModes() {
  // Help and version are handled in Create() (print and return immediately)
  if (args_.show_help || args_.show_version) {
    return 0;  // Success exit
  }

  // Config test mode
  if (args_.config_test_mode) {
    return HandleConfigTestMode();
  }

  return -1;  // Not a special mode, continue normal execution
}

int Application::HandleConfigTestMode() {
  // Print configuration test results
  return config_manager_->PrintConfigTest();
}

mygram::utils::Expected<void, mygram::utils::Error> Application::CheckRootPrivilege() {  // static
#ifndef _WIN32
  if (getuid() == 0 || geteuid() == 0) {
    std::cerr << "ERROR: Running MygramDB as root is not allowed for security reasons.\n";
    std::cerr << "Please run as a non-privileged user.\n";
    std::cerr << "\n";
    std::cerr << "Recommended approaches:\n";
    std::cerr << "  - systemd: Use User= and Group= directives in service file\n";
    std::cerr << "  - Docker: Use USER directive in Dockerfile (already configured)\n";
    std::cerr << "  - Manual: Run as a dedicated user (e.g., 'sudo -u mygramdb mygramdb -c config.yaml')\n";
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kPermissionDenied, "Running as root is not allowed"));
  }
#endif
  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> Application::VerifyDumpDirectory() {
  const std::string& dump_dir = config_manager_->GetConfig().dump.dir;

  try {
    std::filesystem::path dump_path(dump_dir);

    // Create directory if it doesn't exist
    if (!std::filesystem::exists(dump_path)) {
      spdlog::info("Creating dump directory: {}", dump_dir);
      std::filesystem::create_directories(dump_path);
    }

    // SECURITY: Validate that the dump directory is within allowed bounds
    // Resolve to canonical path to prevent directory traversal attacks
    std::filesystem::path canonical_dump = std::filesystem::canonical(dump_path);

    // Define the base allowed directory (current working directory or parent)
    // This prevents malicious configurations like "../../../etc/" from writing outside project
    std::filesystem::path current_dir = std::filesystem::current_path();
    std::filesystem::path allowed_base = current_dir.parent_path();  // Allow one level up for flexibility

    // Check if canonical dump path starts with allowed base
    auto dump_parts = canonical_dump.begin();
    auto base_parts = allowed_base.begin();
    bool within_bounds = true;

    while (base_parts != allowed_base.end()) {
      if (dump_parts == canonical_dump.end() || *dump_parts != *base_parts) {
        within_bounds = false;
        break;
      }
      ++dump_parts;
      ++base_parts;
    }

    if (!within_bounds) {
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
          mygram::utils::ErrorCode::kPermissionDenied,
          "Dump directory path traversal detected. Path must be within allowed directory: " + canonical_dump.string() +
              " is outside " + allowed_base.string()));
    }

    // Check if directory is writable by attempting to create a test file
    std::filesystem::path test_file = dump_path / ".write_test";
    std::ofstream test_stream(test_file);
    if (!test_stream.is_open()) {
      return mygram::utils::MakeUnexpected(mygram::utils::MakeError(mygram::utils::ErrorCode::kPermissionDenied,
                                                                    "Dump directory is not writable: " + dump_dir));
    }
    test_stream.close();
    std::filesystem::remove(test_file);

    spdlog::debug("Dump directory verified: {} (canonical: {})", dump_dir, canonical_dump.string());
  } catch (const std::exception& e) {
    return mygram::utils::MakeUnexpected(mygram::utils::MakeError(
        mygram::utils::ErrorCode::kIOError, "Failed to verify dump directory: " + std::string(e.what())));
  }

  return {};
}

mygram::utils::Expected<void, mygram::utils::Error> Application::DaemonizeIfRequested() const {
  if (!args_.daemon_mode) {
    return {};  // Not requested, nothing to do
  }

  spdlog::info("Daemonizing process...");
  if (!utils::Daemonize()) {
    return mygram::utils::MakeUnexpected(
        mygram::utils::MakeError(mygram::utils::ErrorCode::kInternalError, "Failed to daemonize process"));
  }

  // Note: After daemonization, stdout/stderr are redirected to /dev/null
  // All output must go through spdlog to be visible (configure file logging if needed)
  return {};
}

}  // namespace mygramdb::app
