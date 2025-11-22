/**
 * @file application.h
 * @brief Main application class
 */

#ifndef MYGRAMDB_APP_APPLICATION_H_
#define MYGRAMDB_APP_APPLICATION_H_

#include <memory>

#include "app/command_line_parser.h"
#include "app/configuration_manager.h"
#include "app/server_orchestrator.h"
#include "app/signal_manager.h"
#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::app {

// Import Expected from utils namespace
using mygram::utils::Error;
using mygram::utils::Expected;

/**
 * @brief Main application class
 *
 * Orchestrates the entire application lifecycle:
 * 1. Parse command-line arguments
 * 2. Load configuration
 * 3. Setup signal handlers
 * 4. Initialize components
 * 5. Run main loop (poll signals for shutdown)
 * 6. Graceful shutdown
 *
 * Design Pattern: Facade + Orchestrator
 * - Application is the single entry point for main()
 * - Delegates to specialized components for each concern
 * - Ensures proper initialization and cleanup order
 *
 * Usage:
 * @code
 * auto app = Application::Create(argc, argv);
 * if (!app) {
 *   std::cerr << "Failed to create application: " << app.error().to_string() << "\n";
 *   return 1;
 * }
 * return (*app)->Run();
 * @endcode
 */
class Application {
 public:
  /**
   * @brief Create application from command-line arguments
   * @param argc Argument count
   * @param argv Argument values
   * @return Expected with application instance or error
   *
   * This factory method:
   * 1. Parses command-line arguments
   * 2. Loads configuration file
   * 3. Returns application instance ready to run
   *
   * Note: Does NOT apply logging config or initialize servers
   *       (those happen in Run() method)
   */
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays) - Standard C/C++ main signature
  static Expected<std::unique_ptr<Application>, mygram::utils::Error> Create(int argc, char* argv[]);

  /**
   * @brief Destructor (ensures cleanup even on exception)
   */
  ~Application();

  // Non-copyable, non-movable
  Application(const Application&) = delete;
  Application& operator=(const Application&) = delete;
  Application(Application&&) = delete;
  Application& operator=(Application&&) = delete;

  /**
   * @brief Run the application
   * @return Exit code (0 = success, non-zero = error)
   *
   * Workflow:
   * 1. Handle special modes (--help, --version, --config-test)
   * 2. Check root privilege
   * 3. Apply logging configuration
   * 4. Daemonize (if --daemon)
   * 5. Verify dump directory
   * 6. Setup signal handlers
   * 7. Initialize server components (including RuntimeVariableManager)
   * 8. Start servers
   * 9. Main loop (signal polling for shutdown)
   * 10. Graceful shutdown
   */
  int Run();

 private:
  Application(CommandLineArgs args, std::unique_ptr<ConfigurationManager> config_mgr);

  // Lifecycle steps
  Expected<void, mygram::utils::Error> Initialize();
  Expected<void, mygram::utils::Error> Start();
  void RunMainLoop();
  void Stop();

  // Special modes (return exit code)
  int HandleSpecialModes();
  int HandleConfigTestMode();

  // Pre-initialization checks
  static Expected<void, mygram::utils::Error> CheckRootPrivilege();
  Expected<void, mygram::utils::Error> VerifyDumpDirectory();
  Expected<void, mygram::utils::Error> DaemonizeIfRequested() const;

  // Command-line arguments
  CommandLineArgs args_;

  // Components (initialization order)
  std::unique_ptr<ConfigurationManager> config_manager_;
  std::unique_ptr<SignalManager> signal_manager_;
  std::unique_ptr<ServerOrchestrator> server_orchestrator_;

  // State
  bool initialized_{false};
  bool started_{false};
};

}  // namespace mygramdb::app

#endif  // MYGRAMDB_APP_APPLICATION_H_
