/**
 * @file main.cpp
 * @brief Entry point for MygramDB
 */

#include <csignal>
#include <iostream>
#include <memory>
#include <spdlog/spdlog.h>

namespace {
volatile std::sig_atomic_t g_shutdown_requested = 0;

/**
 * @brief Signal handler for graceful shutdown
 * @param signal Signal number
 */
void SignalHandler(int signal) {
  if (signal == SIGINT || signal == SIGTERM) {
    g_shutdown_requested = 1;
  }
}

}  // namespace

/**
 * @brief Main entry point
 * @param argc Argument count
 * @param argv Argument values
 * @return Exit code
 */
int main(int argc, char* argv[]) {
  // Setup signal handlers
  std::signal(SIGINT, SignalHandler);
  std::signal(SIGTERM, SignalHandler);

  // Setup logging
  spdlog::set_level(spdlog::level::info);
  spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%l] %v");

  spdlog::info("MygramDB v1.0.0 starting...");

  if (argc < 2) {
    spdlog::error("Usage: {} <config.yaml>", argv[0]);
    return 1;
  }

  const char* config_path = argv[1];
  spdlog::info("Loading configuration from: {}", config_path);

  // TODO: Load configuration
  // TODO: Initialize MySQL connection
  // TODO: Build indexes
  // TODO: Start servers
  // TODO: Wait for shutdown signal

  while (!g_shutdown_requested) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  spdlog::info("Shutdown requested, cleaning up...");
  // TODO: Cleanup resources

  spdlog::info("MygramDB stopped");
  return 0;
}
