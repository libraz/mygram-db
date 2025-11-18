/**
 * @file main.cpp
 * @brief Entry point for MygramDB
 */

#include <iostream>

#include "app/application.h"

/**
 * @brief Main entry point
 * @param argc Argument count
 * @param argv Argument values
 * @return Exit code (0 = success, non-zero = error)
 */
int main(int argc, char* argv[]) {
  // Create application
  auto app = mygramdb::app::Application::Create(argc, argv);
  if (!app) {
    std::cerr << "Failed to create application: " << app.error().to_string() << "\n";
    return 1;
  }

  // Run application
  return (*app)->Run();
}
