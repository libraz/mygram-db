/**
 * @file main.cpp
 * @brief Entry point for MygramDB
 */

#include <iostream>

#ifdef USE_MYSQL
#include <mysql.h>
#endif

#include "app/application.h"

/**
 * @brief Main entry point
 * @param argc Argument count
 * @param argv Argument values
 * @return Exit code (0 = success, non-zero = error)
 */
int main(int argc, char* argv[]) {
#ifdef USE_MYSQL
  // Initialize MySQL client library before any threads are created.
  // mysql_library_init() is not thread-safe and must be called from main thread.
  if (mysql_library_init(0, nullptr, nullptr) != 0) {
    std::cerr << "Failed to initialize MySQL client library\n";
    return 1;
  }
#endif

  // Create application
  auto app = mygramdb::app::Application::Create(argc, argv);
  if (!app) {
    std::cerr << "Failed to create application: " << app.error().to_string() << "\n";
#ifdef USE_MYSQL
    mysql_library_end();
#endif
    return 1;
  }

  // Run application
  int result = (*app)->Run();

#ifdef USE_MYSQL
  mysql_library_end();
#endif

  return result;
}
