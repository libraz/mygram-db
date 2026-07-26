/**
 * @file application_lifecycle.h
 * @brief Helpers that enforce process-level application lifetime ordering
 */

#ifndef MYGRAMDB_APP_APPLICATION_LIFECYCLE_H_
#define MYGRAMDB_APP_APPLICATION_LIFECYCLE_H_

#include <iostream>
#include <memory>

#include "utils/error.h"
#include "utils/expected.h"

namespace mygramdb::app {

/**
 * @brief Run an application while keeping its ownership inside this scope.
 *
 * The application is destroyed before this function returns. Process entry
 * points can therefore perform global library teardown immediately after the
 * call without risking that application-owned handles outlive the library.
 *
 * @tparam ApplicationType Application-like type exposing int Run().
 * @param application Application creation result.
 * @param error_stream Stream used for creation diagnostics.
 * @return Application exit code, or 1 when creation failed.
 */
template <typename ApplicationType>
int RunApplicationInScope(mygram::utils::Expected<std::unique_ptr<ApplicationType>, mygram::utils::Error> application,
                          std::ostream& error_stream = std::cerr) {
  if (!application) {
    error_stream << "Failed to create application: " << application.error().to_string() << '\n';
    return 1;
  }

  return (*application)->Run();
}

}  // namespace mygramdb::app

#endif  // MYGRAMDB_APP_APPLICATION_LIFECYCLE_H_
