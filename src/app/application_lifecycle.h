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
 * @brief Run checks that must complete before any configured file is opened.
 *
 * Root rejection deliberately precedes daemon path resolution and logging
 * setup so a forbidden root invocation cannot create operator-controlled
 * directories or files.
 */
template <typename PrivilegeCheck, typename DaemonPathResolver>
mygram::utils::Expected<void, mygram::utils::Error> RunPreFileStartupChecks(bool daemon_mode,
                                                                            PrivilegeCheck&& privilege_check,
                                                                            DaemonPathResolver&& daemon_path_resolver) {
  auto privilege_result = privilege_check();
  if (!privilege_result) {
    return mygram::utils::MakeUnexpected(privilege_result.error());
  }
  if (daemon_mode) {
    auto path_result = daemon_path_resolver();
    if (!path_result) {
      return mygram::utils::MakeUnexpected(path_result.error());
    }
  }
  return {};
}

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
