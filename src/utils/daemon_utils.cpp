/**
 * @file daemon_utils.cpp
 * @brief Daemon process utilities implementation
 */

#include "utils/daemon_utils.h"

#ifndef _WIN32
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cstdlib>
#endif

#include <spdlog/spdlog.h>

#include "utils/structured_log.h"

namespace mygramdb::utils {

bool Daemonize() {
#ifdef _WIN32
  // Windows does not support traditional UNIX daemonization
  mygram::utils::StructuredLog().Event("daemon_not_supported").Field("platform", "Windows").Warn();
  return false;
#else
  // Step 1: Fork and exit parent
  pid_t pid = fork();

  if (pid < 0) {
    // Fork failed
    mygram::utils::StructuredLog()
        .Event("daemon_error")
        .Field("type", "fork_failed")
        .Field("context", "first_fork")
        .Field("error", "Failed to fork process for daemonization")
        .Error();
    return false;
  }

  if (pid > 0) {
    // Parent process - exit successfully
    std::exit(0);
  }

  // Step 2: Create a new session
  // The child process becomes session leader
  if (setsid() < 0) {
    mygram::utils::StructuredLog()
        .Event("daemon_error")
        .Field("type", "setsid_failed")
        .Field("context", "create_new_session")
        .Field("error", "Failed to create new session with setsid()")
        .Error();
    return false;
  }

  // Step 3: Fork again (optional but recommended)
  // This ensures the daemon cannot acquire a controlling terminal
  pid = fork();

  if (pid < 0) {
    mygram::utils::StructuredLog()
        .Event("daemon_error")
        .Field("type", "fork_failed")
        .Field("context", "second_fork")
        .Field("error", "Failed to fork process (second fork)")
        .Error();
    return false;
  }

  if (pid > 0) {
    // First child exits
    std::exit(0);
  }

  // Step 4: Change working directory to root
  // This prevents the daemon from holding any directory in use
  if (chdir("/") < 0) {
    mygram::utils::StructuredLog().Event("daemon_warning").Field("type", "chdir_failed").Field("target", "/").Warn();
    // Not fatal, continue
  }

  // Step 5: Set file mode creation mask
  umask(0);

  // Step 6: Redirect standard file descriptors to /dev/null
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg)
  int null_fd = open("/dev/null", O_RDWR, 0);

  if (null_fd != -1) {
    // Redirect stdin, stdout, stderr
    dup2(null_fd, STDIN_FILENO);
    dup2(null_fd, STDOUT_FILENO);
    dup2(null_fd, STDERR_FILENO);

    if (null_fd > STDERR_FILENO) {
      close(null_fd);
    }
  } else {
    mygram::utils::StructuredLog()
        .Event("daemon_warning")
        .Field("error", "Failed to open /dev/null for file descriptor redirection")
        .Warn();
    // Not fatal, continue
  }

  mygram::utils::StructuredLog().Event("process_daemonized").Info();
  return true;
#endif
}

}  // namespace mygramdb::utils
