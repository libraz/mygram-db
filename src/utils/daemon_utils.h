/**
 * @file daemon_utils.h
 * @brief Daemon process utilities
 */

#pragma once

namespace mygramdb::utils {

/**
 * @brief Daemonize the current process
 *
 * This function performs the following steps:
 * 1. Fork the process and exit the parent
 * 2. Create a new session with setsid()
 * 3. Redirect stdin, stdout, stderr to /dev/null
 *
 * @return true on success, false on failure
 *
 * @note This function should NOT be called when running under systemd
 *       (systemd expects Type=simple with foreground processes)
 * @note Windows is not supported - this is a no-op on Windows
 */
bool Daemonize();

}  // namespace mygramdb::utils
