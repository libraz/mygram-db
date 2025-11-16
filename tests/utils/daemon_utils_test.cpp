/**
 * @file daemon_utils_test.cpp
 * @brief Unit tests for daemon utilities
 */

#include "utils/daemon_utils.h"

#include <gtest/gtest.h>

#ifndef _WIN32
#include <fcntl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <csignal>
#include <cstdio>
#include <filesystem>
#include <fstream>
#endif

using namespace mygramdb::utils;

#ifndef _WIN32

/**
 * @brief Test that Daemonize() successfully creates a background process
 */
TEST(DaemonUtilsTest, DaemonizeSuccess) {
  // Create a test file to verify daemon execution
  const char* temp_file = std::tmpnam(nullptr);
  std::string test_file = std::string(temp_file) + ".daemon_test";

  // Remove if exists
  std::filesystem::remove(test_file);

  pid_t pid = fork();
  ASSERT_NE(pid, -1) << "Fork failed";

  if (pid == 0) {
    // Child process: try to daemonize
    bool result = Daemonize();

    if (result) {
      // Write a marker file to prove daemon started
      std::ofstream ofs(test_file);
      ofs << "daemon_running" << std::endl;
      ofs.close();

      // Sleep briefly to ensure file is written
      sleep(1);
    }

    // Exit child
    std::exit(result ? 0 : 1);
  } else {
    // Parent process: wait for child
    int status;
    waitpid(pid, &status, 0);

    // Give daemon time to write file
    sleep(2);

    // Check if marker file was created
    EXPECT_TRUE(std::filesystem::exists(test_file)) << "Daemon did not create marker file";

    if (std::filesystem::exists(test_file)) {
      std::ifstream ifs(test_file);
      std::string content;
      std::getline(ifs, content);
      EXPECT_EQ(content, "daemon_running");
      ifs.close();

      // Cleanup
      std::filesystem::remove(test_file);
    }
  }
}

/**
 * @brief Test that daemon process has no controlling terminal
 */
TEST(DaemonUtilsTest, NoControllingTerminal) {
  const char* temp_file = std::tmpnam(nullptr);
  std::string test_file = std::string(temp_file) + ".ctty_test";

  std::filesystem::remove(test_file);

  pid_t pid = fork();
  ASSERT_NE(pid, -1);

  if (pid == 0) {
    bool result = Daemonize();

    if (result) {
      // Check if we have a controlling terminal
      // After daemonization, this should fail
      int tty_fd = open("/dev/tty", O_RDWR);
      bool has_ctty = (tty_fd >= 0);

      if (tty_fd >= 0) {
        close(tty_fd);
      }

      // Write result to file
      std::ofstream ofs(test_file);
      ofs << (has_ctty ? "has_ctty" : "no_ctty") << std::endl;
      ofs.close();

      sleep(1);
    }

    std::exit(0);
  } else {
    int status;
    waitpid(pid, &status, 0);
    sleep(2);

    if (std::filesystem::exists(test_file)) {
      std::ifstream ifs(test_file);
      std::string content;
      std::getline(ifs, content);

      // After daemonization, should NOT have controlling terminal
      EXPECT_EQ(content, "no_ctty") << "Daemon still has controlling terminal";

      ifs.close();
      std::filesystem::remove(test_file);
    }
  }
}

/**
 * @brief Test that daemon process is in a new session
 */
TEST(DaemonUtilsTest, NewSessionCreated) {
  const char* temp_file = std::tmpnam(nullptr);
  std::string test_file = std::string(temp_file) + ".session_test";

  std::filesystem::remove(test_file);

  pid_t original_sid = getsid(0);

  pid_t pid = fork();
  ASSERT_NE(pid, -1);

  if (pid == 0) {
    pid_t child_sid_before = getsid(0);

    bool result = Daemonize();

    if (result) {
      pid_t daemon_sid = getsid(0);

      // Write session IDs to file
      std::ofstream ofs(test_file);
      ofs << child_sid_before << " " << daemon_sid << std::endl;
      ofs.close();

      sleep(1);
    }

    std::exit(0);
  } else {
    int status;
    waitpid(pid, &status, 0);
    sleep(2);

    if (std::filesystem::exists(test_file)) {
      std::ifstream ifs(test_file);
      pid_t child_sid, daemon_sid;
      ifs >> child_sid >> daemon_sid;
      ifs.close();

      // Daemon should be in a different session than original
      EXPECT_NE(daemon_sid, original_sid) << "Daemon not in new session";

      std::filesystem::remove(test_file);
    }
  }
}

/**
 * @brief Test daemon working directory is root
 */
TEST(DaemonUtilsTest, WorkingDirectoryIsRoot) {
  const char* temp_file = std::tmpnam(nullptr);
  std::string test_file = std::string(temp_file) + ".cwd_test";

  std::filesystem::remove(test_file);

  pid_t pid = fork();
  ASSERT_NE(pid, -1);

  if (pid == 0) {
    bool result = Daemonize();

    if (result) {
      char cwd[PATH_MAX];
      if (getcwd(cwd, sizeof(cwd)) != nullptr) {
        std::ofstream ofs(test_file);
        ofs << cwd << std::endl;
        ofs.close();

        sleep(1);
      }
    }

    std::exit(0);
  } else {
    int status;
    waitpid(pid, &status, 0);
    sleep(2);

    if (std::filesystem::exists(test_file)) {
      std::ifstream ifs(test_file);
      std::string cwd;
      std::getline(ifs, cwd);
      ifs.close();

      // Working directory should be root after daemonization
      EXPECT_EQ(cwd, "/") << "Daemon working directory is not /";

      std::filesystem::remove(test_file);
    }
  }
}

#else

/**
 * @brief Test that Daemonize returns false on Windows
 */
TEST(DaemonUtilsTest, WindowsNotSupported) {
  bool result = Daemonize();
  EXPECT_FALSE(result) << "Daemonize should return false on Windows";
}

#endif
