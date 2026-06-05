/**
 * @file errno_utils.h
 * @brief Helpers for formatting captured errno values.
 */

#pragma once

#include <cstring>
#include <string>

#include "utils/namespace_compat.h"

namespace mygramdb::utils {

inline std::string FormatErrno(const char* syscall_label, int captured_errno) {
  std::string msg = syscall_label;
  msg += " failed: ";
  msg += std::strerror(captured_errno);
  msg += " (errno=";
  msg += std::to_string(captured_errno);
  msg += ")";
  return msg;
}

}  // namespace mygramdb::utils
