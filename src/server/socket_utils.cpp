#include "server/socket_utils.h"

#include <cerrno>
#include <cstring>

#include "server/log_field_names.h"
#include "utils/structured_log.h"

namespace mygramdb::server::socket_utils {

bool TrySetSockOpt(int fd, int level, int optname, const void* val, socklen_t len, std::string_view label) {
  if (::setsockopt(fd, level, optname, val, len) >= 0) {
    return true;
  }
  // Snapshot errno *before* StructuredLog construction; any allocation in the
  // logger could otherwise scribble over errno.
  const int saved_errno = errno;
  mygram::utils::StructuredLog()
      .Event("setsockopt_failed")
      .Field("option", label)
      .Field(log_fields::kFieldFd, static_cast<int64_t>(fd))
      .Field(log_fields::kFieldError, std::strerror(saved_errno))
      .Warn();
  return false;
}

bool TrySetSockOpt(int fd, int level, int optname, int val, std::string_view label) {
  return TrySetSockOpt(fd, level, optname, &val, sizeof(val), label);
}

}  // namespace mygramdb::server::socket_utils
