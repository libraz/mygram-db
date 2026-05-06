/**
 * @file socket_utils.h
 * @brief Small helpers around setsockopt() that centralize structured logging.
 *
 * The acceptor sets a dozen-plus socket options on the listening socket and on
 * each accepted client fd. Each call previously had its own inline boilerplate
 * (setsockopt + errno snapshot + StructuredLog warning). M-7 collapses that
 * duplication into a single helper here so that:
 *   - the warning event name (`setsockopt_failed`) and field shape stay
 *     consistent across call sites, and
 *   - adding a new tunable (e.g. SO_LINGER) is a one-line change.
 */

#pragma once

#include <sys/socket.h>

#include <string_view>

namespace mygramdb::server::socket_utils {

/**
 * @brief Tries to set a socket option, logging a structured warning on failure.
 *
 * This helper centralizes the per-option boilerplate (setsockopt() call,
 * errno preservation, structured warning emission) that previously lived
 * inline at every call site in connection_acceptor.cpp. For optional
 * tunings (TCP_KEEPALIVE_*, SO_RCVBUF, etc.) failure is non-fatal — the
 * socket continues to operate with the kernel's default value.
 *
 * @param fd      Open socket file descriptor.
 * @param level   Socket option level (e.g. SOL_SOCKET, IPPROTO_TCP).
 * @param optname Option name (e.g. SO_KEEPALIVE).
 * @param val     Pointer to the option value.
 * @param len     Length of the option value.
 * @param label   Human-readable identifier for the option, included in the
 *                warning event (e.g. "SO_KEEPALIVE", "TCP_NODELAY").
 * @return true on success, false on failure.
 */
bool TrySetSockOpt(int fd, int level, int optname, const void* val, socklen_t len, std::string_view label);

/**
 * @brief Convenience overload for int-valued options.
 *
 * The most common shape: an int flag like 0/1 or a buffer-size value.
 */
bool TrySetSockOpt(int fd, int level, int optname, int val, std::string_view label);

}  // namespace mygramdb::server::socket_utils
