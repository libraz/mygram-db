/**
 * @file binlog_reader_internal.h
 * @brief Internal declarations for binlog_reader split translation units
 *
 * This header is not part of the public API. It provides shared constants
 * and helpers between binlog_reader.cpp, binlog_reader_threads.cpp, and
 * binlog_reader_utils.cpp.
 */

#pragma once

#ifdef USE_MYSQL

#include <cstdint>

#include "mysql/connection.h"

namespace mygramdb::mysql {

/// MySQL error code: binlog position has been purged
constexpr unsigned int kMySQLErrBinlogPurged = 1236;

/// MySQL error code: server connection lost (CR_SERVER_LOST)
constexpr unsigned int kMySQLErrServerLost = 2013;

/// MySQL error code: server has gone away (CR_SERVER_GONE_ERROR)
constexpr unsigned int kMySQLErrGoneAway = 2006;

namespace internal {

/// Create a sub-config from a source, optionally overriding the read timeout.
/// @param src Source connection config
/// @param read_timeout_override If non-zero, overrides the read timeout
inline Connection::Config MakeSubConfig(const Connection::Config& src, uint32_t read_timeout_override = 0) {
  Connection::Config dst;
  dst.host = src.host;
  dst.port = src.port;
  dst.user = src.user;
  dst.password = src.password;
  dst.database = src.database;
  dst.connect_timeout = src.connect_timeout;
  dst.read_timeout = (read_timeout_override > 0) ? read_timeout_override : src.read_timeout;
  dst.write_timeout = src.write_timeout;
  dst.ssl_enable = src.ssl_enable;
  dst.ssl_ca = src.ssl_ca;
  dst.ssl_cert = src.ssl_cert;
  dst.ssl_key = src.ssl_key;
  dst.ssl_verify_server_cert = src.ssl_verify_server_cert;
  return dst;
}

}  // namespace internal

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
