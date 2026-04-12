/**
 * @file protocol_constants.h
 * @brief Shared protocol response prefix constants for MygramDB TCP protocol
 *
 * These constants define the lengths of common response prefixes used by both
 * the server and client implementations.
 */

#pragma once

#include <cstddef>

namespace mygramdb::server {
namespace protocol {

constexpr size_t kErrorPrefixLen = 6;             // "ERROR "
constexpr size_t kOkSavedPrefixLen = 9;           // "OK SAVED "
constexpr size_t kOkLoadedPrefixLen = 10;         // "OK LOADED "
constexpr size_t kOkInfoPrefixLen = 8;            // "OK INFO\r\n" prefix
constexpr size_t kOkReplicationPrefixLen = 15;    // "OK REPLICATION\r\n" prefix
constexpr size_t kDefaultRecvBufferSize = 65536;  // 64KB receive buffer

}  // namespace protocol
}  // namespace mygramdb::server
