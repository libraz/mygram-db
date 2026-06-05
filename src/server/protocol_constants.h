/**
 * @file protocol_constants.h
 * @brief Shared protocol response prefix constants for MygramDB TCP protocol
 *
 * These constants define byte offsets used with substr() to extract the data
 * payload from protocol response strings. For simple responses (ERROR, SAVED,
 * LOADED), the offset equals the full prefix length including the trailing
 * space. For multi-line responses (INFO, REPLICATION), the offset skips past
 * the status keyword and \r, leaving the \n delimiter for subsequent
 * line-ending processing.
 */

#pragma once

#include <cstddef>

namespace mygramdb::server {
namespace protocol {

constexpr size_t kErrorPrefixLen = 6;                   // Byte offset past "ERROR " (6 bytes)
constexpr size_t kOkSavedPrefixLen = 9;                 // Byte offset past "OK SAVED " (9 bytes)
constexpr size_t kOkLoadedPrefixLen = 10;               // Byte offset past "OK LOADED " (10 bytes)
constexpr size_t kDefaultClientRecvBufferSize = 65536;  // 64KB client receive buffer

}  // namespace protocol
}  // namespace mygramdb::server
