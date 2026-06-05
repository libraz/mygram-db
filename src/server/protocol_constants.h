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
#include <string_view>

namespace mygramdb::server {
namespace protocol {

inline constexpr std::string_view kOkPrefix = "OK ";
inline constexpr std::string_view kPlusOkPrefix = "+OK";
inline constexpr std::string_view kErrorPrefix = "ERROR ";
inline constexpr std::string_view kOkResultsPrefix = "OK RESULTS";
inline constexpr std::string_view kOkResultsWithSpacePrefix = "OK RESULTS ";
inline constexpr std::string_view kOkCountPrefix = "OK COUNT";
inline constexpr std::string_view kOkCountWithSpacePrefix = "OK COUNT ";
inline constexpr std::string_view kOkDocPrefix = "OK DOC";
inline constexpr std::string_view kOkDocWithSpacePrefix = "OK DOC ";
inline constexpr std::string_view kOkInfoPrefix = "OK INFO";
inline constexpr std::string_view kOkFacetPrefix = "OK FACET";
inline constexpr std::string_view kOkFacetWithSpacePrefix = "OK FACET ";
inline constexpr std::string_view kOkSavedPrefix = "OK SAVED ";
inline constexpr std::string_view kOkLoadedPrefix = "OK LOADED ";
inline constexpr std::string_view kOkReplicationPrefix = "OK REPLICATION";
inline constexpr std::string_view kOkReplicationHeader = "OK REPLICATION\r\n";
inline constexpr std::string_view kOkReplicationStopped = "OK REPLICATION_STOPPED";
inline constexpr std::string_view kOkReplicationStarted = "OK REPLICATION_STARTED";
inline constexpr std::string_view kOkDumpStartedPrefix = "OK DUMP_STARTED ";
inline constexpr std::string_view kOkDumpInfoPrefix = "OK DUMP_INFO";
inline constexpr std::string_view kOkDumpStatusPrefix = "OK DUMP_STATUS";
inline constexpr std::string_view kOkDumpVerifiedPrefix = "OK DUMP_VERIFIED";
inline constexpr std::string_view kOkCacheStatsPrefix = "OK CACHE_STATS";
inline constexpr std::string_view kOkCacheCleared = "OK CACHE_CLEARED";
inline constexpr std::string_view kOkCacheEnabled = "OK CACHE_ENABLED";
inline constexpr std::string_view kOkCacheDisabled = "OK CACHE_DISABLED";
inline constexpr std::string_view kOkDebugOn = "OK DEBUG_ON";
inline constexpr std::string_view kOkDebugOff = "OK DEBUG_OFF";
inline constexpr std::string_view kOkOptimizedPrefix = "OK OPTIMIZED";
inline constexpr std::string_view kOkSyncPrefix = "OK SYNC";

constexpr size_t kOkPrefixLen = kOkPrefix.size();
constexpr size_t kPlusOkPrefixLen = kPlusOkPrefix.size();
constexpr size_t kErrorPrefixLen = kErrorPrefix.size();
constexpr size_t kOkSavedPrefixLen = kOkSavedPrefix.size();
constexpr size_t kOkLoadedPrefixLen = kOkLoadedPrefix.size();
constexpr size_t kDefaultClientRecvBufferSize = 65536;  // 64KB client receive buffer

}  // namespace protocol
}  // namespace mygramdb::server
