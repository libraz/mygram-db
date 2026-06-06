/**
 * @file protocol_detection.h
 * @brief Protocol response completion detection for MygramDB client
 *
 * This header provides the IsResponseComplete function used by the client
 * library to determine when a full protocol response has been received from
 * the server. It is separated into its own header so it can be unit tested.
 */

#pragma once

#include <string>
#include <string_view>

#include "server/protocol_constants.h"

namespace mygramdb::client::detail {

namespace proto = mygramdb::server::protocol;

/**
 * @brief Check whether a string ends with a given suffix
 *
 * @param str   The string to inspect
 * @param suffix The suffix to test for
 * @return true if @p str has @p suffix at its end
 */
inline bool EndsWith(std::string_view str, std::string_view suffix) {
  if (str.size() < suffix.size()) {
    return false;
  }
  return str.compare(str.size() - suffix.size(), suffix.size(), suffix) == 0;
}

struct ResponseCompletionState {
  size_t first_crlf = std::string_view::npos;
  size_t scan_offset = 0;
};

/**
 * @brief Check if a response buffer contains a complete protocol response
 *
 * The MygramDB protocol has two response formats:
 *
 * 1. Single-line responses: "OK RESULTS ...\r\n", "ERROR ...\r\n", etc.
 *    These are complete when the response ends with \r\n and the first
 *    \r\n is at the very end (no internal line breaks).
 *
 * 2. Multi-line responses: INFO, REPLICATION STATUS, CONFIG, FACET,
 *    CACHE_STATS, DUMP_INFO, DUMP_STATUS, and responses with DEBUG blocks.
 *    These contain internal \r\n delimiters and use specific end markers:
 *    - INFO, REPLICATION STATUS, CACHE_STATS, DUMP_INFO, DUMP_STATUS
 *      end with "END\r\n"
 *    - CONFIG (+OK prefix), FACET, and DEBUG blocks end with "\r\n\r\n"
 *
 * @param response The accumulated response data so far
 * @return true if the response is complete and we can stop reading
 */
inline bool IsResponseComplete(std::string_view response, ResponseCompletionState& state) {
  // Minimum valid response: "X\r\n" (3 bytes)
  constexpr size_t kMinResponseSize = 3;
  if (response.size() < kMinResponseSize) {
    return false;
  }

  // Response must end with \r\n
  if (response[response.size() - 2] != '\r' || response[response.size() - 1] != '\n') {
    return false;
  }

  // Find the first \r\n to identify the response type from the first line.
  // The stateful overload remembers the search position so callers that append
  // chunks do not rescan the full accumulated response after every recv().
  if (state.first_crlf == std::string_view::npos) {
    state.first_crlf = response.find("\r\n", state.scan_offset);
    if (state.first_crlf == std::string_view::npos) {
      state.scan_offset = response.size() > 0 ? response.size() - 1 : 0;
      return false;
    }
  }
  const size_t first_crlf = state.first_crlf;
  if (first_crlf == std::string_view::npos) {
    return false;
  }

  // Check if this is a single-line response (first \r\n is at the end)
  bool is_single_line = (first_crlf == response.size() - 2);

  constexpr std::string_view kEndMarker = "END\r\n";
  constexpr std::string_view kDoubleCrlf = "\r\n\r\n";

  // INFO: first line is exactly "OK INFO"
  if (first_crlf == proto::kOkInfoPrefix.size() &&
      response.compare(0, proto::kOkInfoPrefix.size(), proto::kOkInfoPrefix) == 0) {
    return EndsWith(response, kEndMarker);
  }

  // REPLICATION STATUS: first line is exactly "OK REPLICATION"
  if (first_crlf == proto::kOkReplicationPrefix.size() &&
      response.compare(0, proto::kOkReplicationPrefix.size(), proto::kOkReplicationPrefix) == 0) {
    return EndsWith(response, kEndMarker);
  }

  // CACHE_STATS: first line is exactly "OK CACHE_STATS"
  if (first_crlf == proto::kOkCacheStatsPrefix.size() &&
      response.compare(0, proto::kOkCacheStatsPrefix.size(), proto::kOkCacheStatsPrefix) == 0) {
    return EndsWith(response, kEndMarker);
  }

  // DUMP_INFO: first line starts with "OK DUMP_INFO" (followed by " <filepath>")
  if (first_crlf >= proto::kOkDumpInfoPrefix.size() &&
      response.compare(0, proto::kOkDumpInfoPrefix.size(), proto::kOkDumpInfoPrefix) == 0) {
    return EndsWith(response, kEndMarker);
  }

  // DUMP_STATUS: first line is exactly "OK DUMP_STATUS"
  if (first_crlf == proto::kOkDumpStatusPrefix.size() &&
      response.compare(0, proto::kOkDumpStatusPrefix.size(), proto::kOkDumpStatusPrefix) == 0) {
    return EndsWith(response, kEndMarker);
  }

  // SYNC_STATUS: first line is exactly "OK SYNC_STATUS"
  constexpr std::string_view kOkSyncStatusPrefix = "OK SYNC_STATUS";
  if (first_crlf == kOkSyncStatusPrefix.size() &&
      response.compare(0, kOkSyncStatusPrefix.size(), kOkSyncStatusPrefix) == 0) {
    return EndsWith(response, kEndMarker);
  }

  // CONFIG: first line starts with "+OK"
  if (first_crlf >= proto::kPlusOkPrefix.size() &&
      response.compare(0, proto::kPlusOkPrefix.size(), proto::kPlusOkPrefix) == 0) {
    return EndsWith(response, kDoubleCrlf);
  }

  // FACET: first line starts with "OK FACET"
  if (first_crlf >= proto::kOkFacetPrefix.size() &&
      response.compare(0, proto::kOkFacetPrefix.size(), proto::kOkFacetPrefix) == 0) {
    return EndsWith(response, kDoubleCrlf);
  }

  // For other response types (SEARCH, COUNT, GET, SAVE, LOAD, ERROR, etc.):
  // If the first \r\n is at the end, it's a single-line response and is complete.
  // If there's content after the first \r\n (e.g., DEBUG block or highlight lines),
  // the response is multi-line and ends with \r\n\r\n.
  if (is_single_line) {
    return true;
  }

  // Multi-line response with content after first line (e.g., SEARCH with DEBUG)
  return EndsWith(response, kDoubleCrlf);
}

inline bool IsResponseComplete(std::string_view response) {
  ResponseCompletionState state;
  return IsResponseComplete(response, state);
}

}  // namespace mygramdb::client::detail
