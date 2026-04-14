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

namespace mygramdb::client::detail {

/**
 * @brief Check if a response buffer contains a complete protocol response
 *
 * The MygramDB protocol has two response formats:
 *
 * 1. Single-line responses: "OK RESULTS ...\r\n", "ERROR ...\r\n", etc.
 *    These are complete when the response ends with \r\n and the first
 *    \r\n is at the very end (no internal line breaks).
 *
 * 2. Multi-line responses: INFO, REPLICATION STATUS, CONFIG, FACET, and
 *    responses with DEBUG blocks. These contain internal \r\n delimiters
 *    and use specific end markers:
 *    - INFO and REPLICATION STATUS end with "END\r\n"
 *    - CONFIG (+OK prefix), FACET, and DEBUG blocks end with "\r\n\r\n"
 *
 * @param response The accumulated response data so far
 * @return true if the response is complete and we can stop reading
 */
inline bool IsResponseComplete(const std::string& response) {
  // Minimum valid response: "X\r\n" (3 bytes)
  constexpr size_t kMinResponseSize = 3;
  if (response.size() < kMinResponseSize) {
    return false;
  }

  // Response must end with \r\n
  if (response[response.size() - 2] != '\r' || response[response.size() - 1] != '\n') {
    return false;
  }

  // Find the first \r\n to identify the response type from the first line
  size_t first_crlf = response.find("\r\n");
  if (first_crlf == std::string::npos) {
    return false;
  }

  // Check if this is a single-line response (first \r\n is at the end)
  bool is_single_line = (first_crlf == response.size() - 2);

  // Known prefix lengths for multi-line response detection
  constexpr size_t kOkInfoLen = 7;          // "OK INFO"
  constexpr size_t kOkReplicationLen = 14;  // "OK REPLICATION"
  constexpr size_t kEndMarkerLen = 5;       // "END\r\n"
  constexpr size_t kDoubleCrlfLen = 4;      // "\r\n\r\n"
  constexpr size_t kPlusOkLen = 3;          // "+OK"
  constexpr size_t kOkFacetLen = 8;         // "OK FACET"

  // INFO: first line is exactly "OK INFO"
  if (first_crlf == kOkInfoLen && response.compare(0, kOkInfoLen, "OK INFO") == 0) {
    return response.size() >= kEndMarkerLen &&
           response.compare(response.size() - kEndMarkerLen, kEndMarkerLen, "END\r\n") == 0;
  }

  // REPLICATION STATUS: first line is exactly "OK REPLICATION"
  if (first_crlf == kOkReplicationLen && response.compare(0, kOkReplicationLen, "OK REPLICATION") == 0) {
    return response.size() >= kEndMarkerLen &&
           response.compare(response.size() - kEndMarkerLen, kEndMarkerLen, "END\r\n") == 0;
  }

  // CONFIG: first line starts with "+OK"
  if (first_crlf >= kPlusOkLen && response.compare(0, kPlusOkLen, "+OK") == 0) {
    return response.size() >= kDoubleCrlfLen &&
           response.compare(response.size() - kDoubleCrlfLen, kDoubleCrlfLen, "\r\n\r\n") == 0;
  }

  // FACET: first line starts with "OK FACET"
  if (first_crlf >= kOkFacetLen && response.compare(0, kOkFacetLen, "OK FACET") == 0) {
    return response.size() >= kDoubleCrlfLen &&
           response.compare(response.size() - kDoubleCrlfLen, kDoubleCrlfLen, "\r\n\r\n") == 0;
  }

  // For other response types (SEARCH, COUNT, GET, SAVE, LOAD, ERROR, etc.):
  // If the first \r\n is at the end, it's a single-line response and is complete.
  // If there's content after the first \r\n (e.g., DEBUG block or highlight lines),
  // the response is multi-line and ends with \r\n\r\n.
  if (is_single_line) {
    return true;
  }

  // Multi-line response with content after first line (e.g., SEARCH with DEBUG)
  return response.size() >= kDoubleCrlfLen &&
         response.compare(response.size() - kDoubleCrlfLen, kDoubleCrlfLen, "\r\n\r\n") == 0;
}

}  // namespace mygramdb::client::detail
