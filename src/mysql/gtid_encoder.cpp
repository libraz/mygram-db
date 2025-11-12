#include "gtid_encoder.h"

#include <cctype>
#include <cstring>
#include <sstream>
#include <stdexcept>

// NOLINTBEGIN(cppcoreguidelines-avoid-*,cppcoreguidelines-pro-*,readability-magic-numbers) - GTID
// binary encoding

namespace mygramdb::mysql {

std::vector<uint8_t> GtidEncoder::Encode(const std::string& gtid_set) {
  if (gtid_set.empty()) {
    // Empty GTID set: just return 8 bytes of zeros
    std::vector<uint8_t> result(8, 0);
    return result;
  }

  // Parse GTID set string
  std::vector<Sid> sids;
  std::istringstream input_stream(gtid_set);
  std::string sid_part;

  // Split by comma for multiple SIDs (e.g., "uuid1:1-3,uuid2:5-7")
  // Note: MySQL GTID sets can have multiple UUIDs separated by commas
  while (std::getline(input_stream, sid_part, ',')) {
    // Trim whitespace
    size_t start = sid_part.find_first_not_of(" \t");
    size_t end = sid_part.find_last_not_of(" \t");
    if (start == std::string::npos) {
      continue;
    }
    sid_part = sid_part.substr(start, end - start + 1);

    // Find the colon that separates UUID from intervals
    size_t colon_pos = sid_part.find(':');
    if (colon_pos == std::string::npos) {
      throw std::invalid_argument("Invalid GTID format: missing colon");
    }

    Sid sid;
    std::string uuid_str = sid_part.substr(0, colon_pos);
    std::string intervals_str = sid_part.substr(colon_pos + 1);

    // Parse UUID
    ParseUuid(uuid_str, sid.uuid.data());

    // Parse intervals (e.g., "1-3:5-7:9")
    std::istringstream interval_ss(intervals_str);
    std::string interval_str;
    while (std::getline(interval_ss, interval_str, ':')) {
      sid.intervals.push_back(ParseInterval(interval_str));
    }

    sids.push_back(sid);
  }

  // Calculate total size needed
  size_t total_size = 8;  // n_sids
  for (const auto& sid : sids) {
    total_size += 16;                         // UUID
    total_size += 8;                          // n_intervals
    total_size += 16 * sid.intervals.size();  // intervals (start+end each 8 bytes)
  }

  // Encode to binary
  std::vector<uint8_t> result;
  result.reserve(total_size);

  // Store number of SIDs
  StoreInt64(result, sids.size());

  // Store each SID
  for (const auto& sid : sids) {
    // Store UUID (16 bytes)
    result.insert(result.end(), sid.uuid.begin(), sid.uuid.end());

    // Store number of intervals
    StoreInt64(result, sid.intervals.size());

    // Store each interval
    for (const auto& interval : sid.intervals) {
      StoreInt64(result, interval.start);
      StoreInt64(result, interval.end);
    }
  }

  return result;
}

void GtidEncoder::ParseUuid(const std::string& uuid_str, uint8_t* uuid_bytes) {
  // Expected format: "61d5b289-bccc-11f0-b921-cabbb4ee51f6"
  // Remove dashes and parse hex digits
  if (uuid_str.length() != 36) {
    throw std::invalid_argument("Invalid UUID length: " + uuid_str);
  }

  std::string hex_str;
  for (char character : uuid_str) {
    if (character != '-') {
      hex_str += character;
    }
  }

  if (hex_str.length() != 32) {
    throw std::invalid_argument("Invalid UUID format: " + uuid_str);
  }

  // Convert hex string to bytes
  for (size_t i = 0; i < 16; ++i) {
    std::string byte_str = hex_str.substr(i * 2, 2);
    char* end = nullptr;
    unsigned long byte_val = std::strtoul(byte_str.c_str(), &end, 16);
    if (end != byte_str.c_str() + 2) {
      throw std::invalid_argument("Invalid UUID hex digits: " + uuid_str);
    }
    uuid_bytes[i] = static_cast<uint8_t>(byte_val);
  }
}

GtidEncoder::Interval GtidEncoder::ParseInterval(const std::string& interval_str) {
  // Trim whitespace
  size_t start_pos = interval_str.find_first_not_of(" \t");
  size_t end_pos = interval_str.find_last_not_of(" \t");
  if (start_pos == std::string::npos) {
    throw std::invalid_argument("Invalid interval: empty string");
  }
  std::string trimmed = interval_str.substr(start_pos, end_pos - start_pos + 1);

  Interval interval{};
  size_t dash_pos = trimmed.find('-');

  if (dash_pos == std::string::npos) {
    // Single transaction number (e.g., "5")
    interval.start = std::stoll(trimmed);
    interval.end = interval.start + 1;  // exclusive end
  } else {
    // Range (e.g., "1-3" means transactions 1,2,3)
    std::string start_str = trimmed.substr(0, dash_pos);
    std::string end_str = trimmed.substr(dash_pos + 1);
    interval.start = std::stoll(start_str);
    int64_t end_inclusive = std::stoll(end_str);
    interval.end = end_inclusive + 1;  // convert to exclusive
  }

  if (interval.start <= 0 || interval.end <= interval.start) {
    throw std::invalid_argument("Invalid interval range: " + interval_str);
  }

  return interval;
}

void GtidEncoder::StoreInt64(std::vector<uint8_t>& buffer, uint64_t value) {
  // Store in little-endian format (MySQL protocol uses little-endian)
  buffer.push_back(static_cast<uint8_t>(value & 0xFF));
  buffer.push_back(static_cast<uint8_t>((value >> 8) & 0xFF));
  buffer.push_back(static_cast<uint8_t>((value >> 16) & 0xFF));
  buffer.push_back(static_cast<uint8_t>((value >> 24) & 0xFF));
  buffer.push_back(static_cast<uint8_t>((value >> 32) & 0xFF));
  buffer.push_back(static_cast<uint8_t>((value >> 40) & 0xFF));
  buffer.push_back(static_cast<uint8_t>((value >> 48) & 0xFF));
  buffer.push_back(static_cast<uint8_t>((value >> 56) & 0xFF));
}

}  // namespace mygramdb::mysql

// NOLINTEND(cppcoreguidelines-avoid-*,cppcoreguidelines-pro-*,readability-magic-numbers)
