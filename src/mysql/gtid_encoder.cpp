#include "gtid_encoder.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <limits>
#include <map>
#include <optional>
#include <sstream>

#include "utils/numeric_parse.h"
#include "utils/string_utils.h"

// NOLINTBEGIN(cppcoreguidelines-avoid-*,cppcoreguidelines-pro-*,readability-magic-numbers) - GTID
// binary encoding

namespace mygramdb::mysql {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

namespace {

struct MysqlGtidInterval {
  uint64_t start = 0;
  uint64_t end = 0;  // inclusive
};

using MysqlGtidSet = std::map<std::string, std::vector<MysqlGtidInterval>>;

std::optional<MysqlGtidInterval> ParseMysqlGtidInterval(std::string_view interval) {
  auto dash_pos = interval.find('-');
  if (dash_pos == std::string_view::npos) {
    auto value = mygram::utils::ParseNumeric<uint64_t>(interval);
    if (!value.has_value()) {
      return std::nullopt;
    }
    return MysqlGtidInterval{*value, *value};
  }

  auto start = mygram::utils::ParseNumeric<uint64_t>(interval.substr(0, dash_pos));
  auto end = mygram::utils::ParseNumeric<uint64_t>(interval.substr(dash_pos + 1));
  if (!start.has_value() || !end.has_value() || *start > *end) {
    return std::nullopt;
  }
  return MysqlGtidInterval{*start, *end};
}

std::optional<std::pair<std::string, std::vector<MysqlGtidInterval>>> ParseMysqlGtidSetEntry(std::string_view entry) {
  auto colon_pos = entry.find(':');
  if (colon_pos == std::string_view::npos) {
    return std::nullopt;
  }

  std::string uuid(entry.substr(0, colon_pos));
  std::string_view intervals_text = entry.substr(colon_pos + 1);
  if (uuid.empty() || intervals_text.empty()) {
    return std::nullopt;
  }

  std::vector<std::string_view> parts;
  while (!intervals_text.empty()) {
    auto next_colon = intervals_text.find(':');
    parts.push_back(intervals_text.substr(0, next_colon));
    if (next_colon == std::string_view::npos) {
      break;
    }
    intervals_text.remove_prefix(next_colon + 1);
  }
  if (parts.empty()) {
    return std::nullopt;
  }

  std::string key = std::move(uuid);
  size_t interval_start = 0;
  if (!ParseMysqlGtidInterval(parts.front()).has_value()) {
    if (parts.size() < 2 || parts.front().empty()) {
      return std::nullopt;
    }
    key += ':';
    key += parts.front();
    interval_start = 1;
  }

  std::vector<MysqlGtidInterval> intervals;
  for (size_t i = interval_start; i < parts.size(); ++i) {
    auto interval = ParseMysqlGtidInterval(parts[i]);
    if (!interval.has_value()) {
      return std::nullopt;
    }
    intervals.push_back(*interval);
  }

  return std::make_pair(std::move(key), std::move(intervals));
}

void NormalizeMysqlGtidIntervals(std::vector<MysqlGtidInterval>& intervals) {
  std::sort(intervals.begin(), intervals.end(), [](const MysqlGtidInterval& left, const MysqlGtidInterval& right) {
    return left.start < right.start || (left.start == right.start && left.end < right.end);
  });

  std::vector<MysqlGtidInterval> merged;
  for (const auto& interval : intervals) {
    if (merged.empty() || merged.back().end == std::numeric_limits<uint64_t>::max() ||
        interval.start > merged.back().end + 1) {
      merged.push_back(interval);
      continue;
    }
    merged.back().end = std::max(merged.back().end, interval.end);
  }
  intervals = std::move(merged);
}

std::optional<MysqlGtidSet> ParseMysqlGtidSet(std::string_view gtid_set) {
  MysqlGtidSet parsed;
  while (!gtid_set.empty()) {
    auto comma_pos = gtid_set.find(',');
    std::string_view entry = gtid_set.substr(0, comma_pos);
    while (!entry.empty() && std::isspace(static_cast<unsigned char>(entry.front())) != 0) {
      entry.remove_prefix(1);
    }
    while (!entry.empty() && std::isspace(static_cast<unsigned char>(entry.back())) != 0) {
      entry.remove_suffix(1);
    }

    auto parsed_entry = ParseMysqlGtidSetEntry(entry);
    if (!parsed_entry.has_value()) {
      return std::nullopt;
    }
    auto& intervals = parsed[parsed_entry->first];
    intervals.insert(intervals.end(), parsed_entry->second.begin(), parsed_entry->second.end());

    if (comma_pos == std::string_view::npos) {
      break;
    }
    gtid_set.remove_prefix(comma_pos + 1);
  }

  for (auto& [uuid, intervals] : parsed) {
    (void)uuid;
    NormalizeMysqlGtidIntervals(intervals);
  }
  return parsed;
}

std::string MysqlGtidSetToString(const MysqlGtidSet& gtid_set) {
  std::ostringstream oss;
  bool first_uuid = true;
  for (const auto& [uuid, uuid_intervals] : gtid_set) {
    if (!first_uuid) {
      oss << ',';
    }
    first_uuid = false;
    oss << uuid << ':';
    bool first_interval = true;
    for (const auto& interval : uuid_intervals) {
      if (!first_interval) {
        oss << ':';
      }
      first_interval = false;
      if (interval.start == interval.end) {
        oss << interval.start;
      } else {
        oss << interval.start << '-' << interval.end;
      }
    }
  }
  return oss.str();
}

}  // namespace

mygram::utils::Expected<std::vector<uint8_t>, Error> GtidEncoder::Encode(const std::string& gtid_set) {
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
    size_t start = sid_part.find_first_not_of(" \t\n\r");
    size_t end = sid_part.find_last_not_of(" \t\n\r");
    if (start == std::string::npos) {
      continue;
    }
    sid_part = sid_part.substr(start, end - start + 1);

    // Find the colon that separates UUID from intervals
    size_t colon_pos = sid_part.find(':');
    if (colon_pos == std::string::npos) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid GTID format: missing colon"));
    }

    Sid sid;
    std::string uuid_str = sid_part.substr(0, colon_pos);
    std::string intervals_str = sid_part.substr(colon_pos + 1);

    // Parse UUID
    auto parse_result = ParseUuid(uuid_str, sid.uuid.data());
    if (!parse_result) {
      return MakeUnexpected(parse_result.error());
    }

    // Parse intervals (e.g., "1-3:5-7:9")
    std::istringstream interval_ss(intervals_str);
    std::string interval_str;
    while (std::getline(interval_ss, interval_str, ':')) {
      if (!interval_str.empty()) {
        auto interval_result = ParseInterval(interval_str);
        if (!interval_result) {
          return MakeUnexpected(interval_result.error());
        }
        sid.intervals.push_back(*interval_result);
      }
    }

    // Validate: SID must have at least one interval
    if (sid.intervals.empty()) {
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid GTID set: UUID without intervals: " + uuid_str));
    }

    // Sort intervals by start position
    std::sort(sid.intervals.begin(), sid.intervals.end(),
              [](const Interval& a, const Interval& b) { return a.start < b.start; });

    // Merge overlapping or adjacent intervals
    if (sid.intervals.size() > 1) {
      std::vector<Interval> merged;
      merged.push_back(sid.intervals[0]);
      for (size_t i = 1; i < sid.intervals.size(); ++i) {
        if (sid.intervals[i].start <= merged.back().end) {
          // Overlapping or adjacent: extend the end
          merged.back().end = std::max(merged.back().end, sid.intervals[i].end);
        } else {
          merged.push_back(sid.intervals[i]);
        }
      }
      sid.intervals = std::move(merged);
    }

    sids.push_back(sid);
  }

  // Merge SIDs with identical UUIDs (e.g., "uuid1:1-100, uuid1:200-300")
  // MySQL protocol expects unique SID entries
  if (sids.size() > 1) {
    std::map<std::array<uint8_t, 16>, std::vector<Interval>> merged_map;
    for (auto& sid : sids) {
      auto& intervals = merged_map[sid.uuid];
      intervals.insert(intervals.end(), sid.intervals.begin(), sid.intervals.end());
    }

    sids.clear();
    for (auto& [uuid, intervals] : merged_map) {
      Sid merged_sid;
      merged_sid.uuid = uuid;

      // Sort and merge intervals
      std::sort(intervals.begin(), intervals.end(),
                [](const Interval& a, const Interval& b) { return a.start < b.start; });
      std::vector<Interval> merged_intervals;
      merged_intervals.push_back(intervals[0]);
      for (size_t i = 1; i < intervals.size(); ++i) {
        if (intervals[i].start <= merged_intervals.back().end) {
          merged_intervals.back().end = std::max(merged_intervals.back().end, intervals[i].end);
        } else {
          merged_intervals.push_back(intervals[i]);
        }
      }
      merged_sid.intervals = std::move(merged_intervals);
      sids.push_back(std::move(merged_sid));
    }
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

mygram::utils::Expected<void, Error> GtidEncoder::ParseUuid(const std::string& uuid_str, uint8_t* uuid_bytes) {
  // Expected format: "61d5b289-bccc-11f0-b921-cabbb4ee51f6"
  // Remove dashes and parse hex digits
  if (uuid_str.length() != 36) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid UUID length: " + uuid_str));
  }

  std::string hex_str;
  for (char character : uuid_str) {
    if (character != '-') {
      hex_str += character;
    }
  }

  if (hex_str.length() != 32) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid UUID format: " + uuid_str));
  }

  // Convert hex string to bytes
  for (size_t i = 0; i < 16; ++i) {
    std::string_view byte_str(hex_str.data() + (i * 2), 2);
    auto byte_val = mygram::utils::ParseNumeric<uint32_t>(byte_str, 16);
    if (!byte_val.has_value() || *byte_val > std::numeric_limits<uint8_t>::max()) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid UUID hex digits: " + uuid_str));
    }
    uuid_bytes[i] = static_cast<uint8_t>(*byte_val);
  }

  return {};
}

mygram::utils::Expected<GtidEncoder::Interval, Error> GtidEncoder::ParseInterval(const std::string& interval_str) {
  // Trim whitespace
  size_t start_pos = interval_str.find_first_not_of(" \t\n\r");
  size_t end_pos = interval_str.find_last_not_of(" \t\n\r");
  if (start_pos == std::string::npos) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid interval: empty string"));
  }
  std::string trimmed = interval_str.substr(start_pos, end_pos - start_pos + 1);

  Interval interval{};
  size_t dash_pos = trimmed.find('-');

  if (dash_pos == std::string::npos) {
    // Single transaction number (e.g., "5")
    auto start = mygram::utils::ParseNumeric<uint64_t>(trimmed);
    if (!start.has_value()) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid interval number format: " + interval_str));
    }
    interval.start = *start;
    // Check for overflow before adding 1
    if (interval.start >= std::numeric_limits<uint64_t>::max()) {
      return MakeUnexpected(MakeError(ErrorCode::kOutOfRange, "Transaction ID overflow: cannot add 1 to UINT64_MAX"));
    }
    interval.end = interval.start + 1;  // exclusive end
  } else {
    // Range (e.g., "1-3" means transactions 1,2,3)
    std::string_view trimmed_view(trimmed);
    std::string_view start_str = mygram::utils::TrimAsciiWhitespaceView(trimmed_view.substr(0, dash_pos));
    std::string_view end_str = mygram::utils::TrimAsciiWhitespaceView(trimmed_view.substr(dash_pos + 1));
    auto start = mygram::utils::ParseNumeric<uint64_t>(start_str);
    auto end = mygram::utils::ParseNumeric<uint64_t>(end_str);
    if (!start.has_value() || !end.has_value()) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid interval number format: " + interval_str));
    }
    interval.start = *start;
    uint64_t end_inclusive = *end;
    // Check for overflow before adding 1
    if (end_inclusive >= std::numeric_limits<uint64_t>::max()) {
      return MakeUnexpected(MakeError(ErrorCode::kOutOfRange, "Transaction ID overflow: cannot add 1 to UINT64_MAX"));
    }
    interval.end = end_inclusive + 1;  // convert to exclusive
  }

  if (interval.start <= 0 || interval.end <= interval.start) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid interval range: " + interval_str));
  }

  return interval;
}

std::string GtidEncoder::ExtractUuid(const std::string& gtid_str) {
  size_t colon_pos = gtid_str.find(':');
  if (colon_pos == std::string::npos || colon_pos == 0) {
    return "";
  }
  return gtid_str.substr(0, colon_pos);
}

bool GtidEncoder::IsValidGtidSet(const std::string& gtid_set) {
  if (gtid_set.empty()) {
    return false;
  }

  std::istringstream input_stream(gtid_set);
  std::string entry;
  while (std::getline(input_stream, entry, ',')) {
    // Trim whitespace
    size_t start = entry.find_first_not_of(" \t\n\r");
    size_t end = entry.find_last_not_of(" \t\n\r");
    if (start == std::string::npos) {
      return false;
    }
    entry = entry.substr(start, end - start + 1);

    // Each entry must have at least one colon separating UUID from intervals
    if (entry.find(':') == std::string::npos) {
      return false;
    }
  }
  return true;
}

std::optional<std::string> GtidEncoder::MergeSingleGtidIntoSet(std::string_view current_gtid,
                                                               std::string_view next_gtid) {
  auto current_set = ParseMysqlGtidSet(current_gtid);
  auto next_entry = ParseMysqlGtidSetEntry(next_gtid);
  if (!current_set.has_value() || !next_entry.has_value() || next_entry->second.size() != 1) {
    return std::nullopt;
  }

  auto& intervals = (*current_set)[next_entry->first];
  intervals.push_back(next_entry->second.front());
  NormalizeMysqlGtidIntervals(intervals);
  return MysqlGtidSetToString(*current_set);
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
