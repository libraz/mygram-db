/**
 * @file gtid_grammar.cpp
 * @brief The one grammar for MySQL and MariaDB GTID position text
 */

#include "utils/gtid_grammar.h"

#include <algorithm>
#include <limits>
#include <sstream>

#include "utils/numeric_parse.h"
#include "utils/string_utils.h"

namespace mygramdb::utils {

namespace {

/// Positions of the dashes in an 8-4-4-4-12 UUID.
constexpr std::array<size_t, 4> kUuidDashPositions = {8, 13, 18, 23};

/// A MySQL 8.4 GTID tag: a letter or underscore, then up to 31 alphanumerics or underscores.
bool IsGtidTag(std::string_view text) {
  constexpr size_t kMaxTagLength = 32;
  if (text.empty() || text.size() > kMaxTagLength) {
    return false;
  }
  const auto is_tag_byte = [](char chr, bool first) {
    const bool letter = (chr >= 'a' && chr <= 'z') || (chr >= 'A' && chr <= 'Z');
    return letter || chr == '_' || (!first && chr >= '0' && chr <= '9');
  };
  for (size_t i = 0; i < text.size(); ++i) {
    if (!is_tag_byte(text[i], i == 0)) {
      return false;
    }
  }
  return true;
}

std::string FormatUuid(const std::array<uint8_t, 16>& bytes) {
  constexpr std::array<char, 16> kHex = {'0', '1', '2', '3', '4', '5', '6', '7',
                                         '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
  std::string text;
  text.reserve(kGtidUuidTextLength);
  for (uint8_t byte : bytes) {
    if (std::find(kUuidDashPositions.begin(), kUuidDashPositions.end(), text.size()) != kUuidDashPositions.end()) {
      text.push_back('-');
    }
    text.push_back(kHex[byte >> 4U]);
    text.push_back(kHex[byte & 0x0FU]);
  }
  return text;
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

/// Split on a separator, keeping empty pieces.
std::vector<std::string_view> SplitView(std::string_view text, char separator) {
  std::vector<std::string_view> pieces;
  size_t start = 0;
  while (true) {
    const size_t next = text.find(separator, start);
    pieces.push_back(text.substr(start, next == std::string_view::npos ? std::string_view::npos : next - start));
    if (next == std::string_view::npos) {
      return pieces;
    }
    start = next + 1;
  }
}

Expected<MysqlGtidInterval, Error> ParseMysqlGtidInterval(std::string_view interval_text) {
  const std::string_view trimmed = TrimAsciiWhitespaceView(interval_text);
  const size_t dash_pos = trimmed.find('-');
  const std::string_view start_text =
      TrimAsciiWhitespaceView(dash_pos == std::string_view::npos ? trimmed : trimmed.substr(0, dash_pos));
  const std::string_view end_text =
      TrimAsciiWhitespaceView(dash_pos == std::string_view::npos ? trimmed : trimmed.substr(dash_pos + 1));

  auto start = ParseNumeric<uint64_t>(start_text);
  auto end = ParseNumeric<uint64_t>(end_text);
  if (!start.has_value() || !end.has_value()) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid interval number format: " + std::string(interval_text)));
  }
  // The binlog dump protocol stores an exclusive end, so the last transaction
  // number has to leave room for one more.
  if (*end == std::numeric_limits<uint64_t>::max()) {
    return MakeUnexpected(MakeError(ErrorCode::kOutOfRange, "Transaction ID overflow: cannot add 1 to UINT64_MAX"));
  }
  if (*start == 0 || *end < *start) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid interval range: " + std::string(interval_text)));
  }
  return MysqlGtidInterval{*start, *end};
}

/// Whether a MariaDB GTID segment is a non-empty run of ASCII digits.
bool AllDigits(std::string_view text) {
  return !text.empty() && std::all_of(text.begin(), text.end(), [](char chr) { return chr >= '0' && chr <= '9'; });
}

}  // namespace

Expected<std::array<uint8_t, 16>, Error> ParseUuid(std::string_view uuid_text) {
  if (uuid_text.length() != kGtidUuidTextLength) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid UUID length: " + std::string(uuid_text)));
  }

  std::string hex_text;
  for (size_t i = 0; i < uuid_text.size(); ++i) {
    const bool dash_expected =
        std::find(kUuidDashPositions.begin(), kUuidDashPositions.end(), i) != kUuidDashPositions.end();
    if ((uuid_text[i] == '-') != dash_expected) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid UUID format: " + std::string(uuid_text)));
    }
    if (!dash_expected) {
      hex_text += uuid_text[i];
    }
  }

  std::array<uint8_t, 16> bytes{};
  for (size_t i = 0; i < bytes.size(); ++i) {
    auto byte_value = ParseNumeric<uint32_t>(std::string_view(hex_text).substr(i * 2, 2), 16);
    if (!byte_value.has_value() || *byte_value > std::numeric_limits<uint8_t>::max()) {
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid UUID hex digits: " + std::string(uuid_text)));
    }
    bytes[i] = static_cast<uint8_t>(*byte_value);
  }
  return bytes;
}

Expected<MysqlGtidSet, Error> ParseMysqlGtidSet(std::string_view gtid_set) {
  MysqlGtidSet parsed;
  for (std::string_view entry : SplitView(gtid_set, ',')) {
    entry = TrimAsciiWhitespaceView(entry);
    if (entry.empty()) {
      continue;
    }

    const std::vector<std::string_view> parts = SplitView(entry, ':');
    if (parts.size() < 2) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid GTID format: missing colon"));
    }

    auto uuid = ParseUuid(TrimAsciiWhitespaceView(parts[0]));
    if (!uuid) {
      return MakeUnexpected(uuid.error());
    }
    const std::string uuid_key = FormatUuid(*uuid);

    // MySQL 8.4 lets a tag switch the source of the intervals that follow it,
    // so "uuid:1-3:tag:7" holds 1-3 untagged and 7 under the tag.
    std::string key = uuid_key;
    bool key_has_interval = false;
    for (size_t i = 1; i < parts.size(); ++i) {
      const std::string_view part = TrimAsciiWhitespaceView(parts[i]);
      if (part.empty()) {
        continue;
      }
      if (IsGtidTag(part)) {
        if (i > 1 && !key_has_interval) {
          return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID,
                                          "Invalid GTID set: tag without intervals: " + std::string(entry)));
        }
        // MySQL compares tags case-insensitively and reports them in lowercase.
        key = uuid_key + ":" + ToLower(part);
        key_has_interval = false;
        continue;
      }
      auto interval = ParseMysqlGtidInterval(part);
      if (!interval) {
        return MakeUnexpected(interval.error());
      }
      parsed[key].push_back(*interval);
      key_has_interval = true;
    }
    if (!key_has_interval) {
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLInvalidGTID, "Invalid GTID set: UUID without intervals: " + std::string(entry)));
    }
  }

  for (auto& [key, intervals] : parsed) {
    (void)key;
    NormalizeMysqlGtidIntervals(intervals);
  }
  return parsed;
}

std::string FormatMysqlGtidSet(const MysqlGtidSet& gtid_set) {
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

Expected<MariaDBGtidPosition, Error> ParseMariaDBGtid(std::string_view gtid) {
  const std::string_view trimmed = TrimAsciiWhitespaceView(gtid);
  if (trimmed.empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kMariaDBInvalidGTID, "Empty GTID string"));
  }

  const std::vector<std::string_view> segments = SplitView(trimmed, '-');
  if (segments.size() != 3) {
    return MakeUnexpected(MakeError(ErrorCode::kMariaDBInvalidGTID,
                                    "Invalid MariaDB GTID format: expected domain-server-seq", std::string(gtid)));
  }
  if (!AllDigits(segments[0]) || !AllDigits(segments[1]) || !AllDigits(segments[2])) {
    return MakeUnexpected(MakeError(ErrorCode::kMariaDBInvalidGTID, "Invalid MariaDB GTID format: non-numeric segment",
                                    std::string(gtid)));
  }

  auto domain_id = ParseNumeric<uint32_t>(segments[0]);
  auto server_id = ParseNumeric<uint32_t>(segments[1]);
  auto sequence_no = ParseNumeric<uint64_t>(segments[2]);
  if (!domain_id || !server_id || !sequence_no) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMariaDBInvalidGTID, "Invalid MariaDB GTID: segment out of range", std::string(gtid)));
  }
  return MariaDBGtidPosition{*domain_id, *server_id, *sequence_no};
}

Expected<std::vector<MariaDBGtidPosition>, Error> ParseMariaDBGtidList(std::string_view gtid_list) {
  std::vector<MariaDBGtidPosition> result;
  for (std::string_view entry : SplitView(TrimAsciiWhitespaceView(gtid_list), ',')) {
    if (TrimAsciiWhitespaceView(entry).empty()) {
      continue;
    }
    auto parsed = ParseMariaDBGtid(entry);
    if (!parsed) {
      return MakeUnexpected(parsed.error());
    }
    result.push_back(*parsed);
  }
  return result;
}

Expected<void, Error> ValidateGtidPosition(std::string_view position) {
  if (TrimAsciiWhitespaceView(position).empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID, "GTID position is empty"));
  }
  if (ParseMariaDBGtidList(position)) {
    return {};
  }
  auto mysql_set = ParseMysqlGtidSet(position);
  if (!mysql_set) {
    // An overflowing transaction number is an invalid position like any other defect.
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidGTID, mysql_set.error().message()));
  }
  return {};
}

}  // namespace mygramdb::utils
