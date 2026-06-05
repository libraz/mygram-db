/**
 * @file mariadb_gtid.cpp
 * @brief MariaDB GTID parsing and representation implementation
 */

#include "mysql/mariadb_gtid.h"

#include <algorithm>
#include <charconv>
#include <string_view>

#include "utils/string_utils.h"

namespace mygramdb::mysql {

namespace {

/**
 * @brief Count occurrences of a character in a string_view
 */
size_t CountChar(std::string_view sv, char ch) {
  size_t count = 0;
  for (char c : sv) {
    if (c == ch) {
      ++count;
    }
  }
  return count;
}

/**
 * @brief Check if all characters in a string_view are digits
 */
bool AllDigits(std::string_view sv) {
  return !sv.empty() && std::all_of(sv.begin(), sv.end(), [](char c) { return c >= '0' && c <= '9'; });
}

}  // namespace

mygram::utils::Expected<MariaDBGTID, mygram::utils::Error> MariaDBGTID::Parse(const std::string& gtid_str) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  std::string_view sv = mygram::utils::TrimAsciiWhitespaceView(std::string_view(gtid_str));

  if (sv.empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kMariaDBInvalidGTID, "Empty GTID string"));
  }

  // MariaDB GTID format: "domain_id-server_id-sequence_no"
  // Must have exactly 2 dashes
  if (CountChar(sv, '-') != 2) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMariaDBInvalidGTID, "Invalid MariaDB GTID format: expected domain-server-seq", gtid_str));
  }

  // Split by dashes
  auto first_dash = sv.find('-');
  auto second_dash = sv.find('-', first_dash + 1);

  std::string_view domain_str = sv.substr(0, first_dash);
  std::string_view server_str = sv.substr(first_dash + 1, second_dash - first_dash - 1);
  std::string_view seq_str = sv.substr(second_dash + 1);

  // Validate all segments are numeric
  if (!AllDigits(domain_str) || !AllDigits(server_str) || !AllDigits(seq_str)) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMariaDBInvalidGTID, "Invalid MariaDB GTID format: non-numeric segment", gtid_str));
  }

  MariaDBGTID gtid;

  // Parse domain_id (uint32_t)
  auto [ptr1, ec1] = std::from_chars(domain_str.data(), domain_str.data() + domain_str.size(), gtid.domain_id);
  if (ec1 != std::errc{}) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMariaDBInvalidGTID, "Invalid MariaDB GTID: domain_id out of range", gtid_str));
  }

  // Parse server_id (uint32_t)
  auto [ptr2, ec2] = std::from_chars(server_str.data(), server_str.data() + server_str.size(), gtid.server_id);
  if (ec2 != std::errc{}) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMariaDBInvalidGTID, "Invalid MariaDB GTID: server_id out of range", gtid_str));
  }

  // Parse sequence_no (uint64_t)
  auto [ptr3, ec3] = std::from_chars(seq_str.data(), seq_str.data() + seq_str.size(), gtid.sequence_no);
  if (ec3 != std::errc{}) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMariaDBInvalidGTID, "Invalid MariaDB GTID: sequence_no out of range", gtid_str));
  }

  return gtid;
}

mygram::utils::Expected<std::vector<MariaDBGTID>, mygram::utils::Error> MariaDBGTID::ParseSet(
    const std::string& gtid_set_str) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  std::string_view sv = mygram::utils::TrimAsciiWhitespaceView(std::string_view(gtid_set_str));

  if (sv.empty()) {
    return std::vector<MariaDBGTID>{};
  }

  std::vector<MariaDBGTID> result;

  // Split by commas
  size_t start = 0;
  while (start < sv.size()) {
    size_t comma_pos = sv.find(',', start);
    std::string_view entry;
    if (comma_pos == std::string_view::npos) {
      entry = sv.substr(start);
      start = sv.size();
    } else {
      entry = sv.substr(start, comma_pos - start);
      start = comma_pos + 1;
    }

    entry = mygram::utils::TrimAsciiWhitespaceView(entry);
    if (entry.empty()) {
      continue;
    }

    auto parsed = Parse(std::string(entry));
    if (!parsed) {
      return MakeUnexpected(parsed.error());
    }
    result.push_back(*parsed);
  }

  return result;
}

std::string MariaDBGTID::ToString() const {
  return std::to_string(domain_id) + "-" + std::to_string(server_id) + "-" + std::to_string(sequence_no);
}

std::string MariaDBGTID::SetToString(const std::vector<MariaDBGTID>& gtids) {
  std::string result;
  for (size_t i = 0; i < gtids.size(); ++i) {
    if (i > 0) {
      result += ",";
    }
    result += gtids[i].ToString();
  }
  return result;
}

bool MariaDBGTID::IsMariaDBGtidFormat(const std::string& gtid_str) {
  std::string_view sv = mygram::utils::TrimAsciiWhitespaceView(std::string_view(gtid_str));
  if (sv.empty()) {
    return false;
  }

  // MariaDB GTID: exactly 2 dashes, all segments numeric
  // MySQL GTID: 4 dashes (UUID format), hex segments
  if (CountChar(sv, '-') != 2) {
    return false;
  }

  auto first_dash = sv.find('-');
  auto second_dash = sv.find('-', first_dash + 1);

  std::string_view domain_str = sv.substr(0, first_dash);
  std::string_view server_str = sv.substr(first_dash + 1, second_dash - first_dash - 1);
  std::string_view seq_str = sv.substr(second_dash + 1);

  return AllDigits(domain_str) && AllDigits(server_str) && AllDigits(seq_str);
}

}  // namespace mygramdb::mysql
