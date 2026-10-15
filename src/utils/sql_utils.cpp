/**
 * @file sql_utils.cpp
 * @brief SQL identifier quoting, MySQL literal encoding and table-name matching
 */

#include "utils/sql_utils.h"

#include <array>
#include <cctype>
#include <charconv>
#include <cstdio>
#include <system_error>

namespace mygramdb::utils {

Expected<std::string, Error> QuoteSQLIdentifier(std::string_view identifier) {
  if (identifier.empty()) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "SQL identifier must not be empty"));
  }
  if (identifier.find('\0') != std::string_view::npos) {
    return MakeUnexpected(MakeError(ErrorCode::kInvalidArgument, "SQL identifier must not contain NUL"));
  }

  std::string quoted;
  quoted.reserve(identifier.size() + 2);
  quoted.push_back('`');
  for (char chr : identifier) {
    quoted.push_back(chr);
    if (chr == '`') {
      quoted.push_back('`');
    }
  }
  quoted.push_back('`');
  return quoted;
}

Expected<std::string, Error> QuoteQualifiedSQLIdentifier(std::string_view database, std::string_view identifier) {
  auto quoted_identifier = QuoteSQLIdentifier(identifier);
  if (!quoted_identifier) {
    return MakeUnexpected(quoted_identifier.error());
  }
  if (database.empty()) {
    return quoted_identifier;
  }

  auto quoted_database = QuoteSQLIdentifier(database);
  if (!quoted_database) {
    return MakeUnexpected(quoted_database.error());
  }
  return *quoted_database + "." + *quoted_identifier;
}

std::string EncodeMySQLStringLiteral(std::string_view value) {
  constexpr std::array<char, 16> kHex = {
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F',
  };
  std::string encoded;
  encoded.reserve(14 + value.size() * 2);
  // Keep this a literal (coercibility 4) rather than a CONVERT expression
  // (coercibility 2). The latter forces the connection's default utf8mb4
  // collation and can make comparison with an explicitly collated column fail
  // with "Illegal mix of collations". The character-set introducer preserves
  // byte-exact, SQL-mode-independent encoding while allowing the column's
  // implicit collation to win.
  encoded.append("_utf8mb4 X'");
  for (unsigned char byte : value) {
    encoded.push_back(kHex[byte >> 4U]);
    encoded.push_back(kHex[byte & 0x0FU]);
  }
  encoded.push_back('\'');
  return encoded;
}

std::optional<std::string> FormatMySQLTimeLiteral(int64_t seconds) {
  constexpr int64_t kSecondsPerMinute = 60;
  constexpr int64_t kSecondsPerHour = 3600;
  // MySQL's TIME range is -838:59:59 to 838:59:59.
  constexpr int64_t kMaxTimeSeconds = (838 * kSecondsPerHour) + (59 * kSecondsPerMinute) + 59;

  if (seconds < -kMaxTimeSeconds || seconds > kMaxTimeSeconds) {
    return std::nullopt;
  }

  const int64_t magnitude = seconds < 0 ? -seconds : seconds;
  const auto two_digits = [](int64_t component) {
    std::string text = std::to_string(component);
    return text.size() < 2 ? "0" + text : text;
  };

  std::string literal;
  if (seconds < 0) {
    literal.push_back('-');
  }
  literal += two_digits(magnitude / kSecondsPerHour);
  literal.push_back(':');
  literal += two_digits((magnitude / kSecondsPerMinute) % kSecondsPerMinute);
  literal.push_back(':');
  literal += two_digits(magnitude % kSecondsPerMinute);
  return literal;
}

std::optional<std::string> FormatMySQLDateTimeLiteral(int64_t epoch_seconds, int32_t timezone_offset_seconds) {
  constexpr int64_t kSecondsPerDay = 86400;
  constexpr int64_t kMinLocalEpoch = -30610224000;  // 1000-01-01 00:00:00
  constexpr int64_t kMaxLocalEpoch = 253402300799;  // 9999-12-31 23:59:59

  const int64_t offset = timezone_offset_seconds;
  if (epoch_seconds < kMinLocalEpoch - offset || epoch_seconds > kMaxLocalEpoch - offset) {
    return std::nullopt;
  }

  // The local bound check above proves this addition remains representable and
  // is inside MySQL's DATETIME range before calendar arithmetic begins.
  const int64_t local = epoch_seconds + offset;
  // Floor division: a pre-epoch instant belongs to the day it falls in, not to
  // the day truncation toward zero would put it in.
  int64_t days = local / kSecondsPerDay;
  int64_t seconds_of_day = local % kSecondsPerDay;
  if (seconds_of_day < 0) {
    seconds_of_day += kSecondsPerDay;
    --days;
  }

  // Days since 1970-01-01 to a proleptic Gregorian date, shifting the era so
  // that the leap-day irregularity falls at the end of the 400-year cycle.
  int64_t shifted = days + 719468;
  const int64_t era = (shifted >= 0 ? shifted : shifted - 146096) / 146097;
  const int64_t day_of_era = shifted - era * 146097;
  const int64_t year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146096) / 365;
  const int64_t day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
  const int64_t month_index = (5 * day_of_year + 2) / 153;
  const int64_t day = day_of_year - (153 * month_index + 2) / 5 + 1;
  const int64_t month = month_index < 10 ? month_index + 3 : month_index - 9;
  const int64_t year = year_of_era + era * 400 + (month <= 2 ? 1 : 0);

  std::array<char, 32> buffer{};
  const int written = std::snprintf(
      buffer.data(), buffer.size(), "%04lld-%02lld-%02lld %02lld:%02lld:%02lld", static_cast<long long>(year),
      static_cast<long long>(month), static_cast<long long>(day), static_cast<long long>(seconds_of_day / 3600),
      static_cast<long long>((seconds_of_day / 60) % 60), static_cast<long long>(seconds_of_day % 60));
  if (written <= 0) {
    return std::nullopt;
  }
  return std::string(buffer.data(), static_cast<size_t>(written));
}

std::string FormatMySQLDoubleLiteral(double value) {
  std::array<char, 40> buffer{};
  const auto result =
      std::to_chars(buffer.data(), buffer.data() + buffer.size(), value, std::chars_format::scientific, 17);
  if (result.ec != std::errc()) {
    return "0e0";
  }
  return std::string(buffer.data(), result.ptr);
}

bool MatchTableName(const std::string& str, size_t& pos, const std::string& table_name) {
  size_t saved_pos = pos;

  // Skip optional backtick
  bool has_backtick = false;
  if (pos < str.length() && str[pos] == '`') {
    has_backtick = true;
    ++pos;
  }

  // Match table name
  if (pos + table_name.length() > str.length()) {
    pos = saved_pos;
    return false;
  }

  if (str.compare(pos, table_name.length(), table_name) != 0) {
    pos = saved_pos;
    return false;
  }

  pos += table_name.length();

  // Skip optional closing backtick
  if (has_backtick && pos < str.length() && str[pos] == '`') {
    ++pos;
  }

  // Ensure the match is a complete word (not a prefix of a longer identifier)
  // After the table name (and optional backtick), the next character must be:
  // - End of string
  // - Whitespace
  // - Semicolon
  // - Not an identifier character (alphanumeric or underscore)
  if (pos < str.length()) {
    char next_char = str[pos];
    if (std::isalnum(static_cast<unsigned char>(next_char)) != 0 || next_char == '_') {
      pos = saved_pos;
      return false;  // Table name is a prefix of a longer identifier
    }
  }

  return true;
}

}  // namespace mygramdb::utils
