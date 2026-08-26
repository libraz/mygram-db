/**
 * @file value_canonicalizer.cpp
 * @brief Shared canonical string representation for snapshot and binlog values
 */

#include "mysql/value_canonicalizer.h"

#ifdef USE_MYSQL

#include <ctime>
#include <iomanip>
#include <limits>
#include <sstream>

#include "utils/string_utils.h"

namespace mygramdb::mysql {

namespace {

template <typename T>
std::string CanonicalizeFloatingPoint(std::string_view value) {
  std::istringstream input{std::string(value)};
  input.imbue(std::locale::classic());
  T parsed{};
  if (!(input >> parsed)) {
    return std::string(value);
  }
  input >> std::ws;
  if (!input.eof()) {
    return std::string(value);
  }

  std::ostringstream output;
  output.imbue(std::locale::classic());
  output << std::setprecision(std::numeric_limits<T>::max_digits10) << parsed;
  return output.str();
}

}  // namespace

std::string CanonicalizeColumnValue(std::string_view value, CanonicalValueKind kind) {
  if (kind == CanonicalValueKind::kText) {
    return mygram::utils::SanitizeUtf8(value);
  }
  if (kind == CanonicalValueKind::kBitset) {
    // A BIT column arrives from the result set as its raw storage bytes, most
    // significant first. The binlog decoder reads the same bytes the same way,
    // so both sides publish the decimal value rather than the bytes.
    uint64_t bits = 0;
    for (const char byte : value) {
      bits = (bits << 8) | static_cast<unsigned char>(byte);
    }
    return std::to_string(bits);
  }
  if (value.empty()) {
    return std::string(value);
  }

  if (kind == CanonicalValueKind::kTemporal) {
    const size_t dot = value.find('.');
    if (dot == std::string_view::npos) {
      return std::string(value);
    }
    std::string canonical(value.substr(0, dot + 1));
    std::string_view fractional = value.substr(dot + 1);
    canonical.append(fractional.substr(0, 6));
    canonical.append(6 - std::min<size_t>(fractional.size(), 6), '0');
    return canonical;
  }

  if (kind == CanonicalValueKind::kFloat) {
    return CanonicalizeFloatingPoint<float>(value);
  }
  if (kind == CanonicalValueKind::kDouble) {
    return CanonicalizeFloatingPoint<double>(value);
  }

  size_t sign_length = (value.front() == '-' || value.front() == '+') ? 1 : 0;
  const size_t decimal_pos = value.find('.');
  const size_t integer_end = decimal_pos == std::string_view::npos ? value.size() : decimal_pos;
  size_t first_digit = sign_length;
  while (first_digit + 1 < integer_end && value[first_digit] == '0') {
    ++first_digit;
  }

  std::string canonical;
  canonical.reserve(value.size());
  if (sign_length != 0 && value.front() == '-') {
    canonical.push_back('-');
  }
  canonical.append(value.substr(first_digit));
  if (canonical == "-0" || canonical.rfind("-0.", 0) == 0) {
    bool all_zero = true;
    for (char chr : canonical) {
      if (chr >= '1' && chr <= '9') {
        all_zero = false;
        break;
      }
    }
    if (all_zero) {
      canonical.erase(canonical.begin());
    }
  }
  return canonical;
}

mygram::utils::Expected<std::string, mygram::utils::Error> FormatUtcTimestamp(uint32_t epoch_seconds,
                                                                              uint32_t microseconds,
                                                                              uint8_t precision) {
  using mygram::utils::ErrorCode;
  using mygram::utils::MakeError;
  using mygram::utils::MakeUnexpected;

  if (precision > 6 || microseconds >= 1000000) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidMetadata, "Invalid TIMESTAMP fractional value"));
  }

  // MySQL reserves an all-zero binary TIMESTAMP for the legacy zero temporal
  // value. The snapshot connection renders it as zero calendar text, so keep
  // the binlog path identical instead of interpreting it as the Unix epoch.
  if (epoch_seconds == 0) {
    if (microseconds != 0) {
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLInvalidMetadata, "Zero TIMESTAMP has a non-zero fractional value"));
    }
    std::string zero_timestamp = "0000-00-00 00:00:00";
    if (precision > 0) {
      zero_timestamp += ".000000";
    }
    return zero_timestamp;
  }

  const std::time_t timestamp = static_cast<std::time_t>(epoch_seconds);
  std::tm utc{};
#if defined(_WIN32)
  if (gmtime_s(&utc, &timestamp) != 0) {
#else
  if (gmtime_r(&timestamp, &utc) == nullptr) {
#endif
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidMetadata, "TIMESTAMP is outside UTC calendar range"));
  }

  std::ostringstream oss;
  oss << std::put_time(&utc, "%Y-%m-%d %H:%M:%S");
  if (precision > 0) {
    oss << '.' << std::setfill('0') << std::setw(6) << microseconds;
  }
  return oss.str();
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
