/**
 * @file value_canonicalizer.cpp
 * @brief Shared canonical string representation for snapshot and binlog values
 */

#include "mysql/value_canonicalizer.h"

#ifdef USE_MYSQL

#include <ctime>
#include <iomanip>
#include <limits>
#include <locale>
#include <sstream>
#include <utility>

#include "mybinlog/json.h"
#include "utils/string_utils.h"

namespace mygramdb::mysql {

namespace {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

template <typename T>
std::string FormatClassicLocale(T value) {
  std::ostringstream output;
  output.imbue(std::locale::classic());
  output << std::setprecision(std::numeric_limits<T>::max_digits10) << value;
  return output.str();
}

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

  return FormatClassicLocale(parsed);
}

/// Append @p value in decimal, left-padded with zeros to at least @p width digits.
void AppendPadded(std::string& out, uint64_t value, size_t width) {
  const std::string digits = std::to_string(value);
  if (digits.size() < width) {
    out.append(width - digits.size(), '0');
  }
  out += digits;
}

void AppendDate(std::string& out, const mybinlog::Temporal& temporal) {
  AppendPadded(out, temporal.year, 4);
  out.push_back('-');
  AppendPadded(out, temporal.month, 2);
  out.push_back('-');
  AppendPadded(out, temporal.day, 2);
}

/// HH:MM:SS, then six fractional digits when the column declares a fractional precision.
void AppendClock(std::string& out, const mybinlog::Temporal& temporal) {
  AppendPadded(out, temporal.hour, 2);
  out.push_back(':');
  AppendPadded(out, temporal.minute, 2);
  out.push_back(':');
  AppendPadded(out, temporal.second, 2);
  if (temporal.fsp > 0) {
    out.push_back('.');
    AppendPadded(out, temporal.microsecond, 6);
  }
}

std::string HexEncode(const std::string& bytes) {
  static constexpr char kDigits[] = "0123456789abcdef";
  std::string hex;
  hex.reserve(bytes.size() * 2);
  for (const char byte : bytes) {
    const auto value = static_cast<unsigned char>(byte);
    hex.push_back(kDigits[value >> 4]);
    hex.push_back(kDigits[value & 0x0F]);
  }
  return hex;
}

/// The declared labels an ENUM ordinal or a SET bitmask selects, as the result set returns them.
Expected<std::string, Error> EnumSetText(mybinlog::Value::Kind kind, uint64_t value,
                                         const std::vector<std::string>* labels) {
  const bool is_enum = kind == mybinlog::Value::Kind::kEnum;
  const char* kind_name = is_enum ? "ENUM" : "SET";
  if (labels == nullptr) {
    return std::to_string(value);
  }
  if (labels->empty()) {
    return MakeUnexpected(
        MakeError(ErrorCode::kMySQLInvalidMetadata, std::string(kind_name) + " labels are unavailable"));
  }

  if (is_enum) {
    if (value == 0) {
      return std::string{};
    }
    if (value > labels->size()) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidMetadata, "ENUM ordinal is outside label table"));
    }
    return (*labels)[value - 1];
  }

  constexpr size_t kMaxSetMembers = 64;
  if (labels->size() > kMaxSetMembers) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidMetadata, "SET has more than 64 labels"));
  }
  std::string decoded_labels;
  for (size_t index = 0; index < labels->size(); ++index) {
    if ((value & (uint64_t{1} << index)) == 0) {
      continue;
    }
    if (!decoded_labels.empty()) {
      decoded_labels.push_back(',');
    }
    decoded_labels += (*labels)[index];
  }
  if (labels->size() < kMaxSetMembers && (value >> labels->size()) != 0) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidMetadata, "SET bitmask is outside label table"));
  }
  return decoded_labels;
}

}  // namespace

std::string FormatFloatingPoint(float value) {
  return FormatClassicLocale(value);
}

std::string FormatFloatingPoint(double value) {
  return FormatClassicLocale(value);
}

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

ErrorCode RowDecodeErrorCode(mybinlog::Status status) {
  switch (status) {
    case mybinlog::Status::kTruncated:
    case mybinlog::Status::kLimitExceeded:
      return ErrorCode::kMySQLFieldTruncated;
    case mybinlog::Status::kInvalidMetadata:
    case mybinlog::Status::kMalformedValue:
      return ErrorCode::kMySQLInvalidMetadata;
    case mybinlog::Status::kUnsupported:
      return ErrorCode::kMySQLUnsupportedType;
    case mybinlog::Status::kOk:
    case mybinlog::Status::kChecksumMismatch:
    case mybinlog::Status::kNeedMoreData:
      break;
  }
  return ErrorCode::kMySQLBinlogError;
}

Expected<std::string, Error> CanonicalizeBinlogValue(mybinlog::Value&& value, const std::vector<std::string>* labels) {
  using Kind = mybinlog::Value::Kind;
  switch (value.kind) {
    case Kind::kNull:
      return std::string{};
    case Kind::kInt:
      return std::to_string(value.i);
    case Kind::kUInt:
    case Kind::kBit:
      return std::to_string(value.u);
    case Kind::kYear:
      // The zero year is four zeros on both paths; any other YEAR is 1901..2155.
      return value.u == 0 ? std::string{"0000"} : std::to_string(value.u);
    case Kind::kDouble:
      // A FLOAT arrives widened to double; narrowing it back is exact, and it
      // has to be printed with a float's round-trip digits.
      if (value.type == mybinlog::ColumnType::kFloat) {
        return FormatFloatingPoint(static_cast<float>(value.d));
      }
      return FormatFloatingPoint(value.d);
    case Kind::kDecimal:
      return CanonicalizeColumnValue(value.bytes, CanonicalValueKind::kDecimal);
    case Kind::kDate: {
      std::string text;
      AppendDate(text, value.t);
      return text;
    }
    case Kind::kDateTime: {
      std::string text;
      AppendDate(text, value.t);
      text.push_back(' ');
      AppendClock(text, value.t);
      return text;
    }
    case Kind::kTime: {
      std::string text;
      if (value.t.negative) {
        text.push_back('-');
      }
      AppendClock(text, value.t);
      return text;
    }
    case Kind::kTimestamp:
      // A TIMESTAMP's seconds are four unsigned bytes on the wire.
      return FormatUtcTimestamp(static_cast<uint32_t>(value.epoch_seconds), value.microsecond, value.fsp);
    case Kind::kEnum:
    case Kind::kSet:
      return EnumSetText(value.kind, value.u, labels);
    case Kind::kText:
    case Kind::kBytes:
      // GEOMETRY and VECTOR payloads are not text; they publish as lowercase hex.
      if (value.type == mybinlog::ColumnType::kGeometry || value.type == mybinlog::ColumnType::kVector) {
        return HexEncode(value.bytes);
      }
      if (mygram::utils::IsValidUtf8(value.bytes)) {
        return std::move(value.bytes);
      }
      return CanonicalizeColumnValue(value.bytes, CanonicalValueKind::kText);
    case Kind::kJson: {
      std::string text;
      const auto status =
          mybinlog::JsonBinaryToText(reinterpret_cast<const uint8_t*>(value.bytes.data()), value.bytes.size(), &text);
      if (status != mybinlog::Status::kOk) {
        return MakeUnexpected(MakeError(RowDecodeErrorCode(status),
                                        std::string("Undecodable JSON value: ") + mybinlog::StatusName(status)));
      }
      return text;
    }
    case Kind::kJsonDiff:
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLUnsupportedType, "Partial JSON update requires binlog_row_value_options=''"));
    case Kind::kAbsent:
      return MakeUnexpected(
          MakeError(ErrorCode::kMySQLBinlogError, "Column absent from row image requires binlog_row_image=FULL"));
  }
  return MakeUnexpected(MakeError(ErrorCode::kMySQLUnsupportedType, "Unknown decoded value kind"));
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
