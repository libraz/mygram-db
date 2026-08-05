/**
 * @file binary_json.cpp
 * @brief Decoder for MySQL's binary JSON representation.
 */

#include "mysql/binary_json.h"

#ifdef USE_MYSQL

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string_view>
#include <vector>

#include "utils/error.h"

namespace mygramdb::mysql {
namespace {

using mygram::utils::Error;
using mygram::utils::ErrorCode;
using mygram::utils::Expected;
using mygram::utils::MakeError;
using mygram::utils::MakeUnexpected;

constexpr uint8_t kSmallObject = 0x00;
constexpr uint8_t kLargeObject = 0x01;
constexpr uint8_t kSmallArray = 0x02;
constexpr uint8_t kLargeArray = 0x03;
constexpr uint8_t kLiteral = 0x04;
constexpr uint8_t kInt16 = 0x05;
constexpr uint8_t kUint16 = 0x06;
constexpr uint8_t kInt32 = 0x07;
constexpr uint8_t kUint32 = 0x08;
constexpr uint8_t kInt64 = 0x09;
constexpr uint8_t kUint64 = 0x0A;
constexpr uint8_t kDouble = 0x0B;
constexpr uint8_t kString = 0x0C;
constexpr uint8_t kOpaque = 0x0F;
constexpr size_t kMaxDepth = 100;
constexpr uint64_t kMaxElements = 1'000'000;

Expected<uint64_t, Error> ReadLittleEndian(const unsigned char* data, size_t size, size_t width) {
  if (width == 0 || width > sizeof(uint64_t) || size < width) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Binary JSON integer is truncated"));
  }
  uint64_t value = 0;
  for (size_t index = 0; index < width; ++index) {
    value |= static_cast<uint64_t>(data[index]) << (index * 8);
  }
  return value;
}

Expected<std::pair<uint64_t, size_t>, Error> ReadVariableLength(const unsigned char* data, size_t size) {
  uint64_t value = 0;
  for (size_t index = 0; index < size && index < 10; ++index) {
    const uint8_t byte = data[index];
    if (index == 9 && (byte & 0xFEU) != 0) {
      break;
    }
    value |= static_cast<uint64_t>(byte & 0x7FU) << (index * 7);
    if ((byte & 0x80U) == 0) {
      return std::pair<uint64_t, size_t>{value, index + 1};
    }
  }
  return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidMetadata, "Invalid binary JSON variable-length integer"));
}

Expected<void, Error> OutputLimitExceeded() {
  return MakeUnexpected(
      MakeError(ErrorCode::kMySQLInvalidMetadata, "Binary JSON decoded output exceeds configured limit"));
}

Expected<void, Error> AppendText(std::string_view value, std::string& output, size_t max_output_bytes) {
  if (value.size() > max_output_bytes - std::min(output.size(), max_output_bytes)) {
    return OutputLimitExceeded();
  }
  output.append(value);
  return {};
}

Expected<void, Error> AppendCharacter(char value, std::string& output, size_t max_output_bytes) {
  if (output.size() >= max_output_bytes) {
    return OutputLimitExceeded();
  }
  output.push_back(value);
  return {};
}

Expected<void, Error> AppendEscapedString(std::string_view value, std::string& output, size_t max_output_bytes) {
  static constexpr char kHex[] = "0123456789abcdef";
  size_t encoded_size = 2;
  for (const unsigned char byte : value) {
    const size_t increment =
        byte < 0x20U ? ((byte == '\b' || byte == '\f' || byte == '\n' || byte == '\r' || byte == '\t') ? 2 : 6)
                     : ((byte == '"' || byte == '\\') ? 2 : 1);
    if (increment > max_output_bytes - std::min(encoded_size, max_output_bytes)) {
      return OutputLimitExceeded();
    }
    encoded_size += increment;
  }
  if (encoded_size > max_output_bytes - std::min(output.size(), max_output_bytes)) {
    return OutputLimitExceeded();
  }

  output.push_back('"');
  for (const unsigned char byte : value) {
    switch (byte) {
      case '"':
        output += "\\\"";
        break;
      case '\\':
        output += "\\\\";
        break;
      case '\b':
        output += "\\b";
        break;
      case '\f':
        output += "\\f";
        break;
      case '\n':
        output += "\\n";
        break;
      case '\r':
        output += "\\r";
        break;
      case '\t':
        output += "\\t";
        break;
      default:
        if (byte < 0x20U) {
          output += "\\u00";
          output.push_back(kHex[byte >> 4]);
          output.push_back(kHex[byte & 0x0FU]);
        } else {
          output.push_back(static_cast<char>(byte));
        }
    }
  }
  output.push_back('"');
  return {};
}

Expected<void, Error> AppendOpaquePlaceholder(const unsigned char* data, size_t size, std::string& output,
                                              size_t max_output_bytes) {
  // MySQL encodes opaque JSON values as: field type (1 byte), variable-length
  // payload length, then the raw value. DATE/TIME and DECIMAL are common
  // examples. We do not yet reconstruct every MySQL field type, but must
  // validate the envelope before emitting a JSON string; otherwise a malformed
  // opaque value could silently desynchronise a containing JSON document.
  if (size < 2) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Binary JSON value is truncated"));
  }
  auto payload_length = ReadVariableLength(data + 1, size - 1);
  if (!payload_length) {
    return MakeUnexpected(payload_length.error());
  }
  const size_t header_size = 1 + payload_length->second;
  if (payload_length->first > size - header_size) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Binary JSON value is truncated"));
  }

  return AppendEscapedString("__mygramdb_opaque_json_type_" + std::to_string(data[0]) + "__", output, max_output_bytes);
}

class Decoder {
 public:
  explicit Decoder(size_t max_output_bytes) : max_output_bytes_(max_output_bytes) {}

  Expected<std::string, Error> Decode(const unsigned char* data, size_t size) {
    if (data == nullptr || size == 0) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Binary JSON value is empty"));
    }
    std::string output;
    auto result = DecodeValue(data[0], data + 1, size - 1, 0, output);
    if (!result) {
      return MakeUnexpected(result.error());
    }
    return output;
  }

 private:
  Expected<void, Error> DecodeValue(uint8_t type, const unsigned char* data, size_t size, size_t depth,
                                    std::string& output) {
    if (depth > kMaxDepth) {
      return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidMetadata, "Binary JSON nesting is too deep"));
    }
    switch (type) {
      case kSmallObject:
        return DecodeContainer(type, data, size, false, false, depth, output);
      case kLargeObject:
        return DecodeContainer(type, data, size, true, false, depth, output);
      case kSmallArray:
        return DecodeContainer(type, data, size, false, true, depth, output);
      case kLargeArray:
        return DecodeContainer(type, data, size, true, true, depth, output);
      case kLiteral:
        if (size < 1) {
          return Truncated();
        }
        if (data[0] == 0) {
          return AppendText("null", output, max_output_bytes_);
        } else if (data[0] == 1) {
          return AppendText("true", output, max_output_bytes_);
        } else if (data[0] == 2) {
          return AppendText("false", output, max_output_bytes_);
        } else {
          return Invalid("Unknown binary JSON literal");
        }
      case kInt16:
        return AppendSigned(data, size, 2, output);
      case kUint16:
        return AppendUnsigned(data, size, 2, output);
      case kInt32:
        return AppendSigned(data, size, 4, output);
      case kUint32:
        return AppendUnsigned(data, size, 4, output);
      case kInt64:
        return AppendSigned(data, size, 8, output);
      case kUint64:
        return AppendUnsigned(data, size, 8, output);
      case kDouble:
        return AppendDouble(data, size, output);
      case kString:
        return AppendString(data, size, output);
      case kOpaque:
        return AppendOpaquePlaceholder(data, size, output, max_output_bytes_);
      default:
        return Invalid("Unknown binary JSON type");
    }
  }

  Expected<void, Error> DecodeContainer(uint8_t container_type, const unsigned char* data, size_t size, bool large,
                                        bool array, size_t depth, std::string& output) {
    const auto identity = std::pair<uint8_t, const unsigned char*>{container_type, data};
    if (std::find(active_containers_.begin(), active_containers_.end(), identity) != active_containers_.end()) {
      return Invalid("Binary JSON container cycle detected");
    }
    active_containers_.push_back(identity);
    struct ActiveContainerGuard {
      std::vector<std::pair<uint8_t, const unsigned char*>>& active;
      ~ActiveContainerGuard() { active.pop_back(); }
    } active_guard{active_containers_};

    const size_t offset_width = large ? 4 : 2;
    const size_t header_size = offset_width * 2;
    if (size < header_size) {
      return Truncated();
    }
    auto count_result = ReadLittleEndian(data, size, offset_width);
    auto bytes_result = ReadLittleEndian(data + offset_width, size - offset_width, offset_width);
    if (!count_result || !bytes_result) {
      return Truncated();
    }
    const uint64_t count = *count_result;
    const uint64_t encoded_size = *bytes_result;
    if (count > kMaxElements || encoded_size < header_size || encoded_size > size) {
      return Invalid("Invalid binary JSON container size");
    }

    const size_t key_entry_size = offset_width + 2;
    const size_t value_entry_size = offset_width + 1;
    const uint64_t entry_bytes = count * static_cast<uint64_t>(value_entry_size + (array ? 0 : key_entry_size));
    if (entry_bytes > encoded_size - header_size) {
      return Invalid("Binary JSON entry table exceeds container");
    }
    const size_t key_table = header_size;
    const size_t value_table = header_size + static_cast<size_t>(count) * (array ? 0 : key_entry_size);

    if (auto appended = AppendCharacter(array ? '[' : '{', output, max_output_bytes_); !appended) {
      return appended;
    }
    for (uint64_t index = 0; index < count; ++index) {
      if (index != 0) {
        if (auto appended = AppendCharacter(',', output, max_output_bytes_); !appended) {
          return appended;
        }
      }
      if (!array) {
        const unsigned char* key_entry = data + key_table + static_cast<size_t>(index) * key_entry_size;
        auto key_offset = ReadLittleEndian(key_entry, key_entry_size, offset_width);
        auto key_length = ReadLittleEndian(key_entry + offset_width, 2, 2);
        if (!key_offset || !key_length || *key_offset > encoded_size || *key_length > encoded_size - *key_offset) {
          return Invalid("Binary JSON key exceeds container");
        }
        if (auto appended = AppendEscapedString({reinterpret_cast<const char*>(data + static_cast<size_t>(*key_offset)),
                                                 static_cast<size_t>(*key_length)},
                                                output, max_output_bytes_);
            !appended) {
          return appended;
        }
        if (auto appended = AppendCharacter(':', output, max_output_bytes_); !appended) {
          return appended;
        }
      }

      const unsigned char* value_entry = data + value_table + static_cast<size_t>(index) * value_entry_size;
      const uint8_t type = value_entry[0];
      if (IsInline(type, large)) {
        auto inline_result = DecodeValue(type, value_entry + 1, offset_width, depth + 1, output);
        if (!inline_result) {
          return inline_result;
        }
        continue;
      }
      auto value_offset = ReadLittleEndian(value_entry + 1, offset_width, offset_width);
      if (!value_offset || *value_offset >= encoded_size) {
        return Invalid("Binary JSON value offset exceeds container");
      }
      auto value_result = DecodeValue(type, data + static_cast<size_t>(*value_offset),
                                      static_cast<size_t>(encoded_size - *value_offset), depth + 1, output);
      if (!value_result) {
        return value_result;
      }
    }
    return AppendCharacter(array ? ']' : '}', output, max_output_bytes_);
  }

  static bool IsInline(uint8_t type, bool large) {
    return type == kLiteral || type == kInt16 || type == kUint16 || (large && (type == kInt32 || type == kUint32));
  }

  Expected<void, Error> AppendUnsigned(const unsigned char* data, size_t size, size_t width, std::string& output) {
    auto value = ReadLittleEndian(data, size, width);
    if (!value) {
      return MakeUnexpected(value.error());
    }
    return AppendText(std::to_string(*value), output, max_output_bytes_);
  }

  Expected<void, Error> AppendSigned(const unsigned char* data, size_t size, size_t width, std::string& output) {
    auto value = ReadLittleEndian(data, size, width);
    if (!value) {
      return MakeUnexpected(value.error());
    }
    int64_t signed_value = 0;
    if (width == 2) {
      signed_value = static_cast<int16_t>(*value);
    } else if (width == 4) {
      signed_value = static_cast<int32_t>(*value);
    } else {
      std::memcpy(&signed_value, &*value, sizeof(signed_value));
    }
    return AppendText(std::to_string(signed_value), output, max_output_bytes_);
  }

  Expected<void, Error> AppendDouble(const unsigned char* data, size_t size, std::string& output) {
    auto bits = ReadLittleEndian(data, size, 8);
    if (!bits) {
      return MakeUnexpected(bits.error());
    }
    double value = 0;
    std::memcpy(&value, &*bits, sizeof(value));
    if (!std::isfinite(value)) {
      return Invalid("Non-finite binary JSON number");
    }
    std::ostringstream stream;
    stream << std::setprecision(std::numeric_limits<double>::max_digits10) << value;
    return AppendText(stream.str(), output, max_output_bytes_);
  }

  Expected<void, Error> AppendString(const unsigned char* data, size_t size, std::string& output) {
    auto length = ReadVariableLength(data, size);
    if (!length || length->first > size - length->second) {
      return length ? Truncated() : MakeUnexpected(length.error());
    }
    return AppendEscapedString(
        {reinterpret_cast<const char*>(data + length->second), static_cast<size_t>(length->first)}, output,
        max_output_bytes_);
  }

  static Expected<void, Error> Truncated() {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLFieldTruncated, "Binary JSON value is truncated"));
  }

  static Expected<void, Error> Invalid(const std::string& message) {
    return MakeUnexpected(MakeError(ErrorCode::kMySQLInvalidMetadata, message));
  }

  size_t max_output_bytes_;
  std::vector<std::pair<uint8_t, const unsigned char*>> active_containers_;
};

}  // namespace

Expected<std::string, Error> DecodeBinaryJson(const unsigned char* data, size_t size) {
  return DecodeBinaryJson(data, size, kMaxBinaryJsonOutputBytes);
}

Expected<std::string, Error> DecodeBinaryJson(const unsigned char* data, size_t size, size_t max_output_bytes) {
  return Decoder{max_output_bytes}.Decode(data, size);
}

}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
