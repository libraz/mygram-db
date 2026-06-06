/**
 * @file structured_log.h
 * @brief Structured logging utilities for JSON-formatted logs
 *
 * Provides helper functions for logging events in structured JSON format,
 * making it easier to parse logs programmatically for monitoring and analysis.
 */

#pragma once

#include <spdlog/spdlog.h>

#include <atomic>
#include <cstdio>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "utils/error.h"
#include "utils/namespace_compat.h"

namespace mygramdb::utils {

/**
 * @brief Maximum query / request length to log.
 *
 * Untrusted client input is truncated to this many characters before being
 * placed into structured log fields. This bounds log volume on long requests
 * and limits the blast radius of log-injection sequences embedded in queries.
 */
constexpr size_t kMaxQueryLogLength = 200;

inline bool IsDebugLogEnabled() {
  return spdlog::should_log(spdlog::level::debug);
}

/**
 * @brief Log output format
 */
enum class LogFormat : std::uint8_t {
  JSON,  // {"event":"name","field":"value"}
  TEXT   // event=name field=value
};

/**
 * @brief Structured log builder for JSON or text-formatted logs
 *
 * Example usage:
 * @code
 * StructuredLog()
 *   .Event("binlog_error")
 *   .Field("type", "connection_lost")
 *   .Field("gtid", current_gtid)
 *   .Field("retry_count", retry_count)
 *   .Error();
 * @endcode
 *
 * Format can be changed globally via SetFormat():
 * @code
 * StructuredLog::SetFormat(LogFormat::TEXT);  // Switch to text format
 * @endcode
 */
class StructuredLog {
 public:
  StructuredLog() = default;

  /**
   * @brief Set global log format (JSON or TEXT)
   * Thread-safe: Uses atomic store with relaxed memory order
   */
  static void SetFormat(LogFormat format) { format_.store(format, std::memory_order_relaxed); }

  /**
   * @brief Get current log format
   * Thread-safe: Uses atomic load with relaxed memory order
   */
  static LogFormat GetFormat() { return format_.load(std::memory_order_relaxed); }

  /**
   * @brief Parse format string to LogFormat enum
   * @param format_str Format string ("json" or "text")
   * @return LogFormat enum value (defaults to JSON for unknown values)
   */
  static LogFormat ParseFormat(const std::string& format_str) {
    if (format_str == "text") {
      return LogFormat::TEXT;
    }
    return LogFormat::JSON;  // Default to JSON
  }

  /**
   * @brief Set event type
   */
  StructuredLog& Event(const std::string& event) {
    event_ = event;
    return *this;
  }

  /**
   * @brief Add string field (const char*)
   */
  StructuredLog& Field(std::string_view key, const char* value) {
    if (format_snapshot_ == LogFormat::JSON) {
      fields_.push_back(MakeJSONField(key, Escape(std::string(value))));
    } else {
      fields_.push_back(MakeTextField(key, std::string(value)));
    }
    return *this;
  }

  /**
   * @brief Add string field (std::string)
   */
  StructuredLog& Field(std::string_view key, const std::string& value) {
    if (format_snapshot_ == LogFormat::JSON) {
      fields_.push_back(MakeJSONField(key, Escape(value)));
    } else {
      fields_.push_back(MakeTextField(key, value));
    }
    return *this;
  }

  /**
   * @brief Add string field (std::string_view)
   */
  StructuredLog& Field(std::string_view key, std::string_view value) {
    if (format_snapshot_ == LogFormat::JSON) {
      fields_.push_back(MakeJSONField(key, Escape(std::string(value))));
    } else {
      fields_.push_back(MakeTextField(key, std::string(value)));
    }
    return *this;
  }

  /**
   * @brief Add signed integer field
   */
  template <typename T,
            typename std::enable_if_t<
                std::is_integral_v<T> && std::is_signed_v<T> && !std::is_same_v<std::remove_cv_t<T>, bool>, int> = 0>
  StructuredLog& Field(std::string_view key, T value) {
    std::string val_str = std::to_string(value);
    if (format_snapshot_ == LogFormat::JSON) {
      fields_.push_back(MakeJSONField(key, val_str, false));
    } else {
      fields_.push_back(MakeRawTextField(key, val_str));
    }
    return *this;
  }

  /**
   * @brief Add unsigned integer field
   */
  template <typename T,
            typename std::enable_if_t<
                std::is_integral_v<T> && std::is_unsigned_v<T> && !std::is_same_v<std::remove_cv_t<T>, bool>, int> = 0>
  StructuredLog& Field(std::string_view key, T value) {
    std::string val_str = std::to_string(value);
    if (format_snapshot_ == LogFormat::JSON) {
      fields_.push_back(MakeJSONField(key, val_str, false));
    } else {
      fields_.push_back(MakeRawTextField(key, val_str));
    }
    return *this;
  }

  /**
   * @brief Add double field
   */
  StructuredLog& Field(std::string_view key, double value) {
    std::string val_str = std::to_string(value);
    if (format_snapshot_ == LogFormat::JSON) {
      fields_.push_back(MakeJSONField(key, val_str, false));
    } else {
      fields_.push_back(MakeRawTextField(key, val_str));
    }
    return *this;
  }

  /**
   * @brief Add boolean field
   */
  StructuredLog& Field(std::string_view key, bool value) {
    std::string val_str = value ? "true" : "false";
    if (format_snapshot_ == LogFormat::JSON) {
      fields_.push_back(MakeJSONField(key, val_str, false));
    } else {
      fields_.push_back(MakeRawTextField(key, val_str));
    }
    return *this;
  }

  /**
   * @brief Add an Error object as two fields: "error" (message) and "error_code" (numeric).
   *
   * Shortcut for `.Field("error", err.message()).Field("error_code", static_cast<int64_t>(err.code()))`.
   * Pairs the human-readable error message with its numeric code so log aggregators can
   * group/filter on `error_code` even when message text varies.
   *
   * @param err Error object to log
   * @return Reference to *this for chaining
   */
  StructuredLog& FieldError(const mygram::utils::Error& err) {
    Field("error", err.message());
    Field("error_code", static_cast<int64_t>(err.code()));
    return *this;
  }

  /**
   * @brief Add message field (optional, for human-readable context)
   */
  StructuredLog& Message(const std::string& message) {
    message_ = message;
    return *this;
  }

  /**
   * @brief Log as error level
   */
  void Error() { spdlog::error("{}", Build()); }

  /**
   * @brief Log as warning level
   */
  void Warn() { spdlog::warn("{}", Build()); }

  /**
   * @brief Log as info level
   */
  void Info() { spdlog::info("{}", Build()); }

  /**
   * @brief Log as debug level
   */
  void Debug() { spdlog::debug("{}", Build()); }

  /**
   * @brief Log as trace level
   */
  void Trace() { spdlog::trace("{}", Build()); }

  /**
   * @brief Log as critical level
   */
  void Critical() { spdlog::critical("{}", Build()); }

 private:
  std::string event_;
  std::string message_;
  LogFormat format_snapshot_{GetFormat()};
  std::vector<std::string> fields_;
  static inline std::atomic<LogFormat> format_{LogFormat::JSON};  // Default to JSON (thread-safe)

  /**
   * @brief Build log string in selected format
   * Uses the format captured when this builder was constructed.
   */
  std::string Build() const {
    if (format_snapshot_ == LogFormat::TEXT) {
      return BuildText();
    }
    return BuildJSON();
  }

  /**
   * @brief Build JSON string
   */
  std::string BuildJSON() const {
    std::string json;
    json += '{';

    bool first = true;

    // Add event type
    if (!event_.empty()) {
      json += R"("event":")";
      json += Escape(event_);
      json += '"';
      first = false;
    }

    // Add message if present
    if (!message_.empty()) {
      if (!first) {
        json += ',';
      }
      json += R"("message":")";
      json += Escape(message_);
      json += '"';
      first = false;
    }

    // Add custom fields
    for (const auto& field : fields_) {
      if (!first) {
        json += ',';
      }
      json += field;
      first = false;
    }

    json += '}';
    return json;
  }

  /**
   * @brief Build text string (key=value format)
   */
  std::string BuildText() const {
    std::string text;

    bool first = true;

    // Add event type
    if (!event_.empty()) {
      text += "event=";
      text += EscapeText(event_);
      first = false;
    }

    // Add message if present
    if (!message_.empty()) {
      if (!first) {
        text += ' ';
      }
      text += "message=\"";
      text += EscapeText(message_);
      text += '"';
      first = false;
    }

    // Add custom fields
    for (const auto& field : fields_) {
      if (!first) {
        text += ' ';
      }
      text += field;
      first = false;
    }

    return text;
  }

  /**
   * @brief Create a JSON field
   */
  static std::string MakeJSONField(std::string_view key, const std::string& value, bool quoted = true) {
    std::string result;
    if (quoted) {
      result.reserve(key.size() + value.size() + 5);  // "key":"value"
      result += '"';
      result += key;
      result += "\":\"";
      result += value;
      result += '"';
    } else {
      result.reserve(key.size() + value.size() + 3);  // "key":value
      result += '"';
      result += key;
      result += "\":";
      result += value;
    }
    return result;
  }

  /**
   * @brief Create a text field (key=value or key="value")
   */
  static std::string MakeTextField(std::string_view key, const std::string& value) {
    // Quote string values that contain spaces or special characters
    std::string result;
    if (value.find(' ') != std::string::npos || value.find('"') != std::string::npos ||
        value.find('\n') != std::string::npos) {
      result.reserve(key.size() + value.size() + 3);
      result.append(key);
      result += "=\"";
      result += EscapeText(value);
      result += '"';
      return result;
    }
    result.reserve(key.size() + value.size() + 1);
    result.append(key);
    result += '=';
    result += value;
    return result;
  }

  static std::string MakeRawTextField(std::string_view key, const std::string& value) {
    std::string result;
    result.reserve(key.size() + value.size() + 1);
    result.append(key);
    result += '=';
    result += value;
    return result;
  }

 public:
  static std::string TruncateUtf8Prefix(std::string_view value, size_t max_bytes) {
    if (value.size() <= max_bytes) {
      return std::string(value);
    }

    size_t length = max_bytes;
    while (length > 0) {
      const auto byte = static_cast<unsigned char>(value[length]);
      if ((byte & 0xC0u) != 0x80u) {
        break;
      }
      --length;
    }
    return std::string(value.substr(0, length));
  }

 private:
  /**
   * @brief Escape text for text format (escape quotes and backslashes)
   */
  static std::string EscapeText(const std::string& str) {
    std::string escaped;
    escaped.reserve(str.size());
    for (char chr : str) {
      if (chr == '"' || chr == '\\') {
        escaped += '\\';
        escaped += chr;
      } else if (chr == '\n') {
        escaped += "\\n";
      } else if (chr == '\r') {
        escaped += "\\r";
      } else if (chr == '\t') {
        escaped += "\\t";
      } else {
        escaped += chr;
      }
    }
    return escaped;
  }

  /**
   * @brief Escape JSON string
   */
  static std::string Escape(const std::string& str) {
    // Control character threshold for JSON escaping (0x20 = space)
    constexpr char kControlCharThreshold = 0x20;

    std::string escaped;
    escaped.reserve(str.size());
    for (char chr : str) {
      switch (chr) {
        case '"':
          escaped += R"(\")";
          break;
        case '\\':
          escaped += R"(\\)";
          break;
        case '\b':
          escaped += R"(\b)";
          break;
        case '\f':
          escaped += R"(\f)";
          break;
        case '\n':
          escaped += R"(\n)";
          break;
        case '\r':
          escaped += R"(\r)";
          break;
        case '\t':
          escaped += R"(\t)";
          break;
        default:
          if (chr >= 0 && chr < kControlCharThreshold) {
            // Control characters: \u00XX format
            char buf[7];  // NOLINT(modernize-avoid-c-arrays)
            std::snprintf(buf, sizeof(buf), "\\u%04x", static_cast<unsigned char>(chr));
            escaped += buf;
          } else {
            escaped += chr;
          }
      }
    }
    return escaped;
  }
};

/**
 * @brief Log MySQL connection error in structured format
 */
inline void LogMySQLConnectionError(const std::string& host, int port, const std::string& error_msg) {
  StructuredLog()
      .Event("mysql_connection_error")
      .Field("host", host)
      .Field("port", static_cast<int64_t>(port))
      .Field("error", error_msg)
      .Error();
}

/**
 * @brief Log MySQL query error in structured format
 */
inline void LogMySQLQueryError(const std::string& query, const std::string& error_msg) {
  StructuredLog()
      .Event("mysql_query_error")
      .Field("query", StructuredLog::TruncateUtf8Prefix(query, kMaxQueryLogLength))
      .Field("error", error_msg)
      .Error();
}

/**
 * @brief Log binlog replication error in structured format
 */
inline void LogBinlogError(const std::string& error_type, const std::string& gtid, const std::string& error_msg,
                           int retry_count = 0) {
  StructuredLog()
      .Event("binlog_error")
      .Field("type", error_type)
      .Field("gtid", gtid)
      .Field("retry_count", static_cast<int64_t>(retry_count))
      .Field("error", error_msg)
      .Error();
}

/**
 * @brief Log storage error in structured format
 */
inline void LogStorageError(const std::string& operation, const std::string& filepath, const std::string& error_msg) {
  StructuredLog()
      .Event("storage_error")
      .Field("operation", operation)
      .Field("filepath", filepath)
      .Field("error", error_msg)
      .Error();
}

/**
 * @brief Log query parsing error in structured format
 */
inline void LogQueryParseError(const std::string& query, const std::string& error_msg, size_t error_position = 0) {
  StructuredLog()
      .Event("query_parse_error")
      .Field("query", StructuredLog::TruncateUtf8Prefix(query, kMaxQueryLogLength))
      .Field("error", error_msg)
      .Field("position", static_cast<int64_t>(error_position))
      .Error();
}

}  // namespace mygramdb::utils
