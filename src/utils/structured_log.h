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
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>

namespace mygram::utils {

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
  StructuredLog& Field(const std::string& key, const char* value) {
    fields_.push_back(MakeJSONField(key, Escape(std::string(value))));
    fields_text_.push_back(MakeTextField(key, std::string(value)));
    return *this;
  }

  /**
   * @brief Add string field (std::string)
   */
  StructuredLog& Field(const std::string& key, const std::string& value) {
    fields_.push_back(MakeJSONField(key, Escape(value)));
    fields_text_.push_back(MakeTextField(key, value));
    return *this;
  }

  /**
   * @brief Add string field (std::string_view)
   */
  StructuredLog& Field(const std::string& key, std::string_view value) {
    fields_.push_back(MakeJSONField(key, Escape(std::string(value))));
    fields_text_.push_back(MakeTextField(key, std::string(value)));
    return *this;
  }

  /**
   * @brief Add integer field
   */
  StructuredLog& Field(const std::string& key, int64_t value) {
    std::string val_str = std::to_string(value);
    fields_.push_back(MakeJSONField(key, val_str, true));  // Quoted for JSON
    fields_text_.push_back(key + "=" + val_str);
    return *this;
  }

  /**
   * @brief Add unsigned integer field
   */
  StructuredLog& Field(const std::string& key, uint64_t value) {
    std::string val_str = std::to_string(value);
    fields_.push_back(MakeJSONField(key, val_str, true));  // Quoted for JSON
    fields_text_.push_back(key + "=" + val_str);
    return *this;
  }

  /**
   * @brief Add double field
   */
  StructuredLog& Field(const std::string& key, double value) {
    std::ostringstream oss;
    oss << value;
    std::string val_str = oss.str();
    fields_.push_back(MakeJSONField(key, val_str, true));  // Quoted for JSON
    fields_text_.push_back(key + "=" + val_str);
    return *this;
  }

  /**
   * @brief Add boolean field
   */
  StructuredLog& Field(const std::string& key, bool value) {
    std::string val_str = value ? "true" : "false";
    fields_.push_back(MakeJSONField(key, val_str, false));  // No quotes for booleans in JSON
    fields_text_.push_back(key + "=" + val_str);
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
   * @brief Log as critical level
   */
  void Critical() { spdlog::critical("{}", Build()); }

 private:
  std::string event_;
  std::string message_;
  std::vector<std::string> fields_;                               // JSON format fields
  std::vector<std::string> fields_text_;                          // Text format fields
  static inline std::atomic<LogFormat> format_{LogFormat::JSON};  // Default to JSON (thread-safe)

  /**
   * @brief Build log string in selected format
   * Thread-safe: Reads format atomically
   */
  std::string Build() const {
    if (format_.load(std::memory_order_relaxed) == LogFormat::TEXT) {
      return BuildText();
    }
    return BuildJSON();
  }

  /**
   * @brief Build JSON string
   */
  std::string BuildJSON() const {
    std::ostringstream json;
    json << "{";

    bool first = true;

    // Add event type
    if (!event_.empty()) {
      json << R"("event":")" << Escape(event_) << R"(")";
      first = false;
    }

    // Add message if present
    if (!message_.empty()) {
      if (!first) {
        json << ",";
      }
      json << R"("message":")" << Escape(message_) << R"(")";
      first = false;
    }

    // Add custom fields
    for (const auto& field : fields_) {
      if (!first) {
        json << ",";
      }
      json << field;
      first = false;
    }

    json << "}";
    return json.str();
  }

  /**
   * @brief Build text string (key=value format)
   */
  std::string BuildText() const {
    std::ostringstream text;

    bool first = true;

    // Add event type
    if (!event_.empty()) {
      text << "event=" << EscapeText(event_);
      first = false;
    }

    // Add message if present
    if (!message_.empty()) {
      if (!first) {
        text << " ";
      }
      text << "message=\"" << EscapeText(message_) << "\"";
      first = false;
    }

    // Add custom fields
    for (const auto& field : fields_text_) {
      if (!first) {
        text << " ";
      }
      text << field;
      first = false;
    }

    return text.str();
  }

  /**
   * @brief Create a JSON field
   */
  static std::string MakeJSONField(const std::string& key, const std::string& value, bool quoted = true) {
    std::ostringstream oss;
    oss << "\"" << key << "\":";
    if (quoted) {
      oss << "\"" << value << "\"";
    } else {
      oss << value;
    }
    return oss.str();
  }

  /**
   * @brief Create a text field (key=value or key="value")
   */
  static std::string MakeTextField(const std::string& key, const std::string& value) {
    // Quote string values that contain spaces or special characters
    if (value.find(' ') != std::string::npos || value.find('"') != std::string::npos ||
        value.find('\n') != std::string::npos) {
      return key + "=\"" + EscapeText(value) + "\"";
    }
    return key + "=" + value;
  }

  /**
   * @brief Escape text for text format (escape quotes and backslashes)
   */
  static std::string EscapeText(const std::string& str) {
    std::ostringstream escaped;
    for (char chr : str) {
      if (chr == '"' || chr == '\\') {
        escaped << '\\' << chr;
      } else if (chr == '\n') {
        escaped << "\\n";
      } else if (chr == '\r') {
        escaped << "\\r";
      } else if (chr == '\t') {
        escaped << "\\t";
      } else {
        escaped << chr;
      }
    }
    return escaped.str();
  }

  /**
   * @brief Escape JSON string
   */
  static std::string Escape(const std::string& str) {
    // Control character threshold for JSON escaping (0x20 = space)
    constexpr char kControlCharThreshold = 0x20;

    std::ostringstream escaped;
    for (char chr : str) {
      switch (chr) {
        case '"':
          escaped << R"(\")";
          break;
        case '\\':
          escaped << R"(\\)";
          break;
        case '\b':
          escaped << R"(\b)";
          break;
        case '\f':
          escaped << R"(\f)";
          break;
        case '\n':
          escaped << R"(\n)";
          break;
        case '\r':
          escaped << R"(\r)";
          break;
        case '\t':
          escaped << R"(\t)";
          break;
        default:
          if (chr >= 0 && chr < kControlCharThreshold) {
            // Control characters
            escaped << R"(\u)" << std::hex << std::setw(4) << std::setfill('0') << static_cast<int>(chr);
          } else {
            escaped << chr;
          }
      }
    }
    return escaped.str();
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
  // Maximum query length to log (prevent log spam)
  constexpr size_t kMaxQueryLogLength = 200;

  StructuredLog()
      .Event("mysql_query_error")
      .Field("query", query.substr(0, kMaxQueryLogLength))
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
  // Maximum query length to log (prevent log spam)
  constexpr size_t kMaxQueryLogLength = 200;

  StructuredLog()
      .Event("query_parse_error")
      .Field("query", query.substr(0, kMaxQueryLogLength))
      .Field("error", error_msg)
      .Field("position", static_cast<int64_t>(error_position))
      .Error();
}

}  // namespace mygram::utils
