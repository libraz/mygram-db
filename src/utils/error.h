/**
 * @file error.h
 * @brief Common error types and error codes for MygramDB
 *
 * This file defines a unified error handling system using error codes and
 * structured error types. All modules should use these error types with
 * Expected<T, E> for consistent error handling.
 */

#pragma once

#include <cstdint>
#include <sstream>
#include <string>
#include <utility>

namespace mygram::utils {

/**
 * @brief Error codes for all MygramDB modules
 *
 * Error codes are organized by module using ranges:
 * - 0-999: General errors
 * - 1000-1999: Configuration errors
 * - 2000-2999: MySQL/Database errors
 * - 3000-3999: Query parsing errors
 * - 4000-4999: Index/Search errors
 * - 5000-5999: Storage/Snapshot errors
 * - 6000-6999: Network/Server errors
 * - 7000-7999: Client errors
 * - 8000-8999: Cache errors
 */
enum class ErrorCode : std::uint16_t {
  // ===== General Errors (0-999) =====
  kSuccess = 0,           ///< Operation succeeded (not an error)
  kUnknown = 1,           ///< Unknown error
  kInvalidArgument = 2,   ///< Invalid argument provided
  kOutOfRange = 3,        ///< Value out of range
  kNotImplemented = 4,    ///< Feature not implemented
  kInternalError = 5,     ///< Internal error
  kIOError = 6,           ///< I/O error (file read/write)
  kPermissionDenied = 7,  ///< Permission denied
  kNotFound = 8,          ///< Resource not found
  kAlreadyExists = 9,     ///< Resource already exists
  kTimeout = 10,          ///< Operation timed out
  kCancelled = 11,        ///< Operation cancelled

  // ===== Configuration Errors (1000-1999) =====
  kConfigFileNotFound = 1000,     ///< Configuration file not found
  kConfigParseError = 1001,       ///< Failed to parse configuration file
  kConfigValidationError = 1002,  ///< Configuration validation failed
  kConfigMissingRequired = 1003,  ///< Required configuration field missing
  kConfigInvalidValue = 1004,     ///< Invalid configuration value
  kConfigSchemaError = 1005,      ///< JSON schema validation error
  kConfigYamlError = 1006,        ///< YAML parsing error
  kConfigJsonError = 1007,        ///< JSON parsing error

  // ===== MySQL/Database Errors (2000-2999) =====
  kMySQLConnectionFailed = 2000,  ///< Failed to connect to MySQL
  kMySQLQueryFailed = 2001,       ///< MySQL query execution failed
  kMySQLDisconnected = 2002,      ///< MySQL connection lost
  kMySQLAuthFailed = 2003,        ///< MySQL authentication failed
  kMySQLTimeout = 2004,           ///< MySQL operation timed out
  kMySQLInvalidGTID = 2005,       ///< Invalid GTID format
  kMySQLGTIDNotEnabled = 2006,    ///< GTID mode not enabled
  kMySQLReplicationError = 2007,  ///< Replication error
  kMySQLBinlogError = 2008,       ///< Binlog reading error
  kMySQLTableNotFound = 2009,     ///< Table not found
  kMySQLColumnNotFound = 2010,    ///< Column not found
  kMySQLDuplicateColumn = 2011,   ///< Duplicate column in unique constraint
  kMySQLInvalidSchema = 2012,     ///< Invalid schema/table structure

  // ===== Query Parsing Errors (3000-3999) =====
  kQuerySyntaxError = 3000,           ///< Query syntax error
  kQueryInvalidToken = 3001,          ///< Invalid token in query
  kQueryUnexpectedToken = 3002,       ///< Unexpected token in query
  kQueryMissingOperand = 3003,        ///< Missing operand in expression
  kQueryInvalidOperator = 3004,       ///< Invalid operator
  kQueryTooLong = 3005,               ///< Query exceeds maximum length
  kQueryInvalidFilter = 3006,         ///< Invalid filter specification
  kQueryInvalidSort = 3007,           ///< Invalid sort specification
  kQueryInvalidLimit = 3008,          ///< Invalid limit value
  kQueryInvalidOffset = 3009,         ///< Invalid offset value
  kQueryExpressionParseError = 3010,  ///< Search expression parsing failed
  kQueryASTBuildError = 3011,         ///< Failed to build query AST

  // ===== Index/Search Errors (4000-4999) =====
  kIndexNotFound = 4000,               ///< Index not found
  kIndexCorrupted = 4001,              ///< Index data corrupted
  kIndexSerializationFailed = 4002,    ///< Index serialization failed
  kIndexDeserializationFailed = 4003,  ///< Index deserialization failed
  kIndexDocumentNotFound = 4004,       ///< Document not found in index
  kIndexInvalidDocID = 4005,           ///< Invalid document ID
  kIndexFull = 4006,                   ///< Index capacity exceeded

  // ===== Storage/Snapshot Errors (5000-5999) =====
  kStorageFileNotFound = 5000,         ///< Snapshot file not found
  kStorageReadError = 5001,            ///< Failed to read from storage
  kStorageWriteError = 5002,           ///< Failed to write to storage
  kStorageCorrupted = 5003,            ///< Storage data corrupted
  kStorageCRCMismatch = 5004,          ///< CRC checksum mismatch
  kStorageVersionMismatch = 5005,      ///< Storage format version mismatch
  kStorageCompressionFailed = 5006,    ///< Compression failed
  kStorageDecompressionFailed = 5007,  ///< Decompression failed
  kStorageInvalidFormat = 5008,        ///< Invalid storage format
  kStorageSnapshotBuildFailed = 5009,  ///< Snapshot build failed
  kStorageDocIdExhausted = 5010,       ///< DocID space exhausted (uint32_t overflow)
  kStorageDumpReadError = 5011,        ///< Failed to read from dump file
  kStorageDumpWriteError = 5012,       ///< Failed to write to dump file

  // ===== Network/Server Errors (6000-6999) =====
  kNetworkBindFailed = 6000,            ///< Failed to bind to port
  kNetworkListenFailed = 6001,          ///< Failed to listen on socket
  kNetworkAcceptFailed = 6002,          ///< Failed to accept connection
  kNetworkConnectionRefused = 6003,     ///< Connection refused
  kNetworkConnectionClosed = 6004,      ///< Connection closed by peer
  kNetworkSendFailed = 6005,            ///< Failed to send data
  kNetworkReceiveFailed = 6006,         ///< Failed to receive data
  kNetworkInvalidRequest = 6007,        ///< Invalid request received
  kNetworkProtocolError = 6008,         ///< Protocol error
  kNetworkIPNotAllowed = 6009,          ///< IP address not in allowed CIDRs
  kNetworkServerNotStarted = 6010,      ///< Server not started
  kNetworkAlreadyRunning = 6011,        ///< Server already running
  kNetworkSocketCreationFailed = 6012,  ///< Failed to create socket
  kNetworkInvalidBindAddress = 6013,    ///< Invalid bind address

  // ===== Client Errors (7000-7999) =====
  kClientNotConnected = 7000,      ///< Client not connected
  kClientConnectionFailed = 7001,  ///< Failed to connect to server
  kClientSendFailed = 7002,        ///< Failed to send request
  kClientReceiveFailed = 7003,     ///< Failed to receive response
  kClientInvalidResponse = 7004,   ///< Invalid response from server
  kClientTimeout = 7005,           ///< Client operation timed out
  kClientAlreadyConnected = 7006,  ///< Already connected
  kClientCommandFailed = 7007,     ///< Command execution failed
  kClientConnectionClosed = 7008,  ///< Connection closed by server
  kClientInvalidArgument = 7009,   ///< Invalid argument provided
  kClientServerError = 7010,       ///< Server returned an error
  kClientProtocolError = 7011,     ///< Protocol error or unexpected response format

  // ===== Cache Errors (8000-8999) =====
  kCacheMiss = 8000,                 ///< Cache miss (not an error, but informational)
  kCacheDisabled = 8001,             ///< Cache is disabled
  kCacheCompressionFailed = 8002,    ///< Cache compression failed
  kCacheDecompressionFailed = 8003,  ///< Cache decompression failed
};

/**
 * @brief Convert error code to string representation
 */
inline const char* ErrorCodeToString(ErrorCode code) {
  switch (code) {
    // General
    case ErrorCode::kSuccess:
      return "Success";
    case ErrorCode::kUnknown:
      return "Unknown error";
    case ErrorCode::kInvalidArgument:
      return "Invalid argument";
    case ErrorCode::kOutOfRange:
      return "Out of range";
    case ErrorCode::kNotImplemented:
      return "Not implemented";
    case ErrorCode::kInternalError:
      return "Internal error";
    case ErrorCode::kIOError:
      return "I/O error";
    case ErrorCode::kPermissionDenied:
      return "Permission denied";
    case ErrorCode::kNotFound:
      return "Not found";
    case ErrorCode::kAlreadyExists:
      return "Already exists";
    case ErrorCode::kTimeout:
      return "Timeout";
    case ErrorCode::kCancelled:
      return "Cancelled";

    // Configuration
    case ErrorCode::kConfigFileNotFound:
      return "Configuration file not found";
    case ErrorCode::kConfigParseError:
      return "Configuration parse error";
    case ErrorCode::kConfigValidationError:
      return "Configuration validation error";
    case ErrorCode::kConfigMissingRequired:
      return "Missing required configuration";
    case ErrorCode::kConfigInvalidValue:
      return "Invalid configuration value";
    case ErrorCode::kConfigSchemaError:
      return "JSON schema error";
    case ErrorCode::kConfigYamlError:
      return "YAML parsing error";
    case ErrorCode::kConfigJsonError:
      return "JSON parsing error";

    // MySQL
    case ErrorCode::kMySQLConnectionFailed:
      return "MySQL connection failed";
    case ErrorCode::kMySQLQueryFailed:
      return "MySQL query failed";
    case ErrorCode::kMySQLDisconnected:
      return "MySQL disconnected";
    case ErrorCode::kMySQLAuthFailed:
      return "MySQL authentication failed";
    case ErrorCode::kMySQLTimeout:
      return "MySQL timeout";
    case ErrorCode::kMySQLInvalidGTID:
      return "Invalid GTID";
    case ErrorCode::kMySQLGTIDNotEnabled:
      return "GTID mode not enabled";
    case ErrorCode::kMySQLReplicationError:
      return "Replication error";
    case ErrorCode::kMySQLBinlogError:
      return "Binlog error";
    case ErrorCode::kMySQLTableNotFound:
      return "Table not found";
    case ErrorCode::kMySQLColumnNotFound:
      return "Column not found";
    case ErrorCode::kMySQLDuplicateColumn:
      return "Duplicate column";
    case ErrorCode::kMySQLInvalidSchema:
      return "Invalid schema";

    // Query
    case ErrorCode::kQuerySyntaxError:
      return "Query syntax error";
    case ErrorCode::kQueryInvalidToken:
      return "Invalid token";
    case ErrorCode::kQueryUnexpectedToken:
      return "Unexpected token";
    case ErrorCode::kQueryMissingOperand:
      return "Missing operand";
    case ErrorCode::kQueryInvalidOperator:
      return "Invalid operator";
    case ErrorCode::kQueryTooLong:
      return "Query too long";
    case ErrorCode::kQueryInvalidFilter:
      return "Invalid filter";
    case ErrorCode::kQueryInvalidSort:
      return "Invalid sort";
    case ErrorCode::kQueryInvalidLimit:
      return "Invalid limit";
    case ErrorCode::kQueryInvalidOffset:
      return "Invalid offset";
    case ErrorCode::kQueryExpressionParseError:
      return "Expression parse error";
    case ErrorCode::kQueryASTBuildError:
      return "AST build error";

    // Index
    case ErrorCode::kIndexNotFound:
      return "Index not found";
    case ErrorCode::kIndexCorrupted:
      return "Index corrupted";
    case ErrorCode::kIndexSerializationFailed:
      return "Index serialization failed";
    case ErrorCode::kIndexDeserializationFailed:
      return "Index deserialization failed";
    case ErrorCode::kIndexDocumentNotFound:
      return "Document not found";
    case ErrorCode::kIndexInvalidDocID:
      return "Invalid document ID";
    case ErrorCode::kIndexFull:
      return "Index full";

    // Storage
    case ErrorCode::kStorageFileNotFound:
      return "Storage file not found";
    case ErrorCode::kStorageReadError:
      return "Storage read error";
    case ErrorCode::kStorageWriteError:
      return "Storage write error";
    case ErrorCode::kStorageCorrupted:
      return "Storage corrupted";
    case ErrorCode::kStorageCRCMismatch:
      return "CRC mismatch";
    case ErrorCode::kStorageVersionMismatch:
      return "Version mismatch";
    case ErrorCode::kStorageCompressionFailed:
      return "Compression failed";
    case ErrorCode::kStorageDecompressionFailed:
      return "Decompression failed";
    case ErrorCode::kStorageInvalidFormat:
      return "Invalid format";
    case ErrorCode::kStorageSnapshotBuildFailed:
      return "Snapshot build failed";
    case ErrorCode::kStorageDocIdExhausted:
      return "DocID exhausted";

    // Network
    case ErrorCode::kNetworkBindFailed:
      return "Bind failed";
    case ErrorCode::kNetworkListenFailed:
      return "Listen failed";
    case ErrorCode::kNetworkAcceptFailed:
      return "Accept failed";
    case ErrorCode::kNetworkConnectionRefused:
      return "Connection refused";
    case ErrorCode::kNetworkConnectionClosed:
      return "Connection closed";
    case ErrorCode::kNetworkSendFailed:
      return "Send failed";
    case ErrorCode::kNetworkReceiveFailed:
      return "Receive failed";
    case ErrorCode::kNetworkInvalidRequest:
      return "Invalid request";
    case ErrorCode::kNetworkProtocolError:
      return "Protocol error";
    case ErrorCode::kNetworkIPNotAllowed:
      return "IP not allowed";
    case ErrorCode::kNetworkServerNotStarted:
      return "Server not started";
    case ErrorCode::kNetworkAlreadyRunning:
      return "Server already running";
    case ErrorCode::kNetworkSocketCreationFailed:
      return "Socket creation failed";
    case ErrorCode::kNetworkInvalidBindAddress:
      return "Invalid bind address";

    // Client
    case ErrorCode::kClientNotConnected:
      return "Not connected";
    case ErrorCode::kClientConnectionFailed:
      return "Connection failed";
    case ErrorCode::kClientSendFailed:
      return "Send failed";
    case ErrorCode::kClientReceiveFailed:
      return "Receive failed";
    case ErrorCode::kClientInvalidResponse:
      return "Invalid response";
    case ErrorCode::kClientTimeout:
      return "Timeout";
    case ErrorCode::kClientAlreadyConnected:
      return "Already connected";
    case ErrorCode::kClientCommandFailed:
      return "Command failed";
    case ErrorCode::kClientConnectionClosed:
      return "Connection closed";
    case ErrorCode::kClientInvalidArgument:
      return "Invalid argument";
    case ErrorCode::kClientServerError:
      return "Server error";
    case ErrorCode::kClientProtocolError:
      return "Protocol error";

    // Cache
    case ErrorCode::kCacheMiss:
      return "Cache miss";
    case ErrorCode::kCacheDisabled:
      return "Cache disabled";
    case ErrorCode::kCacheCompressionFailed:
      return "Cache compression failed";
    case ErrorCode::kCacheDecompressionFailed:
      return "Cache decompression failed";

    default:
      return "Unknown error code";
  }
}

/**
 * @brief Base error class for all MygramDB errors
 *
 * This class provides a structured error with error code, message, and optional context.
 * It can be used with Expected<T, Error> for type-safe error handling.
 */
class Error {
 public:
  /**
   * @brief Default constructor (success/no error)
   */
  Error() : code_(ErrorCode::kSuccess) {}

  /**
   * @brief Construct with error code only
   */
  explicit Error(ErrorCode code) : code_(code), message_(ErrorCodeToString(code)) {}

  /**
   * @brief Construct with error code and custom message
   */
  Error(ErrorCode code, std::string message) : code_(code), message_(std::move(message)) {}

  /**
   * @brief Construct with error code, message, and context
   */
  Error(ErrorCode code, std::string message, std::string context)
      : code_(code), message_(std::move(message)), context_(std::move(context)) {}

  /**
   * @brief Get the error code
   */
  [[nodiscard]] ErrorCode code() const { return code_; }

  /**
   * @brief Get the error message
   */
  [[nodiscard]] const std::string& message() const { return message_; }

  /**
   * @brief Get the error context (optional additional information)
   */
  [[nodiscard]] const std::string& context() const { return context_; }

  /**
   * @brief Check if this represents an error (not success)
   */
  [[nodiscard]] bool is_error() const { return code_ != ErrorCode::kSuccess; }

  /**
   * @brief Get formatted error string including code, message, and context
   */
  [[nodiscard]] std::string to_string() const {
    std::ostringstream oss;
    oss << "[" << ErrorCodeToString(code_) << " (" << static_cast<int>(code_) << ")]";
    if (!message_.empty()) {
      oss << " " << message_;
    }
    if (!context_.empty()) {
      oss << " (context: " << context_ << ")";
    }
    return oss.str();
  }

  /**
   * @brief Implicit conversion to string (for compatibility)
   */
  operator std::string() const { return to_string(); }

  /**
   * @brief Get message as C string (for compatibility with legacy code)
   */
  [[nodiscard]] const char* what() const noexcept { return message_.c_str(); }

 private:
  ErrorCode code_;
  std::string message_;
  std::string context_;
};

/**
 * @brief Helper function to create an Error from ErrorCode
 */
inline Error MakeError(ErrorCode code) {
  return Error(code);
}

/**
 * @brief Helper function to create an Error with custom message
 */
inline Error MakeError(ErrorCode code, const std::string& message) {
  return {code, message};
}

/**
 * @brief Helper function to create an Error with message and context
 */
inline Error MakeError(ErrorCode code, const std::string& message, const std::string& context) {
  return {code, message, context};
}

/**
 * @brief Create an error with file and line information
 *
 * This is implemented as a constexpr template function for better type safety
 * and debuggability compared to a macro.
 *
 * @param code Error code
 * @param message Error message
 * @param file Source file (automatically filled by compiler)
 * @param line Line number (automatically filled by compiler)
 * @return Error object with context information
 */
template <typename... Args>
inline Error MakeErrorWithLocation(ErrorCode code, const std::string& message, const char* file = __builtin_FILE(),
                                   int line = __builtin_LINE()) {
  return {code, message, std::string(file) + ":" + std::to_string(line)};
}

// Keep macro for backward compatibility, but prefer MakeErrorWithLocation()
// NOLINTNEXTLINE(cppcoreguidelines-macro-usage)
#define MYGRAM_ERROR(code, message) \
  mygram::utils::MakeError(code, message, std::string(__FILE__) + ":" + std::to_string(__LINE__))

}  // namespace mygram::utils
