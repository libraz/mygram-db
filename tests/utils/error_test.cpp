/**
 * @file error_test.cpp
 * @brief Unit tests for Error class and error codes
 */

#include "utils/error.h"

#include <gtest/gtest.h>

using namespace mygram::utils;

// ========== Test ErrorCode enum ==========

TEST(ErrorCodeTest, ErrorCodeValues) {
  EXPECT_EQ(static_cast<int>(ErrorCode::kSuccess), 0);
  EXPECT_EQ(static_cast<int>(ErrorCode::kUnknown), 1);
  EXPECT_EQ(static_cast<int>(ErrorCode::kConfigFileNotFound), 1000);
  EXPECT_EQ(static_cast<int>(ErrorCode::kMySQLConnectionFailed), 2000);
  EXPECT_EQ(static_cast<int>(ErrorCode::kQuerySyntaxError), 3000);
  EXPECT_EQ(static_cast<int>(ErrorCode::kIndexNotFound), 4000);
  EXPECT_EQ(static_cast<int>(ErrorCode::kStorageFileNotFound), 5000);
  EXPECT_EQ(static_cast<int>(ErrorCode::kNetworkBindFailed), 6000);
  EXPECT_EQ(static_cast<int>(ErrorCode::kClientNotConnected), 7000);
  EXPECT_EQ(static_cast<int>(ErrorCode::kCacheMiss), 8000);
}

TEST(ErrorCodeTest, ErrorCodeToString) {
  EXPECT_STREQ(ErrorCodeToString(ErrorCode::kSuccess), "Success");
  EXPECT_STREQ(ErrorCodeToString(ErrorCode::kUnknown), "Unknown error");
  EXPECT_STREQ(ErrorCodeToString(ErrorCode::kInvalidArgument), "Invalid argument");
  EXPECT_STREQ(ErrorCodeToString(ErrorCode::kConfigFileNotFound), "Configuration file not found");
  EXPECT_STREQ(ErrorCodeToString(ErrorCode::kMySQLConnectionFailed), "MySQL connection failed");
  EXPECT_STREQ(ErrorCodeToString(ErrorCode::kQuerySyntaxError), "Query syntax error");
  EXPECT_STREQ(ErrorCodeToString(ErrorCode::kIndexNotFound), "Index not found");
  EXPECT_STREQ(ErrorCodeToString(ErrorCode::kStorageFileNotFound), "Storage file not found");
  EXPECT_STREQ(ErrorCodeToString(ErrorCode::kNetworkBindFailed), "Bind failed");
  EXPECT_STREQ(ErrorCodeToString(ErrorCode::kClientNotConnected), "Not connected");
  EXPECT_STREQ(ErrorCodeToString(ErrorCode::kCacheMiss), "Cache miss");
}

// ========== Test Error class ==========

TEST(ErrorTest, DefaultConstructor) {
  Error error;
  EXPECT_EQ(error.code(), ErrorCode::kSuccess);
  EXPECT_FALSE(error.is_error());
}

TEST(ErrorTest, CodeOnlyConstructor) {
  Error error(ErrorCode::kInvalidArgument);
  EXPECT_EQ(error.code(), ErrorCode::kInvalidArgument);
  EXPECT_EQ(error.message(), "Invalid argument");
  EXPECT_TRUE(error.context().empty());
  EXPECT_TRUE(error.is_error());
}

TEST(ErrorTest, CodeAndMessageConstructor) {
  Error error(ErrorCode::kNotFound, "User not found");
  EXPECT_EQ(error.code(), ErrorCode::kNotFound);
  EXPECT_EQ(error.message(), "User not found");
  EXPECT_TRUE(error.context().empty());
  EXPECT_TRUE(error.is_error());
}

TEST(ErrorTest, FullConstructor) {
  Error error(ErrorCode::kTimeout, "Operation timed out", "query.cpp:42");
  EXPECT_EQ(error.code(), ErrorCode::kTimeout);
  EXPECT_EQ(error.message(), "Operation timed out");
  EXPECT_EQ(error.context(), "query.cpp:42");
  EXPECT_TRUE(error.is_error());
}

TEST(ErrorTest, ToString) {
  Error error1(ErrorCode::kInvalidArgument);
  // When constructed with code only, message is set to ErrorCodeToString(code)
  EXPECT_EQ(error1.to_string(), "[Invalid argument (2)] Invalid argument");

  Error error2(ErrorCode::kNotFound, "User not found");
  EXPECT_EQ(error2.to_string(), "[Not found (8)] User not found");

  Error error3(ErrorCode::kTimeout, "Operation timed out", "query.cpp:42");
  EXPECT_EQ(error3.to_string(), "[Timeout (10)] Operation timed out (context: query.cpp:42)");
}

TEST(ErrorTest, StringConversion) {
  Error error(ErrorCode::kInvalidArgument, "Invalid input");
  std::string str = error;
  EXPECT_EQ(str, "[Invalid argument (2)] Invalid input");
}

TEST(ErrorTest, WhatMethod) {
  Error error(ErrorCode::kNotFound, "Resource not found");
  EXPECT_STREQ(error.what(), "Resource not found");
}

// ========== Test helper functions ==========

TEST(ErrorTest, MakeErrorCodeOnly) {
  auto error = MakeError(ErrorCode::kInternalError);
  EXPECT_EQ(error.code(), ErrorCode::kInternalError);
  EXPECT_EQ(error.message(), "Internal error");
}

TEST(ErrorTest, MakeErrorWithMessage) {
  auto error = MakeError(ErrorCode::kIOError, "Failed to read file");
  EXPECT_EQ(error.code(), ErrorCode::kIOError);
  EXPECT_EQ(error.message(), "Failed to read file");
}

TEST(ErrorTest, MakeErrorWithContext) {
  auto error = MakeError(ErrorCode::kPermissionDenied, "Access denied", "/etc/passwd");
  EXPECT_EQ(error.code(), ErrorCode::kPermissionDenied);
  EXPECT_EQ(error.message(), "Access denied");
  EXPECT_EQ(error.context(), "/etc/passwd");
}

// ========== Test MYGRAM_ERROR macro ==========

TEST(ErrorTest, MygramErrorMacro) {
  auto error = MYGRAM_ERROR(ErrorCode::kUnknown, "Something went wrong");
  EXPECT_EQ(error.code(), ErrorCode::kUnknown);
  EXPECT_EQ(error.message(), "Something went wrong");
  // Context should contain file:line information
  EXPECT_FALSE(error.context().empty());
  EXPECT_NE(error.context().find("error_test.cpp"), std::string::npos);
}

// ========== Test module-specific error codes ==========

TEST(ErrorTest, ConfigErrorCodes) {
  Error file_not_found(ErrorCode::kConfigFileNotFound);
  Error parse_error(ErrorCode::kConfigParseError);
  Error validation_error(ErrorCode::kConfigValidationError);

  EXPECT_EQ(file_not_found.message(), "Configuration file not found");
  EXPECT_EQ(parse_error.message(), "Configuration parse error");
  EXPECT_EQ(validation_error.message(), "Configuration validation error");
}

TEST(ErrorTest, MySQLErrorCodes) {
  Error connection_failed(ErrorCode::kMySQLConnectionFailed);
  Error query_failed(ErrorCode::kMySQLQueryFailed);
  Error invalid_gtid(ErrorCode::kMySQLInvalidGTID);
  Error replication_error(ErrorCode::kMySQLReplicationError);

  EXPECT_EQ(connection_failed.message(), "MySQL connection failed");
  EXPECT_EQ(query_failed.message(), "MySQL query failed");
  EXPECT_EQ(invalid_gtid.message(), "Invalid GTID");
  EXPECT_EQ(replication_error.message(), "Replication error");
}

TEST(ErrorTest, QueryErrorCodes) {
  Error syntax_error(ErrorCode::kQuerySyntaxError);
  Error invalid_token(ErrorCode::kQueryInvalidToken);
  Error missing_operand(ErrorCode::kQueryMissingOperand);
  Error too_long(ErrorCode::kQueryTooLong);

  EXPECT_EQ(syntax_error.message(), "Query syntax error");
  EXPECT_EQ(invalid_token.message(), "Invalid token");
  EXPECT_EQ(missing_operand.message(), "Missing operand");
  EXPECT_EQ(too_long.message(), "Query too long");
}

TEST(ErrorTest, IndexErrorCodes) {
  Error not_found(ErrorCode::kIndexNotFound);
  Error corrupted(ErrorCode::kIndexCorrupted);
  Error serialization_failed(ErrorCode::kIndexSerializationFailed);

  EXPECT_EQ(not_found.message(), "Index not found");
  EXPECT_EQ(corrupted.message(), "Index corrupted");
  EXPECT_EQ(serialization_failed.message(), "Index serialization failed");
}

TEST(ErrorTest, StorageErrorCodes) {
  Error file_not_found(ErrorCode::kStorageFileNotFound);
  Error read_error(ErrorCode::kStorageReadError);
  Error crc_mismatch(ErrorCode::kStorageCRCMismatch);
  Error version_mismatch(ErrorCode::kStorageVersionMismatch);

  EXPECT_EQ(file_not_found.message(), "Storage file not found");
  EXPECT_EQ(read_error.message(), "Storage read error");
  EXPECT_EQ(crc_mismatch.message(), "CRC mismatch");
  EXPECT_EQ(version_mismatch.message(), "Version mismatch");
}

TEST(ErrorTest, NetworkErrorCodes) {
  Error bind_failed(ErrorCode::kNetworkBindFailed);
  Error connection_refused(ErrorCode::kNetworkConnectionRefused);
  Error protocol_error(ErrorCode::kNetworkProtocolError);
  Error ip_not_allowed(ErrorCode::kNetworkIPNotAllowed);

  EXPECT_EQ(bind_failed.message(), "Bind failed");
  EXPECT_EQ(connection_refused.message(), "Connection refused");
  EXPECT_EQ(protocol_error.message(), "Protocol error");
  EXPECT_EQ(ip_not_allowed.message(), "IP not allowed");
}

TEST(ErrorTest, ClientErrorCodes) {
  Error not_connected(ErrorCode::kClientNotConnected);
  Error connection_failed(ErrorCode::kClientConnectionFailed);
  Error invalid_response(ErrorCode::kClientInvalidResponse);
  Error timeout(ErrorCode::kClientTimeout);

  EXPECT_EQ(not_connected.message(), "Not connected");
  EXPECT_EQ(connection_failed.message(), "Connection failed");
  EXPECT_EQ(invalid_response.message(), "Invalid response");
  EXPECT_EQ(timeout.message(), "Timeout");
}

TEST(ErrorTest, CacheErrorCodes) {
  Error cache_miss(ErrorCode::kCacheMiss);
  Error cache_disabled(ErrorCode::kCacheDisabled);
  Error compression_failed(ErrorCode::kCacheCompressionFailed);

  EXPECT_EQ(cache_miss.message(), "Cache miss");
  EXPECT_EQ(cache_disabled.message(), "Cache disabled");
  EXPECT_EQ(compression_failed.message(), "Cache compression failed");
}

// ========== Test error propagation patterns ==========

TEST(ErrorTest, ErrorPropagation) {
  // Simulating a chain of operations
  auto error1 = MakeError(ErrorCode::kStorageReadError, "Failed to read block");
  auto error2 = MakeError(error1.code(), error1.message(), "snapshot.dat:1024");

  EXPECT_EQ(error2.code(), ErrorCode::kStorageReadError);
  EXPECT_EQ(error2.message(), "Failed to read block");
  EXPECT_EQ(error2.context(), "snapshot.dat:1024");
}
