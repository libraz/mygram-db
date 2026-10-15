/**
 * @file binlog_stream_unit_test.cpp
 * @brief Unit tests for the binlog stream helpers that need no server.
 */

#include <gtest/gtest.h>

#include <ios>
#include <string>
#include <vector>

#include "mysql/binlog_stream.h"
#include "mysql/mariadb_binlog_stream.h"

#ifdef USE_MYSQL

namespace mygramdb::mysql {
namespace {

/**
 * @brief Positions a MariaDB source can legitimately be asked to resume from.
 */
TEST(MariaDBGtidPositionTest, AcceptsThePositionsMariaDBItselfProduces) {
  // No executed GTIDs: the documented way to ask for the server's live position.
  EXPECT_TRUE(IsSafeMariaDBGtidPosition(""));

  EXPECT_TRUE(IsSafeMariaDBGtidPosition("0-1-100"));
  EXPECT_TRUE(IsSafeMariaDBGtidPosition("0-1-0"));
  // Multiple domains, which is how a multi-source position is expressed.
  EXPECT_TRUE(IsSafeMariaDBGtidPosition("0-1-100,1-2-200,2-3-300"));
  // Sequence numbers reach 64 bits.
  EXPECT_TRUE(IsSafeMariaDBGtidPosition("4294967295-4294967295-18446744073709551615"));
}

/**
 * @brief Anything that could end the quoted literal is refused.
 *
 * The position is interpolated into `SET @slave_connect_state = '<gtid>'`, and
 * it arrives from a dump file, a SYNC or an operator command rather than from
 * the server. Each character below is one that would let a crafted position
 * close the literal and continue the statement.
 */
TEST(MariaDBGtidPositionTest, RefusesEveryCharacterThatCouldEndTheLiteral) {
  const std::vector<std::string> rejected = {
      "0-1-1'",                           // closes the literal directly
      "0-1-1' OR '1'='1",                 // closes it and continues the expression
      "0-1-1'; DROP TABLE articles; --",  // closes it and starts a new statement
      "0-1-1\\",                          // escapes the closing quote
      "0-1-1\"",                          // the other quote character
      "0-1-1`",                           // identifier quoting
      "0-1-1;",                           // statement separator
      "0-1-1 ",                           // trailing space
      " 0-1-1",                           // leading space
      "0-1-1\n",                          // newline
      "0-1-1\r",                          // carriage return
      "0-1-1\t",                          // tab
      "0-1-1/*comment*/",                 // comment introducer
      "0-1-1#",                           // the other comment introducer
      "0-1-1%",                           // wildcard
      "0-1-1_",                           // the single-character wildcard
      "0-1-1(",                           // call syntax
      "0-1-1)",
      "0-1-1=",
      "0-1-1+",
      "0-1-1@",  // another session variable
      "0-1-1$",
      "0-1-1:",
  };
  for (const auto& position : rejected) {
    EXPECT_FALSE(IsSafeMariaDBGtidPosition(position)) << "accepted a position containing an unsafe character";
  }

  // A NUL byte truncates the C string handed to the driver, so everything after
  // it would be dropped silently rather than rejected.
  const std::string with_nul = std::string("0-1-1\0DROP", 10);
  ASSERT_EQ(with_nul.size(), 10U);
  EXPECT_FALSE(IsSafeMariaDBGtidPosition(with_nul)) << "accepted a position containing a NUL byte";

  // High bytes are refused regardless of what the process locale would call
  // alphanumeric.
  EXPECT_FALSE(IsSafeMariaDBGtidPosition("0-1-1\xC3\xA9"));
  EXPECT_FALSE(IsSafeMariaDBGtidPosition("0-1-1\x80"));
  EXPECT_FALSE(IsSafeMariaDBGtidPosition("0-1-1\xFF"));
}

/**
 * @brief Every byte value is classified, and only the documented set passes.
 *
 * Enumerating the whole range pins the accepted set exactly, so widening it
 * later has to be a deliberate edit to this expectation rather than a side
 * effect of changing how the check is written.
 */
TEST(MariaDBGtidPositionTest, AcceptsExactlyTheDocumentedCharacterSet) {
  for (int value = 0; value <= 0xFF; ++value) {
    const auto byte = static_cast<unsigned char>(value);
    const bool expected = (byte >= '0' && byte <= '9') || (byte >= 'a' && byte <= 'z') ||
                          (byte >= 'A' && byte <= 'Z') || byte == '-' || byte == ',' || byte == '.';
    const std::string position(1, static_cast<char>(byte));
    EXPECT_EQ(IsSafeMariaDBGtidPosition(position), expected)
        << "byte 0x" << std::hex << value << " was classified against the documented set";
  }
}

/**
 * @brief A fetch error that ends the transport reconnects; only position and protocol errors stop.
 */
TEST(BinlogFetchErrorTest, TransportEndingErrorsReconnectAndOthersStop) {
  using Status = BinlogFetchResult::Status;

  EXPECT_EQ(ClassifyFetchError(2013, ""), Status::kConnectionLost);
  // Source restart, kill and network errors all leave the position intact.
  for (const unsigned int err_no : {2006U, 2055U, 1053U, 1152U, 1158U, 1159U, 1160U, 1161U, 1927U, 4031U}) {
    EXPECT_EQ(ClassifyFetchError(err_no, ""), Status::kServerGoneAway) << err_no;
  }

  // 1236 is purged only when the server says so; every other fatal dump error stops without claiming it.
  EXPECT_EQ(ClassifyFetchError(1236,
                               "Got fatal error 1236 from source when reading data from binary log: 'The replica "
                               "is connecting using CHANGE REPLICATION SOURCE TO SOURCE_AUTO_POSITION = 1, but "
                               "the source has purged binary logs containing GTIDs that the replica requires.'"),
            Status::kBinlogPurged);
  EXPECT_EQ(ClassifyFetchError(1236,
                               "Got fatal error 1236 from master when reading data from binary log: 'Could not "
                               "find GTID state requested by slave in any binlog files. Probably the slave state "
                               "is too old and required binlog files have been purged.'"),
            Status::kBinlogPurged);
  for (const char* message : {"Got fatal error 1236 from source when reading data from binary log: 'Cannot replicate "
                              "because the replica has more GTIDs than the source'",
                              "Got fatal error 1236 from source when reading data from binary log: 'log event entry "
                              "exceeded max_allowed_packet; Increase max_allowed_packet on source'",
                              "Got fatal error 1236 from source when reading data from binary log: 'binlog truncated "
                              "in the middle of event'"}) {
    EXPECT_EQ(ClassifyFetchError(1236, message), Status::kError) << message;
  }
  EXPECT_FALSE(IsPurgedPositionError(1064, "purged binary logs containing GTIDs"));
  // A malformed packet, a parse error or an access denial does not heal on reconnect.
  for (const unsigned int err_no : {2027U, 1064U, 1045U}) {
    EXPECT_EQ(ClassifyFetchError(err_no, ""), Status::kError) << err_no;
  }
}

}  // namespace
}  // namespace mygramdb::mysql

#endif  // USE_MYSQL
