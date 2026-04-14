/**
 * @file null_binlog_reader_test.cpp
 * @brief Unit tests for NullBinlogReader
 *
 * Verifies that NullBinlogReader implements IBinlogReader interface
 * and all methods return expected safe defaults.
 */

#include "mysql/null_binlog_reader.h"

#include <gtest/gtest.h>

namespace mygramdb::mysql {

/**
 * @brief Test that NullBinlogReader can be created and destroyed
 */
TEST(NullBinlogReaderTest, ConstructionAndDestruction) {
  NullBinlogReader reader;
  // Should construct and destruct without issues
}

/**
 * @brief Test that NullBinlogReader implements IBinlogReader interface
 */
TEST(NullBinlogReaderTest, ImplementsIBinlogReaderInterface) {
  NullBinlogReader reader;

  // Should be usable through the interface pointer
  IBinlogReader* interface_ptr = &reader;
  EXPECT_FALSE(interface_ptr->IsRunning());
}

/**
 * @brief Test IsRunning returns false
 */
TEST(NullBinlogReaderTest, IsRunningReturnsFalse) {
  NullBinlogReader reader;
  EXPECT_FALSE(reader.IsRunning());
}

/**
 * @brief Test GetCurrentGTID returns empty string
 */
TEST(NullBinlogReaderTest, GetCurrentGTIDReturnsEmpty) {
  NullBinlogReader reader;
  EXPECT_EQ(reader.GetCurrentGTID(), "");
}

/**
 * @brief Test GetLastError returns a meaningful message
 */
TEST(NullBinlogReaderTest, GetLastErrorReturnsMessage) {
  NullBinlogReader reader;
  std::string error = reader.GetLastError();
  EXPECT_FALSE(error.empty());
  EXPECT_TRUE(error.find("MySQL") != std::string::npos || error.find("not compiled") != std::string::npos);
}

/**
 * @brief Test GetProcessedEvents returns 0
 */
TEST(NullBinlogReaderTest, GetProcessedEventsReturnsZero) {
  NullBinlogReader reader;
  EXPECT_EQ(reader.GetProcessedEvents(), 0);
}

/**
 * @brief Test GetQueueSize returns 0
 */
TEST(NullBinlogReaderTest, GetQueueSizeReturnsZero) {
  NullBinlogReader reader;
  EXPECT_EQ(reader.GetQueueSize(), 0);
}

/**
 * @brief Test Start returns error (MySQL not compiled)
 */
TEST(NullBinlogReaderTest, StartReturnsError) {
  NullBinlogReader reader;
  auto result = reader.Start();
  EXPECT_FALSE(result);
  EXPECT_FALSE(result.error().message().empty());
}

/**
 * @brief Test Stop does not crash
 */
TEST(NullBinlogReaderTest, StopDoesNotCrash) {
  NullBinlogReader reader;
  reader.Stop();
  // If we reach here, Stop() did not crash
}

/**
 * @brief Test SetCurrentGTID does not crash
 */
TEST(NullBinlogReaderTest, SetCurrentGTIDDoesNotCrash) {
  NullBinlogReader reader;
  reader.SetCurrentGTID("some-gtid-value");
  // SetCurrentGTID is a no-op, but should not crash
  EXPECT_EQ(reader.GetCurrentGTID(), "");
}

/**
 * @brief Test multiple calls to methods are safe
 */
TEST(NullBinlogReaderTest, MultipleCalls) {
  NullBinlogReader reader;

  for (int i = 0; i < 10; ++i) {
    EXPECT_FALSE(reader.IsRunning());
    EXPECT_EQ(reader.GetCurrentGTID(), "");
    EXPECT_EQ(reader.GetProcessedEvents(), 0);
    EXPECT_EQ(reader.GetQueueSize(), 0);
    reader.Stop();
    reader.SetCurrentGTID("gtid-" + std::to_string(i));
  }
}

}  // namespace mygramdb::mysql
