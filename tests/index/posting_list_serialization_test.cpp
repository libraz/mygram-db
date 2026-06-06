#include <gtest/gtest.h>

#include "index/posting_list.h"

using namespace mygramdb::index;

TEST(PostingListSerializationTest, RoundTripDeltaCompressed) {
  PostingList pl;
  pl.Add(10);
  pl.Add(20);
  pl.Add(30);

  std::vector<uint8_t> buffer;
  ASSERT_TRUE(pl.Serialize(buffer));

  PostingList pl2;
  size_t offset = 0;
  ASSERT_TRUE(pl2.Deserialize(buffer, offset));
  EXPECT_EQ(offset, buffer.size());

  EXPECT_TRUE(pl2.Contains(10));
  EXPECT_TRUE(pl2.Contains(20));
  EXPECT_TRUE(pl2.Contains(30));
  EXPECT_FALSE(pl2.Contains(15));
  EXPECT_EQ(pl2.Size(), 3);
}

TEST(PostingListSerializationTest, LittleEndianByteOrder) {
  PostingList pl;
  pl.Add(1);
  pl.Add(2);

  std::vector<uint8_t> buffer;
  ASSERT_TRUE(pl.Serialize(buffer));

  // byte 0: strategy (kDeltaCompressed = 0)
  EXPECT_EQ(buffer[0], 0);

  // bytes 1-4: size = 2 in little-endian
  EXPECT_EQ(buffer[1], 2);  // LSB
  EXPECT_EQ(buffer[2], 0);
  EXPECT_EQ(buffer[3], 0);
  EXPECT_EQ(buffer[4], 0);  // MSB
}

TEST(PostingListSerializationTest, RoundTripEmpty) {
  PostingList pl;
  std::vector<uint8_t> buffer;
  ASSERT_TRUE(pl.Serialize(buffer));

  PostingList pl2;
  size_t offset = 0;
  ASSERT_TRUE(pl2.Deserialize(buffer, offset));
  EXPECT_EQ(pl2.Size(), 0);
}

TEST(PostingListSerializationTest, RoundTripLargeList) {
  PostingList pl(100);  // low threshold to trigger roaring
  for (uint32_t i = 0; i < 200; i++) {
    pl.Add(i * 3);
  }

  std::vector<uint8_t> buffer;
  ASSERT_TRUE(pl.Serialize(buffer));

  PostingList pl2(100);
  size_t offset = 0;
  ASSERT_TRUE(pl2.Deserialize(buffer, offset));
  EXPECT_EQ(pl2.Size(), 200);

  for (uint32_t i = 0; i < 200; i++) {
    EXPECT_TRUE(pl2.Contains(i * 3));
  }
}

TEST(PostingListSerializationTest, RejectsInternallyInvalidRoaringBitmap) {
  PostingList pl(0.01);
  for (uint32_t i = 0; i < 200; i++) {
    pl.Add(i * 3);
  }
  pl.Optimize(600);

  std::vector<uint8_t> buffer;
  ASSERT_TRUE(pl.Serialize(buffer));
  ASSERT_EQ(buffer[0], static_cast<uint8_t>(PostingStrategy::kRoaringBitmap));

  constexpr size_t kHeaderSize = 5;
  ASSERT_GT(buffer.size(), kHeaderSize + 16);
  buffer[kHeaderSize + 16] = 3;

  PostingList deserialized(0.01);
  size_t offset = 0;
  EXPECT_FALSE(deserialized.Deserialize(buffer, offset));
  EXPECT_EQ(deserialized.Size(), 0u);
}
