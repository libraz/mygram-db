/**
 * @file document_store_serialization_test.cpp
 * @brief Comprehensive serialization tests for DocumentStore FilterValue types
 */

#include <gtest/gtest.h>

#include <cmath>
#include <cstdio>

#include "storage/document_store.h"

using namespace mygramdb::storage;

/**
 * @brief Test fixture for serialization tests
 */
class DocumentStoreSerializationTest : public ::testing::Test {
 protected:
  void SetUp() override { test_file_ = "/tmp/test_docstore_" + std::to_string(std::time(nullptr)); }

  void TearDown() override { std::remove((test_file_ + ".docs").c_str()); }

  std::string test_file_;
};

/**
 * @brief Test std::monostate (NULL) serialization
 */
TEST_F(DocumentStoreSerializationTest, MonostateNullValue) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters;
  filters["null_field"] = std::monostate{};

  store1.AddDocument("doc1", filters);
  ASSERT_TRUE(store1.SaveToFile(test_file_ + ".docs"));

  DocumentStore store2;
  ASSERT_TRUE(store2.LoadFromFile(test_file_ + ".docs"));

  auto doc = store2.GetDocument(1);
  ASSERT_TRUE(doc.has_value());
  ASSERT_EQ(doc->filters.size(), 1);
  EXPECT_TRUE(std::holds_alternative<std::monostate>(doc->filters["null_field"]));
}

/**
 * @brief Test bool serialization
 */
TEST_F(DocumentStoreSerializationTest, BoolValue) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters;
  filters["bool_true"] = true;
  filters["bool_false"] = false;

  store1.AddDocument("doc1", filters);
  ASSERT_TRUE(store1.SaveToFile(test_file_ + ".docs"));

  DocumentStore store2;
  ASSERT_TRUE(store2.LoadFromFile(test_file_ + ".docs"));

  auto doc = store2.GetDocument(1);
  ASSERT_TRUE(doc.has_value());
  EXPECT_TRUE(std::get<bool>(doc->filters["bool_true"]));
  EXPECT_FALSE(std::get<bool>(doc->filters["bool_false"]));
}

/**
 * @brief Test int8_t serialization
 */
TEST_F(DocumentStoreSerializationTest, Int8Value) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters;
  filters["int8_min"] = static_cast<int8_t>(-128);
  filters["int8_max"] = static_cast<int8_t>(127);
  filters["int8_zero"] = static_cast<int8_t>(0);

  store1.AddDocument("doc1", filters);
  ASSERT_TRUE(store1.SaveToFile(test_file_ + ".docs"));

  DocumentStore store2;
  ASSERT_TRUE(store2.LoadFromFile(test_file_ + ".docs"));

  auto doc = store2.GetDocument(1);
  ASSERT_TRUE(doc.has_value());
  EXPECT_EQ(std::get<int8_t>(doc->filters["int8_min"]), -128);
  EXPECT_EQ(std::get<int8_t>(doc->filters["int8_max"]), 127);
  EXPECT_EQ(std::get<int8_t>(doc->filters["int8_zero"]), 0);
}

/**
 * @brief Test uint8_t serialization
 */
TEST_F(DocumentStoreSerializationTest, UInt8Value) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters;
  filters["uint8_min"] = static_cast<uint8_t>(0);
  filters["uint8_max"] = static_cast<uint8_t>(255);

  store1.AddDocument("doc1", filters);
  ASSERT_TRUE(store1.SaveToFile(test_file_ + ".docs"));

  DocumentStore store2;
  ASSERT_TRUE(store2.LoadFromFile(test_file_ + ".docs"));

  auto doc = store2.GetDocument(1);
  ASSERT_TRUE(doc.has_value());
  EXPECT_EQ(std::get<uint8_t>(doc->filters["uint8_min"]), 0);
  EXPECT_EQ(std::get<uint8_t>(doc->filters["uint8_max"]), 255);
}

/**
 * @brief Test int16_t serialization
 */
TEST_F(DocumentStoreSerializationTest, Int16Value) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters;
  filters["int16_min"] = static_cast<int16_t>(-32768);
  filters["int16_max"] = static_cast<int16_t>(32767);

  store1.AddDocument("doc1", filters);
  ASSERT_TRUE(store1.SaveToFile(test_file_ + ".docs"));

  DocumentStore store2;
  ASSERT_TRUE(store2.LoadFromFile(test_file_ + ".docs"));

  auto doc = store2.GetDocument(1);
  ASSERT_TRUE(doc.has_value());
  EXPECT_EQ(std::get<int16_t>(doc->filters["int16_min"]), -32768);
  EXPECT_EQ(std::get<int16_t>(doc->filters["int16_max"]), 32767);
}

/**
 * @brief Test uint16_t serialization
 */
TEST_F(DocumentStoreSerializationTest, UInt16Value) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters;
  filters["uint16_min"] = static_cast<uint16_t>(0);
  filters["uint16_max"] = static_cast<uint16_t>(65535);

  store1.AddDocument("doc1", filters);
  ASSERT_TRUE(store1.SaveToFile(test_file_ + ".docs"));

  DocumentStore store2;
  ASSERT_TRUE(store2.LoadFromFile(test_file_ + ".docs"));

  auto doc = store2.GetDocument(1);
  ASSERT_TRUE(doc.has_value());
  EXPECT_EQ(std::get<uint16_t>(doc->filters["uint16_min"]), 0);
  EXPECT_EQ(std::get<uint16_t>(doc->filters["uint16_max"]), 65535);
}

/**
 * @brief Test int32_t serialization
 */
TEST_F(DocumentStoreSerializationTest, Int32Value) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters;
  filters["int32_min"] = static_cast<int32_t>(-2147483648);
  filters["int32_max"] = static_cast<int32_t>(2147483647);

  store1.AddDocument("doc1", filters);
  ASSERT_TRUE(store1.SaveToFile(test_file_ + ".docs"));

  DocumentStore store2;
  ASSERT_TRUE(store2.LoadFromFile(test_file_ + ".docs"));

  auto doc = store2.GetDocument(1);
  ASSERT_TRUE(doc.has_value());
  EXPECT_EQ(std::get<int32_t>(doc->filters["int32_min"]), -2147483648);
  EXPECT_EQ(std::get<int32_t>(doc->filters["int32_max"]), 2147483647);
}

/**
 * @brief Test uint32_t serialization
 */
TEST_F(DocumentStoreSerializationTest, UInt32Value) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters;
  filters["uint32_min"] = static_cast<uint32_t>(0);
  filters["uint32_max"] = static_cast<uint32_t>(4294967295);

  store1.AddDocument("doc1", filters);
  ASSERT_TRUE(store1.SaveToFile(test_file_ + ".docs"));

  DocumentStore store2;
  ASSERT_TRUE(store2.LoadFromFile(test_file_ + ".docs"));

  auto doc = store2.GetDocument(1);
  ASSERT_TRUE(doc.has_value());
  EXPECT_EQ(std::get<uint32_t>(doc->filters["uint32_min"]), 0);
  EXPECT_EQ(std::get<uint32_t>(doc->filters["uint32_max"]), 4294967295);
}

/**
 * @brief Test int64_t serialization
 */
TEST_F(DocumentStoreSerializationTest, Int64Value) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters;
  filters["int64_min"] = static_cast<int64_t>(-9223372036854775807LL - 1);
  filters["int64_max"] = static_cast<int64_t>(9223372036854775807LL);

  store1.AddDocument("doc1", filters);
  ASSERT_TRUE(store1.SaveToFile(test_file_ + ".docs"));

  DocumentStore store2;
  ASSERT_TRUE(store2.LoadFromFile(test_file_ + ".docs"));

  auto doc = store2.GetDocument(1);
  ASSERT_TRUE(doc.has_value());
  EXPECT_EQ(std::get<int64_t>(doc->filters["int64_min"]), -9223372036854775807LL - 1);
  EXPECT_EQ(std::get<int64_t>(doc->filters["int64_max"]), 9223372036854775807LL);
}

/**
 * @brief Test uint64_t serialization
 */
TEST_F(DocumentStoreSerializationTest, UInt64Value) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters;
  filters["uint64_min"] = static_cast<uint64_t>(0);
  filters["uint64_max"] = static_cast<uint64_t>(18446744073709551615ULL);

  store1.AddDocument("doc1", filters);
  ASSERT_TRUE(store1.SaveToFile(test_file_ + ".docs"));

  DocumentStore store2;
  ASSERT_TRUE(store2.LoadFromFile(test_file_ + ".docs"));

  auto doc = store2.GetDocument(1);
  ASSERT_TRUE(doc.has_value());
  EXPECT_EQ(std::get<uint64_t>(doc->filters["uint64_min"]), 0);
  EXPECT_EQ(std::get<uint64_t>(doc->filters["uint64_max"]), 18446744073709551615ULL);
}

/**
 * @brief Test std::string serialization
 */
TEST_F(DocumentStoreSerializationTest, StringValue) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters;
  filters["str_empty"] = std::string("");
  filters["str_simple"] = std::string("hello");
  filters["str_unicode"] = std::string("こんにちは世界");
  filters["str_long"] = std::string(1000, 'x');

  store1.AddDocument("doc1", filters);
  ASSERT_TRUE(store1.SaveToFile(test_file_ + ".docs"));

  DocumentStore store2;
  ASSERT_TRUE(store2.LoadFromFile(test_file_ + ".docs"));

  auto doc = store2.GetDocument(1);
  ASSERT_TRUE(doc.has_value());
  EXPECT_EQ(std::get<std::string>(doc->filters["str_empty"]), "");
  EXPECT_EQ(std::get<std::string>(doc->filters["str_simple"]), "hello");
  EXPECT_EQ(std::get<std::string>(doc->filters["str_unicode"]), "こんにちは世界");
  EXPECT_EQ(std::get<std::string>(doc->filters["str_long"]), std::string(1000, 'x'));
}

/**
 * @brief Test double serialization
 */
TEST_F(DocumentStoreSerializationTest, DoubleValue) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters;
  filters["double_zero"] = 0.0;
  filters["double_positive"] = 123.456;
  filters["double_negative"] = -987.654;
  filters["double_small"] = 1.23e-100;
  filters["double_large"] = 9.87e100;

  store1.AddDocument("doc1", filters);
  ASSERT_TRUE(store1.SaveToFile(test_file_ + ".docs"));

  DocumentStore store2;
  ASSERT_TRUE(store2.LoadFromFile(test_file_ + ".docs"));

  auto doc = store2.GetDocument(1);
  ASSERT_TRUE(doc.has_value());
  EXPECT_DOUBLE_EQ(std::get<double>(doc->filters["double_zero"]), 0.0);
  EXPECT_DOUBLE_EQ(std::get<double>(doc->filters["double_positive"]), 123.456);
  EXPECT_DOUBLE_EQ(std::get<double>(doc->filters["double_negative"]), -987.654);
  EXPECT_DOUBLE_EQ(std::get<double>(doc->filters["double_small"]), 1.23e-100);
  EXPECT_DOUBLE_EQ(std::get<double>(doc->filters["double_large"]), 9.87e100);
}

/**
 * @brief Test all FilterValue types in a single document
 */
TEST_F(DocumentStoreSerializationTest, AllTypesInOneDocument) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters;
  filters["null"] = std::monostate{};
  filters["bool"] = true;
  filters["int8"] = static_cast<int8_t>(-42);
  filters["uint8"] = static_cast<uint8_t>(200);
  filters["int16"] = static_cast<int16_t>(-1000);
  filters["uint16"] = static_cast<uint16_t>(50000);
  filters["int32"] = static_cast<int32_t>(-100000);
  filters["uint32"] = static_cast<uint32_t>(3000000);
  filters["int64"] = static_cast<int64_t>(-1000000000LL);
  filters["uint64"] = static_cast<uint64_t>(9000000000ULL);
  filters["string"] = std::string("test value");
  filters["double"] = 3.14159;

  store1.AddDocument("doc1", filters);
  ASSERT_TRUE(store1.SaveToFile(test_file_ + ".docs"));

  DocumentStore store2;
  ASSERT_TRUE(store2.LoadFromFile(test_file_ + ".docs"));

  auto doc = store2.GetDocument(1);
  ASSERT_TRUE(doc.has_value());
  ASSERT_EQ(doc->filters.size(), 12);

  EXPECT_TRUE(std::holds_alternative<std::monostate>(doc->filters["null"]));
  EXPECT_EQ(std::get<bool>(doc->filters["bool"]), true);
  EXPECT_EQ(std::get<int8_t>(doc->filters["int8"]), -42);
  EXPECT_EQ(std::get<uint8_t>(doc->filters["uint8"]), 200);
  EXPECT_EQ(std::get<int16_t>(doc->filters["int16"]), -1000);
  EXPECT_EQ(std::get<uint16_t>(doc->filters["uint16"]), 50000);
  EXPECT_EQ(std::get<int32_t>(doc->filters["int32"]), -100000);
  EXPECT_EQ(std::get<uint32_t>(doc->filters["uint32"]), 3000000);
  EXPECT_EQ(std::get<int64_t>(doc->filters["int64"]), -1000000000LL);
  EXPECT_EQ(std::get<uint64_t>(doc->filters["uint64"]), 9000000000ULL);
  EXPECT_EQ(std::get<std::string>(doc->filters["string"]), "test value");
  EXPECT_DOUBLE_EQ(std::get<double>(doc->filters["double"]), 3.14159);
}

/**
 * @brief Test multiple documents with mixed types
 */
TEST_F(DocumentStoreSerializationTest, MultipleDocumentsMixedTypes) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters1;
  filters1["status"] = static_cast<int32_t>(1);
  filters1["name"] = std::string("Alice");

  std::unordered_map<std::string, FilterValue> filters2;
  filters2["status"] = static_cast<int32_t>(2);
  filters2["name"] = std::string("Bob");
  filters2["score"] = 95.5;

  std::unordered_map<std::string, FilterValue> filters3;
  filters3["status"] = static_cast<int32_t>(0);
  filters3["active"] = false;

  store1.AddDocument("doc1", filters1);
  store1.AddDocument("doc2", filters2);
  store1.AddDocument("doc3", filters3);

  ASSERT_TRUE(store1.SaveToFile(test_file_ + ".docs"));

  DocumentStore store2;
  ASSERT_TRUE(store2.LoadFromFile(test_file_ + ".docs"));

  EXPECT_EQ(store2.Size(), 3);

  auto doc1 = store2.GetDocument(1);
  ASSERT_TRUE(doc1.has_value());
  EXPECT_EQ(std::get<int32_t>(doc1->filters["status"]), 1);
  EXPECT_EQ(std::get<std::string>(doc1->filters["name"]), "Alice");

  auto doc2 = store2.GetDocument(2);
  ASSERT_TRUE(doc2.has_value());
  EXPECT_EQ(std::get<int32_t>(doc2->filters["status"]), 2);
  EXPECT_EQ(std::get<std::string>(doc2->filters["name"]), "Bob");
  EXPECT_DOUBLE_EQ(std::get<double>(doc2->filters["score"]), 95.5);

  auto doc3 = store2.GetDocument(3);
  ASSERT_TRUE(doc3.has_value());
  EXPECT_EQ(std::get<int32_t>(doc3->filters["status"]), 0);
  EXPECT_EQ(std::get<bool>(doc3->filters["active"]), false);
}

/**
 * @brief Test stream-based serialization with all types
 */
TEST_F(DocumentStoreSerializationTest, StreamSerializationAllTypes) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters;
  filters["null"] = std::monostate{};
  filters["bool"] = true;
  filters["int8"] = static_cast<int8_t>(-42);
  filters["uint8"] = static_cast<uint8_t>(200);
  filters["int16"] = static_cast<int16_t>(-1000);
  filters["uint16"] = static_cast<uint16_t>(50000);
  filters["int32"] = static_cast<int32_t>(-100000);
  filters["uint32"] = static_cast<uint32_t>(3000000);
  filters["int64"] = static_cast<int64_t>(-1000000000LL);
  filters["uint64"] = static_cast<uint64_t>(9000000000ULL);
  filters["string"] = std::string("test value");
  filters["double"] = 3.14159;

  store1.AddDocument("doc1", filters);

  // Serialize to stringstream
  std::stringstream stream;
  ASSERT_TRUE(store1.SaveToStream(stream));

  // Deserialize from stringstream
  DocumentStore store2;
  ASSERT_TRUE(store2.LoadFromStream(stream));

  auto doc = store2.GetDocument(1);
  ASSERT_TRUE(doc.has_value());
  ASSERT_EQ(doc->filters.size(), 12);

  EXPECT_TRUE(std::holds_alternative<std::monostate>(doc->filters["null"]));
  EXPECT_EQ(std::get<bool>(doc->filters["bool"]), true);
  EXPECT_EQ(std::get<int8_t>(doc->filters["int8"]), -42);
  EXPECT_EQ(std::get<uint8_t>(doc->filters["uint8"]), 200);
  EXPECT_EQ(std::get<int16_t>(doc->filters["int16"]), -1000);
  EXPECT_EQ(std::get<uint16_t>(doc->filters["uint16"]), 50000);
  EXPECT_EQ(std::get<int32_t>(doc->filters["int32"]), -100000);
  EXPECT_EQ(std::get<uint32_t>(doc->filters["uint32"]), 3000000);
  EXPECT_EQ(std::get<int64_t>(doc->filters["int64"]), -1000000000LL);
  EXPECT_EQ(std::get<uint64_t>(doc->filters["uint64"]), 9000000000ULL);
  EXPECT_EQ(std::get<std::string>(doc->filters["string"]), "test value");
  EXPECT_DOUBLE_EQ(std::get<double>(doc->filters["double"]), 3.14159);
}

/**
 * @brief Test stream-based serialization with GTID
 */
TEST_F(DocumentStoreSerializationTest, StreamSerializationWithGTID) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters;
  filters["id"] = static_cast<int32_t>(42);
  filters["name"] = std::string("test");

  store1.AddDocument("doc1", filters);

  // Serialize with GTID
  std::string original_gtid = "00000000-0000-0000-0000-000000000000:1-100";
  std::stringstream stream;
  ASSERT_TRUE(store1.SaveToStream(stream, original_gtid));

  // Deserialize and verify GTID
  DocumentStore store2;
  std::string loaded_gtid;
  ASSERT_TRUE(store2.LoadFromStream(stream, &loaded_gtid));

  EXPECT_EQ(loaded_gtid, original_gtid);

  auto doc = store2.GetDocument(1);
  ASSERT_TRUE(doc.has_value());
  EXPECT_EQ(std::get<int32_t>(doc->filters["id"]), 42);
  EXPECT_EQ(std::get<std::string>(doc->filters["name"]), "test");
}

/**
 * @brief Test stream-based serialization with empty GTID
 */
TEST_F(DocumentStoreSerializationTest, StreamSerializationEmptyGTID) {
  DocumentStore store1;

  std::unordered_map<std::string, FilterValue> filters;
  filters["value"] = static_cast<int32_t>(123);

  store1.AddDocument("doc1", filters);

  // Serialize without GTID
  std::stringstream stream;
  ASSERT_TRUE(store1.SaveToStream(stream, ""));

  // Deserialize and verify empty GTID
  DocumentStore store2;
  std::string loaded_gtid;
  ASSERT_TRUE(store2.LoadFromStream(stream, &loaded_gtid));

  EXPECT_EQ(loaded_gtid, "");

  auto doc = store2.GetDocument(1);
  ASSERT_TRUE(doc.has_value());
  EXPECT_EQ(std::get<int32_t>(doc->filters["value"]), 123);
}

/**
 * @brief Test stream-based serialization with multiple documents
 */
TEST_F(DocumentStoreSerializationTest, StreamSerializationMultipleDocuments) {
  DocumentStore store1;

  // Add 100 documents with various data
  for (int i = 1; i <= 100; ++i) {
    std::unordered_map<std::string, FilterValue> filters;
    filters["id"] = static_cast<int32_t>(i);
    filters["value"] = static_cast<double>(i * 1.5);
    filters["name"] = std::string("doc_") + std::to_string(i);
    store1.AddDocument("pk_" + std::to_string(i), filters);
  }

  // Serialize to stream
  std::stringstream stream;
  ASSERT_TRUE(store1.SaveToStream(stream));

  // Deserialize from stream
  DocumentStore store2;
  ASSERT_TRUE(store2.LoadFromStream(stream));

  EXPECT_EQ(store2.Size(), 100);

  // Verify random documents
  auto doc1 = store2.GetDocument(1);
  ASSERT_TRUE(doc1.has_value());
  EXPECT_EQ(std::get<int32_t>(doc1->filters["id"]), 1);
  EXPECT_DOUBLE_EQ(std::get<double>(doc1->filters["value"]), 1.5);
  EXPECT_EQ(std::get<std::string>(doc1->filters["name"]), "doc_1");

  auto doc50 = store2.GetDocument(50);
  ASSERT_TRUE(doc50.has_value());
  EXPECT_EQ(std::get<int32_t>(doc50->filters["id"]), 50);
  EXPECT_DOUBLE_EQ(std::get<double>(doc50->filters["value"]), 75.0);
  EXPECT_EQ(std::get<std::string>(doc50->filters["name"]), "doc_50");

  auto doc100 = store2.GetDocument(100);
  ASSERT_TRUE(doc100.has_value());
  EXPECT_EQ(std::get<int32_t>(doc100->filters["id"]), 100);
  EXPECT_DOUBLE_EQ(std::get<double>(doc100->filters["value"]), 150.0);
  EXPECT_EQ(std::get<std::string>(doc100->filters["name"]), "doc_100");
}
