/**
 * @file response_formatter_get_test.cpp
 * @brief Tests for GET response formatting with various filter types
 */

#include <gtest/gtest.h>

#include "server/response_formatter.h"
#include "storage/document_store.h"

namespace mygramdb::server {

/**
 * @brief Test GET response with int64_t filter
 */
TEST(ResponseFormatterGetTest, Int64Filter) {
  storage::Document doc;
  doc.primary_key = "pk1";
  doc.filters["status"] = static_cast<int64_t>(1);

  auto response = ResponseFormatter::FormatGetResponse(doc);
  EXPECT_EQ(response, "OK DOC pk1 status=1");
}

/**
 * @brief Test GET response with string filter
 */
TEST(ResponseFormatterGetTest, StringFilter) {
  storage::Document doc;
  doc.primary_key = "pk2";
  doc.filters["category"] = std::string("tech");

  auto response = ResponseFormatter::FormatGetResponse(doc);
  EXPECT_EQ(response, "OK DOC pk2 category=tech");
}

/**
 * @brief Test GET response with double filter
 */
TEST(ResponseFormatterGetTest, DoubleFilter) {
  storage::Document doc;
  doc.primary_key = "pk3";
  doc.filters["score"] = 95.5;

  auto response = ResponseFormatter::FormatGetResponse(doc);
  EXPECT_EQ(response, "OK DOC pk3 score=95.500000");
}

/**
 * @brief Test GET response with multiple filter types
 */
TEST(ResponseFormatterGetTest, MultipleFilterTypes) {
  storage::Document doc;
  doc.primary_key = "pk4";
  doc.filters["status"] = static_cast<int64_t>(1);
  doc.filters["category"] = std::string("tech");
  doc.filters["score"] = 98.75;

  auto response = ResponseFormatter::FormatGetResponse(doc);
  
  // Response should contain all filters (order may vary)
  EXPECT_TRUE(response.find("OK DOC pk4") != std::string::npos);
  EXPECT_TRUE(response.find("status=1") != std::string::npos);
  EXPECT_TRUE(response.find("category=tech") != std::string::npos);
  EXPECT_TRUE(response.find("score=98.750000") != std::string::npos);
}

/**
 * @brief Test GET response with bool filter
 */
TEST(ResponseFormatterGetTest, BoolFilter) {
  storage::Document doc;
  doc.primary_key = "pk5";
  doc.filters["active"] = true;
  doc.filters["deleted"] = false;

  auto response = ResponseFormatter::FormatGetResponse(doc);
  
  EXPECT_TRUE(response.find("OK DOC pk5") != std::string::npos);
  EXPECT_TRUE(response.find("active=true") != std::string::npos);
  EXPECT_TRUE(response.find("deleted=false") != std::string::npos);
}

/**
 * @brief Test GET response with NULL filter (std::monostate)
 */
TEST(ResponseFormatterGetTest, NullFilter) {
  storage::Document doc;
  doc.primary_key = "pk6";
  doc.filters["optional"] = std::monostate{};

  auto response = ResponseFormatter::FormatGetResponse(doc);
  EXPECT_TRUE(response.find("OK DOC pk6") != std::string::npos);
  EXPECT_TRUE(response.find("optional=NULL") != std::string::npos);
}

/**
 * @brief Test GET response with various integer types
 */
TEST(ResponseFormatterGetTest, VariousIntegerTypes) {
  storage::Document doc;
  doc.primary_key = "pk7";
  doc.filters["int8"] = static_cast<int8_t>(127);
  doc.filters["uint8"] = static_cast<uint8_t>(255);
  doc.filters["int16"] = static_cast<int16_t>(32767);
  doc.filters["uint16"] = static_cast<uint16_t>(65535);
  doc.filters["int32"] = static_cast<int32_t>(2147483647);
  doc.filters["uint32"] = static_cast<uint32_t>(4294967295);

  auto response = ResponseFormatter::FormatGetResponse(doc);
  
  EXPECT_TRUE(response.find("OK DOC pk7") != std::string::npos);
  EXPECT_TRUE(response.find("int8=127") != std::string::npos);
  EXPECT_TRUE(response.find("uint8=255") != std::string::npos);
  EXPECT_TRUE(response.find("int16=32767") != std::string::npos);
  EXPECT_TRUE(response.find("uint16=65535") != std::string::npos);
  EXPECT_TRUE(response.find("int32=2147483647") != std::string::npos);
  EXPECT_TRUE(response.find("uint32=4294967295") != std::string::npos);
}

/**
 * @brief Test GET response with no filters
 */
TEST(ResponseFormatterGetTest, NoFilters) {
  storage::Document doc;
  doc.primary_key = "pk8";

  auto response = ResponseFormatter::FormatGetResponse(doc);
  EXPECT_EQ(response, "OK DOC pk8");
}

/**
 * @brief Test GET response for document not found
 */
TEST(ResponseFormatterGetTest, DocumentNotFound) {
  std::optional<storage::Document> doc = std::nullopt;

  auto response = ResponseFormatter::FormatGetResponse(doc);
  EXPECT_EQ(response, "ERROR Document not found");
}

/**
 * @brief Test GET response with floating point edge cases
 */
TEST(ResponseFormatterGetTest, FloatingPointEdgeCases) {
  storage::Document doc;
  doc.primary_key = "pk9";
  doc.filters["zero"] = 0.0;
  doc.filters["negative"] = -123.456;
  doc.filters["small"] = 0.000001;

  auto response = ResponseFormatter::FormatGetResponse(doc);
  
  EXPECT_TRUE(response.find("OK DOC pk9") != std::string::npos);
  EXPECT_TRUE(response.find("zero=0.000000") != std::string::npos);
  EXPECT_TRUE(response.find("negative=-123.456000") != std::string::npos);
  EXPECT_TRUE(response.find("small=0.000001") != std::string::npos);
}

}  // namespace mygramdb::server
