/**
 * @file search_pipeline_double_filter_test.cpp
 * @brief DOUBLE equality must not depend on which filter path evaluates it
 *
 * ApplyFiltersWithBitmap serves EQ/NE-only filter sets from the bitmap index
 * and hands everything else to ApplyFilters. Adding a filter that can only
 * remove rows must therefore never add one.
 */

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <vector>

#include "server/search_pipeline.h"
#include "storage/document_store.h"

namespace mygramdb::server::search_pipeline {

class DoubleFilterPathParityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    doc_store_ = std::make_unique<storage::DocumentStore>();

    // Sums to 0.30000000000000004: within 1e-9 of 0.3, but not the same value.
    accumulated_ = 0.1 + 0.2;
    ASSERT_NE(accumulated_, 0.3);

    doc_ids_.push_back(AddDocument("pk0", accumulated_));
    doc_ids_.push_back(AddDocument("pk1", 0.3));
    doc_ids_.push_back(AddDocument("pk2", 0.5));
  }

  storage::DocId AddDocument(const std::string& pk, double price) {
    auto id = doc_store_->AddDocument(
        pk, {{"price", storage::FilterValue{price}}, {"qty", storage::FilterValue{int64_t{1}}}}, "text " + pk);
    EXPECT_TRUE(id.has_value());
    return *id;
  }

  /// A filter that keeps every document, added only to force the fallback path.
  static query::FilterCondition OrthogonalFilter() { return {"qty", query::FilterOp::GTE, "0"}; }

  std::unique_ptr<storage::DocumentStore> doc_store_;
  std::vector<storage::DocId> doc_ids_;
  double accumulated_ = 0.0;
};

TEST_F(DoubleFilterPathParityTest, EqIsUnchangedByAnOrthogonalFilter) {
  std::vector<query::FilterCondition> alone = {{"price", query::FilterOp::EQ, "0.3"}};
  std::vector<query::FilterCondition> combined = {{"price", query::FilterOp::EQ, "0.3"}, OrthogonalFilter()};

  auto alone_result = ApplyFiltersWithBitmap(doc_ids_, alone, doc_store_.get());
  auto combined_result = ApplyFiltersWithBitmap(doc_ids_, combined, doc_store_.get());

  EXPECT_EQ(alone_result, combined_result);
}

TEST_F(DoubleFilterPathParityTest, NeIsUnchangedByAnOrthogonalFilter) {
  std::vector<query::FilterCondition> alone = {{"price", query::FilterOp::NE, "0.3"}};
  std::vector<query::FilterCondition> combined = {{"price", query::FilterOp::NE, "0.3"}, OrthogonalFilter()};

  auto alone_result = ApplyFiltersWithBitmap(doc_ids_, alone, doc_store_.get());
  auto combined_result = ApplyFiltersWithBitmap(doc_ids_, combined, doc_store_.get());

  EXPECT_EQ(alone_result, combined_result);
}

TEST_F(DoubleFilterPathParityTest, BitmapAndFallbackAgreeOnEq) {
  std::vector<query::FilterCondition> filters = {{"price", query::FilterOp::EQ, "0.3"}};

  auto bitmap_result = ApplyFiltersWithBitmap(doc_ids_, filters, doc_store_.get());
  auto fallback_result = ApplyFilters(doc_ids_, filters, doc_store_.get());

  EXPECT_EQ(bitmap_result, fallback_result);
}

TEST_F(DoubleFilterPathParityTest, BitmapAndFallbackAgreeOnNe) {
  std::vector<query::FilterCondition> filters = {{"price", query::FilterOp::NE, "0.3"}};

  auto bitmap_result = ApplyFiltersWithBitmap(doc_ids_, filters, doc_store_.get());
  auto fallback_result = ApplyFilters(doc_ids_, filters, doc_store_.get());

  EXPECT_EQ(bitmap_result, fallback_result);
}

TEST_F(DoubleFilterPathParityTest, RangeComparisonStillMatchesExactly) {
  std::vector<query::FilterCondition> filters = {{"price", query::FilterOp::GT, "0.3"}};

  auto result = ApplyFilters(doc_ids_, filters, doc_store_.get());

  ASSERT_EQ(result.size(), 2U);
  EXPECT_EQ(result[0], doc_ids_[0]);
  EXPECT_EQ(result[1], doc_ids_[2]);
}

}  // namespace mygramdb::server::search_pipeline
