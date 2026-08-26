/**
 * @file result_sorter_ordering_test.cpp
 * @brief Strict weak ordering guarantees for primary key sorting
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "query/query_parser.h"
#include "query/result_sorter.h"
#include "storage/document_store.h"

using namespace mygramdb::query;
using namespace mygramdb::storage;

namespace {

// DocId that never belongs to a document, exercising the fallback key derived
// from the DocId itself when the primary key lookup misses.
constexpr DocId kMissingDocId = 1'000'000;

// Result count that keeps SortAndPaginate on the direct-comparator path.
constexpr size_t kSmallResultCount = 60;

// Result count that puts SortAndPaginate on the pre-computed key path.
constexpr size_t kLargeResultCount = 150;

/**
 * @brief Build primary keys mixing numeric, non-numeric and zero-padded forms
 *
 * All generated keys are distinct, so the resulting order is fully determined
 * by the sort keys rather than by the DocId tie-break.
 */
std::vector<std::string> MixedPrimaryKeys(size_t count) {
  std::vector<std::string> keys;
  keys.reserve(count);
  for (size_t i = 0; i < count; ++i) {
    switch (i % 5) {
      case 0:
        keys.push_back(std::to_string(i));
        break;
      case 1:
        keys.push_back(std::to_string(i * 7 + 1000));
        break;
      case 2:
        keys.push_back("sku-" + std::to_string(i));
        break;
      case 3:
        keys.push_back(std::to_string(i) + "a");
        break;
      default:
        keys.push_back("00" + std::to_string(i));
        break;
    }
  }
  return keys;
}

}  // namespace

class ResultSorterOrderingTest : public ::testing::Test {
 protected:
  void SetUp() override { doc_store_.Clear(); }

  DocId Add(const std::string& primary_key) {
    auto result = doc_store_.AddDocument(primary_key);
    EXPECT_TRUE(result.has_value());
    return result.has_value() ? *result : kMissingDocId;
  }

  std::vector<DocId> AddAll(const std::vector<std::string>& primary_keys) {
    std::vector<DocId> doc_ids;
    doc_ids.reserve(primary_keys.size());
    for (const auto& primary_key : primary_keys) {
      doc_ids.push_back(Add(primary_key));
    }
    return doc_ids;
  }

  /**
   * @brief Sort by primary key, requesting every input row
   *
   * A limit equal to the input size keeps the full-sort strategy, so the path
   * taken is decided purely by the result count.
   */
  std::vector<DocId> SortByPrimaryKey(std::vector<DocId> input, SortOrder order) {
    auto limit = static_cast<uint32_t>(input.size());
    return SortByPrimaryKey(std::move(input), order, limit);
  }

  std::vector<DocId> SortByPrimaryKey(std::vector<DocId> input, SortOrder order, uint32_t limit) {
    Query query;
    query.type = QueryType::SEARCH;
    query.table = "test";
    query.search_text = "test";
    query.limit = limit;
    query.offset = 0;
    query.order_by = OrderByClause{"", order};

    auto result = ResultSorter::SortAndPaginate(input, doc_store_, query);
    if (!result.has_value()) {
      ADD_FAILURE() << result.error().message();
      return {};
    }
    return result.value();
  }

  /**
   * @brief Read the comparator's verdict for one pair through the public API
   *
   * Sorting a two-element result set stays on the direct-comparator path. A
   * sort places @p lhs first exactly when the comparator does not report
   * `rhs < lhs`, which for an asymmetric comparator is `lhs < rhs`.
   */
  bool ComparatorLess(DocId lhs, DocId rhs, SortOrder order) {
    auto sorted = SortByPrimaryKey({rhs, lhs}, order);
    EXPECT_EQ(sorted.size(), 2U);
    return !sorted.empty() && sorted.front() == lhs;
  }

  std::vector<std::string> PrimaryKeysOf(const std::vector<DocId>& doc_ids) {
    std::vector<std::string> keys;
    keys.reserve(doc_ids.size());
    for (DocId doc_id : doc_ids) {
      keys.push_back(doc_store_.GetPrimaryKey(doc_id).value_or("<missing>"));
    }
    return keys;
  }

  DocumentStore doc_store_;
};

/**
 * @brief The primary key comparator must be a strict weak ordering
 *
 * The value set mixes the forms a VARCHAR primary key can take: plain numbers,
 * numbers with leading zeros, values that merely start with digits, signed and
 * explicitly positive forms, whitespace padding, an empty key, a value longer
 * than any numeric key, and a numeric literal too large for uint64_t.
 */
TEST_F(ResultSorterOrderingTest, PrimaryKeyComparatorIsStrictWeakOrdering) {
  const std::vector<std::string> keys = {"9",
                                         "10",
                                         "1a",
                                         "",
                                         "0",
                                         "7",
                                         "007",
                                         " 5",
                                         "5 ",
                                         "-3",
                                         "+7",
                                         "99999999999999999999999",
                                         "zzzzzzzzzzzzzzzzzzzzzzzzzzzz"};

  std::vector<DocId> docs = AddAll(keys);
  std::vector<std::string> labels;
  labels.reserve(keys.size() + 2);
  for (const auto& key : keys) {
    labels.push_back("'" + key + "'");
  }

  // A removed document keeps its DocId in the result set while its primary key
  // lookup misses, placing the DocId fallback in the middle of the DocId range
  // instead of past its end.
  const size_t removed_index = keys.size() / 2;
  ASSERT_TRUE(doc_store_.RemoveDocument(docs[removed_index]));
  labels[removed_index] = "<removed>";

  docs.push_back(kMissingDocId);
  labels.emplace_back("<missing>");

  const size_t count = docs.size();
  for (SortOrder order : {SortOrder::ASC, SortOrder::DESC}) {
    const char* order_name = (order == SortOrder::ASC) ? "ASC" : "DESC";

    std::vector<uint8_t> less(count * count, 0);
    for (size_t i = 0; i < count; ++i) {
      for (size_t j = 0; j < count; ++j) {
        if (i != j) {
          less[(i * count) + j] = ComparatorLess(docs[i], docs[j], order) ? 1 : 0;
        }
      }
    }

    for (size_t i = 0; i < count; ++i) {
      for (size_t j = i + 1; j < count; ++j) {
        EXPECT_NE(less[(i * count) + j], less[(j * count) + i])
            << order_name << " asymmetry broken for " << labels[i] << " and " << labels[j];
      }
    }

    for (size_t i = 0; i < count; ++i) {
      for (size_t j = 0; j < count; ++j) {
        if (i == j || less[(i * count) + j] == 0) {
          continue;
        }
        for (size_t k = 0; k < count; ++k) {
          if (k == i || k == j || less[(j * count) + k] == 0) {
            continue;
          }
          EXPECT_EQ(less[(i * count) + k], 1)
              << order_name << " transitivity broken: " << labels[i] << " < " << labels[j] << " and " << labels[j]
              << " < " << labels[k] << " but not " << labels[i] << " < " << labels[k];
        }
      }
    }
  }
}

/**
 * @brief Both sort strategies must agree on a mixed primary key set
 *
 * The relative order of a subset small enough for the direct-comparator path
 * has to match that subset's order inside a result set large enough for the
 * pre-computed key path.
 */
TEST_F(ResultSorterOrderingTest, SmallAndLargeResultSetsAgreeOnMixedPrimaryKeys) {
  std::vector<DocId> docs = AddAll(MixedPrimaryKeys(kLargeResultCount));
  std::vector<DocId> subset(docs.begin(), docs.begin() + kSmallResultCount);

  for (SortOrder order : {SortOrder::ASC, SortOrder::DESC}) {
    const char* order_name = (order == SortOrder::ASC) ? "ASC" : "DESC";

    auto large_sorted = SortByPrimaryKey(docs, order);
    ASSERT_EQ(large_sorted.size(), kLargeResultCount);

    std::vector<DocId> projected;
    projected.reserve(subset.size());
    for (DocId doc_id : large_sorted) {
      if (std::find(subset.begin(), subset.end(), doc_id) != subset.end()) {
        projected.push_back(doc_id);
      }
    }
    ASSERT_EQ(projected.size(), kSmallResultCount);

    auto small_sorted = SortByPrimaryKey(subset, order);
    ASSERT_EQ(small_sorted.size(), kSmallResultCount);

    EXPECT_EQ(PrimaryKeysOf(small_sorted), PrimaryKeysOf(projected)) << order_name << " paths disagree";
  }
}

#ifdef MYGRAMDB_QUERY_TEST_HOOKS
/**
 * @brief The comparator-based partial sort must agree with pre-computed keys
 *
 * Forcing the batched pre-computed partial sort to bail out routes the same
 * input through std::partial_sort with the comparator, so its top K has to
 * match the head of the pre-computed full sort.
 */
TEST_F(ResultSorterOrderingTest, ComparatorPartialSortAgreesWithPrecomputedKeys) {
  std::vector<DocId> docs = AddAll(MixedPrimaryKeys(kLargeResultCount));
  constexpr uint32_t kTopK = 70;

  for (SortOrder order : {SortOrder::ASC, SortOrder::DESC}) {
    const char* order_name = (order == SortOrder::ASC) ? "ASC" : "DESC";

    auto full_sorted = SortByPrimaryKey(docs, order);
    ASSERT_EQ(full_sorted.size(), kLargeResultCount);
    std::vector<DocId> expected_head(full_sorted.begin(), full_sorted.begin() + kTopK);

    ResultSorter::ForceSchwartzianPartialFailureForTesting(true);
    auto partial_sorted = SortByPrimaryKey(docs, order, kTopK);
    ResultSorter::ForceSchwartzianPartialFailureForTesting(false);

    ASSERT_EQ(partial_sorted.size(), kTopK);
    EXPECT_EQ(PrimaryKeysOf(partial_sorted), PrimaryKeysOf(expected_head)) << order_name << " paths disagree";
  }
}
#endif

/**
 * @brief A purely numeric primary key keeps its numeric order on both paths
 */
TEST_F(ResultSorterOrderingTest, PurelyNumericPrimaryKeyOrderIsNumeric) {
  std::vector<DocId> small = AddAll({"100", "50", "200", "150", "75", "3", "1000"});

  EXPECT_EQ(PrimaryKeysOf(SortByPrimaryKey(small, SortOrder::ASC)),
            (std::vector<std::string>{"3", "50", "75", "100", "150", "200", "1000"}));
  EXPECT_EQ(PrimaryKeysOf(SortByPrimaryKey(small, SortOrder::DESC)),
            (std::vector<std::string>{"1000", "200", "150", "100", "75", "50", "3"}));

  doc_store_.Clear();

  std::vector<std::string> numeric_keys;
  numeric_keys.reserve(kLargeResultCount);
  for (size_t i = 0; i < kLargeResultCount; ++i) {
    numeric_keys.push_back(std::to_string(i * 10));
  }
  std::vector<DocId> large = AddAll(numeric_keys);

  auto ascending = PrimaryKeysOf(SortByPrimaryKey(large, SortOrder::ASC));
  ASSERT_EQ(ascending.size(), kLargeResultCount);
  for (size_t i = 0; i < kLargeResultCount; ++i) {
    EXPECT_EQ(ascending[i], std::to_string(i * 10)) << "at index " << i;
  }

  auto descending = PrimaryKeysOf(SortByPrimaryKey(large, SortOrder::DESC));
  ASSERT_EQ(descending.size(), kLargeResultCount);
  for (size_t i = 0; i < kLargeResultCount; ++i) {
    EXPECT_EQ(descending[i], std::to_string((kLargeResultCount - 1 - i) * 10)) << "at index " << i;
  }
}

/**
 * @brief A purely non-numeric primary key keeps its byte order on both paths
 */
TEST_F(ResultSorterOrderingTest, PurelyNonNumericPrimaryKeyOrderIsLexicographic) {
  std::vector<DocId> small = AddAll({"charlie", "alice", "Bob", "bob", "zeta", "apple-2", "apple-1"});

  EXPECT_EQ(PrimaryKeysOf(SortByPrimaryKey(small, SortOrder::ASC)),
            (std::vector<std::string>{"Bob", "alice", "apple-1", "apple-2", "bob", "charlie", "zeta"}));
  EXPECT_EQ(PrimaryKeysOf(SortByPrimaryKey(small, SortOrder::DESC)),
            (std::vector<std::string>{"zeta", "charlie", "bob", "apple-2", "apple-1", "alice", "Bob"}));

  doc_store_.Clear();

  std::vector<std::string> string_keys;
  string_keys.reserve(kLargeResultCount);
  for (size_t i = 0; i < kLargeResultCount; ++i) {
    std::string index = std::to_string(i);
    string_keys.push_back("item-" + std::string(3 - index.size(), '0') + index);
  }
  std::vector<DocId> large = AddAll(string_keys);

  auto ascending = PrimaryKeysOf(SortByPrimaryKey(large, SortOrder::ASC));
  ASSERT_EQ(ascending.size(), kLargeResultCount);
  for (size_t i = 0; i < kLargeResultCount; ++i) {
    EXPECT_EQ(ascending[i], string_keys[i]) << "at index " << i;
  }

  auto descending = PrimaryKeysOf(SortByPrimaryKey(large, SortOrder::DESC));
  ASSERT_EQ(descending.size(), kLargeResultCount);
  for (size_t i = 0; i < kLargeResultCount; ++i) {
    EXPECT_EQ(descending[i], string_keys[kLargeResultCount - 1 - i]) << "at index " << i;
  }
}
