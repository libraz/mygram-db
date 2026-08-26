/**
 * @file search_pipeline_hybrid_exact_test.cpp
 * @brief Mixed CJK/ASCII terms must select the same documents on every search path
 *
 * With kanji_ngram_size != ngram_size a term that mixes kanji and ASCII
 * produces n-grams covering only part of it, so the n-gram intersection admits
 * documents that do not contain the term. Every path that derives candidates
 * from an n-gram intersection has to reject those, independently of
 * memory.verify_text.
 */

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "query/synonym_dictionary.h"
#include "server/search_pipeline.h"
#include "storage/document_store.h"

namespace mygramdb::server::search_pipeline {

namespace {

// Japanese string constants (hex-escaped for portable source encoding)
constexpr const char* kCatDog = "\xe7\x8c\xab\xe7\x8a\xac";  // 猫犬

const std::string kTermCatDogAb = std::string(kCatDog) + "ab";
const std::string kDocMatching = std::string(kCatDog) + "ab report";
const std::string kDocNgramOnly = std::string(kCatDog) + "cd report";

bool Contains(const std::vector<storage::DocId>& docs, storage::DocId id) {
  return std::find(docs.begin(), docs.end(), id) != docs.end();
}

}  // namespace

class HybridFragmentExactTextTest : public ::testing::Test {
 protected:
  void SetUp() override {
    test_dir_ = std::filesystem::temp_directory_path() /
                ("mygramdb_hybrid_exact_" + std::to_string(::getpid()) + "_" + std::to_string(counter_.fetch_add(1)));
    std::filesystem::create_directories(test_dir_);

    index_ = std::make_unique<index::Index>(
        /*ngram_size=*/3, /*kanji_ngram_size=*/2,
        /*roaring_threshold=*/0.1, /*cross_boundary_ngrams=*/false,
        /*normalize_nfkc=*/true, /*normalize_width=*/"half", /*normalize_lower=*/true);
    doc_store_ = std::make_unique<storage::DocumentStore>();
    synonym_dict_ = std::make_unique<query::SynonymDictionary>();

    matching_doc_ = AddAndIndex("pk1", kDocMatching);
    ngram_only_doc_ = AddAndIndex("pk2", kDocNgramOnly);
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  storage::DocId AddAndIndex(const std::string& pk, const std::string& text) {
    const std::string normalized = index_->NormalizeText(text);
    auto id = doc_store_->AddDocument(pk, {}, normalized);
    EXPECT_TRUE(id.has_value());
    index_->AddDocument(*id, normalized);
    return *id;
  }

  /// Load a dictionary that does not mention the query term, so synonym
  /// expansion cannot change which documents are reachable.
  void LoadUnrelatedSynonyms() {
    const auto path = test_dir_ / "synonyms.tsv";
    std::ofstream output(path);
    output << "alpha\tbeta\n";
    output.close();
    auto normalizer = [this](std::string_view text) { return index_->NormalizeText(text); };
    ASSERT_TRUE(synonym_dict_->LoadFromFile(path.string(), normalizer).has_value());
  }

  FullPipelineParams MakeParams() {
    FullPipelineParams params;
    params.current_index = index_.get();
    params.current_doc_store = doc_store_.get();
    params.full_config = &config_;
    params.ngram_size = 3;
    params.kanji_ngram_size = 2;
    params.cross_boundary_ngrams = false;
    params.filter_threshold = 1000;
    params.primary_key_column = "id";
    return params;
  }

  static query::Query MakeQuery(const std::string& search_text) {
    query::Query query;
    query.type = query::QueryType::SEARCH;
    query.table = "test";
    query.search_text = search_text;
    query.limit = 100;
    return query;
  }

  static std::atomic<int> counter_;
  std::filesystem::path test_dir_;
  config::Config config_;  ///< memory.verify_text stays at its default "off"
  std::unique_ptr<index::Index> index_;
  std::unique_ptr<storage::DocumentStore> doc_store_;
  std::unique_ptr<query::SynonymDictionary> synonym_dict_;
  storage::DocId matching_doc_ = 0;
  storage::DocId ngram_only_doc_ = 0;
};

std::atomic<int> HybridFragmentExactTextTest::counter_{0};

TEST_F(HybridFragmentExactTextTest, RegularPathRejectsUncoveredFragment) {
  auto params = MakeParams();
  auto output = ExecuteFullPipeline(MakeQuery(kTermCatDogAb), params);

  ASSERT_TRUE(output.has_value()) << output.error().message();
  EXPECT_TRUE(Contains(output->results, matching_doc_));
  EXPECT_FALSE(Contains(output->results, ngram_only_doc_));
}

TEST_F(HybridFragmentExactTextTest, FuzzyPathRejectsUncoveredFragment) {
  auto query = MakeQuery(kTermCatDogAb);
  query.fuzzy_max_distance = 1;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.has_value()) << output.error().message();
  EXPECT_TRUE(Contains(output->results, matching_doc_));
  EXPECT_FALSE(Contains(output->results, ngram_only_doc_));
}

TEST_F(HybridFragmentExactTextTest, SynonymPathRejectsUncoveredFragment) {
  LoadUnrelatedSynonyms();

  auto params = MakeParams();
  params.synonym_dict = synonym_dict_.get();
  auto output = ExecuteFullPipeline(MakeQuery(kTermCatDogAb), params);

  ASSERT_TRUE(output.has_value()) << output.error().message();
  EXPECT_TRUE(Contains(output->results, matching_doc_));
  EXPECT_FALSE(Contains(output->results, ngram_only_doc_));
}

TEST_F(HybridFragmentExactTextTest, BooleanPathRejectsUncoveredFragment) {
  auto params = MakeParams();
  auto output = ExecuteFullPipeline(MakeQuery(kTermCatDogAb + " AND report"), params);

  ASSERT_TRUE(output.has_value()) << output.error().message();
  EXPECT_TRUE(Contains(output->results, matching_doc_));
  EXPECT_FALSE(Contains(output->results, ngram_only_doc_));
}

TEST_F(HybridFragmentExactTextTest, EnablingSynonymsDoesNotChangeResultSet) {
  LoadUnrelatedSynonyms();

  auto params = MakeParams();
  auto without_synonyms = ExecuteFullPipeline(MakeQuery(kTermCatDogAb), params);
  ASSERT_TRUE(without_synonyms.has_value()) << without_synonyms.error().message();

  params.synonym_dict = synonym_dict_.get();
  auto with_synonyms = ExecuteFullPipeline(MakeQuery(kTermCatDogAb), params);
  ASSERT_TRUE(with_synonyms.has_value()) << with_synonyms.error().message();

  EXPECT_EQ(without_synonyms->results, with_synonyms->results);
}

}  // namespace mygramdb::server::search_pipeline
