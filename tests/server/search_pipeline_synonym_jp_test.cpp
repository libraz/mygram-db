/**
 * @file search_pipeline_synonym_jp_test.cpp
 * @brief Japanese-language E2E tests for synonym-aware search pipeline
 *
 * Covers:
 * - Bidirectional kana/kanji synonym expansion through ExecuteFullPipeline
 * - Half-width kana input reaching full-width dictionary entries
 * - Asymmetric ngram_size vs kanji_ngram_size with synonym expansion
 * - Multi-group AND semantics (group-internal OR, cross-group AND)
 * - verify_text post-filter correctly using synonym normalized_terms
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
constexpr const char* kHiraganaApple = "\xe3\x82\x8a\xe3\x82\x93\xe3\x81\x94";  // りんご
constexpr const char* kKatakanaApple = "\xe3\x83\xaa\xe3\x83\xb3\xe3\x82\xb4";  // リンゴ
constexpr const char* kKanjiApple = "\xe6\x9e\x97\xe6\xaa\x8e";                 // 林檎
constexpr const char* kKanjiOrange = "\xe3\x81\xbf\xe3\x81\x8b\xe3\x82\x93";    // みかん (hiragana)
constexpr const char* kKatakanaDatabase =
    "\xe3\x83\x87\xe3\x83\xbc\xe3\x82\xbf\xe3\x83\x99\xe3\x83\xbc\xe3\x82\xb9";  // データベース
constexpr const char* kHalfKanaDatabase =
    "\xef\xbe\x83\xef\xbe\x9e\xef\xbd\xb0\xef\xbe\x80\xef\xbe\x8d\xef\xbe\x9e\xef\xbd\xb0\xef\xbd\xbd";  // ﾃﾞｰﾀﾍﾞｰｽ
constexpr const char* kTokyo = "\xe6\x9d\xb1\xe4\xba\xac";                                               // 東京
constexpr const char* kTokyoTo = "\xe6\x9d\xb1\xe4\xba\xac\xe9\x83\xbd";                                 // 東京都
constexpr const char* kOsaka = "\xe5\xa4\xa7\xe9\x98\xaa";                                               // 大阪
// Doc text helpers
const std::string kDocTokyoTo =
    "\xe6\x9d\xb1\xe4\xba\xac\xe9\x83\xbd\xe7\x9f\xa5\xe4\xba\x8b\xe9\x81\xb8\xe6\x8c\x99";  // 東京都知事選挙
const std::string kDocTokyoTower =
    "\xe6\x9d\xb1\xe4\xba\xac\xe3\x82\xbf\xe3\x83\xaf\xe3\x83\xbc\xe8\xa6\xb3\xe5\x85\x89";    // 東京タワー観光
const std::string kDocOsaka = "\xe5\xa4\xa7\xe9\x98\xaa\xe5\x9f\x8e\xe8\xa6\x8b\xe5\xad\xa6";  // 大阪城見学

const std::string kDocKanjiJuice =
    std::string(kKanjiApple) + "\xe3\x82\xb8\xe3\x83\xa5\xe3\x83\xbc\xe3\x82\xb9";             // 林檎ジュース
const std::string kDocKatakanaPie = std::string(kKatakanaApple) + "\xe3\x83\x91\xe3\x82\xa4";  // リンゴパイ
const std::string kDocOrangeJelly = std::string(kKanjiOrange) + "\xe3\x82\xbc\xe3\x83\xaa\xe3\x83\xbc";  // みかんゼリー

const std::string kDocDatabaseDesign =
    std::string(kKatakanaDatabase) + "\xe8\xa8\xad\xe8\xa8\x88\xe5\x85\xa5\xe9\x96\x80";  // データベース設計入門
// NOTE: use "database" (7 chars) rather than "DB" (2 chars) so that
// ngram_size=3 can produce ASCII ngrams for the synonym variant.
const std::string kDocDatabaseAdmin = "database administration guide";
const std::string kDocWebServer =
    "\xe3\x82\xa6\xe3\x82\xa7\xe3\x83\x96\xe3\x82\xb5\xe3\x83\xbc\xe3\x83\x90\xe6\xa7\x8b\xe7\xaf\x89";  // ウェブサーバ構築

// Smartphone group constants
constexpr const char* kSmartphone =
    "\xe3\x82\xb9\xe3\x83\x9e\xe3\x83\xbc\xe3\x83\x88\xe3\x83\x95\xe3\x82\xa9\xe3\x83\xb3";  // スマートフォン
constexpr const char* kSumaho = "\xe3\x82\xb9\xe3\x83\x9e\xe3\x83\x9b";                      // スマホ
constexpr const char* kKeitai = "\xe6\x90\xba\xe5\xb8\xaf";                                  // 携帯

const std::string kDocAppleSmartphone = std::string(kKanjiApple) + kSmartphone;     // 林檎スマートフォン
const std::string kDocHiraganaAppleKeitai = std::string(kHiraganaApple) + kKeitai;  // りんご携帯
const std::string kDocKanjiAppleTablet =
    std::string(kKanjiApple) + "\xe3\x82\xbf\xe3\x83\x96\xe3\x83\xac\xe3\x83\x83\xe3\x83\x88";  // 林檎タブレット
const std::string kDocMikanSumaho = std::string(kKanjiOrange) + kSumaho;                        // みかんスマホ

const std::string kDocNewSumaho =
    "\xe6\x96\xb0\xe5\x9e\x8b" + std::string(kSumaho) + "\xe7\x99\xbb\xe5\xa0\xb4";  // 新型スマホ登場
const std::string kDocLatestSmartphone =
    "\xe6\x9c\x80\xe6\x96\xb0" + std::string(kSmartphone) + "\xe7\x89\xb9\xe9\x9b\x86";  // 最新スマートフォン特集
const std::string kDocTabletRelease =
    "\xe3\x82\xbf\xe3\x83\x96\xe3\x83\xac\xe3\x83\x83\xe3\x83\x88\xe6\x96\xb0\xe7\x99\xba\xe5\xa3\xb2";  // タブレット新発売

bool Contains(const std::vector<storage::DocId>& v, storage::DocId id) {
  return std::find(v.begin(), v.end(), id) != v.end();
}

}  // namespace

class JapaneseSynonymPipelineTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Include PID to avoid collisions across parallel ctest processes that each
    // start their own g_counter_ at 0.
    auto dir_name =
        "mygramdb_syn_jp_pipeline_" + std::to_string(::getpid()) + "_" + std::to_string(g_counter_.fetch_add(1));
    test_dir_ = std::filesystem::temp_directory_path() / dir_name;
    std::filesystem::create_directories(test_dir_);

    index_ = std::make_unique<index::Index>(
        /*ngram_size=*/3, /*kanji_ngram_size=*/2,
        /*roaring_threshold=*/0.1, /*cross_boundary_ngrams=*/true,
        /*normalize_nfkc=*/true, /*normalize_width=*/"half", /*normalize_lower=*/true);
    doc_store_ = std::make_unique<storage::DocumentStore>();
    synonym_dict_ = std::make_unique<query::SynonymDictionary>();
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  std::string WriteTSV(const std::string& content, const std::string& filename = "synonyms.tsv") {
    auto path = test_dir_ / filename;
    std::ofstream ofs(path);
    ofs << content;
    return path.string();
  }

  void LoadDict(const std::string& tsv_content) {
    auto path = WriteTSV(tsv_content);
    auto normalizer = [this](std::string_view sv) { return index_->NormalizeText(sv); };
    auto r = synonym_dict_->LoadFromFile(path, normalizer);
    ASSERT_TRUE(r.has_value()) << "dict load failed";
  }

  storage::DocId AddAndIndex(const std::string& pk, const std::string& text) {
    std::string normalized = index_->NormalizeText(text);
    auto id = doc_store_->AddDocument(pk, {}, normalized);
    EXPECT_TRUE(id.has_value());
    index_->AddDocument(*id, normalized);
    return *id;
  }

  FullPipelineParams MakeParams() {
    FullPipelineParams p;
    p.current_index = index_.get();
    p.current_doc_store = doc_store_.get();
    p.synonym_dict = synonym_dict_.get();
    p.ngram_size = 3;
    p.kanji_ngram_size = 2;
    p.cross_boundary_ngrams = true;
    p.filter_threshold = 1000;
    p.primary_key_column = "id";
    return p;
  }

  static std::atomic<int> g_counter_;
  std::filesystem::path test_dir_;
  std::unique_ptr<index::Index> index_;
  std::unique_ptr<storage::DocumentStore> doc_store_;
  std::unique_ptr<query::SynonymDictionary> synonym_dict_;
};

std::atomic<int> JapaneseSynonymPipelineTest::g_counter_{0};

// TC-SP-01: Hiragana query expands to katakana and kanji documents.
TEST_F(JapaneseSynonymPipelineTest, KanaKanjiSynonymExpansion_SearchByHiragana) {
  LoadDict(std::string(kHiraganaApple) + "\t" + kKatakanaApple + "\t" + kKanjiApple + "\n");

  auto pk1 = AddAndIndex("pk1", kDocKanjiJuice);
  auto pk2 = AddAndIndex("pk2", kDocKatakanaPie);
  auto pk3 = AddAndIndex("pk3", kDocOrangeJelly);

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = kHiraganaApple;
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.success) << output.error_message;
  EXPECT_EQ(output.results.size(), 2U);
  EXPECT_TRUE(Contains(output.results, pk1));
  EXPECT_TRUE(Contains(output.results, pk2));
  EXPECT_FALSE(Contains(output.results, pk3));
}

// TC-SP-02: Kanji query returns the same set via synonym expansion.
TEST_F(JapaneseSynonymPipelineTest, KanaKanjiSynonymExpansion_SearchByKanji) {
  LoadDict(std::string(kHiraganaApple) + "\t" + kKatakanaApple + "\t" + kKanjiApple + "\n");

  auto pk1 = AddAndIndex("pk1", kDocKanjiJuice);
  auto pk2 = AddAndIndex("pk2", kDocKatakanaPie);
  auto pk3 = AddAndIndex("pk3", kDocOrangeJelly);

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = kKanjiApple;
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.success) << output.error_message;
  EXPECT_EQ(output.results.size(), 2U);
  EXPECT_TRUE(Contains(output.results, pk1));
  EXPECT_TRUE(Contains(output.results, pk2));
  EXPECT_FALSE(Contains(output.results, pk3));
}

// TC-SP-03: Half-width kana query normalizes to full-width katakana and
// reaches the katakana-tagged synonym group (which also contains "db").
TEST_F(JapaneseSynonymPipelineTest, HalfWidthKanaQueryMatchesFullWidthDocument) {
  // Use "database" (7 chars) as the ASCII alternative so that ngram_size=3
  // can produce ASCII trigrams for the non-kana synonym variant.
  LoadDict(std::string(kHalfKanaDatabase) + "\t" + kKatakanaDatabase + "\tdatabase\n");

  auto pk1 = AddAndIndex("pk1", kDocDatabaseDesign);
  auto pk2 = AddAndIndex("pk2", kDocDatabaseAdmin);
  auto pk3 = AddAndIndex("pk3", kDocWebServer);

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = kHalfKanaDatabase;
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.success) << output.error_message;
  EXPECT_TRUE(Contains(output.results, pk1));
  EXPECT_TRUE(Contains(output.results, pk2));
  EXPECT_FALSE(Contains(output.results, pk3));
}

// TC-SP-04: Kanji bigrams (kanji_ngram_size=2) still allow synonym expansion
// where "東京" and "東京都" are synonyms, and a document with "東京" alone
// is reachable via the shorter term even though "東京都" is its synonym.
TEST_F(JapaneseSynonymPipelineTest, KanjiNgramSizeDoesNotBreakSynonymExpansion) {
  LoadDict(std::string(kTokyo) + "\t" + kTokyoTo + "\n");

  auto pk1 = AddAndIndex("pk1", kDocTokyoTo);
  auto pk2 = AddAndIndex("pk2", kDocTokyoTower);
  auto pk3 = AddAndIndex("pk3", kDocOsaka);

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = kTokyo;
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.success) << output.error_message;
  EXPECT_TRUE(Contains(output.results, pk1));
  EXPECT_TRUE(Contains(output.results, pk2));
  EXPECT_FALSE(Contains(output.results, pk3));
}

// TC-SP-05: OR within a group, AND across groups. Fruit + phone both required.
TEST_F(JapaneseSynonymPipelineTest, SynonymOrSemantics_MultipleQueryTerms_AndAcrossGroups) {
  std::string tsv =
      std::string(kHiraganaApple) + "\t" + kKanjiApple + "\n" + kSmartphone + "\t" + kSumaho + "\t" + kKeitai + "\n";
  LoadDict(tsv);

  auto pk1 = AddAndIndex("pk1", kDocAppleSmartphone);
  auto pk2 = AddAndIndex("pk2", kDocHiraganaAppleKeitai);
  auto pk3 = AddAndIndex("pk3", kDocKanjiAppleTablet);
  auto pk4 = AddAndIndex("pk4", kDocMikanSumaho);

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = kHiraganaApple;
  query.and_terms = {kSmartphone};
  query.limit = 100;

  auto params = MakeParams();
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.success) << output.error_message;
  EXPECT_TRUE(Contains(output.results, pk1));
  EXPECT_TRUE(Contains(output.results, pk2));
  EXPECT_FALSE(Contains(output.results, pk3));
  EXPECT_FALSE(Contains(output.results, pk4));
}

// TC-SP-06: verify_text="all" must accept documents that match only via a
// synonym expansion. Ensures PostFilterByTextWithSynonyms checks every term
// in the group's normalized_terms, not just the original query term.
TEST_F(JapaneseSynonymPipelineTest, PostFilterByTextWithSynonyms_VerifyText) {
  LoadDict(std::string(kSumaho) + "\t" + kSmartphone + "\n");

  auto pk1 = AddAndIndex("pk1", kDocNewSumaho);
  auto pk2 = AddAndIndex("pk2", kDocLatestSmartphone);
  auto pk3 = AddAndIndex("pk3", kDocTabletRelease);

  config::Config config;
  config.memory.verify_text = "all";

  query::Query query;
  query.type = query::QueryType::SEARCH;
  query.table = "test";
  query.search_text = kSumaho;
  query.limit = 100;

  auto params = MakeParams();
  params.full_config = &config;
  auto output = ExecuteFullPipeline(query, params);

  ASSERT_TRUE(output.success) << output.error_message;
  EXPECT_TRUE(Contains(output.results, pk1));
  EXPECT_TRUE(Contains(output.results, pk2));
  EXPECT_FALSE(Contains(output.results, pk3));
}

}  // namespace mygramdb::server::search_pipeline
