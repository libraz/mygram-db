/**
 * @file synonym_dictionary_test.cpp
 * @brief Tests for SynonymDictionary class
 */

#include "query/synonym_dictionary.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "index/index.h"

using mygramdb::query::SynonymDictionary;

namespace {

// Counter for unique temp directories (avoids race when ctest runs tests in parallel)
std::atomic<int> g_temp_counter{0};

// Identity normalizer for testing
std::string IdentityNormalizer(std::string_view text) {
  return std::string(text);
}

// Lowercase normalizer
std::string LowerNormalizer(std::string_view text) {
  std::string result(text);
  std::transform(result.begin(), result.end(), result.begin(), ::tolower);
  return result;
}

// Japanese string constants (hex-escaped for portable source encoding)
constexpr const char* kHalfKanaTest = "\xef\xbe\x83\xef\xbd\xbd\xef\xbe\x84";   // ﾃｽﾄ
constexpr const char* kKatakanaTest = "\xe3\x83\x86\xe3\x82\xb9\xe3\x83\x88";   // テスト
constexpr const char* kHiraganaApple = "\xe3\x82\x8a\xe3\x82\x93\xe3\x81\x94";  // りんご
constexpr const char* kKatakanaApple = "\xe3\x83\xaa\xe3\x83\xb3\xe3\x82\xb4";  // リンゴ
constexpr const char* kKanjiApple = "\xe6\x9e\x97\xe6\xaa\x8e";                 // 林檎
constexpr const char* kFullWidthDB = "\xef\xbc\xa4\xef\xbc\xa2";                // ＤＢ
constexpr const char* kKatakanaDatabase =
    "\xe3\x83\x87\xe3\x83\xbc\xe3\x82\xbf\xe3\x83\x99\xe3\x83\xbc\xe3\x82\xb9";  // データベース
constexpr const char* kHalfKanaDatabase =
    "\xef\xbe\x83\xef\xbe\x9e\xef\xbd\xb0\xef\xbe\x80\xef\xbe\x8d\xef\xbe\x9e\xef\xbd\xb0\xef\xbd\xbd";  // ﾃﾞｰﾀﾍﾞｰｽ
constexpr const char* kKatakanaServer = "\xe3\x82\xb5\xe3\x83\xbc\xe3\x83\x90";                          // サーバ

}  // namespace

class SynonymDictionaryTest : public ::testing::Test {
 protected:
  std::filesystem::path test_dir_;

  void SetUp() override {
    // Include PID to avoid collisions across parallel ctest processes that each
    // start their own g_temp_counter at 0.
    auto dir_name =
        "mygramdb_synonym_test_" + std::to_string(::getpid()) + "_" + std::to_string(g_temp_counter.fetch_add(1));
    test_dir_ = std::filesystem::temp_directory_path() / dir_name;
    std::filesystem::create_directories(test_dir_);
  }

  void TearDown() override { std::filesystem::remove_all(test_dir_); }

  std::string CreateTempTSV(const std::string& content) { return CreateTempTSV(content, "synonyms.tsv"); }

  std::string CreateTempTSV(const std::string& content, const std::string& filename) {
    auto path = test_dir_ / filename;
    std::ofstream ofs(path);
    ofs << content;
    return path.string();
  }
};

/// Fixture that provides a real Index-based normalizer so tests exercise the
/// same NFKC + width + lower pipeline that production code uses.
class RealNormalizerSynonymDictionaryTest : public SynonymDictionaryTest {
 protected:
  void SetUp() override {
    SynonymDictionaryTest::SetUp();
    index_ = std::make_unique<mygramdb::index::Index>(
        /*ngram_size=*/2, /*kanji_ngram_size=*/1,
        /*roaring_threshold=*/0.1, /*cross_boundary_ngrams=*/true,
        /*normalize_nfkc=*/true, /*normalize_width=*/"half", /*normalize_lower=*/true);
  }

  std::function<std::string(std::string_view)> Normalizer() {
    return [this](std::string_view sv) { return index_->NormalizeText(sv); };
  }

  std::unique_ptr<mygramdb::index::Index> index_;
};

TEST_F(SynonymDictionaryTest, LoadFromFileBasic) {
  auto path = CreateTempTSV("nyc\tnew york city\tny\n");
  SynonymDictionary dict;
  auto result = dict.LoadFromFile(path, IdentityNormalizer);
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(dict.GroupCount(), 1);
  EXPECT_EQ(dict.TermCount(), 3);
}

TEST_F(SynonymDictionaryTest, ExpandReturnsGroup) {
  auto path = CreateTempTSV("nyc\tnew york city\n");
  SynonymDictionary dict;
  dict.LoadFromFile(path, IdentityNormalizer);

  auto expanded = dict.Expand("nyc");
  EXPECT_EQ(expanded.size(), 2);
  // Should contain both terms (sorted by the dictionary)
  EXPECT_TRUE(std::find(expanded.begin(), expanded.end(), "nyc") != expanded.end());
  EXPECT_TRUE(std::find(expanded.begin(), expanded.end(), "new york city") != expanded.end());
}

TEST_F(SynonymDictionaryTest, ExpandBidirectional) {
  auto path = CreateTempTSV("nyc\tnew york city\n");
  SynonymDictionary dict;
  dict.LoadFromFile(path, IdentityNormalizer);

  auto from_nyc = dict.Expand("nyc");
  auto from_nycity = dict.Expand("new york city");
  EXPECT_EQ(from_nyc, from_nycity);
}

TEST_F(SynonymDictionaryTest, ExpandUnknownTerm) {
  auto path = CreateTempTSV("nyc\tnew york city\n");
  SynonymDictionary dict;
  dict.LoadFromFile(path, IdentityNormalizer);

  auto expanded = dict.Expand("unknown");
  ASSERT_EQ(expanded.size(), 1);
  EXPECT_EQ(expanded[0], "unknown");
}

TEST_F(SynonymDictionaryTest, NormalizationApplied) {
  auto path = CreateTempTSV("NYC\tNew York City\n");
  SynonymDictionary dict;
  dict.LoadFromFile(path, LowerNormalizer);

  auto expanded = dict.Expand("nyc");
  EXPECT_EQ(expanded.size(), 2);
  EXPECT_TRUE(std::find(expanded.begin(), expanded.end(), "nyc") != expanded.end());
  EXPECT_TRUE(std::find(expanded.begin(), expanded.end(), "new york city") != expanded.end());
}

TEST_F(SynonymDictionaryTest, MultipleGroups) {
  auto path = CreateTempTSV("nyc\tnew york city\ntokyo\t\xe6\x9d\xb1\xe4\xba\xac\n");
  SynonymDictionary dict;
  dict.LoadFromFile(path, IdentityNormalizer);

  EXPECT_EQ(dict.GroupCount(), 2);
  EXPECT_EQ(dict.TermCount(), 4);

  auto nyc = dict.Expand("nyc");
  EXPECT_EQ(nyc.size(), 2);

  auto tokyo = dict.Expand("tokyo");
  EXPECT_EQ(tokyo.size(), 2);
}

TEST_F(SynonymDictionaryTest, EmptyFile) {
  auto path = CreateTempTSV("");
  SynonymDictionary dict;
  auto result = dict.LoadFromFile(path, IdentityNormalizer);
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(dict.IsEmpty());
}

TEST_F(SynonymDictionaryTest, CommentsAndEmptyLines) {
  auto path = CreateTempTSV("# Comment\n\nnyc\tnew york city\n# Another comment\n");
  SynonymDictionary dict;
  dict.LoadFromFile(path, IdentityNormalizer);
  EXPECT_EQ(dict.GroupCount(), 1);
}

TEST_F(SynonymDictionaryTest, SingleTermLineSkipped) {
  auto path = CreateTempTSV("alone\n");
  SynonymDictionary dict;
  dict.LoadFromFile(path, IdentityNormalizer);
  EXPECT_TRUE(dict.IsEmpty());
}

TEST_F(SynonymDictionaryTest, SaveLoadRoundTrip) {
  auto path = CreateTempTSV("nyc\tnew york city\ntokyo\t\xe6\x9d\xb1\xe4\xba\xac\n");
  SynonymDictionary dict;
  dict.LoadFromFile(path, IdentityNormalizer);

  // Save
  std::ostringstream oss;
  ASSERT_TRUE(dict.SaveToStream(oss));

  // Load into new dict
  SynonymDictionary dict2;
  std::istringstream iss(oss.str());
  ASSERT_TRUE(dict2.LoadFromStream(iss));

  EXPECT_EQ(dict2.GroupCount(), 2);
  EXPECT_EQ(dict2.TermCount(), 4);

  auto nyc = dict2.Expand("nyc");
  EXPECT_EQ(nyc.size(), 2);
  EXPECT_TRUE(std::find(nyc.begin(), nyc.end(), "new york city") != nyc.end());
}

TEST_F(SynonymDictionaryTest, FileNotFound) {
  SynonymDictionary dict;
  auto result = dict.LoadFromFile("/nonexistent/path/synonyms.tsv", IdentityNormalizer);
  EXPECT_FALSE(result.has_value());
}

TEST_F(SynonymDictionaryTest, Clear) {
  auto path = CreateTempTSV("nyc\tnew york city\n");
  SynonymDictionary dict;
  dict.LoadFromFile(path, IdentityNormalizer);
  EXPECT_FALSE(dict.IsEmpty());

  dict.Clear();
  EXPECT_TRUE(dict.IsEmpty());
  EXPECT_EQ(dict.GroupCount(), 0);
  EXPECT_EQ(dict.TermCount(), 0);
}

TEST_F(SynonymDictionaryTest, DuplicateTermsInGroupDeduped) {
  auto path = CreateTempTSV("nyc\tnyc\tnew york city\n");
  SynonymDictionary dict;
  dict.LoadFromFile(path, IdentityNormalizer);

  EXPECT_EQ(dict.GroupCount(), 1);
  EXPECT_EQ(dict.TermCount(), 2);
}

TEST_F(SynonymDictionaryTest, MoveConstructor) {
  auto path = CreateTempTSV("nyc\tnew york city\n");
  SynonymDictionary dict;
  dict.LoadFromFile(path, IdentityNormalizer);

  SynonymDictionary dict2(std::move(dict));
  EXPECT_EQ(dict2.GroupCount(), 1);
  EXPECT_EQ(dict2.TermCount(), 2);

  auto expanded = dict2.Expand("nyc");
  EXPECT_EQ(expanded.size(), 2);
}

TEST_F(SynonymDictionaryTest, MoveAssignment) {
  auto path = CreateTempTSV("nyc\tnew york city\n");
  SynonymDictionary dict;
  dict.LoadFromFile(path, IdentityNormalizer);

  SynonymDictionary dict2;
  dict2 = std::move(dict);
  EXPECT_EQ(dict2.GroupCount(), 1);
  EXPECT_EQ(dict2.TermCount(), 2);
}

TEST_F(SynonymDictionaryTest, ForEachTermIteratesAllTerms) {
  auto path = CreateTempTSV("a\tb\tc\nd\te\n");
  SynonymDictionary dict;
  ASSERT_TRUE(dict.LoadFromFile(path, IdentityNormalizer).has_value());

  std::vector<std::string> seen;
  dict.ForEachTerm([&seen](const std::string& term) { seen.push_back(term); });
  std::sort(seen.begin(), seen.end());

  ASSERT_EQ(seen.size(), 5);
  EXPECT_EQ(seen[0], "a");
  EXPECT_EQ(seen[1], "b");
  EXPECT_EQ(seen[2], "c");
  EXPECT_EQ(seen[3], "d");
  EXPECT_EQ(seen[4], "e");
}

// =============================================================================
// Japanese / NFKC normalization tests (TC-SD-01..07)
//
// These tests use the real Index normalizer (nfkc=true, width="half",
// lower=true) to validate that kana/kanji and half-width / full-width
// variants collapse or expand correctly during dictionary loading.
// =============================================================================

// TC-SD-01: Half-width kana and full-width katakana normalize to the same
// token. After dedup only one term remains, so the group is skipped entirely.
TEST_F(RealNormalizerSynonymDictionaryTest, NfkcNormalizationCollapseHiraganaKatakana) {
  auto path = CreateTempTSV(std::string(kHalfKanaTest) + "\t" + kKatakanaTest + "\n");
  SynonymDictionary dict;
  auto result = dict.LoadFromFile(path, Normalizer());
  ASSERT_TRUE(result.has_value());
  EXPECT_TRUE(dict.IsEmpty());
}

// TC-SD-02: Hiragana and katakana remain distinct after NFKC; both map into
// the same synonym group and expand to each other.
TEST_F(RealNormalizerSynonymDictionaryTest, NfkcNormalizationHiraganaKatakanaDistinct) {
  auto path = CreateTempTSV(std::string(kHiraganaApple) + "\t" + kKatakanaApple + "\n");
  SynonymDictionary dict;
  auto result = dict.LoadFromFile(path, Normalizer());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(dict.GroupCount(), 1);
  EXPECT_EQ(dict.TermCount(), 2);

  auto expanded = dict.Expand(kHiraganaApple);
  EXPECT_EQ(expanded.size(), 2);
  EXPECT_TRUE(std::find(expanded.begin(), expanded.end(), kHiraganaApple) != expanded.end());
  EXPECT_TRUE(std::find(expanded.begin(), expanded.end(), kKatakanaApple) != expanded.end());
}

// TC-SD-03: Hiragana + katakana + kanji all remain distinct under NFKC and
// form a single three-element synonym group.
TEST_F(RealNormalizerSynonymDictionaryTest, NfkcNormalizationKanaKanjiGroup) {
  auto path = CreateTempTSV(std::string(kHiraganaApple) + "\t" + kKatakanaApple + "\t" + kKanjiApple + "\n");
  SynonymDictionary dict;
  auto result = dict.LoadFromFile(path, Normalizer());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(dict.GroupCount(), 1);
  EXPECT_EQ(dict.TermCount(), 3);

  auto expanded = dict.Expand(kKanjiApple);
  EXPECT_EQ(expanded.size(), 3);
  EXPECT_TRUE(std::find(expanded.begin(), expanded.end(), kHiraganaApple) != expanded.end());
  EXPECT_TRUE(std::find(expanded.begin(), expanded.end(), kKatakanaApple) != expanded.end());
  EXPECT_TRUE(std::find(expanded.begin(), expanded.end(), kKanjiApple) != expanded.end());
}

// TC-SD-04: Full-width ASCII is folded to half-width + lowercased; a katakana
// kana phrase and lowercase "database" remain distinct. All three form one
// group.
TEST_F(RealNormalizerSynonymDictionaryTest, NfkcNormalizationWidthAndCase) {
  auto path = CreateTempTSV(std::string(kFullWidthDB) + "\t" + kKatakanaDatabase + "\tdatabase\n");
  SynonymDictionary dict;
  auto result = dict.LoadFromFile(path, Normalizer());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(dict.GroupCount(), 1);
  EXPECT_EQ(dict.TermCount(), 3);

  auto from_db = dict.Expand("db");
  EXPECT_EQ(from_db.size(), 3);
  EXPECT_TRUE(std::find(from_db.begin(), from_db.end(), "db") != from_db.end());
  EXPECT_TRUE(std::find(from_db.begin(), from_db.end(), "database") != from_db.end());
  EXPECT_TRUE(std::find(from_db.begin(), from_db.end(), kKatakanaDatabase) != from_db.end());

  auto from_database = dict.Expand("database");
  EXPECT_EQ(from_database.size(), 3);
  EXPECT_TRUE(std::find(from_database.begin(), from_database.end(), "db") != from_database.end());
  EXPECT_TRUE(std::find(from_database.begin(), from_database.end(), kKatakanaDatabase) != from_database.end());
}

// TC-SD-05: Half-width kana + full-width katakana normalize to the same token
// and get deduped; the surviving pair {データベース, db} forms the group.
TEST_F(RealNormalizerSynonymDictionaryTest, NfkcTripleEquivalence) {
  auto path = CreateTempTSV(std::string(kHalfKanaDatabase) + "\t" + kKatakanaDatabase + "\tDB\n");
  SynonymDictionary dict;
  auto result = dict.LoadFromFile(path, Normalizer());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(dict.GroupCount(), 1);
  EXPECT_EQ(dict.TermCount(), 2);

  auto from_katakana = dict.Expand(kKatakanaDatabase);
  EXPECT_EQ(from_katakana.size(), 2);
  EXPECT_TRUE(std::find(from_katakana.begin(), from_katakana.end(), kKatakanaDatabase) != from_katakana.end());
  EXPECT_TRUE(std::find(from_katakana.begin(), from_katakana.end(), "db") != from_katakana.end());

  auto from_db = dict.Expand("db");
  EXPECT_EQ(from_db.size(), 2);
  EXPECT_TRUE(std::find(from_db.begin(), from_db.end(), kKatakanaDatabase) != from_db.end());
  EXPECT_TRUE(std::find(from_db.begin(), from_db.end(), "db") != from_db.end());
}

// TC-SD-06: Two independent mixed-script groups coexist without cross-talk.
TEST_F(RealNormalizerSynonymDictionaryTest, MultipleNfkcGroupsWithMixedScript) {
  auto path = CreateTempTSV(std::string(kHiraganaApple) + "\t" + kKatakanaApple + "\t" + kKanjiApple + "\n" +
                            kKatakanaServer + "\tserver\n");
  SynonymDictionary dict;
  auto result = dict.LoadFromFile(path, Normalizer());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(dict.GroupCount(), 2);
  EXPECT_EQ(dict.TermCount(), 5);

  auto fruit = dict.Expand(kKanjiApple);
  EXPECT_EQ(fruit.size(), 3);
  EXPECT_TRUE(std::find(fruit.begin(), fruit.end(), kKanjiApple) != fruit.end());
  EXPECT_TRUE(std::find(fruit.begin(), fruit.end(), kHiraganaApple) != fruit.end());
  EXPECT_TRUE(std::find(fruit.begin(), fruit.end(), kKatakanaApple) != fruit.end());
  EXPECT_TRUE(std::find(fruit.begin(), fruit.end(), "server") == fruit.end());
  EXPECT_TRUE(std::find(fruit.begin(), fruit.end(), kKatakanaServer) == fruit.end());

  auto server = dict.Expand("server");
  EXPECT_EQ(server.size(), 2);
  EXPECT_TRUE(std::find(server.begin(), server.end(), "server") != server.end());
  EXPECT_TRUE(std::find(server.begin(), server.end(), kKatakanaServer) != server.end());
  EXPECT_TRUE(std::find(server.begin(), server.end(), kKanjiApple) == server.end());
}

// TC-SD-07: When a later group would reuse a term from an earlier group, the
// existing loader keeps the first group and drops the second entirely (since
// after filtering only one new term remains).
TEST_F(RealNormalizerSynonymDictionaryTest, FirstEntryWinsWhenTermConflictsAcrossGroups) {
  auto path = CreateTempTSV(std::string(kHiraganaApple) + "\t" + kKatakanaApple + "\n" + kKatakanaApple + "\t" +
                            kKanjiApple + "\n");
  SynonymDictionary dict;
  auto result = dict.LoadFromFile(path, Normalizer());
  ASSERT_TRUE(result.has_value());
  EXPECT_EQ(dict.GroupCount(), 1);
  EXPECT_EQ(dict.TermCount(), 2);

  // 林檎 never entered any group, so Expand returns the singleton.
  auto kanji = dict.Expand(kKanjiApple);
  ASSERT_EQ(kanji.size(), 1);
  EXPECT_EQ(kanji[0], kKanjiApple);

  auto hira = dict.Expand(kHiraganaApple);
  EXPECT_EQ(hira.size(), 2);
  EXPECT_TRUE(std::find(hira.begin(), hira.end(), kHiraganaApple) != hira.end());
  EXPECT_TRUE(std::find(hira.begin(), hira.end(), kKatakanaApple) != hira.end());
  EXPECT_TRUE(std::find(hira.begin(), hira.end(), kKanjiApple) == hira.end());
}
