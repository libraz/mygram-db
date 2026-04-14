/**
 * @file synonym_dictionary_test.cpp
 * @brief Tests for SynonymDictionary class
 */

#include "query/synonym_dictionary.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <sstream>

using mygramdb::query::SynonymDictionary;

namespace {

// Helper to create a temp TSV file
std::string CreateTempTSV(const std::string& content) {
  auto dir = std::filesystem::temp_directory_path() / "mygramdb_synonym_test";
  std::filesystem::create_directories(dir);
  auto path = dir / "synonyms.tsv";
  std::ofstream ofs(path);
  ofs << content;
  return path.string();
}

void CleanupTempDir() {
  std::filesystem::remove_all(std::filesystem::temp_directory_path() / "mygramdb_synonym_test");
}

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

}  // namespace

class SynonymDictionaryTest : public ::testing::Test {
 protected:
  void TearDown() override { CleanupTempDir(); }
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
