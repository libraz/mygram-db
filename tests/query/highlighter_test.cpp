/**
 * @file highlighter_test.cpp
 * @brief Tests for search result highlighter
 */

#include "query/highlighter.h"

#include <gtest/gtest.h>

namespace mygramdb::query {
namespace {

class HighlighterTest : public ::testing::Test {
 protected:
  HighlightOptions default_opts_;
};

// --- FindMatchPositions ---

TEST_F(HighlighterTest, FindMatchPositions_SingleTerm) {
  auto positions = Highlighter::FindMatchPositions("hello world hello", {"hello"});
  ASSERT_EQ(positions.size(), 2);
  EXPECT_EQ(positions[0].first, 0);
  EXPECT_EQ(positions[0].second, 5);
  EXPECT_EQ(positions[1].first, 12);
  EXPECT_EQ(positions[1].second, 17);
}

TEST_F(HighlighterTest, FindMatchPositions_MultipleTerms) {
  auto positions = Highlighter::FindMatchPositions("foo bar baz", {"foo", "baz"});
  ASSERT_EQ(positions.size(), 2);
  EXPECT_EQ(positions[0].first, 0);
  EXPECT_EQ(positions[0].second, 3);
  EXPECT_EQ(positions[1].first, 8);
  EXPECT_EQ(positions[1].second, 11);
}

TEST_F(HighlighterTest, FindMatchPositions_NoMatch) {
  auto positions = Highlighter::FindMatchPositions("hello world", {"xyz"});
  EXPECT_TRUE(positions.empty());
}

TEST_F(HighlighterTest, FindMatchPositions_OverlappingMatches) {
  // "aa" in "aaa" should only match once at position 0 (non-overlapping)
  auto positions = Highlighter::FindMatchPositions("aaa", {"aa"});
  ASSERT_EQ(positions.size(), 1);
  EXPECT_EQ(positions[0].first, 0);
  EXPECT_EQ(positions[0].second, 2);
}

TEST_F(HighlighterTest, FindMatchPositions_OverlappingSameStartKeepsLongest) {
  auto positions = Highlighter::FindMatchPositions("hello world", {"he", "hello"});
  ASSERT_EQ(positions.size(), 1);
  EXPECT_EQ(positions[0].first, 0);
  EXPECT_EQ(positions[0].second, 5);
}

TEST_F(HighlighterTest, FindMatchPositions_UTF8) {
  // Japanese text: "東京タワー" (5 code points)
  auto positions = Highlighter::FindMatchPositions("東京タワー", {"東京"});
  ASSERT_EQ(positions.size(), 1);
  EXPECT_EQ(positions[0].first, 0);
  EXPECT_EQ(positions[0].second, 2);
}

TEST_F(HighlighterTest, FindMatchPositions_EmptyTerm) {
  auto positions = Highlighter::FindMatchPositions("hello", {""});
  EXPECT_TRUE(positions.empty());
}

TEST_F(HighlighterTest, FindMatchPositions_EmptyText) {
  auto positions = Highlighter::FindMatchPositions("", {"hello"});
  EXPECT_TRUE(positions.empty());
}

// --- Generate ---

TEST_F(HighlighterTest, Generate_BasicHighlight) {
  auto result = Highlighter::Generate("hello world", {"hello"}, default_opts_);
  EXPECT_EQ(result.snippet, "<em>hello</em> world");
}

TEST_F(HighlighterTest, Generate_MultipleTerms) {
  auto result = Highlighter::Generate("hello beautiful world", {"hello", "world"}, default_opts_);
  EXPECT_EQ(result.snippet, "<em>hello</em> beautiful <em>world</em>");
}

TEST_F(HighlighterTest, Generate_NoMatch) {
  HighlightOptions opts;
  opts.snippet_length = 10;
  auto result = Highlighter::Generate("hello beautiful world", {"xyz"}, opts);
  // Should return prefix with ellipsis
  EXPECT_EQ(result.snippet, "hello beau...");
}

TEST_F(HighlighterTest, Generate_EmptyText) {
  auto result = Highlighter::Generate("", {"hello"}, default_opts_);
  EXPECT_EQ(result.snippet, "");
}

TEST_F(HighlighterTest, Generate_EmptyTerms) {
  auto result = Highlighter::Generate("hello world", {}, default_opts_);
  EXPECT_EQ(result.snippet, "hello world");
}

TEST_F(HighlighterTest, Generate_CustomTags) {
  HighlightOptions opts;
  opts.open_tag = "<b>";
  opts.close_tag = "</b>";
  auto result = Highlighter::Generate("hello world", {"hello"}, opts);
  EXPECT_EQ(result.snippet, "<b>hello</b> world");
}

TEST_F(HighlighterTest, Generate_ShortSnippet) {
  HighlightOptions opts;
  opts.snippet_length = 10;
  // Long text with match in the middle
  std::string text = "aaaaaaaaaa bbbbbbbbbb cccc dddd eeeeeeeeee";
  auto result = Highlighter::Generate(text, {"cccc"}, opts);
  // Should have ellipsis before and after the context window
  EXPECT_NE(result.snippet.find("<em>cccc</em>"), std::string::npos);
  EXPECT_NE(result.snippet.find("..."), std::string::npos);
}

TEST_F(HighlighterTest, Generate_UTF8Highlight) {
  auto result = Highlighter::Generate("東京タワーは東京にある", {"東京"}, default_opts_);
  // Both occurrences should be highlighted
  auto first = result.snippet.find("<em>東京</em>");
  ASSERT_NE(first, std::string::npos);
  auto second = result.snippet.find("<em>東京</em>", first + 1);
  EXPECT_NE(second, std::string::npos);
}

TEST_F(HighlighterTest, Generate_MultipleFragments) {
  HighlightOptions opts;
  opts.snippet_length = 6;  // Very short context → separate fragments
  opts.max_fragments = 2;
  // Matches far apart to force separate fragments
  std::string text(200, 'x');
  text[0] = 'A';
  text[199] = 'B';
  auto result = Highlighter::Generate(text, {"A", "B"}, opts);
  // Should have "..." separator between fragments
  EXPECT_NE(result.snippet.find("<em>A</em>"), std::string::npos);
  EXPECT_NE(result.snippet.find("..."), std::string::npos);
}

TEST_F(HighlighterTest, Generate_MaxFragmentsLimit) {
  HighlightOptions opts;
  opts.snippet_length = 4;
  opts.max_fragments = 1;
  // Two matches far apart, but only 1 fragment allowed
  std::string text = "alpha " + std::string(200, 'x') + " beta";
  auto result = Highlighter::Generate(text, {"alpha", "beta"}, opts);
  // Should only contain the first match's fragment
  EXPECT_NE(result.snippet.find("<em>alpha</em>"), std::string::npos);
}

TEST_F(HighlighterTest, Generate_MatchAtBeginning) {
  HighlightOptions opts;
  opts.snippet_length = 20;
  auto result = Highlighter::Generate("keyword at the start", {"keyword"}, opts);
  // No leading ellipsis since match starts at beginning
  EXPECT_EQ(result.snippet.substr(0, 4), "<em>");
}

TEST_F(HighlighterTest, Generate_MatchAtEnd) {
  HighlightOptions opts;
  opts.snippet_length = 20;
  auto result = Highlighter::Generate("at the end keyword", {"keyword"}, opts);
  // No trailing ellipsis since match is at the end
  auto pos = result.snippet.rfind("</em>");
  ASSERT_NE(pos, std::string::npos);
  EXPECT_EQ(result.snippet.substr(pos), "</em>");
}

TEST_F(HighlighterTest, Generate_WholeTextFitsInSnippet) {
  HighlightOptions opts;
  opts.snippet_length = 200;  // Much larger than text
  auto result = Highlighter::Generate("short text with keyword", {"keyword"}, opts);
  // No ellipsis needed
  EXPECT_EQ(result.snippet.find("..."), std::string::npos);
  EXPECT_NE(result.snippet.find("<em>keyword</em>"), std::string::npos);
}

// ============================================================================
// Bug fix regression tests: incremental codepoint counting
// ============================================================================

/**
 * @brief Regression test: FindMatchPositions with many matches is efficient
 *
 * Verifies that incremental codepoint counting produces correct results
 * for text with many repeated matches (the optimization target).
 */
TEST_F(HighlighterTest, FindMatchPositions_ManyMatches) {
  // Create text with "ab" repeated many times
  std::string text;
  for (int i = 0; i < 100; ++i) {
    text += "ab ";
  }
  // Remove trailing space
  text.pop_back();

  auto positions = Highlighter::FindMatchPositions(text, {"ab"});
  EXPECT_EQ(positions.size(), 100u);

  // Verify first and last positions are correct
  EXPECT_EQ(positions[0].first, 0u);
  EXPECT_EQ(positions[0].second, 2u);
  // Last "ab" starts at position 99*3 = 297 codepoints
  EXPECT_EQ(positions[99].first, 297u);
  EXPECT_EQ(positions[99].second, 299u);
}

/**
 * @brief Regression test: FindMatchPositions with multi-byte UTF-8
 *
 * Ensures the incremental codepoint counter handles multi-byte sequences correctly.
 */
TEST_F(HighlighterTest, FindMatchPositions_MultiByteManyMatches) {
  // "あいう" repeated with a separator
  std::string text;
  for (int i = 0; i < 10; ++i) {
    if (i > 0)
      text += " ";
    text += "あいう";
  }

  auto positions = Highlighter::FindMatchPositions(text, {"あいう"});
  EXPECT_EQ(positions.size(), 10u);

  // First match at codepoint 0-3
  EXPECT_EQ(positions[0].first, 0u);
  EXPECT_EQ(positions[0].second, 3u);

  // Second match at codepoint 4-7 (3 chars + 1 space)
  EXPECT_EQ(positions[1].first, 4u);
  EXPECT_EQ(positions[1].second, 7u);
}

}  // namespace
}  // namespace mygramdb::query
