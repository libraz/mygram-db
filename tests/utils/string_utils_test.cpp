/**
 * @file string_utils_test.cpp
 * @brief Unit tests for string utility functions
 */

#include "utils/string_utils.h"
#include <gtest/gtest.h>

using namespace mygramdb::utils;

/**
 * @brief Test UTF-8 to codepoints conversion
 */
TEST(StringUtilsTest, Utf8ToCodepoints) {
  // ASCII
  auto codepoints = Utf8ToCodepoints("abc");
  ASSERT_EQ(codepoints.size(), 3);
  EXPECT_EQ(codepoints[0], 0x61);  // 'a'
  EXPECT_EQ(codepoints[1], 0x62);  // 'b'
  EXPECT_EQ(codepoints[2], 0x63);  // 'c'

  // Japanese (Hiragana)
  codepoints = Utf8ToCodepoints("あい");
  ASSERT_EQ(codepoints.size(), 2);
  EXPECT_EQ(codepoints[0], 0x3042);  // 'あ'
  EXPECT_EQ(codepoints[1], 0x3044);  // 'い'

  // Mixed
  codepoints = Utf8ToCodepoints("aあb");
  ASSERT_EQ(codepoints.size(), 3);
  EXPECT_EQ(codepoints[0], 0x61);    // 'a'
  EXPECT_EQ(codepoints[1], 0x3042);  // 'あ'
  EXPECT_EQ(codepoints[2], 0x62);    // 'b'

  // Empty
  codepoints = Utf8ToCodepoints("");
  EXPECT_EQ(codepoints.size(), 0);
}

/**
 * @brief Test codepoints to UTF-8 conversion
 */
TEST(StringUtilsTest, CodepointsToUtf8) {
  // ASCII
  std::string text = CodepointsToUtf8({0x61, 0x62, 0x63});
  EXPECT_EQ(text, "abc");

  // Japanese
  text = CodepointsToUtf8({0x3042, 0x3044});
  EXPECT_EQ(text, "あい");

  // Mixed
  text = CodepointsToUtf8({0x61, 0x3042, 0x62});
  EXPECT_EQ(text, "aあb");

  // Empty
  text = CodepointsToUtf8({});
  EXPECT_EQ(text, "");
}

/**
 * @brief Test round-trip conversion
 */
TEST(StringUtilsTest, RoundTrip) {
  std::string original = "Hello世界ライブ";
  auto codepoints = Utf8ToCodepoints(original);
  std::string result = CodepointsToUtf8(codepoints);
  EXPECT_EQ(result, original);
}

/**
 * @brief Test unigram generation
 */
TEST(StringUtilsTest, GenerateUnigramsASCII) {
  auto ngrams = GenerateNgrams("abc", 1);
  ASSERT_EQ(ngrams.size(), 3);
  EXPECT_EQ(ngrams[0], "a");
  EXPECT_EQ(ngrams[1], "b");
  EXPECT_EQ(ngrams[2], "c");
}

/**
 * @brief Test unigram generation for Japanese text
 */
TEST(StringUtilsTest, GenerateUnigramsJapanese) {
  auto ngrams = GenerateNgrams("ライブ", 1);
  ASSERT_EQ(ngrams.size(), 3);
  EXPECT_EQ(ngrams[0], "ラ");
  EXPECT_EQ(ngrams[1], "イ");
  EXPECT_EQ(ngrams[2], "ブ");
}

/**
 * @brief Test bigram generation
 */
TEST(StringUtilsTest, GenerateBigrams) {
  auto ngrams = GenerateNgrams("abc", 2);
  ASSERT_EQ(ngrams.size(), 2);
  EXPECT_EQ(ngrams[0], "ab");
  EXPECT_EQ(ngrams[1], "bc");
}

/**
 * @brief Test bigram generation for Japanese
 */
TEST(StringUtilsTest, GenerateBigramsJapanese) {
  auto ngrams = GenerateNgrams("ライブ", 2);
  ASSERT_EQ(ngrams.size(), 2);
  EXPECT_EQ(ngrams[0], "ライ");
  EXPECT_EQ(ngrams[1], "イブ");
}

/**
 * @brief Test empty string
 */
TEST(StringUtilsTest, GenerateNgramsEmpty) {
  auto ngrams = GenerateNgrams("", 1);
  EXPECT_EQ(ngrams.size(), 0);
}

/**
 * @brief Test string shorter than n
 */
TEST(StringUtilsTest, GenerateNgramsTooShort) {
  auto ngrams = GenerateNgrams("a", 2);
  EXPECT_EQ(ngrams.size(), 0);
}

/**
 * @brief Test text normalization (basic lowercase)
 */
TEST(StringUtilsTest, NormalizeTextLowercase) {
  // ASCII lowercase
  std::string normalized = NormalizeText("ABC", false, "keep", true);
  EXPECT_EQ(normalized, "abc");

  // No lowercase
  normalized = NormalizeText("ABC", false, "keep", false);
  EXPECT_EQ(normalized, "ABC");
}

#ifdef USE_ICU
/**
 * @brief Test NFKC normalization with ICU
 *
 * NFKC (Normalization Form KC) is Compatibility Decomposition, followed by
 * Canonical Composition. It normalizes compatibility characters.
 */
TEST(StringUtilsTest, NormalizeTextNFKC) {
  // Full-width ASCII to half-width (compatibility normalization)
  // "ＡＢＣ" (U+FF21, U+FF22, U+FF23) -> "ABC" (U+0041, U+0042, U+0043)
  std::string normalized = NormalizeText("ＡＢＣ", true, "keep", false);
  EXPECT_EQ(normalized, "ABC");

  // Ligature decomposition: "ﬁ" (U+FB01) -> "fi" (U+0066, U+0069)
  normalized = NormalizeText("ﬁle", true, "keep", false);
  EXPECT_EQ(normalized, "file");

  // Circled numbers: "①②③" -> "123"
  normalized = NormalizeText("①②③", true, "keep", false);
  EXPECT_EQ(normalized, "123");

  // Half-width katakana to full-width: "ｱｲｳ" -> "アイウ"
  normalized = NormalizeText("ｱｲｳ", true, "keep", false);
  EXPECT_EQ(normalized, "アイウ");
}

/**
 * @brief Test width conversion with ICU
 */
TEST(StringUtilsTest, NormalizeTextWidthConversion) {
  // Full-width to half-width (narrow)
  // "ＡＢＣ" -> "ABC"
  std::string normalized = NormalizeText("ＡＢＣ", false, "narrow", false);
  EXPECT_EQ(normalized, "ABC");

  // Full-width digits to half-width
  normalized = NormalizeText("１２３", false, "narrow", false);
  EXPECT_EQ(normalized, "123");

  // Half-width to full-width (wide)
  // "ABC" -> "ＡＢＣ"
  normalized = NormalizeText("ABC", false, "wide", false);
  EXPECT_EQ(normalized, "ＡＢＣ");

  // Half-width digits to full-width
  normalized = NormalizeText("123", false, "wide", false);
  EXPECT_EQ(normalized, "１２３");

  // Keep original width
  normalized = NormalizeText("ABC", false, "keep", false);
  EXPECT_EQ(normalized, "ABC");
}

/**
 * @brief Test combined normalization: NFKC + width + lowercase
 */
TEST(StringUtilsTest, NormalizeTextCombined) {
  // Full-width "ＡＢＣ" -> NFKC -> narrow -> lowercase -> "abc"
  std::string normalized = NormalizeText("ＡＢＣ", true, "narrow", true);
  EXPECT_EQ(normalized, "abc");

  // NFKC normalizes half-width katakana to full-width katakana
  // Full-width ASCII is converted to half-width by NFKC
  // "ｱｲｳＡＢＣ" -> NFKC -> "アイウABC"
  // Note: Half-width katakana (ｱｲｳ) becomes full-width (アイウ) via NFKC
  // Full-width ASCII (ＡＢＣ) becomes half-width (ABC) via NFKC
  normalized = NormalizeText("ｱｲｳＡＢＣ", true, "keep", false);
  EXPECT_EQ(normalized, "アイウABC");

  // With lowercase
  normalized = NormalizeText("ｱｲｳＡＢＣ", true, "keep", true);
  EXPECT_EQ(normalized, "アイウabc");
}

/**
 * @brief Test Japanese text normalization for search
 *
 * This is a realistic test case for Japanese text search.
 */
TEST(StringUtilsTest, NormalizeTextJapaneseSearch) {
  // Normalize "ライブ" (full-width katakana) for search
  // NFKC keeps full-width katakana as-is
  std::string normalized = NormalizeText("ライブ", true, "keep", false);
  EXPECT_EQ(normalized, "ライブ");  // Full-width katakana stays as-is

  // Normalize "ﾗｲﾌﾞ" (half-width katakana) for search
  // NFKC converts half-width katakana to full-width katakana
  normalized = NormalizeText("ﾗｲﾌﾞ", true, "keep", false);
  EXPECT_EQ(normalized, "ライブ");  // Half-width -> full-width via NFKC

  // Both should normalize to the same form for matching
  std::string text1 = NormalizeText("ライブ", true, "keep", false);
  std::string text2 = NormalizeText("ﾗｲﾌﾞ", true, "keep", false);
  EXPECT_EQ(text1, text2);
}

/**
 * @brief Test lowercase conversion for Japanese text
 */
TEST(StringUtilsTest, NormalizeTextJapaneseLowercase) {
  // Mixed ASCII + Japanese with lowercase
  // NFKC converts full-width ASCII to half-width
  std::string normalized = NormalizeText("ＡＢＣあいう", true, "keep", true);
  EXPECT_EQ(normalized, "abcあいう");

  // Katakana should not be affected by lowercase
  normalized = NormalizeText("ライブ", true, "keep", true);
  EXPECT_EQ(normalized, "ライブ");
}

/**
 * @brief Test edge cases for normalization
 */
TEST(StringUtilsTest, NormalizeTextEdgeCases) {
  // Empty string
  std::string normalized = NormalizeText("", true, "narrow", true);
  EXPECT_EQ(normalized, "");

  // Single character
  normalized = NormalizeText("Ａ", true, "narrow", true);
  EXPECT_EQ(normalized, "a");

  // Spaces and punctuation
  normalized = NormalizeText("　！？", true, "narrow", false);
  EXPECT_EQ(normalized, " !?");  // Full-width space/punctuation to half-width
}
#endif  // USE_ICU
