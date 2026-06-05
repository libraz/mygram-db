/**
 * @file string_utils_test.cpp
 * @brief Unit tests for string utility functions
 */

#include "utils/string_utils.h"

#include <gtest/gtest.h>

#include <cerrno>

#include "utils/errno_utils.h"
#include "utils/numeric_parse.h"

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

/**
 * @brief Test 4-byte UTF-8 characters (emojis)
 */
TEST(StringUtilsTest, FourByteEmoji) {
  // Single emoji (U+1F600 - 😀)
  auto codepoints = Utf8ToCodepoints("😀");
  ASSERT_EQ(codepoints.size(), 1);
  EXPECT_EQ(codepoints[0], 0x1F600);

  // Round trip
  std::string emoji = CodepointsToUtf8({0x1F600});
  EXPECT_EQ(emoji, "😀");

  // Multiple emojis
  codepoints = Utf8ToCodepoints("😀🎉👍");
  ASSERT_EQ(codepoints.size(), 3);
  EXPECT_EQ(codepoints[0], 0x1F600);  // 😀
  EXPECT_EQ(codepoints[1], 0x1F389);  // 🎉
  EXPECT_EQ(codepoints[2], 0x1F44D);  // 👍
}

/**
 * @brief Test emoji with text
 */
TEST(StringUtilsTest, EmojiWithText) {
  // Mixed: ASCII + Japanese + emoji
  auto codepoints = Utf8ToCodepoints("Hello😀世界🎉");
  ASSERT_EQ(codepoints.size(), 9);    // H e l l o 😀 世 界 🎉
  EXPECT_EQ(codepoints[0], 0x48);     // 'H'
  EXPECT_EQ(codepoints[5], 0x1F600);  // 😀
  EXPECT_EQ(codepoints[6], 0x4E16);   // 世
  EXPECT_EQ(codepoints[7], 0x754C);   // 界
  EXPECT_EQ(codepoints[8], 0x1F389);  // 🎉

  // Round trip
  std::string text = "Hello😀世界🎉";
  auto cp = Utf8ToCodepoints(text);
  std::string result = CodepointsToUtf8(cp);
  EXPECT_EQ(result, text);
}

/**
 * @brief Test emoji unigram generation
 */
TEST(StringUtilsTest, EmojiUnigrams) {
  auto ngrams = GenerateNgrams("😀🎉👍", 1);
  ASSERT_EQ(ngrams.size(), 3);
  EXPECT_EQ(ngrams[0], "😀");
  EXPECT_EQ(ngrams[1], "🎉");
  EXPECT_EQ(ngrams[2], "👍");
}

/**
 * @brief Test emoji bigram generation
 */
TEST(StringUtilsTest, EmojiBigrams) {
  auto ngrams = GenerateNgrams("😀🎉👍", 2);
  ASSERT_EQ(ngrams.size(), 2);
  EXPECT_EQ(ngrams[0], "😀🎉");
  EXPECT_EQ(ngrams[1], "🎉👍");
}

/**
 * @brief Test emoji with mixed text ngrams
 */
TEST(StringUtilsTest, EmojiMixedNgrams) {
  // "Hello😀" - unigrams
  auto ngrams = GenerateNgrams("Hello😀", 1);
  ASSERT_EQ(ngrams.size(), 6);
  EXPECT_EQ(ngrams[0], "H");
  EXPECT_EQ(ngrams[1], "e");
  EXPECT_EQ(ngrams[2], "l");
  EXPECT_EQ(ngrams[3], "l");
  EXPECT_EQ(ngrams[4], "o");
  EXPECT_EQ(ngrams[5], "😀");

  // "日本😀語" - bigrams
  ngrams = GenerateNgrams("日本😀語", 2);
  ASSERT_EQ(ngrams.size(), 3);
  EXPECT_EQ(ngrams[0], "日本");
  EXPECT_EQ(ngrams[1], "本😀");
  EXPECT_EQ(ngrams[2], "😀語");
}

/**
 * @brief Test complex emoji (with ZWJ - Zero Width Joiner)
 */
TEST(StringUtilsTest, ComplexEmoji) {
  // Family emoji: 👨‍👩‍👧‍👦 (U+1F468 U+200D U+1F469 U+200D U+1F467 U+200D U+1F466)
  // This is actually multiple codepoints joined with ZWJ (U+200D)
  std::string family = "👨‍👩‍👧‍👦";
  auto codepoints = Utf8ToCodepoints(family);

  // Should have 7 codepoints: man, ZWJ, woman, ZWJ, girl, ZWJ, boy
  EXPECT_GE(codepoints.size(), 4);  // At least the base emojis

  // Round trip should preserve the emoji
  std::string result = CodepointsToUtf8(codepoints);
  EXPECT_EQ(result, family);
}

/**
 * @brief Test emoji with skin tone modifiers
 */
TEST(StringUtilsTest, EmojiSkinTone) {
  // Thumbs up with medium skin tone: 👍🏽 (U+1F44D U+1F3FD)
  std::string thumbs = "👍🏽";
  auto codepoints = Utf8ToCodepoints(thumbs);

  // Should have 2 codepoints: thumbs up + skin tone modifier
  ASSERT_EQ(codepoints.size(), 2);
  EXPECT_EQ(codepoints[0], 0x1F44D);  // 👍
  EXPECT_EQ(codepoints[1], 0x1F3FD);  // 🏽 (medium skin tone)

  // Round trip
  std::string result = CodepointsToUtf8(codepoints);
  EXPECT_EQ(result, thumbs);
}

#ifdef USE_ICU
/**
 * @brief Test emoji normalization
 */
TEST(StringUtilsTest, EmojiNormalization) {
  // Emojis should pass through normalization unchanged
  std::string normalized = NormalizeText("Hello😀世界🎉", true, "keep", true);
  EXPECT_EQ(normalized, "hello😀世界🎉");  // Only ASCII lowercased

  // Emoji with Japanese text
  normalized = NormalizeText("ライブ😀楽しい🎉", true, "keep", false);
  EXPECT_EQ(normalized, "ライブ😀楽しい🎉");  // Emojis preserved
}
#endif  // USE_ICU

/**
 * @brief Test hybrid n-grams: ASCII only text
 *
 * For ASCII-only text with ascii_ngram_size=2, should generate bigrams
 */
TEST(StringUtilsTest, GenerateHybridNgramsASCIIOnly) {
  // ascii_ngram_size=2, kanji_ngram_size=1
  auto ngrams = GenerateHybridNgrams("hello", 2, 1);

  // Should generate bigrams: "he", "el", "ll", "lo"
  ASSERT_EQ(ngrams.size(), 4);
  EXPECT_EQ(ngrams[0], "he");
  EXPECT_EQ(ngrams[1], "el");
  EXPECT_EQ(ngrams[2], "ll");
  EXPECT_EQ(ngrams[3], "lo");
}

/**
 * @brief Test hybrid n-grams: CJK only text with unigrams
 *
 * For CJK-only text with kanji_ngram_size=1, should generate unigrams
 */
TEST(StringUtilsTest, GenerateHybridNgramsKanjiUnigrams) {
  // ascii_ngram_size=2, kanji_ngram_size=1
  auto ngrams = GenerateHybridNgrams("東方艦", 2, 1);

  // Should generate unigrams: "東", "方", "艦"
  ASSERT_EQ(ngrams.size(), 3);
  EXPECT_EQ(ngrams[0], "東");
  EXPECT_EQ(ngrams[1], "方");
  EXPECT_EQ(ngrams[2], "艦");
}

/**
 * @brief Test hybrid n-grams: CJK only text with bigrams
 *
 * For CJK-only text with kanji_ngram_size=2, should generate bigrams
 */
TEST(StringUtilsTest, GenerateHybridNgramsKanjiBigrams) {
  // ascii_ngram_size=2, kanji_ngram_size=2
  auto ngrams = GenerateHybridNgrams("東方艦", 2, 2);

  // Should generate bigrams: "東方", "方艦"
  ASSERT_EQ(ngrams.size(), 2);
  EXPECT_EQ(ngrams[0], "東方");
  EXPECT_EQ(ngrams[1], "方艦");
}

/**
 * @brief Test hybrid n-grams: Mixed CJK/ASCII text
 *
 * CRITICAL TEST: Mixed text should NOT create cross-boundary n-grams
 * "東方Project" should generate:
 * - CJK unigrams: "東", "方"
 * - ASCII bigrams: "Pr", "ro", "oj", "je", "ec", "ct"
 * - NO mixed n-grams like "方P" or "tP"
 */
TEST(StringUtilsTest, GenerateHybridNgramsMixedText) {
  // ascii_ngram_size=2, kanji_ngram_size=1
  auto ngrams = GenerateHybridNgrams("東方Project", 2, 1);

  // Expected n-grams:
  // - CJK unigrams: "東", "方"
  // - ASCII bigrams: "Pr", "ro", "oj", "je", "ec", "ct"
  // Total: 2 + 6 = 8
  ASSERT_EQ(ngrams.size(), 8);

  // Verify CJK unigrams
  EXPECT_EQ(ngrams[0], "東");
  EXPECT_EQ(ngrams[1], "方");

  // Verify ASCII bigrams
  EXPECT_EQ(ngrams[2], "Pr");
  EXPECT_EQ(ngrams[3], "ro");
  EXPECT_EQ(ngrams[4], "oj");
  EXPECT_EQ(ngrams[5], "je");
  EXPECT_EQ(ngrams[6], "ec");
  EXPECT_EQ(ngrams[7], "ct");

  // Verify NO mixed n-grams (critical!)
  for (const auto& ngram : ngrams) {
    // Check if ngram contains both Kanji and non-Kanji characters
    bool has_kanji = false;
    bool has_non_kanji = false;

    auto codepoints = Utf8ToCodepoints(ngram);
    for (uint32_t cp : codepoints) {
      // Kanji (CJK Ideographs only)
      if (cp >= 0x4E00 && cp <= 0x9FFF) {  // CJK Ideographs (main block)
        has_kanji = true;
      } else {
        // Non-Kanji (ASCII, Hiragana, Katakana, etc.)
        has_non_kanji = true;
      }
    }

    // N-gram should be either purely Kanji or purely non-Kanji, not mixed
    EXPECT_FALSE(has_kanji && has_non_kanji) << "Found mixed Kanji/non-Kanji n-gram: " << ngram;
  }
}

/**
 * @brief Test hybrid n-grams: Kanji + ASCII boundaries
 *
 * Test "艦隊ABC" to ensure Kanji/ASCII boundaries are respected
 */
TEST(StringUtilsTest, GenerateHybridNgramsMixedBoundaries) {
  // ascii_ngram_size=2, kanji_ngram_size=1
  auto ngrams = GenerateHybridNgrams("艦隊ABC", 2, 1);

  // "艦" (U+8266) - CJK Ideograph (Kanji)
  // "隊" (U+968A) - CJK Ideograph (Kanji)
  // "ABC" - ASCII

  // Expected:
  // - Kanji unigrams: "艦", "隊"
  // - ASCII bigrams: "AB", "BC"
  // Total: 2 + 2 = 4

  ASSERT_EQ(ngrams.size(), 4);
  EXPECT_EQ(ngrams[0], "艦");
  EXPECT_EQ(ngrams[1], "隊");
  EXPECT_EQ(ngrams[2], "AB");
  EXPECT_EQ(ngrams[3], "BC");
}

/**
 * @brief Test hybrid n-grams: Hiragana should use bigrams (ascii_ngram_size=2)
 *
 * Hiragana are NOT CJK Ideographs, so they should be processed with ascii_ngram_size
 */
TEST(StringUtilsTest, GenerateHybridNgramsHiraganaBigrams) {
  // ascii_ngram_size=2, kanji_ngram_size=1
  auto ngrams = GenerateHybridNgrams("これは", 2, 1);

  // "これは" (all Hiragana)
  // Expected bigrams: "これ", "れは"
  ASSERT_EQ(ngrams.size(), 2);
  EXPECT_EQ(ngrams[0], "これ");
  EXPECT_EQ(ngrams[1], "れは");
}

/**
 * @brief Test hybrid n-grams: Katakana should use bigrams (ascii_ngram_size=2)
 *
 * Katakana are NOT CJK Ideographs, so they should be processed with ascii_ngram_size
 */
TEST(StringUtilsTest, GenerateHybridNgramsKatakanaBigrams) {
  // ascii_ngram_size=2, kanji_ngram_size=1
  auto ngrams = GenerateHybridNgrams("ライブ", 2, 1);

  // "ライブ" (all Katakana)
  // Expected bigrams: "ライ", "イブ"
  ASSERT_EQ(ngrams.size(), 2);
  EXPECT_EQ(ngrams[0], "ライ");
  EXPECT_EQ(ngrams[1], "イブ");
}

/**
 * @brief Test hybrid n-grams: Single CJK character
 *
 * CRITICAL TEST FOR THE BUG: Single Kanji should be indexed with kanji_ngram_size=1
 */
TEST(StringUtilsTest, GenerateHybridNgramsSingleKanji) {
  // ascii_ngram_size=2, kanji_ngram_size=1
  auto ngrams = GenerateHybridNgrams("東", 2, 1);

  // Should generate single unigram: "東"
  ASSERT_EQ(ngrams.size(), 1);
  EXPECT_EQ(ngrams[0], "東");

  // Test other single Kanji
  ngrams = GenerateHybridNgrams("艦", 2, 1);
  ASSERT_EQ(ngrams.size(), 1);
  EXPECT_EQ(ngrams[0], "艦");

  ngrams = GenerateHybridNgrams("二", 2, 1);
  ASSERT_EQ(ngrams.size(), 1);
  EXPECT_EQ(ngrams[0], "二");
}

/**
 * @brief Test hybrid n-grams: Two consecutive CJK characters
 *
 * With kanji_ngram_size=1, should generate 2 unigrams
 */
TEST(StringUtilsTest, GenerateHybridNgramsTwoKanji) {
  // ascii_ngram_size=2, kanji_ngram_size=1
  auto ngrams = GenerateHybridNgrams("二次", 2, 1);

  // Should generate unigrams: "二", "次"
  ASSERT_EQ(ngrams.size(), 2);
  EXPECT_EQ(ngrams[0], "二");
  EXPECT_EQ(ngrams[1], "次");
}

/**
 * @brief Test hybrid n-grams: Empty string
 */
TEST(StringUtilsTest, GenerateHybridNgramsEmpty) {
  auto ngrams = GenerateHybridNgrams("", 2, 1);
  EXPECT_EQ(ngrams.size(), 0);
}

/**
 * @brief Test hybrid n-grams: Text too short for n-gram size
 */
TEST(StringUtilsTest, GenerateHybridNgramsTooShort) {
  // Single ASCII character with ascii_ngram_size=2
  auto ngrams = GenerateHybridNgrams("a", 2, 1);
  EXPECT_EQ(ngrams.size(), 0);  // Cannot generate bigram from single char
}

/**
 * @brief Test cross-boundary N-grams enabled (default): CJK to ASCII boundary
 *
 * With cross_boundary_ngrams=true, N-grams spanning CJK/non-CJK boundaries
 * should be generated using the starting character's ngram_size.
 */
TEST(StringUtilsTest, CrossBoundaryNgramsEnabled_CJKToASCII) {
  // "漢字ABC" with kanji_ngram_size=2, ascii_ngram_size=3
  // Position 0: "漢" (CJK), ngram_size=2 -> "漢字" (both CJK, normal)
  // Position 1: "字" (CJK), ngram_size=2 -> "字A" (cross-boundary, generated!)
  // Position 2: "A" (non-CJK), ngram_size=3 -> "ABC" (all non-CJK, normal)
  auto ngrams = GenerateHybridNgrams("漢字ABC", 3, 2, true);

  ASSERT_EQ(ngrams.size(), 3);
  EXPECT_EQ(ngrams[0], "漢字");
  EXPECT_EQ(ngrams[1], "字A");
  EXPECT_EQ(ngrams[2], "ABC");
}

/**
 * @brief Test cross-boundary N-grams enabled: ASCII to CJK boundary
 *
 * "Hello世界" with ascii_ngram_size=3 and kanji_ngram_size=2
 */
TEST(StringUtilsTest, CrossBoundaryNgramsEnabled_ASCIIToCJK) {
  // Position 0: "H" (non-CJK), ngram_size=3 -> "Hel"
  // Position 1: "e" (non-CJK), ngram_size=3 -> "ell"
  // Position 2: "l" (non-CJK), ngram_size=3 -> "llo"
  // Position 3: "l" (non-CJK), ngram_size=3 -> "lo世" (cross-boundary, generated!)
  // Position 4: "o" (non-CJK), ngram_size=3 -> "o世界" (cross-boundary, generated!)
  // Position 5: "世" (CJK), ngram_size=2 -> "世界" (both CJK, normal)
  auto ngrams = GenerateHybridNgrams("Hello世界", 3, 2, true);

  ASSERT_EQ(ngrams.size(), 6);
  EXPECT_EQ(ngrams[0], "Hel");
  EXPECT_EQ(ngrams[1], "ell");
  EXPECT_EQ(ngrams[2], "llo");
  EXPECT_EQ(ngrams[3], "lo世");
  EXPECT_EQ(ngrams[4], "o世界");
  EXPECT_EQ(ngrams[5], "世界");
}

/**
 * @brief Test cross-boundary N-grams disabled: preserves old behavior
 *
 * Same inputs as enabled tests should produce no cross-boundary N-grams.
 */
TEST(StringUtilsTest, CrossBoundaryNgramsDisabled_CJKToASCII) {
  // "漢字ABC" with kanji_ngram_size=2, ascii_ngram_size=3, cross_boundary=false
  // Position 1: "字A" would cross boundary -> rejected
  auto ngrams = GenerateHybridNgrams("漢字ABC", 3, 2, false);

  ASSERT_EQ(ngrams.size(), 2);
  EXPECT_EQ(ngrams[0], "漢字");
  EXPECT_EQ(ngrams[1], "ABC");
}

TEST(StringUtilsTest, CrossBoundaryNgramsDisabled_ASCIIToCJK) {
  // "Hello世界" with ascii_ngram_size=3, kanji_ngram_size=2, cross_boundary=false
  // "lo世" and "o世界" would cross boundary -> rejected
  auto ngrams = GenerateHybridNgrams("Hello世界", 3, 2, false);

  ASSERT_EQ(ngrams.size(), 4);
  EXPECT_EQ(ngrams[0], "Hel");
  EXPECT_EQ(ngrams[1], "ell");
  EXPECT_EQ(ngrams[2], "llo");
  EXPECT_EQ(ngrams[3], "世界");
}

/**
 * @brief Test pure CJK text works the same regardless of cross_boundary setting
 */
TEST(StringUtilsTest, CrossBoundaryNgramsPureCJK) {
  auto ngrams_enabled = GenerateHybridNgrams("東方艦隊", 2, 2, true);
  auto ngrams_disabled = GenerateHybridNgrams("東方艦隊", 2, 2, false);

  ASSERT_EQ(ngrams_enabled.size(), ngrams_disabled.size());
  ASSERT_EQ(ngrams_enabled.size(), 3);
  for (size_t i = 0; i < ngrams_enabled.size(); ++i) {
    EXPECT_EQ(ngrams_enabled[i], ngrams_disabled[i]);
  }
}

/**
 * @brief Test pure ASCII text works the same regardless of cross_boundary setting
 */
TEST(StringUtilsTest, CrossBoundaryNgramsPureASCII) {
  auto ngrams_enabled = GenerateHybridNgrams("hello", 2, 1, true);
  auto ngrams_disabled = GenerateHybridNgrams("hello", 2, 1, false);

  ASSERT_EQ(ngrams_enabled.size(), ngrams_disabled.size());
  ASSERT_EQ(ngrams_enabled.size(), 4);
  for (size_t i = 0; i < ngrams_enabled.size(); ++i) {
    EXPECT_EQ(ngrams_enabled[i], ngrams_disabled[i]);
  }
}

/**
 * @brief Test single CJK char followed by ASCII with cross-boundary enabled
 */
TEST(StringUtilsTest, CrossBoundaryNgramsSingleCJKThenASCII) {
  // "字AB" with kanji_ngram_size=2, ascii_ngram_size=2, cross_boundary=true
  // Position 0: "字" (CJK), ngram_size=2 -> "字A" (cross-boundary, generated!)
  // Position 1: "A" (non-CJK), ngram_size=2 -> "AB"
  auto ngrams = GenerateHybridNgrams("字AB", 2, 2, true);

  ASSERT_EQ(ngrams.size(), 2);
  EXPECT_EQ(ngrams[0], "字A");
  EXPECT_EQ(ngrams[1], "AB");
}

/**
 * @brief Test single ASCII char followed by CJK with cross-boundary enabled
 */
TEST(StringUtilsTest, CrossBoundaryNgramsSingleASCIIThenCJK) {
  // "A漢字" with kanji_ngram_size=2, ascii_ngram_size=2, cross_boundary=true
  // Position 0: "A" (non-CJK), ngram_size=2 -> "A漢" (cross-boundary, generated!)
  // Position 1: "漢" (CJK), ngram_size=2 -> "漢字" (normal)
  auto ngrams = GenerateHybridNgrams("A漢字", 2, 2, true);

  ASSERT_EQ(ngrams.size(), 2);
  EXPECT_EQ(ngrams[0], "A漢");
  EXPECT_EQ(ngrams[1], "漢字");
}

/**
 * @brief Test cross-boundary with kanji_ngram_size=1 (unigrams never cross boundaries)
 */
TEST(StringUtilsTest, CrossBoundaryNgramsUnigrams) {
  // With kanji_ngram_size=1, CJK unigrams can't cross boundaries (size 1)
  // So enabling/disabling cross_boundary has no effect for CJK chars
  auto ngrams_enabled = GenerateHybridNgrams("漢字ABC", 3, 1, true);
  auto ngrams_disabled = GenerateHybridNgrams("漢字ABC", 3, 1, false);

  ASSERT_EQ(ngrams_enabled.size(), ngrams_disabled.size());
  for (size_t i = 0; i < ngrams_enabled.size(); ++i) {
    EXPECT_EQ(ngrams_enabled[i], ngrams_disabled[i]);
  }
}

// ============================================================================
// ToUpper tests
// ============================================================================

TEST(StringUtilsTest, TrimAsciiWhitespaceBasic) {
  EXPECT_EQ(TrimAsciiWhitespace("  hello\t\r\n"), "hello");
  EXPECT_EQ(TrimAsciiWhitespace("\tvalue with spaces\n"), "value with spaces");
  EXPECT_EQ(TrimAsciiWhitespace(""), "");
  EXPECT_EQ(TrimAsciiWhitespace(" \t\r\n"), "");
}

TEST(StringUtilsTest, TrimAsciiWhitespaceViewReturnsSubview) {
  std::string input = "\r\nabc\t";
  std::string_view trimmed = TrimAsciiWhitespaceView(input);
  EXPECT_EQ(trimmed, "abc");
  EXPECT_EQ(trimmed.data(), input.data() + 2);
}

TEST(StringUtilsTest, ToLowerBasic) {
  EXPECT_EQ(ToLower("HELLO"), "hello");
  EXPECT_EQ(ToLower("Mixed-CASE_123"), "mixed-case_123");
  EXPECT_EQ(ToLower(""), "");
}

TEST(StringUtilsTest, FormatErrnoIncludesSyscallMessageAndCode) {
  std::string message = FormatErrno("open", EACCES);
  EXPECT_NE(message.find("open failed:"), std::string::npos);
  EXPECT_NE(message.find("errno=" + std::to_string(EACCES)), std::string::npos);
}

TEST(StringUtilsTest, ParseNumericSupportsIntegerBaseAndRequiresFullInput) {
  auto hex_value = ParseNumeric<uint32_t>("ff", 16);
  ASSERT_TRUE(hex_value.has_value());
  EXPECT_EQ(*hex_value, 255u);

  EXPECT_FALSE(ParseNumeric<uint32_t>("ffx", 16).has_value());
  EXPECT_FALSE(ParseNumeric<uint32_t>("12x", 10).has_value());
}

/**
 * @brief Test basic lowercase to uppercase conversion
 */
TEST(StringUtilsTest, ToUpperBasic) {
  EXPECT_EQ(ToUpper("hello"), "HELLO");
  EXPECT_EQ(ToUpper("world"), "WORLD");
}

/**
 * @brief Test ToUpper with empty string
 */
TEST(StringUtilsTest, ToUpperEmpty) {
  EXPECT_EQ(ToUpper(""), "");
}

/**
 * @brief Test ToUpper with already-uppercase string
 */
TEST(StringUtilsTest, ToUpperAlreadyUppercase) {
  EXPECT_EQ(ToUpper("HELLO"), "HELLO");
  EXPECT_EQ(ToUpper("ABC"), "ABC");
}

/**
 * @brief Test ToUpper with mixed case, numbers, and symbols
 */
TEST(StringUtilsTest, ToUpperMixedCaseNumbersSymbols) {
  EXPECT_EQ(ToUpper("Hello World"), "HELLO WORLD");
  EXPECT_EQ(ToUpper("abc123"), "ABC123");
  EXPECT_EQ(ToUpper("test-value_1.0"), "TEST-VALUE_1.0");
  EXPECT_EQ(ToUpper("foo@bar!"), "FOO@BAR!");
}

// ============================================================================
// GenerateQueryNgrams tests
// ============================================================================

/**
 * @brief Test GenerateQueryNgrams: kanji path (kanji_ngram_size > 0)
 *
 * When kanji_ngram_size > 0, should delegate to GenerateHybridNgrams
 * with the given ngram_size, kanji_ngram_size, and cross_boundary_ngrams.
 */
TEST(StringUtilsTest, GenerateQueryNgramsKanjiPath) {
  // kanji_ngram_size=1, ngram_size=2 -> GenerateHybridNgrams("東方Project", 2, 1, true)
  auto ngrams = GenerateQueryNgrams("東方Project", 2, 1, true);

  // Same result as GenerateHybridNgrams("東方Project", 2, 1, true)
  auto expected = GenerateHybridNgrams("東方Project", 2, 1, true);
  ASSERT_EQ(ngrams.size(), expected.size());
  for (size_t i = 0; i < ngrams.size(); ++i) {
    EXPECT_EQ(ngrams[i], expected[i]);
  }

  // Verify CJK unigrams and ASCII bigrams are present
  EXPECT_EQ(ngrams[0], "東");
  EXPECT_EQ(ngrams[1], "方");
  EXPECT_EQ(ngrams[2], "Pr");
}

/**
 * @brief Test GenerateQueryNgrams: kanji path with kanji bigrams
 *
 * When kanji_ngram_size=2, CJK characters should also produce bigrams.
 */
TEST(StringUtilsTest, GenerateQueryNgramsKanjiPathBigrams) {
  // kanji_ngram_size=2, ngram_size=3 -> GenerateHybridNgrams("漢字ABC", 3, 2, true)
  auto ngrams = GenerateQueryNgrams("漢字ABC", 3, 2, true);
  auto expected = GenerateHybridNgrams("漢字ABC", 3, 2, true);

  ASSERT_EQ(ngrams.size(), expected.size());
  for (size_t i = 0; i < ngrams.size(); ++i) {
    EXPECT_EQ(ngrams[i], expected[i]);
  }
}

/**
 * @brief Test GenerateQueryNgrams: default hybrid path (ngram_size == 0)
 *
 * When ngram_size == 0 and kanji_ngram_size == 0, should delegate to
 * GenerateHybridNgrams with default parameters (ascii=2, kanji=1, cross=true).
 */
TEST(StringUtilsTest, GenerateQueryNgramsDefaultHybridPath) {
  // ngram_size=0, kanji_ngram_size=0 -> GenerateHybridNgrams("東方Project")
  auto ngrams = GenerateQueryNgrams("東方Project", 0, 0, false);

  // Defaults: ascii_ngram_size=2, kanji_ngram_size=1, cross_boundary=true
  auto expected = GenerateHybridNgrams("東方Project");
  ASSERT_EQ(ngrams.size(), expected.size());
  for (size_t i = 0; i < ngrams.size(); ++i) {
    EXPECT_EQ(ngrams[i], expected[i]);
  }
}

/**
 * @brief Test GenerateQueryNgrams: default hybrid path with ASCII-only text
 *
 * Even with ASCII-only text, ngram_size=0 uses hybrid defaults (bigrams).
 */
TEST(StringUtilsTest, GenerateQueryNgramsDefaultHybridPathASCII) {
  auto ngrams = GenerateQueryNgrams("hello", 0, 0, true);
  auto expected = GenerateHybridNgrams("hello");

  ASSERT_EQ(ngrams.size(), expected.size());
  for (size_t i = 0; i < ngrams.size(); ++i) {
    EXPECT_EQ(ngrams[i], expected[i]);
  }

  // Defaults produce bigrams for ASCII
  ASSERT_EQ(ngrams.size(), 4);
  EXPECT_EQ(ngrams[0], "he");
  EXPECT_EQ(ngrams[1], "el");
  EXPECT_EQ(ngrams[2], "ll");
  EXPECT_EQ(ngrams[3], "lo");
}

/**
 * @brief Test GenerateQueryNgrams: normal ngram path (ngram_size > 0, kanji_ngram_size == 0)
 *
 * When ngram_size > 0 and kanji_ngram_size == 0, should delegate to
 * GenerateNgrams with the given ngram_size (no CJK-aware splitting).
 */
TEST(StringUtilsTest, GenerateQueryNgramsNormalNgramPath) {
  // ngram_size=2, kanji_ngram_size=0 -> GenerateNgrams("hello", 2)
  auto ngrams = GenerateQueryNgrams("hello", 2, 0, true);
  auto expected = GenerateNgrams("hello", 2);

  ASSERT_EQ(ngrams.size(), expected.size());
  for (size_t i = 0; i < ngrams.size(); ++i) {
    EXPECT_EQ(ngrams[i], expected[i]);
  }

  ASSERT_EQ(ngrams.size(), 4);
  EXPECT_EQ(ngrams[0], "he");
  EXPECT_EQ(ngrams[1], "el");
  EXPECT_EQ(ngrams[2], "ll");
  EXPECT_EQ(ngrams[3], "lo");
}

/**
 * @brief Test GenerateQueryNgrams: normal ngram path with unigrams
 *
 * ngram_size=1 should produce character-level unigrams via GenerateNgrams.
 */
TEST(StringUtilsTest, GenerateQueryNgramsNormalNgramPathUnigrams) {
  auto ngrams = GenerateQueryNgrams("abc", 1, 0, false);
  auto expected = GenerateNgrams("abc", 1);

  ASSERT_EQ(ngrams.size(), expected.size());
  for (size_t i = 0; i < ngrams.size(); ++i) {
    EXPECT_EQ(ngrams[i], expected[i]);
  }

  ASSERT_EQ(ngrams.size(), 3);
  EXPECT_EQ(ngrams[0], "a");
  EXPECT_EQ(ngrams[1], "b");
  EXPECT_EQ(ngrams[2], "c");
}

/**
 * @brief Test GenerateQueryNgrams: normal ngram path treats CJK like any other character
 *
 * When using the plain GenerateNgrams path (kanji_ngram_size=0, ngram_size>0),
 * CJK characters are not treated specially - bigrams span across CJK/ASCII boundaries.
 */
TEST(StringUtilsTest, GenerateQueryNgramsNormalNgramPathMixed) {
  // ngram_size=2, kanji_ngram_size=0 -> GenerateNgrams("東方", 2)
  // Plain ngrams treat everything uniformly
  auto ngrams = GenerateQueryNgrams("東方", 2, 0, true);
  auto expected = GenerateNgrams("東方", 2);

  ASSERT_EQ(ngrams.size(), expected.size());
  ASSERT_EQ(ngrams.size(), 1);
  EXPECT_EQ(ngrams[0], "東方");
}

/**
 * @brief Test GenerateQueryNgrams: empty input returns empty vector for all branches
 */
TEST(StringUtilsTest, GenerateQueryNgramsEmptyInput) {
  // Kanji path (kanji_ngram_size > 0)
  auto ngrams1 = GenerateQueryNgrams("", 2, 1, true);
  EXPECT_TRUE(ngrams1.empty());

  // Default hybrid path (ngram_size == 0)
  auto ngrams2 = GenerateQueryNgrams("", 0, 0, true);
  EXPECT_TRUE(ngrams2.empty());

  // Normal ngram path (ngram_size > 0, kanji_ngram_size == 0)
  auto ngrams3 = GenerateQueryNgrams("", 2, 0, false);
  EXPECT_TRUE(ngrams3.empty());
}

/**
 * @brief Test GenerateQueryNgrams: cross_boundary_ngrams flag is passed through
 *
 * The flag should only affect the kanji path (kanji_ngram_size > 0).
 * Test that enabling/disabling it produces different results for mixed text.
 */
TEST(StringUtilsTest, GenerateQueryNgramsCrossBoundaryFlag) {
  // With cross_boundary=true: "字A" cross-boundary ngram should be generated
  auto ngrams_enabled = GenerateQueryNgrams("漢字ABC", 3, 2, true);
  // With cross_boundary=false: "字A" should be rejected
  auto ngrams_disabled = GenerateQueryNgrams("漢字ABC", 3, 2, false);

  // Verify they produce different results
  EXPECT_NE(ngrams_enabled.size(), ngrams_disabled.size());

  // Enabled should match GenerateHybridNgrams with cross_boundary=true
  auto expected_enabled = GenerateHybridNgrams("漢字ABC", 3, 2, true);
  ASSERT_EQ(ngrams_enabled.size(), expected_enabled.size());
  for (size_t i = 0; i < ngrams_enabled.size(); ++i) {
    EXPECT_EQ(ngrams_enabled[i], expected_enabled[i]);
  }

  // Disabled should match GenerateHybridNgrams with cross_boundary=false
  auto expected_disabled = GenerateHybridNgrams("漢字ABC", 3, 2, false);
  ASSERT_EQ(ngrams_disabled.size(), expected_disabled.size());
  for (size_t i = 0; i < ngrams_disabled.size(); ++i) {
    EXPECT_EQ(ngrams_disabled[i], expected_disabled[i]);
  }
}

/**
 * @brief Test GenerateQueryNgrams: cross_boundary flag is ignored in default hybrid path
 *
 * When ngram_size == 0, GenerateHybridNgrams is called with defaults,
 * so the cross_boundary_ngrams parameter is not forwarded.
 */
TEST(StringUtilsTest, GenerateQueryNgramsCrossBoundaryIgnoredInDefaultPath) {
  // Both should produce the same result since default path ignores the flag
  auto ngrams_true = GenerateQueryNgrams("漢字ABC", 0, 0, true);
  auto ngrams_false = GenerateQueryNgrams("漢字ABC", 0, 0, false);

  ASSERT_EQ(ngrams_true.size(), ngrams_false.size());
  for (size_t i = 0; i < ngrams_true.size(); ++i) {
    EXPECT_EQ(ngrams_true[i], ngrams_false[i]);
  }
}

// ============================================================================
// Utf8ToCodepoints buffer overload tests
// ============================================================================

/**
 * @brief Test buffer overload with ASCII text that fits
 */
TEST(StringUtilsTest, Utf8ToCodepointsBufferAscii) {
  uint32_t buffer[16];
  size_t count = Utf8ToCodepoints("abc", buffer, 16);
  ASSERT_EQ(count, 3);
  EXPECT_EQ(buffer[0], 0x61);  // 'a'
  EXPECT_EQ(buffer[1], 0x62);  // 'b'
  EXPECT_EQ(buffer[2], 0x63);  // 'c'
}

/**
 * @brief Test buffer overload with exact-fit capacity
 */
TEST(StringUtilsTest, Utf8ToCodepointsBufferExactFit) {
  uint32_t buffer[3];
  size_t count = Utf8ToCodepoints("abc", buffer, 3);
  ASSERT_EQ(count, 3);
  EXPECT_EQ(buffer[0], 0x61);
  EXPECT_EQ(buffer[1], 0x62);
  EXPECT_EQ(buffer[2], 0x63);
}

/**
 * @brief Test buffer overload returns SIZE_MAX when text exceeds capacity
 */
TEST(StringUtilsTest, Utf8ToCodepointsBufferOverflow) {
  uint32_t buffer[2];
  size_t count = Utf8ToCodepoints("abc", buffer, 2);
  EXPECT_EQ(count, SIZE_MAX);  // Buffer too small, should return SIZE_MAX
}

/**
 * @brief Test buffer overload with empty string
 */
TEST(StringUtilsTest, Utf8ToCodepointsBufferEmpty) {
  uint32_t buffer[4];
  size_t count = Utf8ToCodepoints("", buffer, 4);
  EXPECT_EQ(count, 0);  // Empty string, 0 codepoints
}

/**
 * @brief Test buffer overload with multi-byte UTF-8 characters (Japanese)
 */
TEST(StringUtilsTest, Utf8ToCodepointsBufferMultiByte) {
  uint32_t buffer[8];
  size_t count = Utf8ToCodepoints("あいう", buffer, 8);
  ASSERT_EQ(count, 3);
  EXPECT_EQ(buffer[0], 0x3042);  // 'あ'
  EXPECT_EQ(buffer[1], 0x3044);  // 'い'
  EXPECT_EQ(buffer[2], 0x3046);  // 'う'
}

/**
 * @brief Test buffer overload with 4-byte UTF-8 characters (emoji)
 */
TEST(StringUtilsTest, Utf8ToCodepointsBufferEmoji) {
  uint32_t buffer[4];
  // Two emojis: 2 codepoints
  size_t count = Utf8ToCodepoints("😀🎉", buffer, 4);
  ASSERT_EQ(count, 2);
  EXPECT_EQ(buffer[0], 0x1F600);  // 😀
  EXPECT_EQ(buffer[1], 0x1F389);  // 🎉
}

/**
 * @brief Test buffer overload with mixed ASCII and multi-byte
 */
TEST(StringUtilsTest, Utf8ToCodepointsBufferMixed) {
  uint32_t buffer[8];
  size_t count = Utf8ToCodepoints("aあb", buffer, 8);
  ASSERT_EQ(count, 3);
  EXPECT_EQ(buffer[0], 0x61);    // 'a'
  EXPECT_EQ(buffer[1], 0x3042);  // 'あ'
  EXPECT_EQ(buffer[2], 0x62);    // 'b'
}

/**
 * @brief Test buffer overload overflow with multi-byte characters
 *
 * 3 codepoints but buffer capacity is 2 -- should return SIZE_MAX.
 */
TEST(StringUtilsTest, Utf8ToCodepointsBufferMultiByteOverflow) {
  uint32_t buffer[2];
  size_t count = Utf8ToCodepoints("あいう", buffer, 2);
  EXPECT_EQ(count, SIZE_MAX);  // 3 codepoints > capacity 2
}

/**
 * @brief Test buffer overload matches vector overload results
 */
TEST(StringUtilsTest, Utf8ToCodepointsBufferMatchesVectorOverload) {
  std::string text = "Hello世界🎉test";
  auto vec = Utf8ToCodepoints(text);

  uint32_t buffer[64];
  size_t count = Utf8ToCodepoints(text, buffer, 64);

  ASSERT_EQ(count, vec.size());
  for (size_t i = 0; i < count; ++i) {
    EXPECT_EQ(buffer[i], vec[i]) << "Mismatch at index " << i;
  }
}

//  fix: GenerateHybridNgrams zero/negative guard
TEST(StringUtilsBugFixTest, GenerateHybridNgramsZeroSizeReturnsEmpty) {
  EXPECT_TRUE(GenerateHybridNgrams("hello", 0, 1).empty());
  EXPECT_TRUE(GenerateHybridNgrams("hello", 2, 0).empty());
  EXPECT_TRUE(GenerateHybridNgrams("hello", 0, 0).empty());
}

TEST(StringUtilsBugFixTest, GenerateHybridNgramsNegativeSizeReturnsEmpty) {
  EXPECT_TRUE(GenerateHybridNgrams("hello", -1, 1).empty());
  EXPECT_TRUE(GenerateHybridNgrams("hello", 2, -1).empty());
}

/**
 * @brief Test CountCodePoints with invalid UTF-8 continuation bytes
 *
 * Regression test for: bare continuation bytes (0x80-0xBF) were counted
 * as codepoints instead of being skipped.
 */
TEST(StringUtilsBugFixTest, CountCodePointsSkipsBareContinuationBytes) {
  // Valid ASCII
  EXPECT_EQ(CountCodePoints("abc"), 3u);

  // Valid multi-byte: "あ" = 3 bytes (E3 81 82)
  EXPECT_EQ(CountCodePoints("あ"), 1u);

  // Bare continuation byte (0x80) should not be counted
  std::string bare_cont = "a";
  bare_cont += static_cast<char>(0x80);
  bare_cont += "b";
  EXPECT_EQ(CountCodePoints(bare_cont), 2u);  // 'a' + 'b', continuation skipped

  // Multiple bare continuation bytes
  std::string multi_cont;
  multi_cont += static_cast<char>(0x80);
  multi_cont += static_cast<char>(0xBF);
  multi_cont += static_cast<char>(0x90);
  EXPECT_EQ(CountCodePoints(multi_cont), 0u);  // All continuation bytes, none counted

  // Invalid lead byte (0xFF) should not be counted
  std::string invalid_lead = "a";
  invalid_lead += static_cast<char>(0xFF);
  invalid_lead += "b";
  EXPECT_EQ(CountCodePoints(invalid_lead), 2u);  // 'a' + 'b', invalid byte skipped

  // Mixed valid and invalid bytes
  std::string mixed;
  mixed += static_cast<char>(0x80);  // bare continuation, skip
  mixed += "a";                      // valid ASCII, count
  mixed += static_cast<char>(0xBF);  // bare continuation, skip
  EXPECT_EQ(CountCodePoints(mixed), 1u);

  // Empty string
  EXPECT_EQ(CountCodePoints(""), 0u);
}

// ============================================================
// IsUnicodeWhitespace tests
// ============================================================

TEST(StringUtilsTest, IsUnicodeWhitespace_AsciiSpace) {
  size_t len = 0;
  EXPECT_TRUE(IsUnicodeWhitespace(" ", 0, len));
  EXPECT_EQ(len, 1u);
}

TEST(StringUtilsTest, IsUnicodeWhitespace_AsciiTab) {
  size_t len = 0;
  EXPECT_TRUE(IsUnicodeWhitespace("\t", 0, len));
  EXPECT_EQ(len, 1u);
}

TEST(StringUtilsTest, IsUnicodeWhitespace_AsciiNewline) {
  size_t len = 0;
  EXPECT_TRUE(IsUnicodeWhitespace("\n", 0, len));
  EXPECT_EQ(len, 1u);
}

TEST(StringUtilsTest, IsUnicodeWhitespace_NonSpace) {
  size_t len = 0;
  EXPECT_FALSE(IsUnicodeWhitespace("a", 0, len));
  EXPECT_FALSE(IsUnicodeWhitespace("Z", 0, len));
  EXPECT_FALSE(IsUnicodeWhitespace("1", 0, len));
}

TEST(StringUtilsTest, IsUnicodeWhitespace_NoBreakSpace) {
  // U+00A0 = 0xC2 0xA0
  std::string nbsp("\xC2\xA0");
  size_t len = 0;
  EXPECT_TRUE(IsUnicodeWhitespace(nbsp, 0, len));
  EXPECT_EQ(len, 2u);
}

TEST(StringUtilsTest, IsUnicodeWhitespace_OghamSpaceMark) {
  // U+1680 = 0xE1 0x9A 0x80
  std::string ogham("\xE1\x9A\x80");
  size_t len = 0;
  EXPECT_TRUE(IsUnicodeWhitespace(ogham, 0, len));
  EXPECT_EQ(len, 3u);
}

TEST(StringUtilsTest, IsUnicodeWhitespace_EnSpace) {
  // U+2002 (En Space) = 0xE2 0x80 0x82
  std::string en_space("\xE2\x80\x82");
  size_t len = 0;
  EXPECT_TRUE(IsUnicodeWhitespace(en_space, 0, len));
  EXPECT_EQ(len, 3u);
}

TEST(StringUtilsTest, IsUnicodeWhitespace_ZeroWidthSpace) {
  // U+200B (Zero Width Space) = 0xE2 0x80 0x8B
  std::string zws("\xE2\x80\x8B");
  size_t len = 0;
  EXPECT_TRUE(IsUnicodeWhitespace(zws, 0, len));
  EXPECT_EQ(len, 3u);
}

TEST(StringUtilsTest, IsUnicodeWhitespace_LineSeparator) {
  // U+2028 = 0xE2 0x80 0xA8
  std::string ls("\xE2\x80\xA8");
  size_t len = 0;
  EXPECT_TRUE(IsUnicodeWhitespace(ls, 0, len));
  EXPECT_EQ(len, 3u);
}

TEST(StringUtilsTest, IsUnicodeWhitespace_ParagraphSeparator) {
  // U+2029 = 0xE2 0x80 0xA9
  std::string ps("\xE2\x80\xA9");
  size_t len = 0;
  EXPECT_TRUE(IsUnicodeWhitespace(ps, 0, len));
  EXPECT_EQ(len, 3u);
}

TEST(StringUtilsTest, IsUnicodeWhitespace_NarrowNoBreakSpace) {
  // U+202F = 0xE2 0x80 0xAF
  std::string nnbsp("\xE2\x80\xAF");
  size_t len = 0;
  EXPECT_TRUE(IsUnicodeWhitespace(nnbsp, 0, len));
  EXPECT_EQ(len, 3u);
}

TEST(StringUtilsTest, IsUnicodeWhitespace_MediumMathematicalSpace) {
  // U+205F = 0xE2 0x81 0x9F
  std::string mms("\xE2\x81\x9F");
  size_t len = 0;
  EXPECT_TRUE(IsUnicodeWhitespace(mms, 0, len));
  EXPECT_EQ(len, 3u);
}

TEST(StringUtilsTest, IsUnicodeWhitespace_IdeographicSpace) {
  // U+3000 = 0xE3 0x80 0x80
  std::string ideo("\xE3\x80\x80");
  size_t len = 0;
  EXPECT_TRUE(IsUnicodeWhitespace(ideo, 0, len));
  EXPECT_EQ(len, 3u);
}

TEST(StringUtilsTest, IsUnicodeWhitespace_MidString) {
  // Test detecting whitespace at a non-zero position
  std::string text =
      "ab\xC2\xA0"
      "cd";
  size_t len = 0;
  EXPECT_FALSE(IsUnicodeWhitespace(text, 0, len));
  EXPECT_FALSE(IsUnicodeWhitespace(text, 1, len));
  EXPECT_TRUE(IsUnicodeWhitespace(text, 2, len));
  EXPECT_EQ(len, 2u);
}

TEST(StringUtilsTest, IsUnicodeWhitespace_EmptyAndOutOfBounds) {
  size_t len = 0;
  EXPECT_FALSE(IsUnicodeWhitespace("", 0, len));
  EXPECT_FALSE(IsUnicodeWhitespace("a", 5, len));
}

TEST(StringUtilsTest, IsUnicodeWhitespace_TruncatedSequence) {
  // Only the first byte of a 2-byte sequence
  std::string truncated("\xC2");
  size_t len = 0;
  EXPECT_FALSE(IsUnicodeWhitespace(truncated, 0, len));
}

// ============================================================================
// Bug fix regression tests
// ============================================================================

/**
 * @brief Regression test: ngram_size=0 with kanji_ngram_size>0 must not return empty
 *
 * Previously, GenerateQueryNgrams(text, 0, 1, true) called GenerateHybridNgrams
 * with ascii_ngram_size=0, which hit an early-return guard and produced zero
 * n-grams for all input. The fix substitutes the default ASCII n-gram size (2).
 */
TEST(StringUtilsBugFixTest, GenerateQueryNgramsZeroNgramSizeWithKanjiSize) {
  // Pure ASCII with kanji_ngram_size=1, ngram_size=0 (default) — must produce bigrams
  auto ngrams = GenerateQueryNgrams("hello", 0, 1, true);
  EXPECT_FALSE(ngrams.empty());
  // Should behave like GenerateHybridNgrams("hello", 2, 1, true)
  auto expected = GenerateHybridNgrams("hello", 2, 1, true);
  EXPECT_EQ(ngrams, expected);

  // Mixed CJK + ASCII
  auto mixed = GenerateQueryNgrams("漢字test", 0, 1, true);
  EXPECT_FALSE(mixed.empty());
  auto mixed_expected = GenerateHybridNgrams("漢字test", 2, 1, true);
  EXPECT_EQ(mixed, mixed_expected);

  // Pure CJK
  auto cjk = GenerateQueryNgrams("東京都", 0, 1, false);
  EXPECT_FALSE(cjk.empty());

  // With kanji_ngram_size=2, ngram_size=0
  auto kanji2 = GenerateQueryNgrams("漢字ABC", 0, 2, true);
  EXPECT_FALSE(kanji2.empty());
  auto kanji2_expected = GenerateHybridNgrams("漢字ABC", 2, 2, true);
  EXPECT_EQ(kanji2, kanji2_expected);
}
