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
