/**
 * @file string_utils.cpp
 * @brief Implementation of string utility functions
 */

#include "utils/string_utils.h"
#include <algorithm>
#include <cctype>

#ifdef USE_ICU
#include <unicode/normalizer2.h>
#include <unicode/unistr.h>
#include <unicode/brkiter.h>
#include <unicode/translit.h>
#endif

namespace mygramdb {
namespace utils {

namespace {

/**
 * @brief Get number of bytes in UTF-8 character from first byte
 */
int Utf8CharLength(unsigned char first_byte) {
  if ((first_byte & 0x80) == 0) return 1;  // 0xxxxxxx
  if ((first_byte & 0xE0) == 0xC0) return 2;  // 110xxxxx
  if ((first_byte & 0xF0) == 0xE0) return 3;  // 1110xxxx
  if ((first_byte & 0xF8) == 0xF0) return 4;  // 11110xxx
  return 1;  // Invalid, treat as 1 byte
}

}  // namespace

std::vector<uint32_t> Utf8ToCodepoints(const std::string& text) {
  std::vector<uint32_t> codepoints;
  codepoints.reserve(text.size());  // Over-allocate for ASCII

  for (size_t i = 0; i < text.size();) {
    unsigned char first_byte = static_cast<unsigned char>(text[i]);
    int char_len = Utf8CharLength(first_byte);

    if (i + char_len > text.size()) {
      // Incomplete UTF-8 sequence, skip
      ++i;
      continue;
    }

    uint32_t codepoint = 0;

    if (char_len == 1) {
      codepoint = first_byte;
    } else if (char_len == 2) {
      codepoint = ((first_byte & 0x1F) << 6) |
                  (static_cast<unsigned char>(text[i + 1]) & 0x3F);
    } else if (char_len == 3) {
      codepoint = ((first_byte & 0x0F) << 12) |
                  ((static_cast<unsigned char>(text[i + 1]) & 0x3F) << 6) |
                  (static_cast<unsigned char>(text[i + 2]) & 0x3F);
    } else if (char_len == 4) {
      codepoint = ((first_byte & 0x07) << 18) |
                  ((static_cast<unsigned char>(text[i + 1]) & 0x3F) << 12) |
                  ((static_cast<unsigned char>(text[i + 2]) & 0x3F) << 6) |
                  (static_cast<unsigned char>(text[i + 3]) & 0x3F);
    }

    codepoints.push_back(codepoint);
    i += char_len;
  }

  return codepoints;
}

std::string CodepointsToUtf8(const std::vector<uint32_t>& codepoints) {
  std::string result;
  result.reserve(codepoints.size() * 3);  // Estimate

  for (uint32_t cp : codepoints) {
    if (cp <= 0x7F) {
      result += static_cast<char>(cp);
    } else if (cp <= 0x7FF) {
      result += static_cast<char>(0xC0 | (cp >> 6));
      result += static_cast<char>(0x80 | (cp & 0x3F));
    } else if (cp <= 0xFFFF) {
      result += static_cast<char>(0xE0 | (cp >> 12));
      result += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
      result += static_cast<char>(0x80 | (cp & 0x3F));
    } else if (cp <= 0x10FFFF) {
      result += static_cast<char>(0xF0 | (cp >> 18));
      result += static_cast<char>(0x80 | ((cp >> 12) & 0x3F));
      result += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
      result += static_cast<char>(0x80 | (cp & 0x3F));
    }
  }

  return result;
}

#ifdef USE_ICU
std::string NormalizeTextICU(const std::string& text, bool nfkc,
                             const std::string& width, bool lower) {
  UErrorCode status = U_ZERO_ERROR;

  // Convert UTF-8 to UnicodeString
  icu::UnicodeString ustr = icu::UnicodeString::fromUTF8(text);

  // NFKC normalization
  if (nfkc) {
    const icu::Normalizer2* normalizer = icu::Normalizer2::getNFKCInstance(status);
    if (U_SUCCESS(status)) {
      icu::UnicodeString normalized;
      normalizer->normalize(ustr, normalized, status);
      if (U_SUCCESS(status)) {
        ustr = normalized;
      }
    }
  }

  // Width conversion
  if (width == "narrow") {
    // Full-width to half-width conversion
    icu::Transliterator* trans = icu::Transliterator::createInstance(
        "Fullwidth-Halfwidth", UTRANS_FORWARD, status);
    if (U_SUCCESS(status) && trans != nullptr) {
      trans->transliterate(ustr);
      delete trans;
    }
  } else if (width == "wide") {
    // Half-width to full-width conversion
    icu::Transliterator* trans = icu::Transliterator::createInstance(
        "Halfwidth-Fullwidth", UTRANS_FORWARD, status);
    if (U_SUCCESS(status) && trans != nullptr) {
      trans->transliterate(ustr);
      delete trans;
    }
  }

  // Lowercase conversion
  if (lower) {
    ustr.toLower();
  }

  // Convert back to UTF-8
  std::string result;
  ustr.toUTF8String(result);
  return result;
}
#endif

std::string NormalizeText(const std::string& text, bool nfkc,
                         const std::string& width, bool lower) {
#ifdef USE_ICU
  return NormalizeTextICU(text, nfkc, width, lower);
#else
  // Fallback implementation: simple lowercasing only
  std::string result = text;

  if (lower) {
    std::transform(result.begin(), result.end(), result.begin(),
                   [](unsigned char c) { return std::tolower(c); });
  }

  return result;
#endif
}

std::vector<std::string> GenerateNgrams(const std::string& text, int n) {
  std::vector<std::string> ngrams;

  // Convert to codepoints for proper character-level n-grams
  std::vector<uint32_t> codepoints = Utf8ToCodepoints(text);

  if (codepoints.empty() || n <= 0) {
    return ngrams;
  }

  // For n=1 (unigrams), just return each character
  if (n == 1) {
    ngrams.reserve(codepoints.size());
    for (uint32_t cp : codepoints) {
      ngrams.push_back(CodepointsToUtf8({cp}));
    }
    return ngrams;
  }

  // For n > 1
  if (codepoints.size() < static_cast<size_t>(n)) {
    return ngrams;
  }

  ngrams.reserve(codepoints.size() - n + 1);
  for (size_t i = 0; i <= codepoints.size() - n; ++i) {
    std::vector<uint32_t> ngram_cp(codepoints.begin() + i,
                                   codepoints.begin() + i + n);
    ngrams.push_back(CodepointsToUtf8(ngram_cp));
  }

  return ngrams;
}

namespace {

/**
 * @brief Check if codepoint is CJK Ideograph (Kanji)
 *
 * CJK Unified Ideographs ranges:
 * - 4E00-9FFF: Common and uncommon Kanji
 * - 3400-4DBF: Extension A
 * - 20000-2A6DF: Extension B
 * - 2A700-2B73F: Extension C
 * - 2B740-2B81F: Extension D
 * - F900-FAFF: Compatibility Ideographs
 */
bool IsCJKIdeograph(uint32_t codepoint) {
  return (codepoint >= 0x4E00 && codepoint <= 0x9FFF) ||    // Main block
         (codepoint >= 0x3400 && codepoint <= 0x4DBF) ||    // Extension A
         (codepoint >= 0x20000 && codepoint <= 0x2A6DF) ||  // Extension B
         (codepoint >= 0x2A700 && codepoint <= 0x2B73F) ||  // Extension C
         (codepoint >= 0x2B740 && codepoint <= 0x2B81F) ||  // Extension D
         (codepoint >= 0xF900 && codepoint <= 0xFAFF);      // Compatibility
}

}  // namespace

std::vector<std::string> GenerateHybridNgrams(const std::string& text) {
  std::vector<std::string> ngrams;

  // Convert to codepoints for proper character-level processing
  std::vector<uint32_t> codepoints = Utf8ToCodepoints(text);

  if (codepoints.empty()) {
    return ngrams;
  }

  ngrams.reserve(codepoints.size());  // Estimate

  for (size_t i = 0; i < codepoints.size(); ++i) {
    uint32_t cp = codepoints[i];

    if (IsCJKIdeograph(cp)) {
      // Kanji: tokenize as single character (unigram)
      ngrams.push_back(CodepointsToUtf8({cp}));
    } else {
      // Non-Kanji: tokenize as bigram with next character
      if (i + 1 < codepoints.size()) {
        uint32_t next_cp = codepoints[i + 1];
        ngrams.push_back(CodepointsToUtf8({cp, next_cp}));

        // If next is also non-Kanji, we'll create overlapping bigrams
        // This ensures "abc" -> ["ab", "bc"] not just ["ab", "c"]
      }
      // Last non-Kanji character: include as single if it's the only one left
      else if (i == codepoints.size() - 1 && i > 0) {
        // Check if previous was also non-Kanji (already handled in bigram)
        if (IsCJKIdeograph(codepoints[i - 1])) {
          // Previous was Kanji, this is standalone non-Kanji at end
          // Skip single non-Kanji at end (not searchable by design)
        }
      }
    }
  }

  return ngrams;
}

}  // namespace utils
}  // namespace mygramdb
