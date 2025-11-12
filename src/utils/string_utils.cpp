/**
 * @file string_utils.cpp
 * @brief Implementation of string utility functions
 */

#include "utils/string_utils.h"

#include <algorithm>
#include <array>
#include <cctype>
#include <iomanip>
#include <sstream>

#ifdef USE_ICU
#include <unicode/brkiter.h>
#include <unicode/normalizer2.h>
#include <unicode/translit.h>
#include <unicode/unistr.h>
#endif

namespace mygramdb::utils {

namespace {

// UTF-8 byte masks and patterns
constexpr uint8_t kUtf8OneByteMask = 0x80;       // 10000000
constexpr uint8_t kUtf8TwoByteMask = 0xE0;       // 11100000
constexpr uint8_t kUtf8TwoBytePattern = 0xC0;    // 11000000
constexpr uint8_t kUtf8ThreeByteMask = 0xF0;     // 11110000
constexpr uint8_t kUtf8ThreeBytePattern = 0xE0;  // 11100000
constexpr uint8_t kUtf8FourByteMask = 0xF8;      // 11111000
constexpr uint8_t kUtf8FourBytePattern = 0xF0;   // 11110000

// UTF-8 continuation byte masks
constexpr uint8_t kUtf8ContinuationMask = 0x3F;     // 00111111
constexpr uint8_t kUtf8ContinuationPattern = 0x80;  // 10000000

// UTF-8 data extraction masks
constexpr uint8_t kUtf8TwoByteDatMask = 0x1F;    // 00011111
constexpr uint8_t kUtf8ThreeByteDatMask = 0x0F;  // 00001111
constexpr uint8_t kUtf8FourByteDatMask = 0x07;   // 00000111

// UTF-8 bit shift amounts
constexpr int kUtf8Shift6 = 6;
constexpr int kUtf8Shift12 = 12;
constexpr int kUtf8Shift18 = 18;

// Unicode codepoint ranges
constexpr uint32_t kUnicodeMaxOneByte = 0x7F;
constexpr uint32_t kUnicodeMaxTwoByte = 0x7FF;
constexpr uint32_t kUnicodeMaxThreeByte = 0xFFFF;
constexpr uint32_t kUnicodeMaxCodepoint = 0x10FFFF;

// CJK Ideograph ranges (Kanji)
constexpr uint32_t kCjkMainStart = 0x4E00;
constexpr uint32_t kCjkMainEnd = 0x9FFF;
constexpr uint32_t kCjkExtAStart = 0x3400;
constexpr uint32_t kCjkExtAEnd = 0x4DBF;
constexpr uint32_t kCjkExtBStart = 0x20000;
constexpr uint32_t kCjkExtBEnd = 0x2A6DF;
constexpr uint32_t kCjkExtCStart = 0x2A700;
constexpr uint32_t kCjkExtCEnd = 0x2B73F;
constexpr uint32_t kCjkExtDStart = 0x2B740;
constexpr uint32_t kCjkExtDEnd = 0x2B81F;
constexpr uint32_t kCjkCompatStart = 0xF900;
constexpr uint32_t kCjkCompatEnd = 0xFAFF;

// Byte formatting constants
constexpr double kBytesPerKilobyte = 1024.0;
constexpr double kLargeUnitThreshold = 100.0;
constexpr double kMediumUnitThreshold = 10.0;

/**
 * @brief Get number of bytes in UTF-8 character from first byte
 */
int Utf8CharLength(unsigned char first_byte) {
  if ((first_byte & kUtf8OneByteMask) == 0) {
    return 1;  // 0xxxxxxx
  }
  if ((first_byte & kUtf8TwoByteMask) == kUtf8TwoBytePattern) {
    return 2;  // 110xxxxx
  }
  if ((first_byte & kUtf8ThreeByteMask) == kUtf8ThreeBytePattern) {
    return 3;  // 1110xxxx
  }
  if ((first_byte & kUtf8FourByteMask) == kUtf8FourBytePattern) {
    return 4;  // 11110xxx
  }
  return 1;  // Invalid, treat as 1 byte
}

}  // namespace

std::vector<uint32_t> Utf8ToCodepoints(const std::string& text) {
  std::vector<uint32_t> codepoints;
  codepoints.reserve(text.size());  // Over-allocate for ASCII

  for (size_t i = 0; i < text.size();) {
    auto first_byte = static_cast<unsigned char>(text[i]);
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
      codepoint = ((first_byte & kUtf8TwoByteDatMask) << kUtf8Shift6) |
                  (static_cast<unsigned char>(text[i + 1]) & kUtf8ContinuationMask);
    } else if (char_len == 3) {
      codepoint = ((first_byte & kUtf8ThreeByteDatMask) << kUtf8Shift12) |
                  ((static_cast<unsigned char>(text[i + 1]) & kUtf8ContinuationMask) << kUtf8Shift6) |
                  (static_cast<unsigned char>(text[i + 2]) & kUtf8ContinuationMask);
    } else if (char_len == 4) {
      codepoint = ((first_byte & kUtf8FourByteDatMask) << kUtf8Shift18) |
                  ((static_cast<unsigned char>(text[i + 1]) & kUtf8ContinuationMask) << kUtf8Shift12) |
                  ((static_cast<unsigned char>(text[i + 2]) & kUtf8ContinuationMask) << kUtf8Shift6) |
                  (static_cast<unsigned char>(text[i + 3]) & kUtf8ContinuationMask);
    }

    codepoints.push_back(codepoint);
    i += char_len;
  }

  return codepoints;
}

std::string CodepointsToUtf8(const std::vector<uint32_t>& codepoints) {
  std::string result;
  result.reserve(codepoints.size() * 3);  // Estimate

  for (uint32_t codepoint : codepoints) {
    if (codepoint <= kUnicodeMaxOneByte) {
      result += static_cast<char>(codepoint);
    } else if (codepoint <= kUnicodeMaxTwoByte) {
      result += static_cast<char>(kUtf8TwoBytePattern | (codepoint >> kUtf8Shift6));
      result += static_cast<char>(kUtf8ContinuationPattern | (codepoint & kUtf8ContinuationMask));
    } else if (codepoint <= kUnicodeMaxThreeByte) {
      result += static_cast<char>(kUtf8ThreeBytePattern | (codepoint >> kUtf8Shift12));
      result += static_cast<char>(kUtf8ContinuationPattern | ((codepoint >> kUtf8Shift6) & kUtf8ContinuationMask));
      result += static_cast<char>(kUtf8ContinuationPattern | (codepoint & kUtf8ContinuationMask));
    } else if (codepoint <= kUnicodeMaxCodepoint) {
      result += static_cast<char>(kUtf8FourBytePattern | (codepoint >> kUtf8Shift18));
      result += static_cast<char>(kUtf8ContinuationPattern | ((codepoint >> kUtf8Shift12) & kUtf8ContinuationMask));
      result += static_cast<char>(kUtf8ContinuationPattern | ((codepoint >> kUtf8Shift6) & kUtf8ContinuationMask));
      result += static_cast<char>(kUtf8ContinuationPattern | (codepoint & kUtf8ContinuationMask));
    }
  }

  return result;
}

#ifdef USE_ICU
std::string NormalizeTextICU(const std::string& text, bool nfkc, const std::string& width, bool lower) {
  UErrorCode status = U_ZERO_ERROR;

  // Convert UTF-8 to UnicodeString
  icu::UnicodeString ustr = icu::UnicodeString::fromUTF8(text);

  // NFKC normalization
  if (nfkc) {
    const icu::Normalizer2* normalizer = icu::Normalizer2::getNFKCInstance(status);
    if (U_SUCCESS(status) != 0) {
      icu::UnicodeString normalized;
      normalizer->normalize(ustr, normalized, status);
      if (U_SUCCESS(status) != 0) {
        ustr = normalized;
      }
    }
  }

  // Width conversion
  if (width == "narrow") {
    // Full-width to half-width conversion
    std::unique_ptr<icu::Transliterator> trans(
        icu::Transliterator::createInstance("Fullwidth-Halfwidth", UTRANS_FORWARD, status));
    if ((U_SUCCESS(status) != 0) && trans != nullptr) {
      trans->transliterate(ustr);
    }
  } else if (width == "wide") {
    // Half-width to full-width conversion
    std::unique_ptr<icu::Transliterator> trans(
        icu::Transliterator::createInstance("Halfwidth-Fullwidth", UTRANS_FORWARD, status));
    if ((U_SUCCESS(status) != 0) && trans != nullptr) {
      trans->transliterate(ustr);
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

std::string NormalizeText(const std::string& text, bool nfkc, const std::string& width, bool lower) {
#ifdef USE_ICU
  return NormalizeTextICU(text, nfkc, width, lower);
#else
  // Fallback implementation: simple lowercasing only
  std::string result = text;

  if (lower) {
    std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) { return std::tolower(c); });
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
    for (uint32_t codepoint : codepoints) {
      ngrams.push_back(CodepointsToUtf8({codepoint}));
    }
    return ngrams;
  }

  // For n > 1
  if (codepoints.size() < static_cast<size_t>(n)) {
    return ngrams;
  }

  ngrams.reserve(codepoints.size() - n + 1);
  for (size_t i = 0; i <= codepoints.size() - n; ++i) {
    std::vector<uint32_t> ngram_cp(codepoints.begin() + static_cast<std::ptrdiff_t>(i),
                                   codepoints.begin() + static_cast<std::ptrdiff_t>(i + n));
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
  return (codepoint >= kCjkMainStart && codepoint <= kCjkMainEnd) ||    // Main block
         (codepoint >= kCjkExtAStart && codepoint <= kCjkExtAEnd) ||    // Extension A
         (codepoint >= kCjkExtBStart && codepoint <= kCjkExtBEnd) ||    // Extension B
         (codepoint >= kCjkExtCStart && codepoint <= kCjkExtCEnd) ||    // Extension C
         (codepoint >= kCjkExtDStart && codepoint <= kCjkExtDEnd) ||    // Extension D
         (codepoint >= kCjkCompatStart && codepoint <= kCjkCompatEnd);  // Compatibility
}

}  // namespace

std::vector<std::string> GenerateHybridNgrams(const std::string& text, int ascii_ngram_size, int kanji_ngram_size) {
  std::vector<std::string> ngrams;

  // Convert to codepoints for proper character-level processing
  std::vector<uint32_t> codepoints = Utf8ToCodepoints(text);

  if (codepoints.empty()) {
    return ngrams;
  }

  ngrams.reserve(codepoints.size());  // Estimate

  for (size_t i = 0; i < codepoints.size(); ++i) {
    uint32_t codepoint = codepoints[i];

    if (IsCJKIdeograph(codepoint)) {
      // CJK character: use kanji_ngram_size
      if (i + kanji_ngram_size <= codepoints.size()) {
        std::vector<uint32_t> ngram_codepoints;
        ngram_codepoints.reserve(kanji_ngram_size);
        for (int j = 0; j < kanji_ngram_size; ++j) {
          ngram_codepoints.push_back(codepoints[i + j]);
        }
        ngrams.push_back(CodepointsToUtf8(ngram_codepoints));
      }
    } else {
      // Non-CJK character: use ascii_ngram_size
      if (i + ascii_ngram_size <= codepoints.size()) {
        std::vector<uint32_t> ngram_codepoints;
        ngram_codepoints.reserve(ascii_ngram_size);
        for (int j = 0; j < ascii_ngram_size; ++j) {
          ngram_codepoints.push_back(codepoints[i + j]);
        }
        ngrams.push_back(CodepointsToUtf8(ngram_codepoints));
      }
    }
  }

  return ngrams;
}

std::string FormatBytes(size_t bytes) {
  constexpr std::array<const char*, 5> kUnits = {"B", "KB", "MB", "GB", "TB"};

  if (bytes == 0) {
    return "0B";
  }

  size_t unit_index = 0;
  auto size = static_cast<double>(bytes);

  while (size >= kBytesPerKilobyte && unit_index < kUnits.size() - 1) {
    size /= kBytesPerKilobyte;
    unit_index++;
  }

  // Format with appropriate precision
  std::ostringstream oss;
  // NOLINTBEGIN(cppcoreguidelines-pro-bounds-constant-array-index)
  if (size >= kLargeUnitThreshold) {
    oss << std::fixed << std::setprecision(0) << size << kUnits[unit_index];
  } else if (size >= kMediumUnitThreshold) {
    oss << std::fixed << std::setprecision(1) << size << kUnits[unit_index];
  } else {
    oss << std::fixed << std::setprecision(2) << size << kUnits[unit_index];
  }
  // NOLINTEND(cppcoreguidelines-pro-bounds-constant-array-index)

  return oss.str();
}

}  // namespace mygramdb::utils
