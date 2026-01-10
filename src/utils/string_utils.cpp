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

#include "utils/constants.h"

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

// UTF-16 surrogate pair range (invalid in UTF-8)
constexpr uint32_t kSurrogateStart = 0xD800;
constexpr uint32_t kSurrogateEnd = 0xDFFF;

// Minimum codepoint values for each UTF-8 encoding length (to detect overlong encoding)
constexpr uint32_t kMinTwoByteCodepoint = 0x80;
constexpr uint32_t kMinThreeByteCodepoint = 0x800;
constexpr uint32_t kMinFourByteCodepoint = 0x10000;

/**
 * @brief Check if a byte is a valid UTF-8 continuation byte (10xxxxxx)
 */
inline bool IsValidContinuationByte(unsigned char byte) {
  // NOLINTNEXTLINE(cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
  return (byte & 0xC0) == kUtf8ContinuationPattern;
}

/**
 * @brief Check if codepoint is in UTF-16 surrogate range (invalid in UTF-8)
 */
inline bool IsSurrogateCodepoint(uint32_t codepoint) {
  return codepoint >= kSurrogateStart && codepoint <= kSurrogateEnd;
}

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

// Byte formatting constants - use shared kBytesPerKilobyteDouble from constants.h
using mygram::constants::kBytesPerKilobyteDouble;
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

std::vector<uint32_t> Utf8ToCodepoints(std::string_view text) {
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
    bool valid = true;

    if (char_len == 1) {
      codepoint = first_byte;
    } else if (char_len == 2) {
      auto byte1 = static_cast<unsigned char>(text[i + 1]);
      // Validate continuation byte
      if (!IsValidContinuationByte(byte1)) {
        valid = false;
      } else {
        codepoint = ((first_byte & kUtf8TwoByteDatMask) << kUtf8Shift6) | (byte1 & kUtf8ContinuationMask);
        // Check for overlong encoding
        if (codepoint < kMinTwoByteCodepoint) {
          valid = false;
        }
      }
    } else if (char_len == 3) {
      auto byte1 = static_cast<unsigned char>(text[i + 1]);
      auto byte2 = static_cast<unsigned char>(text[i + 2]);
      // Validate continuation bytes
      if (!IsValidContinuationByte(byte1) || !IsValidContinuationByte(byte2)) {
        valid = false;
      } else {
        codepoint = ((first_byte & kUtf8ThreeByteDatMask) << kUtf8Shift12) |
                    ((byte1 & kUtf8ContinuationMask) << kUtf8Shift6) | (byte2 & kUtf8ContinuationMask);
        // Check for overlong encoding
        if (codepoint < kMinThreeByteCodepoint) {
          valid = false;
        }
        // Check for UTF-16 surrogate (invalid in UTF-8)
        if (IsSurrogateCodepoint(codepoint)) {
          valid = false;
        }
      }
    } else if (char_len == 4) {
      auto byte1 = static_cast<unsigned char>(text[i + 1]);
      auto byte2 = static_cast<unsigned char>(text[i + 2]);
      auto byte3 = static_cast<unsigned char>(text[i + 3]);
      // Validate continuation bytes
      if (!IsValidContinuationByte(byte1) || !IsValidContinuationByte(byte2) || !IsValidContinuationByte(byte3)) {
        valid = false;
      } else {
        codepoint = ((first_byte & kUtf8FourByteDatMask) << kUtf8Shift18) |
                    ((byte1 & kUtf8ContinuationMask) << kUtf8Shift12) |
                    ((byte2 & kUtf8ContinuationMask) << kUtf8Shift6) | (byte3 & kUtf8ContinuationMask);
        // Check for overlong encoding
        if (codepoint < kMinFourByteCodepoint) {
          valid = false;
        }
        // Check for codepoint beyond Unicode maximum
        if (codepoint > kUnicodeMaxCodepoint) {
          valid = false;
        }
      }
    }

    if (valid) {
      codepoints.push_back(codepoint);
      i += char_len;
    } else {
      // Invalid sequence, skip one byte and try again (replacement strategy)
      ++i;
    }
  }

  return codepoints;
}

std::string CodepointsToUtf8(const std::vector<uint32_t>& codepoints) {
  std::string result;
  result.reserve(codepoints.size() * 3);  // Estimate

  for (uint32_t codepoint : codepoints) {
    // Skip invalid codepoints: surrogates and beyond Unicode max
    if (IsSurrogateCodepoint(codepoint) || codepoint > kUnicodeMaxCodepoint) {
      continue;
    }

    if (codepoint <= kUnicodeMaxOneByte) {
      result += static_cast<char>(codepoint);
    } else if (codepoint <= kUnicodeMaxTwoByte) {
      result += static_cast<char>(kUtf8TwoBytePattern | (codepoint >> kUtf8Shift6));
      result += static_cast<char>(kUtf8ContinuationPattern | (codepoint & kUtf8ContinuationMask));
    } else if (codepoint <= kUnicodeMaxThreeByte) {
      result += static_cast<char>(kUtf8ThreeBytePattern | (codepoint >> kUtf8Shift12));
      result += static_cast<char>(kUtf8ContinuationPattern | ((codepoint >> kUtf8Shift6) & kUtf8ContinuationMask));
      result += static_cast<char>(kUtf8ContinuationPattern | (codepoint & kUtf8ContinuationMask));
    } else {
      result += static_cast<char>(kUtf8FourBytePattern | (codepoint >> kUtf8Shift18));
      result += static_cast<char>(kUtf8ContinuationPattern | ((codepoint >> kUtf8Shift12) & kUtf8ContinuationMask));
      result += static_cast<char>(kUtf8ContinuationPattern | ((codepoint >> kUtf8Shift6) & kUtf8ContinuationMask));
      result += static_cast<char>(kUtf8ContinuationPattern | (codepoint & kUtf8ContinuationMask));
    }
  }

  return result;
}

#ifdef USE_ICU
std::string NormalizeTextICU(std::string_view text, bool nfkc, std::string_view width, bool lower) {
  UErrorCode status = U_ZERO_ERROR;

  // Convert UTF-8 to UnicodeString
  icu::UnicodeString ustr =
      icu::UnicodeString::fromUTF8(icu::StringPiece(text.data(), static_cast<int32_t>(text.size())));

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
    std::unique_ptr<icu::Transliterator> trans(
        icu::Transliterator::createInstance("Fullwidth-Halfwidth", UTRANS_FORWARD, status));
    if ((U_SUCCESS(status)) && trans != nullptr) {
      trans->transliterate(ustr);
    }
  } else if (width == "wide") {
    // Half-width to full-width conversion
    std::unique_ptr<icu::Transliterator> trans(
        icu::Transliterator::createInstance("Halfwidth-Fullwidth", UTRANS_FORWARD, status));
    if ((U_SUCCESS(status)) && trans != nullptr) {
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

std::string NormalizeText(std::string_view text, bool nfkc, std::string_view width, bool lower) {
#ifdef USE_ICU
  return NormalizeTextICU(text, nfkc, width, lower);
#else
  // Fallback implementation: simple lowercasing only
  std::string result(text);

  if (lower) {
    std::transform(result.begin(), result.end(), result.begin(), [](unsigned char c) { return std::tolower(c); });
  }

  return result;
#endif
}

std::vector<std::string> GenerateNgrams(std::string_view text, int n) {
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
 * @brief Check if codepoint is CJK Ideograph (Kanji only, excluding Hiragana/Katakana)
 *
 * CJK Unified Ideographs ranges:
 * - 4E00-9FFF: Common and uncommon Kanji
 * - 3400-4DBF: Extension A
 * - 20000-2A6DF: Extension B
 * - 2A700-2B73F: Extension C
 * - 2B740-2B81F: Extension D
 * - F900-FAFF: Compatibility Ideographs
 *
 * Note: Hiragana (3040-309F) and Katakana (30A0-30FF) are intentionally excluded.
 * They will be processed with ascii_ngram_size instead of kanji_ngram_size.
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

std::vector<std::string> GenerateHybridNgrams(std::string_view text, int ascii_ngram_size, int kanji_ngram_size) {
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
        // Check if all next kanji_ngram_size characters are also CJK
        bool all_cjk = true;
        for (int j = 0; j < kanji_ngram_size; ++j) {
          if (!IsCJKIdeograph(codepoints[i + j])) {
            all_cjk = false;
            break;
          }
        }

        if (all_cjk) {
          std::vector<uint32_t> ngram_codepoints;
          ngram_codepoints.reserve(kanji_ngram_size);
          for (int j = 0; j < kanji_ngram_size; ++j) {
            ngram_codepoints.push_back(codepoints[i + j]);
          }
          ngrams.push_back(CodepointsToUtf8(ngram_codepoints));
        }
      }
    } else {
      // Non-CJK character: use ascii_ngram_size
      if (i + ascii_ngram_size <= codepoints.size()) {
        // Check if all next ascii_ngram_size characters are also non-CJK
        bool all_non_cjk = true;
        for (int j = 0; j < ascii_ngram_size; ++j) {
          if (IsCJKIdeograph(codepoints[i + j])) {
            all_non_cjk = false;
            break;
          }
        }

        if (all_non_cjk) {
          std::vector<uint32_t> ngram_codepoints;
          ngram_codepoints.reserve(ascii_ngram_size);
          for (int j = 0; j < ascii_ngram_size; ++j) {
            ngram_codepoints.push_back(codepoints[i + j]);
          }
          ngrams.push_back(CodepointsToUtf8(ngram_codepoints));
        }
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

  while (size >= kBytesPerKilobyteDouble && unit_index < kUnits.size() - 1) {
    size /= kBytesPerKilobyteDouble;
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

// NOLINTBEGIN(readability-identifier-length,cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
// Suppressing warnings: Short variable name 'i' is idiomatic for byte-level iteration.
// Magic numbers are standard UTF-8 byte patterns (0xC0, 0xC2, 0x0F, 0x3F, etc.) well-known in UTF-8 processing.
bool IsValidUtf8(std::string_view text) {
  size_t i = 0;
  while (i < text.size()) {
    auto first_byte = static_cast<unsigned char>(text[i]);

    if ((first_byte & kUtf8OneByteMask) == 0) {
      // ASCII (0xxxxxxx)
      ++i;
    } else if ((first_byte & kUtf8TwoByteMask) == kUtf8TwoBytePattern) {
      // 2-byte sequence (110xxxxx 10xxxxxx)
      if (i + 2 > text.size()) {
        return false;
      }
      // Check for overlong encoding (C0, C1 are invalid)
      if (first_byte < 0xC2) {
        return false;
      }
      // Verify continuation byte
      auto second = static_cast<unsigned char>(text[i + 1]);
      if ((second & 0xC0) != kUtf8ContinuationPattern) {
        return false;
      }
      i += 2;
    } else if ((first_byte & kUtf8ThreeByteMask) == kUtf8ThreeBytePattern) {
      // 3-byte sequence (1110xxxx 10xxxxxx 10xxxxxx)
      if (i + 3 > text.size()) {
        return false;
      }
      auto second = static_cast<unsigned char>(text[i + 1]);
      auto third = static_cast<unsigned char>(text[i + 2]);
      // Verify continuation bytes
      if ((second & 0xC0) != kUtf8ContinuationPattern || (third & 0xC0) != kUtf8ContinuationPattern) {
        return false;
      }
      // Check for overlong encoding and surrogate pairs
      uint32_t codepoint = ((first_byte & 0x0F) << 12) | ((second & 0x3F) << 6) | (third & 0x3F);
      if (codepoint < 0x800 || (codepoint >= 0xD800 && codepoint <= 0xDFFF)) {
        return false;
      }
      i += 3;
    } else if ((first_byte & kUtf8FourByteMask) == kUtf8FourBytePattern) {
      // 4-byte sequence (11110xxx 10xxxxxx 10xxxxxx 10xxxxxx)
      if (i + 4 > text.size()) {
        return false;
      }
      // Check for invalid start bytes (F5-FF)
      if (first_byte > 0xF4) {
        return false;
      }
      auto second = static_cast<unsigned char>(text[i + 1]);
      auto third = static_cast<unsigned char>(text[i + 2]);
      auto fourth = static_cast<unsigned char>(text[i + 3]);
      // Verify continuation bytes
      if ((second & 0xC0) != kUtf8ContinuationPattern || (third & 0xC0) != kUtf8ContinuationPattern ||
          (fourth & 0xC0) != kUtf8ContinuationPattern) {
        return false;
      }
      // Check for overlong encoding and out-of-range codepoints
      uint32_t codepoint =
          ((first_byte & 0x07) << 18) | ((second & 0x3F) << 12) | ((third & 0x3F) << 6) | (fourth & 0x3F);
      if (codepoint < 0x10000 || codepoint > kUnicodeMaxCodepoint) {
        return false;
      }
      i += 4;
    } else {
      // Invalid start byte
      return false;
    }
  }
  return true;
}
// NOLINTEND(readability-identifier-length,cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)

// NOLINTBEGIN(readability-identifier-length,cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)
// NOLINTBEGIN(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
// Suppressing warnings: Short variable name 'i' is idiomatic for byte-level iteration.
// Magic numbers are standard UTF-8 byte patterns well-known in UTF-8 processing.
// C-style array for kReplacementChar is used for efficient string appending.
std::string SanitizeUtf8(std::string_view text) {
  // U+FFFD replacement character in UTF-8
  constexpr char kReplacementChar[] = "\xEF\xBF\xBD";

  std::string result;
  result.reserve(text.size());

  size_t i = 0;
  while (i < text.size()) {
    auto first_byte = static_cast<unsigned char>(text[i]);

    if ((first_byte & kUtf8OneByteMask) == 0) {
      // Valid ASCII (0xxxxxxx)
      result += text[i];
      ++i;
    } else if ((first_byte & kUtf8TwoByteMask) == kUtf8TwoBytePattern) {
      // Potential 2-byte sequence
      if (first_byte < 0xC2) {
        // Invalid overlong encoding
        result += kReplacementChar;
        ++i;
        continue;
      }
      if (i + 2 > text.size()) {
        // Incomplete sequence
        result += kReplacementChar;
        ++i;
        continue;
      }
      auto second = static_cast<unsigned char>(text[i + 1]);
      if ((second & 0xC0) != kUtf8ContinuationPattern) {
        // Invalid continuation byte
        result += kReplacementChar;
        ++i;
        continue;
      }
      // Valid 2-byte sequence
      result += text[i];
      result += text[i + 1];
      i += 2;
    } else if ((first_byte & kUtf8ThreeByteMask) == kUtf8ThreeBytePattern) {
      // Potential 3-byte sequence
      if (i + 3 > text.size()) {
        result += kReplacementChar;
        ++i;
        continue;
      }
      auto second = static_cast<unsigned char>(text[i + 1]);
      auto third = static_cast<unsigned char>(text[i + 2]);
      if ((second & 0xC0) != kUtf8ContinuationPattern || (third & 0xC0) != kUtf8ContinuationPattern) {
        result += kReplacementChar;
        ++i;
        continue;
      }
      // Check for overlong encoding and surrogate pairs
      uint32_t codepoint = ((first_byte & 0x0F) << 12) | ((second & 0x3F) << 6) | (third & 0x3F);
      if (codepoint < 0x800 || (codepoint >= 0xD800 && codepoint <= 0xDFFF)) {
        result += kReplacementChar;
        ++i;
        continue;
      }
      // Valid 3-byte sequence
      result += text[i];
      result += text[i + 1];
      result += text[i + 2];
      i += 3;
    } else if ((first_byte & kUtf8FourByteMask) == kUtf8FourBytePattern) {
      // Potential 4-byte sequence
      if (first_byte > 0xF4) {
        result += kReplacementChar;
        ++i;
        continue;
      }
      if (i + 4 > text.size()) {
        result += kReplacementChar;
        ++i;
        continue;
      }
      auto second = static_cast<unsigned char>(text[i + 1]);
      auto third = static_cast<unsigned char>(text[i + 2]);
      auto fourth = static_cast<unsigned char>(text[i + 3]);
      if ((second & 0xC0) != kUtf8ContinuationPattern || (third & 0xC0) != kUtf8ContinuationPattern ||
          (fourth & 0xC0) != kUtf8ContinuationPattern) {
        result += kReplacementChar;
        ++i;
        continue;
      }
      uint32_t codepoint =
          ((first_byte & 0x07) << 18) | ((second & 0x3F) << 12) | ((third & 0x3F) << 6) | (fourth & 0x3F);
      if (codepoint < 0x10000 || codepoint > kUnicodeMaxCodepoint) {
        result += kReplacementChar;
        ++i;
        continue;
      }
      // Valid 4-byte sequence
      result += text[i];
      result += text[i + 1];
      result += text[i + 2];
      result += text[i + 3];
      i += 4;
    } else {
      // Invalid start byte (continuation byte or 0xFF/0xFE)
      result += kReplacementChar;
      ++i;
    }
  }

  return result;
}
// NOLINTEND(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
// NOLINTEND(readability-identifier-length,cppcoreguidelines-avoid-magic-numbers,readability-magic-numbers)

}  // namespace mygramdb::utils
