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

namespace mygram::utils {

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
constexpr uint8_t kUtf8ContinuationCheckMask = 0xC0;  // 11000000 (mask for verifying continuation bytes)
constexpr uint8_t kUtf8ContinuationMask = 0x3F;       // 00111111 (mask for extracting data from continuation bytes)
constexpr uint8_t kUtf8ContinuationPattern = 0x80;    // 10000000

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
 * @brief Try to parse a single UTF-8 character from a byte sequence.
 *
 * Returns the number of bytes in a valid UTF-8 sequence starting at data[0],
 * or -1 if the sequence is invalid. If valid, writes the decoded codepoint to *out_cp.
 *
 * @param data Pointer to the start of the byte sequence
 * @param available Number of bytes available from data onward
 * @param out_cp Output: decoded Unicode codepoint (only written on success)
 * @return Number of bytes consumed (1-4), or -1 on invalid sequence
 */
int TryParseUtf8Char(const unsigned char* data, size_t available, uint32_t* out_cp) {
  if (available == 0) {
    return -1;
  }
  auto first_byte = data[0];

  if ((first_byte & kUtf8OneByteMask) == 0) {
    // ASCII (0xxxxxxx)
    *out_cp = first_byte;
    return 1;
  }
  if ((first_byte & kUtf8TwoByteMask) == kUtf8TwoBytePattern) {
    // 2-byte sequence (110xxxxx 10xxxxxx)
    if (first_byte < 0xC2) {
      return -1;  // Overlong encoding
    }
    if (available < 2) {
      return -1;
    }
    auto second = data[1];
    if ((second & kUtf8ContinuationCheckMask) != kUtf8ContinuationPattern) {
      return -1;
    }
    *out_cp = ((first_byte & kUtf8TwoByteDatMask) << kUtf8Shift6) | (second & kUtf8ContinuationMask);
    return 2;
  }
  if ((first_byte & kUtf8ThreeByteMask) == kUtf8ThreeBytePattern) {
    // 3-byte sequence (1110xxxx 10xxxxxx 10xxxxxx)
    if (available < 3) {
      return -1;
    }
    auto second = data[1];
    auto third = data[2];
    if ((second & kUtf8ContinuationCheckMask) != kUtf8ContinuationPattern ||
        (third & kUtf8ContinuationCheckMask) != kUtf8ContinuationPattern) {
      return -1;
    }
    uint32_t codepoint = ((first_byte & kUtf8ThreeByteDatMask) << kUtf8Shift12) |
                         ((second & kUtf8ContinuationMask) << kUtf8Shift6) | (third & kUtf8ContinuationMask);
    if (codepoint < kMinThreeByteCodepoint || (codepoint >= kSurrogateStart && codepoint <= kSurrogateEnd)) {
      return -1;  // Overlong or surrogate
    }
    *out_cp = codepoint;
    return 3;
  }
  if ((first_byte & kUtf8FourByteMask) == kUtf8FourBytePattern) {
    // 4-byte sequence (11110xxx 10xxxxxx 10xxxxxx 10xxxxxx)
    if (first_byte > 0xF4) {
      return -1;  // Invalid start byte (F5-FF)
    }
    if (available < 4) {
      return -1;
    }
    auto second = data[1];
    auto third = data[2];
    auto fourth = data[3];
    if ((second & kUtf8ContinuationCheckMask) != kUtf8ContinuationPattern ||
        (third & kUtf8ContinuationCheckMask) != kUtf8ContinuationPattern ||
        (fourth & kUtf8ContinuationCheckMask) != kUtf8ContinuationPattern) {
      return -1;
    }
    uint32_t codepoint = ((first_byte & kUtf8FourByteDatMask) << kUtf8Shift18) |
                         ((second & kUtf8ContinuationMask) << kUtf8Shift12) |
                         ((third & kUtf8ContinuationMask) << kUtf8Shift6) | (fourth & kUtf8ContinuationMask);
    if (codepoint < kMinFourByteCodepoint || codepoint > kUnicodeMaxCodepoint) {
      return -1;  // Overlong or out of range
    }
    *out_cp = codepoint;
    return 4;
  }
  // Invalid start byte
  return -1;
}

/**
 * @brief Check if a byte is a valid UTF-8 continuation byte (10xxxxxx)
 */
inline bool IsValidContinuationByte(unsigned char byte) {
  return (byte & kUtf8ContinuationCheckMask) == kUtf8ContinuationPattern;
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
  codepoints.reserve(text.size() / 2 + 1);  // Balance between ASCII and CJK

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

size_t Utf8ToCodepoints(std::string_view text, uint32_t* buffer, size_t buffer_capacity) {
  size_t count = 0;

  for (size_t i = 0; i < text.size();) {
    auto first_byte = static_cast<unsigned char>(text[i]);
    int char_len = Utf8CharLength(first_byte);

    if (i + char_len > text.size()) {
      ++i;
      continue;
    }

    uint32_t codepoint = 0;
    bool valid = true;

    if (char_len == 1) {
      codepoint = first_byte;
    } else if (char_len == 2) {
      auto byte1 = static_cast<unsigned char>(text[i + 1]);
      if (!IsValidContinuationByte(byte1)) {
        valid = false;
      } else {
        codepoint = ((first_byte & kUtf8TwoByteDatMask) << kUtf8Shift6) | (byte1 & kUtf8ContinuationMask);
        if (codepoint < kMinTwoByteCodepoint) {
          valid = false;
        }
      }
    } else if (char_len == 3) {
      auto byte1 = static_cast<unsigned char>(text[i + 1]);
      auto byte2 = static_cast<unsigned char>(text[i + 2]);
      if (!IsValidContinuationByte(byte1) || !IsValidContinuationByte(byte2)) {
        valid = false;
      } else {
        codepoint = ((first_byte & kUtf8ThreeByteDatMask) << kUtf8Shift12) |
                    ((byte1 & kUtf8ContinuationMask) << kUtf8Shift6) | (byte2 & kUtf8ContinuationMask);
        if (codepoint < kMinThreeByteCodepoint) {
          valid = false;
        }
        if (IsSurrogateCodepoint(codepoint)) {
          valid = false;
        }
      }
    } else if (char_len == 4) {
      auto byte1 = static_cast<unsigned char>(text[i + 1]);
      auto byte2 = static_cast<unsigned char>(text[i + 2]);
      auto byte3 = static_cast<unsigned char>(text[i + 3]);
      if (!IsValidContinuationByte(byte1) || !IsValidContinuationByte(byte2) || !IsValidContinuationByte(byte3)) {
        valid = false;
      } else {
        codepoint = ((first_byte & kUtf8FourByteDatMask) << kUtf8Shift18) |
                    ((byte1 & kUtf8ContinuationMask) << kUtf8Shift12) |
                    ((byte2 & kUtf8ContinuationMask) << kUtf8Shift6) | (byte3 & kUtf8ContinuationMask);
        if (codepoint < kMinFourByteCodepoint) {
          valid = false;
        }
        if (codepoint > kUnicodeMaxCodepoint) {
          valid = false;
        }
      }
    }

    if (valid) {
      if (count >= buffer_capacity) {
        return 0;  // Buffer too small, caller should fall back to vector version
      }
      buffer[count++] = codepoint;
      i += char_len;
    } else {
      ++i;
    }
  }

  return count;
}

std::string CodepointsToUtf8(const uint32_t* begin, const uint32_t* end) {
  std::string result;
  result.reserve(static_cast<size_t>(end - begin) * 3);  // Estimate

  for (const uint32_t* it = begin; it != end; ++it) {
    uint32_t codepoint = *it;
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

std::string CodepointsToUtf8(const std::vector<uint32_t>& codepoints) {
  return CodepointsToUtf8(codepoints.data(), codepoints.data() + codepoints.size());
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
  // Use stack buffer for short strings to avoid heap allocation
  constexpr size_t kStackBufSize = 128;
  std::array<uint32_t, kStackBufSize> stack_buf{};
  size_t cp_count = Utf8ToCodepoints(text, stack_buf.data(), kStackBufSize);
  const uint32_t* cp_data = stack_buf.data();

  // Fall back to heap for long strings
  std::vector<uint32_t> heap_buf;
  if (cp_count == 0 && !text.empty()) {
    heap_buf = Utf8ToCodepoints(text);
    cp_count = heap_buf.size();
    cp_data = heap_buf.data();
  }

  if (cp_count == 0 || n <= 0) {
    return ngrams;
  }

  // For n=1 (unigrams), just return each character
  if (n == 1) {
    ngrams.reserve(cp_count);
    for (size_t i = 0; i < cp_count; ++i) {
      ngrams.push_back(CodepointsToUtf8(cp_data + i, cp_data + i + 1));
    }
    return ngrams;
  }

  // For n > 1
  if (cp_count < static_cast<size_t>(n)) {
    return ngrams;
  }

  ngrams.reserve(cp_count - n + 1);
  for (size_t i = 0; i <= cp_count - n; ++i) {
    ngrams.push_back(CodepointsToUtf8(cp_data + i, cp_data + i + n));
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

std::vector<std::string> GenerateHybridNgrams(std::string_view text, int ascii_ngram_size, int kanji_ngram_size,
                                              bool cross_boundary_ngrams) {
  std::vector<std::string> ngrams;

  if (ascii_ngram_size <= 0 || kanji_ngram_size <= 0) {
    return ngrams;
  }

  // Convert to codepoints for proper character-level processing
  // Use stack buffer for short strings to avoid heap allocation
  constexpr size_t kStackBufSize = 128;
  std::array<uint32_t, kStackBufSize> stack_buf{};
  size_t cp_count = Utf8ToCodepoints(text, stack_buf.data(), kStackBufSize);
  const uint32_t* cp_data = stack_buf.data();

  // Fall back to heap for long strings
  std::vector<uint32_t> heap_buf;
  if (cp_count == 0 && !text.empty()) {
    heap_buf = Utf8ToCodepoints(text);
    cp_count = heap_buf.size();
    cp_data = heap_buf.data();
  }

  if (cp_count == 0) {
    return ngrams;
  }

  ngrams.reserve(cp_count);  // Estimate

  for (size_t i = 0; i < cp_count; ++i) {
    uint32_t codepoint = cp_data[i];

    // Determine n-gram size based on the starting character type
    bool start_is_cjk = IsCJKIdeograph(codepoint);
    int ngram_size = start_is_cjk ? kanji_ngram_size : ascii_ngram_size;

    if (i + ngram_size > cp_count) {
      continue;
    }

    if (!cross_boundary_ngrams) {
      // Legacy behavior: reject N-grams that span CJK/non-CJK boundaries
      bool boundary_crossed = false;
      for (int j = 1; j < ngram_size; ++j) {
        if (IsCJKIdeograph(cp_data[i + j]) != start_is_cjk) {
          boundary_crossed = true;
          break;
        }
      }
      if (boundary_crossed) {
        continue;
      }
    }

    ngrams.push_back(CodepointsToUtf8(cp_data + i, cp_data + i + ngram_size));
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

// NOLINTBEGIN(readability-identifier-length)
// Suppressing warnings: Short variable name 'i' is idiomatic for byte-level iteration.
bool IsValidUtf8(std::string_view text) {
  const auto* data = reinterpret_cast<const unsigned char*>(text.data());
  size_t i = 0;
  while (i < text.size()) {
    uint32_t codepoint = 0;
    int char_len = TryParseUtf8Char(data + i, text.size() - i, &codepoint);
    if (char_len < 0) {
      return false;
    }
    i += static_cast<size_t>(char_len);
  }
  return true;
}
// NOLINTEND(readability-identifier-length)

// NOLINTBEGIN(readability-identifier-length)
// NOLINTBEGIN(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
// Suppressing warnings: Short variable name 'i' is idiomatic for byte-level iteration.
// C-style array for kReplacementChar is used for efficient string appending.
std::string SanitizeUtf8(std::string_view text) {
  // U+FFFD replacement character in UTF-8
  constexpr char kReplacementChar[] = "\xEF\xBF\xBD";

  std::string result;
  result.reserve(text.size());

  const auto* data = reinterpret_cast<const unsigned char*>(text.data());
  size_t i = 0;
  while (i < text.size()) {
    uint32_t codepoint = 0;
    int char_len = TryParseUtf8Char(data + i, text.size() - i, &codepoint);
    if (char_len < 0) {
      // Invalid byte - replace with U+FFFD and skip one byte
      result += kReplacementChar;
      ++i;
    } else {
      // Valid sequence - append original bytes
      result.append(text.data() + i, static_cast<size_t>(char_len));
      i += static_cast<size_t>(char_len);
    }
  }

  return result;
}
// NOLINTEND(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays,cppcoreguidelines-pro-bounds-array-to-pointer-decay)
// NOLINTEND(readability-identifier-length)

std::string ToUpper(std::string_view str) {
  std::string result(str);
  std::transform(result.begin(), result.end(), result.begin(),
                 [](unsigned char character) { return std::toupper(character); });
  return result;
}

std::string ReplaceAll(std::string_view str, std::string_view from, std::string_view to) {
  std::string result(str);
  if (from.empty()) {
    return result;
  }
  size_t pos = 0;
  while ((pos = result.find(from, pos)) != std::string::npos) {
    result.replace(pos, from.size(), to);
    pos += to.size();
  }
  return result;
}

std::pair<std::string, std::string> SplitOnFirst(std::string_view str, std::string_view delimiter) {
  auto pos = str.find(delimiter);
  if (pos == std::string_view::npos) {
    return {std::string(str), ""};
  }
  return {std::string(str.substr(0, pos)), std::string(str.substr(pos + delimiter.size()))};
}

std::vector<std::string> GenerateQueryNgrams(std::string_view normalized, int ngram_size, int kanji_ngram_size,
                                             bool cross_boundary_ngrams) {
  if (kanji_ngram_size > 0) {
    return GenerateHybridNgrams(normalized, ngram_size, kanji_ngram_size, cross_boundary_ngrams);
  }
  if (ngram_size == 0) {
    return GenerateHybridNgrams(normalized);
  }
  return GenerateNgrams(normalized, ngram_size);
}

uint32_t CountCodePoints(std::string_view text) {
  uint32_t count = 0;
  for (size_t i = 0; i < text.size();) {
    auto byte = static_cast<unsigned char>(text[i]);
    if (byte < 0x80) {
      i += 1;
    } else if ((byte & 0xE0) == 0xC0) {
      i += 2;
    } else if ((byte & 0xF0) == 0xE0) {
      i += 3;
    } else if ((byte & 0xF8) == 0xF0) {
      i += 4;
    } else {
      i += 1;  // Invalid byte, skip
    }
    ++count;
  }
  return count;
}

}  // namespace mygram::utils
