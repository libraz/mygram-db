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

namespace mygramdb {
namespace utils {

namespace {

/**
 * @brief Get number of bytes in UTF-8 character from first byte
 */
int Utf8CharLength(unsigned char first_byte) {
  if ((first_byte & 0x80) == 0) {
    return 1;  // 0xxxxxxx
  }
  if ((first_byte & 0xE0) == 0xC0) {
    return 2;  // 110xxxxx
  }
  if ((first_byte & 0xF0) == 0xE0) {
    return 3;  // 1110xxxx
  }
  if ((first_byte & 0xF8) == 0xF0) {
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
      codepoint = ((first_byte & 0x1F) << 6) | (static_cast<unsigned char>(text[i + 1]) & 0x3F);
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

  for (uint32_t codepoint : codepoints) {
    if (codepoint <= 0x7F) {
      result += static_cast<char>(codepoint);
    } else if (codepoint <= 0x7FF) {
      result += static_cast<char>(0xC0 | (codepoint >> 6));
      result += static_cast<char>(0x80 | (codepoint & 0x3F));
    } else if (codepoint <= 0xFFFF) {
      result += static_cast<char>(0xE0 | (codepoint >> 12));
      result += static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F));
      result += static_cast<char>(0x80 | (codepoint & 0x3F));
    } else if (codepoint <= 0x10FFFF) {
      result += static_cast<char>(0xF0 | (codepoint >> 18));
      result += static_cast<char>(0x80 | ((codepoint >> 12) & 0x3F));
      result += static_cast<char>(0x80 | ((codepoint >> 6) & 0x3F));
      result += static_cast<char>(0x80 | (codepoint & 0x3F));
    }
  }

  return result;
}

#ifdef USE_ICU
std::string NormalizeTextICU(const std::string& text, bool nfkc, const std::string& width,
                             bool lower) {
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

std::string NormalizeText(const std::string& text, bool nfkc, const std::string& width,
                          bool lower) {
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
  return (codepoint >= 0x4E00 && codepoint <= 0x9FFF) ||    // Main block
         (codepoint >= 0x3400 && codepoint <= 0x4DBF) ||    // Extension A
         (codepoint >= 0x20000 && codepoint <= 0x2A6DF) ||  // Extension B
         (codepoint >= 0x2A700 && codepoint <= 0x2B73F) ||  // Extension C
         (codepoint >= 0x2B740 && codepoint <= 0x2B81F) ||  // Extension D
         (codepoint >= 0xF900 && codepoint <= 0xFAFF);      // Compatibility
}

}  // namespace

std::vector<std::string> GenerateHybridNgrams(const std::string& text, int ascii_ngram_size,
                                              int kanji_ngram_size) {
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
        for (int j = 0; j < kanji_ngram_size; ++j) {
          ngram_codepoints.push_back(codepoints[i + j]);
        }
        ngrams.push_back(CodepointsToUtf8(ngram_codepoints));
      }
    } else {
      // Non-CJK character: use ascii_ngram_size
      if (i + ascii_ngram_size <= codepoints.size()) {
        std::vector<uint32_t> ngram_codepoints;
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

  while (size >= 1024.0 && unit_index < kUnits.size() - 1) {
    size /= 1024.0;
    unit_index++;
  }

  // Format with appropriate precision
  std::ostringstream oss;
  // NOLINTBEGIN(cppcoreguidelines-pro-bounds-constant-array-index)
  if (size >= 100.0) {
    oss << std::fixed << std::setprecision(0) << size << kUnits[unit_index];
  } else if (size >= 10.0) {
    oss << std::fixed << std::setprecision(1) << size << kUnits[unit_index];
  } else {
    oss << std::fixed << std::setprecision(2) << size << kUnits[unit_index];
  }
  // NOLINTEND(cppcoreguidelines-pro-bounds-constant-array-index)

  return oss.str();
}

}  // namespace utils
}  // namespace mygramdb
