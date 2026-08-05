/**
 * @file highlighter.cpp
 * @brief Search result snippet generation with keyword highlighting
 */

#include "query/highlighter.h"

#include <algorithm>
#include <memory>
#include <utility>

#include "utils/string_utils.h"

#ifdef USE_ICU
#include <unicode/brkiter.h>
#include <unicode/locid.h>
#include <unicode/unistr.h>
#endif

namespace mygramdb::query {

namespace {

struct OriginalSegment {
  size_t byte_start;
  size_t byte_end;
  uint32_t cp_start;
  uint32_t cp_end;
};

std::vector<OriginalSegment> SegmentOriginalText(std::string_view text) {
  std::vector<OriginalSegment> segments;
#ifdef USE_ICU
  UErrorCode status = U_ZERO_ERROR;
  std::unique_ptr<icu::BreakIterator> iterator(
      icu::BreakIterator::createCharacterInstance(icu::Locale::getRoot(), status));
  if (U_SUCCESS(status) && iterator != nullptr) {
    const icu::UnicodeString unicode_text =
        icu::UnicodeString::fromUTF8(icu::StringPiece(text.data(), static_cast<int32_t>(text.size())));
    iterator->setText(unicode_text);
    int32_t utf16_start = iterator->first();
    size_t byte_start = 0;
    uint32_t cp_start = 0;
    for (int32_t utf16_end = iterator->next(); utf16_end != icu::BreakIterator::DONE;
         utf16_start = utf16_end, utf16_end = iterator->next()) {
      const icu::UnicodeString cluster = unicode_text.tempSubStringBetween(utf16_start, utf16_end);
      std::string utf8_cluster;
      cluster.toUTF8String(utf8_cluster);
      const size_t byte_end = byte_start + utf8_cluster.size();
      const uint32_t cp_end = cp_start + static_cast<uint32_t>(cluster.countChar32());
      segments.push_back({byte_start, byte_end, cp_start, cp_end});
      byte_start = byte_end;
      cp_start = cp_end;
    }
    if (byte_start == text.size()) {
      return segments;
    }
    segments.clear();
  }
#endif

  const auto* data = reinterpret_cast<const unsigned char*>(text.data());
  size_t byte_offset = 0;
  uint32_t cp_offset = 0;
  while (byte_offset < text.size()) {
    uint32_t codepoint = 0;
    const int parsed_length =
        mygram::utils::TryParseUtf8Char(data + byte_offset, text.size() - byte_offset, &codepoint);
    const size_t char_length = parsed_length < 0 ? 1 : static_cast<size_t>(parsed_length);
    segments.push_back({byte_offset, byte_offset + char_length, cp_offset, cp_offset + 1});
    byte_offset += char_length;
    ++cp_offset;
  }
  return segments;
}

/// @brief Convert a code-point offset to a byte offset in UTF-8 text
size_t CpToByte(std::string_view text, uint32_t cp_offset) {
  size_t byte_pos = 0;
  uint32_t cp_count = 0;
  const auto* data = reinterpret_cast<const unsigned char*>(text.data());
  while (byte_pos < text.size() && cp_count < cp_offset) {
    uint32_t codepoint = 0;
    const int char_length = mygram::utils::TryParseUtf8Char(data + byte_pos, text.size() - byte_pos, &codepoint);
    if (char_length < 0) {
      ++byte_pos;
      continue;
    }
    byte_pos += static_cast<size_t>(char_length);
    ++cp_count;
  }
  return byte_pos;
}

/// @brief Get total code point count (narrowed to uint32_t for codepoint
/// position arithmetic -- no practical text exceeds 4G codepoints)
uint32_t TotalCodePoints(std::string_view text) {
  return static_cast<uint32_t>(mygram::utils::CountCodePoints(text));
}

/// @brief Extract a UTF-8 substring by code-point range [cp_start, cp_end)
std::string SubstrByCodePoints(std::string_view text, uint32_t cp_start, uint32_t cp_end) {
  size_t byte_start = CpToByte(text, cp_start);
  size_t byte_end = CpToByte(text, cp_end);
  if (byte_start > text.size()) {
    byte_start = text.size();
  }
  if (byte_end > text.size()) {
    byte_end = text.size();
  }
  return std::string(text.substr(byte_start, byte_end - byte_start));
}

/// @brief Merge overlapping windows into non-overlapping ranges
std::vector<std::pair<uint32_t, uint32_t>> MergeWindows(std::vector<std::pair<uint32_t, uint32_t>>& windows) {
  if (windows.empty()) {
    return {};
  }
  std::sort(windows.begin(), windows.end());
  std::vector<std::pair<uint32_t, uint32_t>> merged;
  merged.push_back(windows[0]);
  for (size_t i = 1; i < windows.size(); ++i) {
    auto& last = merged.back();
    if (windows[i].first <= last.second) {
      last.second = std::max(last.second, windows[i].second);
    } else {
      merged.push_back(windows[i]);
    }
  }
  return merged;
}

/// @brief Enforce the documented code-point bound after overlapping windows merge.
///
/// Dense matches can merge many individually bounded windows into one document-sized
/// range. Anchor a truncated range on the first match in the merged window so every
/// emitted fragment remains at most snippet_length code points.
void ClampWindows(std::vector<std::pair<uint32_t, uint32_t>>& windows,
                  const std::vector<std::pair<uint32_t, uint32_t>>& matches, uint32_t snippet_length,
                  uint32_t total_cp) {
  for (auto& [win_start, win_end] : windows) {
    if (win_end - win_start <= snippet_length) {
      continue;
    }

    auto first_match = std::find_if(matches.begin(), matches.end(), [&](const auto& match) {
      return match.second > win_start && match.first < win_end;
    });
    const uint32_t anchor = first_match != matches.end() ? first_match->first : win_start;
    const uint32_t left_context = snippet_length / 2;
    uint32_t clamped_start = anchor > left_context ? anchor - left_context : 0;
    if (clamped_start + snippet_length > total_cp) {
      clamped_start = total_cp > snippet_length ? total_cp - snippet_length : 0;
    }
    win_start = std::max(win_start, clamped_start);
    win_end = std::min(win_end, win_start + snippet_length);
  }
}

HighlightResult GenerateWithPositions(std::string_view text,
                                      const std::vector<std::pair<uint32_t, uint32_t>>& match_positions,
                                      const HighlightOptions& options) {
  HighlightResult result;
  const uint32_t total_cp = TotalCodePoints(text);
  if (match_positions.empty()) {
    const uint32_t snippet_end = std::min(total_cp, options.snippet_length);
    result.snippet = SubstrByCodePoints(text, 0, snippet_end);
    if (snippet_end < total_cp) {
      result.snippet += "...";
    }
    return result;
  }

  const uint32_t context_radius = options.snippet_length / 2;
  std::vector<std::pair<uint32_t, uint32_t>> windows;
  for (const auto& [start, end] : match_positions) {
    const uint32_t win_start = start > context_radius ? start - context_radius : 0;
    const uint32_t win_end = std::min(total_cp, end + context_radius);
    windows.emplace_back(win_start, win_end);
  }

  auto merged = MergeWindows(windows);
  ClampWindows(merged, match_positions, options.snippet_length, total_cp);
  if (merged.size() > options.max_fragments) {
    merged.resize(options.max_fragments);
  }

  std::string snippet;
  for (size_t wi = 0; wi < merged.size(); ++wi) {
    if (wi > 0) {
      snippet += "...";
    }
    auto [win_start, win_end] = merged[wi];
    if (win_start > 0 && wi == 0) {
      snippet += "...";
    }

    uint32_t cursor = win_start;
    for (const auto& [m_start, m_end] : match_positions) {
      if (m_end <= win_start || m_start >= win_end) {
        continue;
      }
      if (m_start > cursor) {
        snippet += SubstrByCodePoints(text, cursor, m_start);
      }
      const uint32_t clipped_start = std::max(m_start, win_start);
      const uint32_t clipped_end = std::min(m_end, win_end);
      if (clipped_start >= clipped_end) {
        continue;
      }
      snippet += options.open_tag;
      snippet += SubstrByCodePoints(text, clipped_start, clipped_end);
      snippet += options.close_tag;
      cursor = clipped_end;
    }
    if (cursor < win_end) {
      snippet += SubstrByCodePoints(text, cursor, win_end);
    }
    if (win_end < total_cp && wi == merged.size() - 1) {
      snippet += "...";
    }
  }
  result.snippet = std::move(snippet);
  return result;
}

std::vector<std::pair<uint32_t, uint32_t>> RemoveOverlappingMatchPositions(
    std::vector<std::pair<uint32_t, uint32_t>> positions) {
  // Keep the longest match for a shared start, then retain only the first
  // non-overlapping range. GenerateWithPositions expects this invariant: a
  // later overlapping range would otherwise append already-emitted text.
  std::sort(positions.begin(), positions.end(), [](const auto& lhs, const auto& rhs) {
    if (lhs.first != rhs.first) {
      return lhs.first < rhs.first;
    }
    return lhs.second > rhs.second;
  });

  std::vector<std::pair<uint32_t, uint32_t>> deduped;
  deduped.reserve(positions.size());
  for (const auto& pos : positions) {
    if (!deduped.empty() && pos.first < deduped.back().second) {
      continue;
    }
    deduped.push_back(pos);
  }
  return deduped;
}

}  // namespace

std::vector<std::pair<uint32_t, uint32_t>> Highlighter::FindMatchPositions(
    std::string_view normalized_text, const std::vector<std::string>& search_terms) {
  std::vector<std::pair<uint32_t, uint32_t>> positions;

  for (const auto& term : search_terms) {
    if (term.empty()) {
      continue;
    }
    if (term.size() > normalized_text.size()) {
      continue;
    }

    // Do byte-level matching first with string_view::find to locate candidate
    // positions, then convert only matched byte offsets to codepoint positions.
    // This avoids converting the entire document text to a codepoint array
    // when there are few or no matches.
    auto term_cp_count = mygram::utils::CountCodePoints(term);

    // Track byte/codepoint position from previous match to count incrementally
    // instead of re-scanning from byte 0 for every match (O(n) per match ->
    // O(n) total per term).
    size_t prev_byte_pos = 0;
    size_t prev_cp_pos = 0;

    size_t search_start = 0;
    while (search_start <= normalized_text.size() - term.size()) {
      size_t byte_pos = normalized_text.find(term, search_start);
      if (byte_pos == std::string_view::npos) {
        break;
      }

      // Incrementally count codepoints from previous match position
      prev_cp_pos += mygram::utils::CountCodePoints(normalized_text.substr(prev_byte_pos, byte_pos - prev_byte_pos));
      prev_byte_pos = byte_pos;

      auto cp_start = static_cast<uint32_t>(prev_cp_pos);
      auto cp_end = static_cast<uint32_t>(prev_cp_pos + term_cp_count);
      positions.emplace_back(cp_start, cp_end);

      // Skip past the match (non-overlapping)
      search_start = byte_pos + term.size();
    }
  }

  return RemoveOverlappingMatchPositions(std::move(positions));
}

HighlightResult Highlighter::Generate(std::string_view normalized_text, const std::vector<std::string>& search_terms,
                                      const HighlightOptions& options) {
  if (normalized_text.empty()) {
    HighlightResult result;
    result.snippet = std::string(normalized_text);
    return result;
  }

  auto match_positions = FindMatchPositions(normalized_text, search_terms);
  return GenerateWithPositions(normalized_text, match_positions, options);
}

HighlightResult Highlighter::GenerateOriginal(std::string_view original_text,
                                              const std::vector<std::string>& normalized_search_terms,
                                              const std::function<std::string(std::string_view)>& normalizer,
                                              const HighlightOptions& options) {
  if (original_text.empty()) {
    HighlightResult result;
    result.snippet = std::string(original_text);
    return result;
  }

  // A negation-only query has no positive terms to highlight. It still must
  // use the no-match snippet path, otherwise a full document is returned.
  if (normalized_search_terms.empty()) {
    return GenerateWithPositions(original_text, {}, options);
  }

  const std::string normalized_text = normalizer(original_text);
  std::vector<std::pair<uint32_t, uint32_t>> normalized_to_original;
  for (const auto& segment : SegmentOriginalText(original_text)) {
    const std::string normalized_piece =
        normalizer(original_text.substr(segment.byte_start, segment.byte_end - segment.byte_start));
    const size_t normalized_cp_count = mygram::utils::CountCodePoints(normalized_piece);
    for (size_t i = 0; i < normalized_cp_count; ++i) {
      normalized_to_original.emplace_back(segment.cp_start, segment.cp_end);
    }
  }

  // The full normalized string is authoritative for matching. A custom
  // normalizer that composes across grapheme boundaries cannot be mapped
  // safely; fall back to the bounded no-match snippet instead of highlighting
  // the wrong original bytes.
  if (normalized_to_original.size() != mygram::utils::CountCodePoints(normalized_text)) {
    return GenerateWithPositions(original_text, {}, options);
  }

  const auto normalized_matches = FindMatchPositions(normalized_text, normalized_search_terms);
  std::vector<std::pair<uint32_t, uint32_t>> original_matches;
  original_matches.reserve(normalized_matches.size());
  for (const auto& [start, end] : normalized_matches) {
    if (start >= normalized_to_original.size() || end == 0 || end > normalized_to_original.size()) {
      continue;
    }
    original_matches.emplace_back(normalized_to_original[start].first, normalized_to_original[end - 1].second);
  }
  return GenerateWithPositions(original_text, RemoveOverlappingMatchPositions(std::move(original_matches)), options);
}

}  // namespace mygramdb::query
