/**
 * @file highlighter.cpp
 * @brief Search result snippet generation with keyword highlighting
 */

#include "query/highlighter.h"

#include <algorithm>
#include <utility>

#include "utils/string_utils.h"

namespace mygramdb::query {

namespace {

/// @brief Convert a code-point offset to a byte offset in UTF-8 text
size_t CpToByte(std::string_view text, uint32_t cp_offset) {
  size_t byte_pos = 0;
  uint32_t cp_count = 0;
  while (byte_pos < text.size() && cp_count < cp_offset) {
    auto byte = static_cast<unsigned char>(text[byte_pos]);
    if (byte < 0x80) {
      byte_pos += 1;
    } else if ((byte & 0xE0) == 0xC0) {
      byte_pos += 2;
    } else if (byte < 0xC0) {
      // Continuation byte (0x80-0xBF) encountered as start byte — skip it.
      // NOTE: This does not fully validate UTF-8 (e.g., overlong encodings,
      // missing continuation bytes). A more robust approach would use
      // TryParseUtf8Char() from string_utils.cpp, but that function is not
      // currently exposed in the header. For highlighter purposes, this
      // simple heuristic is sufficient since input is pre-validated.
      byte_pos += 1;
    } else if (byte < 0xF0) {
      byte_pos += 3;
    } else {
      byte_pos += 4;
    }
    if (byte_pos > text.size()) {
      byte_pos = text.size();
      break;
    }
    cp_count++;
  }
  return byte_pos;
}

/// @brief Get total code point count
uint32_t TotalCodePoints(std::string_view text) {
  return mygram::utils::CountCodePoints(text);
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

}  // namespace

std::vector<std::pair<uint32_t, uint32_t>> Highlighter::FindMatchPositions(
    std::string_view normalized_text, const std::vector<std::string>& search_terms) {
  // Convert text to codepoints for position tracking
  auto text_cps = mygram::utils::Utf8ToCodepoints(normalized_text);
  uint32_t text_len = static_cast<uint32_t>(text_cps.size());

  std::vector<std::pair<uint32_t, uint32_t>> positions;

  for (const auto& term : search_terms) {
    if (term.empty()) {
      continue;
    }
    auto term_cps = mygram::utils::Utf8ToCodepoints(term);
    uint32_t term_len = static_cast<uint32_t>(term_cps.size());
    if (term_len > text_len) {
      continue;
    }

    // Find all occurrences of term (non-overlapping, greedy left-to-right)
    for (uint32_t i = 0; i + term_len <= text_len; ++i) {
      bool match = true;
      for (uint32_t j = 0; j < term_len; ++j) {
        if (text_cps[i + j] != term_cps[j]) {
          match = false;
          break;
        }
      }
      if (match) {
        positions.emplace_back(i, i + term_len);
        i += term_len - 1;  // Skip past match (loop will increment by 1)
      }
    }
  }

  // Sort by start position, then by end position (longer match first)
  std::sort(positions.begin(), positions.end());

  // Remove overlapping matches (keep the first one)
  std::vector<std::pair<uint32_t, uint32_t>> deduped;
  for (const auto& pos : positions) {
    if (!deduped.empty() && pos.first < deduped.back().second) {
      continue;  // Overlaps with previous match
    }
    deduped.push_back(pos);
  }

  return deduped;
}

HighlightResult Highlighter::Generate(std::string_view normalized_text, const std::vector<std::string>& search_terms,
                                      const HighlightOptions& options) {
  HighlightResult result;

  if (normalized_text.empty() || search_terms.empty()) {
    result.snippet = std::string(normalized_text);
    return result;
  }

  // Find all match positions
  auto match_positions = FindMatchPositions(normalized_text, search_terms);

  if (match_positions.empty()) {
    // No matches found — return prefix of text as snippet
    uint32_t total_cp = TotalCodePoints(normalized_text);
    uint32_t snippet_end = std::min(total_cp, options.snippet_length);
    result.snippet = SubstrByCodePoints(normalized_text, 0, snippet_end);
    if (snippet_end < total_cp) {
      result.snippet += "...";
    }
    return result;
  }

  uint32_t total_cp = TotalCodePoints(normalized_text);
  uint32_t context_radius = options.snippet_length / 2;

  // Build context windows around each match
  std::vector<std::pair<uint32_t, uint32_t>> windows;
  for (const auto& [start, end] : match_positions) {
    uint32_t win_start = (start > context_radius) ? (start - context_radius) : 0;
    uint32_t win_end = std::min(total_cp, end + context_radius);
    windows.emplace_back(win_start, win_end);
  }

  // Merge overlapping windows
  auto merged = MergeWindows(windows);

  // Limit to max_fragments
  if (merged.size() > options.max_fragments) {
    merged.resize(options.max_fragments);
  }

  // Build snippet for each window with highlight tags
  std::string snippet;
  for (size_t wi = 0; wi < merged.size(); ++wi) {
    if (wi > 0) {
      snippet += "...";
    }

    auto [win_start, win_end] = merged[wi];

    // Prefix ellipsis if window doesn't start at beginning
    if (win_start > 0 && wi == 0) {
      snippet += "...";
    }

    // Walk through the window, inserting highlight tags around matches
    uint32_t cursor = win_start;
    for (const auto& [m_start, m_end] : match_positions) {
      // Skip matches outside this window
      if (m_end <= win_start || m_start >= win_end) {
        continue;
      }

      // Text before this match (within window)
      if (m_start > cursor) {
        snippet += SubstrByCodePoints(normalized_text, cursor, m_start);
      }

      // The highlighted match
      snippet += options.open_tag;
      snippet += SubstrByCodePoints(normalized_text, m_start, m_end);
      snippet += options.close_tag;

      cursor = m_end;
    }

    // Remaining text after last match in this window
    if (cursor < win_end) {
      snippet += SubstrByCodePoints(normalized_text, cursor, win_end);
    }

    // Suffix ellipsis if window doesn't reach end
    if (win_end < total_cp && wi == merged.size() - 1) {
      snippet += "...";
    }
  }

  result.snippet = std::move(snippet);
  return result;
}

}  // namespace mygramdb::query
