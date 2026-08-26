/**
 * @file highlighter_cost_test.cpp
 * @brief What snippet generation costs as a document grows.
 *
 * HIGHLIGHT runs on the request thread for every document on the page, so the
 * work it does per document is multiplied by the page size. Mapping normalized
 * match offsets back onto the original text needs to know how many normalized
 * code points each grapheme cluster of the original contributes, and the
 * normalizer is the expensive part of that: a full ICU pipeline per call.
 *
 * The stable measurement is therefore the number of normalizer invocations, not
 * a duration. It is counted for the same document rendered at several lengths
 * over a fixed alphabet, so the reported growth is attributable to document
 * length alone. Wall clock is printed alongside for context only.
 */

#include <gtest/gtest.h>

#include <array>
#include <chrono>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "query/highlighter.h"
#include "utils/string_utils.h"

namespace mygramdb::query {
namespace {

constexpr std::array<size_t, 3> kDocumentLengths = {2000, 8000, 32000};
constexpr const char* kMatchTerm = "tokyo";

/// @brief Append filler words until the text reaches the requested length.
///
/// The running length is carried rather than recounted, so building a fixture
/// stays linear and a profile of this test shows the highlighter rather than
/// the harness.
template <size_t N>
std::string BuildDocument(std::string_view prefix, const std::array<std::string_view, N>& words, size_t cluster_count) {
  std::string text(prefix);
  size_t length = mygram::utils::CountCodePoints(text);
  size_t word = 0;
  while (length < cluster_count) {
    const std::string_view next = words[word % N];
    text.append(next);
    length += mygram::utils::CountCodePoints(next);
    ++word;
  }
  return text;
}

/// @brief Latin filler over a small alphabet, with one occurrence of the term.
std::string MakeLatinDocument(size_t cluster_count) {
  static constexpr std::array<std::string_view, 10> kWords = {"the ",  "quick ", "brown ", "fox ", "jumps ",
                                                              "over ", "lazy ",  "dog ",   "and ", "again "};
  return BuildDocument("tokyo tower ", kWords, cluster_count);
}

/// @brief Japanese filler over a small character set, with one term occurrence.
std::string MakeJapaneseDocument(size_t cluster_count) {
  static constexpr std::array<std::string_view, 10> kWords = {"東京", "都心", "検索", "全文", "索引",
                                                              "文書", "更新", "同期", "複製", "接続"};
  return BuildDocument("tokyo タワー ", kWords, cluster_count);
}

struct HighlightCost {
  size_t normalizer_calls = 0;
  double elapsed_ms = 0.0;
  size_t snippet_bytes = 0;
};

HighlightCost MeasureHighlight(const std::string& document) {
  HighlightOptions options;
  options.snippet_length = 100;
  options.max_fragments = 3;

  HighlightCost cost;
  const auto normalizer = [&cost](std::string_view text) {
    ++cost.normalizer_calls;
    return mygram::utils::NormalizeText(text, true, "narrow", true);
  };
  const std::vector<std::string> terms = {normalizer(kMatchTerm)};
  cost.normalizer_calls = 0;

  const auto start = std::chrono::steady_clock::now();
  const auto result = Highlighter::GenerateOriginal(document, terms, normalizer, options);
  const auto end = std::chrono::steady_clock::now();
  cost.elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();
  cost.snippet_bytes = result.snippet.size();
  return cost;
}

void ReportRow(std::string_view script, size_t clusters, const HighlightCost& cost) {
  std::cout << "  " << std::left << std::setw(10) << script << std::right << std::setw(10) << clusters << std::setw(14)
            << cost.normalizer_calls << std::setw(12) << std::fixed << std::setprecision(3) << cost.elapsed_ms
            << std::setw(10) << cost.snippet_bytes << "\n";
}

}  // namespace

class HighlighterCostTest : public ::testing::Test {};

/**
 * @brief Normalizer invocations must not scale with document length.
 *
 * The snippet is bounded by snippet_length and max_fragments, and the match set
 * is the same at every length, so a longer document must not buy more
 * normalization work. The assertion is on the growth factor rather than an
 * absolute count, which keeps it meaningful on any machine and under any ICU
 * build.
 */
TEST_F(HighlighterCostTest, NormalizerCallsDoNotGrowWithDocumentLength) {
  std::cout << "\nHIGHLIGHT cost by document length (one match, snippet_length=100)\n";
  std::cout << "  " << std::left << std::setw(10) << "script" << std::right << std::setw(10) << "clusters"
            << std::setw(14) << "normalize()" << std::setw(12) << "ms" << std::setw(10) << "bytes"
            << "\n";

  std::vector<HighlightCost> latin;
  std::vector<HighlightCost> japanese;
  for (size_t clusters : kDocumentLengths) {
    latin.push_back(MeasureHighlight(MakeLatinDocument(clusters)));
    ReportRow("latin", clusters, latin.back());
  }
  for (size_t clusters : kDocumentLengths) {
    japanese.push_back(MeasureHighlight(MakeJapaneseDocument(clusters)));
    ReportRow("japanese", clusters, japanese.back());
  }

  // A 16x longer document over the same alphabet must not cost materially more
  // normalizer calls. The bound leaves room for the fixed per-request calls and
  // for the handful of distinct clusters the longer text introduces.
  const double latin_growth =
      static_cast<double>(latin.back().normalizer_calls) / static_cast<double>(latin.front().normalizer_calls);
  const double japanese_growth =
      static_cast<double>(japanese.back().normalizer_calls) / static_cast<double>(japanese.front().normalizer_calls);
  std::cout << "  growth over 16x length: latin x" << std::fixed << std::setprecision(2) << latin_growth
            << ", japanese x" << japanese_growth << "\n";

  EXPECT_LT(latin_growth, 1.5);
  EXPECT_LT(japanese_growth, 1.5);
}

/**
 * @brief A document with no match must not be normalized cluster by cluster.
 *
 * Highlighting a page can include documents that matched through the index but
 * do not contain the highlight term literally. Those render the leading
 * snippet, and that outcome does not depend on any offset mapping.
 */
TEST_F(HighlighterCostTest, NoMatchCostsAFixedNumberOfNormalizerCalls) {
  HighlightOptions options;
  options.snippet_length = 100;
  options.max_fragments = 3;

  std::cout << "\nHIGHLIGHT cost with no match in the document\n";
  std::cout << "  " << std::left << std::setw(10) << "script" << std::right << std::setw(10) << "clusters"
            << std::setw(14) << "normalize()" << std::setw(12) << "ms" << std::setw(10) << "bytes"
            << "\n";

  std::vector<size_t> calls;
  for (size_t clusters : kDocumentLengths) {
    std::string document = MakeLatinDocument(clusters);
    size_t normalizer_calls = 0;
    const auto normalizer = [&normalizer_calls](std::string_view text) {
      ++normalizer_calls;
      return mygram::utils::NormalizeText(text, true, "narrow", true);
    };
    const std::vector<std::string> terms = {normalizer("nonexistentterm")};
    normalizer_calls = 0;

    const auto start = std::chrono::steady_clock::now();
    const auto result = Highlighter::GenerateOriginal(document, terms, normalizer, options);
    const auto end = std::chrono::steady_clock::now();
    HighlightCost cost;
    cost.normalizer_calls = normalizer_calls;
    cost.elapsed_ms = std::chrono::duration<double, std::milli>(end - start).count();
    cost.snippet_bytes = result.snippet.size();
    ReportRow("latin", clusters, cost);
    calls.push_back(normalizer_calls);
  }

  // One pass over the document decides there is nothing to highlight; nothing
  // beyond that is a function of length.
  EXPECT_LE(calls.back(), calls.front() + 1);
}

}  // namespace mygramdb::query
