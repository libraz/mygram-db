/**
 * @file mygram_cli_completion_test.cpp
 * @brief Tests for mygram-cli tab completion, which only exists with readline.
 *
 * Built as its own binary because HAVE_READLINE also replaces the interactive
 * prompt, which the main CLI test asserts in its no-readline form. As there,
 * the CLI source is included directly with main() renamed.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdlib>
#include <string>
#include <vector>

// Rename main() to avoid clashing with gtest's main.
#define main cli_main
#include "cli/mygram-cli.cpp"  // NOLINT(bugprone-suspicious-include)
#undef main

namespace {

/// Drive completion for one input line and return the keyword set it offered.
std::vector<std::string> CompletionKeywordsFor(std::string line) {
  rl_line_buffer = line.data();
  current_keywords.clear();
  char** matches = CommandCompletion("", static_cast<int>(line.size()), static_cast<int>(line.size()));
  if (matches != nullptr) {
    for (char** match = matches; *match != nullptr; ++match) {
      // NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory) - readline uses malloc
      free(*match);
    }
    // NOLINTNEXTLINE(cppcoreguidelines-no-malloc,cppcoreguidelines-owning-memory) - readline uses malloc
    free(matches);
  }
  return current_keywords;
}

}  // namespace

TEST(CliCompletionTest, SearchContextOffersEveryClauseTheServerAccepts) {
  // Discovering the protocol through the CLI must not make a clause the parser
  // accepts look like an HTTP-only feature.
  const auto keywords = CompletionKeywordsFor("SEARCH app.threads golang ");
  for (const auto* clause : {"AND", "OR", "NOT", "FILTER", "SORT", "LIMIT", "OFFSET", "HIGHLIGHT", "FUZZY"}) {
    EXPECT_NE(std::find(keywords.begin(), keywords.end(), clause), keywords.end())
        << "SEARCH clause missing from tab completion: " << clause;
  }
}

TEST(CliCompletionTest, SortContextStillOffersOnlySortKeywords) {
  const auto keywords = CompletionKeywordsFor("SEARCH app.threads golang SORT ");
  EXPECT_EQ(keywords, (std::vector<std::string>{"BY", "ASC", "DESC"}));
}
