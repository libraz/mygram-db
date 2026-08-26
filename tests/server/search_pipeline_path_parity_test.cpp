/**
 * @file search_pipeline_path_parity_test.cpp
 * @brief The four search execution paths must agree where they are equivalent
 *
 * ExecuteFullPipeline dispatches to one of four executors — the boolean AST
 * evaluator, the fuzzy threshold search, the synonym-aware search, and the
 * plain n-gram intersection — and each carries its own copy of the
 * post-processing that follows the candidate set: the NOT filter, the column
 * filters, the verify_text post-filter and the hybrid-fragment exact-text
 * guard. A rule applied on one executor and not another is invisible to tests
 * that exercise each path on its own.
 *
 * Every case here derives all four requests from one input and compares the
 * returned document sets, not their sizes.
 *
 * The reductions used, verified against ExecuteFullPipeline's dispatch:
 *
 *  - Fuzzy with max_distance 0. The threshold ExecuteWithFuzzy computes is
 *    |ngrams| - 0, which is the full n-gram AND the plain path performs.
 *    PostFilterByFuzzyText at distance 0 accepts only an exact substring.
 *  - A synonym dictionary that expands nothing. SynonymDictionary::Expand
 *    returns the input term alone, so every group is a single variant and the
 *    OR-within-group union degenerates to the plain term search.
 *  - `T OR T` for the boolean path. A bare term does not reach the AST
 *    evaluator at all: ContainsBooleanSyntax has to see an operator between two
 *    primaries, so neither `T` nor `(T)` selects it. Repeating the term is the
 *    smallest expression that does, and its union with itself is the term.
 *
 * Terms are chosen long enough to generate n-grams. A term shorter than the
 * n-gram size takes the plain path's substring fallback but makes the fuzzy
 * path report an empty term, so the paths are genuinely not equivalent there
 * and that case is not claimed as one.
 */

#include <gtest/gtest.h>
#include <unistd.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "config/config.h"
#include "index/index.h"
#include "query/query_parser.h"
#include "query/synonym_dictionary.h"
#include "server/search_pipeline.h"
#include "storage/document_store.h"

namespace mygramdb::server::search_pipeline {

namespace {

/// Which executor ExecuteFullPipeline is being steered into.
enum class Path { kRegular, kFuzzyZero, kSynonymEmpty, kBoolean };

const char* PathName(Path path) {
  switch (path) {
    case Path::kRegular:
      return "regular";
    case Path::kFuzzyZero:
      return "fuzzy(distance 0)";
    case Path::kSynonymEmpty:
      return "synonym(empty dictionary)";
    case Path::kBoolean:
      return "boolean AST";
  }
  return "unknown";
}

// Japanese text, hex-escaped so the source encoding does not matter.
constexpr const char* kCatDog = "\xe7\x8c\xab\xe7\x8a\xac";  // 猫犬

}  // namespace

/**
 * @brief One table, one configuration, four ways to ask the same question.
 *
 * kanji_ngram_size differs from ngram_size so that a term mixing CJK and ASCII
 * produces n-grams covering only part of it — the shape that makes the n-gram
 * intersection admit documents that do not contain the term, and that every
 * path has to reject independently.
 */
class SearchPathParityTest : public ::testing::Test {
 protected:
  static constexpr int kNgramSize = 3;
  static constexpr int kKanjiNgramSize = 2;

  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("mygramdb_path_parity_" + std::to_string(::getpid()) + "_" + std::to_string(counter_.fetch_add(1)));
    std::filesystem::create_directories(dir_);

    index_ = std::make_unique<index::Index>(kNgramSize, kKanjiNgramSize, /*roaring_threshold=*/0.1,
                                            /*cross_boundary_ngrams=*/false, /*normalize_nfkc=*/true,
                                            /*normalize_width=*/"half", /*normalize_lower=*/true);
    doc_store_ = std::make_unique<storage::DocumentStore>();
    empty_synonyms_ = std::make_unique<query::SynonymDictionary>();

    AddDocument("doc_alpha", "alpha report published", {{"category", "tech"}, {"status", "published"}});
    AddDocument("doc_beta", "beta report draft", {{"category", "tech"}, {"status", "draft"}});
    AddDocument("doc_gamma", "alpha summary published", {{"category", "news"}, {"status", "published"}});
    AddDocument("doc_delta", "unrelated content", {{"category", "news"}, {"status", "published"}});
    // Contains the mixed CJK/ASCII term in full.
    AddDocument("doc_hybrid_match", std::string(kCatDog) + "ab report", {{"category", "tech"}});
    // Shares every n-gram the mixed term generates without containing it.
    AddDocument("doc_hybrid_ngram_only", std::string(kCatDog) + "cd report", {{"category", "tech"}});
  }

  void TearDown() override { std::filesystem::remove_all(dir_); }

  void AddDocument(const std::string& primary_key, const std::string& text,
                   const std::vector<std::pair<std::string, std::string>>& filters) {
    storage::FilterMap filter_map;
    for (const auto& [column, value] : filters) {
      filter_map[column] = value;
    }
    const std::string normalized = index_->NormalizeText(text);
    auto doc_id = doc_store_->AddDocument(primary_key, filter_map, normalized, text);
    ASSERT_TRUE(doc_id.has_value());
    index_->AddDocument(*doc_id, normalized);
  }

  /// A dictionary that names terms the corpus never uses, so loading it cannot
  /// change which documents are reachable while still selecting the synonym
  /// executor.
  void LoadUnrelatedSynonyms() {
    const auto path = dir_ / "synonyms.tsv";
    std::ofstream output(path);
    output << "zeta\teta\n";
    output.close();
    auto normalizer = [this](std::string_view text) { return index_->NormalizeText(text); };
    ASSERT_TRUE(unrelated_synonyms_.LoadFromFile(path.string(), normalizer).has_value());
  }

  FullPipelineParams MakeParams(Path path, const query::SynonymDictionary* synonym_dict = nullptr) {
    FullPipelineParams params;
    params.current_index = index_.get();
    params.current_doc_store = doc_store_.get();
    params.full_config = &config_;
    params.ngram_size = kNgramSize;
    params.kanji_ngram_size = kKanjiNgramSize;
    params.cross_boundary_ngrams = false;
    params.filter_threshold = filter_threshold_;
    params.primary_key_column = "id";
    if (path == Path::kSynonymEmpty) {
      params.synonym_dict = synonym_dict != nullptr ? synonym_dict : empty_synonyms_.get();
    }
    return params;
  }

  /// Build the request for @p path from the one @p term the caller supplied.
  static query::Query MakeQuery(Path path, const std::string& term) {
    query::Query query;
    query.type = query::QueryType::SEARCH;
    query.table = "test";
    query.limit = 100;
    if (path == Path::kBoolean) {
      query.search_text = term + " OR " + term;
    } else {
      query.search_text = term;
    }
    if (path == Path::kFuzzyZero) {
      query.fuzzy_max_distance = 0;
    }
    return query;
  }

  /// Run @p term down @p path and return the primary keys it selects.
  std::vector<std::string> RunPath(Path path, const std::string& term,
                                   const std::function<void(query::Query&)>& decorate = {},
                                   const query::SynonymDictionary* synonym_dict = nullptr) {
    query::Query query = MakeQuery(path, term);
    if (decorate) {
      decorate(query);
    }
    auto params = MakeParams(path, synonym_dict);
    auto output = ExecuteFullPipeline(query, params);
    EXPECT_TRUE(output.has_value()) << PathName(path) << ": "
                                    << (output.has_value() ? std::string{} : output.error().message());
    if (!output) {
      return {};
    }

    std::vector<std::string> primary_keys;
    primary_keys.reserve(output->results.size());
    for (const auto doc_id : output->results) {
      auto primary_key = doc_store_->GetPrimaryKey(doc_id);
      EXPECT_TRUE(primary_key.has_value());
      primary_keys.push_back(primary_key.value_or(""));
    }
    std::sort(primary_keys.begin(), primary_keys.end());
    return primary_keys;
  }

  /// Compare all four paths against the plain path for one input.
  void ExpectAllPathsAgree(const std::string& term, const std::function<void(query::Query&)>& decorate = {},
                           const query::SynonymDictionary* synonym_dict = nullptr) {
    SCOPED_TRACE("term: " + term);
    const auto regular = RunPath(Path::kRegular, term, decorate);
    for (const Path path : {Path::kFuzzyZero, Path::kSynonymEmpty, Path::kBoolean}) {
      SCOPED_TRACE(PathName(path));
      EXPECT_EQ(RunPath(path, term, decorate, synonym_dict), regular);
    }
  }

  static std::atomic<int> counter_;
  std::filesystem::path dir_;
  config::Config config_;  ///< memory.verify_text stays at its default "off"
  size_t filter_threshold_ = 1000;
  std::unique_ptr<index::Index> index_;
  std::unique_ptr<storage::DocumentStore> doc_store_;
  std::unique_ptr<query::SynonymDictionary> empty_synonyms_;
  query::SynonymDictionary unrelated_synonyms_;
};

std::atomic<int> SearchPathParityTest::counter_{0};

// ---------------------------------------------------------------------------
// The reductions themselves
// ---------------------------------------------------------------------------

// Each path has to be the one the test believes it is exercising, or every
// comparison below is comparing the plain path with itself.
TEST_F(SearchPathParityTest, EachRequestShapeSelectsTheIntendedExecutor) {
  auto regular = ExecuteFullPipeline(MakeQuery(Path::kRegular, "report"), MakeParams(Path::kRegular));
  ASSERT_TRUE(regular.has_value()) << regular.error().message();
  EXPECT_EQ(regular->path_taken, PipelinePath::REGULAR);

  auto fuzzy = ExecuteFullPipeline(MakeQuery(Path::kFuzzyZero, "report"), MakeParams(Path::kFuzzyZero));
  ASSERT_TRUE(fuzzy.has_value()) << fuzzy.error().message();
  EXPECT_EQ(fuzzy->path_taken, PipelinePath::FUZZY);

  auto synonym = ExecuteFullPipeline(MakeQuery(Path::kSynonymEmpty, "report"), MakeParams(Path::kSynonymEmpty));
  ASSERT_TRUE(synonym.has_value()) << synonym.error().message();
  EXPECT_EQ(synonym->path_taken, PipelinePath::SYNONYM);

  // The boolean expression reaches the AST evaluator, which reports itself as
  // REGULAR when neither fuzzy nor synonyms are in play. What distinguishes it
  // is that a bare term does not reach it at all.
  auto boolean = ExecuteFullPipeline(MakeQuery(Path::kBoolean, "report"), MakeParams(Path::kBoolean));
  ASSERT_TRUE(boolean.has_value()) << boolean.error().message();
  EXPECT_EQ(boolean->path_taken, PipelinePath::REGULAR);
  // Only the AST path leaves this false for a single-term query, because only
  // it declines to claim the single-term n-gram AND reproduces its semantics.
  EXPECT_TRUE(regular->semantics_reproducible_by_single_term_ngram_and);
  EXPECT_FALSE(boolean->semantics_reproducible_by_single_term_ngram_and);
}

// ---------------------------------------------------------------------------
// Equivalent inputs, compared by the documents they select
// ---------------------------------------------------------------------------

TEST_F(SearchPathParityTest, PlainTermSelectsTheSameDocumentsOnEveryPath) {
  const auto expected = std::vector<std::string>{"doc_alpha", "doc_beta", "doc_hybrid_match", "doc_hybrid_ngram_only"};
  EXPECT_EQ(RunPath(Path::kRegular, "report"), expected);
  ExpectAllPathsAgree("report");
}

TEST_F(SearchPathParityTest, TermMatchingNothingSelectsNothingOnEveryPath) {
  EXPECT_TRUE(RunPath(Path::kRegular, "nonexistentterm").empty());
  ExpectAllPathsAgree("nonexistentterm");
}

TEST_F(SearchPathParityTest, NotTermIsExcludedOnEveryPath) {
  auto exclude_draft = [](query::Query& query) { query.not_terms = {"draft"}; };
  EXPECT_EQ(RunPath(Path::kRegular, "report", exclude_draft),
            (std::vector<std::string>{"doc_alpha", "doc_hybrid_match", "doc_hybrid_ngram_only"}));
  ExpectAllPathsAgree("report", exclude_draft);
}

TEST_F(SearchPathParityTest, NotTermUnderTheVerifyingPolicyIsExcludedOnEveryPath) {
  // Exclusion evidence is held to the verification policy, and each executor
  // decides that for itself.
  config_.memory.verify_text = "all";
  auto exclude_draft = [](query::Query& query) { query.not_terms = {"draft"}; };
  ExpectAllPathsAgree("report", exclude_draft);
}

TEST_F(SearchPathParityTest, ColumnFiltersApplyOnEveryPath) {
  auto only_tech = [](query::Query& query) { query.filters.push_back({"category", query::FilterOp::EQ, "tech"}); };
  EXPECT_EQ(RunPath(Path::kRegular, "report", only_tech),
            (std::vector<std::string>{"doc_alpha", "doc_beta", "doc_hybrid_match", "doc_hybrid_ngram_only"}));
  ExpectAllPathsAgree("report", only_tech);

  auto not_equal_tech = [](query::Query& query) { query.filters.push_back({"category", query::FilterOp::NE, "tech"}); };
  ExpectAllPathsAgree("report", not_equal_tech);
}

TEST_F(SearchPathParityTest, NotTermAndColumnFilterCombineTheSameWayOnEveryPath) {
  auto both = [](query::Query& query) {
    query.not_terms = {"draft"};
    query.filters.push_back({"status", query::FilterOp::EQ, "published"});
  };
  EXPECT_EQ(RunPath(Path::kRegular, "report", both), (std::vector<std::string>{"doc_alpha"}));
  ExpectAllPathsAgree("report", both);
}

// The n-gram intersection cannot distinguish a document that contains a mixed
// CJK/ASCII term from one that merely carries all of its fragments, so every
// path has to reject the second — independently of memory.verify_text.
TEST_F(SearchPathParityTest, HybridFragmentGuardRejectsTheSameDocumentOnEveryPath) {
  const std::string hybrid_term = std::string(kCatDog) + "ab";
  EXPECT_EQ(RunPath(Path::kRegular, hybrid_term), (std::vector<std::string>{"doc_hybrid_match"}));
  ExpectAllPathsAgree(hybrid_term);
}

TEST_F(SearchPathParityTest, HybridFragmentGuardHoldsUnderTheVerifyingPolicyToo) {
  config_.memory.verify_text = "all";
  const std::string hybrid_term = std::string(kCatDog) + "ab";
  EXPECT_EQ(RunPath(Path::kRegular, hybrid_term), (std::vector<std::string>{"doc_hybrid_match"}));
  ExpectAllPathsAgree(hybrid_term);
}

TEST_F(SearchPathParityTest, VerifyTextPolicyChangesNothingBetweenPaths) {
  for (const char* policy : {"off", "ascii", "all"}) {
    SCOPED_TRACE(policy);
    config_.memory.verify_text = policy;
    ExpectAllPathsAgree("report");
    ExpectAllPathsAgree("alpha");
    ExpectAllPathsAgree("published");
    ExpectAllPathsAgree(kCatDog);
    ExpectAllPathsAgree(std::string(kCatDog) + "ab");
  }
}

TEST_F(SearchPathParityTest, SeveralNotTermsAreExcludedTheSameWayOnEveryPath) {
  auto exclude_two = [](query::Query& query) { query.not_terms = {"draft", "summary"}; };
  ExpectAllPathsAgree("report", exclude_two);
  ExpectAllPathsAgree("published", exclude_two);
}

// A NOT term that shares every n-gram of a document without occurring in it is
// the exclusion-side counterpart of the hybrid-fragment guard: excluding on
// n-gram evidence alone removes a document the term never appears in.
TEST_F(SearchPathParityTest, HybridNotTermExcludesTheSameDocumentsOnEveryPath) {
  auto exclude_hybrid = [](query::Query& query) { query.not_terms = {std::string(kCatDog) + "cd"}; };
  ExpectAllPathsAgree("report", exclude_hybrid);

  config_.memory.verify_text = "all";
  ExpectAllPathsAgree("report", exclude_hybrid);
}

// `T NOT U` and `T AND (NOT U)` are the same request written two ways: one
// reaches ApplyNotFilter through query.not_terms, the other reaches the AST
// evaluator's NOT node. They have to select the same documents.
TEST_F(SearchPathParityTest, ClauseNotAndAstNotSelectTheSameDocuments) {
  for (const char* policy : {"off", "all"}) {
    SCOPED_TRACE(policy);
    config_.memory.verify_text = policy;

    query::Query clause_form;
    clause_form.type = query::QueryType::SEARCH;
    clause_form.table = "test";
    clause_form.limit = 100;
    clause_form.search_text = "report";
    clause_form.not_terms = {"draft"};

    query::Query ast_form = clause_form;
    ast_form.not_terms.clear();
    ast_form.search_text = "report AND (NOT draft)";

    auto clause_output = ExecuteFullPipeline(clause_form, MakeParams(Path::kRegular));
    auto ast_output = ExecuteFullPipeline(ast_form, MakeParams(Path::kRegular));
    ASSERT_TRUE(clause_output.has_value()) << clause_output.error().message();
    ASSERT_TRUE(ast_output.has_value()) << ast_output.error().message();
    EXPECT_EQ(clause_output->results, ast_output->results);
    EXPECT_FALSE(clause_output->results.empty());
  }
}

// The plain path swaps between FilterByNgrams and SearchAnd at
// filter_threshold, and the other three ignore the setting entirely. Whichever
// branch it takes, the answer has to be the same one the others produce.
TEST_F(SearchPathParityTest, TheFilterThresholdBranchDoesNotChangeTheAnswer) {
  auto and_summary = [](query::Query& query) { query.and_terms = {"published"}; };

  filter_threshold_ = 1000;  // Small candidate set: FilterByNgrams branch.
  const auto below = RunPath(Path::kRegular, "alpha", and_summary);
  ExpectAllPathsAgree("alpha", and_summary);

  filter_threshold_ = 0;  // Forces the SearchAnd branch on the same input.
  const auto above = RunPath(Path::kRegular, "alpha", and_summary);
  EXPECT_EQ(below, above);
  ExpectAllPathsAgree("alpha", and_summary);
}

// A synonym dictionary that does not mention the query term must not change
// the answer either, which separates "the synonym executor ran" from "synonyms
// applied".
TEST_F(SearchPathParityTest, ADictionaryThatDoesNotMentionTheTermChangesNothing) {
  LoadUnrelatedSynonyms();
  ExpectAllPathsAgree("report", {}, &unrelated_synonyms_);
  ExpectAllPathsAgree(std::string(kCatDog) + "ab", {}, &unrelated_synonyms_);
}

// ---------------------------------------------------------------------------
// Where the paths are not equivalent, stated rather than asserted away
// ---------------------------------------------------------------------------

// A term shorter than the n-gram size generates no n-grams. The plain path
// falls back to a substring scan of the stored text; the fuzzy path has no such
// fallback and reports the term as empty. This is a genuine difference in what
// the two executors can answer, not a bug the parity tests above cover: the
// comparisons above deliberately use terms long enough to avoid it.
TEST_F(SearchPathParityTest, TermTooShortForNgramsIsNotAPathEquivalence) {
  const std::string short_term = "ab";  // Shorter than kNgramSize.

  auto regular = ExecuteFullPipeline(MakeQuery(Path::kRegular, short_term), MakeParams(Path::kRegular));
  auto fuzzy = ExecuteFullPipeline(MakeQuery(Path::kFuzzyZero, short_term), MakeParams(Path::kFuzzyZero));

  // Whatever each returns, they are reached by different code and the suite
  // does not claim they agree. Recording the current answers keeps a later
  // change to either one visible.
  ASSERT_TRUE(regular.has_value()) << regular.error().message();
  ASSERT_TRUE(fuzzy.has_value()) << fuzzy.error().message();
  EXPECT_FALSE(regular->empty_term_detected);
  EXPECT_TRUE(fuzzy->empty_term_detected);
  EXPECT_TRUE(fuzzy->results.empty());
}

}  // namespace mygramdb::server::search_pipeline
