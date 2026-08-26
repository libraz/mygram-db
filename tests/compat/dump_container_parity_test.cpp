/**
 * @file dump_container_parity_test.cpp
 * @brief Write one in-memory state through both dump containers and compare
 *
 * `dump_format_v1.cpp` and `dump_format_v2.cpp` are independent write/read
 * pairs. Each has its own round-trip tests, which every implementation passes
 * on its own terms; neither notices when the two drift apart. This file derives
 * both artifacts from a single source state, restores both, and asserts the two
 * restorations agree — and, where spec/persistence-formats.md documents a field
 * only one container carries, asserts that difference rather than skipping it.
 *
 * The tokenizer settings, the table name and the document content come from
 * compat/corpus_manifest.h so the state written here is described in the same
 * place as the checked-in fixtures.
 */

#include <gtest/gtest.h>

#include <algorithm>
#include <filesystem>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "compat/corpus_manifest.h"
#include "config/config.h"
#include "index/index.h"
#include "storage/document_store.h"
#include "storage/dump_format.h"
#include "storage/dump_format_v1.h"
#include "storage/dump_format_v2.h"
#include "storage/dump_source_identity.h"
#include "utils/string_utils.h"

namespace {

using mygram::utils::ErrorCode;
using mygramdb::index::Index;
using mygramdb::storage::DocumentStore;
using mygramdb::storage::DumpSourceIdentity;
using mygramdb::storage::DumpStatistics;
using mygramdb::storage::FilterValue;
using mygramdb::storage::TableStatistics;

namespace corpus = mygramdb::compat;

using TableContexts = std::unordered_map<std::string, std::pair<Index*, DocumentStore*>>;

/// One table's index and document store, kept together so a restore target and
/// the source state can be built by the same code.
struct TableState {
  std::unique_ptr<Index> index;
  std::unique_ptr<DocumentStore> doc_store;

  TableContexts Contexts() {
    TableContexts contexts;
    contexts.emplace(std::string(corpus::kTableName), std::make_pair(index.get(), doc_store.get()));
    return contexts;
  }
};

TableState MakeEmptyState() {
  return TableState{corpus::MakeCorpusIndex(), std::make_unique<DocumentStore>()};
}

/**
 * @brief The one source state both containers are written from.
 *
 * Documents, filters and next_doc_id follow the corpus manifest; the index is
 * built by indexing each document's normalized text, so the term table is
 * whatever the live tokenizer produces for that content.
 */
TableState MakeSourceState() {
  TableState state = MakeEmptyState();

  for (const auto& document : corpus::CorpusDocuments()) {
    mygramdb::storage::FilterMap filters;
    if (document.doc_id == 1) {
      filters[std::string(corpus::kIntFilterName)] = corpus::kIntFilterValue;
    } else if (document.doc_id == 2) {
      filters[std::string(corpus::kStringFilterName)] = std::string(corpus::kStringFilterValue);
    }

    auto doc_id =
        state.doc_store->AddDocument(document.primary_key, filters, document.normalized_text, document.original_text);
    EXPECT_TRUE(doc_id.has_value());
    EXPECT_EQ(*doc_id, document.doc_id);
    state.index->AddDocument(*doc_id, std::string(document.normalized_text));
  }
  return state;
}

/// Every n-gram the source content can produce, so posting lists are compared
/// term by term rather than only by count.
std::vector<std::string> SourceNgrams() {
  std::vector<std::string> ngrams;
  for (const auto& document : corpus::CorpusDocuments()) {
    auto term_ngrams = mygram::utils::GenerateQueryNgrams(std::string(document.normalized_text), corpus::kNgramSize,
                                                          corpus::kKanjiNgramSize, corpus::kCrossBoundaryNgrams);
    ngrams.insert(ngrams.end(), term_ngrams.begin(), term_ngrams.end());
  }
  std::sort(ngrams.begin(), ngrams.end());
  ngrams.erase(std::unique(ngrams.begin(), ngrams.end()), ngrams.end());
  return ngrams;
}

/// Statistics both writers are handed, so the optional sections are exercised.
DumpStatistics SourceStatistics() {
  DumpStatistics stats;
  stats.total_documents = 3;
  stats.total_terms = 5;
  stats.total_index_bytes = 4096;
  stats.total_docstore_bytes = 2048;
  stats.dump_time_ms = 17;
  return stats;
}

std::unordered_map<std::string, TableStatistics> SourceTableStatistics() {
  TableStatistics table_stats;
  table_stats.document_count = 3;
  table_stats.term_count = 5;
  table_stats.index_bytes = 4096;
  table_stats.docstore_bytes = 2048;
  table_stats.next_doc_id = corpus::kNextDocId;
  table_stats.last_update_time = 1700000000;
  return {{std::string(corpus::kTableName), table_stats}};
}

/// Everything a restore produces, gathered so two restores can be diffed.
struct Restoration {
  std::string gtid;
  mygramdb::config::Config config;
  DumpSourceIdentity source_identity;
  DumpStatistics stats;
  std::unordered_map<std::string, TableStatistics> table_stats;
  TableState state = MakeEmptyState();
};

void ExpectSameIndexContent(const Index& lhs, const Index& rhs, const char* what) {
  EXPECT_EQ(lhs.TermCount(), rhs.TermCount()) << what;
  EXPECT_EQ(lhs.GetNgramSize(), rhs.GetNgramSize()) << what;
  for (const auto& ngram : SourceNgrams()) {
    EXPECT_EQ(lhs.Count(ngram), rhs.Count(ngram)) << what << ", term: " << ngram;
    EXPECT_EQ(lhs.SearchOr({ngram}), rhs.SearchOr({ngram})) << what << ", term: " << ngram;
  }
}

void ExpectSameDocStoreContent(const DocumentStore& lhs, const DocumentStore& rhs, const char* what) {
  ASSERT_EQ(lhs.Size(), rhs.Size()) << what;
  EXPECT_EQ(lhs.GetAllDocIds(), rhs.GetAllDocIds()) << what;

  for (const auto doc_id : lhs.GetAllDocIds()) {
    EXPECT_EQ(lhs.GetPrimaryKey(doc_id), rhs.GetPrimaryKey(doc_id)) << what << ", doc: " << doc_id;
    EXPECT_EQ(lhs.GetNormalizedText(doc_id), rhs.GetNormalizedText(doc_id)) << what << ", doc: " << doc_id;
    EXPECT_EQ(lhs.GetOriginalText(doc_id), rhs.GetOriginalText(doc_id)) << what << ", doc: " << doc_id;
    EXPECT_EQ(lhs.GetFilterValue(doc_id, corpus::kIntFilterName), rhs.GetFilterValue(doc_id, corpus::kIntFilterName))
        << what << ", doc: " << doc_id;
    EXPECT_EQ(lhs.GetFilterValue(doc_id, corpus::kStringFilterName),
              rhs.GetFilterValue(doc_id, corpus::kStringFilterName))
        << what << ", doc: " << doc_id;
  }

  const auto primary_keys = corpus::CorpusDocuments();
  for (const auto& document : primary_keys) {
    EXPECT_EQ(lhs.GetDocId(document.primary_key), rhs.GetDocId(document.primary_key)) << what;
  }

  // The filter index is rebuilt from the decoded values rather than serialized,
  // so it is compared through the lookup it exists to serve.
  EXPECT_EQ(lhs.FilterByValue(corpus::kIntFilterName, FilterValue(corpus::kIntFilterValue)),
            rhs.FilterByValue(corpus::kIntFilterName, FilterValue(corpus::kIntFilterValue)))
      << what;
  EXPECT_EQ(lhs.FilterByValue(corpus::kStringFilterName, FilterValue(std::string(corpus::kStringFilterValue))),
            rhs.FilterByValue(corpus::kStringFilterName, FilterValue(std::string(corpus::kStringFilterValue))))
      << what;
}

/// The configuration fields spec/persistence-formats.md section 5 says the
/// config section carries.
void ExpectSameConfig(const mygramdb::config::Config& lhs, const mygramdb::config::Config& rhs, const char* what) {
  EXPECT_EQ(lhs.mysql.host, rhs.mysql.host) << what;
  EXPECT_EQ(lhs.mysql.port, rhs.mysql.port) << what;
  EXPECT_EQ(lhs.mysql.database, rhs.mysql.database) << what;
  EXPECT_EQ(lhs.memory.verify_text, rhs.memory.verify_text) << what;
  EXPECT_EQ(lhs.memory.normalize.nfkc, rhs.memory.normalize.nfkc) << what;
  EXPECT_EQ(lhs.memory.normalize.width, rhs.memory.normalize.width) << what;
  EXPECT_EQ(lhs.memory.normalize.lower, rhs.memory.normalize.lower) << what;
  ASSERT_EQ(lhs.tables.size(), rhs.tables.size()) << what;
  for (size_t i = 0; i < lhs.tables.size(); ++i) {
    EXPECT_EQ(lhs.tables[i].name, rhs.tables[i].name) << what;
    EXPECT_EQ(lhs.tables[i].primary_key, rhs.tables[i].primary_key) << what;
    EXPECT_EQ(lhs.tables[i].ngram_size, rhs.tables[i].ngram_size) << what;
    EXPECT_EQ(lhs.tables[i].kanji_ngram_size, rhs.tables[i].kanji_ngram_size) << what;
    EXPECT_EQ(lhs.tables[i].cross_boundary_ngrams, rhs.tables[i].cross_boundary_ngrams) << what;
  }
}

void ExpectSameStatistics(const DumpStatistics& lhs, const DumpStatistics& rhs, const char* what) {
  EXPECT_EQ(lhs.total_documents, rhs.total_documents) << what;
  EXPECT_EQ(lhs.total_terms, rhs.total_terms) << what;
  EXPECT_EQ(lhs.total_index_bytes, rhs.total_index_bytes) << what;
  EXPECT_EQ(lhs.total_docstore_bytes, rhs.total_docstore_bytes) << what;
  EXPECT_EQ(lhs.dump_time_ms, rhs.dump_time_ms) << what;
}

void ExpectSameTableStatistics(const std::unordered_map<std::string, TableStatistics>& lhs,
                               const std::unordered_map<std::string, TableStatistics>& rhs, const char* what) {
  ASSERT_EQ(lhs.size(), rhs.size()) << what;
  for (const auto& [name, left] : lhs) {
    const auto right = rhs.find(name);
    ASSERT_NE(right, rhs.end()) << what << ", table: " << name;
    EXPECT_EQ(left.document_count, right->second.document_count) << what;
    EXPECT_EQ(left.term_count, right->second.term_count) << what;
    EXPECT_EQ(left.index_bytes, right->second.index_bytes) << what;
    EXPECT_EQ(left.docstore_bytes, right->second.docstore_bytes) << what;
    EXPECT_EQ(left.next_doc_id, right->second.next_doc_id) << what;
    EXPECT_EQ(left.last_update_time, right->second.last_update_time) << what;
  }
}

class DumpContainerParityTest : public ::testing::Test {
 protected:
  void SetUp() override {
    dir_ = std::filesystem::temp_directory_path() /
           ("mygramdb_dump_parity_" + std::to_string(::testing::UnitTest::GetInstance()->random_seed()) + "_" +
            std::string(::testing::UnitTest::GetInstance()->current_test_info()->name()));
    std::filesystem::remove_all(dir_);
    std::filesystem::create_directories(dir_);

    source_ = MakeSourceState();
    auto contexts = source_.Contexts();
    const auto config = corpus::CorpusConfig();
    const auto stats = SourceStatistics();
    const auto table_stats = SourceTableStatistics();

    v1_path_ = (dir_ / "state_v1.dmp").string();
    v2_path_ = (dir_ / "state_v2.dmp").string();

    ASSERT_TRUE(mygramdb::storage::dump_v1::WriteDumpV1(v1_path_, std::string(corpus::kGtid), config, contexts, &stats,
                                                        &table_stats)
                    .has_value());
    ASSERT_TRUE(mygramdb::storage::dump_v2::WriteDumpV2(v2_path_, std::string(corpus::kGtid), config, contexts, &stats,
                                                        &table_stats, {}, {}, corpus::kSourceServerUuid)
                    .has_value());
  }

  void TearDown() override { std::filesystem::remove_all(dir_); }

  /// Restore through the auto-detecting reader, which is the entry point both
  /// server load paths use.
  Restoration Restore(const std::string& path) {
    Restoration restored;
    auto contexts = restored.state.Contexts();
    auto result =
        mygramdb::storage::dump_v2::ReadDump(path, restored.gtid, restored.config, contexts, &restored.stats,
                                             &restored.table_stats, nullptr, {}, {}, &restored.source_identity);
    EXPECT_TRUE(result.has_value()) << path << ": " << (result.has_value() ? "" : result.error().message());
    return restored;
  }

  std::filesystem::path dir_;
  std::string v1_path_;
  std::string v2_path_;
  TableState source_ = MakeEmptyState();
};

// ---------------------------------------------------------------------------
// What both containers must reproduce identically
// ---------------------------------------------------------------------------

TEST_F(DumpContainerParityTest, BothContainersRestoreTheSameIndex) {
  Restoration from_v1 = Restore(v1_path_);
  Restoration from_v2 = Restore(v2_path_);

  ExpectSameIndexContent(*from_v1.state.index, *from_v2.state.index, "V1 against V2");
  // Anchored to the source, so a change that broke both writers the same way
  // would still be caught.
  ExpectSameIndexContent(*from_v1.state.index, *source_.index, "V1 against the source state");
  EXPECT_GT(source_.index->TermCount(), 0U);
}

TEST_F(DumpContainerParityTest, BothContainersRestoreTheSameDocumentStore) {
  Restoration from_v1 = Restore(v1_path_);
  Restoration from_v2 = Restore(v2_path_);

  ExpectSameDocStoreContent(*from_v1.state.doc_store, *from_v2.state.doc_store, "V1 against V2");
  ExpectSameDocStoreContent(*from_v1.state.doc_store, *source_.doc_store, "V1 against the source state");
}

// next_doc_id is not readable from outside the store, so it is compared through
// the identifier each restore hands the next document.
TEST_F(DumpContainerParityTest, BothContainersRestoreTheSameNextDocId) {
  Restoration from_v1 = Restore(v1_path_);
  Restoration from_v2 = Restore(v2_path_);

  auto v1_next = from_v1.state.doc_store->AddDocument("delta");
  auto v2_next = from_v2.state.doc_store->AddDocument("delta");
  ASSERT_TRUE(v1_next.has_value());
  ASSERT_TRUE(v2_next.has_value());
  EXPECT_EQ(*v1_next, *v2_next);
  EXPECT_EQ(*v1_next, corpus::kNextDocId);
}

TEST_F(DumpContainerParityTest, BothContainersRestoreTheSameGtid) {
  Restoration from_v1 = Restore(v1_path_);
  Restoration from_v2 = Restore(v2_path_);

  EXPECT_EQ(from_v1.gtid, from_v2.gtid);
  EXPECT_EQ(from_v1.gtid, corpus::kGtid);
}

TEST_F(DumpContainerParityTest, BothContainersRestoreTheSameConfiguration) {
  Restoration from_v1 = Restore(v1_path_);
  Restoration from_v2 = Restore(v2_path_);

  ExpectSameConfig(from_v1.config, from_v2.config, "V1 against V2");
  ExpectSameConfig(from_v1.config, corpus::CorpusConfig(), "V1 against the source configuration");
}

TEST_F(DumpContainerParityTest, BothContainersRestoreTheSameStatistics) {
  Restoration from_v1 = Restore(v1_path_);
  Restoration from_v2 = Restore(v2_path_);

  ExpectSameStatistics(from_v1.stats, from_v2.stats, "V1 against V2");
  ExpectSameStatistics(from_v1.stats, SourceStatistics(), "V1 against the source statistics");
  ExpectSameTableStatistics(from_v1.table_stats, from_v2.table_stats, "V1 against V2");
  ExpectSameTableStatistics(from_v1.table_stats, SourceTableStatistics(), "V1 against the source statistics");
}

// ---------------------------------------------------------------------------
// The documented differences, asserted rather than skipped
// ---------------------------------------------------------------------------

// spec/persistence-formats.md 1.4 and 4.3: the MySQL source server UUID has no
// field in a V1 container, so a V1 artifact's source is unknown whatever the
// state it was written from. WriteDumpV1 has no parameter for it either.
TEST_F(DumpContainerParityTest, OnlyTheV2ContainerCarriesTheMysqlSourceServerUuid) {
  Restoration from_v1 = Restore(v1_path_);
  Restoration from_v2 = Restore(v2_path_);

  EXPECT_FALSE(from_v1.source_identity.recorded);
  EXPECT_TRUE(from_v1.source_identity.uuid.empty());

  EXPECT_TRUE(from_v2.source_identity.recorded);
  EXPECT_EQ(from_v2.source_identity.uuid, corpus::kSourceServerUuid);
}

// spec/persistence-formats.md 1.2 and 1.3: V1 folds compatibility metadata into
// the config section behind a header flag and has no section framing; V2 gives
// every payload an envelope and records how many there are.
TEST_F(DumpContainerParityTest, SectionFramingIsReportedOnlyForTheV2Container) {
  mygramdb::storage::dump_v2::DumpV2Info v1_info;
  ASSERT_TRUE(mygramdb::storage::dump_v2::GetDumpInfo(v1_path_, v1_info).has_value());
  mygramdb::storage::dump_v2::DumpV2Info v2_info;
  ASSERT_TRUE(mygramdb::storage::dump_v2::GetDumpInfo(v2_path_, v2_info).has_value());

  EXPECT_EQ(v1_info.version, static_cast<uint32_t>(mygramdb::storage::dump_format::FormatVersion::V1));
  EXPECT_EQ(v2_info.version, static_cast<uint32_t>(mygramdb::storage::dump_format::FormatVersion::V2));

  // Both agree on what the artifact contains.
  EXPECT_EQ(v1_info.gtid, v2_info.gtid);
  EXPECT_EQ(v1_info.gtid, corpus::kGtid);
  EXPECT_EQ(v1_info.table_count, v2_info.table_count);
  EXPECT_EQ(v1_info.table_count, 1U);
  EXPECT_TRUE(v1_info.has_statistics);
  EXPECT_TRUE(v2_info.has_statistics);

  // They disagree on section framing, because only V2 has any.
  EXPECT_EQ(v1_info.section_count, 0U);
  EXPECT_TRUE(v1_info.section_types.empty());
  // kConfig, kCompatibilityMetadata, kStatistics and one kTableData.
  EXPECT_EQ(v2_info.section_count, 4U);
  EXPECT_EQ(v2_info.section_types, (std::vector<mygramdb::storage::dump_format::SectionType>{
                                       mygramdb::storage::dump_format::SectionType::kConfig,
                                       mygramdb::storage::dump_format::SectionType::kCompatibilityMetadata,
                                       mygramdb::storage::dump_format::SectionType::kStatistics,
                                       mygramdb::storage::dump_format::SectionType::kTableData}));
}

// spec/persistence-formats.md 1.2 and 1.3: the two writers set different flag
// sets, because kHasCompatibilityMetadata only means anything in V1.
TEST_F(DumpContainerParityTest, HeaderFlagsDifferBecauseOnlyV1GatesItsMetadataOnOne) {
  mygramdb::storage::dump_v2::DumpV2Info v1_info;
  ASSERT_TRUE(mygramdb::storage::dump_v2::GetDumpInfo(v1_path_, v1_info).has_value());
  mygramdb::storage::dump_v2::DumpV2Info v2_info;
  ASSERT_TRUE(mygramdb::storage::dump_v2::GetDumpInfo(v2_path_, v2_info).has_value());

  EXPECT_EQ(v1_info.flags, static_cast<uint32_t>(mygramdb::storage::dump_format::flags_v1::kWithCRC |
                                                 mygramdb::storage::dump_format::flags_v1::kWithStatistics |
                                                 mygramdb::storage::dump_format::flags_v1::kHasCompatibilityMetadata));
  EXPECT_EQ(v2_info.flags, static_cast<uint32_t>(mygramdb::storage::dump_format::flags_v2::kWithCRC |
                                                 mygramdb::storage::dump_format::flags_v2::kWithStatistics));
}

// spec/persistence-formats.md 4.1: a version-specific reader refuses the other
// container, so the shared restoration above only holds through the dispatcher.
TEST_F(DumpContainerParityTest, EachVersionSpecificReaderRefusesTheOtherContainer) {
  TableState from_v2_via_v1_reader = MakeEmptyState();
  auto v1_contexts = from_v2_via_v1_reader.Contexts();
  std::string gtid;
  mygramdb::config::Config config;
  auto v1_reader_on_v2 = mygramdb::storage::dump_v1::ReadDumpV1(v2_path_, gtid, config, v1_contexts);
  ASSERT_FALSE(v1_reader_on_v2.has_value());
  EXPECT_EQ(v1_reader_on_v2.error().code(), ErrorCode::kStorageVersionMismatch);

  TableState from_v1_via_v2_reader = MakeEmptyState();
  auto v2_contexts = from_v1_via_v2_reader.Contexts();
  auto v2_reader_on_v1 = mygramdb::storage::dump_v2::ReadDumpV2(v1_path_, gtid, config, v2_contexts);
  ASSERT_FALSE(v2_reader_on_v1.has_value());
  EXPECT_EQ(v2_reader_on_v1.error().code(), ErrorCode::kStorageDumpReadError);
  EXPECT_NE(v2_reader_on_v1.error().message().find("Not a V2 dump file"), std::string::npos);
}

// spec/persistence-formats.md 1.6: both containers detect a corrupted byte, so
// the extra per-section CRC in V2 does not come at the cost of the file-level
// one. The byte perturbed sits inside the body, past either header.
TEST_F(DumpContainerParityTest, BothContainersRefuseACorruptedBody) {
  for (const auto& path : {v1_path_, v2_path_}) {
    SCOPED_TRACE(path);
    const auto scratch = dir_ / "corrupted.dmp";
    std::filesystem::copy_file(path, scratch, std::filesystem::copy_options::overwrite_existing);
    {
      std::fstream file(scratch, std::ios::binary | std::ios::in | std::ios::out);
      ASSERT_TRUE(file);
      const auto size = std::filesystem::file_size(scratch);
      file.seekg(static_cast<std::streamoff>(size / 2));
      char byte = 0;
      file.read(&byte, 1);
      byte = static_cast<char>(byte ^ 0xFF);
      file.seekp(static_cast<std::streamoff>(size / 2));
      file.write(&byte, 1);
    }

    TableState target = MakeEmptyState();
    auto contexts = target.Contexts();
    std::string gtid;
    mygramdb::config::Config config;
    auto result = mygramdb::storage::dump_v2::ReadDump(scratch.string(), gtid, config, contexts);
    ASSERT_FALSE(result.has_value()) << "a flipped body byte was accepted";
    EXPECT_EQ(result.error().code(), ErrorCode::kStorageDumpReadError);
    // The refusal happens before any live state is replaced, in both containers.
    EXPECT_EQ(target.index->TermCount(), 0U);
    EXPECT_EQ(target.doc_store->Size(), 0U);
    std::filesystem::remove(scratch);
  }
}

}  // namespace
