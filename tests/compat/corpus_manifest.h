/**
 * @file corpus_manifest.h
 * @brief Shared description of the pinned persistence corpus
 *
 * Both the corpus generator and the corpus load test include this header, so
 * the bytes that get written and the content that gets asserted come from one
 * declaration. The byte layouts described here are the ones documented in
 * spec/persistence-formats.md; every offset comment below refers to that file.
 */

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "config/config.h"
#include "index/index.h"

namespace mygramdb::compat {

// ============================================================================
// Fixture file names
// ============================================================================

/// Standalone index payloads ("MGIX"), one per accepted format version.
inline constexpr std::string_view kIndexV1File = "index_v1.mgix";
inline constexpr std::string_view kIndexV2File = "index_v2.mgix";
inline constexpr std::string_view kIndexV3File = "index_v3.mgix";
inline constexpr std::string_view kIndexV4File = "index_v4.mgix";
/// Index payload carrying a version the reader must refuse.
inline constexpr std::string_view kIndexV5File = "index_v5_unsupported.mgix";

/// Standalone document store payloads ("MGDS"), one per accepted version.
inline constexpr std::string_view kDocStoreV1File = "docstore_v1.mgds";
inline constexpr std::string_view kDocStoreV2File = "docstore_v2.mgds";
inline constexpr std::string_view kDocStoreV3File = "docstore_v3.mgds";
/// Document store payload carrying a version the reader must refuse.
inline constexpr std::string_view kDocStoreV4File = "docstore_v4_unsupported.mgds";

/// Container V1 with compatibility metadata version 1, index V1, docstore V1.
inline constexpr std::string_view kDumpV1MetaV1File = "dump_v1_meta_v1.dmp";
/// Container V1 as WriteDumpV1 emits it: metadata version 2, index V4, docstore V3.
inline constexpr std::string_view kDumpV1MetaV2File = "dump_v1_meta_v2.dmp";
/// Container V2 carrying payload versions no writer emits any more: index V3, docstore V2.
inline constexpr std::string_view kDumpV2LegacyPayloadsFile = "dump_v2_legacy_payloads.dmp";
/// Container V2 as WriteDumpV2 emits it: metadata version 2, index V4, docstore V3.
inline constexpr std::string_view kDumpV2CurrentFile = "dump_v2_current.dmp";
/// Container carrying a version above the ceiling the readers accept.
inline constexpr std::string_view kDumpV3File = "dump_v3_unsupported.dmp";
/// Container V2 whose compatibility metadata section carries an unsupported version.
inline constexpr std::string_view kDumpMetaV3File = "dump_v2_meta_v3_unsupported.dmp";

/// Dumps written by the release trees named in the file names. Unlike every
/// other fixture these were produced by an earlier release's own writer, so
/// their content follows that release's defaults rather than the manifest.
inline constexpr std::string_view kReleaseV132File = "release_v1_3_2.dmp";
inline constexpr std::string_view kReleaseV154File = "release_v1_5_4.dmp";
inline constexpr std::string_view kReleaseV161File = "release_v1_6_1.dmp";
inline constexpr std::string_view kReleaseV190File = "release_v1_9_0.dmp";

// ============================================================================
// Tokenizer and normalization settings recorded by every fixture
// ============================================================================
//
// The index decoder refuses a payload whose recorded tokenizer settings
// disagree with the live Index it loads into, so a reader of this corpus has
// to construct its Index with exactly these values.

inline constexpr int kNgramSize = 2;
inline constexpr int kKanjiNgramSize = 2;
inline constexpr bool kCrossBoundaryNgrams = true;
inline constexpr bool kNormalizeNfkc = true;
inline constexpr std::string_view kNormalizeWidth = "narrow";
inline constexpr bool kNormalizeLower = false;
inline constexpr double kRoaringThreshold = config::defaults::kRoaringThreshold;

inline constexpr std::string_view kTableName = "articles";
inline constexpr std::string_view kGtid = "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-42";
inline constexpr std::string_view kSourceServerUuid = "3e11fa47-71ca-11e1-9e33-c80aa9429562";
inline constexpr std::string_view kMysqlHost = "127.0.0.1";
inline constexpr int kMysqlPort = 3306;
inline constexpr std::string_view kMysqlDatabase = "corpus";
inline constexpr std::string_view kVerifyText = "off";

// ============================================================================
// Index content
// ============================================================================

/// One term of the pinned index, with the document IDs in its posting list.
struct CorpusTerm {
  std::string_view term;
  std::vector<uint32_t> doc_ids;
};

/**
 * @brief Terms every index fixture encodes, in fixture byte order.
 *
 * The term table layout is identical in index versions 1 through 4, so all
 * four fixtures carry this same content and differ only in their header and
 * their CRC32 trailer.
 */
inline std::vector<CorpusTerm> CorpusTerms() {
  return {
      {"he", {1, 3}}, {"el", {1}}, {"ll", {1, 2}}, {"lo", {2, 3}}, {"日本", {2}},
  };
}

// ============================================================================
// Document store content
// ============================================================================

/// One document of the pinned document store, in fixture byte order.
struct CorpusDocument {
  uint32_t doc_id;
  std::string_view primary_key;
  /// Normalized text, recorded from document store version 2 onwards.
  std::string_view normalized_text;
  /// Original text, recorded from document store version 3 onwards.
  std::string_view original_text;
};

inline std::vector<CorpusDocument> CorpusDocuments() {
  return {
      {1, "alpha", "hello", "Hello"},
      {2, "beta", "hello日本", "Hello日本"},
      {3, "gamma", "helo", "HELO"},
  };
}

/// next_doc_id recorded by every document store fixture.
inline constexpr uint32_t kNextDocId = 4;

/// Filter recorded on document 1: an INT column.
inline constexpr std::string_view kIntFilterName = "status";
inline constexpr int32_t kIntFilterValue = 7;
/// Filter recorded on document 2: a VARCHAR column.
inline constexpr std::string_view kStringFilterName = "category";
inline constexpr std::string_view kStringFilterValue = "news";
/// Document 3 records no filters at all.

// ============================================================================
// Configuration recorded in every dump fixture's config section
// ============================================================================

/**
 * @brief The configuration a dump fixture serializes and a loader must match.
 *
 * FindDumpConfigMismatch compares memory.verify_text, the three normalization
 * settings and every table's n-gram settings, so a server loading this corpus
 * has to be running exactly this configuration.
 *
 * The table carries an explicit database because that check identifies a table
 * by its qualified `database.table` name, and DeserializeConfig fills an absent
 * database from mysql.database. A fixture that left it empty would resolve to no
 * table at all, and the n-gram comparisons would be skipped rather than made.
 */
inline config::Config CorpusConfig() {
  config::Config config;
  config.mysql.host = std::string(kMysqlHost);
  config.mysql.port = kMysqlPort;
  config.mysql.database = std::string(kMysqlDatabase);

  config.memory.verify_text = std::string(kVerifyText);
  config.memory.normalize.nfkc = kNormalizeNfkc;
  config.memory.normalize.width = std::string(kNormalizeWidth);
  config.memory.normalize.lower = kNormalizeLower;
  config.memory.roaring_threshold = kRoaringThreshold;

  config::TableConfig table;
  table.name = std::string(kTableName);
  table.database = std::string(kMysqlDatabase);
  table.primary_key = "id";
  table.text_source.column = "body";
  table.ngram_size = kNgramSize;
  table.kanji_ngram_size = kKanjiNgramSize;
  table.cross_boundary_ngrams = kCrossBoundaryNgrams;
  config.tables.push_back(std::move(table));

  return config;
}

/**
 * @brief Construct an Index configured the way every fixture records.
 *
 * A dump restore builds its staging Index from the live Index's settings, so
 * the object handed to the read path decides whether the recorded tokenizer
 * fields are accepted.
 */
inline std::unique_ptr<index::Index> MakeCorpusIndex() {
  return std::make_unique<index::Index>(kNgramSize, kKanjiNgramSize, kRoaringThreshold, kCrossBoundaryNgrams,
                                        kNormalizeNfkc, std::string(kNormalizeWidth), kNormalizeLower);
}

}  // namespace mygramdb::compat
