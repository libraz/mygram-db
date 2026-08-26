/**
 * @file generate_corpus.cpp
 * @brief Emit the pinned persistence corpus from an explicit byte layout
 *
 * Every fixture is assembled here from the layout documented in
 * spec/persistence-formats.md, byte by byte, so that a maintainer adding a new
 * format version can add a fixture by copying the nearest builder below and
 * adjusting the header. Two fixtures are instead produced by the real writers,
 * because the current code can still emit those versions.
 *
 * The output of this program is checked into tests/compat/corpus/ and is what
 * the load test reads. Running this program overwrites the corpus and is a
 * deliberate act; see tests/compat/README.md.
 *
 * Usage: generate_compat_corpus <output-directory>
 */

#include <array>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "compat/corpus_manifest.h"
#include "index/index.h"
#include "storage/document_store.h"
#include "storage/dump_format.h"
#include "storage/dump_format_v1.h"
#include "storage/dump_format_v2.h"
#include "utils/crc32.h"

namespace {

using mygramdb::compat::CorpusDocuments;
using mygramdb::compat::CorpusTerms;

// ============================================================================
// Little-endian primitives
//
// Every multi-byte integer in every persistence format is little-endian, so
// these append explicitly rather than memcpy-ing host representations.
// ============================================================================

void AppendU8(std::string& out, uint8_t value) {
  out.push_back(static_cast<char>(value));
}

void AppendU32(std::string& out, uint32_t value) {
  for (int shift = 0; shift < 32; shift += 8) {
    out.push_back(static_cast<char>((value >> shift) & 0xFFU));
  }
}

void AppendU64(std::string& out, uint64_t value) {
  for (int shift = 0; shift < 64; shift += 8) {
    out.push_back(static_cast<char>((value >> shift) & 0xFFU));
  }
}

void AppendBytes(std::string& out, std::string_view bytes) {
  out.append(bytes.data(), bytes.size());
}

/// Length-prefixed string: little-endian uint32 length, then the bytes, no NUL.
void AppendString(std::string& out, std::string_view value) {
  AppendU32(out, static_cast<uint32_t>(value.size()));
  AppendBytes(out, value);
}

/// Overwrite a little-endian uint32 already present in the buffer.
void PatchU32(std::string& buffer, size_t offset, uint32_t value) {
  for (int i = 0; i < 4; ++i) {
    buffer[offset + static_cast<size_t>(i)] = static_cast<char>((value >> (8 * i)) & 0xFFU);
  }
}

/// Overwrite a little-endian uint64 already present in the buffer.
void PatchU64(std::string& buffer, size_t offset, uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    buffer[offset + static_cast<size_t>(i)] = static_cast<char>((value >> (8 * i)) & 0xFFU);
  }
}

uint32_t Crc32(std::string_view data) {
  return mygramdb::utils::ComputeCRC32(data.data(), data.size());
}

// ============================================================================
// Index payload ("MGIX")
// ============================================================================

/**
 * @brief Encode one posting list.
 *
 * Posting list bytes are not versioned by the MGIX header: strategy byte,
 * little-endian uint32 entry count, then the entries. The fixed-width delta
 * strategy stores the first document ID absolutely and every following entry
 * as the gap from its predecessor.
 */
std::string EncodePostingList(const std::vector<uint32_t>& doc_ids) {
  constexpr uint8_t kFixedWidthDelta = 0;
  std::string out;
  AppendU8(out, kFixedWidthDelta);
  AppendU32(out, static_cast<uint32_t>(doc_ids.size()));
  uint32_t previous = 0;
  for (size_t i = 0; i < doc_ids.size(); ++i) {
    AppendU32(out, i == 0 ? doc_ids[i] : doc_ids[i] - previous);
    previous = doc_ids[i];
  }
  return out;
}

/// term_count, then per term: uint32 length, bytes, uint64 posting size, posting bytes.
std::string BuildIndexTermTable() {
  const auto terms = CorpusTerms();
  std::string out;
  AppendU64(out, static_cast<uint64_t>(terms.size()));
  for (const auto& term : terms) {
    AppendU32(out, static_cast<uint32_t>(term.term.size()));
    AppendBytes(out, term.term);
    const std::string postings = EncodePostingList(term.doc_ids);
    AppendU64(out, static_cast<uint64_t>(postings.size()));
    AppendBytes(out, postings);
  }
  return out;
}

/**
 * @brief Build an index payload of the requested format version.
 *
 * Shared prefix: magic "MGIX", uint32 version, uint32 ngram_size.
 * V3 and V4 insert uint32 kanji_ngram_size and a cross_boundary_ngrams byte.
 * V4 further inserts normalize_nfkc, a length-prefixed normalize_width and
 * normalize_lower. V1 has no trailer; V2, V3 and V4 append a CRC32 over
 * everything that precedes it.
 */
std::string BuildIndexPayload(uint32_t version) {
  namespace corpus = mygramdb::compat;
  std::string out;
  AppendBytes(out, "MGIX");
  AppendU32(out, version);
  AppendU32(out, static_cast<uint32_t>(corpus::kNgramSize));

  if (version >= 3) {
    AppendU32(out, static_cast<uint32_t>(corpus::kKanjiNgramSize));
    AppendU8(out, corpus::kCrossBoundaryNgrams ? 1 : 0);
  }
  if (version >= 4) {
    AppendU8(out, corpus::kNormalizeNfkc ? 1 : 0);
    AppendString(out, corpus::kNormalizeWidth);
    AppendU8(out, corpus::kNormalizeLower ? 1 : 0);
  }

  AppendBytes(out, BuildIndexTermTable());

  if (version >= 2) {
    AppendU32(out, Crc32(out));
  }
  return out;
}

// ============================================================================
// Document store payload ("MGDS")
// ============================================================================

/**
 * @brief Build a document store payload of the requested format version.
 *
 * Header: magic "MGDS", uint32 version, uint32 next_doc_id, length-prefixed
 * GTID, uint64 document count. Each document then records its uint32 ID, a
 * length-prefixed primary key and its filters. V2 appends the normalized text
 * per document, V3 additionally the original text.
 */
std::string BuildDocStorePayload(uint32_t version, std::string_view gtid) {
  namespace corpus = mygramdb::compat;
  constexpr uint8_t kTypeIndexInt32 = 6;
  constexpr uint8_t kTypeIndexString = 11;

  const auto documents = CorpusDocuments();
  std::string out;
  AppendBytes(out, "MGDS");
  AppendU32(out, version);
  AppendU32(out, corpus::kNextDocId);
  AppendString(out, gtid);
  AppendU64(out, static_cast<uint64_t>(documents.size()));

  for (const auto& document : documents) {
    AppendU32(out, document.doc_id);
    AppendString(out, document.primary_key);

    if (document.doc_id == 1) {
      // One INT filter: name, variant index, then the raw little-endian value.
      AppendU32(out, 1);
      AppendString(out, corpus::kIntFilterName);
      AppendU8(out, kTypeIndexInt32);
      AppendU32(out, static_cast<uint32_t>(corpus::kIntFilterValue));
    } else if (document.doc_id == 2) {
      // One VARCHAR filter: name, variant index, then a length-prefixed value.
      AppendU32(out, 1);
      AppendString(out, corpus::kStringFilterName);
      AppendU8(out, kTypeIndexString);
      AppendString(out, corpus::kStringFilterValue);
    } else {
      AppendU32(out, 0);
    }

    if (version >= 2) {
      AppendString(out, document.normalized_text);
    }
    if (version >= 3) {
      AppendString(out, document.original_text);
    }
  }
  return out;
}

// ============================================================================
// Compatibility metadata
// ============================================================================

/**
 * @brief Build a compatibility metadata payload.
 *
 * Version 1 records only memory.verify_text. Version 2 appends the MySQL
 * source server UUID.
 */
std::string BuildCompatibilityMetadata(uint32_t version) {
  namespace corpus = mygramdb::compat;
  std::string out;
  AppendU32(out, version);
  AppendString(out, corpus::kVerifyText);
  if (version >= 2) {
    AppendString(out, corpus::kSourceServerUuid);
  }
  return out;
}

/// Serialize the corpus configuration through the real config encoder.
std::string BuildConfigSection() {
  std::ostringstream stream;
  const auto config = mygramdb::compat::CorpusConfig();
  auto result = mygramdb::storage::dump_v1::SerializeConfig(stream, config);
  if (!result) {
    throw std::runtime_error("SerializeConfig failed: " + result.error().message());
  }
  return stream.str();
}

// ============================================================================
// Dump containers
// ============================================================================

/// One table entry: name, table statistics, index payload, docstore payload.
std::string BuildTableEntry(std::string_view index_payload, std::string_view docstore_payload) {
  std::string out;
  AppendString(out, mygramdb::compat::kTableName);
  AppendU32(out, 0);  // table_stats_len: no per-table statistics recorded
  AppendU64(out, static_cast<uint64_t>(index_payload.size()));
  AppendBytes(out, index_payload);
  AppendU64(out, static_cast<uint64_t>(docstore_payload.size()));
  AppendBytes(out, docstore_payload);
  return out;
}

/// V2 section envelope: uint32 type, uint32 CRC32 of the data, uint64 length.
void AppendSection(std::string& out, uint32_t section_type, std::string_view data) {
  AppendU32(out, section_type);
  AppendU32(out, Crc32(data));
  AppendU64(out, static_cast<uint64_t>(data.size()));
  AppendBytes(out, data);
}

/// Field offsets shared by both container versions.
constexpr size_t kTotalFileSizeOffset = 24;
constexpr size_t kFileCrc32Offset = 32;

/**
 * @brief Patch total_file_size and the file-level CRC32 into a finished dump.
 *
 * The file CRC covers the whole file with the four bytes of the CRC field
 * itself replaced by zeros, seeded at zero.
 */
void FinalizeDump(std::string& dump) {
  PatchU64(dump, kTotalFileSizeOffset, static_cast<uint64_t>(dump.size()));
  PatchU32(dump, kFileCrc32Offset, 0);
  PatchU32(dump, kFileCrc32Offset, Crc32(dump));
}

/**
 * @brief Container V1: a flat, implicitly ordered sequence of sections.
 *
 * The compatibility metadata is appended to the tail of the config section and
 * is only decoded when the header sets kHasCompatibilityMetadata.
 */
std::string BuildDumpV1(uint32_t compatibility_metadata_version, std::string_view index_payload,
                        std::string_view docstore_payload) {
  namespace corpus = mygramdb::compat;
  constexpr uint32_t kWithCrc = 0x10;
  constexpr uint32_t kHasCompatibilityMetadata = 0x20;
  constexpr uint64_t kFixedTimestamp = 1'700'000'000;

  const std::string config_section = BuildConfigSection() + BuildCompatibilityMetadata(compatibility_metadata_version);

  std::string out;
  AppendBytes(out, "MGDB");
  AppendU32(out, 1);                                                 // container version
  AppendU32(out, static_cast<uint32_t>(32 + corpus::kGtid.size()));  // header_size
  AppendU32(out, kWithCrc | kHasCompatibilityMetadata);              // flags
  AppendU64(out, kFixedTimestamp);                                   // dump_timestamp
  AppendU64(out, 0);                                                 // total_file_size, patched below
  AppendU32(out, 0);                                                 // file_crc32, patched below
  AppendString(out, corpus::kGtid);

  AppendU32(out, static_cast<uint32_t>(config_section.size()));
  AppendBytes(out, config_section);
  AppendU32(out, 0);  // stats_len: no dump statistics recorded
  AppendU32(out, 1);  // table_count
  AppendBytes(out, BuildTableEntry(index_payload, docstore_payload));

  FinalizeDump(out);
  return out;
}

/**
 * @brief Container V2: a flat run of self-describing section envelopes.
 *
 * Section order matters to the reader: the compatibility metadata section must
 * follow the config section.
 */
std::string BuildDumpV2(uint32_t compatibility_metadata_version, std::string_view index_payload,
                        std::string_view docstore_payload) {
  namespace corpus = mygramdb::compat;
  constexpr uint32_t kWithCrc = 0x10;
  constexpr uint32_t kSectionConfig = 1;
  constexpr uint32_t kSectionTableData = 3;
  constexpr uint32_t kSectionCompatibilityMetadata = 8;
  constexpr uint64_t kFixedTimestamp = 1'700'000'000;
  constexpr uint32_t kSectionCount = 3;

  std::string out;
  AppendBytes(out, "MGDB");
  AppendU32(out, 2);                                                 // container version
  AppendU32(out, static_cast<uint32_t>(36 + corpus::kGtid.size()));  // header_size
  AppendU32(out, kWithCrc);                                          // flags
  AppendU64(out, kFixedTimestamp);                                   // dump_timestamp
  AppendU64(out, 0);                                                 // total_file_size, patched below
  AppendU32(out, 0);                                                 // file_crc32, patched below
  AppendU32(out, kSectionCount);
  AppendString(out, corpus::kGtid);

  AppendSection(out, kSectionConfig, BuildConfigSection());
  AppendSection(out, kSectionCompatibilityMetadata, BuildCompatibilityMetadata(compatibility_metadata_version));
  AppendSection(out, kSectionTableData, BuildTableEntry(index_payload, docstore_payload));

  FinalizeDump(out);
  return out;
}

/// Container carrying a version above the ceiling both readers accept.
std::string BuildUnsupportedContainer() {
  std::string out;
  AppendBytes(out, "MGDB");
  AppendU32(out, 3);
  AppendU32(out, 40);
  AppendU32(out, 0x10);
  AppendU64(out, 1'700'000'000);
  AppendU64(out, 0);
  AppendU32(out, 0);
  AppendU32(out, 0);
  AppendString(out, "");
  FinalizeDump(out);
  return out;
}

// ============================================================================
// Output
// ============================================================================

void WriteFixture(const std::filesystem::path& directory, std::string_view name, std::string_view bytes) {
  const std::filesystem::path path = directory / std::string(name);
  std::ofstream stream(path, std::ios::binary | std::ios::trunc);
  if (!stream) {
    throw std::runtime_error("cannot open fixture for writing: " + path.string());
  }
  stream.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
  stream.close();
  std::cout << name << "  " << bytes.size() << " bytes\n";
}

/**
 * @brief Load the hand-assembled payloads into live objects.
 *
 * The two fixtures written by the real dump writers need an Index and a
 * DocumentStore holding exactly the corpus content. Loading the hand-assembled
 * current-version payloads is how they get it, which also proves at generation
 * time that the hand-assembled bytes are readable.
 */
struct LiveTable {
  std::unique_ptr<mygramdb::index::Index> index;
  std::unique_ptr<mygramdb::storage::DocumentStore> doc_store;
};

LiveTable LoadLiveTable(std::string_view index_v4, std::string_view docstore_v3) {
  LiveTable table;
  table.index = mygramdb::compat::MakeCorpusIndex();
  std::istringstream index_stream{std::string(index_v4)};
  if (auto result = table.index->LoadFromStream(index_stream); !result) {
    throw std::runtime_error("index V4 fixture did not load: " + result.error().message());
  }

  table.doc_store = std::make_unique<mygramdb::storage::DocumentStore>();
  std::istringstream doc_stream{std::string(docstore_v3)};
  if (auto result = table.doc_store->LoadFromStream(doc_stream); !result) {
    throw std::runtime_error("docstore V3 fixture did not load: " + result.error().message());
  }
  return table;
}

}  // namespace

int main(int argc, char** argv) {
  if (argc != 2) {
    std::cerr << "usage: generate_compat_corpus <output-directory>\n";
    return 2;
  }

  try {
    const std::filesystem::path directory(argv[1]);
    std::filesystem::create_directories(directory);

    namespace corpus = mygramdb::compat;

    // Standalone index payloads, one per accepted version plus one the reader
    // must refuse.
    const std::string index_v1 = BuildIndexPayload(1);
    const std::string index_v2 = BuildIndexPayload(2);
    const std::string index_v3 = BuildIndexPayload(3);
    const std::string index_v4 = BuildIndexPayload(4);
    WriteFixture(directory, corpus::kIndexV1File, index_v1);
    WriteFixture(directory, corpus::kIndexV2File, index_v2);
    WriteFixture(directory, corpus::kIndexV3File, index_v3);
    WriteFixture(directory, corpus::kIndexV4File, index_v4);
    WriteFixture(directory, corpus::kIndexV5File, BuildIndexPayload(5));

    // Standalone document store payloads. The standalone fixtures record the
    // corpus GTID; the payloads embedded in a dump record none, matching what
    // the dump writers emit.
    WriteFixture(directory, corpus::kDocStoreV1File, BuildDocStorePayload(1, corpus::kGtid));
    WriteFixture(directory, corpus::kDocStoreV2File, BuildDocStorePayload(2, corpus::kGtid));
    WriteFixture(directory, corpus::kDocStoreV3File, BuildDocStorePayload(3, corpus::kGtid));
    WriteFixture(directory, corpus::kDocStoreV4File, BuildDocStorePayload(4, corpus::kGtid));

    const std::string embedded_docstore_v1 = BuildDocStorePayload(1, "");
    const std::string embedded_docstore_v2 = BuildDocStorePayload(2, "");
    const std::string embedded_docstore_v3 = BuildDocStorePayload(3, "");

    // Container V1 with the compatibility metadata version nothing writes any
    // more, carrying the oldest embedded payload versions.
    WriteFixture(directory, corpus::kDumpV1MetaV1File, BuildDumpV1(1, index_v1, embedded_docstore_v1));

    // Container V2 carrying embedded payload versions no writer emits.
    WriteFixture(directory, corpus::kDumpV2LegacyPayloadsFile, BuildDumpV2(2, index_v3, embedded_docstore_v2));

    // Container V2 whose compatibility metadata version is above the ceiling.
    WriteFixture(directory, corpus::kDumpMetaV3File, BuildDumpV2(3, index_v4, embedded_docstore_v3));

    WriteFixture(directory, corpus::kDumpV3File, BuildUnsupportedContainer());

    // The two containers the current code can still write go through the real
    // writers rather than the builders above.
    LiveTable table = LoadLiveTable(index_v4, embedded_docstore_v3);
    std::unordered_map<std::string, std::pair<mygramdb::index::Index*, mygramdb::storage::DocumentStore*>> contexts;
    contexts.emplace(std::string(corpus::kTableName), std::make_pair(table.index.get(), table.doc_store.get()));
    const auto config = corpus::CorpusConfig();

    const std::filesystem::path v1_path = directory / std::string(corpus::kDumpV1MetaV2File);
    if (auto result =
            mygramdb::storage::dump_v1::WriteDumpV1(v1_path.string(), std::string(corpus::kGtid), config, contexts);
        !result) {
      throw std::runtime_error("WriteDumpV1 failed: " + result.error().message());
    }
    std::cout << corpus::kDumpV1MetaV2File << "  written by WriteDumpV1\n";

    const std::filesystem::path v2_path = directory / std::string(corpus::kDumpV2CurrentFile);
    if (auto result =
            mygramdb::storage::dump_v2::WriteDumpV2(v2_path.string(), std::string(corpus::kGtid), config, contexts,
                                                    nullptr, nullptr, {}, {}, corpus::kSourceServerUuid);
        !result) {
      throw std::runtime_error("WriteDumpV2 failed: " + result.error().message());
    }
    std::cout << corpus::kDumpV2CurrentFile << "  written by WriteDumpV2\n";

    return 0;
  } catch (const std::exception& error) {
    std::cerr << "corpus generation failed: " << error.what() << "\n";
    return 1;
  }
}
