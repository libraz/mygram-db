# Persistence Formats

This file is normative. It describes the on-disk artifacts MygramDB writes and the exact set of format versions the current code accepts when reading them. The version-acceptance policy in this document is a hard compatibility contract: narrowing it — refusing a version that is accepted today, or adding a load-time check that rejects an artifact an earlier release wrote — is an externally visible surface change, not an internal refactor.

A dump file is a nested artifact. The outer container (`.dmp`) carries its own version; inside it, every table contributes an independently versioned index payload and an independently versioned document-store payload. The three version numbers advance independently and are documented separately below.

## 1. Dump (snapshot) container format

### 1.1 Fixed file header

Every dump, regardless of container version, begins with the same 8 bytes (`src/storage/dump_format.h`).

| File offset | Width | Field | Meaning |
|---|---|---|---|
| 0 | 4 | `magic` | ASCII `MGDB`, byte-for-byte, not endian-converted (`src/storage/dump_format_v2.cpp`) |
| 4 | 4 | `version` | Container format version, little-endian `uint32` (`src/storage/dump_format_v2.cpp`) |

All multi-byte integers in every dump section are written little-endian and converted on both ends (`src/utils/binary_io.h`, `src/utils/endian_utils.h`). Strings are length-prefixed with a little-endian `uint32` and are not NUL-terminated (`src/storage/dump_format_v1_internal.h`). Reads of length-prefixed strings are bounded per field type: identifiers 1 KiB, config values 4 KiB, paths 8 KiB, text content 16 MiB, general 1 MiB (`src/storage/dump_format_v1.h`). GTIDs are bounded at 64 KiB in both container versions, and both writers reject a longer one before emitting any byte (`src/storage/dump_format_v1.h`, `src/storage/dump_format_v1.cpp`, `src/storage/dump_format_v2.cpp`).

Container versions that exist: `V1 = 1` and `V2 = 2` (`src/storage/dump_format.h`).

### 1.2 Container V1 layout

`WriteDumpV1` emits, after the fixed header (`src/storage/dump_format_v1.cpp`):

| File offset | Width | Field | Meaning |
|---|---|---|---|
| 8 | 4 | `header_size` | `32 + gtid.size()`. A literal `0` is accepted as well and means the writing release never recorded the field (`src/storage/dump_format_v1.h`) |
| 12 | 4 | `flags` | See below |
| 16 | 8 | `dump_timestamp` | Unix seconds at write time |
| 24 | 8 | `total_file_size` | Patched in place after the body is written (offset constant `src/storage/dump_format_v1.h`) |
| 32 | 4 | `file_crc32` | Patched in place last (offset constant `src/storage/dump_format_v1.h`) |
| 36 | 4 | `gtid_length` | |
| 40 | N | `gtid` | UTF-8 replication position |

Every citation in this table is `src/storage/dump_format_v1.cpp` unless the cell names another file.

The header struct's own doc comment (`src/storage/dump_format_v1.h`) tabulates the same fields relative to the start of the V1 header; add 8 to convert those to file offsets.

The body follows immediately:

```
config_len      : uint32          # SerializeConfig + SerializeCompatibilityMetadata, concatenated
config_data     : config_len bytes
stats_len       : uint32          # 0 when no DumpStatistics was supplied
stats_data      : stats_len bytes
table_count     : uint32
  per table:
    table_name        : uint32 length + bytes
    table_stats_len   : uint32          # 0 when absent
    table_stats_data  : table_stats_len bytes
    index_len         : uint64
    index_data        : index_len bytes   # section 2
    docstore_len      : uint64
    docstore_data     : docstore_len bytes # section 3
```

The statistics length field is always present even with no statistics (`src/storage/dump_format_v1.cpp`). Compatibility metadata is appended *inside* the config section rather than getting its own section (`src/storage/dump_format_v1.cpp`), and the reader only decodes it when `flags_v1::kHasCompatibilityMetadata` is set.

V1 flags (`src/storage/dump_format.h`): `kCompressed = 0x1`, `kEncrypted = 0x2`, `kIncremental = 0x4`, `kWithStatistics = 0x8`, `kWithCRC = 0x10`, `kHasCompatibilityMetadata = 0x20`. `WriteDumpV1` sets `kWithCRC | kHasCompatibilityMetadata`, plus `kWithStatistics` when statistics are supplied (`src/storage/dump_format_v1.cpp`). The first three are declared but never set and never read anywhere in the tree.

### 1.3 Container V2 layout

`WriteDumpV2` emits, after the fixed header (`src/storage/dump_format_v2.cpp`):

| File offset | Width | Field | Meaning |
|---|---|---|---|
| 8 | 4 | `header_size` | `36 + gtid.size()` |
| 12 | 4 | `flags` | See below |
| 16 | 8 | `dump_timestamp` | Unix seconds at write time |
| 24 | 8 | `total_file_size` | Patched in place (offset constant `src/storage/dump_format_v2.h`) |
| 32 | 4 | `file_crc32` | Patched in place last (offset constant `src/storage/dump_format_v2.h`) |
| 36 | 4 | `section_count` | Patched in place (offset constant `src/storage/dump_format_v2.h`) |
| 40 | 4 | `gtid_length` | |
| 44 | N | `gtid` | UTF-8 replication position |

Every citation in this table is `src/storage/dump_format_v2.cpp` unless the cell names another file.

The body is a flat run of exactly `section_count` sections. Each section is a 16-byte envelope followed by its data (`src/storage/dump_format.h`, written at `src/storage/dump_format_v2.cpp`):

| Envelope offset | Width | Field |
|---|---|---|
| 0 | 4 | `type` (`SectionType`, little-endian `uint32`) |
| 4 | 4 | `crc32` of the section data only |
| 8 | 8 | `data_length` of the section data |

Section types (`src/storage/dump_format.h`): `kConfig = 1`, `kStatistics = 2`, `kTableData = 3`, `kTableBM25 = 5`, `kTableSynonyms = 7`, `kCompatibilityMetadata = 8`.

`WriteDumpV2` emits, in this order: one `kConfig` section, one `kCompatibilityMetadata` section, an optional `kStatistics` section, then one `kTableData` section per table (`src/storage/dump_format_v2.cpp`). `kTableBM25` and `kTableSynonyms` are never written.

A `kTableData` section's payload has the same shape as one V1 table entry — name, table-stats length + data, `uint64` index length + data, `uint64` docstore length + data (`src/storage/dump_format_v2.cpp`). There is no `table_count` field in V2; the table count is the number of `kTableData` sections.

V2 flags (`src/storage/dump_format.h`): `kWithStatistics = 0x8`, `kWithCRC = 0x10`, `kHasBM25Data = 0x20`, `kHasSynonymData = 0x80`. `WriteDumpV2` sets `kWithCRC`, plus `kWithStatistics` when statistics are supplied (`src/storage/dump_format_v2.cpp`). `kHasBM25Data` and `kHasSynonymData` are never set and never read.

On POSIX, table sections are streamed straight to the file descriptor and their envelope CRC and length are patched back afterwards (`src/storage/dump_format_v2.cpp`); on Windows the section is buffered in memory first. The resulting bytes are identical.

### 1.4 What changed between V1 and V2

- V1 is a flat sequence of implicitly ordered sections; V2 wraps each section in a self-describing envelope, so a reader that meets an unrecognized `SectionType` skips `data_length` bytes and continues (`src/storage/dump_format_v2.cpp`).
- V1 has one file-level CRC32 and nothing else; V2 adds a per-section CRC32 in every envelope, verified as the section is decoded (`src/storage/dump_format_v2.cpp`).
- V1 folds compatibility metadata into the tail of the config section behind a header flag; V2 gives it a dedicated `kCompatibilityMetadata` section (`src/storage/dump_format_v2.cpp`).
- V2's compatibility metadata carries the MySQL source server UUID; V1's, as written, carries an empty UUID and the V1 reader never reports it, so a V1 dump's source is unknown (see §4.3).
- V2 adds `section_count` to the header, and rejects `section_count == 0` (`src/storage/dump_format_v2.cpp`).

### 1.5 Compression

**No dump section is compressed, in either container version.** There is no compression framing to document. The `flags_v1::kCompressed` bit (`src/storage/dump_format.h`) is declared and never set or tested by any code path. lz4 is linked into the build, but only for query-result caching (`src/cache/result_compressor.cpp`, `src/cache/query_cache.cpp`); no storage or index translation unit references it.

### 1.6 Integrity

Checksums are CRC32 as implemented by zlib, polynomial `0xEDB88320` (`src/utils/crc32.h`, `src/storage/dump_format_v2.cpp`).

**File-level CRC32.** Covers the entire file from byte 0 to `total_file_size`, with the 4 bytes of the `file_crc32` field itself substituted with zeros (`src/storage/dump_format_internal.cpp`, and the `pread`-based variant `src/storage/dump_format_internal.cpp`). The seed is 0.

Before the CRC is checked, both readers compare the actual file size against `total_file_size` and refuse a mismatch — this is the truncation and append detector (`src/storage/dump_format_v1.cpp`, `src/storage/dump_format_v2.cpp`). On CRC mismatch the load fails with `kStorageDumpReadError` (5011) and sets `IntegrityError::type = FileCRC` (`src/storage/dump_format_v1.cpp`, `src/storage/dump_format_v2.cpp`). No partial state is applied: table state is staged into fresh objects and swapped in only after every section has decoded and the caller's validator has passed (`src/storage/dump_format_internal.cpp`, `src/storage/dump_load_access.h`).

The V1 reader verifies the file CRC unconditionally (`src/storage/dump_format_v1.cpp`). The V2 reader verifies it only when `flags_v2::kWithCRC` is set in the header (`src/storage/dump_format_v2.cpp`); a checksum value of zero is a legitimate checksum and is compared normally, not treated as "absent".

**Section-level CRC32 (V2 only).** Each envelope's `crc32` covers exactly the section's `data_length` bytes. The reader hashes the bytes while the decoder consumes them and compares after the section is fully read (`src/storage/dump_format_v2.cpp`), failing with `kStorageDumpReadError` and `IntegrityError::type = SectionCRC`.

**Structural checks applied on load.** V2 rejects: a duplicate `kConfig`, `kCompatibilityMetadata` or `kStatistics` section (`src/storage/dump_format_v2.cpp`); a `kCompatibilityMetadata` section that precedes `kConfig`; a duplicate table section; a section whose declared length runs past end of file; an envelope `data_length` above 4 GiB (`src/storage/dump_format_v2.h`); a decoder that leaves bytes unconsumed inside its bounded payload; a section count that disagrees with the header; and a missing `kConfig` section. Both versions reject a dump whose table set does not exactly match the configured tables (`src/storage/dump_format_internal.cpp`), and a zero-length index or docstore payload (`src/storage/dump_format_v1.cpp`, `src/storage/dump_format_v2.cpp`).

Both readers additionally enforce `RestoreLimits` — a memory budget and a per-section byte ceiling, defaulting to 4 GiB and 2 GiB (`src/storage/dump_format.h`) and supplied from `dump.restore_memory_budget_mb` / `dump.restore_max_section_mb` at the call sites (`src/server/handlers/dump_handler.cpp`, `src/app/server_orchestrator.cpp`). An index payload is additionally pre-screened at three times its encoded length before materialization (`src/storage/dump_format_internal.cpp`). A document payload is pre-screened against the smallest store its bytes could decode into, computed from the section length and the document count read out of the payload header before the documents themselves are decoded (`src/storage/dump_format_internal.cpp`, applied at `src/storage/dump_format_v1.cpp` and `src/storage/dump_format_v2.cpp`); a section that cannot fit fails with `kStorageDumpReadError` and leaves the live index and document store untouched. `WriteDumpV2` refuses to *write* an artifact that could not be restored under the same limits (`src/storage/dump_format_v2.cpp`).

`VerifyDumpIntegrity` checks magic, version, header consistency, file size and file CRC for both versions, and additionally every section envelope CRC for V2 (`src/storage/dump_format_v2.cpp`, `src/storage/dump_format_v1_integrity.cpp`). It does not deserialize payloads. The V1 branch is dispatched without the caller's `RestoreLimits`, which the V1 verifier does not accept (`src/storage/dump_format_v2.cpp`).

### 1.7 Provenance and metadata recorded in a dump

| Recorded value | Where | Enforced on load? |
|---|---|---|
| `dump_timestamp` | header (`src/storage/dump_format_v2.cpp`) | No. Surfaced through `DUMP INFO` only (`src/storage/dump_format_v2.cpp`) |
| `gtid` | header (`src/storage/dump_format_v2.cpp`) | By the caller's validator, not the format: startup restore refuses an empty GTID when replication is enabled (`src/app/server_orchestrator.cpp`); `DUMP LOAD` refuses to replace a non-empty running GTID with an empty one (`src/server/handlers/dump_handler.cpp`) |
| `total_file_size`, `file_crc32` | header | Yes, always (§1.6) |
| `section_count` | header (V2) | Yes (`src/storage/dump_format_v2.cpp`) |
| MySQL `host` / `port` / `database` | config section (`src/storage/dump_format_v1_config.cpp`) | Yes, by both load paths (`src/server/handlers/dump_handler.cpp`, `src/app/server_orchestrator.cpp`) |
| MySQL `user` / `password` | **Not recorded.** Empty strings are written in their place by design (`src/storage/dump_format_v1_config.cpp`) | n/a |
| `memory.verify_text` | compatibility metadata (`src/storage/dump_format_v1_config.cpp`) | Yes (§4) |
| MySQL source server UUID | compatibility metadata v2 (`src/storage/dump_format_v1_config.cpp`) | Yes, when the dump records one (§4.3) |
| Tokenizer and normalization settings | config section, per table (`src/storage/dump_format_v1_config.cpp`) | Yes (§4) |
| `DumpStatistics`, `TableStatistics` | optional sections (`src/storage/dump_format.h`) | No. Descriptive only |

## 2. Index serialization format

One index payload is embedded per table inside a dump, and the same bytes are produced by `Index::SaveToFile` (`src/index/index_serialization.cpp`). The payload is self-delimiting only in the sense that the dump gives it an explicit byte length; the index decoder itself consumes a whole buffer (`src/index/index_serialization.cpp`).

Magic is ASCII `MGIX` (`src/index/index_serialization.cpp`). Version is a little-endian `uint32` at offset 4. Versions declared: V1 through V4 (`src/index/index_format.h`).

### 2.1 Layout by version

All four versions share this prefix:

| Offset | Width | Field |
|---|---|---|
| 0 | 4 | magic `MGIX` |
| 4 | 4 | `version` |
| 8 | 4 | `ngram_size` |

**V1** continues directly with the term table and has no trailer:

```
term_count : uint64
  per term:
    term_len     : uint32     # rejected above 10000 (index_serialization.cpp:505)
    term_bytes   : term_len
    posting_size : uint64     # rejected above 100_000_000 (index_serialization.cpp:547)
    posting_data : posting_size
```

Minimum V1 payload is 20 bytes (`src/index/index_serialization.cpp`).

**V2** is V1 plus a 4-byte CRC32 trailer appended after the last posting list, computed over everything that precedes it with seed 0 (`src/index/index_serialization.cpp`).

**V3** inserts two tokenizer fields after `ngram_size`, before `term_count`, and keeps the V2 trailer:

| Offset | Width | Field |
|---|---|---|
| 12 | 4 | `kanji_ngram_size` |
| 16 | 1 | `cross_boundary_ngrams` (0 or 1) |

Minimum V3 payload is 25 bytes plus the 4-byte trailer (`src/index/index_serialization.cpp`).

**V4** — the version the current code writes — adds the normalization settings after `cross_boundary_ngrams` and keeps the trailer (`src/index/index_serialization.cpp`):

| Offset | Width | Field |
|---|---|---|
| 17 | 1 | `normalize_nfkc` (0 or 1) |
| 18 | 4 | `normalize_width_len` |
| 22 | `normalize_width_len` | `normalize_width` (UTF-8) |
| 22 + len | 1 | `normalize_lower` (0 or 1) |

Minimum V4 payload is 31 bytes plus the trailer (`src/index/index_serialization.cpp`).

### 2.2 Configuration agreement enforced by the index decoder

The index decoder is not a pure deserializer: it refuses payloads whose recorded tokenizer settings disagree with the live `Index` object it is loading into. `ngram_size` must match for every version (`src/index/index_serialization.cpp`). For V3 and V4, `kanji_ngram_size` and `cross_boundary_ngrams` must match (`src/index/index_serialization.cpp`). For V4, `normalize_nfkc`, `normalize_width` and `normalize_lower` must all match (`src/index/index_serialization.cpp`). Every one of these fails with `kStorageVersionMismatch` (5005).

Because a dump load constructs the staging `Index` from the *live* index's settings (`src/storage/dump_format_internal.cpp`), these checks compare the dump against the running configuration, duplicating part of the config-level check in §4 at a lower layer.

### 2.3 Write and read support

The writer always emits `kCurrentFormatVersion`, which is V4 (`src/index/index_format.h`, written at `src/index/index_serialization.cpp`). V1, V2, V3 and V4 are all accepted on read, matching the published range `index.serialization_version_accepted_min` / `_max` (`src/index/index_format.h`, rendered at `src/app/surface_descriptor.cpp`); the reader tests the four values explicitly and rejects anything else at `src/index/index_serialization.cpp` with `kStorageVersionMismatch` and the version number in the error context.

Acceptance does not imply the payload carries its own checksum: V1 has no CRC32 trailer at all, and the trailer is verified only for V2, V3 and V4 (`src/index/index_serialization.cpp`). A V1 index payload's only integrity check is the enclosing dump's file-level, and for a V2 container section-level, CRC.

**Untested read paths.** No test constructs a V2 or a V3 index payload — the only synthetic `MGIX` payloads in the suite are version 1 (`tests/index/index_serialization_test.cpp`) and version 99 (`tests/index/index_advanced_test.cpp`). The V1 read path is reached only by malformed-input tests that assert on the rejection, never by a test that loads a well-formed V1 index and asserts on the recovered terms. V4 is covered by round-trip through the dump tests. **V2 and V3 index read paths therefore have no test coverage at all, and V1 has rejection-path coverage only.**

## 3. Document store serialization format

One document-store payload is embedded per table inside a dump, and the same bytes are produced by `DocumentStore::SaveToFile` (`src/storage/document_store_persistence.cpp`).

Magic is ASCII `MGDS` (`src/storage/document_store_persistence.cpp`). Version is a little-endian `uint32` at offset 4. There is no checksum in this payload; integrity is provided by the enclosing dump's file-level and (V2) section-level CRC.

```
magic        : 4 bytes "MGDS"
version      : uint32
next_doc_id  : uint32
gtid_len     : uint32          # rejected above 1024 (document_store_persistence.cpp:49)
gtid         : gtid_len bytes
doc_count    : uint64          # rejected above 1_000_000_000 (document_store_persistence.cpp:50)
  per document:
    doc_id       : uint32
    pk_len       : uint32
    pk           : pk_len bytes
    filter_count : uint32
      per filter:
        name_len : uint32
        name     : name_len bytes
        type_idx : uint8       # std::variant index into FilterValue (document_store.h:73-86)
        value    : type-dependent; monostate writes nothing, string writes uint32 len + bytes
    v2+: norm_text_len : uint32 ; norm_text : bytes
    v3+: orig_text_len : uint32 ; orig_text : bytes
```

Version differences: V2 adds the normalized text field per document (`src/storage/document_store_persistence.cpp`), V3 adds the original text field. Nothing else changed.

The filter index is **not** serialized. It is rebuilt from the decoded per-document filter values after the document loop (`src/storage/document_store_persistence.cpp`); `src/storage/filter_index.cpp` contains no persistence entry points.

Post-decode invariants that reject a structurally valid but semantically impossible payload: `next_doc_id` must exceed every loaded document ID (`src/storage/document_store_persistence.cpp`); the doc-id and primary-key maps must be bijective and their size must equal `doc_count`; a duplicate filter name within one document is rejected; an unrecognized `type_idx` is rejected.

Only version 3 is written (`src/storage/document_store_persistence.cpp`). Versions 1, 2 and 3 are accepted on read; `version < 1 || version > 3` fails with `kStorageCorrupted` (5003) at `src/storage/document_store_persistence.cpp`. The V1 and V2 read paths are exercised by hand-built payloads in `tests/storage/document_store_test.cpp`.

## 4. Version-acceptance policy

### 4.1 Dump container

| Format | Version | Written by current code? | Read by current code? | Read path rejects because |
|---|---|---|---|---|
| Dump container | 0, or any value below 1 | No | **No** | `src/storage/dump_format_v2.cpp` — `version < kMinSupportedVersion` (1) → `kStorageDumpReadError` "Unsupported dump version". Direct V1 API: `src/storage/dump_format_v1.cpp` → `kStorageVersionMismatch` "Dump file version too old" |
| Dump container | 1 | Not by any server path. `dump_v2::WriteDump` always writes V2 (`src/storage/dump_format_v2.cpp`), and both server callers go through it (`src/server/handlers/dump_handler.cpp`, `src/server/snapshot_scheduler.cpp`). `dump_v1::WriteDumpV1` remains a public library entry point (`src/storage/dump_format_v1.h`) and is used only by tests | **Yes** — accepted and dispatched at `src/storage/dump_format_v2.cpp`. See §4.3 for load-time refusals that apply on top of this | — |
| Dump container | 2 | **Yes**, always (`src/storage/dump_format_v2.cpp`) | **Yes** (`src/storage/dump_format_v2.cpp`) | — |
| Dump container | 3 or higher | No | **No** | `src/storage/dump_format_v2.cpp` — `version > kMaxSupportedVersion` (2) → `kStorageDumpReadError` "Unsupported dump version". `VerifyDumpIntegrity`: `src/storage/dump_format_v2.cpp`. `GetDumpInfo`: `src/storage/dump_format_v2.cpp`. Direct V1 API: `src/storage/dump_format_v1.cpp` → `kStorageVersionMismatch` "Dump file version too new" |
| Dump container | any valid but wrong version handed to a version-specific API | n/a | n/a | `dump_v1::ReadDumpV1` on a V2 file: `src/storage/dump_format_v1.cpp` → `kStorageVersionMismatch` "Dump file version not implemented". `dump_v2::ReadDumpV2` on a V1 file: `src/storage/dump_format_v2.cpp` → `kStorageDumpReadError` "Not a V2 dump file". `dump_v1::VerifyDumpIntegrity` on a V2 file: `src/storage/dump_format_v1_integrity.cpp`. `dump_v1::GetDumpInfo` on a V2 file: `src/storage/dump_format_v1_integrity.cpp` |
| Dump container | any version, wrong magic | n/a | **No** | `src/storage/dump_format_v2.cpp` → `kStorageDumpReadError` "Invalid magic number" |

### 4.2 Embedded payloads

| Format | Version | Written by current code? | Read by current code? | Read path rejects because |
|---|---|---|---|---|
| Index payload | 1 | No | **Yes** (rejection-path test coverage only) | — |
| Index payload | 2 | No | **Yes** (no test coverage) | — |
| Index payload | 3 | No | **Yes** (no test coverage) | — |
| Index payload | 4 | **Yes** (`src/index/index_format.h`) | **Yes** | — |
| Index payload | 0, or 5 and above | No | **No** | `src/index/index_serialization.cpp` → `kStorageVersionMismatch` "Unsupported index format version" |
| Index payload | any version, wrong magic | n/a | **No** | `src/index/index_serialization.cpp` → `kStorageInvalidFormat` "Invalid magic number in index data" |
| Index payload | V2/V3/V4 with a missing or wrong CRC32 trailer | n/a | **No** | Missing trailer: `src/index/index_serialization.cpp` → `kStorageInvalidFormat`. Mismatch: `src/index/index_serialization.cpp` → `kStorageCRCMismatch` (5004) |
| Document store payload | 1 | No | **Yes** | — |
| Document store payload | 2 | No | **Yes** | — |
| Document store payload | 3 | **Yes** (`src/storage/document_store_persistence.cpp`) | **Yes** | — |
| Document store payload | 0, or 4 and above | No | **No** | `src/storage/document_store_persistence.cpp` → `kStorageCorrupted` "Unsupported document store file version" |
| Document store payload | any version, wrong magic | n/a | **No** | `src/storage/document_store_persistence.cpp` → `kStorageCorrupted` "Invalid document store file format (bad magic number)" |
| Compatibility metadata | 1 | No | **Yes**. Records no source server UUID, which §4.3 treats as an unknown source | — |
| Compatibility metadata | 2 | **Yes** (`src/storage/dump_format_v1_config.cpp`) | **Yes** | — |
| Compatibility metadata | anything else | No | **No** | `src/storage/dump_format_v1_config.cpp` → `kStorageVersionMismatch` "Unsupported compatibility metadata version" |

Compatibility metadata version 1 carries only `memory.verify_text`; version 2 appends the MySQL source server UUID (`src/storage/dump_format_v1_config.cpp`).

### 4.3 Load-time refusals that apply on top of container acceptance

Container acceptance is necessary but not sufficient. Both server load paths install a validator that runs after the whole dump has decoded and before any live table state is replaced (`src/storage/dump_format_v1.cpp`, `src/storage/dump_format_v2.cpp`). A refusal there is non-destructive: no table state has been swapped in and no replication state has changed.

**The source-identity rule is expressed once.** `FindDumpSourceIdentityMismatch` (`src/storage/dump_source_identity.h`) holds it, and both paths call it and nothing else: startup restore at `src/app/server_orchestrator.cpp` and `DUMP LOAD` at `src/server/handlers/dump_handler.cpp`. What a dump records is carried as `DumpSourceIdentity` (`src/storage/dump_source_identity.h`), which distinguishes an artifact that has no field for the UUID from one that recorded an empty value.

Which dumps carry the field at all:

- Only compatibility metadata version 2 records it (`src/storage/dump_format_v1_config.cpp`); version 1 leaves the source unrecorded.
- A V1 container never carries one whatever its metadata version says, because `ReadDumpV1` has no output parameter for it (`src/storage/dump_format_v1.h`) and the dispatcher leaves the field unrecorded before dispatching (`src/storage/dump_format_v2.cpp`).

The rule those two paths then apply:

- **A dump that records no source server UUID is accepted.** It makes no claim about where it came from, so there is nothing for the running server to disagree with (`src/storage/dump_source_identity.h`). Every other check — host, port, database, GTID, and the configuration comparison of §5 — still applies unchanged.
- **A dump that recorded an empty UUID is refused** whenever the running server has one (`src/storage/dump_source_identity.h`), with the message `dump does not record its MySQL source server UUID`. The UUID passed to `WriteDump` comes from the binlog reader (`src/server/handlers/dump_handler.cpp`, `src/server/snapshot_scheduler.cpp`), so this is a dump taken with no binlog reader attached, or by a build without `USE_MYSQL`.
- **A dump whose recorded UUID differs from the running server's is refused** (`src/storage/dump_source_identity.h`), with the message `dump MySQL source server UUID does not match the running MySQL source`.
- **When the running server's own UUID is unknown the check is skipped** (`src/storage/dump_source_identity.h`). For `DUMP LOAD` that is the case with no binlog reader attached (`src/server/handlers/dump_handler.cpp`); startup restore reaches the validator only once `GetServerUUID` has succeeded (`src/app/server_orchestrator.cpp`).

## 5. Configuration compatibility on load

The config section records the full server configuration minus credentials (`src/storage/dump_format_v1_config.cpp`). The subset that must agree with the running configuration is decided by `FindDumpConfigMismatch` (`src/server/dump_config_validator.h`), which both load paths call first (`src/app/server_orchestrator.cpp`, `src/server/handlers/dump_handler.cpp`). Any mismatch aborts the load with `kStorageVersionMismatch` (5005) and leaves live table state untouched.

| Value | Rejection condition |
|---|---|
| `memory.verify_text` | Dump records no value **and** the running config is not `off` |
| `memory.verify_text` | Dump records a value that differs from the running config |
| `memory.normalize.nfkc` | Differs |
| `memory.normalize.width` | Differs |
| `memory.normalize.lower` | Differs |
| `tables[*].ngram_size` | Differs, for any table whose qualified `database.table` name appears in both the dump and the running config; a dump entry that records no database is matched by its bare name |
| `tables[*].kanji_ngram_size` | Differs |
| `tables[*].cross_boundary_ngrams` | Differs |

Every row in this table is enforced in `src/server/dump_config_validator.h`.

A table present in the dump but absent from the running config is skipped by this comparison (`src/server/dump_config_validator.h`), but the table-set check in the format layer rejects the dump anyway (`src/storage/dump_format_internal.cpp`).

Beyond `FindDumpConfigMismatch`, both load paths independently require `mysql.host`, `mysql.port` and `mysql.database` to match (`src/app/server_orchestrator.cpp`, `src/server/handlers/dump_handler.cpp`), plus the source-UUID and GTID rules in §4.3.

### 5.1 Table entries written before `cross_boundary_ngrams` existed

The config section carries no version of its own, and one field was added to each table entry after the first releases: `cross_boundary_ngrams`, a single byte written between `kanji_ngram_size` and `posting.block_size` (`src/storage/dump_format_v1_config.cpp`). A table entry from a release that predates it is one byte shorter.

The decoder tells the two layouts apart by that byte's value (`src/storage/dump_format_v1_config.cpp`). It is a serialized bool, so `0` and `1` mean the field is present; any other value is the least significant octet of the `posting.block_size` that follows it in the older layout, and the four-byte value is reassembled from it (`src/storage/dump_format_v1_config.cpp`). Where the field is absent, the running default stands rather than a decoded value (`src/config/config.h`).

The two layouts are therefore distinguishable exactly when the older entry's `posting.block_size` is not congruent to 0 or 1 modulo 256. An older entry that does record such a block size decodes misaligned, and the table entry is refused as it is today; nothing decodes into the wrong field silently, because every later field in the entry — including the length-prefixed `posting.use_roaring` — is then read from shifted bytes and fails its bound. Pinned by `DumpFormatV1Test.ConfigWithoutCrossBoundaryNgramsDecodes`.

The "records no value" case in §5's first row is reachable in two ways. `DeserializeConfig` clears `memory.verify_text` before decoding, as an explicit unknown sentinel (`src/storage/dump_format_v1_config.cpp`); it is then filled in only if a V1 dump sets `flags_v1::kHasCompatibilityMetadata` (`src/storage/dump_format_v1.cpp`) or a V2 dump carries a `kCompatibilityMetadata` section (`src/storage/dump_format_v2.cpp`). A dump from a release that predates either mechanism therefore loads only into a server running `memory.verify_text = off` — which is the default (`src/config/config.h`).

Every other value in the config section round-trips into `config::Config` and is returned to the caller (`src/storage/dump_format_v1.cpp`, `src/storage/dump_format_v2.cpp`), but is not compared against the running configuration.

## 6. Known divergences

Each item states what a shipped document asserts and what the code does, with citations for both. No remedies are proposed.

**6.1 "Old V1 dumps still loadable" does not reach v1.3.2.** `docs/releases/v1.6.0.md` states that V1 dumps remain loadable after the V2 format was introduced. That holds from v1.5.4 onwards, at the container layer (`src/storage/dump_format_v2.cpp`) and at the server layer (§4.3). It does not hold for v1.3.2, which wrote every posting list's strategy byte, element count and delta values with their octets in the reverse order to `PostingList::Serialize` (`src/index/posting_list.cpp`). Nothing in the artifact records which order was used, so `PostingList::Deserialize` (`src/index/posting_list.cpp`) cannot tell the two apart, and such a dump fails with `kStorageDumpReadError` and the message `LoadFromStream failed for index` (`src/storage/dump_format_internal.cpp`) after its header and its config section have decoded. Pinned by `ReleaseDumpCorpusRejectionTest.DumpWrittenByV132IsRefusedForItsPostingListByteOrder`. The header and config concessions that get such a dump that far are described in §1.2 and §5.1.

**6.2 Forward compatibility of the version ceiling.** `src/storage/dump_format.h` states that `kMaxSupportedVersion` "can support newer versions for forward compatibility". It is set equal to `kCurrentVersion` (`src/storage/dump_format.h`), so no version above 2 is accepted by any reader (`src/storage/dump_format_v2.cpp`). Forward compatibility in V2 is provided at section granularity — unknown `SectionType` values are skipped (`src/storage/dump_format_v2.cpp`) — not at container-version granularity.

**6.3 Compression and encryption flags.** `src/storage/dump_format.h` documents `kCompressed`, `kEncrypted` and `kIncremental` as reserved for future use. No write path sets them and no read path tests them, in either container version (`src/storage/dump_format_v1.cpp`, `src/storage/dump_format_v2.cpp`). No dump section is compressed or encrypted; lz4 is present in the build only for query-result caching (`src/cache/result_compressor.cpp`).

**6.4 `kWithCRC` described as always set.** `src/storage/dump_format.h` annotates `kWithCRC` as "always set in V1". That is true of what `WriteDumpV1` produces (`src/storage/dump_format_v1.cpp`), and the V1 reader verifies the file CRC unconditionally, ignoring the flag (`src/storage/dump_format_v1.cpp`). The V2 reader, by contrast, treats the flag as authoritative and skips file-level CRC verification entirely when it is clear (`src/storage/dump_format_v2.cpp`, and in the verifier `src/storage/dump_format_v2.cpp`). The same bit therefore has advisory meaning in one container version and no meaning in the other.

**6.5 BM25 and synonym sections.** `docs/releases/v1.6.0.md` and `docs/releases/v1.6.0.md` describe the V2 format as carrying BM25 and synonym data, and the section types `kTableBM25 = 5` and `kTableSynonyms = 7` plus flags `kHasBM25Data` and `kHasSynonymData` exist (`src/storage/dump_format.h`). `WriteDumpV2` emits neither section and sets neither flag (`src/storage/dump_format_v2.cpp`), and `ReadDumpV2`'s dispatch has no case for either type, so both would take the unknown-section skip path (`src/storage/dump_format_v2.cpp`). BM25 corpus statistics are recomputed from the restored document store after a load instead (`src/app/server_orchestrator.cpp`).

**6.6 Untested accepted versions.** The acceptance policy above is wider than the tested surface. Index payload versions 2 and 3 are accepted with no test constructing such a payload; index version 1 is reached only by tests that assert on rejection (`tests/index/index_serialization_test.cpp`, `tests/index/index_advanced_test.cpp`). Document store versions 1 and 2 are covered (`tests/storage/document_store_test.cpp`), as is dump container version 1 (`tests/storage/dump_format_v2_test.cpp`).
