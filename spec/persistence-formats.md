# Persistence Formats

This file is normative. It describes the on-disk artifacts MygramDB writes and the exact set of format versions the current code accepts when reading them. The version-acceptance policy in this document is a hard compatibility contract: narrowing it — refusing a version that is accepted today, or adding a load-time check that rejects an artifact an earlier release wrote — is an externally visible surface change, not an internal refactor.

A dump file is a nested artifact. The outer container (`.dmp`) carries its own version; inside it, every table contributes an independently versioned index payload and an independently versioned document-store payload. The three version numbers advance independently and are documented separately below.

## 1. Dump (snapshot) container format

### 1.1 Fixed file header

Every dump, regardless of container version, begins with the same 8 bytes (`dump_format.h:33`, `dump_format.h:49`).

| File offset | Width | Field | Meaning |
|---|---|---|---|
| 0 | 4 | `magic` | ASCII `MGDB`, byte-for-byte, not endian-converted (`dump_format_v2.cpp:544`) |
| 4 | 4 | `version` | Container format version, little-endian `uint32` (`dump_format_v2.cpp:545`) |

All multi-byte integers in every dump section are written little-endian and converted on both ends (`binary_io.h:39`, `binary_io.h:70`, `endian_utils.h:75`). Strings are length-prefixed with a little-endian `uint32` and are not NUL-terminated (`dump_format_v1_internal.h:28`, `dump_format_v1_internal.h:46`). Reads of length-prefixed strings are bounded per field type: identifiers 1 KiB, config values 4 KiB, paths 8 KiB, text content 16 MiB, general 1 MiB (`dump_format_v1.h:127-143`). GTIDs are bounded at 64 KiB in both container versions, and both writers reject a longer one before emitting any byte (`dump_format_v1.h:137`, `dump_format_v1.cpp:154-157`, `dump_format_v1.cpp:319-322`, `dump_format_v2.cpp:301-304`).

Container versions that exist: `V1 = 1` and `V2 = 2` (`dump_format.h:55-58`).

### 1.2 Container V1 layout

`WriteDumpV1` emits, after the fixed header (`dump_format_v1.cpp:413-599`):

| File offset | Width | Field | Meaning |
|---|---|---|---|
| 8 | 4 | `header_size` | `32 + gtid.size()` (`dump_format_v1.cpp:423`, checked at `dump_format_v1.cpp:201-215`). A literal `0` is accepted as well and means the writing release never recorded the field (`dump_format_v1.h:185`, `dump_format_v1.cpp:207`) |
| 12 | 4 | `flags` | See below (`dump_format_v1.cpp:424-428`) |
| 16 | 8 | `dump_timestamp` | Unix seconds at write time (`dump_format_v1.cpp:429`) |
| 24 | 8 | `total_file_size` | Patched in place after the body is written (`dump_format_v1.cpp:615`, offset constant `dump_format_v1.h:156`) |
| 32 | 4 | `file_crc32` | Patched in place last (`dump_format_v1.cpp:630`, offset constant `dump_format_v1.h:158`) |
| 36 | 4 | `gtid_length` | |
| 40 | N | `gtid` | UTF-8 replication position |

The header struct's own doc comment (`dump_format_v1.h:166-177`) tabulates the same fields relative to the start of the V1 header; add 8 to convert those to file offsets.

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

The statistics length field is always present even with no statistics (`dump_format_v1.cpp:468-473`). Compatibility metadata is appended *inside* the config section rather than getting its own section (`dump_format_v1.cpp:444-447`), and the reader only decodes it when `flags_v1::kHasCompatibilityMetadata` is set (`dump_format_v1.cpp:825`).

V1 flags (`dump_format.h:76-84`): `kCompressed = 0x1`, `kEncrypted = 0x2`, `kIncremental = 0x4`, `kWithStatistics = 0x8`, `kWithCRC = 0x10`, `kHasCompatibilityMetadata = 0x20`. `WriteDumpV1` sets `kWithCRC | kHasCompatibilityMetadata`, plus `kWithStatistics` when statistics are supplied (`dump_format_v1.cpp:424-428`). The first three are declared but never set and never read anywhere in the tree.

### 1.3 Container V2 layout

`WriteDumpV2` emits, after the fixed header (`dump_format_v2.cpp:544-731`):

| File offset | Width | Field | Meaning |
|---|---|---|---|
| 8 | 4 | `header_size` | `36 + gtid.size()` (`dump_format_v2.cpp:555`, checked at `dump_format_v2.cpp:70-80`) |
| 12 | 4 | `flags` | See below (`dump_format_v2.cpp:556-559`) |
| 16 | 8 | `dump_timestamp` | Unix seconds at write time (`dump_format_v2.cpp:560`) |
| 24 | 8 | `total_file_size` | Patched in place (`dump_format_v2.cpp:749`, offset constant `dump_format_v2.h:128`) |
| 32 | 4 | `file_crc32` | Patched in place last (`dump_format_v2.cpp:765`, offset constant `dump_format_v2.h:130`) |
| 36 | 4 | `section_count` | Patched in place (`dump_format_v2.cpp:752`, offset constant `dump_format_v2.h:132`) |
| 40 | 4 | `gtid_length` | |
| 44 | N | `gtid` | UTF-8 replication position |

The body is a flat run of exactly `section_count` sections. Each section is a 16-byte envelope followed by its data (`dump_format.h:111-118`, written at `dump_format_v2.cpp:358-383`):

| Envelope offset | Width | Field |
|---|---|---|
| 0 | 4 | `type` (`SectionType`, little-endian `uint32`) |
| 4 | 4 | `crc32` of the section data only |
| 8 | 8 | `data_length` of the section data |

Section types (`dump_format.h:94-102`): `kConfig = 1`, `kStatistics = 2`, `kTableData = 3`, `kTableBM25 = 5`, `kTableSynonyms = 7`, `kCompatibilityMetadata = 8`.

`WriteDumpV2` emits, in this order: one `kConfig` section, one `kCompatibilityMetadata` section, an optional `kStatistics` section, then one `kTableData` section per table (`dump_format_v2.cpp:568-731`). `kTableBM25` and `kTableSynonyms` are never written.

A `kTableData` section's payload has the same shape as one V1 table entry — name, table-stats length + data, `uint64` index length + data, `uint64` docstore length + data (`dump_format_v2.cpp:179-267`). There is no `table_count` field in V2; the table count is the number of `kTableData` sections.

V2 flags (`dump_format.h:126-132`): `kWithStatistics = 0x8`, `kWithCRC = 0x10`, `kHasBM25Data = 0x20`, `kHasSynonymData = 0x80`. `WriteDumpV2` sets `kWithCRC`, plus `kWithStatistics` when statistics are supplied (`dump_format_v2.cpp:556-559`). `kHasBM25Data` and `kHasSynonymData` are never set and never read.

On POSIX, table sections are streamed straight to the file descriptor and their envelope CRC and length are patched back afterwards (`dump_format_v2.cpp:153-291`); on Windows the section is buffered in memory first (`dump_format_v2.cpp:648-725`). The resulting bytes are identical.

### 1.4 What changed between V1 and V2

- V1 is a flat sequence of implicitly ordered sections; V2 wraps each section in a self-describing envelope, so a reader that meets an unrecognized `SectionType` skips `data_length` bytes and continues (`dump_format_v2.cpp:1221-1231`).
- V1 has one file-level CRC32 and nothing else; V2 adds a per-section CRC32 in every envelope, verified as the section is decoded (`dump_format_v2.cpp:1238-1253`).
- V1 folds compatibility metadata into the tail of the config section behind a header flag; V2 gives it a dedicated `kCompatibilityMetadata` section (`dump_format_v2.cpp:591-610`).
- V2's compatibility metadata carries the MySQL source server UUID; V1's, as written, carries an empty UUID and the V1 reader never reports it, so a V1 dump's source is unknown (see §4.3).
- V2 adds `section_count` to the header, and rejects `section_count == 0` (`dump_format_v2.cpp:84-86`).

### 1.5 Compression

**No dump section is compressed, in either container version.** There is no compression framing to document. The `flags_v1::kCompressed` bit (`dump_format.h:78`) is declared and never set or tested by any code path. lz4 is linked into the build, but only for query-result caching (`src/cache/result_compressor.cpp`, `src/cache/query_cache.cpp`); no storage or index translation unit references it.

### 1.6 Integrity

Checksums are CRC32 as implemented by zlib, polynomial `0xEDB88320` (`crc32.h:37-51`, `dump_format_v2.cpp:93-103`).

**File-level CRC32.** Covers the entire file from byte 0 to `total_file_size`, with the 4 bytes of the `file_crc32` field itself substituted with zeros (`dump_format_internal.cpp:161-191`, and the `pread`-based variant `dump_format_internal.cpp:98-133`). The seed is 0.

Before the CRC is checked, both readers compare the actual file size against `total_file_size` and refuse a mismatch — this is the truncation and append detector (`dump_format_v1.cpp:754-767`, `dump_format_v2.cpp:925-938`). On CRC mismatch the load fails with `kStorageDumpReadError` (5011) and sets `IntegrityError::type = FileCRC` (`dump_format_v1.cpp:786-798`, `dump_format_v2.cpp:951-964`). No partial state is applied: table state is staged into fresh objects and swapped in only after every section has decoded and the caller's validator has passed (`dump_format_internal.cpp:287-325`, `dump_load_access.h:29-74`).

The V1 reader verifies the file CRC unconditionally (`dump_format_v1.cpp:770-799`). The V2 reader verifies it only when `flags_v2::kWithCRC` is set in the header (`dump_format_v2.cpp:943`); a checksum value of zero is a legitimate checksum and is compared normally, not treated as "absent".

**Section-level CRC32 (V2 only).** Each envelope's `crc32` covers exactly the section's `data_length` bytes. The reader hashes the bytes while the decoder consumes them and compares after the section is fully read (`dump_format_v2.cpp:1013`, `dump_format_v2.cpp:1238-1253`), failing with `kStorageDumpReadError` and `IntegrityError::type = SectionCRC`.

**Structural checks applied on load.** V2 rejects: a duplicate `kConfig`, `kCompatibilityMetadata` or `kStatistics` section (`dump_format_v2.cpp:1018`, `:1036`, `:1060`); a `kCompatibilityMetadata` section that precedes `kConfig` (`dump_format_v2.cpp:1040`); a duplicate table section (`dump_format_v2.cpp:1089`); a section whose declared length runs past end of file (`dump_format_v2.cpp:992`); an envelope `data_length` above 4 GiB (`dump_format_v2.h:96`, enforced at `dump_format_v2.cpp:409`); a decoder that leaves bytes unconsumed inside its bounded payload (`dump_format_v2.cpp:1234`); a section count that disagrees with the header (`dump_format_v2.cpp:1259`); and a missing `kConfig` section (`dump_format_v2.cpp:1273`). Both versions reject a dump whose table set does not exactly match the configured tables (`dump_format_internal.cpp:327-361`), and a zero-length index or docstore payload (`dump_format_v1.cpp:975`, `dump_format_v1.cpp:1011`, `dump_format_v2.cpp:1153`, `dump_format_v2.cpp:1187`).

Both readers additionally enforce `RestoreLimits` — a memory budget and a per-section byte ceiling, defaulting to 4 GiB and 2 GiB (`dump_format.h:180-183`) and supplied from `dump.restore_memory_budget_mb` / `dump.restore_max_section_mb` at the call sites (`dump_handler.cpp:589-594`, `server_orchestrator.cpp:581-584`). An index payload is additionally pre-screened at three times its encoded length before materialization (`dump_format_internal.cpp:273-289`). A document payload is pre-screened against the smallest store its bytes could decode into, computed from the section length and the document count read out of the payload header before the documents themselves are decoded (`dump_format_internal.cpp:291-308`, `:310-347`, `:349-360`, applied at `dump_format_v1.cpp:1023-1030` and `dump_format_v2.cpp:1195-1202`); a section that cannot fit fails with `kStorageDumpReadError` and leaves the live index and document store untouched. `WriteDumpV2` refuses to *write* an artifact that could not be restored under the same limits (`dump_format_v2.cpp:437-461`, `:575`, `:598`, `:619`, `:273`).

`VerifyDumpIntegrity` checks magic, version, header consistency, file size and file CRC for both versions, and additionally every section envelope CRC for V2 (`dump_format_v2.cpp:1382-1531`, `dump_format_v1_integrity.cpp:29-141`). It does not deserialize payloads. The V1 branch is dispatched without the caller's `RestoreLimits`, which the V1 verifier does not accept (`dump_format_v2.cpp:1420-1422`).

### 1.7 Provenance and metadata recorded in a dump

| Recorded value | Where | Enforced on load? |
|---|---|---|
| `dump_timestamp` | header (`dump_format_v2.cpp:560`) | No. Surfaced through `DUMP INFO` only (`dump_format_v2.cpp:1586`) |
| `gtid` | header (`dump_format_v2.cpp:561`) | By the caller's validator, not the format: startup restore refuses an empty GTID when replication is enabled (`server_orchestrator.cpp:574-578`); `DUMP LOAD` refuses to replace a non-empty running GTID with an empty one (`dump_handler.cpp:571-582`) |
| `total_file_size`, `file_crc32` | header | Yes, always (§1.6) |
| `section_count` | header (V2) | Yes (`dump_format_v2.cpp:1259`) |
| MySQL `host` / `port` / `database` | config section (`dump_format_v1_config.cpp:347-364`) | Yes, by both load paths (`dump_handler.cpp:544-555`, `server_orchestrator.cpp:561-567`) |
| MySQL `user` / `password` | **Not recorded.** Empty strings are written in their place by design (`dump_format_v1_config.cpp:353-361`) | n/a |
| `memory.verify_text` | compatibility metadata (`dump_format_v1_config.cpp:721`) | Yes (§4) |
| MySQL source server UUID | compatibility metadata v2 (`dump_format_v1_config.cpp:724`) | Yes, when the dump records one (§4.3) |
| Tokenizer and normalization settings | config section, per table (`dump_format_v1_config.cpp:166-178`, `:448-456`) | Yes (§4) |
| `DumpStatistics`, `TableStatistics` | optional sections (`dump_format.h:199-227`) | No. Descriptive only |

## 2. Index serialization format

One index payload is embedded per table inside a dump, and the same bytes are produced by `Index::SaveToFile` (`index_serialization.cpp:49`). The payload is self-delimiting only in the sense that the dump gives it an explicit byte length; the index decoder itself consumes a whole buffer (`index_serialization.cpp:279`).

Magic is ASCII `MGIX` (`index_serialization.cpp:140`, checked at `index_serialization.cpp:301`). Version is a little-endian `uint32` at offset 4. Versions declared: V1 through V4 (`index_serialization.cpp:38-42`).

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

Minimum V1 payload is 20 bytes (`index_serialization.cpp:284`).

**V2** is V1 plus a 4-byte CRC32 trailer appended after the last posting list, computed over everything that precedes it with seed 0 (`index_serialization.cpp:201`, verified at `index_serialization.cpp:347-367`).

**V3** inserts two tokenizer fields after `ngram_size`, before `term_count`, and keeps the V2 trailer:

| Offset | Width | Field |
|---|---|---|
| 12 | 4 | `kanji_ngram_size` |
| 16 | 1 | `cross_boundary_ngrams` (0 or 1) |

Minimum V3 payload is 25 bytes plus the 4-byte trailer (`index_serialization.cpp:286`, `:332`).

**V4** — the version the current code writes — adds the normalization settings after `cross_boundary_ngrams` and keeps the trailer (`index_serialization.cpp:113-119`, `:150-157`):

| Offset | Width | Field |
|---|---|---|
| 17 | 1 | `normalize_nfkc` (0 or 1) |
| 18 | 4 | `normalize_width_len` |
| 22 | `normalize_width_len` | `normalize_width` (UTF-8) |
| 22 + len | 1 | `normalize_lower` (0 or 1) |

Minimum V4 payload is 31 bytes plus the trailer (`index_serialization.cpp:288`, `:334`).

### 2.2 Configuration agreement enforced by the index decoder

The index decoder is not a pure deserializer: it refuses payloads whose recorded tokenizer settings disagree with the live `Index` object it is loading into. `ngram_size` must match for every version (`index_serialization.cpp:383-392`). For V3 and V4, `kanji_ngram_size` and `cross_boundary_ngrams` must match (`index_serialization.cpp:404-426`). For V4, `normalize_nfkc`, `normalize_width` and `normalize_lower` must all match (`index_serialization.cpp:452-464`). Every one of these fails with `kStorageVersionMismatch` (5005).

Because a dump load constructs the staging `Index` from the *live* index's settings (`dump_format_internal.cpp:288-291`), these checks compare the dump against the running configuration, duplicating part of the config-level check in §4 at a lower layer.

### 2.3 Write and read support

Only V4 is written (`index_serialization.cpp:42`, `:143`). V1, V2, V3 and V4 are all accepted on read; anything else is rejected at `index_serialization.cpp:316-326` with `kStorageVersionMismatch` and the version number in the error context.

**Untested read paths.** No test constructs a V2 or a V3 index payload — the only synthetic `MGIX` payloads in the suite are version 1 (`tests/index/index_serialization_test.cpp:46`) and version 99 (`tests/index/index_advanced_test.cpp:568`). The V1 read path is reached only by malformed-input tests that assert on the rejection, never by a test that loads a well-formed V1 index and asserts on the recovered terms. V4 is covered by round-trip through the dump tests. **V2 and V3 index read paths therefore have no test coverage at all, and V1 has rejection-path coverage only.**

## 3. Document store serialization format

One document-store payload is embedded per table inside a dump, and the same bytes are produced by `DocumentStore::SaveToFile` (`document_store_persistence.cpp:612`).

Magic is ASCII `MGDS` (`document_store_persistence.cpp:69`, checked at `document_store_persistence.cpp:183`). Version is a little-endian `uint32` at offset 4. There is no checksum in this payload; integrity is provided by the enclosing dump's file-level and (V2) section-level CRC.

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

Version differences: V2 adds the normalized text field per document (`document_store_persistence.cpp:498`), V3 adds the original text field (`document_store_persistence.cpp:522`). Nothing else changed.

The filter index is **not** serialized. It is rebuilt from the decoded per-document filter values after the document loop (`document_store_persistence.cpp:553-557`); `filter_index.cpp` contains no persistence entry points.

Post-decode invariants that reject a structurally valid but semantically impossible payload: `next_doc_id` must exceed every loaded document ID (`document_store_persistence.cpp:559`); the doc-id and primary-key maps must be bijective and their size must equal `doc_count` (`document_store_persistence.cpp:563-573`); a duplicate filter name within one document is rejected (`document_store_persistence.cpp:487`); an unrecognized `type_idx` is rejected (`document_store_persistence.cpp:482`).

Only version 3 is written (`document_store_persistence.cpp:72`). Versions 1, 2 and 3 are accepted on read; `version < 1 || version > 3` fails with `kStorageCorrupted` (5003) at `document_store_persistence.cpp:194`. The V1 and V2 read paths are exercised by hand-built payloads in `tests/storage/document_store_test.cpp:1373-1450`.

## 4. Version-acceptance policy

### 4.1 Dump container

| Format | Version | Written by current code? | Read by current code? | Read path rejects because |
|---|---|---|---|---|
| Dump container | 0, or any value below 1 | No | **No** | `dump_format_v2.cpp:1363` — `version < kMinSupportedVersion` (1) → `kStorageDumpReadError` "Unsupported dump version". Direct V1 API: `dump_format_v1.cpp:704-713` → `kStorageVersionMismatch` "Dump file version too old" |
| Dump container | 1 | Not by any server path. `dump_v2::WriteDump` always writes V2 (`dump_format_v2.cpp:1328-1330`), and both server callers go through it (`dump_handler.cpp:315`, `snapshot_scheduler.cpp:318`). `dump_v1::WriteDumpV1` remains a public library entry point (`dump_format_v1.h:318`) and is used only by tests | **Yes** — accepted and dispatched at `dump_format_v2.cpp:1373`. See §4.3 for load-time refusals that apply on top of this | — |
| Dump container | 2 | **Yes**, always (`dump_format_v2.cpp:545`) | **Yes** (`dump_format_v2.cpp:1378`) | — |
| Dump container | 3 or higher | No | **No** | `dump_format_v2.cpp:1363` — `version > kMaxSupportedVersion` (2) → `kStorageDumpReadError` "Unsupported dump version". `VerifyDumpIntegrity`: `dump_format_v2.cpp:1412`. `GetDumpInfo`: `dump_format_v2.cpp:1556`. Direct V1 API: `dump_format_v1.cpp:694-702` → `kStorageVersionMismatch` "Dump file version too new" |
| Dump container | any valid but wrong version handed to a version-specific API | n/a | n/a | `dump_v1::ReadDumpV1` on a V2 file: `dump_format_v1.cpp:716-725` → `kStorageVersionMismatch` "Dump file version not implemented". `dump_v2::ReadDumpV2` on a V1 file: `dump_format_v2.cpp:892-901` → `kStorageDumpReadError` "Not a V2 dump file". `dump_v1::VerifyDumpIntegrity` on a V2 file: `dump_format_v1_integrity.cpp:67-71`. `dump_v1::GetDumpInfo` on a V2 file: `dump_format_v1_integrity.cpp:200-203` |
| Dump container | any version, wrong magic | n/a | **No** | `dump_format_v2.cpp:1353` → `kStorageDumpReadError` "Invalid magic number" |

### 4.2 Embedded payloads

| Format | Version | Written by current code? | Read by current code? | Read path rejects because |
|---|---|---|---|---|
| Index payload | 1 | No | **Yes** (rejection-path test coverage only) | — |
| Index payload | 2 | No | **Yes** (no test coverage) | — |
| Index payload | 3 | No | **Yes** (no test coverage) | — |
| Index payload | 4 | **Yes** (`index_serialization.cpp:42`) | **Yes** | — |
| Index payload | 0, or 5 and above | No | **No** | `index_serialization.cpp:316-326` → `kStorageVersionMismatch` "Unsupported index format version" |
| Index payload | any version, wrong magic | n/a | **No** | `index_serialization.cpp:301-309` → `kStorageInvalidFormat` "Invalid magic number in index data" |
| Index payload | V2/V3/V4 with a missing or wrong CRC32 trailer | n/a | **No** | Missing trailer: `index_serialization.cpp:337-345` → `kStorageInvalidFormat`. Mismatch: `index_serialization.cpp:358-367` → `kStorageCRCMismatch` (5004) |
| Document store payload | 1 | No | **Yes** | — |
| Document store payload | 2 | No | **Yes** | — |
| Document store payload | 3 | **Yes** (`document_store_persistence.cpp:72`) | **Yes** | — |
| Document store payload | 0, or 4 and above | No | **No** | `document_store_persistence.cpp:194-197` → `kStorageCorrupted` "Unsupported document store file version" |
| Document store payload | any version, wrong magic | n/a | **No** | `document_store_persistence.cpp:183-186` → `kStorageCorrupted` "Invalid document store file format (bad magic number)" |
| Compatibility metadata | 1 | No | **Yes**. Records no source server UUID, which §4.3 treats as an unknown source | — |
| Compatibility metadata | 2 | **Yes** (`dump_format_v1_config.cpp:716`) | **Yes** | — |
| Compatibility metadata | anything else | No | **No** | `dump_format_v1_config.cpp:738-741` → `kStorageVersionMismatch` "Unsupported compatibility metadata version" |

Compatibility metadata version 1 carries only `memory.verify_text`; version 2 appends the MySQL source server UUID (`dump_format_v1_config.cpp:721-726`, read at `:742-758`).

### 4.3 Load-time refusals that apply on top of container acceptance

Container acceptance is necessary but not sufficient. Both server load paths install a validator that runs after the whole dump has decoded and before any live table state is replaced (`dump_format_v1.cpp:1049-1053`, `dump_format_v2.cpp:1286-1290`). A refusal there is non-destructive: no table state has been swapped in and no replication state has changed.

**The source-identity rule is expressed once.** `FindDumpSourceIdentityMismatch` (`dump_source_identity.h:61-75`) holds it, and both paths call it and nothing else: startup restore at `server_orchestrator.cpp:568-573` and `DUMP LOAD` at `dump_handler.cpp:556-567`. What a dump records is carried as `DumpSourceIdentity` (`dump_source_identity.h:27-32`), which distinguishes an artifact that has no field for the UUID from one that recorded an empty value.

Which dumps carry the field at all:

- Only compatibility metadata version 2 records it (`dump_format_v1_config.cpp:749-758`); version 1 leaves the source unrecorded (`dump_format_v1_config.cpp:745-748`).
- A V1 container never carries one whatever its metadata version says, because `ReadDumpV1` has no output parameter for it (`dump_format_v1.h:368-373`) and the dispatcher leaves the field unrecorded before dispatching (`dump_format_v2.cpp:1339-1343`).

The rule those two paths then apply:

- **A dump that records no source server UUID is accepted.** It makes no claim about where it came from, so there is nothing for the running server to disagree with (`dump_source_identity.h:63-65`). Every other check — host, port, database, GTID, and the configuration comparison of §5 — still applies unchanged.
- **A dump that recorded an empty UUID is refused** whenever the running server has one (`dump_source_identity.h:66-69`), with the message `dump does not record its MySQL source server UUID`. The UUID passed to `WriteDump` comes from the binlog reader (`dump_handler.cpp:300-304`, `snapshot_scheduler.cpp:310-315`), so this is a dump taken with no binlog reader attached, or by a build without `USE_MYSQL`.
- **A dump whose recorded UUID differs from the running server's is refused** (`dump_source_identity.h:70-73`), with the message `dump MySQL source server UUID does not match the running MySQL source`.
- **When the running server's own UUID is unknown the check is skipped** (`dump_source_identity.h:63-65`). For `DUMP LOAD` that is the case with no binlog reader attached (`dump_handler.cpp:517-520`); startup restore reaches the validator only once `GetServerUUID` has succeeded (`server_orchestrator.cpp:539-540`).

## 5. Configuration compatibility on load

The config section records the full server configuration minus credentials (`dump_format_v1_config.cpp:345-514`). The subset that must agree with the running configuration is decided by `FindDumpConfigMismatch` (`dump_config_validator.h:25-62`), which both load paths call first (`server_orchestrator.cpp:557`, `dump_handler.cpp:534`). Any mismatch aborts the load with `kStorageVersionMismatch` (5005) and leaves live table state untouched.

| Value | Rejection condition | Citation |
|---|---|---|
| `memory.verify_text` | Dump records no value **and** the running config is not `off` | `dump_config_validator.h:27-31` |
| `memory.verify_text` | Dump records a value that differs from the running config | `dump_config_validator.h:32-34` |
| `memory.normalize.nfkc` | Differs | `dump_config_validator.h:35-37` |
| `memory.normalize.width` | Differs | `dump_config_validator.h:38-40` |
| `memory.normalize.lower` | Differs | `dump_config_validator.h:41-43` |
| `tables[*].ngram_size` | Differs, for any table named in both the dump and the running config | `dump_config_validator.h:50-52` |
| `tables[*].kanji_ngram_size` | Differs | `dump_config_validator.h:53-55` |
| `tables[*].cross_boundary_ngrams` | Differs | `dump_config_validator.h:56-58` |

A table present in the dump but absent from the running config is skipped by this comparison (`dump_config_validator.h:47-49`), but the table-set check in the format layer rejects the dump anyway (`dump_format_internal.cpp:327-361`).

Beyond `FindDumpConfigMismatch`, both load paths independently require `mysql.host`, `mysql.port` and `mysql.database` to match (`server_orchestrator.cpp:561-567`, `dump_handler.cpp:544-555`), plus the source-UUID and GTID rules in §4.3.

### 5.1 Table entries written before `cross_boundary_ngrams` existed

The config section carries no version of its own, and one field was added to each table entry after the first releases: `cross_boundary_ngrams`, a single byte written between `kanji_ngram_size` and `posting.block_size` (`dump_format_v1_config.cpp:174-178`). A table entry from a release that predates it is one byte shorter.

The decoder tells the two layouts apart by that byte's value (`dump_format_v1_config.cpp:312-332`). It is a serialized bool, so `0` and `1` mean the field is present; any other value is the least significant octet of the `posting.block_size` that follows it in the older layout, and the four-byte value is reassembled from it (`dump_format_v1_config.cpp:205-217`). Where the field is absent, the running default stands rather than a decoded value (`config.h:213`).

The two layouts are therefore distinguishable exactly when the older entry's `posting.block_size` is not congruent to 0 or 1 modulo 256. An older entry that does record such a block size decodes misaligned, and the table entry is refused as it is today; nothing decodes into the wrong field silently, because every later field in the entry — including the length-prefixed `posting.use_roaring` — is then read from shifted bytes and fails its bound. Pinned by `DumpFormatV1Test.ConfigWithoutCrossBoundaryNgramsDecodes`.

The "records no value" case in §5's first row is reachable in two ways. `DeserializeConfig` clears `memory.verify_text` before decoding, as an explicit unknown sentinel (`dump_format_v1_config.cpp:521`); it is then filled in only if a V1 dump sets `flags_v1::kHasCompatibilityMetadata` (`dump_format_v1.cpp:825`) or a V2 dump carries a `kCompatibilityMetadata` section (`dump_format_v2.cpp:1035-1056`). A dump from a release that predates either mechanism therefore loads only into a server running `memory.verify_text = off` — which is the default (`config.h:329`).

Every other value in the config section round-trips into `config::Config` and is returned to the caller (`dump_format_v1.cpp:1059`, `dump_format_v2.cpp:1296`), but is not compared against the running configuration.

## 6. Known divergences

Each item states what a shipped document asserts and what the code does, with citations for both. No remedies are proposed.

**6.1 "Old V1 dumps still loadable" does not reach v1.3.2.** `docs/releases/v1.6.0.md:338` states that V1 dumps remain loadable after the V2 format was introduced. That holds from v1.5.4 onwards, at the container layer (`dump_format_v2.cpp:1363`, `:1371`) and at the server layer (§4.3). It does not hold for v1.3.2, which wrote every posting list's strategy byte, element count and delta values with their octets in the reverse order to `PostingList::Serialize` (`posting_list.cpp:973-1022`). Nothing in the artifact records which order was used, so `PostingList::Deserialize` (`posting_list.cpp:1025`) cannot tell the two apart, and such a dump fails with `kStorageDumpReadError` and the message `LoadFromStream failed for index` (`dump_format_internal.cpp:294`) after its header and its config section have decoded. Pinned by `ReleaseDumpCorpusRejectionTest.DumpWrittenByV132IsRefusedForItsPostingListByteOrder`. The header and config concessions that get such a dump that far are described in §1.2 and §5.1.

**6.2 Forward compatibility of the version ceiling.** `dump_format.h:40` states that `kMaxSupportedVersion` "can support newer versions for forward compatibility". It is set equal to `kCurrentVersion` (`dump_format.h:37`, `:41`), so no version above 2 is accepted by any reader (`dump_format_v2.cpp:1363`, `:1410`, `:1554`). Forward compatibility in V2 is provided at section granularity — unknown `SectionType` values are skipped (`dump_format_v2.cpp:1221-1231`) — not at container-version granularity.

**6.3 Compression and encryption flags.** `dump_format.h:71-74` documents `kCompressed`, `kEncrypted` and `kIncremental` as reserved for future use. No write path sets them and no read path tests them, in either container version (`dump_format_v1.cpp:424-428`, `dump_format_v2.cpp:556-559`). No dump section is compressed or encrypted; lz4 is present in the build only for query-result caching (`src/cache/result_compressor.cpp`).

**6.4 `kWithCRC` described as always set.** `dump_format.h:82` annotates `kWithCRC` as "always set in V1". That is true of what `WriteDumpV1` produces (`dump_format_v1.cpp:424`), and the V1 reader verifies the file CRC unconditionally, ignoring the flag (`dump_format_v1.cpp:770-799`). The V2 reader, by contrast, treats the flag as authoritative and skips file-level CRC verification entirely when it is clear (`dump_format_v2.cpp:943`, and in the verifier `dump_format_v2.cpp:1461`). The same bit therefore has advisory meaning in one container version and no meaning in the other.

**6.5 BM25 and synonym sections.** `docs/releases/v1.6.0.md:20` and `:168-190` describe the V2 format as carrying BM25 and synonym data, and the section types `kTableBM25 = 5` and `kTableSynonyms = 7` plus flags `kHasBM25Data` and `kHasSynonymData` exist (`dump_format.h:98-99`, `:130-131`). `WriteDumpV2` emits neither section and sets neither flag (`dump_format_v2.cpp:568-731`), and `ReadDumpV2`'s dispatch has no case for either type, so both would take the unknown-section skip path (`dump_format_v2.cpp:1016-1231`). BM25 corpus statistics are recomputed from the restored document store after a load instead (`server_orchestrator.cpp:588-600`).

**6.6 Index format version comment.** `index_serialization.cpp:37` labels the version block "Current serialization format version (writes V4 with tokenizer config and CRC32 trailer)". That is accurate for the write side. The read side accepts V1 through V4 (`index_serialization.cpp:316`), and V1 payloads carry no CRC32 trailer at all (`index_serialization.cpp:330`), so an accepted index payload is not guaranteed to be checksummed by its own format — only by the enclosing dump's CRC.

**6.7 Untested accepted versions.** The acceptance policy above is wider than the tested surface. Index payload versions 2 and 3 are accepted with no test constructing such a payload; index version 1 is reached only by tests that assert on rejection (`tests/index/index_serialization_test.cpp:31-71`, `tests/index/index_advanced_test.cpp:562-586`). Document store versions 1 and 2 are covered (`tests/storage/document_store_test.cpp:1373-1450`), as is dump container version 1 (`tests/storage/dump_format_v2_test.cpp:909-960`, `:1439-1450`, `:1718-1735`).
