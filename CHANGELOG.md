<!-- markdownlint-disable MD024 -->
# Changelog

All notable changes to MygramDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note**: For detailed release information, see [docs/releases/](docs/releases/).

## [Unreleased]

## [1.10.0] - 2026-08-09

### Breaking Change

- **Runtime MySQL endpoint changes removed** — `mysql.host` and `mysql.port` are startup-only and `SET` returns an immutable-variable error. Automatic reconnects stay on the configured endpoint and reject a changed source-server UUID; deployments that previously used runtime failover must update configuration and restart MygramDB.
- **Admin token required for non-loopback binds** — `api.admin_token` is mandatory when the TCP bind is not loopback and no Unix socket is configured. Administrative commands are gated behind `AUTH` on the same TCP connection or an HTTP `Bearer` credential; a configuration that previously exposed them unauthenticated on a routable address now fails to start.
- **Fail-closed configuration validation** — a public bind with an open CIDR list, CORS without an allowed origin, a TLS listener without a CA, a missing GTID, and an unresolvable `mysql.user` are all rejected while the configuration is parsed. The built-in schema is validated first, so a custom schema can only add constraints.
- **Docker defaults to localhost** — published API and HTTP ports bind to `127.0.0.1` rather than every host interface, `API_BIND`, `API_HTTP_BIND` and `NETWORK_ALLOW_CIDRS` default to localhost only, and `API_ADMIN_TOKEN` is mandatory with the placeholder value rejected. In-container listeners moved to `API_CONTAINER_BIND` and `API_HTTP_CONTAINER_BIND`.
- **Container password no longer defaults** — the entrypoint fails when `MYSQL_PASSWORD` is unset instead of substituting a placeholder that could only surface later as a replication error.
- **`DUMP SAVE --with-stats` removed** — the flag is no longer part of the grammar.
- **Cache keys rebuilt** — the key serializer prefix changed, so entries built by earlier versions are not reused after upgrade.

### Added

- **Admin authentication** — `AUTH` on TCP and `Bearer` on HTTP gate administrative commands, with constant-time comparison and redaction from request logs. `POST /optimize`, HTTP connection caps, and `X-Forwarded-For` honored only from trusted proxies.
- **Error codes across both protocols** — TCP `ERROR` frames carry a numeric prefix, HTTP error bodies carry `error_code`, and `protocol::ParseErrorFrame` decodes coded and legacy frames through one parser. New codes: `2017` undecodable binlog event, `6028` server loading, `6029` server not ready, `6030` server busy.
- **Readiness surface** — `data_initialized` and readiness in TCP `INFO`, evaluated from the same inputs as the HTTP health endpoint.
- **Startup dump restore** — `dump.load_on_startup` restores `dump.dir/default_filename` before falling back to a MySQL initial snapshot, rejecting a dump whose host, port, database, source server UUID or GTID does not match the configured source.
- **Configuration keys** — `cache.invalidation.max_queue_size`, `dump.load_on_startup`, `api.http.max_connections`, `api.http.trusted_proxies`, and `mysql.ignored_ddl_prefixes`.
- **Installable client SDK** — `MygramDB::client_static` and `client_shared` ship with a CMake package config, version file, pkg-config file, and self-contained headers. The API gains `QueryMode`/`MYGRAM_QUERY_BOOLEAN`, facet pagination with a total distinct value count, expression parse and convert variants returning a diagnostic string, connect and operation timeouts, typed `Error` values parsed from `ERROR` frames, and standalone C and C++ examples.
- **Client deadlines and bounds** — one total command deadline instead of a per-read reset, a response frame cap, rejection of control characters and quotes in commands and identifiers, strict `COUNT` parsing, typed `CacheStatistics`, and `struct_size`-resolved `MygramSearchOptions_C` fields for two-way compatibility.
- **CLI timeouts and typed retries** — `--timeout`, `--connect-timeout` and `--retry-interval`; retry and connection-loss decisions come from the server's error code rather than message substrings. `DUMP SAVE [path]` is parsed locally including quoted paths, and banners, prompts and errors go to stderr so stdout carries only results.
- **Query grammar** — `AUTH`, `SORT BY <column>`, `AND NOT <term>`, and compact `SET name=value`.
- **Dump provenance** — compatibility metadata version 2 records the source server UUID; version 1 metadata still restores.
- **Statistics** — `invalidation_queue_memory_bytes`, text normalization failures, and denial, dump and pool metrics on a monotonic uptime.
- **Fuzzing build** — `FUZZER_ENGINE` selects the standalone driver or libFuzzer, with instrumentation applied to the whole build.

### Fixed

- **Query cache reached neither the replication path nor HTTP** — the cache is created while the TCP server starts, so the binlog reader and HTTP server were both constructed with a null pointer. Row events skipped invalidation entirely, leaving a cached result served after the row behind it changed, and every HTTP request ran outside the cache while caching was reported as disabled.
- **Replication lag read as caught up** — the applied timestamp is stamped where the position advances, so the commit path can no longer advance the position without it.
- **Recovery from an undecodable binlog event** — XA prepare and MariaDB compressed events stay in the binlog after the producing server setting is changed, so `SYNC` restarts from the snapshot marker instead of replaying an interval that can never succeed. A successful start clears the previous run's error rather than leaving a stale diagnostic on the status and health surfaces.
- **Rejected MySQL transport options are fatal** — a client library that rejects an SSL mode, CA, certificate or key fails the connection instead of logging a warning; an unset SSL mode otherwise falls back to `SSL_MODE_PREFERRED`, turning a configuration that demands a verified server into an opportunistic one.
- **Locale-independent input validation** — MariaDB GTID positions and initial-load numeric literals are classified by byte range rather than through `<cctype>`, whose answer depends on the process locale. The filter comparison operator is checked where it becomes SQL, and a bare sign or dot with no digit is rejected.
- **Binlog frame bounds** — `TABLE_MAP` and `ROWS` parsers require the full common header before reading the event size field that lives inside it.
- **Transient and deterministic replication failures separated** — the same-GTID replay-stop budget applies only to deterministic failures, the metadata connection reconnects under backoff, and the failure kind is published atomically so a transient request cannot downgrade a deterministic one.
- **Restore and verification memory bounded** — index sections are checked against `dump.restore_memory_budget_mb` before materialization, `DUMP VERIFY` refuses a section longer than `dump.restore_max_section_mb`, and section CRC is streamed instead of buffered.
- **Filter columns holding only NULL** — per-column reference counts keep such a column visible to `HasColumn` and name resolution.
- **Highlight mapping over grapheme clusters** — the original text is segmented with an ICU character break iterator so combining marks and halfwidth kana dakuten are not normalized in isolation, with a per-codepoint fallback when ICU is unavailable.
- **Cache keys faithful to whitespace** — whitespace is no longer collapsed and trimmed for the key, which let two queries that execution treats differently share one entry.
- **Invalidation queue bounded per entry** — the size limit applies to each pending identity rather than once per batch, its pending and in-flight bytes are charged to the shared cache budget, and the overflow log reports only the identities actually dropped.
- **Cache lifecycle** — LRU maintenance starts only after every eviction callback is installed, a canonical key is required, and statistics are reported even while the cache is disabled.
- **Posting list ownership** — roaring bitmaps are owned through an RAII handle, moved-from lists reset, top-N iterates on stack storage, and publishing an optimized term requires pointer identity as well as a matching version.
- **Startup ordering and paths** — the root privilege check runs before path resolution and log file creation; TLS, Unix socket and synonym paths are absolutized so daemon mode survives a working-directory change.
- **Replication resilience** — reader and worker threads are wrapped so an uncaught exception cannot end the process, replication stops only on DDL that can carry row data, binlog checksums are verified with little-endian reads, and binary JSON output is bounded with cycle detection.
- **Authentication attempts counted** — `AUTH` is recorded in command statistics; the dispatcher returned before the shared counting site, so repeated token guesses did not appear in command traffic at all.
- **Docker sample environment usable end to end** — the sample database grants the replication account `REPLICATION CLIENT`, without which startup failed before the server accepted a connection; the initialization script declares `utf8mb4`, so the multibyte sample rows are no longer stored double-encoded and searchable only by their ASCII text; the compose stack builds its initial index at startup instead of reporting `not_ready` until a client sends `SYNC`; and `NETWORK_ALLOW_CIDRS` covers the Docker bridge range, because a published port presents the gateway address rather than the host's.
- **Missing replication privilege reported directly** — the binary log status query keeps its own permission error instead of retrying a statement that MySQL 8.4 removed, which replaced the cause with a parse error.
- **Benchmark environment matches the shipped defaults** — the benchmark stack publishes its ports on `127.0.0.1`, sets an admin token, and no longer combines a universal allow list with a non-loopback bind, a combination the configuration validator rejects.
- **Error codes name the cause on both surfaces** — the HTTP error helper defaulted its code, so most responses carried `error_code: 1` where the TCP surface named the fault: the same unknown table reported `4007` on TCP and `1` over HTTP. Every HTTP and TCP error site now supplies a code, the default is gone so a new site cannot omit one, and rate limiting, administrative rejection, request-shape faults and internal errors are distinguishable by code.
- **Diagnosable error access** — `Expected::error()` throws instead of aborting, and fuzzy matching scans codepoint windows so a hit inside a continuous CJK word is found.

### Changed

- **Text canonicalization** — UTF-8 sanitization moved inside `CanonicalizeColumnValue`, so snapshot and binlog inputs share one path, and VARCHAR, BLOB and CHAR decoding routes through it.
- **Shared UTF-8 primitives** — one strict decoder backs code point counting, and the fullwidth/halfwidth transliterators are held in thread-local caches instead of being created per call.
- **Replication pause** — an RAII guard owns pause, drain publication and restore for `DUMP SAVE`, `DUMP LOAD` and scheduled snapshots, restarting the reader only for the last releaser.
- **Reactor construction** — `IoReactor` no longer holds the thread pool and dispatcher pointers it never used; the acceptor passes them per connection.
- **`eviction_batch_size` documentation** — describes the minimum, not the maximum, entries evicted per capacity pass.

### Performance

- **Bounded peak memory on the search path** — candidate text is materialized in bounded chunks with the store mutex released for BM25 scoring, term-frequency counting and every text post-filter, instead of copying a large fraction of the corpus up front.
- **Interned filter columns** — per-document filter values are stored against interned column ids in a small sorted vector rather than a hash map plus an owned column-name string per document and column. The dump format is unchanged.
- **Candidate filtering** — a posting list is walked once against a sorted candidate list, so n-gram filtering costs O(candidates + postings) instead of a locked rescan per candidate and term. Roaring cardinality is tracked exactly through point mutations and delta capacity left by removals is released.
- **Cache invalidation sweeps** — dedicated reverse indexes for the filtered and text-sensitive triggers mean a row event visits only the entries it can affect, and periodic maintenance walks a bounded, resumable slice of the LRU list per tick rather than every entry under the exclusive lock.
- **Row image decoding** — only the columns the table configuration reads back are decoded; a non-indexed BLOB was previously materialized once per INSERT or DELETE and twice per UPDATE for a value nothing consumes.
- **Term info reuse** — one deduplicated term-info lookup is threaded through boolean evaluation, NOT filtering, synonym expansion and verification, and the full doc-id scan is skipped when the AST has no NOT node.
- **Statistics aggregation** — per-table statistics are aggregated once into a bounded snapshot served to `INFO`, `/metrics` and Prometheus instead of locking every table per metric.
- **Cache construction** — hash buckets are no longer reserved from `max_memory_bytes`, which allocated hundreds of MiB for a large empty cache.

## [1.9.0] - 2026-07-27

### Breaking Change

- **Legacy bare `SAVE`/`LOAD` rejected** — `SAVE <path>` and `LOAD <path>` return a migration error instead of executing; use `DUMP SAVE` and `DUMP LOAD`.
- **Configured column types and collations validated** — `BINARY`/`VARBINARY`/`BLOB` columns are rejected, and character columns must use a `utf8mb4`, `utf8`/`utf8mb3`, or `ascii` collation. A configuration that previously started (for example one indexing a `latin1` column) now fails closed.
- **Configuration identifiers validated** — table, database, primary key, filter, and text-source names are validated as SQL identifiers while the configuration is parsed.

### Added

- **Cross-surface search parity** — HTTP search/count/facet now treat `q` as literal text by default and accept explicit `"mode": "boolean"` expressions; both transports route through the shared parser. `FACET` executes on the search pipeline with typed unknown-column errors and offset support.
- **Typed search options** — `SearchOptions` and `MygramSearchOptions_C` expose comparison filters, fuzzy distance, highlight tags and limits, and pagination, with the new `mygramclient_search_with_options()` and `mygramclient_convert_search_expression()` entry points.
- **New runtime knobs** — TCP idle-connection reaping (`idle_timeout_sec`, `reaper_interval_sec`), per-connection pending-frame limits (`max_pending_frames`, `max_pending_frame_bytes`), and the `mediumint` filter type.
- **SQL generation helpers** — `QuoteSQLIdentifier`, `QuoteQualifiedSQLIdentifier`, and hex `EncodeMySQLStringLiteral` keep generated statements semantically identical under `NO_BACKSLASH_ESCAPES`.
- **Docker environment surface** — BM25, cache, rate-limit, and TLS variables are exposed; the entrypoint validates typed environment values, quotes generated YAML, and never overwrites an existing bind-mounted configuration file.
- **Build options** — `ENABLE_UBSAN` for UndefinedBehaviorSanitizer builds and opt-in `BUILD_FUZZERS` for a local binlog and dump-header fuzz harness.

### Fixed

- **Binlog value decoding** — MySQL binary JSON, ENUM/SET labels, and negative TIME2 fractional seconds are decoded instead of forwarded as raw bytes, DECIMAL precision/scale metadata is bounded, and a shared value canonicalizer makes binlog TIMESTAMP and DECIMAL text match the snapshot representation.
- **Replication position tracking** — `ReplicationPositionState` separates received from pending-applied GTIDs and fails closed when a COMMIT GTID does not match the pending one; reconnect backoff is cancellable and resets only after an event is fetched from a reopened stream.
- **MariaDB event handling** — standalone transactions, GTID list events, and annotate-rows events are handled explicitly, and the server flavor is detected with a capability probe instead of version-string parsing.
- **Cache key collisions** — each entry stores a canonical query discriminator compared on lookup, so a digest collision fails closed instead of serving another query's result; `FACET` queries are cached in the SEARCH DocId namespace.
- **Cache lifecycle** — the whole Start/Stop and Enable/Disable transition is serialized under the state mutex, invalidation worker thread creation failure is reported, stale entries are evicted after a staleness check, and the configured `eviction_batch_size` is enforced.
- **Reactor EOF coherence** — events queued before EOF or a read pause are discarded, EOF is published and read interest disarmed under the frame mutex, the resume path re-extracts buffered frames before restarting kernel reads, and `EPOLLRDHUP` is armed only together with read interest so epoll matches kqueue `EV_EOF` semantics.
- **Boolean term parsing** — all non-delimiter characters are treated as term characters, so `c++`, e-mail addresses, version strings, and hyphenated terms parse as single terms; the term-count limit is enforced during parsing and a leading `NOT` parses as a boolean expression.
- **Highlight windows** — merged windows are clamped to the documented snippet length, and highlights can be generated from pre-normalization text.
- **Dump load safety** — a `DUMP LOAD` that would wipe a non-empty replication position is refused, and the loaded configuration and GTID are validated together before any live store is replaced.
- **Posting list deserialization** — `PostingList::Deserialize` is overflow-safe and commits strategy and payload only after the full body validates; the V4 index term count is validated against the remaining payload before it is read.
- **Portable primitives** — datetime conversion uses signed epoch seconds with a portable civil-date algorithm preserving valid pre-1970 values, CIDR matching covers IPv6 and IPv4-mapped IPv6 peers, `AF_UNIX` connections report a stable `unix` peer identity, and a failure to open the parent directory during a durability sync is reported instead of silently ignored.
- **Startup signals** — a signal received during initialization or server startup is reported as an orderly shutdown instead of a startup failure, and the rotating log sink appends across restarts and SIGHUP reopen instead of truncating.

### Changed

- **Streaming initial load** — the initial snapshot is fetched with an unbuffered streaming result, checking cancellation between rows; transaction ownership moved to the caller, and primary key, filter, and text values are canonicalized to match the binlog representation.
- **Shared dump internals** — the streambufs, CRC helpers, and pending-section logic duplicated between dump v1 and v2 moved into `dump_format_internal`, with sections read through a bounded stream under a shared `RestoreLimits` budget. The document store keeps pre-normalization text (format v3; v1 and v2 dumps still load).
- **`SHOW VARIABLES` surface** — generated from the configuration projection instead of a hand-maintained map, with knobs that are parsed but not yet enforced labelled as such. `SHOW VARIABLES LIKE` requires exactly one pattern.
- **`CONFIG VERIFY` paths** — resolved against the active configuration file's directory rather than the process working directory.
- **`SYNC` cancellation** — `SYNC STOP` is an asynchronous cancellation, each `SYNC` gets a coherent configuration snapshot, and the shutdown flag no longer lets replication workers restart.
- **Denial logging and listener** — attacker-driven denial logging is bounded on both transports, and the TCP listener binds over IPv6 as well as IPv4.
- **Docker environment names** — the stale `SNAPSHOT_DIR`, `SNAPSHOT_INTERVAL_SEC`, and `SNAPSHOT_RETAIN` entries were removed from the sample environment and compose file; the entrypoint has read `DUMP_DIR`, `DUMP_INTERVAL_SEC`, and `DUMP_RETAIN` since v1.8.1.

### Performance

- **In-place posting mutation** — documents are inserted and removed by patching neighbouring deltas instead of decoding and re-encoding the whole posting list.
- **Reduced index/search contention** — adding to an existing term takes only a shared map lock, so indexing no longer blocks unrelated searches.
- **Automatic Roaring conversion** — a posting list converts to Roaring once it exceeds the entry threshold, and a restored index preserves the configured threshold instead of falling back to the default.

### Testing

- The E2E interpreter is provisioned through a shared `python-env.sh` with rye as the project standard and a lockfile-based venv fallback; the benchmark seed project carries its own lockfiles.
- Added an autouse fixture restoring the tracked table shape and re-syncing after every DDL test, a `verify_text` post-filter regression test with fixtures in both the MySQL and MariaDB init SQL, MariaDB non-transactional GTID and checksum-recovery coverage, and a service-independent unit test directory for the wait helpers.
- Sleeps and magic numbers in the dump, reconnect, memory, and health tests were replaced with polling on observed events, and Makefile targets were added for MySQL-backed tests, the large dump restore test, and the matrix and failover E2E suites.

**Detailed Release Notes**: [docs/releases/v1.9.0.md](docs/releases/v1.9.0.md)

## [1.8.1] - 2026-07-16

### Added

- **C client parity and ABI-safe configuration** — Added typed raw boolean search/highlight functions and the size/versioned `MygramClientConfigV2_C` with Unix-domain-socket and asynchronous DUMP SAVE timeout settings. The legacy C configuration remains ABI-compatible.
- **Boolean filter type** — Filter columns can be declared `boolean`, restricted to equality and NULL operators, materialized identically across the initial snapshot and binlog paths.
- **DDL schema-compatibility enforcement** — The binlog reader fingerprints the configured primary-key, text, and filter columns at startup and revalidates after each DDL on a monitored table; unrelated `ADD COLUMN`/`ADD INDEX` stay compatible, while a drop, rename, or semantic change to a monitored column stops replication before advancing the GTID and requires an explicit `SYNC` or configuration change to recover.
- **V2 dump restore limits** — `dump.restore_memory_budget_mb` and `dump.restore_max_section_mb` bound the memory staged during a V2 restore so a corrupt or malicious dump cannot exhaust memory before the atomic swap.
- **Cache accounting metric** — INFO and Prometheus now expose the shared accounted cache total, including container and invalidation-index overhead.

### Fixed

- **Critical snapshot/binlog position gap** — Initial and shared snapshots capture a conservative GTID lower bound before opening the consistent snapshot, preventing commits from being permanently skipped.
- **Critical SYNC data integrity** — SYNC builds replacement state on the side, swaps only after success, rolls back a failed restart, and resumes the shared reader from its pre-SYNC drained GTID so other tables do not lose events.
- **Critical MariaDB compressed-event handling** — `log_bin_compress=ON` and compressed event IDs 165–168 now fail closed before GTID advancement.
- **Snapshot/binlog materialization parity** — A shared text-source materializer and filter-value converter make the initial snapshot and row-based binlog paths produce identical results (absent/NULL/present state, boolean filters, and TIMESTAMP compared as a UTC epoch); every MySQL session is pinned to UTC, and a configured text column absent from a FULL row image now fails closed.
- **Fail-closed HTTP access control** — The CIDR allow list applies to every non-probe HTTP request, so an empty, omitted, or invalid list denies access instead of allowing all; only `/health/live` and `/health/ready` bypass it, and `/health/detail` now follows the same access control as other endpoints.
- **Reconnect transactionality** — Candidate MySQL connections are validated before publication; failed reader restart restores the old connection and replication position, and reconnect now shares long-operation admission with DUMP/SYNC/snapshots.
- **Reactor and lifecycle bounds** — Read/frame budgets and watermarks prevent single-client starvation, HTTP lifecycle transitions are serialized, reactor shutdown closes every registered connection exactly once, connection registrations are tokenized to prevent reused-FD dispatch, and shutdown drain failures propagate as errors.
- **Process-wide reactor memory admission** — `api.tcp.max_total_buffered_bytes` caps aggregate pending request frames and unsent responses across all clients, preventing per-connection limits from multiplying into unbounded process memory.
- **Cache semantic and ABA correctness** — Cache keys use a versioned, length-prefixed binary encoding that includes execution semantics and synonym revision so search text cannot collide with filter structure; text-sensitive entries invalidate safely; monotonic epochs and entry generations prevent stale deferred work from deleting or reinserting a newer incarnation.
- **Literal reserved-word search and required empty strings** — Quoted parser provenance is preserved across TCP/client paths, and required string filters now distinguish an explicitly empty value from an omitted field.
- **Dump verify_text metadata** — V1/V2 dump compatibility metadata is extended so `verify_text` semantics survive save and load.
- **Filter schema and CIDR validation** — Declaring a column as both a required and optional filter with conflicting types is rejected at load, and invalid IPv4 CIDRs in `network.allow_cidrs` are rejected instead of silently ignored.
- **Dump replacement safety** — `ReplaceLoadedTables` rejects duplicate or null table replacements and propagates the failure instead of applying a partial swap.
- **Configuration activation** — Enabled synonym dictionaries fail startup when absent or invalid, while failed reloads preserve the last published dictionary.

### Changed

- **Long-running operation coordinator** — `SYNC`, `DUMP SAVE`, `DUMP LOAD`, `OPTIMIZE`, and automatic snapshots now pass through a single atomic admission point; each acquires a token and overlapping requests fail with a descriptive error naming the in-progress operation, and shutdown drains the active token before exiting.
- `cache.max_memory_mb` is enforced as a shared budget across cache entries, container/LRU/table indexes, and invalidation metadata.
- `api.tcp.thread_pool_queue_size: 0` now retains its documented unbounded meaning.

### Testing

- Added cross-version E2E coverage (MariaDB compression, real-server failover, DDL, multi-table isolation, protocol/Unicode/connection-stress) on configurable isolated ports, plus C ABI, ctypes, operation-coordinator, reactor-lifecycle, cache-invalidation, and configuration regression tests.

**Detailed Release Notes**: [docs/releases/v1.8.1.md](docs/releases/v1.8.1.md)

## [1.8.0] - 2026-07-03

### Added

- **Date-only string and fractional epoch second support** — `DateTimeToEpoch`/`ParseDatetimeValue` now accept `YYYY-MM-DD` (interpreted as midnight) and truncate `TIMESTAMP2`-style fractional epoch values to whole seconds.
- **Database-qualified required-table validation** — Required-table existence checks now query `INFORMATION_SCHEMA` per table's configured database instead of relying on the connection's default database.

### Fixed

- **Critical: filter-only UPDATE could remove a document from the full-text index** — A binlog UPDATE handler decided index mutation based on whether the after-image text was empty rather than whether it changed, so a filter-only update (e.g. changing `status`) could silently drop a still-qualifying document from search results.
- **Critical: UPDATE changing the primary key left a stale document under the old key** — Primary-key changes on UPDATE are now split into a DELETE for the old key followed by an INSERT for the new key.
- **Critical: reverse search ordering was wrong for partial result sets** — `Index::SearchAnd(reverse=true)` now consistently returns highest-DocID-first regardless of whether the result set was truncated to `limit`.
- **Critical: table-key resolution could route a request to the wrong table** — An exact match on a registered table key now always takes priority over qualified-name fallback scanning.
- **Binlog fail-fast hardening** — CRC32 checksum mismatches, `TABLE_MAP_EVENT` parse failures, missing column metadata, and tagged GTIDs now trigger a reconnect or hard failure instead of continuing on inconsistent state; truncated UPDATE row images are now always propagated as an error.
- **TIMESTAMP/DATE filter values** — Binlog and initial-load filter extraction now decode these through the datetime parser instead of as raw integers.
- **HTTP query construction rebuilt** — `q` is treated as literal search text by default (no reserved-keyword smuggling), with explicit boolean mode available; filter/sort/facet column names resolve case-insensitively while preserving original casing; `SORT _score` is rejected explicitly when normalized text storage is disabled.
- **HTTP reads rejected during table synchronization** — Search/count/facet/get now return `503` while a table is synchronizing; `/health/ready` surfaces `sync_in_progress`.
- **Boolean query AST fixes** — Correct substring fallback for terms too short for n-grams, literal-phrase handling for quoted `AND`/`OR`/`NOT`, the `<>` filter operator, filter/sort column-case preservation, `FUZZY` distance validation, `NOT`-term cache-invalidation registration, and `SearchRaw` boolean-expression transport/parsing for nested `OR` groups.
- **Query-length error message** — HTTP now includes the configured limit value, matching the TCP/parser path.
- **Pipelined requests misclassified as buffer overflow** — The reactor's read-buffer cap now applies only to the unframed tail, not already-extracted complete frames.
- **SYNC/OPTIMIZE/snapshot concurrency guards** — `SYNC` is rejected while `OPTIMIZE`/`DUMP` is in progress; scheduled snapshots skip during `OPTIMIZE`; orphaned temp snapshot files are cleaned up.
- **Daemon-mode relative paths** — `dump.dir`/`logging.file` are absolutized before daemonizing, fixing paths broken by the post-fork `chdir("/")`.
- **V2 dump GTID length** — Now bounded by a dedicated 64 KB limit instead of the unrelated V1 path-length limit.
- **CLI DOC output** — Escape sequences (`\n`, `\r`, `\t`, `\\`, `\"`, `\xHH`) are now fully decoded instead of passed through verbatim; exit-code detection no longer misfires on successful payloads containing disconnect-related wording.
- **Client socket teardown and C API** — Socket state is consistently torn down on every send/receive failure; `FACET` no longer drops rows whose value starts with `#`; C API clears last-error on success and null-checks allocations.

### Changed

- **Default `ngram_size` changed from `1` to `2` (bigram)** when omitted from configuration.
- `ENUM`/`SET` filter types are now explicitly rejected instead of silently accepted.
- `Expected<T, E>`/`Expected<void, E>` are now `[[nodiscard]]`.

### Testing

- Regression tests added for every fix above; full unit suite (3722 tests, incl. SLOW/LOAD) and full Docker E2E suite (200 tests) pass cleanly.

**Detailed Release Notes**: [docs/releases/v1.8.0.md](docs/releases/v1.8.0.md)

## [1.7.0] - 2026-06-15

### Added

- **Database-qualified table identity** — Tables now carry a `(database, name)` identity written as `app_db.articles`. The effective database is `tables[*].database` when set, otherwise `mysql.database`. A single instance can index same-named tables from different databases (e.g. `live_db.articles` and `archive_db.articles`).
- **Dump metadata preserves per-table databases** — New V1/V2 dump paths retain table-level database names so multi-database identities round-trip without one table overwriting another.

### Changed

- **Table references accept an optional database qualifier** — TCP/CLI/C++/C API table arguments and HTTP routes accept `database.table` (e.g. `app_db.articles`). **In single-database deployments, bare names such as `articles` continue to work unchanged** — they resolve to the only configured database. A qualifier becomes **required only when the configuration spans two or more databases**, where a bare (ambiguous) name is rejected with a clear error.

### Breaking Change

- **HTTP table routes restructured** — Table routes are now `POST /tables/{identity}/search`, `POST /tables/{identity}/count`, `POST /tables/{identity}/facet`, and `GET /tables/{identity}/{primary_key}`, where `{identity}` is `database.table` or, in single-database deployments, a bare `table`. The previous `/{table}/...` routes are removed; HTTP clients must move to the `/tables/...` paths. (TCP, CLI, and client-library bare-name access is unchanged for single-database deployments.)
- **Multi-database configurations require qualified table references** — When two or more databases are configured, every TCP/CLI/C++/C API/HTTP table reference must use `database.table`.

Migration:

1. HTTP clients: change request paths from `/{table}/...` to `/tables/{table}/...` (single database) or `/tables/{database}.{table}/...`.
2. Single-database TCP/CLI/client-library users: no change required — bare table names keep working.
3. Multi-database users: qualify every table reference as `<database>.<table>`.
4. Keep old dumps for rollback, then create a fresh dump after upgrading so restored metadata includes qualified table identities.

**Detailed Release Notes**: [docs/releases/v1.7.0.md](docs/releases/v1.7.0.md)

## [1.6.1] - 2026-05-07

### Added

- **HTTP JSON search API: sort, fuzzy, highlight** — `/search` accepts `{"sort": {...}}`, `{"fuzzy": 1|2}`, and `{"highlight": {...}}` matching the TCP protocol; highlight tags capped at 256 bytes; synonym expansion applied before snippet generation
- **Network hardening** — `TCP_NODELAY` on accepted sockets, configurable HTTP body size (`api.http.max_body_bytes`, returns 413 for oversize), idle-connection reaper, `api.http.read_timeout_sec`/`write_timeout_sec`
- **Shared RateLimiter** — TcpServer and HttpServer share one limiter so a client cannot get 2x the quota by spreading load across protocols
- **Synonym dictionary diagnostics** — Startup `synonym_variant_unreachable` warning for terms shorter than `ngram_size`/`kanji_ngram_size`; loader emits `synonym_group_collapsed` and `synonym_group_term_conflict` events with raw token preview and source line number
- **`utils::PeriodicWorker`** — Generic background-thread helper; migrated `RateLimiter::SweeperLoop` and `QueryCache::RefreshLRUWorker`
- **`replication_pause::Scope`** — Move-only RAII wrapper around process-wide replication-pause counter
- **`utils::OperationGuard::TryAcquire()`** — Atomic test-and-set + RAII release with `Release()` / `Dismiss()` semantics
- **`utils::ResolveSafePath`** — Unified path validation API
- **New error codes** — `kTableNotFound` (4007), `kCatalogNotInitialized` (4008), `kNetworkAcceptorNoHandler` (6025), `kServerShuttingDown` (6027)

### Fixed

- **DUMP LOAD always restores replication** — Filepath validation runs before binlog stop; ScopeGuard ensures replication restart and flag clearance on every error path (P0-A)
- **HttpServer::Start race** — `compare_exchange_strong` replaces check-then-set; eliminates double-spawn and skipped-join (P0-C)
- **SyncOperationManager three-phase StartSync** — Slot claimed unconditionally during validate-and-claim phase; closes the window where a fresh burst all reached spawn (P0-D)
- **SnapshotScheduler lifecycle** — `start_stop_mutex_` serializes Start/Stop; `replication_paused_for_dump` flag pulses around `WriteDump`
- **TcpServer four-phase Stop** — Set `shutdown_in_progress_` → join dump worker / stop sync manager / scheduler → stop reactor / acceptor → drain thread pool last
- **Cache lock-order deadlock** — `RemoveEntryLocked` defers eviction callbacks; `FireEvictionCallbacks()` runs after releasing `mutex_`
- **Cache phantom metadata** — `CacheManager::Clear`/`ClearTable` serialize via `mutex_` (P0-B); `QueryCache::Clear` invokes `eviction_callback_` for every entry (P0-G)
- **Cache double-unregister race** — `EraseWithoutCallback` used in `InvalidationQueue::ProcessBatch` to prevent corruption of `table_to_cache_keys_` and `ngram_to_cache_keys_`
- **Reactor stranded entries** — `IoReactor::Register` holds `mux_lifecycle_` (shared) and `connections_mutex_` (unique) across running check, map insert, and mux Add
- **ReactorConnection close race** — `OnReadable` holds `frame_mutex_` and `write_mutex_` for atomic empty-and-eof close transition
- **DrainTask invariant** — `drain_scheduled_` kept true while task decides to reschedule
- **IoReactor Start/Stop slot race** — `start_stop_mutex_` serializes both phases; documented lock order
- **KqueueMultiplexer interest race** — `interest_mutex_` held across kevent syscall and map update; per-filter serialization in `Add`/`Modify`/`Remove`
- **ConnectionAcceptor `server_fd_` data race** — Promoted to `std::atomic<int>` with exchange-on-Stop
- **ConnectionAcceptor `reactor_handler_` data race** — `Start()` split into `Start()` (bind/listen) and `StartAccepting()` (spawn thread)
- **DumpProgress::StartWorker data race** — `worker_thread` assignment moved inside `DumpProgress::mutex`
- **PeriodicWorker post-unlock recheck** — `should_stop_.load(acquire)` after unlock prevents one extra callback after Stop
- **HttpServer::Start join-deadlock** — Promise/future startup handshake removed; `bind_to_port` synchronous on calling thread
- **IoReactor wakeup** — `EventMultiplexer::Wake()` (epoll: eventfd, kqueue: EVFILT_USER) wakes sleeping `Poll()` immediately on Stop
- **ConnectionAcceptor EMFILE backoff** — `condition_variable::wait_for` instead of `sleep_for`; Stop returns in microseconds
- **HTTP search input validation** — `IsValidTableName` and `ValidateQueryTextNoReservedClauses` reject smuggled clauses (LIMIT/OFFSET/etc.) and unsafe table names with HTTP 400
- **HTTP search raw error response** — Sort/pagination failures now route through `ResponseFormatter::FormatError` for the standard ERROR prefix
- **HTTP `/health/*` not counted in `total_requests`** — Probes no longer distort QPS metrics (H-N7)
- **SYNC_STOP registration** — `SYNC_STOP` was missing from `InitDispatcher`, causing `"Unknown query type"`; fail-fast handler-table validation added
- **`sync_mutex_` released before `join()`** — `StartSync` switched to `unique_lock` to avoid deadlock with `BuildSnapshotAsync`'s terminal `update_state` lambda
- **CacheManager Disable/Enable order** — `enabled_=false` set before `Clear`; queue started before `enabled_=true`
- **InvalidationQueue restart** — `Start()` resets `stopped_=false` so Stop/Start cycles do not silently drop Enqueues
- **Decompression failure dedup** — `LookupInternal` dedupes via pending-keys set (P0-E)
- **MygramClient correctness (8 categories)** — DEBUG response parsing (colon-vs-equals), GetReplicationStatus parsing, INFO key mapping (`active_connections`/`index_size_bytes`), connect timeout (poll-based `ConnectWithTimeout`), identifier validation, OFFSET-only emission, mutex serialization (`Impl` was missing one despite thread-safe header), C API NULL array guard
- **mygram-cli rewrite on MygramClient** — Hostname resolution (getaddrinfo replaces inet_pton), response truncation (full-response loop), REPLICATION CRLF, port range validation, SIGPIPE handling, whitespace arg quoting; ~500 LOC duplicate implementation removed
- **`SimplifySearchExpression`** — OR-only and parenthesized expressions now produce valid `main_term` instead of returning false
- **Protocol detection END markers** — `CACHE_STATS`, `DUMP_INFO`, `DUMP_STATUS` recognized so client no longer hangs until socket timeout
- **CLI bogus DEBUG OPTIMIZE completion** — Removed; OPTIMIZE is a top-level command
- **TOCTOU on dump-save in-progress** — `compare_exchange_strong` replaces load+store(true) in `HandleDumpSave`/`HandleDumpLoad`
- **`replication_paused_for_dump` reference counter** — Process-wide atomic; first-pauser stops binlog, last-releaser starts; migrated DumpSaveWorker, HandleDumpLoad, SnapshotScheduler::TakeSnapshot
- **CONFIG VERIFY symlink TOCTOU** — `O_NOFOLLOW` probe narrows the window between symlink check and `LoadConfig`
- **CacheKey hash collisions** — Switched from XOR to Fibonacci-mixed step
- **HighlightTag size limit** — Reject `open_tag`/`close_tag` longer than 256 bytes with HTTP 400
- **`IsSafeJsonColumnName`** — Use unsigned-char comparison for `$`; remove redundant `isspace`/`iscntrl` guards already covered by ascii_safe whitelist

### Performance

- **Cache memory accuracy** — `kSharedPtrControlBlockOverhead` (24 B) + `kHashMapNodeOverhead` (32 B) added to memory accounting; addresses ~5–10% RSS under-report
- **Batch eviction callback** — `Clear`/`ClearTable`/`EvictForSpace`/`RefreshLRU` take `InvalidationManager::mutex_` once per bulk operation instead of once per key (H-M7)
- **`UnregisterCacheEntries(vector)`** — Single mutex acquisition for batch unregister
- **`filter_columns_changed` O(k)** — `InvalidateAffectedEntries` uses `table_to_cache_keys_` reverse index instead of O(N) walk (H-M2)
- **`InvalidationManager::ClearTable` O(k)** — Reverse index instead of full metadata scan
- **PendingKey typed pair** — Removes per-event O(k) hex-string allocation on the invalidation hot path
- **`RateLimiter` background sweep** — Dedicated thread eliminates O(n) latency spikes on the request hot path
- **`ThreadPool` shutdown** — Condition variable instead of 10ms sleep polling

### Changed

- **Unified HTTP/TCP search pipeline** — `ExecuteSearchPipeline` (290 LOC) split into a 53-line orchestrator plus four small helpers; both protocols route through `search_pipeline::ExecuteFullPipeline`
- **`FacetHandler`** routes through `ExecuteFullPipeline` so synonym/fuzzy/cache apply identically to facet-scoped searches
- **`PrepareHttpSearchQuery`** — Extracts ~100-line shared preamble between HandleSearch and HandleCount
- **`CommandHandler::CheckNotLoading()`** — Single-line replacement for open-coded loading checks
- **`HttpServer::ResolveHttpTableContext`** — Consolidates table-name validation + lookup + null-check
- **Structured log event names** — 60+ sites normalized to `<module>_<verb>_<outcome>`; `server_error` / `server_warning` catch-all events replaced with dedicated names
- **`log_field_names.h`** — Canonical field-name constants (`kFieldFilepath`, `kFieldFd`, `kFieldClientIp`, etc.)
- **`StructuredLog::FieldError(const Error&)`** — Emits message and error_code together; applied across dump/admin/io_reactor/sync_operation_manager
- **`ResponseFormatter::FormatOk` / `FormatStatus`** — Replace hand-rolled `+OK` / `OK ...` literals across handlers
- **`Expected<void, Error>` for `IBinlogReader::Start()`** — Replaces bool + `GetLastError()` at four call sites
- **`reactor_poll_failed`** promoted from Warn to Error
- **`config_verify_failed`** downgraded from Error to Warn (client-input mistake)
- **Request log truncation** — `RequestDispatcher` truncates request field at `kMaxQueryLogLength`; emits separate `request_full_length` to bound log volume
- **Build system** — `mygramdb_runtime_config` extracted to break circular dep; consolidated MySQL detection; portable `NPROC` for macOS
- **TableCatalog** — Mutex removed (immutable post-construction); const overload added

### Removed

- Dead code: `FormatConfigResponse`, `kOkInfoPrefixLen`/`kOkReplicationPrefixLen`, `TcpServer::StartSync`/`GetSyncStatus`, `TcpServer::shutdown_requested_`, `kDefaultConnectionRecvTimeoutSec`, `kSyncPollIntervalMs`, pointless try/catch wrappers around `make_unique` in `ServerLifecycleManager::Init*` and `RequestDispatcher::Dispatch`

### Testing

- E2E test ports shifted: MySQL `13306` → `23306`, HTTP `18080` → `20080` to avoid conflicts
- New test files: `socket_utils_test`, `periodic_worker_test` (SLOW), `replication_pause_counter_test`, `binlog_reader_stop_contract_test`, `search_pipeline_synonym_jp_test`, `roaring_bitmap_ptr_test`, `facet_handler_test`, `cache_handler_test`, `admin_handler_test`, `tcp_server_lifecycle_test`
- ~50 new/rewritten cases in `mygram_cli_test.cpp` and `mygramclient_test.cpp`

**Detailed Release Notes**: [docs/releases/v1.6.1.md](docs/releases/v1.6.1.md)

## [1.6.0] - 2026-04-15

### Added

- **BM25 relevance scoring** — `SORT _score` ranks results by term frequency and document length using BM25 ranking function (k1=1.2, b=0.75)
- **Synonym dictionary** — Automatic OR-group search expansion from TSV synonym files; configure via `synonyms.enable` and `synonyms.file`
- **HIGHLIGHT clause** — Snippet generation with configurable tags (`TAG`), snippet length (`SNIPPET_LEN`), and fragment count (`MAX_FRAGMENTS`)
- **Fuzzy search** — `FUZZY [1|2]` clause for Levenshtein edit distance matching
- **FACET command** — Filter column value aggregation with document counts, optionally scoped to search results
- **V2 dump format** — Section envelope with per-section CRC32, forward compatibility for unknown section types; auto-detected on `DUMP LOAD`
- **MariaDB binlog replication** — Full support for MariaDB 10.6+/11.x with MariaDB-native GTID format (`domain-server-sequence`); auto-detected from server version
- **E2E test matrix** — MariaDB support in e2e test suite with matrix runner (MySQL 8.4, 9.4, MariaDB 10.11, 11.4)

### Fixed

- **DUMP LOAD premature guard release** — `dump_load_in_progress` flag now stays active through replication restart and BM25 cache rebuild; released only after the entire success path completes
- **BM25 cache hit out-of-bounds read** — Regenerate `term_infos` before BM25 scoring when the cache-hit path skips `GenerateTermInfos()`
- **HIGHLIGHT rejected when text storage disabled** — Explicit validation when `verify_text` is `"off"`
- **Text storage not disabled when verify_text is off** — Prevents wasted memory
- **Missing `<condition_variable>` include** — `query_cache.h` no longer relies on transitive inclusion
- **Security hardening** — Password masking, SQL injection prevention, path traversal fixes, null byte injection rejection
- **Thread safety** — Atomic conversions for cache/document store fields, mutex protection for binlog reader, TOCTOU fixes

### Changed

- **Search pipeline consolidation** — Extracted `GenerateTermInfos()`, `Execute()`, and `SearchPipelineResult` into reusable pipeline components
- **Error propagation** — `DecodeFieldValue` and GTID parsing use `Expected<T, Error>` instead of sentinel strings
- **Handler consolidation** — Clear ownership boundaries and layer decoupling across handlers

### Testing

- New test files: `bm25_scorer_test`, `highlighter_test`, `synonym_dictionary_test`, `edit_distance_test`, `filter_index_facet_test`, `dump_format_v2_test`, `mariadb_gtid_test`, `mariadb_event_parser_test`, `search_pipeline_test`, `bm25_sort_test`, `index_serialization_test`
- E2E: Stale process cleanup, MariaDB replication timing race fix
- Full unit and end-to-end matrix across supported MySQL and MariaDB versions

**Detailed Release Notes**: [docs/releases/v1.6.0.md](docs/releases/v1.6.0.md)

## [1.5.4] - 2026-04-13

### Added

- **CRC32 binlog checksum verification** — Each received binlog event is verified against its trailing CRC32 checksum; corrupted events are logged and skipped with an observable `crc_errors_` counter
- **New error codes** — `kMySQLFieldTruncated` (2013), `kMySQLInvalidMetadata` (2014), `kMySQLUnsupportedType` (2015), `kMySQLBinlogChecksumMismatch` (2016), `kSyncThreadCreationFailed` (4013)
- **comparison_utils.h** — Generic `CompareValues<T>`, `CompareDoubleValues`, `CompareDoubleValuesRelative` helpers
- **protocol_constants.h** — Shared TCP protocol constants replacing parallel local definitions
- **FilterMap type alias** — `absl::flat_hash_map` with transparent hash for heterogeneous lookup throughout document store, filter index, and binlog modules

### Fixed

- **Use-after-free in FilterIndex::GetEqBitmap** — Returns a copy instead of a raw pointer into internal storage, closing a race where concurrent writers could free the bitmap
- **Deadlock in SyncOperationManager::StopSync** — Joins background threads after releasing `sync_mutex_` instead of while holding it
- **SYNC data race** — Stop replication before clearing index/doc_store during SYNC; invalidate search cache after rebuild
- **CRC32 stripping in QUERY_EVENT** — Strip 4-byte checksum from effective length to avoid parsing garbage bytes
- **NULL column parsing** — Skip buffer bounds check for NULL columns that consume no bytes
- **Memory tracking underflow** — Cache entries record heap footprint at insertion time to prevent `total_memory_bytes_` going negative
- **Filter-column invalidation bypass** — Always invalidate filter-bearing entries when filter columns change, even when text also changed
- **Partial network I/O** — Handle partial send/recv in CLI and client with proper loops
- **PostingList serialization endianness** — Use little-endian helpers; use safe deserialization for Roaring bitmaps
- **Exponential backtracking in LIKE** — Replace with O(n*m) dynamic programming
- **Security hardening** — Password masking in SHOW VARIABLES, SQL injection prevention, path traversal fix, port range validation, null byte injection rejection
- **Thread safety** — Atomic conversions for cache fields, mutex protection for `last_error_`, TOCTOU fixes in SnapshotScheduler, atomic ResetStats in RateLimiter, atomic `store_texts_` in DocumentStore
- **RPM packaging** — Remove Oracle MySQL-specific dependency for AlmaLinux compatibility; stop auto-restart on upgrade

### Changed

- **8 large files split into focused modules** — All `.cpp` files over 1000 lines decomposed into logically grouped translation units (dump_format_v1, document_store, rows_parser, binlog_reader, query_parser, config, http_server, index)
- **Sentinel strings replaced with Expected errors** — `DecodeFieldValue` returns `Expected<std::string, Error>` instead of sentinel strings
- **Sync error codes relocated** — `kSyncTableNotFound`/`kSyncAlreadyInProgress`/`kSyncMemoryCritical` moved from network range (6030–6032) to business-logic range (4010–4012)
- **CacheManager::Insert()** — ngrams parameter changed from `std::set` to `std::vector`
- **SyncOperationManager::StartSync()** — Return type changed from `std::string` to `Expected<std::string, Error>`
- **PostingList performance** — O(1) monotonic add fast-path, cached `last_doc_id_`, `lower_bound` in Remove, lock-free `SizeApprox`/`MemoryUsageApprox`
- **Index performance** — Heterogeneous lookup, reused temp vectors in SearchOr/SearchNot, `absl::flat_hash_map` in batch/snapshot paths
- **Search pipeline** — Batch filter/PK lookups, sorted set_union for ngrams
- **RateLimiter cleanup** — Time-based instead of request-count-based to eliminate latency spikes
- **Dead code removal** — `ProcessRow`, `CleanupOldClients`, `Index::SearchOrInternal`; `DumpManager` marked `[[deprecated]]`

### Testing

- New test files: `binlog_crc32_test`, `comparison_utils_test`, `config_security_test`, `connection_acceptor_tcp_test`, `search_pipeline_test`, `atomic_file_writer_test`, `dump_format_v1_bounds_test`, `posting_list_serialization_test`, `invalidation_manager_test`, `cache_thread_safety_test`, `flag_guard_test`, `network_utils_test`
- Expanded coverage for filter_index, rate_limiter, query_parser, document_store, index, posting_list, rows_parser, cache_manager, config, error codes

**Detailed Release Notes**: [docs/releases/v1.5.4.md](docs/releases/v1.5.4.md)

## [1.5.3] - 2026-04-12

### Added

- **Reactor I/O model (epoll/kqueue)** — New event-driven TCP path replaces the blocking one-thread-per-connection loop; a single event-loop thread plus a bounded worker pool serves thousands of persistent connections
- **Per-connection slow-reader backpressure** — `api.tcp.max_write_queue_bytes` (default 16 MiB) force-closes clients whose enqueued response bytes exceed the cap
- **Reactor error codes** — `kNetworkReactorUnsupported`/`PollFailed`/`RegisterFailed`/`ModifyFailed`/`RemoveFailed`/`QueueFull`/`AlreadyOpen` (6016–6023)

### Fixed

- **TCP half-close drain regression** — `shutdown(SHUT_WR)` + `recv()` clients now receive their response; `kHangup` events no longer short-circuit to `OnError`, and `read_eof_` is tracked separately from `closing_` so the drain task can enqueue the response
- **Rate limiting silently disabled under reactor** — `api.rate_limiting.enable = true` is now honored on every accepted connection; the reactor handler calls `getpeername()` + `AllowRequest()` before `Register()` and returns `SERVER_BUSY` on rejection
- **Unix domain socket acceptor could not start** — Removed the dead secondary `unix_acceptor_` that collided with the primary acceptor's own UDS bind; UDS now flows end-to-end through the primary acceptor's reactor handler
- **Grafana memory usage PromQL** — Use `ignoring(type)` on the division so `mygramdb_memory_used_bytes{type="total"}` matches the denominator label set

### Changed

- **Blocking I/O path removed entirely** — `ConnectionIOHandler`, `TcpServer::HandleConnection`, the `api.tcp.io_model` feature flag, `connection_contexts_` map, `ConnectionAcceptor::SetConnectionHandler`, and the `BlockingMode` ctest entries are all deleted
- **Thread-pool auto-size floor reverted** — Dropped the emergency `hw*4`/64-worker mitigation for blocking-mode starvation; restored `max(hw*2, 4)`
- **Reactor hot-path polish** — epoll/kqueue poll buffers grow on demand up to 4 KiB entries; `Register`/`Stop` race closed by re-checking `running_` under `mux_lifecycle_` shared; `OnWritable` empty-queue teardown flattened

### Testing

- New unit tests: `event_multiplexer_test`, `io_reactor_test`, `reactor_connection_test`
- New integration tests: `reactor_integration_test` (write backpressure, many-idle-connections, half-close, rate limit, UDS, max query length), `reactor_starvation_regression_test`, `thread_pool_saturation_test` (migrated, assertion inverted for reactor default)
- e2e `test_half_close_write` now passes (previously failing on reactor path)

**Detailed Release Notes**: [docs/releases/v1.5.3.md](docs/releases/v1.5.3.md)

## [1.5.2] - 2026-04-09

### Added

- **MySQL 9.x compatibility** — Support for `MYSQL_TYPE_VECTOR` (type 242) in binlog parser; tables with VECTOR columns replicate without errors
- **MySQL version-switchable e2e tests** — `./e2e/run-all.sh --mysql-version 9.6` to run tests against different MySQL versions
- **VECTOR replication e2e tests** — INSERT/UPDATE/DELETE/batch scenarios for tables with VECTOR columns (MySQL 9.x only)
- **VECTOR unit tests** — `calc_field_size()`, TABLE_MAP metadata parsing, and row data decoding for VECTOR type

### Fixed

- **MySQL 8.4+ authentication** — Added `MYSQL_OPT_GET_SERVER_PUBLIC_KEY` for `caching_sha2_password` without SSL, fixing connection failures on MySQL 8.4+ and 9.x
- **e2e Docker compatibility** — Removed `--binlog-format=ROW` and `--mysql-native-password=ON` options that are deprecated/removed in MySQL 9.x
- **Flaky truncate memory test** — Increased timeout and added `sync()` before truncate to reduce intermittent failures

## [1.5.1] - 2026-04-01

### Added

- **DEB package support** — Ubuntu 22.04 (Jammy) and 24.04 (Noble) packages with systemd integration, user creation, and proper directory layout
- **EL10 RPM support** — RHEL/AlmaLinux/Rocky Linux 10 packages alongside existing EL9; parametric Dockerfile with `EL_VERSION` build argument
- **Package verification test suite** — Automated install, startup, health check, and search validation across all target distros (`support/testing/test-pkg-verify.sh`)
- **Multi-distro CI release pipeline** — GitHub Releases now publish 8 packages (EL9/EL10 RPM + Jammy/Noble DEB x 2 architectures); filtered to main packages only

### Fixed

- **SIGILL on non-build CPUs** — Added `MYGRAMDB_PORTABLE_BUILD=ON` to Docker build to prevent illegal instruction crashes
- **Coverage target shell escaping** — Added `VERBATIM` for correct argument escaping
- **Sanitizer CI configuration** — Switched to manual-only triggers, fixed label-exclude pipe escaping
- **Bench compose normalization** — Enabled `MEMORY_NORMALIZE_LOWER` for consistent search behavior

### Changed

- Upgraded third-party dependencies and hardened CI pipeline security
- Excluded LOAD tests from default CI runs; labeled query parser perf tests as SLOW
- Release artifacts filtered to main packages only (no debuginfo/debugsource/src)

### Code Quality

- Applied `clang-format` (Google style) across source and test files

**Detailed Release Notes**: [docs/releases/v1.5.1.md](docs/releases/v1.5.1.md)

## [1.5.0] - 2026-03-23

### Added

- **verify_text post-filter** — Eliminates n-gram false positives by verifying matches against original document text; configurable globally or per-table (`memory.verify_text: on`)
- **Docker benchmark environment** — One-command setup (`make bench-up`) with 1.1M Wikipedia dataset for MygramDB vs MySQL FULLTEXT comparison
- **Atomic file writer** — Crash-safe snapshot writes using write-to-temp + atomic rename
- **Search pipeline extraction** — Composable search pipeline for cleaner feature insertion

### Changed

- **Namespace rename** — `mygramdb::utils` → `mygram::utils` across all source and test files (internal only, no public API change)
- **MySQL 8.0+ required** — Dropped MySQL 5.7 support

### Code Quality

- Applied `clang-format` (Google style) across all source and test files

**Detailed Release Notes**: [docs/releases/v1.5.0.md](docs/releases/v1.5.0.md)

## [1.4.0] - 2026-03-16

### Added

- **Unix domain socket support** - Local connections via Unix sockets for reduced latency and improved security; supported in server, CLI, and client library
- **Prometheus cache metrics** - Full cache observability with hit/miss rates, memory accounting, eviction stats, and TTL expiration tracking via `/metrics` endpoint
- **Structured benchmark suite** - Python-based benchmark framework with anomaly detection, MySQL comparison reports, and connection pool saturation analysis
- **Python-based end-to-end test suite** - Comprehensive e2e tests covering replication, DDL, cache, concurrency, resilience, memory, and multi-table scenarios

### Fixed

- **Critical: Cache correctness and data integrity** - Resolve invalidation consistency, double-counting stats, query normalization, and result sorter edge cases
- **Critical: Binlog reconnection and GTID consistency** - Fix reconnection failures, GTID snapshot consistency, and reconnection gap causing event loss
- **High: FilterIndex thread safety** - Make FilterIndex thread-safe and eliminate bitmap filter use-after-free crashes
- **High: PostingList deadlocks** - Fix deadlocks in PostingList operations and reduce SaveToStream lock contention
- **High: Document resurrection on Optimize race** - Fix race condition where removed documents reappear during Optimize
- **High: Stale connection recovery** - Add automatic recovery for stale MySQL connections in binlog reader
- **High: Thread safety across components** - Fix thread safety issues in connection handling, config propagation, and multiple modules
- **High: GTID set handling for reconnection** - Fix GTID set handling and migrate GtidEncoder to Expected<T, Error>
- **High: V2 rows event parsing** - Fix V2 rows event parsing issues with MySQL 8.4 support
- **Medium: Cache decompression failure leak** - Fix memory leak on decompression failures and add invalidation queue backpressure
- **Medium: Security hardening** - Harden HTTP server and dump operations, fix GTID single-to-range conversion
- **Medium: Cache hit counting** - Fix inaccurate cache hit counting and add TTL expiration statistics

### Performance

- **Bitmap-based filter index** - Roaring bitmap-based FilterIndex for efficient filter evaluation replacing per-document checks
- **Batch sort key lookups** - Reduce lock acquisitions during result sorting

### Testing

- Python-based e2e integration test suite (replication, DDL, cache, concurrency, resilience, memory, multi-table, edge cases)
- Consolidated bug-fix tests into main test suites
- Refactored C++ MySQL tests into unit tests with expanded coverage
- New tests: FilterIndex, Unix socket, cache metrics, optimize concurrency

**Detailed Release Notes**: [docs/releases/v1.4.0.md](docs/releases/v1.4.0.md)

## [1.3.9] - 2026-01-10

### Fixed

- **Critical: Binlog replication fixes** - Thread premature termination, multi-row events, GTID updates
- **High: Memory management** - RAII Roaring iterator, PostingList cleanup, N-gram eviction
- **High: GTID and concurrency** - Race conditions in GTID handling, transaction ID overflow, deadlocks
- **High: Configuration security** - Path traversal prevention, symlink vulnerability fix, TOCTOU race
- **Critical: SYNC instance replacement** - Fix broken replication after SYNC cancellation
- **Medium: Query and search** - Zero-division guard, total_results calculation, filter operators

### Added

- GEOMETRY type support in binlog replication
- ROW_V1, ROTATE_EVENT, HEARTBEAT_EVENT handling
- binlog_row_image MINIMAL/NOBLOB support
- DECIMAL precision handling
- Environment variable credentials
- Strong DocId typing
- BinlogEvent factory pattern

### Performance

- QueryCache lock optimization
- Heterogeneous lookup for Index and DocumentStore
- Response string concatenation improvements
- ApplyFilters variant overhead reduction

### Testing

- 12 new test files across cache, index, mysql, server, and storage modules

**Detailed Release Notes**: [docs/releases/v1.3.9.md](docs/releases/v1.3.9.md)

## [1.3.8] - 2025-12-21

### Added

- **C API: mygramclient_send_command** - Generic command sending function for arbitrary command execution; useful for custom commands and future protocol extensions

### Fixed

- **Medium: TCP protocol CRLF line endings** - Fix inconsistent line endings in multi-line responses (CONFIG HELP/SHOW/VERIFY, SYNC STATUS) that could cause client timeouts; now consistently uses CRLF (`\r\n`) per TCP text protocol conventions

### Testing

- 8 new tests for C API send_command and CRLF line ending compliance

**Detailed Release Notes**: [docs/releases/v1.3.8.md](docs/releases/v1.3.8.md)

## [1.3.7] - 2025-12-02

### Added

- **Async DUMP SAVE** - Non-blocking dump operations; returns immediately with `OK DUMP_STARTED <filepath>` while dump runs in background
- **DUMP STATUS command** - Real-time progress monitoring for dump operations with status, tables processed, elapsed time, and error reporting

### Fixed

- **Critical: Use-After-Free in RemoveDocument** - Copy primary key string before erasing map entry; add stress tests with SLOW label for regression detection
- **Critical: SIGSEGV on SET/SHOW VARIABLES** - Transfer VariableHandler ownership to TcpServer to prevent use-after-free crash
- **Medium: auto-dump/manual dump conflict** - Add mutual exclusion between SnapshotScheduler and manual DUMP SAVE operations
- **Low: DUMP LOAD GTID restoration** - Restore GTID even when replication was not running, enabling manual REPLICATION START after load
- **Low: BinlogReader::IsRunning()** - Check both running_ and should_stop_ flags; reset should_stop_ in Stop() for proper restart

### Refactoring

- **Structured logging migration** - Convert spdlog calls to StructuredLog format across 25+ files
- **IBinlogReader interface** - Introduce interface for better testability; handlers use interface instead of concrete class
- **Flag renaming** - read_only_ -> dump_save_in_progress_, loading_ -> dump_load_in_progress_ for clarity

### Documentation

- Document async DUMP SAVE behavior and DUMP STATUS command in protocol.md (EN/JA)

**Detailed Release Notes**: [docs/releases/v1.3.7.md](docs/releases/v1.3.7.md)

## [1.3.6] - 2025-11-26

### Performance

- **Cache key optimization** - Exclude LIMIT/OFFSET from cache keys to improve hit rate; single cache entry now serves all pagination variants of the same query

### Refactoring

- **Log verbosity reduction** - Demote startup/shutdown messages to debug level for cleaner production logs; adopt structured logging for config loading

**Detailed Release Notes**: [docs/releases/v1.3.6.md](docs/releases/v1.3.6.md)

## [1.3.5] - 2025-11-26

### Performance

- **Parallel query optimization** - Schwartzian Transform with partial_sort eliminates 99.9996% of lock acquisitions during parallel query execution
- **snprintf replacement** - ToZeroPaddedString using std::to_chars (~10x faster, no locale lock contention)
- **Zero-copy I/O** - Replace send() with writev() for response sending
- **Batch primary key lookups** - DocumentStore::GetPrimaryKeysBatch() for single lock acquisition
- **RCU pattern for index search** - TakePostingSnapshots() method reduces lock contention under high read concurrency

### Fixed

- **Medium: Primary key sort optimization** - Explicit column name (SORT id ASC) now recognized as equivalent to implicit sort (SORT ASC)
- **SIGPIPE handling** - Add process-wide SIGPIPE ignore and SO_NOSIGPIPE for macOS

### Added

- OPTIMIZE command now accepts optional table parameter
- Python benchmark tool for MygramDB vs MySQL comparison (`support/benchmark/`)

**Detailed Release Notes**: [docs/releases/v1.3.5.md](docs/releases/v1.3.5.md)

## [1.3.4] - 2025-11-25

### Added

- **Zero-downtime log rotation** - SIGUSR1 signal handler for seamless log file rotation (similar to nginx)
- 60+ new unit tests across multiple modules (CommandLineParser, BinlogFilterEvaluator, GTIDEncoder, TableCatalog, SnapshotScheduler, TableMetadataCache, CacheKey, ErrorCodeToString)

### Fixed

- **Medium: Permission errors for non-monitored tables** - Skip FetchColumnNames for tables not in monitoring configuration, preventing SELECT permission errors

### Testing

- New test files: command_line_parser_test, signal_manager_test, configuration_manager_test, binlog_filter_evaluator_test, gtid_encoder_test, binlog_reader_multitable_test, table_metadata_test, table_catalog_test, snapshot_scheduler_test, cache_key_test, error_test
- Test configuration improvements: RUN_SERIAL for slow tests, RESOURCE_LOCK for shared resources

**Detailed Release Notes**: [docs/releases/v1.3.4.md](docs/releases/v1.3.4.md)

## [1.3.3] - 2025-11-25

### Fixed

- **Critical: DATETIME2 parsing produces invalid dates** - Fix MySQL DATETIME2 binlog format parsing by adding missing `DATETIMEF_INT_OFS` offset subtraction and correcting year/month calculation (`ym/13`, `ym%13` instead of bitwise extraction)
- **Critical: server_id not passed to MySQL replication** - Fix hardcoded `server_id=1001` in binlog reader, now uses config value; prevents replication conflicts when multiple instances connect to same MySQL server
- **Medium: TIME2 type not implemented** - Implement MySQL TIME2 binlog format with `TIMEF_INT_OFS` offset and fractional seconds support
- **Low: TIMESTAMP2 big-endian handling** - Fix TIMESTAMP2 byte order (big-endian) and separate fractional seconds handling from legacy TIMESTAMP

### Added

- 15 new unit tests for datetime/time parsing (`DateTimeParsingTest` suite)
- Unit test for server_id=0 validation (`StartFailsWithZeroServerId`)
- Source code references to MySQL 8.4.7 (`mysys/my_time.cc`) in parsing implementation

**Detailed Release Notes**: [docs/releases/v1.3.3.md](docs/releases/v1.3.3.md)

## [1.3.2] - 2025-11-25

🚨 **CRITICAL UPGRADE - All v1.3.0 and v1.3.1 users with MySQL replication must upgrade immediately**

### Fixed

- **Critical: Binlog event parsing offset error** - Remove duplicate OK byte skip in BinlogEventParser (already handled by BinlogReader)
- **Critical: Binlog checksum boundary error** - Exclude 4-byte checksum from event parsing boundary to prevent buffer overrun
- **Critical: Extra row info length calculation** - Fix MySQL 8.0 ROWS_EVENT_V2 extra_row_info_len interpretation (includes packed integer itself)
- **High: Binlog purged error detection** - Detect errno 1236 (binlog position purged) and stop with actionable error message

### Added

- Enhanced debugging logs for TABLE_MAP_EVENT parsing with field-by-field validation
- Structured logging for BinlogReader lifecycle events (connection, stream, GTID)
- Structured logging for SyncOperationManager operations

### Changed

- Improved binlog fetch diagnostics (log first fetch result, no-data occurrences)
- Reduced log verbosity for production (debug level for routine replication events)

**Detailed Release Notes**: [docs/releases/v1.3.2.md](docs/releases/v1.3.2.md)

## [1.3.1] - 2025-11-24

🚨 **CRITICAL UPGRADE - All v1.3.0 users must upgrade immediately**

### Fixed

- **Critical: Replication corruption after SYNC** - Auto-restart replication with updated GTID
- **Critical: GTID validation** - Block REPLICATION START before initial SYNC
- **Critical: Logging configuration crash** - Fix initialization order
- **Critical: Missing mutual exclusion** - Prevent concurrent operation data corruption (DUMP/OPTIMIZE/SYNC)
- **Critical: Replication race conditions** - Add state flags to prevent manual interference during auto-management
- **High: TTL expiration not implemented** - Implement cache TTL expiration to prevent memory leak
- **Medium: Rate limiter callback** - Fix runtime toggle for rate limiting
- **Medium: BinlogReader performance** - Use unique_ptr to eliminate expensive copying
- **Low: DocID overflow logic** - Simplify complex overflow handling
- **Low: Code quality** - clang-tidy compliance, thread-safe StructuredLog

### Added

- New documentation: `docs/en/replication_management.md`, `docs/ja/replication_management.md`

### Testing

- 52+ new test cases, 1,228+ lines of test code
- Comprehensive coverage for all critical bugs

**Detailed Release Notes**: [docs/releases/v1.3.1.md](docs/releases/v1.3.1.md)

## [1.3.0] - 2025-11-22

### ⚠️ BREAKING CHANGES

- **Dump file format incompatibility** - DATETIME/DATE/TIME now stored as epoch seconds (rebuild required)
- **SIGHUP hot reload removed** - Use MySQL-style `SET`/`SHOW VARIABLES` commands instead

### Added

- MySQL-style `SET`/`SHOW VARIABLES` for runtime configuration with 11 mutable variables
- Zero-downtime MySQL failover with GTID preservation
- MySQL TIME type support (-838:59:59 to 838:59:59)
- Timezone-aware DATETIME/DATE processing (`mysql.datetime_timezone` config)
- Query parameter support in snapshot API (filter/sort during snapshot)

### Fixed

- **Critical: Primary key column name sorting** - `SORT id ASC/DESC` now works as documented

### Changed

- Renamed `SnapshotBuilder` → `InitialLoader` (moved to `src/loader/`)
- Enhanced CacheManager/QueryCache APIs for runtime configuration

### Testing

- 71 new test cases (1,894 lines): RuntimeVariableManager (46), MySQL failover (10), Variable handler (15)

**Detailed Release Notes**: [docs/releases/v1.3.0.md](docs/releases/v1.3.0.md)

## [1.2.5] - 2025-11-22

### Fixed

- Excessive warning logs for multi-table databases (changed to debug level)

**Detailed Release Notes**: [docs/releases/v1.2.5.md](docs/releases/v1.2.5.md)

## [1.2.4] - 2025-11-21

### Fixed

- **Critical: GTID parsing crash** - MySQL 8.4 compatibility (handle newlines in GTID strings)
- RPM package upgrade failures (suppress systemd errors)

**Detailed Release Notes**: [docs/releases/v1.2.4.md](docs/releases/v1.2.4.md)

## [1.2.3] - 2025-11-20

### Added

- Configurable MySQL session timeout (`replication.session_timeout_sec`)
- C API for parsing web-style search expressions (`mygram_parse_search_expr()`)

**Detailed Release Notes**: [docs/releases/v1.2.3.md](docs/releases/v1.2.3.md)

## [1.2.2] - 2025-11-20

### Fixed

- Docker entrypoint POSIX compatibility (replaced bashism with POSIX code)

**Detailed Release Notes**: [docs/releases/v1.2.2.md](docs/releases/v1.2.2.md)

## [1.2.1] - 2025-11-19

### Added

- `NETWORK_ALLOW_CIDRS` environment variable for Docker
- RPM testing environment in `support/testing/`

### Fixed

- MySQL 8.4 compatibility in docker-compose.yml
- Connection refused errors in Docker without custom config

**Detailed Release Notes**: [docs/releases/v1.2.1.md](docs/releases/v1.2.1.md)

## [1.2.0] - 2025-11-19

### ⚠️ BREAKING CHANGES

- **Network ACL now deny-by-default** - Must configure `network.allow_cidrs`

### Added

- MySQL failover detection with server UUID tracking
- Rate limiting with token bucket algorithm
- Connection limits
- Differential test execution (50-90% CI time reduction)
- Multi-architecture RPM builds (x86_64, aarch64)
- HTTP COUNT endpoint (`POST /{table}/count`)
- Type-safe error handling (`Expected<T, Error>`)
- Structured logging (JSON/text)

### Changed

- `LOG_JSON` → `LOG_FORMAT` (backward compatible)
- Extracted application layer from main.cpp (656→24 lines)
- Optimized string handling with `std::string_view`

### Security

- Network ACL deny-by-default
- Rate limiting
- Connection limits

**Detailed Release Notes**: [docs/releases/v1.2.0.md](docs/releases/v1.2.0.md)

## [1.1.0] - 2025-11-17

### ⚠️ BREAKING CHANGES

- Query syntax: `ORDER BY` → `SORT`
- Dump commands: `SAVE/LOAD` → `DUMP SAVE/LOAD`
- LIMIT syntax: Added `LIMIT offset,count`

### Added

- Query result caching with n-gram invalidation
- Network ACL with CIDR filtering
- Prometheus metrics endpoint
- MySQL SSL/TLS support
- SYNC command for manual synchronization
- Automatic dump saves
- RPM packaging

**Detailed Release Notes**: [docs/releases/v1.1.0.md](docs/releases/v1.1.0.md)

## [1.0.0] - 2025-11-13

Initial release with core search engine functionality and MySQL replication support.

---

[Unreleased]: https://github.com/libraz/mygram-db/compare/v1.10.0...HEAD
[1.10.0]: https://github.com/libraz/mygram-db/compare/v1.9.0...v1.10.0
[1.9.0]: https://github.com/libraz/mygram-db/compare/v1.8.1...v1.9.0
[1.8.1]: https://github.com/libraz/mygram-db/compare/v1.8.0...v1.8.1
[1.8.0]: https://github.com/libraz/mygram-db/compare/v1.7.0...v1.8.0
[1.7.0]: https://github.com/libraz/mygram-db/compare/v1.6.1...v1.7.0
[1.6.1]: https://github.com/libraz/mygram-db/compare/v1.6.0...v1.6.1
[1.6.0]: https://github.com/libraz/mygram-db/compare/v1.5.4...v1.6.0
[1.5.4]: https://github.com/libraz/mygram-db/compare/v1.5.3...v1.5.4
[1.5.3]: https://github.com/libraz/mygram-db/compare/v1.5.2...v1.5.3
[1.5.2]: https://github.com/libraz/mygram-db/compare/v1.5.1...v1.5.2
[1.5.1]: https://github.com/libraz/mygram-db/compare/v1.5.0...v1.5.1
[1.5.0]: https://github.com/libraz/mygram-db/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/libraz/mygram-db/compare/v1.3.9...v1.4.0
[1.3.9]: https://github.com/libraz/mygram-db/compare/v1.3.8...v1.3.9
[1.3.8]: https://github.com/libraz/mygram-db/compare/v1.3.7...v1.3.8
[1.3.7]: https://github.com/libraz/mygram-db/compare/v1.3.6...v1.3.7
[1.3.6]: https://github.com/libraz/mygram-db/compare/v1.3.5...v1.3.6
[1.3.5]: https://github.com/libraz/mygram-db/compare/v1.3.4...v1.3.5
[1.3.4]: https://github.com/libraz/mygram-db/compare/v1.3.3...v1.3.4
[1.3.3]: https://github.com/libraz/mygram-db/compare/v1.3.2...v1.3.3
[1.3.2]: https://github.com/libraz/mygram-db/compare/v1.3.1...v1.3.2
[1.3.1]: https://github.com/libraz/mygram-db/compare/v1.3.0...v1.3.1
[1.3.0]: https://github.com/libraz/mygram-db/compare/v1.2.5...v1.3.0
[1.2.5]: https://github.com/libraz/mygram-db/compare/v1.2.4...v1.2.5
[1.2.4]: https://github.com/libraz/mygram-db/compare/v1.2.3...v1.2.4
[1.2.3]: https://github.com/libraz/mygram-db/compare/v1.2.2...v1.2.3
[1.2.2]: https://github.com/libraz/mygram-db/compare/v1.2.1...v1.2.2
[1.2.1]: https://github.com/libraz/mygram-db/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/libraz/mygram-db/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/libraz/mygram-db/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/libraz/mygram-db/releases/tag/v1.0.0
