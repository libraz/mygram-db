<!-- markdownlint-disable MD024 -->
# Changelog

All notable changes to MygramDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note**: For detailed release information, see [docs/releases/](docs/releases/).

## [Unreleased]

### Breaking Change

- **Table identity is now database-qualified** — Table references are moving from bare table names such as `articles` to `(database, name)` identities written as `app_db.articles`. This is an intentional early-stage SemVer exception planned for v1.7.0.
- **HTTP table routes are database-qualified** — Use `POST /tables/{database}/{table}/search`, `POST /tables/{database}/{table}/count`, `POST /tables/{database}/{table}/facet`, and `GET /tables/{database}/{table}/{primary_key}`. Legacy bare routes such as `POST /articles/search` are deprecated and will not be kept as a compatibility contract.
- **TCP, CLI, C++, and C API table arguments are database-qualified** — Rewrite commands and client calls from `SEARCH articles hello`, `client.Search("articles", ...)`, or `mygramclient_search(client, "articles", ...)` to `SEARCH app_db.articles hello`, `client.Search("app_db.articles", ...)`, and `mygramclient_search(client, "app_db.articles", ...)`.
- **Dump metadata preserves per-table databases** — New V1/V2 dump paths retain table-level database names so `live_db.articles` and `archive_db.articles` can round-trip without one table overwriting the other.

Migration checklist:

1. For every configured table, identify the effective database: `tables[*].database` when set, otherwise `mysql.database`.
2. Replace every TCP/CLI/C++/C API table argument with `<database>.<table>`.
3. Replace HTTP table routes from `/{table}/...` to `/tables/{database}/{table}/...`.
4. Keep old dumps for rollback, then create a fresh dump after upgrading so restored metadata includes qualified table identities.

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
- **SYNC_STOP registration** — `SYNC_STOP` was missing from `InitDispatcher`, causing `"Unknown query type"`; fail-fast handler-table validation added (CR-8)
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
- Full matrix: 3026 unit tests, 4 e2e targets (MySQL 8.4/9.4, MariaDB 10.11/11.4)

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

[Unreleased]: https://github.com/libraz/mygram-db/compare/v1.6.1...HEAD
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
