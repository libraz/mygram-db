<!-- markdownlint-disable MD024 -->
# Changelog

All notable changes to MygramDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**Note**: For detailed release information, see [docs/releases/](docs/releases/).

## [Unreleased]

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

[Unreleased]: https://github.com/libraz/mygram-db/compare/v1.5.3...HEAD
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
