# Changelog

All notable changes to MygramDB will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.2.4] - 2025-11-21

### Fixed

- **Critical: Fixed GTID parsing crash during replication startup (MySQL 8.4 compatibility)**
  - MySQL 8.4 returns GTID strings with embedded newlines for readability (e.g., `uuid1:1-100,\nuuid2:1-200`)
  - Long GTID strings from multiple replication sources would cause `std::invalid_argument` crash
  - Now properly removes whitespace from GTID strings, following MySQL's official parser behavior
  - Affects systems replicating from multiple MySQL sources (6+ UUIDs)
- Fixed RPM package upgrade failures
  - Added error suppression to `%systemd_preun` macro
  - Eliminates "fg: no job control" errors during package updates
  - Ensures smooth upgrades without manual intervention

### Added

- Comprehensive GTID whitespace handling tests
  - 5 test cases covering various whitespace patterns (newlines, spaces, tabs)
  - Real-world 6-UUID production scenario validation
  - Documented MySQL 8.4 GTID formatting behavior

## [1.2.3] - 2025-11-20

### Added

- Configurable MySQL session timeout to prevent disconnection during long operations
  - New configuration option: `replication.session_timeout_sec` (default: 28800 seconds / 8 hours)
  - Automatically sets `wait_timeout` and `interactive_timeout` on MySQL connection
  - Prevents timeout issues during large table synchronization or slow binlog processing
- C API for parsing web-style search expressions
  - New public API: `mygram_parse_search_expr()` for parsing search queries
  - Enables integration with other C/C++ applications
  - Supports all existing query syntax (SEARCH, FILTER, SORT, LIMIT)

## [1.2.2] - 2025-11-20

### Fixed

- Fixed Docker entrypoint script POSIX compatibility issue
  - Replaced bashism (`<<<` here-string and array syntax) with POSIX-compliant code
  - Script now works correctly in Docker containers where `/bin/sh` is linked to `dash`
  - Fixes "Syntax error: redirection unexpected" error when using `NETWORK_ALLOW_CIDRS`

## [1.2.1] - 2025-11-19

### Added

- Added `NETWORK_ALLOW_CIDRS` environment variable support to Docker entrypoint
  - Enables easy network ACL configuration in Docker environments
  - Supports comma-separated CIDR list (e.g., `10.0.0.0/8,172.16.0.0/12`)
  - Default value: `0.0.0.0/0` (allow all) for development convenience
- Added RPM testing environment in `support/testing/`
  - Docker-based environment for testing RPM packages
  - Includes Rocky Linux 9 and MySQL 8.4 setup

### Fixed

- Fixed MySQL 8.4 compatibility in docker-compose.yml
  - Replaced deprecated `--default-authentication-plugin` with `--mysql-native-password=ON`
- Fixed connection refused errors when using Docker without custom config
  - Network ACL is now properly configured via environment variables

### Changed

- Updated `.gitignore` to use `/Testing/` instead of `Testing/` to avoid excluding support/testing directory
- Aligned example configuration files (YAML/JSON) with consistent defaults
  - Changed `allow_cidrs` from `127.0.0.1/32` to `0.0.0.0/0` for easier initial setup
  - Fixed JSON configs: added `ssl_enable`, corrected `dump.interval_sec`, `api.http.enable`, `logging.file`

### Documentation

- Added network ACL configuration examples to Docker deployment guide
- Added security warnings for production environments in README
- Updated configuration reference with Docker environment variable examples
- Added comprehensive documentation for network security settings

## [1.2.0] - 2025-11-19

### ⚠️ BREAKING CHANGES

**Network ACL now deny-by-default** - Servers will reject all connections unless explicitly allowed.

**Required action before upgrade:**

```yaml
# Add to config.yaml to allow connections
network:
  allow_cidrs:
    - "127.0.0.1/32"      # localhost only (secure default)
    # - "10.0.0.0/8"      # or your network range
    # - "0.0.0.0/0"       # or allow all (NOT RECOMMENDED)
```

**Why this change?** Fail-closed security is industry best practice. Previous versions accepted all connections by default, which posed a security risk.

### Added

- Configuration hot reload via SIGHUP signal for zero-downtime updates
- MySQL failover detection with server UUID tracking and validation
- Rate limiting with token bucket algorithm (configurable per-client IP)
- Connection limits to prevent file descriptor exhaustion
- Differential test execution for faster CI feedback (50-90% time reduction)
- Multi-architecture RPM builds (x86_64 and aarch64)
- Health endpoint metrics: current GTID, processed events, queue size
- HTTP COUNT endpoint (`POST /{table}/count`)
- Type-safe error handling with `Expected<T, Error>` pattern
- Structured logging system with JSON/text format support
- Linux CI testing infrastructure with Docker

### Changed

- LOG_JSON environment variable replaced with LOG_FORMAT (json/text) - backward compatible
- Extracted application layer from monolithic main.cpp (656→24 lines)
- Refactored binlog processing into modular parser/processor/evaluator
- Optimized string handling with C++17 `std::string_view` (zero-copy)
- Precomputed cache keys in QueryParser to reduce lock contention
- Extended Schwartzian transform to all filter columns (96% lock reduction)
- Reorganized test suite into focused, granular test files

### Fixed

- Compiler warnings in result_sorter and query_cache_test
- Security vulnerability: replaced deprecated tmpnam() with mkstemp()
- DocID overflow check to use full UINT32_MAX range
- SyncState thread safety with atomic total_rows
- PostingList thread safety with shared_mutex protection
- Resource leaks in ConnectionAcceptor and TcpServer (RAII guards)
- ThreadPool active_workers race condition
- macOS CI test timing issues with platform-specific thresholds

### Security

- Network ACL now deny-by-default (requires explicit allow_cidrs)
- Rate limiting to prevent abuse (disabled by default)
- Connection limits to prevent resource exhaustion
- FD leak fixes with RAII guards

### Documentation

- Added bilingual operations guide (EN/JA) for SIGHUP and failover scenarios
- Added Linux testing guide for Docker-based CI workflow
- Added architecture documentation for system design overview
- Added RPM build guide for multi-architecture packaging
- Updated configuration guide with security best practices

### Performance

- std::string_view migration eliminates unnecessary string copies
- Precomputed cache keys avoid redundant normalization
- Extended Schwartzian transform reduces lock contention by 96%

**Detailed Release Notes**: See [docs/releases/v1.2.0.md](docs/releases/v1.2.0.md)

## [1.1.0] - 2025-11-17

### ⚠️ BREAKING CHANGES

1. **Query Syntax**: `ORDER BY` → `SORT` (e.g., `SEARCH "text" SORT -id LIMIT 100`)
2. **Dump Commands**: `SAVE/LOAD` → `DUMP SAVE/LOAD` (new format with CRC32 verification)
3. **Configuration**: `replication.state_file` removed (auto-managed)
4. **LIMIT Enhancement**: Added MySQL-compatible `LIMIT offset,count` syntax

See [docs/releases/v1.1.0.md](docs/releases/v1.1.0.md) for migration guide.

### Added

- Query result caching with n-gram based invalidation
- Network access control with CIDR-based IP filtering
- Prometheus metrics endpoint (`/metrics`)
- MySQL SSL/TLS support
- SYNC command for manual table synchronization
- Automatic dump saves with configurable intervals
- Runtime configuration help system (`--help-config`)
- RPM packaging and GitHub Actions release workflow
- Input validation and DoS protection
- Primary key validation for table sync

### Changed

- Query syntax: `ORDER BY` replaced with `SORT` keyword
- Dump system: `SAVE/LOAD` replaced with `DUMP SAVE/LOAD/VERIFY/INFO`
- LIMIT syntax: Added `LIMIT offset,count` support (MySQL-compatible)
- Server architecture: Extracted components (ConnectionAcceptor, handlers)
- Storage: Unified dump management with versioned format

### Fixed

- Critical concurrency bugs in cache and MySQL handling
- Test parallelism issues causing hangs
- Memory safety issues in thread handling
- Server shutdown synchronization

### Security

- Network ACL with allow_cidrs configuration (default: allow all)
- MySQL SSL/TLS encryption support
- Query length validation
- Bounds checking in dump/document loaders

**Detailed Release Notes**: See [docs/releases/v1.1.0.md](docs/releases/v1.1.0.md)

## [1.0.0] - 2025-11-13

Initial release of MygramDB with core search engine functionality and MySQL replication support.

---

[Unreleased]: https://github.com/libraz/mygram-db/compare/v1.2.3...HEAD
[1.2.3]: https://github.com/libraz/mygram-db/compare/v1.2.2...v1.2.3
[1.2.2]: https://github.com/libraz/mygram-db/compare/v1.2.1...v1.2.2
[1.2.1]: https://github.com/libraz/mygram-db/compare/v1.2.0...v1.2.1
[1.2.0]: https://github.com/libraz/mygram-db/compare/v1.1.0...v1.2.0
[1.1.0]: https://github.com/libraz/mygram-db/compare/v1.0.0...v1.1.0
[1.0.0]: https://github.com/libraz/mygram-db/releases/tag/v1.0.0
