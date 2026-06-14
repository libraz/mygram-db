# MygramDB

[![Docs](https://img.shields.io/badge/docs-mygramdb.libraz.net-2563eb)](https://mygramdb.libraz.net)
[![CI](https://img.shields.io/github/actions/workflow/status/libraz/mygram-db/ci.yml?branch=main&label=CI)](https://github.com/libraz/mygram-db/actions)
[![Version](https://img.shields.io/github/v/release/libraz/mygram-db?label=version)](https://github.com/libraz/mygram-db/releases)
[![Docker](https://img.shields.io/badge/docker-ghcr.io-blue?logo=docker)](https://github.com/libraz/mygram-db/pkgs/container/mygram-db)
[![codecov](https://codecov.io/gh/libraz/mygram-db/branch/main/graph/badge.svg)](https://codecov.io/gh/libraz/mygram-db)
[![License](https://img.shields.io/github/license/libraz/mygram-db)](https://github.com/libraz/mygram-db/blob/main/LICENSE)
[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue?logo=c%2B%2B)](https://en.cppreference.com/w/cpp/17)
[![MySQL](https://img.shields.io/badge/MySQL-8.4--9.6-blue?logo=mysql)](https://dev.mysql.com/)
[![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS-lightgrey)](https://github.com/libraz/mygram-db)

In-memory full-text search engine with MySQL binlog replication. Sub-millisecond queries on million-row datasets.

## Why MygramDB?

MySQL FULLTEXT scans B-tree pages on disk and struggles with common terms and concurrent load. MygramDB keeps a compressed n-gram index entirely in memory, syncing via GTID binlog replication.

## Performance

Benchmarked on 1.1M Wikipedia articles (EN + JA), MygramDB v1.5.0 vs MySQL 8.4 FULLTEXT with ngram parser:

| Query Type | MySQL | MygramDB | Speedup |
|------------|-------|----------|---------|
| **Search** (SORT id LIMIT 100) | 507–2,566ms | 0.08–0.42ms | 1,200–6,700x |
| **CJK search** (Japanese bi-gram) | 4–1,204ms | 1–4ms | 2–1,100x |
| **COUNT** | 416–1,797ms | 0.08ms | 5,500–21,600x |
| **Concurrent** (4 connections) | 8 QPS | 11,766 QPS | 1,400x |

- Sub-millisecond latency for most queries, no cache warmup needed
- v1.5.0 `verify_text` eliminates n-gram false positives (exact match with MySQL results)
- Reproducible: `make bench-up && make bench-run` ([details](https://mygramdb.libraz.net/docs/performance))

## Quick Start

### Docker (Production Ready)

**Prerequisites:** Ensure MySQL has GTID mode enabled:
```sql
-- Check GTID mode (should be ON)
SHOW VARIABLES LIKE 'gtid_mode';

-- If OFF, enable GTID mode (MySQL 8.0+ / 9.x)
SET GLOBAL enforce_gtid_consistency = ON;
SET GLOBAL gtid_mode = OFF_PERMISSIVE;
SET GLOBAL gtid_mode = ON_PERMISSIVE;
SET GLOBAL gtid_mode = ON;
```

**Start MygramDB:**
```bash
docker run -d --name mygramdb \
  -p 11016:11016 \
  -e MYSQL_HOST=your-mysql-host \
  -e MYSQL_USER=repl_user \
  -e MYSQL_PASSWORD=your_password \
  -e MYSQL_DATABASE=mydb \
  -e TABLE_NAME=articles \
  -e TABLE_PRIMARY_KEY=id \
  -e TABLE_TEXT_COLUMN=content \
  -e TABLE_NGRAM_SIZE=2 \
  -e REPLICATION_SERVER_ID=12345 \
  -e NETWORK_ALLOW_CIDRS=0.0.0.0/0 \
  ghcr.io/libraz/mygram-db:latest

# Check logs
docker logs -f mygramdb

# Trigger initial data sync (required on first start)
docker exec mygramdb mygram-cli -p 11016 SYNC articles

# Try a search
docker exec mygramdb mygram-cli -p 11016 SEARCH articles "hello world"
```

**Security Note:** `NETWORK_ALLOW_CIDRS=0.0.0.0/0` allows connections from any IP address. For production, restrict to specific IP ranges:
```bash
# Production example: Allow only from application servers
-e NETWORK_ALLOW_CIDRS=10.0.0.0/8,172.16.0.0/12
```

### Docker Compose (with Test MySQL)

```bash
git clone https://github.com/libraz/mygram-db.git
cd mygram-db
docker-compose up -d

# Wait for MySQL to be ready (check with docker-compose logs -f)

# Trigger initial data sync
docker-compose exec mygramdb mygram-cli -p 11016 SYNC articles

# Try searching
docker-compose exec mygramdb mygram-cli -p 11016 SEARCH articles "hello"
```

Includes MySQL 8.4 with sample data for instant testing. Also tested with MySQL 9.4 and MariaDB 10.11/11.4.

## Basic Usage

```bash
# Search with pagination
SEARCH articles "hello world" SORT id LIMIT 100

# Sort by relevance (BM25)
SEARCH articles "hello world" SORT _score DESC LIMIT 10

# Highlighted results
SEARCH articles "hello" HIGHLIGHT TAG <b> </b> LIMIT 10

# Fuzzy search (edit distance 1)
SEARCH articles "machne" FUZZY LIMIT 10

# Faceted aggregation
FACET articles category "tech"

# Count matches
COUNT articles "hello world"

# Multi-term AND search
SEARCH articles hello AND world

# With filters
SEARCH articles tech FILTER status=1 LIMIT 100

# Get by primary key
GET articles 12345
```

See [Protocol Reference](https://mygramdb.libraz.net/docs/protocol) for all commands.

## Features

- **Fast**: Sub-millisecond search on million-row datasets
- **BM25 Relevance**: `SORT _score` for TF-IDF based relevance ranking
- **Highlighting**: `HIGHLIGHT` clause returns snippets with matched terms tagged
- **Fuzzy Search**: `FUZZY` clause for Levenshtein edit distance matching
- **Synonyms**: Automatic query expansion from TSV synonym dictionaries
- **Faceted Search**: `FACET` command aggregates filter column values with counts
- **MySQL/MariaDB Replication**: Real-time GTID-based binlog streaming (MySQL 8.4+, MariaDB 10.6+)
- **Runtime Variables**: MySQL-style SET/SHOW VARIABLES for zero-downtime config changes
- **MySQL Failover**: Switch MySQL servers at runtime with GTID position preservation
- **Multiple Tables**: Index multiple tables in one instance
- **Dual Protocol**: TCP (memcached-style) and HTTP/REST API
- **High Concurrency**: Thread pool supporting 10,000+ connections
- **Unicode**: ICU-based normalization for CJK/multilingual text
- **Compression**: Hybrid Delta encoding + Roaring bitmaps
- **Easy Deploy**: Single binary or Docker container

## Architecture

```mermaid
graph LR
    MySQL[MySQL Primary] -->|binlog GTID| MygramDB1[MygramDB #1]
    MySQL -->|binlog GTID| MygramDB2[MygramDB #2]

    MygramDB1 -->|Search| App[Application]
    MygramDB2 -->|Search| App
    App -->|Write| MySQL
```

MygramDB acts as a specialized read replica for full-text search, while MySQL handles writes and normal queries.

## When to Use MygramDB

✅ **Good fit:**
- Search-heavy workloads (read >> write)
- Millions of documents with full-text search
- Need sub-100ms search latency
- Simple deployment requirements
- Japanese/CJK text with ngrams

❌ **Not recommended:**
- Write-heavy workloads
- Dataset doesn't fit in RAM (~1-2GB per million docs)
- Need distributed search across nodes
- Complex aggregations/analytics

## Documentation

📖 **Full documentation:** https://mygramdb.libraz.net

- **[CHANGELOG](CHANGELOG.md)** - Version history and release notes
- [Docker Deployment Guide](https://mygramdb.libraz.net/docs/docker-deployment) - Production Docker setup
- [Configuration Guide](https://mygramdb.libraz.net/docs/configuration) - All configuration options
- [Protocol Reference](https://mygramdb.libraz.net/docs/protocol) - Complete command reference
- [HTTP API Reference](https://mygramdb.libraz.net/docs/http-api) - REST API documentation
- [Performance Guide](https://mygramdb.libraz.net/docs/performance) - Benchmarks and optimization
- [Replication Guide](https://mygramdb.libraz.net/docs/replication) - MySQL replication setup
- [Operations Guide](https://mygramdb.libraz.net/docs/operations) - Runtime variables and MySQL failover
- [Installation Guide](https://mygramdb.libraz.net/docs/installation) - Build from source
- [Development Guide](https://mygramdb.libraz.net/docs/development) - Contributing guidelines
- [Client Library](https://mygramdb.libraz.net/docs/client-library) - C/C++ client library

### Release Notes

- [Latest Release](https://github.com/libraz/mygram-db/releases/latest) - Download binaries
- [Detailed Release Notes](docs/releases/) - Version-specific migration guides

## Requirements

**System:**
- RAM: ~1-2GB per million documents
- OS: Linux or macOS

**MySQL:**
- MySQL 8.4+ / 9.x (tested with 8.4 and 9.4)
- MariaDB 10.6+ / 11.x (tested with 10.11 and 11.4)
- GTID mode enabled (`gtid_mode=ON` for MySQL, GTID enabled for MariaDB)
- Binary log format: ROW (`binlog_format=ROW`)
- Replication privileges: `REPLICATION SLAVE`, `REPLICATION CLIENT`

See [Installation Guide](https://mygramdb.libraz.net/docs/installation) for details.

## License

[MIT License](LICENSE)

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

For development environment setup, see [Development Guide](https://mygramdb.libraz.net/docs/development).

## Authors

- libraz <libraz@libraz.net>

## Related Projects

- [mysql-event-stream](https://github.com/libraz/mysql-event-stream) - Standalone MySQL CDC library extracted from MygramDB's replication layer
- [go-mygram-client](https://github.com/libraz/go-mygram-client) - Go client library
- [node-mygramdb-client](https://github.com/libraz/node-mygramdb-client) - Node.js client library ([npm](https://www.npmjs.com/package/mygramdb-client))
- [python-mygramdb-client](https://github.com/libraz/python-mygramdb-client) - Python client library

## Acknowledgments

- [Roaring Bitmaps](https://roaringbitmap.org/) for compressed bitmaps
- [ICU](https://icu.unicode.org/) for Unicode support
- [spdlog](https://github.com/gabime/spdlog) for logging
- [yaml-cpp](https://github.com/jbeder/yaml-cpp) for configuration
