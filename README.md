# MygramDB

[![CI](https://img.shields.io/github/actions/workflow/status/libraz/mygram-db/ci.yml?branch=main&label=CI)](https://github.com/libraz/mygram-db/actions)
[![Version](https://img.shields.io/github/v/release/libraz/mygram-db?label=version)](https://github.com/libraz/mygram-db/releases)
[![codecov](https://codecov.io/gh/libraz/mygram-db/branch/main/graph/badge.svg)](https://codecov.io/gh/libraz/mygram-db)
[![License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/libraz/mygram-db/blob/main/LICENSE)
[![C++17](https://img.shields.io/badge/C%2B%2B-17-blue?logo=c%2B%2B)](https://en.cppreference.com/w/cpp/17)
[![Platform](https://img.shields.io/badge/platform-Linux%20%7C%20macOS-lightgrey)](https://github.com/libraz/mygram-db)
[![Docs](https://img.shields.io/badge/docs-mygramdb.libraz.net-2563eb)](https://mygramdb.libraz.net)
[![Docker](https://img.shields.io/badge/docker-ghcr.io-blue?logo=docker)](https://github.com/libraz/mygram-db/pkgs/container/mygram-db)
[![MySQL](https://img.shields.io/badge/MySQL-8.4--9.7-blue?logo=mysql)](https://dev.mysql.com/)

**MygramDB is an in-memory full-text search engine synchronized from MySQL or
MariaDB binlogs.** Keep MySQL/MariaDB as the source of truth and send search
traffic to MygramDB.

**Use it when you need to:**

- **Search MySQL data quickly** — n-gram search, filters, facets, BM25 sorting, highlights, and fuzzy matching run from memory.
- **Keep application writes unchanged** — GTID binlog replication follows INSERT, UPDATE, DELETE, and supported schema changes.
- **Operate a separate search replica** — use TCP or HTTP for reads while MySQL/MariaDB continues to own writes and SQL access.

📖 **[Documentation](https://mygramdb.libraz.net)** &nbsp;·&nbsp; **[Getting started](https://mygramdb.libraz.net/docs/getting-started)** &nbsp;·&nbsp; **[Docker deployment](https://mygramdb.libraz.net/docs/docker-deployment)** &nbsp;·&nbsp; **[Query syntax](https://mygramdb.libraz.net/docs/query-syntax)** &nbsp;·&nbsp; **[Operations](https://mygramdb.libraz.net/docs/operations)**

## What it includes

- GTID-based MySQL 8.4+/9.x and MariaDB 10.11+/11.x replication
- TCP protocol, HTTP API, and C/C++ client libraries
- Multi-table indexing, runtime configuration, DUMP save/load, and automatic reconnection to the configured MySQL endpoint
- ICU normalization for CJK and other multilingual text

See the [documentation](https://mygramdb.libraz.net) for installation,
configuration, deployment, protocol and HTTP API references, replication
requirements, and performance data.

MySQL connection settings are startup-only. Changing `mysql.host` or
`mysql.port` requires a restart; automatic reconnection never switches to a
different endpoint, and a source-server UUID change is rejected.

## Credentials from the environment

Credentials can stay out of the configuration file entirely. MygramDB reads
these variables itself and they take precedence over the corresponding
configuration keys:

| Variable | Key it overrides |
| --- | --- |
| `MYGRAM_MYSQL_HOST` | `mysql.host` |
| `MYGRAM_MYSQL_PORT` | `mysql.port` |
| `MYGRAM_MYSQL_USER` | `mysql.user` |
| `MYGRAM_MYSQL_PASSWORD` | `mysql.password` |
| `MYGRAM_MYSQL_DATABASE` | `mysql.database` |
| `MYGRAM_API_ADMIN_TOKEN` | `api.admin_token` |

A variable set to the empty string counts as unset and falls through to the
configuration file.

`MYGRAM_API_ADMIN_TOKEN` is read by the server. It is not the same thing as the
`API_ADMIN_TOKEN` in `.env`, which the Docker entrypoint writes into the
generated `config.yaml`; use `API_ADMIN_TOKEN` with the shipped compose files
and `MYGRAM_API_ADMIN_TOKEN` when you run the binary yourself.

`CONFIG VERIFY` inspects the supplied file on its own and does not apply these
overrides, so a configuration that relies on them is reported as incomplete
there even though the server starts from it.

## Network safety

The shipped Docker environment is localhost-only by default. For a
non-loopback TCP bind, set a high-entropy `API_ADMIN_TOKEN`; MygramDB rejects
the configuration when it is absent. Before a TCP connection can use an
administrative command (`SET`, `SHOW VARIABLES`, `DUMP`, `SYNC`,
`REPLICATION`, `OPTIMIZE`, `CACHE`, `CONFIG`, or `DEBUG`), it must send
`AUTH <token>` on that same connection. The HTTP `POST /optimize` endpoint uses the same token as an
`Authorization: Bearer <token>` credential. Docker Compose deliberately refuses
to start until the placeholder token in `.env` is replaced.

Keep the TCP port behind a restrictive `NETWORK_ALLOW_CIDRS` list and a private
or encrypted network. The TCP protocol itself does not encrypt the `AUTH`
token. Configuration also rejects a universal IPv4 or IPv6 allow list with a
public bind.

`NETWORK_ALLOW_CIDRS` is matched against the address the server sees. Under
Docker that is the bridge gateway rather than the host address, so the shipped
compose files allow the bridge range and rely on the loopback ports mapping to
bound host exposure.

## Development

```bash
make build
make test
```

The [development guide](https://mygramdb.libraz.net/docs/development) covers
the full test matrix, E2E suites, sanitizer runs, and local fuzzing.

## License

[MIT License](LICENSE)

## Related projects

- [mysql-event-stream](https://github.com/libraz/mysql-event-stream) — MySQL/MariaDB CDC library extracted from MygramDB
- [go-mygram-client](https://github.com/libraz/go-mygram-client) — Go client library
- [node-mygramdb-client](https://github.com/libraz/node-mygramdb-client) — Node.js client library
- [python-mygramdb-client](https://github.com/libraz/python-mygramdb-client) — Python client library
