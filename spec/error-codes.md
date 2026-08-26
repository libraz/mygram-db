# Error Code Registry

This file is normative for MygramDB's error-code surface. It describes what the code does today: every value defined in `src/utils/error.h`, where that value is constructed, and which client-visible surfaces can carry it. Error codes are part of the external contract — adding, removing, or renumbering one is a surface change, as is moving a code from `internal` to a client-visible surface.

## Summary

| Range | Module | Codes defined |
|-------|--------|---------------|
| 0-999 | General | 12 |
| 1000-1999 | Configuration | 10 |
| 2000-2999 | Database/Connection | 21 |
| 3000-3999 | Query/Request parsing | 12 |
| 4000-4999 | Business logic (Index/Search) | 14 |
| 5000-5999 | Storage/Snapshot | 13 |
| 6000-6999 | Network/Server | 29 |
| 7000-7999 | Client | 13 |
| 8000-8999 | Cache | 5 |
| **Total** | | **129** |

Of the 129 defined codes, 109 have at least one construction site in `src/` and 20 have none.

The enumerator names below are C++ identifiers (`mygramdb::utils::ErrorCode`). No surface transmits the name — only the unsigned 16-bit number travels over TCP, HTTP, and the C ABI.

## Surface vocabulary

The Surfaces column uses these terms.

| Term | Meaning |
|------|---------|
| `TCP` | Appears as the numeric field of an `ERROR <code> <message>` frame on the text protocol (`src/server/response_formatter.cpp`). |
| `TCP+` | `TCP`, and consequently `CLI`, `C++ SDK`, and `C ABI` — see the propagation rule below. |
| `HTTP` | Appears as `error_code` in a JSON error body (`src/server/http_server.cpp`). |
| `CLI` | Rendered by `mygram-cli`. |
| `C++ SDK` | Reachable as `Error::code()` from `MygramClient`. |
| `C ABI` | Reachable through `mygramclient_get_last_error_code()`. |
| `status field` | Carried as the `last_error_code` value of `INFO` / `REPLICATION STATUS` (TCP), `GET /replication/status` (HTTP), and `mygramdb_replication_last_error_code` (`GET /metrics`) — a reported replication attribute, not an error response. |
| `internal` | Constructed, but consumed inside the process (logged, branched on, or masked by an outer code) and never rendered on a client surface. |
| `unreferenced` | No construction site anywhere in `src/`. |

### The propagation rule behind `TCP+`

`protocol::ParseErrorFrame` (`src/server/protocol_constants.h`) extracts the leading decimal token of an `ERROR` frame, and `lib::ParseServerErrorResponse` (`src/client/mygramclient.cpp`) casts that number directly to `ErrorCode` with no membership check:

```cpp
return MakeError(static_cast<ErrorCode>(*frame->code), std::string(frame->message));
```

The resulting `Error` flows into every `Expected<T, Error>` returned by the C++ SDK, into `mygramclient_get_last_error_code()` via `set_last_error` (`src/client/mygramclient_c.cpp`), and into `mygram-cli`'s printed output. Therefore any code the server can place in a TCP `ERROR` frame is automatically observable on the CLI, the C++ SDK, and the C ABI. `TCP+` is the shorthand for that set.

The HTTP surface does not have an equivalent blanket path: `HttpServer::SendError` is called with an explicit code at each site, and the only place HTTP re-reads a code out of a TCP frame is `POST /optimize` (`src/server/http_server.cpp`).

## Registry

### General (0-999)

| Code | Symbolic name | Meaning | Surfaces | Emitting site(s) |
|------|---------------|---------|----------|------------------|
| 0 | `kSuccess` | Operation succeeded; not an error | `status field`, `C ABI` | `src/client/mygramclient_c.cpp`, `src/mysql/binlog_reader_interface.h` |
| 1 | `kUnknown` | Unknown error | `HTTP`, `C ABI` | `src/server/http_server.cpp`, `src/client/mygramclient_c.cpp` |
| 2 | `kInvalidArgument` | Invalid argument provided | `TCP+`, `HTTP`, `internal` | `src/server/handlers/admin_handler.cpp`, `src/utils/safe_path.cpp`, `src/index/bm25_scorer.cpp` (46 sites total). Not emitted by `SET`: the runtime-variable path reports through the Configuration range (1004, 1008, 1009) |
| 3 | `kOutOfRange` | Value out of range | `internal` — masked into 2008 at `src/mysql/mysql_binlog_stream.cpp` | `src/mysql/gtid_encoder.cpp` |
| 4 | `kNotImplemented` | Feature not implemented | `TCP+`, `HTTP` | `src/server/handlers/replication_handler.cpp`, `src/server/response_formatter.cpp`, `src/server/http_server.cpp` (10 sites total) |
| 5 | `kInternalError` | Internal error | `TCP+`, `HTTP`, `internal` | `src/server/handlers/search_handler.cpp`, `src/server/http_server.cpp`, `src/server/search_pipeline.cpp` (48 sites total) |
| 6 | `kIOError` | I/O error (file read/write) | `internal` — startup configuration | `src/app/configuration_manager.cpp`, `src/app/application.cpp` (7 sites total) |
| 7 | `kPermissionDenied` | Permission denied | `TCP+` (AUTH), `HTTP` (CIDR allow-list, bearer token), `internal` (privilege drop, MySQL grants) | `src/server/request_dispatcher.cpp`, `src/server/http_server.cpp`, `src/app/application.cpp` (9 sites total) |
| 8 | `kNotFound` | Resource not found | `TCP+` | `src/server/sync_operation_manager.cpp`, `src/server/handlers/admin_handler.cpp`. Document lookups no longer share this code; they report 4004 |
| 9 | `kAlreadyExists` | Resource already exists | `TCP+` — `REPLICATION START` when replication is already running | `src/server/handlers/replication_handler.cpp` |
| 10 | `kTimeout` | Operation timed out | `unreferenced` | — |
| 11 | `kCancelled` | Operation cancelled | `internal` — startup abort during shutdown | `src/app/server_orchestrator.cpp`, `src/mysql/gtid_waiter.cpp` |

### Configuration (1000-1999)

All Configuration codes except 1007 reach TCP through the administrative commands `CONFIG VERIFY`, `CONFIG SHOW`, `CONFIG HELP`, and `SET`, which forward the underlying code verbatim (`src/server/handlers/admin_handler.cpp`; `src/server/handlers/variable_handler.cpp`). None of them reach the HTTP surface: `GET /config` reports failures as 5.

| Code | Symbolic name | Meaning | Surfaces | Emitting site(s) |
|------|---------------|---------|----------|------------------|
| 1000 | `kConfigFileNotFound` | Configuration file not found | `TCP+` (`CONFIG VERIFY`), `internal` (startup, synonym-dictionary loading) | `src/server/handlers/admin_handler.cpp`, `src/config/config_loader.cpp`, `src/query/synonym_dictionary.cpp` (5 sites total) |
| 1001 | `kConfigParseError` | Configuration parse error | `TCP+` (`CONFIG VERIFY`), `internal` (startup, synonym-dictionary loading) | `src/config/config_loader.cpp`, `src/query/synonym_dictionary.cpp` (6 sites total) |
| 1002 | `kConfigValidationError` | Configuration validation error | `TCP+` (`CONFIG VERIFY`) | `src/config/config_validator.cpp`, `src/config/config.cpp` |
| 1003 | `kConfigMissingRequired` | Missing required configuration | `TCP+` (`CONFIG VERIFY`) | `src/config/config.cpp` (4 sites total) |
| 1004 | `kConfigInvalidValue` | Invalid configuration value | `TCP+` (`CONFIG VERIFY`, `CONFIG SHOW`, `CONFIG HELP`, `SET`) — the value of a known, mutable variable was rejected; the name-level rejections are 1008 and 1009 | `src/config/runtime_variable_manager.cpp`, `src/config/config_validator.cpp`, `src/config/config.cpp` (60 sites total) |
| 1005 | `kConfigSchemaError` | JSON schema error | `TCP+` (`CONFIG HELP`) | `src/config/config_help.cpp` |
| 1006 | `kConfigYamlError` | YAML parsing error | `TCP+` (`CONFIG VERIFY`) | `src/config/config_loader.cpp` |
| 1007 | `kConfigJsonError` | JSON parsing error | `internal` — only reachable for a JSON config file at startup; `CONFIG VERIFY` rejects any extension other than `.yaml`/`.yml` (`src/server/handlers/admin_handler.cpp`) | `src/config/config_loader.cpp` |
| 1008 | `kConfigUnknownVariable` | No configuration variable by that name | `TCP+` (`SET`) — `src/server/handlers/variable_handler.cpp` is reached first, by the pre-assignment lookup | `src/config/runtime_variable_manager.cpp` |
| 1009 | `kConfigVariableNotMutable` | Variable exists but cannot be changed at runtime | `TCP+` (`SET`) | `src/config/runtime_variable_manager.cpp` |

### Database/Connection (2000-2999)

`BinlogReader::Start()` returns the underlying error verbatim (`src/mysql/binlog_reader.cpp`), and `ReplicationHandler` forwards that code into the TCP frame (`src/server/handlers/replication_handler.cpp`). Separately, `BinlogReader::SetLastError` (`src/mysql/binlog_reader.h`) publishes a code into the `status field`. Codes are marked `TCP+` here only where a `REPLICATION START` path can return them.

| Code | Symbolic name | Meaning | Surfaces | Emitting site(s) |
|------|---------------|---------|----------|------------------|
| 2000 | `kMySQLConnectionFailed` | MySQL connection failed | `status field`, `internal` (startup) | `src/mysql/binlog_reader.cpp`, `src/mysql/connection.cpp` (9 sites total) |
| 2001 | `kMySQLQueryFailed` | MySQL query failed | `TCP+` (`REPLICATION START`), `status field` | `src/mysql/connection_validator.cpp`, `src/mysql/connection.cpp`, `src/mysql/ddl_schema_validator.cpp` (13 sites total) |
| 2002 | `kMySQLDisconnected` | MySQL disconnected | `TCP+` (`REPLICATION START`), `status field` | `src/mysql/binlog_reader_threads.cpp`, `src/mysql/connection_validator.cpp`, `src/mysql/connection.cpp` (16 sites total) |
| 2003 | `kMySQLAuthFailed` | MySQL authentication failed | `status field`, `internal` (startup) — a server that answered and refused the credentials, as opposed to 2000 for one that could not be reached | `src/mysql/connection.cpp` (classified from the native error) |
| 2004 | `kMySQLTimeout` | MySQL timeout | `unreferenced` — the only appearance is a comparison in `IsRetryableSchemaValidationError` (`src/mysql/binlog_reader_utils.cpp`) | — |
| 2005 | `kMySQLInvalidGTID` | Invalid GTID | `TCP+` (`REPLICATION START`), `status field`, `internal` | `src/mysql/connection_validator.cpp`, `src/mysql/gtid_encoder.cpp`, `src/mysql/connection.cpp` (18 sites total) |
| 2006 | `kMySQLGTIDNotEnabled` | GTID mode not enabled | `TCP+` (`REPLICATION START`), `status field` | `src/mysql/connection_validator.cpp`, `src/mysql/binlog_reader.cpp` |
| 2007 | `kMySQLReplicationError` | Replication error | `TCP+` (`REPLICATION START`), `status field` | `src/mysql/connection_validator.cpp` (4 sites total) |
| 2008 | `kMySQLBinlogError` | Binlog error | `TCP+` (`REPLICATION START`), `status field`. The code the reader publishes for a failed binlog fetch and for a monitored row event it cannot parse | `src/mysql/binlog_reader_threads.cpp`, `src/mysql/connection_validator.cpp`, `src/mysql/mysql_binlog_stream.cpp` (54 sites total) |
| 2009 | `kMySQLTableNotFound` | Table not found | `TCP+` (`REPLICATION START`), `status field` | `src/mysql/connection_validator.cpp`, `src/mysql/connection.cpp` |
| 2010 | `kMySQLColumnNotFound` | Column not found | `TCP+` (`REPLICATION START` via `DDLSchemaValidator::Capture`), `status field` | `src/mysql/ddl_schema_validator.cpp`, `src/mysql/connection.cpp` |
| 2011 | `kMySQLDuplicateColumn` | Duplicate column | `undetermined` — constructed in a column-metadata helper; no traced call chain reaches a client surface | `src/mysql/connection.cpp` |
| 2012 | `kMySQLInvalidSchema` | Invalid schema | `TCP+` (`REPLICATION START`), `status field` | `src/mysql/binlog_reader.cpp`, `src/mysql/ddl_schema_validator.cpp`, `src/mysql/binlog_reader_utils.cpp` (15 sites total) |
| 2013 | `kMySQLFieldTruncated` | Field data truncated | `status field` (checksum path only, `src/mysql/binlog_reader_threads.cpp`); `internal` on the row-decode path, where the outer handler masks it into 2008 | `src/mysql/binlog_checksum.h`, `src/mysql/rows_parser_field_decoder.cpp`, `src/mysql/binary_json.cpp` (38 sites total) |
| 2014 | `kMySQLInvalidMetadata` | Invalid field metadata | `status field` (column-metadata path only, `src/mysql/binlog_reader_utils.cpp`, where `SHOW COLUMNS` disagrees with the `TABLE_MAP` column count); `internal` on the row-decode path, where the outer handler reports 2008 instead | `src/mysql/rows_parser_field_decoder.cpp`, `src/mysql/binary_json.cpp`, `src/mysql/value_canonicalizer.cpp` (27 sites total) |
| 2015 | `kMySQLUnsupportedType` | Unsupported column type | `internal` — row-decode only; the outer handler reports 2008 instead | `src/mysql/rows_parser.cpp`, `src/mysql/rows_parser_field_decoder.cpp` |
| 2016 | `kMySQLBinlogChecksumMismatch` | Binlog checksum mismatch | `status field` | `src/mysql/binlog_checksum.h` |
| 2017 | `kMySQLUndecodableBinlogEvent` | Undecodable binlog event | `status field` | `src/mysql/binlog_reader_threads.cpp` |
| 2020 | `kMariaDBInvalidGTID` | Invalid MariaDB GTID | `undetermined` — constructed in GTID parsing shared by the reader thread and the validator; no single call chain traced to a surface | `src/mysql/mariadb_gtid.cpp` (6 sites total) |
| 2021 | `kMariaDBProtocolError` | MariaDB protocol error | `status field` — reaches `SetLastError` through `SetupSession`/`Open` (`src/mysql/binlog_reader_threads.cpp`) | `src/mysql/mariadb_binlog_stream.cpp` (6 sites total) |
| 2022 | `kMariaDBUnsupportedVersion` | MariaDB version not supported | `unreferenced` | — |

### Query/Request parsing (3000-3999)

Query-parser codes reach TCP through `RequestDispatcher::Dispatch` (`src/server/request_dispatcher.cpp`) and HTTP through the JSON body validators.

| Code | Symbolic name | Meaning | Surfaces | Emitting site(s) |
|------|---------------|---------|----------|------------------|
| 3000 | `kQuerySyntaxError` | Query syntax error | `TCP+`, `HTTP` | `src/query/query_parser.cpp`, `src/query/query_parser_commands.cpp`, `src/server/http_server.cpp` (106 sites total) |
| 3001 | `kQueryInvalidToken` | Invalid token | `TCP+`, `HTTP` | `src/server/http_server.cpp`, `src/server/search_pipeline.cpp` (4 sites total) |
| 3002 | `kQueryUnexpectedToken` | Unexpected token | `unreferenced` | — |
| 3003 | `kQueryMissingOperand` | Missing operand | `unreferenced` | — |
| 3004 | `kQueryInvalidOperator` | Invalid operator | `unreferenced` | — |
| 3005 | `kQueryTooLong` | Query too long | `TCP+`, `HTTP` — the only construction site is in `QueryParser`; both surfaces reach it by configuring that parser with `api.max_query_length` and forwarding its error (`src/server/request_dispatcher.cpp`, `src/server/http_server.cpp`) | `src/query/query_parser.cpp` |
| 3006 | `kQueryInvalidFilter` | Invalid filter | `TCP+`, `HTTP` | `src/query/query_parser_clauses.cpp`, `src/server/http_server.cpp` (12 sites total) |
| 3007 | `kQueryInvalidSort` | Invalid sort | `TCP+`, `HTTP` | `src/query/query_parser_clauses.cpp`, `src/server/http_server.cpp`, `src/query/result_sorter.cpp` (13 sites total) |
| 3008 | `kQueryInvalidLimit` | Invalid limit | `TCP+`, `HTTP` | `src/query/query_parser_clauses.cpp`, `src/server/http_server.cpp` (12 sites total) |
| 3009 | `kQueryInvalidOffset` | Invalid offset | `TCP+`, `HTTP` | `src/query/query_parser_clauses.cpp`, `src/server/http_server.cpp` (7 sites total) |
| 3010 | `kQueryExpressionParseError` | Expression parse error | `TCP+`, `HTTP` | `src/server/search_pipeline.cpp` |
| 3011 | `kQueryASTBuildError` | AST build error | `unreferenced` | — |

### Business logic — Index/Search (4000-4999)

| Code | Symbolic name | Meaning | Surfaces | Emitting site(s) |
|------|---------------|---------|----------|------------------|
| 4000 | `kIndexNotFound` | Index not found | `TCP+`, `HTTP` — search and facet pipelines | `src/server/search_pipeline.cpp` (7 sites total) |
| 4001 | `kIndexCorrupted` | Index corrupted | `unreferenced` | — |
| 4002 | `kIndexSerializationFailed` | Index serialization failed | `internal` — masked into 5012 by `src/storage/dump_format_v2.cpp` | `src/index/index_serialization.cpp` (4 sites total) |
| 4003 | `kIndexDeserializationFailed` | Index deserialization failed | `internal` — masked into 5011 by `src/storage/dump_format_internal.cpp` | `src/index/index_serialization.cpp` |
| 4004 | `kIndexDocumentNotFound` | Document not found | `TCP+` (`GET`), `HTTP` (`GET /tables/{table}/{primary_key}`) | `src/server/response_formatter.cpp`, `src/server/handlers/document_handler.cpp`, `src/server/http_server.cpp` |
| 4005 | `kIndexInvalidDocID` | Invalid document ID | `unreferenced` | — |
| 4006 | `kIndexFull` | Index full | `unreferenced` | — |
| 4007 | `kTableNotFound` | Table not found in catalog | `TCP+`, `HTTP` (404) | `src/server/handlers/command_handler.cpp`, `src/server/http_server.cpp`, `src/server/search_pipeline.cpp` (8 sites total) |
| 4008 | `kCatalogNotInitialized` | Table catalog not initialized | `TCP+` | `src/server/handlers/command_handler.cpp`, `src/server/handlers/admin_handler.cpp`, `src/server/handlers/cache_handler.cpp` (4 sites total) |
| 4010 | `kSyncTableNotFound` | Table not found for SYNC | `TCP+` (`SYNC`) | `src/server/sync_operation_manager.cpp` |
| 4011 | `kSyncAlreadyInProgress` | SYNC already in progress | `TCP+` (`SYNC`) | `src/server/sync_operation_manager.cpp` (4 sites total) |
| 4012 | `kSyncMemoryCritical` | Memory critically low for SYNC | `TCP+` (`SYNC`) | `src/server/sync_operation_manager.cpp` |
| 4013 | `kSyncThreadCreationFailed` | Failed to create sync thread | `TCP+` (`SYNC`) | `src/server/sync_operation_manager.cpp` |
| 4014 | `kSyncManagerNull` | `SyncOperationManager` dependency is null | `internal` — construction-time guard in `SyncHandler::Create`, evaluated during server initialization | `src/server/handlers/sync_handler.h` |

4009 is not assigned.

### Storage/Snapshot (5000-5999)

`DUMP LOAD`, `DUMP VERIFY`, and `DUMP INFO` forward the underlying code verbatim (`src/server/handlers/dump_handler.cpp`). `DUMP SAVE` does not — see Known divergences.

| Code | Symbolic name | Meaning | Surfaces | Emitting site(s) |
|------|---------------|---------|----------|------------------|
| 5000 | `kStorageFileNotFound` | Storage file not found | `internal` | `src/index/index_serialization.cpp`, `src/utils/atomic_file_writer.cpp` |
| 5001 | `kStorageReadError` | Storage read error | `TCP+` (`DUMP LOAD` — the document-store loader is propagated verbatim by `src/storage/dump_format_internal.cpp`), `internal` | `src/storage/document_store_persistence.cpp` (17 sites total) |
| 5002 | `kStorageWriteError` | Storage write error | `internal` — masked into 5012 on the dump path | `src/utils/atomic_file_writer.cpp`, `src/storage/document_store_persistence.cpp`, `src/storage/document_store.cpp` (16 sites total) |
| 5003 | `kStorageCorrupted` | Storage corrupted | `TCP+` (`DUMP LOAD`, document-store payload), `internal` (index payload) | `src/storage/document_store_persistence.cpp`, `src/index/index_serialization.cpp` (39 sites total) |
| 5004 | `kStorageCRCMismatch` | CRC mismatch | `internal` — masked into 5011 by `src/storage/dump_format_internal.cpp` | `src/index/index_serialization.cpp` |
| 5005 | `kStorageVersionMismatch` | Version mismatch | `TCP+` (`DUMP LOAD`, `DUMP VERIFY`, `DUMP INFO`), `internal` (startup) | `src/server/handlers/dump_handler.cpp`, `src/storage/dump_format_v1.cpp`, `src/app/server_orchestrator.cpp` (17 sites total) |
| 5006 | `kStorageCompressionFailed` | Compression failed | `unreferenced` | — |
| 5007 | `kStorageDecompressionFailed` | Decompression failed | `unreferenced` | — |
| 5008 | `kStorageInvalidFormat` | Invalid format | `internal` — masked into 5011 by `src/storage/dump_format_internal.cpp` | `src/index/index_serialization.cpp` (5 sites total) |
| 5009 | `kStorageSnapshotBuildFailed` | Snapshot build failed | `internal` — initial load and startup snapshot build | `src/loader/initial_loader.cpp`, `src/app/server_orchestrator.cpp` (24 sites total) |
| 5010 | `kStorageDocIdExhausted` | DocID exhausted | `internal` | `src/storage/document_store.cpp` |
| 5011 | `kStorageDumpReadError` | Storage dump read error | `TCP+` (`DUMP LOAD`, `DUMP VERIFY`, `DUMP INFO`), `internal` (startup restore) | `src/storage/dump_format_v2.cpp`, `src/storage/dump_format_v1.cpp`, `src/server/handlers/dump_handler.cpp` (227 sites total) |
| 5012 | `kStorageDumpWriteError` | Storage dump write error | `TCP+` (`DUMP SAVE`) | `src/server/handlers/dump_handler.cpp`, `src/storage/dump_format_v2.cpp`, `src/storage/dump_format_v1.cpp` (174 sites total) |

### Network/Server (6000-6999)

Codes 6000-6025 are lifecycle and socket-layer failures raised before or outside request handling; unless noted, they are returned to the process that started the listener rather than to a connected client.

| Code | Symbolic name | Meaning | Surfaces | Emitting site(s) |
|------|---------------|---------|----------|------------------|
| 6000 | `kNetworkBindFailed` | Bind failed | `internal` — startup | `src/server/connection_acceptor.cpp`, `src/server/http_server.cpp` (5 sites total) |
| 6001 | `kNetworkListenFailed` | Listen failed | `internal` — startup | `src/server/connection_acceptor.cpp` |
| 6002 | `kNetworkAcceptFailed` | Accept failed | `unreferenced` | — |
| 6003 | `kNetworkConnectionRefused` | Connection refused | `unreferenced` | — |
| 6004 | `kNetworkConnectionClosed` | Connection closed | `unreferenced` | — |
| 6005 | `kNetworkSendFailed` | Send failed | `unreferenced` | — |
| 6006 | `kNetworkReceiveFailed` | Receive failed | `unreferenced` | — |
| 6007 | `kNetworkInvalidRequest` | Invalid request | `TCP+` (oversized request frame), `HTTP` (415 on a non-JSON `Content-Type`) | `src/server/reactor_connection.cpp`, `src/server/http_server.cpp` |
| 6008 | `kNetworkProtocolError` | Protocol error | `unreferenced` | — |
| 6010 | `kNetworkServerNotStarted` | Server not started | `internal` | `src/server/connection_acceptor.cpp`, `src/server/io_reactor.cpp` |
| 6011 | `kNetworkAlreadyRunning` | Server already running | `internal` — startup and periodic-worker guards | `src/server/connection_acceptor.cpp`, `src/server/tcp_server.cpp`, `src/server/http_server.cpp` (5 sites total) |
| 6012 | `kNetworkSocketCreationFailed` | Socket creation failed | `internal` — startup | `src/server/connection_acceptor.cpp`, `src/server/io_reactor.cpp` (4 sites total) |
| 6013 | `kNetworkInvalidBindAddress` | Invalid bind address | `internal` — startup | `src/server/connection_acceptor.cpp` |
| 6014 | `kNetworkUnixSocketPathTooLong` | Unix socket path too long | `internal` — startup | `src/server/connection_acceptor.cpp` |
| 6015 | `kNetworkUnixSocketStale` | Unix socket already in use | `internal` — startup | `src/server/connection_acceptor.cpp` |
| 6016 | `kNetworkReactorUnsupported` | Event multiplexer not supported on this platform | `internal` — startup | `src/server/io_reactor.cpp` |
| 6017 | `kNetworkReactorInitFailed` | Event multiplexer initialization failed | `internal` | `src/server/reactor/epoll_multiplexer.cpp`, `src/server/reactor/kqueue_multiplexer.cpp` (6 sites total) |
| 6018 | `kNetworkReactorRegisterFailed` | Event multiplexer register failed | `internal` | `src/server/reactor/epoll_multiplexer.cpp`, `src/server/reactor/kqueue_multiplexer.cpp` |
| 6019 | `kNetworkReactorModifyFailed` | Event multiplexer modify failed | `TCP+` (read-backpressure interest update, `src/server/reactor_connection.cpp`), `internal` elsewhere | `src/server/reactor/epoll_multiplexer.cpp`, `src/server/reactor/kqueue_multiplexer.cpp`, `src/server/io_reactor.cpp` (8 sites total) |
| 6020 | `kNetworkReactorRemoveFailed` | Event multiplexer remove failed | `internal` | `src/server/reactor/epoll_multiplexer.cpp`, `src/server/reactor/kqueue_multiplexer.cpp` |
| 6021 | `kNetworkReactorPollFailed` | Event multiplexer poll failed | `internal` | `src/server/reactor/epoll_multiplexer.cpp`, `src/server/reactor/kqueue_multiplexer.cpp` (5 sites total) |
| 6023 | `kNetworkReactorAlreadyOpen` | Event multiplexer already opened | `internal` | `src/server/reactor/epoll_multiplexer.cpp`, `src/server/reactor/kqueue_multiplexer.cpp` |
| 6024 | `kNetworkNullDependency` | Required dependency is null | `unreferenced` | — |
| 6025 | `kNetworkAcceptorNoHandler` | Acceptor reactor handler not installed | `internal` — startup | `src/server/connection_acceptor.cpp` |
| 6026 | `kServerInitMissingDependency` | Server initialization missing required dependency | `TCP+`, `HTTP` (503 on `POST /optimize`), `internal` (startup) | `src/server/handlers/admin_handler.cpp`, `src/server/handlers/variable_handler.cpp`, `src/server/http_server.cpp` (9 sites total) |
| 6027 | `kServerShuttingDown` | Server is shutting down | `TCP+` (`SYNC`), `internal` (TCP shutdown) | `src/server/sync_operation_manager.cpp`, `src/server/tcp_server.cpp` |
| 6028 | `kServerLoading` | Server is loading | `TCP+`, `HTTP` (503) | `src/server/handlers/command_handler.cpp`, `src/server/search_pipeline.cpp`, `src/server/http_server.cpp` (4 sites total) |
| 6029 | `kServerNotReady` | Server is not ready | `TCP+`, `HTTP` (503) | `src/server/handlers/command_handler.cpp`, `src/server/handlers/replication_handler.cpp`, `src/server/http_server.cpp` (6 sites total) |
| 6030 | `kServerBusy` | Server is busy | `TCP+` (rate limit, long-operation conflict, backpressure), `HTTP` (429 on rate limit) | `src/server/request_dispatcher.cpp`, `src/server/http_server.cpp`, `src/server/handlers/dump_handler.cpp` (30 sites total) |

6009 and 6022 are not assigned.

### Client (7000-7999)

Client codes are produced inside the client library and `mygram-cli`; they never travel over the wire. They surface as `Error::code()` on the C++ SDK and through `mygramclient_get_last_error_code()` on the C ABI.

| Code | Symbolic name | Meaning | Surfaces | Emitting site(s) |
|------|---------------|---------|----------|------------------|
| 7000 | `kClientNotConnected` | Not connected | `C++ SDK`, `C ABI`, `CLI` | `src/client/mygramclient.cpp`, `src/cli/mygram-cli.cpp` |
| 7001 | `kClientConnectionFailed` | Connection failed | `C++ SDK`, `C ABI`, `CLI` | `src/client/mygramclient.cpp` (11 sites total) |
| 7002 | `kClientSendFailed` | Send failed | `C++ SDK`, `C ABI`, `CLI` | `src/client/mygramclient.cpp` |
| 7003 | `kClientReceiveFailed` | Receive failed | `C++ SDK`, `C ABI`, `CLI` | `src/client/mygramclient.cpp` |
| 7004 | `kClientInvalidResponse` | Invalid response | `C++ SDK`, `C ABI`, `CLI` | `src/client/mygramclient.cpp` (5 sites total) |
| 7005 | `kClientTimeout` | Timeout | `C++ SDK`, `C ABI`, `CLI` | `src/client/mygramclient.cpp` (4 sites total) |
| 7006 | `kClientAlreadyConnected` | Already connected | `C++ SDK`, `C ABI` | `src/client/mygramclient.cpp` |
| 7007 | `kClientCommandFailed` | Command failed | `C ABI` — allocation and command-dispatch failures inside the C wrapper | `src/client/mygramclient_c.cpp` (27 sites total) |
| 7008 | `kClientConnectionClosed` | Connection closed | `C++ SDK`, `C ABI`, `CLI` | `src/client/mygramclient.cpp` |
| 7009 | `kClientInvalidArgument` | Invalid argument | `C++ SDK`, `C ABI` | `src/client/mygramclient.cpp`, `src/client/mygramclient_c.cpp` (40 sites total) |
| 7010 | `kClientServerError` | Server error | `C++ SDK`, `C ABI`, `CLI` — assigned when a server `ERROR` frame omits a numeric code | `src/client/mygramclient.cpp` |
| 7011 | `kClientProtocolError` | Protocol error | `C++ SDK`, `C ABI`, `CLI` | `src/client/mygramclient.cpp` (15 sites total) |
| 7012 | `kClientExpressionParseError` | Expression parse error | `C++ SDK` only — the C ABI expression functions are free functions with no client handle, so they return `-1` and a `diagnostic` string, and the code itself is not reachable through `mygramclient_get_last_error_code()` | `src/client/search_expression.cpp` (10 sites total) |

### Cache (8000-8999)

| Code | Symbolic name | Meaning | Surfaces | Emitting site(s) |
|------|---------------|---------|----------|------------------|
| 8000 | `kCacheMiss` | Cache miss | `unreferenced` | — |
| 8001 | `kCacheDisabled` | Cache disabled | `TCP+` (`CACHE` commands, `SET cache.enabled`) | `src/server/handlers/cache_handler.cpp`, `src/config/runtime_variable_manager.cpp` (7 sites total) |
| 8002 | `kCacheCompressionFailed` | Cache compression failed | `internal` — the caller degrades to "not stored" (`src/cache/query_cache.cpp`) | `src/cache/result_compressor.cpp` |
| 8003 | `kCacheDecompressionFailed` | Cache decompression failed | `internal` — the caller degrades to a cache miss (`src/cache/query_cache.cpp`) | `src/cache/result_compressor.cpp` (4 sites total) |
| 8004 | `kCacheWorkerStartFailed` | Cache worker start failed | `internal` — logged, and the cache is left disabled (`src/cache/cache_manager.cpp`) | `src/cache/invalidation_queue.cpp` |

## Code-to-HTTP-status mapping

There is no table mapping error codes to HTTP statuses. The status is chosen at each call site and passed to `HttpServer::SendError` alongside the code, so the same code can appear under different statuses.

The only code-driven status function is `HttpStatusForQueryError` (`src/server/http_server.cpp`), applied to search, count, and facet pipeline failures:

```cpp
int HttpStatusForQueryError(const Error& error) {
  if (error.code() == ErrorCode::kServerShuttingDown || error.code() == ErrorCode::kServerLoading) {
    return kHttpServiceUnavailable;
  }
  if (error.code() == ErrorCode::kInternalError) {
    return kHttpInternalServerError;
  }
  return kHttpBadRequest;
}
```

A second, narrower pairing lives in `ResolveHttpTableContext` (`src/server/http_server.cpp`), which returns a status and a code together: 400 with 3001 for a malformed table name, 400 with 3000 for an unqualified name under a multi-database configuration, 404 with 4007 for an unknown table, and 500 with 5 for a table context missing its index or document store.

Every other status/code pair is a literal at the call site. The status constants are defined at `src/server/http_server.cpp`. The pairings observed across the HTTP handlers:

| Status | Codes seen with it |
|--------|--------------------|
| 400 Bad Request | 2, 3000, 3001, 3005, 3006, 3007, 3008, 3009 |
| 401 Unauthorized | 7 |
| 403 Forbidden | 7 |
| 404 Not Found | 8, 4007 |
| 415 Unsupported Media Type | 6007 |
| 429 Too Many Requests | 6030 |
| 500 Internal Server Error | 5 |
| 503 Service Unavailable | 1, 4, 5, 6026, 6028, 6029, and any code parsed out of an `OPTIMIZE` frame |

The 503 row is the widest because `POST /optimize` re-parses the TCP frame returned by the OPTIMIZE handler and reuses whatever code it finds, always under 503 (`src/server/http_server.cpp`).

## Message rendering

### TCP text protocol

`ResponseFormatter::FormatError` (`src/server/response_formatter.cpp`) produces:

```
ERROR <code> <message>
```

`<code>` is the decimal `std::uint16_t`. Every `\r`, `\n`, `\t`, and any other control byte inside `<message>` is replaced with a single space, so the frame is always one line. The `\r\n` terminator is appended by the connection layer, not by `FormatError`.

The `Error` overload (`src/server/response_formatter.cpp`) appends a non-empty context before delegating:

```
ERROR <code> <message> (context: <context>)
```

Context is therefore included on TCP whenever the emitting site populated it.

### HTTP

`HttpServer::SendError` (`src/server/http_server.cpp`) produces a JSON object with `Content-Type: application/json`:

```json
{"error":"<message>","error_code":<code>}
```

The `Error` overload (`src/server/http_server.cpp`) builds the `error` string with `ResponseFormatter::FormatErrorMessage` (`src/server/response_formatter.cpp`) — the same helper the TCP formatter uses — so a non-empty context is appended to the message:

```json
{"error":"<message> (context: <context>)","error_code":<code>}
```

Context is therefore included on HTTP whenever the call site passes an `Error`. Sites that call the four-argument overload with a literal message string have no `Error` object and so carry no context, exactly as `FormatError(std::string_view, ErrorCode)` does on TCP.

### CLI

For a server-produced frame, `mygram-cli` strips the six-byte `ERROR ` prefix and prints the remainder to stderr, so the numeric code stays visible (`src/cli/mygram-cli.cpp`):

```
(error) <code> <message>
```

For failures the client detects itself, the CLI composes the text from the message alone and the code is not printed (`src/cli/mygram-cli.cpp`):

```
(error) Not connected
(error) SERVER_DISCONNECTED: <message>
(error) SERVER_TIMEOUT: <message>
(error) <message>
```

Any context carried by a server frame is included, because it is already part of the frame's message field. Context on a locally constructed client error is dropped.

### C++ SDK

The SDK exposes the structured `Error`; `Error::to_string()` (`src/utils/error.h`) renders:

```
[<default text for the code> (<code>)] <message> (context: <context>)
```

The `<default text>` is `ErrorCodeToString(code)`; for a code outside the enum it is `Unknown error code`. The message and context segments are omitted when empty. Callers may instead read `code()`, `message()`, and `context()` separately.

### C ABI

`mygramclient_get_last_error_code()` returns the code as an `int`; `mygramclient_get_last_error()` returns a message string whose shape depends on where the error came from:

```
// Error propagated from the C++ client (src/client/mygramclient_c.cpp:128)
[<default text for the code> (<code>)] <message> (context: <context>)

// Error constructed inside the C wrapper (src/client/mygramclient_c.cpp:119)
<message>
```

Context is therefore present only for propagated errors.

## Known divergences

Factual observations about the current behaviour.

- **The C ABI's message shape is not uniform.** Propagated errors render through `Error::to_string()`, so their text is prefixed with `[<name> (<code>)]`; errors constructed inside the C wrapper render as a bare message. Both are returned by the same accessor (`src/client/mygramclient_c.cpp`).

- **Search-expression parsing has two codes for one condition.** Server-side expression parsing failures use 3010 `kQueryExpressionParseError` (`src/server/search_pipeline.cpp`); client-side parsing of the same expression grammar uses 7012 `kClientExpressionParseError` (`src/client/search_expression.cpp`).

- **7012 has no C ABI representation.** `mygramclient_parse_search_expression_ex` and `mygramclient_convert_search_expression_ex` are free functions with no client handle, so they return `-1` plus a `diagnostic` string and never set the last-error code (`src/client/mygramclient_c.cpp`).

- **`DUMP SAVE` discards the underlying storage code.** `DUMP LOAD`, `DUMP VERIFY`, and `DUMP INFO` forward `result.error().code()` verbatim (`src/server/handlers/dump_handler.cpp`), but the synchronous `DUMP SAVE` path returns a fixed 5012 with the message `"Dump save failed"` regardless of cause.

- **Index-payload errors are masked on both dump directions, document-store errors are not.** Index serialization failures become 5012 (`src/storage/dump_format_v2.cpp`) and index deserialization failures become 5011 (`src/storage/dump_format_internal.cpp`), so 4002, 4003, 5004, and 5008 never reach a client. The document-store loader is returned unchanged (`src/storage/dump_format_internal.cpp`), so 5001 and 5003 do reach the client from the same command.

- **Document-store save failures are handled two ways within one file.** `src/storage/dump_format_v2.cpp` replaces the underlying error with 5012, while `src/storage/dump_format_v2.cpp` returns it unchanged.

- **Replication connect failures record one code and return another.** On a failed binlog or metadata connection, `SetLastError` stores 2000 while the value returned to the caller — and thus to a `REPLICATION START` frame — is whatever `Connection::Connect` produced (`src/mysql/binlog_reader.cpp`). The `last_error_code` field and the error frame can therefore disagree for the same event.

- **Row-decode codes are dropped in favour of 2008.** `SetLastError` has a single overload and always carries an explicit code (`src/mysql/binlog_reader.h`), but the row-decode path does not hand it the decoder's `Error`: when a monitored `ROWS_EVENT` fails to decode, the reader constructs a fresh 2008 `kMySQLBinlogError` and publishes that as the replication status (`src/mysql/binlog_reader_threads.cpp`). The 2013, 2014, and 2015 codes raised inside the decoder therefore never reach `last_error_code` from this path. The condition survives only as the decoder's own structured log event, which names the column and event type but not the numeric code (`src/mysql/rows_parser.cpp`).

- **`HttpStatusForQueryError`'s 6027 branch is unreachable.** The function maps `kServerShuttingDown` to 503 (`src/server/http_server.cpp`), but the only constructors of 6027 are `SyncOperationManager::StartSync` and `TcpServer::Stop` (`src/server/sync_operation_manager.cpp`, `src/server/tcp_server.cpp`), neither of which feeds the search, count, or facet pipelines that call the function.

- **Any 16-bit value the server sends becomes an `ErrorCode` on the client.** `ParseServerErrorResponse` casts the frame's numeric token without checking membership in the enum (`src/client/mygramclient.cpp`). A code the client's build does not know renders as `Unknown error code` in `Error::to_string()` while `code()` returns the raw number.

- **Two codes sit outside the range their name implies.** 4014 `kSyncManagerNull` reports a null-dependency condition that the Network/Server range covers elsewhere (6024 `kNetworkNullDependency`); the header records this as deliberate (`src/server/handlers/sync_handler.h`). 6011 `kNetworkAlreadyRunning` is also used by `PeriodicWorker`, a utility with no network role (`src/utils/periodic_worker.cpp`).

- **No duplicate numeric values.** All 129 enumerators have distinct values. Three numbers inside allocated ranges are unassigned: 4009, 6009, and 6022.

- **20 codes are defined but never constructed:** 10, 2004, 2022, 3002, 3003, 3004, 3011, 4001, 4005, 4006, 5006, 5007, 6002, 6003, 6004, 6005, 6006, 6008, 6024, 8000. All except 2022 and 6024 appear in `tests/utils/error_test.cpp`, which asserts their `ErrorCodeToString` text; none has a production construction site. 2004's only appearance is a comparison (`src/mysql/binlog_reader_utils.cpp`).
