# HTTP Route Specification

This file is normative. It describes the HTTP surface as the code in `src/server/` behaves today, derived by reading that code; where prose elsewhere in the repository disagrees, this file follows the code. Every statement names the file that implements it, so the spec can be re-verified against the source; see `spec/README.md` for the citation rules and the check that enforces them.

The HTTP surface is served by `HttpServer`, which embeds cpp-httplib at the version pinned in `third_party/CMakeLists.txt`. Routes are declared in the `HttpServer::Routes()` descriptor table (`src/server/http_server.cpp`) and registered from it, in table order, by `HttpServer::SetupRoutes`. cpp-httplib matches in registration order, so the table order is behavioural.

## Route index

| Method | Path | Handler | Auth | Rate limited | Counted in `total_requests` | Counted as command |
|---|---|---|---|---|---|---|
| POST | `/tables/{identity}/search` | `HandleSearch` | none | yes | yes | `SEARCH` |
| POST | `/tables/{identity}/count` | `HandleCount` | none | yes | yes | `COUNT` |
| POST | `/tables/{identity}/facet` | `HandleFacet` | none | yes | yes | `FACET` |
| GET | `/tables/{identity}/{primary_key}` | `HandleGet` | none | yes | yes | `GET` |
| GET | `/info` | `HandleInfo` | none | yes | yes | `INFO` |
| GET | `/health` | `HandleHealth` | none | no | no | no |
| GET | `/health/live` | `HandleHealthLive` | none | no | no | no |
| GET | `/health/ready` | `HandleHealthReady` | none | no | no | no |
| GET | `/health/detail` | `HandleHealthDetail` | none | yes | no | no |
| GET | `/config` | `HandleConfig` | bearer token | yes | yes | `CONFIG_SHOW` |
| GET | `/replication/status` | `HandleReplicationStatus` | bearer token | yes | yes | `REPLICATION_STATUS` |
| POST | `/optimize` | `HandleOptimize` | bearer token | yes | yes | `OPTIMIZE` |
| GET | `/metrics` | `HandleMetrics` | none | yes | yes | no |
| OPTIONS | `.*` | CORS preflight, only when CORS is active | none | yes | no when served, yes when denied | no |

Every entry in this table is implemented in `src/server/http_server.cpp`.

`OPTIONS .*` is registered only when `enable_cors` is true **and** `cors_allow_origin` is non-empty (`src/server/http_server.cpp`).

The *Rate limited* and *Counted in `total_requests`* columns are declared per route in the same descriptor table, whose fields are `HttpServer::RouteDescriptor` (`src/server/http_server.h`), and are read by the pre-routing handler through `FindLiteralRoute` (`src/server/http_server.cpp`), so a route's accounting is the same whether the request is served or rejected. A path the table does not name — including every regex route and every unmatched path — is counted and rate limited.

`OPTIONS` is the exception to that generalization. `FindLiteralRoute` compares the request method against `GET` or `POST` only (`src/server/http_server.cpp`), so a preflight never resolves to a descriptor and is treated as an unnamed path: a preflight denied by the network ACL or the rate limiter increments `total_requests`, while a preflight that is served does not, because the CORS handler records nothing (`HttpServer::SetupCors`, `src/server/http_server.cpp`).

The *Auth* column is the descriptor's `requires_admin_token` flag, declared in the same table and enforced by the shared route wrapper before the handler is dispatched (`src/server/http_server.cpp`). Three routes carry it: `GET /config`, `GET /replication/status` and `POST /optimize`. The credential gate lives only in that wrapper, so an uncredentialed request on those three routes never reaches handler code — see *Authentication*. Two handlers read the credential without gating on it: `HandleHealthReady` and `HandleHealthDetail` call `AdminCredentialsAccepted` to decide which replication fields to expose, not whether to admit the request (`src/server/http_server.cpp`).

### Path matching rules

- Fixed paths (`/info`, `/health`, `/health/live`, `/health/ready`, `/health/detail`, `/config`, `/replication/status`, `/optimize`, `/metrics`) are registered as exact literal strings (`src/server/http_server.cpp`). A trailing slash does not match: `/info/` is not `/info`.
- Method and path are both case-sensitive as registered. cpp-httplib accepts only the methods `GET, HEAD, POST, PUT, DELETE, CONNECT, OPTIONS, TRACE, PATCH, PRI` on the request line; anything else fails request-line parsing.
- The table routes use the regex `/tables/([^/]+)/search`, `.../count`, `.../facet` (`src/server/http_server.cpp`) and `/tables/([^/]+)/(.+)` for GET. `{identity}` is capture group 1 (`src/server/http_server.cpp`); the GET primary key is capture group 2. The GET key group is greedy, so a primary key containing `/` — including one written as `%2F`, which is decoded before matching — is captured whole rather than failing to match.
- The request path is percent-decoded and its `#fragment` stripped before routing; the query string is split off into `req.params`. No handler reads `req.params`, so **query-string parameters are ignored on every route**.
- The GET document route is last in the table (`src/server/http_server.cpp`), so it cannot shadow the fixed endpoints. It does match `GET /tables/{identity}/search`, which is treated as a document lookup for the primary key `search`, and — being greedy — `GET /tables/{identity}/search/anything`, as a lookup for `search/anything`.
- Unmatched method/path combinations receive cpp-httplib's default `404` with an empty body. No `set_error_handler` is registered, so these 404s are **not** JSON.

---

## POST /tables/{identity}/search

Full-text search. Request body must be a JSON object; `Content-Type` must be `application/json` (parameters after `;` are ignored, matching is ASCII-case-insensitive) — `src/server/http_server.cpp`.

### Request body fields

| Field | Type | Required | Default | Validation |
|---|---|---|---|---|
| `q` | string | yes | — | Presence; type; no `\r`, `\n`, `\0`; non-empty. Length is checked once, on the assembled expression (see *Query construction*) |
| `mode` | string | no | `"literal"` | Must be `"literal"` or `"boolean"` |
| `limit` | integer | no | `api.default_limit` | Must be an integer; `1 <= limit <= 1000` (`config::defaults::kMaxLimit`, `src/config/config.h`) |
| `offset` | integer | no | `0` | Must be an integer; `0 <= offset <= 4294967295` |
| `filters` | object | no | — | Must be an object; parsed by `ParseFiltersFromJson` |
| `sort` | object | no | primary key DESC | `ParseSortFromJson` |
| `highlight` | object | no | — | `ParseHighlightFromJson` |
| `fuzzy` | integer | no | — | Must be an integer; must be `1` or `2` |

Every citation in this table is `src/server/http_server.cpp` unless the cell names another file.

Unknown fields are ignored on this route (there is no allowlist check; contrast `/optimize`, `src/server/http_server.cpp`).

**`filters`** accepts two shapes per column (`src/server/http_server.cpp`):

- `{"col": <scalar>}` — operator defaults to `EQ` (`src/server/http_server.cpp`).
- `{"col": {"op": "<OP>", "value": <scalar>}}` — `op` defaults to `"EQ"` when absent (`src/server/http_server.cpp`); parsed by `QueryParser::ParseFilterOp` (`src/query/query_parser_clauses.cpp`).

Caps applied to `filters`:

| Rule | Limit | Citation |
|---|---|---|
| Number of conditions | ≤ 64 (`QueryParser::kMaxTermCount`) | `ParseFiltersFromJson`, `src/server/http_server.cpp` |
| Column name | `IsSafeColumnName`: 1–128 bytes of `[A-Za-z0-9_.$-]` (`src/query/query_parser.cpp`) | `QueryParser::ValidateFilterCondition` (`src/query/query_parser.cpp`), shared with the TCP `FILTER` clause. On the HTTP surface the column is rejected earlier, by `ParseFiltersFromJson` (`src/server/http_server.cpp`), with the same `kQueryInvalidFilter` |
| Value type | string, integer, float, boolean only (`JsonFilterValueToString`) | `src/server/http_server.cpp` |
| Value length | ≤ 1024 bytes (`kMaxFilterValueLength`) | `QueryParser::ValidateFilterCondition`, shared with the TCP `FILTER` clause |

Non-string filter values are coerced to strings: integers via `std::to_string(int64_t)`, floats via `std::to_string(double)` (fixed six decimals), booleans to `"1"`/`"0"` (`src/server/http_server.cpp`). Because `filters` is a JSON object, at most one condition per column can be expressed.

**`sort`** (`src/server/http_server.cpp`):

| Field | Type | Required | Validation |
|---|---|---|---|
| `column` | string | no | `QueryParser::IsSafeSortColumn` — `_score` or `IsSafeColumnName` — shared with the TCP `SORT` clause |
| `order` | string | no | `QueryParser::ParseSortOrder` — `ASC`/`DESC`, ASCII-case-insensitive; default `DESC` — shared with the TCP `SORT` clause |

Omitting `column` orders by the primary key, mirroring the TCP shorthand `SORT ASC` / `SORT DESC` (`ParseSortFromJson`, `src/server/http_server.cpp`). A named column is passed through as written and resolved by `ResultSorter`, which accepts the primary-key column name as well as any filter column (`src/query/result_sorter.cpp`).

**`highlight`** (`src/server/http_server.cpp`):

| Field | Type | Default | Range | Citation |
|---|---|---|---|---|
| `open_tag` | string | `<em>` | ≤ 256 bytes (`kMaxHighlightTagLength`) | `ParseHighlightFromJson` |
| `close_tag` | string | `</em>` | ≤ 256 bytes (`kMaxHighlightTagLength`) | `ParseHighlightFromJson` |
| `snippet_length` | integer | `100` | `kMinSnippetLength`–`kMaxSnippetLength` (1–10000), shared with `HIGHLIGHT SNIPPET_LEN` | `ParseHighlightUint` |
| `max_fragments` | integer | `3` | `kMinHighlightFragments`–`kMaxHighlightFragments` (1–100), shared with `HIGHLIGHT MAX_FRAGMENTS` | `ParseHighlightUint` |

Every citation in this table is `src/server/http_server.cpp`. The defaults are the member initializers of `query::HighlightOptions` (`src/query/query_parser.h`), so an omitted field takes the same value the TCP `HIGHLIGHT` clause leaves in place.

### Query construction

- `mode: "literal"` (default): the request is turned into the text command `SEARCH <resolved_table> "<escaped q>"` and run through `QueryParser::Parse`, where `<escaped q>` is produced by `QueryParser::QuoteSearchLiteral`, the escaper the parser itself uses to re-emit a quoted token (`src/query/query_parser_commands.cpp`). A parser error is returned as 400 with the parser's own error code.
- `mode: "boolean"`: `q` is assigned directly to both `search_text` and `search_expression` without invoking the parser (`src/server/http_server.cpp`).

After construction, `ApplyHttpQueryOptions` applies pagination, filters, sort, highlight and fuzzy, then validates the assembled query against `api.max_query_length` via `QueryParser::ValidateQueryLength` (`src/server/http_server.cpp`, `src/query/query_parser.cpp`). This is the only length check on the route, and it counts code points, not bytes (`src/query/query_parser.cpp`).

### Success response

`200`, `Content-Type: application/json`, body serialized compact with invalid UTF-8 replaced (`src/server/http_server.cpp`).

```json
{
  "count": 42,
  "limit": 100,
  "offset": 0,
  "results": [
    {
      "primary_key": "doc_1",
      "filters": { "category": "tech", "views": 120, "published": true, "rating": 4.5, "archived_at": null },
      "highlight": "an <em>example</em> snippet"
    }
  ]
}
```

- `count` is the pre-pagination total, or the top-N total when that optimization applied (`src/server/http_server.cpp`).
- `limit` and `offset` echo the effective values (`src/server/http_server.cpp`).
- `filters` is present only when the document has at least one filter value (`src/server/http_server.cpp`). `std::monostate` serializes as `null`, `TimeValue` as its integer `seconds`, everything else as its native JSON type (`src/server/http_server.cpp`).
- `highlight` is present only when the request carried `highlight`; it is `""` when the document has neither original nor normalized stored text (`src/server/http_server.cpp`).
- Documents that fail to load are skipped (`src/server/http_server.cpp`), so `results.length` can be smaller than `limit` even when `count` is larger.

### Error responses

| Status | Condition | `error_code` | Citation |
|---|---|---|---|
| 400 | Invalid JSON body | `kQuerySyntaxError` (3000) | `src/server/http_server.cpp` |
| 400 | Missing / non-string `q`, bad `mode` | `kQuerySyntaxError` (3000) | `src/server/http_server.cpp` |
| 400 | `q` contains `\r`/`\n`/`\0` | `kQueryInvalidToken` (3001) | `src/server/http_server.cpp` |
| 400 | `q` empty | `kQuerySyntaxError` (3000) | `src/server/http_server.cpp` |
| 400 | Assembled query exceeds `api.max_query_length` characters | `kQueryTooLong` (3005) | `src/query/query_parser.cpp` |
| 400 | Bad `limit` | `kQueryInvalidLimit` (3008) | `HttpServer::ApplyHttpQueryOptions`, `src/server/http_server.cpp` |
| 400 | Bad `offset` | `kQueryInvalidOffset` (3009) | `HttpServer::ApplyHttpQueryOptions`, `src/server/http_server.cpp` |
| 400 | `filters` not an object / bad condition | `kQueryInvalidFilter` (3006) | `HttpServer::ApplyHttpQueryOptions` and `ParseFiltersFromJson`, `src/server/http_server.cpp` |
| 400 | Bad `sort` | `kQueryInvalidSort` (3007) | `ParseSortFromJson`, `src/server/http_server.cpp` |
| 400 | Bad `highlight` / `fuzzy` | `kQuerySyntaxError` (3000) | `ParseHighlightFromJson` / `ParseFuzzyFromJson`, `src/server/http_server.cpp` |
| 400 | Invalid table name | `kQueryInvalidToken` (3001) | `src/server/http_server.cpp` |
| 400 | Bare table name under a multi-database configuration | `kQuerySyntaxError` (3000) | `HttpServer::ResolveHttpTableContext`, `src/server/http_server.cpp` |
| 400 | `HIGHLIGHT` requested but normalized text storage is off | `kNotImplemented` (4) | `HttpServer::HandleSearch`, `src/server/http_server.cpp` |
| 400 | `sort._score` with BM25 disabled | `kQueryInvalidSort` (3007) | `src/server/search_pipeline.cpp` |
| 400 | `sort._score` with normalized text storage off | `kNotImplemented` (4) | `src/server/search_pipeline.cpp` |
| 400 | `sort._score` when the table's index or BM25 statistics are missing | `kTableNotFound` (4007) | `src/server/search_pipeline.cpp` |
| 400 | Any other pipeline / sorter error | error's own code | `src/server/search_pipeline.cpp` |
| 404 | Table not resolvable | `kTableNotFound` (4007) | `src/server/http_server.cpp` |
| 415 | `Content-Type` not `application/json` | `kNetworkInvalidRequest` (6007) | `src/server/http_server.cpp` |
| 500 | Resolved table has a null index or document store | `kInternalError` (5) | `HttpServer::ResolveHttpTableContext`, `src/server/http_server.cpp` |
| 500 | Unhandled exception in the handler | `kInternalError` (5) | `HttpServer::HandleSearch`'s `catch`, `src/server/http_server.cpp` |
| 503 | Server loading a dump | `kServerLoading` (6028) | `HttpServer::PrepareHttpJsonRequest`, `src/server/http_server.cpp` |
| 503 | Table is synchronizing | `kServerNotReady` (6029) | `HttpServer::RejectIfTableSyncing`, `src/server/http_server.cpp` |

Error bodies always have exactly two fields (`src/server/http_server.cpp`):

```json
{ "error": "Invalid limit: must be between 1 and 1000", "error_code": 3008 }
```

---

## POST /tables/{identity}/count

Same preamble, table resolution, `q`, `mode` and `filters` handling as `/search` (`src/server/http_server.cpp` calls `PrepareHttpSearchQuery` with `apply_pagination=false`).

Additional validation: the fields `limit`, `offset`, `sort`, `highlight` and `fuzzy` are **rejected**, each with `kQuerySyntaxError` and status 400 (`src/server/http_server.cpp`). Because `apply_pagination` is false, no default limit is applied and `sort`/`highlight`/`fuzzy` parsing is skipped (`src/server/http_server.cpp`).

Success: `200`, `application/json`.

```json
{ "count": 42 }
```

`count` is the full match count with no pagination applied (`src/server/http_server.cpp`).

Error responses are the same set as `/search` minus the pagination, sort, highlight and fuzzy rows, plus the rejected-field row above.

---

## POST /tables/{identity}/facet

Facet value counts. Preamble identical to `/search` (`src/server/http_server.cpp` calls `PrepareHttpJsonRequest`).

### Request body fields

| Field | Type | Required | Default | Validation |
|---|---|---|---|---|
| `column` | string | yes | — | Presence; type; `IsSafeColumnName` |
| `q` | string | no | — | Must be a string; empty allowed; same control-character and length rules as `/search` |
| `mode` | string | no | `"literal"` | Must be a string, and exactly `"literal"` or `"boolean"` (`ParseHttpQueryMode`) |
| `limit` | integer | no | `api.default_limit` | Same rules as `/search` (`src/server/http_server.cpp` passes `apply_pagination=true`) |
| `offset` | integer | no | `0` | Same rules as `/search` |
| `filters` | object | no | — | Same rules as `/search` |

Every citation in this table is `src/server/http_server.cpp`.

`sort`, `highlight` and `fuzzy` are rejected with `kQuerySyntaxError` / 400 (`src/server/http_server.cpp`).

In literal mode the search expression is quoted with the same escaper used by `/search` (`src/server/http_server.cpp`); in boolean mode `q` is used verbatim. Unlike `/search`, the facet path never invokes `QueryParser::Parse` — the query is assembled field by field (`src/server/http_server.cpp`).

`limit`/`offset` are applied to the *value list*, after `total_count` has been recorded (`src/server/search_pipeline.cpp`).

Success: `200`, `application/json`.

```json
{
  "column": "category",
  "count": 3,
  "total_count": 17,
  "facets": [
    { "value": "tech", "count": 120 },
    { "value": "sports", "count": 44 },
    { "value": "food", "count": 9 }
  ]
}
```

- `count` is the number of returned facet entries after pagination (`src/server/http_server.cpp`).
- `total_count` is the number of distinct values before pagination (`src/server/http_server.cpp`, `src/server/search_pipeline.cpp`).

Errors: as `/search`, plus three of this route's own (all 400):

| Condition | `error_code` | Citation |
|---|---|---|
| Missing / non-string `column` | `kQuerySyntaxError` (3000) | `PrepareHttpFacetQuery`, `src/server/http_server.cpp` |
| `column` fails `IsSafeColumnName` — `Invalid facet column` | `kQueryInvalidToken` (3001) | `PrepareHttpFacetQuery`, `src/server/http_server.cpp` |
| `column` is well-formed but resolves to no configured filter column — `Facet column "…" not found` | `kIndexNotFound` (4000) | `ExecuteFacetPipeline`, `src/server/search_pipeline.cpp` |

The last of these is raised inside the pipeline, so it reaches the client through `HttpStatusForQueryError` and is 400 rather than 404 (`src/server/http_server.cpp`).

---

## GET /tables/{identity}/{primary_key}

Fetch one document by primary key. No body, no `Content-Type` requirement (`src/server/http_server.cpp`).

Both path segments are percent-decoded by cpp-httplib before matching. `{identity}` goes through the same `QueryParser::IsSafeTableName` / qualification / catalog resolution as the POST routes (`src/server/http_server.cpp`). `{primary_key}` is everything after the table segment, used verbatim with no validation (`src/server/http_server.cpp`); because decoding happens before matching and the route's key group is greedy, a key containing `/` is reachable as `%2F`.

Success: `200`, `application/json`.

```json
{
  "primary_key": "doc_1",
  "filters": { "category": "tech", "views": 120 }
}
```

`filters` is omitted when the document has none (`src/server/http_server.cpp`).

| Status | Condition | `error_code` |
|---|---|---|
| 400 | Invalid table name | `kQueryInvalidToken` (3001) |
| 400 | Bare table name under multi-database config | `kQuerySyntaxError` (3000) |
| 404 | Table not resolvable | `kTableNotFound` (4007) |
| 404 | No such primary key, or document fetch returned empty | `kIndexDocumentNotFound` (4004) |
| 500 | Null index or document store; unhandled exception | `kInternalError` (5) |
| 503 | Server loading | `kServerLoading` (6028) |
| 503 | Table synchronizing | `kServerNotReady` (6029) |

Every row in this table is implemented in `src/server/http_server.cpp`.

---

## GET /info

Server, memory, index, per-table and cache statistics. No parameters. Success `200`, `application/json` (`src/server/http_server.cpp`).

Statistics are read from the shared TCP `ServerStats` when one was injected, otherwise from the HTTP-local instance (`src/server/http_server.h`). Memory accounting comes from a bounded snapshot cache shared with `/metrics` (`src/server/http_server.cpp`).

```json
{
  "server": "MygramDB",
  "version": "1.2.3",
  "uptime_seconds": 3600,
  "total_requests": 1024,
  "total_commands_processed": 1000,
  "memory": {
    "used_memory_bytes": 104857600,
    "used_memory_human": "100.00 MB",
    "peak_memory_bytes": 125829120,
    "peak_memory_human": "120.00 MB",
    "used_memory_index": "60.00 MB",
    "used_memory_documents": "40.00 MB",
    "total_system_memory": 17179869184,
    "total_system_memory_human": "16.00 GB",
    "available_system_memory": 8589934592,
    "available_system_memory_human": "8.00 GB",
    "system_memory_usage_ratio": 0.5,
    "process_rss": 157286400,
    "process_rss_human": "150.00 MB",
    "process_rss_peak": 167772160,
    "process_rss_peak_human": "160.00 MB",
    "memory_health": "HEALTHY"
  },
  "index": {
    "total_documents": 1000,
    "total_terms": 5000,
    "total_postings": 40000,
    "avg_postings_per_term": 8.0,
    "delta_encoded_lists": 4000,
    "roaring_bitmap_lists": 1000
  },
  "tables": {
    "app.articles": {
      "documents": 1000,
      "terms": 5000,
      "postings": 40000,
      "ngram_size": 2,
      "memory_bytes": 104857600,
      "memory_human": "100.00 MB"
    }
  },
  "cache": {
    "enabled": true,
    "hits": 10, "misses": 5,
    "misses_not_found": 3, "misses_ttl_expired": 1, "misses_invalidated": 1,
    "total_queries": 15, "hit_rate": 0.6667,
    "current_entries": 8,
    "memory_bytes": 4096, "memory_human": "4.00 KB",
    "invalidation_index_memory_bytes": 512,
    "invalidation_queue_memory_bytes": 128,
    "accounted_memory_bytes": 4736, "accounted_memory_human": "4.62 KB",
    "evictions": 0, "ttl_expirations": 1,
    "rejection_count": 0, "rejection_oversize": 0,
    "rejection_memory_budget": 0, "rejection_duplicate": 0,
    "stale_entry_removals": 0, "decompression_failures": 0, "stale_lru_entries": 0,
    "invalidations_immediate": 2, "invalidations_deferred": 0, "invalidations_batches": 1,
    "avg_hit_latency_ms": 0.12, "avg_miss_latency_ms": 3.4, "total_time_saved_ms": 32.8
  }
}
```

Conditional fields:

- The system-memory block (`total_system_memory` … `system_memory_usage_ratio`) appears only when `GetSystemMemoryInfo()` succeeds; `system_memory_usage_ratio` additionally requires a non-zero total (`src/server/http_server.cpp`).
- The process block (`process_rss*`) appears only when `GetProcessMemoryInfo()` succeeds (`src/server/http_server.cpp`).
- `index.avg_postings_per_term` appears only when `total_terms > 0` (`src/server/http_server.cpp`).
- When the cache manager is absent or disabled, `cache` is exactly `{"enabled": false}` (`src/server/http_server.cpp`).
- A table whose metrics are missing from the snapshot is omitted from `tables` (`src/server/http_server.cpp`).

Errors: `500` `kInternalError` on an unhandled exception (`src/server/http_server.cpp`).

---

## GET /health

Always `200`, `application/json` (`src/server/http_server.cpp`).

```json
{ "status": "ok", "timestamp": 1756150000 }
```

`timestamp` is Unix seconds from `system_clock` (`src/server/http_server.cpp`).

## GET /health/live

Always `200` while the process serves requests (`src/server/http_server.cpp`).

```json
{ "status": "alive", "timestamp": 1756150000 }
```

## GET /health/ready

`200` when ready, `503` when not (`src/server/http_server.cpp`).

Readiness is not decided here. `HttpServer::CurrentReadinessInputs` collects the binlog reader, the dump-pause flag, the sync state and the initial-data checker (`src/server/http_server.cpp`), and `EvaluateReadiness` classifies them (`src/server/readiness.cpp`); this route renders the verdict. The same verdict backs `GET /health/detail` (`src/server/http_server.cpp`) and the TCP `INFO` command (`src/server/handlers/admin_handler.cpp`), so the three cannot disagree.

Readiness is `data_initialized && !loading && !sync_in_progress && replication_available` (`src/server/readiness.cpp`). Replication availability is one of seven states (`src/server/readiness.h`), classified in this order (`src/server/readiness.cpp`):

| Order | State | Condition | Available |
|---|---|---|---|
| 1 | `disabled` | no binlog reader wired, or a build without MySQL support | yes |
| 2 | `connected` | `IsRunning()` | yes |
| 3 | `starting` | `IsStarting()` | yes |
| 4 | `paused_for_dump` | `replication_paused_for_dump` | yes |
| 5 | `paused_for_sync` | a SYNC is in progress; it stops the reader through `replication_pause::Scope` without raising the dump flag | yes |
| 6 | `failed` | `GetReplicationState() == kFailed` | no |
| 7 | `disconnected` | otherwise | no |

A SYNC therefore leaves replication *available* while still making the server *not ready*: the table being rebuilt cannot answer queries yet.

```json
{
  "loading": false,
  "data_initialized": true,
  "replication_running": true,
  "replication_starting": false,
  "replication_paused_for_dump": false,
  "sync_in_progress": false,
  "replication_last_error": "",
  "replication_last_error_code": 0,
  "replication_crc_errors": 0,
  "replication_schema_incompatible": false,
  "replication_last_applied_unixtime": 1756149998,
  "replication_seconds_since_last_applied": 2,
  "status": "ready",
  "timestamp": 1756150000
}
```

The whole `replication_*` block is present only in `USE_MYSQL` builds with a non-null binlog reader (`src/server/http_server.cpp`). `replication_running` reports availability rather than the reader's raw running flag (`src/server/http_server.cpp`), so it stays `true` through the `starting`, `paused_for_dump` and `paused_for_sync` states (`src/server/readiness.cpp`).

`replication_last_error` is the *description of the error code*, produced by `ReplicationErrorSummary` via `ErrorCodeToString` (`src/server/readiness.cpp`), and is empty when the reader reports no error. It is deliberately not the reader's own message: that text is verbatim MySQL output and can name the replication account and the address the server connects from, so it is confined to the credentialed `GET /replication/status`.

This route requires no token, but two of its fields are credential-conditional: `replication_crc_errors` and `replication_schema_incompatible` are emitted only when the request carries accepted admin credentials (`src/server/http_server.cpp`). They are part of what `GET /replication/status` protects, so an uncredentialed probe does not receive them here either.

The remaining conditional fields are `reason`, added only when the verdict is not ready (see below), and the `replication_*` block as a whole. `loading`, `data_initialized`, `status` and `timestamp` are unconditional (`src/server/http_server.cpp`).

When not ready, `status` is `"not_ready"` and a `reason` field is added, chosen in this order (`src/server/readiness.cpp`):

| Order | `reason` | Condition |
|---|---|---|
| 1 | `Initial data has not been loaded` | `!initial_data_ready` |
| 2 | `Server is loading` | `is_loading` |
| 3 | `Replication stopped due to an incompatible schema` | `HasSchemaIncompatibleError()` |
| 4 | the error code's own description, never MySQL's text | `GetLastError()` non-empty (`src/server/readiness.cpp`) |
| 5 | `SYNC is in progress` | `sync_in_progress` |
| 6 | `Replication is not running` | otherwise |

## GET /health/detail

Always `200`, `application/json` (`src/server/http_server.cpp`).

```json
{
  "status": "healthy",
  "timestamp": 1756150000,
  "uptime_seconds": 3600,
  "components": {
    "server": { "status": "ready", "loading": false },
    "index": { "status": "ok", "total_terms": 5000, "total_documents": 1000 },
    "cache": {
      "status": "ok", "enabled": true, "hit_rate": 0.6667,
      "total_hits": 10, "total_misses": 5, "current_entries": 8
    },
    "binlog": {
      "status": "connected",
      "running": true,
      "starting": false,
      "current_gtid": "uuid:1-100",
      "processed_events": 1234,
      "queue_size": 0,
      "replication_state": "running",
      "crc_errors": 0,
      "schema_incompatible": false,
      "last_error_code": 0,
      "last_error": "",
      "last_applied_unixtime": 1756149998,
      "seconds_since_last_applied": 2
    }
  }
}
```

- Top-level `status` is `"healthy"` when the readiness verdict is ready and `"degraded"` otherwise — the same verdict `GET /health/ready` renders, so this route never reports a fault the readiness probe calls healthy (`src/server/http_server.cpp`).
- `reason` is present only when `status` is `"degraded"`, and carries the same string `/health/ready` puts in its own `reason` (`src/server/http_server.cpp`).
- `uptime_seconds` here is measured from this `HttpServer`'s construction, not from the shared server stats (`src/server/http_server.cpp`).
- `components.cache` appears only when a cache manager is wired (`src/server/http_server.cpp`).
- `components.binlog` appears only in `USE_MYSQL` builds with a non-null reader (`src/server/http_server.cpp`). `status` is the availability state's own name — `connected`, `starting`, `paused_for_dump`, `paused_for_sync`, `failed` or `disconnected` (`src/server/http_server.cpp`, `src/server/readiness.cpp`). Outside `connected`/`starting` the object carries `running: false` and `paused_for_dump` instead of `starting`/`current_gtid`/`processed_events`/`queue_size` (`src/server/http_server.cpp`).
- `components.binlog.replication_state` is the reader's own three-state lifecycle value, rendered by `ToString(ReplicationState)` as one of `running`, `stopped` or `failed` (`src/mysql/binlog_reader_interface.h`), and is independent of the availability classification (`src/server/http_server.cpp`).
- `components.binlog.last_error` is the *description of the error code*, produced by `ReplicationErrorSummary` via `ErrorCodeToString` (`src/server/readiness.cpp`), the same string `/health/ready` reports as `replication_last_error`. The reader's raw MySQL text is exposed only by `GET /replication/status`.
- This route requires no token, but the binlog fields that `GET /replication/status` protects are credential-conditional: `current_gtid`, `processed_events` and `queue_size` (`src/server/http_server.cpp`), and `crc_errors` and `schema_incompatible`, are emitted only when the request carries accepted admin credentials.
- Credentials aside, the conditional parts of the body are the ones named above: `reason`, `components.cache`, `components.binlog`, and the availability-driven split inside `components.binlog`. `status`, `timestamp`, `uptime_seconds`, `components.server` and `components.index` are unconditional (`src/server/http_server.cpp`).

---

## GET /config

Redacted configuration summary. No parameters. `200`, `application/json` (`src/server/http_server.cpp`).

Requires a bearer token when `api.admin_token` is configured (`src/server/http_server.cpp`). The check runs in the shared route wrapper, so an uncredentialed request gets `401` without the handler assembling any part of the body, and without `CONFIG_SHOW` being counted (`src/server/http_server.cpp`).

```json
{
  "mysql": { "configured": true, "database_defined": true },
  "api": { "tcp": { "enabled": true }, "http": { "enabled": true, "cors_enabled": false } },
  "network": { "allow_cidrs_configured": false },
  "replication": { "enable": true },
  "notes": "Sensitive configuration values are redacted over HTTP. Use CONFIG SHOW over a secured connection for details."
}
```

`api.tcp.enabled` is a hard-coded `true` (`src/server/http_server.cpp`). No credential, bind or port value is exposed.

| Status | Condition | `error_code` |
|---|---|---|
| 401 | Missing or wrong bearer token (adds `WWW-Authenticate: Bearer`) | `kPermissionDenied` (7) |
| 500 | No configuration wired | `kInternalError` (5) |
| 500 | Unhandled exception | `kInternalError` (5) |

Every row in this table is implemented in `src/server/http_server.cpp`.

---

## GET /replication/status

`200`, `application/json` (`src/server/http_server.cpp`).

Requires a bearer token when `api.admin_token` is configured (`src/server/http_server.cpp`). The check runs in the shared route wrapper, so an uncredentialed request gets `401` without the handler reading the binlog reader, and without `REPLICATION_STATUS` being counted (`src/server/http_server.cpp`).

```json
{
  "enabled": true,
  "status": "running",
  "current_gtid": "uuid:1-100",
  "processed_events": 1234,
  "queue_size": 0,
  "crc_errors": 0,
  "schema_incompatible": false,
  "last_error_code": 0,
  "last_error": "",
  "last_applied_unixtime": 1756149998,
  "seconds_since_last_applied": 2
}
```

- `enabled` reflects `IsRunning()`, not configuration (`src/server/http_server.cpp`).
- `status` is the reader's lifecycle value rendered by `ToString(ReplicationState)`, one of `running`, `stopped` or `failed` (`src/mysql/binlog_reader_interface.h`).
- `last_error` is the reader's raw `GetLastError()` text, which is MySQL's own message (`src/server/http_server.cpp`). This route is the only place that surface text is exposed: `GET /health/ready` and `GET /health/detail` report the same fault as the error code's description instead.

| Status | Condition | `error_code` |
|---|---|---|
| 401 | Missing or wrong bearer token (adds `WWW-Authenticate: Bearer`) | `kPermissionDenied` (7) |
| 500 | Unhandled exception | `kInternalError` (5) |
| 503 | Replication not configured (null binlog reader) | `kNotImplemented` (4) |
| 503 | Built without MySQL support | `kNotImplemented` (4) |

Every row in this table is implemented in `src/server/http_server.cpp`.

---

## POST /optimize

Runs the shared maintenance handler for one table or for every configured table. Requires a bearer token when `api.admin_token` is configured (`src/server/http_server.cpp`), as do `GET /config` and `GET /replication/status`.

Order of checks: network ACL → rate limit → bearer token → `Content-Type` → JSON parse → object shape → field allowlist → table resolution → callback availability. The ACL and the rate limiter run in the pre-routing handler (`src/server/http_server.cpp`), the token in `HttpServer::SetupRoutes`' shared route wrapper, and everything from `Content-Type` onward inside `HttpServer::HandleOptimize`. The bearer token is therefore validated **before** `Content-Type`: a request with no credentials and a wrong `Content-Type` gets `401`, not `415`.

### Request body

| Field | Type | Required | Meaning |
|---|---|---|---|
| `table` | string | no | Optimize this table; absent or `{}` optimizes every configured table (`src/server/http_server.cpp`) |

The body must be a JSON object (`src/server/http_server.cpp`) and **any field other than `table` is rejected**.

### Success

`200`, `application/json`, when the callback's return value starts with `"OK "` (`src/server/http_server.cpp`):

```json
{ "status": "ok", "result": "OPTIMIZED tables=1 elapsed_ms=12" }
```

`result` is the callback string with the `"OK "` prefix stripped.

### Errors

| Status | Condition | `error_code` |
|---|---|---|
| 400 | Body is not a JSON object | `kQuerySyntaxError` (3000) |
| 400 | Invalid JSON | `kQuerySyntaxError` (3000) |
| 400 | Unsupported field | `kQuerySyntaxError` (3000) |
| 400 | `table` not a string | `kQuerySyntaxError` (3000) |
| 400 | Invalid table name | `kQueryInvalidToken` (3001) |
| 401 | Missing or wrong bearer token (adds `WWW-Authenticate: Bearer`) | `kPermissionDenied` (7) |
| 404 | Table not resolvable | `kTableNotFound` (4007) |
| 415 | `Content-Type` not `application/json` | `kNetworkInvalidRequest` (6007) |
| 500 | Resolved table has a null index or document store (`ResolveHttpTableContext`) | `kInternalError` (5) |
| 503 | No optimize callback wired | `kServerInitMissingDependency` (6026) |
| 503 | Callback returned a coded `ERROR` frame | the frame's own code |
| 503 | Callback returned anything else non-`OK` | `kInternalError` (5) |

Every row in this table is implemented in `src/server/http_server.cpp`.

Every callback failure becomes `503`, regardless of the error code carried in the frame.

---

## GET /metrics

Prometheus text exposition. `200`, `Content-Type: text/plain; version=0.0.4; charset=utf-8` (`src/server/http_server.cpp`). Body is produced by `ResponseFormatter::FormatPrometheusMetrics` (`src/server/response_formatter.cpp` onward).

Metric families emitted (all prefixed `mygramdb_`): `server_info`, `server_uptime_seconds`, `server_commands_total`, `text_normalization_failures_total`, `command_total{command}`, `memory_used_bytes{type}`, `memory_peak_bytes`, `memory_fragmentation_ratio`, `memory_system_total_bytes`, `memory_system_available_bytes`, `memory_system_usage_ratio`, `memory_process_rss_bytes`, `memory_process_rss_peak_bytes`, `memory_health_status`, `index_documents_total{table}`, `index_terms_total{table}`, `index_postings_total{table}`, `index_postings_per_term_avg{table}`, `index_delta_encoded_lists{table}`, `index_roaring_bitmap_lists{table}`, `index_optimization_in_progress{table}`, `clients_connected`, `clients_total`, `dump_last_success_timestamp_seconds`, `dump_failures_total{trigger}`, `requests_denied_total{reason,surface}`, `thread_pool_queue_depth`, `thread_pool_queue_capacity`, `thread_pool_workers`, `replication_*`, `cache_*` (`src/server/response_formatter.cpp`).

`requests_denied_total` carries a `surface` label with values `tcp` and `http`, so ACL and rate-limit denials are attributable per protocol (`src/server/response_formatter.cpp`).

Errors: `500` `kInternalError` (`application/json`) on an unhandled exception (`src/server/http_server.cpp`).

---

## Authentication

| Property | Value | Citation |
|---|---|---|
| Scope | `GET /config`, `GET /replication/status`, `POST /optimize` — the routes whose descriptor sets `requires_admin_token` | |
| Enforcement point | The shared route wrapper, before the handler is dispatched. It is the only place a request is admitted or refused on credentials; `HandleHealthReady` and `HandleHealthDetail` read the credential too, but only to choose which replication fields to emit | |
| Secret | `api.admin_token`, overridable by `MYGRAM_API_ADMIN_TOKEN` | `src/config/config.cpp`, `src/config/config-schema.json` |
| Gate condition | `api.admin_token` is non-empty; an empty token disables the check entirely and all three routes answer any caller | |
| Transport | `Authorization` request header | |
| Scheme | Literal prefix `Bearer ` (one trailing space, case-sensitive) | |
| Comparison | `ConstantTimeEqual` — length difference folded in, compared over `max(len)` bytes | |
| Failure status | `401` with `WWW-Authenticate: Bearer` | |
| Failure body | `{"error":"Administrative endpoint requires a valid bearer token","error_code":7}`, `Content-Type: application/json` | |
| Failure accounting | Increments `total_requests`; records no command counter, because the handler that would record one never runs | |

Every citation in this table is `src/server/http_server.cpp` unless the cell names another file.

An empty `api.admin_token` leaves the three routes above open. Configuration validation refuses to start in that state whenever `api.tcp.bind` or `api.http.bind` is not a loopback address, so an open administrative surface is reachable only from loopback or through a Unix socket (`src/config/config.cpp`).

There is no session, cookie, API-key-parameter or query-string authentication path; credentials are presented per request.

Two routes consult `api.admin_token` without requiring it. `GET /health/ready` and `GET /health/detail` answer every caller, but suppress the replication fields that `GET /replication/status` protects unless the request carries accepted credentials (`src/server/http_server.cpp`). No other route reads the token.

## Network ACL

A pre-routing handler runs before every route, including `/health*` and `/metrics` (`src/server/http_server.cpp`).

- The client identity is `req.remote_addr`, or the literal `"unknown"` when empty (`src/server/http_server.cpp`).
- `X-Forwarded-For` is honoured only when the direct peer matches `api.http.trusted_proxies` exactly (`src/server/http_server.cpp`).
- A peer outside `network.allow_cidrs` gets `403` with `kPermissionDenied` and message `Access denied by network.allow_cidrs` (`src/server/http_server.cpp`).
- ACL denials increment `requests_denied_total{reason="acl",surface="http"}`, and `total_requests` only on a route the table marks `counts_requests` (`src/server/http_server.cpp`).
- ACL rejection happens **before** the rate limiter, so denied peers cannot consume or evict rate-limit buckets (`src/server/http_server.cpp`).
- Repeated denials from the same IP are log-suppressed by `DenialLogLimiter` (`src/server/denial_log_limiter.h`), applied at `src/server/http_server.cpp`.

## Rate limiting

- Applied in the same pre-routing handler, to every route the descriptor table marks `rate_limited` (`src/server/http_server.cpp`).
- `GET /health`, `GET /health/live` and `GET /health/ready` are **exempt** (`src/server/http_server.cpp`). A `429` on a liveness or readiness probe is read as a dead process by whatever is polling it, so traffic that happens to share the bucket could take a healthy server out of rotation. What still bounds them: `network.allow_cidrs` is enforced first and is unaffected (`src/server/http_server.cpp`), `api.http.max_connections` caps accepted sockets, and each response is a fixed-size JSON object built from atomics — no index walk, no document store access, no amplification.
- `GET /health/detail` stays rate limited (`src/server/http_server.cpp`): it walks every table's index and document store under the generation lock, so its cost grows with the corpus.
- Bucket key is the client IP string (`src/server/http_server.cpp`).
- The limiter instance is shared with the TCP server, so one client's quota spans both protocols. `TcpServer::AttachTo` (`src/server/tcp_server.cpp`) puts its own limiter into the shared-component bundle and `HttpServer::AdoptSharedComponents` (`src/server/http_server.cpp`) takes it.
- When no limiter is injected and `api.rate_limiting.enable` is true, `HttpServer` constructs a private one from config (`src/server/http_server.cpp`).
- Algorithm is a per-client token bucket with `capacity` burst and `refill_rate` tokens/second. `max_clients` bounds the size of the tracking table, not admission: once it is reached, an untracked client's request evicts the least-recently-seen bucket and is then served normally, so the only cause of a denial is an exhausted bucket (`src/server/rate_limiter.cpp`).
- Denial: `429` with `kServerBusy` (6030) and message `Rate limit exceeded` (`src/server/http_server.cpp`).
- Denials increment `requests_denied_total{reason="rate_limit",surface="http"}`, and `total_requests` only on a route the table marks `counts_requests` (`src/server/http_server.cpp`).

## Request size limits, timeouts and concurrency caps

| Limit | Value | Citation |
|---|---|---|
| Request body | `api.http.max_body_bytes`, default 16 MiB; oversize bodies get cpp-httplib's `413` before any handler runs. `0` lifts the cap entirely | `src/server/http_server.cpp`, `src/server/http_server.h`, `src/config/config.h` |
| Read timeout | `api.http.read_timeout_sec`, default 5 s; non-positive values fall back to the default | `src/server/http_server.cpp`, `src/server/http_server.h`, `src/config/config.h` |
| Write timeout | `api.http.write_timeout_sec`, default 5 s; same fallback | `src/server/http_server.cpp`, `src/server/http_server.h`, `src/config/config.h` |
| Concurrent connections | `api.http.max_connections`, default 10000, clamped to ≥ 1; enforced at the accepted-socket boundary by `CappedHttpTaskQueue`, covering both queued and active sockets | `src/server/http_server.cpp`, `src/config/config.h` |
| Worker threads | cpp-httplib defaults: `max(8, hardware_concurrency-1)` initial, ×4 maximum | `src/server/http_server.cpp` for the initial count; the rest is cpp-httplib's own default, fixed by the version pinned in `third_party/CMakeLists.txt` |
| Request URI | 8192 bytes; longer gets `414` | cpp-httplib's own default (`third_party/CMakeLists.txt`) |
| Header line | 8192 bytes | cpp-httplib's own default (`third_party/CMakeLists.txt`) |
| Keep-alive | 5 s idle timeout, 100 requests per connection | cpp-httplib's own defaults (`third_party/CMakeLists.txt`) |
| Startup readiness | `Start()` waits up to 5 s for the accept loop to come up, otherwise fails with `kNetworkBindFailed` | `src/server/http_server.cpp` |

Rejections at the cpp-httplib layer (`413`, `414`, `400` on a malformed request line) carry cpp-httplib's own body, not the JSON error object. The same holds for an exception escaping `HttpServer::HandleOptimize` or any of `HandleHealth`, `HandleHealthLive`, `HandleHealthReady` and `HandleHealthDetail` (`src/server/http_server.cpp`): those five handlers have no `try`/`catch`, and no `set_exception_handler` is registered, so an uncaught exception becomes cpp-httplib's own non-JSON `500`. Every other handler catches `std::exception` and answers `500` with `kInternalError` in the JSON envelope.

## CORS and security headers

CORS is active only when `api.http.enable_cors` is true **and** `api.http.cors_allow_origin` is non-empty; a missing origin logs `http_cors_disabled_missing_allow_origin` and leaves CORS off rather than defaulting to a wildcard (`src/server/http_server.cpp`).

When active:

| Header | Value | Where |
|---|---|---|
| `Access-Control-Allow-Origin` | configured `cors_allow_origin` verbatim | every response, via the post-routing handler |
| `Access-Control-Allow-Methods` | `GET, POST, OPTIONS` | `OPTIONS` preflight only |
| `Access-Control-Allow-Headers` | `Content-Type, Authorization` | `OPTIONS` preflight only |

Every row in this table is implemented in `src/server/http_server.cpp`.

The preflight response is `204` with no body (`src/server/http_server.cpp`).

No other security headers are emitted: there is no `Strict-Transport-Security`, `X-Content-Type-Options`, `X-Frame-Options`, `Content-Security-Policy`, `Referrer-Policy`, or `Access-Control-Allow-Credentials` anywhere in `src/server/http_server.cpp`. TLS is not terminated by this server; only `httplib::Server` (plain HTTP) is instantiated (`src/server/http_server.cpp`).

## Status-code mapping

Only three internal codes are mapped by range/identity; every other query-path error becomes `400` (`src/server/http_server.cpp`):

| Internal code | Numeric | HTTP status |
|---|---|---|
| `kServerShuttingDown` | 6027 | 503 |
| `kServerLoading` | 6028 | 503 |
| `kInternalError` | 5 | 500 |
| anything else reaching `HttpStatusForQueryError` | — | 400 |

Statuses chosen directly by the pre-routing handler, the route wrapper and the route handlers, independent of that helper:

| Condition | Status | Internal code |
|---|---|---|
| ACL denial | 403 | `kPermissionDenied` (7) |
| Rate limit | 429 | `kServerBusy` (6030) |
| Wrong `Content-Type` | 415 | `kNetworkInvalidRequest` (6007) |
| Missing/bad bearer token on a route that requires one | 401 | `kPermissionDenied` (7) |
| Table not found | 404 | `kTableNotFound` (4007) |
| Document not found | 404 | `kIndexDocumentNotFound` (4004) |
| Null index/doc store | 500 | `kInternalError` (5) |
| Server loading | 503 | `kServerLoading` (6028) |
| Table synchronizing | 503 | `kServerNotReady` (6029) |
| Replication not configured / not compiled | 503 | `kNotImplemented` (4) |
| Optimize callback failure | 503 | frame code or `kInternalError` |
| Optimize callback missing | 503 | `kServerInitMissingDependency` (6026) |
| Not ready (`/health/ready`) | 503 | — (health bodies carry no `error_code`) |

Every row in this table is implemented in `src/server/http_server.cpp`.

By error-code range (ranges from the project's error taxonomy, `src/utils/error.h`):

| Range | Module | HTTP statuses actually produced |
|---|---|---|
| 0–999 | General | 500 (`kInternalError` 5), 401/403 (`kPermissionDenied` 7), 400 (`kInvalidArgument` 2, `kNotImplemented` 4), 503 (`kNotImplemented` 4 on the replication route) |
| 3000–3999 | Query/Request parsing | 400 exclusively |
| 4000–4999 | Index/Search | 404 for `kIndexDocumentNotFound` (4004); 404 for `kTableNotFound` (4007) raised during table resolution, 400 when the same code arrives from `ScoreAndSortByRelevance` through `HttpStatusForQueryError`; 400 for `kIndexNotFound` (4000) and for every other code reaching `HttpStatusForQueryError` |
| 6000–6999 | Network/Server | 415 (6007), 503 (6026/6027/6028/6029), 429 (6030) |

`kNotImplemented` (4) maps to 400 when it arrives through `HttpStatusForQueryError` (e.g. the highlight/text-storage error at `src/server/http_server.cpp`) and to 503 when a handler picks the status directly.

---

## Divergences from the TCP surface

The TCP surface is normative. The differences below are recorded as observed, with no fix implied.

In every table of this section, a citation in the *HTTP behaviour* column is `src/server/http_server.cpp` unless the cell names another file.

### Authentication and authorization

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 3 | Credential transport | `AUTH <token>` command, sets per-connection state (`src/server/request_dispatcher.cpp`) | `Authorization: Bearer <token>` per request, no session |

Which operations are administrative does not differ between the surfaces: a route's `requires_admin_token` field (`src/server/http_server.h`) is enforced in the shared route wrapper before any handler runs (`src/server/http_server.cpp`), so `GET /config` and `GET /replication/status` answer an uncredentialed request with 401 and `kPermissionDenied` exactly as the TCP surface does. The credential check precedes the `Content-Type` check on `/optimize` for the same reason. Pinned by `HttpTcpConsistencyTest.AdministrativeReportsAreTokenGatedOnBothSurfaces` and `.HttpOptimizeChecksTheTokenBeforeTheContentType`.

### Table resolution

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 5 | Table-name character set | No whitelist; the token is whatever the tokenizer produced (`src/query/query_parser_commands.cpp`) | `QueryParser::IsSafeTableName`: ≤ `kMaxTableNameLength` (256) bytes, ASCII limited to `[A-Za-z0-9_.-]` plus well-formed non-ASCII UTF-8; anything else is 400 `kQueryInvalidToken` (3001). The check is `QueryParser::IsSafeTableName` (`src/query/query_parser.cpp`) and the rejection is emitted by `ResolveHttpTableContext` |
| 6 | Bare name under a multi-database configuration | `kTableNotFound` (4007) with message `Bare table names are not supported; use <database>.<table>: …` (`src/server/handlers/command_handler.cpp`) | Same message and the same `QueryParser::IsDatabaseQualifiedTableName` test, but `kQuerySyntaxError` (3000) and HTTP 400 (`ResolveHttpTableContext`) |
| 7 | Partially initialised table context | Reported as `Index not available` / `Document store not available`, `kInternalError` (`src/server/handlers/search_handler.cpp`) | Reported earlier during resolution as `Table context has null index or doc_store`, `kInternalError` / 500 |

### Accepted parameters

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 8 | Repeated conditions on one filter column | `FILTER` clauses are a list; the same column may appear more than once (`src/query/query_parser_clauses.cpp`) | `filters` is a JSON object, so at most one condition per column survives |
| 9 | Filter value starting with an operator character | Rejected: `FILTER value must not start with an operator character`, `kQueryInvalidFilter` (`src/query/query_parser_clauses.cpp`) | No such check; the value is accepted |
| 10 | Non-string filter values | Values arrive as text tokens only (`src/query/query_parser_clauses.cpp`) | JSON integers, floats and booleans are coerced: `true`→`"1"`, `false`→`"0"`, floats via `std::to_string(double)` (fixed six decimals) |
| 12 | Boolean-expression validation | `q` goes through `ParseSearchTextTokens`, which rejects unmatched/unclosed parentheses, the deprecated `ORDER` keyword, comma-separated table lists, and splits flat `AND`/`NOT` clauses (`src/query/query_parser_commands.cpp`) | `mode:"boolean"` assigns `q` straight to `search_text`/`search_expression`; none of those checks or the clause split run |
| 13 | FACET search text | Runs through the parser's shared search-text extraction (`src/query/query_parser_commands.cpp`) | Assembled field by field; `QueryParser::Parse` is never invoked on the facet path |
| 14 | COUNT clause rejection | Rejects `SORT`, `ORDER`, and any clause other than `AND`/`NOT`/`FILTER`, including `LIMIT`/`OFFSET`, all as `kQuerySyntaxError` (`src/query/query_parser_commands.cpp`) | Rejects exactly the fields `limit`, `offset`, `sort`, `highlight`, `fuzzy` as `kQuerySyntaxError`; other unknown JSON fields are silently ignored |
| 15 | Unknown clause / field | On `FACET` and `COUNT` an unrecognised clause keyword is a hard `kQuerySyntaxError` (`src/query/query_parser_commands.cpp`). On `SEARCH` it is not: trailing tokens are absorbed into the search text, so `SEARCH t term BOGUSCLAUSE 3` answers `OK RESULTS 0` | Unknown JSON body fields are ignored on `/search`, `/count` and `/facet`; only `/optimize` enforces an allowlist |

### Validation limits

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 16 | LIMIT above 1000 | `kQuerySyntaxError` (3000), message `LIMIT exceeds maximum of 1000` (`src/query/query_parser_commands.cpp`) | `kQueryInvalidLimit` (3008), message `Invalid limit: must be between 1 and 1000` |
| 17 | More than 64 filter conditions | `kQuerySyntaxError` (3000) (`src/query/query_parser_commands.cpp`) | `kQueryInvalidFilter` (3006) |
| 18 | Invalid facet column | `kQuerySyntaxError` (3000) (`src/query/query_parser_commands.cpp`) | `kQueryInvalidToken` (3001) |
| 19 | Highlight tag over 256 bytes | `kQuerySyntaxError`, message names `HIGHLIGHT TAG open/close tag` (`src/query/query_parser_clauses.cpp`) | `kQuerySyntaxError`, message names `Field 'highlight.open_tag'`. At the default `api.max_query_length` of 128 characters a 256-byte tag makes the assembled TCP command 268 characters, so that surface answers `kQueryTooLong` (`src/query/query_parser.cpp`) before the tag cap is reached; only the rejection can be compared |
| 21 | Control characters in search text | No explicit `\r`/`\n`/`\0` rejection in the parser; framing removes the line terminator | `q` containing `\r`, `\n` or `\0` is 400 `kQueryInvalidToken` |

### Error mapping

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 25 | OPTIMIZE failure | The handler returns the condition's own code, e.g. `kServerBusy` (6030) when a DUMP LOAD or another long operation is in flight (`src/server/handlers/debug_handler.cpp`), reached over HTTP through the shared entry point `TcpServer::HandleOptimizeRequest` (`src/server/tcp_server.cpp`) | The frame's code is preserved in the body but the status is always 503, whatever the code |
| 26 | Error envelope | Single line `ERROR <code> <message>`, parsed by `ParseErrorFrame` (`src/server/protocol_constants.h`) | JSON object `{"error": …, "error_code": …}` |
| 27 | Transport-layer rejections | All failures are `ERROR` frames | `413`, `414` and malformed-request-line `400` come from cpp-httplib with a non-JSON body; unmatched routes get an empty-bodied `404` |

### Escaping and quoting

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 28 | Literal search text | The client supplies the quoting; the tokenizer processes `\n`, `\t`, `\r`, `\\` and `\"` escape sequences (`src/query/query_parser.cpp`) | The server quotes `q` itself with `QueryParser::QuoteSearchLiteral`, the parser's own escaper for `\` and `"`; `\n`-style sequences in `q` stay literal because control characters were already rejected (`src/query/query_parser_commands.cpp`) |
| 29 | Response escaping | Primary keys and facet values are escaped/sanitised for the delimited text protocol (`src/server/response_formatter.cpp`) | Values are emitted as JSON strings with invalid UTF-8 replaced by U+FFFD |
| 31 | Floating-point filter values in responses | Fixed six decimals (`src/server/response_formatter.cpp`) | Full JSON double serialization |

### Result payload shape

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 32 | SEARCH results | `OK RESULTS <total> <pk> <pk> …` — primary keys only, no filter values (`src/server/response_formatter.cpp`) | Each result object carries `primary_key` plus the document's `filters` |
| 33 | Missing documents in a result page | Counted and logged, but the response still lists only resolvable keys (`src/server/response_formatter.cpp`) | Silently skipped, so `results.length` can be below `limit` while `count` is higher |
| 34 | Debug output | `DEBUG ON` adds a `# DEBUG` block with timings, n-gram counts, cache status and the applied sort (`src/server/response_formatter.cpp`) | No debug mode exists on the HTTP surface; `conn_ctx.debug_mode` has no HTTP equivalent |
| 35 | FACET counts | `OK FACET <returned> <total>` then `value\tcount` lines (`src/server/response_formatter.cpp`) | `column`, `count`, `total_count`, `facets[]` — carries the column name, which the TCP frame does not |

### Sorting and scoring

Both surfaces order a result page through `search_pipeline::SortAndPaginateResults`, which routes `SORT _score` to `ScoreAndSortByRelevance` and every other order to `ResultSorter::SortAndPaginate`, and both build snippets through `search_pipeline::GenerateHighlightSnippets`. Sort resolution, scoring and highlighting therefore have no surface-specific behaviour of their own.

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 38 | Top-N optimization | `ApplySearchTopNOptimization`, with the chosen strategy surfaced in debug output (`src/server/handlers/search_handler.cpp`) | Same call, but the strategy is not reported anywhere in the response |
| 47 | Order of the HIGHLIGHT storage check | Rejected before the page is ordered, so a request that both asks for `HIGHLIGHT` without stored text and orders by an unavailable `_score` reports the highlight fault (`src/server/handlers/search_handler.cpp`) | Rejected after the page is ordered, so the same request reports the sort fault instead |

### Request and rate-limit accounting

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 39 | `total_requests` | Incremented once per dispatched request, including rate-limited and malformed input (`src/server/request_dispatcher.cpp`) | Incremented per handler, in the pre-routing denial branches, and in the route wrapper's credential rejection, on every route the descriptor table marks `counts_requests`; `/health`, `/health/live`, `/health/ready` and `/health/detail` never increment it, on any outcome |
| 41 | Rate-limit token consumption | One token per parsed request (`src/server/request_dispatcher.cpp`) | One token per HTTP request on every route the descriptor table marks `rate_limited`, which includes `/metrics`, `/health/detail` and CORS preflights but excludes `/health`, `/health/live` and `/health/ready` |
| 42 | Command counters | Incremented after a successful parse, before dispatch, for every command type (`src/server/request_dispatcher.cpp`) | Incremented per handler; `/metrics` and the health routes record no command at all |
| 43 | Network ACL enforcement point | Applied when a connection is accepted | Applied per request, before routing |
| 44 | Denial counters | `requests_denied_total{surface="tcp"}`, with additional `connection_limit` and `pool_full` reasons | `requests_denied_total{surface="http"}`, only `acl` and `rate_limit` reasons are ever produced (`src/server/response_formatter.cpp`) |

### Readiness reporting

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 45 | Overall readiness | `INFO` reports `readiness: ready` / `not_ready` from the shared verdict (`src/server/handlers/admin_handler.cpp`, `src/server/response_formatter.cpp`) | `/health/ready` and `/health/detail` render the same verdict, adding the `reason` and the per-component breakdown the text frame has no room for |
| 46 | Replication state in the overall report | `INFO`'s `replication_status` line is the reader's raw `IsRunning()`, so a dump pause or a SYNC reads as `stopped` (`src/server/response_formatter.cpp`) | `/health/detail` reports the availability classification, which names `paused_for_dump` and `paused_for_sync` separately from `disconnected` |

### TCP commands with no HTTP route

`AUTH`, `DUMP SAVE/LOAD/VERIFY/INFO/STATUS`, `SAVE`, `LOAD`, `REPLICATION STOP`, `REPLICATION START`, `SYNC`, `SYNC STATUS`, `SYNC STOP`, `CONFIG HELP`, `CONFIG VERIFY`, `DEBUG ON`, `DEBUG OFF`, `CACHE CLEAR`, `CACHE STATS`, `CACHE ENABLE`, `CACHE DISABLE`, `SET`, and `SHOW VARIABLES` exist only on the TCP surface (`src/query/query_parser.h`, `src/server/http_server.cpp`). Cache statistics are the only one of these that is partially reachable over HTTP, as the read-only `cache` object inside `GET /info` (`src/server/http_server.cpp`).

`GET /metrics`, `GET /health`, `GET /health/live`, `GET /health/ready` and `GET /health/detail` have no TCP counterpart (`src/server/http_server.cpp`).
