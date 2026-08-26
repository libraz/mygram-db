# HTTP Route Specification

This file is normative. It describes the HTTP surface as the code in `src/server/` behaves today, derived by reading that code; where prose elsewhere in the repository disagrees, this file follows the code. Every statement carries a `file:line` citation so the spec can be re-verified mechanically. Line numbers refer to the state of the tree in which this file was written.

The HTTP surface is served by `HttpServer`, which embeds cpp-httplib 0.51.0 (`build/_deps/httplib-src/httplib.h:11`). Routes are declared in the `HttpServer::Routes()` descriptor table (`src/server/http_server.cpp:667-681`) and registered from it, in table order, by `HttpServer::SetupRoutes` (`src/server/http_server.cpp:714-738`). cpp-httplib matches in registration order, so the table order is behavioural.

## Route index

| Method | Path | Handler | Auth | Rate limited | Counted in `total_requests` | Counted as command |
|---|---|---|---|---|---|---|
| POST | `/tables/{identity}/search` | `HandleSearch` (`src/server/http_server.cpp:1338`) | none | yes | yes (`src/server/http_server.cpp:1339`) | `SEARCH` (`src/server/http_server.cpp:1346`) |
| POST | `/tables/{identity}/count` | `HandleCount` (`src/server/http_server.cpp:1451`) | none | yes | yes (`src/server/http_server.cpp:1452`) | `COUNT` (`src/server/http_server.cpp:1459`) |
| POST | `/tables/{identity}/facet` | `HandleFacet` (`src/server/http_server.cpp:1492`) | none | yes | yes (`src/server/http_server.cpp:1493`) | `FACET` (`src/server/http_server.cpp:1500`) |
| GET | `/tables/{identity}/{primary_key}` | `HandleGet` (`src/server/http_server.cpp:1543`) | none | yes | yes (`src/server/http_server.cpp:1544`) | `GET` (`src/server/http_server.cpp:1569`) |
| GET | `/info` | `HandleInfo` (`src/server/http_server.cpp:1603`) | none | yes | yes (`src/server/http_server.cpp:1605`) | `INFO` (`src/server/http_server.cpp:1606`) |
| GET | `/health` | `HandleHealth` (`src/server/http_server.cpp:1788`) | none | no (`src/server/http_server.cpp:672`) | no (`src/server/http_server.cpp:672`) | no |
| GET | `/health/live` | `HandleHealthLive` (`src/server/http_server.cpp:1802`) | none | no (`src/server/http_server.cpp:673`) | no (`src/server/http_server.cpp:673`) | no |
| GET | `/health/ready` | `HandleHealthReady` (`src/server/http_server.cpp:1814`) | none | no (`src/server/http_server.cpp:674`) | no (`src/server/http_server.cpp:674`) | no |
| GET | `/health/detail` | `HandleHealthDetail` (`src/server/http_server.cpp:1852`) | none | yes (`src/server/http_server.cpp:675`) | no (`src/server/http_server.cpp:675`) | no |
| GET | `/config` | `HandleConfig` (`src/server/http_server.cpp:1950`) | none | yes | yes (`src/server/http_server.cpp:1951`) | `CONFIG_SHOW` (`src/server/http_server.cpp:1952`) |
| GET | `/replication/status` | `HandleReplicationStatus` (`src/server/http_server.cpp:2000`) | none | yes | yes (`src/server/http_server.cpp:2001`) | `REPLICATION_STATUS` (`src/server/http_server.cpp:2002`) |
| POST | `/optimize` | `HandleOptimize` (`src/server/http_server.cpp:2041`) | bearer token (`src/server/http_server.cpp:2050-2063`) | yes | yes (`src/server/http_server.cpp:2042`) | `OPTIMIZE` (`src/server/http_server.cpp:2106`) |
| GET | `/metrics` | `HandleMetrics` (`src/server/http_server.cpp:2126`) | none | yes | yes (`src/server/http_server.cpp:2127`) | no |
| OPTIONS | `.*` | CORS preflight, only when CORS is active | none | yes | no | no |

`OPTIONS .*` is registered only when `enable_cors` is true **and** `cors_allow_origin` is non-empty (`src/server/http_server.cpp:677-681`, `src/server/http_server.cpp:777-782`).

The *Rate limited* and *Counted in `total_requests`* columns are declared per route in the same descriptor table (`src/server/http_server.h:287-311`) and read by the pre-routing handler through `FindLiteralRoute` (`src/server/http_server.cpp:685-693`, `:744-750`), so a route's accounting is the same whether the request is served or rejected. A path the table does not name — including every regex route and every unmatched path — is counted and rate limited.

### Path matching rules

- Fixed paths (`/info`, `/health`, `/health/live`, `/health/ready`, `/health/detail`, `/config`, `/replication/status`, `/optimize`, `/metrics`) are registered as exact literal strings (`src/server/http_server.cpp:703-711`). A trailing slash does not match: `/info/` is not `/info`.
- Method and path are both case-sensitive as registered. cpp-httplib accepts only the methods `GET, HEAD, POST, PUT, DELETE, CONNECT, OPTIONS, TRACE, PATCH, PRI` on the request line (`build/_deps/httplib-src/httplib.h:11611-11618`); anything else fails request-line parsing.
- The table routes use the regex `/tables/([^/]+)/search`, `.../count`, `.../facet` (`src/server/http_server.cpp:572`, `:573`, `:574`) and `/tables/([^/]+)/(.+)` for GET (`src/server/http_server.cpp:584`). `{identity}` is capture group 1 (`src/server/http_server.cpp:180-182`); the GET primary key is capture group 2 (`src/server/http_server.cpp:184-186`). The GET key group is greedy, so a primary key containing `/` — including one written as `%2F`, which is decoded before matching — is captured whole rather than failing to match.
- The request path is percent-decoded and its `#fragment` stripped before routing; the query string is split off into `req.params` (`build/_deps/httplib-src/httplib.h:11626-11641`). No handler reads `req.params`, so **query-string parameters are ignored on every route**.
- The GET document route is last in the table (`src/server/http_server.cpp:584`), so it cannot shadow the fixed endpoints. It does match `GET /tables/{identity}/search`, which is treated as a document lookup for the primary key `search`, and — being greedy — `GET /tables/{identity}/search/anything`, as a lookup for `search/anything`.
- Unmatched method/path combinations receive cpp-httplib's default `404` with an empty body (`build/_deps/httplib-src/httplib.h:12742`, `:12773`). No `set_error_handler` is registered, so these 404s are **not** JSON.

---

## POST /tables/{identity}/search

Full-text search. Request body must be a JSON object; `Content-Type` must be `application/json` (parameters after `;` are ignored, matching is ASCII-case-insensitive) — `src/server/http_server.cpp:376-388`, enforced at `src/server/http_server.cpp:1040-1044`.

### Request body fields

| Field | Type | Required | Default | Validation |
|---|---|---|---|---|
| `q` | string | yes | — | Presence `src/server/http_server.cpp:1206-1209`; type `:1212-1215`; no `\r`, `\n`, `\0` `:1178-1184`; non-empty `:1185-1188`. Length is checked once, on the assembled expression (see *Query construction*) |
| `mode` | string | no | `"literal"` | Must be `"literal"` or `"boolean"` (`src/server/http_server.cpp:137-152`) |
| `limit` | integer | no | `api.default_limit` (`src/server/http_server.cpp:1115-1117`) | Must be an integer `:1082-1086`; `1 <= limit <= 1000` (`config::defaults::kMaxLimit`, `src/config/config.h:63`) `:1088-1093` |
| `offset` | integer | no | `0` | Must be an integer `:1099-1103`; `0 <= offset <= 4294967295` `:1105-1110` |
| `filters` | object | no | — | Must be an object `:1120-1123`; parsed by `ParseFiltersFromJson` `:309-360` |
| `sort` | object | no | primary key DESC | `ParseSortFromJson` `:428-462` |
| `highlight` | object | no | — | `ParseHighlightFromJson` `:466-502` |
| `fuzzy` | integer | no | — | Must be an integer `:505-507`; must be `1` or `2` `:509-511` |

Unknown fields are ignored on this route (there is no allowlist check; contrast `/optimize`, `src/server/http_server.cpp:2077-2083`).

**`filters`** accepts two shapes per column (`src/server/http_server.cpp:317-358`):

- `{"col": <scalar>}` — operator defaults to `EQ` (`src/server/http_server.cpp:341-342`).
- `{"col": {"op": "<OP>", "value": <scalar>}}` — `op` defaults to `"EQ"` when absent (`src/server/http_server.cpp:327`); parsed by `QueryParser::ParseFilterOp` (`src/query/query_parser_clauses.cpp:468`).

Caps applied to `filters`:

| Rule | Limit | Citation |
|---|---|---|
| Number of conditions | ≤ 64 (`QueryParser::kMaxTermCount`) | `ParseFiltersFromJson`, `src/server/http_server.cpp` |
| Column name | `IsSafeColumnName`: 1–128 bytes of `[A-Za-z0-9_.$-]` (`src/query/query_parser.cpp`) | `QueryParser::ValidateFilterCondition`, shared with the TCP `FILTER` clause (`src/query/query_parser_clauses.cpp`) |
| Value type | string, integer, float, boolean only (`JsonFilterValueToString`, `src/server/http_server.cpp`) | `src/server/http_server.cpp` |
| Value length | ≤ 1024 bytes (`kMaxFilterValueLength`) | `QueryParser::ValidateFilterCondition`, shared with the TCP `FILTER` clause |

Non-string filter values are coerced to strings: integers via `std::to_string(int64_t)`, floats via `std::to_string(double)` (fixed six decimals), booleans to `"1"`/`"0"` (`src/server/http_server.cpp:177-191`). Because `filters` is a JSON object, at most one condition per column can be expressed.

**`sort`** (`src/server/http_server.cpp:428-462`):

| Field | Type | Required | Validation |
|---|---|---|---|
| `column` | string | no | `QueryParser::IsSafeSortColumn` — `_score` or `IsSafeColumnName` — shared with the TCP `SORT` clause |
| `order` | string | no | `QueryParser::ParseSortOrder` — `ASC`/`DESC`, ASCII-case-insensitive; default `DESC` — shared with the TCP `SORT` clause |

Omitting `column` orders by the primary key, the shorthand `SORT ASC` / `SORT DESC` carries on the TCP surface (`src/server/http_server.cpp:432-434`). A named column is passed through as written and resolved by `ResultSorter`, which accepts the primary-key column name as well as any filter column (`src/query/result_sorter.cpp:528-549`).

**`highlight`** (`src/server/http_server.cpp:466-502`):

| Field | Type | Range | Citation |
|---|---|---|---|
| `open_tag` | string | ≤ 256 bytes (`kMaxHighlightTagLength`) | `ParseHighlightFromJson`, `src/server/http_server.cpp` |
| `close_tag` | string | ≤ 256 bytes (`kMaxHighlightTagLength`) | `ParseHighlightFromJson`, `src/server/http_server.cpp` |
| `snippet_length` | integer | `kMinSnippetLength`–`kMaxSnippetLength` (1–10000), shared with `HIGHLIGHT SNIPPET_LEN` | `ParseHighlightUint`, `src/server/http_server.cpp` |
| `max_fragments` | integer | `kMinHighlightFragments`–`kMaxHighlightFragments` (1–100), shared with `HIGHLIGHT MAX_FRAGMENTS` | `ParseHighlightUint`, `src/server/http_server.cpp` |

### Query construction

- `mode: "literal"` (default): the request is turned into the text command `SEARCH <resolved_table> "<escaped q>"` and run through `QueryParser::Parse`, where `<escaped q>` is produced by `QueryParser::QuoteSearchLiteral`, the escaper the parser itself uses to re-emit a quoted token (`src/query/query_parser_commands.cpp`). A parser error is returned as 400 with the parser's own error code.
- `mode: "boolean"`: `q` is assigned directly to both `search_text` and `search_expression` without invoking the parser (`src/server/http_server.cpp:1234-1238`).

After construction, `ApplyHttpQueryOptions` applies pagination, filters, sort, highlight and fuzzy, then validates the assembled query against `api.max_query_length` via `QueryParser::ValidateQueryLength` (`src/server/http_server.cpp:1168-1173`, `src/query/query_parser.cpp:523-538`). This is the only length check on the route, and it counts code points, not bytes (`src/query/query_parser.cpp:21-50`).

### Success response

`200`, `Content-Type: application/json`, body serialized compact with invalid UTF-8 replaced (`src/server/http_server.cpp:2153-2156`).

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

- `count` is the pre-pagination total, or the top-N total when that optimization applied (`src/server/http_server.cpp:1366-1373`, `:1385`).
- `limit` and `offset` echo the effective values (`src/server/http_server.cpp:1386-1387`).
- `filters` is present only when the document has at least one filter value (`src/server/http_server.cpp:1412-1418`). `std::monostate` serializes as `null`, `TimeValue` as its integer `seconds`, everything else as its native JSON type (`src/server/http_server.cpp:154-170`).
- `highlight` is present only when the request carried `highlight`; it is `""` when the document has neither original nor normalized stored text (`src/server/http_server.cpp:1420-1432`).
- Documents that fail to load are skipped (`src/server/http_server.cpp:1406-1407`), so `results.length` can be smaller than `limit` even when `count` is larger.

### Error responses

| Status | Condition | `error_code` | Citation |
|---|---|---|---|
| 400 | Invalid JSON body | `kQuerySyntaxError` (3000) | `src/server/http_server.cpp:1064-1067` |
| 400 | Missing / non-string `q`, bad `mode` | `kQuerySyntaxError` (3000) | `:1196`, `:1202`, `:142`, `:151` |
| 400 | `q` contains `\r`/`\n`/`\0` | `kQueryInvalidToken` (3001) | `:1162` |
| 400 | `q` empty | `kQuerySyntaxError` (3000) | `:1168` |
| 400 | Assembled query exceeds `api.max_query_length` characters | `kQueryTooLong` (3005) | `:1170`, `src/query/query_parser.cpp:530-534` |
| 400 | Bad `limit` | `kQueryInvalidLimit` (3008) | `:1083`, `:1089` |
| 400 | Bad `offset` | `kQueryInvalidOffset` (3009) | `:1100`, `:1106` |
| 400 | `filters` not an object / bad condition | `kQueryInvalidFilter` (3006) | `:1121`, `:313`, `:319`, `:330`, `:337`, `:353` |
| 400 | Bad `sort` | `kQueryInvalidSort` (3007) | `:430`, `:437`, `:441`, `:448`, `:456` |
| 400 | Bad `highlight` / `fuzzy` | `kQuerySyntaxError` (3000) | `:453`, `:460`, `:468`, `:474`, `:506`, `:510` |
| 400 | Invalid table name | `kQueryInvalidToken` (3001) | `:969-973` |
| 400 | Bare table name under a multi-database configuration | `kQuerySyntaxError` (3000) | `:977-982` |
| 400 | `HIGHLIGHT` requested but normalized text storage is off | `kNotImplemented` (4) | `:1394-1401` |
| 400 | `sort._score` with BM25 disabled | `kQueryInvalidSort` (3007) | `src/server/search_pipeline.cpp:813-816` via `:556`, mapped by `:418-426` |
| 400 | `sort._score` with normalized text storage off | `kNotImplemented` (4) | `src/server/search_pipeline.cpp:817-822` |
| 400 | `sort._score` when the table's index or BM25 statistics are missing | `kTableNotFound` (4007) | `src/server/search_pipeline.cpp:823-825` |
| 400 | Any other pipeline / sorter error | error's own code | `:1361`, `:1378` via `:401-409` |
| 404 | Table not resolvable | `kTableNotFound` (4007) | `:985-999` |
| 415 | `Content-Type` not `application/json` | `kNetworkInvalidRequest` (6007) | `:1041` |
| 500 | Resolved table has a null index or document store | `kInternalError` (5) | `:1001-1006` |
| 500 | Unhandled exception in the handler | `kInternalError` (5) | `:1441-1448` |
| 503 | Server loading a dump | `kServerLoading` (6028) | `:1045-1049` |
| 503 | Table is synchronizing | `kServerNotReady` (6029) | `:1014-1036` |

Error bodies always have exactly two fields (`src/server/http_server.cpp:2158-2164`):

```json
{ "error": "Invalid limit: must be between 1 and 1000", "error_code": 3008 }
```

---

## POST /tables/{identity}/count

Same preamble, table resolution, `q`, `mode` and `filters` handling as `/search` (`src/server/http_server.cpp:1455` calls `PrepareHttpSearchQuery` with `apply_pagination=false`).

Additional validation: the fields `limit`, `offset`, `sort`, `highlight` and `fuzzy` are **rejected**, each with `kQuerySyntaxError` and status 400 (`src/server/http_server.cpp:1206-1219`). Because `apply_pagination` is false, no default limit is applied and `sort`/`highlight`/`fuzzy` parsing is skipped (`src/server/http_server.cpp:1253`).

Success: `200`, `application/json`.

```json
{ "count": 42 }
```

`count` is the full match count with no pagination applied (`src/server/http_server.cpp:1477-1478`).

Error responses are the same set as `/search` minus the pagination, sort, highlight and fuzzy rows, plus the rejected-field row above.

---

## POST /tables/{identity}/facet

Facet value counts. Preamble identical to `/search` (`src/server/http_server.cpp:1267` calls `PrepareHttpJsonRequest`).

### Request body fields

| Field | Type | Required | Default | Validation |
|---|---|---|---|---|
| `column` | string | yes | — | Presence `src/server/http_server.cpp:1273-1276`; type `:1277-1280`; `IsSafeColumnName` `:1296-1300` |
| `q` | string | no | — | Must be a string `:1282-1285`; empty allowed `:1315`; same control-character and length rules as `/search` `:1159-1182` |
| `mode` | string | no | `"literal"` | `:1302-1306` via `:137-152` |
| `limit` | integer | no | `api.default_limit` | Same rules as `/search` (`:1325-1326` passes `apply_pagination=true`) |
| `offset` | integer | no | `0` | Same rules as `/search` |
| `filters` | object | no | — | Same rules as `/search` |

`sort`, `highlight` and `fuzzy` are rejected with `kQuerySyntaxError` / 400 (`src/server/http_server.cpp:1287-1294`).

In literal mode the search expression is quoted with the same escaper used by `/search` (`src/server/http_server.cpp:1320-1321`); in boolean mode `q` is used verbatim. Unlike `/search`, the facet path never invokes `QueryParser::Parse` — the query is assembled field by field (`src/server/http_server.cpp:1308-1323`).

`limit`/`offset` are applied to the *value list*, after `total_count` has been recorded (`src/server/search_pipeline.cpp:2140-2148`).

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

- `count` is the number of returned facet entries after pagination (`src/server/http_server.cpp:1528`).
- `total_count` is the number of distinct values before pagination (`src/server/http_server.cpp:1529`, `src/server/search_pipeline.cpp:2140`).

Errors: as `/search`, with `Invalid facet column` reported as `kQueryInvalidToken` / 400 (`src/server/http_server.cpp:1298`) and missing/non-string `column` as `kQuerySyntaxError` / 400.

---

## GET /tables/{identity}/{primary_key}

Fetch one document by primary key. No body, no `Content-Type` requirement (`src/server/http_server.cpp:1543-1601`).

Both path segments are percent-decoded by cpp-httplib before matching (`build/_deps/httplib-src/httplib.h:11637-11638`). `{identity}` goes through the same `IsValidTableName` / qualification / catalog resolution as the POST routes (`src/server/http_server.cpp:1460-1464`). `{primary_key}` is everything after the table segment, used verbatim with no validation (`src/server/http_server.cpp:1459`, `:1473`); because decoding happens before matching and the route's key group is greedy, a key containing `/` is reachable as `%2F`.

Success: `200`, `application/json`.

```json
{
  "primary_key": "doc_1",
  "filters": { "category": "tech", "views": 120 }
}
```

`filters` is omitted when the document has none (`src/server/http_server.cpp:1587-1593`).

| Status | Condition | `error_code` | Citation |
|---|---|---|---|
| 400 | Invalid table name | `kQueryInvalidToken` (3001) | `:969-973` |
| 400 | Bare table name under multi-database config | `kQuerySyntaxError` (3000) | `:977-982` |
| 404 | Table not resolvable | `kTableNotFound` (4007) | `:985-999` |
| 404 | No such primary key, or document fetch returned empty | `kNotFound` (8) | `:1572-1581` |
| 500 | Null index or document store; unhandled exception | `kInternalError` (5) | `:1001-1006`, `:1597-1600` |
| 503 | Server loading | `kServerLoading` (6028) | `:1548-1552` |
| 503 | Table synchronizing | `kServerNotReady` (6029) | `:1563-1565` |

---

## GET /info

Server, memory, index, per-table and cache statistics. No parameters. Success `200`, `application/json` (`src/server/http_server.cpp:1603-1754`).

Statistics are read from the shared TCP `ServerStats` when one was injected, otherwise from the HTTP-local instance (`src/server/http_server.h:529-538`). Memory accounting comes from a bounded snapshot cache shared with `/metrics` (`src/server/http_server.cpp:1624-1626`).

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

- The system-memory block (`total_system_memory` … `system_memory_usage_ratio`) appears only when `GetSystemMemoryInfo()` succeeds; `system_memory_usage_ratio` additionally requires a non-zero total (`src/server/http_server.cpp:1661-1672`).
- The process block (`process_rss*`) appears only when `GetProcessMemoryInfo()` succeeds (`src/server/http_server.cpp:1675-1681`).
- `index.avg_postings_per_term` appears only when `total_terms > 0` (`src/server/http_server.cpp:1694-1697`).
- When the cache manager is absent or disabled, `cache` is exactly `{"enabled": false}` (`src/server/http_server.cpp:1739-1741`).
- A table whose metrics are missing from the snapshot is omitted from `tables` (`src/server/http_server.cpp:1630-1633`).

Errors: `500` `kInternalError` on an unhandled exception (`src/server/http_server.cpp:1746-1753`).

---

## GET /health

Always `200`, `application/json` (`src/server/http_server.cpp:1756-1768`).

```json
{ "status": "ok", "timestamp": 1756150000 }
```

`timestamp` is Unix seconds from `system_clock` (`src/server/http_server.cpp:1764-1765`).

## GET /health/live

Always `200` while the process serves requests (`src/server/http_server.cpp:1770-1780`).

```json
{ "status": "alive", "timestamp": 1756150000 }
```

## GET /health/ready

`200` when ready, `503` when not (`src/server/http_server.cpp:1843-1849`).

Readiness is not decided here. `HttpServer::CurrentReadinessInputs` collects the binlog reader, the dump-pause flag, the sync state and the initial-data checker (`src/server/http_server.cpp:1772-1786`), and `EvaluateReadiness` classifies them (`src/server/readiness.cpp:92-107`); this route renders the verdict (`src/server/http_server.cpp:1817`). The same verdict backs `GET /health/detail` (`src/server/http_server.cpp:1859`) and the TCP `INFO` command (`src/server/handlers/admin_handler.cpp:59-71`), so the three cannot disagree.

Readiness is `data_initialized && !loading && !sync_in_progress && replication_available` (`src/server/readiness.cpp:101-102`). Replication availability is one of seven states (`src/server/readiness.h:26-34`), classified in this order (`src/server/readiness.cpp:15-45`):

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

The `replication_*` block is present only in `USE_MYSQL` builds with a non-null binlog reader (`src/server/http_server.cpp:1822-1841`). `replication_running` reports availability, so it stays `true` through `starting`, `paused_for_dump` and `paused_for_sync` (`src/server/http_server.cpp:1824`).

When not ready, `status` is `"not_ready"` and a `reason` field is added, chosen in this order (`src/server/readiness.cpp:47-64`):

| Order | `reason` | Condition |
|---|---|---|
| 1 | `Initial data has not been loaded` | `!initial_data_ready` |
| 2 | `Server is loading` | `is_loading` |
| 3 | `Replication stopped due to an incompatible schema` | `HasSchemaIncompatibleError()` |
| 4 | the error code's own description, never MySQL's text | `GetLastError()` non-empty (`src/server/readiness.cpp:109-114`) |
| 5 | `SYNC is in progress` | `sync_in_progress` |
| 6 | `Replication is not running` | otherwise |

## GET /health/detail

Always `200`, `application/json` (`src/server/http_server.cpp:1852-1953`).

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
      "replication_state": "RUNNING",
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

- Top-level `status` is `"healthy"` when the readiness verdict is ready and `"degraded"` otherwise — the same verdict `GET /health/ready` renders, so this route never reports a fault the readiness probe calls healthy (`src/server/http_server.cpp:1859-1861`).
- `reason` is present only when `status` is `"degraded"`, and carries the same string `/health/ready` puts in its own `reason` (`src/server/http_server.cpp:1862-1864`).
- `uptime_seconds` here is measured from this `HttpServer`'s construction, not from the shared server stats (`src/server/http_server.cpp:1868-1871`).
- `components.cache` appears only when a cache manager is wired (`src/server/http_server.cpp:1902-1913`).
- `components.binlog` appears only in `USE_MYSQL` builds with a non-null reader (`src/server/http_server.cpp:1916-1947`). `status` is the availability state's own name — `connected`, `starting`, `paused_for_dump`, `paused_for_sync`, `failed` or `disconnected` (`src/server/http_server.cpp:1924`, `src/server/readiness.cpp:68-86`). Outside `connected`/`starting` the object carries `running: false` and `paused_for_dump` instead of `starting`/`current_gtid`/`processed_events`/`queue_size` (`src/server/http_server.cpp:1933-1936`).
- `components.binlog.replication_state` is the reader's own three-state lifecycle value and is independent of the availability classification (`src/server/http_server.cpp:1937`, `src/mysql/binlog_reader_interface.h:22-32`).

---

## GET /config

Redacted configuration summary. No parameters. `200`, `application/json` (`src/server/http_server.cpp:1950-1998`).

```json
{
  "mysql": { "configured": true, "database_defined": true },
  "api": { "tcp": { "enabled": true }, "http": { "enabled": true, "cors_enabled": false } },
  "network": { "allow_cidrs_configured": false },
  "replication": { "enable": true },
  "notes": "Sensitive configuration values are redacted over HTTP. Use CONFIG SHOW over a secured connection for details."
}
```

`api.tcp.enabled` is a hard-coded `true` (`src/server/http_server.cpp:1970`). No credential, bind or port value is exposed.

| Status | Condition | `error_code` | Citation |
|---|---|---|---|
| 500 | No configuration wired | `kInternalError` (5) | `:1954-1957` |
| 500 | Unhandled exception | `kInternalError` (5) | `:1990-1997` |

---

## GET /replication/status

`200`, `application/json` (`src/server/http_server.cpp:2000-2039`).

```json
{
  "enabled": true,
  "status": "RUNNING",
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

`enabled` reflects `IsRunning()`, not configuration (`src/server/http_server.cpp:2012-2014`).

| Status | Condition | `error_code` | Citation |
|---|---|---|---|
| 500 | Unhandled exception | `kInternalError` (5) | `:2028-2035` |
| 503 | Replication not configured (null binlog reader) | `kNotImplemented` (4) | `:2005-2008` |
| 503 | Built without MySQL support | `kNotImplemented` (4) | `:2036-2038` |

---

## POST /optimize

Runs the shared maintenance handler for one table or for every configured table. This is the only route with an authentication gate (`src/server/http_server.cpp:2050-2063`).

Order of checks: `Content-Type` → bearer token → JSON parse → object shape → field allowlist → table resolution → callback availability (`src/server/http_server.cpp:2044-2104`). Content-type is validated **before** authentication.

### Request body

| Field | Type | Required | Meaning |
|---|---|---|---|
| `table` | string | no | Optimize this table; absent or `{}` optimizes every configured table (`src/server/http_server.cpp:2085-2098`, `:2107`) |

The body must be a JSON object (`src/server/http_server.cpp:2073-2076`) and **any field other than `table` is rejected** (`src/server/http_server.cpp:2077-2083`).

### Success

`200`, `application/json`, when the callback's return value starts with `"OK "` (`src/server/http_server.cpp:2108-2115`):

```json
{ "status": "ok", "result": "OPTIMIZED tables=1 elapsed_ms=12" }
```

`result` is the callback string with the `"OK "` prefix stripped.

### Errors

| Status | Condition | `error_code` | Citation |
|---|---|---|---|
| 400 | Body is not a JSON object | `kQuerySyntaxError` (3000) | `:2074` |
| 400 | Invalid JSON | `kQuerySyntaxError` (3000) | `:2069` |
| 400 | Unsupported field | `kQuerySyntaxError` (3000) | `:2080` |
| 400 | `table` not a string | `kQuerySyntaxError` (3000) | `:2088` |
| 400 | Invalid table name | `kQueryInvalidToken` (3001) | `:969-973` |
| 401 | Missing or wrong bearer token (adds `WWW-Authenticate: Bearer`) | `kPermissionDenied` (7) | `:2058-2060` |
| 404 | Table not resolvable | `kTableNotFound` (4007) | `:985-999` |
| 415 | `Content-Type` not `application/json` | `kNetworkInvalidRequest` (6007) | `:2045` |
| 503 | No optimize callback wired | `kServerInitMissingDependency` (6026) | `:2101-2103` |
| 503 | Callback returned a coded `ERROR` frame | the frame's own code | `:2117-2121` |
| 503 | Callback returned anything else non-`OK` | `kInternalError` (5) | `:2123` |

Every callback failure becomes `503`, regardless of the error code carried in the frame.

---

## GET /metrics

Prometheus text exposition. `200`, `Content-Type: text/plain; version=0.0.4; charset=utf-8` (`src/server/http_server.cpp:2141-2142`). Body is produced by `ResponseFormatter::FormatPrometheusMetrics` (`src/server/response_formatter.cpp:756` onward).

Metric families emitted (all prefixed `mygramdb_`): `server_info`, `server_uptime_seconds`, `server_commands_total`, `text_normalization_failures_total`, `command_total{command}`, `memory_used_bytes{type}`, `memory_peak_bytes`, `memory_fragmentation_ratio`, `memory_system_total_bytes`, `memory_system_available_bytes`, `memory_system_usage_ratio`, `memory_process_rss_bytes`, `memory_process_rss_peak_bytes`, `memory_health_status`, `index_documents_total{table}`, `index_terms_total{table}`, `index_postings_total{table}`, `index_postings_per_term_avg{table}`, `index_delta_encoded_lists{table}`, `index_roaring_bitmap_lists{table}`, `index_optimization_in_progress{table}`, `clients_connected`, `clients_total`, `dump_last_success_timestamp_seconds`, `dump_failures_total{trigger}`, `requests_denied_total{reason,surface}`, `thread_pool_queue_depth`, `thread_pool_queue_capacity`, `thread_pool_workers`, `replication_*`, `cache_*` (`src/server/response_formatter.cpp:756-1188`).

`requests_denied_total` carries a `surface` label with values `tcp` and `http`, so ACL and rate-limit denials are attributable per protocol (`src/server/response_formatter.cpp:990-1001`).

Errors: `500` `kInternalError` (`application/json`) on an unhandled exception (`src/server/http_server.cpp:2143-2150`).

---

## Authentication

| Property | Value | Citation |
|---|---|---|
| Scope | `POST /optimize` only | `src/server/http_server.cpp:2050` |
| Gate condition | `api.admin_token` is non-empty; an empty token disables the check entirely | `src/server/http_server.cpp:2050` |
| Transport | `Authorization` request header | `src/server/http_server.cpp:2052` |
| Scheme | Literal prefix `Bearer ` (one trailing space, case-sensitive) | `src/server/http_server.cpp:2051-2054` |
| Comparison | `ConstantTimeEqual` — length difference folded in, compared over `max(len)` bytes | `src/server/http_server.cpp:390-399` |
| Failure status | `401` with `WWW-Authenticate: Bearer` | `src/server/http_server.cpp:2058-2059` |
| Failure body | `{"error":"Administrative endpoint requires a valid bearer token","error_code":7}` | `src/server/http_server.cpp:2059-2060` |

There is no session, cookie, API-key-parameter or query-string authentication path. No other route consults `api.admin_token`.

## Network ACL

A pre-routing handler runs before every route, including `/health*` and `/metrics` (`src/server/http_server.cpp:740-792`).

- The client identity is `req.remote_addr`, or the literal `"unknown"` when empty (`src/server/http_server.cpp:742`).
- `X-Forwarded-For` is honoured only when the direct peer matches `api.http.trusted_proxies` exactly (`src/server/http_server.cpp:651-655`).
- A peer outside `network.allow_cidrs` gets `403` with `kPermissionDenied` and message `Access denied by network.allow_cidrs` (`src/server/http_server.cpp:755-770`).
- ACL denials increment `requests_denied_total{reason="acl",surface="http"}`, and `total_requests` only on a route the table marks `counts_requests` (`src/server/http_server.cpp:756-759`).
- ACL rejection happens **before** the rate limiter, so denied peers cannot consume or evict rate-limit buckets (`src/server/http_server.cpp:755-773`).
- Repeated denials from the same IP are log-suppressed by `DenialLogLimiter` (`src/server/http_server.cpp:760-767`).

## Rate limiting

- Applied in the same pre-routing handler, to every route the descriptor table marks `rate_limited` (`src/server/http_server.cpp:750`, `:773`).
- `GET /health`, `GET /health/live` and `GET /health/ready` are **exempt** (`src/server/http_server.cpp:672-674`). A `429` on a liveness or readiness probe is read as a dead process by whatever is polling it, so traffic that happens to share the bucket could take a healthy server out of rotation. What still bounds them: `network.allow_cidrs` is enforced first and is unaffected (`src/server/http_server.cpp:755`), `api.http.max_connections` caps accepted sockets, and each response is a fixed-size JSON object built from atomics — no index walk, no document store access, no amplification.
- `GET /health/detail` stays rate limited (`src/server/http_server.cpp:675`): it walks every table's index and document store under the generation lock (`src/server/http_server.cpp:1882-1895`), so its cost grows with the corpus.
- Bucket key is the client IP string (`src/server/http_server.cpp:742`, `:773`).
- When `ServerLifecycleManager`/`ServerOrchestrator` wires the server, the limiter instance is shared with the TCP server, so one client's quota spans both protocols (`src/app/server_orchestrator.cpp:943`, `src/server/http_server.h:318-324`).
- When no limiter is injected and `api.rate_limiting.enable` is true, `HttpServer` constructs a private one from config (`src/server/http_server.cpp:627-637`).
- Algorithm is a per-client token bucket with `capacity` burst and `refill_rate` tokens/second. `max_clients` bounds the size of the tracking table, not admission: once it is reached, an untracked client's request evicts the least-recently-seen bucket and is then served normally, so the only cause of a denial is an exhausted bucket (`src/server/rate_limiter.cpp:111-173`).
- Denial: `429` with `kServerBusy` (6030) and message `Rate limit exceeded` (`src/server/http_server.cpp:786`).
- Denials increment `requests_denied_total{reason="rate_limit",surface="http"}`, and `total_requests` only on a route the table marks `counts_requests` (`src/server/http_server.cpp:774-777`).

## Request size limits, timeouts and concurrency caps

| Limit | Value | Citation |
|---|---|---|
| Request body | `api.http.max_body_bytes`, default 16 MiB; oversize bodies get cpp-httplib's `413` before any handler runs. `0` lifts the cap entirely | `src/server/http_server.cpp:501-502`, `src/server/http_server.h:85`, `src/config/config.h:450` |
| Read timeout | `api.http.read_timeout_sec`, default 5 s; non-positive values fall back to the default | `src/server/http_server.cpp:658`, `src/server/http_server.h:103-105`, `src/config/config.h:436` |
| Write timeout | `api.http.write_timeout_sec`, default 5 s; same fallback | `src/server/http_server.cpp:659`, `src/server/http_server.h:106-108`, `src/config/config.h:442` |
| Concurrent connections | `api.http.max_connections`, default 10000, clamped to ≥ 1; enforced at the accepted-socket boundary by `CappedHttpTaskQueue`, covering both queued and active sockets | `src/server/http_server.cpp:84-121`, `:646-649`, `src/config/config.h:425` |
| Worker threads | cpp-httplib defaults: `max(8, hardware_concurrency-1)` initial, ×4 maximum | `src/server/http_server.cpp:90`, `build/_deps/httplib-src/httplib.h:162-170` |
| Request URI | 8192 bytes; longer gets `414` | `build/_deps/httplib-src/httplib.h:110`, `:12551` |
| Header line | 8192 bytes | `build/_deps/httplib-src/httplib.h:114` |
| Keep-alive | 5 s idle timeout, 100 requests per connection | `build/_deps/httplib-src/httplib.h:26`, `:34` |
| Startup readiness | `Start()` waits up to 5 s for the accept loop to come up, otherwise fails with `kNetworkBindFailed` | `src/server/http_server.cpp:71`, `:888-908` |

Rejections at the cpp-httplib layer (`413`, `414`, `400` on a malformed request line) carry cpp-httplib's own body, not the JSON error object.

## CORS and security headers

CORS is active only when `api.http.enable_cors` is true **and** `api.http.cors_allow_origin` is non-empty; a missing origin logs `http_cors_disabled_missing_allow_origin` and leaves CORS off rather than defaulting to a wildcard (`src/server/http_server.cpp:677-681`).

When active:

| Header | Value | Where | Citation |
|---|---|---|---|
| `Access-Control-Allow-Origin` | configured `cors_allow_origin` verbatim | every response, via the post-routing handler | `src/server/http_server.cpp:785-787` |
| `Access-Control-Allow-Methods` | `GET, POST, OPTIONS` | `OPTIONS` preflight only | `src/server/http_server.cpp:779` |
| `Access-Control-Allow-Headers` | `Content-Type, Authorization` | `OPTIONS` preflight only | `src/server/http_server.cpp:780` |

The preflight response is `204` with no body (`src/server/http_server.cpp:781`).

No other security headers are emitted: there is no `Strict-Transport-Security`, `X-Content-Type-Options`, `X-Frame-Options`, `Content-Security-Policy`, `Referrer-Policy`, or `Access-Control-Allow-Credentials` anywhere in `src/server/http_server.cpp`. TLS is not terminated by this server; only `httplib::Server` (plain HTTP) is instantiated (`src/server/http_server.cpp:640`).

## Status-code mapping

Only three internal codes are mapped by range/identity; every other query-path error becomes `400` (`src/server/http_server.cpp:401-409`):

| Internal code | Numeric | HTTP status |
|---|---|---|
| `kServerShuttingDown` | 6027 | 503 |
| `kServerLoading` | 6028 | 503 |
| `kInternalError` | 5 | 500 |
| anything else reaching `HttpStatusForQueryError` | — | 400 |

Statuses chosen directly by handlers, independent of that helper:

| Condition | Status | Internal code | Citation |
|---|---|---|---|
| ACL denial | 403 | `kPermissionDenied` (7) | `src/server/http_server.cpp:749-750` |
| Rate limit | 429 | `kServerBusy` (6030) | `src/server/http_server.cpp:765` |
| Wrong `Content-Type` | 415 | `kNetworkInvalidRequest` (6007) | `src/server/http_server.cpp:1041`, `:2045` |
| Missing/bad bearer token | 401 | `kPermissionDenied` (7) | `src/server/http_server.cpp:2059` |
| Table not found | 404 | `kTableNotFound` (4007) | `src/server/http_server.cpp:987-989` |
| Document not found | 404 | `kNotFound` (8) | `src/server/http_server.cpp:1573`, `:1579` |
| Null index/doc store | 500 | `kInternalError` (5) | `src/server/http_server.cpp:1002-1004` |
| Server loading | 503 | `kServerLoading` (6028) | `src/server/http_server.cpp:1046-1047` |
| Table synchronizing | 503 | `kServerNotReady` (6029) | `src/server/http_server.cpp:1016-1017` |
| Replication not configured / not compiled | 503 | `kNotImplemented` (4) | `src/server/http_server.cpp:2006`, `:2037` |
| Optimize callback failure | 503 | frame code or `kInternalError` | `src/server/http_server.cpp:2120`, `:2123` |
| Optimize callback missing | 503 | `kServerInitMissingDependency` (6026) | `src/server/http_server.cpp:2101-2102` |
| Not ready (`/health/ready`) | 503 | — (health bodies carry no `error_code`) | `src/server/http_server.cpp:1849` |

By error-code range (ranges from the project's error taxonomy, `src/utils/error.h`):

| Range | Module | HTTP statuses actually produced |
|---|---|---|
| 0–999 | General | 500 (`kInternalError` 5), 404 (`kNotFound` 8), 401/403 (`kPermissionDenied` 7), 400 (`kInvalidArgument` 2, `kNotImplemented` 4), 503 (`kNotImplemented` 4 on the replication route) |
| 3000–3999 | Query/Request parsing | 400 exclusively |
| 4000–4999 | Index/Search | 404 for `kTableNotFound` (4007); 400 for others reaching `HttpStatusForQueryError` |
| 6000–6999 | Network/Server | 415 (6007), 503 (6026/6027/6028/6029), 429 (6030) |

`kNotImplemented` (4) maps to 400 when it arrives through `HttpStatusForQueryError` (e.g. the highlight/text-storage error at `src/server/http_server.cpp:1399`) and to 503 when a handler picks the status directly (`src/server/http_server.cpp:2006`).

---

## Divergences from the TCP surface

The TCP surface is normative. The differences below are recorded as observed, with no fix implied.

### Authentication and authorization

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 3 | Credential transport | `AUTH <token>` command, sets per-connection state (`src/server/request_dispatcher.cpp:150-161`) | `Authorization: Bearer <token>` per request, no session (`src/server/http_server.cpp:610-622`) |

Which commands are administrative no longer differs: a route's `requires_admin_token` field (`src/server/http_server.cpp:570-571`) is enforced in the shared route wrapper before any handler runs (`:610-622`), so `GET /config` and `GET /replication/status` answer an uncredentialed request with 401 and `kPermissionDenied` exactly as the TCP surface does. The credential check precedes the `Content-Type` check on `/optimize` for the same reason. Pinned by `HttpTcpConsistencyTest.AdministrativeReportsAreTokenGatedOnBothSurfaces` and `.HttpOptimizeChecksTheTokenBeforeTheContentType`.

### Table resolution

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 5 | Table-name character set | No whitelist; the token is whatever the tokenizer produced (`src/query/query_parser_commands.cpp:350`, `:437`) | `QueryParser::IsSafeTableName`: ≤ `kMaxTableNameLength` (256) bytes, ASCII limited to `[A-Za-z0-9_.-]` plus well-formed non-ASCII UTF-8; anything else is 400 `kQueryInvalidToken` (`src/query/query_parser.cpp`, `ResolveHttpTableContext`) |
| 6 | Bare name under a multi-database configuration | `kTableNotFound` (4007) with message `Bare table names are not supported; use <database>.<table>: …` (`src/server/handlers/command_handler.cpp`) | Same message and the same `QueryParser::IsDatabaseQualifiedTableName` test, but `kQuerySyntaxError` (3000) and HTTP 400 (`ResolveHttpTableContext`, `src/server/http_server.cpp`) |
| 7 | Partially initialised table context | Reported as `Index not available` / `Document store not available`, `kInternalError` (`src/server/handlers/search_handler.cpp:214-216`, `:333-335`) | Reported earlier during resolution as `Table context has null index or doc_store`, `kInternalError` / 500 (`src/server/http_server.cpp:1001-1006`) |

### Accepted parameters

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 8 | Repeated conditions on one filter column | `FILTER` clauses are a list; the same column may appear more than once (`src/query/query_parser_clauses.cpp:97`) | `filters` is a JSON object, so at most one condition per column survives (`src/server/http_server.cpp:317`) |
| 9 | Filter value starting with an operator character | Rejected: `FILTER value must not start with an operator character`, `kQueryInvalidFilter` (`src/query/query_parser_clauses.cpp:35-41`, `:138`, `:177`) | No such check; the value is accepted (`src/server/http_server.cpp:309-358`) |
| 10 | Non-string filter values | Values arrive as text tokens only (`src/query/query_parser_clauses.cpp:150`, `:176`) | JSON integers, floats and booleans are coerced: `true`→`"1"`, `false`→`"0"`, floats via `std::to_string(double)` (fixed six decimals) (`src/server/http_server.cpp:177-191`) |
| 12 | Boolean-expression validation | `q` goes through `ParseSearchTextTokens`, which rejects unmatched/unclosed parentheses, the deprecated `ORDER` keyword, comma-separated table lists, and splits flat `AND`/`NOT` clauses (`src/query/query_parser_commands.cpp:72-217`) | `mode:"boolean"` assigns `q` straight to `search_text`/`search_expression`; none of those checks or the clause split run (`src/server/http_server.cpp:1234-1238`) |
| 13 | FACET search text | Runs through the parser's shared search-text extraction (`src/query/query_parser_commands.cpp:467`) | Assembled field by field; `QueryParser::Parse` is never invoked on the facet path (`src/server/http_server.cpp:1308-1323`) |
| 14 | COUNT clause rejection | Rejects `SORT`, `ORDER`, and any clause other than `AND`/`NOT`/`FILTER`, including `LIMIT`/`OFFSET`, all as `kQuerySyntaxError` (`src/query/query_parser_commands.cpp:382-394`) | Rejects exactly the fields `limit`, `offset`, `sort`, `highlight`, `fuzzy` as `kQuerySyntaxError`; other unknown JSON fields are silently ignored (`src/server/http_server.cpp:1206-1219`) |
| 15 | Unknown clause / field | On `FACET` and `COUNT` an unrecognised clause keyword is a hard `kQuerySyntaxError` (`src/query/query_parser_commands.cpp:301-303`, `:507-509`). On `SEARCH` it is not: trailing tokens are absorbed into the search text, so `SEARCH t term BOGUSCLAUSE 3` answers `OK RESULTS 0` | Unknown JSON body fields are ignored on `/search`, `/count` and `/facet`; only `/optimize` enforces an allowlist (`src/server/http_server.cpp:2077-2083`) |

### Validation limits

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 16 | LIMIT above 1000 | `kQuerySyntaxError` (3000), message `LIMIT exceeds maximum of 1000` (`src/query/query_parser_commands.cpp:322-326`, `:528-532`) | `kQueryInvalidLimit` (3008), message `Invalid limit: must be between 1 and 1000` (`src/server/http_server.cpp:1088-1093`) |
| 17 | More than 64 filter conditions | `kQuerySyntaxError` (3000) (`src/query/query_parser_commands.cpp:316-319`, `:406-409`, `:522-525`) | `kQueryInvalidFilter` (3006) (`src/server/http_server.cpp:311-315`) |
| 18 | Invalid facet column | `kQuerySyntaxError` (3000) (`src/query/query_parser_commands.cpp:462-465`) | `kQueryInvalidToken` (3001) (`src/server/http_server.cpp:1296-1300`) |
| 19 | Highlight tag over 256 bytes | `kQuerySyntaxError`, message names `HIGHLIGHT TAG open/close tag` (`src/query/query_parser_clauses.cpp:390-399`) | `kQuerySyntaxError`, message names `Field 'highlight.open_tag'` (`src/server/http_server.cpp:477-490`). At the default `api.max_query_length` of 128 characters a 256-byte tag makes the assembled TCP command 268 characters, so that surface answers `kQueryTooLong` before the tag cap is reached; only the rejection can be compared |
| 21 | Control characters in search text | No explicit `\r`/`\n`/`\0` rejection in the parser; framing removes the line terminator | `q` containing `\r`, `\n` or `\0` is 400 `kQueryInvalidToken` (`src/server/http_server.cpp:1159-1166`) |

### Error mapping

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 25 | OPTIMIZE failure | The handler returns the condition's own code, e.g. `kServerBusy` (6030) when a DUMP LOAD or another long operation is in flight (`src/server/handlers/debug_handler.cpp:43-72`), reached over HTTP through the shared entry point `TcpServer::HandleOptimizeRequest` (`src/server/tcp_server.cpp:93`) | The frame's code is preserved in the body but the status is always 503, whatever the code (`src/server/http_server.cpp:2117-2123`) |
| 26 | Error envelope | Single line `ERROR <code> <message>`, parsed by `ParseErrorFrame` (`src/server/protocol_constants.h:75-92`) | JSON object `{"error": …, "error_code": …}` (`src/server/http_server.cpp:2158-2164`) |
| 27 | Transport-layer rejections | All failures are `ERROR` frames | `413`, `414` and malformed-request-line `400` come from cpp-httplib with a non-JSON body (`build/_deps/httplib-src/httplib.h:12283`, `:12551`, `:12526`); unmatched routes get an empty-bodied `404` (`:12773`) |

### Escaping and quoting

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 28 | Literal search text | The client supplies the quoting; the tokenizer processes `\n`, `\t`, `\r`, `\\` and `\"` escape sequences (`src/query/query_parser.cpp:545-561`) | The server quotes `q` itself with `QueryParser::QuoteSearchLiteral`, the parser's own escaper for `\` and `"`; `\n`-style sequences in `q` stay literal because control characters were already rejected (`src/query/query_parser_commands.cpp`) |
| 29 | Response escaping | Primary keys and facet values are escaped/sanitised for the delimited text protocol (`src/server/response_formatter.cpp:309`, `:418`) | Values are emitted as JSON strings with invalid UTF-8 replaced by U+FFFD (`src/server/http_server.cpp:2155`) |
| 31 | Floating-point filter values in responses | Fixed six decimals (`src/server/response_formatter.cpp:458-459`) | Full JSON double serialization (`src/server/http_server.cpp:154-170`) |

### Result payload shape

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 32 | SEARCH results | `OK RESULTS <total> <pk> <pk> …` — primary keys only, no filter values (`src/server/response_formatter.cpp:300-311`) | Each result object carries `primary_key` plus the document's `filters` (`src/server/http_server.cpp:1408-1418`) |
| 33 | Missing documents in a result page | Counted and logged, but the response still lists only resolvable keys (`src/server/response_formatter.cpp:305-311`) | Silently skipped, so `results.length` can be below `limit` while `count` is higher (`src/server/http_server.cpp:1406-1407`) |
| 34 | Debug output | `DEBUG ON` adds a `# DEBUG` block with timings, n-gram counts, cache status and the applied sort (`src/server/response_formatter.cpp:227-276`) | No debug mode exists on the HTTP surface; `conn_ctx.debug_mode` has no HTTP equivalent |
| 35 | FACET counts | `OK FACET <returned> <total>` then `value\tcount` lines (`src/server/response_formatter.cpp:408-431`) | `column`, `count`, `total_count`, `facets[]` — carries the column name, which the TCP frame does not (`src/server/http_server.cpp:1526-1530`) |

### Sorting and scoring

Both surfaces order a result page through `search_pipeline::SortAndPaginateResults`, which routes `SORT _score` to `ScoreAndSortByRelevance` and every other order to `ResultSorter::SortAndPaginate`, and both build snippets through `search_pipeline::GenerateHighlightSnippets`. Sort resolution, scoring and highlighting therefore have no surface-specific behaviour of their own.

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 38 | Top-N optimization | `ApplySearchTopNOptimization`, with the chosen strategy surfaced in debug output (`src/server/handlers/search_handler.cpp`) | Same call, but the strategy is not reported anywhere in the response (`src/server/http_server.cpp`) |
| 47 | Order of the HIGHLIGHT storage check | Rejected before the page is ordered, so a request that both asks for `HIGHLIGHT` without stored text and orders by an unavailable `_score` reports the highlight fault (`src/server/handlers/search_handler.cpp`) | Rejected after the page is ordered, so the same request reports the sort fault instead (`src/server/http_server.cpp`) |

### Request and rate-limit accounting

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 39 | `total_requests` | Incremented once per dispatched request, including rate-limited and malformed input (`src/server/request_dispatcher.cpp:104`) | Incremented per handler, and in the pre-routing denial branches, on every route the descriptor table marks `counts_requests`; `/health`, `/health/live`, `/health/ready` and `/health/detail` never increment it, on any outcome (`src/server/http_server.cpp:672-675`, `:756-759`, `:774-777`) |
| 41 | Rate-limit token consumption | One token per parsed request (`src/server/request_dispatcher.cpp:110`) | One token per HTTP request on every route the descriptor table marks `rate_limited`, which includes `/metrics`, `/health/detail` and CORS preflights but excludes `/health`, `/health/live` and `/health/ready` (`src/server/http_server.cpp:672-675`, `:773`) |
| 42 | Command counters | Incremented after a successful parse, before dispatch, for every command type (`src/server/request_dispatcher.cpp:175`) | Incremented per handler; `/metrics` and the health routes record no command at all (`src/server/http_server.cpp:2126-2142`) |
| 43 | Network ACL enforcement point | Applied when a connection is accepted | Applied per request, before routing (`src/server/http_server.cpp:732-752`) |
| 44 | Denial counters | `requests_denied_total{surface="tcp"}`, with additional `connection_limit` and `pool_full` reasons | `requests_denied_total{surface="http"}`, only `acl` and `rate_limit` reasons are ever produced (`src/server/response_formatter.cpp:990-1001`) |

### Readiness reporting

| # | Aspect | TCP behaviour | HTTP behaviour |
|---|---|---|---|
| 45 | Overall readiness | `INFO` reports `readiness: ready` / `not_ready` from the shared verdict (`src/server/handlers/admin_handler.cpp:62-72`, `src/server/response_formatter.cpp:488`) | `/health/ready` and `/health/detail` render the same verdict, adding the `reason` and the per-component breakdown the text frame has no room for (`src/server/http_server.cpp:1817`, `:1859`) |
| 46 | Replication state in the overall report | `INFO`'s `replication_status` line is the reader's raw `IsRunning()`, so a dump pause or a SYNC reads as `stopped` (`src/server/response_formatter.cpp:623`) | `/health/detail` reports the availability classification, which names `paused_for_dump` and `paused_for_sync` separately from `disconnected` (`src/server/http_server.cpp:1924`) |

### TCP commands with no HTTP route

`AUTH`, `DUMP SAVE/LOAD/VERIFY/INFO/STATUS`, `SAVE`, `LOAD`, `REPLICATION STOP`, `REPLICATION START`, `SYNC`, `SYNC STATUS`, `SYNC STOP`, `CONFIG HELP`, `CONFIG VERIFY`, `DEBUG ON`, `DEBUG OFF`, `CACHE CLEAR`, `CACHE STATS`, `CACHE ENABLE`, `CACHE DISABLE`, `SET`, and `SHOW VARIABLES` exist only on the TCP surface (`src/query/query_parser.h:37-83`, `src/server/http_server.cpp:682-729`). Cache statistics are the only one of these that is partially reachable over HTTP, as the read-only `cache` object inside `GET /info` (`src/server/http_server.cpp:1706-1742`).

`GET /metrics`, `GET /health`, `GET /health/live`, `GET /health/ready` and `GET /health/detail` have no TCP counterpart (`src/server/http_server.cpp:699-722`).
