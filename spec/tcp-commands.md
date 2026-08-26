# TCP Text Protocol — Command Specification

This file is normative. It describes the observed current behavior of the MygramDB TCP text
surface as implemented in `src/query/` and `src/server/`. Where the implementation deviates
from anything stated here, the implementation is the bug. Every claim names the file that
implements it, so the document can be re-verified against the source; see `spec/README.md`
for the citation rules and the check that enforces them.

---

## 1. Wire framing

| Property | Value | Implementation |
|---|---|---|
| Request frame delimiter | `\r\n` (CRLF) | `src/server/reactor_connection.cpp` |
| Response frame terminator | `\r\n`, appended by the transport to every response body | `src/server/reactor_connection.cpp` |
| Encoding | Raw bytes; UTF-8 expected in search text. Invalid UTF-8 in search terms is rejected by the pipeline | `src/server/search_pipeline.cpp` |
| Receive chunk size | 4096 bytes per `recv()` | `src/server/reactor_connection.cpp` |
| Max unframed tail (effective max request line) | 1 MiB (`kMaxReadBufferBytes`) | `src/server/reactor_connection.h` |
| Max single frame length | 1 MiB, and additionally bounded by the remaining pending-frame byte budget | `src/server/reactor_connection.cpp` |
| Max queued frames per connection | 1024 (`kMaxPendingFrames`), configurable via `api.tcp.max_pending_frames` | `src/server/reactor_connection.h`, `src/server/server_types.h` |
| Max queued frame bytes per connection | 4 MiB (`kMaxPendingFrameBytes`), configurable via `api.tcp.max_pending_frame_bytes` | `src/server/reactor_connection.h`, `src/server/server_types.h` |
| Per-connection unsent response cap | 16 MiB (`api.tcp.max_write_queue_bytes`) | `src/server/reactor_connection.h`, `src/server/server_types.h` |
| Shared read+write byte budget across all connections | 256 MiB (`api.tcp.max_total_buffered_bytes`) | `src/server/server_types.h` |
| Read fairness budget per readable event | 64 KiB or 64 frames, whichever comes first | `src/server/reactor_connection.h`, `src/server/reactor_connection.cpp` |

### 1.1 Frame extraction rules

- Frames are split on the exact two-byte sequence `\r\n`. A lone `CR` not followed by `LF` is
  skipped and scanning continues; it does not terminate a frame
  (`src/server/reactor_connection.cpp`).
- A trailing `CR` at the end of the read buffer is retained so a following `LF` from the next
  `recv()` can complete the frame (`src/server/reactor_connection.cpp`).
- A bare `\r\n` yields a zero-length frame, which is dispatched normally
  (`src/server/reactor_connection.cpp`) and answered with `ERROR 3000 Empty query`
  (`src/query/query_parser.cpp`).
- Pipelining is supported: multiple CRLF-delimited frames in one `recv()` are all queued and
  dispatched in order by a single drain worker (`src/server/reactor_connection.cpp`).
- Backpressure: reads are disarmed at 75% of either pending-frame limit and re-armed at 50%
  (`src/server/reactor_connection.cpp`).

### 1.2 Limit-exceeded behavior

Every citation in this table is `src/server/reactor_connection.cpp` unless the cell names
another file.

| Condition | Server action | Implementation |
|---|---|---|
| Unframed tail exceeds 1 MiB | Best-effort send `ERROR 6007 request too large`, then close | |
| Pending-frame count/byte cap exceeded, or shared budget exhausted | Best-effort send `ERROR 6030 server busy`, then close | |
| Event multiplexer rejects the backpressure interest update | Best-effort send `ERROR 6019 event multiplexer modify failed`, then close | |
| Unsent response bytes exceed the per-connection cap | Connection closed with no error frame | |
| Thread pool queue full when scheduling a drain task | Send `ERROR 6030 SERVER_BUSY Server is too busy, please try again later` on that request's own connection, count it in `requests_denied_pool_full_tcp`, then close | |
| Reactor registration fails at accept time | Send `ERROR 6030 SERVER_BUSY Server is too busy, please try again later`, then close | `src/server/connection_acceptor.cpp` |

The best-effort error frames above are written with a blocking `send()` loop only when the
write queue is empty, and are followed unconditionally by connection teardown
(`src/server/reactor_connection.cpp`).

### 1.3 Timeouts

| Timeout | Default | Config key | Implementation |
|---|---|---|---|
| Initial read (first complete frame) | 60 s | `api.tcp.recv_timeout_sec` | `src/server/server_types.h`, `src/server/io_reactor.cpp` |
| Idle (no read or write activity) | 300 s | `api.tcp.idle_timeout_sec` | `src/server/server_types.h`, `src/server/io_reactor.cpp` |
| Reaper sweep interval | 5 s | `api.tcp.reaper_interval_sec` | `src/server/server_types.h`, `src/server/io_reactor.cpp` |
| Poll timeout | 100 ms | not configurable | `src/server/io_reactor.h` |

`0` disables either timeout (`src/server/io_reactor.cpp`).
A connection with a request currently queued or executing in the drain worker is exempt from
idle reaping (`src/server/io_reactor.cpp`). **Reaping closes the socket without
sending any frame** (`src/server/io_reactor.cpp`).

TCP keepalive is applied per accepted socket: enabled by default, 60 s idle, 20 s interval,
3 probes (`src/server/server_types.h`, `src/server/connection_acceptor.cpp`).

### 1.4 Admission control before any command is parsed

1. **Connection limit** — `api.tcp.max_connections` (default 10000). Exceeding it closes the
   accepted socket immediately with no response
   (`src/server/connection_acceptor.cpp`, `src/server/server_types.h`).
2. **Network ACL** — `network.allow_cidrs`. A peer outside the allow list is closed with no
   response. Not applied on Unix domain sockets
   (`src/server/connection_acceptor.cpp`).

---

## 2. Tokenization, quoting and escaping

Every request frame is tokenized by `QueryParser::Tokenize`
(`src/query/query_parser.cpp`) before command dispatch.

### 2.1 Splitting

- Outside quotes, tokens are split on ASCII **and** Unicode whitespace
  (`src/query/query_parser.cpp`).
- `"` and `'` both open a quoted region. Opening a quote flushes any pending unquoted token
  first, so `abc"def"` yields two tokens `abc` and `def`
  (`src/query/query_parser.cpp`).
- Inside quotes, whitespace is literal (`src/query/query_parser.cpp`).
- A closing quote always emits a token, **including an empty one**, and the token is flagged
  as quoted (`src/query/query_parser.cpp`). The quoted flag suppresses clause-keyword
  and parenthesis interpretation for that token
  (`src/query/query_parser_commands.cpp`).

### 2.2 Escape sequences

Backslash is escape syntax **only inside a quoted literal**. Outside quotes a backslash is a
literal character, so Windows-style paths survive intact
(`src/query/query_parser.cpp`).

| Sequence | Produces |
|---|---|
| `\n` | LF |
| `\t` | TAB |
| `\r` | CR |
| `\\` | `\` |
| `\"` | `"` |
| `\'` | `'` |
| `\xHH` | the byte `0xHH` |
| `\<other>` | `<other>` verbatim; the backslash is dropped |

Every sequence above is decoded in `src/query/query_parser.cpp`.

`\x` not followed by two valid hex digits (or with fewer than two bytes remaining) emits the
literal character `x` and drops the backslash (`src/query/query_parser.cpp`).

### 2.3 Tokenizer failure modes

| Input condition | Result |
|---|---|
| Backslash is the final byte of the frame | `ERROR 3000 Unterminated escape sequence at end of input` (`src/query/query_parser.cpp`) |
| Quote never closed | `ERROR 3000 Unclosed quote: <char>` (`src/query/query_parser.cpp`) |
| No tokens produced | `ERROR 3000 Empty query` (`src/query/query_parser.cpp`) |

### 2.4 Parenthesis accounting

Parentheses inside quoted regions are ignored for depth tracking. A quote preceded by an odd
number of backslashes does not toggle the quote state
(`src/query/query_parser.cpp`). Unbalanced parentheses in search text produce
`ERROR 3000 Unmatched closing parenthesis` or `ERROR 3000 Unclosed parenthesis`
(`src/query/query_parser_commands.cpp`).

---

## 3. Case sensitivity

- **Command and subcommand keywords are case-insensitive.** All dispatch comparisons go
  through `EqualsIgnoreCase` (`src/query/query_parser_internal.h`, used throughout
  `src/query/query_parser.cpp`).
- **Clause keywords are case-insensitive** (`AND`, `OR`, `NOT`, `FILTER`, `SORT`, `LIMIT`,
  `OFFSET`, `HIGHLIGHT`, `FUZZY`, `FACET`) — `src/query/query_parser_internal.h`.
- **Table names, column names, primary keys, file paths and variable values preserve their
  original case** (`src/query/query_parser.cpp`,
  `src/query/query_parser_clauses.cpp`).
- Named filter operators (`EQ`, `NE`, `GT`, `GTE`, `LT`, `LTE`) are case-insensitive; symbolic
  ones are exact (`src/query/query_parser_clauses.cpp`).
- `ASC` / `DESC` / `BY` are case-insensitive (`src/query/query_parser_clauses.cpp`).

---

## 4. Authentication

The shared secret is `api.admin_token` (`src/config/config.h`), surfaced to the dispatcher
as `ServerConfig::admin_token` (`src/server/server_types.h`).

### 4.1 `AUTH` command

```
AUTH <shared-secret>
```

Every citation in this table is `src/server/request_dispatcher.cpp` unless the cell names
another file.

| Property | Behavior | Implementation |
|---|---|---|
| Arity | Exactly one argument, non-empty. Any other count → `ERROR 3000 AUTH requires exactly one non-empty token` | `src/query/query_parser.cpp` |
| Comparison | Constant-time over the maximum of both lengths | |
| Success | `OK AUTHENTICATED`; sets per-connection `admin_authenticated` | |
| Failure (wrong token, or no token configured) | `ERROR 7 Authentication failed`; clears `admin_authenticated` | |
| Statistics | Counted whether it succeeds or fails | |
| Logging | The whole request is logged as `AUTH <redacted>` | |
| Scope | Per TCP connection; `conn_ctx_` is owned by `ReactorConnection` and lives for the connection's lifetime | `src/server/reactor_connection.h` |

The connection is **not** closed on a failed AUTH; unlimited retries are possible subject to
rate limiting.

### 4.2 Gated commands

When `api.admin_token` is non-empty, the following commands require a prior successful `AUTH`
on the same connection; otherwise they return
`ERROR 7 Administrative command requires AUTH`
(`src/server/request_dispatcher.cpp`):

`DUMP SAVE`, `DUMP LOAD`, `DUMP VERIFY`, `DUMP INFO`, `DUMP STATUS`,
`REPLICATION STATUS`, `REPLICATION STOP`, `REPLICATION START`,
`SYNC`, `SYNC STATUS`, `SYNC STOP`,
`CONFIG HELP`, `CONFIG SHOW`, `CONFIG VERIFY`,
`OPTIMIZE`, `DEBUG ON`, `DEBUG OFF`,
`CACHE CLEAR`, `CACHE STATS`, `CACHE ENABLE`, `CACHE DISABLE`,
`SET`, `SHOW VARIABLES`.

**Permitted pre-auth:** `AUTH`, `SEARCH`, `COUNT`, `GET`, `FACET`, `INFO`. When
`api.admin_token` is empty, the gate is disabled entirely and every command is permitted
(`src/server/request_dispatcher.cpp`).

---

## 5. Rate limiting

| Property | Behavior | Implementation |
|---|---|---|
| Algorithm | Token bucket, one token per request | `src/server/rate_limiter.h` |
| Unit counted | One dispatched request frame — including malformed frames and failed AUTH, because the check runs before parsing | `src/server/request_dispatcher.cpp` |
| Granularity | Per peer IP address | `src/server/request_dispatcher.cpp`, `src/server/rate_limiter.h` |
| Unknown/unavailable peer address | Falls back to the shared literal key `unknown`; never fails open | `src/server/request_dispatcher.cpp` |
| Shared with HTTP | **Yes.** `TcpServer` owns the `RateLimiter` as a `shared_ptr` and `HttpServer` co-owns the same instance, so one client IP draws from a single bucket across both surfaces | `src/server/tcp_server.cpp`, `src/app/server_orchestrator.cpp`, `src/server/http_server.cpp` |
| Denial response | `ERROR 6030 Rate limit exceeded` (connection stays open) | `src/server/request_dispatcher.cpp` |
| Denial metric | `mygramdb_requests_denied_total{reason="rate_limit",surface="tcp"}` | `src/server/request_dispatcher.cpp`, `src/server/response_formatter.cpp` |
| Defaults | disabled; capacity 100, refill 10/s, max 10000 tracked clients | `src/config/config.h` |
| Tracking-table full | `max_clients` bounds memory, not admission: the least-recently-seen bucket is evicted and the new client is served, so no request is denied for a reason other than an exhausted bucket | `src/server/rate_limiter.cpp` |

Denial log lines are themselves rate-limited under the key `tcp:<ip>`; the HTTP surface uses
`http:<ip>`, so the two surfaces have separate log suppression but share the token bucket
(`src/server/request_dispatcher.cpp`, `src/server/http_server.cpp`).

---

## 6. Command table

Every command below is dispatched from `QueryParser::Parse`
(`src/query/query_parser.cpp`) and routed by `RequestDispatcher::Dispatch`
(`src/server/request_dispatcher.cpp`) to a handler registered in
`src/server/server_lifecycle_manager.cpp`.

| Command | Min/max args | Mutates state | Auth-gated | Rate-limited | Handler |
|---|---|---|---|---|---|
| `AUTH <secret>` | 1 / 1 | connection auth flag | n/a | yes | dispatcher inline (`src/server/request_dispatcher.cpp`) |
| `SEARCH <table> <text> [clauses]` | 2 / unbounded | no | no | yes | `src/server/handlers/search_handler.cpp` |
| `COUNT <table> <text> [clauses]` | 2 / unbounded | no | no | yes | `src/server/handlers/search_handler.cpp` |
| `GET <table> <primary_key>` | 2 / 2 | no | no | yes | `src/server/handlers/document_handler.cpp` |
| `FACET <table> <column> [text] [clauses]` | 2 / unbounded | no | no | yes | `src/server/handlers/facet_handler.cpp` |
| `INFO` | 0 / 0 (extras ignored) | yes — refreshes peak-memory statistics | no | yes | `src/server/handlers/admin_handler.cpp` |
| `DUMP SAVE [filepath]` | 0 / 1 | yes — writes a snapshot | yes | yes | `src/server/handlers/dump_handler.cpp` |
| `DUMP LOAD <filepath>` | 1 / 1 (extras ignored) | yes — replaces all index state | yes | yes | `src/server/handlers/dump_handler.cpp` |
| `DUMP VERIFY <filepath>` | 1 / 1 (extras ignored) | no | yes | yes | `src/server/handlers/dump_handler.cpp` |
| `DUMP INFO <filepath>` | 1 / 1 (extras ignored) | no | yes | yes | `src/server/handlers/dump_handler.cpp` |
| `DUMP STATUS` | 0 / 0 (extras ignored) | no | yes | yes | `src/server/handlers/dump_handler.cpp` |
| `CONFIG HELP [path]` | 0 / 1 (extras ignored) | no | yes | yes | `src/server/handlers/admin_handler.cpp` |
| `CONFIG SHOW [path]` | 0 / 1 (extras ignored) | no | yes | yes | `src/server/handlers/admin_handler.cpp` |
| `CONFIG VERIFY <filepath>` | 1 / 1 (extras ignored) | no | yes | yes | `src/server/handlers/admin_handler.cpp` |
| `REPLICATION STATUS` | 0 / 0 (extras ignored) | no | yes | yes | `src/server/handlers/replication_handler.cpp` |
| `REPLICATION STOP` | 0 / 0 (extras ignored) | yes — stops binlog reader | yes | yes | `src/server/handlers/replication_handler.cpp` |
| `REPLICATION START` | 0 / 0 (extras ignored) | yes — starts binlog reader | yes | yes | `src/server/handlers/replication_handler.cpp` |
| `SYNC <table>` | 1 / 1 (extras ignored) | yes — rebuilds a table | yes | yes | `src/server/handlers/sync_handler.cpp` |
| `SYNC STATUS` | 0 / 0 (extras ignored) | no | yes | yes | `src/server/handlers/sync_handler.cpp` |
| `SYNC STOP [table]` | 0 / 1 (extras ignored) | yes — requests cancellation | yes | yes | `src/server/handlers/sync_handler.cpp` |
| `OPTIMIZE [table]` | 0 / 1 (extras ignored) | yes — rebuilds posting lists | yes | yes | `src/server/handlers/debug_handler.cpp` |
| `DEBUG ON` / `DEBUG OFF` | 1 / 1 (extras ignored) | yes — connection debug flag | yes | yes | `src/server/handlers/debug_handler.cpp` |
| `CACHE CLEAR [table]` | 0 / 1 (extras ignored) | yes — evicts cache entries | yes | yes | `src/server/handlers/cache_handler.cpp` |
| `CACHE STATS` | 0 / 0 (extras ignored) | no | yes | yes | `src/server/handlers/cache_handler.cpp` |
| `CACHE ENABLE` / `CACHE DISABLE` | 0 / 0 (extras ignored) | yes — cache enablement | yes | yes | `src/server/handlers/cache_handler.cpp` |
| `SET <var> = <value> [, ...]` | 1 / unbounded | yes — runtime configuration | yes | yes | `src/server/handlers/variable_handler.cpp` |
| `SHOW VARIABLES [LIKE '<pattern>']` | 1 / 3 | no | yes | yes | `src/server/handlers/variable_handler.cpp` |
| `SAVE ...` | — | retired | — | yes | rejected at parse (`src/query/query_parser.cpp`) |
| `LOAD ...` | — | retired | — | yes | rejected at parse (`src/query/query_parser.cpp`) |

Any unrecognized first token yields `ERROR 3000 Unknown command: <token>`
(`src/query/query_parser.cpp`).

---

## 7. Global parser limits

| Limit | Value | Applies to | Implementation |
|---|---|---|---|
| `QueryParser::kMaxTermCount` | 64 | AND terms, NOT terms, FILTER conditions — each counted independently. Exceeding → `ERROR 3000 Too many {AND terms\|NOT terms\|FILTER conditions} (max 64)` | `src/query/query_parser.h`, enforced at `src/query/query_parser_commands.cpp` |
| `QueryParser::kMaxFilterColumnNameLength` | 128 | FILTER, SORT and FACET column names | `src/query/query_parser.h`, `src/query/query_parser.cpp` |
| `QueryParser::kMaxFilterValueLength` | 1024 | FILTER value. Exceeding → `ERROR 3006` | `src/query/query_parser.h`, `src/query/query_parser_clauses.cpp` |
| `QueryParser::kMaxHighlightTagLength` | 256 bytes | each `HIGHLIGHT TAG` argument | `src/query/query_parser.h`, `src/query/query_parser_clauses.cpp` |
| `kMaxLimit` | 1000 | `LIMIT` on SEARCH and FACET. Exceeding → `ERROR 3000 LIMIT exceeds maximum of 1000` | `src/query/query_parser_internal.h`, `src/config/config.h`, `src/query/query_parser_commands.cpp` |
| `api.default_limit` | 100 (valid 5–1000) | `LIMIT` substituted for SEARCH and FACET when not explicit | `src/config/config.h`, `src/server/request_dispatcher.cpp` |
| `api.max_query_length` | 128 characters | sum of search text + AND terms + NOT terms + filter columns/values + sort column + highlight tags. `0` disables. Exceeding → `ERROR 3005` | `src/config/config.h`, `src/query/query_parser.cpp` |
| `QueryASTParser::kMaxRecursionDepth` | 32 | nesting depth of a boolean search expression | `src/query/query_ast.h` |
| `QueryASTParser::kMaxTermCount` | 64 | TERM nodes inside a boolean search expression | `src/query/query_ast.h` |

Query-length validation runs for `SEARCH`, `COUNT` and `FACET` only
(`src/query/query_parser_commands.cpp`).

---

## 8. Search-family commands

### 8.1 `SEARCH`

```
SEARCH <table> <search_text> [AND <term>]... [NOT <term>]...
       [FILTER <col> <op> <value>]... [SORT [BY] <col>|ASC|DESC [ASC|DESC]]
       [LIMIT <n>|<offset>,<count>] [OFFSET <n>]
       [HIGHLIGHT [TAG <open> <close>] [SNIPPET_LEN <n>] [MAX_FRAGMENTS <n>]]
       [FUZZY [1|2]]
```

- Minimum 3 tokens (`SEARCH` + table + text). Fewer →
  `ERROR 3000 SEARCH requires at least table and search text`
  (`src/query/query_parser_commands.cpp`).
- **Clause order is not enforced.** After search text, the parser loops over remaining tokens
  and dispatches on whichever clause keyword appears
  (`src/query/query_parser_commands.cpp`). An unrecognized token in that position →
  `ERROR 3000 Unknown keyword: <token>` (`src/query/query_parser_commands.cpp`).
- `AND` / `NOT` / `FILTER` accumulate. `SORT`, `HIGHLIGHT` and `FUZZY` may repeat; the last
  occurrence wins. `LIMIT` and `OFFSET` may appear **once only**.
- `ORDER` anywhere in the command →
  `ERROR 3000 ORDER BY is not supported. Use SORT instead. Example: SEARCH table text SORT column DESC`
  (`src/query/query_parser_commands.cpp`; the search-text scanner produces the shorter
  variant `ORDER BY is not supported. Use SORT instead.`).
- A comma in the table position or a bare `,` token →
  `ERROR 3000 Multiple tables not supported. Hint: MygramDB searches a single table at a time. Use separate queries for multiple tables.`
  (`src/query/query_parser_commands.cpp`).

**Search-text consumption.** Tokens are consumed until a clause keyword is reached at
parenthesis depth 0. Which keywords stop consumption depends on whether the expression is a
boolean expression (`src/query/query_parser_commands.cpp`):

- *Legacy mode* (default): stops at `AND`, `OR`, `NOT`, `FILTER`, `SORT`, `LIMIT`, `OFFSET`,
  `HIGHLIGHT`, `FUZZY`, `FACET`.
- *Boolean-expression mode*, entered when the expression contains a top-level `OR`, a
  parenthesized operand after a top-level boolean operator, or a leading unary `NOT`
  (`src/query/query_parser_commands.cpp`): stops only at `FILTER`, `SORT`, `LIMIT`,
  `OFFSET`, `HIGHLIGHT`, `FUZZY`, `FACET`, so `AND`/`OR`/`NOT` remain part of the expression
  and are parsed by `QueryASTParser` (`src/server/search_pipeline.cpp`).

An all-empty search text (e.g. `SEARCH t ""`) →
`ERROR 3000 SEARCH requires non-empty search text` (`src/query/query_parser_commands.cpp`).
A malformed boolean expression →
`ERROR 3010 Invalid boolean search expression: <detail>` (`src/server/search_pipeline.cpp`).
Invalid UTF-8 in any search term → `ERROR 3001 Invalid UTF-8 in query text`
(`src/server/search_pipeline.cpp`).

**Success response — without HIGHLIGHT** (`src/server/response_formatter.cpp`):

```
OK RESULTS <total_matched> <pk1> <pk2> ... <pkN>\r\n
```

`<total_matched>` is the match count before pagination. Primary keys are space-separated on
the header line and escaped per §11.1. Documents present in the index but missing from the
document store are silently omitted from the list while `<total_matched>` still counts them
(`src/server/response_formatter.cpp`).

**Success response — with HIGHLIGHT** (`src/server/response_formatter.cpp`):

```
OK RESULTS <total_matched>\r\n<pk>\t<snippet>\r\n<pk>\t<snippet>\r\n...\r\n
```

Snippets are sanitized per §11.2. The body already ends with `\r\n`, so the transport
terminator makes the frame end `\r\n\r\n`.

**Debug block.** When the connection is in debug mode (`DEBUG ON`), a block is appended before
the terminator (`src/server/response_formatter.cpp`):

```
\r\n\r\n# DEBUG\r\n
query_time: <ms>ms\r\n
index_time: <ms>ms\r\n
[filter_time: <ms>ms\r\n]
terms: <n>\r\n
ngrams: <n>\r\n
[candidates: <n>\r\n after_intersection: <n>\r\n after_not: <n>\r\n after_filters: <n>\r\n]
final: <n>\r\n
[optimization: <label>\r\n]
[sort: <col> ASC|DESC[ (default)]\r\n]
limit: <n>[ (default)]\r\n
[offset: <n>[ (default)]\r\n]
[highlight: on\r\n]
cache: hit|miss|disabled\r\n
[cache_age_ms: <v>\r\n cache_saved_ms: <v>\r\n]
[cache_reason: not_found|invalidated\r\n cache_cost_ms: <v>\r\n]
[cache_key: <v>\r\n]
```

The bracketed pipeline-stage counts and extended cache fields are emitted for the
non-highlight form only; the highlight form emits `highlight: on` and the headline cache line
only (`src/server/response_formatter.cpp`). Stage counts are also suppressed on a
cache hit (`src/server/response_formatter.cpp`, `src/server/handlers/search_handler.cpp`).

**Handler-level errors:**

| Condition | Response | Implementation |
|---|---|---|
| Bare table name under a multi-database configuration | `ERROR 4007 Bare table names are not supported; use <database>.<table>: <name>` | `src/server/handlers/command_handler.cpp` |
| Unknown table | `ERROR 4007 Table not found: <name>` | `src/server/handlers/command_handler.cpp` |
| `DUMP LOAD` in progress | `ERROR 6028 Server is loading, please try again later` | `src/server/handlers/command_handler.cpp` |
| Table is being rebuilt by `SYNC` | `ERROR 6029 Table '<name>' is synchronizing, please try again later` | `src/server/handlers/command_handler.cpp` |
| `HIGHLIGHT` without stored normalized text | `ERROR 4 HIGHLIGHT requires normalized text storage. Set memory.verify_text to "ascii" or "all" in configuration.` | `src/server/handlers/search_handler.cpp` |
| `SORT _score` with BM25 disabled | `ERROR 3007 SORT _score requires BM25 to be enabled in configuration` | `src/server/search_pipeline.cpp` |
| `SORT _score` without stored normalized text | `ERROR 4 SORT _score requires normalized text storage. ...` | `src/server/search_pipeline.cpp` |
| Index or document store unavailable | `ERROR 5 Index not available` / `ERROR 5 Document store not available` | `src/server/handlers/search_handler.cpp` |
| A search term too short to produce n-grams, with normalized text not stored | `ERROR 4000 Query term is too short for n-gram search and requires normalized text storage. Set memory.verify_text to "ascii" or "all" in configuration.` | `src/server/search_pipeline.cpp` |

### 8.2 `COUNT`

```
COUNT <table> <search_text> [AND <term>]... [NOT <term>]... [FILTER <col> <op> <value>]...
```

- Minimum 3 tokens; fewer → `ERROR 3000 COUNT requires at least table and search text`
  (`src/query/query_parser_commands.cpp`).
- Only `AND`, `NOT` and `FILTER` are accepted. Anything else →
  `ERROR 3000 COUNT only supports AND, NOT and FILTER clauses`
  (`src/query/query_parser_commands.cpp`). `SORT` gets a dedicated message
  (`ERROR 3000 COUNT does not support SORT clause. Use SEARCH if you need sorted results.`,
  `src/query/query_parser_commands.cpp`) and `ORDER` gets
  `ERROR 3000 ORDER BY is not supported. Use SORT instead (note: COUNT does not support sorting).`
- Search-text extraction, boolean-expression detection and the term/filter caps are identical
  to `SEARCH`.

**Success response** (`src/server/response_formatter.cpp`):

```
OK COUNT <n>\r\n
```

With debug mode enabled, the debug block is a reduced form carrying `query_time`,
`index_time`, `terms`, `ngrams` and the extended cache section — it never carries the
pipeline-stage counts (`src/server/response_formatter.cpp`).

### 8.3 `GET`

```
GET <table> <primary_key>
```

Exactly 3 tokens. Any other count → `ERROR 3000 GET requires table and primary_key`
(`src/query/query_parser_commands.cpp`). No clauses are accepted; extra tokens are an
arity error, not ignored.

**Success response** (`src/server/response_formatter.cpp`):

```
OK DOC <primary_key>[ <col>=<value>]...\r\n
```

Filter column values are rendered per type: `NULL` for absent, `true`/`false` for booleans,
strings escaped per §11.1, doubles at 6 decimal places, `TimeValue` as integer seconds, 8-bit
integers widened to `int` (`src/server/response_formatter.cpp`).

Document absent → `ERROR 4004 Document not found`
(`src/server/handlers/document_handler.cpp`, `src/server/response_formatter.cpp`).
The loading and syncing pre-flight checks of §8.1 apply
(`src/server/handlers/document_handler.cpp`).

### 8.4 `FACET`

```
FACET <table> <column> [search_text] [AND <term>]... [NOT <term>]...
      [FILTER <col> <op> <value>]... [LIMIT <n>|<offset>,<count>] [OFFSET <n>]
```

- Table and column are both mandatory: `ERROR 3000 FACET requires table name` /
  `ERROR 3000 FACET requires column name` (`src/query/query_parser_commands.cpp`).
- The column must satisfy `IsSafeColumnName`; otherwise `ERROR 3000 Invalid facet column`
  (`src/query/query_parser_commands.cpp`).
- **Search text is optional** (`require_search_text = false`,
  `src/query/query_parser_commands.cpp`).
- Accepted clauses are `AND`, `NOT`, `FILTER`, `LIMIT`, `OFFSET` only. Anything else →
  `ERROR 3000 FACET: Unknown clause: <token>` (`src/query/query_parser_commands.cpp`).
  `SORT`, `HIGHLIGHT` and `FUZZY` are therefore rejected on FACET.
- Term caps and the 1000 `LIMIT` cap apply as for `SEARCH`
  (`src/query/query_parser_commands.cpp`).

**Success response** (`src/server/response_formatter.cpp`):

```
OK FACET <returned_value_count> <total_distinct_values>\r\n
<value>\t<count>\r\n
<value>\t<count>\r\n
...
[# query_time_ms: <v>\r\n# matched_documents: <n>\r\n# distinct_values: <n>\r\n]
\r\n
```

Values are sanitized per §11.2. The `#`-prefixed lines appear only in debug mode
(`src/server/handlers/facet_handler.cpp`). The body already ends with `\r\n`, so the
frame ends `\r\n\r\n`.

**Handler-level errors.** The loading, syncing and table-resolution checks of §8.1 apply
(`src/server/handlers/facet_handler.cpp`). Beyond those, every citation in the table below is
`src/server/search_pipeline.cpp`:

| Condition | Response |
|---|---|
| Column is not a filterable column of the resolved table | `ERROR 4000 Facet column "<name>" not found` |
| Document store has no filter index | `ERROR 4000 Filter index not available` |
| Index or document store unavailable | `ERROR 4000 Index not available` / `ERROR 4000 Document store not available` |
| `DUMP LOAD` in progress, re-checked immediately before the scan | `ERROR 6028 Server is loading, please try again later` |

---

## 9. Clause grammar

### 9.1 `AND <term>` / `NOT <term>`

- Each consumes exactly one following token. Missing → `ERROR 3000 AND requires a term` /
  `ERROR 3000 NOT requires a term` (`src/query/query_parser_clauses.cpp`).
- `AND NOT <term>` is folded into a single NOT term rather than indexing the literal keyword
  (`src/query/query_parser_clauses.cpp`).

### 9.2 `FILTER`

Two accepted forms (`src/query/query_parser_clauses.cpp`):

```
FILTER <column> <op> <value>          -- three separate tokens
FILTER <column><op><value>            -- one compound token, e.g. FILTER status=active
FILTER <column><op> <value>           -- compound column+operator, value as next token
```

Operators, longest-match first when scanning a compound token
(`src/query/query_parser_clauses.cpp`):

| Symbolic | Alternate | Named | Enum |
|---|---|---|---|
| `=` | `==` | `EQ` | `FilterOp::EQ` |
| `!=` | `<>` | `NE` | `FilterOp::NE` |
| `>` | — | `GT` | `FilterOp::GT` |
| `>=` | `≥` (U+2265) | `GTE` | `FilterOp::GTE` |
| `<` | — | `LT` | `FilterOp::LT` |
| `<=` | `≤` (U+2264) | `LTE` | `FilterOp::LTE` |

(`src/query/query_parser_clauses.cpp`)

Every citation in the table below is `src/query/query_parser_clauses.cpp`.

| Condition | Response |
|---|---|
| Fewer than three tokens available and no compound form matched | `ERROR 3006 FILTER requires column, operator, and value` |
| Operator token unrecognized | `ERROR 3006 Invalid filter operator: <token>` |
| Value begins with `=`, `<`, `>` or `!` | `ERROR 3006 FILTER value must not start with an operator character` |
| Column fails `IsSafeColumnName` | `ERROR 3006 Invalid filter column` |
| Value longer than 1024 bytes | `ERROR 3006 FILTER value exceeds maximum length (1024)` |

`IsSafeColumnName` accepts 1–128 characters from `[A-Za-z0-9_.$-]` only; any other byte,
including all non-ASCII, is rejected (`src/query/query_parser.cpp`).

### 9.3 `LIMIT`

```
LIMIT <count>
LIMIT <offset>,<count>
```

Every citation in the table below is `src/query/query_parser_clauses.cpp` unless the cell
names another file.

| Condition | Response |
|---|---|
| No argument | `ERROR 3008 LIMIT requires a number or offset,count` |
| Second `LIMIT` | `ERROR 3008 LIMIT specified more than once` |
| `LIMIT o,c` when `OFFSET` already given | `ERROR 3009 OFFSET specified more than once (LIMIT offset,count + OFFSET)` |
| Leading `-` on the offset | `ERROR 3008 LIMIT offset must be non-negative` |
| Leading `-` on the count | `ERROR 3008 LIMIT count must be positive` |
| Unparseable `o,c` | `ERROR 3008 Invalid LIMIT offset,count format: <token>` |
| `count` is 0 | `ERROR 3008 LIMIT count must be positive` |
| Leading `-` on a bare limit | `ERROR 3008 LIMIT must be positive` |
| Unparseable bare limit | `ERROR 3008 Invalid LIMIT value: <token>` |
| Bare limit is 0 | `ERROR 3008 LIMIT must be positive` |
| Value above 1000 | `ERROR 3000 LIMIT exceeds maximum of 1000` (post-clause check, `src/query/query_parser_commands.cpp`) |

Both offset and count are parsed as `uint32_t`.

### 9.4 `OFFSET <n>`

Every citation in the table below is `src/query/query_parser_clauses.cpp`.

| Condition | Response |
|---|---|
| No argument | `ERROR 3009 OFFSET requires a number` |
| Second `OFFSET` (including one already set by `LIMIT o,c`) | `ERROR 3009 OFFSET specified more than once` |
| Leading `-` | `ERROR 3009 OFFSET must be non-negative` |
| Unparseable | `ERROR 3009 Invalid OFFSET value: <token>` |

There is no upper bound on `OFFSET` beyond `uint32_t` range.

### 9.5 `SORT`

```
SORT ASC|DESC                 -- primary-key ordering shorthand
SORT [BY] <column> [ASC|DESC] -- default DESC
```

- The optional `BY` is consumed if present; `SORT BY` with nothing after →
  `ERROR 3007 SORT BY requires a column name` (`src/query/query_parser_clauses.cpp`).
- No argument at all → `ERROR 3007 SORT requires a column name or ASC/DESC`
  (`src/query/query_parser_clauses.cpp`).
- Default direction is `DESC` (`src/query/query_parser.h`).
- `_score` selects BM25 relevance ordering (`src/query/query_parser.h`).
- A comma in the column name → `ERROR 3007 Multiple column sorting is not supported. Sort by a
  single column only.` (`src/query/query_parser_clauses.cpp`).
- Column failing `IsSafeColumnName` → `ERROR 3007 Invalid sort column`
  (`src/query/query_parser_clauses.cpp`).
- A non-clause-keyword token immediately after the sort spec → `ERROR 3007 Multiple column
  sorting is not supported. Hint: Sort by a single column only. Use application-level sorting
  for complex requirements.` (`src/query/query_parser_clauses.cpp`).
- Because `SORT` itself is a clause keyword, `SORT a ASC SORT b DESC` parses and the **last**
  clause wins (`src/query/query_parser_clauses.cpp`).

### 9.6 `HIGHLIGHT`

```
HIGHLIGHT [TAG <open> <close>] [SNIPPET_LEN <n>] [MAX_FRAGMENTS <n>]
```

Sub-options may appear in any order and any subset; parsing stops at the first token that is
not a recognized sub-option, leaving it for the outer clause loop
(`src/query/query_parser_clauses.cpp`).

| Option | Default | Bounds | Error |
|---|---|---|---|
| `TAG <open> <close>` | `<em>` / `</em>` | ≤ 256 bytes each | missing args → `ERROR 3000 HIGHLIGHT TAG requires open and close tag arguments`; oversize → `ERROR 3000 HIGHLIGHT TAG {open\|close} tag must be at most 256 bytes` (`src/query/query_parser_clauses.cpp`) |
| `SNIPPET_LEN <n>` | 100 code points | 1–10000 | missing → `ERROR 3000 HIGHLIGHT SNIPPET_LEN requires a number`; unparseable → `ERROR 3000 Invalid HIGHLIGHT SNIPPET_LEN value`; out of range → `ERROR 3000 HIGHLIGHT SNIPPET_LEN must be between 1 and 10000` (`src/query/query_parser_clauses.cpp`) |
| `MAX_FRAGMENTS <n>` | 3 | 1–100 | missing → `ERROR 3000 HIGHLIGHT MAX_FRAGMENTS requires a number`; unparseable → `ERROR 3000 Invalid HIGHLIGHT MAX_FRAGMENTS value`; out of range → `ERROR 3000 HIGHLIGHT MAX_FRAGMENTS must be between 1 and 100` (`src/query/query_parser_clauses.cpp`) |

Defaults are declared at `src/query/query_parser.h`.

### 9.7 `FUZZY`

```
FUZZY [1|2]
```

- The distance argument is consumed only if the next token is not a clause keyword
  (`src/query/query_parser_clauses.cpp`). `FUZZY` immediately followed by `LIMIT` therefore
  takes the default distance of 1 (`src/query/query_parser_clauses.cpp`).
- A non-clause-keyword argument that is unparseable or outside `1..2` →
  `ERROR 3000 FUZZY distance must be 1 or 2, got: <token>`
  (`src/query/query_parser_clauses.cpp`).

---

## 10. Administrative commands

### 10.1 `INFO`

Takes no arguments; any extra tokens are ignored (`src/query/query_parser.cpp`).

```
OK INFO\r\n
\r\n
# Server\r\n
version: <v>\r\n
uptime_seconds: <n>\r\n
data_initialized: true|false\r\n
readiness: ready|not_ready\r\n
\r\n
# Stats\r\n ...
# Commandstats\r\n ...
# Memory\r\n ...
# Index\r\n ...
# Tables\r\ntables: <comma-separated names>\r\n
\r\n
# Clients\r\nconnected_clients: <n>\r\n
\r\n
[# Replication\r\n ...]
# Cache\r\n ...
\r\n
END
```

(`src/server/response_formatter.cpp`). The `# Replication` section is present only in
`USE_MYSQL` builds (`src/server/response_formatter.cpp`). Command counters are emitted
only when non-zero (`src/server/response_formatter.cpp`). `INFO` refreshes the
peak-memory statistic as a side effect (`src/server/handlers/admin_handler.cpp`).
If the table catalog is uninitialized → `ERROR 4008 Table catalog not initialized`
(`src/server/handlers/admin_handler.cpp`).

### 10.2 `DUMP`

`DUMP` without a subcommand → `ERROR 3000 DUMP requires a subcommand (SAVE, LOAD, VERIFY,
INFO, STATUS)` (`src/query/query_parser.cpp`). Unknown subcommand →
`ERROR 3000 Unknown DUMP subcommand: <token>` (`src/query/query_parser.cpp`).

Argument handling for every subcommand below is implemented in `src/query/query_parser.cpp`.

| Subcommand | Argument handling | Success response |
|---|---|---|
| `DUMP SAVE [filepath]` | All trailing tokens are scanned; the last non-empty token not starting with `-` becomes the filepath. A token starting with `-` → `ERROR 3000 Unknown DUMP SAVE flag: <token>`. Empty tokens are skipped | Async (progress tracking available): `OK DUMP_STARTED <filepath>` (`src/server/handlers/dump_handler.cpp`). Synchronous fallback: `OK SAVED <filepath>` |
| `DUMP LOAD <filepath>` | Uses `tokens[2]`; extras ignored. Missing → `ERROR 3000 DUMP LOAD requires a filepath` | `OK LOADED <filepath>` (`src/server/handlers/dump_handler.cpp`, `src/server/response_formatter.cpp`) |
| `DUMP VERIFY <filepath>` | Uses `tokens[2]`; extras ignored. Missing → `ERROR 3000 DUMP VERIFY requires a filepath` | `OK DUMP_VERIFIED <filepath>` (`src/server/handlers/dump_handler.cpp`) |
| `DUMP INFO <filepath>` | Uses `tokens[2]`; extras ignored. Missing → `ERROR 3000 DUMP INFO requires a filepath` | multi-line, see below |
| `DUMP STATUS` | No arguments | multi-line, see below |

`DUMP INFO` response (`src/server/handlers/dump_handler.cpp`):

```
OK DUMP_INFO <filepath>\r\n
version: <n>\r\n
gtid: <v>\r\n
tables: <n>\r\n
flags: <n>\r\n
file_size: <n>\r\n
timestamp: <n>\r\n
has_statistics: true|false\r\n
END
```

`DUMP STATUS` response (`src/server/handlers/dump_handler.cpp`):

```
OK DUMP_STATUS\r\n
save_in_progress: true|false\r\n
load_in_progress: true|false\r\n
replication_paused_for_dump: true|false\r\n
status: IDLE|SAVING|LOADING|COMPLETED|FAILED\r\n
[filepath: <v>\r\n tables_processed: <n>\r\n tables_total: <n>\r\n]
[current_table: <v>\r\n]
[elapsed_seconds: <v>\r\n]
[error: <v>\r\n]
[result_filepath: <v>\r\n]
END
```

Without progress tracking the `status` value is one of `SAVE_IN_PROGRESS`, `LOAD_IN_PROGRESS`,
`IDLE` and no detail lines are emitted (`src/server/handlers/dump_handler.cpp`).

**Path resolution.** `DUMP SAVE/LOAD/VERIFY/INFO` filepaths are resolved against the dump
directory with traversal rejection; failures surface the resolver's own error code
(`src/server/handlers/dump_handler.cpp`).

**Mutual-exclusion errors.** The code depends on which operation blocks the request. A
conflict with `DUMP SAVE`, `DUMP LOAD`, `OPTIMIZE` or the long-operation coordinator answers
`ERROR 6030` from the per-command sites cited in the table below. A conflict with an in-flight
`SYNC` answers `ERROR 4011`, because every such check is delegated to
`SyncOperationManager::CheckNoSyncInProgress`, which is the sole emitter of that code on these
paths (`src/server/sync_operation_manager.cpp`).

| Command | Blocked by | Code | Implementation |
|---|---|---|---|
| `DUMP SAVE` | `DUMP LOAD`, another `DUMP SAVE`, `OPTIMIZE`, any other long operation | 6030 | `src/server/handlers/dump_handler.cpp` |
| `DUMP SAVE` | any in-flight `SYNC` | 4011 | `src/server/handlers/dump_handler.cpp` |
| `DUMP LOAD` | `OPTIMIZE`, `DUMP SAVE`, another `DUMP LOAD` | 6030 | `src/server/handlers/dump_handler.cpp` |
| `DUMP LOAD` | any in-flight `SYNC` | 4011 | `src/server/handlers/dump_handler.cpp` |
| `OPTIMIZE` | `DUMP LOAD`, another `OPTIMIZE`, any other long operation, critical memory, insufficient memory | 6030 | `src/server/handlers/debug_handler.cpp` |
| `OPTIMIZE` | any in-flight `SYNC` | 4011 | `src/server/handlers/debug_handler.cpp` |
| `SYNC` | `OPTIMIZE`, `DUMP SAVE`, `DUMP LOAD` | 6030 | `src/server/handlers/sync_handler.cpp` |
| `SYNC` | another long operation, or a `SYNC` already claiming the table | 4011 | `src/server/sync_operation_manager.cpp` |
| `REPLICATION START` | MySQL reconnecting, replication paused for dump, `DUMP LOAD`, `DUMP SAVE` | 6030 | `src/server/handlers/replication_handler.cpp` |
| `REPLICATION START` | any in-flight `SYNC` | 4011 | `src/server/handlers/replication_handler.cpp` |
| `REPLICATION STOP` | replication paused for dump | 6030 | `src/server/handlers/replication_handler.cpp` |

`DUMP SAVE` additionally requires a non-empty GTID position →
`ERROR 6029 Cannot save dump without GTID position. Please run SYNC command first to establish
initial position.` (`src/server/handlers/dump_handler.cpp`).

### 10.3 `CONFIG`

`CONFIG` with no subcommand defaults to `CONFIG SHOW`
(`src/query/query_parser.cpp`). Unknown subcommand →
`ERROR 3000 Unknown CONFIG subcommand: <token> (expected HELP, SHOW, or VERIFY)`
(`src/query/query_parser.cpp`).

| Subcommand | Behavior | Success response |
|---|---|---|
| `CONFIG HELP [path]` | Optional path from `tokens[2]`; extras ignored (`src/query/query_parser.cpp`). Unknown path → `ERROR 8 Configuration path not found: <path>` (`src/server/handlers/admin_handler.cpp`) | `+OK\r\n<help body ending in \r\n>` (`src/server/handlers/admin_handler.cpp`) |
| `CONFIG SHOW [path]` | Optional path from `tokens[2]`; extras ignored (`src/query/query_parser.cpp`). Unknown path → `ERROR 1004 CONFIG SHOW failed: Path not found: <path>` (`src/server/handlers/admin_handler.cpp`, `src/config/config_help.cpp`). Server configuration unavailable → `ERROR 6026 Server configuration is not available` | `+OK\r\n<config body ending in \r\n>` (`src/server/handlers/admin_handler.cpp`) |
| `CONFIG VERIFY <filepath>` | Mandatory; missing → `ERROR 3000 CONFIG VERIFY requires a filepath` (`src/query/query_parser.cpp`) | `+OK\r\nConfiguration is valid\r\n  Tables: <n> (<names>)\r\n  MySQL: <host>:<port>\r\n` (`src/server/handlers/admin_handler.cpp`) |

`CONFIG VERIFY` path restrictions (`src/server/handlers/admin_handler.cpp`):

| Condition | Response |
|---|---|
| Absolute path | `ERROR 2 CONFIG VERIFY: absolute paths not allowed` |
| Contains `..` | `ERROR 2 CONFIG VERIFY: path traversal (..) not allowed` |
| Extension not `.yaml`/`.yml` | `ERROR 2 CONFIG VERIFY only accepts .yaml or .yml files` |
| Escapes the config directory | `ERROR 2 CONFIG VERIFY: path traversal (..) not allowed` |
| Any symlink in the resolved path, or an `O_NOFOLLOW` open failure | `ERROR 2 CONFIG VERIFY: symbolic links are not allowed` |
| Not found | `ERROR 1000 CONFIG VERIFY: file not found: <path>` |
| Not a regular file | `ERROR 2 CONFIG VERIFY: not a regular file` |
| Config directory unknown | `ERROR 6026 CONFIG VERIFY: active configuration directory is unavailable` |
| Validation failure | `ERROR <config error code> Configuration validation failed:   <message>` (embedded CRLF is flattened to spaces by the error formatter) |

`CONFIG VERIFY` deliberately omits `mysql.user` and `mysql.password` from its summary
(`src/server/handlers/admin_handler.cpp`).

### 10.4 `REPLICATION`

`REPLICATION` with no subcommand → `ERROR 3000 REPLICATION requires a subcommand (STATUS,
STOP, START)` (`src/query/query_parser.cpp`). Unknown subcommand →
`ERROR 3000 Unknown REPLICATION subcommand: <token>` (`src/query/query_parser.cpp`).

`REPLICATION STATUS` response (`src/server/response_formatter.cpp`):

```
OK REPLICATION\r\n
status: <state>\r\n
current_gtid: <v>\r\n
processed_events: <n>\r\n
queue_size: <n>\r\n
crc_errors: <n>\r\n
schema_incompatible: true|false\r\n
last_error_code: <n>\r\n
last_error: <v>\r\n
last_applied_unixtime: <n>\r\n
seconds_since_last_applied: <n>\r\n
END
```

With no binlog reader configured the body is `OK REPLICATION\r\nstatus: not_configured\r\nEND`
(`src/server/response_formatter.cpp`). In non-MySQL builds it is
`ERROR 4 MySQL support not compiled` (`src/server/response_formatter.cpp`).

| Command | Success | Notable errors |
|---|---|---|
| `REPLICATION STOP` | `OK REPLICATION_STOPPED` (`src/server/response_formatter.cpp`) | `ERROR 6029 Replication is not running`; `ERROR 4 Replication is not configured`; `ERROR 6030 Cannot stop replication while DUMP SAVE/LOAD is in progress. ...` (`src/server/handlers/replication_handler.cpp`) |
| `REPLICATION START` | `OK REPLICATION_STARTED` (`src/server/response_formatter.cpp`) | `ERROR 9 Replication is already running`; `ERROR 6029 Cannot start replication without GTID position. ...`; `ERROR 4 Replication is not configured`; start failure → `ERROR <code> Failed to start replication: <message>` (`src/server/handlers/replication_handler.cpp`) |

### 10.5 `SYNC`

```
SYNC <table>
SYNC STATUS
SYNC STOP [table]
```

`SYNC` with no argument → `ERROR 3000 SYNC requires a table name or STATUS/STOP subcommand`
(`src/query/query_parser.cpp`). Because `STATUS` and `STOP` are matched
case-insensitively before falling through to the table-name branch, a table literally named
`status` or `stop` cannot be synced by name (`src/query/query_parser.cpp`).

| Command | Success response |
|---|---|
| `SYNC <table>` | `OK SYNC STARTED table=<resolved>` (`src/server/sync_operation_manager.cpp`) |
| `SYNC STOP` (all) | `OK SYNC CANCELLATION REQUESTED count=<n>` (`src/server/sync_operation_manager.cpp`) |
| `SYNC STOP <table>` | `OK SYNC CANCELLATION REQUESTED table=<resolved>` (`src/server/sync_operation_manager.cpp`) |

`SYNC STATUS` response when at least one table has state
(`src/server/sync_operation_manager.cpp`):

```
OK SYNC_STATUS\r\n
table=<name> status=STARTING|IN_PROGRESS|COMPLETED|FAILED|CANCELLING|CANCELLED[ <details>]\r\n
...
END\r\n
```

Detail suffixes by status: `IN_PROGRESS` adds `progress=<done>/<total> rows (<pct>%)` (or
`progress=<done> rows` when the total is unknown) and `rate=<n> rows/s`; `COMPLETED` adds
`rows=<n> time=<s>s [gtid=<v>] replication=<v>`; `FAILED` adds `rows=<n> error="<msg>"
[replication=<v>]`; `CANCELLED` adds `error="<msg>" [replication=<v>]`
(`src/server/sync_operation_manager.cpp`).

With no state at all:

```
OK SYNC_STATUS\r\nstatus=IDLE message="No sync operation performed"\r\nEND\r\n
```

(`src/server/sync_operation_manager.cpp`)

`SYNC STOP` with nothing running → `ERROR 8 No active SYNC operations to stop` (all) or
`ERROR 8 No active SYNC operation for table: <name>` (specific)
(`src/server/sync_operation_manager.cpp`).

`SYNC <table>` errors, in the order they are evaluated:

| Condition | Response | Implementation |
|---|---|---|
| Table catalog uninitialized | `ERROR 4008 Table catalog not initialized` | `src/server/handlers/command_handler.cpp` |
| Unknown table, or a bare name under a multi-database configuration | `ERROR 4007 Table not found: <name>` / `ERROR 4007 Bare table names are not supported; use <database>.<table>: <name>` | `src/server/handlers/command_handler.cpp` |
| `OPTIMIZE`, `DUMP SAVE` or `DUMP LOAD` in progress | `ERROR 6030 Cannot start SYNC while <operation> is in progress` | `src/server/handlers/sync_handler.cpp` |
| Shutdown already requested | `ERROR 6027 Cannot start SYNC for '<name>': server is shutting down` | `src/server/sync_operation_manager.cpp` |
| Table absent from the sync manager's own table set | `ERROR 4010 Table '<name>' not found` | `src/server/sync_operation_manager.cpp` |
| Another long operation holds the coordinator, or a `SYNC` already claims the table | `ERROR 4011 <detail>` | `src/server/sync_operation_manager.cpp` |
| Memory critically low | `ERROR 4012 Memory critically low. Cannot start SYNC.` | `src/server/sync_operation_manager.cpp` |
| Worker thread could not be created | `ERROR 4013 Failed to create sync thread: <detail>` | `src/server/sync_operation_manager.cpp` |

`SYNC STOP <table>` resolves the table name first (`src/server/handlers/sync_handler.cpp`),
so it can also answer `ERROR 4007` or `ERROR 4008` from the shared resolver
(`src/server/handlers/command_handler.cpp`).

### 10.6 `OPTIMIZE [table]`

Optional table from `tokens[1]`; extras ignored (`src/query/query_parser.cpp`). With no
table, all catalog tables are optimized; an empty catalog →
`ERROR 4007 No tables are available for optimization`
(`src/server/handlers/debug_handler.cpp`).

```
OK OPTIMIZED tables=<n> terms=<n> delta=<n> roaring=<n> memory=<human>\r\n
```

(`src/server/handlers/debug_handler.cpp`)

Failure to optimize a table → `ERROR 5 Failed to optimize table: <name>`
(`src/server/handlers/debug_handler.cpp`). Memory gates are described in §10.2.

### 10.7 `DEBUG ON` / `DEBUG OFF`

Missing mode → `ERROR 3000 DEBUG requires ON or OFF`; a mode other than `ON`/`OFF` →
`ERROR 3000 DEBUG requires ON or OFF, got: <token>` (`src/query/query_parser.cpp`).
Extras are ignored.

Responses: `OK DEBUG_ON` / `OK DEBUG_OFF`
(`src/server/handlers/debug_handler.cpp`).

The debug flag is **per connection**, stored in `ConnectionContext::debug_mode`
(`src/server/server_types.h`), and enables the debug blocks described in §8.1, §8.2 and
§8.4.

### 10.8 `CACHE`

`CACHE` with no subcommand → `ERROR 3000 CACHE requires a subcommand (CLEAR, STATS, ENABLE,
DISABLE)` (`src/query/query_parser.cpp`). Unknown subcommand →
`ERROR 3000 Unknown CACHE subcommand: <token>` (`src/query/query_parser.cpp`).

Every citation in the table below is `src/server/handlers/cache_handler.cpp`.

| Command | Success response | Errors |
|---|---|---|
| `CACHE CLEAR` | `OK CACHE_CLEARED` | `ERROR 8001 Cache not configured`; `ERROR 8001 Cache is disabled` |
| `CACHE CLEAR <table>` | `OK CACHE_CLEARED table=<resolved>` | `ERROR 4008 Table catalog is not available`; `ERROR 4007 Table not found or ambiguous: <name>` |
| `CACHE STATS` | multi-line, see below | `ERROR 8001 Cache not configured` |
| `CACHE ENABLE` | `OK CACHE_ENABLED` | `ERROR 8001 Cache not configured`; `ERROR 8001 Cache cannot be enabled: server was started with cache disabled. Please restart the server with cache.enabled = true in configuration.` |
| `CACHE DISABLE` | `OK CACHE_DISABLED` | `ERROR 8001 Cache not configured` |

`CACHE STATS` response (`src/server/handlers/cache_handler.cpp`):

```
OK CACHE_STATS\r\n
\r\n
# Cache\r\n
enabled: true|false\r\n
total_queries: <n>\r\n
cache_hits: <n>\r\n
cache_misses: <n>\r\n
hit_rate: <0.0000>\r\n
current_entries: <n>\r\n
current_memory_bytes: <n>\r\n
invalidation_index_memory_bytes: <n>\r\n
invalidation_queue_memory_bytes: <n>\r\n
accounted_memory_bytes: <n>\r\n
evictions: <n>\r\n
ttl_expirations: <n>\r\n
rejection_count: <n>\r\n
rejection_oversize: <n>\r\n
rejection_memory_budget: <n>\r\n
rejection_duplicate: <n>\r\n
stale_entry_removals: <n>\r\n
decompression_failures: <n>\r\n
stale_lru_entries: <n>\r\n
invalidations_immediate: <n>\r\n
invalidations_deferred: <n>\r\n
invalidations_batches: <n>\r\n
[avg_cache_hit_time_ms: <v>\r\n]
[avg_cache_miss_time_ms: <v>\r\n]
total_time_saved_ms: <v>\r\n
\r\n
END
```

`CACHE STATS` reports statistics even when the cache is disabled; only `CACHE CLEAR` requires
it to be enabled.

### 10.9 `SET`

```
SET <name> = <value> [, <name> = <value>]...
SET <name>=<value>[,]  ...
```

Both the spaced three-token form and the compact single-token `name=value` form are accepted,
and they may be mixed (`src/query/query_parser.cpp`). A trailing comma attached to a
value, or a standalone `,` token, separates assignments
(`src/query/query_parser.cpp`).

| Condition | Response |
|---|---|
| Compact form with empty name or value | `ERROR 3000 SET: Expected variable = value` (`src/query/query_parser.cpp`) |
| Spaced form with fewer than three remaining tokens | `ERROR 3000 SET: Expected variable = value` (`src/query/query_parser.cpp`) |
| Middle token is not `=` | `ERROR 3000 SET: Expected '=' after variable name` (`src/query/query_parser.cpp`) |
| A token that is neither a separator nor the start of a new assignment | `ERROR 3000 SET: Expected ',' or end of query` (`src/query/query_parser.cpp`) |
| No assignments parsed | `ERROR 3000 SET: No variable assignments found` (`src/query/query_parser.cpp`) |
| No variable by that name | `ERROR 1008 Failed to set variable '<name>': Unknown variable: <name>` (`src/config/runtime_variable_manager.cpp`, reached from `src/server/handlers/variable_handler.cpp`) |
| Variable exists but is startup-only | `ERROR 1009 Failed to set variable '<name>': Variable '<name>' is immutable (requires restart)` (`src/config/runtime_variable_manager.cpp`) |
| Value rejected for a mutable variable | `ERROR 1004 Failed to set variable '<name>': <reason>` (`src/config/runtime_variable_manager.cpp`) |
| `cache.enabled = true` when the cache subsystem cannot start | `ERROR 8001 Failed to set variable 'cache.enabled': Cache cannot be enabled` (`src/config/runtime_variable_manager.cpp`) |

**Success responses** (`src/server/handlers/variable_handler.cpp`):

```
+OK Variable '<name>' set to '<value>'\r\n        -- exactly one assignment
+OK <n> variables set\r\n                          -- two or more assignments
```

Multi-assignment `SET` is all-or-nothing: a failure rolls back every earlier assignment in the
same command before returning `ERROR <code> Failed to set variable '<name>': <message>`
(`src/server/handlers/variable_handler.cpp`). With no runtime variable manager →
`ERROR 6026 Runtime variable manager not initialized`
(`src/server/handlers/variable_handler.cpp`).

### 10.10 `SHOW VARIABLES`

```
SHOW VARIABLES
SHOW VARIABLES LIKE '<pattern>'
```

Every citation in the table below is `src/query/query_parser.cpp`.

| Condition | Response |
|---|---|
| `SHOW` alone | `ERROR 3000 SHOW: Expected subcommand` |
| Subcommand other than `VARIABLES` | `ERROR 3000 SHOW: Unknown subcommand: <token>` |
| Third token is not `LIKE` | `ERROR 3000 SHOW VARIABLES: Expected LIKE 'pattern'` |
| Token count is not exactly 4 with `LIKE` | `ERROR 3000 SHOW VARIABLES LIKE: Expected exactly one pattern` |

With no runtime variable manager → `ERROR 6026 Runtime variable manager not initialized`
(`src/server/handlers/variable_handler.cpp`).

The pattern supports `%` (any sequence) and `_` (single character), matched
case-insensitively (`src/server/handlers/variable_handler.cpp`).

**Empty result:**

```
+OK 0 rows\r\n
```

(`src/server/handlers/variable_handler.cpp`)

**Non-empty result** — a bare MySQL-style ASCII table with CRLF line endings and **no status
prefix** (`src/server/handlers/variable_handler.cpp`):

```
+----------------------+-----------------+---------+\r\n
| Variable_name        | Value           | Mutable |\r\n
+----------------------+-----------------+---------+\r\n
| <name>               | <value>         | YES|NO  |\r\n
...
+----------------------+-----------------+---------+\r\n
<n> row[s] in set\r\n
```

Name and value columns have minimum widths of 20 and 15 characters respectively
(`src/server/handlers/variable_handler.cpp`).

---

## 11. Value escaping in responses

### 11.1 Primary keys and `GET` string values

Applied to `OK RESULTS` primary keys, `OK DOC` primary keys, and `GET` string filter values
(`src/server/response_formatter.cpp`).

A value is emitted verbatim unless it is empty or contains ASCII whitespace, `"`, `\`, a
control character, or any Unicode whitespace — in which case it is wrapped in double quotes
with the following escapes (`src/server/response_formatter.cpp`):

| Byte | Emitted as |
|---|---|
| `\` | `\\` |
| `"` | `\"` |
| CR | `\r` |
| LF | `\n` |
| TAB | `\t` |
| other control character | `\xHH` (uppercase hex) |

### 11.2 Tab- and line-delimited fields

Applied to highlight snippets, facet values, `SYNC STATUS` fields, and `DUMP` status/info
fields (`src/server/response_formatter.cpp`, exposed as
`ResponseFormatter::SanitizeDelimitedField` at `src/server/response_formatter.cpp`).
Every CR, LF, TAB and control character is replaced by a single space. No quoting is applied.

---

## 12. Response frame catalogue

Three status shapes exist. None of the formatters append the transport terminator; that is
added once by the connection layer (`src/server/reactor_connection.cpp`).

| Shape | Producer | Used by |
|---|---|---|
| `OK <body>` | `ResponseFormatter::FormatStatus` (`src/server/response_formatter.cpp`) | `AUTHENTICATED`, `RESULTS`, `COUNT`, `DOC`, `FACET`, `INFO`, `SAVED`, `LOADED`, `DUMP_*`, `CACHE_*`, `DEBUG_*`, `OPTIMIZED`, `REPLICATION*`, `SYNC*` |
| `+OK` / `+OK <body>` | `ResponseFormatter::FormatOk` (`src/server/response_formatter.cpp`) | `CONFIG HELP`, `CONFIG SHOW`, `CONFIG VERIFY`, `SET`, empty `SHOW VARIABLES` |
| `ERROR <code> <message>` | `ResponseFormatter::FormatError` (`src/server/response_formatter.cpp`) | every failure path |

Response prefixes are centralized in `src/server/protocol_constants.h`.

Multi-line responses that terminate with a bare `END` line: `INFO`
(`src/server/response_formatter.cpp`), `REPLICATION STATUS`
(`src/server/response_formatter.cpp`), `DUMP INFO`
(`src/server/handlers/dump_handler.cpp`), `DUMP STATUS`
(`src/server/handlers/dump_handler.cpp`), `CACHE STATS`
(`src/server/handlers/cache_handler.cpp`).

### 12.1 Error frame format

```
ERROR <numeric-code> <message>
```

- `<numeric-code>` is the decimal `uint16` value of `mygram::utils::ErrorCode`
  (`src/server/response_formatter.cpp`).
- The message has every CR, LF, TAB and control character replaced by a space, so an error
  frame is always exactly one line (`src/server/response_formatter.cpp`).
- When the `Error` object carries context, the message becomes
  `<message> (context: <context>)` (`src/server/response_formatter.cpp`).
- Clients parse the frame with `protocol::ParseErrorFrame`, which treats the first token as a
  code only when it is a complete non-zero decimal `uint16`; otherwise the whole payload is
  treated as a legacy uncoded message (`src/server/protocol_constants.h`).

### 12.2 Error codes reachable on the TCP surface

| Code | Symbol | Typical TCP trigger |
|---|---|---|
| 2 | `kInvalidArgument` | `CONFIG VERIFY` path rejections; `DUMP` filepath resolution rejections |
| 4 | `kNotImplemented` | `HIGHLIGHT`/`SORT _score` without text storage; replication not configured; non-MySQL build |
| 5 | `kInternalError` | index/document store unavailable; handler/query-type mismatch; optimize failure; an exception escaping the drain worker |
| 7 | `kPermissionDenied` | failed `AUTH`; administrative command without `AUTH` |
| 8 | `kNotFound` | `CONFIG HELP` unknown path; `SYNC STOP` with nothing running |
| 9 | `kAlreadyExists` | `REPLICATION START` while already running |
| 1000 | `kConfigFileNotFound` | `CONFIG VERIFY` missing file |
| 1001-1006 | config error codes | `CONFIG VERIFY` validation failure (code propagated from the loader); `CONFIG SHOW` unknown path and a `SET` value rejected for a mutable variable (both 1004) |
| 1008 | `kConfigUnknownVariable` | `SET` naming a variable that does not exist |
| 1009 | `kConfigVariableNotMutable` | `SET` on a variable that is not runtime-mutable |
| 2001, 2002, 2005-2010, 2012 | MySQL error codes | `REPLICATION START` failure (code propagated from `BinlogReader::Start`) |
| 3000 | `kQuerySyntaxError` | every parser-level syntax/arity error; unknown query type |
| 3001 | `kQueryInvalidToken` | invalid UTF-8 in search text |
| 3005 | `kQueryTooLong` | `api.max_query_length` exceeded |
| 3006 | `kQueryInvalidFilter` | FILTER column/operator/value rejections |
| 3007 | `kQueryInvalidSort` | SORT rejections; `SORT _score` with BM25 disabled |
| 3008 | `kQueryInvalidLimit` | LIMIT rejections |
| 3009 | `kQueryInvalidOffset` | OFFSET rejections |
| 3010 | `kQueryExpressionParseError` | malformed boolean search expression |
| 4000 | `kIndexNotFound` | search term too short for n-gram search without stored normalized text; unknown `FACET` column; index, document store or filter index missing behind a resolved table |
| 4004 | `kIndexDocumentNotFound` | `GET` miss |
| 4007 | `kTableNotFound` | unknown/ambiguous table; bare name under multi-database config; empty catalog on `OPTIMIZE` |
| 4008 | `kCatalogNotInitialized` | catalog missing on `INFO`, `OPTIMIZE`, `CACHE CLEAR <table>`, `SYNC` |
| 4010 | `kSyncTableNotFound` | `SYNC` for a table the sync manager does not track |
| 4011 | `kSyncAlreadyInProgress` | `SYNC` while another long operation or `SYNC` claims the table; `DUMP SAVE`, `DUMP LOAD`, `OPTIMIZE` or `REPLICATION START` while any `SYNC` is in flight |
| 4012 | `kSyncMemoryCritical` | `SYNC` refused because memory is critically low |
| 4013 | `kSyncThreadCreationFailed` | `SYNC` worker thread could not be created |
| 5001 / 5003 | `kStorageReadError` / `kStorageCorrupted` | `DUMP LOAD` document-store payload failure (propagated verbatim by the loader) |
| 5005 | `kStorageVersionMismatch` | `DUMP LOAD` config / MySQL-source / server-UUID mismatch |
| 5011 / 5012 | `kStorageDumpReadError` / `kStorageDumpWriteError` | dump read/write failure |
| 6007 | `kNetworkInvalidRequest` | request frame larger than the 1 MiB read cap |
| 6019 | `kNetworkReactorModifyFailed` | backpressure interest update rejected by the event multiplexer |
| 6026 | `kServerInitMissingDependency` | configuration or variable manager unavailable |
| 6027 | `kServerShuttingDown` | `SYNC` requested after shutdown has been initiated |
| 6028 | `kServerLoading` | request during `DUMP LOAD` |
| 6029 | `kServerNotReady` | table synchronizing; missing GTID; replication not running |
| 6030 | `kServerBusy` | rate limit; frame-queue overflow; thread-pool saturation; conflicts with `DUMP SAVE`/`DUMP LOAD`/`OPTIMIZE`; memory gates |
| 8001 | `kCacheDisabled` | `CACHE` commands with the cache unconfigured or disabled; `SET cache.enabled = true` when the subsystem cannot start |

Numeric values are defined in `src/utils/error.h`.

---

## 13. Known divergences

The following are places where two code paths in the current tree disagree about the same
thing. They are recorded as facts; no remedy is proposed here.

1. **`SHOW VARIABLES` has no status prefix when it returns rows.** The empty result is
   `+OK 0 rows` (`src/server/handlers/variable_handler.cpp`), but a non-empty result is
   returned as a bare ASCII table beginning with `+---`. Every other command on this surface
   begins with `OK `, `+OK` or `ERROR ` (`src/server/response_formatter.cpp`), so a
   client keying on those prefixes cannot classify a non-empty `SHOW VARIABLES` reply.

2. **`SEARCH` has two different frame terminations depending on `HIGHLIGHT`.**
   `FormatSearchResponse` emits no trailing CRLF (`src/server/response_formatter.cpp`),
   while `FormatSearchResponseWithHighlights` appends one and `FormatFacetResponse` does
   likewise. After the transport terminator
   (`src/server/reactor_connection.cpp`) the same command therefore ends in either `\r\n`
   or `\r\n\r\n`.

3. **Dump filepaths are sanitized on some responses and not others.** `DUMP VERIFY`
   (`src/server/handlers/dump_handler.cpp`) and `DUMP INFO` pass the path through
   `SanitizeDelimitedField`, while `DUMP SAVE` and `DUMP LOAD`
   (`src/server/handlers/dump_handler.cpp` → `src/server/response_formatter.cpp`) emit it raw.

4. **`SYNC STATUS` terminates its body differently from every other multi-line response.**
   `GetSyncStatus` emits `END\r\n` (`src/server/sync_operation_manager.cpp`,
   `src/server/response_formatter.cpp`), whereas `INFO`, `REPLICATION STATUS`, `DUMP INFO`
   (`src/server/handlers/dump_handler.cpp`), `DUMP STATUS` and `CACHE STATS`
   (`src/server/handlers/cache_handler.cpp`) all emit a bare `END`.

5. **`QueryType::SAVE` and `QueryType::LOAD` are unreachable but still counted.** The enum
   values exist (`src/query/query_parser.h`) and `ServerStats::IncrementCommand` has
   dedicated counters for them (`src/server/server_stats.cpp`), which `INFO`
   (`src/server/response_formatter.cpp`) and the Prometheus exposition can emit — yet `Parse`
   rejects both keywords
   outright (`src/query/query_parser.cpp`), so `cmd_save` and `cmd_load` can never be
   non-zero. The same header's class documentation still lists `SAVE [filename]` and
   `LOAD [filename]` as supported commands (`src/query/query_parser.h`).

6. **The documented and configured defaults for `api.max_query_length` differ.**
   `config::defaults::kDefaultQueryLengthLimit` is `128` (`src/config/config.h`), while
   `ReactorConnection`'s read-cap rationale states the default is "~64 KiB" when justifying its
   1 MiB read buffer (`src/server/reactor_connection.h`).

7. **`INFO` and `CONFIG SHOW` are classified differently despite comparable disclosure.**
   `INFO` is not in the administrative set (`src/server/request_dispatcher.cpp`) and is
   therefore answerable before `AUTH`, yet it discloses the table list, memory figures, the
   current replication GTID and the cache configuration snapshot
   (`src/server/response_formatter.cpp`). `CONFIG SHOW` is administrative and requires
   `AUTH` (`src/server/request_dispatcher.cpp`).

8. **`CACHE CLEAR` requires the cache to be enabled; the sibling subcommands do not.**
   `HandleClear` rejects a disabled cache with `ERROR 8001 Cache is disabled`
   (`src/server/handlers/cache_handler.cpp`), while `CACHE STATS` and `CACHE DISABLE` proceed
   and only require the cache manager to exist.

9. **`ResponseFormatter::FormatSaveResponse` is dead code that duplicates a live format.** It
   produces `OK SAVED <path>` (`src/server/response_formatter.cpp`) but has no caller;
   the synchronous `DUMP SAVE` path builds the identical frame through `FormatStatus`
   (`src/server/handlers/dump_handler.cpp`). Its sibling `FormatLoadResponse`
   (`src/server/response_formatter.cpp`) is the one actually used by `DUMP LOAD`.
