# TCP Text Protocol — Command Specification

This file is normative. It describes the observed current behavior of the MygramDB TCP text
surface as implemented in `src/query/` and `src/server/`. Where the implementation deviates
from anything stated here, the implementation is the bug. Every claim carries a
repo-relative `file:line` citation so the document can be mechanically re-verified against
the source.

---

## 1. Wire framing

| Property | Value | Implementation |
|---|---|---|
| Request frame delimiter | `\r\n` (CRLF) | `src/server/reactor_connection.cpp:40` |
| Response frame terminator | `\r\n`, appended by the transport to every response body | `src/server/reactor_connection.cpp:42`, `src/server/reactor_connection.cpp:791` |
| Encoding | Raw bytes; UTF-8 expected in search text. Invalid UTF-8 in search terms is rejected by the pipeline | `src/server/search_pipeline.cpp:1770` |
| Receive chunk size | 4096 bytes per `recv()` | `src/server/reactor_connection.cpp:39` |
| Max unframed tail (effective max request line) | 1 MiB (`kMaxReadBufferBytes`) | `src/server/reactor_connection.h:134` |
| Max single frame length | 1 MiB, and additionally bounded by the remaining pending-frame byte budget | `src/server/reactor_connection.cpp:510-511` |
| Max queued frames per connection | 1024 (`kMaxPendingFrames`), configurable via `api.tcp.max_pending_frames` | `src/server/reactor_connection.h:150`, `src/server/server_types.h:83` |
| Max queued frame bytes per connection | 4 MiB (`kMaxPendingFrameBytes`), configurable via `api.tcp.max_pending_frame_bytes` | `src/server/reactor_connection.h:151`, `src/server/server_types.h:84-85` |
| Per-connection unsent response cap | 16 MiB (`api.tcp.max_write_queue_bytes`) | `src/server/reactor_connection.h:141`, `src/server/server_types.h:78-80` |
| Shared read+write byte budget across all connections | 256 MiB (`api.tcp.max_total_buffered_bytes`) | `src/server/server_types.h:81-82` |
| Read fairness budget per readable event | 64 KiB or 64 frames, whichever comes first | `src/server/reactor_connection.h:145-146`, `src/server/reactor_connection.cpp:195` |

### 1.1 Frame extraction rules

- Frames are split on the exact two-byte sequence `\r\n`. A lone `CR` not followed by `LF` is
  skipped and scanning continues; it does not terminate a frame
  (`src/server/reactor_connection.cpp:503-507`).
- A trailing `CR` at the end of the read buffer is retained so a following `LF` from the next
  `recv()` can complete the frame (`src/server/reactor_connection.cpp:541-550`).
- A bare `\r\n` yields a zero-length frame, which is dispatched normally
  (`src/server/reactor_connection.cpp:533`) and answered with `ERROR 3000 Empty query`
  (`src/query/query_parser.cpp:107-113`).
- Pipelining is supported: multiple CRLF-delimited frames in one `recv()` are all queued and
  dispatched in order by a single drain worker (`src/server/reactor_connection.cpp:609-650`).
- Backpressure: reads are disarmed at 75% of either pending-frame limit and re-armed at 50%
  (`src/server/reactor_connection.cpp:370-380`, `src/server/reactor_connection.cpp:443-467`).

### 1.2 Limit-exceeded behavior

| Condition | Server action | Implementation |
|---|---|---|
| Unframed tail exceeds 1 MiB | Best-effort send `ERROR 6007 request too large`, then close | `src/server/reactor_connection.cpp:172`, `src/server/reactor_connection.cpp:425` |
| Pending-frame count/byte cap exceeded, or shared budget exhausted | Best-effort send `ERROR 6030 server busy`, then close | `src/server/reactor_connection.cpp:176`, `src/server/reactor_connection.cpp:387`, `src/server/reactor_connection.cpp:418` |
| Event multiplexer rejects the backpressure interest update | Best-effort send `ERROR 6019 event multiplexer modify failed`, then close | `src/server/reactor_connection.cpp:179-181`, `src/server/reactor_connection.cpp:414-416` |
| Unsent response bytes exceed the per-connection cap | Connection closed with no error frame | `src/server/reactor_connection.cpp:760-770` |
| Thread pool queue full when scheduling a drain task | Send `ERROR 6030 SERVER_BUSY Server is too busy, please try again later` on that request's own connection, count it in `requests_denied_pool_full_tcp`, then close | `src/server/reactor_connection.cpp:447-457`, `src/server/reactor_connection.cpp:459-475` |
| Reactor registration fails at accept time | Send `ERROR 6030 SERVER_BUSY Server is too busy, please try again later`, then close | `src/server/connection_acceptor.cpp:589-593` |

The best-effort error frames above are written with a blocking `send()` loop only when the
write queue is empty, and are followed unconditionally by connection teardown
(`src/server/reactor_connection.cpp:51-67`, `src/server/reactor_connection.cpp:393-404`).

### 1.3 Timeouts

| Timeout | Default | Config key | Implementation |
|---|---|---|---|
| Initial read (first complete frame) | 60 s | `api.tcp.recv_timeout_sec` | `src/server/server_types.h:74`, `src/server/io_reactor.cpp:468-474` |
| Idle (no read or write activity) | 300 s | `api.tcp.idle_timeout_sec` | `src/server/server_types.h:75`, `src/server/io_reactor.cpp:476-489` |
| Reaper sweep interval | 5 s | `api.tcp.reaper_interval_sec` | `src/server/server_types.h:76`, `src/server/io_reactor.cpp:434` |
| Poll timeout | 100 ms | not configurable | `src/server/io_reactor.h:68` |

`0` disables either timeout (`src/server/io_reactor.cpp:468`, `src/server/io_reactor.cpp:476`).
A connection with a request currently queued or executing in the drain worker is exempt from
idle reaping (`src/server/io_reactor.cpp:481-483`). **Reaping closes the socket without
sending any frame** (`src/server/io_reactor.cpp:493-502`).

TCP keepalive is applied per accepted socket: enabled by default, 60 s idle, 20 s interval,
3 probes (`src/server/server_types.h:89-94`, `src/server/connection_acceptor.cpp:539-554`).

### 1.4 Admission control before any command is parsed

1. **Connection limit** — `api.tcp.max_connections` (default 10000). Exceeding it closes the
   accepted socket immediately with no response
   (`src/server/connection_acceptor.cpp:488-502`, `src/server/server_types.h:56`).
2. **Network ACL** — `network.allow_cidrs`. A peer outside the allow list is closed with no
   response. Not applied on Unix domain sockets
   (`src/server/connection_acceptor.cpp:504-526`).

---

## 2. Tokenization, quoting and escaping

Every request frame is tokenized by `QueryParser::Tokenize`
(`src/query/query_parser.cpp:538-676`) before command dispatch.

### 2.1 Splitting

- Outside quotes, tokens are split on ASCII **and** Unicode whitespace
  (`src/query/query_parser.cpp:626-639`).
- `"` and `'` both open a quoted region. Opening a quote flushes any pending unquoted token
  first, so `abc"def"` yields two tokens `abc` and `def`
  (`src/query/query_parser.cpp:613-624`).
- Inside quotes, whitespace is literal (`src/query/query_parser.cpp:652-653`).
- A closing quote always emits a token, **including an empty one**, and the token is flagged
  as quoted (`src/query/query_parser.cpp:642-650`). The quoted flag suppresses clause-keyword
  and parenthesis interpretation for that token
  (`src/query/query_parser_commands.cpp:134-151`).

### 2.2 Escape sequences

Backslash is escape syntax **only inside a quoted literal**. Outside quotes a backslash is a
literal character, so Windows-style paths survive intact
(`src/query/query_parser.cpp:601-611`).

| Sequence | Produces | Implementation |
|---|---|---|
| `\n` | LF | `src/query/query_parser.cpp:551-552` |
| `\t` | TAB | `src/query/query_parser.cpp:553-554` |
| `\r` | CR | `src/query/query_parser.cpp:555-556` |
| `\\` | `\` | `src/query/query_parser.cpp:557-558` |
| `\"` | `"` | `src/query/query_parser.cpp:559-560` |
| `\'` | `'` | `src/query/query_parser.cpp:561-562` |
| `\xHH` | the byte `0xHH` | `src/query/query_parser.cpp:569-590` |
| `\<other>` | `<other>` verbatim; the backslash is dropped | `src/query/query_parser.cpp:593-595` |

`\x` not followed by two valid hex digits (or with fewer than two bytes remaining) emits the
literal character `x` and drops the backslash (`src/query/query_parser.cpp:591`).

### 2.3 Tokenizer failure modes

| Input condition | Result |
|---|---|
| Backslash is the final byte of the frame | `ERROR 3000 Unterminated escape sequence at end of input` (`src/query/query_parser.cpp:657-661`) |
| Quote never closed | `ERROR 3000 Unclosed quote: <char>` (`src/query/query_parser.cpp:663-667`) |
| No tokens produced | `ERROR 3000 Empty query` (`src/query/query_parser.cpp:107-113`) |

### 2.4 Parenthesis accounting

Parentheses inside quoted regions are ignored for depth tracking. A quote preceded by an odd
number of backslashes does not toggle the quote state
(`src/query/query_parser.cpp:54-96`). Unbalanced parentheses in search text produce
`ERROR 3000 Unmatched closing parenthesis` or `ERROR 3000 Unclosed parenthesis`
(`src/query/query_parser_commands.cpp:139-143`, `src/query/query_parser_commands.cpp:164-168`).

---

## 3. Case sensitivity

- **Command and subcommand keywords are case-insensitive.** All dispatch comparisons go
  through `EqualsIgnoreCase` (`src/query/query_parser_internal.h:28-35`, used throughout
  `src/query/query_parser.cpp:118-496`).
- **Clause keywords are case-insensitive** (`AND`, `OR`, `NOT`, `FILTER`, `SORT`, `LIMIT`,
  `OFFSET`, `HIGHLIGHT`, `FUZZY`, `FACET`) — `src/query/query_parser_internal.h:42-47`.
- **Table names, column names, primary keys, file paths and variable values preserve their
  original case** (`src/query/query_parser.cpp:303`, `src/query/query_parser.cpp:310`,
  `src/query/query_parser_clauses.cpp:324`).
- Named filter operators (`EQ`, `NE`, `GT`, `GTE`, `LT`, `LTE`) are case-insensitive; symbolic
  ones are exact (`src/query/query_parser_clauses.cpp:468-491`).
- `ASC` / `DESC` / `BY` are case-insensitive (`src/query/query_parser_clauses.cpp:302-343`).

---

## 4. Authentication

The shared secret is `api.admin_token` (`src/config/config.h:459`), surfaced to the dispatcher
as `ServerConfig::admin_token` (`src/server/server_types.h:99`).

### 4.1 `AUTH` command

```
AUTH <shared-secret>
```

| Property | Behavior | Implementation |
|---|---|---|
| Arity | Exactly one argument, non-empty. Any other count → `ERROR 3000 AUTH requires exactly one non-empty token` | `src/query/query_parser.cpp:121-130` |
| Comparison | Constant-time over the maximum of both lengths | `src/server/request_dispatcher.cpp:53-62` |
| Success | `OK AUTHENTICATED`; sets per-connection `admin_authenticated` | `src/server/request_dispatcher.cpp:159-160` |
| Failure (wrong token, or no token configured) | `ERROR 7 Authentication failed`; clears `admin_authenticated` | `src/server/request_dispatcher.cpp:155-158` |
| Statistics | Counted whether it succeeds or fails | `src/server/request_dispatcher.cpp:154` |
| Logging | The whole request is logged as `AUTH <redacted>` | `src/server/request_dispatcher.cpp:64-82`, `src/server/request_dispatcher.cpp:127` |
| Scope | Per TCP connection; `conn_ctx_` is owned by `ReactorConnection` and lives for the connection's lifetime | `src/server/reactor_connection.h:453` |

The connection is **not** closed on a failed AUTH; unlimited retries are possible subject to
rate limiting.

### 4.2 Gated commands

When `api.admin_token` is non-empty, the following commands require a prior successful `AUTH`
on the same connection; otherwise they return
`ERROR 7 Administrative command requires AUTH`
(`src/server/request_dispatcher.cpp:163-167`):

`DUMP SAVE`, `DUMP LOAD`, `DUMP VERIFY`, `DUMP INFO`, `DUMP STATUS`,
`REPLICATION STATUS`, `REPLICATION STOP`, `REPLICATION START`,
`SYNC`, `SYNC STATUS`, `SYNC STOP`,
`CONFIG HELP`, `CONFIG SHOW`, `CONFIG VERIFY`,
`OPTIMIZE`, `DEBUG ON`, `DEBUG OFF`,
`CACHE CLEAR`, `CACHE STATS`, `CACHE ENABLE`, `CACHE DISABLE`,
`SET`, `SHOW VARIABLES`
(`src/server/request_dispatcher.cpp:22-51`).

**Permitted pre-auth:** `AUTH`, `SEARCH`, `COUNT`, `GET`, `FACET`, `INFO`. When
`api.admin_token` is empty, the gate is disabled entirely and every command is permitted
(`src/server/request_dispatcher.cpp:163`).

---

## 5. Rate limiting

| Property | Behavior | Implementation |
|---|---|---|
| Algorithm | Token bucket, one token per request | `src/server/rate_limiter.h:29-87` |
| Unit counted | One dispatched request frame — including malformed frames and failed AUTH, because the check runs before parsing | `src/server/request_dispatcher.cpp:104-121` |
| Granularity | Per peer IP address | `src/server/request_dispatcher.cpp:109`, `src/server/rate_limiter.h:135` |
| Unknown/unavailable peer address | Falls back to the shared literal key `unknown`; never fails open | `src/server/request_dispatcher.cpp:107-109` |
| Shared with HTTP | **Yes.** `TcpServer` owns the `RateLimiter` as a `shared_ptr` and `HttpServer` co-owns the same instance, so one client IP draws from a single bucket across both surfaces | `src/server/tcp_server.cpp:127-131`, `src/app/server_orchestrator.cpp:943`, `src/server/http_server.cpp:760` |
| Denial response | `ERROR 6030 Rate limit exceeded` (connection stays open) | `src/server/request_dispatcher.cpp:120` |
| Denial metric | `mygramdb_requests_denied_total{reason="rate_limit",surface="tcp"}` | `src/server/request_dispatcher.cpp:111`, `src/server/response_formatter.cpp:992-993` |
| Defaults | disabled; capacity 100, refill 10/s, max 10000 tracked clients | `src/config/config.h:350-352`, `src/config/config.h:479-484` |
| Tracking-table full | `max_clients` bounds memory, not admission: the least-recently-seen bucket is evicted and the new client is served, so no request is denied for a reason other than an exhausted bucket | `src/server/rate_limiter.cpp:130-150` |

Denial log lines are themselves rate-limited under the key `tcp:<ip>`; the HTTP surface uses
`http:<ip>`, so the two surfaces have separate log suppression but share the token bucket
(`src/server/request_dispatcher.cpp:112`, `src/server/http_server.cpp:763`).

---

## 6. Command table

Every command below is dispatched from `QueryParser::Parse`
(`src/query/query_parser.cpp:98-500`) and routed by `RequestDispatcher::Dispatch`
(`src/server/request_dispatcher.cpp:101-195`) to a handler registered in
`src/server/server_lifecycle_manager.cpp:358-391`.

| Command | Min/max args | Mutates state | Auth-gated | Rate-limited | Handler |
|---|---|---|---|---|---|
| `AUTH <secret>` | 1 / 1 | connection auth flag | n/a | yes | dispatcher inline (`src/server/request_dispatcher.cpp:150-161`) |
| `SEARCH <table> <text> [clauses]` | 2 / unbounded | no | no | yes | `src/server/handlers/search_handler.cpp:320` |
| `COUNT <table> <text> [clauses]` | 2 / unbounded | no | no | yes | `src/server/handlers/search_handler.cpp:522` |
| `GET <table> <primary_key>` | 2 / 2 | no | no | yes | `src/server/handlers/document_handler.cpp:10` |
| `FACET <table> <column> [text] [clauses]` | 2 / unbounded | no | no | yes | `src/server/handlers/facet_handler.cpp:24` |
| `INFO` | 0 / 0 (extras ignored) | yes — refreshes peak-memory statistics | no | yes | `src/server/handlers/admin_handler.cpp:40` |
| `DUMP SAVE [filepath]` | 0 / 1 | yes — writes a snapshot | yes | yes | `src/server/handlers/dump_handler.cpp:75` |
| `DUMP LOAD <filepath>` | 1 / 1 (extras ignored) | yes — replaces all index state | yes | yes | `src/server/handlers/dump_handler.cpp:374` |
| `DUMP VERIFY <filepath>` | 1 / 1 (extras ignored) | no | yes | yes | `src/server/handlers/dump_handler.cpp:720` |
| `DUMP INFO <filepath>` | 1 / 1 (extras ignored) | no | yes | yes | `src/server/handlers/dump_handler.cpp:760` |
| `DUMP STATUS` | 0 / 0 (extras ignored) | no | yes | yes | `src/server/handlers/dump_handler.cpp:801` |
| `CONFIG HELP [path]` | 0 / 1 (extras ignored) | no | yes | yes | `src/server/handlers/admin_handler.cpp:92` |
| `CONFIG SHOW [path]` | 0 / 1 (extras ignored) | no | yes | yes | `src/server/handlers/admin_handler.cpp:120` |
| `CONFIG VERIFY <filepath>` | 1 / 1 (extras ignored) | no | yes | yes | `src/server/handlers/admin_handler.cpp:144` |
| `REPLICATION STATUS` | 0 / 0 (extras ignored) | no | yes | yes | `src/server/handlers/replication_handler.cpp:20` |
| `REPLICATION STOP` | 0 / 0 (extras ignored) | yes — stops binlog reader | yes | yes | `src/server/handlers/replication_handler.cpp:23` |
| `REPLICATION START` | 0 / 0 (extras ignored) | yes — starts binlog reader | yes | yes | `src/server/handlers/replication_handler.cpp:46` |
| `SYNC <table>` | 1 / 1 (extras ignored) | yes — rebuilds a table | yes | yes | `src/server/handlers/sync_handler.cpp:29` |
| `SYNC STATUS` | 0 / 0 (extras ignored) | no | yes | yes | `src/server/handlers/sync_handler.cpp:61` |
| `SYNC STOP [table]` | 0 / 1 (extras ignored) | yes — requests cancellation | yes | yes | `src/server/handlers/sync_handler.cpp:65` |
| `OPTIMIZE [table]` | 0 / 1 (extras ignored) | yes — rebuilds posting lists | yes | yes | `src/server/handlers/debug_handler.cpp:43` |
| `DEBUG ON` / `DEBUG OFF` | 1 / 1 (extras ignored) | yes — connection debug flag | yes | yes | `src/server/handlers/debug_handler.cpp:25`, `:34` |
| `CACHE CLEAR [table]` | 0 / 1 (extras ignored) | yes — evicts cache entries | yes | yes | `src/server/handlers/cache_handler.cpp:36` |
| `CACHE STATS` | 0 / 0 (extras ignored) | no | yes | yes | `src/server/handlers/cache_handler.cpp:64` |
| `CACHE ENABLE` / `CACHE DISABLE` | 0 / 0 (extras ignored) | yes — cache enablement | yes | yes | `src/server/handlers/cache_handler.cpp:125`, `:142` |
| `SET <var> = <value> [, ...]` | 1 / unbounded | yes — runtime configuration | yes | yes | `src/server/handlers/variable_handler.cpp:43` |
| `SHOW VARIABLES [LIKE '<pattern>']` | 1 / 3 | no | yes | yes | `src/server/handlers/variable_handler.cpp:93` |
| `SAVE ...` | — | retired | — | yes | rejected at parse (`src/query/query_parser.cpp:143-146`) |
| `LOAD ...` | — | retired | — | yes | rejected at parse (`src/query/query_parser.cpp:147-150`) |

Any unrecognized first token yields `ERROR 3000 Unknown command: <token>`
(`src/query/query_parser.cpp:498-499`).

---

## 7. Global parser limits

| Limit | Value | Applies to | Implementation |
|---|---|---|---|
| `QueryParser::kMaxTermCount` | 64 | AND terms, NOT terms, FILTER conditions — each counted independently. Exceeding → `ERROR 3000 Too many {AND terms\|NOT terms\|FILTER conditions} (max 64)` | `src/query/query_parser.h:272`, enforced at `src/query/query_parser_commands.cpp:308-319`, `:398-409`, `:514-525` |
| `QueryParser::kMaxFilterColumnNameLength` | 128 | FILTER, SORT and FACET column names | `src/query/query_parser.h:273`, `src/query/query_parser.cpp:506-519` |
| `QueryParser::kMaxFilterValueLength` | 1024 | FILTER value. Exceeding → `ERROR 3006` | `src/query/query_parser.h:274`, `src/query/query_parser_clauses.cpp:92-95` |
| `QueryParser::kMaxHighlightTagLength` | 256 bytes | each `HIGHLIGHT TAG` argument | `src/query/query_parser.h:276`, `src/query/query_parser_clauses.cpp:390-399` |
| `kMaxLimit` | 1000 | `LIMIT` on SEARCH and FACET. Exceeding → `ERROR 3000 LIMIT exceeds maximum of 1000` | `src/query/query_parser_internal.h:18`, `src/config/config.h:63`, `src/query/query_parser_commands.cpp:322-326`, `:528-532` |
| `api.default_limit` | 100 (valid 5–1000) | `LIMIT` substituted for SEARCH and FACET when not explicit | `src/config/config.h:61`, `src/config/config.h:466`, `src/server/request_dispatcher.cpp:170-172` |
| `api.max_query_length` | 128 characters | sum of search text + AND terms + NOT terms + filter columns/values + sort column + highlight tags. `0` disables. Exceeding → `ERROR 3005` | `src/config/config.h:64`, `src/config/config.h:471`, `src/query/query_parser.cpp:21-48`, `src/query/query_parser.cpp:521-536` |
| `QueryASTParser::kMaxRecursionDepth` | 32 | nesting depth of a boolean search expression | `src/query/query_ast.h:184` |
| `QueryASTParser::kMaxTermCount` | 64 | TERM nodes inside a boolean search expression | `src/query/query_ast.h:185` |

Query-length validation runs for `SEARCH`, `COUNT` and `FACET` only
(`src/query/query_parser_commands.cpp:328-334`, `:411-417`, `:534-540`).

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
  (`src/query/query_parser_commands.cpp:228-231`).
- **Clause order is not enforced.** After search text, the parser loops over remaining tokens
  and dispatches on whichever clause keyword appears
  (`src/query/query_parser_commands.cpp:246-305`). An unrecognized token in that position →
  `ERROR 3000 Unknown keyword: <token>` (`src/query/query_parser_commands.cpp:300-304`).
- `AND` / `NOT` / `FILTER` accumulate. `SORT`, `HIGHLIGHT` and `FUZZY` may repeat; the last
  occurrence wins. `LIMIT` and `OFFSET` may appear **once only**.
- `ORDER` anywhere in the command →
  `ERROR 3000 ORDER BY is not supported. Use SORT instead. Example: SEARCH table text SORT column DESC`
  (`src/query/query_parser_commands.cpp:265-269`; the search-text scanner produces the shorter
  variant `ORDER BY is not supported. Use SORT instead.` at
  `src/query/query_parser_commands.cpp:154-158`).
- A comma in the table position or a bare `,` token →
  `ERROR 3000 Multiple tables not supported. Hint: MygramDB searches a single table at a time. Use separate queries for multiple tables.`
  (`src/query/query_parser_commands.cpp:73-79`).

**Search-text consumption.** Tokens are consumed until a clause keyword is reached at
parenthesis depth 0. Which keywords stop consumption depends on whether the expression is a
boolean expression (`src/query/query_parser_commands.cpp:89-151`):

- *Legacy mode* (default): stops at `AND`, `OR`, `NOT`, `FILTER`, `SORT`, `LIMIT`, `OFFSET`,
  `HIGHLIGHT`, `FUZZY`, `FACET`.
- *Boolean-expression mode*, entered when the expression contains a top-level `OR`, a
  parenthesized operand after a top-level boolean operator, or a leading unary `NOT`
  (`src/query/query_parser_commands.cpp:121-124`): stops only at `FILTER`, `SORT`, `LIMIT`,
  `OFFSET`, `HIGHLIGHT`, `FUZZY`, `FACET`, so `AND`/`OR`/`NOT` remain part of the expression
  and are parsed by `QueryASTParser` (`src/server/search_pipeline.cpp:1775-1783`).

An all-empty search text (e.g. `SEARCH t ""`) →
`ERROR 3000 SEARCH requires non-empty search text` (`src/query/query_parser_commands.cpp:213-217`).
A malformed boolean expression →
`ERROR 3010 Invalid boolean search expression: <detail>` (`src/server/search_pipeline.cpp:1779-1783`).
Invalid UTF-8 in any search term → `ERROR 3001 Invalid UTF-8 in query text`
(`src/server/search_pipeline.cpp:1770-1773`).

**Success response — without HIGHLIGHT** (`src/server/response_formatter.cpp:280-332`):

```
OK RESULTS <total_matched> <pk1> <pk2> ... <pkN>\r\n
```

`<total_matched>` is the match count before pagination. Primary keys are space-separated on
the header line and escaped per §11.1. Documents present in the index but missing from the
document store are silently omitted from the list while `<total_matched>` still counts them
(`src/server/response_formatter.cpp:306-324`).

**Success response — with HIGHLIGHT** (`src/server/response_formatter.cpp:334-377`):

```
OK RESULTS <total_matched>\r\n<pk>\t<snippet>\r\n<pk>\t<snippet>\r\n...\r\n
```

Snippets are sanitized per §11.2. The body already ends with `\r\n`, so the transport
terminator makes the frame end `\r\n\r\n`.

**Debug block.** When the connection is in debug mode (`DEBUG ON`), a block is appended before
the terminator (`src/server/response_formatter.cpp:227-276`):

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
only (`src/server/response_formatter.cpp:238-273`). Stage counts are also suppressed on a
cache hit (`src/server/response_formatter.cpp:238`, `src/server/handlers/search_handler.cpp:123`).

**Handler-level errors:**

| Condition | Response | Implementation |
|---|---|---|
| Bare table name under a multi-database configuration | `ERROR 4007 Bare table names are not supported; use <database>.<table>: <name>` | `src/server/handlers/command_handler.cpp:39-42` |
| Unknown table | `ERROR 4007 Table not found: <name>` | `src/server/handlers/command_handler.cpp:44-47` |
| `DUMP LOAD` in progress | `ERROR 6028 Server is loading, please try again later` | `src/server/handlers/command_handler.cpp:78-84` |
| Table is being rebuilt by `SYNC` | `ERROR 6029 Table '<name>' is synchronizing, please try again later` | `src/server/handlers/command_handler.cpp:107-109` |
| `HIGHLIGHT` without stored normalized text | `ERROR 4 HIGHLIGHT requires normalized text storage. Set memory.verify_text to "ascii" or "all" in configuration.` | `src/server/handlers/search_handler.cpp:398-403` |
| `SORT _score` with BM25 disabled | `ERROR 3007 SORT _score requires BM25 to be enabled in configuration` | `src/server/handlers/search_handler.cpp:408-411` |
| `SORT _score` without stored normalized text | `ERROR 4 SORT _score requires normalized text storage. ...` | `src/server/handlers/search_handler.cpp:412-417` |
| Index or document store unavailable | `ERROR 5 Index not available` / `ERROR 5 Document store not available` | `src/server/handlers/search_handler.cpp:214-216`, `:333-335` |

### 8.2 `COUNT`

```
COUNT <table> <search_text> [AND <term>]... [NOT <term>]... [FILTER <col> <op> <value>]...
```

- Minimum 3 tokens; fewer → `ERROR 3000 COUNT requires at least table and search text`
  (`src/query/query_parser_commands.cpp:345-348`).
- Only `AND`, `NOT` and `FILTER` are accepted. Anything else →
  `ERROR 3000 COUNT only supports AND, NOT and FILTER clauses`
  (`src/query/query_parser_commands.cpp:390-394`). `SORT` gets a dedicated message
  (`ERROR 3000 COUNT does not support SORT clause. Use SEARCH if you need sorted results.`,
  `src/query/query_parser_commands.cpp:386-389`) and `ORDER` gets
  `ERROR 3000 ORDER BY is not supported. Use SORT instead (note: COUNT does not support sorting).`
  (`src/query/query_parser_commands.cpp:382-385`).
- Search-text extraction, boolean-expression detection and the term/filter caps are identical
  to `SEARCH`.

**Success response** (`src/server/response_formatter.cpp:379-406`):

```
OK COUNT <n>\r\n
```

With debug mode enabled, the debug block is a reduced form carrying `query_time`,
`index_time`, `terms`, `ngrams` and the extended cache section — it never carries the
pipeline-stage counts (`src/server/response_formatter.cpp:393-405`).

### 8.3 `GET`

```
GET <table> <primary_key>
```

Exactly 3 tokens. Any other count → `ERROR 3000 GET requires table and primary_key`
(`src/query/query_parser_commands.cpp:432-435`). No clauses are accepted; extra tokens are an
arity error, not ignored.

**Success response** (`src/server/response_formatter.cpp:434-473`):

```
OK DOC <primary_key>[ <col>=<value>]...\r\n
```

Filter column values are rendered per type: `NULL` for absent, `true`/`false` for booleans,
strings escaped per §11.1, doubles at 6 decimal places, `TimeValue` as integer seconds, 8-bit
integers widened to `int` (`src/server/response_formatter.cpp:447-470`).

Document absent → `ERROR 4004 Document not found`
(`src/server/handlers/document_handler.cpp:35-37`, `src/server/response_formatter.cpp:436`).
The loading and syncing pre-flight checks of §8.1 apply
(`src/server/handlers/document_handler.cpp:13-19`).

### 8.4 `FACET`

```
FACET <table> <column> [search_text] [AND <term>]... [NOT <term>]...
      [FILTER <col> <op> <value>]... [LIMIT <n>|<offset>,<count>] [OFFSET <n>]
```

- Table and column are both mandatory: `ERROR 3000 FACET requires table name` /
  `ERROR 3000 FACET requires column name` (`src/query/query_parser_commands.cpp:449-459`).
- The column must satisfy `IsSafeColumnName`; otherwise `ERROR 3000 Invalid facet column`
  (`src/query/query_parser_commands.cpp:462-465`).
- **Search text is optional** (`require_search_text = false`,
  `src/query/query_parser_commands.cpp:467`, `src/query/query_parser_commands.cpp:170-172`).
- Accepted clauses are `AND`, `NOT`, `FILTER`, `LIMIT`, `OFFSET` only. Anything else →
  `ERROR 3000 FACET: Unknown clause: <token>` (`src/query/query_parser_commands.cpp:506-510`).
  `SORT`, `HIGHLIGHT` and `FUZZY` are therefore rejected on FACET.
- Term caps and the 1000 `LIMIT` cap apply as for `SEARCH`
  (`src/query/query_parser_commands.cpp:513-532`).

**Success response** (`src/server/response_formatter.cpp:408-432`):

```
OK FACET <returned_value_count> <total_distinct_values>\r\n
<value>\t<count>\r\n
<value>\t<count>\r\n
...
[# query_time_ms: <v>\r\n# matched_documents: <n>\r\n# distinct_values: <n>\r\n]
\r\n
```

Values are sanitized per §11.2. The `#`-prefixed lines appear only in debug mode
(`src/server/handlers/facet_handler.cpp:70-76`). The body already ends with `\r\n`, so the
frame ends `\r\n\r\n`.

---

## 9. Clause grammar

### 9.1 `AND <term>` / `NOT <term>`

- Each consumes exactly one following token. Missing → `ERROR 3000 AND requires a term` /
  `ERROR 3000 NOT requires a term` (`src/query/query_parser_clauses.cpp:50-52`, `:70-72`).
- `AND NOT <term>` is folded into a single NOT term rather than indexing the literal keyword
  (`src/query/query_parser_clauses.cpp:57-59`).

### 9.2 `FILTER`

Two accepted forms (`src/query/query_parser_clauses.cpp:101-181`):

```
FILTER <column> <op> <value>          -- three separate tokens
FILTER <column><op><value>            -- one compound token, e.g. FILTER status=active
FILTER <column><op> <value>           -- compound column+operator, value as next token
```

Operators, longest-match first when scanning a compound token
(`src/query/query_parser_clauses.cpp:112`):

| Symbolic | Alternate | Named | Enum |
|---|---|---|---|
| `=` | `==` | `EQ` | `FilterOp::EQ` |
| `!=` | `<>` | `NE` | `FilterOp::NE` |
| `>` | — | `GT` | `FilterOp::GT` |
| `>=` | `≥` (U+2265) | `GTE` | `FilterOp::GTE` |
| `<` | — | `LT` | `FilterOp::LT` |
| `<=` | `≤` (U+2264) | `LTE` | `FilterOp::LTE` |

(`src/query/query_parser_clauses.cpp:468-491`)

| Condition | Response |
|---|---|
| Fewer than three tokens available and no compound form matched | `ERROR 3006 FILTER requires column, operator, and value` (`src/query/query_parser_clauses.cpp:103-105`, `:164-166`) |
| Operator token unrecognized | `ERROR 3006 Invalid filter operator: <token>` (`src/query/query_parser_clauses.cpp:170-173`) |
| Value begins with `=`, `<`, `>` or `!` | `ERROR 3006 FILTER value must not start with an operator character` (`src/query/query_parser_clauses.cpp:27-41`) |
| Column fails `IsSafeColumnName` | `ERROR 3006 Invalid filter column` (`src/query/query_parser_clauses.cpp:89-91`) |
| Value longer than 1024 bytes | `ERROR 3006 FILTER value exceeds maximum length (1024)` (`src/query/query_parser_clauses.cpp:92-95`) |

`IsSafeColumnName` accepts 1–128 characters from `[A-Za-z0-9_.$-]` only; any other byte,
including all non-ASCII, is rejected (`src/query/query_parser.cpp:506-519`).

### 9.3 `LIMIT`

```
LIMIT <count>
LIMIT <offset>,<count>
```

| Condition | Response |
|---|---|
| No argument | `ERROR 3008 LIMIT requires a number or offset,count` (`src/query/query_parser_clauses.cpp:188-190`) |
| Second `LIMIT` | `ERROR 3008 LIMIT specified more than once` (`src/query/query_parser_clauses.cpp:192-194`) |
| `LIMIT o,c` when `OFFSET` already given | `ERROR 3009 OFFSET specified more than once (LIMIT offset,count + OFFSET)` (`src/query/query_parser_clauses.cpp:201-204`) |
| Leading `-` on the offset | `ERROR 3008 LIMIT offset must be non-negative` (`src/query/query_parser_clauses.cpp:211-213`) |
| Leading `-` on the count | `ERROR 3008 LIMIT count must be positive` (`src/query/query_parser_clauses.cpp:214-216`) |
| Unparseable `o,c` | `ERROR 3008 Invalid LIMIT offset,count format: <token>` (`src/query/query_parser_clauses.cpp:218-228`) |
| `count` is 0 | `ERROR 3008 LIMIT count must be positive` (`src/query/query_parser_clauses.cpp:230-232`) |
| Leading `-` on a bare limit | `ERROR 3008 LIMIT must be positive` (`src/query/query_parser_clauses.cpp:241-243`) |
| Unparseable bare limit | `ERROR 3008 Invalid LIMIT value: <token>` (`src/query/query_parser_clauses.cpp:245-248`) |
| Bare limit is 0 | `ERROR 3008 LIMIT must be positive` (`src/query/query_parser_clauses.cpp:250-252`) |
| Value above 1000 | `ERROR 3000 LIMIT exceeds maximum of 1000` (post-clause check, `src/query/query_parser_commands.cpp:322-326`) |

Both offset and count are parsed as `uint32_t`.

### 9.4 `OFFSET <n>`

| Condition | Response |
|---|---|
| No argument | `ERROR 3009 OFFSET requires a number` (`src/query/query_parser_clauses.cpp:266-268`) |
| Second `OFFSET` (including one already set by `LIMIT o,c`) | `ERROR 3009 OFFSET specified more than once` (`src/query/query_parser_clauses.cpp:272-274`) |
| Leading `-` | `ERROR 3009 OFFSET must be non-negative` (`src/query/query_parser_clauses.cpp:277-279`) |
| Unparseable | `ERROR 3009 Invalid OFFSET value: <token>` (`src/query/query_parser_clauses.cpp:281-284`) |

There is no upper bound on `OFFSET` beyond `uint32_t` range.

### 9.5 `SORT`

```
SORT ASC|DESC                 -- primary-key ordering shorthand
SORT [BY] <column> [ASC|DESC] -- default DESC
```

- The optional `BY` is consumed if present; `SORT BY` with nothing after →
  `ERROR 3007 SORT BY requires a column name` (`src/query/query_parser_clauses.cpp:302-307`).
- No argument at all → `ERROR 3007 SORT requires a column name or ASC/DESC`
  (`src/query/query_parser_clauses.cpp:298-300`).
- Default direction is `DESC` (`src/query/query_parser.h:142`).
- `_score` selects BM25 relevance ordering (`src/query/query_parser.h:152`).
- A comma in the column name → `ERROR 3007 Multiple column sorting is not supported. Sort by a
  single column only.` (`src/query/query_parser_clauses.cpp:327-330`).
- Column failing `IsSafeColumnName` → `ERROR 3007 Invalid sort column`
  (`src/query/query_parser_clauses.cpp:332-334`).
- A non-clause-keyword token immediately after the sort spec → `ERROR 3007 Multiple column
  sorting is not supported. Hint: Sort by a single column only. Use application-level sorting
  for complex requirements.` (`src/query/query_parser_clauses.cpp:351-364`).
- Because `SORT` itself is a clause keyword, `SORT a ASC SORT b DESC` parses and the **last**
  clause wins (`src/query/query_parser_clauses.cpp:366`).

### 9.6 `HIGHLIGHT`

```
HIGHLIGHT [TAG <open> <close>] [SNIPPET_LEN <n>] [MAX_FRAGMENTS <n>]
```

Sub-options may appear in any order and any subset; parsing stops at the first token that is
not a recognized sub-option, leaving it for the outer clause loop
(`src/query/query_parser_clauses.cpp:377-436`).

| Option | Default | Bounds | Error |
|---|---|---|---|
| `TAG <open> <close>` | `<em>` / `</em>` | ≤ 256 bytes each | missing args → `ERROR 3000 HIGHLIGHT TAG requires open and close tag arguments`; oversize → `ERROR 3000 HIGHLIGHT TAG {open\|close} tag must be at most 256 bytes` (`src/query/query_parser_clauses.cpp:383-399`) |
| `SNIPPET_LEN <n>` | 100 code points | 1–10000 | missing → `ERROR 3000 HIGHLIGHT SNIPPET_LEN requires a number`; unparseable → `ERROR 3000 Invalid HIGHLIGHT SNIPPET_LEN value`; out of range → `ERROR 3000 HIGHLIGHT SNIPPET_LEN must be between 1 and 10000` (`src/query/query_parser_clauses.cpp:400-415`) |
| `MAX_FRAGMENTS <n>` | 3 | 1–100 | missing → `ERROR 3000 HIGHLIGHT MAX_FRAGMENTS requires a number`; unparseable → `ERROR 3000 Invalid HIGHLIGHT MAX_FRAGMENTS value`; out of range → `ERROR 3000 HIGHLIGHT MAX_FRAGMENTS must be between 1 and 100` (`src/query/query_parser_clauses.cpp:416-431`) |

Defaults are declared at `src/query/query_parser.h:158-163`.

### 9.7 `FUZZY`

```
FUZZY [1|2]
```

- The distance argument is consumed only if the next token is not a clause keyword
  (`src/query/query_parser_clauses.cpp:449`). `FUZZY` immediately followed by `LIMIT` therefore
  takes the default distance of 1 (`src/query/query_parser_clauses.cpp:446`).
- A non-clause-keyword argument that is unparseable or outside `1..2` →
  `ERROR 3000 FUZZY distance must be 1 or 2, got: <token>`
  (`src/query/query_parser_clauses.cpp:454-461`).

---

## 10. Administrative commands

### 10.1 `INFO`

Takes no arguments; any extra tokens are ignored (`src/query/query_parser.cpp:137-142`).

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

(`src/server/response_formatter.cpp:475-696`). The `# Replication` section is present only in
`USE_MYSQL` builds (`src/server/response_formatter.cpp:620-643`). Command counters are emitted
only when non-zero (`src/server/response_formatter.cpp:502-537`). `INFO` refreshes the
peak-memory statistic as a side effect (`src/server/handlers/admin_handler.cpp:52-55`).
If the table catalog is uninitialized → `ERROR 4008 Table catalog not initialized`
(`src/server/handlers/admin_handler.cpp:46-49`).

### 10.2 `DUMP`

`DUMP` without a subcommand → `ERROR 3000 DUMP requires a subcommand (SAVE, LOAD, VERIFY,
INFO, STATUS)` (`src/query/query_parser.cpp:154-157`). Unknown subcommand →
`ERROR 3000 Unknown DUMP subcommand: <token>` (`src/query/query_parser.cpp:213-215`).

| Subcommand | Argument handling | Success response |
|---|---|---|
| `DUMP SAVE [filepath]` | All trailing tokens are scanned; the last non-empty token not starting with `-` becomes the filepath. A token starting with `-` → `ERROR 3000 Unknown DUMP SAVE flag: <token>`. Empty tokens are skipped (`src/query/query_parser.cpp:163-179`) | Async (progress tracking available): `OK DUMP_STARTED <filepath>` (`src/server/handlers/dump_handler.cpp:234`). Synchronous fallback: `OK SAVED <filepath>` (`src/server/handlers/dump_handler.cpp:247`) |
| `DUMP LOAD <filepath>` | Uses `tokens[2]`; extras ignored. Missing → `ERROR 3000 DUMP LOAD requires a filepath` (`src/query/query_parser.cpp:184-189`) | `OK LOADED <filepath>` (`src/server/handlers/dump_handler.cpp:698`, `src/server/response_formatter.cpp:705-710`) |
| `DUMP VERIFY <filepath>` | Uses `tokens[2]`; extras ignored. Missing → `ERROR 3000 DUMP VERIFY requires a filepath` (`src/query/query_parser.cpp:194-199`) | `OK DUMP_VERIFIED <filepath>` (`src/server/handlers/dump_handler.cpp:744`) |
| `DUMP INFO <filepath>` | Uses `tokens[2]`; extras ignored. Missing → `ERROR 3000 DUMP INFO requires a filepath` (`src/query/query_parser.cpp:204-209`) | multi-line, see below |
| `DUMP STATUS` | No arguments (`src/query/query_parser.cpp:210-212`) | multi-line, see below |

`DUMP INFO` response (`src/server/handlers/dump_handler.cpp:787-798`):

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

`DUMP STATUS` response (`src/server/handlers/dump_handler.cpp:801-880`):

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
`IDLE` and no detail lines are emitted (`src/server/handlers/dump_handler.cpp:866-876`).

**Path resolution.** `DUMP SAVE/LOAD/VERIFY/INFO` filepaths are resolved against the dump
directory with traversal rejection; failures surface the resolver's own error code
(`src/server/handlers/dump_handler.cpp:47-51`).

**Mutual-exclusion errors** (all `ERROR 6030`):

| Command | Blocked by | Implementation |
|---|---|---|
| `DUMP SAVE` | `DUMP LOAD`, another `DUMP SAVE`, `OPTIMIZE`, any in-flight `SYNC`, any other long operation | `src/server/handlers/dump_handler.cpp:90-104`, `:132-177` |
| `DUMP LOAD` | `OPTIMIZE`, `DUMP SAVE`, another `DUMP LOAD`, any in-flight `SYNC` | `src/server/handlers/dump_handler.cpp:376-399`, `:438-458` |
| `OPTIMIZE` | `DUMP LOAD`, another `OPTIMIZE`, any in-flight `SYNC`, critical memory, insufficient memory | `src/server/handlers/debug_handler.cpp:46-106`, `:143-158` |
| `SYNC` | `OPTIMIZE`, `DUMP SAVE`, `DUMP LOAD` | `src/server/handlers/sync_handler.cpp:34-53` |
| `REPLICATION START` | MySQL reconnecting, replication paused for dump, in-flight `SYNC`, `DUMP LOAD`, `DUMP SAVE` | `src/server/handlers/replication_handler.cpp:49-86` |
| `REPLICATION STOP` | replication paused for dump | `src/server/handlers/replication_handler.cpp:25-30` |

`DUMP SAVE` additionally requires a non-empty GTID position →
`ERROR 6029 Cannot save dump without GTID position. Please run SYNC command first to establish
initial position.` (`src/server/handlers/dump_handler.cpp:81-87`).

### 10.3 `CONFIG`

`CONFIG` with no subcommand defaults to `CONFIG SHOW`
(`src/query/query_parser.cpp:256-259`). Unknown subcommand →
`ERROR 3000 Unknown CONFIG subcommand: <token> (expected HELP, SHOW, or VERIFY)`
(`src/query/query_parser.cpp:252-254`).

| Subcommand | Behavior | Success response |
|---|---|---|
| `CONFIG HELP [path]` | Optional path from `tokens[2]`; extras ignored (`src/query/query_parser.cpp:228-234`). Unknown path → `ERROR 8 Configuration path not found: <path>` (`src/server/handlers/admin_handler.cpp:111-113`) | `+OK\r\n<help body ending in \r\n>` (`src/server/handlers/admin_handler.cpp:106`, `:117`) |
| `CONFIG SHOW [path]` | Optional path from `tokens[2]`; extras ignored (`src/query/query_parser.cpp:235-241`) | `+OK\r\n<config body ending in \r\n>` (`src/server/handlers/admin_handler.cpp:141`) |
| `CONFIG VERIFY <filepath>` | Mandatory; missing → `ERROR 3000 CONFIG VERIFY requires a filepath` (`src/query/query_parser.cpp:246-251`) | `+OK\r\nConfiguration is valid\r\n  Tables: <n> (<names>)\r\n  MySQL: <host>:<port>\r\n` (`src/server/handlers/admin_handler.cpp:264-282`) |

`CONFIG VERIFY` path restrictions (`src/server/handlers/admin_handler.cpp:150-241`):

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
(`src/server/handlers/admin_handler.cpp:278-280`).

### 10.4 `REPLICATION`

`REPLICATION` with no subcommand → `ERROR 3000 REPLICATION requires a subcommand (STATUS,
STOP, START)` (`src/query/query_parser.cpp:266-268`). Unknown subcommand →
`ERROR 3000 Unknown REPLICATION subcommand: <token>` (`src/query/query_parser.cpp:281-283`).

`REPLICATION STATUS` response (`src/server/response_formatter.cpp:712-738`):

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
(`src/server/response_formatter.cpp:728-730`). In non-MySQL builds it is
`ERROR 4 MySQL support not compiled` (`src/server/response_formatter.cpp:736`).

| Command | Success | Notable errors |
|---|---|---|
| `REPLICATION STOP` | `OK REPLICATION_STOPPED` (`src/server/response_formatter.cpp:740-742`) | `ERROR 6029 Replication is not running`; `ERROR 4 Replication is not configured`; `ERROR 6030 Cannot stop replication while DUMP SAVE/LOAD is in progress. ...` (`src/server/handlers/replication_handler.cpp:25-42`) |
| `REPLICATION START` | `OK REPLICATION_STARTED` (`src/server/response_formatter.cpp:744-746`) | `ERROR 9 Replication is already running`; `ERROR 6029 Cannot start replication without GTID position. ...`; `ERROR 4 Replication is not configured`; start failure → `ERROR <code> Failed to start replication: <message>` (`src/server/handlers/replication_handler.cpp:88-125`) |

### 10.5 `SYNC`

```
SYNC <table>
SYNC STATUS
SYNC STOP [table]
```

`SYNC` with no argument → `ERROR 3000 SYNC requires a table name or STATUS/STOP subcommand`
(`src/query/query_parser.cpp:313-315`). Because `STATUS` and `STOP` are matched
case-insensitively before falling through to the table-name branch, a table literally named
`status` or `stop` cannot be synced by name (`src/query/query_parser.cpp:293-311`).

| Command | Success response |
|---|---|
| `SYNC <table>` | `OK SYNC STARTED table=<resolved>` (`src/server/sync_operation_manager.cpp:288`) |
| `SYNC STOP` (all) | `OK SYNC CANCELLATION REQUESTED count=<n>` (`src/server/sync_operation_manager.cpp:421-422`) |
| `SYNC STOP <table>` | `OK SYNC CANCELLATION REQUESTED table=<resolved>` (`src/server/sync_operation_manager.cpp:461`) |

`SYNC STATUS` response when at least one table has state
(`src/server/sync_operation_manager.cpp:291-358`):

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
(`src/server/sync_operation_manager.cpp:309-344`).

With no state at all:

```
OK SYNC_STATUS\r\nstatus=IDLE message="No sync operation performed"\r\nEND\r\n
```

(`src/server/sync_operation_manager.cpp:349-355`)

`SYNC STOP` with nothing running → `ERROR 8 No active SYNC operations to stop` (all) or
`ERROR 8 No active SYNC operation for table: <name>` (specific)
(`src/server/sync_operation_manager.cpp:384`, `:431`, `:438`).

### 10.6 `OPTIMIZE [table]`

Optional table from `tokens[1]`; extras ignored (`src/query/query_parser.cpp:320-330`). With no
table, all catalog tables are optimized; an empty catalog →
`ERROR 4007 No tables are available for optimization`
(`src/server/handlers/debug_handler.cpp:109-114`).

```
OK OPTIMIZED tables=<n> terms=<n> delta=<n> roaring=<n> memory=<human>\r\n
```

(`src/server/handlers/debug_handler.cpp:181-184`)

Failure to optimize a table → `ERROR 5 Failed to optimize table: <name>`
(`src/server/handlers/debug_handler.cpp:169-172`). Memory gates are described in §10.2.

### 10.7 `DEBUG ON` / `DEBUG OFF`

Missing mode → `ERROR 3000 DEBUG requires ON or OFF`; a mode other than `ON`/`OFF` →
`ERROR 3000 DEBUG requires ON or OFF, got: <token>` (`src/query/query_parser.cpp:334-350`).
Extras are ignored.

Responses: `OK DEBUG_ON` / `OK DEBUG_OFF`
(`src/server/handlers/debug_handler.cpp:31`, `:40`).

The debug flag is **per connection**, stored in `ConnectionContext::debug_mode`
(`src/server/server_types.h:145`), and enables the debug blocks described in §8.1, §8.2 and
§8.4.

### 10.8 `CACHE`

`CACHE` with no subcommand → `ERROR 3000 CACHE requires a subcommand (CLEAR, STATS, ENABLE,
DISABLE)` (`src/query/query_parser.cpp:357-359`). Unknown subcommand →
`ERROR 3000 Unknown CACHE subcommand: <token>` (`src/query/query_parser.cpp:379-381`).

| Command | Success response | Errors |
|---|---|---|
| `CACHE CLEAR` | `OK CACHE_CLEARED` (`src/server/handlers/cache_handler.cpp:47`) | `ERROR 8001 Cache not configured`; `ERROR 8001 Cache is disabled` (`src/server/handlers/cache_handler.cpp:37-42`) |
| `CACHE CLEAR <table>` | `OK CACHE_CLEARED table=<resolved>` (`src/server/handlers/cache_handler.cpp:61`) | `ERROR 4008 Table catalog is not available`; `ERROR 4007 Table not found or ambiguous: <name>` (`src/server/handlers/cache_handler.cpp:51-59`) |
| `CACHE STATS` | multi-line, see below | `ERROR 8001 Cache not configured` (`src/server/handlers/cache_handler.cpp:66-68`) |
| `CACHE ENABLE` | `OK CACHE_ENABLED` (`src/server/handlers/cache_handler.cpp:139`) | `ERROR 8001 Cache not configured`; `ERROR 8001 Cache cannot be enabled: server was started with cache disabled. Please restart the server with cache.enabled = true in configuration.` (`src/server/handlers/cache_handler.cpp:127-137`) |
| `CACHE DISABLE` | `OK CACHE_DISABLED` (`src/server/handlers/cache_handler.cpp:149`) | `ERROR 8001 Cache not configured` (`src/server/handlers/cache_handler.cpp:144-146`) |

`CACHE STATS` response (`src/server/handlers/cache_handler.cpp:72-122`):

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
and they may be mixed (`src/query/query_parser.cpp:394-451`). A trailing comma attached to a
value, or a standalone `,` token, separates assignments
(`src/query/query_parser.cpp:427-450`).

| Condition | Response |
|---|---|
| Compact form with empty name or value | `ERROR 3000 SET: Expected variable = value` (`src/query/query_parser.cpp:404-407`) |
| Spaced form with fewer than three remaining tokens | `ERROR 3000 SET: Expected variable = value` (`src/query/query_parser.cpp:411-414`) |
| Middle token is not `=` | `ERROR 3000 SET: Expected '=' after variable name` (`src/query/query_parser.cpp:420-423`) |
| A token that is neither a separator nor the start of a new assignment | `ERROR 3000 SET: Expected ',' or end of query` (`src/query/query_parser.cpp:443-450`) |
| No assignments parsed | `ERROR 3000 SET: No variable assignments found` (`src/query/query_parser.cpp:453-456`) |
| No variable by that name | `ERROR 1008 Failed to set variable '<name>': Unknown variable: <name>` (`src/config/runtime_variable_manager.cpp:223`, reached from `src/server/handlers/variable_handler.cpp:56`) |
| Variable exists but is startup-only | `ERROR 1009 Failed to set variable '<name>': Variable '<name>' is immutable (requires restart)` (`src/config/runtime_variable_manager.cpp:86`) |
| Value rejected for a mutable variable | `ERROR 1004 Failed to set variable '<name>': <reason>` (`src/config/runtime_variable_manager.cpp:295`) |
| `cache.enabled = true` when the cache subsystem cannot start | `ERROR 8001 Failed to set variable 'cache.enabled': Cache cannot be enabled` (`src/config/runtime_variable_manager.cpp:481`) |

**Success responses** (`src/server/handlers/variable_handler.cpp:80-91`):

```
+OK Variable '<name>' set to '<value>'\r\n        -- exactly one assignment
+OK <n> variables set\r\n                          -- two or more assignments
```

Multi-assignment `SET` is all-or-nothing: a failure rolls back every earlier assignment in the
same command before returning `ERROR <code> Failed to set variable '<name>': <message>`
(`src/server/handlers/variable_handler.cpp:55-78`). With no runtime variable manager →
`ERROR 6026 Runtime variable manager not initialized`
(`src/server/handlers/variable_handler.cpp:44-47`).

### 10.10 `SHOW VARIABLES`

```
SHOW VARIABLES
SHOW VARIABLES LIKE '<pattern>'
```

| Condition | Response |
|---|---|
| `SHOW` alone | `ERROR 3000 SHOW: Expected subcommand` (`src/query/query_parser.cpp:463-465`) |
| Subcommand other than `VARIABLES` | `ERROR 3000 SHOW: Unknown subcommand: <token>` (`src/query/query_parser.cpp:489-490`) |
| Third token is not `LIKE` | `ERROR 3000 SHOW VARIABLES: Expected LIKE 'pattern'` (`src/query/query_parser.cpp:476-479`) |
| Token count is not exactly 4 with `LIKE` | `ERROR 3000 SHOW VARIABLES LIKE: Expected exactly one pattern` (`src/query/query_parser.cpp:480-483`) |

The pattern supports `%` (any sequence) and `_` (single character), matched
case-insensitively (`src/server/handlers/variable_handler.cpp:186-218`).

**Empty result:**

```
+OK 0 rows\r\n
```

(`src/server/handlers/variable_handler.cpp:131-133`)

**Non-empty result** — a bare MySQL-style ASCII table with CRLF line endings and **no status
prefix** (`src/server/handlers/variable_handler.cpp:148-183`):

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
(`src/server/handlers/variable_handler.cpp:24-25`).

---

## 11. Value escaping in responses

### 11.1 Primary keys and `GET` string values

Applied to `OK RESULTS` primary keys, `OK DOC` primary keys, and `GET` string filter values
(`src/server/response_formatter.cpp:35-126`).

A value is emitted verbatim unless it is empty or contains ASCII whitespace, `"`, `\`, a
control character, or any Unicode whitespace — in which case it is wrapped in double quotes
with the following escapes (`src/server/response_formatter.cpp:97-125`):

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
fields (`src/server/response_formatter.cpp:150-161`, exposed as
`ResponseFormatter::SanitizeDelimitedField` at `src/server/response_formatter.cpp:1257-1259`).
Every CR, LF, TAB and control character is replaced by a single space. No quoting is applied.

---

## 12. Response frame catalogue

Three status shapes exist. None of the formatters append the transport terminator; that is
added once by the connection layer (`src/server/reactor_connection.cpp:791`).

| Shape | Producer | Used by |
|---|---|---|
| `OK <body>` | `ResponseFormatter::FormatStatus` (`src/server/response_formatter.cpp:1249-1255`) | `AUTHENTICATED`, `RESULTS`, `COUNT`, `DOC`, `FACET`, `INFO`, `SAVED`, `LOADED`, `DUMP_*`, `CACHE_*`, `DEBUG_*`, `OPTIMIZED`, `REPLICATION*`, `SYNC*` |
| `+OK` / `+OK <body>` | `ResponseFormatter::FormatOk` (`src/server/response_formatter.cpp:1237-1247`) | `CONFIG HELP`, `CONFIG SHOW`, `CONFIG VERIFY`, `SET`, empty `SHOW VARIABLES` |
| `ERROR <code> <message>` | `ResponseFormatter::FormatError` (`src/server/response_formatter.cpp:1212-1235`) | every failure path |

Response prefixes are centralized in `src/server/protocol_constants.h:24-54`.

Multi-line responses that terminate with a bare `END` line: `INFO`
(`src/server/response_formatter.cpp:694`), `REPLICATION STATUS`
(`src/server/response_formatter.cpp:732`), `DUMP INFO`
(`src/server/handlers/dump_handler.cpp:796`), `DUMP STATUS`
(`src/server/handlers/dump_handler.cpp:878`), `CACHE STATS`
(`src/server/handlers/cache_handler.cpp:121`).

### 12.1 Error frame format

```
ERROR <numeric-code> <message>
```

- `<numeric-code>` is the decimal `uint16` value of `mygram::utils::ErrorCode`
  (`src/server/response_formatter.cpp:1213-1215`).
- The message has every CR, LF, TAB and control character replaced by a space, so an error
  frame is always exactly one line (`src/server/response_formatter.cpp:1217-1223`).
- When the `Error` object carries context, the message becomes
  `<message> (context: <context>)` (`src/server/response_formatter.cpp:1227-1234`).
- Clients parse the frame with `protocol::ParseErrorFrame`, which treats the first token as a
  code only when it is a complete non-zero decimal `uint16`; otherwise the whole payload is
  treated as a legacy uncoded message (`src/server/protocol_constants.h:75-92`).

### 12.2 Error codes reachable on the TCP surface

| Code | Symbol | Typical TCP trigger |
|---|---|---|
| 2 | `kInvalidArgument` | `CONFIG VERIFY` path rejections |
| 4 | `kNotImplemented` | `HIGHLIGHT`/`SORT _score` without text storage; replication not configured; non-MySQL build |
| 5 | `kInternalError` | index/document store unavailable; handler/query-type mismatch; optimize failure |
| 7 | `kPermissionDenied` | failed `AUTH`; administrative command without `AUTH` |
| 8 | `kNotFound` | `GET` miss; `CONFIG HELP` unknown path; `SYNC STOP` with nothing running |
| 9 | `kAlreadyExists` | `REPLICATION START` while already running |
| 1000 | `kConfigFileNotFound` | `CONFIG VERIFY` missing file |
| 1002 etc. | config error codes | `CONFIG VERIFY` validation failure (code propagated from the loader) |
| 3000 | `kQuerySyntaxError` | every parser-level syntax/arity error; unknown query type |
| 3001 | `kQueryInvalidToken` | invalid UTF-8 in search text |
| 3005 | `kQueryTooLong` | `api.max_query_length` exceeded |
| 3006 | `kQueryInvalidFilter` | FILTER column/operator/value rejections |
| 3007 | `kQueryInvalidSort` | SORT rejections; `SORT _score` with BM25 disabled |
| 3008 | `kQueryInvalidLimit` | LIMIT rejections |
| 3009 | `kQueryInvalidOffset` | OFFSET rejections |
| 3010 | `kQueryExpressionParseError` | malformed boolean search expression |
| 4007 | `kTableNotFound` | unknown/ambiguous table; bare name under multi-database config; empty catalog on `OPTIMIZE` |
| 4008 | `kCatalogNotInitialized` | catalog missing on `INFO`, `OPTIMIZE`, `CACHE CLEAR <table>` |
| 5005 | `kStorageVersionMismatch` | `DUMP LOAD` config / MySQL-source / server-UUID mismatch |
| 5011 / 5012 | `kStorageDumpReadError` / `kStorageDumpWriteError` | dump read/write failure |
| 6007 | `kNetworkInvalidRequest` | request frame larger than the 1 MiB read cap |
| 6026 | `kServerInitMissingDependency` | configuration or variable manager unavailable |
| 6028 | `kServerLoading` | request during `DUMP LOAD` |
| 6029 | `kServerNotReady` | table synchronizing; missing GTID; replication not running |
| 6030 | `kServerBusy` | rate limit; frame-queue overflow; thread-pool saturation; long-operation conflicts; memory gates |
| 8001 | `kCacheDisabled` | `CACHE` commands with the cache unconfigured or disabled |

Numeric values are defined in `src/utils/error.h:37-181`.

---

## 13. Known divergences

The following are places where two code paths in the current tree disagree about the same
thing. They are recorded as facts; no remedy is proposed here.

1. **`SHOW VARIABLES` has no status prefix when it returns rows.** The empty result is
   `+OK 0 rows` (`src/server/handlers/variable_handler.cpp:131-133`), but a non-empty result is
   returned as a bare ASCII table beginning with `+---`
   (`src/server/handlers/variable_handler.cpp:148-183`). Every other command on this surface
   begins with `OK `, `+OK` or `ERROR ` (`src/server/response_formatter.cpp:1212-1255`), so a
   client keying on those prefixes cannot classify a non-empty `SHOW VARIABLES` reply.

2. **`SEARCH` has two different frame terminations depending on `HIGHLIGHT`.**
   `FormatSearchResponse` emits no trailing CRLF (`src/server/response_formatter.cpp:280-332`),
   while `FormatSearchResponseWithHighlights` appends one
   (`src/server/response_formatter.cpp:375`) and `FormatFacetResponse` does likewise
   (`src/server/response_formatter.cpp:430`). After the transport terminator
   (`src/server/reactor_connection.cpp:791`) the same command therefore ends in either `\r\n`
   or `\r\n\r\n`.

3. **Dump filepaths are sanitized on some responses and not others.** `DUMP VERIFY`
   (`src/server/handlers/dump_handler.cpp:744`) and `DUMP INFO`
   (`src/server/handlers/dump_handler.cpp:788`) pass the path through
   `SanitizeDelimitedField`, while `DUMP SAVE` (`src/server/handlers/dump_handler.cpp:234`,
   `:247`) and `DUMP LOAD` (`src/server/handlers/dump_handler.cpp:698` →
   `src/server/response_formatter.cpp:705-710`) emit it raw.

4. **`SYNC STATUS` terminates its body differently from every other multi-line response.**
   `GetSyncStatus` emits `END\r\n` (`src/server/sync_operation_manager.cpp:354`,
   `:357`), whereas `INFO` (`src/server/response_formatter.cpp:694`), `REPLICATION STATUS`
   (`src/server/response_formatter.cpp:732`), `DUMP INFO`
   (`src/server/handlers/dump_handler.cpp:796`), `DUMP STATUS`
   (`src/server/handlers/dump_handler.cpp:878`) and `CACHE STATS`
   (`src/server/handlers/cache_handler.cpp:121`) all emit a bare `END`.

5. **`QueryType::SAVE` and `QueryType::LOAD` are unreachable but still counted.** The enum
   values exist (`src/query/query_parser.h:52-53`) and `ServerStats::IncrementCommand` has
   dedicated counters for them (`src/server/server_stats.cpp:37-42`), which `INFO`
   (`src/server/response_formatter.cpp:514-519`) and the Prometheus exposition
   (`src/server/response_formatter.cpp:794-799`) can emit — yet `Parse` rejects both keywords
   outright (`src/query/query_parser.cpp:143-150`), so `cmd_save` and `cmd_load` can never be
   non-zero. The same header's class documentation still lists `SAVE [filename]` and
   `LOAD [filename]` as supported commands (`src/query/query_parser.h:254-255`).

6. **The documented and configured defaults for `api.max_query_length` differ.**
   `config::defaults::kDefaultQueryLengthLimit` is `128` (`src/config/config.h:64`), while
   `ReactorConnection`'s read-cap rationale states the default is "~64 KiB" when justifying its
   1 MiB read buffer (`src/server/reactor_connection.h:130-133`).

7. **`INFO` and `CONFIG SHOW` are classified differently despite comparable disclosure.**
   `INFO` is not in the administrative set (`src/server/request_dispatcher.cpp:22-51`) and is
   therefore answerable before `AUTH`, yet it discloses the table list, memory figures, the
   current replication GTID and the cache configuration snapshot
   (`src/server/response_formatter.cpp:601-688`). `CONFIG SHOW` is administrative and requires
   `AUTH` (`src/server/request_dispatcher.cpp:35-36`).

8. **`CACHE CLEAR` requires the cache to be enabled; the sibling subcommands do not.**
   `HandleClear` rejects a disabled cache with `ERROR 8001 Cache is disabled`
   (`src/server/handlers/cache_handler.cpp:40-42`), while `CACHE STATS`
   (`src/server/handlers/cache_handler.cpp:64-70`) and `CACHE DISABLE`
   (`src/server/handlers/cache_handler.cpp:142-149`) proceed and only require the cache manager
   to exist.

9. **`ResponseFormatter::FormatSaveResponse` is dead code that duplicates a live format.** It
   produces `OK SAVED <path>` (`src/server/response_formatter.cpp:698-703`) but has no caller;
   the synchronous `DUMP SAVE` path builds the identical frame through `FormatStatus`
   (`src/server/handlers/dump_handler.cpp:247`). Its sibling `FormatLoadResponse`
   (`src/server/response_formatter.cpp:705-710`) is the one actually used by `DUMP LOAD`.
