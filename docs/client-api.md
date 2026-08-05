# Client API parity

The C and C++ clients use the same wire protocol. The table below is the public parity contract for features that need explicit API support.

| Capability | C++ API | C API |
| --- | --- | --- |
| Literal search | `Search` | `mygramclient_search` |
| Grouped/boolean expression | `SearchRaw` | `mygramclient_search_raw` |
| Grouped/boolean expression with highlights | `SearchRawWithHighlights` | `mygramclient_search_raw_with_highlights` |
| Lossless web-expression conversion | `ConvertSearchExpression` | `mygramclient_convert_search_expression` |
| Expression failure diagnostics | typed `Error` | `mygramclient_parse_search_expression_ex` / `mygramclient_convert_search_expression_ex` |
| Typed literal/boolean mode, comparison filters, fuzzy, highlight | `Search(..., SearchOptions)` | `mygramclient_search_with_options` |
| Paginated facets and total distinct value count | `Facet(..., offset)` / `FacetResponse::total_count` | `mygramclient_facet_paged` / `MygramFacetResult_C::total_count` |
| TCP host, port, request timeout, receive buffer | `ClientConfig` | `MygramClientConfig_C` or `MygramClientConfigV2_C` |
| Unix domain socket | `ClientConfig::unix_socket_path` | `MygramClientConfigV2_C::unix_socket_path` |
| Connection timeout | `ClientConfig::connect_timeout_ms` | `MygramClientConfigV2_C::connect_timeout_ms` |
| Asynchronous DUMP SAVE completion timeout | `ClientConfig::dump_save_timeout_ms` | `MygramClientConfigV2_C::dump_save_timeout_ms` |
| DUMP LOAD / VERIFY timeout | `ClientConfig::dump_load_timeout_ms` / `dump_verify_timeout_ms` | Matching V2 fields |
| OPTIMIZE timeout | `ClientConfig::optimize_timeout_ms` | `MygramClientConfigV2_C::optimize_timeout_ms` |
| Maximum response frame size | `ClientConfig::max_response_bytes` | `MygramClientConfigV2_C::max_response_bytes` |

Use `Search` for literal user text. It quotes standalone reserved words such as `AND`, `FILTER`, and `LIMIT`. Set `SearchOptions::query_mode` to `QueryMode::kBoolean` when an intentional expression such as `alpha AND (xqz OR jkv)` must be combined with typed filters, sorting, fuzzy search, or highlights. `SearchRaw` remains the compact expression-only API.

## Search semantics across surfaces

Literal text is the default for typed clients and HTTP. Boolean syntax is always
an explicit choice, so `alpha AND beta` does not change meaning when an
application moves between surfaces.

| Surface | Literal text | Boolean expression | Filters / fuzzy / highlight |
| --- | --- | --- | --- |
| TCP | quote the search token | unquoted expression | protocol clauses |
| HTTP | `"mode": "literal"` (default) | `"mode": "boolean"` | JSON fields |
| C++ | `Search` | `SearchRaw` | `SearchOptions` |
| C | `mygramclient_search` | `mygramclient_search_raw` | `MygramSearchOptions_C` |

For the C options API, zero-initialize `MygramSearchOptions_C`, set
`struct_size`, and use `limit == 0` for the server default. The zero default
`MYGRAM_QUERY_LITERAL` preserves literal search; select `MYGRAM_QUERY_BOOLEAN`
for a boolean expression. Comparison filters support `=`, `!=`, `>`, `>=`,
`<`, and `<=`; fuzzy distance is 1 or 2.

For facet navigation, C++ callers pass `offset` as the final `Facet` argument.
C callers use `mygramclient_facet_paged`, or
`mygramclient_facet_advanced_paged` when AND/NOT/FILTER clauses are required.
`facets.size()` / `count` is the number of values in the returned page, while
`total_count` is the distinct value count before OFFSET and LIMIT. The original
C facet functions remain available and use offset zero.

The standalone C expression helpers do not take a client handle. Use their
`_ex` variants when a concrete parse diagnostic is required; on failure the
`diagnostic` output is allocated and must be released with
`mygramclient_free_string`. The original functions remain as ABI-compatible
diagnostic-free wrappers.

New C programs should use the size/versioned configuration:

```c
MygramClientConfigV2_C config = {0};
config.struct_size = sizeof(config);
config.version = MYGRAMCLIENT_CONFIG_V2_VERSION;
config.host = "127.0.0.1";
config.port = 11016;
config.timeout_ms = 5000;
config.connect_timeout_ms = 3000;
config.dump_save_timeout_ms = 600000;
config.dump_load_timeout_ms = 600000;
config.dump_verify_timeout_ms = 600000;
config.optimize_timeout_ms = 600000;
config.max_response_bytes = 64 * 1024 * 1024;

MygramClient_C *client = mygramclient_create_v2(&config);
```

Set `unix_socket_path` to use a Unix domain socket; it takes precedence over the TCP host and port. `timeout_ms` is a total ordinary-command deadline, not a timeout reset by each partial receive. `connect_timeout_ms` separates connection establishment from request execution. DUMP SAVE, DUMP LOAD, DUMP VERIFY, and OPTIMIZE have operation-specific deadlines because they can legitimately exceed an ordinary request timeout. `max_response_bytes` bounds one response frame. A zero V2 field uses the C++ client default; a zero operation-specific C++ field falls back to `timeout_ms`. The legacy `MygramClientConfig_C` and `mygramclient_create` remain ABI-compatible and support the original TCP fields.

`timeout_ms == 0` and `recv_buffer_size == 0` select the defaults on both the C
and C++ APIs. Receive buffers larger than 16 MiB are clamped to 16 MiB.

A C handle may be shared between threads. Connection lifecycle calls and
commands are serialized on the handle; disconnect waits for an in-flight
command to finish and does not cancel it. Do not call `mygramclient_destroy()`
while another thread is using the handle. Its error state is synchronized, and
`mygramclient_get_last_error()` returns a thread-local snapshot valid until the
same thread calls that getter again. Every writable out-parameter is initialized
to `NULL` (or zero for numeric outputs) before an operation that can fail.

Future V2 fields may be appended. A caller must initialize the structure to zero and set `struct_size` to the size it knows. The library ignores fields beyond that size and rejects unknown version values or a structure shorter than the original V2 prefix.

Complete C and C++ connect/search/result-iteration examples are installed under
`share/doc/mygramdb/examples/client`. Their `CMakeLists.txt` builds directly
against the installed `MygramDBClient` package.
