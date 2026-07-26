# Client API parity

The C and C++ clients use the same wire protocol. The table below is the public parity contract for features that need explicit API support.

| Capability | C++ API | C API |
| --- | --- | --- |
| Literal search | `Search` | `mygramclient_search` |
| Grouped/boolean expression | `SearchRaw` | `mygramclient_search_raw` |
| Grouped/boolean expression with highlights | `SearchRawWithHighlights` | `mygramclient_search_raw_with_highlights` |
| Lossless web-expression conversion | `ConvertSearchExpression` | `mygramclient_convert_search_expression` |
| Typed comparison filters, fuzzy search, highlight options | `Search(..., SearchOptions)` | `mygramclient_search_with_options` |
| TCP host, port, request timeout, receive buffer | `ClientConfig` | `MygramClientConfig_C` or `MygramClientConfigV2_C` |
| Unix domain socket | `ClientConfig::unix_socket_path` | `MygramClientConfigV2_C::unix_socket_path` |
| Asynchronous DUMP SAVE completion timeout | `ClientConfig::dump_save_timeout_ms` | `MygramClientConfigV2_C::dump_save_timeout_ms` |

Use `Search` for literal user text. It quotes standalone reserved words such as `AND`, `FILTER`, and `LIMIT`. Use `SearchRaw` only when the input is an intentional protocol expression such as `alpha AND (xqz OR jkv)`.

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
`struct_size`, and use `limit == 0` for the server default. Comparison filters
support `=`, `!=`, `>`, `>=`, `<`, and `<=`; fuzzy distance is 1 or 2.

New C programs should use the size/versioned configuration:

```c
MygramClientConfigV2_C config = {0};
config.struct_size = sizeof(config);
config.version = MYGRAMCLIENT_CONFIG_V2_VERSION;
config.host = "127.0.0.1";
config.port = 11016;
config.timeout_ms = 5000;
config.dump_save_timeout_ms = 600000;

MygramClient_C *client = mygramclient_create_v2(&config);
```

Set `unix_socket_path` to use a Unix domain socket; it takes precedence over the TCP host and port. `dump_save_timeout_ms` controls the total polling deadline for an asynchronous DUMP SAVE. A zero field uses the C++ client default. The legacy `MygramClientConfig_C` and `mygramclient_create` remain ABI-compatible and support the original TCP fields.

`timeout_ms == 0` and `recv_buffer_size == 0` select the defaults on both the C
and C++ APIs. Receive buffers larger than 16 MiB are clamped to 16 MiB.

A C handle may be shared between threads. Its error state is synchronized, and
`mygramclient_get_last_error()` returns a thread-local snapshot valid until the
same thread calls that getter again. Every writable out-parameter is initialized
to `NULL` (or zero for numeric outputs) before an operation that can fail.

Future V2 fields may be appended. A caller must initialize the structure to zero and set `struct_size` to the size it knows. The library ignores fields beyond that size and rejects unknown version values or a structure shorter than the original V2 prefix.
