# Configuration Keys

This file is normative for MygramDB's configuration surface. It describes what the code does today: the keys the parser accepts, the exact defaults it produces, the ranges it enforces, and which keys can be changed while the server is running. Changing a default, widening or narrowing a range, or moving a key between the startup-only and runtime-mutable classes is a surface change.

Configuration is loaded from a YAML (`.yaml`, `.yml`) or JSON (`.json`) file. Format is chosen by extension; an unrecognized extension is tried as YAML and then as JSON (`src/config/config_loader.cpp`). YAML is converted to JSON before anything else, so YAML and JSON accept exactly the same key set (`src/config/config.cpp`). Every configuration is validated against a JSON Schema that is compiled into the binary from `src/config/config-schema.json` (`src/config/CMakeLists.txt`); this happens for every load and cannot be turned off.

The root object requires `mysql` and `tables`, and rejects unknown top-level keys. Every object in the schema sets `additionalProperties: false`, so a misspelled key anywhere is a hard startup failure rather than a silently ignored value.

## Reading the tables

| Column | Meaning |
|---|---|
| Key | Fully qualified dotted path. `tables[]` denotes a per-table array element. |
| Type | Type as enforced by the schema. |
| Valid range / allowed values | Schema constraint, plus any additional constraint the parser applies. |
| Default | The value the code produces when the key is absent. |
| Mutability | `startup-only` or `runtime-mutable`. `unread` marks a key that is parsed and stored but never consumed by any component (see [Unread keys](#unread-keys)). |
| Validated where | Where the value is constrained. `src/config/config-schema.json` alone means schema-only, enforced by `src/config/config_validator.cpp`. |

Runtime mutation is available through the variable surface. The mutable set is an explicit allowlist at `src/config/runtime_variable_manager.cpp`; every other key is readable but rejects mutation with error 1009 `kConfigVariableNotMutable`, "immutable (requires restart)". A name that is not a configuration key at all is rejected with 1008 `kConfigUnknownVariable` (`src/config/runtime_variable_manager.cpp`), and a rejected value for a mutable key with 1004 `kConfigInvalidValue`.

## `mysql`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `mysql.host` | string | any | `127.0.0.1` | startup-only | `src/config/config-schema.json` |
| `mysql.port` | integer | 1–65535 | `3306` | startup-only | `src/config/config-schema.json`, env path `src/config/config.cpp` |
| `mysql.user` | string | minLength 1; must be non-empty after env override | (none — required) | startup-only | `src/config/config.cpp` |
| `mysql.password` | string | any | `""` | startup-only | `src/config/config-schema.json` |
| `mysql.database` | string | minLength 1, no NUL; must be a quotable SQL identifier | (none — required) | startup-only | `src/config/config.cpp`, `src/utils/sql_utils.cpp` |
| `mysql.use_gtid` | boolean | `true` only | `true` | startup-only · unread | `src/config/config.cpp` |
| `mysql.binlog_format` | string | `ROW` only | `ROW` | startup-only · unread | `src/config/config.cpp` |
| `mysql.binlog_row_image` | string | `FULL` only | `FULL` | startup-only · unread | `src/config/config.cpp` |
| `mysql.connect_timeout_ms` | integer | 100–60000 | `3000` | startup-only | `src/config/config-schema.json` |
| `mysql.read_timeout_ms` | integer | 1000–86400000 | `3600000` | startup-only | `src/config/config-schema.json` |
| `mysql.write_timeout_ms` | integer | 1000–86400000 | `3600000` | startup-only | `src/config/config-schema.json` |
| `mysql.session_timeout_sec` | integer | 60–86400 | `3600` | startup-only | `src/config/config-schema.json` |
| `mysql.ssl_enable` | boolean | — | `false` | startup-only | `src/config/config-schema.json` |
| `mysql.ssl_ca` | string | no `..` component, no NUL; required when `ssl_enable` and `ssl_verify_server_cert` are both true | `""` | startup-only | `src/config/config.cpp` |
| `mysql.ssl_cert` | string | no `..` component, no NUL | `""` | startup-only | `src/config/config.cpp` |
| `mysql.ssl_key` | string | no `..` component, no NUL | `""` | startup-only | `src/config/config.cpp` |
| `mysql.ssl_verify_server_cert` | boolean | — | `true` | startup-only | `src/config/config-schema.json` |
| `mysql.ignored_ddl_prefixes` | string[] | each entry non-empty, no `;`, first word one of `ALTER`, `ANALYZE`, `CHECK`, `CREATE`, `DROP`, `OPTIMIZE`, `RENAME`, `REPAIR`, `TRUNCATE` | `[]` | startup-only | `src/config/config.cpp` |
| `mysql.datetime_timezone` | string | `^[+-]([01][0-9]\|2[0-3]):[0-5][0-9]$` | `+00:00` | startup-only | `src/config/config-schema.json`, parsed at `src/config/config.cpp` |

`mysql.binlog_format` and `mysql.binlog_row_image` are assertions about the server, not settings sent to it. The parser rejects any value other than `ROW`/`FULL`, and the actual enforcement queries the live server (`src/mysql/connection_validator.cpp`). The configured values themselves are never used to drive behavior.

## `tables[]`

Each element requires `name` and `text_source`. Table identity is `(database, name)`; a duplicate identity is a hard failure (`src/config/config.cpp`).

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `tables[].name` | string | minLength 1, no NUL, quotable SQL identifier | (none — required) | startup-only | `src/config/config.cpp` |
| `tables[].database` | string | minLength 1, no NUL, quotable SQL identifier | value of `mysql.database` | startup-only | `src/config/config.cpp` |
| `tables[].primary_key` | string | minLength 1, no NUL, quotable SQL identifier | `id` | startup-only | `src/config/config.cpp` |
| `tables[].text_source.column` | string | minLength 1, no NUL, quotable SQL identifier; exactly one of `column`/`concat` required | `""` | startup-only | `src/config/config.cpp`, `src/config/config-schema.json` |
| `tables[].text_source.concat` | string[] | minItems 2, each minLength 1, no NUL, quotable SQL identifier | `[]` | startup-only | `src/config/config.cpp`, `src/config/config-schema.json` |
| `tables[].text_source.delimiter` | string | any | `" "` (single space) | startup-only | `src/config/config-schema.json` |
| `tables[].required_filters[].name` | string | minLength 1, no NUL, quotable SQL identifier | (none — required) | startup-only | `src/config/config.cpp` |
| `tables[].required_filters[].type` | string | `tinyint`, `tinyint_unsigned`, `smallint`, `smallint_unsigned`, `mediumint`, `mediumint_unsigned`, `int`, `int_unsigned`, `bigint`, `bigint_unsigned`, `double`, `string`, `varchar`, `text`, `datetime`, `date`, `timestamp`, `time`, `boolean` | (none — required) | startup-only | `src/config/config.cpp`, `src/config/config-schema.json` |
| `tables[].required_filters[].op` | string | `=`, `!=`, `<`, `>`, `<=`, `>=`, `IS NULL`, `IS NOT NULL`; `boolean` type restricted to `=`, `!=`, `IS NULL`, `IS NOT NULL` | (none — required) | startup-only | `src/config/config.cpp` |
| `tables[].required_filters[].value` | string \| number \| boolean | must be absent for `IS NULL`/`IS NOT NULL`, present otherwise; empty string permitted only for `string`/`varchar`/`text` | (none) | startup-only | `src/config/config.cpp` |
| `tables[].required_filters[].bitmap_index` | boolean | — | `false` | startup-only · unread | `src/config/config-schema.json` |
| `tables[].filters[].name` | string | minLength 1, no NUL, quotable SQL identifier | (none — required) | startup-only | `src/config/config.cpp` |
| `tables[].filters[].type` | string | same enum as `required_filters[].type` | (none — required) | startup-only | `src/config/config.cpp` |
| `tables[].filters[].dict_compress` | boolean | — | `false` | startup-only · unread | `src/config/config-schema.json` |
| `tables[].filters[].bitmap_index` | boolean | — | `false` | startup-only · unread | `src/config/config-schema.json` |
| `tables[].filters[].bucket` | string | `minute`, `hour`, `day` | `""` | startup-only · unread | `src/config/config-schema.json` |
| `tables[].ngram_size` | integer | 1–10 | `2`, or `index.ngram_size` when that legacy key is present | startup-only | `src/config/config-schema.json`, inheritance at `src/config/config.cpp` |
| `tables[].kanji_ngram_size` | integer | 0–10; `0` means inherit the effective `ngram_size` | effective `ngram_size` | startup-only | `src/config/config.cpp` |
| `tables[].cross_boundary_ngrams` | boolean | — | `true` | startup-only | `src/config/config-schema.json` |
| `tables[].posting.block_size` | integer | 8–1024 | `128` | startup-only · unread | `src/config/config-schema.json` |
| `tables[].posting.freq_bits` | integer | `0`, `4`, `8` | `0` | startup-only · unread | `src/config/config-schema.json` |
| `tables[].posting.use_roaring` | string | `auto`, `always`, `never` | `auto` | startup-only · unread | `src/config/config-schema.json` |
| `tables[].synonyms.enable` | boolean | — | `false` | startup-only | `src/config/config-schema.json` |
| `tables[].synonyms.file` | string | minLength 1, no `..` component, no NUL; required when `enable` is true | `""` | startup-only | `src/config/config.cpp`, `src/config/config-schema.json` |

A column named in both `required_filters` and `filters` must carry the same `type` in both places; conflicting types are a hard failure (`src/config/config.cpp`). `tables[].where_clause` is not accepted: the schema rejects it as an unknown property, and the parser carries an explicit rejection with a migration message (`src/config/config.cpp`).

## `index` (legacy)

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `index.ngram_size` | integer | 1–10 | `2` | startup-only | `src/config/config-schema.json`, applied at `src/config/config.cpp` |

This value is applied only to tables that do not carry their own `ngram_size` (`src/config/config.cpp`). It is not stored on `Config` and does not appear in the variable surface.

## `build`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `build.mode` | string | `select_snapshot` only | `select_snapshot` | startup-only · unread | `src/config/config-schema.json` |
| `build.batch_size` | integer | 100–100000 | `5000` | startup-only | `src/config/config-schema.json`, consumed at `src/loader/initial_loader.cpp` |
| `build.parallelism` | integer | 1–64 | `2` | startup-only · unread | `src/config/config-schema.json` |
| `build.throttle_ms` | integer | 0–10000 | `0` | startup-only · unread | `src/config/config-schema.json` |

## `replication`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `replication.enable` | boolean | — | `true` | startup-only | `src/config/config-schema.json` |
| `replication.auto_initial_snapshot` | boolean | — | `false` | startup-only | `src/config/config-schema.json`, cross-check `src/config/config.cpp` |
| `replication.server_id` | integer | 1–4294967295; must be non-zero when `enable` is true | `0` | startup-only | `src/config/config.cpp`, `src/config/config-schema.json` |
| `replication.start_from` | string | `snapshot`, `latest`, or `gtid=<UUID:txn>` (the GTID form must contain a `:`) | `snapshot` | startup-only | `src/config/config.cpp` |
| `replication.queue_size` | integer | 100–1000000 | `10000` | startup-only | `src/config/config-schema.json` |
| `replication.reconnect_backoff_min_ms` | integer | 100–60000 | `500` | startup-only · unread | `src/config/config-schema.json` |
| `replication.reconnect_backoff_max_ms` | integer | 1000–600000 | `10000` | startup-only · unread | `src/config/config-schema.json` |

`auto_initial_snapshot: true` requires `start_from: snapshot`; any other start position is a hard failure (`src/config/config.cpp`).

## `memory`

Every key in this table is constrained by `src/config/config-schema.json`; a cell in the last column is filled in only where another file is also involved.

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `memory.hard_limit_mb` | integer | ≥ 256 | `8192` | startup-only · unread | |
| `memory.soft_target_mb` | integer | ≥ 128 | `4096` | startup-only · unread | |
| `memory.arena_chunk_mb` | integer | 1–1024 | `64` | startup-only · unread | |
| `memory.roaring_threshold` | number | 0.0–1.0 | `0.18` | startup-only | `src/config/config-schema.json`, consumed at `src/app/server_orchestrator.cpp` |
| `memory.minute_epoch` | boolean | — | `true` | startup-only · unread | |
| `memory.normalize.nfkc` | boolean | — | `true` | startup-only | |
| `memory.normalize.width` | string | `keep`, `narrow`, `wide` | `narrow` | startup-only | |
| `memory.normalize.lower` | boolean | — | `false` | startup-only | |
| `memory.verify_text` | string | `off`, `ascii`, `all` | `off` | startup-only | `src/config/config-schema.json`, consumed at `src/server/search_pipeline.cpp` |

## `dump`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `dump.dir` | string | no `..` component, no NUL; also rejected before and after normalization at startup | `/var/lib/mygramdb/dumps` | startup-only | `src/config/config.cpp`, `src/app/application.cpp` |
| `dump.default_filename` | string | non-empty basename (no `/`, no `\`), no `..` component | `mygramdb.dmp` | startup-only | `src/config/config.cpp` |
| `dump.load_on_startup` | boolean | — | `false` | startup-only | `src/config/config-schema.json`, consumed at `src/app/server_orchestrator.cpp` |
| `dump.interval_sec` | integer | 0–86400; `0` disables periodic dumps | `0` | startup-only | `src/config/config-schema.json`, consumed at `src/server/snapshot_scheduler.cpp` |
| `dump.retain` | integer | 1–100 | `3` | startup-only · unread | `src/config/config-schema.json` |
| `dump.restore_memory_budget_mb` | integer | 1–1048576 | `4096` | startup-only | `src/config/config-schema.json`, consumed at `src/app/server_orchestrator.cpp` |
| `dump.restore_max_section_mb` | integer | 1–1048576 | `2048` | startup-only | `src/config/config-schema.json`, consumed at `src/app/server_orchestrator.cpp` |

## `api.tcp`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `api.tcp.bind` | string | valid IP literal (with optional IPv6 scope) or hostname; no `/`, no `..`, no whitespace, no NUL | `127.0.0.1` | startup-only | `src/config/config.cpp`, `src/config/config_validator.cpp` |
| `api.tcp.port` | integer | 1–65535 | `11016` | startup-only | `src/config/config-schema.json` |
| `api.tcp.max_connections` | integer | 1–1000000 | `10000` | startup-only | `src/config/config-schema.json`, consumed at `src/server/connection_acceptor.cpp` |
| `api.tcp.worker_threads` | integer | 0–16384; `0` = auto | `0` | startup-only | `src/config/config-schema.json`, auto-sizing at `src/server/thread_pool.cpp` |
| `api.tcp.recv_timeout_sec` | integer | 0–86400; `0` disables the first-frame deadline | `60` | startup-only | `src/config/config-schema.json` |
| `api.tcp.idle_timeout_sec` | integer | 0–86400; `0` disables idle reaping | `300` | startup-only | `src/config/config-schema.json` |
| `api.tcp.reaper_interval_sec` | integer | 1–3600 | `5` | startup-only | `src/config/config-schema.json` |
| `api.tcp.thread_pool_queue_size` | integer | 0–1000000; `0` = unbounded | `1000` | startup-only | `src/config/config-schema.json` |
| `api.tcp.keepalive.enabled` | boolean | — | `true` | startup-only | `src/config/config-schema.json` |
| `api.tcp.keepalive.idle_sec` | integer | 1–86400 | `60` | startup-only | `src/config/config-schema.json` |
| `api.tcp.keepalive.interval_sec` | integer | 1–3600 | `20` | startup-only | `src/config/config-schema.json` |
| `api.tcp.keepalive.probe_count` | integer | 1–32 | `3` | startup-only | `src/config/config-schema.json` |
| `api.tcp.max_write_queue_bytes` | integer | 4096–1073741824 | `16777216` (16 MiB) | startup-only | `src/config/config-schema.json` |
| `api.tcp.max_total_buffered_bytes` | integer | 1048576–68719476736 | `268435456` (256 MiB) | startup-only | `src/config/config-schema.json` |
| `api.tcp.max_pending_frames` | integer | 4–1000000 | `1024` | startup-only | `src/config/config-schema.json` |
| `api.tcp.max_pending_frame_bytes` | integer | 4096–1073741824; must be ≥ min(`api.max_query_length`, 1 MiB), or 1 MiB when `max_query_length` is 0 | `4194304` (4 MiB) | startup-only | `src/config/config.cpp` |

## `api.http`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `api.http.enable` | boolean | — | `false` | startup-only | `src/config/config-schema.json` |
| `api.http.bind` | string | same rule as `api.tcp.bind` | `127.0.0.1` | startup-only | `src/config/config.cpp`, `src/config/config_validator.cpp` |
| `api.http.port` | integer | 1–65535 | `8080` | startup-only | `src/config/config-schema.json` |
| `api.http.max_connections` | integer | 1–1000000 | `10000` | startup-only | `src/config/config-schema.json`, consumed at `src/server/http_server.cpp` |
| `api.http.trusted_proxies` | string[] | every entry must be a numeric IPv4 or IPv6 address | `[]` | startup-only | `src/config/config.cpp` |
| `api.http.enable_cors` | boolean | — | `false` | startup-only | `src/config/config-schema.json` |
| `api.http.cors_allow_origin` | string | must be non-empty when `api.http.enable` and `api.http.enable_cors` are both true | `""` | startup-only | `src/config/config.cpp` |
| `api.http.read_timeout_sec` | integer | 1–3600 | `5` | startup-only | `src/config/config-schema.json` |
| `api.http.write_timeout_sec` | integer | 1–3600 | `5` | startup-only | `src/config/config-schema.json` |
| `api.http.max_body_bytes` | integer | 0–1073741824; `0` disables the limit (`set_payload_max_length(SIZE_MAX)`, which is what lifting it takes — omitting the call falls back to cpp-httplib's compiled-in 100 MB ceiling) | `16777216` (16 MiB) | startup-only | `src/config/config-schema.json`, applied at `src/server/http_server.cpp` |

## `api` (top-level)

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `api.default_limit` | integer | 5–1000 at load; 5–1000 at runtime | `100` | runtime-mutable | `src/config/config-schema.json`, runtime `src/config/runtime_variable_manager.cpp` |
| `api.max_query_length` | integer | 0–4096 at load; 0–4096 at runtime; `0` = unlimited | `128` | runtime-mutable | `src/config/config-schema.json`, runtime `src/config/runtime_variable_manager.cpp` |
| `api.admin_token` | string | any; must be non-empty when either listener is publicly bound — `api.tcp.bind` is not loopback and no Unix socket is configured, or `api.http.enable` is true and `api.http.bind` is not loopback | `""` | startup-only | `src/config/config.cpp` |

## `api.rate_limiting`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `api.rate_limiting.enable` | boolean | — | `false` | runtime-mutable | `src/config/config-schema.json`, runtime `src/config/runtime_variable_manager.cpp` |
| `api.rate_limiting.capacity` | integer | 1–10000 at load; 1–10000 at runtime | `100` | runtime-mutable | `src/config/config-schema.json`, runtime `src/config/runtime_variable_manager.cpp` |
| `api.rate_limiting.refill_rate` | integer | 1–1000 at load; 1–1000 at runtime | `10` | runtime-mutable | `src/config/config-schema.json`, runtime `src/config/runtime_variable_manager.cpp` |
| `api.rate_limiting.max_clients` | integer | 10–100000 | `10000` | startup-only | `src/config/config-schema.json` |

The TCP server always constructs a rate limiter, carrying `enable` as its initial state (`src/server/tcp_server.cpp`), so toggling `api.rate_limiting.enable` at runtime takes effect even when rate limiting was off at startup. The limiter instance is shared with the HTTP server, so a client's quota spans both protocols.

## `api.unix_socket`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `api.unix_socket.path` | string | no `..` component, no NUL; empty disables the Unix socket | `""` | startup-only | `src/config/config.cpp` |

A configured Unix socket replaces the TCP listener, which is why `api.tcp.bind` is exempted from the public-bind checks when this key is set (`src/config/config.cpp`).

## `server` (legacy)

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `server.host` | string | same rule as `api.tcp.bind` | (unset; falls through to `api.tcp.bind`) | startup-only | `src/config/config.cpp` |
| `server.port` | integer | 1–65535 | (unset; falls through to `api.tcp.port`) | startup-only | `src/config/config-schema.json` |

These write into `api.tcp.bind` / `api.tcp.port`. The `server` block is parsed before the `api` block (`src/config/config.cpp`), so when both are present `api.tcp` wins. They have no independent presence in the variable surface.

## `network`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `network.allow_cidrs` | string[] | every entry must parse as an IPv4 or IPv6 CIDR | `[]` | startup-only | `src/config/config.cpp` |

An empty or omitted list denies every request, health probes included (fail-closed, `src/utils/network_utils.h`) and produces a startup warning rather than a failure (`src/app/server_orchestrator.cpp`). A universal entry (`0.0.0.0/0`, `::/0`) combined with a non-loopback `api.tcp.bind` or `api.http.bind` is a hard failure (`src/config/config.cpp`).

## `logging`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `logging.level` | string | `debug`, `info`, `warn`, `error` | `info` | runtime-mutable | `src/config/config-schema.json`, runtime `src/config/runtime_variable_manager.cpp` |
| `logging.format` | string | `json`, `text` | `json` | runtime-mutable | `src/config/config-schema.json`, runtime `src/config/runtime_variable_manager.cpp` |
| `logging.file` | string | no `..` component, no NUL; empty means stdout | `""` | startup-only | `src/config/config.cpp` |

When `logging.file` is set, the log is a rotating file sink capped at 100 MiB per file with 5 retained rotations (`src/app/configuration_manager.cpp`). Those two bounds are compiled in and are not configuration keys.

## `cache`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `cache.enabled` | boolean | — | `true` | runtime-mutable | `src/config/config-schema.json`, runtime `src/config/runtime_variable_manager.cpp` |
| `cache.max_memory_mb` | integer | ≥ 1 in the schema; parser additionally rejects negative values, values above 1048576 MB, and — only when `cache.enabled` is true — any value above 50% of detected physical memory | `32` (stored as 33554432 bytes) | startup-only | `src/config/config.cpp` |
| `cache.min_query_cost_ms` | number | ≥ 0.0 at load; ≥ 0 at runtime | `10.0` | runtime-mutable | `src/config/config-schema.json`, runtime `src/config/runtime_variable_manager.cpp` |
| `cache.ttl_seconds` | integer | ≥ 0 at load; ≥ 0 at runtime; `0` = no TTL | `3600` | runtime-mutable | `src/config/config-schema.json`, runtime `src/config/runtime_variable_manager.cpp` |
| `cache.invalidation_strategy` | string | `ngram`, `table` | `ngram` | startup-only | `src/config/config-schema.json`, consumed at `src/cache/cache_manager.cpp` |
| `cache.compression_enabled` | boolean | — | `true` | startup-only | `src/config/config-schema.json` |
| `cache.eviction_batch_size` | integer | ≥ 1 | `10` | startup-only | `src/config/config-schema.json`, consumed at `src/cache/cache_manager.cpp` |
| `cache.invalidation.batch_size` | integer | ≥ 1 | `1000` | startup-only | `src/config/config-schema.json`, consumed at `src/cache/cache_manager.cpp` |
| `cache.invalidation.max_delay_ms` | integer | ≥ 0 | `100` | startup-only | `src/config/config-schema.json`, consumed at `src/cache/cache_manager.cpp` |
| `cache.invalidation.max_queue_size` | integer | ≥ 1 | `100000` | startup-only | `src/config/config-schema.json`, consumed at `src/cache/cache_manager.cpp` |

`cache.max_memory_mb` is the configuration key; the value is stored internally in bytes. The variable surface exposes both `cache.max_memory_mb` and a derived read-only `cache.max_memory_bytes` (`src/config/runtime_variable_manager.cpp`); the latter is not a configuration key and cannot appear in a config file.

The cache internals are constructed even when `cache.enabled` is `false` (`src/cache/cache_manager.cpp`), which is what makes the runtime toggle effective from a startup-disabled state.

## `bm25`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `bm25.enable` | boolean | — | `false` | startup-only | `src/config/config-schema.json` |
| `bm25.k1` | number | ≥ 0.0 | `1.2` | startup-only | `src/config/config-schema.json` |
| `bm25.b` | number | 0.0–1.0 | `0.75` | startup-only | `src/config/config-schema.json` |

## Unread keys

These keys are accepted by the schema, parsed into the configuration structure, echoed by the variable surface, and written into the dump config section (both container versions) — but no component ever reads them to make a decision. Setting them changes nothing about server behavior.

- `mysql.use_gtid` — compatibility field; only `true` is accepted (`src/config/config.cpp`).
- `mysql.binlog_format`, `mysql.binlog_row_image` — the effective values are read from the live MySQL server (`src/mysql/connection_validator.cpp`).
- `tables[].required_filters[].bitmap_index`
- `tables[].filters[].dict_compress`, `tables[].filters[].bitmap_index`, `tables[].filters[].bucket`
- `tables[].posting.block_size`, `tables[].posting.freq_bits`, `tables[].posting.use_roaring`
- `build.mode` — the schema enum has one member and nothing branches on it.
- `build.parallelism`, `build.throttle_ms`
- `replication.reconnect_backoff_min_ms`, `replication.reconnect_backoff_max_ms`
- `memory.hard_limit_mb`, `memory.soft_target_mb`, `memory.arena_chunk_mb`, `memory.minute_epoch`
- `dump.retain` — no dump-rotation code consumes it.

## Unschematized keys

- `tables[].required_filters[].operator` — the parser accepts `operator` as an alias for `op` (`src/config/config.cpp`). The schema declares `additionalProperties: false` on the required-filter object and does not list `operator` (`src/config/config-schema.json`), so schema validation rejects the key before the parser sees it. The alias is unreachable through the normal load path.

No other key read by the parser is absent from the schema.

## CLI flags (server binary)

Parsed by `src/app/command_line_parser.cpp`. `-h`/`--help`, `-v`/`--version` and `--print-surface` are scanned across the whole argument vector first and short-circuit everything else, including config loading (`src/app/command_line_parser.cpp`).

| Flag | Argument | Effect | Overrides |
|---|---|---|---|
| `-c`, `--config` | file path | Configuration file to load | — |
| (positional) | file path | Same as `--config`; a second positional argument is an error | — |
| `-d`, `--daemon` | — | Daemonize; also absolutizes `dump.dir`, `logging.file`, `mysql.ssl_ca`, `mysql.ssl_cert`, `mysql.ssl_key`, `api.unix_socket.path`, and each `tables[].synonyms.file` before any file is opened (`src/app/configuration_manager.cpp`) | — |
| `-t`, `--config-test` | — | Load the config, validate referenced files, print a summary, exit (`src/app/configuration_manager.cpp`) | — |
| `-s`, `--schema` | schema file path | Validate the config against this schema *in addition to* the built-in schema | — |
| `-h`, `--help` | — | Print usage and exit 0 | — |
| `-v`, `--version` | — | Print version and exit 0 | — |
| `--print-surface` | — | Print the external surface snapshot and exit 0 without reading a configuration file (`src/app/surface_descriptor.cpp`) | — |

A configuration file path is mandatory; omitting it is an error (`src/app/command_line_parser.cpp`). An unrecognized `-`-prefixed argument is an error (`src/app/command_line_parser.cpp`).

No CLI flag overrides a configuration key. The only override mechanism is the environment.

### Precedence

For the six keys that participate in environment overrides, the order is:

1. Environment variable (highest)
2. Configuration file value
3. Built-in default (lowest)

This is implemented at `src/config/config.cpp`. An environment variable set to the empty string counts as unset and falls through to the configuration file (`src/config/config.cpp`).

Environment overrides are applied only on the normal load path. The validation-only path used by `CONFIG VERIFY` deliberately skips them so the file itself is what gets inspected (`src/config/config_loader.cpp`, `src/config/config.h`). A configuration that supplies `mysql.user` only through `MYGRAM_MYSQL_USER` therefore starts but fails `CONFIG VERIFY`; the failure message states that environment overrides do not apply on that path (`src/config/config.cpp`).

## Environment variables

Every override in this table is applied in `src/config/config.cpp`.

| Variable | Key it overrides | Behavior on invalid input |
|---|---|---|
| `MYGRAM_MYSQL_HOST` | `mysql.host` | — |
| `MYGRAM_MYSQL_PORT` | `mysql.port` | Hard startup failure if not an integer in 1–65535 |
| `MYGRAM_MYSQL_USER` | `mysql.user` | — |
| `MYGRAM_MYSQL_PASSWORD` | `mysql.password` | — |
| `MYGRAM_MYSQL_DATABASE` | `mysql.database` | Hard startup failure if the result is empty or unquotable |
| `MYGRAM_API_ADMIN_TOKEN` | `api.admin_token` | — |

These six are the only environment variables read anywhere in `src/`. `MYGRAM_API_ADMIN_TOKEN` differs from the others in that it has no default-value fallback: when neither the environment variable nor `api.admin_token` is set, the result is the empty string regardless of any prior struct value (`src/config/config.cpp`).

## Client CLI flags (`mygram-cli`)

Parsed by `src/cli/mygram-cli.cpp`. The first argument that is not a recognized option begins the command; every remaining argument is taken verbatim as part of that command (`src/cli/mygram-cli.cpp`). With no command, the client runs interactively.

| Flag | Argument | Effect | Default |
|---|---|---|---|
| `-h` | HOST | Server hostname or IP | `127.0.0.1` |
| `-p` | PORT | Server port; rejected outside 1–65535 | `11016` |
| `-s` | SOCKET_PATH | Connect over a Unix domain socket instead of TCP | (unset) |
| `--timeout` | MS | Command timeout; also sets the dump save/load/verify and optimize timeouts to the same value | `30000`; `300000` for dump and optimize operations |
| `--connect-timeout` | MS | Connection timeout; must be a positive integer | `30000` |
| `--retry` | N | Retry a refused connection N times; must be ≥ 0 | `0` |
| `--retry-interval` | SEC | Seconds between retries; must be ≥ 0 | `3` |
| `--wait-ready` | — | Keep retrying until the server answers; sets the retry count to 100 unless `--retry` was given explicitly (`src/cli/mygram-cli.cpp`) | off |
| `--version`, `-V` | — | Print client version and exit 0 | — |
| `--help` | — | Print usage and exit 0 | — |

`-h` is the host flag, not help; help is `--help` only. The client reads no environment variables and no configuration file.

## Validation failures

### Hard startup failure

The process exits non-zero. Config-file problems surface before any socket or log file is opened.

**File and format**

- Configuration file missing, unreadable, or empty — `src/config/config_loader.cpp`
- YAML syntax error — `src/config/config_loader.cpp`
- JSON syntax error — `src/config/config_loader.cpp`
- Unrecognized extension and the content parses as neither YAML nor JSON — `src/config/config_loader.cpp`
- A `--schema` file that cannot be read — `src/config/config_loader.cpp`

**Schema**

- Any violation of the built-in schema: unknown property at any level, wrong type, out-of-range number, invalid enum member, missing `mysql`/`tables`, a table missing `name` or `text_source`, `text_source` with neither or both of `column`/`concat`, `tables` empty, `synonyms.enable: true` without `file` — `src/config/config_validator.cpp`
- Any violation of a `--schema` file, checked after the built-in schema passes — `src/config/config_validator.cpp`

**MySQL**

- `mysql.user` empty after environment override — `src/config/config.cpp`. The message names the sources
  the parse actually consulted, so on the validation-only path it reports that environment overrides
  are not applied instead of offering `MYGRAM_MYSQL_USER` as a remedy — `src/config/config.cpp`
- `mysql.database` empty or not a quotable identifier — `src/config/config.cpp`
- `MYGRAM_MYSQL_PORT` not an integer in 1–65535 — `src/config/config.cpp`
- `mysql.use_gtid: false` — `src/config/config.cpp`
- `mysql.binlog_format` other than `ROW` — `src/config/config.cpp`
- `mysql.binlog_row_image` other than `FULL` — `src/config/config.cpp`
- `mysql.ignored_ddl_prefixes` entry that is blank, contains `;`, or leads with a non-DDL verb — `src/config/config.cpp`
- `mysql.ssl_enable` and `mysql.ssl_verify_server_cert` both true with an empty `mysql.ssl_ca` — `src/config/config.cpp`
- `..` component or NUL in any of `mysql.ssl_ca`, `mysql.ssl_cert`, `mysql.ssl_key` — `src/config/config_validator.cpp`

**Tables**

- Table name, database, primary key, text-source column, concat column, or filter name that is empty or not a quotable identifier — `src/config/config.cpp`
- Filter type `enum` or `set` — `src/config/config.cpp`
- Filter type `float` — `src/config/config.cpp`
- Any other unsupported filter type — `src/config/config.cpp`
- `kanji_ngram_size` outside 0–10 — `src/config/config.cpp`
- `where_clause` present — `src/config/config.cpp`
- Invalid required-filter operator — `src/config/config.cpp`
- Boolean required filter with an ordering operator — `src/config/config.cpp`
- `value` present with `IS NULL`/`IS NOT NULL`, or absent with any other operator — `src/config/config.cpp`
- Empty `value` on a non-string filter type — `src/config/config.cpp`
- `value` given as an array, object, or null — `src/config/config.cpp`
- Same filter column declared with two different types — `src/config/config.cpp`
- `synonyms.enable: true` with an empty `synonyms.file` — `src/config/config.cpp`
- `..` component or NUL in `synonyms.file` — `src/config/config.cpp`
- Two tables with the same `(database, name)` identity — `src/config/config.cpp`

**Configured columns, checked against MySQL when replication starts**

Every column named by `primary_key`, `text_source` or a filter is looked up with `SHOW FULL COLUMNS` and refused when the initial snapshot and the binlog row image would not produce the same string for it (`src/mysql/ddl_schema_validator.cpp`). Each of these is a hard failure with `kMySQLInvalidSchema`:

- `BINARY`, `VARBINARY`, `TINYBLOB`, `BLOB`, `MEDIUMBLOB`, `LONGBLOB` — `src/mysql/ddl_schema_validator.cpp`
- A character type whose collation is not `utf8mb4_*`, `utf8mb3_*`, `utf8_*` or `ascii_*` — `src/mysql/ddl_schema_validator.cpp`
- Any `ZEROFILL` column: the result set carries the padded digits and the row image the unpadded number — `src/mysql/ddl_schema_validator.cpp`
- `JSON`, `GEOMETRY` and its subtypes, `VECTOR`, `FLOAT`: both paths produce a string and the two differ — `src/mysql/ddl_schema_validator.cpp`, `src/mysql/column_type_support.h`
- A declared type this build does not decode from a row image, such as a vendor type absent from `src/mysql/column_type_support.cpp` — `src/mysql/ddl_schema_validator.cpp`

The types that pass are the ones both paths render identically: the integer widths, `DECIMAL`, `DOUBLE`, `DATE`, `DATETIME`, `TIMESTAMP`, `TIME`, `YEAR`, `BIT`, `CHAR`, `VARCHAR`, the `TEXT` family, `ENUM` and `SET` (`src/mysql/column_type_support.h`).

**Replication**

- `replication.enable: true` with `server_id` of 0 — `src/config/config.cpp`
- `start_from` that is not `snapshot`, `latest`, or `gtid=…` — `src/config/config.cpp`
- `gtid=` value with no `:` — `src/config/config.cpp`
- `auto_initial_snapshot: true` with a `start_from` other than `snapshot` — `src/config/config.cpp`

**Network and API**

- `api.http.enable_cors: true` with an empty `api.http.cors_allow_origin` (when HTTP is enabled) — `src/config/config.cpp`
- Non-loopback `api.tcp.bind` with an empty `api.admin_token` and no Unix socket — `src/config/config.cpp`
- `api.http.enable: true` with a non-loopback `api.http.bind` and an empty `api.admin_token`, independently of how the TCP listener is configured: a Unix socket or a loopback `api.tcp.bind` does not exempt the HTTP listener, and the message names `api.http.bind` — `src/config/config.cpp`
- `network.allow_cidrs` containing a universal CIDR while `api.tcp.bind` or an enabled `api.http.bind` is non-loopback — `src/config/config.cpp`
- `network.allow_cidrs` entry that is not a valid CIDR — `src/config/config.cpp`
- `api.http.trusted_proxies` entry that is not a numeric IP address — `src/config/config.cpp`
- `api.tcp.max_pending_frame_bytes` below the effective `api.max_query_length` — `src/config/config.cpp`
- Bind address containing `/`, `..`, whitespace, or NUL, or that is neither a valid IP literal nor a valid hostname — `src/config/config_validator.cpp`
- `..` component or NUL in `api.unix_socket.path` — `src/config/config.cpp`

**Cache, dump, logging**

- Negative `cache.max_memory_mb` — `src/config/config.cpp`
- `cache.max_memory_mb` above 1048576 — `src/config/config.cpp`
- `cache.max_memory_mb` above 50% of detected physical memory, checked only when `cache.enabled` is true — `src/config/config.cpp`
- `..` component or NUL in `dump.dir` — `src/config/config.cpp`
- `dump.default_filename` that is empty, contains a separator, or contains `..` — `src/config/config.cpp`
- `..` component or NUL in `logging.file` — `src/config/config.cpp`
- `dump.dir` containing a `..` component before creation, or after symlink normalization — `src/app/application.cpp`
- Log file or log directory that cannot be created — `src/app/configuration_manager.cpp`
- Running as root — `src/app/application.cpp`

**Referenced files (`--config-test` only)**

`--config-test` additionally opens files the configuration points at and fails if they are missing, irregular, or empty (`src/app/configuration_manager.cpp`): `mysql.ssl_ca`, `mysql.ssl_cert`, `mysql.ssl_key` when `mysql.ssl_enable` is true, and every enabled `tables[].synonyms.file`. A normal startup performs the equivalent synonym check while building table contexts and fails there instead (`src/app/server_orchestrator.cpp`).

### Warning and continue

- `network.allow_cidrs` empty or omitted — every connection will be denied, health probes included; logged as `network_acl_empty` and startup proceeds (`src/app/server_orchestrator.cpp`).
- Physical memory size cannot be detected — the `cache.max_memory_mb` ratio check is skipped; logged as `config_warning` with reason `system_memory_info_unavailable` (`src/config/config.cpp`).
- `dump.load_on_startup: true` and the dump cannot be read, fails an integrity or identity check, or the MySQL source UUID is unavailable — logged as `startup_dump_load_failed` with `action: fallback_to_mysql_snapshot`, and startup continues by building a snapshot from MySQL (`src/app/server_orchestrator.cpp`).
- `logging.level` outside the enum — logged as "Unknown log level, keeping current level" and startup continues (`src/app/configuration_manager.cpp`). Unreachable through a schema-validated configuration, because the schema enum rejects the value first.

## Known divergences

Each item states both sides with citations. No fixes are proposed.

1. **`api.tcp.worker_threads` auto-sizing formula.** The schema description and the header comment both say `0` means `max(hardware_concurrency() * 4, 64)` (`src/config/config-schema.json`, `src/config/config.h`). The implementation computes `max(hardware_concurrency() * 2, 4)`, substituting 4 for `hardware_concurrency()` when the runtime reports 0 (`src/server/thread_pool.cpp`).

2. **`replication.server_id` is unchecked when the `replication` block is omitted.** The schema's `if`/`then` requiring `server_id` lives inside the `replication` subschema (`src/config/config-schema.json`), so it does not apply when the block is absent. The parser's `server_id != 0` check is likewise inside `if (root.contains("replication"))` (`src/config/config.cpp`). With `replication` omitted the configuration loads cleanly with `enable = true` (`src/config/config.h`) and `server_id = 0`; the failure surfaces later when the binlog reader starts (`src/mysql/binlog_reader.cpp`).

3. **`--schema` is additive, not an override.** The server's help text says "Use `--schema` only to override with a custom schema" (`src/app/command_line_parser.cpp`). The validator runs the built-in schema first and only then the custom one, so a custom schema can add constraints but cannot relax any (`src/config/config_validator.cpp`).

4. **`tables[].required_filters[].operator` alias is unreachable.** The parser accepts `operator` as a synonym for `op` (`src/config/config.cpp`), but the required-filter schema sets `additionalProperties: false` and does not declare it (`src/config/config-schema.json`).

5. **`mysql` description omits two environment variables.** The schema's `mysql` description lists `MYGRAM_MYSQL_USER`, `MYGRAM_MYSQL_PASSWORD`, `MYGRAM_MYSQL_HOST`, and `MYGRAM_MYSQL_DATABASE` (`src/config/config-schema.json`). The parser also honors `MYGRAM_MYSQL_PORT` (`src/config/config.cpp`). Separately, `MYGRAM_API_ADMIN_TOKEN` is documented on `api.admin_token` (`src/config/config-schema.json`) but is read outside the `mysql` block (`src/config/config.cpp`).

6. **`cache.max_memory_mb` has no schema maximum but two code maxima.** The schema declares only `minimum: 1` (`src/config/config-schema.json`). The parser caps the value at 1048576 MB (`src/config/config.cpp`) and additionally rejects anything above 50% of detected physical memory, that second check running only when `cache.enabled` is true (`src/config/config.cpp`).

7. **`api.tcp.keepalive` description cites a design document by path.** The schema description ends with a reference to `docs/ja/design/reactor-io-refactor.md §1.1` (`src/config/config-schema.json`); no such file exists in the repository.

8. **`api.tcp.worker_threads` is described against a superseded I/O model.** The schema and the header state that each persistent client holds one worker for its entire lifetime, making the worker count the cap on concurrent persistent clients (`src/config/config-schema.json`, `src/config/config.h`). The server runs a reactor (`src/server/io_reactor.cpp`, `src/server/reactor_connection.cpp`) in which persistent connections live in the reactor's connection map and consume a worker only briefly per completed frame (`src/server/thread_pool.cpp`), so the worker count does not bound concurrent persistent clients.

9. **`api.max_query_length` is bounds-checked against `api.tcp.max_pending_frame_bytes` only at load.** The load path rejects a `max_pending_frame_bytes` below the effective query limit (`src/config/config.cpp`). The runtime setter validates only the 0–4096 range (`src/config/runtime_variable_manager.cpp`) and does not re-check the frame-byte cap, so raising `api.max_query_length` at runtime can put the pair into a combination the loader would have refused.

10. **`mysql.datetime_timezone` is validated for format at load but parsed later.** The schema enforces the `±HH:MM` pattern (`src/config/config-schema.json`); the value is turned into a timezone offset on demand (`src/config/config.cpp`), so a schema-passing but otherwise unusable value would surface as a runtime error rather than a load error.

Neither `README.md` nor anything under `docs/` states a configuration default, so there are no README/docs default divergences to report. `examples/config.yaml`, `examples/config.json`, `examples/config-minimal.yaml`, and `examples/config-minimal.json` are sample configurations rather than statements of default values.
