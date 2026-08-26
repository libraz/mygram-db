# Configuration Keys

This file is normative for MygramDB's configuration surface. It describes what the code does today: the keys the parser accepts, the exact defaults it produces, the ranges it enforces, and which keys can be changed while the server is running. Changing a default, widening or narrowing a range, or moving a key between the startup-only and runtime-mutable classes is a surface change.

Configuration is loaded from a YAML (`.yaml`, `.yml`) or JSON (`.json`) file. Format is chosen by extension; an unrecognized extension is tried as YAML and then as JSON (`config_loader.cpp:264`). YAML is converted to JSON before anything else, so YAML and JSON accept exactly the same key set (`config.cpp:367`). Every configuration is validated against a JSON Schema that is compiled into the binary from `src/config/config-schema.json` (`src/config/CMakeLists.txt:12`); this happens for every load and cannot be turned off.

The root object requires `mysql` and `tables`, and rejects unknown top-level keys. Every object in the schema sets `additionalProperties: false`, so a misspelled key anywhere is a hard startup failure rather than a silently ignored value.

## Reading the tables

| Column | Meaning |
|---|---|
| Key | Fully qualified dotted path. `tables[]` denotes a per-table array element. |
| Type | Type as enforced by the schema. |
| Valid range / allowed values | Schema constraint, plus any additional constraint the parser applies. |
| Default | The value the code produces when the key is absent. |
| Mutability | `startup-only` or `runtime-mutable`. `unread` marks a key that is parsed and stored but never consumed by any component (see [Unread keys](#unread-keys)). |
| Validated where | Where the value is constrained. `config-schema.json:N` means schema-only, enforced by `config_validator.cpp:210`. |

Runtime mutation is available through the variable surface. The mutable set is an explicit allowlist at `runtime_variable_manager.cpp:48`; every other key is readable but rejects mutation with an "immutable (requires restart)" error (`runtime_variable_manager.cpp:83`).

## `mysql`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `mysql.host` | string | any | `127.0.0.1` | startup-only | `config-schema.json:15` |
| `mysql.port` | integer | 1–65535 | `3306` | startup-only | `config-schema.json:20`, env path `config.cpp:424` |
| `mysql.user` | string | minLength 1; must be non-empty after env override | (none — required) | startup-only | `config.cpp:452` |
| `mysql.password` | string | any | `""` | startup-only | `config-schema.json:32` |
| `mysql.database` | string | minLength 1, no NUL; must be a quotable SQL identifier | (none — required) | startup-only | `config.cpp:478`, `sql_utils.cpp:13` |
| `mysql.use_gtid` | boolean | `true` only | `true` | startup-only · unread | `config.cpp:483` |
| `mysql.binlog_format` | string | `ROW` only | `ROW` | startup-only · unread | `config.cpp:491` |
| `mysql.binlog_row_image` | string | `FULL` only | `FULL` | startup-only · unread | `config.cpp:499` |
| `mysql.connect_timeout_ms` | integer | 100–60000 | `3000` | startup-only | `config-schema.json:60` |
| `mysql.read_timeout_ms` | integer | 1000–86400000 | `3600000` | startup-only | `config-schema.json:67` |
| `mysql.write_timeout_ms` | integer | 1000–86400000 | `3600000` | startup-only | `config-schema.json:74` |
| `mysql.session_timeout_sec` | integer | 60–86400 | `3600` | startup-only | `config-schema.json:81` |
| `mysql.ssl_enable` | boolean | — | `false` | startup-only | `config-schema.json:88` |
| `mysql.ssl_ca` | string | no `..` component, no NUL; required when `ssl_enable` and `ssl_verify_server_cert` are both true | `""` | startup-only | `config.cpp:521`, `config.cpp:565` |
| `mysql.ssl_cert` | string | no `..` component, no NUL | `""` | startup-only | `config.cpp:527` |
| `mysql.ssl_key` | string | no `..` component, no NUL | `""` | startup-only | `config.cpp:533` |
| `mysql.ssl_verify_server_cert` | boolean | — | `true` | startup-only | `config-schema.json:105` |
| `mysql.ignored_ddl_prefixes` | string[] | each entry non-empty, no `;`, first word one of `ALTER`, `ANALYZE`, `CHECK`, `CREATE`, `DROP`, `OPTIMIZE`, `RENAME`, `REPAIR`, `TRUNCATE` | `[]` | startup-only | `config.cpp:542` |
| `mysql.datetime_timezone` | string | `^[+-]([01][0-9]\|2[0-3]):[0-5][0-9]$` | `+00:00` | startup-only | `config-schema.json:116`, parsed at `config.cpp:1458` |

`mysql.binlog_format` and `mysql.binlog_row_image` are assertions about the server, not settings sent to it. The parser rejects any value other than `ROW`/`FULL`, and the actual enforcement queries the live server (`connection_validator.cpp:497`, `connection_validator.cpp:525`). The configured values themselves are never used to drive behavior.

## `tables[]`

Each element requires `name` and `text_source`. Table identity is `(database, name)`; a duplicate identity is a hard failure (`config.cpp:972`).

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `tables[].name` | string | minLength 1, no NUL, quotable SQL identifier | (none — required) | startup-only | `config.cpp:767` |
| `tables[].database` | string | minLength 1, no NUL, quotable SQL identifier | value of `mysql.database` | startup-only | `config.cpp:773`, defaulted at `config.cpp:956` |
| `tables[].primary_key` | string | minLength 1, no NUL, quotable SQL identifier | `id` | startup-only | `config.cpp:780` |
| `tables[].text_source.column` | string | minLength 1, no NUL, quotable SQL identifier; exactly one of `column`/`concat` required | `""` | startup-only | `config.cpp:824`, `config-schema.json:179` |
| `tables[].text_source.concat` | string[] | minItems 2, each minLength 1, no NUL, quotable SQL identifier | `[]` | startup-only | `config.cpp:831`, `config-schema.json:171` |
| `tables[].text_source.delimiter` | string | any | `" "` (single space) | startup-only | `config-schema.json:173` |
| `tables[].required_filters[].name` | string | minLength 1, no NUL, quotable SQL identifier | (none — required) | startup-only | `config.cpp:596` |
| `tables[].required_filters[].type` | string | `tinyint`, `tinyint_unsigned`, `smallint`, `smallint_unsigned`, `mediumint`, `mediumint_unsigned`, `int`, `int_unsigned`, `bigint`, `bigint_unsigned`, `float`, `double`, `string`, `varchar`, `text`, `datetime`, `date`, `timestamp`, `time`, `boolean` | (none — required) | startup-only | `config.cpp:615`, `config.cpp:183` |
| `tables[].required_filters[].op` | string | `=`, `!=`, `<`, `>`, `<=`, `>=`, `IS NULL`, `IS NOT NULL`; `boolean` type restricted to `=`, `!=`, `IS NULL`, `IS NOT NULL` | (none — required) | startup-only | `config.cpp:650`, `config.cpp:664` |
| `tables[].required_filters[].value` | string \| number \| boolean | must be absent for `IS NULL`/`IS NOT NULL`, present otherwise; empty string permitted only for `string`/`varchar`/`text` | (none) | startup-only | `config.cpp:672`, `config.cpp:687`, `config.cpp:700` |
| `tables[].required_filters[].bitmap_index` | boolean | — | `false` | startup-only · unread | `config-schema.json:225` |
| `tables[].filters[].name` | string | minLength 1, no NUL, quotable SQL identifier | (none — required) | startup-only | `config.cpp:722` |
| `tables[].filters[].type` | string | same enum as `required_filters[].type` | (none — required) | startup-only | `config.cpp:728` |
| `tables[].filters[].dict_compress` | boolean | — | `false` | startup-only · unread | `config-schema.json:261` |
| `tables[].filters[].bitmap_index` | boolean | — | `false` | startup-only · unread | `config-schema.json:266` |
| `tables[].filters[].bucket` | string | `minute`, `hour`, `day` | `""` | startup-only · unread | `config-schema.json:271` |
| `tables[].ngram_size` | integer | 1–10 | `2`, or `index.ngram_size` when that legacy key is present | startup-only | `config-schema.json:279`, inheritance at `config.cpp:960` |
| `tables[].kanji_ngram_size` | integer | 0–10; `0` means inherit the effective `ngram_size` | effective `ngram_size` | startup-only | `config.cpp:788`, inheritance at `config.cpp:966` |
| `tables[].cross_boundary_ngrams` | boolean | — | `true` | startup-only | `config-schema.json:293` |
| `tables[].posting.block_size` | integer | 8–1024 | `128` | startup-only · unread | `config-schema.json:303` |
| `tables[].posting.freq_bits` | integer | `0`, `4`, `8` | `0` | startup-only · unread | `config-schema.json:310` |
| `tables[].posting.use_roaring` | string | `auto`, `always`, `never` | `auto` | startup-only · unread | `config-schema.json:316` |
| `tables[].synonyms.enable` | boolean | — | `false` | startup-only | `config-schema.json:338` |
| `tables[].synonyms.file` | string | minLength 1, no `..` component, no NUL; required when `enable` is true | `""` | startup-only | `config.cpp:908`, `config.cpp:913`, `config-schema.json:328` |

A column named in both `required_filters` and `filters` must carry the same `type` in both places; conflicting types are a hard failure (`config.cpp:866`). `tables[].where_clause` is not accepted: the schema rejects it as an unknown property, and the parser carries an explicit rejection with a migration message (`config.cpp:804`).

## `index` (legacy)

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `index.ngram_size` | integer | 1–10 | `2` | startup-only | `config-schema.json:358`, applied at `config.cpp:944` |

This value is applied only to tables that do not carry their own `ngram_size` (`config.cpp:960`). It is not stored on `Config` and does not appear in the variable surface.

## `build`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `build.mode` | string | `select_snapshot` only | `select_snapshot` | startup-only · unread | `config-schema.json:372` |
| `build.batch_size` | integer | 100–100000 | `5000` | startup-only | `config-schema.json:378`, consumed at `initial_loader.cpp:281` |
| `build.parallelism` | integer | 1–64 | `2` | startup-only · unread | `config-schema.json:385` |
| `build.throttle_ms` | integer | 0–10000 | `0` | startup-only · unread | `config-schema.json:392` |

## `replication`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `replication.enable` | boolean | — | `true` | startup-only | `config-schema.json:406` |
| `replication.auto_initial_snapshot` | boolean | — | `false` | startup-only | `config-schema.json:411`, cross-check `config.cpp:1073` |
| `replication.server_id` | integer | 1–4294967295; must be non-zero when `enable` is true | `0` | startup-only | `config.cpp:1026`, `config-schema.json:449` |
| `replication.start_from` | string | `snapshot`, `latest`, or `gtid=<UUID:txn>` (the GTID form must contain a `:`) | `snapshot` | startup-only | `config.cpp:1042`, `config.cpp:1060` |
| `replication.queue_size` | integer | 100–1000000 | `10000` | startup-only | `config-schema.json:427` |
| `replication.reconnect_backoff_min_ms` | integer | 100–60000 | `500` | startup-only · unread | `config-schema.json:434` |
| `replication.reconnect_backoff_max_ms` | integer | 1000–600000 | `10000` | startup-only · unread | `config-schema.json:441` |

`auto_initial_snapshot: true` requires `start_from: snapshot`; any other start position is a hard failure (`config.cpp:1073`).

## `memory`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `memory.hard_limit_mb` | integer | ≥ 256 | `8192` | startup-only · unread | `config-schema.json:465` |
| `memory.soft_target_mb` | integer | ≥ 128 | `4096` | startup-only · unread | `config-schema.json:471` |
| `memory.arena_chunk_mb` | integer | 1–1024 | `64` | startup-only · unread | `config-schema.json:477` |
| `memory.roaring_threshold` | number | 0.0–1.0 | `0.18` | startup-only | `config-schema.json:484`, consumed at `server_orchestrator.cpp:379` |
| `memory.minute_epoch` | boolean | — | `true` | startup-only · unread | `config-schema.json:491` |
| `memory.normalize.nfkc` | boolean | — | `true` | startup-only | `config-schema.json:501` |
| `memory.normalize.width` | string | `keep`, `narrow`, `wide` | `narrow` | startup-only | `config-schema.json:506` |
| `memory.normalize.lower` | boolean | — | `false` | startup-only | `config-schema.json:512` |
| `memory.verify_text` | string | `off`, `ascii`, `all` | `off` | startup-only | `config-schema.json:519`, consumed at `search_pipeline.cpp:42` |

## `dump`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `dump.dir` | string | no `..` component, no NUL; also rejected before and after normalization at startup | `/var/lib/mygramdb/dumps` | startup-only | `config.cpp:1125`, `application.cpp:346` |
| `dump.default_filename` | string | non-empty basename (no `/`, no `\`), no `..` component | `mygramdb.dmp` | startup-only | `config.cpp:1131`, `config.cpp:305` |
| `dump.load_on_startup` | boolean | — | `false` | startup-only | `config-schema.json:542`, consumed at `server_orchestrator.cpp:534` |
| `dump.interval_sec` | integer | 0–86400; `0` disables periodic dumps | `0` | startup-only | `config-schema.json:547`, consumed at `snapshot_scheduler.cpp:97` |
| `dump.retain` | integer | 1–100 | `3` | startup-only · unread | `config-schema.json:554` |
| `dump.restore_memory_budget_mb` | integer | 1–1048576 | `4096` | startup-only | `config-schema.json:561`, consumed at `server_orchestrator.cpp:585` |
| `dump.restore_max_section_mb` | integer | 1–1048576 | `2048` | startup-only | `config-schema.json:568`, consumed at `server_orchestrator.cpp:586` |

## `api.tcp`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `api.tcp.bind` | string | valid IP literal (with optional IPv6 scope) or hostname; no `/`, no `..`, no whitespace, no NUL | `127.0.0.1` | startup-only | `config.cpp:1172`, `config_validator.cpp:117` |
| `api.tcp.port` | integer | 1–65535 | `11016` | startup-only | `config-schema.json:592` |
| `api.tcp.max_connections` | integer | 1–1000000 | `10000` | startup-only | `config-schema.json:599`, consumed at `connection_acceptor.cpp:490` |
| `api.tcp.worker_threads` | integer | 0–16384; `0` = auto | `0` | startup-only | `config-schema.json:606`, auto-sizing at `thread_pool.cpp:32` |
| `api.tcp.recv_timeout_sec` | integer | 0–86400; `0` disables the first-frame deadline | `60` | startup-only | `config-schema.json:613` |
| `api.tcp.idle_timeout_sec` | integer | 0–86400; `0` disables idle reaping | `300` | startup-only | `config-schema.json:620` |
| `api.tcp.reaper_interval_sec` | integer | 1–3600 | `5` | startup-only | `config-schema.json:627` |
| `api.tcp.thread_pool_queue_size` | integer | 0–1000000; `0` = unbounded | `1000` | startup-only | `config-schema.json:634` |
| `api.tcp.keepalive.enabled` | boolean | — | `true` | startup-only | `config-schema.json:646` |
| `api.tcp.keepalive.idle_sec` | integer | 1–86400 | `60` | startup-only | `config-schema.json:651` |
| `api.tcp.keepalive.interval_sec` | integer | 1–3600 | `20` | startup-only | `config-schema.json:658` |
| `api.tcp.keepalive.probe_count` | integer | 1–32 | `3` | startup-only | `config-schema.json:665` |
| `api.tcp.max_write_queue_bytes` | integer | 4096–1073741824 | `16777216` (16 MiB) | startup-only | `config-schema.json:674` |
| `api.tcp.max_total_buffered_bytes` | integer | 1048576–68719476736 | `268435456` (256 MiB) | startup-only | `config-schema.json:681` |
| `api.tcp.max_pending_frames` | integer | 4–1000000 | `1024` | startup-only | `config-schema.json:688` |
| `api.tcp.max_pending_frame_bytes` | integer | 4096–1073741824; must be ≥ min(`api.max_query_length`, 1 MiB), or 1 MiB when `max_query_length` is 0 | `4194304` (4 MiB) | startup-only | `config.cpp:165` |

## `api.http`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `api.http.enable` | boolean | — | `false` | startup-only | `config-schema.json:709` |
| `api.http.bind` | string | same rule as `api.tcp.bind` | `127.0.0.1` | startup-only | `config.cpp:1232`, `config_validator.cpp:117` |
| `api.http.port` | integer | 1–65535 | `8080` | startup-only | `config-schema.json:719` |
| `api.http.max_connections` | integer | 1–1000000 | `10000` | startup-only | `config-schema.json:726`, consumed at `http_server.cpp:646` |
| `api.http.trusted_proxies` | string[] | every entry must be a numeric IPv4 or IPv6 address | `[]` | startup-only | `config.cpp:1244` |
| `api.http.enable_cors` | boolean | — | `false` | startup-only | `config-schema.json:739` |
| `api.http.cors_allow_origin` | string | must be non-empty when `api.http.enable` and `api.http.enable_cors` are both true | `""` | startup-only | `config.cpp:119` |
| `api.http.read_timeout_sec` | integer | 1–3600 | `5` | startup-only | `config-schema.json:749` |
| `api.http.write_timeout_sec` | integer | 1–3600 | `5` | startup-only | `config-schema.json:756` |
| `api.http.max_body_bytes` | integer | 0–1073741824; `0` disables the limit (no `set_payload_max_length` call, leaving cpp-httplib's effectively unbounded default) | `16777216` (16 MiB) | startup-only | `config-schema.json:763`, applied at `http_server.cpp:665` |

## `api` (top-level)

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `api.default_limit` | integer | 5–1000 at load; 5–1000 at runtime | `100` | runtime-mutable | `config-schema.json:772`, runtime `runtime_variable_manager.cpp:329` |
| `api.max_query_length` | integer | 0–4096 at load; 0–4096 at runtime; `0` = unlimited | `128` | runtime-mutable | `config-schema.json:779`, runtime `runtime_variable_manager.cpp:352` |
| `api.admin_token` | string | any; must be non-empty when `api.tcp.bind` is not loopback and no Unix socket is configured | `""` | startup-only | `config.cpp:125` |

## `api.rate_limiting`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `api.rate_limiting.enable` | boolean | — | `false` | runtime-mutable | `config-schema.json:791`, runtime `runtime_variable_manager.cpp:374` |
| `api.rate_limiting.capacity` | integer | 1–10000 at load; 1–10000 at runtime | `100` | runtime-mutable | `config-schema.json:796`, runtime `runtime_variable_manager.cpp:397` |
| `api.rate_limiting.refill_rate` | integer | 1–1000 at load; 1–1000 at runtime | `10` | runtime-mutable | `config-schema.json:803`, runtime `runtime_variable_manager.cpp:425` |
| `api.rate_limiting.max_clients` | integer | 10–100000 | `10000` | startup-only | `config-schema.json:810` |

The TCP server always constructs a rate limiter, carrying `enable` as its initial state (`tcp_server.cpp:127`), so toggling `api.rate_limiting.enable` at runtime takes effect even when rate limiting was off at startup. The limiter instance is shared with the HTTP server, so a client's quota spans both protocols.

## `api.unix_socket`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `api.unix_socket.path` | string | no `..` component, no NUL; empty disables the Unix socket | `""` | startup-only | `config.cpp:1292` |

A configured Unix socket replaces the TCP listener, which is why `api.tcp.bind` is exempted from the public-bind checks when this key is set (`config.cpp:152`).

## `server` (legacy)

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `server.host` | string | same rule as `api.tcp.bind` | (unset; falls through to `api.tcp.bind`) | startup-only | `config.cpp:1158` |
| `server.port` | integer | 1–65535 | (unset; falls through to `api.tcp.port`) | startup-only | `config-schema.json:847` |

These write into `api.tcp.bind` / `api.tcp.port`. The `server` block is parsed before the `api` block (`config.cpp:1153` then `config.cpp:1166`), so when both are present `api.tcp` wins. They have no independent presence in the variable surface.

## `network`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `network.allow_cidrs` | string[] | every entry must parse as an IPv4 or IPv6 CIDR | `[]` | startup-only | `config.cpp:1313` |

An empty or omitted list denies every non-probe request (fail-closed, `network_utils.h:100`) and produces a startup warning rather than a failure (`server_orchestrator.cpp:903`). A universal entry (`0.0.0.0/0`, `::/0`) combined with a non-loopback `api.tcp.bind` or `api.http.bind` is a hard failure (`config.cpp:132`).

## `logging`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `logging.level` | string | `debug`, `info`, `warn`, `error` | `info` | runtime-mutable | `config-schema.json:874`, runtime `runtime_variable_manager.cpp:288` |
| `logging.format` | string | `json`, `text` | `json` | runtime-mutable | `config-schema.json:880`, runtime `runtime_variable_manager.cpp:313` |
| `logging.file` | string | no `..` component, no NUL; empty means stdout | `""` | startup-only | `config.cpp:1340` |

When `logging.file` is set, the log is a rotating file sink capped at 100 MiB per file with 5 retained rotations (`configuration_manager.cpp:25`). Those two bounds are compiled in and are not configuration keys.

## `cache`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `cache.enabled` | boolean | — | `true` | runtime-mutable | `config-schema.json:898`, runtime `runtime_variable_manager.cpp:452` |
| `cache.max_memory_mb` | integer | ≥ 1 in the schema; parser additionally rejects negative values, values above 1048576 MB, and any value above 50% of detected physical memory | `32` (stored as 33554432 bytes) | startup-only | `config.cpp:1358`, `config.cpp:1363`, `config.cpp:1409` |
| `cache.min_query_cost_ms` | number | ≥ 0.0 at load; ≥ 0 at runtime | `10.0` | runtime-mutable | `config-schema.json:909`, runtime `runtime_variable_manager.cpp:491` |
| `cache.ttl_seconds` | integer | ≥ 0 at load; ≥ 0 at runtime; `0` = no TTL | `3600` | runtime-mutable | `config-schema.json:915`, runtime `runtime_variable_manager.cpp:513` |
| `cache.invalidation_strategy` | string | `ngram`, `table` | `ngram` | startup-only | `config-schema.json:921`, consumed at `cache_manager.cpp:20` |
| `cache.compression_enabled` | boolean | — | `true` | startup-only | `config-schema.json:927` |
| `cache.eviction_batch_size` | integer | ≥ 1 | `10` | startup-only | `config-schema.json:932`, consumed at `cache_manager.cpp:25` |
| `cache.invalidation.batch_size` | integer | ≥ 1 | `1000` | startup-only | `config-schema.json:943`, consumed at `cache_manager.cpp:57` |
| `cache.invalidation.max_delay_ms` | integer | ≥ 0 | `100` | startup-only | `config-schema.json:949`, consumed at `cache_manager.cpp:58` |
| `cache.invalidation.max_queue_size` | integer | ≥ 1 | `100000` | startup-only | `config-schema.json:955` |

`cache.max_memory_mb` is the configuration key; the value is stored internally in bytes. The variable surface exposes both `cache.max_memory_mb` and a derived read-only `cache.max_memory_bytes` (`runtime_variable_manager.cpp:231`); the latter is not a configuration key and cannot appear in a config file.

The cache internals are constructed even when `cache.enabled` is `false` (`cache_manager.cpp:21`), which is what makes the runtime toggle effective from a startup-disabled state.

## `bm25`

| Key | Type | Valid range / allowed values | Default | Mutability | Validated where |
|---|---|---|---|---|---|
| `bm25.enable` | boolean | — | `false` | startup-only | `config-schema.json:970` |
| `bm25.k1` | number | ≥ 0.0 | `1.2` | startup-only | `config-schema.json:975` |
| `bm25.b` | number | 0.0–1.0 | `0.75` | startup-only | `config-schema.json:981` |

## Unread keys

These keys are accepted by the schema, parsed into the configuration structure, echoed by the variable surface, and written into V1 dump metadata — but no component ever reads them to make a decision. Setting them changes nothing about server behavior.

- `mysql.use_gtid` — compatibility field; only `true` is accepted (`config.cpp:483`).
- `mysql.binlog_format`, `mysql.binlog_row_image` — the effective values are read from the live MySQL server (`connection_validator.cpp:497`, `connection_validator.cpp:525`).
- `tables[].required_filters[].bitmap_index`
- `tables[].filters[].dict_compress`, `tables[].filters[].bitmap_index`, `tables[].filters[].bucket`
- `tables[].posting.block_size`, `tables[].posting.freq_bits`, `tables[].posting.use_roaring`
- `build.mode` — the schema enum has one member and nothing branches on it.
- `build.parallelism`, `build.throttle_ms`
- `replication.reconnect_backoff_min_ms`, `replication.reconnect_backoff_max_ms`
- `memory.hard_limit_mb`, `memory.soft_target_mb`, `memory.arena_chunk_mb`, `memory.minute_epoch`
- `dump.retain` — no dump-rotation code consumes it.

## Unschematized keys

- `tables[].required_filters[].operator` — the parser accepts `operator` as an alias for `op` (`config.cpp:620`). The schema declares `additionalProperties: false` on the required-filter object and does not list `operator` (`config-schema.json:194`), so schema validation rejects the key before the parser sees it. The alias is unreachable through the normal load path.

No other key read by the parser is absent from the schema.

## CLI flags (server binary)

Parsed by `command_line_parser.cpp:30`. `-h`/`--help` and `-v`/`--version` are scanned across the whole argument vector first and short-circuit everything else, including config loading (`command_line_parser.cpp:40`).

| Flag | Argument | Effect | Overrides |
|---|---|---|---|
| `-c`, `--config` | file path | Configuration file to load | — |
| (positional) | file path | Same as `--config`; a second positional argument is an error | — |
| `-d`, `--daemon` | — | Daemonize; also absolutizes `dump.dir`, `logging.file`, `mysql.ssl_ca`, `mysql.ssl_cert`, `mysql.ssl_key`, `api.unix_socket.path`, and each `tables[].synonyms.file` before any file is opened (`configuration_manager.cpp:200`) | — |
| `-t`, `--config-test` | — | Load the config, validate referenced files, print a summary, exit (`configuration_manager.cpp:127`) | — |
| `-s`, `--schema` | schema file path | Validate the config against this schema *in addition to* the built-in schema | — |
| `-h`, `--help` | — | Print usage and exit 0 | — |
| `-v`, `--version` | — | Print version and exit 0 | — |

A configuration file path is mandatory; omitting it is an error (`command_line_parser.cpp:99`). An unrecognized `-`-prefixed argument is an error (`command_line_parser.cpp:82`).

No CLI flag overrides a configuration key. The only override mechanism is the environment.

### Precedence

For the five keys that participate in environment overrides, the order is:

1. Environment variable (highest)
2. Configuration file value
3. Built-in default (lowest)

This is implemented at `config.cpp:350`. An environment variable set to the empty string counts as unset and falls through to the configuration file (`config.cpp:334`).

Environment overrides are applied only on the normal load path. The validation-only path used by `CONFIG VERIFY` deliberately skips them so the file itself is what gets inspected (`config_loader.cpp:325`, `config.h:572`).

## Environment variables

| Variable | Key it overrides | Behavior on invalid input | Citation |
|---|---|---|---|
| `MYGRAM_MYSQL_HOST` | `mysql.host` | — | `config.cpp:415` |
| `MYGRAM_MYSQL_PORT` | `mysql.port` | Hard startup failure if not an integer in 1–65535 | `config.cpp:421` |
| `MYGRAM_MYSQL_USER` | `mysql.user` | — | `config.cpp:449` |
| `MYGRAM_MYSQL_PASSWORD` | `mysql.password` | — | `config.cpp:464` |
| `MYGRAM_MYSQL_DATABASE` | `mysql.database` | Hard startup failure if the result is empty or unquotable | `config.cpp:475` |
| `MYGRAM_API_ADMIN_TOKEN` | `api.admin_token` | — | `config.cpp:1303` |

These six are the only environment variables read anywhere in `src/`. `MYGRAM_API_ADMIN_TOKEN` differs from the others in that it has no default-value fallback: when neither the environment variable nor `api.admin_token` is set, the result is the empty string regardless of any prior struct value (`config.cpp:1302`).

## Client CLI flags (`mygram-cli`)

Parsed by `mygram-cli.cpp:1206`. The first argument that is not a recognized option begins the command; every remaining argument is taken verbatim as part of that command (`mygram-cli.cpp:1328`). With no command, the client runs interactively.

| Flag | Argument | Effect | Default |
|---|---|---|---|
| `-h` | HOST | Server hostname or IP | `127.0.0.1` |
| `-p` | PORT | Server port; rejected outside 1–65535 | `11016` |
| `-s` | SOCKET_PATH | Connect over a Unix domain socket instead of TCP | (unset) |
| `--timeout` | MS | Command timeout; also sets the dump save/load/verify and optimize timeouts to the same value | `30000`; `300000` for dump and optimize operations |
| `--connect-timeout` | MS | Connection timeout; must be a positive integer | `30000` |
| `--retry` | N | Retry a refused connection N times; must be ≥ 0 | `0` |
| `--retry-interval` | SEC | Seconds between retries; must be ≥ 0 | `3` |
| `--wait-ready` | — | Keep retrying until the server answers; sets the retry count to 100 unless `--retry` was given explicitly (`mygram-cli.cpp:1338`) | off |
| `--version`, `-V` | — | Print client version and exit 0 | — |
| `--help` | — | Print usage and exit 0 | — |

`-h` is the host flag, not help; help is `--help` only. The client reads no environment variables and no configuration file.

## Validation failures

### Hard startup failure

The process exits non-zero. Config-file problems surface before any socket or log file is opened.

**File and format**

- Configuration file missing, unreadable, or empty — `config_loader.cpp:59`, `config_loader.cpp:83`
- YAML syntax error — `config_loader.cpp:222`
- JSON syntax error — `config_loader.cpp:118`
- Unrecognized extension and the content parses as neither YAML nor JSON — `config_loader.cpp:307`
- A `--schema` file that cannot be read — `config_loader.cpp:138`

**Schema**

- Any violation of the built-in schema: unknown property at any level, wrong type, out-of-range number, invalid enum member, missing `mysql`/`tables`, a table missing `name` or `text_source`, `text_source` with neither or both of `column`/`concat`, `tables` empty, `synonyms.enable: true` without `file` — `config_validator.cpp:210`
- Any violation of a `--schema` file, checked after the built-in schema passes — `config_validator.cpp:214`

**MySQL**

- `mysql.user` empty after environment override — `config.cpp:452`
- `mysql.database` empty or not a quotable identifier — `config.cpp:478`
- `MYGRAM_MYSQL_PORT` not an integer in 1–65535 — `config.cpp:433`
- `mysql.use_gtid: false` — `config.cpp:483`
- `mysql.binlog_format` other than `ROW` — `config.cpp:491`
- `mysql.binlog_row_image` other than `FULL` — `config.cpp:499`
- `mysql.ignored_ddl_prefixes` entry that is blank, contains `;`, or leads with a non-DDL verb — `config.cpp:545`, `config.cpp:555`
- `mysql.ssl_enable` and `mysql.ssl_verify_server_cert` both true with an empty `mysql.ssl_ca` — `config.cpp:565`
- `..` component or NUL in any of `mysql.ssl_ca`, `mysql.ssl_cert`, `mysql.ssl_key` — `config_validator.cpp:99`

**Tables**

- Table name, database, primary key, text-source column, concat column, or filter name that is empty or not a quotable identifier — `config.cpp:767`, `config.cpp:773`, `config.cpp:780`, `config.cpp:824`, `config.cpp:831`, `config.cpp:596`, `config.cpp:722`
- Filter type `enum` or `set` — `config.cpp:200`
- Any other unsupported filter type — `config.cpp:208`
- `kanji_ngram_size` outside 0–10 — `config.cpp:788`
- `where_clause` present — `config.cpp:804`
- Invalid required-filter operator — `config.cpp:651`
- Boolean required filter with an ordering operator — `config.cpp:664`
- `value` present with `IS NULL`/`IS NOT NULL`, or absent with any other operator — `config.cpp:672`, `config.cpp:687`
- Empty `value` on a non-string filter type — `config.cpp:701`
- `value` given as an array, object, or null — `config.cpp:639`
- Same filter column declared with two different types — `config.cpp:870`, `config.cpp:879`
- `synonyms.enable: true` with an empty `synonyms.file` — `config.cpp:913`
- `..` component or NUL in `synonyms.file` — `config.cpp:908`
- Two tables with the same `(database, name)` identity — `config.cpp:976`

**Replication**

- `replication.enable: true` with `server_id` of 0 — `config.cpp:1026`
- `start_from` that is not `snapshot`, `latest`, or `gtid=…` — `config.cpp:1046`
- `gtid=` value with no `:` — `config.cpp:1064`
- `auto_initial_snapshot: true` with a `start_from` other than `snapshot` — `config.cpp:1073`

**Network and API**

- `api.http.enable_cors: true` with an empty `api.http.cors_allow_origin` (when HTTP is enabled) — `config.cpp:119`
- Non-loopback `api.tcp.bind` with an empty `api.admin_token` and no Unix socket — `config.cpp:125`
- `network.allow_cidrs` containing a universal CIDR while `api.tcp.bind` or an enabled `api.http.bind` is non-loopback — `config.cpp:144`
- `network.allow_cidrs` entry that is not a valid CIDR — `config.cpp:1314`
- `api.http.trusted_proxies` entry that is not a numeric IP address — `config.cpp:1245`
- `api.tcp.max_pending_frame_bytes` below the effective `api.max_query_length` — `config.cpp:174`
- Bind address containing `/`, `..`, whitespace, or NUL, or that is neither a valid IP literal nor a valid hostname — `config_validator.cpp:128`–`config_validator.cpp:164`
- `..` component or NUL in `api.unix_socket.path` — `config.cpp:1292`

**Cache, dump, logging**

- Negative `cache.max_memory_mb` — `config.cpp:1358`
- `cache.max_memory_mb` above 1048576 — `config.cpp:1363`
- `cache.max_memory_mb` above 50% of detected physical memory — `config.cpp:1409`
- `..` component or NUL in `dump.dir` — `config.cpp:1125`
- `dump.default_filename` that is empty, contains a separator, or contains `..` — `config.cpp:1131`
- `..` component or NUL in `logging.file` — `config.cpp:1340`
- `dump.dir` containing a `..` component before creation, or after symlink normalization — `application.cpp:346`, `application.cpp:368`
- Log file or log directory that cannot be created — `configuration_manager.cpp:164`
- Running as root — `application.cpp:99`

**Referenced files (`--config-test` only)**

`--config-test` additionally opens files the configuration points at and fails if they are missing, irregular, or empty (`configuration_manager.cpp:70`): `mysql.ssl_ca`, `mysql.ssl_cert`, `mysql.ssl_key` when `mysql.ssl_enable` is true, and every enabled `tables[].synonyms.file`. A normal startup performs the equivalent synonym check while building table contexts and fails there instead (`server_orchestrator.cpp:392`).

### Warning and continue

- `network.allow_cidrs` empty or omitted — every non-probe connection will be denied; logged as `network_acl_empty` and startup proceeds (`server_orchestrator.cpp:903`).
- Physical memory size cannot be detected — the `cache.max_memory_mb` ratio check is skipped; logged as `config_warning` with reason `system_memory_info_unavailable` (`config.cpp:1425`).
- `dump.load_on_startup: true` and the dump cannot be read, fails an integrity or identity check, or the MySQL source UUID is unavailable — logged as `startup_dump_load_failed` with `action: fallback_to_mysql_snapshot`, and startup continues by building a snapshot from MySQL (`server_orchestrator.cpp:621`, `server_orchestrator.cpp:628`).
- `logging.level` outside the enum — logged as "Unknown log level, keeping current level" and startup continues (`configuration_manager.cpp:183`). Unreachable through a schema-validated configuration, because the schema enum rejects the value first.

## Known divergences

Each item states both sides with citations. No fixes are proposed.

1. **`api.tcp.worker_threads` auto-sizing formula.** The schema description and the header comment both say `0` means `max(hardware_concurrency() * 4, 64)` (`config-schema.json:608`, `config.h:365`). The implementation computes `max(hardware_concurrency() * 2, 4)`, substituting 4 for `hardware_concurrency()` when the runtime reports 0 (`thread_pool.cpp:32`–`thread_pool.cpp:39`).

2. **`replication.server_id` is unchecked when the `replication` block is omitted.** The schema's `if`/`then` requiring `server_id` lives inside the `replication` subschema (`config-schema.json:449`), so it does not apply when the block is absent. The parser's `server_id != 0` check is likewise inside `if (root.contains("replication"))` (`config.cpp:1001`, check at `config.cpp:1026`). With `replication` omitted the configuration loads cleanly with `enable = true` (`config.h:305`) and `server_id = 0` (`config.h:307`); the failure surfaces later when the binlog reader starts (`binlog_reader.cpp:192`).

3. **`--schema` is additive, not an override.** The server's help text says "Use `--schema` only to override with a custom schema" (`command_line_parser.cpp:130`). The validator runs the built-in schema first and only then the custom one, so a custom schema can add constraints but cannot relax any (`config_validator.cpp:210`–`config_validator.cpp:219`).

4. **`tables[].required_filters[].operator` alias is unreachable.** The parser accepts `operator` as a synonym for `op` (`config.cpp:620`), but the required-filter schema sets `additionalProperties: false` and does not declare it (`config-schema.json:194`).

5. **`mysql` description omits two environment variables.** The schema's `mysql` description lists `MYGRAM_MYSQL_USER`, `MYGRAM_MYSQL_PASSWORD`, `MYGRAM_MYSQL_HOST`, and `MYGRAM_MYSQL_DATABASE` (`config-schema.json:12`). The parser also honors `MYGRAM_MYSQL_PORT` (`config.cpp:421`). Separately, `MYGRAM_API_ADMIN_TOKEN` is documented on `api.admin_token` (`config-schema.json:833`) but is read outside the `mysql` block (`config.cpp:1303`).

6. **`cache.max_memory_mb` has no schema maximum but two code maxima.** The schema declares only `minimum: 1` (`config-schema.json:903`). The parser caps the value at 1048576 MB (`config.cpp:1354`, rejected at `config.cpp:1363`) and additionally rejects anything above 50% of detected physical memory (`config.cpp:1404`, rejected at `config.cpp:1409`).

7. **`api.tcp.keepalive` description cites a design document by path.** The schema description ends with a reference to `docs/ja/design/reactor-io-refactor.md §1.1` (`config-schema.json:643`); no such file exists in the repository.

8. **`api.tcp.worker_threads` is described against a superseded I/O model.** The schema and the header state that each persistent client holds one worker for its entire lifetime, making the worker count the cap on concurrent persistent clients (`config-schema.json:608`, `config.h:361`). The server runs a reactor (`src/server/io_reactor.cpp`, `src/server/reactor_connection.cpp`) in which persistent connections live in the reactor's connection map and consume a worker only briefly per completed frame (`thread_pool.cpp:23`), so the worker count does not bound concurrent persistent clients.

9. **`api.max_query_length` is bounds-checked against `api.tcp.max_pending_frame_bytes` only at load.** The load path rejects a `max_pending_frame_bytes` below the effective query limit (`config.cpp:174`). The runtime setter validates only the 0–4096 range (`runtime_variable_manager.cpp:352`) and does not re-check the frame-byte cap, so raising `api.max_query_length` at runtime can put the pair into a combination the loader would have refused.

10. **`mysql.datetime_timezone` is validated for format at load but parsed later.** The schema enforces the `±HH:MM` pattern (`config-schema.json:120`); the value is turned into a timezone offset on demand (`config.cpp:1458`), so a schema-passing but otherwise unusable value would surface as a runtime error rather than a load error.

Neither `README.md` nor anything under `docs/` states a configuration default, so there are no README/docs default divergences to report. `examples/config.yaml`, `examples/config.json`, `examples/config-minimal.yaml`, and `examples/config-minimal.json` are sample configurations rather than statements of default values.
