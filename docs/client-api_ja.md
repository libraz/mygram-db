# クライアントAPIの機能対応

CクライアントとC++クライアントは同じwire protocolを使用します。明示的なAPI対応が必要な機能については、次の表を公開契約とします。

| 機能 | C++ API | C API |
| --- | --- | --- |
| リテラル検索 | `Search` | `mygramclient_search` |
| グループ化・boolean式 | `SearchRaw` | `mygramclient_search_raw` |
| ハイライト付きグループ化・boolean式 | `SearchRawWithHighlights` | `mygramclient_search_raw_with_highlights` |
| web式のlossless変換 | `ConvertSearchExpression` | `mygramclient_convert_search_expression` |
| 型付き比較filter、あいまい検索、highlight option | `Search(..., SearchOptions)` | `mygramclient_search_with_options` |
| TCP host、port、要求timeout、受信buffer | `ClientConfig` | `MygramClientConfig_C` または `MygramClientConfigV2_C` |
| Unix domain socket | `ClientConfig::unix_socket_path` | `MygramClientConfigV2_C::unix_socket_path` |
| 非同期DUMP SAVE完了timeout | `ClientConfig::dump_save_timeout_ms` | `MygramClientConfigV2_C::dump_save_timeout_ms` |

利用者が入力した通常の文字列には `Search` を使用してください。`AND`、`FILTER`、`LIMIT` などの単独予約語は自動的に引用されます。`alpha AND (xqz OR jkv)` のようなprotocol式を意図的に実行する場合だけ `SearchRaw` を使用します。

## 各surfaceの検索semantics

typed clientとHTTPはリテラル検索が既定です。boolean syntaxは常に明示的に
選ぶため、applicationがsurfaceを変更しても `alpha AND beta` の意味は
変わりません。

| Surface | リテラルtext | Boolean式 | Filter / fuzzy / highlight |
| --- | --- | --- | --- |
| TCP | 検索tokenをquote | quoteしない式 | protocol clause |
| HTTP | `"mode": "literal"`（既定） | `"mode": "boolean"` | JSON field |
| C++ | `Search` | `SearchRaw` | `SearchOptions` |
| C | `mygramclient_search` | `mygramclient_search_raw` | `MygramSearchOptions_C` |

C options APIでは `MygramSearchOptions_C` をゼロ初期化し、`struct_size` を
設定してください。`limit == 0` はserver既定値を使います。比較filterは
`=`、`!=`、`>`、`>=`、`<`、`<=`、fuzzy distanceは1または2を指定できます。

新しいCプログラムではsize/version付き設定を使用します。

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

Unix domain socketを使う場合は `unix_socket_path` を設定します。この設定はTCPのhost/portより優先されます。`dump_save_timeout_ms` は非同期DUMP SAVEをpollする合計期限です。ゼロの場合はC++クライアントの既定値を使用します。従来の `MygramClientConfig_C` と `mygramclient_create` はABI互換のまま残り、元のTCP設定を引き続き利用できます。

`timeout_ms == 0` と `recv_buffer_size == 0` はC/C++の両APIで既定値を
選びます。16 MiBを超える受信bufferは16 MiBにclampされます。

C handleはthread間で共有できます。error stateへのaccessは同期され、
`mygramclient_get_last_error()` は同じthreadが次にgetterを呼ぶまで有効な
thread-local snapshotを返します。失敗し得るoperationは、書き込み可能な
pointer out-parameterを事前に `NULL`、numeric outputを0に初期化します。

V2には将来fieldを末尾追加できます。呼び出し側は構造体をゼロ初期化し、自身が認識するsizeを `struct_size` に設定してください。libraryはそのsizeを越えるfieldを無視し、未知のversionまたは元のV2 prefixより短い構造体を拒否します。
