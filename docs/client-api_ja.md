# クライアントAPIの機能対応

CクライアントとC++クライアントは同じwire protocolを使用します。明示的なAPI対応が必要な機能については、次の表を公開契約とします。

| 機能 | C++ API | C API |
| --- | --- | --- |
| リテラル検索 | `Search` | `mygramclient_search` |
| グループ化・boolean式 | `SearchRaw` | `mygramclient_search_raw` |
| ハイライト付きグループ化・boolean式 | `SearchRawWithHighlights` | `mygramclient_search_raw_with_highlights` |
| web式のlossless変換 | `ConvertSearchExpression` | `mygramclient_convert_search_expression` |
| 式解析の失敗診断 | typed `Error` | `mygramclient_parse_search_expression_ex` / `mygramclient_convert_search_expression_ex` |
| 型付きliteral/boolean mode、比較filter、fuzzy、highlight | `Search(..., SearchOptions)` | `mygramclient_search_with_options` |
| facetのpaginationとdistinct値総数 | `Facet(..., offset)` / `FacetResponse::total_count` | `mygramclient_facet_paged` / `MygramFacetResult_C::total_count` |
| TCP host、port、要求timeout、受信buffer | `ClientConfig` | `MygramClientConfig_C` または `MygramClientConfigV2_C` |
| Unix domain socket | `ClientConfig::unix_socket_path` | `MygramClientConfigV2_C::unix_socket_path` |
| 接続timeout | `ClientConfig::connect_timeout_ms` | `MygramClientConfigV2_C::connect_timeout_ms` |
| 非同期DUMP SAVE完了timeout | `ClientConfig::dump_save_timeout_ms` | `MygramClientConfigV2_C::dump_save_timeout_ms` |
| DUMP LOAD / VERIFY timeout | `ClientConfig::dump_load_timeout_ms` / `dump_verify_timeout_ms` | 対応するV2 field |
| OPTIMIZE timeout | `ClientConfig::optimize_timeout_ms` | `MygramClientConfigV2_C::optimize_timeout_ms` |

利用者が入力した通常の文字列には `Search` を使用してください。`AND`、`FILTER`、`LIMIT` などの単独予約語は自動的に引用されます。`alpha AND (xqz OR jkv)` のような意図的な式を型付きfilter、sort、fuzzy、highlightと組み合わせる場合は、`SearchOptions::query_mode` に `QueryMode::kBoolean` を設定します。`SearchRaw` は式だけを渡す簡潔なAPIとして残ります。

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
設定してください。`limit == 0` はserver既定値を使います。ゼロ既定の
`MYGRAM_QUERY_LITERAL` はliteral検索を維持し、boolean式には
`MYGRAM_QUERY_BOOLEAN` を指定します。比較filterは `=`、`!=`、`>`、`>=`、
`<`、`<=`、fuzzy distanceは1または2を指定できます。

facet navigationでは、C++ callerは `Facet` の最後の引数に `offset` を渡します。
C callerは `mygramclient_facet_paged`、AND/NOT/FILTER clauseも必要な場合は
`mygramclient_facet_advanced_paged` を使用します。`facets.size()` / `count` は
返却page内の値数、`total_count` はOFFSET/LIMIT適用前のdistinct値総数です。
既存のC facet関数も残り、offset 0として動作します。

standaloneのC式helperはclient handleを取りません。具体的な解析診断が必要な場合は
`_ex` variantを使用してください。失敗時の `diagnostic` はallocateされるため、
`mygramclient_free_string` で解放します。既存関数はABI互換の診断なしwrapperとして
残ります。

新しいCプログラムではsize/version付き設定を使用します。

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

Unix domain socketを使う場合は `unix_socket_path` を設定します。この設定はTCPのhost/portより優先されます。`timeout_ms` は部分受信ごとに延長されない通常command全体の期限です。`connect_timeout_ms` は接続確立の期限を要求実行から分離します。DUMP SAVE、DUMP LOAD、DUMP VERIFY、OPTIMIZEには、通常要求より長時間かかり得るため操作別の期限があります。`max_response_bytes` は1 response frameの上限です。V2のゼロfieldはC++クライアントの既定値を使用し、C++ APIで操作別timeoutをゼロにすると `timeout_ms` へfallbackします。従来の `MygramClientConfig_C` と `mygramclient_create` はABI互換のまま残り、元のTCP設定を引き続き利用できます。

`timeout_ms == 0` と `recv_buffer_size == 0` はC/C++の両APIで既定値を
選びます。16 MiBを超える受信bufferは16 MiBにclampされます。

C handleはthread間で共有できます。接続lifecycle callとcommandはhandle上で
直列化され、disconnectは実行中commandをcancelせず完了まで待ちます。別threadが
handleを使用中に `mygramclient_destroy()` を呼ばないでください。error stateへの
accessは同期され、`mygramclient_get_last_error()` は同じthreadが次にgetterを
呼ぶまで有効なthread-local snapshotを返します。失敗し得るoperationは、書き込み
可能なpointer out-parameterを事前に `NULL`、numeric outputを0に初期化します。

V2には将来fieldを末尾追加できます。呼び出し側は構造体をゼロ初期化し、自身が認識するsizeを `struct_size` に設定してください。libraryはそのsizeを越えるfieldを無視し、未知のversionまたは元のV2 prefixより短い構造体を拒否します。

C/C++のconnect、search、result走査を一通り示すexampleは
`share/doc/mygramdb/examples/client` にinstallされます。同梱の
`CMakeLists.txt` はinstall済み `MygramDBClient` packageを直接使用します。
