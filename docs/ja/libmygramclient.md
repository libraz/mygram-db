# libmygramclient - MygramDB クライアントライブラリ

## 概要

libmygramclientは、MygramDBサーバーに接続してクエリを実行するためのC/C++クライアントライブラリです。モダンなC++ APIと、バインディング（node-gypなど）に適したC APIの両方を提供します。

## 機能

- すべてのMygramDBプロトコルコマンドを完全サポート
- RAIIと型安全性を備えたモダンなC++17 API
- 他の言語との統合が容易なC API
- スレッドセーフな接続管理
- 静的ライブラリと共有ライブラリの両方をビルド
- 4バイトUTF-8文字（絵文字：😀🎉👍）を含む完全なUnicodeサポート

## ビルド

ライブラリはMygramDBと一緒に自動的にビルドされます：

```bash
make
```

これにより以下が作成されます：

- `build/lib/libmygramclient.a` - 静的ライブラリ
- `build/lib/libmygramclient.dylib` (macOS) または `libmygramclient.so` (Linux) - 共有ライブラリ

## インストール

```bash
sudo make install
```

これにより以下がインストールされます：

- ヘッダーファイル：`/usr/local/include/mygramdb/`
- ライブラリ：`/usr/local/lib/`

## C++ API

### 基本的な使い方

```cpp
#include <mygramdb/mygramclient.h>
#include <iostream>

using namespace mygramdb::client;

int main() {
    // クライアント設定
    ClientConfig config;
    config.host = "localhost";
    config.port = 11211;
    config.timeout_ms = 5000;

    // クライアント作成
    MygramClient client(config);

    // 接続
    if (auto err = client.Connect()) {
        std::cerr << "接続失敗: " << *err << std::endl;
        return 1;
    }

    // 検索
    auto result = client.Search("articles", "hello world", 100);
    if (auto* err = std::get_if<Error>(&result)) {
        std::cerr << "検索失敗: " << err->message << std::endl;
        return 1;
    }

    auto resp = std::get<SearchResponse>(result);
    std::cout << resp.total_count << "件の結果を発見\n";
    for (const auto& doc : resp.results) {
        std::cout << "  - " << doc.primary_key << "\n";
    }

    return 0;
}
```

### libmygramclientを使ってコンパイル

```bash
g++ -std=c++17 -o myapp myapp.cpp -lmygramclient
```

### 高度な検索

```cpp
// AND、NOT、FILTERを使った検索
std::vector<std::string> and_terms = {"AI"};
std::vector<std::string> not_terms = {"old"};
std::vector<std::pair<std::string, std::string>> filters = {
    {"status", "active"},
    {"category", "tech"}
};

auto result = client.Search(
    "articles",           // テーブル名
    "technology",         // クエリ
    50,                   // 上限
    0,                    // オフセット
    and_terms,            // AND条件
    not_terms,            // NOT条件
    filters,              // フィルター
    "created_at",         // ソート列
    true                  // 降順
);
```

### COUNT クエリ

```cpp
auto result = client.Count("articles", "hello");
if (auto* resp = std::get_if<CountResponse>(&result)) {
    std::cout << "合計マッチ数: " << resp->count << "\n";
}
```

### ドキュメント取得

```cpp
auto result = client.Get("articles", "12345");
if (auto* doc = std::get_if<Document>(&result)) {
    std::cout << "プライマリキー: " << doc->primary_key << "\n";
    for (const auto& [key, value] : doc->fields) {
        std::cout << "  " << key << " = " << value << "\n";
    }
}
```

### サーバー情報

```cpp
auto result = client.Info();
if (auto* info = std::get_if<ServerInfo>(&result)) {
    std::cout << "バージョン: " << info->version << "\n";
    std::cout << "ドキュメント数: " << info->doc_count << "\n";
    std::cout << "稼働時間: " << info->uptime_seconds << "秒\n";
}
```

### デバッグモード

```cpp
// デバッグモード有効化
client.EnableDebug();

auto result = client.Search("articles", "hello", 10);
if (auto* resp = std::get_if<SearchResponse>(&result)) {
    if (resp->debug) {
        std::cout << "クエリ時間: " << resp->debug->query_time_ms << "ms\n";
        std::cout << "候補数: " << resp->debug->candidates << "\n";
    }
}

// デバッグモード無効化
client.DisableDebug();
```

### 検索式パーサー

ライブラリには、Googleライクな検索構文をMygramDBクエリ形式に変換する、Web検索スタイルの検索式パーサーが含まれています。

#### 構文

- `term1 term2` - スペース区切りで暗黙的AND（両方とも必ず含まれる）
- `"phrase"` - ダブルクォートでフレーズ検索（スペースを含む完全一致）
- `+term` - 明示的な必須語句（プレフィックスなしと同じ）
- `-term` - 除外語句（結果に含まれない）
- `term1 OR term2` - 語句間の論理OR
- `(expr)` - 括弧によるグループ化
- 全角スペース（`　`）も区切りとしてサポート（日本語テキストに便利）

#### 例

```cpp
#include <mygramdb/search_expression.h>

using namespace mygramdb::client;

// 暗黙的AND（スペース区切り）
auto result = ParseSearchExpression("golang tutorial");
auto expr = std::get<SearchExpression>(result);
// expr.required_terms = ["golang", "tutorial"]

// フレーズ検索（ダブルクォート）
auto result = ParseSearchExpression("\"機械学習\" チュートリアル");
auto expr = std::get<SearchExpression>(result);
// expr.required_terms = ["\"機械学習\"", "チュートリアル"]

// 除外語句
auto result = ParseSearchExpression("golang -old -deprecated");
auto expr = std::get<SearchExpression>(result);
// expr.required_terms = ["golang"]
// expr.excluded_terms = ["old", "deprecated"]

// OR式
auto result = ParseSearchExpression("python OR ruby");
auto expr = std::get<SearchExpression>(result);
// expr.raw_expression = "python OR ruby"

// ダブルクォート付き複雑な式
auto result = ParseSearchExpression("\"ディープラーニング\" +(チュートリアル OR ガイド) -古い");
auto expr = std::get<SearchExpression>(result);
// expr.required_terms = ["\"ディープラーニング\"", "(チュートリアル OR ガイド)"]
// expr.excluded_terms = ["古い"]

// 全角スペース
auto result = ParseSearchExpression("機械学習　チュートリアル");
auto expr = std::get<SearchExpression>(result);
// expr.required_terms = ["機械学習", "チュートリアル"]

// 絵文字検索（4バイトUTF-8文字）
auto result = ParseSearchExpression("😀 tutorial -😢");
auto expr = std::get<SearchExpression>(result);
// expr.required_terms = ["😀", "tutorial"]
// expr.excluded_terms = ["😢"]
```

#### クエリ文字列への変換

```cpp
// QueryAST互換の文字列に直接変換
auto result = ConvertSearchExpression("+golang -old");
if (result.index() == 0) {
    std::string query = std::get<0>(result);
    // query = "golang AND NOT old"

    // MygramClientで使用
    auto search_result = client.Search("articles", query, 100);
}
```

#### 式の例

| 入力 | 出力クエリ | 説明 |
|-------|-------------|-------------|
| `golang tutorial` | `golang AND tutorial` | 暗黙的AND - 両方の語句が必須 |
| `"機械学習"` | `"機械学習"` | フレーズ検索 |
| `golang -old` | `golang AND NOT old` | 「golang」を含み、「old」を含まない |
| `python OR ruby` | `(python OR ruby)` | 「python」または「ruby」のいずれか |
| `"深層学習" チュートリアル` | `"深層学習" AND チュートリアル` | フレーズと語句 |
| `golang +(tutorial OR guide)` | `golang AND (tutorial OR guide)` | 「golang」AND（「tutorial」OR「guide」） |
| `AI machine -learning` | `AI AND machine AND NOT learning` | 「AI」と「machine」を含み、「learning」を除外 |
| `機械学習　チュートリアル` | `機械学習 AND チュートリアル` | 全角スペース区切り |
| `😀 tutorial -😢` | `😀 AND tutorial AND NOT 😢` | 絵文字検索（4バイトUTF-8） |

#### 簡易API（後方互換性）

OR/グループ化を使わない単純なユースケース向け：

```cpp
std::string main_term;
std::vector<std::string> and_terms;
std::vector<std::string> not_terms;

bool success = SimplifySearchExpression(
    "+golang +tutorial -old",
    main_term,
    and_terms,
    not_terms
);

if (success) {
    // main_term = "golang"
    // and_terms = ["tutorial"]
    // not_terms = ["old"]
}
```

**注意:** ORと括弧を含む複雑な式は、`SimplifySearchExpression()`を使用すると意味が失われます。

## C API

### 基本的な使い方

```c
#include <mygramdb/mygramclient_c.h>
#include <stdio.h>

int main() {
    // クライアント設定
    MygramClientConfig_C config = {
        .host = "localhost",
        .port = 11211,
        .timeout_ms = 5000,
        .recv_buffer_size = 65536
    };

    // クライアント作成
    MygramClient_C* client = mygramclient_create(&config);
    if (!client) {
        fprintf(stderr, "クライアント作成失敗\n");
        return 1;
    }

    // 接続
    if (mygramclient_connect(client) != 0) {
        fprintf(stderr, "接続失敗: %s\n",
                mygramclient_get_last_error(client));
        mygramclient_destroy(client);
        return 1;
    }

    // 検索
    MygramSearchResult_C* result = NULL;
    if (mygramclient_search(client, "articles", "hello", 100, 0, &result) == 0) {
        printf("%llu件の結果を発見（%zu件表示）:\n",
               result->total_count, result->count);
        for (size_t i = 0; i < result->count; i++) {
            printf("  - %s\n", result->primary_keys[i]);
        }
        mygramclient_free_search_result(result);
    } else {
        fprintf(stderr, "検索失敗: %s\n",
                mygramclient_get_last_error(client));
    }

    // クリーンアップ
    mygramclient_disconnect(client);
    mygramclient_destroy(client);

    return 0;
}
```

### Cプログラムのコンパイル

```bash
gcc -o myapp myapp.c -lmygramclient
```

### 高度な検索（C API）

```c
const char* and_terms[] = {"AI"};
const char* not_terms[] = {"old"};
const char* filter_keys[] = {"status", "category"};
const char* filter_values[] = {"active", "tech"};

MygramSearchResult_C* result = NULL;
int ret = mygramclient_search_advanced(
    client,
    "articles",           // テーブル名
    "technology",         // クエリ
    50,                   // 上限
    0,                    // オフセット
    and_terms, 1,         // AND条件
    not_terms, 1,         // NOT条件
    filter_keys, filter_values, 2,  // フィルター
    "created_at",         // ソート列
    1,                    // 降順
    &result
);

if (ret == 0) {
    // 結果を処理
    mygramclient_free_search_result(result);
}
```

## Node.jsバインディングの例

node-gypでC APIを使用：

```javascript
// binding.gyp
{
  "targets": [{
    "target_name": "mygramdb",
    "sources": [ "src/mygramdb_node.cpp" ],
    "include_dirs": [
      "/usr/local/include",
      "<!(node -p \"require('node-addon-api').include_dir\")"
    ],
    "libraries": [
      "-L/usr/local/lib",
      "-lmygramclient"
    ],
    "cflags!": [ "-fno-exceptions" ],
    "cflags_cc!": [ "-fno-exceptions" ],
    "defines": [ "NAPI_DISABLE_CPP_EXCEPTIONS" ]
  }]
}
```

```cpp
// src/mygramdb_node.cpp（簡略化した例）
#include <napi.h>
#include <mygramdb/mygramclient_c.h>

Napi::Value Search(const Napi::CallbackInfo& info) {
    Napi::Env env = info.Env();

    // パラメータ取得
    std::string table = info[0].As<Napi::String>();
    std::string query = info[1].As<Napi::String>();
    uint32_t limit = info[2].As<Napi::Number>().Uint32Value();

    // クライアント作成と接続
    MygramClientConfig_C config = {
        .host = "localhost",
        .port = 11211,
        .timeout_ms = 5000,
        .recv_buffer_size = 65536
    };

    MygramClient_C* client = mygramclient_create(&config);
    if (mygramclient_connect(client) != 0) {
        Napi::Error::New(env, mygramclient_get_last_error(client)).ThrowAsJavaScriptException();
        mygramclient_destroy(client);
        return env.Null();
    }

    // 検索
    MygramSearchResult_C* result = NULL;
    if (mygramclient_search(client, table.c_str(), query.c_str(), limit, 0, &result) != 0) {
        Napi::Error::New(env, mygramclient_get_last_error(client)).ThrowAsJavaScriptException();
        mygramclient_destroy(client);
        return env.Null();
    }

    // JavaScript配列に変換
    Napi::Array jsResults = Napi::Array::New(env, result->count);
    for (size_t i = 0; i < result->count; i++) {
        jsResults[i] = Napi::String::New(env, result->primary_keys[i]);
    }

    // クリーンアップ
    mygramclient_free_search_result(result);
    mygramclient_disconnect(client);
    mygramclient_destroy(client);

    return jsResults;
}

Napi::Object Init(Napi::Env env, Napi::Object exports) {
    exports.Set("search", Napi::Function::New(env, Search));
    return exports;
}

NODE_API_MODULE(mygramdb, Init)
```

## APIリファレンス

### C++ APIクラス

#### ClientConfig

- `host` - サーバーホスト名（デフォルト: "127.0.0.1"）
- `port` - サーバーポート（デフォルト: 11211）
- `timeout_ms` - 接続タイムアウト（デフォルト: 5000）
- `recv_buffer_size` - 受信バッファサイズ（デフォルト: 65536）

#### SearchResponse

- `results` - SearchResultのベクター
- `total_count` - マッチしたドキュメントの総数
- `debug` - オプションのデバッグ情報

#### Error

- `message` - エラーメッセージ文字列

### C API関数

完全な関数ドキュメントは `mygramclient_c.h` を参照してください。

主な関数：

- `mygramclient_create()` - クライアント作成
- `mygramclient_connect()` - サーバーに接続
- `mygramclient_search()` - シンプル検索
- `mygramclient_search_advanced()` - フィルター付き高度な検索
- `mygramclient_count()` - マッチ数カウント
- `mygramclient_get()` - キーでドキュメント取得
- `mygramclient_free_*()` - 結果構造体の解放

## スレッド安全性

MygramClientクラスは単一のTCP接続を管理し、**スレッドセーフではありません**。マルチスレッドアプリケーションでは、スレッドごとに1つのクライアントインスタンスを作成するか、適切な同期を使用してください。

## エラー処理

### C++ API

関数は `std::variant<T, Error>` を返します：

- 成功: 結果型Tを含む
- 失敗: メッセージ付きのErrorを含む

エラーチェックには `std::get_if<Error>` を使用：

```cpp
auto result = client.Search(...);
if (auto* err = std::get_if<Error>(&result)) {
    // エラー処理: err->message
} else {
    auto resp = std::get<SearchResponse>(result);
    // respを使用
}
```

### C API

関数は成功時に0、エラー時に-1を返します。エラーメッセージの取得には `mygramclient_get_last_error()` を使用してください。

## ライセンス

MITライセンス（LICENSEファイル参照）
