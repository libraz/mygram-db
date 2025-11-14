# 設定ガイド

MygramDB は **YAML** と **JSON** の両方の設定フォーマットに対応しており、自動的に **JSON Schema 検証**を行います。このガイドでは、利用可能なすべての設定オプションを説明します。

## 設定ファイルのフォーマット

MygramDB はファイル拡張子に基づいて設定フォーマットを自動検出します：

- `.yaml` または `.yml` → YAML 形式
- `.json` → JSON 形式

すべての設定は起動時に組み込みの JSON Schema に対して自動的に検証され、不正な設定（タイポ、間違った型、未知のキー）が即座に検出されます。

## 設定ファイルの構造

YAML または JSON 形式で設定ファイルを作成します：

### YAML 形式 (config.yaml)

```yaml
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "repl_user"
  password: "your_password_here"
  database: "mydb"
  use_gtid: true
  binlog_format: "ROW"
  binlog_row_image: "FULL"
  connect_timeout_ms: 3000

tables:
  - name: "articles"
    primary_key: "id"
    text_source:
      column: "content"
    required_filters: []
    filters: []
    ngram_size: 2
    kanji_ngram_size: 1
    posting:
      block_size: 128
      freq_bits: 0
      use_roaring: "auto"

build:
  mode: "select_snapshot"
  batch_size: 5000
  parallelism: 2
  throttle_ms: 0

replication:
  enable: true
  server_id: 12345
  start_from: "snapshot"
  queue_size: 10000
  reconnect_backoff_min_ms: 500
  reconnect_backoff_max_ms: 10000

memory:
  hard_limit_mb: 8192
  soft_target_mb: 4096
  arena_chunk_mb: 64
  roaring_threshold: 0.18
  minute_epoch: true
  normalize:
    nfkc: true
    width: "narrow"
    lower: false

dump:
  dir: "/var/lib/mygramdb/dumps"
  default_filename: "mygramdb.dmp"
  interval_sec: 600
  retain: 3

api:
  tcp:
    bind: "0.0.0.0"
    port: 11016
  http:
    enable: true
    bind: "127.0.0.1"
    port: 8080
  default_limit: 100
  max_query_length: 128

network:
  allow_cidrs: []

logging:
  level: "info"
  json: true
```

### JSON 形式 (config.json)

JSON 形式での同じ設定：

```json
{
  "mysql": {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "repl_user",
    "password": "your_password_here",
    "database": "mydb",
    "use_gtid": true,
    "binlog_format": "ROW",
    "binlog_row_image": "FULL",
    "connect_timeout_ms": 3000
  },
  "tables": [
    {
      "name": "articles",
      "primary_key": "id",
      "text_source": {
        "column": "content"
      },
      "required_filters": [],
      "filters": [],
      "ngram_size": 2,
      "kanji_ngram_size": 1,
      "posting": {
        "block_size": 128,
        "freq_bits": 0,
        "use_roaring": "auto"
      }
    }
  ],
  "build": {
    "mode": "select_snapshot",
    "batch_size": 5000,
    "parallelism": 2,
    "throttle_ms": 0
  },
  "replication": {
    "enable": true,
    "server_id": 12345,
    "start_from": "snapshot",
    "queue_size": 10000,
    "reconnect_backoff_min_ms": 500,
    "reconnect_backoff_max_ms": 10000
  },
  "memory": {
    "hard_limit_mb": 8192,
    "soft_target_mb": 4096,
    "arena_chunk_mb": 64,
    "roaring_threshold": 0.18,
    "minute_epoch": true,
    "normalize": {
      "nfkc": true,
      "width": "narrow",
      "lower": false
    }
  },
  "dump": {
    "dir": "/var/lib/mygramdb/dumps",
    "default_filename": "mygramdb.dmp",
    "interval_sec": 600,
    "retain": 3
  },
  "api": {
    "tcp": {
      "bind": "0.0.0.0",
      "port": 11016
    },
    "http": {
      "enable": true,
      "bind": "127.0.0.1",
      "port": 8080
    },
    "default_limit": 100,
    "max_query_length": 128
    }
  },
  "network": {
    "allow_cidrs": []
  },
  "logging": {
    "level": "info",
    "json": true
  }
}
```

**注意:** このガイドのすべての例は可読性のため YAML 形式を使用していますが、すべての設定は JSON 形式でも使用できます。

## MySQL セクション

MySQL サーバーの接続設定：

- **host**: MySQL サーバーのホスト名または IP アドレス（デフォルト: `127.0.0.1`）
- **port**: MySQL サーバーのポート（デフォルト: `3306`）
- **user**: レプリケーション用の MySQL ユーザー名（必須）
- **password**: MySQL ユーザーのパスワード（必須）
- **database**: データベース名（必須）
- **use_gtid**: GTID ベースレプリケーションを有効化（デフォルト: `true`、レプリケーションに必須）
- **binlog_format**: バイナリログ形式（デフォルト: `ROW`、レプリケーションに必須）
- **binlog_row_image**: 行イメージ形式（デフォルト: `FULL`、レプリケーションに必須）
- **connect_timeout_ms**: 接続タイムアウト（ミリ秒、デフォルト: `3000`）

## Tables セクション

テーブル設定（インスタンスごとに1テーブルをサポート）：

### 基本設定

- **name**: MySQL データベースのテーブル名（必須）
- **primary_key**: プライマリキーのカラム名（デフォルト: `id`、単一カラムである必要があります）
- **ngram_size**: ASCII/英数字文字の N-gram サイズ（デフォルト: `2`）
  - 1 = ユニグラム、2 = バイグラム等
  - 多言語混在コンテンツ: 2 を推奨
  - 英語のみ: 3 以上を使用
- **kanji_ngram_size**: CJK文字（漢字/仮名/汉字）の N-gram サイズ（デフォルト: `0`）
  - 0 に設定または省略すると、すべての文字に `ngram_size` の値を使用
  - 日本語/中国語テキスト: 1（ユニグラム）を推奨
  - ハイブリッドトークン化: ASCII文字とCJK文字で異なるN-gramサイズを使用可能

### Text Source

全文検索用にインデックス化するカラムを定義：

**単一カラム:**

```yaml
text_source:
  column: "content"
  delimiter: " "                    # デフォルト: " "（concat 指定時に使用）
```

**複数カラム（連結）:**

```yaml
text_source:
  concat: ["title", "body"]
  delimiter: " "
```

### 必須フィルタ（データ存在条件）

必須フィルタは、データをインデックスに含めるための条件を定義します。この条件に合致しないデータは**一切インデックス化されません**。

Binlogレプリケーション時の動作：
- この条件から**外れた**データはインデックスから**削除**されます
- この条件に**入った**データはインデックスに**追加**されます
- この条件内で変更されたデータは通常通り更新されます

```yaml
required_filters:
  - name: "enabled"                 # カラム名
    type: "int"                     # カラム型（下記参照）
    op: "="                         # 演算子
    value: 1                        # 比較値
    bitmap_index: false             # 検索時フィルタリング用のビットマップインデックスを有効化

  - name: "deleted_at"
    type: "datetime"
    op: "IS NULL"                   # 削除されていないレコードのみインデックス化
    bitmap_index: false
```

**サポートされる演算子:**

- 比較演算子: `=`, `!=`, `<`, `>`, `<=`, `>=`
- NULL チェック: `IS NULL`, `IS NOT NULL`

**重要な注意事項:**

- すべての `required_filters` 条件は AND ロジックで結合されます
- `IS NULL` および `IS NOT NULL` 演算子の場合、`value` フィールドは省略する必要があります
- `required_filters` で使用するカラムはテーブルスキーマに含まれている必要があります
- これらのフィルタはスナップショットビルド時と Binlog レプリケーション時の両方で評価されます

### オプションフィルタ（検索時フィルタリング）

オプションフィルタは検索時のフィルタリングに使用されます。データのインデックス化には**影響しません**。

```yaml
filters:
  - name: "status"
    type: "int"
    dict_compress: false            # デフォルト: false
    bitmap_index: false             # デフォルト: false

  - name: "category"
    type: "string"
    dict_compress: false
    bitmap_index: false

  - name: "created_at"
    type: "datetime"
    bucket: "minute"                # オプション: "minute", "hour", "day"
```

**フィルター型:**

- 整数型: `tinyint`, `tinyint_unsigned`, `smallint`, `smallint_unsigned`, `int`, `int_unsigned`, `mediumint`, `mediumint_unsigned`, `bigint`
- 浮動小数点型: `float`, `double`
- 文字列型: `string`, `varchar`, `text`
- 日付型: `datetime`, `date`, `timestamp`

**フィルターオプション:**

- **dict_compress**: 辞書圧縮を有効化（低カーディナリティカラム推奨）
- **bitmap_index**: フィルター高速化のためのビットマップインデックスを有効化
- **bucket**: 日時のバケット化（`minute`, `hour`, `day`）でカーディナリティを削減

### Posting List 設定

転置インデックスの保存方法を制御：

```yaml
posting:
  block_size: 128                   # デフォルト: 128
  freq_bits: 0                      # 0=ブール値、4 または 8 で語頻度（デフォルト: 0）
  use_roaring: "auto"               # "auto", "always", "never"（デフォルト: auto）
```

## Build セクション

インデックスビルド設定：

- **mode**: ビルドモード（デフォルト: `select_snapshot`、現在唯一のオプション）
- **batch_size**: スナップショット時の1バッチあたりの行数（デフォルト: `5000`）
- **parallelism**: 並列ビルドスレッド数（デフォルト: `2`）
- **throttle_ms**: バッチ間の遅延（ミリ秒、デフォルト: `0`）

## Replication セクション

MySQL binlog レプリケーション設定：

- **enable**: binlog レプリケーションを有効化（デフォルト: `true`）
- **server_id**: MySQL サーバー ID（必須、レプリケーショントポロジー内でゼロ以外の一意の値である必要があります）
  - ランダムな数値を生成するか、環境に応じた一意の値を使用
  - 例: `12345`
- **start_from**: レプリケーション開始位置（デフォルト: `snapshot`）
  - `snapshot`: スナップショット GTID から開始（推奨）
  - `latest`: 現在の GTID から開始
  - `gtid=<UUID:txn>`: 特定の GTID から開始（例: `gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:1`）
- **queue_size**: Binlog イベントキューサイズ（デフォルト: `10000`）
- **reconnect_backoff_min_ms**: 最小再接続バックオフ遅延（デフォルト: `500`）
- **reconnect_backoff_max_ms**: 最大再接続バックオフ遅延（デフォルト: `10000`）

## Memory セクション

メモリ管理設定：

- **hard_limit_mb**: ハードメモリ制限（MB、デフォルト: `8192`）
- **soft_target_mb**: ソフトメモリ目標（MB、デフォルト: `4096`）
- **arena_chunk_mb**: アリーナチャンクサイズ（MB、デフォルト: `64`）
- **roaring_threshold**: Roaring ビットマップ閾値（デフォルト: `0.18`）
- **minute_epoch**: 分精度のエポックを使用（デフォルト: `true`）

### テキスト正規化

```yaml
normalize:
  nfkc: true                        # デフォルト: true、NFKC 正規化
  width: "narrow"                   # "keep", "narrow", "wide"（デフォルト: narrow）
  lower: false                      # 小文字変換（デフォルト: false）
```

## Dump セクション

ダンプ（スナップショット）の永続化設定と自動バックアップ：

- **dir**: ダンプディレクトリのパス（デフォルト: `/var/lib/mygramdb/dumps`）
  - ディレクトリが存在しない場合は起動時に自動作成されます
  - 起動時に書き込み権限が検証されます
- **default_filename**: 手動`DUMP SAVE`コマンドのデフォルトファイル名（デフォルト: `mygramdb.dmp`）
- **interval_sec**: 自動保存間隔（秒、デフォルト: `600`、`0` = 無効）
  - 自動ダンプはタイムスタンプベースのファイル名で保存されます：`auto_YYYYMMDD_HHMMSS.dmp`
  - Redis RDB永続化と同様の仕組み
- **retain**: 保持する自動保存ダンプ数（デフォルト: `3`）
  - 古い自動保存ファイルは自動的にクリーンアップされます
  - 手動ダンプ（`DUMP SAVE`コマンド経由）はクリーンアップの影響を受けません

## API セクション

API サーバー設定：

### TCP API

```yaml
api:
  tcp:
    bind: "0.0.0.0"                 # デフォルト: 0.0.0.0（全インターフェース）
    port: 11016                     # デフォルト: 11016
```

### HTTP API（オプション）

```yaml
api:
  http:
    enable: true                    # デフォルト: true
    bind: "127.0.0.1"               # デフォルト: 127.0.0.1（ローカルホストのみ）
    port: 8080                      # デフォルト: 8080
  default_limit: 100                # LIMIT 省略時のデフォルト (5-1000)
  max_query_length: 128             # クエリ式の最大長 (0 = 無制限)

### クエリ関連のデフォルト

- **default_limit**: SEARCH で `LIMIT` を指定しない場合に自動的に適用されます。レスポンス肥大化を防ぐため、5〜1000 の範囲で設定可能（既定 100）。
- **max_query_length**: 検索語・AND/NOT 条件・FILTER 値を合計したクエリ式の長さ上限です。既定 128 文字、`0` を設定すると無制限。非常に長いクエリによるリソース消費を抑制します。
```

## Network セクション（オプション）

ネットワークセキュリティ設定：

- **allow_cidrs**: 許可 CIDR リスト（デフォルト: `[]` = すべて許可）
  - 空の場合、すべての IP アドレスが許可されます
  - 指定された場合、これらの IP 範囲からの接続のみが受け入れられます
  - 標準 CIDR 表記をサポート（例: `192.168.1.0/24`, `10.0.0.0/8`）
  - 複数の CIDR 範囲を指定可能

```yaml
network:
  allow_cidrs:
    - "192.168.1.0/24"
    - "10.0.0.0/8"
    - "172.16.0.0/16"
```

**一般的な CIDR 範囲:**

- プライベートネットワーク:
  - `10.0.0.0/8` - クラス A プライベートネットワーク
  - `172.16.0.0/12` - クラス B プライベートネットワーク
  - `192.168.0.0/16` - クラス C プライベートネットワーク
- ローカルホスト: `127.0.0.1/32`
- 単一 IP: `192.168.1.100/32`

## Logging セクション

ロギング設定：

- **level**: ログレベル（デフォルト: `info`）
  - オプション: `debug`, `info`, `warn`, `error`
- **json**: JSON 形式出力（デフォルト: `true`）

```yaml
logging:
  level: "info"
  json: true
```

## Cache セクション

クエリ結果キャッシュの設定：

```yaml
cache:
  enabled: true                       # デフォルト: true（有効）
  max_memory_mb: 32                   # デフォルト: 32MB
  min_query_cost_ms: 10.0             # デフォルト: 10.0ms
  ttl_seconds: 3600                   # デフォルト: 3600（1時間）
  invalidation_strategy: "ngram"      # デフォルト: "ngram"
  compression_enabled: true           # デフォルト: true（LZ4）
  eviction_batch_size: 10             # デフォルト: 10
  invalidation:
    batch_size: 1000                  # デフォルト: 1000
    max_delay_ms: 100                 # デフォルト: 100ms
```

**設定項目：**

- **enabled**: クエリキャッシュの有効/無効（デフォルト: `true`）
- **max_memory_mb**: キャッシュの最大メモリ使用量（MB）（デフォルト: `32`）
  - メモリ制限に達すると、古いエントリが自動的に削除されます
- **min_query_cost_ms**: キャッシュする最小クエリ実行時間（デフォルト: `10.0`）
  - この閾値より長く実行されたクエリのみキャッシュされます
  - 非常に高速なクエリをキャッシュすることを防ぎます
- **ttl_seconds**: キャッシュエントリの有効期間（秒）（デフォルト: `3600`、0 = TTL なし）
  - エントリはこの期間後に自動的に期限切れになります
- **invalidation_strategy**: データ変更時のキャッシュ無効化方法（デフォルト: `"ngram"`）
  - `"ngram"`: 影響を受ける n-gram に一致するエントリを無効化（精密、推奨）
  - `"table"`: テーブルのすべてのエントリを無効化（シンプル、効率は低い）
- **compression_enabled**: キャッシュ結果の LZ4 圧縮を有効化（デフォルト: `true`）
  - 大きな結果セットのメモリ使用量を削減
  - 圧縮/展開による小さな CPU オーバーヘッド
- **eviction_batch_size**: メモリ不足時に一度に削除するエントリ数（デフォルト: `10`）

**無効化キュー設定：**

- **invalidation.batch_size**: N 個のユニークな（テーブル、n-gram）ペア後に無効化を処理（デフォルト: `1000`）
- **invalidation.max_delay_ms**: 無効化処理前の最大遅延（デフォルト: `100`）

**動作原理：**

1. クエリ実行時間が `min_query_cost_ms` を超える場合、初回実行後に結果がキャッシュされます
2. 同一クエリの再実行時は、キャッシュされた結果を即座に返します（1ms 未満）
3. binlog レプリケーション経由でデータが変更された場合：
   - 影響を受ける n-gram が特定されます
   - それらの n-gram に一致するキャッシュエントリが無効化されます
   - 次回のクエリでキャッシュエントリが再構築されます
4. データが変更されていなくても、`ttl_seconds` 後にエントリは期限切れになります

**パフォーマンスへの影響：**

- キャッシュヒット: < 1ms（メモリルックアップ + オプションの展開）
- キャッシュミス: 通常のクエリ時間 + 小さなキャッシュオーバーヘッド
- メモリ使用量: `max_memory_mb` で制御、自動削除あり

**使用を推奨する場合：**

- 頻繁に繰り返されるクエリ（検索候補、人気の検索）
- クエリコストが高いクエリ（複雑なフィルタ、大きな結果セット）
- 読み取り中心のワークロードで繰り返しパターンがある場合

**無効化を推奨する場合：**

- 非常にユニークなクエリパターン（繰り返しが少ない）
- 非常に高速なクエリ（常に < 10ms）
- メモリの制約が厳しい場合

## 自動検証

MygramDB は起動時に組み込みの JSON Schema を使用して、すべての設定ファイル（YAML と JSON）を自動的に検証します。この検証により以下が保証されます：

- **構文の正しさ**: 有効な YAML/JSON 形式
- **型チェック**: すべてのフィールドの正しいデータ型
- **必須フィールド**: すべての必須設定が存在
- **値の制約**: 値が有効な範囲と列挙値内にある
- **未知のキー**: タイポやサポートされていないオプションを検出

検証が失敗した場合、MygramDB は問題の正確な位置を示す詳細なエラーメッセージを表示します。

## 実行時設定ヘルプ

MygramDB は、サーバーを再起動せずに設定オプションを探索し、現在の設定を表示し、設定ファイルを検証するための実行時コマンドを提供します。

### 設定ヘルプの取得

実行時に `CONFIG HELP` コマンドを使用して設定のヘルプを照会できます:

```bash
# すべての設定セクションを表示
echo "CONFIG HELP" | nc localhost 11016
```

**出力例:**
```
+OK
Available configuration sections:
  mysql        - MySQL接続設定
  tables       - テーブル設定（複数テーブル対応）
  build        - インデックスビルド設定
  replication  - レプリケーション設定
  memory       - メモリ管理
  ...
```

### 詳細なヘルプの表示

特定の設定オプションのヘルプを取得:

```bash
# MySQL セクションのヘルプ
echo "CONFIG HELP mysql" | nc localhost 11016

# 特定プロパティのヘルプ
echo "CONFIG HELP mysql.port" | nc localhost 11016
```

これにより以下が表示されます:
- プロパティの型（文字列、整数、ブール値など）
- デフォルト値
- 有効な範囲または許可される値
- 説明
- フィールドが必須かどうか

### 現在の設定の表示

機密フィールドをマスクした実行中の設定を表示:

```bash
# 設定全体を表示
echo "CONFIG SHOW" | nc localhost 11016

# 特定セクションを表示
echo "CONFIG SHOW mysql" | nc localhost 11016

# 特定プロパティを表示
echo "CONFIG SHOW mysql.port" | nc localhost 11016
```

**注意**: 機密フィールド（パスワード、シークレット、キー、トークン）は出力で自動的に `***` としてマスクされます。

### 設定ファイルの検証

デプロイ前に設定ファイルを検証:

```bash
echo "CONFIG VERIFY /path/to/config.yaml" | nc localhost 11016
```

これにより、実行中のサーバーにロードすることなく設定を検証します。設定が有効な場合、サマリーが表示されます:

```
+OK
Configuration is valid
  Tables: 2 (articles, products)
  MySQL: repl_user@127.0.0.1:3306
```

無効な場合、詳細なエラーメッセージが表示されます:

```
-ERR Configuration validation failed:
  - mysql.port: value 99999 exceeds maximum 65535
  - tables[0].name: missing required field
```

完全な CONFIG コマンド構文については、[プロトコルリファレンス](protocol.md#config-コマンド)を参照してください。

---

## 設定のテスト

サーバーを起動する前に、設定ファイルをテストできます:

```bash
# YAML 設定をテスト
./build/bin/mygramdb -t config.yaml

# JSON 設定をテスト
./build/bin/mygramdb -t config.json

# またはロングオプションを使用
./build/bin/mygramdb --config-test config.yaml
```

これにより以下が実行されます:

1. 設定ファイルを解析
2. JSON Schema に対して検証
3. 有効な場合は設定サマリーを表示

設定が有効な場合、以下を表示します:

- MySQL 接続設定
- テーブル設定
- API サーバー設定
- レプリケーション状態
- ロギングレベル

### カスタムスキーマ（上級者向け）

設定の拡張やテストのため、カスタムスキーマに対して検証することができます:

```bash
./build/bin/mygramdb config.yaml --schema custom-schema.json
```

## 設定例

すべての利用可能なオプションを含む完全な設定例:
- YAML: `examples/config.yaml`
- JSON: `examples/config.json`

クイックスタート用の最小限の設定例:
- YAML: `examples/config-minimal.yaml`
- JSON: `examples/config-minimal.json`

各サンプルファイルの詳細については `examples/README.md` を参照してください。

## 使い方

設定ファイルを作成したら、MygramDB を起動できます：

```bash
# ヘルプを表示
./build/bin/mygramdb --help

# バージョンを表示
./build/bin/mygramdb --version

# 設定をテスト
./build/bin/mygramdb -t config.yaml

# サーバーを起動（両方の形式をサポート）
./build/bin/mygramdb -c config.yaml
# または
./build/bin/mygramdb config.yaml
```

**コマンドラインオプション:**
- `-c, --config <file>` - 設定ファイルのパス
- `-t, --config-test` - 設定をテストして終了
- `-h, --help` - ヘルプメッセージを表示
- `-v, --version` - バージョン情報を表示
- `-s, --schema <file>` - カスタム JSON Schema を使用（上級者向け）
