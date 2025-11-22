# MygramDB 設定リファレンス

**バージョン**: 1.0
**最終更新**: 2025-11-18

---

## 目次

1. [概要](#概要)
2. [設定ファイル形式](#設定ファイル形式)
3. [設定セクション](#設定セクション)
   - [MySQL接続](#mysql接続)
   - [テーブル設定](#テーブル設定)
   - [インデックス構築](#インデックス構築)
   - [レプリケーション](#レプリケーション)
   - [メモリ管理](#メモリ管理)
   - [ダンプ永続化](#ダンプ永続化)
   - [APIサーバー](#apiサーバー)
   - [ネットワークセキュリティ](#ネットワークセキュリティ)
   - [ロギング](#ロギング)
   - [クエリキャッシュ](#クエリキャッシュ)
4. [ホットリロード](#ホットリロード)
5. [本番環境での推奨事項](#本番環境での推奨事項)
6. [トラブルシューティング](#トラブルシューティング)

---

## 概要

MygramDBは、サーバーの動作、MySQL接続設定、テーブルインデックスパラメータ、および運用ポリシーを定義するために、YAMLまたはJSON設定ファイルを使用します。

### 主な機能

- **複数の形式**: YAML (`.yaml`, `.yml`) または JSON (`.json`)
- **スキーマ検証**: 組み込みJSON Schemaによる自動検証
- **ホットリロード**: SIGHUP シグナルによるライブ設定更新のサポート
- **環境別設定**: 開発、ステージング、本番環境向けの簡単なカスタマイズ

### 設定ファイルの場所

- **デフォルト**: カレントディレクトリの `config.yaml`
- **カスタム**: `--config=/path/to/config.yaml` コマンドラインオプションで指定

---

## 設定ファイル形式

### YAML形式(推奨)

```yaml
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "repl_user"
  password: "your_password"
  database: "mydb"

tables:
  - name: "articles"
    text_source:
      column: "content"
    ngram_size: 2
```

### JSON形式

```json
{
  "mysql": {
    "host": "127.0.0.1",
    "port": 3306,
    "user": "repl_user",
    "password": "your_password",
    "database": "mydb"
  },
  "tables": [
    {
      "name": "articles",
      "text_source": {
        "column": "content"
      },
      "ngram_size": 2
    }
  ]
}
```

### スキーマ検証

MygramDBは、組み込みのJSON Schemaに対して設定ファイルを自動的に検証します。無効な設定(タイプミス、間違った型、不明なキー)は、起動時に検出され報告されます。

---

## 設定セクション

### MySQL接続

```yaml
mysql:
  host: "127.0.0.1"                 # MySQLサーバーのホスト名またはIP
  port: 3306                        # MySQLサーバーのポート
  user: "repl_user"                 # レプリケーション用のMySQLユーザー名
  password: "your_password_here"    # MySQLユーザーのパスワード
  database: "mydb"                  # データベース名
  use_gtid: true                    # GTIDベースのレプリケーションを有効化(必須)
  binlog_format: "ROW"              # バイナリログ形式(必須: ROW)
  binlog_row_image: "FULL"          # 行イメージ形式(必須: FULL)
  connect_timeout_ms: 3000          # 接続タイムアウト(ミリ秒)
  session_timeout_sec: 3600         # セッションタイムアウト(秒、デフォルト: 3600 = 1時間)
                                    # スナップショット作成などの長時間処理中の切断を防ぐ
  datetime_timezone: "+00:00"       # DATETIME/DATEカラムのタイムゾーン(デフォルト: "+00:00" UTC)
                                    # 形式: [+-]HH:MM (例: "+09:00" JST, "-05:00" EST)

  # SSL/TLS設定(オプションだが本番環境では推奨)
  ssl_enable: false                 # SSL/TLSを有効化
  ssl_ca: "/path/to/ca-cert.pem"    # CA証明書
  ssl_cert: "/path/to/client-cert.pem"  # クライアント証明書
  ssl_key: "/path/to/client-key.pem"    # クライアント秘密鍵
  ssl_verify_server_cert: true      # サーバー証明書を検証
```

#### パラメータ

| パラメータ | 型 | デフォルト | 説明 | ホットリロード |
|-----------|------|---------|-------------|------------|
| `host` | string | `127.0.0.1` | MySQLサーバーのホスト名またはIPアドレス | ✅ 可能 |
| `port` | integer | `3306` | MySQLサーバーのポート | ✅ 可能 |
| `user` | string | *必須* | MySQLユーザー名(REPLICATION SLAVE、REPLICATION CLIENT権限が必要) | ✅ 可能 |
| `password` | string | *必須* | MySQLユーザーのパスワード | ✅ 可能 |
| `database` | string | *必須* | MySQLデータベース名 | ❌ 不可(再起動が必要) |
| `use_gtid` | boolean | `true` | GTIDベースのレプリケーションを有効化(レプリケーションに必須) | ❌ 不可 |
| `binlog_format` | string | `ROW` | バイナリログ形式(ROWである必要がある) | ❌ 不可 |
| `binlog_row_image` | string | `FULL` | 行イメージ形式(FULLである必要がある) | ❌ 不可 |
| `connect_timeout_ms` | integer | `3000` | 接続タイムアウト(ミリ秒) | ✅ 可能 |
| `session_timeout_sec` | integer | `3600` | セッションタイムアウト(秒) - スナップショット作成などの長時間処理中の切断を防ぐ | ✅ 可能 |
| `datetime_timezone` | string | `+00:00` | DATETIME/DATEカラムのタイムゾーン。形式: `[+-]HH:MM`。TIMESTAMPカラムは常にUTC。 | ❌ 不可(再起動が必要) |
| `ssl_enable` | boolean | `false` | SSL/TLS接続を有効化 | ✅ 可能 |
| `ssl_ca` | string | `` | CA証明書ファイルへのパス | ✅ 可能 |
| `ssl_cert` | string | `` | クライアント証明書ファイルへのパス | ✅ 可能 |
| `ssl_key` | string | `` | クライアント秘密鍵ファイルへのパス | ✅ 可能 |
| `ssl_verify_server_cert` | boolean | `true` | サーバー証明書を検証 | ✅ 可能 |

#### MySQLユーザー権限

MySQLユーザーは以下の権限を持っている必要があります:

```sql
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_user'@'%';
FLUSH PRIVILEGES;
```

#### MySQLサーバーの要件

- **GTIDモード**: 有効化されている必要がある(`gtid_mode=ON`)
- **バイナリログ形式**: ROWである必要がある(`binlog_format=ROW`)
- **行イメージ**: FULLである必要がある(`binlog_row_image=FULL`)

以下で確認:

```sql
SHOW VARIABLES LIKE 'gtid_mode';
SHOW VARIABLES LIKE 'binlog_format';
SHOW VARIABLES LIKE 'binlog_row_image';
```

---

### テーブル設定

```yaml
tables:
  - name: "articles"                # MySQLデータベース内のテーブル名
    primary_key: "id"               # プライマリキーカラム名

    # テキストソース設定
    text_source:
      column: "content"             # インデックス対象の単一カラム
      # または
      # concat: ["title", "body"]   # 連結する複数のカラム
      # delimiter: " "              # 連結時の区切り文字

    # 必須フィルタ(データ存在条件)
    required_filters:
      - name: "enabled"             # カラム名
        type: "int"                 # カラムの型
        op: "="                     # 演算子
        value: 1                    # 比較値
        bitmap_index: false         # ビットマップインデックスを有効化

      - name: "deleted_at"
        type: "datetime"
        op: "IS NULL"               # IS NULL/IS NOT NULLには値不要
        bitmap_index: false

    # オプションフィルタ(検索時フィルタリング)
    filters:
      - name: "status"              # カラム名
        type: "int"                 # カラムの型
        dict_compress: false        # 辞書圧縮
        bitmap_index: false         # ビットマップインデックス

      - name: "category"
        type: "string"
        dict_compress: false
        bitmap_index: false

      - name: "created_at"
        type: "datetime"
        # bucket: "minute"          # datetime値をバケット化

    # N-gram設定
    ngram_size: 2                   # ASCII用のN-gramサイズ(1=unigram, 2=bigram)
    kanji_ngram_size: 1             # CJK用のN-gramサイズ(0 = ngram_sizeを使用)

    # ポスティングリスト設定
    posting:
      block_size: 128               # デルタ符号化のブロックサイズ
      freq_bits: 0                  # 単語頻度ビット数: 0、4、または8
      use_roaring: "auto"           # Roaringビットマップの使用: auto、always、never
```

#### テーブルパラメータ

| パラメータ | 型 | デフォルト | 説明 | ホットリロード |
|-----------|------|---------|-------------|------------|
| `name` | string | *必須* | MySQLテーブル名 | ❌ 不可 |
| `primary_key` | string | `id` | プライマリキーカラム名(単一カラムのPRIMARY KEYまたはUNIQUE KEYである必要がある) | ❌ 不可 |
| `text_source.column` | string | *concatが未設定の場合必須* | 全文検索用にインデックス化する単一カラム | ❌ 不可 |
| `text_source.concat` | array | *columnが未設定の場合必須* | インデックス化のために連結する複数のカラム | ❌ 不可 |
| `text_source.delimiter` | string | ` ` (スペース) | 複数カラム連結時の区切り文字 | ❌ 不可 |
| `ngram_size` | integer | `2` | ASCII/英数字用のN-gramサイズ(1-4を推奨) | ⚠️ 部分的(キャッシュクリア) |
| `kanji_ngram_size` | integer | `0` | CJK文字用のN-gramサイズ(0 = ngram_sizeを使用、1-2を推奨) | ⚠️ 部分的(キャッシュクリア) |

#### 必須フィルタとオプションフィルタの違い

**必須フィルタ**(データ存在条件):
- どの行をインデックス化するかを定義
- これらの条件に一致しないデータは**インデックスから除外**される
- binlogレプリケーション中に変更があると**追加/削除**操作がトリガーされる
- 使用例:
  - 公開された記事のみインデックス化: `status = 'published'`
  - 削除されたレコードを除外: `deleted_at IS NULL`
  - 有効なレコードのみインデックス化: `enabled = 1`

**オプションフィルタ**(検索時フィルタリング):
- **検索中**のフィルタリングに使用
- どのデータをインデックス化するかには影響しない
- (required_filtersに一致する)すべての行がインデックス化される
- 使用例:
  - 検索時にカテゴリ、ステータス、日付範囲でフィルタリング
  - カスタムカラムでソート

#### フィルタ型

| 型 | MySQLの型 | 説明 |
|------|-------------|-------------|
| `tinyint` | TINYINT | 符号付き8ビット整数(-128から127) |
| `tinyint_unsigned` | TINYINT UNSIGNED | 符号なし8ビット整数(0から255) |
| `smallint` | SMALLINT | 符号付き16ビット整数 |
| `smallint_unsigned` | SMALLINT UNSIGNED | 符号なし16ビット整数 |
| `int` | INT | 符号付き32ビット整数(レガシー: "int"も受け入れる) |
| `int_unsigned` | INT UNSIGNED | 符号なし32ビット整数 |
| `bigint` | BIGINT | 符号付き64ビット整数 |
| `float` | FLOAT | 単精度浮動小数点数 |
| `double` | DOUBLE | 倍精度浮動小数点数 |
| `string`, `varchar`, `text` | VARCHAR, TEXT, CHAR | 文字列値 |
| `datetime` | DATETIME | 日付/時刻値(タイムゾーン対応、`datetime_timezone`で設定可能) |
| `date` | DATE | 日付値(タイムゾーン対応、`datetime_timezone`で設定可能) |
| `timestamp` | TIMESTAMP | タイムスタンプ値(常にUTC、`datetime_timezone`の影響を受けない) |
| `time` | TIME | 時刻値(00:00:00からの秒数として格納、負数サポート) |

#### 必須フィルタの演算子

| 演算子 | 説明 | 例 |
|----------|-------------|---------|
| `=` | 等しい | `status = 'published'` |
| `!=` | 等しくない | `type != 'draft'` |
| `<` | より小さい | `priority < 10` |
| `>` | より大きい | `score > 50` |
| `<=` | 以下 | `age <= 18` |
| `>=` | 以上 | `rating >= 4.0` |
| `IS NULL` | NULLである | `deleted_at IS NULL` |
| `IS NOT NULL` | NULLでない | `published_at IS NOT NULL` |

#### N-gramサイズの推奨値

| 言語 | コンテンツの種類 | `ngram_size` | `kanji_ngram_size` |
|----------|--------------|--------------|---------------------|
| 英語 | 記事、ドキュメント | `2` | `0` または `2` |
| 日本語 | 混在(漢字/かな/ASCII) | `2` | `1` |
| 中国語 | 混在(漢字/ASCII) | `2` | `1` |
| コード | ソースコード | `3` | `0` または `3` |

**トレードオフ**:
- **小さいN-gram(1)**: 高い再現率、多くの誤検出、大きいインデックス
- **大きいN-gram(3-4)**: 高い精度、少ない結果、小さいインデックス

#### ポスティングリスト設定

| パラメータ | 型 | デフォルト | 説明 |
|-----------|------|---------|-------------|
| `block_size` | integer | `128` | デルタ符号化圧縮のブロックサイズ(64-256を推奨) |
| `freq_bits` | integer | `0` | 単語頻度ビット数: `0`(ブール値)、`4`、または`8`(ランキングサポート) |
| `use_roaring` | string | `auto` | Roaringビットマップの使用: `auto`(閾値ベース)、`always`、`never` |

**Roaringビットマップの閾値**: ポスティングリストの密度が18%を超えると自動的にRoaringビットマップに切り替わります(`memory.roaring_threshold`で設定可能)。

---

### インデックス構築

```yaml
build:
  mode: "select_snapshot"           # 構築モード(現在はselect_snapshotのみ)
  batch_size: 5000                  # スナップショット中のバッチあたりの行数
  parallelism: 2                    # 並列構築スレッド数
  throttle_ms: 0                    # バッチ間のスロットル遅延(ミリ秒)
```

#### パラメータ

| パラメータ | 型 | デフォルト | 説明 | ホットリロード |
|-----------|------|---------|-------------|------------|
| `mode` | string | `select_snapshot` | 構築モード(現在は`select_snapshot`のみ) | ❌ 不可 |
| `batch_size` | integer | `5000` | スナップショット中にバッチごとに処理する行数 | ❌ 不可 |
| `parallelism` | integer | `2` | 並列構築スレッド数 | ❌ 不可 |
| `throttle_ms` | integer | `0` | バッチ間のスロットル遅延(ミリ秒)(0 = スロットルなし) | ❌ 不可 |

**パフォーマンスチューニング**:
- **大きい`batch_size`(10000+)**: 初期スナップショットが高速、メモリ使用量が多い
- **小さい`batch_size`(1000-2000)**: スナップショットが低速、メモリ使用量が少ない
- **高い`parallelism`**: マルチコアシステムで高速、メモリ使用量が多い
- **0以外の`throttle_ms`**: スナップショット中のMySQL負荷を軽減(本番データベースで有用)

---

### レプリケーション

```yaml
replication:
  enable: true                      # binlogレプリケーションを有効化
  auto_initial_snapshot: false      # 起動時に自動的にスナップショットを構築
  server_id: 12345                  # MySQLサーバーID(一意である必要がある)
  start_from: "snapshot"            # レプリケーション開始位置
  queue_size: 10000                 # binlogイベントキューサイズ
  reconnect_backoff_min_ms: 500     # 最小再接続バックオフ遅延
  reconnect_backoff_max_ms: 10000   # 最大再接続バックオフ遅延
```

#### パラメータ

| パラメータ | 型 | デフォルト | 説明 | ホットリロード |
|-----------|------|---------|-------------|------------|
| `enable` | boolean | `true` | binlogレプリケーションを有効化 | ❌ 不可 |
| `auto_initial_snapshot` | boolean | `false` | 起動時に自動的にスナップショットを構築 | ❌ 不可 |
| `server_id` | integer | *必須* | MySQLサーバーID(0以外でレプリケーショントポロジ内で一意である必要がある) | ❌ 不可 |
| `start_from` | string | `snapshot` | レプリケーション開始位置: `snapshot`、`latest`、または`gtid=<UUID:txn>` | ❌ 不可 |
| `queue_size` | integer | `10000` | binlogイベントキューサイズ(リーダーとプロセッサ間のバッファ) | ❌ 不可 |
| `reconnect_backoff_min_ms` | integer | `500` | 最小再接続バックオフ遅延(ミリ秒) | ❌ 不可 |
| `reconnect_backoff_max_ms` | integer | `10000` | 最大再接続バックオフ遅延(ミリ秒) | ❌ 不可 |

#### レプリケーション開始位置

| 値 | 説明 | 使用例 |
|-------|-------------|----------|
| `snapshot` | スナップショットGTIDから開始(推奨) | スナップショットデータとの整合性を保証 |
| `latest` | 現在のGTIDから開始(新しい変更のみ) | 履歴データをスキップし、新しい変更のみ追跡 |
| `gtid=<UUID:txn>` | 特定のGTIDから開始 | 既知の位置から再開 |

**例**:
```yaml
start_from: "gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:1"
```

#### サーバーIDの要件

- **0以外である必要がある**
- **トポロジ内のすべてのMySQLレプリカで一意である必要がある**
- **推奨**: ランダムな数値を生成するか、環境ごとに一意の値を使用

**ランダムなサーバーIDを生成**:
```bash
echo $((RANDOM * RANDOM))
```

---

### メモリ管理

```yaml
memory:
  hard_limit_mb: 8192               # ハードメモリ制限(MB)
  soft_target_mb: 4096              # ソフトメモリターゲット(MB)
  arena_chunk_mb: 64                # アリーナチャンクサイズ(MB)
  roaring_threshold: 0.18           # Roaringビットマップ閾値(密度)
  minute_epoch: true                # 分精度エポックを使用

  # テキスト正規化
  normalize:
    nfkc: true                      # NFKC正規化
    width: "narrow"                 # 幅変換: keep、narrow、wide
    lower: false                    # 小文字変換
```

#### パラメータ

| パラメータ | 型 | デフォルト | 説明 | ホットリロード |
|-----------|------|---------|-------------|------------|
| `hard_limit_mb` | integer | `8192` | ハードメモリ制限(MB)(OOM保護) | ❌ 不可 |
| `soft_target_mb` | integer | `4096` | ソフトメモリターゲット(MB)(退避トリガー) | ❌ 不可 |
| `arena_chunk_mb` | integer | `64` | アリーナチャンクサイズ(MB)(メモリアロケータ) | ❌ 不可 |
| `roaring_threshold` | float | `0.18` | Roaringビットマップのポスティングリスト密度閾値(0.0-1.0) | ❌ 不可 |
| `minute_epoch` | boolean | `true` | タイムスタンプに分精度エポックを使用 | ❌ 不可 |
| `normalize.nfkc` | boolean | `true` | NFKC正規化を適用(Unicode互換性) | ❌ 不可 |
| `normalize.width` | string | `narrow` | 幅変換: `keep`、`narrow`、`wide` | ❌ 不可 |
| `normalize.lower` | boolean | `false` | テキストを小文字に変換 | ❌ 不可 |

#### テキスト正規化

**NFKC正規化**(`normalize.nfkc`):
- **日本語**および**CJK**コンテンツに推奨
- Unicode互換文字を正規化
- 例: `㍻`(U+337B) → `平成`(U+5E73 U+6210)

**幅変換**(`normalize.width`):
- `keep`: 変換なし
- `narrow`: 全角 → 半角(例: `Ａ` → `A`)
- `wide`: 半角 → 全角(例: `A` → `Ａ`)

**小文字変換**(`normalize.lower`):
- `true`: 小文字に変換(大文字小文字を区別しない検索)
- `false`: 大文字小文字を保持(大文字小文字を区別する検索)

---

### ダンプ永続化

```yaml
dump:
  dir: "/var/lib/mygramdb/dumps"    # ダンプディレクトリパス
  default_filename: "mygramdb.dmp"  # 手動ダンプのデフォルトファイル名
  interval_sec: 0                   # 自動保存間隔(0 = 無効)
  retain: 3                         # 保持する自動保存ダンプの数
```

#### パラメータ

| パラメータ | 型 | デフォルト | 説明 | ホットリロード |
|-----------|------|---------|-------------|------------|
| `dir` | string | `/var/lib/mygramdb/dumps` | ダンプディレクトリパス(自動的に作成される) | ❌ 不可 |
| `default_filename` | string | `mygramdb.dmp` | 手動`DUMP SAVE`のデフォルトファイル名 | ❌ 不可 |
| `interval_sec` | integer | `0` | 自動保存間隔(秒)(0 = 無効) | ✅ 可能 |
| `retain` | integer | `3` | 保持する自動保存ダンプの数(クリーンアップ) | ✅ 可能 |

#### 自動保存の動作

**`interval_sec > 0`の場合**:
- N秒ごとに自動的にスナップショットを保存
- ファイル名: `auto_YYYYMMDD_HHMMSS.dmp`
- 古い自動保存ファイルはクリーンアップされる(最新の`retain`個のファイルを保持)
- 手動ダンプ(`DUMP SAVE`経由)はクリーンアップの**対象外**

**推奨値**:
- **開発**: `0`(無効)
- **本番**: `7200`(2時間)

**類似機能**:
- Redis RDB永続化

---

### APIサーバー

```yaml
api:
  tcp:
    bind: "127.0.0.1"               # TCPバインドアドレス
    port: 11016                     # TCPポート
    max_connections: 10000          # 最大同時接続数

  http:
    enable: false                   # HTTP/JSON APIを有効化
    bind: "127.0.0.1"               # HTTPバインドアドレス
    port: 8080                      # HTTPポート
    enable_cors: false              # CORSヘッダーを有効化
    cors_allow_origin: ""           # CORSが有効な場合に許可するOrigin

  default_limit: 100                # 指定されていない場合のデフォルトLIMIT
  max_query_length: 128             # 最大クエリ式長

  # レート制限(オプション)
  rate_limiting:
    enable: false                   # レート制限を有効化
    capacity: 100                   # クライアントあたりの最大トークン数(バースト)
    refill_rate: 10                 # クライアントあたり毎秒追加されるトークン数
    max_clients: 10000              # 追跡する最大クライアント数
```

#### TCPサーバーパラメータ

| パラメータ | 型 | デフォルト | 説明 | ホットリロード |
|-----------|------|---------|-------------|------------|
| `tcp.bind` | string | `127.0.0.1` | TCPバインドアドレス(すべてのインターフェースには`0.0.0.0`を使用) | ❌ 不可 |
| `tcp.port` | integer | `11016` | TCPポート | ❌ 不可 |
| `tcp.max_connections` | integer | `10000` | 最大同時接続数(ファイルディスクリプタ枯渇を防止) | ❌ 不可 |

**セキュリティ推奨事項**:
- **開発**: `127.0.0.1`(localhostのみ)
- **本番**: `network.allow_cidrs`を使用してアクセスを制限

#### HTTPサーバーパラメータ

| パラメータ | 型 | デフォルト | 説明 | ホットリロード |
|-----------|------|---------|-------------|------------|
| `http.enable` | boolean | `false` | HTTP/JSON APIを有効化(デフォルトで無効) | ❌ 不可 |
| `http.bind` | string | `127.0.0.1` | HTTPバインドアドレス | ❌ 不可 |
| `http.port` | integer | `8080` | HTTPポート | ❌ 不可 |
| `http.enable_cors` | boolean | `false` | ブラウザクライアント用のCORSヘッダーを有効化 | ❌ 不可 |
| `http.cors_allow_origin` | string | `` | CORSが有効な場合に許可するOrigin(例: `https://app.example.com`) | ❌ 不可 |

**HTTPエンドポイント**:
- `POST /{table}/search`: 検索クエリ
- `GET /{table}/:id`: プライマリキーでドキュメントを取得
- `GET /info`: サーバー情報
- `GET /health`: ヘルスチェック(Kubernetes対応)

#### クエリパラメータ

| パラメータ | 型 | デフォルト | 説明 | ホットリロード |
|-----------|------|---------|-------------|------------|
| `default_limit` | integer | `100` | 指定されていない場合のデフォルトLIMIT(5-1000) | ⚠️ 部分的 |
| `max_query_length` | integer | `128` | 最大クエリ式長(0 = 無制限) | ⚠️ 部分的 |

#### レート制限(トークンバケットアルゴリズム)

| パラメータ | 型 | デフォルト | 説明 | ホットリロード |
|-----------|------|---------|-------------|------------|
| `rate_limiting.enable` | boolean | `false` | クライアントIPごとのレート制限を有効化 | ⚠️ 部分的 |
| `rate_limiting.capacity` | integer | `100` | クライアントあたりの最大トークン数(バーストサイズ) | ⚠️ 部分的 |
| `rate_limiting.refill_rate` | integer | `10` | クライアントあたり毎秒追加されるトークン数(持続レート) | ⚠️ 部分的 |
| `rate_limiting.max_clients` | integer | `10000` | 追跡する最大クライアント数(メモリ管理) | ⚠️ 部分的 |

**動作原理**:
- 各クライアントIPには`capacity`個のトークンを持つトークンバケットがある
- 各リクエストは1トークンを消費
- トークンは毎秒`refill_rate`で補充される
- バケットが空になると、リクエストはレート制限される(HTTP 429)

**例**:
```yaml
rate_limiting:
  enable: true
  capacity: 100       # 100リクエストのバーストを許可
  refill_rate: 10     # 持続レート: IPあたり10リクエスト/秒
```

---

### ネットワークセキュリティ

```yaml
network:
  allow_cidrs:                      # 許可するCIDRリスト(フェイルクローズ)
    - "127.0.0.1/32"                # localhostのみ(最も安全)
    # - "192.168.1.0/24"            # ローカルネットワーク
    # - "10.0.0.0/8"                # プライベートネットワーク
    # - "0.0.0.0/0"                 # 警告: すべて許可(非推奨)
```

#### パラメータ

| パラメータ | 型 | デフォルト | 説明 | ホットリロード |
|-----------|------|---------|-------------|------------|
| `allow_cidrs` | array | *必須* | 許可するCIDRリスト(空 = **すべて拒否**) | ❌ 不可 |

**セキュリティポリシー**:
- **フェイルクローズ**: 空の`allow_cidrs`はすべての接続を拒否
- **明示的許可リスト**: 許可するIP範囲を明示的に設定する必要がある
- **適用対象**: TCPとHTTP APIの両方

**例**:
```yaml
# localhostのみ(最も安全)
allow_cidrs:
  - "127.0.0.1/32"
  - "::1/128"  # IPv6 localhost

# ローカルネットワーク
allow_cidrs:
  - "192.168.1.0/24"

# プライベートネットワーク
allow_cidrs:
  - "10.0.0.0/8"
  - "172.16.0.0/12"
  - "192.168.0.0/16"

# すべて許可(本番環境では非推奨)
allow_cidrs:
  - "0.0.0.0/0"
  - "::/0"
```

---

### ロギング

```yaml
logging:
  level: "info"                     # ログレベル
  format: "json"                    # ログ出力形式: "json" または "text"
  file: ""                          # ログファイルパス(空 = stdout)
```

#### パラメータ

| パラメータ | 型 | デフォルト | 説明 | ホットリロード |
|-----------|------|---------|-------------|------------|
| `level` | string | `info` | ログレベル: `debug`、`info`、`warn`、`error` | ✅ 可能 |
| `format` | string | `json` | ログ出力形式: `json`(構造化) または `text`(人間が読みやすい) | ❌ 不可 |
| `file` | string | `` | ログファイルパス(空 = stdout、デーモンモードでは必須) | ❌ 不可 |

**ログレベル**:
- **`debug`**: 詳細ログ(開発のみ)
- **`info`**: 標準的な操作メッセージ(本番環境で推奨)
- **`warn`**: 警告(即座の対応が不要な異常)
- **`error`**: エラー(注意が必要)

**ログ形式**:
- **`json`**: 構造化JSON形式(本番環境で推奨、解析が容易)
- **`text`**: 人間が読みやすいkey=value形式(開発環境で推奨)

**ログ出力**:
- **空の`file`**: stdoutに出力(Docker/systemdで推奨)
- **パス**: ファイルに出力(例: `/var/log/mygramdb/mygramdb.log`)
- **デーモンモード**: `-d/--daemon`使用時には`file`が**必須**

---

### クエリキャッシュ

```yaml
cache:
  enabled: true                     # クエリ結果キャッシュを有効化
  max_memory_mb: 32                 # 最大キャッシュメモリ(MB)
  min_query_cost_ms: 10.0           # キャッシュする最小クエリコスト(ミリ秒)
  ttl_seconds: 3600                 # キャッシュエントリのTTL(0 = TTLなし)
  invalidation_strategy: "ngram"    # 無効化戦略

  # 詳細チューニング
  compression_enabled: true         # LZ4圧縮を有効化
  eviction_batch_size: 10           # 退避バッチサイズ

  # 無効化キュー
  invalidation:
    batch_size: 1000                # N個の一意なペア後に処理
    max_delay_ms: 100               # 処理前の最大遅延(ミリ秒)
```

#### パラメータ

| パラメータ | 型 | デフォルト | 説明 | ホットリロード |
|-----------|------|---------|-------------|------------|
| `enabled` | boolean | `true` | クエリ結果キャッシュを有効化 | ⚠️ 部分的(キャッシュクリア) |
| `max_memory_mb` | integer | `32` | 最大キャッシュメモリ(MB) | ⚠️ 部分的 |
| `min_query_cost_ms` | float | `10.0` | キャッシュする最小クエリコスト(ミリ秒) | ⚠️ 部分的 |
| `ttl_seconds` | integer | `3600` | キャッシュエントリのTTL(秒)(0 = TTLなし) | ⚠️ 部分的 |
| `invalidation_strategy` | string | `ngram` | 無効化戦略: `ngram`、`table` | ⚠️ 部分的 |
| `compression_enabled` | boolean | `true` | キャッシュされた結果のLZ4圧縮を有効化 | ⚠️ 部分的 |
| `eviction_batch_size` | integer | `10` | 一度に退避するエントリ数 | ⚠️ 部分的 |
| `invalidation.batch_size` | integer | `1000` | N個の一意な(table, ngram)ペア後に無効化を処理 | ⚠️ 部分的 |
| `invalidation.max_delay_ms` | integer | `100` | 無効化処理前の最大遅延 | ⚠️ 部分的 |

#### 無効化戦略

**`ngram`(推奨)**:
- **精度**: 変更されたN-gramを使用するクエリのみを無効化
- **効率**: 最小限のキャッシュ無効化
- **使用例**: 本番環境

**`table`**:
- **積極的**: 任意の変更でテーブルキャッシュ全体を無効化
- **シンプル**: N-gram追跡のオーバーヘッドなし
- **使用例**: 非常に高い書き込みレート、小さいキャッシュ

#### キャッシュチューニング

**`min_query_cost_ms`**:
- この閾値より長いクエリのみをキャッシュ
- **高い値**: キャッシュされるクエリが少ない、メモリ使用量が少ない
- **低い値**: キャッシュされるクエリが多い、メモリ使用量が多い
- **推奨**: 10-50ミリ秒

**`ttl_seconds`**:
- キャッシュエントリの有効期間
- **0**: TTLなし(無効化または退避されるまでキャッシュ)
- **3600**: 1時間(頻繁に変更されるデータに推奨)

**`compression_enabled`**:
- キャッシュされた結果のLZ4圧縮
- **true**: メモリ使用量が少ない、わずかなCPUオーバーヘッド
- **false**: メモリ使用量が多い、CPUオーバーヘッドなし

---

## ホットリロード

### SIGHUPによるサポート

MygramDBは、`SIGHUP`シグナルを送信することで、再起動せずに**ライブ設定リロード**をサポートします:

```bash
# プロセスIDを検索
ps aux | grep mygramdb

# SIGHUPシグナルを送信
kill -HUP <pid>

# またはsystemdを使用
systemctl reload mygramdb
```

### リロード可能な設定

| セクション | 設定 | リロード動作 |
|---------|---------|-----------------|
| **Logging** | `level` | ✅ 即座に適用 |
| **MySQL** | `host`、`port`、`user`、`password` | ✅ 新しいMySQLサーバーに再接続 |
| **MySQL** | `ssl_*` | ✅ 新しいSSL設定で再接続 |
| **Dump** | `interval_sec`、`retain` | ✅ スケジューラ設定を更新 |
| **Cache** | すべての設定 | ⚠️ キャッシュクリア、新しい設定を適用 |
| **API** | `default_limit`、`max_query_length` | ⚠️ 新しいクエリに適用 |
| **API** | `rate_limiting.*` | ⚠️ レート制限リセット |

### リロード不可能な設定(再起動が必要)

| セクション | 設定 | 理由 |
|---------|---------|--------|
| **MySQL** | `database` | データベース接続は変更できない |
| **MySQL** | `use_gtid`、`binlog_format`、`binlog_row_image` | レプリケーションモードは変更できない |
| **Tables** | すべての設定 | テーブルスキーマとインデックス構造は変更できない |
| **Build** | すべての設定 | 構築設定は起動時のみ |
| **Replication** | `enable`、`server_id`、`start_from`、`queue_size` | レプリケーション設定は起動時のみ |
| **Memory** | すべての設定 | メモリアロケータは変更できない |
| **API** | `tcp.bind`、`tcp.port`、`http.bind`、`http.port` | ソケットは再バインドできない |
| **Network** | `allow_cidrs` | ネットワークセキュリティポリシーは変更できない |
| **Logging** | `json`、`file` | ログ出力は変更できない |

### リロードワークフロー

1. **設定ファイルを編集**:
   ```bash
   vim /etc/mygramdb/config.yaml
   ```

2. **設定を検証**(オプション):
   ```bash
   mygramdb --config=/etc/mygramdb/config.yaml --validate
   ```

3. **SIGHUPシグナルを送信**:
   ```bash
   kill -HUP $(cat /var/run/mygramdb.pid)
   ```

4. **リロードを確認**:
   ```bash
   tail -f /var/log/mygramdb/mygramdb.log
   ```

   期待される出力:
   ```
   Configuration reload requested (SIGHUP received)
   Logging level changed: info -> debug
   Configuration reload completed successfully
   ```

### リロード失敗時の処理

設定リロードが失敗した場合:
- **現在の設定が継続**(ダウンタイムなし)
- **詳細とともにエラーがログに記録**
- **サーバーは古い設定で動作を継続**

```
Failed to reload configuration: Invalid YAML syntax at line 42
Continuing with current configuration
```

---

## 本番環境での推奨事項

### セキュリティ

1. **MySQL接続**:
   - ✅ SSL/TLSを有効化(`ssl_enable: true`)
   - ✅ 強力なパスワードを使用
   - ✅ MySQLユーザー権限を制限(SELECT、REPLICATION SLAVE、REPLICATION CLIENTのみ)

2. **ネットワークセキュリティ**:
   - ✅ `network.allow_cidrs`を設定(デフォルトでフェイルクローズ)
   - ✅ localhostのみのアクセスには`tcp.bind: 127.0.0.1`を使用
   - ✅ インターネット公開デプロイにはリバースプロキシ(nginx、haproxy)を使用

3. **レート制限**:
   - ✅ DoS攻撃を防ぐために`rate_limiting`を有効化
   - ✅ システムの`ulimit -n`に基づいて`tcp.max_connections`を設定

### パフォーマンス

1. **メモリ**:
   - ✅ `memory.hard_limit_mb`をシステムRAMの50-70%に設定
   - ✅ `memory.soft_target_mb`を`hard_limit_mb`の50%に設定

2. **キャッシュ**:
   - ✅ キャッシュを有効化(`cache.enabled: true`)
   - ✅ `ngram`無効化戦略を使用
   - ✅ `min_query_cost_ms`を10-50ミリ秒に設定

3. **ダンプ永続化**:
   - ✅ 自動保存を有効化(`dump.interval_sec: 7200`で2時間)
   - ✅ 3-7個のスナップショットを保持(`dump.retain: 3-7`)

4. **レプリケーション**:
   - ✅ `queue_size: 10000`に設定(デフォルトで十分)
   - ✅ `REPLICATION STATUS`でキューサイズを監視

### モニタリング

1. **ロギング**:
   - ✅ JSONロギングを使用(`logging.format: "json"`)
   - ✅ 集中ログ(ELK、Lokiなど)にログを送信

2. **ヘルスチェック**:
   - ✅ KubernetesプローブにはHTTP `/health`エンドポイントを使用
   - ✅ `INFO`統計を監視

3. **メトリクス**:
   - ✅ キャッシュヒット率を追跡
   - ✅ レプリケーションラグを監視
   - ✅ 接続失敗時にアラート

### 高可用性

1. **ダンプ永続化**:
   - ✅ 高速再起動のために自動保存を有効化
   - ✅ ダンプファイルをS3/GCSにバックアップ

2. **レプリケーション**:
   - ✅ 整合性のために`start_from: "snapshot"`を使用
   - ✅ binlogリーダーのステータスを監視

3. **フェイルオーバー**:
   - ✅ MygramDBは接続喪失時に自動的にMySQLに再接続
   - ✅ フェイルオーバーにはMySQLレプリケーション(マスター-スレーブ)を使用

---

## トラブルシューティング

### 設定検証エラー

**問題**: 設定ファイルに無効な構文または未知のキーがある

**解決方法**:
```bash
# 設定を検証
mygramdb --config=/path/to/config.yaml --validate

# JSON Schema検証エラーを確認
mygramdb --config=/path/to/config.yaml 2>&1 | grep -A5 "Schema validation failed"
```

### MySQL接続エラー

**問題**: MySQLに接続できない

**診断**:
```bash
# MySQL接続を確認
mysql -h <host> -P <port> -u <user> -p<password>

# GTIDモードを確認
mysql> SHOW VARIABLES LIKE 'gtid_mode';

# binlog形式を確認
mysql> SHOW VARIABLES LIKE 'binlog_format';

# ユーザー権限を確認
mysql> SHOW GRANTS FOR 'repl_user'@'%';
```

**解決方法**:
- MySQLでGTIDモードを有効化
- binlog形式をROWに設定
- REPLICATION SLAVE、REPLICATION CLIENT権限を付与

### ホットリロードが機能しない

**問題**: SIGHUPシグナルが設定リロードをトリガーしない

**診断**:
```bash
# プロセスが実行中か確認
ps aux | grep mygramdb

# SIGHUPを送信
kill -HUP <pid>

# ログを確認
tail -f /var/log/mygramdb/mygramdb.log
```

**解決方法**:
- 設定ファイルに構文エラーがないことを確認
- ファイルパスが正しいことを確認
- エラーの詳細についてログ出力を確認

### キャッシュが機能しない

**問題**: キャッシュヒット率が0%

**診断**:
```bash
# キャッシュ統計を確認
CACHE STATS

# キャッシュが有効か確認
grep "cache.enabled" config.yaml
```

**解決方法**:
- キャッシュを有効化: `cache.enabled: true`
- `max_memory_mb`を増やす
- `min_query_cost_ms`を下げる

---

## 参照

- [アーキテクチャリファレンス](architecture.md)
- [APIリファレンス](api.md)
- [デプロイメントガイド](deployment.md)
- [開発ガイド](development.md)
