# プロトコルリファレンス

MygramDB は TCP 上でシンプルなテキストベースのプロトコルを使用します（memcached と類似）。

## 接続

TCP 経由で MygramDB に接続：

```bash
telnet localhost 11311
```

または CLI クライアントを使用：

```bash
./build/bin/mygram-cli -h localhost -p 11311
```

## コマンド形式

コマンドはテキストベース、1行につき1コマンド。レスポンスは改行で終了します。

## SEARCH コマンド

指定されたテキストを含むドキュメントを検索します。

### 構文

```
SEARCH <table> <text> [AND <term>...] [NOT <term>...] [FILTER <col=val>...] [LIMIT <n>] [OFFSET <n>]
```

### パラメータ

- **table**: テーブル名
- **text**: 検索テキストまたはフレーズ
- **AND term**: 追加の必須項（複数可）
- **NOT term**: 除外項（複数可）
- **FILTER col=val**: カラム値でフィルター（複数可）
- **LIMIT n**: 最大結果数（デフォルト: 1000）
- **OFFSET n**: ページネーション用の結果オフセット（デフォルト: 0）

### 例

基本的な検索:
```
SEARCH articles こんにちは
```

フレーズ検索（引用符で囲む）:
```
SEARCH articles "ライブ配信" LIMIT 100
```

複数の必須項:
```
SEARCH articles 技術 AND AI
```

AND と NOT を組み合わせ:
```
SEARCH articles ニュース AND 速報 NOT 古い
```

フィルター付き:
```
SEARCH articles ニュース NOT 古い FILTER status=1
```

ページネーション:
```
SEARCH articles 技術 FILTER category=AI LIMIT 50 OFFSET 100
```

### 検索構文機能

**引用符付き文字列:**
- フレーズ検索には単一 `'` または二重 `"` 引用符を使用
- 例: `"hello world"` は完全なフレーズを検索
- エスケープシーケンスをサポート: `\n`, `\t`, `\r`, `\\`, `\"`, `\'`

**AND 演算子:**
- 指定されたすべての項を含む文書を検索
- 例: `term1 AND term2 AND term3`

**NOT 演算子:**
- 特定の項を含む文書を除外
- 例: `term1 NOT excluded`

### レスポンス

```
OK RESULTS <total_count> <id1> <id2> <id3> ...
```

例:
```
OK RESULTS 3 101 205 387
```

## COUNT コマンド

検索条件に一致する文書数をカウント（ID を返さない）。

### 構文

```
COUNT <table> <text> [AND <term>...] [NOT <term>...] [FILTER <col=val>...]
```

### 例

基本的なカウント:
```
COUNT articles こんにちは
```

複数項でカウント:
```
COUNT articles 技術 AND AI
```

フィルター付きカウント:
```
COUNT articles ニュース NOT 古い FILTER status=1
```

### レスポンス

```
OK COUNT <number>
```

例:
```
OK COUNT 42
```

## GET コマンド

プライマリキーでドキュメントを取得します。

### 構文

```
GET <table> <primary_key>
```

### 例

```
GET articles 12345
```

### レスポンス

```
OK DOC <primary_key> <filter1=value1> <filter2=value2> ...
```

例:
```
OK DOC 12345 status=1 category=tech created_at=2024-01-15T10:30:00
```

## INFO コマンド

サーバー情報と統計を取得します（Redis風の詳細統計）。

### 構文

```
INFO
```

### レスポンス

Redis スタイルのセクション形式で詳細な統計情報を返します：

```
OK INFO

# Server
version: MygramDB 1.0.0
uptime_seconds: <seconds>

# Stats
total_commands_processed: <count>
total_connections_received: <count>
total_requests: <count>

# Commandstats
cmd_search: <count>
cmd_count: <count>
cmd_get: <count>
cmd_info: <count>
...

# Memory
used_memory_bytes: <bytes>
used_memory_human: <human_readable>
used_memory_peak_bytes: <bytes>
used_memory_peak_human: <human_readable>
used_memory_index: <human_readable>
used_memory_documents: <human_readable>
memory_fragmentation_ratio: <ratio>

# Index
total_documents: <count>
total_terms: <count>
total_postings: <count>
avg_postings_per_term: <average>
delta_encoded_lists: <count>
roaring_bitmap_lists: <count>
ngram_size: <size>

# Clients
connected_clients: <count>

# Replication (MySQL が有効な場合)
replication_status: running|stopped
replication_gtid: <gtid>
replication_events: <count>

END
```

例:
```
OK INFO

# Server
version: MygramDB 1.0.0
uptime_seconds: 3600

# Stats
total_commands_processed: 15000
total_connections_received: 250
total_requests: 15000

# Commandstats
cmd_search: 10000
cmd_count: 3000
cmd_get: 2000

# Memory
used_memory_bytes: 524288000
used_memory_human: 500MB
used_memory_peak_bytes: 629145600
used_memory_peak_human: 600MB
used_memory_index: 400MB
used_memory_documents: 100MB
memory_fragmentation_ratio: 1.20

# Index
total_documents: 1000000
total_terms: 1500000
total_postings: 5000000
avg_postings_per_term: 3.33
delta_encoded_lists: 1200000
roaring_bitmap_lists: 300000
ngram_size: 1
optimization_status: idle

# Clients
connected_clients: 5

END
```

## CONFIG コマンド

現在のサーバー設定（すべての設定項目）を取得します。

### 構文

```
CONFIG
```

### レスポンス

YAML 形式で以下の情報を返します:
- MySQL 接続設定
- テーブル設定（名前、primary_key、ngram_size、フィルター数）
- API サーバー設定（バインドアドレスとポート）
- レプリケーション設定（enable、server_id、start_from、state_file）
- メモリ設定（制限、閾値）
- スナップショットディレクトリ
- ロギングレベル
- 実行時状態（接続数、稼働時間、読み取り専用モード）

例:
```
CONFIG
OK CONFIG
  mysql:
    host: 127.0.0.1
    port: 3306
    user: repl_user
    database: mydb
    use_gtid: true
  tables: 1
    - name: articles
      primary_key: id
      ngram_size: 1
      filters: 3
  api:
    tcp.bind: 0.0.0.0
    tcp.port: 11311
  replication:
    enable: true
    server_id: 12345
    start_from: snapshot
    state_file: ./mygramdb_replication.state
  memory:
    hard_limit_mb: 8192
    soft_target_mb: 4096
    roaring_threshold: 0.18
  snapshot:
    dir: /var/lib/mygramdb/snapshots
  logging:
    level: info
  runtime:
    connections: 5
    max_connections: 1000
    read_only: false
    uptime: 3600s
```

## SAVE コマンド

現在のインデックススナップショットをディスクに保存します。

### 構文

```
SAVE [<filepath>]
```

### 例

デフォルトの場所に保存:
```
SAVE
```

特定のファイルに保存:
```
SAVE /path/to/snapshot.bin
```

### レスポンス

```
OK SAVED <filepath>
```

## LOAD コマンド

インデックススナップショットをディスクから読み込みます。

### 構文

```
LOAD <filepath>
```

### 例

```
LOAD /path/to/snapshot.bin
```

### レスポンス

```
OK LOADED <filepath> docs=<count>
```

## REPLICATION STATUS コマンド

現在のレプリケーション状態を取得します。

### 構文

```
REPLICATION STATUS
```

### レスポンス

```
OK REPLICATION status=<running|stopped> gtid=<current_gtid>
```

例:
```
OK REPLICATION status=running gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100
```

## REPLICATION STOP コマンド

Binlog レプリケーションを停止します（インデックスは読み取り専用になります）。

### 構文

```
REPLICATION STOP
```

### レスポンス

```
OK REPLICATION STOPPED
```

## REPLICATION START コマンド

Binlog レプリケーションを再開します。

### 構文

```
REPLICATION START
```

### レスポンス

```
OK REPLICATION STARTED
```

## OPTIMIZE コマンド

インデックスの転置リストを最適化します（密度に応じてDelta EncodingからRoaring Bitmapに変換）。

### 構文

```
OPTIMIZE
```

### 動作

- Binlogレプリケーションを一時停止
- バッチ単位で転置リストをコピー＆最適化
- クエリ処理は継続（古いインデックスで動作）
- 最適化完了後にアトミックにスイッチ
- Binlogレプリケーションを再開

### メモリ使用量

- 一時的に**インデックス部分のみ約2倍**（ドキュメントストアは変化なし）
- 全体では約1.05〜1.1倍のメモリ使用量増加
- バッチ処理により段階的にメモリを解放

### 注意事項

- 最適化中は新しいOPTIMIZEコマンドは受け付けません
- `INFO`コマンドで`optimization_status`を確認可能
- 大規模なインデックスでは数秒〜数十秒かかる場合があります

### レスポンス

```
OK OPTIMIZED terms=<total> delta=<count> roaring=<count>
```

例:
```
OK OPTIMIZED terms=1500000 delta=1200000 roaring=300000
```

エラー（既に最適化中の場合）:
```
ERROR Optimization already in progress
```

## エラーレスポンス

すべてのエラーは以下の形式に従います：

```
ERROR <error_message>
```

例:
```
ERROR Unknown command
ERROR Table not found: products
ERROR Invalid GTID format
```

## CLI クライアントの機能

CLI クライアント（`mygram-cli`）は、以下の機能を備えたインタラクティブシェルを提供します：

- **タブ補完**: TAB キーを押すとコマンド名を自動補完（GNU Readline が必要）
- **コマンド履歴**: ↑/↓ 矢印キーで履歴をナビゲート（GNU Readline が必要）
- **行編集**: Ctrl+A、Ctrl+E などの完全な行編集（GNU Readline が必要）
- **エラーハンドリング**: 適切なエラーメッセージを表示（クラッシュしません）

### 対話モード

```bash
./build/bin/mygram-cli
> SEARCH articles こんにちは
OK RESULTS 5 1 2 3 4 5
> quit
```

### 単一コマンドモード

```bash
./build/bin/mygram-cli SEARCH articles "こんにちは"
```

### ヘルプコマンド

対話モードで `help` と入力すると、利用可能なコマンドが表示されます：

```
> help
利用可能なコマンド:
  SEARCH, COUNT, GET    - 検索と取得
  INFO, CONFIG          - サーバー情報
  SAVE, LOAD            - スナップショット管理
  REPLICATION STATUS/STOP/START - レプリケーション制御
  OPTIMIZE              - インデックス最適化
  quit, exit            - クライアント終了
```
