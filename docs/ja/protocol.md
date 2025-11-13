# プロトコルリファレンス

MygramDBは、TCP経由でシンプルなテキストベースのプロトコルを使用します（memcachedと同様）。

## 接続

TCP経由でMygramDBに接続：

```bash
telnet localhost 11016
```

またはCLIクライアントを使用：

```bash
./build/bin/mygram-cli -h localhost -p 11016
```

## コマンドフォーマット

コマンドはテキストベースで、1行に1コマンドです。レスポンスは改行で終了します。

---

## SEARCH コマンド

指定されたテキストを含むドキュメントを検索します。

### 構文

```
SEARCH <table> <text> [OPTIONS]
```

### 基本例

シンプルな検索：
```
SEARCH articles hello
```

フィルタとページネーション付き：
```
SEARCH articles tech FILTER status=1 LIMIT 10
```

### レスポンス

```
OK RESULTS <total_count> <id1> <id2> <id3> ...
```

**詳細なクエリ構文、ブール演算子、フィルタ、ソート、高度な機能については、[クエリ構文ガイド](query_syntax.md)を参照してください。**

---

## COUNT コマンド

検索条件に一致するドキュメント数をカウントします（IDは返しません）。

### 構文

```
COUNT <table> <text> [OPTIONS]
```

### 例

```
COUNT articles tech AND AI FILTER status=1
```

### レスポンス

```
OK COUNT <number>
```

**完全なクエリ構文については、[クエリ構文ガイド](query_syntax.md)を参照してください。**

---

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

例：
```
OK DOC 12345 status=1 category=tech created_at=2024-01-15T10:30:00
```

---

## INFO コマンド

サーバー情報と統計を取得します。

### 構文

```
INFO
```

### レスポンス

```
OK INFO version=<version> uptime=<seconds> total_requests=<count> connections=<count> index_size=<bytes> doc_count=<count>
```

例：
```
OK INFO version=1.0.0 uptime=3600 total_requests=10000 connections=5 index_size=1048576 doc_count=1000000
```

---

## CONFIG コマンド

現在のサーバー設定（すべての設定）を取得します。

### 構文

```
CONFIG
```

### レスポンス

YAML形式の設定を返します：
- MySQL接続設定
- テーブル設定（name, primary_key, ngram_size, filters数）
- APIサーバー設定（bindアドレスとポート）
- レプリケーション設定（enable, server_id, start_from）
- メモリ設定（制限、しきい値）
- スナップショットディレクトリ
- ログレベル
- ランタイムステータス（接続数、稼働時間、read_onlyモード）

---

## CONFIG VERIFY

現在の設定を検証し、システムステータスを確認します。

### 構文

```
CONFIG VERIFY
```

### レスポンス

```
OK CONFIG VERIFIED
tables: <count>

table: <table_name>
  primary_key: <column>
  text_source: <source>
  ngram_size: <size>
  filters: <count>
  required_filters: <count>
  status: loaded|not_loaded
  documents: <count>
  terms: <count>

replication:
  status: running|stopped
  gtid: <gtid>

END
```

---

## DUMP コマンド

DUMPコマンドファミリーは、整合性検証を備えた統一的なスナップショット管理を提供します。

### DUMP SAVE

完全なスナップショットを単一のバイナリファイル（`.dmp`）に保存します。

**構文:**
```
DUMP SAVE [<filepath>] [WITH STATISTICS]
```

**例:**
```
DUMP SAVE /backup/mygramdb.dmp WITH STATISTICS
```

### DUMP LOAD

バイナリファイルからスナップショットを読み込みます。

**構文:**
```
DUMP LOAD [<filepath>]
```

**例:**
```
DUMP LOAD /backup/mygramdb.dmp
```

### DUMP VERIFY

データを読み込まずにスナップショットファイルの整合性を検証します。

**構文:**
```
DUMP VERIFY [<filepath>]
```

**例:**
```
DUMP VERIFY /backup/mygramdb.dmp
```

### DUMP INFO

スナップショットファイルのメタデータ（バージョン、GTID、テーブル数、サイズ、フラグ）を表示します。

**構文:**
```
DUMP INFO [<filepath>]
```

**例:**
```
DUMP INFO /backup/mygramdb.dmp
```

**詳細なスナップショット管理、整合性保護、ベストプラクティス、トラブルシューティングについては、[スナップショットガイド](snapshot.md)を参照してください。**

---

## REPLICATION STATUS

現在のレプリケーションステータスを取得します。

### 構文

```
REPLICATION STATUS
```

### レスポンス

```
OK REPLICATION status=<running|stopped> gtid=<current_gtid>
```

例：
```
OK REPLICATION status=running gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100
```

---

## REPLICATION STOP

binlogレプリケーションを停止します（インデックスは読み取り専用になります）。

### 構文

```
REPLICATION STOP
```

### レスポンス

```
OK REPLICATION STOPPED
```

---

## REPLICATION START

binlogレプリケーションを再開します。

### 構文

```
REPLICATION START
```

### レスポンス

```
OK REPLICATION STARTED
```

---

## OPTIMIZE コマンド

インデックスポスティングリストを最適化します（密度に基づいてデルタエンコーディングをRoaring Bitmapに変換）。

### 構文

```
OPTIMIZE
```

### 動作

- binlogレプリケーションを一時停止
- ポスティングリストをバッチでコピーして最適化
- クエリ処理は継続（古いインデックスを使用）
- 最適化完了後にアトミックに切り替え
- binlogレプリケーションを再開

### メモリ使用量

- **インデックス部分のみ**が一時的に2倍になる（ドキュメントストアは変更なし）
- 全体的なメモリ使用量は約1.05〜1.1倍に増加
- メモリはバッチ処理で段階的に解放

### 注意

- 最適化中は新しいOPTIMIZEコマンドは拒否されます
- `optimization_status`は`INFO`コマンドで確認できます
- 大規模インデックスでは数秒〜数十秒かかる場合があります

### レスポンス

```
OK OPTIMIZED terms=<total> delta=<count> roaring=<count>
```

例：
```
OK OPTIMIZED terms=1500000 delta=1200000 roaring=300000
```

エラー（すでに最適化中の場合）：
```
ERROR Optimization already in progress
```

---

## DEBUG コマンド

現在の接続のデバッグモードを有効/無効にして、詳細なクエリ実行メトリクスを表示します。

### 構文

```
DEBUG ON
DEBUG OFF
```

### 動作

- **接続ごとの状態**: デバッグモードは現在の接続のみで有効/無効
- **クエリタイミング**: 実行時間の内訳を表示（インデックス検索、フィルタリング）
- **検索詳細**: 生成されたn-gram、ポスティングリストサイズ、候補数を表示
- **最適化の可視性**: 適用された最適化戦略を報告
- **パフォーマンス影響**: 最小限のオーバーヘッド、有効時のみメトリクスを収集

### レスポンス

```
OK DEBUG_ON
```

または

```
OK DEBUG_OFF
```

### デバッグ出力フォーマット

デバッグモードが有効な場合、SEARCHとCOUNTコマンドは追加のデバッグ情報を返します：

```
OK RESULTS <count> <id1> <id2> ...

# DEBUG
query_time: <ms>
index_time: <ms>
filter_time: <ms>
terms: <n>
ngrams: <n>
candidates: <n>
after_intersection: <n>
after_not: <n>
after_filters: <n>
final: <n>
optimization: <strategy>
order_by: <column> <direction>
limit: <value> [(default)]
offset: <value> [(default)]
```

### デバッグメトリクスの説明

- **query_time**: 総クエリ実行時間（ミリ秒）
- **index_time**: インデックス検索に費やした時間
- **filter_time**: フィルタ適用に費やした時間（フィルタがある場合）
- **terms**: 検索用語の数
- **ngrams**: 検索用語から生成された総n-gram数
- **candidates**: インデックスからの初期候補ドキュメント
- **after_intersection**: AND用語の交差後の結果
- **after_not**: NOT用語のフィルタリング後の結果（NOTを使用した場合）
- **after_filters**: FILTER条件適用後の結果（フィルタを使用した場合）
- **final**: 一致したドキュメントの総数（LIMIT/OFFSET適用前）
- **optimization**: 使用された戦略（例: `merge_join`、`early_exit`、`none`）
- **order_by**: 適用されたソート（カラムと方向）
- **limit**: 返される最大結果数（明示的に指定されていない場合は「(default)」と表示）
- **offset**: ページネーションの結果オフセット（明示的に指定されていない場合は「(default)」と表示）

---

## エラーレスポンス

すべてのエラーは以下の形式に従います：

```
ERROR <error_message>
```

例：
```
ERROR Unknown command
ERROR Table not found: products
ERROR Invalid GTID format
```

---

## CLIクライアント機能

CLIクライアント（`mygram-cli`）は対話型シェルを提供：

- **タブ補完**: TABキーでコマンド名を自動補完（GNU Readline必須）
- **コマンド履歴**: ↑/↓矢印キーで履歴をナビゲート（GNU Readline必須）
- **行編集**: Ctrl+A、Ctrl+Eなどでフル行編集（GNU Readline必須）
- **エラーハンドリング**: グレースフルなエラーメッセージ（クラッシュしない）

### 対話モード

```bash
./build/bin/mygram-cli
> SEARCH articles hello
OK RESULTS 5 1 2 3 4 5
> quit
```

### 単一コマンドモード

```bash
./build/bin/mygram-cli SEARCH articles "hello world"
```

### helpコマンド

対話モードで`help`と入力すると、利用可能なコマンドが表示されます：

```
> help
Available commands:
  SEARCH, COUNT, GET              - 検索と取得
  INFO, CONFIG, CONFIG VERIFY     - サーバー情報と検証
  DUMP SAVE/LOAD/VERIFY/INFO      - スナップショット管理
  REPLICATION STATUS/STOP/START   - レプリケーション制御
  OPTIMIZE                        - インデックス最適化
  DEBUG ON/OFF                    - デバッグモードの有効/無効
  quit, exit                      - クライアント終了
```

---

## 参照

- [クエリ構文ガイド](query_syntax.md) - 詳細なSEARCH/COUNTクエリ構文
- [スナップショットガイド](snapshot.md) - スナップショット管理とベストプラクティス
- [設定ガイド](configuration.md) - サーバー設定
- [レプリケーション設定](replication.md) - MySQLレプリケーション設定
