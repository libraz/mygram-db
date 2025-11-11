# MySQL レプリケーションガイド

MygramDB はデータ一貫性を保証した GTID ベースの binlog ストリーミングによる MySQL からのリアルタイムレプリケーションをサポートしています。

## 前提条件

### MySQL サーバー要件

MygramDB には以下が必要です:
- **MySQL バージョン**: 5.7.6+ または 8.0+（MySQL 8.0 および 8.4 でテスト済み）
- **GTID モード**: 有効化必須
- **バイナリログ形式**: ROW 形式が必要
- **権限**: レプリケーションユーザーに特定の権限が必要

### GTID モードを有効化

GTID モードが有効かを確認：

```sql
SHOW VARIABLES LIKE 'gtid_mode';
```

GTID モードが OFF の場合は有効化：

```sql
-- GTID モードを有効化（MySQL 5.7 ではサーバー再起動が必要）
SET GLOBAL gtid_mode = ON;
SET GLOBAL enforce_gtid_consistency = ON;
```

### バイナリログを設定

ROW 形式でバイナリログが有効化されているか確認：

```sql
-- バイナリログ形式を確認
SHOW VARIABLES LIKE 'binlog_format';

-- ROW 形式に設定（my.cnf に追加して再起動）
SET GLOBAL binlog_format = ROW;
```

### レプリケーションユーザーを作成

レプリケーション権限を持つユーザーを作成：

```sql
-- レプリケーションユーザーを作成
CREATE USER 'repl_user'@'%' IDENTIFIED BY 'your_password';

-- レプリケーション権限を付与
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_user'@'%';

-- 変更を適用
FLUSH PRIVILEGES;
```

## レプリケーション開始オプション

設定ファイルの `replication.start_from` で設定:

### snapshot（推奨）

初期スナップショットビルド時にキャプチャされた GTID から開始：

```yaml
replication:
  start_from: "snapshot"
```

**動作原理:**
- データ一貫性のため `START TRANSACTION WITH CONSISTENT SNAPSHOT` を使用
- スナップショット時点の `@@global.gtid_executed` を正確にキャプチャ
- スナップショットと binlog レプリケーション間のデータ損失を保証

**使用するタイミング:**
- 初期セットアップ（ほとんどの場合に推奨）
- 一貫性のあるポイントインタイムビューが必要な場合
- ゼロから開始する場合

### latest

現在の GTID ポジションから開始（履歴データは無視）：

```yaml
replication:
  start_from: "latest"
```

**動作原理:**
- `SHOW BINARY LOG STATUS` を使用して最新の GTID を取得
- MygramDB 起動後の変更のみをキャプチャ

**使用するタイミング:**
- リアルタイム変更のみが必要な場合
- 履歴データが重要でない場合

### gtid=UUID:txn

特定の GTID ポジションから開始：

```yaml
replication:
  start_from: "gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:100"
```

**使用するタイミング:**
- 特定のポイントからの手動リカバリ
- テストやデバッグ

### state_file

保存された GTID 状態ファイルから再開：

```yaml
replication:
  start_from: "state_file"
  state_file: "./mygramdb_replication.state"
```

**動作原理:**
- 状態ファイルから GTID を読み込み（自動作成）
- クラッシュリカバリと再開を可能にします

**使用するタイミング:**
- シャットダウン後の MygramDB 再起動
- クラッシュ後の自動再開

## サポートされている操作

### DML 操作

MygramDB は自動的に処理します：

- **INSERT**（WRITE_ROWS イベント）
  - 新しいドキュメントをインデックスとストアに追加
- **UPDATE**（UPDATE_ROWS イベント）
  - ドキュメント内容とフィルターを更新
  - テキストが変更された場合は再インデックス化
- **DELETE**（DELETE_ROWS イベント）
  - インデックスとストアからドキュメントを削除

### DDL 操作

MygramDB はこれらの DDL 操作を処理します：

#### TRUNCATE TABLE

対象テーブルのインデックスとドキュメントストアを自動的にクリア：

```sql
TRUNCATE TABLE articles;
```

MygramDB の動作：
- テーブルからすべてのドキュメントをクリア
- すべての転置インデックスをクリア
- ドキュメント ID カウンターをリセット

#### DROP TABLE

すべてのデータをクリアしてエラーをログ出力：

```sql
DROP TABLE articles;
```

MygramDB の動作：
- すべてのデータをクリア
- エラーメッセージをログ出力
- **手動での再起動/再設定が必要**

#### ALTER TABLE

スキーマ不整合の可能性について警告をログ出力：

```sql
ALTER TABLE articles ADD COLUMN new_col VARCHAR(100);
ALTER TABLE articles MODIFY COLUMN content TEXT;
```

**重要な注意事項:**
- 型変更（例: VARCHAR から TEXT）はレプリケーションの問題を引き起こす可能性があります
- `text_source` または `filters` に影響するカラムの追加/削除は MygramDB の再起動が必要
- **推奨**: スキーマ変更後は MygramDB のスナップショットを再構築してください

## サポートされているカラム型

MygramDB はこれらの MySQL カラム型をレプリケートできます：

### 整数型
- TINYINT、SMALLINT、INT、MEDIUMINT、BIGINT（signed/unsigned）

### 文字列型
- VARCHAR、CHAR、TEXT、BLOB、ENUM、SET

### 日時型
- DATE、TIME、DATETIME、TIMESTAMP（小数秒対応）

### 数値型
- DECIMAL、FLOAT、DOUBLE

### 特殊型
- JSON、BIT、NULL

## レプリケーション機能

### GTID 一貫性

- スナップショットと binlog レプリケーションは一貫性のあるスナップショットトランザクションにより調整
- スナップショットとレプリケーション間のデータ損失なし

### GTID ポジション追跡

- 状態ファイルによるアトミックな永続化
- シャットダウン時の自動保存
- 再起動時の再開

### 自動検証

- 起動時に GTID モードをチェック
- 設定されていない場合は明確なエラーメッセージを表示

### 自動再接続

- 接続断を適切に処理
- 指数バックオフリトライ（設定可能）
- 最後の GTID ポジションから継続

### マルチスレッド処理

- 効率的なリクエスト処理のためのスレッドプールアーキテクチャ
- パフォーマンスチューニング用に調整可能なキューサイズ

## レプリケーションの監視

### レプリケーション状態を確認

CLI または TCP プロトコルを使用：

```bash
# CLI を使用
./build/bin/mygram-cli REPLICATION STATUS

# telnet を使用
echo "REPLICATION STATUS" | nc localhost 11311
```

レスポンス:
```
OK REPLICATION status=running gtid=3E11FA47-71CA-11E1-9E33-C80AA9429562:1-100
```

### レプリケーションを停止

Binlog レプリケーションを停止（インデックスは読み取り専用になります）：

```bash
./build/bin/mygram-cli REPLICATION STOP
```

### レプリケーションを開始

Binlog レプリケーションを再開：

```bash
./build/bin/mygram-cli REPLICATION START
```

## トラブルシューティング

### "GTID mode is not enabled on MySQL server"

**解決策**: MySQL サーバーで GTID モードを有効化：

```sql
SET GLOBAL gtid_mode = ON;
SET GLOBAL enforce_gtid_consistency = ON;
```

その後、MygramDB を再起動。

### "Binary log format is not ROW"

**解決策**: バイナリログ形式を ROW に設定：

```sql
SET GLOBAL binlog_format = ROW;
```

または `my.cnf` に追加して MySQL を再起動：

```ini
[mysqld]
binlog_format = ROW
```

### "Replication lag is high"

**考えられる原因:**
- MySQL での高い書き込み量
- MygramDB のリソース不足
- ネットワークレイテンシ

**解決策:**
- 設定の `replication.queue_size` を増やす
- 高速処理のため `build.parallelism` を増やす
- MygramDB レプリカを追加

### "Lost connection to MySQL server during query"

MygramDB は指数バックオフで自動的に再接続します：

```yaml
replication:
  reconnect_backoff_min_ms: 500
  reconnect_backoff_max_ms: 10000
```

### "Schema mismatch after ALTER TABLE"

**解決策**: スキーマ変更後にスナップショットを再構築：

1. MygramDB を停止
2. 新しいスキーマに合わせて設定ファイルを更新
3. MygramDB を再起動（スナップショットが再構築されます）

## ベストプラクティス

1. **常に GTID モードを使用** して一貫性のあるレプリケーションを実現
2. **初期セットアップには `snapshot` 開始モードを使用**
3. **レプリケーションラグを定期的に監視**
4. **重要なスキーマ変更後はスナップショットを再構築**
5. **本番環境にデプロイする前に設定をテスト**
6. **クラッシュリカバリ用に状態ファイルを保持**
7. **高可用性のために複数のレプリカを使用**

## 設定例

完全なレプリケーション設定：

```yaml
mysql:
  host: "127.0.0.1"
  port: 3306
  user: "repl_user"
  password: "your_password"
  database: "mydb"
  use_gtid: true
  binlog_format: "ROW"
  binlog_row_image: "FULL"

replication:
  enable: true
  server_id: 0                    # 0 = 自動生成
  start_from: "snapshot"          # snapshot|latest|gtid=<UUID:txn>|state_file
  state_file: "./mygramdb_replication.state"
  queue_size: 10000
  reconnect_backoff_min_ms: 500
  reconnect_backoff_max_ms: 10000
```

## 関連項目

- [設定ガイド](configuration.md) - 完全な設定リファレンス
- [プロトコルリファレンス](protocol.md) - REPLICATION コマンド
