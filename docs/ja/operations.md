# 設定ホットリロード & MySQL フェイルオーバー検出 - 運用ガイド

## 概要

本ガイドでは、運用の信頼性を高める2つの重要な機能について説明します：

1. **設定ホットリロード (SIGHUP)**: サーバー再起動なしに設定を再読み込み
2. **MySQL フェイルオーバー検出**: MySQL レプリケーションの自動検証とフェイルオーバー検出

これらの機能により、ゼロダウンタイムでの設定更新と、MySQL マスターのフェイルオーバーシナリオの安全な処理が可能になります。

## 1. 設定ホットリロード (SIGHUP)

`SIGHUP` シグナルを送信することで、実行中のサーバーの設定を再読み込みできます。サーバーを再起動する必要がありません。

### サポートされる設定変更

#### ✅ 自動適用される設定

- **ログレベル**: 即座に変更が反映されます
- **MySQL 接続設定**: 変更された場合、サーバーは以下を実行します：
  - 現在の BinlogReader を停止
  - 既存の MySQL 接続を切断
  - 新しい設定で再接続
  - 最後の GTID から BinlogReader を再起動
  - 新しいサーバーを検証

#### ⚠️ 再起動が必要な設定

以下の設定変更はホットリロード**不可**で、サーバーの完全な再起動が必要です：

- サーバーのポート/バインドアドレス
- テーブル設定（名前、プライマリキー、テキストカラム）
- インデックス設定（n-gram サイズ、ハイブリッドモード）
- キャッシュ設定
- ワーカースレッドプールサイズ

### 使用方法

#### SIGHUP シグナルの送信

```bash
# プロセス ID を確認
ps aux | grep mygramdb

# SIGHUP シグナルを送信（<PID> を実際のプロセス ID に置き換え）
kill -SIGHUP <PID>

# または PID ファイルを使用する場合
kill -SIGHUP $(cat /var/run/mygramdb.pid)
```

#### リロードの確認

サーバーログで確認します：

```log
[info] Configuration reload requested (SIGHUP received)
[info] Logging level changed from 'info' to 'debug'
[info] MySQL connection settings changed, reconnecting...
[info] Stopping binlog reader...
[info] Binlog reader stopped. Processed 12345 events
[info] Creating new MySQL connection with updated settings...
[info] Binlog connection validated successfully
[info] Binlog reader started from GTID: <last-gtid>
[info] Configuration reload completed successfully
```

#### エラーハンドリング

設定の再読み込みに失敗した場合、サーバーは**現在の設定**で動作を継続します：

```log
[error] Failed to reload configuration: Invalid YAML syntax at line 42
[warn] Continuing with current configuration
```

このグレースフル・デグラデーションにより、新しい設定が無効な場合でもサーバーは稼働し続けます。

### 例：MySQL 接続設定の変更

```yaml
# config.yaml
mysql:
  host: "mysql-master-new.example.com"  # 旧ホストから変更
  port: 3306
  user: "replicator"
  password: "new-password"  # パスワード更新
  database: "myapp"
```

```bash
# 再起動なしで変更を適用
kill -SIGHUP $(pidof mygramdb)
```

### 設定リロードのモニタリング

構造化ログを使用して設定リロードイベントを監視：

```bash
# 設定リロードイベントをフィルタ
tail -f /var/log/mygramdb/server.log | grep "Configuration reload"

# 失敗をチェック
tail -f /var/log/mygramdb/server.log | grep -E "(reload.*failed|Continuing with current)"
```

## 2. MySQL フェイルオーバー検出

サーバーは MySQL 接続を自動的に検証し、以下を検出します：

- **フェイルオーバー**: MySQL マスターサーバーの変更（異なるサーバー UUID）
- **無効なサーバー**: テーブルの欠損、GTID モード無効、不整合な状態
- **接続断**: 検証付き自動再接続

これにより、ネットワーク変更やフェイルオーバーイベント後に、誤ったサーバーからレプリケートすることを防ぎます。

### 検証チェック

MySQL への接続または再接続時に、サーバーは以下を実行します：

1. **GTID モードチェック**: `gtid_mode=ON` を確認
2. **サーバー UUID 追跡**: サーバー変更を検出（フェイルオーバー）
3. **テーブル存在確認**: 必要なテーブルがすべて存在することを検証
4. **GTID 整合性**: レプリケーション状態の整合性をチェック

### フェイルオーバー検出時の動作

#### シナリオ 1: 通常の再接続（同じサーバー）

```log
[info] [binlog worker] Reconnected successfully
[info] [binlog worker] Connection validated successfully after reconnect
```

- **動作**: レプリケーションを正常に継続
- **影響**: なし

#### シナリオ 2: フェイルオーバー検出（異なるサーバー）

```log
[info] [binlog worker] Reconnected successfully
[warn] [binlog validation] Server UUID changed: a1b2c3d4-... -> e5f6g7h8-... (failover detected)
{"event":"mysql_failover_detected","old_uuid":"a1b2c3d4-...","new_uuid":"e5f6g7h8-..."}
[info] [binlog worker] Connection validated successfully after reconnect
```

- **動作**: **警告を記録**してレプリケーションを継続
- **影響**: オペレーターにフェイルオーバーイベントを通知
- **次のステップ**:
  - フェイルオーバーが意図的なものか確認
  - レプリケーション遅延をチェック
  - データ整合性を監視

#### シナリオ 3: 無効なサーバー（テーブル欠損）

```log
[info] [binlog worker] Reconnected successfully
{"event":"binlog_connection_validation_failed","gtid":"<current-gtid>","error":"Required tables are missing: users, messages"}
[error] [binlog worker] Connection validation failed after reconnect: Required tables are missing: users, messages
[info] Binlog reader stopped. Processed 12345 events
```

- **動作**: **レプリケーションを停止**（データ破損を防止）
- **影響**: オペレーターが介入するまでレプリケーション停止
- **次のステップ**:
  1. MySQL サーバーが正しいか確認
  2. テーブルスキーマをチェック
  3. 設定を修正するか、正しいサーバーに復元
  4. mygramdb を再起動

#### シナリオ 4: GTID モード無効化

```log
{"event":"connection_validation_failed","reason":"gtid_disabled","error":"GTID mode is not enabled"}
[error] [binlog worker] Connection validation failed after reconnect: GTID mode is not enabled
```

- **動作**: **レプリケーションを停止**
- **影響**: サーバーは binlog reader を起動しません
- **次のステップ**: MySQL サーバーで GTID モードを有効化

### フェイルオーバーイベントの監視

#### 構造化ログクエリ

```bash
# フェイルオーバーイベントを検出
grep 'mysql_failover_detected' /var/log/mygramdb/server.log

# 検証失敗をチェック
grep 'connection_validation_failed' /var/log/mygramdb/server.log

# サーバー UUID 変更を監視
grep 'Server UUID changed' /var/log/mygramdb/server.log
```

#### 構造化ログの出力例

```json
{
  "event": "mysql_failover_detected",
  "old_uuid": "a1b2c3d4-e5f6-1234-5678-90abcdef1234",
  "new_uuid": "e5f6g7h8-i9j0-5678-9012-34567890abcd",
  "timestamp": "2025-11-17T10:30:45Z"
}
```

### 運用手順

#### 計画的な MySQL フェイルオーバー

1. **フェイルオーバー前**:
   - 新しいマスターで GTID が有効か確認
   - 必要なテーブルがすべて新しいマスターに存在するか確認
   - レプリケーション遅延をチェック

2. **フェイルオーバー中**:
   - MySQL 接続の DNS/IP を更新
   - mygramdb に SIGHUP を送信（ホットリロード設定）
   - ログでフェイルオーバー検出を監視

3. **フェイルオーバー後**:
   ```bash
   # 新しいマスターへの接続を確認
   grep "Connection validated successfully" /var/log/mygramdb/server.log | tail -1

   # フェイルオーバー警告をチェック
   grep "mysql_failover_detected" /var/log/mygramdb/server.log | tail -1

   # レプリケーション継続を確認
   # mygramdb のヘルスエンドポイントを使用
   curl http://localhost:8080/health/detail
   ```

#### 予期しないフェイルオーバー（緊急時）

1. **問題の検出**:
   ```bash
   # レプリケーションが停止したかチェック
   grep "Binlog reader stopped" /var/log/mygramdb/server.log | tail -1

   # 検証失敗をチェック
   grep "validation_failed" /var/log/mygramdb/server.log | tail -5
   ```

2. **診断**:
   - サーバー UUID が異なる？ → フェイルオーバー発生
   - テーブルが欠損？ → 誤ったサーバーまたはスキーマ問題
   - GTID 無効？ → 設定問題

3. **復旧**:
   ```bash
   # 誤ったサーバーの場合、設定を更新してリロード
   vim /etc/mygramdb/config.yaml  # MySQL ホスト/ポートを修正
   kill -SIGHUP $(pidof mygramdb)

   # 正しいサーバーだが検証失敗の場合、mygramdb を再起動
   systemctl restart mygramdb
   ```

## 3. ヘルスモニタリング統合

### ヘルスチェックエンドポイント

ヘルスエンドポイントを使用してレプリケーション状態を監視：

```bash
# 基本ヘルスチェック
curl http://localhost:8080/health/live
# 戻り値: 200 OK（サーバーが稼働中）

# レディネスチェック
curl http://localhost:8080/health/ready
# 戻り値: 200（準備完了）、503（ロード中）

# 詳細ヘルスステータス
curl http://localhost:8080/health/detail
```

**詳細ヘルスレスポンスの例**:

```json
{
  "status": "healthy",
  "uptime_seconds": 3600,
  "binlog_reader": {
    "running": true,
    "current_gtid": "a1b2c3d4-e5f6-1234-5678-90abcdef1234:1-12345",
    "processed_events": 12345,
    "queue_size": 42
  },
  "mysql_connection": {
    "connected": true,
    "server_uuid": "a1b2c3d4-e5f6-1234-5678-90abcdef1234"
  }
}
```

### アラートルール

推奨される監視アラート：

1. **フェイルオーバー検出** (INFO)
   - トリガー: `event:mysql_failover_detected`
   - アクション: オペレーターに通知、フェイルオーバーが意図的か確認

2. **検証失敗** (CRITICAL)
   - トリガー: `event:connection_validation_failed`
   - アクション: オンコール呼び出し、レプリケーション停止

3. **レプリケーション停止** (CRITICAL)
   - トリガー: `binlog_reader.running = false`（ヘルスエンドポイント）
   - アクション: オンコール呼び出し、即座に調査

4. **設定リロード失敗** (WARNING)
   - トリガー: `Failed to reload configuration`
   - アクション: 設定構文をチェック、オペレーターに通知

## 4. テストと検証

### SIGHUP リロードのテスト

```bash
# 1. サーバー起動
./bin/mygramdb --config config.yaml

# 2. 設定を変更
vim config.yaml  # ログレベルを変更

# 3. SIGHUP を送信
kill -SIGHUP $(pidof mygramdb)

# 4. ログを確認
tail -f /var/log/mygramdb/server.log
# 期待値: "Configuration reload completed successfully"
```

### フェイルオーバー検出のテスト

**ユニットテスト**（MySQL 不要）：

```bash
# バリデーターのユニットテストを実行
cd build
./bin/connection_validator_test --gtest_filter="ConnectionValidatorUnitTest.*"
```

**統合テスト**（GTID 有効な MySQL が必要）：

```bash
# 環境変数を設定
export ENABLE_MYSQL_INTEGRATION_TESTS=1
export MYSQL_HOST=127.0.0.1
export MYSQL_USER=root
export MYSQL_PASSWORD=test
export MYSQL_DATABASE=test

# 統合テストを実行
ctest -R ConnectionValidatorIntegrationTest -V
```

### 手動フェイルオーバーシミュレーション

```bash
# 1. mysql-server-1 に接続した mygramdb を起動
./bin/mygramdb --config config.yaml

# 2. 設定を mysql-server-2 に変更
vim config.yaml
# 変更: host: "mysql-server-2.example.com"

# 3. リロードをトリガー
kill -SIGHUP $(pidof mygramdb)

# 4. ログでフェイルオーバー検出を確認
tail -100 /var/log/mygramdb/server.log | grep -E "(failover|UUID changed)"
```

## 5. トラブルシューティング

### 問題：設定リロード失敗

**症状**:
```log
[error] Failed to reload configuration: <error message>
[warn] Continuing with current configuration
```

**解決策**:
1. YAML 構文をチェック: `yamllint config.yaml`
2. ファイルパーミッションを確認: `ls -la config.yaml`
3. ログで JSON スキーマ検証エラーを確認
4. 設定を手動でテスト: `./bin/mygramdb --config config.yaml --validate-config`

### 問題：再接続後にレプリケーション停止

**症状**:
```log
[error] Connection validation failed: Required tables are missing
[info] Binlog reader stopped
```

**解決策**:
1. MySQL サーバーが正しいか確認: 設定の `mysql.host` をチェック
2. テーブルが存在するか確認:
   ```sql
   SHOW TABLES FROM myapp;
   ```
3. GTID モードを確認:
   ```sql
   SHOW VARIABLES LIKE 'gtid_mode';
   ```
4. サーバー UUID をチェック:
   ```sql
   SELECT @@server_uuid;
   ```

### 問題：フェイルオーバー警告だがレプリケーション動作中

**症状**:
```log
[warn] Server UUID changed: <old> -> <new> (failover detected)
[info] Connection validated successfully
```

**これは正常です**（以下の場合）:
- 計画的な MySQL フェイルオーバーが発生した
- DNS/IP が新しいマスターに切り替わった
- すべての検証チェックが合格した

**必要なアクション**:
- フェイルオーバーイベントを記録
- レプリケーション遅延を確認
- 整合性の問題を監視

### 問題：再接続試行の繰り返し

**症状**:
```log
[info] Reconnect attempt #5, waiting 5000 ms
[error] [binlog worker] Connection validation failed: <error>
[info] Reconnect attempt #6, waiting 6000 ms
```

**解決策**:
1. MySQL サーバーの可用性をチェック
2. ネットワーク接続を確認
3. 設定の認証情報を確認
4. ファイアウォールルールを確認
5. 設定で `mysql.connect_timeout_ms` の増加を検討

## 6. パフォーマンスへの影響

### SIGHUP リロードの影響

- **ログレベル変更**: 影響なし、即座に反映
- **MySQL 再接続**: 短時間の中断（1〜5秒）
  - Binlog reader が正常に停止
  - 既存のクエリは正常に完了
  - 検索/挿入操作は継続（既存の接続を使用）
  - 最後の GTID からレプリケーション再開

**推奨**: 可能であれば、トラフィックの少ない時間帯に実施

### 検証のオーバーヘッド

- **初回接続**: 検証時間 約100〜200ms
- **再接続**: 追加オーバーヘッド 約50〜100ms
- **影響**: 無視できるレベル（接続/再接続時のみ発生）

**コスト内訳**:
1. GTID モードチェック: 1クエリ（約10ms）
2. サーバー UUID チェック: 1クエリ（約10ms）
3. テーブル存在確認: N クエリ（各約10ms、N = テーブル数）
4. GTID 整合性: 1クエリ（約20ms）

## 7. ベストプラクティス

### 設定管理

1. **バージョン管理**: `config.yaml` を git で管理
2. **検証**: ステージング環境で設定変更をテスト
3. **ロールバックプラン**: 以前の設定のバックアップを保持
4. **ドキュメント化**: すべての設定変更を記録

### モニタリング

1. **ログ集約**: 集中ログシステムにログを送信（ELK、Splunk など）
2. **構造化ログ**: JSON パースを使用したアラート
3. **ヘルスチェック**: `/health/detail` エンドポイントを30秒ごとに監視
4. **メトリクス**: GTID 遅延、イベント処理レートを追跡

### フェイルオーバー手順

1. **MySQL トポロジーをドキュメント化**: マスター/レプリカ構成図を保持
2. **DNS/IP 更新の自動化**: オーケストレーションツールを使用
3. **定期的なフェイルオーバーテスト**: 四半期ごとのフェイルオーバー訓練
4. **遅延の監視**: レプリケーション遅延 > 60秒 でアラート

### セキュリティ

1. **SIGHUP アクセス**: 信頼できるユーザーのみがシグナル送信可能に
2. **設定ファイルパーミッション**: `chmod 600 config.yaml`（機密情報を含む）
3. **ログローテーション**: ディスク容量枯渇を防止
4. **監査証跡**: すべての設定変更とフェイルオーバーをログ記録

## 8. 実装詳細

### ソースファイル

**SIGHUP ハンドラ**:
- `src/main.cpp:36-618` - シグナルハンドラと設定リロードロジック

**ConnectionValidator**:
- `src/mysql/connection_validator.h` - 検証インターフェース
- `src/mysql/connection_validator.cpp` - 検証実装

**BinlogReader 統合**:
- `src/mysql/binlog_reader.h:187-188,255` - UUID 追跡メンバー
- `src/mysql/binlog_reader.cpp:196-201,315-322,386-393,452-459,749-799` - 検証呼び出し

**テスト**:
- `tests/mysql/connection_validator_test.cpp` - 包括的なテストスイート

### 設定スキーマ

ホットリロードとフェイルオーバー検出に関連する設定フィールド：

```yaml
logging:
  level: "info"  # SIGHUP でホットリロード可能

mysql:
  host: "127.0.0.1"           # ホットリロード可能
  port: 3306                   # ホットリロード可能
  user: "replicator"           # ホットリロード可能
  password: "password"         # ホットリロード可能
  database: "myapp"            # ホットリロード可能
  connect_timeout_ms: 10000    # ホットリロード可能
  read_timeout_ms: 30000       # ホットリロード可能
  write_timeout_ms: 30000      # ホットリロード可能
```

## 付録：クイックリファレンス

### コマンド

```bash
# 設定をリロード
kill -SIGHUP $(pidof mygramdb)

# プロセスステータスをチェック
ps aux | grep mygramdb

# ログを監視
tail -f /var/log/mygramdb/server.log

# ヘルスチェック
curl http://localhost:8080/health/detail

# バリデーターテストを実行
./bin/connection_validator_test
```

### ログイベント

| イベント | 重要度 | 意味 |
|---------|--------|------|
| `Configuration reload requested` | INFO | SIGHUP を受信 |
| `Configuration reload completed successfully` | INFO | 設定が適用された |
| `Failed to reload configuration` | ERROR | 設定が無効、旧設定を使用 |
| `mysql_failover_detected` | WARNING | サーバー UUID が変更された |
| `connection_validation_failed` | ERROR | 検証失敗、レプリケーション停止 |
| `Connection validated successfully` | INFO | 検証合格 |

### エラーコード

| エラー | コード | アクション |
|-------|-------|-----------|
| GTID モード無効 | `gtid_disabled` | MySQL で GTID を有効化 |
| テーブル欠損 | `table_validation_failed` | スキーマをチェック |
| サーバー UUID 変更 | `failover_detected` | フェイルオーバーを確認 |
| 接続断 | `connection_failed` | ネットワークをチェック |

**最終更新**: 2025-11-17
