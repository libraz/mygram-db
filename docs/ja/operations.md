# ランタイム変数 & MySQL フェイルオーバー - 運用ガイド

## 概要

本ガイドでは、運用の信頼性を高める2つの重要な機能について説明します：

1. **ランタイム変数 (SET/SHOW VARIABLES)**: MySQL互換コマンドを使用して、サーバー再起動なしにランタイムで設定を変更
2. **MySQL フェイルオーバー**: GTID位置を保持しながら、ランタイムでMySQLサーバーを切り替え

これらの機能により、ゼロダウンタイムでの設定更新と、MySQL マスターのフェイルオーバーシナリオの安全な処理が可能になります。

## 1. ランタイム変数 (SET/SHOW VARIABLES)

MygramDBは、サーバー再起動なしにランタイム設定を変更するための、MySQL互換のSETおよびSHOW VARIABLESコマンドをサポートしています。

### 概要

ランタイム変数により、以下が可能です：
- ログレベルをオンザフライで変更
- フェイルオーバー時にMySQLサーバーを切り替え（GTID位置は保持）
- キャッシュ動作を調整
- APIとレート制限パラメータを変更

### 基本的なコマンド

```sql
-- すべての変数を表示
SHOW VARIABLES;

-- 特定のパターンを表示
SHOW VARIABLES LIKE 'mysql%';
SHOW VARIABLES LIKE 'cache%';

-- 単一の変数を設定
SET logging.level = 'debug';
SET mysql.host = '192.168.1.100';

-- 複数の変数を設定
SET api.default_limit = 200, cache.enabled = true;
```

### 可変変数 vs 不変変数

#### ✅ 可変（ランタイム変更可能）

| 変数 | 説明 | 例 |
|----------|-------------|---------|
| `logging.level` | ログレベル (debug/info/warn/error) | `SET logging.level = 'debug'` |
| `logging.format` | ログフォーマット (json/text) | `SET logging.format = 'json'` |
| `mysql.host` | MySQLサーバーホスト名 | `SET mysql.host = '192.168.1.101'` |
| `mysql.port` | MySQLサーバーポート | `SET mysql.port = 3306` |
| `cache.enabled` | クエリキャッシュの有効/無効 | `SET cache.enabled = false` |
| `cache.min_query_cost_ms` | キャッシュする最小クエリコスト | `SET cache.min_query_cost_ms = 20.0` |
| `cache.ttl_seconds` | キャッシュTTL秒数 | `SET cache.ttl_seconds = 7200` |
| `api.default_limit` | デフォルトLIMIT値 | `SET api.default_limit = 200` |
| `api.max_query_length` | 最大クエリ式長 | `SET api.max_query_length = 256` |
| `rate_limiting.capacity` | トークンバケット容量 | `SET rate_limiting.capacity = 200` |
| `rate_limiting.refill_rate` | 秒あたりトークン数 | `SET rate_limiting.refill_rate = 20` |

#### ⚠️ 不変（再起動が必要）

- サーバーポート/バインドアドレス（`tcp.bind`, `tcp.port`, `http.bind`, `http.port`）
- テーブル設定（名前、プライマリキー、テキストカラム）
- MySQLデータベース名、ユーザー、パスワード
- レプリケーション設定（`server_id`, `start_from`）
- メモリアロケータ設定
- ネットワークセキュリティ（`allow_cidrs`）

### 使用例

#### ログレベルの変更

```sql
-- デバッグ用に詳細度を上げる
SET logging.level = 'debug';

-- 変更を確認
SHOW VARIABLES LIKE 'logging%';

-- 通常に戻す
SET logging.level = 'info';
```

#### メンテナンス中のキャッシュ無効化

```sql
-- メンテナンス前にキャッシュを無効化
SET cache.enabled = false;

-- メンテナンス作業を実行...

-- キャッシュを再有効化
SET cache.enabled = true;
```

#### API制限の調整

```sql
-- 一時的にバルク操作用に制限を増やす
SET api.default_limit = 1000;
SET api.max_query_length = 512;

-- 現在の値を確認
SHOW VARIABLES LIKE 'api%';
```

### 検証とエラーハンドリング

SETコマンドは適用前に値を検証します：

```sql
-- 型不一致エラー
SET api.default_limit = 'invalid';
ERROR: Invalid value for api.default_limit: must be integer

-- 範囲外エラー
SET api.default_limit = 99999;
ERROR: Invalid value for api.default_limit: must be between 5 and 1000

-- 未知の変数エラー
SET unknown.variable = 'value';
ERROR: Unknown variable: unknown.variable

-- 不変変数エラー
SET mysql.database = 'newdb';
ERROR: Variable mysql.database is immutable (requires restart)
```

変数は適用**前**に検証されるため、無効な値が提供されてもサーバーは一貫した状態を保ちます。

### 変数変更のモニタリング

変数変更をサーバーログで監視：

```bash
# 変数変更を監視
tail -f /var/log/mygramdb/server.log | grep -i "variable"

# 現在の設定を確認
mygramclient -c "SHOW VARIABLES;"
```

## 2. ランタイム変数を使用したMySQLフェイルオーバー

MygramDBは、SETコマンドを使用したゼロダウンタイムのMySQLフェイルオーバーをサポートしています。新しいMySQLサーバーに切り替える際、GTID位置は保持されます。

### フェイルオーバーワークフロー

```sql
-- 1. 現在のMySQL接続を確認
SHOW VARIABLES LIKE 'mysql%';

-- 2. 新しいMySQLサーバーに切り替え（レプリカをプライマリに昇格）
SET mysql.host = '192.168.1.101', mysql.port = 3306;

-- 3. 再接続が成功したことを確認
SHOW VARIABLES LIKE 'mysql%';
```

### 仕組み

`SET mysql.host/port` を実行すると：

1. **GTID位置を保存**: 現在のGTID位置を保存
2. **Binlogリーダーを停止**: 旧サーバーからの読み取りを正常に停止
3. **旧接続を切断**: 旧MySQLサーバーへの接続を切断
4. **新接続を作成**: 新MySQLサーバーに接続
5. **新サーバーを検証**: GTIDモード、binlogフォーマット、テーブル存在を確認
6. **GTIDから再開**: 保存されたGTID位置からbinlogリーダーを再起動

### 要件

新しいMySQLサーバーは以下を満たす必要があります：
- GTIDモードが有効（`gtid_mode=ON`）
- ROW binlogフォーマットを使用（`binlog_format=ROW`）
- 保存されたGTID位置がGTIDセットに含まれている
- 設定で定義された全テーブルを保有

### 例：計画的フェイルオーバー

```sql
-- フェイルオーバー前：mysql-primary-1に接続
SHOW VARIABLES LIKE 'mysql.host';
-- 出力: mysql-primary-1.example.com

-- レプリカを新プライマリに昇格
-- （MySQLレプリケーション管理で外部的に実行）

-- MygramDBを新プライマリに切り替え
SET mysql.host = 'mysql-primary-2.example.com';

-- フェイルオーバーを確認
SHOW VARIABLES LIKE 'mysql.host';
-- 出力: mysql-primary-2.example.com
```

サーバーログに以下が表示されます：

```log
[info] Reconnecting to MySQL: mysql-primary-2.example.com:3306
[info] Stopping binlog reader...
[info] Binlog reader stopped. Processed 12345 events
[info] Creating new MySQL connection: mysql-primary-2.example.com:3306
[info] Connection validated successfully
[info] Binlog reader started from GTID: <saved-gtid>
```

### エラーハンドリング

フェイルオーバーが失敗した場合、サーバーはレプリケーションを停止し、詳細なエラーをログに記録します：

```log
[error] Failed to reconnect to new MySQL server: Connection refused
[error] Binlog replication stopped. Manual intervention required.
```

この場合：
1. 新MySQLサーバーがアクセス可能か確認
2. ネットワーク接続を確認
3. MySQL認証情報を確認
4. SETコマンドを再試行するか、サーバーを再起動

## 3. MySQL フェイルオーバー検出

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
   - SETコマンドで新しいMySQLサーバーに切り替え
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
   # 誤ったサーバーの場合、SETコマンドで修正
   mygramclient -c "SET mysql.host = 'correct-host.example.com', mysql.port = 3306;"

   # 正しいサーバーだが検証失敗の場合、mygramdb を再起動
   systemctl restart mygramdb
   ```

## 4. ヘルスモニタリング統合

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

4. **変数設定失敗** (WARNING)
   - トリガー: SET コマンドでのエラー応答
   - アクション: 値の検証をチェック、オペレーターに通知

## 5. テストと検証

### ランタイム変数のテスト

```bash
# 1. サーバー起動
./bin/mygramdb --config config.yaml

# 2. 現在の変数を確認
mygramclient -c "SHOW VARIABLES LIKE 'logging%';"

# 3. 変数を変更
mygramclient -c "SET logging.level = 'debug';"

# 4. ログを確認
tail -f /var/log/mygramdb/server.log
# 期待値: デバッグレベルのログが表示される
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

# 2. 現在の接続を確認
mygramclient -c "SHOW VARIABLES LIKE 'mysql.host';"

# 3. 新しいMySQLサーバーに切り替え
mygramclient -c "SET mysql.host = 'mysql-server-2.example.com';"

# 4. ログでフェイルオーバー検出を確認
tail -100 /var/log/mygramdb/server.log | grep -E "(failover|UUID changed)"
```

## 6. トラブルシューティング

### 問題：変数設定失敗

**症状**:
```
ERROR: Invalid value for api.default_limit: must be between 5 and 1000
```

**解決策**:
1. 変数名を確認: `SHOW VARIABLES LIKE 'variable_name';`
2. 有効な値の範囲を確認（ドキュメント参照）
3. 変数が可変かどうか確認（不変変数は再起動が必要）
4. ログで検証エラーを確認

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

## 7. パフォーマンスへの影響

### ランタイム変数変更の影響

- **ログレベル変更**: 影響なし、即座に反映
- **キャッシュパラメータ変更**: 影響なし、即座に反映
- **MySQL 再接続** (SET mysql.host/port): 短時間の中断（1〜5秒）
  - Binlog reader が正常に停止
  - 既存のクエリは正常に完了
  - 検索/挿入操作は継続（既存の接続を使用）
  - 最後の GTID からレプリケーション再開

**推奨**: MySQL再接続は可能であれば、トラフィックの少ない時間帯に実施

### 検証のオーバーヘッド

- **初回接続**: 検証時間 約100〜200ms
- **再接続**: 追加オーバーヘッド 約50〜100ms
- **影響**: 無視できるレベル（接続/再接続時のみ発生）

**コスト内訳**:
1. GTID モードチェック: 1クエリ（約10ms）
2. サーバー UUID チェック: 1クエリ（約10ms）
3. テーブル存在確認: N クエリ（各約10ms、N = テーブル数）
4. GTID 整合性: 1クエリ（約20ms）

## 8. ベストプラクティス

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

1. **クライアントアクセス制御**: 信頼できるIPアドレスからのみSETコマンドを許可（`network.allow_cidrs`）
2. **設定ファイルパーミッション**: `chmod 600 config.yaml`（機密情報を含む）
3. **ログローテーション**: SIGUSR1シグナルによるゼロダウンタイムログローテーション（セクション10参照）
4. **監査証跡**: すべての変数変更とフェイルオーバーをログ記録

## 9. 実装詳細

### ソースファイル

**ランタイム変数管理**:
- `src/config/runtime_variable_manager.h/cpp` - 変数管理とスレッドセーフ更新
- `src/server/handlers/variable_handler.h/cpp` - SET/SHOW VARIABLESコマンドハンドラ
- `src/query/query_parser.h/cpp` - SET/SHOW構文パーサー

**MySQL再接続**:
- `src/app/mysql_reconnection_handler.h/cpp` - MySQL フェイルオーバーロジック
- `src/mysql/binlog_reader.h/cpp` - GTID位置保存と再開

**ConnectionValidator**:
- `src/mysql/connection_validator.h` - 検証インターフェース
- `src/mysql/connection_validator.cpp` - 検証実装

**テスト**:
- `tests/mysql/connection_validator_test.cpp` - 包括的なテストスイート

### 設定スキーマ

ランタイム変数とフェイルオーバー検出に関連する設定フィールド：

```yaml
logging:
  level: "info"  # SET logging.level で変更可能

mysql:
  host: "127.0.0.1"           # SET mysql.host で変更可能（フェイルオーバー）
  port: 3306                   # SET mysql.port で変更可能（フェイルオーバー）
  user: "replicator"           # 不変（再起動が必要）
  password: "password"         # 不変（再起動が必要）
  database: "myapp"            # 不変（再起動が必要）
  connect_timeout_ms: 10000    # 不変（再起動が必要）
  read_timeout_ms: 30000       # 不変（再起動が必要）
  write_timeout_ms: 30000      # 不変（再起動が必要）

cache:
  enabled: true                # SET cache.enabled で変更可能
  min_query_cost_ms: 10.0     # SET cache.min_query_cost_ms で変更可能
  ttl_seconds: 3600            # SET cache.ttl_seconds で変更可能
```

## 付録：クイックリファレンス

### コマンド

```bash
# すべての変数を表示
mygramclient -c "SHOW VARIABLES;"

# 変数を設定
mygramclient -c "SET logging.level = 'debug';"

# MySQLサーバーを切り替え（フェイルオーバー）
mygramclient -c "SET mysql.host = 'new-host.example.com';"

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
| `variable_changed` | INFO | 変数が変更された |
| `variable_set_failed` | ERROR | 変数設定が失敗（検証エラー） |
| `mysql_reconnection_started` | INFO | MySQL再接続を開始 |
| `mysql_reconnection_completed` | INFO | MySQL再接続が成功 |
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

## 10. SIGUSR1によるログローテーション

MygramDBは、nginxと同様にSIGUSR1シグナルを使用したゼロダウンタイムのログローテーションをサポートしています。サーバーを再起動せずにシームレスにログファイルをローテーションできます。

### 動作の仕組み

1. **現在のログファイルをリネーム**（logrotateまたは手動で）
2. **SIGUSR1シグナルを送信**（MygramDBプロセスに）
3. **MygramDBがログファイルを再オープン**（元のパスで新規ファイルを作成）

プロセスは即座に新しいファイルへのログ出力を開始し、古いファイル（リネーム済み）は圧縮やアーカイブが可能になります。

### 手動ローテーション

```bash
# 1. 現在のログファイルをリネーム
mv /var/log/mygramdb/app.log /var/log/mygramdb/app.log.1

# 2. SIGUSR1を送信してログファイルを再オープン
kill -USR1 $(pidof mygramdb)
# または
kill -USR1 $(cat /var/run/mygramdb.pid)

# 3. MygramDBが新しいapp.logを作成してログ出力を継続
```

### logrotate設定

`/etc/logrotate.d/mygramdb` を作成:

```conf
/var/log/mygramdb/*.log {
    daily
    rotate 7
    compress
    delaycompress
    missingok
    notifempty
    create 0640 mygramdb mygramdb
    sharedscripts
    postrotate
        kill -USR1 $(cat /var/run/mygramdb.pid 2>/dev/null) 2>/dev/null || true
    endscript
}
```

**設定オプション**:
- `daily`: 毎日ローテーション
- `rotate 7`: 7日分のログを保持
- `compress`: ローテーションしたログをgzipで圧縮
- `delaycompress`: 直近のローテーションファイルは圧縮しない（デバッグに便利）
- `missingok`: ログファイルが存在しなくてもエラーにしない
- `notifempty`: 空のファイルはローテーションしない
- `create 0640 mygramdb mygramdb`: 指定したパーミッションと所有者で新規ファイルを作成
- `sharedscripts`: マッチした全ファイルに対してpostrotateを1回だけ実行
- `postrotate`: ローテーション後に実行するコマンド（SIGUSR1を送信）

### 確認方法

SIGUSR1送信後、ログを確認:

```bash
# 新しいログファイルが作成されたことを確認
ls -la /var/log/mygramdb/

# 新しいログファイルに再オープン確認メッセージが含まれることを確認
grep "Log file reopened" /var/log/mygramdb/app.log
```

**ローテーション後の期待されるログエントリ**:
```
[2025-11-25 12:34:56.789] [mygramdb] [info] Log file reopened for rotation
```

### systemd連携

systemdを使用している場合、ログローテーション用のreloadターゲットを追加:

```ini
# /etc/systemd/system/mygramdb.service
[Service]
ExecReload=/bin/kill -USR1 $MAINPID
```

`systemctl reload mygramdb` でログローテーションを実行:

```bash
# systemdを使用してログをローテーション
systemctl reload mygramdb
```

### 重要な注意事項

1. **ファイルロギングが必要**: SIGUSR1はファイルロギングにのみ影響します。stdout出力の場合、シグナルは受信されますがアクションは実行されません。

2. **ログレベルは保持**: 再オープン後もログレベルは変更されません。

3. **シグナル安全性**: SIGUSR1はフラグを設定するだけで、実際のファイル再オープンはメインループで100ms以内に実行されます。

4. **エラーハンドリング**: 再オープンに失敗した場合、エラーはstderr（ログファイルではなく）に出力されます。

### トラブルシューティング

**SIGUSR1後にログファイルが作成されない**:
- ログディレクトリのファイルパーミッションを確認
- プロセスがシグナルを受信したことを確認: `grep "Log file reopened" /var/log/mygramdb/app.log`
- stderrでエラーを確認

**古いログファイルにまだ書き込まれている**:
- mvコマンドがSIGUSR1送信前に完了していることを確認
- プロセスPIDが正しいことを確認
- 正しいプロセスにシグナルが送信されたことを確認

### ソースファイル

- `src/app/signal_manager.h` - SIGUSR1シグナル処理
- `src/app/signal_manager.cpp` - シグナル登録とフラグ管理
- `src/app/configuration_manager.h` - ReopenLogFile()インターフェース
- `src/app/configuration_manager.cpp` - spdlogファイル再オープン実装
- `src/app/application.cpp` - メインループでのシグナルチェック

**最終更新**: 2025-11-25
