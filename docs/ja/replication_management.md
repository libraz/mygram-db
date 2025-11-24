# レプリケーション管理

このドキュメントは、MygramDBがさまざまな操作でMySQLレプリケーションを管理する方法について説明します。

## 概要

MygramDBは、重要な操作中にデータの整合性を確保するため、レプリケーションの開始/停止を自動的に管理します。自動管理が進行中の場合、手動でのレプリケーション制御はブロックされます。

## 自動レプリケーション管理

### DUMP SAVE操作

`DUMP SAVE`を実行する場合：

1. **操作前**: レプリケーションが実行中の場合、自動的に停止されます
   - `replication_paused_for_dump`フラグを`true`に設定
   - ログ: `"Stopped replication before DUMP SAVE (will auto-restart after completion)"`

2. **操作中**: 一貫したスナップショットでダンプファイルが作成されます

3. **操作後**: レプリケーションが自動的に再起動されます（保存の成功/失敗に関わらず）
   - `replication_paused_for_dump`フラグをクリア
   - ログ: `"Auto-restarted replication after DUMP SAVE"` (成功時)
   - ログ: `"replication_restart_failed"`イベント (失敗時、ただしDUMP SAVEは成功)

**理由**: DUMP SAVEは一貫したスナップショットを作成します。レプリケーションを停止することで、スナップショット作成中にbinlogイベントが処理されないようにし、ダンプされたデータとのGTID整合性を維持します。

### DUMP LOAD操作

`DUMP LOAD`を実行する場合：

1. **操作前**: レプリケーションが実行中の場合、自動的に停止されます
   - `replication_paused_for_dump`フラグを`true`に設定
   - ログ: `"Stopped replication before DUMP LOAD (will auto-restart after completion)"`

2. **操作中**: すべてのデータがクリアされ、ダンプファイルから再ロードされます

3. **操作後**: レプリケーションが更新されたGTIDで自動的に再起動されます
   - ロードが成功し、ダンプにGTIDが含まれる場合: ロードされたGTID位置にレプリケーションを更新
   - `replication_paused_for_dump`フラグをクリア
   - ログ: `"Auto-restarted replication after DUMP LOAD"`と新しいGTID (成功時)

**理由**: DUMP LOADはすべてのデータを置き換えます。ロード中にレプリケーションを実行すると以下の問題が発生します：
- データ破損（不完全なデータにbinlogイベントを適用）
- GTID不整合（現在の位置がロードされたデータと一致しない）

### SYNC操作

テーブルに対して`SYNC`を実行する場合：

1. **操作前**: SYNC操作はレプリケーションが実行中かどうかをチェックします
   - 異なるGTIDで実行中の場合、レプリケーションが停止されます
   - 完全なテーブル同期を実行

2. **操作後**: SYNC完了後にレプリケーションが自動的に開始されます
   - `SyncOperationManager`によって管理

**理由**: SYNCは完全なテーブルスキャンと再構築を実行します。完全同期の進行中に増分変更が適用されないように、レプリケーションを停止する必要があります。

### SET VARIABLES mysql.host / mysql.port

MySQL接続設定を変更する場合：

1. **操作前**: レプリケーションが自動的に停止されます
   - `mysql_reconnecting`フラグを`true`に設定
   - 現在のGTID位置を保存
   - 古いMySQL接続を閉じる
   - ログ: `"Stopping replication before MySQL reconnection"`

2. **再接続**: 新しいMySQL接続が確立されます
   - 接続が検証されます（GTIDモード、binlogフォーマット）
   - GTID位置が復元されます

3. **操作後**: 保存されたGTIDからレプリケーションが自動的に再起動されます
   - `mysql_reconnecting`フラグをクリア（成功または失敗時）
   - ログ: `"Restarted replication after MySQL reconnection"` (成功時)

**理由**: MySQLサーバー接続の変更には、レプリケーションの停止、再接続、および同じGTID位置からの再開が必要で、整合性を維持します。

**実装**: `MysqlReconnectionHandler::Reconnect()`で処理
**場所**: `src/app/mysql_reconnection_handler.cpp:32-133`

## 手動レプリケーション制御のブロック

### REPLICATION STARTのブロック

以下のシナリオでは、手動の`REPLICATION START`がブロックされます：

| シナリオ | チェックされるフラグ | エラーメッセージ |
|---------|------------------|----------------|
| MySQL再接続中 | `mysql_reconnecting` | "Cannot start replication while MySQL reconnection is in progress. Replication will automatically restart after reconnection completes." |
| DUMP SAVE/LOAD実行中 | `replication_paused_for_dump` | "Cannot start replication while DUMP SAVE/LOAD is in progress. Replication will automatically restart after DUMP completes." |
| DUMP SAVE実行中 | `read_only` | "Cannot start replication while DUMP SAVE is in progress. Please wait for save to complete." |
| DUMP LOAD実行中 | `loading` | "Cannot start replication while DUMP LOAD is in progress. Please wait for load to complete." |
| SYNC実行中 | `sync_manager->IsAnySyncing()` | "Cannot start replication while SYNC is in progress. SYNC will automatically start replication when complete." |

**実装**: `src/server/handlers/replication_handler.cpp:45-78`

### SET VARIABLESのブロック

SYNC中は`mysql.host`または`mysql.port`の変更がブロックされます：

```
Cannot change 'mysql.host' while SYNC is in progress. Please wait for SYNC to complete.
```

**実装**: `src/server/handlers/variable_handler.cpp:48-57`

## 状態フラグ

### `mysql_reconnecting` (atomic<bool>)

- **目的**: MySQL再接続が進行中であることを示す（mysql.host/port変更時）
- **`true`に設定**: `MysqlReconnectionHandler::Reconnect()`の開始時
- **`false`に設定**: 再接続完了後（成功または失敗）
- **使用者**:
  - `MysqlReconnectionHandler` - 自動再接続を管理
  - `ReplicationHandler` - 手動`REPLICATION START`をブロック

### `replication_paused_for_dump` (atomic<bool>)

- **目的**: DUMP操作のためにレプリケーションが自動的に一時停止されたことを示す
- **`true`に設定**: レプリケーション停止時のDUMP SAVE/LOAD前
- **`false`に設定**: レプリケーション再起動時のDUMP SAVE/LOAD後
- **使用者**:
  - `DumpHandler` - 自動一時停止/再開を管理
  - `ReplicationHandler` - 手動`REPLICATION START`をブロック

### `read_only` (atomic<bool>)

- **目的**: DUMP SAVEが進行中であることを示す（読み取り専用モード）
- **設定者**: `DumpHandler::HandleDumpSave()`内の`FlagGuard`
- **ブロック対象**: REPLICATION START、DUMP LOAD、同時DUMP SAVE

### `loading` (atomic<bool>)

- **目的**: DUMP LOADが進行中であることを示す
- **設定者**: `DumpHandler::HandleDumpLoad()`内の`FlagGuard`
- **ブロック対象**: REPLICATION START、DUMP SAVE、同時DUMP LOAD、OPTIMIZE

## テスト

### ユニットテスト

1. **Replication STARTブロックテスト** (`tests/server/replication_handler_test.cpp`)
   - `ReplicationStartBlockedDuringDumpLoad` - DUMP LOAD中のブロックを検証
   - `ReplicationStartBlockedDuringDumpSave` - DUMP SAVE中のブロックを検証
   - `ReplicationStartBlockedWhenPausedForDump` - `replication_paused_for_dump`フラグが手動再起動をブロックすることを検証
   - `BlockReplicationStartDuringSYNC` - SYNC操作中のブロックを検証

2. **Dump Handlerテスト** (`tests/server/dump_handler_test.cpp`)
   - 32個すべてのテストを`replication_paused_for_dump`と`mysql_reconnecting`フラグで更新
   - テストは適切なフラグの初期化とクリーンアップを検証

3. **サーバーコンポーネントテスト** (`tests/server/*_test.cpp`)
   - すべてのサーバーテストファイルを両方の新しいフラグで更新
   - ServerLifecycleManagerテストはフラグ参照が正しく渡されることを検証

### 統合テスト

実際のMySQLレプリケーションを使用した統合テストの場所：
- `tests/integration/mysql/gtid_dump_test.cpp` - DUMP操作全体のGTID整合性
- `tests/integration/mysql/failover_test.cpp` - レプリケーションフェイルオーバーと再接続

**注意**: BinlogReaderメソッドは仮想メソッドではないため、ユニットテストではモックを使用できません。レプリケーションの自動停止/再起動動作は、実際のMySQLインスタンスを使用した統合テストで検証されます。

## 実装詳細

### ファイルの場所

| コンポーネント | ファイル |
|-------------|---------|
| DUMP SAVE/LOADレプリケーション管理 | `src/server/handlers/dump_handler.cpp:120-130, 194-218, 280-290, 335-366` |
| 手動REPLICATION STARTブロック | `src/server/handlers/replication_handler.cpp:45-78` |
| SET VARIABLES mysql.host/portブロック | `src/server/handlers/variable_handler.cpp:48-57` |
| レプリケーション再起動を伴うMySQL再接続 | `src/app/mysql_reconnection_handler.cpp:32-133` |
| HandlerContextフラグ定義 | `src/server/server_types.h:103-107` |
| TcpServerフラグ所有権 | `src/server/tcp_server.h:106-107, 282-283` |
| ServerLifecycleManagerフラグ渡し | `src/server/server_lifecycle_manager.h:43, 95-97` |

### 構造化ログイベント

すべてのレプリケーション状態変更は構造化イベントでログに記録されます：

```cpp
// レプリケーション一時停止
StructuredLog()
    .Event("replication_paused")
    .Field("operation", "dump_save")
    .Field("reason", "automatic_pause_for_consistency")
    .Info();

// レプリケーション再開
StructuredLog()
    .Event("replication_resumed")
    .Field("operation", "dump_load")
    .Field("reason", "automatic_restart_after_completion")
    .Field("gtid", gtid)
    .Info();

// 再起動失敗（操作自体は成功）
StructuredLog()
    .Event("replication_restart_failed")
    .Field("operation", "dump_save")
    .Field("error", error_message)
    .Error();
```

## まとめ

| 操作 | レプリケーション管理 | 手動制御 |
|-----|------------------|---------|
| DUMP SAVE | 自動停止 → 自動再起動 | 操作中はブロック |
| DUMP LOAD | 自動停止 → 新しいGTIDで自動再起動 | 操作中はブロック |
| SYNC | 自動停止 → 完了後に自動再起動 | SYNC中はブロック |
| SET mysql.host/port | 自動停止 → 再接続 → 自動再起動 | 再接続中は手動REPLICATION STARTをブロック |
| REPLICATION START | N/A | DUMP/SYNC/MySQL再接続中はブロック |

すべての自動レプリケーション管理は、操作全体でデータの整合性とGTIDの完全性を保証します。

## 主要な設計判断

1. **すべてのコードパスで自動再起動**: 操作が成功しても失敗してもレプリケーションが再起動されます（失敗時はエラーログ記録）
2. **フラグベースのブロック**: すべてのブロックは操作開始時にチェックされるアトミックフラグを使用
3. **ユーザーフレンドリーなエラーメッセージ**: すべてのブロックエラーは理由を説明し、自動再起動を待つよう提案
4. **GTIDの保持**: すべての操作は停止/再起動サイクル全体でGTIDの連続性を維持
5. **自動操作中の手動介入不可**: 自動管理が進行中の場合、ユーザーは手動でレプリケーションを再起動できません
