# スナップショットコマンド

MygramDB は、データベースの状態をバックアップおよび復元するためのスナップショットコマンドを提供します。スナップショットは、すべてのインデックス化されたデータ、設定、およびレプリケーション位置（GTID）を含むデータベース全体をキャプチャします。

## 概要

スナップショットは、以下を含む単一のバイナリファイル（`.dmp`）です：
- 完全なデータベース状態（すべてのテーブル、ドキュメント、インデックス）
- 設定情報
- シームレスなリカバリのためのレプリケーション位置（GTID）
- 破損検出のための整合性チェックサム（CRC32）

## コマンド

### DUMP SAVE - スナップショット作成

現在のデータベース状態をスナップショットファイルに保存します。

**構文:**
```
DUMP SAVE <filepath> [WITH STATISTICS]
```

**パラメータ:**
- `<filepath>`: スナップショットファイルの保存先パス
- `WITH STATISTICS`（オプション）: パフォーマンス統計情報をスナップショットに含める

**例:**
```sql
-- 基本的なスナップショット
DUMP SAVE /var/lib/mygramdb/snapshots/mygramdb.dmp

-- 統計情報付きスナップショット
DUMP SAVE /var/lib/mygramdb/snapshots/mygramdb.dmp WITH STATISTICS
```

**レスポンス:**
```
OK SAVE /var/lib/mygramdb/snapshots/mygramdb.dmp
tables: 2
size: 1234567 bytes
gtid: 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10
```

### DUMP LOAD - スナップショット復元

スナップショットファイルを読み込み、データベース状態を復元します。

**構文:**
```
DUMP LOAD <filepath>
```

**パラメータ:**
- `<filepath>`: 読み込むスナップショットファイルのパス

**例:**
```sql
DUMP LOAD /var/lib/mygramdb/snapshots/mygramdb.dmp
```

**レスポンス:**
```
OK LOAD /var/lib/mygramdb/snapshots/mygramdb.dmp
tables: 2
documents: 10000
gtid: 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10
```

**重要な注意事項:**
- スナップショットの読み込みは、現在のすべてのデータを**置き換えます**
- データベースは、スナップショットに保存されたGTIDからレプリケーションを再開します
- すべてのテーブルが現在の設定ファイルに定義されている必要があります

### DUMP VERIFY - 整合性チェック

スナップショットファイルを読み込まずに整合性を検証します。

**構文:**
```
DUMP VERIFY <filepath>
```

**パラメータ:**
- `<filepath>`: 検証するスナップショットファイルのパス

**例:**
```sql
DUMP VERIFY /var/lib/mygramdb/snapshots/mygramdb.dmp
```

**レスポンス（成功）:**
```
OK VERIFY /var/lib/mygramdb/snapshots/mygramdb.dmp
status: valid
crc: verified
size: verified
```

**レスポンス（失敗）:**
```
ERROR CRC mismatch: file may be corrupted
expected: 0x12345678
actual: 0x87654321
```

### DUMP INFO - スナップショットメタデータ表示

スナップショットファイルを読み込まずにメタデータを表示します。

**構文:**
```
DUMP INFO <filepath>
```

**パラメータ:**
- `<filepath>`: スナップショットファイルのパス

**例:**
```sql
DUMP INFO /var/lib/mygramdb/snapshots/mygramdb.dmp
```

**レスポンス:**
```
OK INFO /var/lib/mygramdb/snapshots/mygramdb.dmp
version: 1
gtid: 3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10
tables: 2
flags: 0x18
  statistics: yes
size: 1234567 bytes
timestamp: 1672531200 (2023-01-01 00:00:00 UTC)
```

## 整合性保護

### CRC32チェックサム

すべてのスナップショットファイルには、複数レベルのCRC32チェックサムが含まれます：
- **ファイルレベルCRC**: ファイル全体の破損を検出
- **セクションレベルCRC**: 個別セクション（設定、統計、インデックス、ドキュメントストア）を検証

### ファイルサイズ検証

スナップショットヘッダーには期待されるファイルサイズが含まれます：
- 不完全な書き込みを検出
- ネットワーク転送の失敗をキャッチ
- 切り詰められたファイルを識別

### 検証ワークフロー

読み込み前に必ずスナップショットの整合性を検証してください：

```sql
-- 1. 整合性を検証
DUMP VERIFY mygramdb.dmp

-- 2. メタデータを確認
DUMP INFO mygramdb.dmp

-- 3. 問題なければ読み込み
DUMP LOAD mygramdb.dmp
```

## エラーハンドリング

### CRC不一致

CRC検証が失敗した場合、スナップショットファイルが破損しています：

```
ERROR CRC32 mismatch: expected 0x12345678, got 0x87654321 (file may be corrupted)
```

**対処方法:**
1. 破損したスナップショットは読み込まない
2. バックアップコピーから復元を試みる
3. 破損が頻繁に発生する場合はディスクの健全性をチェック

### ファイル切り詰め

ファイルサイズが期待値と一致しない場合：

```
ERROR File size mismatch: expected 1234567 bytes, got 1000000 bytes (file may be truncated or corrupted)
```

**対処方法:**
1. ファイルが完全に書き込まれなかった
2. SAVE操作を再試行
3. 利用可能なディスク容量を確認

### バージョン不一致

スナップショットのバージョンが新しすぎる場合：

```
ERROR Snapshot file version 2 is newer than supported version 1
```

**対処方法:**
1. MygramDBを新しいバージョンにアップグレード
2. または互換性のあるバージョンで作成されたスナップショットを使用

## ベストプラクティス

### 定期的なバックアップ

災害復旧のために定期的なスナップショットをスケジュールします：

```bash
# 例: 日次バックアップスクリプト
#!/bin/bash
DATE=$(date +%Y%m%d)
echo "DUMP SAVE /var/lib/mygramdb/snapshots/mygramdb_${DATE}.dmp WITH STATISTICS" | mygram-cli
```

### 保持ポリシー

複数のスナップショットバージョンを保持します：

```yaml
# config.yaml
snapshot:
  dir: /var/lib/mygramdb/snapshots
  default_filename: mygramdb.dmp
  interval_sec: 600      # 10分ごとに保存
  retain: 3              # 最新の3つを保持
```

### 読み込み前の検証

読み込み前に必ず整合性を検証します：

```bash
# まず検証
echo "DUMP VERIFY mygramdb.dmp" | mygram-cli

# その後読み込み
echo "DUMP LOAD mygramdb.dmp" | mygram-cli
```

### 安全な保存

スナップショットには機密データが含まれます：
- MySQL接続認証情報
- すべてのドキュメント内容
- レプリケーション位置

**推奨事項:**
1. ファイル権限を600（所有者の読み書きのみ）に設定
2. 暗号化されたボリュームにスナップショットを保存
3. リモートストレージには安全な転送プロトコル（SFTP、SCP）を使用
4. アクセスログを監査

### スナップショットサイズの監視

スナップショットファイルサイズを経時的に追跡します：

```bash
# スナップショットメタデータを確認
echo "DUMP INFO mygramdb.dmp" | mygram-cli
```

スナップショットサイズが予期せず増加する場合：
- インデックスの肥大化をチェック
- ドキュメント保持ポリシーを確認
- データアーカイブを検討

## パフォーマンス特性

### SAVE パフォーマンス

| ドキュメント数 | 平均テキスト長 | 保存時間 | ファイルサイズ |
|---------------|---------------|---------|---------------|
| 10万 | 100バイト | ~1-2秒 | ~20 MB |
| 100万 | 100バイト | ~10-15秒 | ~200 MB |
| 1000万 | 100バイト | ~2-3分 | ~2 GB |

### LOAD パフォーマンス

| ドキュメント数 | 平均テキスト長 | 読み込み時間 |
|---------------|---------------|-------------|
| 10万 | 100バイト | ~2-3秒 |
| 100万 | 100バイト | ~15-20秒 |
| 1000万 | 100バイト | ~3-5分 |

*パフォーマンスはハードウェア、ディスクI/O、データ特性によって異なります*

## 設定

スナップショットの動作は `config.yaml` で設定できます：

```yaml
snapshot:
  # スナップショットファイルのディレクトリ
  dir: /var/lib/mygramdb/snapshots

  # 指定されない場合のデフォルトファイル名
  default_filename: mygramdb.dmp

  # 自動スナップショット間隔（秒）
  interval_sec: 600

  # 保持するスナップショット数（0 = 無制限）
  retain: 3
```

## レプリケーションリカバリ

スナップショットを読み込むと、MygramDBは自動的にレプリケーションを再開します：

1. **GTID抽出**: スナップショットからレプリケーション位置を読み取り
2. **BinlogReader再開**: 保存されたGTIDから継続
3. **追いつき**: 見逃したトランザクションを処理
4. **通常運用**: リアルタイムレプリケーションに戻る

**例:**

```
# 1. サーバーがGTID 1-100でクラッシュ
# 2. GTID 1-80のスナップショットを読み込み
DUMP LOAD mygramdb.dmp

# 3. MygramDBは自動的にトランザクション81-100を処理
# 4. 101+からリアルタイムレプリケーションを再開
```

## トラブルシューティング

### "Failed to open snapshot file"

**原因:** ファイルが存在しないか、権限が不十分

**解決方法:**
```bash
# ファイルの存在を確認
ls -l /path/to/snapshot.dmp

# 権限を確認
chmod 600 /path/to/snapshot.dmp
```

### "Invalid snapshot version"

**原因:** 互換性のないMygramDBバージョンで作成されたスナップショット

**解決方法:**
- MygramDBバージョンの互換性を確認
- 同じメジャーバージョンで作成されたスナップショットを使用

### "Table not found in config"

**原因:** スナップショットに現在の設定で定義されていないテーブルが含まれる

**解決方法:**
- `config.yaml`を更新してスナップショットのすべてのテーブルを含める
- または現在の設定で新しいスナップショットを作成

### "CRC mismatch during load"

**原因:** スナップショットファイルが破損

**解決方法:**
1. DUMP VERIFYを実行して破損セクションを特定
2. バックアップコピーから復元
3. ディスクの健全性とネットワークの信頼性を確認

## 関連コマンド

- **SEARCH**: インデックス化されたドキュメントをクエリ
- **STATUS**: データベースステータスとレプリケーション位置を確認
- **CONFIG RELOAD**: 再起動なしで設定を再読み込み

## 参照

- [設定ガイド](configuration.md)
- [レプリケーション設定](replication.md)
- [パフォーマンスチューニング](performance.md)
