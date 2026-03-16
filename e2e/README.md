# MygramDB E2E Test Suite

MySQL連携を含むエンドツーエンドの結合テスト・負荷テスト・エッジケーステストを自動実行するシステム。

## Quick Start

```bash
# 全テスト実行（Docker image build → compose up → pytest → compose down）
make e2e-test

# スモークテストのみ
make e2e-test-smoke

# 負荷テストのみ
make e2e-test-load

# 後片付け（テスト失敗時にコンテナが残った場合）
make e2e-test-cleanup
```

## 前提条件

- Docker / Docker Compose
- Python 3.10+
- `mygramdb:latest` Docker image（`make docker-build` で作成済みであること）

## アーキテクチャ

```
e2e/
├── docker/                     # テスト専用Docker環境
│   ├── docker-compose.yml      #   inttest_mysql + inttest_mygramdb（ホストポート非公開）
│   └── mysql-init/             #   テーブル定義 + FULLTEXT索引
├── lib/                        # 共通ヘルパー
│   ├── mysql_client.py         #   MySQL直接接続クライアント
│   ├── mygramdb_client.py      #   TCP + HTTPクライアント
│   ├── metrics.py              #   Prometheusメトリクスパーサー
│   ├── stats.py                #   統計計算（p50/p95/p99/QPS）
│   ├── data_generator.py       #   合成データ生成（シード固定）
│   ├── wait.py                 #   ポーリング待機ユーティリティ
│   └── wordlists/              #   英語/日本語/Unicodeワードリスト
├── tests/                      # テストスイート（14カテゴリ, 34ファイル, 70テスト）
│   ├── smoke/                  #   基本疎通（health, sync, info）
│   ├── replication/            #   INSERT/UPDATE/DELETE伝播
│   ├── search/                 #   検索精度・フィルタ・ページネーション
│   ├── unicode/                #   CJK・NFKC正規化・混合スクリプト
│   ├── edge_cases/             #   空文書・大容量・特殊文字
│   ├── ddl/                    #   TRUNCATE・ALTER TABLE
│   ├── concurrency/            #   書込中検索・高速UPDATE
│   ├── cache/                  #   Hit/Miss・無効化
│   ├── memory/                 #   メモリ圧迫・解放
│   ├── statistics/             #   Prometheusカウンタ正確性
│   ├── load/                   #   並行負荷・性能回帰検出
│   ├── persistence/            #   DUMP SAVE/LOAD往復
│   ├── resilience/             #   MySQL再起動復旧
│   └── multi_table/            #   複数テーブル独立性
├── benchmark.py                # CLIベンチマークツール
├── conftest.py                 # pytestフィクスチャ
├── pyproject.toml              # Python依存・pytest/ruff/mypy設定
├── run-all.sh                  # エントリポイント
└── results/                    # 実行時生成
    ├── reports/                #   JUnit XML
    ├── metrics/                #   Prometheusスナップショット
    └── baselines/              #   性能ベースライン（git管理）
```

## テストカテゴリとマーカー

| マーカー | カテゴリ | テスト数 | 内容 |
|---------|---------|---------|------|
| `smoke` | 基本疎通 | 7 | health endpoints, sync, info, TCP ping |
| `replication` | レプリケーション | 8 | INSERT/UPDATE/DELETE伝播, バッチ1000行 |
| `search` | 検索精度 | 10 | 単語検索, フィルタ, ページネーション, MySQL FULLTEXT比較 |
| `unicode` | Unicode | 9 | 日本語/中国語, NFKC, 全角半角, 絵文字 |
| `edge_cases` | 境界条件 | 8 | 空文書, 1MB文書, SQLインジェクション文字列 |
| `ddl` | DDLイベント | 4 | TRUNCATE, ALTER TABLE |
| `concurrency` | 並行アクセス | 4 | 書込中検索(10並列), 高速UPDATE |
| `cache` | キャッシュ | 4 | Miss→Hit, INSERT後無効化, CACHE CLEAR |
| `memory` | メモリ管理 | 3 | ソフト/ハードリミット, TRUNCATE後解放 |
| `statistics` | メトリクス | 8 | レプリケーション/コマンド/キャッシュカウンタ正確性 |
| `load` | 負荷テスト | 1 | 並行負荷, p99回帰検出 |
| `persistence` | 永続化 | 2 | DUMP SAVE→LOAD往復 |
| `resilience` | 障害復旧 | 2 | MySQL再起動後の再接続 |
| `multi_table` | 複数テーブル | 2 | インデックス独立性 |

### カテゴリ別実行

```bash
# pytestマーカーで選択実行
bash e2e/run-all.sh -m smoke
bash e2e/run-all.sh -m "replication or search"
bash e2e/run-all.sh -m "not load"

# 特定ファイルのみ
bash e2e/run-all.sh tests/unicode/test_cjk_search.py
```

## Docker環境

テスト専用のDocker環境を使用し、既存の開発環境と完全に分離されている。

- **コンテナ名**: `inttest_mysql`, `inttest_mygramdb`（既存の `mygramdb_*` と衝突しない）
- **ネットワーク**: `inttest_network`（Docker内部のみ、ホストポート公開なし）
- **メモリ制限**: MygramDB 200MB hard limit / 150MB soft target（メモリ圧迫テスト用）
- **MySQL**: 8.4, GTID有効, binlog ROW形式, utf8mb4

## データ生成

外部ダウンロード不要。チェックイン済みワードリストからシード固定で合成生成。

| データセット | 行数 | 用途 |
|-------------|------|------|
| seed_data | 100 | スモーク・基本検証（session fixture） |
| load test | 1,000+ | 負荷テスト（自動拡張） |
| edge_cases | ~15 | 空文字, 1MB, 絵文字, SQL injection等 |

## ベンチマークCLI

`support/benchmark/benchmark.py` から移行した統合ベンチマークツール。

```bash
# MygramDBベンチマーク
make e2e-benchmark

# カスタム実行
cd e2e && python3 benchmark.py \
  --target mygramdb \
  --table articles \
  --words "hello,world,test" \
  --concurrency 50 \
  --iterations 10 \
  --json-output results/benchmark.json
```

## Python開発

```bash
# リント
make e2e-lint

# フォーマット
make e2e-format

# リント修正 + フォーマット
make e2e-fix
```

## 成功/失敗基準

| カテゴリ | 成功条件 | 失敗条件 |
|---------|---------|---------|
| Smoke | 全チェック pass | 1つでも失敗 |
| Replication | 10秒以内に反映 | タイムアウト or 不一致 |
| Search | 結果セットが期待通り | 不一致 |
| Unicode | 全正規化テスト pass | 検索漏れ |
| Edge cases | クラッシュなし | クラッシュ or ハング |
| DDL | インデックス状態正常 | 不正カウント |
| Concurrency | 最終状態一致 | データ破壊 |
| Cache | Hit/Miss/無効化が正しい | 不正キャッシュ |
| Memory | OOMクラッシュなし | OOM kill |
| Statistics | カウンタが実操作と一致 | ズレ |
| Load | p99 < baseline×1.2, エラー率<1% | 性能劣化 |
| Persistence | ラウンドトリップでデータ保全 | データ消失 |
| Resilience | 60秒以内に再接続 | スタック |
| Multi-table | テーブル間独立 | クロス汚染 |

全カテゴリ pass で終了コード 0。1つでも失敗で非0。
