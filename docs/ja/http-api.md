# HTTP API ガイド

MygramDB は、WebアプリケーションやHTTPクライアントとの統合を容易にするRESTful JSON APIを提供します。

## 設定

`config.yaml` でHTTPサーバーを有効化します：

```yaml
api:
  tcp:
    bind: "127.0.0.1"
    port: 11016
  http:
    enable: true          # HTTPサーバーを有効化
    bind: "127.0.0.1"     # バインドアドレス（デフォルト: ローカルホストのみ）
    port: 8080            # HTTPポート（デフォルト: 8080）
    enable_cors: false    # ブラウザ公開時のみ有効化
    cors_allow_origin: "" # CORS有効時は許可するOriginを指定
```

**セキュリティノート**: デフォルトでは TCP/HTTP サーバーはループバックにのみバインドします。公開する必要がある場合は `api.tcp.bind` / `api.http.bind` を明示的に設定し、`network.allow_cidrs` で許可 IP を厳密に指定し、MygramDB の前段に TLS/認証付きリバースプロキシ等を配置してください。CORS はデフォルトで無効であり、信頼できるOriginに限定して使用します。

## API エンドポイント

すべてのレスポンスは `Content-Type: application/json` のJSON形式です。

### POST /{table}/search

フィルタとページネーションを使用した全文検索。

**リクエスト:**

```http
POST /threads/search HTTP/1.1
Content-Type: application/json

{
  "q": "breaking news AND tech NOT old",
  "filters": {
    "status": 1,
    "category": "tech"
  },
  "limit": 50,
  "offset": 0
}
```

**リクエストボディパラメータ:**

| フィールド | 型 | 必須 | 説明 |
|-----------|-----|------|------|
| `q` | string | はい | AND/NOT演算子を使用した検索クエリ |
| `filters` | object | いいえ | フィルタ条件（カラム: 値のペア） |
| `limit` | integer | いいえ | 返す最大結果数（デフォルト: 100、最大: 1000） |
| `offset` | integer | いいえ | スキップする結果数（デフォルト: 0） |

**クエリ構文:**

- **シンプル検索**: `"keyword"`
- **引用符付きフレーズ**: `"\"breaking news\""` （完全一致フレーズを検索）
- **AND演算子**: `"tech AND AI AND machine learning"`
- **NOT演算子**: `"news NOT sports"`
- **組み合わせ**: `"tech AND AI NOT old"`

**レスポンス (200 OK):**

```json
{
  "count": 2,
  "limit": 50,
  "offset": 0,
  "results": [
    {
      "doc_id": 101,
      "primary_key": "article_101",
      "filters": {
        "status": 1,
        "category": "tech"
      }
    },
    {
      "doc_id": 205,
      "primary_key": "article_205",
      "filters": {
        "status": 1,
        "category": "tech"
      }
    }
  ]
}
```

**エラーレスポンス (400 Bad Request):**

```json
{
  "error": "Missing required field: q"
}
```

**エラーレスポンス (500 Internal Server Error):**

```json
{
  "error": "Internal error: database connection failed"
}
```

### GET /{table}/{id}

ドキュメントIDで単一のドキュメントを取得。

**リクエスト:**

```http
GET /threads/12345 HTTP/1.1
```

**レスポンス (200 OK):**

```json
{
  "doc_id": 12345,
  "primary_key": "thread_12345",
  "filters": {
    "status": 1,
    "user_id": 42
  }
}
```

**エラーレスポンス (404 Not Found):**

```json
{
  "error": "Document not found"
}
```

### GET /info

サーバー情報と詳細統計（Redis風の監視情報）。

**リクエスト:**

```http
GET /info HTTP/1.1
```

**レスポンス (200 OK):**

```json
{
  "server": "MygramDB",
  "version": "1.0.0",
  "uptime_seconds": 3600,
  "total_requests": 15000,
  "total_commands_processed": 15000,
  "memory": {
    "used_memory_bytes": 524288000,
    "used_memory_human": "500.00 MB",
    "peak_memory_bytes": 629145600,
    "peak_memory_human": "600.00 MB",
    "used_memory_index": "400.00 MB",
    "used_memory_documents": "100.00 MB",
    "total_system_memory": 17179869184,
    "total_system_memory_human": "16.00 GB",
    "available_system_memory": 9126805504,
    "available_system_memory_human": "8.50 GB",
    "system_memory_usage_ratio": 0.47,
    "process_rss": 545259520,
    "process_rss_human": "520.00 MB",
    "process_rss_peak": 629145600,
    "process_rss_peak_human": "600.00 MB",
    "memory_health": "HEALTHY"
  },
  "index": {
    "total_documents": 1000000,
    "total_terms": 1500000,
    "total_postings": 5000000,
    "avg_postings_per_term": 3.33,
    "delta_encoded_lists": 1200000,
    "roaring_bitmap_lists": 300000
  },
  "tables": {
    "products": {
      "documents": 500000,
      "terms": 800000,
      "postings": 2500000,
      "ngram_size": 2,
      "memory_bytes": 262144000,
      "memory_human": "250.00 MB"
    },
    "users": {
      "documents": 500000,
      "terms": 700000,
      "postings": 2500000,
      "ngram_size": 1,
      "memory_bytes": 262144000,
      "memory_human": "250.00 MB"
    }
  }
}
```

**レスポンスフィールド:**

| フィールド | 説明 |
|-----------|------|
| `server` | サーバー名（MygramDB） |
| `version` | サーバーバージョン |
| `uptime_seconds` | サーバー稼働時間（秒） |
| `total_requests` | 処理されたリクエストの総数 |
| `total_commands_processed` | 処理されたコマンドの総数 |
| **メモリ（アプリケーション）** | |
| `memory.used_memory_bytes` | 現在のメモリ使用量（バイト）（インデックス + ドキュメント） |
| `memory.used_memory_human` | 人間が読みやすい形式の現在のメモリ使用量 |
| `memory.peak_memory_bytes` | ピーク時のメモリ使用量（バイト） |
| `memory.peak_memory_human` | 人間が読みやすい形式のピークメモリ使用量 |
| `memory.used_memory_index` | インデックスが使用しているメモリ |
| `memory.used_memory_documents` | ドキュメントストアが使用しているメモリ |
| **メモリ（システム）** | |
| `memory.total_system_memory` | 物理RAM総容量（バイト） |
| `memory.total_system_memory_human` | 人間が読みやすい形式のシステムメモリ総容量 |
| `memory.available_system_memory` | 利用可能な物理RAM（バイト） |
| `memory.available_system_memory_human` | 人間が読みやすい形式の利用可能メモリ |
| `memory.system_memory_usage_ratio` | システム全体のメモリ使用率（0.0-1.0） |
| **メモリ（プロセス）** | |
| `memory.process_rss` | プロセスRSS（使用中の物理メモリ）（バイト） |
| `memory.process_rss_human` | 人間が読みやすい形式のプロセスRSS |
| `memory.process_rss_peak` | プロセス開始以降のRSSピーク値（バイト） |
| `memory.process_rss_peak_human` | 人間が読みやすい形式のRSSピーク値 |
| **メモリ（ヘルス）** | |
| `memory.memory_health` | メモリヘルスステータス（HEALTHY/WARNING/CRITICAL/UNKNOWN） |
| **インデックス（集計）** | |
| `index.total_documents` | 全テーブルのドキュメント総数 |
| `index.total_terms` | ユニーク語句の総数 |
| `index.total_postings` | ポスティングの総数 |
| `index.avg_postings_per_term` | 語句あたりの平均ポスティング数 |
| `index.delta_encoded_lists` | Delta圧縮を使用しているポスティングリスト数 |
| `index.roaring_bitmap_lists` | Roaring Bitmapを使用しているポスティングリスト数 |
| **テーブル（テーブルごと）** | |
| `tables.<name>.documents` | テーブル内のドキュメント数 |
| `tables.<name>.terms` | テーブル内の語句数 |
| `tables.<name>.postings` | テーブル内のポスティング数 |
| `tables.<name>.ngram_size` | テーブルのN-gramサイズ |
| `tables.<name>.memory_bytes` | テーブルのメモリ使用量（バイト） |
| `tables.<name>.memory_human` | 人間が読みやすい形式のテーブルメモリ使用量 |

**メモリヘルスステータス:**
- `HEALTHY`: システムメモリの20%以上が利用可能
- `WARNING`: システムメモリの10-20%が利用可能
- `CRITICAL`: システムメモリの10%未満が利用可能（OPTIMIZEは拒否されます）
- `UNKNOWN`: ステータスを判定できない

このエンドポイントはJSON形式をサポートする監視ツールとの統合に適しています。

### GET /metrics

監視とアラーティングのためのPrometheus Exposition Format形式のメトリクスエンドポイント。

**リクエスト:**

```http
GET /metrics HTTP/1.1
```

**レスポンス (200 OK):**

```prometheus
# HELP mygramdb_server_info MygramDB server information
# TYPE mygramdb_server_info gauge
mygramdb_server_info{version="1.0.0"} 1

# HELP mygramdb_server_uptime_seconds Server uptime in seconds
# TYPE mygramdb_server_uptime_seconds counter
mygramdb_server_uptime_seconds 3600

# HELP mygramdb_memory_used_bytes Current memory usage in bytes
# TYPE mygramdb_memory_used_bytes gauge
mygramdb_memory_used_bytes{type="index"} 419430400
mygramdb_memory_used_bytes{type="documents"} 104857600
mygramdb_memory_used_bytes{type="total"} 524288000

# HELP mygramdb_memory_health_status Memory health status (0=UNKNOWN, 1=HEALTHY, 2=WARNING, 3=CRITICAL)
# TYPE mygramdb_memory_health_status gauge
mygramdb_memory_health_status 1

# HELP mygramdb_index_documents_total Total number of documents in the index
# TYPE mygramdb_index_documents_total gauge
mygramdb_index_documents_total{table="products"} 500000
mygramdb_index_documents_total{table="users"} 500000

# HELP mygramdb_command_total Total number of commands executed by type
# TYPE mygramdb_command_total counter
mygramdb_command_total{command="search"} 10000
mygramdb_command_total{command="count"} 2000
mygramdb_command_total{command="get"} 3000
```

**Content-Type**: `text/plain; version=0.0.4; charset=utf-8`

**メトリクスカテゴリ:**

| カテゴリ | 説明 |
|----------|------|
| **サーバーメトリクス** | サーバーバージョン、稼働時間、処理されたコマンド数 |
| **コマンド統計** | コマンドタイプ別の実行回数（search、count、get、infoなど） |
| **メモリメトリクス** | アプリケーションメモリ（index/documents）、システムメモリ、プロセスRSS、ヘルスステータス |
| **インデックスメトリクス** | ドキュメント数、term数、posting数、最適化ステータス（`table`ラベル付きテーブル別） |
| **クライアントメトリクス** | 現在の接続数、累計接続数 |
| **レプリケーションメトリクス** | レプリケーションステータス、処理イベント数、操作別カウンタ（MySQLビルドのみ） |

**メトリクスタイプ:**

- **Counter**: 単調増加する値（例: `mygramdb_command_total`）
- **Gauge**: 増減する値（例: `mygramdb_memory_used_bytes`）

**Prometheusスクレイプ設定:**

```yaml
scrape_configs:
  - job_name: 'mygramdb'
    scrape_interval: 15s
    static_configs:
      - targets: ['localhost:8080']
        labels:
          environment: 'production'
```

**主要機能:**

- **標準Prometheus形式**: すべてのPrometheusベースの監視スタックと互換性あり
- **多次元メトリクス**: ラベルによるグルーピング（例: `table`、`command`、`status`）
- **メモリヘルストラッキング**: アラーティング用の数値ステータス値（1=HEALTHY、2=WARNING、3=CRITICAL）
- **テーブル別メトリクス**: テーブル名ごとに分類されたインデックス統計
- **後方互換性**: 既存の`/info`エンドポイントは変更なし

**/infoとの比較:**

| 機能 | `/info` | `/metrics` |
|------|---------|------------|
| フォーマット | JSON | Prometheusテキスト |
| 用途 | 一般的な監視、デバッグ | Prometheus/Grafana統合 |
| メトリクスタイプ | 汎用的な値 | 型付きメトリクス（Counter/Gauge） |
| 多次元対応 | 限定的 | 完全なラベルサポート |
| 互換性 | 任意のHTTPクライアント | Prometheusエコシステム |

両方のエンドポイントは同じ基礎データを提供しますが、異なる形式です。Prometheus統合には`/metrics`を使用し、一般的な監視や人間が読みやすい出力には`/info`を使用してください。

### GET /health

ロードバランサーと監視用のヘルスチェックエンドポイント。

**リクエスト:**

```http
GET /health HTTP/1.1
```

**レスポンス (200 OK):**

```json
{
  "status": "ok",
  "timestamp": 1699000000
}
```

### GET /config

現在のサーバー設定サマリ（機密値は返却されません）。

**リクエスト:**

```http
GET /config HTTP/1.1
```

**レスポンス (200 OK):**

```json
{
  "mysql": {
    "configured": true,
    "database_defined": true
  },
  "api": {
    "tcp": {
      "enabled": true
    },
    "http": {
      "enabled": true,
      "cors_enabled": false
    }
  },
  "network": {
    "allow_cidrs_configured": false
  },
  "replication": {
    "enable": true
  },
  "notes": "機密情報はHTTP経由では提供されません。安全な接続上で CONFIG SHOW を利用してください。"
}
```

### GET /replication/status

MySQLレプリケーションステータス（レプリケーション有効時のみ）。

**リクエスト:**

```http
GET /replication/status HTTP/1.1
```

**レスポンス (200 OK):**

```json
{
  "enabled": true,
  "current_gtid": "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
}
```

**エラーレスポンス (503 Service Unavailable):**

```json
{
  "error": "Replication not configured"
}
```

## CORS サポート

ブラウザから直接アクセスする場合は `api.http.enable_cors: true` を設定し、`api.http.cors_allow_origin` に信頼できるOriginを指定します。不要な場合は CORS を無効のままにしてください。

**CORS ヘッダー:**

```
Access-Control-Allow-Origin: https://app.example.com
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type
```

## 使用例

### cURL

**検索:**

```bash
curl -X POST http://localhost:8080/threads/search \
  -H "Content-Type: application/json" \
  -d '{
    "q": "機械学習 AND Python",
    "filters": {"status": 1},
    "limit": 10
  }'
```

**ドキュメント取得:**

```bash
curl http://localhost:8080/threads/12345
```

**ヘルスチェック:**

```bash
curl http://localhost:8080/health
```

### JavaScript (fetch)

```javascript
// 検索
const response = await fetch('http://localhost:8080/threads/search', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    q: '機械学習 AND Python',
    filters: { status: 1 },
    limit: 10
  })
});

const data = await response.json();
console.log(`${data.count} 件の結果が見つかりました`);
data.results.forEach(doc => {
  console.log(`ドキュメント ${doc.doc_id}: ${doc.primary_key}`);
});
```

### Python (requests)

```python
import requests

# 検索
response = requests.post('http://localhost:8080/threads/search', json={
    'q': '機械学習 AND Python',
    'filters': {'status': 1},
    'limit': 10
})

data = response.json()
print(f"{data['count']} 件の結果が見つかりました")
for doc in data['results']:
    print(f"ドキュメント {doc['doc_id']}: {doc['primary_key']}")
```

## パフォーマンスの考慮事項

- **コネクションプーリング**: より良いパフォーマンスのためにHTTP keep-aliveを使用
- **ページネーション**: 大きな結果セットには `limit` と `offset` を使用
- **キャッシング**: アプリケーション層で頻繁なクエリのキャッシングを検討
- **ネットワークセキュリティ**: `network.allow_cidrs` を使用して信頼できるIP範囲へのアクセスを制限

## エラーハンドリング

すべてのエラーレスポンスは次の形式に従います：

```json
{
  "error": "エラーメッセージの説明"
}
```

**HTTPステータスコード:**

| コード | 説明 |
|--------|------|
| 200 | 成功 |
| 400 | 不正なリクエスト（無効な入力） |
| 404 | 見つかりません（ドキュメントが存在しない） |
| 500 | 内部サーバーエラー |
| 503 | サービス利用不可（機能が有効でない） |

## 監視

HTTP APIは監視と可観測性のための複数のエンドポイントを提供します：

- **ヘルスチェック**: `GET /health` - ロードバランサー用のシンプルなヘルスチェック
- **JSONメトリクス**: `GET /info` - 一般的な監視ツール用のJSON形式の詳細統計
- **Prometheusメトリクス**: `GET /metrics` - 時系列監視とアラーティング用のPrometheus互換メトリクス
- **レプリケーションステータス**: `GET /replication/status` - MySQLレプリケーションステータス

### 監視スタック統合

**Prometheus + Grafana:**

1. Prometheusで`/metrics`エンドポイントをスクレイプするよう設定
2. MygramDB用のGrafanaダッシュボードをインポート
3. メモリヘルス、クエリレイテンシ、レプリケーション遅延に基づくアラートを設定

**その他の監視ツール:**

- **Datadog/New Relic**: `/info`のJSONエンドポイントをパース
- **Zabbix**: `/health`と`/info`へのHTTPエージェントチェック
- **Nagios/Icinga**: `/health`エンドポイントを使用したチェックスクリプト
