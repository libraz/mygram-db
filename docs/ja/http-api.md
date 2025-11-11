# HTTP API ガイド

MygramDB は、WebアプリケーションやHTTPクライアントとの統合を容易にするRESTful JSON APIを提供します。

## 設定

`config.yaml` でHTTPサーバーを有効化します：

```yaml
api:
  tcp:
    bind: "0.0.0.0"
    port: 11311
  http:
    enable: true          # HTTPサーバーを有効化
    bind: "127.0.0.1"     # バインドアドレス（デフォルト: ローカルホストのみ）
    port: 8080            # HTTPポート（デフォルト: 8080）
```

**セキュリティノート**: デフォルトでは、HTTPサーバーは `127.0.0.1`（ローカルホストのみ）にバインドします。他のマシンからの接続を受け入れる場合は、`bind: "0.0.0.0"` に設定し、`network.allow_cidrs` 設定を使用してアクセスを制限してください。

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

サーバー情報と統計。

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
  "total_requests": 15234,
  "document_count": 1000000,
  "ngram_size": 1
}
```

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

現在のサーバー設定（パスワードはマスクされます）。

**リクエスト:**

```http
GET /config HTTP/1.1
```

**レスポンス (200 OK):**

```json
{
  "mysql": {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "mydb",
    "user": "repl_user"
  },
  "api": {
    "tcp": {
      "bind": "0.0.0.0",
      "port": 11311
    },
    "http": {
      "enable": true,
      "bind": "127.0.0.1",
      "port": 8080
    }
  },
  "replication": {
    "enable": true,
    "server_id": 12345
  }
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

HTTPサーバーは、デフォルトでCORS（クロスオリジンリソース共有）サポートが有効になっており、異なるドメインからWebアプリケーションがリクエストできます。

**CORS ヘッダー:**

```
Access-Control-Allow-Origin: *
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

HTTP APIは監視ツールに適しています：

- **ヘルスチェック**: ロードバランサーのヘルスチェック用に `GET /health`
- **メトリクス**: Prometheus/監視統合用に `GET /info`
- **レプリケーションステータス**: レプリケーション監視用に `GET /replication/status`
