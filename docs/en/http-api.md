# HTTP API Guide

MygramDB provides a RESTful JSON API for easy integration with web applications and HTTP clients.

## Configuration

Enable the HTTP server in your `config.yaml`:

```yaml
api:
  tcp:
    bind: "0.0.0.0"
    port: 11016
  http:
    enable: true          # Enable HTTP server
    bind: "127.0.0.1"     # Bind address (default: localhost only)
    port: 8080            # HTTP port (default: 8080)
```

**Security Note**: By default, the HTTP server binds to `127.0.0.1` (localhost only). To accept connections from other machines, set `bind: "0.0.0.0"` and use the `network.allow_cidrs` setting to restrict access.

## API Endpoints

All responses are in JSON format with `Content-Type: application/json`.

### POST /{table}/search

Full-text search with filters and pagination.

**Request:**

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

**Request Body Parameters:**

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `q` | string | Yes | Search query with AND/NOT operators |
| `filters` | object | No | Filter conditions (column: value pairs) |
| `limit` | integer | No | Maximum results to return (default: 100, max: 1000) |
| `offset` | integer | No | Number of results to skip (default: 0) |

**Query Syntax:**

- **Simple search**: `"keyword"`
- **Quoted phrases**: `"\"breaking news\""` (searches for exact phrase)
- **AND operator**: `"tech AND AI AND machine learning"`
- **NOT operator**: `"news NOT sports"`
- **Combined**: `"tech AND AI NOT old"`

**Response (200 OK):**

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

**Error Response (400 Bad Request):**

```json
{
  "error": "Missing required field: q"
}
```

**Error Response (500 Internal Server Error):**

```json
{
  "error": "Internal error: database connection failed"
}
```

### GET /{table}/{id}

Get a single document by its document ID.

**Request:**

```http
GET /threads/12345 HTTP/1.1
```

**Response (200 OK):**

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

**Error Response (404 Not Found):**

```json
{
  "error": "Document not found"
}
```

### GET /info

Server information and detailed statistics (Redis-style monitoring).

**Request:**

```http
GET /info HTTP/1.1
```

**Response (200 OK):**

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

**Response Fields:**

| Field | Description |
|-------|-------------|
| `server` | Server name (MygramDB) |
| `version` | Server version |
| `uptime_seconds` | Server uptime in seconds |
| `total_requests` | Total number of requests processed |
| `total_commands_processed` | Total number of commands processed |
| **Memory (Application)** | |
| `memory.used_memory_bytes` | Current memory usage in bytes (index + documents) |
| `memory.used_memory_human` | Human-readable current memory usage |
| `memory.peak_memory_bytes` | Peak memory usage in bytes |
| `memory.peak_memory_human` | Human-readable peak memory usage |
| `memory.used_memory_index` | Memory used by index |
| `memory.used_memory_documents` | Memory used by document store |
| **Memory (System)** | |
| `memory.total_system_memory` | Total physical RAM in bytes |
| `memory.total_system_memory_human` | Human-readable total system memory |
| `memory.available_system_memory` | Available physical RAM in bytes |
| `memory.available_system_memory_human` | Human-readable available memory |
| `memory.system_memory_usage_ratio` | System-wide memory usage (0.0-1.0) |
| **Memory (Process)** | |
| `memory.process_rss` | Process RSS (physical memory used) in bytes |
| `memory.process_rss_human` | Human-readable process RSS |
| `memory.process_rss_peak` | Peak RSS since process start in bytes |
| `memory.process_rss_peak_human` | Human-readable peak RSS |
| **Memory (Health)** | |
| `memory.memory_health` | Memory health status (HEALTHY/WARNING/CRITICAL/UNKNOWN) |
| **Index (Aggregated)** | |
| `index.total_documents` | Total number of documents across all tables |
| `index.total_terms` | Total number of unique terms |
| `index.total_postings` | Total number of postings |
| `index.avg_postings_per_term` | Average postings per term |
| `index.delta_encoded_lists` | Number of posting lists using delta encoding |
| `index.roaring_bitmap_lists` | Number of posting lists using Roaring Bitmaps |
| **Tables (Per-table)** | |
| `tables.<name>.documents` | Number of documents in table |
| `tables.<name>.terms` | Number of terms in table |
| `tables.<name>.postings` | Number of postings in table |
| `tables.<name>.ngram_size` | N-gram size for table |
| `tables.<name>.memory_bytes` | Memory usage for table in bytes |
| `tables.<name>.memory_human` | Human-readable memory usage for table |

**Memory Health Status:**
- `HEALTHY`: >20% system memory available
- `WARNING`: 10-20% system memory available
- `CRITICAL`: <10% system memory available (OPTIMIZE will be rejected)
- `UNKNOWN`: Unable to determine status

This endpoint is suitable for integration with Prometheus and other monitoring tools.

### GET /health

Health check endpoint for load balancers and monitoring.

**Request:**

```http
GET /health HTTP/1.1
```

**Response (200 OK):**

```json
{
  "status": "ok",
  "timestamp": 1699000000
}
```

### GET /config

Current server configuration (passwords are masked).

**Request:**

```http
GET /config HTTP/1.1
```

**Response (200 OK):**

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
      "port": 11016
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

MySQL replication status (requires replication enabled).

**Request:**

```http
GET /replication/status HTTP/1.1
```

**Response (200 OK):**

```json
{
  "enabled": true,
  "current_gtid": "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-5"
}
```

**Error Response (503 Service Unavailable):**

```json
{
  "error": "Replication not configured"
}
```

## CORS Support

The HTTP server includes CORS (Cross-Origin Resource Sharing) support enabled by default, allowing web applications to make requests from different domains.

**CORS Headers:**

```
Access-Control-Allow-Origin: *
Access-Control-Allow-Methods: GET, POST, OPTIONS
Access-Control-Allow-Headers: Content-Type
```

## Usage Examples

### cURL

**Search:**

```bash
curl -X POST http://localhost:8080/threads/search \
  -H "Content-Type: application/json" \
  -d '{
    "q": "machine learning AND python",
    "filters": {"status": 1},
    "limit": 10
  }'
```

**Get document:**

```bash
curl http://localhost:8080/threads/12345
```

**Health check:**

```bash
curl http://localhost:8080/health
```

### JavaScript (fetch)

```javascript
// Search
const response = await fetch('http://localhost:8080/threads/search', {
  method: 'POST',
  headers: {
    'Content-Type': 'application/json'
  },
  body: JSON.stringify({
    q: 'machine learning AND python',
    filters: { status: 1 },
    limit: 10
  })
});

const data = await response.json();
console.log(`Found ${data.count} results`);
data.results.forEach(doc => {
  console.log(`Document ${doc.doc_id}: ${doc.primary_key}`);
});
```

### Python (requests)

```python
import requests

# Search
response = requests.post('http://localhost:8080/threads/search', json={
    'q': 'machine learning AND python',
    'filters': {'status': 1},
    'limit': 10
})

data = response.json()
print(f"Found {data['count']} results")
for doc in data['results']:
    print(f"Document {doc['doc_id']}: {doc['primary_key']}")
```

## Performance Considerations

- **Connection Pooling**: Use HTTP keep-alive for better performance
- **Pagination**: Use `limit` and `offset` for large result sets
- **Caching**: Consider caching frequent queries at the application layer
- **Network Security**: Use `network.allow_cidrs` to restrict access to trusted IP ranges

## Error Handling

All error responses follow this format:

```json
{
  "error": "Error message description"
}
```

**HTTP Status Codes:**

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Bad Request (invalid input) |
| 404 | Not Found (document doesn't exist) |
| 500 | Internal Server Error |
| 503 | Service Unavailable (feature not enabled) |

## Monitoring

The HTTP API is suitable for monitoring tools:

- **Health Check**: `GET /health` for load balancer health checks
- **Metrics**: `GET /info` for Prometheus/monitoring integration
- **Replication Status**: `GET /replication/status` for replication monitoring
