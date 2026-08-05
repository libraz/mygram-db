#!/bin/sh
set -e

# MygramDB Docker Entrypoint Script
# Generates configuration from environment variables

MYGRAMDB_BINARY=${MYGRAMDB_BINARY:-/usr/local/bin/mygramdb}

yaml_quote() {
    # Emit one YAML double-quoted scalar. Newlines are encoded rather than
    # allowed to escape the scalar and inject additional configuration keys.
    printf '%s' "$1" | awk 'BEGIN { ORS=""; printf "\"" } {
        if (NR > 1) printf "\\n";
        gsub(/\\/, "\\\\");
        gsub(/"/, "\\\"");
        gsub(/\r/, "\\r");
        printf "%s", $0
    } END { printf "\"" }'
}

require_uint() {
    case "$2" in
        ''|*[!0-9]*)
            echo "ERROR: $1 must be an unsigned integer" >&2
            exit 1
            ;;
    esac
}

require_bool() {
    case "$2" in
        true|false) ;;
        *)
            echo "ERROR: $1 must be true or false" >&2
            exit 1
            ;;
    esac
}

require_number() {
    if ! printf '%s\n' "$2" | grep -Eq '^-?[0-9]+([.][0-9]+)?$'; then
        echo "ERROR: $1 must be a number" >&2
        exit 1
    fi
}

# If SKIP_CONFIG_GEN is set, just execute the command directly
if [ "${SKIP_CONFIG_GEN}" = "true" ]; then
    exec "$@"
fi

# Handle special commands that don't need config
case "$1" in
    --help|-h|--version|-v|help|version)
        exec "$MYGRAMDB_BINARY" "$@"
        ;;
    /bin/sh|/bin/bash|sh|bash)
        # Shell access
        exec "$@"
        ;;
esac

# Default values
MYSQL_HOST=${MYSQL_HOST:-mysql}
MYSQL_PORT=${MYSQL_PORT:-3306}
MYSQL_USER=${MYSQL_USER:-repl_user}
MYSQL_PASSWORD=${MYSQL_PASSWORD:-your_password}
MYSQL_DATABASE=${MYSQL_DATABASE:-mydb}
MYSQL_USE_GTID=${MYSQL_USE_GTID:-true}
MYSQL_CONNECT_TIMEOUT_MS=${MYSQL_CONNECT_TIMEOUT_MS:-3000}
MYSQL_DATETIME_TIMEZONE=${MYSQL_DATETIME_TIMEZONE:-+00:00}
MYSQL_SSL_ENABLE=${MYSQL_SSL_ENABLE:-false}
MYSQL_SSL_CA=${MYSQL_SSL_CA:-}
MYSQL_SSL_CERT=${MYSQL_SSL_CERT:-}
MYSQL_SSL_KEY=${MYSQL_SSL_KEY:-}
MYSQL_SSL_VERIFY_SERVER_CERT=${MYSQL_SSL_VERIFY_SERVER_CERT:-true}

TABLE_NAME=${TABLE_NAME:-articles}
TABLE_PRIMARY_KEY=${TABLE_PRIMARY_KEY:-id}
TABLE_TEXT_COLUMN=${TABLE_TEXT_COLUMN:-content}
TABLE_NGRAM_SIZE=${TABLE_NGRAM_SIZE:-2}
TABLE_KANJI_NGRAM_SIZE=${TABLE_KANJI_NGRAM_SIZE:-0}

REPLICATION_ENABLE=${REPLICATION_ENABLE:-true}
REPLICATION_AUTO_INITIAL_SNAPSHOT=${REPLICATION_AUTO_INITIAL_SNAPSHOT:-false}
REPLICATION_SERVER_ID=${REPLICATION_SERVER_ID:-12345}
REPLICATION_START_FROM=${REPLICATION_START_FROM:-snapshot}
REPLICATION_STATE_FILE=${REPLICATION_STATE_FILE:-/var/lib/mygramdb/replication.state}

BUILD_BATCH_SIZE=${BUILD_BATCH_SIZE:-5000}
BUILD_PARALLELISM=${BUILD_PARALLELISM:-2}

MEMORY_HARD_LIMIT_MB=${MEMORY_HARD_LIMIT_MB:-8192}
MEMORY_SOFT_TARGET_MB=${MEMORY_SOFT_TARGET_MB:-4096}
MEMORY_NORMALIZE_NFKC=${MEMORY_NORMALIZE_NFKC:-true}
MEMORY_NORMALIZE_WIDTH=${MEMORY_NORMALIZE_WIDTH:-narrow}
MEMORY_NORMALIZE_LOWER=${MEMORY_NORMALIZE_LOWER:-false}
MEMORY_VERIFY_TEXT=${MEMORY_VERIFY_TEXT:-off}

DUMP_DIR=${DUMP_DIR:-/var/lib/mygramdb/dumps}
DUMP_INTERVAL_SEC=${DUMP_INTERVAL_SEC:-0}  # 0 = disabled (set to 7200 for 120-minute intervals)
DUMP_RETAIN=${DUMP_RETAIN:-3}

API_BIND=${API_BIND:-127.0.0.1}
API_PORT=${API_PORT:-11016}
API_HTTP_ENABLE=${API_HTTP_ENABLE:-true}
API_HTTP_BIND=${API_HTTP_BIND:-127.0.0.1}
API_HTTP_PORT=${API_HTTP_PORT:-8080}
API_RATE_LIMIT_ENABLE=${API_RATE_LIMIT_ENABLE:-false}
API_RATE_LIMIT_CAPACITY=${API_RATE_LIMIT_CAPACITY:-100}
API_RATE_LIMIT_REFILL_RATE=${API_RATE_LIMIT_REFILL_RATE:-10}
API_RATE_LIMIT_MAX_CLIENTS=${API_RATE_LIMIT_MAX_CLIENTS:-10000}
API_ADMIN_TOKEN=${API_ADMIN_TOKEN:-}

if [ "$API_ADMIN_TOKEN" = "CHANGE_ME_GENERATE_RANDOM_SECRET" ]; then
    echo "ERROR: API_ADMIN_TOKEN must be replaced with a generated secret before starting MygramDB" >&2
    exit 1
fi

BM25_ENABLE=${BM25_ENABLE:-false}
BM25_K1=${BM25_K1:-1.2}
BM25_B=${BM25_B:-0.75}

CACHE_ENABLED=${CACHE_ENABLED:-true}
CACHE_MAX_MEMORY_MB=${CACHE_MAX_MEMORY_MB:-32}
CACHE_MIN_QUERY_COST_MS=${CACHE_MIN_QUERY_COST_MS:-10.0}
CACHE_TTL_SECONDS=${CACHE_TTL_SECONDS:-3600}

NETWORK_ALLOW_CIDRS=${NETWORK_ALLOW_CIDRS:-""}

LOG_LEVEL=${LOG_LEVEL:-info}
LOG_FORMAT=${LOG_FORMAT:-json}

CONFIG_FILE=${CONFIG_FILE:-/etc/mygramdb/config.yaml}

# Create configuration directory
mkdir -p "$(dirname "$CONFIG_FILE")"
mkdir -p "$DUMP_DIR"
mkdir -p "$(dirname "$REPLICATION_STATE_FILE")"

if [ -e "$CONFIG_FILE" ]; then
    echo "Using existing configuration file: $CONFIG_FILE"
else
  for pair in \
      "MYSQL_PORT:$MYSQL_PORT" "MYSQL_CONNECT_TIMEOUT_MS:$MYSQL_CONNECT_TIMEOUT_MS" \
      "TABLE_NGRAM_SIZE:$TABLE_NGRAM_SIZE" "TABLE_KANJI_NGRAM_SIZE:$TABLE_KANJI_NGRAM_SIZE" \
      "REPLICATION_SERVER_ID:$REPLICATION_SERVER_ID" "BUILD_BATCH_SIZE:$BUILD_BATCH_SIZE" \
      "BUILD_PARALLELISM:$BUILD_PARALLELISM" "MEMORY_HARD_LIMIT_MB:$MEMORY_HARD_LIMIT_MB" \
      "MEMORY_SOFT_TARGET_MB:$MEMORY_SOFT_TARGET_MB" "DUMP_INTERVAL_SEC:$DUMP_INTERVAL_SEC" \
      "DUMP_RETAIN:$DUMP_RETAIN" "API_PORT:$API_PORT" "API_HTTP_PORT:$API_HTTP_PORT" \
      "API_RATE_LIMIT_CAPACITY:$API_RATE_LIMIT_CAPACITY" \
      "API_RATE_LIMIT_REFILL_RATE:$API_RATE_LIMIT_REFILL_RATE" \
      "API_RATE_LIMIT_MAX_CLIENTS:$API_RATE_LIMIT_MAX_CLIENTS" \
      "CACHE_MAX_MEMORY_MB:$CACHE_MAX_MEMORY_MB" "CACHE_TTL_SECONDS:$CACHE_TTL_SECONDS"; do
      require_uint "${pair%%:*}" "${pair#*:}"
  done
  for pair in \
      "MYSQL_USE_GTID:$MYSQL_USE_GTID" "MYSQL_SSL_ENABLE:$MYSQL_SSL_ENABLE" \
      "MYSQL_SSL_VERIFY_SERVER_CERT:$MYSQL_SSL_VERIFY_SERVER_CERT" \
      "REPLICATION_ENABLE:$REPLICATION_ENABLE" \
      "REPLICATION_AUTO_INITIAL_SNAPSHOT:$REPLICATION_AUTO_INITIAL_SNAPSHOT" \
      "MEMORY_NORMALIZE_NFKC:$MEMORY_NORMALIZE_NFKC" \
      "MEMORY_NORMALIZE_LOWER:$MEMORY_NORMALIZE_LOWER" \
      "API_HTTP_ENABLE:$API_HTTP_ENABLE" "API_RATE_LIMIT_ENABLE:$API_RATE_LIMIT_ENABLE" \
      "BM25_ENABLE:$BM25_ENABLE" "CACHE_ENABLED:$CACHE_ENABLED"; do
      require_bool "${pair%%:*}" "${pair#*:}"
  done
  require_number BM25_K1 "$BM25_K1"
  require_number BM25_B "$BM25_B"
  require_number CACHE_MIN_QUERY_COST_MS "$CACHE_MIN_QUERY_COST_MS"

  # Generate configuration only when the target does not already exist. This
  # keeps read-only/bind-mounted operator configuration intact.
  cat > "$CONFIG_FILE" <<EOF
# MygramDB Configuration (Auto-generated from environment variables)
# Generated at: $(date -u +"%Y-%m-%d %H:%M:%S UTC")

# MySQL Connection
mysql:
  host: $(yaml_quote "$MYSQL_HOST")
  port: ${MYSQL_PORT}
  user: $(yaml_quote "$MYSQL_USER")
  password: $(yaml_quote "$MYSQL_PASSWORD")
  database: $(yaml_quote "$MYSQL_DATABASE")
  use_gtid: ${MYSQL_USE_GTID}
  connect_timeout_ms: ${MYSQL_CONNECT_TIMEOUT_MS}
  datetime_timezone: $(yaml_quote "$MYSQL_DATETIME_TIMEZONE")
  ssl_enable: ${MYSQL_SSL_ENABLE}
  ssl_ca: $(yaml_quote "$MYSQL_SSL_CA")
  ssl_cert: $(yaml_quote "$MYSQL_SSL_CERT")
  ssl_key: $(yaml_quote "$MYSQL_SSL_KEY")
  ssl_verify_server_cert: ${MYSQL_SSL_VERIFY_SERVER_CERT}

# Table Configuration
tables:
  - name: $(yaml_quote "$TABLE_NAME")
    primary_key: $(yaml_quote "$TABLE_PRIMARY_KEY")
    text_source:
      column: $(yaml_quote "$TABLE_TEXT_COLUMN")
    ngram_size: ${TABLE_NGRAM_SIZE}
    kanji_ngram_size: ${TABLE_KANJI_NGRAM_SIZE}

# Index Build Configuration
build:
  mode: "select_snapshot"
  batch_size: ${BUILD_BATCH_SIZE}
  parallelism: ${BUILD_PARALLELISM}
  throttle_ms: 0

# Replication Configuration
replication:
  enable: ${REPLICATION_ENABLE}
  auto_initial_snapshot: ${REPLICATION_AUTO_INITIAL_SNAPSHOT}
  server_id: ${REPLICATION_SERVER_ID}
  start_from: $(yaml_quote "$REPLICATION_START_FROM")
  queue_size: 10000
  reconnect_backoff_min_ms: 500
  reconnect_backoff_max_ms: 10000

# Memory Management
memory:
  hard_limit_mb: ${MEMORY_HARD_LIMIT_MB}
  soft_target_mb: ${MEMORY_SOFT_TARGET_MB}
  arena_chunk_mb: 64
  roaring_threshold: 0.18
  minute_epoch: true
  normalize:
    nfkc: ${MEMORY_NORMALIZE_NFKC}
    width: $(yaml_quote "$MEMORY_NORMALIZE_WIDTH")
    lower: ${MEMORY_NORMALIZE_LOWER}
  verify_text: $(yaml_quote "$MEMORY_VERIFY_TEXT")

# BM25 relevance scoring
bm25:
  enable: ${BM25_ENABLE}
  k1: ${BM25_K1}
  b: ${BM25_B}

# Query cache
cache:
  enabled: ${CACHE_ENABLED}
  max_memory_mb: ${CACHE_MAX_MEMORY_MB}
  min_query_cost_ms: ${CACHE_MIN_QUERY_COST_MS}
  ttl_seconds: ${CACHE_TTL_SECONDS}

# Dump Persistence
dump:
  dir: $(yaml_quote "$DUMP_DIR")
  interval_sec: ${DUMP_INTERVAL_SEC}
  retain: ${DUMP_RETAIN}

# API Server
api:
  tcp:
    bind: $(yaml_quote "$API_BIND")
    port: ${API_PORT}
  http:
    enable: ${API_HTTP_ENABLE}
    bind: $(yaml_quote "$API_HTTP_BIND")
    port: ${API_HTTP_PORT}
  rate_limiting:
    enable: ${API_RATE_LIMIT_ENABLE}
    capacity: ${API_RATE_LIMIT_CAPACITY}
    refill_rate: ${API_RATE_LIMIT_REFILL_RATE}
    max_clients: ${API_RATE_LIMIT_MAX_CLIENTS}
  admin_token: $(yaml_quote "$API_ADMIN_TOKEN")

# Logging
logging:
  level: $(yaml_quote "$LOG_LEVEL")
  format: $(yaml_quote "$LOG_FORMAT")
EOF

  # Add network ACL configuration if specified
  if [ -n "$NETWORK_ALLOW_CIDRS" ]; then
    cat >> "$CONFIG_FILE" << 'EOF'

# Network Configuration
network:
  allow_cidrs:
EOF
  # Convert comma-separated list to YAML list
  # Use POSIX-compliant method instead of bashism (<<<)
    echo "$NETWORK_ALLOW_CIDRS" | tr ',' '\n' | while read -r cidr; do
      cidr=$(echo "$cidr" | xargs)
      if [ -n "$cidr" ]; then
        printf '    - %s\n' "$(yaml_quote "$cidr")" >> "$CONFIG_FILE"
      fi
    done
  fi

  chmod 600 "$CONFIG_FILE"
  echo "Configuration file generated at: $CONFIG_FILE"
fi
echo "MySQL: ${MYSQL_USER}@${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE}"
echo "Table: ${TABLE_NAME} (primary_key: ${TABLE_PRIMARY_KEY}, text_column: ${TABLE_TEXT_COLUMN})"
echo "Dump: dir=${DUMP_DIR}, interval_sec=${DUMP_INTERVAL_SEC}, retain=${DUMP_RETAIN}"
echo "API Server: ${API_BIND}:${API_PORT}"
echo "HTTP Server: enabled=${API_HTTP_ENABLE}, ${API_HTTP_BIND}:${API_HTTP_PORT}"
echo "Log Level: ${LOG_LEVEL}"

# Execute command based on arguments
case "$1" in
    test-config)
        # Test configuration
        echo "Testing configuration..."
        "$MYGRAMDB_BINARY" -t "$CONFIG_FILE"
        exit $?
        ;;
    mygramdb|/usr/local/bin/mygramdb|"")
        # Start mygramdb with generated config
        echo "Validating configuration..."
        if ! "$MYGRAMDB_BINARY" -t "$CONFIG_FILE"; then
            echo "ERROR: Configuration validation failed!"
            exit 1
        fi
        exec "$MYGRAMDB_BINARY" -c "$CONFIG_FILE"
        ;;
    *)
        # Execute any other command
        exec "$@"
        ;;
esac
