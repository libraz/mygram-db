#!/bin/sh
set -e

# MygramDB Docker Entrypoint Script
# Generates configuration from environment variables

# If SKIP_CONFIG_GEN is set, just execute the command directly
if [ "${SKIP_CONFIG_GEN}" = "true" ]; then
    exec "$@"
fi

# Handle special commands that don't need config
case "$1" in
    --help|-h|--version|-v|help|version)
        exec /usr/local/bin/mygramdb "$@"
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

TABLE_NAME=${TABLE_NAME:-articles}
TABLE_PRIMARY_KEY=${TABLE_PRIMARY_KEY:-id}
TABLE_TEXT_COLUMN=${TABLE_TEXT_COLUMN:-content}
TABLE_NGRAM_SIZE=${TABLE_NGRAM_SIZE:-2}
TABLE_KANJI_NGRAM_SIZE=${TABLE_KANJI_NGRAM_SIZE:-1}

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

DUMP_DIR=${DUMP_DIR:-/var/lib/mygramdb/dumps}
DUMP_INTERVAL_SEC=${DUMP_INTERVAL_SEC:-0}  # 0 = disabled (set to 7200 for 120-minute intervals)
DUMP_RETAIN=${DUMP_RETAIN:-3}

API_BIND=${API_BIND:-0.0.0.0}
API_PORT=${API_PORT:-11016}

NETWORK_ALLOW_CIDRS=${NETWORK_ALLOW_CIDRS:-""}

LOG_LEVEL=${LOG_LEVEL:-info}
LOG_FORMAT=${LOG_FORMAT:-json}

CONFIG_FILE=${CONFIG_FILE:-/etc/mygramdb/config.yaml}

# Create configuration directory
mkdir -p "$(dirname "$CONFIG_FILE")"
mkdir -p "$DUMP_DIR"
mkdir -p "$(dirname "$REPLICATION_STATE_FILE")"

# Generate configuration file
cat > "$CONFIG_FILE" <<EOF
# MygramDB Configuration (Auto-generated from environment variables)
# Generated at: $(date -u +"%Y-%m-%d %H:%M:%S UTC")

# MySQL Connection
mysql:
  host: "${MYSQL_HOST}"
  port: ${MYSQL_PORT}
  user: "${MYSQL_USER}"
  password: "${MYSQL_PASSWORD}"
  database: "${MYSQL_DATABASE}"
  use_gtid: ${MYSQL_USE_GTID}
  connect_timeout_ms: ${MYSQL_CONNECT_TIMEOUT_MS}

# Table Configuration
tables:
  - name: "${TABLE_NAME}"
    primary_key: "${TABLE_PRIMARY_KEY}"
    text_source:
      column: "${TABLE_TEXT_COLUMN}"
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
  start_from: "${REPLICATION_START_FROM}"
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
    width: "${MEMORY_NORMALIZE_WIDTH}"
    lower: ${MEMORY_NORMALIZE_LOWER}

# Dump Persistence
dump:
  dir: "${DUMP_DIR}"
  interval_sec: ${DUMP_INTERVAL_SEC}
  retain: ${DUMP_RETAIN}

# API Server
api:
  tcp:
    bind: "${API_BIND}"
    port: ${API_PORT}

# Logging
logging:
  level: "${LOG_LEVEL}"
  format: "${LOG_FORMAT}"
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
    # Trim whitespace
    cidr=$(echo "$cidr" | xargs)
    if [ -n "$cidr" ]; then
      echo "    - \"$cidr\"" >> "$CONFIG_FILE"
    fi
  done
fi

echo "Configuration file generated at: $CONFIG_FILE"
echo "MySQL: ${MYSQL_USER}@${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE}"
echo "Table: ${TABLE_NAME} (primary_key: ${TABLE_PRIMARY_KEY}, text_column: ${TABLE_TEXT_COLUMN})"
echo "API Server: ${API_BIND}:${API_PORT}"
echo "Log Level: ${LOG_LEVEL}"

# Execute command based on arguments
case "$1" in
    test-config)
        # Test configuration
        echo "Testing configuration..."
        /usr/local/bin/mygramdb -t "$CONFIG_FILE"
        exit $?
        ;;
    mygramdb|/usr/local/bin/mygramdb|"")
        # Start mygramdb with generated config
        echo "Validating configuration..."
        if ! /usr/local/bin/mygramdb -t "$CONFIG_FILE"; then
            echo "ERROR: Configuration validation failed!"
            exit 1
        fi
        exec /usr/local/bin/mygramdb -c "$CONFIG_FILE"
        ;;
    *)
        # Execute any other command
        exec "$@"
        ;;
esac
