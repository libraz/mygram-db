#!/bin/sh
set -eu

entrypoint=$1
mygramdb_binary=$2
test_dir=$3
generated_config=$test_dir/generated.yaml
existing_config=$test_dir/existing.yaml

mkdir -p "$test_dir"
rm -f "$generated_config" "$existing_config"

CONFIG_FILE=$generated_config \
MYGRAMDB_BINARY=$mygramdb_binary \
DUMP_DIR=$test_dir/dumps \
REPLICATION_STATE_FILE=$test_dir/replication.state \
MYSQL_PASSWORD='numeric "12345" \ password
with-newline' \
MYSQL_USER=12345 \
MYSQL_DATABASE=testdb \
TABLE_NAME=docs \
TABLE_TEXT_COLUMN=body \
NETWORK_ALLOW_CIDRS=127.0.0.1/32 \
API_ADMIN_TOKEN='test-admin-token' \
sh "$entrypoint" test-config > "$test_dir/startup.log"

test -s "$generated_config"
grep -Fq "Dump: dir=$test_dir/dumps, interval_sec=0, retain=3" "$test_dir/startup.log"
grep -Fq '  password: "numeric \"12345\" \\ password\nwith-newline"' "$generated_config"
grep -q '^[[:space:]]*kanji_ngram_size: 0$' "$generated_config"
grep -q '^bm25:$' "$generated_config"
grep -q '^cache:$' "$generated_config"
grep -q '^  rate_limiting:$' "$generated_config"
grep -Fq '  admin_token: "test-admin-token"' "$generated_config"

printf '%s\n' '# operator-owned sentinel' > "$existing_config"
CONFIG_FILE=$existing_config \
MYGRAMDB_BINARY=$(command -v true) \
DUMP_DIR=$test_dir/dumps \
REPLICATION_STATE_FILE=$test_dir/replication.state \
NETWORK_ALLOW_CIDRS=127.0.0.1/32 \
sh "$entrypoint" mygramdb
test "$(cat "$existing_config")" = '# operator-owned sentinel'

grep -q 'DUMP_INTERVAL_SEC' "$4"
if grep -q 'SNAPSHOT_INTERVAL_SEC' "$4"; then
  echo "legacy SNAPSHOT_INTERVAL_SEC remains in Docker compose" >&2
  exit 1
fi
grep -Fq '$${API_HTTP_PORT:-8080}/health/live' "$4"

repo_root=$(dirname "$4")
env_example=$repo_root/.env.example
for variable in API_HTTP_ENABLE API_HTTP_BIND API_HTTP_PORT DUMP_DIR DUMP_INTERVAL_SEC \
  DUMP_RETAIN MEMORY_VERIFY_TEXT BM25_ENABLE REPLICATION_AUTO_INITIAL_SNAPSHOT; do
  grep -q "^${variable}=" "$env_example"
done
grep -q '^API_ADMIN_TOKEN=CHANGE_ME_GENERATE_RANDOM_SECRET$' "$env_example"
grep -q '^API_BIND=127\.0\.0\.1$' "$env_example"
grep -q '^API_HTTP_BIND=127\.0\.0\.1$' "$env_example"
grep -q '^NETWORK_ALLOW_CIDRS=127\.0\.0\.1/32,172\.16\.0\.0/12$' "$env_example"
grep -Fq 'API_BIND: ${API_CONTAINER_BIND:-0.0.0.0}' "$4"
grep -Fq 'API_HTTP_BIND: ${API_HTTP_CONTAINER_BIND:-0.0.0.0}' "$4"
grep -Fq '127.0.0.1:${API_PORT:-11016}:${API_PORT:-11016}' "$4"
grep -Fq '127.0.0.1:${API_HTTP_PORT:-8080}:${API_HTTP_PORT:-8080}' "$4"

# A sample stack that cannot serve a query is worse than one that fails to
# start, so the pieces that make it usable are asserted rather than assumed.
grep -Fq 'REPLICATION_AUTO_INITIAL_SNAPSHOT: ${REPLICATION_AUTO_INITIAL_SNAPSHOT:-true}' "$4"
grep -q '^SET NAMES utf8mb4;$' "$repo_root/support/docker/mysql/init/01-create-tables.sql"
grep -q 'REPLICATION CLIENT' "$repo_root/support/docker/mysql/init/02-grant-replication.sh"

grep -q '^bm25:$' "$repo_root/examples/config.yaml"
