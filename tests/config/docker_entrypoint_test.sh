#!/bin/sh
set -eu

entrypoint=$1
mygramdb_binary=$2
test_dir=$3
generated_config=$test_dir/generated.yaml
existing_config=$test_dir/existing.yaml

mkdir -p "$test_dir"
rm -f "$generated_config" "$existing_config"

# help/version are the bare words a container caller naturally reaches for;
# the entrypoint must translate them to --help/--version rather than passing
# the literal word through as a positional argument (which mygramdb would
# otherwise try to open as a config file and fail).
for word in help version; do
  if ! MYGRAMDB_BINARY=$mygramdb_binary sh "$entrypoint" "$word" > "$test_dir/$word.log" 2>&1; then
    echo "entrypoint '$word' exited non-zero; see $test_dir/$word.log" >&2
    exit 1
  fi
done
grep -Fq 'Usage:' "$test_dir/help.log"
grep -Eq '[0-9]+\.[0-9]+\.[0-9]+' "$test_dir/version.log"

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

> "$test_dir/placeholder.log"
if CONFIG_FILE=$test_dir/placeholder.yaml \
  MYGRAMDB_BINARY=$mygramdb_binary \
  DUMP_DIR=$test_dir/dumps \
  REPLICATION_STATE_FILE=$test_dir/replication.state \
  MYSQL_PASSWORD=your_secure_password_here \
  NETWORK_ALLOW_CIDRS=127.0.0.1/32 \
  sh "$entrypoint" test-config > "$test_dir/placeholder.log" 2>&1; then
  echo "entrypoint started with the .env.example placeholder MYSQL_PASSWORD" >&2
  exit 1
fi
grep -Fq 'MYSQL_PASSWORD still holds the .env.example placeholder' "$test_dir/placeholder.log"
test ! -e "$test_dir/placeholder.yaml"

> "$test_dir/root_placeholder.log"
if CONFIG_FILE=$test_dir/root_placeholder.yaml \
  MYGRAMDB_BINARY=$mygramdb_binary \
  DUMP_DIR=$test_dir/dumps \
  REPLICATION_STATE_FILE=$test_dir/replication.state \
  MYSQL_PASSWORD=a_real_password \
  MYSQL_ROOT_PASSWORD=root_secure_password_here \
  NETWORK_ALLOW_CIDRS=127.0.0.1/32 \
  sh "$entrypoint" test-config > "$test_dir/root_placeholder.log" 2>&1; then
  echo "entrypoint started with the .env.example placeholder MYSQL_ROOT_PASSWORD" >&2
  exit 1
fi
grep -Fq 'MYSQL_ROOT_PASSWORD still holds the .env.example placeholder' "$test_dir/root_placeholder.log"
test ! -e "$test_dir/root_placeholder.yaml"

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

# entrypoint.sh's placeholder check for MYSQL_ROOT_PASSWORD only fires if the
# variable actually reaches the mygramdb container; the mysql service's own
# assignment (plus its healthcheck's shell reference, which is a different
# pattern) must not be the only place it appears.
root_password_assignments=$(grep -c 'MYSQL_ROOT_PASSWORD:' "$4")
if [ "$root_password_assignments" -lt 2 ]; then
  echo "MYSQL_ROOT_PASSWORD is not wired into the mygramdb service; entrypoint.sh's placeholder check is unreachable there" >&2
  exit 1
fi

repo_root=$(dirname "$4")
env_example=$repo_root/.env.example
for variable in API_HTTP_ENABLE API_HTTP_BIND API_HTTP_PORT DUMP_DIR DUMP_INTERVAL_SEC \
  DUMP_RETAIN MEMORY_VERIFY_TEXT BM25_ENABLE REPLICATION_AUTO_INITIAL_SNAPSHOT; do
  grep -q "^${variable}=" "$env_example"
done
grep -q '^API_ADMIN_TOKEN=CHANGE_ME_GENERATE_RANDOM_SECRET$' "$env_example"
# The placeholders the entrypoint refuses to start on have to keep matching the
# values the sample environment ships.
grep -q '^MYSQL_PASSWORD=your_secure_password_here$' "$env_example"
grep -q '^MYSQL_ROOT_PASSWORD=root_secure_password_here$' "$env_example"
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

# The Dockerfile HEALTHCHECK must follow API_HTTP_PORT and skip the probe
# entirely when API_HTTP_ENABLE=false (there is no HTTP listener to check in
# that case). Extract the actual CMD line rather than just grepping for the
# variable names, and exercise it with a fake `curl` so the assertions cover
# behavior, not just text presence.
healthcheck_cmd=$(awk '/^HEALTHCHECK /{getline; sub(/^[[:space:]]*CMD[[:space:]]+/, ""); print; exit}' "$repo_root/Dockerfile")
[ -n "$healthcheck_cmd" ]

healthcheck_bin_dir=$test_dir/healthcheck_bin
mkdir -p "$healthcheck_bin_dir"

# A `curl` that always fails: if API_HTTP_ENABLE=false does not short-circuit
# before reaching it, the probe would report unhealthy for a feature the
# operator explicitly disabled.
cat > "$healthcheck_bin_dir/curl" <<'EOF'
#!/bin/sh
exit 1
EOF
chmod +x "$healthcheck_bin_dir/curl"
if ! API_HTTP_ENABLE=false PATH="$healthcheck_bin_dir:$PATH" sh -c "$healthcheck_cmd"; then
  echo "HEALTHCHECK must report healthy when API_HTTP_ENABLE=false instead of probing a disabled listener" >&2
  exit 1
fi

# A `curl` that fails unless invoked with the configured port in its URL
# argument: proves API_HTTP_PORT actually reaches the probe.
cat > "$healthcheck_bin_dir/curl" <<'EOF'
#!/bin/sh
for arg in "$@"; do
  case "$arg" in
    *:9999/health/live) exit 0 ;;
  esac
done
exit 1
EOF
chmod +x "$healthcheck_bin_dir/curl"
if ! API_HTTP_ENABLE=true API_HTTP_PORT=9999 PATH="$healthcheck_bin_dir:$PATH" sh -c "$healthcheck_cmd"; then
  echo "HEALTHCHECK did not probe the configured API_HTTP_PORT" >&2
  exit 1
fi

# The same fake curl with the default port instead: a probe that ignores
# API_HTTP_PORT and always hits 8080 must fail this case.
if API_HTTP_ENABLE=true PATH="$healthcheck_bin_dir:$PATH" sh -c "$healthcheck_cmd"; then
  echo "HEALTHCHECK unexpectedly succeeded without API_HTTP_PORT set to the probe's expected port" >&2
  exit 1
fi
