#!/bin/bash
# Benchmark seed data loader for MygramDB
# Downloads and loads ~1.1M Wikipedia articles into mydb.articles
#
# This script runs inside the MySQL container as a docker-entrypoint-initdb.d
# init script. It expects the database to already exist (created by Docker MySQL env).

set -e

SEED_URL="https://dl.libraz.net/mygram-bench-seed.sql.zst"
MYSQL_CMD="mysql -u root -p${MYSQL_ROOT_PASSWORD:-mygramdb}"

# Switch repl_user to mysql_native_password for libmysqlclient compatibility
$MYSQL_CMD -e "ALTER USER 'repl_user'@'%' IDENTIFIED WITH mysql_native_password BY '${MYSQL_PASSWORD:-mygramdb}'; GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_user'@'%'; GRANT SELECT ON mydb.* TO 'repl_user'@'%'; FLUSH PRIVILEGES;"

echo ""
echo "┌─────────────────────────────────────────────────────────┐"
echo "│           MygramDB Benchmark: Seed Data Loader          │"
echo "├─────────────────────────────────────────────────────────┤"
echo "│  Dataset : 1.1M Wikipedia articles (EN 1M + JA 100K)   │"
echo "│  Source  : Wikipedia CirrusSearch (CC BY-SA 3.0)        │"
echo "│  Format  : mysqldump + zstd (225 MB compressed)         │"
echo "└─────────────────────────────────────────────────────────┘"
echo ""

# ── Step 1: Download ──────────────────────────────────────────
echo "[1/2] Downloading seed data..."
echo "      ${SEED_URL}"

REMOTE_SIZE=$(curl -sfLI "${SEED_URL}" | grep -i content-length | tail -1 | tr -d '\r' | awk '{printf "%.0f", $2/1024/1024}')
if [ -n "${REMOTE_SIZE}" ] && [ "${REMOTE_SIZE}" -gt 0 ] 2>/dev/null; then
    echo "      Size: ${REMOTE_SIZE} MB (compressed)"
fi

if [ -t 0 ]; then
    printf "      Proceed with download? [Y/n] "
    read -r CONFIRM
    case "${CONFIRM}" in
        [nN]*) echo "Aborted."; exit 1 ;;
    esac
else
    echo "      (non-interactive: auto-confirming download)"
fi

echo ""
DOWNLOAD_START=$(date +%s)
curl -fSL --progress-bar "${SEED_URL}" | zstd -d | $MYSQL_CMD mydb
DOWNLOAD_END=$(date +%s)
DOWNLOAD_SEC=$((DOWNLOAD_END - DOWNLOAD_START))

echo ""
echo "      Downloaded and loaded in ${DOWNLOAD_SEC}s"
echo ""

# ── Step 2: Verify ────────────────────────────────────────────
echo "[2/2] Verifying..."
ROW_COUNT=$($MYSQL_CMD -sN mydb -e "SELECT COUNT(*) FROM articles")
HAS_FT=$($MYSQL_CMD -sN mydb -e "SELECT COUNT(*) FROM information_schema.STATISTICS WHERE TABLE_SCHEMA='mydb' AND TABLE_NAME='articles' AND INDEX_TYPE='FULLTEXT'")
EN_COUNT=1000000
JA_COUNT=$((ROW_COUNT - EN_COUNT))

echo ""
echo "┌─────────────────────────────────────────────────────────┐"
echo "│               Seed Data Load Complete                   │"
echo "├─────────────────────────────────────────────────────────┤"
printf "│  Total rows   : %-39s│\n" "$(printf '%s' "${ROW_COUNT}" | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta')"
printf "│  English      : %-39s│\n" "~$(printf '%s' "${EN_COUNT}" | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta') articles"
printf "│  Japanese     : %-39s│\n" "~$(printf '%s' "${JA_COUNT}" | sed ':a;s/\B[0-9]\{3\}\>/,&/;ta') articles"
printf "│  FULLTEXT idx : %-39s│\n" "$([ "${HAS_FT}" -gt 0 ] && echo 'yes (ngram parser)' || echo 'no')"
echo "├─────────────────────────────────────────────────────────┤"
printf "│  Time         : %-39s│\n" "${DOWNLOAD_SEC}s (download + load)"
echo "├─────────────────────────────────────────────────────────┤"
echo "│  GTID binlog replication is ready.                      │"
echo "│  MygramDB will begin indexing once connected.           │"
echo "└─────────────────────────────────────────────────────────┘"
echo ""
