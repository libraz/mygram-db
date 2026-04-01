#!/bin/bash
set -euo pipefail

# =============================================================================
# MygramDB Package Verification Test
# =============================================================================
# Tests package installation, binary functionality, and integration with MySQL.
#
# Usage:
#   test-pkg-verify.sh --type rpm --distro el9 [--arch x86_64] [--integration]
#   test-pkg-verify.sh --type deb --distro jammy [--arch x86_64] [--integration]
#
# Levels:
#   Basic (default):  install, file checks, --version, --help, -t config-test
#   Integration:      + MySQL, server start, health check, search, CLI
# =============================================================================

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Defaults
PKG_TYPE=""
DISTRO=""
# Normalize architecture (macOS returns arm64)
_DETECTED_ARCH="$(uname -m)"
if [ "$_DETECTED_ARCH" = "arm64" ]; then _DETECTED_ARCH="aarch64"; fi
ARCH="$_DETECTED_ARCH"
INTEGRATION=false
CLEANUP=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --type)       PKG_TYPE="$2";   shift 2 ;;
        --distro)     DISTRO="$2";     shift 2 ;;
        --arch)       ARCH="$2";       shift 2 ;;
        --integration) INTEGRATION=true; shift ;;
        --no-cleanup) CLEANUP=false;    shift ;;
        *)            echo "Unknown option: $1"; exit 1 ;;
    esac
done

if [ -z "$PKG_TYPE" ] || [ -z "$DISTRO" ]; then
    echo "Usage: $0 --type {rpm|deb} --distro {el9|el10|jammy|noble} [--arch ARCH] [--integration]"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(git rev-parse --show-toplevel)"
VERSION=$(git describe --tags --abbrev=0 2>/dev/null | sed 's/^v//' || echo "0.0.0")

# Map distro to Docker image and package path
case "$DISTRO" in
    el9)
        BASE_IMAGE="almalinux:9"
        EL_VERSION="9"
        ;;
    el10)
        BASE_IMAGE="almalinux:10"
        EL_VERSION="10"
        ;;
    jammy)
        BASE_IMAGE="ubuntu:22.04"
        UBUNTU_VERSION="22.04"
        ;;
    noble)
        BASE_IMAGE="ubuntu:24.04"
        UBUNTU_VERSION="24.04"
        ;;
    *)
        echo -e "${RED}Unknown distro: $DISTRO${NC}"
        exit 1
        ;;
esac

# Map arch to Docker platform
case "$ARCH" in
    x86_64)  DOCKER_PLATFORM="linux/amd64"; DEB_ARCH="amd64" ;;
    aarch64) DOCKER_PLATFORM="linux/arm64"; DEB_ARCH="arm64" ;;
    *)       echo -e "${RED}Unsupported arch: $ARCH${NC}"; exit 1 ;;
esac

COMPOSE_PROJECT="mygramdb-pkg-test-${DISTRO}-${ARCH}"
NETWORK_NAME="${COMPOSE_PROJECT}_net"
MYSQL_CONTAINER="${COMPOSE_PROJECT}-mysql"
APP_CONTAINER="${COMPOSE_PROJECT}-app"

PASS=0
FAIL=0
SKIP=0

pass() { echo -e "  ${GREEN}✓ $1${NC}"; PASS=$((PASS + 1)); }
fail() { echo -e "  ${RED}✗ $1${NC}"; FAIL=$((FAIL + 1)); }
skip() { echo -e "  ${YELLOW}⊘ $1 (skipped)${NC}"; SKIP=$((SKIP + 1)); }

cleanup() {
    if [ "$CLEANUP" = true ]; then
        echo -e "\n${YELLOW}Cleaning up...${NC}"
        docker rm -f "$APP_CONTAINER" "$MYSQL_CONTAINER" 2>/dev/null || true
        docker network rm "$NETWORK_NAME" 2>/dev/null || true
    fi
}

# Always cleanup on exit if integration mode
if [ "$INTEGRATION" = true ]; then
    trap cleanup EXIT
fi

echo -e "${BLUE}╔══════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  MygramDB Package Verification               ║${NC}"
echo -e "${BLUE}║  Type: ${PKG_TYPE} | Distro: ${DISTRO} | Arch: ${ARCH}${NC}"
echo -e "${BLUE}║  Version: ${VERSION} | Integration: ${INTEGRATION}${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════╝${NC}\n"

# =============================================================================
# Phase 1: Build test container with package installed
# =============================================================================
echo -e "${BLUE}── Phase 1: Package Installation ──${NC}"

# Create a temporary Dockerfile for the test
TMPDIR=$(mktemp -d)
CONFIG_FILE="${TMPDIR}/config-test.yaml"
DOCKERFILE="${TMPDIR}/Dockerfile"

# Write test config (for -t config-test and integration)
cat > "$CONFIG_FILE" << 'YAML'
mysql:
  host: "mysql"
  port: 3306
  user: "repl_user"
  password: "test_password"
  database: "mydb"
  datetime_timezone: "+00:00"

tables:
  - name: "articles"
    primary_key: "id"
    text_source:
      column: "content"
    ngram_size: 2

replication:
  enable: true
  server_id: 12345

api:
  tcp:
    bind: "0.0.0.0"
    port: 11016
  http:
    enable: true
    bind: "0.0.0.0"
    port: 8080

network:
  allow_cidrs:
    - "0.0.0.0/0"

memory:
  hard_limit_mb: 512
  soft_target_mb: 256

logging:
  level: "info"
  format: "json"
YAML

if [ "$PKG_TYPE" = "rpm" ]; then
    cat > "$DOCKERFILE" << DOCKERFILE
FROM ${BASE_IMAGE}
RUN dnf install -y --nogpgcheck https://dev.mysql.com/get/mysql84-community-release-el${EL_VERSION}-1.noarch.rpm && \
    (dnf module disable -y mysql 2>/dev/null || true) && \
    dnf install -y --allowerasing --nogpgcheck procps-ng curl && \
    dnf clean all
COPY dist/rpm/mygramdb-${VERSION}-1.el${EL_VERSION}.${ARCH}.rpm /tmp/mygramdb.rpm
RUN dnf install -y --nogpgcheck /tmp/mygramdb.rpm && rm -f /tmp/mygramdb.rpm
COPY config-test.yaml /etc/mygramdb/config.yaml
DOCKERFILE
else
    cat > "$DOCKERFILE" << DOCKERFILE
FROM ${BASE_IMAGE}
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y procps curl && rm -rf /var/lib/apt/lists/*
COPY dist/deb/mygramdb_${VERSION}-1~${DISTRO}_${DEB_ARCH}.deb /tmp/mygramdb.deb
RUN apt-get update && apt-get install -y /tmp/mygramdb.deb && rm -f /tmp/mygramdb.deb && rm -rf /var/lib/apt/lists/*
COPY config-test.yaml /etc/mygramdb/config.yaml
DOCKERFILE
fi

# Copy required files to temp dir
if [ "$PKG_TYPE" = "rpm" ]; then
    PKG_FILE="${ROOT_DIR}/dist/rpm/mygramdb-${VERSION}-1.el${EL_VERSION}.${ARCH}.rpm"
else
    PKG_FILE="${ROOT_DIR}/dist/deb/mygramdb_${VERSION}-1~${DISTRO}_${DEB_ARCH}.deb"
fi

if [ ! -f "$PKG_FILE" ]; then
    echo -e "${RED}Package not found: ${PKG_FILE}${NC}"
    echo -e "${YELLOW}Build the package first with: make pkg-${PKG_TYPE}-${DISTRO}${NC}"
    rm -rf "$TMPDIR"
    exit 1
fi

if [ "$PKG_TYPE" = "rpm" ]; then
    mkdir -p "${TMPDIR}/dist/rpm"
    cp "$PKG_FILE" "${TMPDIR}/dist/rpm/"
else
    mkdir -p "${TMPDIR}/dist/deb"
    cp "$PKG_FILE" "${TMPDIR}/dist/deb/"
fi

TEST_IMAGE="${COMPOSE_PROJECT}:test"

echo "  Building test image..."
docker buildx build \
    --platform "$DOCKER_PLATFORM" \
    --load \
    -t "$TEST_IMAGE" \
    "$TMPDIR" 2>&1 | tail -1

if [ $? -eq 0 ]; then
    pass "Package installed successfully"
else
    fail "Package installation failed"
    rm -rf "$TMPDIR"
    exit 1
fi

rm -rf "$TMPDIR"

# =============================================================================
# Phase 2: Basic Tests (no MySQL needed)
# =============================================================================
echo -e "\n${BLUE}── Phase 2: Basic Verification ──${NC}"

# Helper to run command in test container
run_test() {
    docker run --rm --platform "$DOCKER_PLATFORM" "$TEST_IMAGE" bash -c "$1" 2>&1
}

# Test: binary exists
if run_test "test -x /usr/bin/mygramdb" > /dev/null 2>&1; then
    pass "mygramdb binary exists and is executable"
else
    fail "mygramdb binary not found"
fi

if run_test "test -x /usr/bin/mygram-cli" > /dev/null 2>&1; then
    pass "mygram-cli binary exists and is executable"
else
    fail "mygram-cli binary not found"
fi

# Test: --version
VERSION_OUTPUT=$(run_test "/usr/bin/mygramdb --version" 2>&1 || true)
if echo "$VERSION_OUTPUT" | grep -qiE "mygramdb|v?[0-9]+\.[0-9]+"; then
    pass "--version: ${VERSION_OUTPUT}"
else
    fail "--version returned unexpected output: ${VERSION_OUTPUT}"
fi

# Test: --help
HELP_OUTPUT=$(run_test "/usr/bin/mygramdb --help" 2>&1 || true)
if echo "$HELP_OUTPUT" | grep -qi "config\|usage\|options"; then
    pass "--help shows usage information"
else
    fail "--help returned unexpected output"
fi

# Test: -t config-test
CONFIG_TEST_OUTPUT=$(run_test "/usr/bin/mygramdb -t -c /etc/mygramdb/config.yaml" 2>&1 || true)
if echo "$CONFIG_TEST_OUTPUT" | grep -qiE "ok\|valid\|pass\|success\|configuration"; then
    pass "-t config-test: configuration is valid"
else
    # Some versions may return exit 0 with no output on success
    CONFIG_TEST_EXIT=$(docker run --rm --platform "$DOCKER_PLATFORM" "$TEST_IMAGE" bash -c "/usr/bin/mygramdb -t -c /etc/mygramdb/config.yaml; echo EXIT:\$?" 2>&1 | grep "EXIT:" | cut -d: -f2)
    if [ "$CONFIG_TEST_EXIT" = "0" ]; then
        pass "-t config-test: configuration is valid (exit 0)"
    else
        fail "-t config-test failed: ${CONFIG_TEST_OUTPUT}"
    fi
fi

# Test: library dependencies
LDD_OUTPUT=$(run_test "ldd /usr/bin/mygramdb" 2>&1 || true)
if echo "$LDD_OUTPUT" | grep -q "not found"; then
    MISSING=$(echo "$LDD_OUTPUT" | grep "not found")
    fail "Missing library dependencies: ${MISSING}"
else
    pass "All library dependencies satisfied"
fi

# Test: file layout
if run_test "test -f /etc/mygramdb/config.yaml.example || test -f /etc/mygramdb/config.yaml" > /dev/null 2>&1; then
    pass "Config file installed in /etc/mygramdb/"
else
    fail "Config file not found in /etc/mygramdb/"
fi

if run_test "test -d /var/lib/mygramdb" > /dev/null 2>&1; then
    pass "Data directory /var/lib/mygramdb exists"
else
    fail "Data directory /var/lib/mygramdb not found"
fi

if run_test "test -d /var/log/mygramdb" > /dev/null 2>&1; then
    pass "Log directory /var/log/mygramdb exists"
else
    fail "Log directory /var/log/mygramdb not found"
fi

# Test: systemd service file
if run_test "test -f /usr/lib/systemd/system/mygramdb.service || test -f /lib/systemd/system/mygramdb.service" > /dev/null 2>&1; then
    pass "Systemd service file installed"
else
    fail "Systemd service file not found"
fi

# Test: mygramdb user created
if run_test "id mygramdb" > /dev/null 2>&1; then
    pass "mygramdb user exists"
else
    fail "mygramdb user not found"
fi

# =============================================================================
# Phase 3: Integration Tests (with MySQL)
# =============================================================================
if [ "$INTEGRATION" = false ]; then
    skip "Integration tests (use --integration to enable)"
else
    echo -e "\n${BLUE}── Phase 3: Integration Tests (MySQL + Server) ──${NC}"

    # Create network
    docker network create "$NETWORK_NAME" 2>/dev/null || true

    # Start MySQL
    echo "  Starting MySQL..."
    docker run -d \
        --name "$MYSQL_CONTAINER" \
        --network "$NETWORK_NAME" \
        --network-alias mysql \
        --platform "$DOCKER_PLATFORM" \
        -e MYSQL_ROOT_PASSWORD=root_password \
        -e MYSQL_DATABASE=mydb \
        -e MYSQL_USER=repl_user \
        -e MYSQL_PASSWORD=test_password \
        mysql:8.4 \
        --server-id=1 \
        --log-bin=mysql-bin \
        --gtid-mode=ON \
        --enforce-gtid-consistency=ON \
        --binlog-format=ROW \
        --binlog-row-image=FULL \
        --character-set-server=utf8mb4 \
        --collation-server=utf8mb4_unicode_ci \
        --mysql-native-password=ON \
        > /dev/null 2>&1

    # Wait for MySQL to be ready
    echo "  Waiting for MySQL to be ready..."
    MYSQL_READY=false
    for i in $(seq 1 60); do
        if docker exec "$MYSQL_CONTAINER" mysqladmin ping -h localhost -u root -proot_password 2>/dev/null | grep -q "alive"; then
            MYSQL_READY=true
            break
        fi
        sleep 2
    done

    if [ "$MYSQL_READY" = false ]; then
        fail "MySQL failed to start within 120s"
    else
        pass "MySQL is ready"

        # Create test table and insert data
        echo "  Creating test table..."
        docker exec "$MYSQL_CONTAINER" mysql -u root -proot_password mydb -e "
            CREATE TABLE IF NOT EXISTS articles (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
                title VARCHAR(255) NOT NULL,
                content TEXT NOT NULL,
                status INT NOT NULL DEFAULT 1,
                category VARCHAR(50),
                enabled TINYINT NOT NULL DEFAULT 1,
                created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                deleted_at DATETIME NULL DEFAULT NULL,
                PRIMARY KEY (id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            INSERT INTO articles (title, content, status, category, enabled) VALUES
                ('Welcome to MygramDB', 'MygramDB is a high-performance in-memory full-text search engine.', 1, 'announcement', 1),
                ('日本語テスト', 'これは日本語の全文検索テストです。MygramDBは高速です。', 1, 'test', 1),
                ('Performance Guide', 'Optimize your search performance with proper n-gram configuration.', 1, 'guide', 1);
        " 2>/dev/null

        # Grant replication privileges and ensure mysql_native_password auth
        docker exec "$MYSQL_CONTAINER" mysql -u root -proot_password -e "
            ALTER USER 'repl_user'@'%' IDENTIFIED WITH mysql_native_password BY 'test_password';
            GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'repl_user'@'%';
            GRANT SELECT ON mydb.* TO 'repl_user'@'%';
            FLUSH PRIVILEGES;
        " 2>/dev/null

        pass "Test data loaded into MySQL"

        # Start MygramDB (must run as non-root; mygramdb user created by package)
        echo "  Starting MygramDB..."
        docker run -d \
            --name "$APP_CONTAINER" \
            --network "$NETWORK_NAME" \
            --platform "$DOCKER_PLATFORM" \
            --user mygramdb \
            "$TEST_IMAGE" \
            /usr/bin/mygramdb -c /etc/mygramdb/config.yaml \
            > /dev/null 2>&1

        # Wait for MygramDB to be ready
        echo "  Waiting for MygramDB to be ready..."
        APP_READY=false
        for i in $(seq 1 60); do
            HEALTH=$(docker exec "$APP_CONTAINER" curl -sf http://localhost:8080/health/live 2>/dev/null || true)
            if echo "$HEALTH" | grep -q "alive"; then
                APP_READY=true
                break
            fi
            # Check if container is still running
            if ! docker ps -q -f "name=$APP_CONTAINER" | grep -q .; then
                echo -e "  ${RED}Container stopped unexpectedly${NC}"
                docker logs "$APP_CONTAINER" 2>&1 | tail -20
                break
            fi
            sleep 2
        done

        if [ "$APP_READY" = false ]; then
            fail "MygramDB failed to start within 120s"
            echo -e "  ${YELLOW}Container logs:${NC}"
            docker logs "$APP_CONTAINER" 2>&1 | tail -20
        else
            pass "MygramDB is alive (HTTP /health/live)"

            # Test: readiness
            sleep 5  # Allow initial snapshot to load
            READY_WAIT=0
            READY_OK=false
            while [ $READY_WAIT -lt 60 ]; do
                READY=$(docker exec "$APP_CONTAINER" curl -sf http://localhost:8080/health/ready 2>/dev/null || true)
                if echo "$READY" | grep -q '"ready"'; then
                    READY_OK=true
                    break
                fi
                sleep 3
                READY_WAIT=$((READY_WAIT + 3))
            done

            if [ "$READY_OK" = true ]; then
                pass "MygramDB is ready (HTTP /health/ready)"
            else
                fail "MygramDB did not become ready within 60s"
                # Continue with other tests anyway
            fi

            # Test: health detail
            DETAIL=$(docker exec "$APP_CONTAINER" curl -sf http://localhost:8080/health/detail 2>/dev/null || true)
            if echo "$DETAIL" | grep -qE '"status"'; then
                pass "Health detail endpoint responds"
            else
                fail "Health detail endpoint failed"
            fi

            # Test: CLI search (via TCP)
            if [ "$READY_OK" = true ]; then
                # Test search via mygram-cli
                CLI_SEARCH=$(docker exec "$APP_CONTAINER" /usr/bin/mygram-cli -h 127.0.0.1 -p 11016 SEARCH articles MygramDB 2>&1 || true)
                if echo "$CLI_SEARCH" | grep -qiE "result|hit|found|MygramDB|id"; then
                    pass "CLI SEARCH returned results"
                else
                    fail "CLI SEARCH failed: ${CLI_SEARCH}"
                fi

                # Test search with Japanese
                CLI_JP=$(docker exec "$APP_CONTAINER" /usr/bin/mygram-cli -h 127.0.0.1 -p 11016 SEARCH articles 日本語 2>&1 || true)
                if echo "$CLI_JP" | grep -qiE "result|hit|found|id"; then
                    pass "CLI SEARCH with Japanese returned results"
                else
                    fail "CLI SEARCH with Japanese failed: ${CLI_JP}"
                fi

                # Test INFO command
                CLI_INFO=$(docker exec "$APP_CONTAINER" /usr/bin/mygram-cli -h 127.0.0.1 -p 11016 INFO 2>&1 || true)
                if echo "$CLI_INFO" | grep -qiE "version|uptime|index|table|articles"; then
                    pass "CLI INFO command works"
                else
                    fail "CLI INFO command failed: ${CLI_INFO}"
                fi
            else
                skip "CLI tests (server not ready)"
            fi
        fi
    fi
fi

# =============================================================================
# Summary
# =============================================================================
echo -e "\n${BLUE}══════════════════════════════════════════════${NC}"
TOTAL=$((PASS + FAIL + SKIP))
echo -e "  Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}, ${YELLOW}${SKIP} skipped${NC} (${TOTAL} total)"

if [ "$FAIL" -gt 0 ]; then
    echo -e "  ${RED}VERIFICATION FAILED${NC}"
    echo -e "${BLUE}══════════════════════════════════════════════${NC}"
    exit 1
else
    echo -e "  ${GREEN}VERIFICATION PASSED${NC}"
    echo -e "${BLUE}══════════════════════════════════════════════${NC}"
    exit 0
fi
