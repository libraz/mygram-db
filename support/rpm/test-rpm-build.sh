#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  MygramDB RPM Build Verification      ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}\n"

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker is not installed${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker is available${NC}\n"

# Build Docker image for RPM building
echo -e "${YELLOW}Building RPM builder Docker image...${NC}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
docker build -f "${SCRIPT_DIR}/Dockerfile.rpmbuild" -t mygramdb-rpmbuild "$(git rev-parse --show-toplevel)"

if [ $? -ne 0 ]; then
    echo -e "${RED}✗ Failed to build Docker image${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker image built successfully${NC}\n"

# Create dist directory
mkdir -p dist/rpm

# Get version from git tag or use default
VERSION=$(git describe --tags --abbrev=0 2>/dev/null | sed 's/^v//' || echo "0.0.0")

# Run RPM build in container
echo -e "${YELLOW}Running RPM build in container...${NC}"
echo -e "${BLUE}Version: ${VERSION}${NC}\n"
docker run --rm \
    -e MYGRAMDB_VERSION="${VERSION}" \
    -v "$(pwd)/dist:/workspace/dist" \
    mygramdb-rpmbuild

if [ $? -eq 0 ]; then
    echo -e "\n${GREEN}╔════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║  RPM Build Verification Complete ✓    ║${NC}"
    echo -e "${GREEN}╚════════════════════════════════════════╝${NC}\n"

    echo -e "${BLUE}Generated RPM packages:${NC}"
    ls -lh dist/rpm/*.rpm

    echo -e "\n${YELLOW}To test the RPM package:${NC}"
    echo -e "  docker run --rm -it -v \$(pwd)/dist:/dist rockylinux:9 bash"
    echo -e "  # Inside container:"
    echo -e "  dnf install -y /dist/rpm/mygramdb-*.x86_64.rpm"
    echo -e "  mygramdb --version"
else
    echo -e "${RED}✗ RPM build failed${NC}"
    exit 1
fi
