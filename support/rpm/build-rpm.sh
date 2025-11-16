#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== MygramDB RPM Build Script ===${NC}"

# Get version from spec file
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
VERSION=$(grep "^Version:" "${SCRIPT_DIR}/mygramdb.spec" | awk '{print $2}')
NAME="mygramdb"

echo -e "${YELLOW}Building ${NAME}-${VERSION}${NC}"

# Create source tarball
echo -e "${YELLOW}Creating source tarball...${NC}"
TARBALL="${NAME}-${VERSION}.tar.gz"
git archive --format=tar.gz --prefix="${NAME}-${VERSION}/" HEAD > "/root/rpmbuild/SOURCES/${TARBALL}"

# Copy spec file
cp "${SCRIPT_DIR}/mygramdb.spec" /root/rpmbuild/SPECS/

# Build RPM
echo -e "${YELLOW}Building RPM package...${NC}"
cd /root/rpmbuild/SPECS
rpmbuild -ba mygramdb.spec

# Check if build succeeded
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ RPM build successful!${NC}"

    # List generated packages
    echo -e "\n${GREEN}Generated packages:${NC}"
    ls -lh /root/rpmbuild/RPMS/x86_64/
    ls -lh /root/rpmbuild/SRPMS/

    # Copy packages to output directory
    mkdir -p /workspace/dist/rpm
    cp /root/rpmbuild/RPMS/x86_64/*.rpm /workspace/dist/rpm/ 2>/dev/null || true
    cp /root/rpmbuild/SRPMS/*.rpm /workspace/dist/rpm/ 2>/dev/null || true

    echo -e "\n${GREEN}Packages copied to dist/rpm/${NC}"
    ls -lh /workspace/dist/rpm/
else
    echo -e "${RED}✗ RPM build failed${NC}"
    exit 1
fi
