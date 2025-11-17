#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse command line arguments
ARCH="${1:-$(uname -m)}"
BUILD_ALL=false

if [ "$ARCH" = "--all" ]; then
    BUILD_ALL=true
fi

echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  MygramDB RPM Build Verification      ║${NC}"
echo -e "${BLUE}╚════════════════════════════════════════╝${NC}\n"

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    echo -e "${RED}✗ Docker is not installed${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Docker is available${NC}\n"

# Function to build for a specific architecture
build_for_arch() {
    local TARGET_ARCH=$1
    local DOCKER_PLATFORM=""

    # Map RPM architecture to Docker platform
    case "$TARGET_ARCH" in
        x86_64)
            DOCKER_PLATFORM="linux/amd64"
            ;;
        aarch64)
            DOCKER_PLATFORM="linux/arm64"
            ;;
        *)
            echo -e "${RED}✗ Unsupported architecture: $TARGET_ARCH${NC}"
            echo -e "${YELLOW}Supported architectures: x86_64, aarch64${NC}"
            return 1
            ;;
    esac

    echo -e "${BLUE}Building for architecture: $TARGET_ARCH (platform: $DOCKER_PLATFORM)${NC}\n"

    # Build Docker image for RPM building
    echo -e "${YELLOW}Building RPM builder Docker image for $TARGET_ARCH...${NC}"
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    ROOT_DIR="$(git rev-parse --show-toplevel)"

    docker buildx build \
        --platform "$DOCKER_PLATFORM" \
        --load \
        -f "${SCRIPT_DIR}/Dockerfile.rpmbuild" \
        -t mygramdb-rpmbuild:$TARGET_ARCH \
        "$ROOT_DIR"

    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Failed to build Docker image for $TARGET_ARCH${NC}"
        return 1
    fi

    echo -e "${GREEN}✓ Docker image built successfully for $TARGET_ARCH${NC}\n"

    # Create dist directory
    mkdir -p "$ROOT_DIR/dist/rpm"

    # Get version from git tag or use default
    VERSION=$(git describe --tags --abbrev=0 2>/dev/null | sed 's/^v//' || echo "0.0.0")

    # Run RPM build in container
    echo -e "${YELLOW}Running RPM build in container for $TARGET_ARCH...${NC}"
    echo -e "${BLUE}Version: ${VERSION}${NC}\n"

    docker run --rm \
        --platform "$DOCKER_PLATFORM" \
        -e MYGRAMDB_VERSION="${VERSION}" \
        -v "$ROOT_DIR/dist:/workspace/dist" \
        mygramdb-rpmbuild:$TARGET_ARCH

    if [ $? -eq 0 ]; then
        echo -e "\n${GREEN}╔════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║  RPM Build Complete for $TARGET_ARCH ✓${NC}"
        echo -e "${GREEN}╚════════════════════════════════════════╝${NC}\n"

        echo -e "${BLUE}Generated RPM packages for $TARGET_ARCH:${NC}"
        ls -lh "$ROOT_DIR/dist/rpm/"*.$TARGET_ARCH.rpm 2>/dev/null || ls -lh "$ROOT_DIR/dist/rpm/"*.src.rpm 2>/dev/null || true

        echo -e "\n${YELLOW}To test the RPM package:${NC}"
        echo -e "  docker run --rm -it --platform $DOCKER_PLATFORM -v \$(git rev-parse --show-toplevel)/dist:/dist rockylinux:9 bash"
        echo -e "  # Inside container:"
        echo -e "  dnf install -y https://dev.mysql.com/get/mysql84-community-release-el9-1.noarch.rpm"
        echo -e "  dnf module disable -y mysql"
        echo -e "  dnf install -y /dist/rpm/mygramdb-*.$TARGET_ARCH.rpm"
        echo -e "  mygramdb --version"
        return 0
    else
        echo -e "${RED}✗ RPM build failed for $TARGET_ARCH${NC}"
        return 1
    fi
}

# Main execution
if [ "$BUILD_ALL" = true ]; then
    echo -e "${BLUE}Building for all architectures: x86_64, aarch64${NC}\n"

    FAILED_BUILDS=()

    for arch in x86_64 aarch64; do
        echo -e "\n${BLUE}═══════════════════════════════════════${NC}"
        echo -e "${BLUE}Building for $arch${NC}"
        echo -e "${BLUE}═══════════════════════════════════════${NC}\n"

        if ! build_for_arch "$arch"; then
            FAILED_BUILDS+=("$arch")
        fi
    done

    echo -e "\n${BLUE}╔════════════════════════════════════════╗${NC}"
    if [ ${#FAILED_BUILDS[@]} -eq 0 ]; then
        echo -e "${GREEN}║  All Builds Completed Successfully ✓  ║${NC}"
        echo -e "${BLUE}╚════════════════════════════════════════╝${NC}\n"

        echo -e "${BLUE}All generated RPM packages:${NC}"
        ls -lh "$(git rev-parse --show-toplevel)/dist/rpm/"*.rpm
        exit 0
    else
        echo -e "${RED}║  Some Builds Failed ✗                 ║${NC}"
        echo -e "${BLUE}╚════════════════════════════════════════╝${NC}\n"

        echo -e "${RED}Failed architectures: ${FAILED_BUILDS[*]}${NC}"
        exit 1
    fi
else
    build_for_arch "$ARCH"
fi
