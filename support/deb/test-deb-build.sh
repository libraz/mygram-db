#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Parse command line arguments
ARCH=""
BUILD_ALL=false
UBUNTU_VERSION="22.04"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ubuntu-version)
            UBUNTU_VERSION="$2"
            shift 2
            ;;
        --all)
            BUILD_ALL=true
            shift
            ;;
        *)
            ARCH="$1"
            shift
            ;;
    esac
done

# Normalize architecture (macOS returns arm64, RPM/DEB uses aarch64)
DETECTED_ARCH=$(uname -m)
if [ "$DETECTED_ARCH" = "arm64" ]; then
    DETECTED_ARCH="aarch64"
fi
ARCH="${ARCH:-$DETECTED_ARCH}"

# Map uname arch to deb/docker conventions
map_arch_to_deb() {
    case "$1" in
        x86_64)  echo "amd64" ;;
        aarch64) echo "arm64" ;;
        *)       echo "$1" ;;
    esac
}

# Map codename
get_codename() {
    case "$1" in
        22.04) echo "jammy" ;;
        24.04) echo "noble" ;;
        *)     echo "unknown" ;;
    esac
}

CODENAME=$(get_codename "$UBUNTU_VERSION")

echo -e "${BLUE}╔════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║  MygramDB DEB Build Verification      ║${NC}"
echo -e "${BLUE}║  Ubuntu: ${UBUNTU_VERSION} (${CODENAME})                  ║${NC}"
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
    local DEB_ARCH=$(map_arch_to_deb "$TARGET_ARCH")

    # Map architecture to Docker platform
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

    echo -e "${BLUE}Building for architecture: $TARGET_ARCH ($DEB_ARCH), Ubuntu ${UBUNTU_VERSION} (${CODENAME})${NC}\n"

    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    ROOT_DIR="$(git rev-parse --show-toplevel)"

    # Build Docker image
    echo -e "${YELLOW}Building DEB builder Docker image...${NC}"
    docker buildx build \
        --platform "$DOCKER_PLATFORM" \
        --build-arg UBUNTU_VERSION="${UBUNTU_VERSION}" \
        --load \
        -f "${SCRIPT_DIR}/Dockerfile.debbuild" \
        -t mygramdb-debbuild:${CODENAME}-$TARGET_ARCH \
        "$ROOT_DIR"

    if [ $? -ne 0 ]; then
        echo -e "${RED}✗ Failed to build Docker image for $TARGET_ARCH (${CODENAME})${NC}"
        return 1
    fi

    echo -e "${GREEN}✓ Docker image built successfully${NC}\n"

    # Create dist directory and source tarball
    mkdir -p "$ROOT_DIR/dist/deb"

    VERSION=$(git describe --tags --abbrev=0 2>/dev/null | sed 's/^v//' || echo "0.0.0")
    NAME="mygramdb"
    TARBALL="${NAME}-${VERSION}.tar.gz"

    echo -e "${YELLOW}Creating source tarball...${NC}"
    mkdir -p "$ROOT_DIR/dist/sources"
    git -C "$ROOT_DIR" archive --format=tar.gz --prefix="${NAME}-${VERSION}/" HEAD \
        > "$ROOT_DIR/dist/sources/${TARBALL}"

    # Run DEB build in container
    echo -e "${YELLOW}Running DEB build in container...${NC}"
    echo -e "${BLUE}Version: ${VERSION}, Codename: ${CODENAME}${NC}\n"

    docker run --rm \
        --platform "$DOCKER_PLATFORM" \
        -e MYGRAMDB_VERSION="${VERSION}" \
        -v "$ROOT_DIR/dist/sources:/sources:ro" \
        -v "$ROOT_DIR/dist:/workspace/dist" \
        mygramdb-debbuild:${CODENAME}-$TARGET_ARCH

    if [ $? -eq 0 ]; then
        echo -e "\n${GREEN}╔════════════════════════════════════════╗${NC}"
        echo -e "${GREEN}║  DEB Build Complete for $TARGET_ARCH (${CODENAME}) ✓${NC}"
        echo -e "${GREEN}╚════════════════════════════════════════╝${NC}\n"

        echo -e "${BLUE}Generated DEB packages:${NC}"
        ls -lh "$ROOT_DIR/dist/deb/"*.deb 2>/dev/null || true

        echo -e "\n${YELLOW}To test the DEB package:${NC}"
        echo -e "  docker run --rm -it --platform $DOCKER_PLATFORM -v \$(git rev-parse --show-toplevel)/dist:/dist ubuntu:${UBUNTU_VERSION} bash"
        echo -e "  # Inside container:"
        echo -e "  apt-get update && apt-get install -y /dist/deb/mygramdb_*.deb"
        echo -e "  mygramdb --version"
        return 0
    else
        echo -e "${RED}✗ DEB build failed for $TARGET_ARCH (${CODENAME})${NC}"
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

        echo -e "${BLUE}All generated DEB packages:${NC}"
        ls -lh "$(git rev-parse --show-toplevel)/dist/deb/"*.deb
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
