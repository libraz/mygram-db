# Development Dockerfile for Linux environment testing
# This mirrors the GitHub Actions CI environment (Ubuntu 22.04)
# Use this to catch platform-specific issues before pushing to CI

FROM ubuntu:22.04

# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install all CI dependencies (matching .github/workflows/ci.yml)
RUN apt-get update && apt-get install -y \
    # Build essentials
    build-essential \
    cmake \
    git \
    pkg-config \
    ccache \
    # MygramDB dependencies
    libmysqlclient-dev \
    libicu-dev \
    libreadline-dev \
    liblz4-dev \
    # Testing and coverage
    lcov \
    # LLVM/Clang for linting
    wget \
    lsb-release \
    software-properties-common \
    gnupg \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Install LLVM/Clang 18 (same as CI)
RUN wget https://apt.llvm.org/llvm.sh && \
    chmod +x llvm.sh && \
    ./llvm.sh 18 && \
    apt-get install -y clang-format-18 clang-tidy-18 && \
    rm llvm.sh && \
    update-alternatives --install /usr/bin/clang-format clang-format /usr/bin/clang-format-18 100 && \
    update-alternatives --install /usr/bin/clang-tidy clang-tidy /usr/bin/clang-tidy-18 100 && \
    rm -rf /var/lib/apt/lists/*

# Configure ccache
RUN ccache --set-config=max_size=500M

# Set working directory
WORKDIR /workspace

# Default compiler (matching CI)
ENV CC=ccache gcc
ENV CXX=ccache g++

# Default command: show help
CMD ["bash", "-c", "echo 'MygramDB Linux Development Environment'; \
     echo ''; \
     echo 'Available commands:'; \
     echo '  make build         - Build the project'; \
     echo '  make test          - Run tests'; \
     echo '  make format-check  - Check code formatting'; \
     echo '  make lint          - Run clang-tidy'; \
     echo '  make clean         - Clean build directory'; \
     echo ''; \
     echo 'Example: docker run --rm -v \$(pwd):/workspace mygramdb-dev make build'"]
