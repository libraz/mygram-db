# Multi-stage build for MygramDB
# Stage 1: Builder - Build the application
FROM ubuntu:22.04 AS builder

# Prevent interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

# Install build dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    pkg-config \
    libmysqlclient-dev \
    libicu-dev \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /build

# Copy source code
COPY . .

# Build the project
RUN mkdir -p build && \
    cd build && \
    cmake -DCMAKE_BUILD_TYPE=Release \
          -DBUILD_TESTS=OFF \
          -DUSE_ICU=ON \
          -DUSE_MYSQL=ON \
          -DCMAKE_INSTALL_PREFIX=/usr/local \
          .. && \
    make -j$(nproc) && \
    make install DESTDIR=/install

# Stage 2: Runtime - Minimal runtime image
FROM ubuntu:22.04

# Install runtime dependencies only
RUN apt-get update && apt-get install -y \
    libstdc++6 \
    libmysqlclient21 \
    libicu70 \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for running the application
RUN groupadd -g 1000 mygramdb && \
    useradd -r -u 1000 -g mygramdb -s /bin/bash mygramdb

# Copy binaries and files from builder
COPY --from=builder /install/usr/local /usr/local

# Create directories for data and configuration
RUN mkdir -p /var/lib/mygramdb /etc/mygramdb && \
    chown -R mygramdb:mygramdb /var/lib/mygramdb /etc/mygramdb

# Copy configuration example
COPY --from=builder /build/examples/config-minimal.yaml /etc/mygramdb/config.yaml.example

# Copy entrypoint script
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Switch to non-root user
USER mygramdb

# Set working directory
WORKDIR /var/lib/mygramdb

# Expose default port (adjust as needed)
EXPOSE 11016

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD pgrep -x mygramdb || exit 1

# Entrypoint script handles configuration generation from env vars
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Default command
CMD ["mygramdb"]
